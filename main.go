package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"flag"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juju/ratelimit"
	"github.com/olivere/elastic"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/yankeguo/diskqueue"
	"github.com/yankeguo/redcon"
)

var (
	optionsFile string
	options     Options
	dev         bool

	server *redcon.Server
	client *elastic.Client

	queue diskqueue.DiskQueue

	limiter       *ratelimit.Bucket
	totalConns    int64
	connsSum      = map[string]int{}
	connsSumMutex = &sync.Mutex{}
	totalCount    int64

	shutdown      bool
	shutdownGroup = &sync.WaitGroup{}

	hostname string
)

var (
	// Version version of xlogd, build time variable
	Version = "unknown"
)

func increaseConnsSum(addr string) int {
	connsSumMutex.Lock()
	defer connsSumMutex.Unlock()
	i := extractIP(addr)
	connsSum[i] = connsSum[i] + 1
	return connsSum[i]
}

func decreaseConnsSum(addr string) int {
	connsSumMutex.Lock()
	defer connsSumMutex.Unlock()
	i := extractIP(addr)
	connsSum[i] = connsSum[i] - 1
	return connsSum[i]
}

func acceptHandlerFunc(conn redcon.Conn) bool {
	log.Info().Int64("conns", atomic.AddInt64(&totalConns, 1)).Int("conns-dup", increaseConnsSum(conn.RemoteAddr())).Str("addr", conn.RemoteAddr()).Msg("connection established")
	return true
}

func checkRecordTopic(r Record) bool {
	if stringSliceContainsIgnoreCase(options.EnforceKeyword, r.Topic) && len(r.Keyword) == 0 {
		return false
	}
	if stringSliceContainsIgnoreCase(options.Ignore, r.Topic) {
		return false
	}
	return true
}

func consumeRawEvent(raw []byte) {
	// ignore event > 1mb
	if len(raw) > 1000000 {
		return
	}
	// warn event > 500k
	if len(raw) > 500000 {
		log.Warn().Int("raw-length", len(raw)).Msg("raw message larger than 500k")
	}
	log.Debug().Int("raw-length", len(raw)).Msg("raw message")
	// unmarshal event
	var event Event
	if err := json.Unmarshal(raw, &event); err != nil {
		log.Debug().Err(err).Str("event", string(raw)).Msg("failed to unmarshal event")
		return
	}
	// convert to record
	if record, ok := event.ToRecord(options.TimeOffset); ok {
		// check should keyword be enforced
		if checkRecordTopic(record) {
			// convert to operation
			o := record.ToOperation()
			// encode operation
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(o); err != nil {
				log.Error().Err(err).Msg("failed to encode operation")
				return
			}
			queue.Put(buf.Bytes())
		}
	} else {
		log.Debug().Str("event", string(raw)).Msg("failed to convert record")
	}
}

func commandHandlerFunc(conn redcon.Conn, cmd redcon.Command) {
	// empty arguments, not possible
	if len(cmd.Args) == 0 {
		conn.WriteError("ERR bad command")
		return
	}
	// extract command
	command := strings.ToLower(string(cmd.Args[0]))
	log.Debug().Str("addr", conn.RemoteAddr()).Str("cmd", command).Int("args", len(cmd.Args)-1).Msg("new command")
	// handle command
	switch command {
	default:
		log.Error().Str("command", command).Msg("unknown message")
		conn.WriteError("ERR unknown command '" + command + "'")
	case "ping":
		conn.WriteString("PONG")
	case "quit":
		conn.WriteString("OK")
		conn.Close()
	case "info":
		if options.Multi {
			// declare as redis 2.4+, supports multiple values in RPUSH/LPUSH
			conn.WriteString("redis_version:2.4")
		} else {
			// declare as redis 2.4-, not support multiple values in RPUSH/LPUSH
			conn.WriteString("redis_version:2.3")
		}
	case "rpush", "lpush":
		// at least 3 arguments, RPUSH xlog "{....}"
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR bad command '" + command + "'")
			return
		}
		// retrieve all events
		for _, raw := range cmd.Args[2:] {
			consumeRawEvent(raw)
		}
		conn.WriteInt64(queue.Depth())
	case "llen":
		conn.WriteInt64(queue.Depth())
	}
}

func closedHandlerFunc(conn redcon.Conn, err error) {
	log.Info().Err(err).Int64("conns", atomic.AddInt64(&totalConns, -1)).Int("conns-dup", decreaseConnsSum(conn.RemoteAddr())).Str("addr", conn.RemoteAddr()).Msg("connection closed")
}

func outputRoutine() {
	shutdownGroup.Add(1)
	defer shutdownGroup.Done()

	// create the queue read channel
	records := queue.ReadChan()

	for {
		// force GC
		runtime.GC()

		// check for the outputExiting
		if shutdown {
			break
		}

		// c counter
		var c int

		// build the bulk
		bs := client.Bulk()

		// timer for 5 seconds
		timer := time.NewTimer(time.Second * 3)

	FOR_LOOP:
		for {
			select {
			case buf := <-records:
				{
					// decode operation
					var o Operation
					if err := gob.NewDecoder(bytes.NewReader(buf)).Decode(&o); err != nil {
						continue FOR_LOOP
					}
					// increase counter
					c++
					// increase total counter
					atomic.AddInt64(&totalCount, 1)
					// create request
					br := elastic.NewBulkIndexRequest().Index(o.Index).Type("_doc").Doc(string(o.Body))
					log.Debug().Msg("new bulk request:\n" + br.String())
					// append request to bulk
					bs = bs.Add(br)
					// break the loop if batch size exceeded
					if c > options.Elasticsearch.Batch.Size {
						log.Debug().Msg("batch size exceeded")
						break FOR_LOOP
					}
				}
			case <-timer.C:
				{
					// break the loop if timeout exceeded
					log.Debug().Msg("batch timeout exceeded")
					break FOR_LOOP
				}
			}
		}

		// clear the timer
		timer.Stop()

		// continue if no records
		if c == 0 {
			continue
		}

		// do the bulk operation
		if _, err := bs.Do(context.Background()); err != nil {
			time.Sleep(500 * time.Millisecond)
			log.Info().Err(err).Msg("failed to bulk insert")
		}
		log.Debug().Msg("bulk committed")

		// slow down loop with limiter
		limiter.Wait(int64(c))
	}
}

func statsRoutine() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		// record current totalCount
		count := totalCount
		// wait for next tick
		<-ticker.C
		// create stats
		r := Stats{
			Timestamp:     time.Now(),
			Hostname:      hostname,
			RecordsTotal:  totalCount,
			Records1M:     totalCount - count,
			RecordsQueued: queue.Depth(),
		}
		// insert stats
		if _, err := client.Index().Index(r.Index()).Type("_doc").BodyJson(&r).Do(context.Background()); err != nil {
			log.Error().Err(err).Msg("failed to write stats")
		} else {
			log.Info().Interface("stats", &r).Msg("stats collected")
		}
	}
}

func waitForSignal() {
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	sig := <-shutdown
	log.Info().Str("signal", sig.String()).Msg("signal caught")
}

func main() {
	var err error

	// collect hostname
	hostname, _ = os.Hostname()

	// init logger
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, NoColor: true, TimeFormat: time.RFC3339})

	// decode command line arguments
	flag.StringVar(&optionsFile, "c", "/etc/xlogd.yml", "config file")
	flag.BoolVar(&dev, "dev", false, "enable dev mode")
	flag.Parse()

	// load options
	log.Info().Str("file", optionsFile).Msg("load options file")
	if options, err = LoadOptions(optionsFile); err != nil {
		log.Error().Err(err).Msg("failed to load options file")
		os.Exit(1)
		return
	}

	// set dev from command line arguments
	if dev {
		options.Dev = true
	}

	// re-init logger
	if options.Dev {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}

	// ensure data dir
	if err = os.MkdirAll(options.DataDir, 0755); err != nil {
		log.Error().Err(err).Msg("failed to ensure xlog data dir")
		os.Exit(1)
		return
	}

	// create the queue
	queue = diskqueue.New("xlogd", options.DataDir, 256*1024*1024, 20, 2*1024*1024, int64(options.Elasticsearch.Batch.Size), time.Second*20)

	// create elasticsearch client
	if client, err = elastic.NewClient(elastic.SetURL(options.Elasticsearch.URLs...)); err != nil {
		log.Error().Err(err).Msg("failed to create elasticsearch client")
		os.Exit(1)
		return
	}

	// initialize limiter
	limiter = ratelimit.NewBucket(
		time.Second/time.Duration(options.Elasticsearch.Batch.Rate),
		int64(options.Elasticsearch.Batch.Burst),
	)

	// create server
	server = redcon.NewServer(options.Bind, commandHandlerFunc, acceptHandlerFunc, closedHandlerFunc)

	// start the server
	setup := make(chan error, 1)
	go server.ListenServeAndSignal(setup)
	if err = <-setup; err != nil {
		log.Error().Err(err).Msg("failed to start server")
		os.Exit(1)
		return
	}
	log.Info().Str("bind", options.Bind).Str("version", Version).Msg("server started")

	// start outputRoutine
	go outputRoutine()

	// start statsRoutine
	go statsRoutine()

	// wait for SIGINT or SIGTERM
	waitForSignal()

	// close the server
	err = server.Close()
	log.Info().Str("bind", options.Bind).Err(err).Msg("server closed")

	// mark to shutdown and wait for output complete
	shutdown = true
	shutdownGroup.Wait()
	log.Info().Msg("output queue drained")

	// close the queue
	queue.Close()
	log.Info().Msg("queue file closed, exiting")
}
