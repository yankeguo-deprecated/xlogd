package main

import (
	"context"
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
	"github.com/tidwall/redcon"
)

var (
	optionsFile string
	options     Options
	dev         bool

	server *redcon.Server
	client *elastic.Client

	records       chan Record
	limiter       *ratelimit.Bucket
	totalConns    int64
	connsSum      = map[string]int{}
	connsSumMutex = &sync.Mutex{}
	totalCount    int64

	shutdown      bool
	shutdownGroup = &sync.WaitGroup{}

	hostname string
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

func checkRecordKeyword(r Record) bool {
	if strSliceContains(options.EnforceKeyword, r.Topic) && len(r.Keyword) == 0 {
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
	if record, ok := event.ToRecord(); ok {
		// check should keyword be enforced
		if checkRecordKeyword(record) {
			// insert into channel
			records <- record
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
		// return error if full
		if len(records) >= options.Capacity {
			time.Sleep(time.Second * 3)
			conn.WriteError("ERR out of capacity")
			return
		}
		// retrieve all events
		for _, raw := range cmd.Args[2:] {
			consumeRawEvent(raw)
		}
		conn.WriteInt(len(records))
	case "llen":
		conn.WriteInt(len(records))
	}
}

func closedHandlerFunc(conn redcon.Conn, err error) {
	log.Info().Err(err).Int64("conns", atomic.AddInt64(&totalConns, -1)).Int("conns-dup", decreaseConnsSum(conn.RemoteAddr())).Str("addr", conn.RemoteAddr()).Msg("connection closed")
}

func outputRoutine() {
	shutdownGroup.Add(1)
	defer shutdownGroup.Done()

	for {
		// force GC
		runtime.GC()

		// check for the outputExiting
		if shutdown && len(records) == 0 {
			break
		}

		// determine size
		l := len(records)

		if l > options.Elasticsearch.Batch.Size {
			// adjust size
			l = options.Elasticsearch.Batch.Size
		} else {
			// wait for 100 msec, slow down loop
			time.Sleep(time.Millisecond * 100)
		}

		// skip if no records
		if l == 0 {
			continue
		}

		// wait for the limiter, slow down loop
		limiter.Wait(int64(l))

		// create bulk
		bs := client.Bulk()

		// add bulk operation
		for i := 0; i < l; i++ {
			// take record
			r := <-records
			// increase total count
			atomic.AddInt64(&totalCount, 1)
			// fix time offset if needed
			if !r.NoTimeOffset {
				r.Timestamp = r.Timestamp.Add(time.Hour * time.Duration(options.TimeOffset))
			}
			// create request
			br := elastic.NewBulkIndexRequest().Index(r.Index()).Type("_doc").Doc(r.Map())
			log.Debug().Msg("new bulk request:\n" + br.String())
			// append request to bulk
			bs = bs.Add(br)
		}

		// do the bulk operation
		if _, err := bs.Do(context.Background()); err != nil {
			time.Sleep(500 * time.Millisecond)
			log.Info().Err(err).Msg("failed to bulk insert")
		}
		log.Debug().Msg("bulk committed")
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
			RecordsQueued: int64(len(records)),
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
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, NoColor: true})

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

	// create elasticsearch client
	if client, err = elastic.NewClient(elastic.SetURL(options.Elasticsearch.URLs...)); err != nil {
		log.Error().Err(err).Msg("failed to create elasticsearch client")
		os.Exit(1)
		return
	}

	// allocate records chan
	records = make(chan Record, options.Capacity)

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
	log.Info().Str("bind", options.Bind).Msg("server started")

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
	log.Info().Msg("output queue drained, exiting")
}
