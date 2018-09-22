package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/olivere/elastic"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	optionsFile string
	options     Options
	dev         bool

	server *redcon.Server
	client *elastic.Client

	records chan Record

	shutdown      bool
	shutdownGroup = &sync.WaitGroup{}
)

func acceptHandlerFunc(conn redcon.Conn) bool {
	log.Info().Str("addr", conn.RemoteAddr()).Msg("connection established")
	return true
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
		conn.WriteError("ERR unknown command '" + command + "'")
	case "ping":
		conn.WriteString("PONG")
	case "quit":
		conn.WriteString("OK")
		conn.Close()
	case "info":
		// declare redis 2.4+, supports multiple value in RPUSH/LPUSH
		conn.WriteString("redis_version:2.4")
	case "rpush", "lpush":
		// at least 3 arguments, RPUSH xlog "{....}"
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR bad command '" + command + "'")
			return
		}
		// retrieve all events
		for _, raw := range cmd.Args[2:] {
			// unmarshal event
			var event Event
			if err := json.Unmarshal(raw, &event); err != nil {
				log.Debug().Err(err).Str("event", string(raw)).Msg("failed to unmarshal event")
				continue
			}
			// convert to record
			if record, ok := event.ToRecord(); ok {
				records <- record
			} else {
				log.Debug().Str("event", string(raw)).Msg("failed to convert record")
			}
		}
		conn.WriteInt(len(records))
	case "llen":
		conn.WriteInt(len(records))
	}
}

func closedHandlerFunc(conn redcon.Conn, err error) {
	log.Info().Err(err).Str("addr", conn.RemoteAddr()).Msg("connection closed")
}

func outputRoutine() {
	shutdownGroup.Add(1)
	defer shutdownGroup.Done()

	// temporary slice of records
	rs := make([]Record, 0, options.Elasticsearch.Batch.Size)

	for {
		// check for the outputExiting
		if shutdown && len(records) == 0 {
			break
		}

		// collect batch of records or wait for timeouts
		tm := time.NewTimer(time.Second * time.Duration(options.Elasticsearch.Batch.Timeout))
	loop:
		for {
			select {
			case r := <-records:
				rs = append(rs, r)
				if len(rs) >= options.Elasticsearch.Batch.Size {
					log.Debug().Msg("batch full")
					break loop
				}
			case <-tm.C:
				log.Debug().Msg("batch timed out")
				break loop
			}
		}
		tm.Stop()

		// continue if no records
		if len(rs) == 0 {
			continue
		}

		// create bulk
		bs := client.Bulk()

		// insert records to elasticsearch
		for _, r := range rs {
			br := elastic.NewBulkIndexRequest().Index(r.Index()).Type("_doc").Doc(r.Map())
			log.Debug().Msg("bulk request:\n" + br.String())
			bs = bs.Add(br)
		}

		// do the bulk operation
		if _, err := bs.Do(context.Background()); err != nil {
			log.Info().Err(err).Msg("failed to bulk insert")
		}
		log.Debug().Msg("bulk committed")

		// clear rs for reuse
		rs = rs[:0]
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
