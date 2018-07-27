package main

import (
	"context"
	"flag"
	"github.com/olivere/elastic"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	optionsFile string
	options     Options

	verboseMode = false

	recordsChan chan Record
	esClient    *elastic.Client

	inputExiting  = false
	inputGroup    = &sync.WaitGroup{}
	outputExiting = false
	outputGroup   = &sync.WaitGroup{}
)

func verbose(s ...interface{}) {
	if verboseMode {
		log.Println(s...)
	}
}

func main() {
	var err error

	// parse flag for options file and dummy mode
	flag.StringVar(&optionsFile, "c", "/etc/xlogd.yml", "config file")
	flag.BoolVar(&verboseMode, "verbose", false, "enable verbose mode, print extra logs")
	flag.Parse()

	// load options
	if options, err = LoadOptions(optionsFile); err != nil {
		panic(err)
	}

	// create elasticsearch client
	if esClient, err = elastic.NewClient(elastic.SetURL(options.Elasticsearch.URLs...)); err != nil {
		panic(err)
		return
	}

	// allocate records chan
	recordsChan = make(chan Record, options.Batch.Size*2*len(options.Redis.URLs))

	// output routines
	go outputRoutine()

	// input routines
	for i := range options.Redis.URLs {
		go inputRoutine(i)
	}

	// wait for SIGINT or SIGTERM
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown

	// wait for all goroutines complete
	log.Println("SIGNAL: exiting...")

	// stop all inputs
	inputExiting = true
	inputGroup.Wait()

	// stop output
	outputExiting = true
	outputGroup.Wait()
}

func inputRoutine(idx int) {
	for {
		// call unsafe input routine
		if err := unsafeInputRoutine(idx); err != nil {
			log.Println(" INPUT: routine failed", idx, err)
		}
		// check shutdown mark
		if inputExiting {
			return
		}
		// sleep 3 seconds and retry
		time.Sleep(time.Second * 3)
	}
}

func unsafeInputRoutine(idx int) (err error) {
	// manage inputGroup
	inputGroup.Add(1)
	defer inputGroup.Done()
	// logging
	log.Println(" INPUT: routine created", idx)
	defer log.Println(" INPUT: routine exited", idx)
	// create redis client
	var rd *Redis
	if rd, err = DialRedis(options.Redis.URLs[idx], options.Redis.Key); err != nil {
		return
	}
	defer rd.Close()
	// BLPOP loop
	var (
		e  Event
		r  Record
		ok bool
	)
	for {
		// check shutdown mark
		if inputExiting {
			verbose(" INPUT: exiting due to shutdown mark", idx)
			return
		}
		// next event
		if e, ok, err = rd.NextEvent(); err != nil {
			verbose(" INPUT: exiting due to error", idx, err)
			return
		} else if !ok {
			verbose(" INPUT: not retrieved", idx)
			continue
		}
		// convert to record
		if r, ok = e.ToRecord(); !ok {
			verbose(" INPUT: conversion failed", idx)
			continue
		}
		// fix time offset
		r.Timestamp = r.Timestamp.Add(time.Hour * time.Duration(options.TimeOffset))
		// append to channel
		recordsChan <- r
		verbose(" INPUT: record queued", idx, r)
	}
	return
}

func outputRoutine() {
	// manage inputGroup
	outputGroup.Add(1)
	defer outputGroup.Done()
	// logging
	log.Println("OUTPUT: running")
	defer log.Println("OUTPUT: exiting")

	// temporary slice of records
	rs := make([]Record, 0, options.Batch.Size)

	for {
		// check for the outputExiting
		if outputExiting && len(recordsChan) == 0 {
			verbose("OUTPUT: exiting due to shutdown mark")
			break
		}

		// collect batch of records or wait for timeouts
		tm := time.NewTimer(time.Second * time.Duration(options.Batch.Timeout))
	loop:
		for {
			select {
			case r := <-recordsChan:
				verbose("OUTPUT: record retrieved")
				rs = append(rs, r)
				if len(rs) >= options.Batch.Size {
					verbose("OUTPUT: temporary slice fulled")
					break loop
				}
			case <-tm.C:
				verbose("OUTPUT: idle timed out")
				break loop
			}
		}
		tm.Stop()

		// continue if no records
		if len(rs) == 0 {
			verbose("OUTPUT: empty temporary slice")
			continue
		}

		// create bulk
		bs := esClient.Bulk()

		// insert records to elasticsearch
		for _, r := range rs {
			br := elastic.NewBulkIndexRequest().Index(r.Index()).Type("_doc").Doc(r)
			verbose("OUTPUT: new bulk index request", br.String())
			bs = bs.Add(br)
		}

		// do the bulk operation
		if _, err := bs.Do(context.Background()); err != nil {
			log.Println("OUTPUT: failed to bulk insert", err)
		}
		verbose("OUTPUT: bulk committed")

		// clear rs for reuse
		rs = rs[:0]
	}
}
