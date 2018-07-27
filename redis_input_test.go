package main

import (
	"testing"
	"time"
)

const testSampleBeatJSON = `{
  "source": "/tmp/test2/test3/test1.20180719.log",
  "message": "[2018/07/20 15:03:00.000] hello world CRID[aaa]\nbbb",
  "beat": {
    "hostname": "test.test"
  }
}`

func TestInput_All(t *testing.T) {
	ri, err := DialRedis("redis://localhost:6379", "xlog.test")
	if err != nil {
		t.Fatal(err)
	}
	defer ri.Close()
	e, ok, err := ri.NextEvent()
	if err != nil || ok {
		t.Fatal(err)
	}
	ri.Client.RPush("xlog.test", "aaaaa")
	e, ok, err = ri.NextEvent()
	// should ignore JSON unmarshal error
	if err != nil || ok {
		t.Fatal(err)
	}
	ri.Client.RPush("xlog.test", testSampleBeatJSON)
	e, ok, err = ri.NextEvent()
	if err != nil || !ok {
		t.Fatal(err)
	}
	r, ok := e.ToRecord()
	if !ok {
		t.Fatal("failed to convert")
	}
	rr := Record{
		Timestamp: time.Date(2018, time.July, 20, 15, 3, 0, 0, time.UTC),
		Message:   "hello world CRID[aaa]\nbbb",
		Env:       "test2",
		Topic:     "test3",
		Project:   "test1",
		Crid:      "aaa",
		Hostname:  "test.test",
	}
	if r != rr {
		t.Fatal("failed")
	}
}
