package main

import (
	"testing"
	"time"
)

func TestRecord_Index(t *testing.T) {
	r := Record{
		Timestamp: time.Date(2018, time.April, 11, 23, 23, 13, 0, time.UTC),
		Topic:     "dummy-topic",
	}
	if r.Index() != "dummy-topic-2018-04-11" {
		t.Error("failed, got", r.Index())
	}
}

func TestRecord_Map(t *testing.T) {
	r := Record{Extra: map[string]interface{}{
		"duration": 20,
	}}
	t.Log(r.Map())
}

func TestEvent_LinePattern(t *testing.T) {
	if eventLinePattern.MatchString("2018/07/20 15:03:00.000 hello world") {
		t.Fatal("failed")
	}
	if !eventLinePattern.MatchString("[2018/07/20 15:03:00.000] hello world") {
		t.Fatal("failed")
	}
}

func TestEvent_ToRecord(t *testing.T) {
	var be Event
	_, ok := be.ToRecord()
	if ok {
		t.Fatal("failed")
	}
	be.Message = "[2018/07/20 15:03:00.000] hello world CRID[aaa]"
	_, ok = be.ToRecord()
	if ok {
		t.Fatal("failed")
	}
	be.Source = "/tmp/test2/test3/test1.20180719.log"
	be.Beat.Hostname = "test.test"
	var r Record
	if r, ok = be.ToRecord(); !ok {
		t.Fatal("failed")
	}
	rr := Record{
		Timestamp: time.Date(2018, time.July, 20, 15, 3, 0, 0, time.UTC),
		Message:   "hello world CRID[aaa]",
		Env:       "test2",
		Topic:     "test3",
		Project:   "test1",
		Crid:      "aaa",
		Hostname:  "test.test",
	}
	t.Log(rr, r)
}
