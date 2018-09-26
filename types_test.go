package main

import (
	"testing"
	"time"
)

func TestStats_Index(t *testing.T) {
	r := Stats{
		Timestamp: time.Date(2018, time.April, 11, 23, 23, 13, 0, time.UTC),
	}
	if r.Index() != "x-xlogd-2018-04-11" {
		t.Error("failed, got", r.Index())
	}
}

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
	if r.Map()["x_duration"] != 20 {
		t.Fatal("1")
	}
}

func TestEvent_ToRecord(t *testing.T) {
	var be Event
	_, ok := be.ToRecord()
	if ok {
		t.Fatal("failed")
	}
	be.Message = "[2018/07/20 15:03:00.000] fakeK[fake] K[world] hello world CRID[aaa] KW[hello]"
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
	if !r.Timestamp.Equal(time.Date(2018, time.July, 20, 15, 3, 0, 0, time.UTC)) {
		t.Fatal("timestamp")
	}
	if r.Env != "test2" {
		t.Fatal("env")
	}
	if r.Topic != "test3" {
		t.Fatal("topic")
	}
	if r.Project != "test1" {
		t.Fatal("project")
	}
	if r.Crid != "aaa" {
		t.Fatal("crid")
	}
	if r.Hostname != "test.test" {
		t.Fatal("hostname")
	}
	if r.Keyword != "world,hello" {
		t.Fatal("keyword")
	}
	if r.Message != "fakeK[fake] K[world] hello world CRID[aaa] KW[hello]" {
		t.Fatal("message")
	}
}

func TestEvent_ToRecord_JSON(t *testing.T) {
	var be Event
	_, ok := be.ToRecord()
	if ok {
		t.Fatal("failed")
	}
	be.Message = `[2018/07/20 15:03:00.000] {"crid":"aaa", "topic":"x-test3", "duration":"aa"}`
	_, ok = be.ToRecord()
	if ok {
		t.Fatal("failed")
	}
	be.Source = "/tmp/test2/_json_/test1.20180719.log"
	be.Beat.Hostname = "test.test"
	var r Record
	if r, ok = be.ToRecord(); !ok {
		t.Fatal("failed")
	}
	if !r.Timestamp.Equal(time.Date(2018, time.July, 20, 15, 3, 0, 0, time.UTC)) {
		t.Fatal("timestamp")
	}
	if r.Env != "test2" {
		t.Fatal("env")
	}
	if r.Topic != "x-test3" {
		t.Fatal("topic")
	}
	if r.Project != "test1" {
		t.Fatal("project")
	}
	if r.Crid != "aaa" {
		t.Fatal("crid")
	}
	if r.Hostname != "test.test" {
		t.Fatal("hostname")
	}
	if r.Keyword != "" {
		t.Fatal("keyword")
	}
	if r.Extra["duration"] != "aa" {
		t.Fatal("extra")
	}
}

func TestLoadOptions(t *testing.T) {
	opt, err := LoadOptions("testdata/xlogd.yml")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(opt)
}
