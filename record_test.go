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
