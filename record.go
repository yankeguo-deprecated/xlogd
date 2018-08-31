package main

import (
	"fmt"
	"time"
)

// Record a log record in collection
type Record struct {
	Timestamp time.Time              `json:"timestamp"`         // the time when record produced
	Hostname  string                 `json:"hostname"`          // the server where record produced
	Env       string                 `json:"env"`               // environment where record produced, for example 'dev'
	Project   string                 `json:"project"`           // project name
	Topic     string                 `json:"topic"`             // topic of log, for example 'access', 'err'
	Crid      string                 `json:"crid"`              // correlation id
	Message   string                 `json:"message,omitempty"` // the actual log message body
	Extra     map[string]interface{} `json:"extra,omitempty"`   // extra structured data

	NoTimeOffset bool `json:"-"` // should skip timestamp offset
}

// Index index for record in elasticsearch
func (r Record) Index() string {
	return fmt.Sprintf("%s-%04d-%02d-%02d", r.Topic, r.Timestamp.Year(), r.Timestamp.Month(), r.Timestamp.Day())
}
