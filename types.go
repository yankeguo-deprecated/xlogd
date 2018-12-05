package main

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/go-yaml/yaml"
	"github.com/pkg/errors"
)

const (
	eventTopicJSON = "_json_"
)

// Stats daemon stats record
type Stats struct {
	Timestamp     time.Time
	Hostname      string `json:"hostname"`
	RecordsQueued int64  `json:"records_queued"`
	RecordsTotal  int64  `json:"records_total"`
	Records1M     int64  `json:"records_1m"`
}

func (r Stats) Index() string {
	return fmt.Sprintf("x-xlogd-%04d-%02d-%02d", r.Timestamp.Year(), r.Timestamp.Month(), r.Timestamp.Day())
}

// Event a single event in redis LIST sent by filebeat
type Event struct {
	Beat struct {
		Hostname string `json:"hostname"`
	} `json:"beat"` // contains hostname
	Message string `json:"message"` // contains timestamp, crid
	Source  string `json:"source"`  // contains env, topic, project
}

// ToRecord implements RecordConvertible
func (b Event) ToRecord() (r Record, ok bool) {
	// assign hostname
	r.Hostname = b.Beat.Hostname
	// decode source field
	if ok = decodeBeatSource(b.Source, &r); !ok {
		return
	}
	// decode message field
	if ok = decodeBeatMessage(b.Message, r.Topic == eventTopicJSON, &r); !ok {
		return
	}
	return
}

// Record a log record in collection
type Record struct {
	Timestamp time.Time              `json:"timestamp"`         // the time when record produced
	Hostname  string                 `json:"hostname"`          // the server where record produced
	Env       string                 `json:"env"`               // environment where record produced, for example 'dev'
	Project   string                 `json:"project"`           // project name
	Topic     string                 `json:"topic"`             // topic of log, for example 'access', 'err'
	Crid      string                 `json:"crid"`              // correlation id
	Message   string                 `json:"message,omitempty"` // the actual log message body
	Keyword   string                 `json:"keyword"`           // comma separated keywords
	Extra     map[string]interface{} `json:"extra,omitempty"`   // extra structured data

	NoTimeOffset bool `json:"-"` // should skip timestamp offset
}

func (r Record) Map() (out map[string]interface{}) {
	out = map[string]interface{}{}
	// assign extra with prefix
	for k, v := range r.Extra {
		out["x_"+k] = v
	}
	// assign fields manually
	out["timestamp"] = r.Timestamp.Format(time.RFC3339Nano)
	out["hostname"] = r.Hostname
	out["env"] = r.Env
	out["project"] = r.Project
	out["topic"] = r.Topic
	out["crid"] = r.Crid
	out["keyword"] = r.Keyword
	if len(r.Message) > 0 {
		out["message"] = r.Message
	}
	return
}

// Index index for record in elasticsearch
func (r Record) Index() string {
	return fmt.Sprintf("%s-%s-%04d-%02d-%02d", r.Topic, r.Env, r.Timestamp.Year(), r.Timestamp.Month(), r.Timestamp.Day())
}

// Options options for xlogd
type Options struct {
	// Dev
	// development mode, will be more verbose
	Dev bool `yaml:"dev"`
	// Bind
	// bind address for redis protocol
	Bind string `yaml:"bind"`
	// Multi
	Multi bool `yaml:"multi"`
	// Capacity
	// capacity of the queue
	Capacity int `yaml:"capacity"`
	// Elasticsearch
	// Elasticsearch options
	Elasticsearch ElasticsearchOptions `yaml:"elasticsearch"`
	// TimeOffset
	// generally timezone information is missing from log files, you may need set a offset to fix it
	// for 'Asia/Shanghai', set TimeOffset to -8
	TimeOffset int `yaml:"time_offset"`
	// EnforceKeyword
	// topic should be keyword enforced
	EnforceKeyword []string `yaml:"enforce_keyword"`
}

// ElasticsearchOptions options for ElasticSearch
type ElasticsearchOptions struct {
	// URLs
	// urls of elasticsearch instances, should be something like http://127.0.0.1:9200
	URLs []string `yaml:"urls"`
	// Batch
	// by default, batch size is 100 and a timeout of 10s
	// that means xlogd will perform a bulk write once cached records reached 100, or been idle for 10 seconds
	Batch BatchOptions `yaml:"batch"`
}

// BatchOptions options for batch processing
type BatchOptions struct {
	// Size
	// batch size
	Size int `yaml:"size"`
	// Rate
	// rate per second reduce elasticsearch write
	Rate int `yaml:"rate"`
	// Burst
	// burst capacity
	Burst int `yaml:"burst"`
}

// LoadOptions load options from yaml file
func LoadOptions(filename string) (opt Options, err error) {
	var buf []byte
	// read and unmarshal
	if buf, err = ioutil.ReadFile(filename); err != nil {
		return
	}
	if err = yaml.Unmarshal(buf, &opt); err != nil {
		return
	}
	// check bind
	if len(opt.Bind) == 0 {
		opt.Bind = "0.0.0.0:6379"
	}
	// check capacity
	if opt.Capacity <= 0 {
		opt.Capacity = 10000
	}
	// check elasticsearch urls
	if len(opt.Elasticsearch.URLs) == 0 {
		err = errors.New("no elasticsearch urls")
		return
	}
	// check batch size
	if opt.Elasticsearch.Batch.Size <= 0 {
		opt.Elasticsearch.Batch.Size = 100
	}
	// check batch limit
	if opt.Elasticsearch.Batch.Rate <= 0 {
		opt.Elasticsearch.Batch.Rate = 1000
	}
	// check batch burst
	if opt.Elasticsearch.Batch.Burst <= 0 {
		opt.Elasticsearch.Batch.Burst = 10000
	}
	return
}
