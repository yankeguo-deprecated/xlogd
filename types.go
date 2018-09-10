package main

import (
	"fmt"
	"github.com/go-yaml/yaml"
	"github.com/pkg/errors"
	"io/ioutil"
	"regexp"
	"time"
)

const (
	eventTopicJSON = "_json_"
)

var (
	eventTimestampLayout = "2006/01/02 15:04:05.000"
	eventLinePattern     = regexp.MustCompile(`^\[(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]`)
	eventCridPattern     = regexp.MustCompile(`CRID\[([0-9a-zA-Z\-]+)\]`)
)

type RedisStats struct {
	Timestamp time.Time `json:"timestamp"`
	Host      string    `json:"host"`
	Hostname  string    `json:"hostname"`
	Depth     int64     `json:"depth"`
}

// Index index for record in elasticsearch
func (r RedisStats) Index() string {
	return fmt.Sprintf("x-xrdepth-%04d-%02d-%02d", r.Timestamp.Year(), r.Timestamp.Month(), r.Timestamp.Day())
}

// EventBeat beat info field
type EventBeat struct {
	Hostname string `json:"hostname"` // hostname
}

// Event a single event in redis LIST sent by filebeat
type Event struct {
	Beat    EventBeat `json:"beat"`    // contains hostname
	Message string    `json:"message"` // contains timestamp, crid
	Source  string    `json:"source"`  // contains env, topic, project
}

// ToRecord implements RecordConvertible
func (b Event) ToRecord() (r Record, ok bool) {
	// assign hostname
	r.Hostname = b.Beat.Hostname
	// decode message field
	if ok = decodeBeatMessage(b.Message, &r); !ok {
		return
	}
	// decode source field
	if ok = decodeBeatSource(b.Source, &r); !ok {
		return
	}
	// decode extra if event.topic == _json_
	if r.Topic == eventTopicJSON {
		if ok = mergeRecordExtra(&r); !ok {
			return
		}
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
	if len(r.Message) > 0 {
		out["message"] = r.Message
	}
	return
}

// Index index for record in elasticsearch
func (r Record) Index() string {
	return fmt.Sprintf("%s-%04d-%02d-%02d", r.Topic, r.Timestamp.Year(), r.Timestamp.Month(), r.Timestamp.Day())
}

// Options options for xlogd
type Options struct {
	// Redis
	// Redis options
	Redis RedisOptions `yaml:"redis"`

	// Elasticsearch
	// Elasticsearch options
	Elasticsearch ElasticsearchOptions `yaml:"elasticsearch"`

	// Batch
	// by default, batch size is 100 and a timeout of 10s
	// that means xlogd will perform a bulk write once cached records reached 100, or been idle for 10 seconds
	Batch BatchOptions `yaml:"batch"`

	// TimeOffset
	// generally timezone information is missing from log files, you may need set a offset to fix it
	// for 'Asia/Shanghai', set TimeOffset to -8
	TimeOffset int `yaml:"time_offset"`
}

// RedisOptions options for redis
type RedisOptions struct {
	// URLs
	// urls of redis instances, should be something like redis://127.0.0.1:6379
	URLs []string `yaml:"urls"`

	// Key
	// key of the LIST, xlogd use BLPOP for beat event fetching
	Key string `yaml:"key"`
}

// ElasticsearchOptions options for ElasticSearch
type ElasticsearchOptions struct {
	// URLs
	// urls of elasticsearch instances, should be something like http://127.0.0.1:9200
	URLs []string `yaml:"urls"`
}

// BatchOptions options for batch processing
type BatchOptions struct {
	// Size
	// batch size
	Size int `yaml:"size"`

	// Timeout
	// batch timeout
	Timeout int `yaml:"timeout"`
}

// LoadOptions load options from yaml file
func LoadOptions(filename string) (opt Options, err error) {
	var buf []byte
	if buf, err = ioutil.ReadFile(filename); err != nil {
		return
	}
	if err = yaml.Unmarshal(buf, &opt); err != nil {
		return
	}
	// check redis urls
	if len(opt.Redis.URLs) == 0 {
		err = errors.New("no redis urls")
		return
	}
	// check redis key
	if len(opt.Redis.Key) == 0 {
		err = errors.New("no redis key")
		return
	}
	// check elasticsearch urls
	if len(opt.Elasticsearch.URLs) == 0 {
		err = errors.New("no elasticsearch urls")
		return
	}
	// check batch size and batch timeout
	if opt.Batch.Size <= 0 {
		opt.Batch.Size = 100
	}
	if opt.Batch.Timeout <= 0 {
		opt.Batch.Timeout = 10
	}
	return
}
