package main

import (
	"github.com/go-yaml/yaml"
	"github.com/pkg/errors"
	"io/ioutil"
)

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
