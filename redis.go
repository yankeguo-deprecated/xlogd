package main

import (
	"encoding/json"
	"github.com/go-redis/redis"
	"time"
)

// Redis wrapper for redis.Client
type Redis struct {
	Client *redis.Client
	Key    string
}

// DialRedis create wrapper for redis.Client and ping
func DialRedis(url string, key string) (r *Redis, err error) {
	// panic on bad redis url
	var opt *redis.Options
	if opt, err = redis.ParseURL(url); err != nil {
		panic(err)
	}
	// create client
	r = &Redis{Client: redis.NewClient(opt), Key: key}
	// ping
	if err = r.Client.Ping().Err(); err != nil {
		r.Client.Close()
	}
	return
}

// NextEvent retrieve next event
func (r *Redis) NextEvent() (e Event, ok bool, err error) {
	// BLPOP
	var ret []string
	if ret, err = r.Client.BLPop(time.Second*3, r.Key).Result(); err != nil {
		// redis.Nil should be ignored, no error, not retrieved
		if err == redis.Nil {
			err = nil
		}
		return
	}
	// length == 0 for timeout, should be ignored, no error, not retrieved
	if len(ret) == 0 {
		return
	}
	// length 1 for single key, 2 for multiple key, so ret[-1] should be fine
	raw := ret[len(ret)-1]
	// unmarshal json
	if err = json.Unmarshal([]byte(raw), &e); err != nil {
		// JSON unmarshal error should be ignored, no error, no retrieved
		err = nil
		return
	}
	// no error, retrieved
	ok = true
	return
}

// Close close the underlying redis client
func (r *Redis) Close() error {
	return r.Client.Close()
}
