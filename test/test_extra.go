package main

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"time"
)

func main() {
	nj := time.Now().Add(-time.Hour * 24).Format(time.RFC3339Nano)
	log.Println("Inner Timestamp:", nj)
	nh := time.Now().Format("2006/01/02 15:04:05.000")
	log.Println("Outter Timestamp:", nh)
	p := map[string]interface{}{
		"testkey1": "testvalue1",
		"testkey2": "testvalue2",
		"testkey3": map[string]interface{}{
			"testkey": "testvalue",
		},
		"timestamp": nj,
		"topic":     "test-json",
		"project":   "test-project",
		"crid":      "12345",
	}
	log.Println("Extra:", p)
	pl, _ := json.Marshal(&p)

	e := map[string]interface{}{
		"message": fmt.Sprintf("[%s] %s", nh, string(pl)),
		"source":  fmt.Sprintf("/var/log/test/_json_/dummy-project.log"),
		"beat": map[string]interface{}{
			"hostname": "test.example.com",
		},
	}
	log.Println("Event:", e)

	el, _ := json.Marshal(&e)
	o, _ := redis.ParseURL("redis://127.0.0.1:6379")
	r := redis.NewClient(o)
	r.RPush("xlog", string(el))
}
