package main

import (
	"encoding/json"
	"regexp"
	"strings"
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

// Beat beat info field
type Beat struct {
	Hostname string `json:"hostname"` // hostname
}

// Event a single event in redis LIST sent by filebeat
type Event struct {
	Beat    Beat   `json:"beat"`    // contains hostname
	Message string `json:"message"` // contains timestamp, crid
	Source  string `json:"source"`  // contains env, topic, project
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
		if ok = decodeExtra(&r); !ok {
			return
		}
	}
	return
}

func decodeBeatMessage(raw string, r *Record) bool {
	var err error
	var match []string
	// trim message
	raw = strings.TrimSpace(raw)
	// search timestamp
	if match = eventLinePattern.FindStringSubmatch(raw); len(match) != 2 {
		return false
	}
	// parse timestamp
	if r.Timestamp, err = time.Parse(eventTimestampLayout, match[1]); err != nil {
		return false
	}
	// trim message
	r.Message = strings.TrimSpace(raw[len(match[0]):])
	// find crid
	if match = eventCridPattern.FindStringSubmatch(r.Message); len(match) == 2 {
		r.Crid = match[1]
	} else {
		r.Crid = "-"
	}
	return true
}

func decodeBeatSource(raw string, r *Record) bool {
	var cs []string
	// trim source
	raw = strings.TrimSpace(raw)
	if cs = strings.Split(raw, "/"); len(cs) < 3 {
		return false
	}
	// assign fields
	r.Env, r.Topic, r.Project = cs[len(cs)-3], cs[len(cs)-2], cs[len(cs)-1]
	// sanitize dot separated filename
	var ss []string
	if ss = strings.Split(r.Project, "."); len(ss) > 0 {
		r.Project = ss[0]
	}
	return true
}

func decodeExtra(r *Record) bool {
	var err error
	r.Extra = map[string]interface{}{}
	if err = json.Unmarshal([]byte(r.Message), &r.Extra); err != nil {
		return false
	}
	// topic must exist
	if !decodeExtraStr(r.Extra, "topic", &r.Topic) {
		return false
	}
	// optional extra 'project', 'crid'
	decodeExtraStr(r.Extra, "project", &r.Project)
	decodeExtraStr(r.Extra, "crid", &r.Crid)
	// optional extract 'timestamp'
	if decodeExtraTime(r.Extra, "timestamp", &r.Timestamp) {
		r.NoTimeOffset = true
	}
	// clear the message
	r.Message = ""
	return true
}

func decodeExtraStr(m map[string]interface{}, key string, out *string) bool {
	if m == nil || out == nil {
		return false
	}
	if val, ok := m[key].(string); ok {
		val = strings.TrimSpace(val)
		delete(m, key) // always delete
		if len(val) > 0 {
			*out = val // update if not empty
			return true
		}
	}
	return false
}

func decodeExtraTime(m map[string]interface{}, key string, out *time.Time) bool {
	if m == nil || out == nil {
		return false
	}
	var tsStr string
	if decodeExtraStr(m, key, &tsStr) {
		if t, err := time.Parse(time.RFC3339, tsStr); err != nil {
			return false
		} else {
			*out = t // update if success
			return true
		}
	}
	return false
}
