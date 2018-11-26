package main

import (
	"strings"
	"time"

	"github.com/yankeguo/byteline"
)

func decodeBeatMessage(raw string, isJSON bool, r *Record) (ok bool) {
	var yyyy, MM, dd, hh, mm, ss, SSS int64
	buf := []byte(raw)
	if buf, _, ok = byteline.Run(
		buf,
		byteline.TrimOperation{Left: true, Right: true},
		byteline.RuneOperation{Remove: true, Allowed: []rune{'['}},
		byteline.NumberOperation{Remove: true, Len: 4, Base: 10, Out: &yyyy},
		byteline.RuneOperation{Remove: true, Allowed: []rune{'-', '/'}},
		byteline.NumberOperation{Remove: true, Len: 2, Base: 10, Out: &MM},
		byteline.RuneOperation{Remove: true, Allowed: []rune{'-', '/'}},
		byteline.NumberOperation{Remove: true, Len: 2, Base: 10, Out: &dd},
		byteline.RuneOperation{Remove: true, Allowed: []rune{' ', '\t'}},
		byteline.NumberOperation{Remove: true, Len: 2, Base: 10, Out: &hh},
		byteline.RuneOperation{Remove: true, Allowed: []rune{':'}},
		byteline.NumberOperation{Remove: true, Len: 2, Base: 10, Out: &mm},
		byteline.RuneOperation{Remove: true, Allowed: []rune{':'}},
		byteline.NumberOperation{Remove: true, Len: 2, Base: 10, Out: &ss},
		byteline.RuneOperation{Remove: true, Allowed: []rune{'.'}},
		byteline.NumberOperation{Remove: true, Len: 3, Base: 10, Out: &SSS},
		byteline.RuneOperation{Remove: true, Allowed: []rune{']'}},
	); !ok {
		return
	}
	// extract the timestamp
	r.Timestamp = time.Date(int(yyyy), time.Month(MM), int(dd), int(hh), int(mm), int(ss), int(int64(time.Millisecond)*SSS), time.UTC)
	// extract extra or CRID/K
	if isJSON {
		if buf, _, ok = byteline.Run(
			buf,
			byteline.TrimOperation{Left: true, Right: true},
			byteline.JSONDecodeOperation{Remove: true, Out: &r.Extra},
		); !ok {
			return
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
	} else {
		if buf, _, ok = byteline.Run(
			buf,
			byteline.TrimOperation{Left: true, Right: true},
			byteline.MarkDecodeOperation{Name: "CRID", Out: &r.Crid},
			byteline.MarkDecodeOperation{Name: "K", Out: &r.Keyword, Combine: true, Separator: ","},
			byteline.MarkDecodeOperation{Name: "KW", Out: &r.Keyword, Combine: true, Separator: ","},
			byteline.MarkDecodeOperation{Name: "KEYWORD", Out: &r.Keyword, Combine: true, Separator: ","},
			byteline.TrimOperation{Left: true, Right: true},
		); !ok {
			return
		}
		// assign the remaining message
		r.Message = string(buf)
	}
	return
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

func strSliceContains(s []string, t string) bool {
	for _, r := range s {
		if r == t {
			return true
		}
	}
	return false
}

func extractIP(addr string) string {
	c := strings.Split(addr, ":")
	if len(c) < 2 {
		return "UNKNOWN"
	} else if len(c) == 2 {
		return c[0]
	} else {
		return strings.Join(c[0:len(c)-1], ":")
	}
}
