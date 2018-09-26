package byteline

import (
	"bytes"
	"encoding/json"
	"strconv"
	"unicode"
	"unicode/utf8"
)

type TrimOperation struct {
	Left  bool
	Right bool
}

func (o TrimOperation) Execute(in []byte) (out []byte, ok bool) {
	// assign out as in
	out = in
	// always ok
	ok = true
	// left
	if o.Left {
		out = bytes.TrimLeftFunc(out, unicode.IsSpace)
	}
	// right
	if o.Right {
		out = bytes.TrimRightFunc(out, unicode.IsSpace)
	}
	return
}

type RuneOperation struct {
	Remove  bool
	Allowed []rune
	Out     *rune
}

func (o RuneOperation) Execute(in []byte) (out []byte, ok bool) {
	// assign out as in
	out = in
	// decode rune
	var r rune // rune
	var s int  // size of rune
	if r, s = utf8.DecodeRune(in); r == utf8.RuneError {
		return
	}
	// check allowed
	if len(o.Allowed) > 0 {
		// check if rune is allowed
		for _, a := range o.Allowed {
			if a == r {
				// mark as ok
				ok = true
				break
			}
		}
	} else {
		// mark as ok
		ok = true
	}
	// if ok
	if ok {
		// assign o.Out
		if o.Out != nil {
			*o.Out = r
		}
		// remove if needed
		if o.Remove {
			out = out[s:]
		}
	}
	return
}

type NumberOperation struct {
	Remove bool
	Base   int
	Len    int
	Out    *int64
}

func (o NumberOperation) Execute(in []byte) (out []byte, ok bool) {
	// assign out as in
	out = in
	// check length
	if len(in) < o.Len {
		return
	}
	// parse int
	var v int64
	var err error
	if v, err = strconv.ParseInt(string(in[0:o.Len]), o.Base, 64); err != nil {
		return
	}
	// assign values
	ok = true
	if o.Out != nil {
		*o.Out = v
	}
	if o.Remove {
		out = out[o.Len:]
	}
	return
}

type MarkDecodeOperation struct {
	Name      string
	Out       *string
	Combine   bool
	Separator string
}

func (o MarkDecodeOperation) Execute(in []byte) (out []byte, ok bool) {
	// assign out as in
	out = in
	// ok is always true
	ok = true
	// if output has zero length, just return
	if len(out) == 0 {
		return
	}
	// if no name specified, just return
	if len(o.Name) == 0 {
		return
	}
	// prefix / suffix
	prefix := []byte(o.Name + "[")
	const suffix = ']'
	// the loop
	var i int
	for {
		// check boundary, break if there is no enough space for a mark
		if i+len(prefix)+utf8.RuneLen(suffix) >= len(out) {
			break
		}
		// match prefix
		if bytes.HasPrefix(out[i:], prefix) {
			// if mark is at the first, or has a leading space
			if i == 0 || UTF8EndsWithSpace(out[0:i]) {
				// skip the prefix
				i += len(prefix)
				// search the suffix
				if s := UTF8IndexOfRune(out[i:], suffix); s > 0 {
					// handle the output
					if o.Out != nil {
						if o.Combine && len(*o.Out) > 0 {
							*o.Out = *o.Out + o.Separator + string(out[i:i+s])
						} else {
							*o.Out = string(out[i : i+s])
						}
					}
					// skip the content and suffix
					i += s + utf8.RuneLen(suffix)
				} else if s == 0 {
					// skip the prefix and suffix
					i += len(prefix) + utf8.RuneLen(suffix)
				} else {
					// missing suffix
					break
				}
			} else {
				// skip the invalid prefix
				i += len(prefix)
			}
		} else {
			// read the current rune
			if r, s := utf8.DecodeRune(out[i:]); r == utf8.RuneError {
				break
			} else {
				// and skip the current rune
				i += s
			}
		}
	}
	return
}

type JSONDecodeOperation struct {
	Remove bool
	Out    *map[string]interface{}
}

func (o JSONDecodeOperation) Execute(in []byte) (out []byte, ok bool) {
	// assign in to out
	out = in
	// decode
	if o.Out != nil {
		if err := json.Unmarshal(in, o.Out); err != nil {
			return
		}
	}
	// mark as ok
	ok = true
	// assign empty slice to out if required
	if o.Remove {
		out = []byte{}
	}
	return
}
