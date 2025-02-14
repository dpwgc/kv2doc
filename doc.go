package kv2doc

import (
	"encoding/json"
	"strconv"
	"time"
)

type Doc map[string]string

func (c Doc) isEmpty() bool {
	return len(c) <= 0
}

func (c Doc) hasKey(key string) bool {
	return len(c[key]) > 0
}

func (c Doc) Json() string {
	marshal, _ := json.Marshal(c)
	return string(marshal)
}

func (c Doc) ID() int64 {
	i, _ := strconv.ParseInt(c[primaryKey], 10, 64)
	return i
}

func (c Doc) Created() time.Time {
	i, _ := strconv.ParseInt(c[createdAt], 10, 64)
	return time.Unix(i/1000, 0)
}

func (c Doc) Updated() time.Time {
	i, _ := strconv.ParseInt(c[updatedAt], 10, 64)
	return time.Unix(i/1000, 0)
}

func (c Doc) isValid() bool {
	i := 0
	for k, v := range c {
		if len(k) > 0 && len(v) > 0 {
			i++
		}
	}
	return i > 0
}

func (c Doc) fields() []string {
	var s []string
	for k, v := range c {
		if len(k) > 0 && len(v) > 0 {
			if k != primaryKey && k != createdAt && k != updatedAt {
				s = append(s, k)
			}
		}
	}
	return s
}

func (c Doc) toBytes() []byte {
	marshal, err := json.Marshal(c)
	if err != nil {
		return nil
	}
	return marshal
}

func (c Doc) fromBytes(src []byte) Doc {
	_ = json.Unmarshal(src, &c)
	return c
}
