package kv2doc

import (
	"encoding/json"
	"strconv"
	"time"
)

type Doc map[string]string

func (c Doc) IsEmpty() bool {
	return len(c) <= 0
}

func (c Doc) HasField(key string) bool {
	return len(c[key]) > 0
}

func (c Doc) ToJson() string {
	return string(c.ToBytes())
}

func (c Doc) FromJson(s string) Doc {
	return c.FromBytes([]byte(s))
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

func (c Doc) IsValid() bool {
	i := 0
	for k, v := range c {
		if len(k) > 0 && len(v) > 0 {
			i++
		}
	}
	return i > 0
}

func (c Doc) Fields() []string {
	var keys []string
	for k, v := range c {
		if len(k) > 0 && len(v) > 0 {
			keys = append(keys, k)
		}
	}
	return keys
}

func (c Doc) Values() []string {
	var values []string
	for k, v := range c {
		if len(k) > 0 && len(v) > 0 {
			values = append(values, v)
		}
	}
	return values
}

func (c Doc) ToBytes() []byte {
	marshal, err := json.Marshal(c)
	if err != nil {
		return nil
	}
	return marshal
}

func (c Doc) FromBytes(src []byte) Doc {
	_ = json.Unmarshal(src, &c)
	return c
}

func (c Doc) UserFields() []string {
	var keys []string
	for k, v := range c {
		if len(k) > 0 && len(v) > 0 {
			if k != primaryKey && k != createdAt && k != updatedAt {
				keys = append(keys, k)
			}
		}
	}
	return keys
}

func (c Doc) UserValues() []string {
	var values []string
	for k, v := range c {
		if len(k) > 0 && len(v) > 0 {
			if k != primaryKey && k != createdAt && k != updatedAt {
				values = append(values, v)
			}
		}
	}
	return values
}
