package kv2doc

import "encoding/json"

type Doc map[string]string

func (c Doc) isEmpty() bool {
	return len(c) <= 0
}

func (c Doc) toString() string {
	marshal, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	return string(marshal)
}

func (c Doc) fromString(src string) Doc {
	_ = json.Unmarshal([]byte(src), &c)
	return c
}
