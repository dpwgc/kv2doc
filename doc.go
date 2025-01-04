package kv2doc

import "encoding/json"

type Document map[string]string

func (c Document) isEmpty() bool {
	return len(c) <= 0
}

func (c Document) toString() string {
	marshal, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	return string(marshal)
}

func (c Document) fromString(src string) Document {
	_ = json.Unmarshal([]byte(src), &c)
	return c
}
