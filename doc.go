package kv2doc

import "encoding/json"

type Doc map[string]string

func (c Doc) isEmpty() bool {
	return len(c) <= 0
}

func (c Doc) hasKey(key string) bool {
	return len(c[key]) > 0
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
