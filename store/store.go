package store

type Store interface {
	DropIndex(index string) error
	SetKV(index string, kvs []KV) error
	GetKV(index, key string) (KV, error)
	ScanKV(index, prefix string, handle func(key, value string) bool) error
}

type KV struct {
	Key   string
	Value string
}

func (c KV) IsExist() bool {
	return len(c.Key) > 0 && len(c.Value) > 0
}
