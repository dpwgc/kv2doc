package store

type Store interface {
	DropIndex(index string) error
	SetKV(index string, kvs []KV) error
	GetKV(index, key string) (KV, error)
	ScanKV(index, prefix string, handle func(key, value string) bool) error
}

type KV struct {
	Exist bool
	Key   string
	Value string
}
