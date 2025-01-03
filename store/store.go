package store

type Store interface {
	DropIndex(index string) error
	SetKV(index string, kvs []KV) error
	GetKV(index, key string) (KV, error)
	ScanKV(index, prefix string, filter func(key, value string) bool) ([]KV, error)
}

type KV struct {
	Exist bool
	Key   string
	Value string
}
