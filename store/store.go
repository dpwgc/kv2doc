package store

type Store interface {
	CreateTable(table string) (err error)
	DropTable(table string) (err error)
	SetKV(table string, kvs []KV) (err error)
	GetKV(table, key string) (kv KV, err error)
	ScanKV(table, prefix string, handle func(key string, value []byte) bool) (err error)
	NextID(table string) (id string, err error)
}

type KV struct {
	Key   string
	Value []byte
}

func (c KV) HasKey() bool {
	return len(c.Key) > 0
}

func (c KV) HasValue() bool {
	return len(c.Value) > 0
}
