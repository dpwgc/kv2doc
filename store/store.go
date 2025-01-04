package store

type Store interface {
	DropTable(table string) error
	SetKV(table string, kvs []KV) error
	GetKV(table, key string) (KV, error)
	ScanKV(table, prefix string, handle func(key, value string) bool) error
}

type KV struct {
	Key   string
	Value string
}

func (c KV) IsExist() bool {
	return len(c.Key) > 0 && len(c.Value) > 0
}
