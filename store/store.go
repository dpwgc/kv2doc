package store

type Store interface {
	CreateIndex(index string) error
	DeleteIndex(index string) error
	PutKv(index string, commands []KvCommand) error
	DeleteKv(index string, commands []KvCommand) error
	GetKv(index, key string) (string, error)
	ListKv(index, prefix string) ([]string, error)
}

type KvCommand struct {
	Key   string
	Value string
}
