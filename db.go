package kv2doc

import (
	"encoding/json"
	"github.com/google/uuid"
	"kv2doc/store"
	"strings"
)

const ID = "_id"

type Document map[string]string

type DB struct {
	store store.Store
}

func NewDB(path string) (*DB, error) {
	bolt, err := store.NewBolt(path)
	if err != nil {
		return nil, err
	}
	return &DB{
		store: bolt,
	}, nil
}

func (c *DB) Drop(index string) error {
	return c.store.DropIndex(index)
}

func (c *DB) Insert(index string, document Document) (string, error) {
	toLower := make(map[string]string, len(document))
	for k, v := range document {
		toLower[strings.ToLower(k)] = v
	}
	for {
		toLower[ID] = uuid.New().String()
		ck, err := c.store.GetKV(index, toKey(ID, toLower[ID]))
		if err != nil {
			return "", err
		}
		if !ck.Exist {
			break
		}
	}
	var commands []store.KV
	marshal, err := json.Marshal(toLower)
	if err != nil {
		return "", err
	}
	commands = append(commands, store.KV{
		Key:   toKey(ID, toLower[ID]),
		Value: string(marshal),
	})
	for k, v := range toLower {
		if k == ID {
			continue
		}
		commands = append(commands, store.KV{
			Key:   toKey(k, v, toLower[ID]),
			Value: toLower[ID],
		})
	}
	err = c.store.SetKV(index, commands)
	if err != nil {
		return "", err
	}
	return toLower[ID], nil
}

func (c *DB) Update(index string, id string, document Document) error {
	toLower := make(map[string]string, len(document))
	for k, v := range document {
		toLower[strings.ToLower(k)] = v
	}
	toLower[ID] = id
	var commands []store.KV
	marshal, err := json.Marshal(toLower)
	if err != nil {
		return err
	}
	commands = append(commands, store.KV{
		Key:   toKey(ID, toLower[ID]),
		Value: string(marshal),
	})
	for k, v := range toLower {
		if k == "_id" {
			continue
		}
		commands = append(commands, store.KV{
			Key:   toKey(k, v, toLower[ID]),
			Value: toLower[ID],
		})
	}
	return c.store.SetKV(index, commands)
}

func (c *DB) Delete(index string, id string) error {
	doc, err := c.store.GetKV(index, toKey(ID, id))
	if err != nil {
		return err
	}
	if !doc.Exist {
		return nil
	}
	document := Document{}
	err = json.Unmarshal([]byte(doc.Value), &document)
	if err != nil {
		return err
	}
	var kvs []store.KV
	kvs = append(kvs, store.KV{
		Key: toKey(ID, id),
	})
	for k, v := range document {
		if k == ID {
			continue
		}
		kvs = append(kvs, store.KV{
			Key: toKey(k, v, document[ID]),
		})
	}
	return c.store.SetKV(index, kvs)
}

func (c *DB) Select(index string, field, value string) ([]Document, error) {
	ids, err := c.store.ScanKV(index, toKey(field, value))
	if err != nil {
		return nil, err
	}
	var docs []Document
	for _, v := range ids {
		docSrc, err := c.store.GetKV(index, toKey(ID, v.Value))
		if err != nil {
			return nil, err
		}
		var doc = Document{}
		err = json.Unmarshal([]byte(docSrc.Value), &doc)
		if err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

func toKey(s ...string) string {
	return strings.Join(s, "/")
}
