package kv2doc

import (
	"encoding/json"
	"github.com/google/uuid"
	"kv2doc/store"
	"strings"
)

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

func (c *DB) CreateIndex(index string) error {
	return c.store.CreateIndex(index)
}

func (c *DB) DeleteIndex(index string) error {
	return c.store.DeleteIndex(index)
}

func (c *DB) PutDocument(index string, document Document) error {
	toLower := make(map[string]string, len(document))
	for k, v := range document {
		toLower[strings.ToLower(k)] = v
	}
	if len(toLower["id"]) <= 0 {
		toLower["id"] = uuid.New().String()
	}
	var commands []store.KvCommand
	marshal, err := json.Marshal(toLower)
	if err != nil {
		return err
	}
	commands = append(commands, store.KvCommand{
		Key:   "id/" + toLower["id"],
		Value: string(marshal),
	})
	for k, v := range toLower {
		if k == "id" {
			continue
		}
		commands = append(commands, store.KvCommand{
			Key:   k + "/" + v + "/" + toLower["id"],
			Value: toLower["id"],
		})
	}
	return c.store.PutKv(index, commands)
}

func (c *DB) DeleteDocument(index string, id string) error {
	str, err := c.store.GetKv(index, "id/"+id)
	if err != nil {
		return err
	}
	if len(str) <= 0 {
		return nil
	}
	document := Document{}
	err = json.Unmarshal([]byte(str), &document)
	if err != nil {
		return err
	}
	var commands []store.KvCommand
	commands = append(commands, store.KvCommand{
		Key: "id/" + id,
	})
	for k, v := range document {
		if k == "id" {
			continue
		}
		commands = append(commands, store.KvCommand{
			Key: k + "/" + v + "/" + document["id"],
		})
	}
	return c.store.DeleteKv(index, commands)
}
