package kv2doc

import "github.com/dpwgc/kv2doc/store"

type Bulk struct {
	db      *DB
	table   string
	actions []action
}

type action struct {
	Id       string
	Document Doc
	Type     int
}

const (
	add = iota
	edit
	del
)

func (c *Bulk) Add(doc Doc) *Bulk {
	c.actions = append(c.actions, action{
		Document: doc,
		Type:     add,
	})
	return c
}

func (c *Bulk) Edit(id string, doc Doc) *Bulk {
	c.actions = append(c.actions, action{
		Id:       id,
		Document: doc,
		Type:     edit,
	})
	return c
}

func (c *Bulk) Delete(id string) *Bulk {
	c.actions = append(c.actions, action{
		Id:   id,
		Type: del,
	})
	return c
}

func (c *Bulk) Exec() (ids []string, err error) {

	c.db.mutex.Lock()
	defer c.db.mutex.Unlock()

	var allKvs []store.KV
	for _, v := range c.actions {
		if v.Type == add {
			kvs, id, err := c.db.add(c.table, v.Document)
			if err != nil {
				return nil, err
			}
			allKvs = append(allKvs, kvs...)
			ids = append(ids, id)
		}
		if v.Type == edit {
			kvs, err := c.db.edit(c.table, v.Id, v.Document)
			if err != nil {
				return nil, err
			}
			allKvs = append(allKvs, kvs...)
			ids = append(ids, v.Id)
		}
		if v.Type == del {
			kvs, err := c.db.delete(c.table, v.Id)
			if err != nil {
				return nil, err
			}
			allKvs = append(allKvs, kvs...)
			ids = append(ids, v.Id)
		}
	}
	err = c.db.store.SetKV(c.table, allKvs)
	if err != nil {
		return nil, err
	}
	return ids, nil
}
