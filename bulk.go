package kv2doc

import "github.com/dpwgc/kv2doc/store"

type Bulk struct {
	db      *DB
	table   string
	actions []Action
}

type Action struct {
	Id       string
	Document Doc
	Type     int
}

const (
	Add = iota
	Edit
	Delete
)

func (c *Bulk) Add(doc Doc) *Bulk {
	c.actions = append(c.actions, Action{
		Document: doc,
		Type:     Add,
	})
	return c
}

func (c *Bulk) Edit(id string, doc Doc) *Bulk {
	c.actions = append(c.actions, Action{
		Id:       id,
		Document: doc,
		Type:     Edit,
	})
	return c
}

func (c *Bulk) Delete(id string) *Bulk {
	c.actions = append(c.actions, Action{
		Id:   id,
		Type: Delete,
	})
	return c
}

func (c *Bulk) Exec() (ids []string, err error) {

	c.db.mutex.Lock()
	defer c.db.mutex.Unlock()

	var allKvs []store.KV
	for _, v := range c.actions {
		if v.Type == Add {
			kvs, id, err := c.db.add(c.table, v.Document)
			if err != nil {
				return nil, err
			}
			allKvs = append(allKvs, kvs...)
			ids = append(ids, id)
		}
		if v.Type == Edit {
			kvs, err := c.db.edit(c.table, v.Id, v.Document)
			if err != nil {
				return nil, err
			}
			allKvs = append(allKvs, kvs...)
			ids = append(ids, v.Id)
		}
		if v.Type == Delete {
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
