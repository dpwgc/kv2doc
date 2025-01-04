package kv2doc

import (
	"github.com/google/uuid"
	"kv2doc/store"
	"strings"
	"sync"
)

const primaryKey = "_id"

type DB struct {
	store store.Store
	mutex *sync.Mutex
}

func NewDB(path string) (*DB, error) {
	bolt, err := store.NewBolt(path)
	if err != nil {
		return nil, err
	}
	return &DB{
		store: bolt,
		mutex: &sync.Mutex{},
	}, nil
}

func (c *DB) Drop(index string) error {
	return c.store.DropIndex(index)
}

func (c *DB) Insert(index string, doc Doc) (id string, err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for {
		id = genID()
		ck, err := c.store.GetKV(index, toPath(primaryKey, id))
		if err != nil {
			return "", err
		}
		if !ck.Exist {
			break
		}
	}
	doc[primaryKey] = id
	var kvs []store.KV
	kvs = append(kvs, store.KV{
		Key:   toPath(primaryKey, id),
		Value: doc.toString(),
	})
	for k, v := range doc {
		kvs = append(kvs, store.KV{
			Key:   toPath(k, v, id),
			Value: id,
		})
	}
	err = c.store.SetKV(index, kvs)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (c *DB) Update(index string, id string, doc Doc) (err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 获取老的文档
	kv, err := c.store.GetKV(index, toPath(primaryKey, id))
	if err != nil {
		return err
	}
	if !kv.Exist {
		return nil
	}
	old := Doc{}.fromString(kv.Value)

	doc[primaryKey] = id
	var kvs []store.KV
	kvs = append(kvs, store.KV{
		Key:   toPath(primaryKey, id),
		Value: doc.toString(),
	})

	for k := range old {
		// 如果新保存的文档不包含这个老的字段
		if len(old[k]) > 0 && len(doc[k]) <= 0 {
			// 删除这个字段
			kvs = append(kvs, store.KV{
				Key: toPath(k, old[k], id),
			})
		}
	}

	for k, v := range doc {
		kvs = append(kvs, store.KV{
			Key:   toPath(k, v, id),
			Value: id,
		})
	}
	return c.store.SetKV(index, kvs)
}

func (c *DB) Delete(index string, id string) (err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	kv, err := c.store.GetKV(index, toPath(primaryKey, id))
	if err != nil {
		return err
	}
	if !kv.Exist {
		return nil
	}
	old := Doc{}.fromString(kv.Value)

	var kvs []store.KV
	kvs = append(kvs, store.KV{
		Key: toPath(primaryKey, id),
	})
	for k, v := range old {
		kvs = append(kvs, store.KV{
			Key: toPath(k, v, old[primaryKey]),
		})
	}
	return c.store.SetKV(index, kvs)
}

func (c *DB) Select(index string, query *Query) (docs []Doc, err error) {
	cursor := 0
	handle := func(key, id string) bool {
		// 到达页数限制，结束检索
		if query.limit.enable && len(docs) >= query.limit.size {
			return false
		}
		for _, v := range query.expressions {
			if v.Middle == eq && key != toPath(v.Left, v.Right, id) {
				return true
			}
			if v.Middle == leftLike && !strings.HasPrefix(key, toPath(v.Left, v.Right)) {
				return true
			}
			if v.Middle == rightLike && !strings.HasSuffix(key, toPath(v.Right, id)) {
				return true
			}
			if v.Middle == like && !strings.HasPrefix(key, toPath(v.Left, v.Right)) && !strings.HasSuffix(key, toPath(v.Right, id)) {
				return true
			}
		}
		// 获取文档内容
		kv, _ := c.store.GetKV(index, toPath(primaryKey, id))
		if kv.Exist {
			doc := Doc{}.fromString(kv.Value)
			if !doc.isEmpty() {
				// 如果还未到达指定游标
				if query.limit.enable && query.limit.cursor > cursor {
					cursor++
				} else {
					docs = append(docs, doc)
				}
			}
		}
		return true
	}
	if len(query.hit.field) > 0 && len(query.hit.value) > 0 {
		// 走索引
		err = c.store.ScanKV(index, toPath(query.hit.field, query.hit.value), handle)
	} else {
		// 全表扫描
		err = c.store.ScanKV(index, "", handle)
	}
	if err != nil {
		return nil, err
	}
	return docs, nil
}

func genID() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

func toPath(s ...string) string {
	return strings.Join(s, "/")
}
