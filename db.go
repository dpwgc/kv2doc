package kv2doc

import (
	"github.com/google/uuid"
	"kv2doc/store"
	"strconv"
	"strings"
	"sync"
)

const primaryKey = "_id"
const primaryPrefix = "p"
const fieldPrefix = "f"

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

func (c *DB) Drop(table string) error {
	return c.store.DropTable(table)
}

func (c *DB) Insert(table string, doc Doc) (id string, err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for {
		id = genID()
		ck, err := c.store.GetKV(table, toPath(primaryPrefix, primaryKey, id))
		if err != nil {
			return "", err
		}
		if !ck.IsExist() {
			break
		}
	}
	doc[primaryKey] = id
	var kvs []store.KV
	kvs = append(kvs, store.KV{
		Key:   toPath(primaryPrefix, primaryKey, id),
		Value: doc.toString(),
	})
	for k, v := range doc {
		kvs = append(kvs, store.KV{
			Key:   toPath(fieldPrefix, k, v, id),
			Value: id,
		})
	}
	err = c.store.SetKV(table, kvs)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (c *DB) Update(table string, id string, doc Doc) (err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 获取老的文档
	kv, err := c.store.GetKV(table, toPath(primaryPrefix, primaryKey, id))
	if err != nil {
		return err
	}
	if !kv.IsExist() {
		return nil
	}
	old := Doc{}.fromString(kv.Value)

	doc[primaryKey] = id
	var kvs []store.KV
	kvs = append(kvs, store.KV{
		Key:   toPath(primaryPrefix, primaryKey, id),
		Value: doc.toString(),
	})

	for k := range old {
		// 如果新保存的文档不包含这个老的字段
		if old.hasKey(k) && !doc.hasKey(k) {
			// 删除这个字段
			kvs = append(kvs, store.KV{
				Key: toPath(fieldPrefix, k, old[k], id),
			})
		}
	}

	for k, v := range doc {
		kvs = append(kvs, store.KV{
			Key:   toPath(fieldPrefix, k, v, id),
			Value: id,
		})
	}
	return c.store.SetKV(table, kvs)
}

func (c *DB) Delete(table string, id string) (err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	kv, err := c.store.GetKV(table, toPath(primaryPrefix, primaryKey, id))
	if err != nil {
		return err
	}
	if !kv.IsExist() {
		return nil
	}
	old := Doc{}.fromString(kv.Value)

	var kvs []store.KV
	kvs = append(kvs, store.KV{
		Key: toPath(primaryPrefix, primaryKey, id),
	})
	for k, v := range old {
		kvs = append(kvs, store.KV{
			Key: toPath(fieldPrefix, k, v, old[primaryKey]),
		})
	}
	return c.store.SetKV(table, kvs)
}

func (c *DB) Select(table string, query *Query) (docs []Doc, err error) {
	cursor := 0
	handle := func(key, value string) bool {
		// 到达页数限制，结束检索
		if query.limit.enable && len(docs) >= query.limit.size {
			return false
		}
		id := ""
		doc := Doc{}
		// 如果是主键，value就是文档内容，直接解析
		if strings.HasPrefix(key, primaryPrefix) {
			doc = doc.fromString(value)
		} else {
			// 不是主键，那value就是文档id，要根据id获取文档内容
			kv, _ := c.store.GetKV(table, toPath(primaryPrefix, primaryKey, value))
			if !kv.IsExist() {
				return true
			}
			doc = doc.fromString(kv.Value)
		}
		if doc.isEmpty() || len(doc[primaryKey]) <= 0 {
			return true
		}
		id = doc[primaryKey]

		must := true
		for _, v := range query.expressions {
			dbVal := toPath(v.Left, doc[v.Left], id)
			if v.Middle == eq && dbVal != toPath(v.Left, v.Right[0], id) {
				must = false
			}
			if v.Middle == ne && dbVal == toPath(v.Left, v.Right[0], id) {
				must = false
			}
			if v.Middle == leftLike && !strings.HasPrefix(dbVal, toPath(v.Left, v.Right[0])) {
				must = false
			}
			if v.Middle == rightLike && !strings.HasSuffix(dbVal, toPath(v.Right[0], id)) {
				must = false
			}
			if v.Middle == like && !strings.HasPrefix(dbVal, toPath(v.Left, v.Right[0])) && !strings.HasSuffix(dbVal, toPath(v.Right[0], id)) {
				must = false
			}
			if v.Middle == gt || v.Middle == gte || v.Middle == lt || v.Middle == lte {
				l, err := toDouble(strings.ReplaceAll(strings.ReplaceAll(dbVal, v.Left+"/", ""), "/"+id, ""))
				if err != nil {
					must = false
				}
				r, err := toDouble(v.Right[0])
				if err != nil {
					must = false
				}
				if v.Middle == gt && !(l > r) {
					must = false
				}
				if v.Middle == gte && !(l >= r) {
					must = false
				}
				if v.Middle == lt && !(l < r) {
					must = false
				}
				if v.Middle == lte && !(l <= r) {
					must = false
				}
			}
			if v.Middle == in {
				has := false
				for i := 0; i < len(v.Right); i++ {
					if dbVal == toPath(v.Left, v.Right[i], id) {
						has = true
					}
				}
				if !has {
					must = false
				}
			}
			if v.Middle == notIn {
				has := false
				for i := 0; i < len(v.Right); i++ {
					if dbVal == toPath(v.Left, v.Right[i], id) {
						has = true
					}
				}
				if has {
					must = false
				}
			}
		}
		if must {
			// 如果还未到达指定游标
			if query.limit.enable && query.limit.cursor > cursor {
				cursor++
			} else {
				docs = append(docs, doc)
			}
		}
		return true
	}
	if query.hit.IsExist() {
		// 走索引
		err = c.store.ScanKV(table, toPath(fieldPrefix, query.hit.field, query.hit.value), handle)
	} else {
		// 全表扫描
		err = c.store.ScanKV(table, primaryPrefix, handle)
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

func toDouble(s string) (float64, error) {
	if !strings.Contains(s, ".") {
		s = s + ".0"
	}
	return strconv.ParseFloat(s, 64)
}
