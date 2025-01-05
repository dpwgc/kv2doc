package kv2doc

import (
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

// NewDB 开启一个数据库，不存在时自动建库，底层基于 BoltDB
func NewDB(path string) (*DB, error) {
	bolt, err := store.NewBolt(path)
	if err != nil {
		return nil, err
	}
	return ByStore(bolt), nil
}

// ByStore 开启一个数据库（自定义底层存储引擎实现）
func ByStore(store store.Store) *DB {
	return &DB{
		store: store,
		mutex: &sync.Mutex{},
	}
}

// Drop 删除指定表
func (c *DB) Drop(table string) error {
	return c.store.DropTable(table)
}

// Insert 在指定表中插入文档记录（表不存在时自动建表）
func (c *DB) Insert(table string, doc Doc) (id string, err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	err = c.store.CreateTable(table)
	if err != nil {
		return "", err
	}

	id, err = c.store.NextID(table)
	if err != nil {
		return "", err
	}

	doc[primaryKey] = id
	var kvs []store.KV
	kvs = append(kvs, store.KV{
		Key:   toPath(primaryPrefix, primaryKey, id),
		Value: doc.toBytes(),
	})
	for k, v := range doc {
		kvs = append(kvs, store.KV{
			Key:   toPath(fieldPrefix, k, v, id),
			Value: []byte(id),
		})
	}
	err = c.store.SetKV(table, kvs)
	if err != nil {
		return "", err
	}
	return id, nil
}

// Update 更新指定表中的指定文档记录
// id 为文档主键 ID，在 Insert 文档记录时会返回
func (c *DB) Update(table string, id string, doc Doc) (err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 获取老的文档
	kv, err := c.store.GetKV(table, toPath(primaryPrefix, primaryKey, id))
	if err != nil {
		return err
	}
	if !kv.HasKey() {
		return nil
	}
	old := Doc{}.fromBytes(kv.Value)

	doc[primaryKey] = id
	var kvs []store.KV
	kvs = append(kvs, store.KV{
		Key:   toPath(primaryPrefix, primaryKey, id),
		Value: doc.toBytes(),
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
			Value: []byte(id),
		})
	}
	return c.store.SetKV(table, kvs)
}

// Delete 删除指定表中的指定文档记录
func (c *DB) Delete(table string, id string) (err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	kv, err := c.store.GetKV(table, toPath(primaryPrefix, primaryKey, id))
	if err != nil {
		return err
	}
	if !kv.HasKey() {
		return nil
	}
	old := Doc{}.fromBytes(kv.Value)

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

// Select 查询文档
func (c *DB) Select(table string) *Query {
	return &Query{
		db:    c,
		table: table,
	}
}

func query(query Query, justCount bool) (count int64, docs []Doc, err error) {
	if len(query.table) <= 0 || query.db == nil {
		return 0, nil, nil
	}
	count = 0
	cursor := 0
	logic := func(key string, value []byte) bool {

		// 到达页数限制，结束检索
		if query.limit.enable && len(docs) >= query.limit.size {
			return false
		}

		doc := Doc{}

		// 如果是主键，value就是文档内容，直接解析
		if strings.HasPrefix(key, primaryPrefix) {
			doc = doc.fromBytes(value)
		} else {
			// 不是主键，那value就是文档id，要根据id获取文档内容
			kv, _ := query.db.store.GetKV(query.table, toPath(primaryPrefix, primaryKey, string(value)))
			if !kv.HasKey() {
				return true
			}
			doc = doc.fromBytes(kv.Value)
		}

		// 跳过异常文档
		if doc.isEmpty() || len(doc[primaryKey]) <= 0 {
			return true
		}

		must := true

		// 自定义过滤逻辑
		if query.customize != nil && !query.customize(doc) {
			must = false
		}

		for _, v := range query.conditions {
			if v.Operator == eq && doc[v.Field] != v.Values[0] {
				must = false
			}
			if v.Operator == ne && doc[v.Field] == v.Values[0] {
				must = false
			}
			if v.Operator == leftLike && !strings.HasPrefix(doc[v.Field], v.Values[0]) {
				must = false
			}
			if v.Operator == rightLike && !strings.HasSuffix(doc[v.Field], v.Values[0]) {
				must = false
			}
			if v.Operator == like && !strings.HasPrefix(doc[v.Field], v.Values[0]) && !strings.HasSuffix(doc[v.Field], v.Values[0]) {
				must = false
			}
			if v.Operator == gt || v.Operator == gte || v.Operator == lt || v.Operator == lte {
				l, err := toDouble(doc[v.Field])
				if err != nil {
					must = false
				}
				r, err := toDouble(v.Values[0])
				if err != nil {
					must = false
				}
				if v.Operator == gt && !(l > r) {
					must = false
				}
				if v.Operator == gte && !(l >= r) {
					must = false
				}
				if v.Operator == lt && !(l < r) {
					must = false
				}
				if v.Operator == lte && !(l <= r) {
					must = false
				}
			}
			if v.Operator == in {
				has := false
				for i := 0; i < len(v.Values); i++ {
					if doc[v.Field] == v.Values[i] {
						has = true
					}
				}
				if !has {
					must = false
				}
			}
			if v.Operator == notIn {
				has := false
				for i := 0; i < len(v.Values); i++ {
					if doc[v.Field] == v.Values[i] {
						has = true
					}
				}
				if has {
					must = false
				}
			}
			if v.Operator == exist && len(doc[v.Field]) <= 0 {
				must = false
			}
			if v.Operator == notExist && len(doc[v.Field]) > 0 {
				must = false
			}
		}
		if must {
			// 如果还未到达指定游标
			if query.limit.enable && query.limit.cursor > cursor {
				cursor++
			} else {
				if justCount {
					count++
				} else {
					docs = append(docs, doc)
				}
			}
		}
		return true
	}
	if query.hit.HasField() {
		// 走索引
		err = query.db.store.ScanKV(query.table, toPath(fieldPrefix, query.hit.field, query.hit.value), logic)
	} else {
		// 全表扫描
		err = query.db.store.ScanKV(query.table, primaryPrefix, logic)
	}
	if err != nil {
		return 0, nil, err
	}
	return count, docs, nil
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
