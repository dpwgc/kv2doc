package kv2doc

import (
	"kv2doc/store"
	"strings"
	"sync"
)

const (
	primaryKey    = "_id"
	primaryPrefix = "p"
	fieldPrefix   = "f"
)

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

// Add 在指定表中插入文档记录（表不存在时自动建表）
func (c *DB) Add(table string, doc Doc) (id string, err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	kvs, id, err := c.add(table, doc)
	if err != nil {
		return "", err
	}

	err = c.store.SetKV(table, kvs)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (c *DB) add(table string, doc Doc) (kvs []store.KV, id string, err error) {
	err = c.store.CreateTable(table)
	if err != nil {
		return nil, "", err
	}

	id, err = c.store.NextID(table)
	if err != nil {
		return nil, "", err
	}

	doc[primaryKey] = id
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
	return kvs, id, nil
}

// Edit 更新指定表中的指定文档记录
// id 为文档主键 ID，在 Add 文档记录时会返回
func (c *DB) Edit(table string, id string, doc Doc) (err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	kvs, err := c.edit(table, id, doc)
	if err != nil {
		return err
	}

	return c.store.SetKV(table, kvs)
}

func (c *DB) edit(table string, id string, doc Doc) (kvs []store.KV, err error) {
	// 获取老的文档
	kv, err := c.store.GetKV(table, toPath(primaryPrefix, primaryKey, id))
	if err != nil {
		return nil, err
	}
	if !kv.HasKey() {
		return nil, nil
	}
	old := Doc{}.fromBytes(kv.Value)

	doc[primaryKey] = id
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

	return kvs, nil
}

// Remove 删除指定表中的指定文档记录
func (c *DB) Remove(table string, id string) (err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	kvs, err := c.remove(table, id)
	if err != nil {
		return err
	}

	return c.store.SetKV(table, kvs)
}

func (c *DB) remove(table string, id string) (kvs []store.KV, err error) {
	kv, err := c.store.GetKV(table, toPath(primaryPrefix, primaryKey, id))
	if err != nil {
		return nil, err
	}
	if !kv.HasKey() {
		return nil, nil
	}
	old := Doc{}.fromBytes(kv.Value)

	kvs = append(kvs, store.KV{
		Key: toPath(primaryPrefix, primaryKey, id),
	})
	for k, v := range old {
		kvs = append(kvs, store.KV{
			Key: toPath(fieldPrefix, k, v, old[primaryKey]),
		})
	}
	return nil, nil
}

type BatchCommand struct {
	Id       string
	Document Doc
	Type     CmdType
}

type CmdType int

const (
	Add CmdType = iota
	Edit
	Remove
)

func (c *DB) Batch(table string, cmd ...BatchCommand) (ids []string, err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	var allKvs []store.KV
	for _, v := range cmd {
		if v.Type == Add {
			kvs, id, err := c.add(table, v.Document)
			if err != nil {
				return nil, err
			}
			allKvs = append(allKvs, kvs...)
			ids = append(ids, id)
		}
		if v.Type == Edit {
			kvs, err := c.edit(table, v.Id, v.Document)
			if err != nil {
				return nil, err
			}
			allKvs = append(allKvs, kvs...)
			ids = append(ids, v.Id)
		}
		if v.Type == Remove {
			kvs, err := c.remove(table, v.Id)
			if err != nil {
				return nil, err
			}
			allKvs = append(allKvs, kvs...)
			ids = append(ids, v.Id)
		}
	}
	err = c.store.SetKV(table, allKvs)
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// Query 查询文档
func (c *DB) Query(table string) *Query {
	return &Query{
		db:     c,
		table:  table,
		parser: NewParser(),
	}
}

// 普通查询
func query(query Query, justCount bool) (count int64, docs []Doc, err error) {
	if len(query.table) <= 0 || query.db == nil {
		return 0, nil, nil
	}
	query.setFilter()
	count = 0
	cursor := 0
	logic := func(key string, value []byte) bool {

		// 到达页数限制，且没有排序规则，结束检索
		if query.orderBy == nil && query.limit.enable && len(docs) >= query.limit.size {
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

		// 过滤逻辑
		if query.filter(doc) {
			// 如果还未到达指定游标（有排序规则时就不走这个了）
			if query.orderBy == nil && query.limit.enable && query.limit.cursor > cursor {
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
	if len(query.index.field) > 0 {
		// 走索引
		err = query.db.store.ScanKV(query.table, toPath(fieldPrefix, query.index.field, query.index.value), logic)
	} else {
		// 全表扫描
		err = query.db.store.ScanKV(query.table, primaryPrefix, logic)
	}
	if err != nil {
		return 0, nil, err
	}
	// 最终排序
	if query.orderBy != nil && len(docs) > 0 {
		Sort(docs, query.orderBy)
		start := query.limit.cursor
		end := start + query.limit.size
		var sorted []Doc
		for i := 0; i < len(docs); i++ {
			if i >= start && i < end {
				sorted = append(sorted, docs[i])
			}
			if i >= end {
				break
			}
		}
		return count, sorted, nil
	}
	return count, docs, nil
}

// 滚动查询
func scroll(query Query, fn func(doc Doc) bool) (err error) {
	if len(query.table) <= 0 || query.db == nil || fn == nil {
		return nil
	}
	query.setFilter()
	if len(query.index.field) > 0 {
		// 走索引
		return query.db.store.ScanKV(query.table, toPath(fieldPrefix, query.index.field, query.index.value), func(key string, value []byte) bool {
			doc := Doc{}
			doc = doc.fromBytes(value)
			return fn(doc)
		})
	} else {
		// 全表扫描
		return query.db.store.ScanKV(query.table, primaryPrefix, func(key string, value []byte) bool {
			doc := Doc{}
			doc = doc.fromBytes(value)
			return fn(doc)
		})
	}
}

func toPath(s ...string) string {
	return strings.Join(s, "/")
}
