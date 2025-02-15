package kv2doc

import (
	"errors"
	"fmt"
	"github.com/dpwgc/kv2doc/store"
	"strings"
	"sync"
	"time"
)

const (
	primaryKey    = "_id"
	createdAt     = "_created"
	updatedAt     = "_updated"
	fields        = "_fields"
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

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(table) <= 0 {
		return errors.New("parameter error")
	}

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
	if len(table) <= 0 && !doc.IsValid() {
		return nil, "", errors.New("parameter error")
	}
	err = c.store.CreateTable(table)
	if err != nil {
		return nil, "", err
	}

	id, err = c.store.NextID(table)
	if err != nil {
		return nil, "", err
	}

	doc[primaryKey] = id
	doc[updatedAt] = fmt.Sprintf("%v", time.Now().UnixMilli())
	doc[createdAt] = doc[updatedAt]
	doc[fields] = "/" + toPath(doc.UserFields()...)
	kvs = append(kvs, store.KV{
		Key:   toPath(primaryPrefix, primaryKey, id),
		Value: doc.ToBytes(),
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
	if len(table) <= 0 && len(id) <= 0 && !doc.IsValid() {
		return nil, errors.New("parameter error")
	}
	// 获取老的文档
	kv, err := c.store.GetKV(table, toPath(primaryPrefix, primaryKey, id))
	if err != nil {
		return nil, err
	}
	if !kv.HasKey() {
		return nil, nil
	}
	old := Doc{}.FromBytes(kv.Value)

	doc[primaryKey] = id
	doc[updatedAt] = fmt.Sprintf("%v", time.Now().UnixMilli())
	doc[createdAt] = old[createdAt]
	doc[fields] = "/" + toPath(doc.UserFields()...)
	kvs = append(kvs, store.KV{
		Key:   toPath(primaryPrefix, primaryKey, id),
		Value: doc.ToBytes(),
	})

	for k := range old {
		// 如果新保存的文档不包含这个老的字段
		if old.HasField(k) && !doc.HasField(k) {
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

// Delete 删除指定表中的指定文档记录
func (c *DB) Delete(table string, id string) (err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	kvs, err := c.delete(table, id)
	if err != nil {
		return err
	}

	return c.store.SetKV(table, kvs)
}

func (c *DB) delete(table string, id string) (kvs []store.KV, err error) {
	if len(table) <= 0 && len(id) <= 0 {
		return nil, errors.New("parameter error")
	}
	kv, err := c.store.GetKV(table, toPath(primaryPrefix, primaryKey, id))
	if err != nil {
		return nil, err
	}
	if !kv.HasKey() {
		return nil, nil
	}
	old := Doc{}.FromBytes(kv.Value)

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

// Bulk 批量操作
func (c *DB) Bulk(table string) *Bulk {
	return &Bulk{
		table: table,
	}
}

// Query 查询文档
func (c *DB) Query(table string) *Query {
	return &Query{
		db:      c,
		table:   table,
		parser:  NewParser(),
		isChild: false,
	}
}

// 查询
func query(query Query, justCount bool) (count int64, docs []Doc, err error) {
	count = 0
	cursor := 0
	// 扫描
	err = scan(query, func(doc Doc) bool {
		// 到达页数限制，且没有排序规则，结束检索
		if query.sort == nil && query.limit.enable && len(docs) >= query.limit.size {
			return false
		}
		// 如果还未到达指定游标（有排序规则时就不走这个了）
		if query.sort == nil && query.limit.enable && query.limit.cursor > cursor {
			cursor++
		} else {
			if justCount {
				count++
			} else {
				docs = append(docs, doc)
			}
		}
		return true
	})
	if err != nil {
		return 0, nil, err
	}
	// 最终排序
	if query.sort != nil && len(docs) > 0 {
		Sort(docs, query.sort)
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

// 扫描
func scan(query Query, fn func(doc Doc) bool) (err error) {
	if len(query.table) <= 0 || query.db == nil || fn == nil {
		return errors.New("parameter error")
	}
	filter := getFilter(query.expressions, query.parser)
	if len(query.index.field) > 0 {
		// 走索引
		return query.db.store.ScanKV(query.table, toPath(fieldPrefix, query.index.field, query.index.value), func(key string, value []byte) bool {
			doc := Doc{}
			kv, _ := query.db.store.GetKV(query.table, toPath(primaryPrefix, primaryKey, string(value)))
			if !kv.HasKey() {
				return true
			}
			doc = doc.FromBytes(kv.Value)
			// 跳过异常文档
			if !doc.IsValid() || len(doc[primaryKey]) <= 0 {
				return true
			}
			// 过滤逻辑
			if filter != nil && !filter(doc) {
				return true
			}
			return fn(doc)
		})
	} else {
		// 全表扫描
		return query.db.store.ScanKV(query.table, primaryPrefix, func(key string, value []byte) bool {
			doc := Doc{}
			doc = doc.FromBytes(value)
			// 跳过异常文档
			if !doc.IsValid() || len(doc[primaryKey]) <= 0 {
				return true
			}
			// 过滤逻辑
			if filter != nil && !filter(doc) {
				return true
			}
			return fn(doc)
		})
	}
}

func getFilter(expressions []string, parser *Parser) func(doc Doc) bool {
	if len(expressions) > 0 {
		return func(doc Doc) bool {
			match, _ := parser.Match(strings.Join(expressions, " && "), doc)
			return match
		}
	}
	return nil
}

func toPath(s ...string) string {
	return strings.Join(s, "/")
}
