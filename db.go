package kv2doc

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"kv2doc/store"
	"reflect"
	"strings"
	"sync"
)

const primaryKey = "_id"

type Document map[string]string

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

func (c *DB) Insert(index string, obj any) (id string, err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	doc := toDocumentFromStruct(obj)

	lower := toLower(doc)
	idPath := ""
	for {
		id = genID()
		idPath = toPath(primaryKey, id)
		ck, err := c.store.GetKV(index, idPath)
		if err != nil {
			return "", err
		}
		if !ck.Exist {
			break
		}
	}
	lower[primaryKey] = id
	var kvs []store.KV
	kvs = append(kvs, store.KV{
		Key:   idPath,
		Value: toString(lower),
	})
	for k, v := range lower {
		if k == primaryKey {
			continue
		}
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

func (c *DB) Update(index string, id string, obj any) (err error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	doc := toDocumentFromStruct(obj)

	idPath := toPath(primaryKey, id)

	ck, err := c.store.GetKV(index, idPath)
	if err != nil {
		return err
	}
	if !ck.Exist {
		return nil
	}

	lower := toLower(doc)
	lower[primaryKey] = id
	var kvs []store.KV
	kvs = append(kvs, store.KV{
		Key:   idPath,
		Value: toString(lower),
	})
	for k, v := range lower {
		if k == primaryKey {
			continue
		}
		kvs = append(kvs, store.KV{
			Key:   toPath(k, v, id),
			Value: id,
		})
	}
	return c.store.SetKV(index, kvs)
}

func (c *DB) Delete(index string, id string) (err error) {
	idPath := toPath(primaryKey, id)
	docSrc, err := c.store.GetKV(index, idPath)
	if err != nil {
		return err
	}
	if !docSrc.Exist {
		return nil
	}
	doc := toDocument(docSrc.Value)
	var kvs []store.KV
	kvs = append(kvs, store.KV{
		Key: idPath,
	})
	for k, v := range doc {
		if k == primaryKey {
			continue
		}
		kvs = append(kvs, store.KV{
			Key: toPath(k, v, doc[primaryKey]),
		})
	}
	return c.store.SetKV(index, kvs)
}

func (c *DB) Select(index string, query *Query) (docs []Document, err error) {
	var ids []store.KV
	if len(query.hitID) > 0 {
		docSrc, err := c.store.GetKV(index, toPath(primaryKey, query.hitID))
		if err != nil {
			return nil, err
		}
		if !docSrc.Exist {
			return nil, nil
		}
		docs = append(docs, toDocument(docSrc.Value))
		return docs, nil
	}
	cache := make(map[string]Document)
	filter := func(key, value string) bool {
		ok := true
		for _, v := range query.expressions {
			if v.Middle == equal && key != toPath(v.Left, v.Right, value) {
				ok = false
			}
			if v.Middle == leftLike && !strings.HasPrefix(key, toPath(v.Left, v.Right)) {
				ok = false
			}
			if v.Middle == rightLike && !strings.HasSuffix(key, toPath(v.Right, value)) {
				ok = false
			}
			if v.Middle == like && !strings.HasPrefix(key, toPath(v.Left, v.Right)) && !strings.HasSuffix(key, toPath(v.Right, value)) {
				ok = false
			}
		}
		if ok {
			// 检查文档是否存在
			docSrc, _ := c.store.GetKV(index, toPath(primaryKey, value))
			doc := toDocument(docSrc.Value)
			if len(doc) <= 0 {
				return false
			}
			cache[value] = doc
		}
		return ok
	}
	if len(query.hitField) > 0 && len(query.hitValue) > 0 {
		// 走索引
		ids, err = c.store.ScanKV(index, toPath(query.hitField, query.hitValue), filter)
	} else {
		// 全表扫描
		ids, err = c.store.ScanKV(index, "", filter)
	}
	if err != nil {
		return nil, err
	}
	for _, v := range ids {
		docs = append(docs, cache[v.Value])
	}
	return docs, nil
}

func NewQuery() *Query {
	return &Query{}
}

type Query struct {
	expressions []Expression
	hitField    string
	hitValue    string
	hitID       string
}

type Expression struct {
	Left   string
	Middle uint8
	Right  string
}

const equal = 1
const like = 2
const leftLike = 3
const rightLike = 4

func (c *Query) Equal(field, value string) *Query {
	if len(field) <= 0 || len(value) <= 0 {
		return c
	}
	c.expressions = append(c.expressions, Expression{
		Left:   field,
		Middle: equal,
		Right:  value,
	})
	if len(c.hitField) <= 0 {
		c.hitField = field
		c.hitValue = value
	}
	if field == primaryKey {
		c.hitID = value
	}
	return c
}

func (c *Query) Like(field, value string) *Query {
	if len(field) <= 0 || len(value) <= 0 || field == primaryKey {
		return c
	}
	c.expressions = append(c.expressions, Expression{
		Left:   field,
		Middle: like,
		Right:  value,
	})
	return c
}

func (c *Query) LeftLike(field, value string) *Query {
	if len(field) <= 0 || len(value) <= 0 || field == primaryKey {
		return c
	}
	c.expressions = append(c.expressions, Expression{
		Left:   field,
		Middle: leftLike,
		Right:  value,
	})
	return c
}

func (c *Query) RightLike(field, value string) *Query {
	if len(field) <= 0 || len(value) <= 0 || field == primaryKey {
		return c
	}
	c.expressions = append(c.expressions, Expression{
		Left:   field,
		Middle: rightLike,
		Right:  value,
	})
	return c
}

func genID() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

func toPath(s ...string) string {
	return strings.Join(s, "/")
}

func toDocument(src string) Document {
	var doc = Document{}
	_ = json.Unmarshal([]byte(src), &doc)
	return doc
}

func toString(doc Document) string {
	marshal, err := json.Marshal(doc)
	if err != nil {
		return ""
	}
	return string(marshal)
}

func toLower(doc Document) Document {
	res := make(map[string]string, len(doc))
	for k, v := range doc {
		res[strings.ToLower(k)] = v
	}
	return res
}

func toDocumentFromStruct(obj any) Document {

	doc, ok := obj.(Document)
	if ok {
		return doc
	}

	doc = make(map[string]string)

	obj1 := reflect.TypeOf(obj)
	obj2 := reflect.ValueOf(obj)

	if obj2.Kind() == reflect.Map {
		marshal, _ := json.Marshal(obj)
		aa := make(map[any]any)
		_ = json.Unmarshal(marshal, &aa)
		for k, v := range aa {
			doc[fmt.Sprintf("%v", k)] = fmt.Sprintf("%v", v)
		}
		return doc
	}

	for i := 0; i < obj1.NumField(); i++ {
		doc[obj1.Field(i).Name] = fmt.Sprintf("%v", obj2.Field(i).Interface())
	}
	return doc
}
