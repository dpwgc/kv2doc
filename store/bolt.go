package store

import (
	"bytes"
	"github.com/boltdb/bolt"
	"strconv"
)

type Bolt struct {
	db          *bolt.DB
	tableExists map[string]bool
}

func NewBolt(path string) (*Bolt, error) {
	// 创建或者打开数据库
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	return &Bolt{
		db:          db,
		tableExists: make(map[string]bool),
	}, nil
}

func (c *Bolt) CreateTable(table string) (err error) {
	if len(table) <= 0 {
		return nil
	}
	defer func() {
		if err == nil {
			c.tableExists[table] = true
		}
	}()
	// table存在就不再执行方法了
	if c.tableExists[table] {
		return nil
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(table))
		return err
	})
}

func (c *Bolt) DropTable(table string) (err error) {
	if len(table) <= 0 {
		return nil
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(table))
	})
}

func (c *Bolt) SetKV(table string, kvs []KV) error {
	if len(table) <= 0 || len(kvs) <= 0 {
		return nil
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(table))
		if bucket != nil {
			for _, v := range kvs {
				if len(v.Key) <= 0 {
					continue
				}
				if len(v.Value) <= 0 {
					err := bucket.Delete([]byte(v.Key))
					if err != nil {
						return err
					}
				} else {
					err := bucket.Put([]byte(v.Key), v.Value)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}

func (c *Bolt) GetKV(table, key string) (kv KV, err error) {
	if len(table) <= 0 || len(key) <= 0 {
		return KV{}, nil
	}
	err = c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(table))
		if bucket != nil {
			kv = KV{
				Key:   key,
				Value: bucket.Get([]byte(key)),
			}
		}
		return nil
	})
	return kv, err
}

func (c *Bolt) ScanKV(table, prefix string, handle func(key string, value []byte) bool) error {
	if len(table) <= 0 || handle == nil {
		return nil
	}
	return c.db.View(func(tx *bolt.Tx) error {
		if len(prefix) > 0 {
			pbs := []byte(prefix)
			cur := tx.Bucket([]byte(table)).Cursor()
			for k, v := cur.Seek(pbs); k != nil && bytes.HasPrefix(k, pbs); k, v = cur.Next() {
				if !handle(string(k), v) {
					return nil
				}
			}
		} else {
			cur := tx.Bucket([]byte(table)).Cursor()
			for k, v := cur.First(); k != nil; k, v = cur.Next() {
				if !handle(string(k), v) {
					return nil
				}
			}
		}
		return nil
	})
}

func (c *Bolt) NextID(table string) (id string, err error) {
	err = c.db.Update(func(tx *bolt.Tx) error {
		id64, err := tx.Bucket([]byte(table)).NextSequence()
		if err != nil {
			return err
		}
		id = strconv.FormatUint(id64, 10)
		return nil
	})
	return id, err
}
