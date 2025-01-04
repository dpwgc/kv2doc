package store

import (
	"bytes"
	"github.com/boltdb/bolt"
)

type Bolt struct {
	db *bolt.DB
}

func NewBolt(path string) (*Bolt, error) {
	// 创建或者打开数据库
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	return &Bolt{
		db: db,
	}, nil
}

func (c *Bolt) DropIndex(index string) (err error) {
	if len(index) <= 0 {
		return nil
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(index))
	})
}

func (c *Bolt) SetKV(index string, kvs []KV) error {
	if len(index) <= 0 || len(kvs) <= 0 {
		return nil
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(index))
		if err != nil {
			return err
		}
		bucket := tx.Bucket([]byte(index))
		if bucket != nil {
			for _, v := range kvs {
				if len(v.Value) <= 0 {
					err := bucket.Delete([]byte(v.Key))
					if err != nil {
						return err
					}
				} else {
					err := bucket.Put([]byte(v.Key), []byte(v.Value))
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}

func (c *Bolt) GetKV(index, key string) (kv KV, err error) {
	if len(index) <= 0 || len(key) <= 0 {
		return KV{}, nil
	}
	err = c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(index))
		if bucket != nil {
			v := string(bucket.Get([]byte(key)))
			if len(v) > 0 {
				kv = KV{
					Key:   key,
					Value: v,
				}
			}
		}
		return nil
	})
	return kv, err
}

func (c *Bolt) ScanKV(index, prefix string, handle func(key, value string) bool) error {
	if len(index) <= 0 || handle == nil {
		return nil
	}
	return c.db.View(func(tx *bolt.Tx) error {
		if len(prefix) > 0 {
			pbs := []byte(prefix)
			cur := tx.Bucket([]byte(index)).Cursor()
			for k, v := cur.Seek(pbs); k != nil && bytes.HasPrefix(k, pbs); k, v = cur.Next() {
				if !handle(string(k), string(v)) {
					return nil
				}
			}
		} else {
			cur := tx.Bucket([]byte(index)).Cursor()
			for k, v := cur.First(); k != nil; k, v = cur.Next() {
				if !handle(string(k), string(v)) {
					return nil
				}
			}
		}
		return nil
	})
}
