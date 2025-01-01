package store

import (
	"bytes"
	"github.com/boltdb/bolt"
)

type Bolt struct {
	maxID int64
	db    *bolt.DB
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

func (c *Bolt) CreateIndex(index string) (err error) {
	return c.db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(index))
		if err != nil {
			return err
		}
		return nil
	})
}

func (c *Bolt) DeleteIndex(index string) (err error) {
	return c.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(index))
	})
}

func (c *Bolt) PutKv(index string, commands []KvCommand) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(index))
		if bucket != nil {
			for _, v := range commands {
				err := bucket.Put([]byte(v.Key), []byte(v.Value))
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (c *Bolt) DeleteKv(index string, commands []KvCommand) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(index))
		if bucket != nil {
			for _, v := range commands {
				err := bucket.Delete([]byte(v.Key))
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (c *Bolt) GetKv(index, key string) (value string, err error) {
	err = c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(index))
		if bucket != nil {
			value = string(bucket.Get([]byte(key)))
		}
		return nil
	})
	return value, err
}

func (c *Bolt) ListKv(index, prefix string) (values []string, err error) {
	err = c.db.View(func(tx *bolt.Tx) error {
		pbs := []byte(prefix)
		cur := tx.Bucket([]byte(index)).Cursor()
		for k, v := cur.Seek(pbs); k != nil && bytes.HasPrefix(k, pbs); k, v = cur.Next() {
			values = append(values, string(v))
		}
		return nil
	})
	return values, err
}
