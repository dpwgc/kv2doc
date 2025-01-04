# kv2doc

## 一个简单的嵌入式文档数据库实现

### 实现功能

* 基本的表结构及文档数据插入/更新/删除
* 暂时只支持 and 交集查询
* 支持 Eq、Ne、Gt、Gte、Lt、Lte、Like、LeftLike、RightLike、In、NotIn 条件查询

***

### 使用示例

```go
package main

import (
	"fmt"
	"kv2doc"
)

func main() {

	// 新建数据库 demo.db
	db, _ := kv2doc.NewDB("demo.db")

	// 往 test_table 表中插入2条数据（无需建表，插入数据时会自动建表，同时为每一个字段都建立索引）
	_, _ = db.Insert("test_table", kv2doc.Doc{
		"title": "hello world 1",
		"type":  "1",
	})
	id, _ := db.Insert("test_table", kv2doc.Doc{
		"title": "hello world 2",
		"type":  "2",
	})

	// 更新第2条数据，新增一个 color 字段
	_ = db.Update("test_table", id, kv2doc.Doc{
		"title": "hello world 2",
		"type":  "2",
		"color": "red",
	})

	// 编写查询条件：title 以 hello 为前缀，并且 type 要大于 1，结果集里取前10条返回
	// 使用 Eq 或 LeftLike 进行查询时，会走最左前缀索引，其他查询方法走全表扫描
	query := kv2doc.NewQuery().LeftLike("title", "hello").Gt("type", "1").Limit(0, 10)

	// 查询数据库
	documents, _ := db.SelectList("test_table", query)

	// 打印查询结果
	for _, v := range documents {
		fmt.Println(v)
	}
}
```

***

### 存储实现原理

#### 假设有一个文档，Json 格式如下（ _id 为文档主键，使用 BoltDB 的 NextSequence 方法获取）

```json
{
  "_id": "123",
  "title": "hello world",
  "type": "1",
  "color": "red"
}
```

#### 在保存此文档时，会将该文档的非主键字段拆解成索引，分别存进 BoltDB 键值对中，Key 是字段名 + 字段值 + 主键 id，Value 是主键 id

| key                     | value |
|-------------------------|-------|
| f/_id/123/123           | 123   |
| f/title/hello world/123 | 123   |
| f/type/1/123            | 123   |
| f/color/red/123         | 123   |

#### 上述表格展示的是字段索引（ key 以 f 前缀开头），只有文档 id，没有文档内容。而真正的文档内容，保存在主键 Key 下（ key 以 p 前缀开头）

| key       | value                                                                 |
|-----------|-----------------------------------------------------------------------|
| p/_id/123 | { "_id": "123", "title": "hello world", "type": "1", "color": "red" } |

***

### 查询实现原理

#### 查询情况可分为两种：索引扫描 or 全表扫描

#### 索引扫描：

* 如果使用了 Eq（等于）或者 LeftLike（具有相同前缀的字符串），则会按最左前缀原则匹配索引

* 例如：执行 LeftLike("title", "hello").Gt("type", "1")，会先利用 BoltDB 的 Cursor 遍历功能扫描所有前缀为 f/title/hello 的 key

* 然后再根据该索引扫描的结果作其他条件筛选（先根据字段索引 value 中的主键 id 找到文档内容，再判断文档中的 type 字段是否大于 1）

```
LeftLike("title", "hello").Gt("type", "1")
->
ScanBoltDB("f/title/hello") get _id
->
doc = getByID("xxx")
->
if doc["type"] > 1
->
hit
```

#### 全表扫描：

* 当全表扫描时，会在 BoltDB 中扫描所有前缀为 p 的 key（即所有存放文档内容的主键 key）,然后再根据文档内容进行匹配

* 例如：执行 Lt("type", "3")

```
Lt("type", "3")
->
ScanBoltDB("p") get doc
->
if doc["type"] < 3 
->
hit
```

***

### 自定义存储实现

#### 可以使用其他带事务功能的键值数据库来充当存储引擎，只需实现下述接口（务必确保所有操作方法都有事务保障），并调用 ByStore 方法生成数据库实例即可

```go
type Store interface {
    DropTable(table string) (err error)
    SetKV(table string, kvs []KV) (err error)
    GetKV(table, key string) (kv KV, err error)
    ScanKV(table, prefix string, handle func(key string, value []byte) bool) (err error)
    NextID(table string) (id string, err error)
}
```

```go
db := kv2doc.ByStore(rocketStore)
db := kv2doc.ByStore(etcdStore)
```