# kv2doc

## 一个嵌入式文档数据库，基于 Go + BoltDB + Expr-lang 实现

### 实现功能

* 支持基本的表结构及文档数据插入/更新/删除/批量增删改操作。
* 支持索引维护及查询（遵循最左前缀原则）。
* 支持简单的条件查询与复杂的嵌套查询。
* 支持列表查询（排序+分页）与滚动查询。

***

### 使用示例

* 安装

```
go get github.com/dpwgc/kv2doc
```

* 代码示例

```go
package main

import (
	"fmt"
	"github.com/dpwgc/kv2doc"
)

func main() {

	// 新建数据库 demo.db
	db, _ := kv2doc.NewDB("demo.db")

	// 往 test_table 表中插入2条数据（无需建表，插入数据时会自动建表，同时为每一个字段都建立索引）
	_, _ = db.Add("test_table", kv2doc.Doc{
		"title": "hello world 1",
		"type":  "1",
	})
	id, _ := db.Add("test_table", kv2doc.Doc{
		"title": "hello world 2",
		"type":  "2",
	})

	// 更新第2条数据，新增一个 color 字段
	_ = db.Edit("test_table", id, kv2doc.Doc{
		"title": "hello world 2",
		"type":  "2",
		"color": "red",
	})

	// 查询文档，筛选条件：title 以 hello 为前缀, type 要大于 0 且 存在 color 字段，结果集按主键ID排序后，取前10条返回
	// 使用 Eq 或 LeftLike 进行查询时，会走最左前缀索引，其他查询方法走全表扫描
	documents, _ := db.Query("test_table").
		LeftLike("title", "hello").
		Must(kv2doc.Expr().Gt("type", "0").Exist("color")).
		Desc("_id").
		Limit(0, 10).
		List()

	// 打印查询结果
	for _, v := range documents {
		fmt.Println(v.ToJson())
	}

	// 查看Query执行计划
	explain := db.Query("test_table").
		LeftLike("title", "hello").
		Must(kv2doc.Expr().Gt("type", "0").Exist("color")).
		Explain()

	// 具体执行逻辑
	fmt.Println("expr:", explain.Expr)

	// 选择了哪个索引
	fmt.Println("index:", explain.Index)

	// 删除表
	_ = db.Drop("test_table")
}
```

***

### 函数说明

| 名称              | 功能                  |
|-----------------|---------------------|
| kv2doc.NewDB    | 创建/打开一个数据库          |
| kv2doc.ByStore  | 创建/打开一个数据库（自定义存储引擎） |
| db.Add          | 新增文档（表不存在时自动建表）     |
| db.Edit         | 编辑文档                |
| db.Delete       | 删除文档                |
| db.Bulk         | 批量操作（增删改）           |
| db.Drop         | 删除表                 |
| db.Query        | 新建查询                |
| Query.Eq        | 等于                  |
| Query.Ne        | 不等于                 |
| Query.Gt        | 大于                  |
| Query.Gte       | 大于等于                |
| Query.Lt        | 小于                  |
| Query.Lte       | 小于等于                |
| Query.In        | 包含                  |
| Query.NotIn     | 不包含                 |
| Query.Like      | 含有                  |
| Query.LeftLike  | 相同前缀                |
| Query.RightLike | 相同后缀                |
| Query.Exist     | 存在                  |
| Query.NotExist  | 不存在                 |
| Query.Must      | 交集语句                |
| Query.Should    | 并集语句                |
| Query.Asc       | 正序                  |
| Query.Desc      | 倒序                  |
| Query.Limit     | 分页                  |
| Query.One       | 返回一个文档              |
| Query.List      | 返回多个文档              |
| Query.Count     | 返回文档数量              |
| Query.Scroll    | 滚动查询文档              |
| Query.Explain   | 查看执行计划              |

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

* 如果使用了 Eq（等于）、 LeftLike（前缀相同）或者 In（数组内必须要有共同前缀才能走索引），会按最左前缀原则匹配索引

* 例如：执行 LeftLike("title", "hello").Gt("type", "1")，会先利用 BoltDB 的 Cursor 遍历功能扫描所有前缀为 f/title/hello 的 key

* 然后再根据该索引扫描的结果作其他条件筛选（先根据字段索引 value 中的主键 id 找到文档内容，再判断文档中的 type 字段是否大于 1）

#### 全表扫描：

* 当全表扫描时，会在 BoltDB 中扫描所有前缀为 p 的 key（即所有存放文档内容的主键 key）,然后再根据文档内容逐条匹配

***

### 自定义存储实现

#### 可以使用其他带事务功能的键值数据库来充当存储引擎，只需实现下述接口（务必确保所有操作方法都有事务保障），并调用 ByStore 方法生成数据库实例即可

```go
type Store interface {
    CreateTable(table string) (err error)
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