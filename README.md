# kv2doc

## 一个简单的嵌入式文档型数据库（基于BoltDB实现）

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
	
	// 更新第2条数据，新增一个color字段
	_ = db.Update("test_table", id, kv2doc.Doc{
		"title": "hello world 2",
		"type":  "2",
		"color": "red",
	})

	// 编写查询条件（使用 Eq 或 LeftLike 进行查询时，会走最左前缀索引，其他查询方法走全表扫描）
	query := kv2doc.NewQuery().LeftLike("title", "hello").Gt("type", "1")

	// 查询数据库
	documents, _ := db.Select("test_table", query)

	// 打印查询结果
	for _, v := range documents {
		fmt.Println(v)
	}
}
```

***

### 实现原理