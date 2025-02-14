package main

import (
	"fmt"
	"kv2doc"
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

	// 查询文档，筛选条件：title 以 hello 为前缀, 并且 type 要大于 0 或者 存在 color 字段，结果集按主键ID排序后，取前10条返回
	// 使用 Eq 或 LeftLike 进行查询时，会走最左前缀索引，其他查询方法走全表扫描
	documents, _ := db.Query("test_table").
		LeftLike("title", "hello").
		Should(kv2doc.Expr().Gt("type", "0").Exist("color")).
		Desc("_id").
		Limit(0, 10).
		List()

	// 打印查询结果
	for _, v := range documents {
		fmt.Println(v)
	}
}
