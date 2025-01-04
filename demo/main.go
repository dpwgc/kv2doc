package main

import (
	"fmt"
	"kv2doc"
)

func main() {

	db, err := kv2doc.NewDB("demo.db")
	if err != nil {
		panic(err)
	}

	_, err = db.Insert("test", kv2doc.Doc{
		"title": "hello world 1",
		"type":  "1",
	})
	if err != nil {
		panic(err)
	}

	_, err = db.Insert("test", kv2doc.Doc{
		"title": "hello world 2",
		"type":  "2",
	})
	if err != nil {
		panic(err)
	}

	_, err = db.Insert("test", kv2doc.Doc{
		"title": "hello world 3",
		"type":  "3",
	})
	if err != nil {
		panic(err)
	}

	documents, err := db.Select("test", kv2doc.NewQuery().Gt("type", "1"))
	if err != nil {
		panic(err)
	}
	for _, v := range documents {
		fmt.Println(v)
	}
}
