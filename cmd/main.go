package main

import (
	"log"

	"github.com/artem-vildanov/small-db/internal"
)

func main() {
	const filePath = "../data/file.data"

	table := internal.NewTable(filePath)
	if err := table.Insert([]byte("qweqewqwe")); err != nil {
		log.Printf("Table.Insert: %s", err.Error())
	}
}
