package main

import (
	"fmt"
	"log"
	"os"
)

// https://www.sqlite.org/fileformat.html

func main() {
	databaseFile := os.Args[1]
	db, err := newDatabaseFile(databaseFile)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.File.Close()

	// testing stuff
	fmt.Println(db)
}
