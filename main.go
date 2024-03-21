package main

import (
	"fmt"
	"log"
	"os"
)

// https://www.sqlite.org/fileformat.html

func main() {
	databaseFile := os.Args[1]
	//databaseFile := "D:/dev/csgo-pro-settings-distribution/prisma/db/data.db"
	//databaseFile := "companies.db"
	//databaseFile := "superheroes.db"
	//databaseFile := "sample.db"
	//databaseFile := "chinook.db"
	db, err := newDatabaseFile(fmt.Sprintf("./db/%s", databaseFile))
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.File.Close()

	// testing stuff

	//fmt.Println(db)
	if err := db.AddPage(int(db.RootPage.Cells[0].LeftPageNumber)); err != nil {
		log.Fatal(err)
	}

	fmt.Println(db.Pages[0].String())
}
