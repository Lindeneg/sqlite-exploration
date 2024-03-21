package main

import (
	"fmt"
	"log"
	"os"
	"strings"
)

// https://www.sqlite.org/fileformat.html
func main() {
	if len(os.Args) < 3 {
		log.Fatal("please provide arguments: file command")
	}
	databaseFile := os.Args[1]
	cmd := os.Args[2]
	db, err := newDatabaseFile(databaseFile)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.File.Close()
	switch cmd {
	case ".dbinfo":
		fmt.Printf("database page size: \t%v\n", db.Header.PageSize)
		fmt.Printf("number of tables: \t%v\n", db.RootPage.Header.CellCount)
		break
	case ".tables":
		var names []string
		if db.RootPage.Header.PageType == InteriorTableType {
			for _, p := range db.Pages {
				names = append(names, p.TablesNames()...)
			}
		} else {
			names = db.RootPage.TablesNames()
		}
		fmt.Println(strings.Join(names, " "))
	case ".all":
		fmt.Println(db)
	default:
		log.Fatal("Unknown command", cmd)
	}
}
