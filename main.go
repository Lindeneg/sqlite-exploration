package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/xwb1989/sqlparser"
)

type selectCtx struct {
	Tables      []string
	Identifiers []string
}

func SQLNodeToString(n sqlparser.SQLNode) []string {
	buf := sqlparser.NewTrackedBuffer(nil)
	n.Format(buf)
	return strings.Split(strings.ToLower(strings.ReplaceAll(buf.String(), " ", "")), ",")
}

func NewSelectCtx(stmt *sqlparser.Select) selectCtx {
	return selectCtx{
		Tables:      SQLNodeToString(stmt.From),
		Identifiers: SQLNodeToString(stmt.SelectExprs),
	}
}

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
		fmt.Printf("number of tables: \t%v\n", len(db.TableNames()))
		break
	case ".tables":
		addTableLeaves(db, db.RootPage)
		fmt.Println(strings.Join(db.TableNames(), " "))
	case ".leaves":
		addTableLeaves(db, db.RootPage)
		fmt.Println(db)
	default:
		stmt, err := sqlparser.Parse(cmd)
		if err != nil {
			log.Fatal("unknown command/query: " + cmd)
		}
		switch stmt := stmt.(type) {
		case *sqlparser.Select:
			handleSelect(stmt, db)
		}
	}
}

type columnCtx struct {
	name      string
	headerIdx int
}

func handleSelect(stmt *sqlparser.Select, db *databaseFile) {
	selectCtx := NewSelectCtx(stmt)
	for _, t := range selectCtx.Tables {
		pages, cell, err := db.FindTableCtx(t, db.RootPage)
		if err != nil {
			fmt.Println("Failed to query table "+t, err)
			continue
		}
		for _, page := range pages {
			columnsCtx := []columnCtx{}
			// get header value and index
			start := cell.GetOffsetFromHeader(len(cell.Header) - 1)
			end := start + cell.Header[len(cell.Header)-1].Value
			data := string(cell.Data[start:end])
			columns := strings.Split(strings.Split(data, "(")[1], ",")
			for i, column := range columns {
				parts := strings.Split(strings.TrimSpace(column), " ")
				name := parts[0]
				if strings.HasPrefix(name, "\"") {
					for _, part := range parts[1:] {
						name += " " + part
						if strings.HasSuffix(part, "\"") {
							break
						}
					}
				} else {
					name = strings.ToLower(strings.TrimSpace(name))
				}
				name = strings.ReplaceAll(name, "[", "")
				name = strings.ReplaceAll(name, "]", "")
				for _, ident := range selectCtx.Identifiers {
					if ident == name {
						columnsCtx = append(columnsCtx, columnCtx{name, i})
					}
				}
			}
			if len(selectCtx.Identifiers) != len(columnsCtx) {
				log.Fatal(fmt.Sprintf("column not found on table %q", t))
			}
			// find index for each column
			// extract values from page cells
			for _, c := range page.Cells {
				s := []string{}
				for _, ct := range columnsCtx {
					h := c.Header[ct.headerIdx]
					switch h.Type {
					case SERIAL_NULL:
						s = append(s, fmt.Sprintf("%d", c.RowID))
					case SERIAL_TEXT:
						offset := c.GetOffsetFromHeader(ct.headerIdx)
						s = append(s, string(c.Data[offset:offset+h.Value]))
					default:
						fmt.Println(h.Type)
					}
				}
				fmt.Println(strings.Join(s, "|"))
			}
		}
	}
}
