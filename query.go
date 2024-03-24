package main

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

type selectCtx struct {
	Tables      []string
	Identifiers []string
}

func NewSelectCtx(stmt *sqlparser.Select) selectCtx {
	return selectCtx{
		Tables:      sqlNodeToString(stmt.From),
		Identifiers: sqlNodeToString(stmt.SelectExprs),
	}
}

func HandleSelect(s selectCtx, d *databaseFile) {
	for _, t := range s.Tables {
		rc, ok := d.Tables[t]
		if !ok {
			fmt.Printf("failed to find root cell for table %s\n", t)
			continue
		}
		pn, err := rc.RootPage()
		if err != nil {
			fmt.Printf("failed to find root page number for cell %d\n", rc.RowID)
			continue
		}
		p, _ := newPageFromNumber(d, pn)
		pa := queryTable(d, p, nil)
		fmt.Println(pa)
	}
}

func queryTable(db *databaseFile, p *page, r []*page) []*page {
	if r == nil {
		r = []*page{}
	}
	isLeaf := p.Header.PageType == LeafTableType
	isInterior := p.Header.PageType == InteriorTableType
	if isLeaf {
		// TODO check conditions
		r = append(r, p)
	} else if isInterior {
		for _, c := range p.Cells {
			if c.LeftPageNumber <= 0 {
				continue
			}
			if pn, err := newPageFromNumber(db, int64(c.LeftPageNumber)); err == nil {
				r = queryTable(db, pn, r)
			} else {
				fmt.Println(err.Error())
			}
		}
	} else {
		fmt.Printf("unhandled page %s\n", p)
	}
	if isInterior && p.Header.RightMostPointer > 0 {
		if pn, err := newPageFromNumber(db, int64(p.Header.RightMostPointer)); err == nil {
			r = queryTable(db, pn, r)
		} else {
			fmt.Println(err.Error())
		}
	}
	return r
}

func sqlNodeToString(n sqlparser.SQLNode) []string {
	buf := sqlparser.NewTrackedBuffer(nil)
	n.Format(buf)
	return strings.Split(strings.ToLower(strings.ReplaceAll(buf.String(), " ", "")), ",")
}
