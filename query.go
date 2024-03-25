package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

const (
	CountIdent = "count(*)"
)

type selectCtx struct {
	Tables      []string
	Identifiers []string
	Constraint  map[string]string
	IsCount     bool
	Limit       int
}

type queryContext struct {
	query     selectCtx
	tableName string
	rootCell  *cell
	count     int
	data      []string
}

func NewSelectCtx(stmt *sqlparser.Select) selectCtx {
	idents := sqlNodeToTrimmedString(stmt.SelectExprs)
	return selectCtx{
		Tables:      sqlNodeToTrimmedString(stmt.From),
		Identifiers: idents,
		Constraint:  sqlWhereToConstraint(stmt.Where),
		IsCount:     len(idents) > 0 && idents[0] == CountIdent,
		Limit:       sqlLimitToInt(stmt.Limit),
	}
}

func newQueryContext(s selectCtx, tableName string, rootCell *cell) *queryContext {
	data := []string{}
	return &queryContext{s, tableName, rootCell, 0, data}
}

func HandleSelect(s selectCtx, d *databaseFile) {
	// TODO look into a channel per table
	for _, tableName := range s.Tables {
		rootCell, ok := d.Tables[tableName]
		if !ok {
			fmt.Printf("failed to find root cell for table %s\n", tableName)
			continue
		}
		pageNumber, err := rootCell.RootPage()
		if err != nil {
			fmt.Printf("failed to find root page number for cell %d\n", rootCell.RowID)
			continue
		}
		page, _ := newPageFromNumber(d, pageNumber)
		q := newQueryContext(s, tableName, rootCell)
		err = queryTable(d, page, q)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		if q.query.IsCount {
			fmt.Println(q.count)
		} else {
			fmt.Println(strings.Join(q.data, "\n"))
		}
	}
}

func queryTable(db *databaseFile, p *page, q *queryContext) error {
	if q.data == nil {
		q.data = []string{}
	}
	isInterior := p.Header.PageType == InteriorTableType
	if !isInterior && p.Header.PageType == LeafTableType {
		if err := handleQueryLeaf(p, q); err != nil {
			return err
		}
	} else if isInterior {
		for _, c := range p.Cells {
			if c.LeftPageNumber <= 0 {
				continue
			}
			pn, err := newPageFromNumber(db, int64(c.LeftPageNumber))
			if err != nil {
				return err
			}
			if err = queryTable(db, pn, q); err != nil {
				return err
			}

		}
	}
	if isInterior && p.Header.RightMostPointer > 0 {
		pn, err := newPageFromNumber(db, int64(p.Header.RightMostPointer))
		if err != nil {
			return err
		}
		if err = queryTable(db, pn, q); err != nil {
			return err
		}
	}
	return nil
}

func handleQueryLeaf(p *page, q *queryContext) error {
	for _, c := range p.Cells {
		if q.query.Limit > 0 && q.count >= q.query.Limit {
			return nil
		}
		// map column values to avoid
		// repeatdly reading from cell
		col := map[string]string{}
		ok, err := handleQueryConstraint(col, c, q)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		strs, err := handleQueryIdentifers(col, c, q)
		if err != nil {
			return err
		}
		if len(strs) > 0 {
			if !q.query.IsCount {
				q.data = append(q.data, strings.Join(strs, "|"))
			}
			q.count++
		}
	}
	return nil

}

func handleQueryConstraint(col map[string]string, c *cell, q *queryContext) (bool, error) {
	for k, v := range q.query.Constraint {
		idx, ok := q.rootCell.ColumnMap[k]
		if !ok {
			return false, errors.New(
				fmt.Sprintf("constraint %q not found on table %q cell %d", k, q.tableName, c.RowID))
		}
		value := string(c.ReadDataFromHeaderIndex(idx))
		if k == "id" && len(value) <= 0 {
			value = fmt.Sprintf("%d", c.RowID)
		}
		col[k] = value
		if strings.ToLower(string(value)) != v {
			return false, nil
		}
	}
	return true, nil
}

func handleQueryIdentifers(col map[string]string, c *cell, q *queryContext) ([]string, error) {
	strs := []string{}
	for _, k := range q.query.Identifiers {
		if q.query.IsCount {
			strs = append(strs, "")
		} else {
			value, ok := col[k]
			if !ok {
				idx, ok := q.rootCell.ColumnMap[k]
				if !ok {
					return strs, errors.New(
						fmt.Sprintf("%q not found on table %q cell %d", k, q.tableName, c.RowID))
				}
				value = string(c.ReadDataFromHeaderIndex(idx))
			}
			if len(value) <= 0 && k == "id" {
				value = fmt.Sprintf("%d", c.RowID)
			}
			if len(value) > 0 {
				strs = append(strs, value)
			}
		}
	}
	return strs, nil
}

func sqlWhereToConstraint(w *sqlparser.Where) map[string]string {
	if w == nil {
		return nil
	}
	r := map[string]string{}
	exprs := sqlNodeToString(w.Expr)
	for _, expr := range exprs {
		kv := strings.Split(expr, "=")
		r[cleanKeyString(kv[0])] = cleanKeyString(kv[1])
	}
	return r
}

func sqlLimitToInt(l *sqlparser.Limit) int {
	if l == nil {
		return 0
	}
	return sqlNodeToInt(l.Rowcount)
}

func sqlNodeToInt(n sqlparser.SQLNode) int {
	buf := sqlparser.NewTrackedBuffer(nil)
	n.Format(buf)
	i, err := strconv.Atoi(buf.String())
	if err != nil {
		return 0
	}
	return i
}

func sqlNodeToString(n sqlparser.SQLNode) []string {
	buf := sqlparser.NewTrackedBuffer(nil)
	n.Format(buf)
	return strings.Split(strings.ToLower(buf.String()), ",")
}

func sqlNodeToTrimmedString(n sqlparser.SQLNode) []string {
	buf := sqlparser.NewTrackedBuffer(nil)
	n.Format(buf)
	return strings.Split(strings.ToLower(strings.ReplaceAll(buf.String(), " ", "")), ",")
}
