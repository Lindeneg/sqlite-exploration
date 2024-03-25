package main

import (
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
	query    selectCtx
	rootCell *cell
	count    int
	data     []string
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

func newQueryContext(q selectCtx, rootCell *cell) *queryContext {
	data := []string{}
	return &queryContext{q, rootCell, 0, data}
}

func HandleSelect(s selectCtx, d *databaseFile) {
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
		query := newQueryContext(s, rootCell)
		queryTable(d, page, query)
		if query.query.IsCount {
			fmt.Println(query.count)
		} else {
			fmt.Println(strings.Join(query.data, "\n"))
		}
	}
}

func queryTable(db *databaseFile, p *page, q *queryContext) error {
	if q.data == nil {
		q.data = []string{}
	}
	isInterior := p.Header.PageType == InteriorTableType
	if !isInterior && p.Header.PageType == LeafTableType {
	Outer:
		for _, cell := range p.Cells {
			if q.query.Limit > 0 && q.count >= q.query.Limit {
				return nil
			}
			// holds result
			strs := []string{}
			// map column values to avoid
			// repeatdly reading from cell
			column := map[string]string{}
			for k, v := range q.query.Constraint {
				idx, ok := q.rootCell.ColumnMap[k]
				// constraint column does not exist
				if !ok {
					continue Outer
				}
				d := string(cell.ReadDataFromHeaderIndex(idx))
				column[k] = d
				// constraint is not satisfied
				if strings.ToLower(string(d)) != v {
					continue Outer
				}
			}
			for _, col := range q.query.Identifiers {
				if q.query.IsCount {
					strs = append(strs, "")
				} else {
					value, ok := column[col]
					if !ok {
						idx, ok := q.rootCell.ColumnMap[col]
						if !ok {
							continue Outer
						}
						value = string(cell.ReadDataFromHeaderIndex(idx))
					}
					if len(value) <= 0 && col == "id" {
						value = fmt.Sprintf("%d", cell.RowID)
					}
					strs = append(strs, value)
				}
			}
			if len(strs) > 0 {
				if !q.query.IsCount {
					q.data = append(q.data, strings.Join(strs, "|"))
				}
				q.count++
			}
		}
	} else if isInterior {
		for _, c := range p.Cells {
			if c.LeftPageNumber <= 0 {
				continue
			}
			if pn, err := newPageFromNumber(db, int64(c.LeftPageNumber)); err == nil {
				queryTable(db, pn, q)
			} else {
				fmt.Println(err.Error())
			}
		}
	} else {
		fmt.Printf("unhandled page %s\n", p)
	}
	if isInterior && p.Header.RightMostPointer > 0 {
		if pn, err := newPageFromNumber(db, int64(p.Header.RightMostPointer)); err == nil {
			queryTable(db, pn, q)
		} else {
			fmt.Println(err.Error())
		}
	}
	return nil
}

func handleQueryLeaf()       {}
func handleQueryConstraint() {}
func handleQueryIdentifers() {}

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
