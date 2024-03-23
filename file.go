package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	DatabaseHeaderString       = "SQLite format 3\000"
	MaxEmbeddedPayloadFraction = 64
	MinEmbeddedPayloadFraction = 32
	LeafPayloadFraction        = 32
	DatabaseHeaderSize         = 100
)

type databaseHeader struct {
	// The header string: "SQLite format 3\000"
	// offset 0, size 16
	HeaderString string
	// The database page size in bytes.
	// offset 16, size 2
	PageSize uint16
	// write file format version
	// offset 18, size 1
	WriteFileFormat uint8
	// read file format version
	// offset 19, size 1
	ReadFileFormat uint8
	// reserved space at the end of each page
	// offset 20, size 1
	ReservedPageSpace uint8
	// maximum embedded payload fraction: 64
	// offset 21, size 1
	MaxEmbeddedPayloadFraction uint8
	// maximum embedded payload fraction: 32
	// offset 22, size 1
	MinEmbeddedPayloadFraction uint8
	// leaf payload fraction: 32
	// offset 23, size 1
	LeafPayloadFraction uint8
	// file change counter
	// offset 24, size 4
	FileChangeCounter uint32
	// size of database file in pages
	// offset 28, size 4
	DatabasePageSize uint32
	// page number of first freelist trunk page
	// offset 32, size 4
	FirstFreeListTrunk uint32
	// total number of freelist pages
	// offset 36, size 4
	NumberOfFreeListPages uint32
	// schema cookie
	// offset 40, size 4
	SchemaCookie uint32
	// schema format number: [1:5]
	// offset 44, size 4
	SchemaFormat uint32
	// page cache size
	// offset 48, size 4
	PageCacheSize uint32
	// page number of largest root b-tree page when in
	// auto-vacuum or incremental-vacuum modes, zero otherwise
	// offset 52, offset 4
	LargestPageInVMode uint32
	// database text encoding
	// 1=utf8,2=utf16le,3=utf16be
	// offset 56, size 4
	TextEncoding uint32
	// user version pragma
	// offset 60, size 4
	UserVersionPragma uint32
	// non-zero for incrementalVMode else false
	// offset 64, size 4
	IncrementalVMode uint32
	// application id pragma
	// offset 68, size 4
	ApplicationID uint32
	// application id pragma
	// offset 72, size 20
	ReservedSpace uint64
	// version-valid-for number
	// offset 92, size 4
	VersionValidfor uint32
	// sqlite version number
	// offset 96, size 4
	SqliteVersion uint32
}

func newDatabaseHeader(f io.ReadSeeker) (*databaseHeader, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	headerBuf := make([]byte, DatabaseHeaderSize)
	if _, err := f.Read(headerBuf); err != nil {
		return nil, err
	}
	h := databaseHeader{}
	h.HeaderString = string(headerBuf[:16])
	if h.HeaderString != DatabaseHeaderString {
		return nil, errors.New("database string is invalid: " + h.HeaderString)
	}
	if err := readBigEndianInt(headerBuf[16:18], &h.PageSize); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[18:19], &h.WriteFileFormat); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[19:20], &h.ReadFileFormat); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[20:21], &h.ReservedPageSpace); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[21:22], &h.MaxEmbeddedPayloadFraction); err != nil {
		return nil, err
	}
	if h.MaxEmbeddedPayloadFraction != MaxEmbeddedPayloadFraction {
		return nil, errors.New(
			fmt.Sprintf("Maximum embedded payload fraction must be %d",
				MaxEmbeddedPayloadFraction))
	}
	if err := readBigEndianInt(headerBuf[22:23], &h.MinEmbeddedPayloadFraction); err != nil {
		return nil, err
	}
	if h.MinEmbeddedPayloadFraction != MinEmbeddedPayloadFraction {
		return nil, errors.New(
			fmt.Sprintf("Minimum embedded payload fraction must be %d",
				MinEmbeddedPayloadFraction))
	}
	if err := readBigEndianInt(headerBuf[23:24], &h.LeafPayloadFraction); err != nil {
		return nil, err
	}
	if h.LeafPayloadFraction != LeafPayloadFraction {
		return nil, errors.New(
			fmt.Sprintf("Leaf payload fraction must be %d",
				LeafPayloadFraction))
	}
	if err := readBigEndianInt(headerBuf[24:28], &h.FileChangeCounter); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[28:32], &h.DatabasePageSize); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[32:36], &h.FirstFreeListTrunk); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[36:40], &h.NumberOfFreeListPages); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[40:44], &h.SchemaCookie); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[44:48], &h.SchemaFormat); err != nil {
		return nil, err
	}
	if h.SchemaFormat < 1 || h.SchemaFormat > 4 {
		return nil, errors.New("schema format must be between 1 and 4")
	}
	if err := readBigEndianInt(headerBuf[48:52], &h.PageCacheSize); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[52:56], &h.LargestPageInVMode); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[56:60], &h.TextEncoding); err != nil {
		return nil, err
	}
	if h.TextEncoding < 1 || h.TextEncoding > 3 {
		return nil, errors.New("schema format must be between 1 and 3")
	}
	if err := readBigEndianInt(headerBuf[60:64], &h.UserVersionPragma); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[64:68], &h.IncrementalVMode); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[68:72], &h.ApplicationID); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[72:92], &h.ReservedSpace); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[92:96], &h.VersionValidfor); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(headerBuf[96:100], &h.SqliteVersion); err != nil {
		return nil, err
	}
	return &h, nil
}

func (d *databaseHeader) String() string {
	return primitiveStructString(d)
}

type databaseFile struct {
	File     *os.File
	Header   *databaseHeader
	RootPage *page
	Pages    []*page
}

func newDatabaseFile(databasePath string) (*databaseFile, error) {
	file, err := os.Open(databasePath)
	if err != nil {
		return nil, err
	}
	db := databaseFile{File: file}
	header, err := newDatabaseHeader(db.File)
	if err != nil {
		return nil, err
	}
	db.Header = header
	rootPage, err := newPage(db.File, true, header.PageSize, DatabaseHeaderSize)
	if err != nil {
		return nil, err
	}
	db.RootPage = rootPage
	return &db, nil
}

func addTableLeaves(db *databaseFile, p *page) error {
	if p.Header.PageType == InteriorTableType {
		for _, c := range p.Cells {
			offset := pageNumberToOffset(int64(db.Header.PageSize), int64(c.LeftPageNumber))
			pp, err := newPage(db.File, false, db.Header.PageSize, offset)
			if err != nil {
				return err
			}
			if pp.Header.PageType == LeafTableType {
				db.Pages = append(db.Pages, pp)
			} else {
				addTableLeaves(db, pp)

			}
		}
	}
	return nil
}

func (d *databaseFile) FindTableRootCell(t string, p *page) (*cell, error) {
	if p.Header.PageType == LeafTableType {
		c, err := p.CellFromTableName(t)
		if err == nil {
			return c, nil
		}
	}
	for _, c := range p.Cells {
		p, _ := d.newPageFromNumber(int64(c.LeftPageNumber))
		return d.FindTableRootCell(t, p)

	}
	return nil, errors.New("failed to find root cell for " + t)
}

func (d *databaseFile) FindTableCtx(t string, root *page) ([]*page, *cell, error) {
	c, err := d.FindTableRootCell(t, root)
	if err != nil {
		return nil, nil, err
	}
	pn, err := c.RootPage()
	if err != nil {
		return nil, nil, err
	}
	p, err := d.newPageFromNumber(pn)
	if err != nil {
		return nil, nil, err
	}
	r, err := d.findLeavesFromInterior(p, nil)
	return r, c, nil
}

func (d *databaseFile) findLeavesFromInterior(root *page, pages []*page) ([]*page, error) {
	if pages == nil {
		pages = []*page{}
	}
	if root.Header.PageType == LeafTableType {
		pages = append(pages, root)
		return pages, nil
	}
	for _, c := range root.Cells {
		p, _ := d.newPageFromNumber(int64(c.LeftPageNumber))
		if p.Header.PageType != LeafTableType {
			return d.findLeavesFromInterior(p, pages)
		} else {
			pages = append(pages, p)
		}
	}
	return pages, nil
}

func (d *databaseFile) TableNames() []string {
	var names []string
	if d.RootPage.Header.PageType == InteriorTableType {
		for _, p := range d.Pages {
			names = append(names, p.TablesNames()...)
		}
	} else {
		names = d.RootPage.TablesNames()
	}
	return names
}

func (d *databaseFile) newPageFromNumber(pageNumber int64) (*page, error) {
	return newPage(d.File, false, d.Header.PageSize,
		pageNumberToOffset(int64(d.Header.PageSize), pageNumber))
}

func (d *databaseFile) String() string {
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf(`DATABASE HEADER
%s
PAGE COUNT: %d

ROOT PAGE
-----------
%s`, d.Header.String(), len(d.Pages)+1, d.RootPage.String()))
	for _, p := range d.Pages {
		buf.WriteString(fmt.Sprintf(`PAGE %d
-----------
%s`, offsetToPageNumber(int64(d.Header.PageSize), p.Offset), p.String()))
	}
	return buf.String()
}
