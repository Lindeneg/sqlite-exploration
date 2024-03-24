package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	DatabaseHeaderMagic        = "SQLite format 3\000"
	DatabaseHeaderSize         = 100
	MaxEmbeddedPayloadFraction = 64
	MinEmbeddedPayloadFraction = 32
	LeafPayloadFraction        = 32
)

// The first 100 bytes of the database file comprise the database file header.
// The database file header is divided into fields as shown by the table below.
// All multibyte fields in the database file header are stored with the most significant byte first (big-endian).
//
// # Offset	Size	Description
//
//	0	    16	    The header string: "SQLite format 3\000"
//
//	16	    2	    The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
//
//	18	    1	    File format write version. 1 for legacy; 2 for WAL.
//
//	19	    1	    File format read version. 1 for legacy; 2 for WAL.
//
//	20	    1	    Bytes of unused "reserved" space at the end of each page. Usually 0.
//
//	21	    1	    Maximum embedded payload fraction. Must be 64.
//	22	    1	    Minimum embedded payload fraction. Must be 32.
//	23	    1	    Leaf payload fraction. Must be 32.
//	24	    4	    File change counter.
//	28	    4	    Size of the database file in pages. The "in-header database size".
//	32	    4	    Page number of the first freelist trunk page.
//	36	    4	    Total number of freelist pages.
//	40	    4	    The schema cookie.
//	44	    4	    The schema format number. Supported schema formats are 1, 2, 3, and 4.
//	48	    4	    Default page cache size.
//	52	    4	    The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
//	56	    4	    The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
//	60	    4	    The "user version" as read and set by the user_version pragma.
//	64	    4	    True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
//	68	    4	    The "Application ID" set by PRAGMA application_id.
//	72	    20	    Reserved for expansion. Must be zero.
//	92	    4	    The version-valid-for number.
//	96	    4	    SQLITE_VERSION_NUMBER
type databaseHeader struct {
	HeaderString               string
	PageSize                   uint16
	WriteFileFormat            uint8
	ReadFileFormat             uint8
	ReservedPageSpace          uint8
	MaxEmbeddedPayloadFraction uint8
	MinEmbeddedPayloadFraction uint8
	LeafPayloadFraction        uint8
	FileChangeCounter          uint32
	DatabasePageSize           uint32
	FirstFreeListTrunk         uint32
	NumberOfFreeListPages      uint32
	SchemaCookie               uint32
	SchemaFormat               uint32
	PageCacheSize              uint32
	LargestPageInVMode         uint32
	TextEncoding               uint32
	UserVersionPragma          uint32
	IncrementalVMode           uint32
	ApplicationID              uint32
	ReservedSpace              uint64
	VersionValidfor            uint32
	SqliteVersion              uint32
}

// Takes an io.ReadSeeker and attempts to parse the first 100 bytes
// as an sqlite 3 header. Return either a pointer to the created
// header struct and a nil error, or a nil header pointer and an error
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
	if h.HeaderString != DatabaseHeaderMagic {
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

type cellMap map[string]*cell

func (c cellMap) String() string {
	var buf strings.Builder
	for k, v := range c {
		buf.WriteString(
			fmt.Sprintf("Key:%s%s\n%s\n", repeatStringDefault(3), k, v))
	}
	return buf.String()
}

// Contains a ptr to the file being parsed,
// the sqlite header of that file and the root page
// which is the first 8 or 12 bytes following the header.
//
// Table pages and index pages from sql_schema is saved as well.
type databaseFile struct {
	File     *os.File
	Header   *databaseHeader
	RootPage *page
	Tables   cellMap
	Indicies cellMap
}

func newDatabaseFile(databasePath string) (*databaseFile, error) {
	file, err := os.Open(databasePath)
	if err != nil {
		return nil, err
	}
	db := &databaseFile{
		File:     file,
		Tables:   make(cellMap),
		Indicies: make(cellMap)}
	header, err := newDatabaseHeader(db.File)
	if err != nil {
		return nil, err
	}
	db.Header = header
	rootPage, err := newPage(db.File, header.PageSize, DatabaseHeaderSize)
	if err != nil {
		return nil, err
	}
	db.RootPage = rootPage
	parseTablesAndIndices(db, db.RootPage)
	return db, nil
}

func (db *databaseFile) TableNames() []string {
	s := []string{}
	for k := range db.Tables {
		s = append(s, k)
	}
	return s
}

func parseTablesAndIndices(db *databaseFile, p *page) {
	isLeaf := p.Header.PageType == LeafTableType
	isInterior := p.Header.PageType == InteriorTableType
	for _, c := range p.Cells {
		if isLeaf {
			t := c.CellType()
			switch t {
			case CellTypeTable:
				if n, err := c.TableName(); err == nil {
					c.ParseColumnMap()
					db.Tables[n] = c
				} else {
					fmt.Println(err.Error())
				}
				break
			case CellTypeIndex:
				if table, key, err := c.IndexCtx(); err == nil {
					db.Indicies[fmt.Sprintf("%s-%s", table, key)] = c
				} else {
					fmt.Println(err.Error())
				}
				break
			default:
				fmt.Printf("cell %d has unknown type %d\n", c.RowID, t)

			}
		} else if isInterior && c.LeftPageNumber > 0 {
			if pn, err := newPageFromNumber(db, int64(c.LeftPageNumber)); err == nil {
				parseTablesAndIndices(db, pn)
			} else {
				fmt.Println(err.Error())
			}
		} else {
			fmt.Printf("unhandled page %s\n", p)
		}
	}
	if isInterior && p.Header.RightMostPointer > 0 {
		if pn, err := newPageFromNumber(db, int64(p.Header.RightMostPointer)); err == nil {
			parseTablesAndIndices(db, pn)
		} else {
			fmt.Println(err.Error())
		}
	}
}

func (d *databaseFile) String() string {
	var buf strings.Builder
	buf.WriteString(
		fmt.Sprintf("DATABASE HEADER\n%s\nROOT PAGE HEADER\n%s\n", d.Header, d.RootPage.Header))
	if len(d.Tables) > 0 {
		buf.WriteString(fmt.Sprintf("TABLES\n%s\n", d.Tables))
	}
	if len(d.Indicies) > 0 {
		buf.WriteString(fmt.Sprintf("INDICIES\n%s\n", d.Indicies))
	}
	return buf.String()
}
