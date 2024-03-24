package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

type serialType int

const (
	SerialNull serialType = iota
	Serial8TwosComplement
	Serial16TwosComplement
	Serial24TwosComplement
	Serial32TwosComplement
	Serial48TwosComplement
	Serial64TwosComplement
	SerialFloat
	Serial0
	Serial1
	SerialInternal1
	SerialInternal2
	SerialBlob
	SerialText
)

type cellType int

const (
	CellTypeUnknown cellType = iota
	CellTypeTable
	CellTypeIndex
)

var (
	TableTypeBytes = []byte{116, 97, 98, 108, 101}
	IndexTypeBytes = []byte{105, 110, 100, 101, 120}
	IndexKeyRegexp = regexp.MustCompile("\\((.*)\\)")
)

type columnMap map[string]int

func (c columnMap) String() string {
	var buf strings.Builder
	for k, v := range c {
		buf.WriteString(
			fmt.Sprintf("(Col=%s,Idx=%d) ", k, v))
	}
	return buf.String()
}

type cellHeader struct {
	Type  serialType
	Value int64
}

func (c cellHeader) String() string {
	return fmt.Sprintf("(Type=%d,Value=%d)", c.Type, c.Value)
}

type cell struct {
	Offset         int64
	PageType       uint8
	LeftPageNumber uint32
	HeaderSize     uint8
	PayloadSize    uint64
	RowID          int64
	ColumnMap      map[string]int
	Header         []cellHeader
	Data           []byte
}

func newCell(f io.ReadSeeker, p *page, offset int64) (*cell, error) {
	cellOffset := offset
	if p.Offset != DatabaseHeaderSize {
		cellOffset += p.Offset

	}
	_, err := f.Seek(cellOffset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, p.PageSize)
	if _, err := f.Read(buf); err != nil {
		return nil, err
	}
	c := cell{Offset: offset, PageType: p.Header.PageType, ColumnMap: make(columnMap)}
	switch c.PageType {
	case LeafTableType:
		if err := parseLeafTableCell(buf, &c); err != nil {
			return nil, err
		}
		break
	case InteriorTableType:
		if err := parseInteriorTable(buf, &c); err != nil {
			return nil, err
		}
		break
	case LeafIndexType:
		return nil, errors.New("LeafIndexType(10) not implemented")
	case InteriorIndexType:
		return nil, errors.New("InteriorIndexType(2) not implemented")
	default:
		return nil, errors.New(fmt.Sprintf("Unknown table type: %d", p.Header.PageType))
	}
	return &c, nil
}

func (c *cell) ParseColumnMap() {
	start := c.HeaderOffsetFromN(len(c.Header) - 1)
	end := start + c.Header[len(c.Header)-1].Value
	data := string(c.Data[start:end])
	columns := strings.Split(strings.Split(data, "(")[1], ",")
	for i, column := range columns {
		parts := strings.Split(strings.TrimSpace(column), " ")
		name := strings.TrimSuffix(parts[0], ")")
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
		name = leniantCleanKeyString(name)
		c.ColumnMap[name] = i
	}
}

func (c *cell) CellType() cellType {
	dataLength := len(c.Data)
	if dataLength <= 0 {
		return CellTypeUnknown
	}
	if len(c.Header) < 1 ||
		c.Header[0].Type != SerialText {
		return CellTypeUnknown
	}
	d := c.Data[:c.Header[0].Value]
	if bytes.Equal(d, TableTypeBytes) {
		return CellTypeTable
	} else if bytes.Equal(d, IndexTypeBytes) {
		return CellTypeIndex
	}
	return CellTypeUnknown
}

func (c *cell) IsTable() bool {
	return c.CellType() == CellTypeTable
}

func (c *cell) IsIndex() bool {
	return c.CellType() == CellTypeIndex
}

// Gets the offset in bytes to the nth header position
func (c *cell) HeaderOffsetFromN(n int) int64 {
	if n >= len(c.Header) {
		return 0
	}
	var offset int64 = 0
	for i := 0; i < n; i++ {
		offset += c.Header[i].Value
	}
	return offset
}

func (c *cell) TableName() (string, error) {
	if c.CellType() == CellTypeUnknown {
		return "", errors.New(fmt.Sprintf("cannot get tablename: cell %d is unknown type", c.RowID))
	}
	offset := c.HeaderOffsetFromN(2)
	return cleanKeyString(string(c.Data[offset : offset+c.Header[2].Value])), nil
}

func (c *cell) IndexCtx() (string, string, error) {
	if !c.IsIndex() {
		return "", "", errors.New(fmt.Sprintf("cannot get index ctx: cell %d is not index", c.RowID))
	}
	name, err := c.TableName()
	if err != nil {
		return "", "", err
	}
	matches := IndexKeyRegexp.FindSubmatch(c.Data)
	key := "1"
	if len(matches) > 1 {
		key = cleanKeyString(string(matches[1]))
	}
	return name, key, nil
}

func (c *cell) RootPage() (int64, error) {
	if c.PageType == InteriorTableType {
		return 0, errors.New("incorrect table type")
	}
	dataLength := len(c.Data)
	if dataLength <= 0 {
		return 0, errors.New("cell contains no data")
	}
	if len(c.Header) < 4 ||
		c.Header[0].Type != SerialText ||
		c.Header[1].Type != SerialText ||
		c.Header[2].Type != SerialText ||
		c.Header[3].Type != Serial8TwosComplement {
		return 0, errors.New("unexpected header types")
	}
	start := c.HeaderOffsetFromN(3)
	end := start + 1
	if end > int64(dataLength-1) {
		return 0, errors.New("unexpected header values")
	}
	return int64(c.Data[start : end+1][0]), nil
}

// leaf table starts with two variants, then a byte array
// and then a 4-byte integer for overflow page ptr
func parseLeafTableCell(buf []byte, c *cell) error {
	var offset int64 = 0
	// get payload length in bytes (which includes header size)
	payloadLength, read := readVarint(buf)
	offset += int64(read)
	// get row id of cell
	rowID, read := readVarint(buf[offset:])
	offset += int64(read)
	c.RowID = rowID
	// get the header length
	headerLength, read := readVarint(buf[offset:])
	c.HeaderSize = uint8(headerLength)
	// set the actual payload size i.e without header length
	c.PayloadSize = uint64(payloadLength) - uint64(c.HeaderSize)
	// read record (header and data)
	dataReader := bytes.NewReader(buf)
	// read header
	headerBuf := make([]byte, c.HeaderSize)
	read, err := dataReader.ReadAt(headerBuf, offset)
	if err != nil {
		return err
	}
	offset += int64(read)
	// skip header size byte
	variants, _ := readVarints(headerBuf[1:])
	// parse variants
	for _, variant := range variants {
		if variant > int64(SerialText) && variant%2 == 1 {
			c.Header = append(c.Header, cellHeader{Type: SerialText, Value: (variant - 13) / 2})
			continue
		}
		if variant > int64(SerialBlob) && variant%2 == 0 {
			c.Header = append(c.Header, cellHeader{Type: SerialBlob, Value: (variant - 12) / 2})
			continue
		}
		// probably much stupid
		c.Header = append(c.Header, cellHeader{Type: serialType(variant), Value: variant})
	}
	// read payload data
	dataBuf := make([]byte, c.PayloadSize)
	_, err = dataReader.ReadAt(dataBuf, offset)
	if err != nil {
		return err
	}
	c.Data = dataBuf
	return nil
}

// interior table only contains the left child
// page number and the row id of the cell
func parseInteriorTable(buf []byte, c *cell) error {
	if err := readBigEndianInt(buf[:4], &c.LeftPageNumber); err != nil {
		return err
	}
	rowID, _ := readVarint(buf[4:])
	c.RowID = rowID
	return nil
}

func (p *cell) String() string {
	switch p.PageType {
	case LeafTableType:
		if len(p.ColumnMap) > 0 {
			return primitiveStructString(struct {
				CellOffset  int64
				HeaderSize  uint8
				PayloadSize uint64
				RowID       int64
				ColumnMap   columnMap
				Header      []cellHeader
			}{
				CellOffset:  p.Offset,
				HeaderSize:  p.HeaderSize,
				PayloadSize: p.PayloadSize,
				RowID:       p.RowID,
				Header:      p.Header,
				ColumnMap:   p.ColumnMap,
			})
		} else {
			return primitiveStructString(struct {
				CellOffset  int64
				HeaderSize  uint8
				PayloadSize uint64
				RowID       int64
				Header      []cellHeader
				Data        string
			}{
				CellOffset:  p.Offset,
				HeaderSize:  p.HeaderSize,
				PayloadSize: p.PayloadSize,
				RowID:       p.RowID,
				Header:      p.Header,
				Data:        string(p.Data),
			})
		}
	case InteriorTableType:
		return primitiveStructString(struct {
			CellOffset     int64
			LeftPageNumber uint32
			RowID          int64
		}{
			CellOffset:     p.Offset,
			LeftPageNumber: p.LeftPageNumber,
			RowID:          p.RowID,
		})
	}
	return ""
}
