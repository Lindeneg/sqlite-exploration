package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
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
	Type serialType
	Size int64
}

func newCellHeader(variant int64) cellHeader {
	if variant > int64(SerialText) && variant%2 == 1 {
		return cellHeader{Type: SerialText, Size: (variant - 13) / 2}
	}
	if variant > int64(SerialBlob) && variant%2 == 0 {
		return cellHeader{Type: SerialBlob, Size: (variant - 12) / 2}
	}
	switch variant {
	case int64(Serial48TwosComplement):
		return cellHeader{Type: Serial48TwosComplement, Size: 6}
	case int64(Serial64TwosComplement):
		return cellHeader{Type: Serial64TwosComplement, Size: 8}
	case int64(SerialFloat):
		return cellHeader{Type: SerialFloat, Size: 8}
	case int64(Serial0):
		return cellHeader{Type: Serial0, Size: 0}
	case int64(Serial1):
		return cellHeader{Type: Serial1, Size: 0}
	}
	return cellHeader{Type: serialType(variant), Size: variant}
}

func (c cellHeader) String() string {
	return fmt.Sprintf("(Type=%d,Size=%d)", c.Type, c.Size)
}

type cell struct {
	Offset         int64
	PageType       uint8
	LeftPageNumber uint32
	HeaderSize     uint8
	PayloadSize    uint64
	FirstOverflow  uint32
	RowID          int64
	ColumnMap      map[string]int
	Header         []cellHeader
	Data           []byte
}

func newCell(f io.ReadSeeker, p *page, offset int64) (*cell, error) {
	if offset == 0 {
		if p.Header.CellContent <= 0 {
			return nil, errors.New(
				fmt.Sprintf("invalid offset 0 on page %d", p.Offset))
		}
		offset = int64(p.Header.CellContent)
	}
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
		if err := parseInteriorTableCell(buf, &c); err != nil {
			return nil, err
		}
		break
	case LeafIndexType:
		if err := parseLeafIndexCell(buf, &c); err != nil {
			return nil, err
		}
	case InteriorIndexType:
		if err := parseInteriorIndexCell(buf, &c); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New(fmt.Sprintf("Unknown table type: %d", p.Header.PageType))
	}
	return &c, nil
}

func (c *cell) ParseColumnMap() {
	if len(c.ColumnMap) > 0 {
		return
	}
	start := c.HeaderOffsetFromN(len(c.Header) - 1)
	end := start + c.Header[len(c.Header)-1].Size
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
		name = cleanKeyString(name)
		name = strings.Split(name, " ")[0]
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
	d := c.Data[:c.Header[0].Size]
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
		offset += c.Header[i].Size
	}
	return offset
}

func (c *cell) TableName() (string, error) {
	if c.CellType() == CellTypeUnknown {
		return "", errors.New(fmt.Sprintf("cannot get tablename: cell %d is unknown type", c.RowID))
	}
	offset := c.HeaderOffsetFromN(2)
	return cleanKeyString(string(c.Data[offset : offset+c.Header[2].Size])), nil
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
		c.Header[2].Type != SerialText {
		return 0, errors.New("unexpected header types")
	}
	val, err := c.ReadDataFromHeaderIndex(3)
	if err != nil {
		return 0, err
	}
	return val.(int64), nil
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
		c.Header = append(c.Header, newCellHeader(variant))
	}
	// read payload data
	dataBuf := make([]byte, c.PayloadSize)
	read, err = dataReader.ReadAt(dataBuf, offset)
	if err != nil {
		return err
	}
	c.Data = dataBuf
	offset += int64(read)
	var overflowPage uint32
	if err := readBigEndianInt(buf[offset:offset+4], &overflowPage); err != nil {
		return err
	}
	c.FirstOverflow = uint32(overflowPage)
	return nil
}

// interior table only contains the left child
// page number and the row id of the cell
func parseInteriorTableCell(buf []byte, c *cell) error {
	if err := readBigEndianInt(buf[:4], &c.LeftPageNumber); err != nil {
		return err
	}
	rowID, _ := readVarint(buf[4:])
	c.RowID = rowID
	return nil
}

func parseLeafIndexCell(buf []byte, c *cell) error {
	var offset int64 = 0
	// get payload length in bytes (which includes header size)
	payloadLength, read := readVarint(buf[offset:])
	offset += int64(read)
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
		c.Header = append(c.Header, newCellHeader(variant))
	}
	// read payload data
	dataBuf := make([]byte, c.PayloadSize)
	read, err = dataReader.ReadAt(dataBuf, offset)
	if err != nil {
		return err
	}
	c.Data = dataBuf
	offset += int64(read)
	var overflowPage uint32
	if err := readBigEndianInt(buf[offset:offset+4], &overflowPage); err != nil {
		return err
	}
	c.FirstOverflow = uint32(overflowPage)
	return nil
}

// index interior contains left child ptr,
// varint with payload size, then payload
func parseInteriorIndexCell(buf []byte, c *cell) error {
	if err := readBigEndianInt(buf[:4], &c.LeftPageNumber); err != nil {
		return err
	}
	var offset int64 = 4
	// get payload length in bytes (which includes header size)
	payloadLength, read := readVarint(buf[offset:])
	offset += int64(read)
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
		c.Header = append(c.Header, newCellHeader(variant))
	}
	// read payload data
	dataBuf := make([]byte, c.PayloadSize)
	read, err = dataReader.ReadAt(dataBuf, offset)
	if err != nil {
		return err
	}
	c.Data = dataBuf
	offset += int64(read)
	var overflowPage uint32
	if err := readBigEndianInt(buf[offset:offset+4], &overflowPage); err != nil {
		return err
	}
	c.FirstOverflow = uint32(overflowPage)
	return nil
}

func (c *cell) ReadDataFromHeaderIndex(headerIdx int) (any, error) {
	h := c.Header[headerIdx]
	start := c.HeaderOffsetFromN(headerIdx)
	end := start + h.Size
	data := c.Data[start:end]
	switch h.Type {
	case 1:
		return int64(int8(data[0])), nil
	case 2:
		return int64(int16(binary.BigEndian.Uint16(data))), nil
	case 3:
		var val int32
		val |= int32(data[0]) << 16
		val |= int32(data[1]) << 8
		val |= int32(data[2])
		// Check if it's negative and convert it accordingly
		if val&(1<<23) != 0 {
			val |= ^((1 << 24) - 1)
		}
		return int64(val), nil
	case 4:
		return int64(int32(binary.BigEndian.Uint32(data))), nil
	case 5:
		var val int64
		val |= int64(data[0]) << 40
		val |= int64(data[1]) << 32
		val |= int64(data[2]) << 24
		val |= int64(data[3]) << 16
		val |= int64(data[4]) << 8
		val |= int64(data[5])
		// Check if it's negative and convert it accordingly
		if val&(1<<47) != 0 {
			val |= ^((1 << 48) - 1)
		}
		return val, nil
	case 6:
		return int64(binary.BigEndian.Uint64(data)), nil
	case 7:
		return math.Float64frombits(binary.BigEndian.Uint64(data)), nil
	case 8:
		return 0, nil
	case 9:
		return 1, nil
	case 12:
	case 13:
		return string(data), nil
	}
	return 0, fmt.Errorf("unsupported format: %d", h.Type)
}

func (p *cell) String() string {
	switch p.PageType {
	case LeafTableType:
		if len(p.ColumnMap) > 0 {
			return primitiveStructString(struct {
				CellOffset    int64
				FirstOverflow uint32
				HeaderSize    uint8
				PayloadSize   uint64
				RowID         int64
				ColumnMap     columnMap
				Header        []cellHeader
			}{
				CellOffset:    p.Offset,
				FirstOverflow: p.FirstOverflow,
				HeaderSize:    p.HeaderSize,
				PayloadSize:   p.PayloadSize,
				RowID:         p.RowID,
				Header:        p.Header,
				ColumnMap:     p.ColumnMap,
			})
		} else {
			return primitiveStructString(struct {
				CellOffset    int64
				FirstOverflow uint32
				HeaderSize    uint8
				PayloadSize   uint64
				RowID         int64
				Header        []cellHeader
				Data          string
			}{
				CellOffset:    p.Offset,
				FirstOverflow: p.FirstOverflow,
				HeaderSize:    p.HeaderSize,
				PayloadSize:   p.PayloadSize,
				RowID:         p.RowID,
				Header:        p.Header,
				Data:          string(p.Data),
			})
		}
	case LeafIndexType:
		return primitiveStructString(struct {
			CellOffset    int64
			FirstOverflow uint32
			HeaderSize    uint8
			PayloadSize   uint64
			Header        []cellHeader
			Data          string
		}{
			CellOffset:    p.Offset,
			FirstOverflow: p.FirstOverflow,
			HeaderSize:    p.HeaderSize,
			PayloadSize:   p.PayloadSize,
			Header:        p.Header,
			Data:          string(p.Data),
		})

	case InteriorIndexType:
		return primitiveStructString(struct {
			CellOffset     int64
			FirstOverflow  uint32
			LeftPageNumber uint32
			PayloadSize    uint64
			Header         []cellHeader
			Data           string
		}{
			CellOffset:     p.Offset,
			FirstOverflow:  p.FirstOverflow,
			LeftPageNumber: p.LeftPageNumber,
			PayloadSize:    p.PayloadSize,
			Header:         p.Header,
			Data:           string(p.Data),
		})
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
