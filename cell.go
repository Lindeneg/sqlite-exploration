package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

type serialType int

const (
	SERIAL_NULL serialType = iota
	SERIAL_8_TWOS_COMPLEMENT
	SERIAL_16_TWOS_COMPLEMENT
	SERIAL_24_TWOS_COMPLEMENT
	SERIAL_32_TWOS_COMPLEMENT
	SERIAL_48_TWOS_COMPLEMENT
	SERIAL_64_TWOS_COMPLEMENT
	SERIAL_754_2008_64_FLOAT
	SERIAL_0
	SERIAL_1
	SERIAL_INTERNAL_1
	SERIAL_INTERNAL_2
	SERIAL_BLOB
	SERIAL_TEXT
)

type cellHeader struct {
	Type  serialType
	Value int64
}

func (c cellHeader) String() string {
	return fmt.Sprintf("(Type=%d,Value=%d)", c.Type, c.Value)
}

type cell struct {
	Offset            int64
	PageType          uint8
	LeftPageNumber    uint32
	HeaderSize        uint8
	PayloadSize       uint64
	RowID             uint8
	FirstOverflowPage uint32
	Header            []cellHeader
	Data              []byte
}

func newCell(f io.ReadSeeker, p *page, offset int64) (*cell, error) {
	cellOffset := offset
	if !p.IsRoot {
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
	c := cell{Offset: offset, PageType: p.Header.PageType}
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

// leaf table starts with two variants, then a byte array
// and then a 4-byte integer for overflow page ptr
func parseLeafTableCell(buf []byte, c *cell) error {
	var offset int64 = 0
	// get payload length in bytes (which includes header size)
	payloadLength, read := readVariant(buf)
	offset += int64(read)
	// get row id of cell
	rowID, read := readVariant(buf[offset:])
	offset += int64(read)
	c.RowID = uint8(rowID)
	// get the header length
	headerLength, read := readVariant(buf[offset:])
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
	variants, _ := readVariants(headerBuf[1:])
	// parse variants
	for _, variant := range variants {
		if variant > int64(SERIAL_TEXT) && variant%2 == 1 {
			c.Header = append(c.Header, cellHeader{Type: SERIAL_TEXT, Value: (variant - 13) / 2})
			continue
		}
		if variant > int64(SERIAL_BLOB) && variant%2 == 0 {
			c.Header = append(c.Header, cellHeader{Type: SERIAL_BLOB, Value: (variant - 12) / 2})
			continue
		}
		// probably much stupid
		c.Header = append(c.Header, cellHeader{Type: serialType(variant), Value: variant})
	}
	// read payload data
	dataBuf := make([]byte, c.PayloadSize)
	read, err = dataReader.ReadAt(dataBuf, offset)
	if err != nil {
		return err
	}
	c.Data = dataBuf
	// read page number of first overflow page
	// these four bytes are 0, if there is no such page
	offset += int64(read)
	var overflowPage uint32
	if err := readBigEndianInt(buf[offset:offset+4], &overflowPage); err != nil {
		return err
	}
	c.FirstOverflowPage = overflowPage
	return nil
}

// interior table only contains the left child
// page number and the row id of the cell
func parseInteriorTable(buf []byte, c *cell) error {
	if err := readBigEndianInt(buf[:4], &c.LeftPageNumber); err != nil {
		return err
	}
	if err := readBigEndianInt(buf[4:5], &c.RowID); err != nil {
		return err
	}
	return nil
}

func (c *cell) IsTable() bool {
	dataLength := len(c.Data)
	if dataLength <= 0 {
		return false
	}
	if len(c.Header) < 3 ||
		c.Header[0].Type != SERIAL_TEXT ||
		c.Header[1].Type != SERIAL_TEXT ||
		c.Header[2].Type != SERIAL_TEXT {
		return false
	}
	start := c.Header[0].Value + c.Header[1].Value + c.Header[2].Value + 1
	end := start + 12
	if end > int64(dataLength-1) {
		return false
	}
	return string(c.Data[start:end]) == "CREATE TABLE"
}

// this is kind of stupid, whole thing probably is actually
func (p *cell) String() string {
	switch p.PageType {
	case LeafTableType:
		return primitiveStructString(struct {
			CellOffset        int64
			HeaderSize        uint8
			PayloadSize       uint64
			RowID             uint8
			FirstOverflowPage uint32
			Header            []cellHeader
			Data              string
		}{
			CellOffset:        p.Offset,
			HeaderSize:        p.HeaderSize,
			PayloadSize:       p.PayloadSize,
			RowID:             p.RowID,
			FirstOverflowPage: p.FirstOverflowPage,
			Header:            p.Header,
			Data:              string(p.Data),
		})
	case InteriorTableType:
		return primitiveStructString(struct {
			CellOffset     int64
			LeftPageNumber uint32
			RowID          uint8
		}{
			CellOffset:     p.Offset,
			LeftPageNumber: p.LeftPageNumber,
			RowID:          p.RowID,
		})
	case LeafIndexType:
		return primitiveStructString(struct {
			CellOffset        int64
			HeaderSize        uint8
			PayloadSize       uint64
			FirstOverflowPage uint32
			Header            []cellHeader
			Data              string
		}{
			CellOffset:        p.Offset,
			HeaderSize:        p.HeaderSize,
			PayloadSize:       p.PayloadSize,
			FirstOverflowPage: p.FirstOverflowPage,
			Header:            p.Header,
			Data:              string(p.Data),
		})
	case InteriorIndexType:
		return primitiveStructString(struct {
			CellOffset        int64
			LeftPageNumber    uint32
			HeaderSize        uint8
			PayloadSize       uint64
			FirstOverflowPage uint32
			Header            []cellHeader
			Data              string
		}{
			CellOffset:        p.Offset,
			LeftPageNumber:    p.LeftPageNumber,
			HeaderSize:        p.HeaderSize,
			PayloadSize:       p.PayloadSize,
			FirstOverflowPage: p.FirstOverflowPage,
			Header:            p.Header,
			Data:              string(p.Data),
		})
	}
	return ""
}
