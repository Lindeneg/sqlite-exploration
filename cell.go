package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

type cell struct {
	Offset            int64
	PageType          uint8
	LeftPageNumber    uint32
	HeaderSize        uint8
	PayloadSize       uint64
	RowID             uint8
	Data              []byte
	FirstOverflowPage uint32
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

// Leaf Table starts with two variants, then a byte array
// and then a 4-byte integer for overflow page ptr
func parseLeafTableCell(buf []byte, c *cell) error {
	var offset int64 = 0
	payloadLength, read, err := readVarint(buf)
	if err != nil {
		return err
	}
	offset += int64(read)
	rowID, read, err := readVarint(buf[offset:])
	if err != nil {
		return err
	}
	offset += int64(read)
	c.RowID = uint8(rowID)
	headerLength, read, err := readVarint(buf[offset:])
	if err != nil {
		return err
	}
	c.HeaderSize = uint8(headerLength)
	c.PayloadSize = uint64(payloadLength) - uint64(c.HeaderSize)
	// TODO: READ HEADER
	// stuff...

	// Read Data
	offset += int64(c.HeaderSize)
	dataReader := bytes.NewReader(buf)
	dataBuf := make([]byte, c.PayloadSize)
	n, _ := dataReader.ReadAt(dataBuf, offset)
	c.Data = dataBuf

	// Read page number of first overflow page
	offset += int64(n)
	var overflowPage uint32
	if err := readBigEndianInt(buf[offset:offset+4], &overflowPage); err != nil {
		return err
	}
	c.FirstOverflowPage = overflowPage
	return nil
}

func parseInteriorTable(buf []byte, c *cell) error {
	if err := readBigEndianInt(buf[:4], &c.LeftPageNumber); err != nil {
		return err
	}
	if err := readBigEndianInt(buf[4:5], &c.RowID); err != nil {
		return err
	}
	return nil
}

func (p *cell) String() string {
	switch p.PageType {
	case LeafTableType:
		return primitiveStructString(struct {
			Offset            int64
			HeaderSize        uint8
			PayloadSize       uint64
			RowID             uint8
			FirstOverflowPage uint32
			Data              string
		}{
			Offset:            p.Offset,
			HeaderSize:        p.HeaderSize,
			PayloadSize:       p.PayloadSize,
			RowID:             p.RowID,
			FirstOverflowPage: p.FirstOverflowPage,
			Data:              string(p.Data),
		})
	case InteriorTableType:
		return primitiveStructString(struct {
			Offset         int64
			LeftPageNumber uint32
			RowID          uint8
		}{
			Offset:         p.Offset,
			LeftPageNumber: p.LeftPageNumber,
			RowID:          p.RowID,
		})
	case LeafIndexType:
		return primitiveStructString(struct {
			Offset            int64
			HeaderSize        uint8
			PayloadSize       uint64
			FirstOverflowPage uint32
			Data              string
		}{
			Offset:            p.Offset,
			HeaderSize:        p.HeaderSize,
			PayloadSize:       p.PayloadSize,
			FirstOverflowPage: p.FirstOverflowPage,
			Data:              string(p.Data),
		})
	case InteriorIndexType:
		return primitiveStructString(struct {
			Offset            int64
			LeftPageNumber    uint32
			HeaderSize        uint8
			PayloadSize       uint64
			FirstOverflowPage uint32
			Data              string
		}{
			Offset:            p.Offset,
			LeftPageNumber:    p.LeftPageNumber,
			HeaderSize:        p.HeaderSize,
			PayloadSize:       p.PayloadSize,
			FirstOverflowPage: p.FirstOverflowPage,
			Data:              string(p.Data),
		})
	}
	return ""
}
