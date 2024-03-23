package main

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	DefaultPageHeaderSize    = 8
	InteriorPageHeaderOffset = 4
	InteriorIndexType        = 2
	InteriorTableType        = 5
	LeafIndexType            = 10
	LeafTableType            = 13
)

type pageHeader struct {
	PageType            uint8
	FirstFreeBlock      uint16
	CellCount           uint16
	CellContent         uint16
	FragmentedFreeBytes uint8
	RightMostPointer    uint32
}

func newPageHeader(f io.ReadSeeker, offset int64) (*pageHeader, error) {
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, DefaultPageHeaderSize)
	if _, err := f.Read(buf); err != nil {
		return nil, err
	}
	p := pageHeader{}
	if err := readBigEndianInt(buf[:1], &p.PageType); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(buf[1:3], &p.FirstFreeBlock); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(buf[3:5], &p.CellCount); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(buf[5:7], &p.CellContent); err != nil {
		return nil, err
	}
	if err := readBigEndianInt(buf[7:8], &p.FragmentedFreeBytes); err != nil {
		return nil, err
	}
	if p.PageType == InteriorTableType {
		extBuf := make([]byte, InteriorPageHeaderOffset)
		if _, err := f.Read(extBuf); err != nil {
			return nil, err
		}
		if err := readBigEndianInt(extBuf, &p.RightMostPointer); err != nil {
			return nil, err
		}
	}
	return &p, nil
}

func (p *pageHeader) String() string {
	return primitiveStructString(p)
}

type page struct {
	Offset   int64
	IsRoot   bool
	PageSize uint16
	Header   *pageHeader
	Cells    []*cell
}

func newPage(f io.ReadSeeker, root bool, pageSize uint16, offset int64) (*page, error) {
	header, err := newPageHeader(f, offset)
	if err != nil {
		return nil, err
	}
	p := page{Header: header, IsRoot: root, PageSize: pageSize, Offset: offset}
	cellPtrBuf := make([]byte, p.Header.CellCount*2)
	if _, err := f.Read(cellPtrBuf); err != nil {
		return nil, err
	}
	for i := 0; i < int(p.Header.CellCount); i++ {
		var cellPtr uint16
		if err := readBigEndianInt(cellPtrBuf[i*2:i*2+2], &cellPtr); err != nil {
			return nil, err
		}
		c, err := newCell(f, &p, int64(cellPtr))
		if err != nil {
			fmt.Println("SAD")
			return nil, err
		}
		p.Cells = append(p.Cells, c)
	}
	return &p, nil
}

func (p *page) TablesNames() []string {
	s := []string{}
	for _, c := range p.Cells {
		name, err := c.TableName()
		if err != nil {
			continue
		}
		s = append(s, name)
	}
	return s
}

func (p *page) CellFromTableName(t string) (*cell, error) {
	for _, c := range p.Cells {
		name, err := c.TableName()
		if err != nil || name != t {
			continue
		}
		return c, nil
	}
	return nil, errors.New("table was not found: " + t)
}

func (p *page) String() string {
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("Page Offset:%s%d\n", repeatStringDefault(11), p.Offset))
	buf.WriteString(fmt.Sprintf("%s\n", p.Header.String()))
	for i, c := range p.Cells {
		buf.WriteString(
			fmt.Sprintf("Cell:%s%d\n%s\n", repeatStringDefault(4), i+1, c.String()))
	}
	return buf.String()
}
