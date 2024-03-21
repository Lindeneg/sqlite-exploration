package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
)

func readBigEndianInt(b []byte, out any) error {
	return binary.Read(bytes.NewReader(b), binary.BigEndian, out)
}

func repeatString(availableSpace int, occupiedSpace int, sym string) string {
	diff := availableSpace - occupiedSpace
	if diff > 0 {
		return strings.Repeat(sym, diff)
	}
	return sym
}

func repeatStringDefault(occupiedSpace int) string {
	diff := 32 - occupiedSpace
	if diff > 0 {
		return strings.Repeat(" ", diff)
	}
	return " "
}

func primitiveStructString(d any) string {
	var buf strings.Builder
	s := reflect.ValueOf(d)
	if s.Kind() == reflect.Pointer {
		s = s.Elem()
	}
	sType := s.Type()
	for i := 0; i < s.NumField(); i++ {
		key := sType.Field(i).Name
		value := s.Field(i).Interface()
		buf.WriteString(fmt.Sprintf("%s:%s%v\n", key, repeatStringDefault(len(key)), value))
	}
	return buf.String()
}

func pageNumberToOffset(pageSize int64, pageNumber int64) int64 {
	return pageSize * (pageNumber - 1)
}

func offsetToPageNumber(pageSize int64, offset int64) int64 {
	return (offset / pageSize) + 1
}

func readVarint(data []byte) (int64, int, error) {
	endIdx := 0
	for i := 0; i < len(data); i++ {
		endIdx++
		if data[i]&0x80 == 0 {
			break
		}
	}
	var val int64 = 0
	dataSlice := data[:endIdx]
	for i, d := range dataSlice {
		if i == len(dataSlice)-1 {
			val += int64(d)
			break
		}
		var n int64 = 0
		if i > 0 {
			n = int64(i) - 1
		}
		val += (int64(d) - 128) * (128 ^ n)
	}
	return val, endIdx, nil
}
