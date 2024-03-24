package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

var (
	LeniantCleanKeyRegexp = regexp.MustCompile("\\[|\\]")
	CleanKeyRegexp        = regexp.MustCompile("\"|\\[|\\]")
)

func cleanKeyString(key string) string {
	k := CleanKeyRegexp.ReplaceAllString(key, "")
	return strings.ToLower(k)
}

func leniantCleanKeyString(key string) string {
	k := LeniantCleanKeyRegexp.ReplaceAllString(key, "")
	return strings.ToLower(k)
}

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

func readVarint(buf []byte) (int64, int) {
	var varint int64 = 0
	var read int = 0
	for i, b := range buf {
		bb := int64(b)
		read += 1
		if i == 8 {
			varint = (varint << 8) | bb
			break
		} else {
			varint = (varint << 7) | (bb & 0x7f)
			if bb < 0x80 {
				break
			}
		}
	}
	return varint, read
}

func readVarints(data []byte) ([]int64, int) {
	varints := []int64{}
	i := 0
	for i < len(data) {
		varint, read := readVarint(data[i:])
		varints = append(varints, varint)
		i += read
	}
	return varints, i
}
