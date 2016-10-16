package utils

import (
	"bufio"
)

// big endian

// BytesToUint64 ()
func BytesToUint64(b []byte) (v uint64) {
	length := uint(len(b))
	for i := uint(0); i < length-1; i++ {
		v += uint64(b[i])
		v <<= 8
	}
	v += uint64(b[length-1])
	return
}

// BytesToUint32 ()
func BytesToUint32(b []byte) (v uint32) {
	length := uint(len(b))
	for i := uint(0); i < length-1; i++ {
		v += uint32(b[i])
		v <<= 8
	}
	v += uint32(b[length-1])
	return
}

// BytesToUint16 ()
func BytesToUint16(b []byte) (v uint16) {
	v += uint16(b[0])
	v <<= 8
	v += uint16(b[1])
	return
}

// Uint64toBytes ()
func Uint64toBytes(b []byte, v uint64) {
	for i := uint(0); i < 8; i++ {
		b[7-i] = byte(v >> (i * 8))
	}
}

// Uint32toBytes ()
func Uint32toBytes(b []byte, v uint32) {
	for i := uint(0); i < 4; i++ {
		b[3-i] = byte(v >> (i * 8))
	}
}

// Uint16toBytes ()
func Uint16toBytes(b []byte, v uint16) {
	b[0] = byte(v >> 8)
	b[1] = byte(v)
}

// Uint8toBytes ()
func Uint8toBytes(b []byte, v uint8) {
	b[0] = byte(v)
}

// BigEndian -
var BigEndian bigEndian

type bigEndian struct{}

func (bigEndian) Uint16(b []byte) uint16 { return uint16(b[1]) | uint16(b[0])<<8 }

func (bigEndian) PutUint16(b []byte, v uint16) {
	b[0] = byte(v >> 8)
	b[1] = byte(v)
}

func (bigEndian) Int32(b []byte) int32 {
	return int32(b[3]) | int32(b[2])<<8 | int32(b[1])<<16 | int32(b[0])<<24
}

func (bigEndian) Uint32(b []byte) uint32 {
	return uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24
}

func (bigEndian) PutUint32(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}

func (bigEndian) WriteUint32(w *bufio.Writer, v uint32) (err error) {
	if err = w.WriteByte(byte(v >> 24)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 16)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 8)); err != nil {
		return
	}
	err = w.WriteByte(byte(v))
	return
}

func (bigEndian) PutInt32(b []byte, v int32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}

func (bigEndian) WriteInt32(w *bufio.Writer, v int32) (err error) {
	if err = w.WriteByte(byte(v >> 24)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 16)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 8)); err != nil {
		return
	}
	err = w.WriteByte(byte(v))
	return
}

func (bigEndian) Int64(b []byte) int64 {
	return int64(b[7]) | int64(b[6])<<8 | int64(b[5])<<16 | int64(b[4])<<24 |
		int64(b[3])<<32 | int64(b[2])<<40 | int64(b[1])<<48 | int64(b[0])<<56
}

func (bigEndian) Uint64(b []byte) uint64 {
	return uint64(b[7]) | uint64(b[6])<<8 | uint64(b[5])<<16 | uint64(b[4])<<24 |
		uint64(b[3])<<32 | uint64(b[2])<<40 | uint64(b[1])<<48 | uint64(b[0])<<56
}

func (bigEndian) PutInt64(b []byte, v int64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

func (bigEndian) WriteInt64(w *bufio.Writer, v int64) (err error) {
	if err = w.WriteByte(byte(v >> 56)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 48)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 40)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 32)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 24)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 16)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 8)); err != nil {
		return
	}
	err = w.WriteByte(byte(v))
	return
}
