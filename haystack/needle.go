package haystack

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"syscall"
	"time"

	"github.com/uukuguy/kds/utils"
	log "github.com/uukuguy/kds/utils/logger"
)

const (
	NeedlePaddingSize = 8

	NeedleMagicSize    = 4
	NeedleCookieSize   = 4
	NeedleKeySize      = 8
	NeedleFlagsSize    = 1
	NeedleSizeSize     = 4
	NeedleChecksumSize = 4

	// constant 21
	NeedleHeaderSize = NeedleMagicSize + NeedleCookieSize + NeedleKeySize + NeedleFlagsSize + NeedleSizeSize
	// constant 8
	NeedleFooterSize = NeedleMagicSize + NeedleChecksumSize

	NeedleMagicOffset  = 0
	NeedleCookieOffset = NeedleMagicOffset + NeedleMagicSize
	NeedleKeyOffset    = NeedleCookieOffset + NeedleCookieSize
	NeedleFlagsOffset  = NeedleKeyOffset + NeedleKeySize
	NeedleSizeOffset   = NeedleFlagsOffset + NeedleFlagsSize

	NeedleDataOffset = NeedleSizeOffset + NeedleSizeSize

	NeedleChecksumOffset = NeedleMagicOffset + NeedleMagicSize
	NeedlePaddingOffset  = NeedleChecksumOffset + NeedleChecksumSize
)

// **************** Needle ****************
type Needle struct {
	HeaderMagic []byte
	Cookie      int32
	Key         int64
	MTime       int64
	Flags       byte
	Size        uint32
	Data        []byte
	//DataReader  *io.Reader
	FooterMagic []byte
	Checksum    uint32
	Padding     []byte

	WriteSize   uint32 // 对齐后实际写入的字节数
	PaddingSize uint32 // 对齐后增加的字节数
	FooterSize  uint32 // 对齐后底部区域的字节数
	AlignedSize uint32 // needle.WriteSize / NeedlePaddingSize

	//IncrOffset uint32

	buffer []byte
}

var (
	headerMagic       = []byte{0x14, 0x15, 0x92, 0x65}
	footerMagic       = []byte{0x35, 0x89, 0x79, 0x32}
	flagNeedleOK      = byte(0)
	flagNeedleDeleted = byte(1)
	// crc32 checksum table, goroutine safe
	crc32Table = crc32.MakeTable(crc32.Koopman)

	padding = [][]byte{nil}

	sysPageSize     = syscall.Getpagesize()
	cacheBufferSize = sysPageSize * 1
	bufferPool      = sync.Pool{
		New: func() interface{} {
			return make([]byte, cacheBufferSize)
		},
	}
)

// -------- init() --------
func init() {
	for i := 1; i < NeedlePaddingSize; i++ {
		padding = append(padding, bytes.Repeat([]byte{byte(0)}, i))
	}
	return
}

// NewNeedle ()
// ======== NewNeedle() ========
func NewNeedle(key int64, cookie int32, size uint32) *Needle {
	needle := new(Needle)
	needle.Cookie = cookie
	needle.Key = key
	needle.Size = size

	needle.init()

	return needle
}

// -------- init() --------
func (_this *Needle) init() {
	_this.HeaderMagic = headerMagic
	_this.MTime = time.Now().Unix()
	// str_mtime = time.Unix(MTime, 0).Format("2006-01-02 15:04:05")
	_this.Flags = flagNeedleOK
	_this.FooterMagic = footerMagic

	_this.adjustSize()
	if _this.PaddingSize > 0 {
		_this.Padding = padding[_this.PaddingSize]
	}

}

// Renew ()
// ======== Renew() ========
func (_this *Needle) Renew(size uint32) {
	_this.Size = size
	_this.init()
}

// DataBuffer ()
// ======== DataBuffer() ========
func (_this *Needle) DataBuffer() []byte {
	return _this.buffer[NeedleDataOffset : NeedleDataOffset+_this.Size]
}

// Close ()
// ======== Close() ========
func (_this *Needle) Close() {
	_this.freeBuffer()
}

// ReadFrom ()
// ======== ReadFrom() ========
func (_this *Needle) ReadFrom(file io.Reader) (err error) {
	_this.Data = make([]byte, _this.Size)
	if _, err = file.Read(_this.Data); err != nil {
		return
	}
	_this.Checksum = crc32.Update(0, crc32Table, _this.Data)
	return
}

// NeedleOffset convert offset to needle offset.
func NeedleOffset(offset int64) uint32 {
	return uint32(offset / NeedlePaddingSize)
}

// BlockOffset get super block file offset.
func BlockOffset(offset uint32) int64 {
	return int64(offset) * NeedlePaddingSize
}

// -------- paddingSize() --------
func paddingSize(d uint32) uint32 {
	var paddingSize uint32
	if a := d % NeedlePaddingSize; a > 0 {
		paddingSize = NeedlePaddingSize - a
	}

	return paddingSize
}

// -------- align() --------
func align(d uint32) uint32 {
	//return (d + NeedlePaddingSize - 1) & ^(NeedlePaddingSize - 1)
	return d + paddingSize(d)
}

// Size get a needle size with meta data.
func Size(n uint32) uint32 {
	return align(NeedleHeaderSize + n + NeedleFooterSize)
}

// -------- adjustSize() --------
func (_this *Needle) adjustSize() {
	_this.WriteSize = uint32(NeedleHeaderSize + _this.Size + NeedleFooterSize)
	//_this.PaddingSize = align(_this.WriteSize) - _this.WriteSize
	_this.PaddingSize = paddingSize(_this.WriteSize)
	_this.WriteSize += _this.PaddingSize
	_this.FooterSize = NeedleFooterSize + _this.PaddingSize
	_this.AlignedSize = uint32(int64(_this.WriteSize) / NeedlePaddingSize)
}

// -------- newBuffer() --------
func (_this *Needle) newBuffer() {
	if _this.WriteSize <= uint32(cacheBufferSize) {
		_this.buffer = bufferPool.Get().([]byte)
	} else {
		_this.buffer = make([]byte, _this.WriteSize)
	}
}

// -------- freeBuffer() --------
func (_this *Needle) freeBuffer() {
	if _this.buffer != nil && len(_this.buffer) <= cacheBufferSize {
		bufferPool.Put(_this.buffer)
	}
}

// FillBuffer ()
// ======== FillBuffer() ========
func (_this *Needle) FillBuffer() (err error) {
	_this.newBuffer()
	_this.fillHeaderBuffer(_this.buffer[NeedleMagicOffset:NeedleDataOffset])
	footerOffset := NeedleDataOffset + _this.Size
	_this.fillDataBuffer(_this.buffer[NeedleDataOffset:footerOffset])
	_this.fillFooterBuffer(_this.buffer[footerOffset : footerOffset+_this.FooterSize])
	return
}

// Buffer ()
// ======== Buffer() ========
func (_this *Needle) Buffer() []byte {

	return _this.buffer[:_this.WriteSize]
}

func (_this *Needle) fillDataBuffer(buf []byte) (err error) {
	if len(buf) != int(_this.Size) {
		err = fmt.Errorf("Wrong buffer size for needle data")
		return
	}

	copy(buf, _this.Data)
	return
}

// -------- fillHeaderBuffer() --------
func (_this *Needle) fillHeaderBuffer(buf []byte) (err error) {
	if len(buf) != int(NeedleHeaderSize) {
		err = fmt.Errorf("Wrong buffer size for needle header")
		return
	}
	// magic
	copy(buf[NeedleMagicOffset:NeedleCookieOffset], _this.HeaderMagic)
	// cookie
	utils.BigEndian.PutInt32(buf[NeedleCookieOffset:NeedleKeyOffset], _this.Cookie)
	// key
	utils.BigEndian.PutInt64(buf[NeedleKeyOffset:NeedleFlagsOffset], _this.Key)
	// flags
	buf[NeedleFlagsOffset] = _this.Flags
	// size
	utils.BigEndian.PutUint32(buf[NeedleSizeOffset:NeedleDataOffset], _this.Size)
	return
}

// -------- fillFooterBuffer() --------
func (_this *Needle) fillFooterBuffer(buf []byte) (err error) {
	if len(buf) != int(_this.FooterSize) {
		err = fmt.Errorf("Wrong buffer size for needle footer")
		return
	}
	// magic
	copy(buf[NeedleMagicOffset:NeedleChecksumOffset], _this.FooterMagic)
	// checksum
	utils.BigEndian.PutUint32(buf[NeedleChecksumOffset:NeedlePaddingOffset], _this.Checksum)
	// padding
	copy(buf[NeedlePaddingOffset:NeedlePaddingOffset+_this.PaddingSize], _this.Padding)
	return
}

// ======== BuildFrom() ========
func (_this *Needle) BuildFrom(buf []byte) (err error) {

	if !bytes.Equal(buf[NeedleMagicOffset:NeedleCookieOffset], headerMagic) {
		err = fmt.Errorf("Needle header magic is wrong.")
		log.Errorf("BuildFrom() failed. %v", err)
		return
	}

	_this.Cookie = utils.BigEndian.Int32(buf[NeedleCookieOffset:NeedleKeyOffset])
	_this.Key = utils.BigEndian.Int64(buf[NeedleKeyOffset:NeedleFlagsOffset])
	_this.Size = utils.BigEndian.Uint32(buf[NeedleSizeOffset:NeedleDataOffset])
	_this.init()

	_this.Flags = buf[NeedleFlagsOffset]
	_this.Checksum = utils.BigEndian.Uint32(buf[NeedleChecksumOffset:NeedlePaddingOffset])
	_this.Data = make([]byte, _this.Size)
	copy(_this.Data, buf[NeedleDataOffset:NeedleDataOffset+_this.Size])
	//utils.LogDebugf("copy Data(offset=%d size=%d) into needle. %s", NeedleDataOffset, _this.Size, _this.Data)
	//utils.LogDebugf("buf: %#v", buf[NeedleDataOffset:NeedleDataOffset+_this.Size])
	//utils.LogDebugf("needle: %#v", *_this)

	return
}

// String ()
// ======== Needle::String() ========
func (_this *Needle) String() string {
	var dn = 16
	if len(_this.Data) < dn {
		dn = len(_this.Data)
	}
	return fmt.Sprintf(`
-----------------------------
WriteSize:      %d

---- head
HeaderSize:     %d
HeaderMagic:    %#v
Cookie:         %d
Key:            %d
Flags:          %d
Size:           %d

---- data
Data:           %#v...

---- foot
FooterSize:     %d
FooterMagic:    %#v
Checksum:       %d
Padding:        %v
-----------------------------
`, _this.WriteSize, NeedleHeaderSize, _this.HeaderMagic, _this.Cookie, _this.Key, _this.Flags, _this.Size,
		_this.Data[:dn], _this.FooterSize, _this.FooterMagic, _this.Checksum, _this.Padding)
}
