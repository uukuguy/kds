package haystack

import (
	"bytes"
	"fmt"
	"github.com/uukuguy/kds/utils"
	"sync"
	"syscall"
	"time"
)

const (
	NEEDLE_PADDINGSIZE = 8

	NEEDLE_MAGIC_SIZE  = 4
	NEEDLE_COOKIE_SIZE = 4
	NEEDLE_KEY_SIZE    = 8
	NEEDLE_FLAGS_SIZE  = 1
	NEEDLE_SIZE_SIZE   = 4

	NEEDLE_CHECKSUM_SIZE = 4

	// constant 21
	NEEDLE_HEADER_SIZE = NEEDLE_MAGIC_SIZE + NEEDLE_COOKIE_SIZE + NEEDLE_KEY_SIZE + NEEDLE_FLAGS_SIZE + NEEDLE_SIZE_SIZE
	// constant 8
	NEEDLE_FOOTER_SIZE = NEEDLE_MAGIC_SIZE + NEEDLE_CHECKSUM_SIZE

	NEEDLE_MAGIC_OFFSET  = 0
	NEEDLE_COOKIE_OfFSET = NEEDLE_MAGIC_OFFSET + NEEDLE_MAGIC_SIZE
	NEEDLE_KEY_OFFSET    = NEEDLE_COOKIE_OfFSET + NEEDLE_COOKIE_SIZE
	NEEDLE_FLAGS_OFFSET  = NEEDLE_KEY_OFFSET + NEEDLE_KEY_SIZE
	NEEDLE_SIZE_OFFSET   = NEEDLE_FLAGS_OFFSET + NEEDLE_FLAGS_SIZE

	NEEDLE_DATA_OFFSET = NEEDLE_SIZE_OFFSET + NEEDLE_SIZE_SIZE

	NEEDLE_CHECKSUM_OFFSET = NEEDLE_MAGIC_OFFSET + NEEDLE_MAGIC_SIZE
	NEEDLE_PADDING_OFFSET  = NEEDLE_CHECKSUM_OFFSET + NEEDLE_CHECKSUM_SIZE
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
	AlignedSize uint32 // needle.WriteSize / NEEDLE_PADDINGSIZE

	//IncrOffset uint32

	buffer []byte
}

var (
	headerMagic       = []byte{0x14, 0x15, 0x92, 0x65}
	footerMagic       = []byte{0x35, 0x89, 0x79, 0x32}
	flagNeedleOK      = byte(0)
	flagNeedleDeleted = byte(1)

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
	for i := 1; i < NEEDLE_PADDINGSIZE; i++ {
		padding = append(padding, bytes.Repeat([]byte{byte(0)}, i))
	}
	return
}

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
func (needle *Needle) init() {
	needle.HeaderMagic = headerMagic
	needle.MTime = time.Now().Unix()
	// str_mtime = time.Unix(MTime, 0).Format("2006-01-02 15:04:05")
	needle.Flags = flagNeedleOK
	needle.FooterMagic = footerMagic

	needle.adjustSize()
	if needle.PaddingSize > 0 {
		needle.Padding = padding[needle.PaddingSize]
	}

	needle.newBuffer()

}

// ======== Renew() ========
func (needle *Needle) Renew(size uint32) {
	needle.Size = size
	needle.init()
}

// ======== DataBuffer() ========
func (needle *Needle) DataBuffer() []byte {
	return needle.buffer[NEEDLE_DATA_OFFSET : NEEDLE_DATA_OFFSET+needle.Size]
}

func (needle *Needle) Close() {
	needle.freeBuffer()
}

// NeedleOffset convert offset to needle offset.
func NeedleOffset(offset int64) uint32 {
	return uint32(offset / NEEDLE_PADDINGSIZE)
}

// BlockOffset get super block file offset.
func BlockOffset(offset uint32) int64 {
	return int64(offset) * NEEDLE_PADDINGSIZE
}

// -------- paddingSize() --------
func paddingSize(d uint32) uint32 {
	var paddingSize uint32 = 0
	if a := d % NEEDLE_PADDINGSIZE; a > 0 {
		paddingSize = NEEDLE_PADDINGSIZE - a
	}

	return paddingSize
}

// -------- align() --------
func align(d uint32) uint32 {
	//return (d + NEEDLE_PADDINGSIZE - 1) & ^(NEEDLE_PADDINGSIZE - 1)
	return d + paddingSize(d)
}

// Size get a needle size with meta data.
func Size(n uint32) uint32 {
	return align(NEEDLE_HEADER_SIZE + n + NEEDLE_FOOTER_SIZE)
}

// -------- adjustSize() --------
func (needle *Needle) adjustSize() {
	needle.WriteSize = uint32(NEEDLE_HEADER_SIZE + needle.Size + NEEDLE_FOOTER_SIZE)
	//needle.PaddingSize = align(needle.WriteSize) - needle.WriteSize
	needle.PaddingSize = paddingSize(needle.WriteSize)
	needle.WriteSize += needle.PaddingSize
	needle.FooterSize = NEEDLE_FOOTER_SIZE + needle.PaddingSize
	needle.AlignedSize = uint32(int64(needle.WriteSize) / NEEDLE_PADDINGSIZE)
}

// -------- newBuffer() --------
func (needle *Needle) newBuffer() {
	if needle.WriteSize <= uint32(cacheBufferSize) {
		needle.buffer = bufferPool.Get().([]byte)
	} else {
		needle.buffer = make([]byte, needle.WriteSize)
	}
}

// -------- freeBuffer() --------
func (needle *Needle) freeBuffer() {
	if needle.buffer != nil && len(needle.buffer) <= cacheBufferSize {
		bufferPool.Put(needle.buffer)
	}
}

// ======== FillBuffer() ========
func (this *Needle) FillBuffer() (err error) {
	this.fillHeaderBuffer(this.buffer[NEEDLE_MAGIC_OFFSET:NEEDLE_DATA_OFFSET])
	footerOffset := NEEDLE_DATA_OFFSET + this.Size
	this.fillDataBuffer(this.buffer[NEEDLE_DATA_OFFSET:footerOffset])
	this.fillFooterBuffer(this.buffer[footerOffset : footerOffset+this.FooterSize])
	return
}

// ======== Buffer() ========
func (needle *Needle) Buffer() []byte {

	return needle.buffer[:needle.WriteSize]
}

func (needle *Needle) fillDataBuffer(buf []byte) (err error) {
	if len(buf) != int(needle.Size) {
		err = fmt.Errorf("Wrong buffer size for needle data")
		return
	}

	copy(buf, needle.Data)
	return
}

// -------- fillHeaderBuffer() --------
func (needle *Needle) fillHeaderBuffer(buf []byte) (err error) {
	if len(buf) != int(NEEDLE_HEADER_SIZE) {
		err = fmt.Errorf("Wrong buffer size for needle header")
		return
	}
	// magic
	copy(buf[NEEDLE_MAGIC_OFFSET:NEEDLE_COOKIE_OfFSET], needle.HeaderMagic)
	// cookie
	utils.BigEndian.PutInt32(buf[NEEDLE_COOKIE_OfFSET:NEEDLE_KEY_OFFSET], needle.Cookie)
	// key
	utils.BigEndian.PutInt64(buf[NEEDLE_KEY_OFFSET:NEEDLE_FLAGS_OFFSET], needle.Key)
	// flags
	buf[NEEDLE_FLAGS_OFFSET] = needle.Flags
	// size
	utils.BigEndian.PutUint32(buf[NEEDLE_SIZE_OFFSET:NEEDLE_DATA_OFFSET], needle.Size)
	return
}

// -------- fillFooterBuffer() --------
func (needle *Needle) fillFooterBuffer(buf []byte) (err error) {
	if len(buf) != int(needle.FooterSize) {
		err = fmt.Errorf("Wrong buffer size for needle footer")
		return
	}
	// magic
	copy(buf[NEEDLE_MAGIC_OFFSET:NEEDLE_CHECKSUM_OFFSET], needle.FooterMagic)
	// checksum
	utils.BigEndian.PutUint32(buf[NEEDLE_CHECKSUM_OFFSET:NEEDLE_PADDING_OFFSET], needle.Checksum)
	// padding
	copy(buf[NEEDLE_PADDING_OFFSET:NEEDLE_PADDING_OFFSET+needle.PaddingSize], needle.Padding)
	return
}

// ======== BuildFrom() ========
func (this *Needle) BuildFrom(buf []byte) (err error) {

	if !bytes.Equal(buf[NEEDLE_MAGIC_OFFSET:NEEDLE_COOKIE_OfFSET], headerMagic) {
		err = fmt.Errorf("Needle header magic is wrong.")
		utils.LogErrorf(err, "BuildFrom()")
		return
	}

	this.Cookie = utils.BigEndian.Int32(buf[NEEDLE_COOKIE_OfFSET:NEEDLE_KEY_OFFSET])
	this.Key = utils.BigEndian.Int64(buf[NEEDLE_KEY_OFFSET:NEEDLE_FLAGS_OFFSET])
	this.Size = utils.BigEndian.Uint32(buf[NEEDLE_SIZE_OFFSET:NEEDLE_DATA_OFFSET])
	this.init()

	this.Flags = buf[NEEDLE_FLAGS_OFFSET]
	this.Checksum = utils.BigEndian.Uint32(buf[NEEDLE_CHECKSUM_OFFSET:NEEDLE_PADDING_OFFSET])
	this.Data = make([]byte, this.Size)
	copy(this.Data, buf[NEEDLE_DATA_OFFSET:NEEDLE_DATA_OFFSET+this.Size])
	//utils.LogDebugf("copy Data(offset=%d size=%d) into needle. %s", NEEDLE_DATA_OFFSET, this.Size, this.Data)
	//utils.LogDebugf("buf: %#v", buf[NEEDLE_DATA_OFFSET:NEEDLE_DATA_OFFSET+this.Size])
	//utils.LogDebugf("needle: %#v", *this)

	return
}

func (needle *Needle) String() string {
	var dn = 16
	if len(needle.Data) < dn {
		dn = len(needle.Data)
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
`, needle.WriteSize, NEEDLE_HEADER_SIZE, needle.HeaderMagic, needle.Cookie, needle.Key, needle.Flags, needle.Size,
		needle.Data[:dn], needle.FooterSize, needle.FooterMagic, needle.Checksum, needle.Padding)
}
