package haystack

import (
	"bytes"
	"fmt"
	"os"
)

const (
	SuperBlockSize          = NeedlePaddingSize
	SuperBlockMagicOffset   = 0
	SuperBlockMagicSize     = 4
	SuperBlockVersionOffset = SuperBlockMagicOffset + SuperBlockMagicSize
	SuperBlockVersionSize   = 1
	SuperBlockPaddingOffset = SuperBlockVersionOffset + SuperBlockVersionSize
	SuperBlockPaddingSize   = SuperBlockSize - SuperBlockPaddingOffset
)

var (
	version1     = byte(1)
	blockVersion = version1

	superblockMagic   = []byte{0x83, 0x84, 0x77, 0x55}
	superblockVersion = []byte{blockVersion}
	superblockPadding = bytes.Repeat([]byte{byte(0)}, SuperBlockPaddingSize)
)

// SuperBlock -
// **************** SuperBlock ****************
type SuperBlock struct {
	Magic   []byte
	Version byte
	Padding []byte
}

// NewSuperBlock -
// ======== NewSuperBlock() ========
func NewSuperBlock() *SuperBlock {
	superblock := &SuperBlock{
		Magic:   superblockMagic,
		Version: blockVersion,
		Padding: superblockPadding,
	}

	return superblock
}

// WriteToFile -
// ======== WriteToFile() ========
func (_this *SuperBlock) WriteToFile(writer *os.File) (writedSize uint64, err error) {
	writedSize = 0

	// Write Super Block Magic
	if _, err = writer.Write(_this.Magic); err != nil {
		return
	}
	writedSize += uint64(len(_this.Magic))

	// Write Super Block Version
	if _, err = writer.Write([]byte{_this.Version}); err != nil {
		return
	}
	writedSize += SuperBlockVersionSize

	// Write Super Block Padding
	if _, err = writer.Write(_this.Padding); err != nil {
		return
	}
	writedSize += uint64(len(_this.Padding))

	return
}

// ReadFromFile -
// ======== ReadFromFile() ========
func (_this *SuperBlock) ReadFromFile(reader *os.File) (err error) {
	var buf = make([]byte, SuperBlockSize)
	if _, err = reader.Read(buf[:SuperBlockSize]); err != nil {
		return
	}

	_this.Magic = buf[SuperBlockMagicOffset : SuperBlockMagicOffset+SuperBlockMagicSize]
	_this.Version = buf[SuperBlockVersionOffset : SuperBlockVersionOffset+SuperBlockVersionSize][0]
	_this.Padding = buf[SuperBlockPaddingOffset : SuperBlockPaddingOffset+SuperBlockPaddingSize]

	if !bytes.Equal(_this.Magic, superblockMagic) {
		return fmt.Errorf("SuperBlock magic number not match.")
	}
	if _this.Version != superblockVersion[0] {
		return fmt.Errorf("SuperBlock Version not match.")
	}

	return
}
