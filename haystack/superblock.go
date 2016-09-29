package haystack

import (
	"bytes"
	"fmt"
	"os"
)

const (
	SUPERBLOCK_SIZE           = NEEDLE_PADDINGSIZE
	SUPERBLOCK_MAGIC_OFFSET   = 0
	SUPERBLOCK_MAGIC_SIZE     = 4
	SUPERBLOCK_VERSION_OFFSET = SUPERBLOCK_MAGIC_OFFSET + SUPERBLOCK_MAGIC_SIZE
	SUPERBLOCK_VERSION_SIZE   = 1
	SUPERBLOCK_PADDING_OFFSET = SUPERBLOCK_VERSION_OFFSET + SUPERBLOCK_VERSION_SIZE
	SUPERBLOCK_PADDING_SIZE   = SUPERBLOCK_SIZE - SUPERBLOCK_PADDING_OFFSET
)

var (
	version1     = byte(1)
	blockVersion = version1

	superblockMagic   = []byte{0x83, 0x84, 0x77, 0x55}
	superblockVersion = []byte{blockVersion}
	superblockPadding = bytes.Repeat([]byte{byte(0)}, SUPERBLOCK_PADDING_SIZE)
)

// **************** SuperBlock ****************
type SuperBlock struct {
	Magic   []byte
	Version byte
	Padding []byte
}

// ======== NewSuperBlock() ========
func NewSuperBlock() *SuperBlock {
	superblock := &SuperBlock{
		Magic:   superblockMagic,
		Version: blockVersion,
		Padding: superblockPadding,
	}

	return superblock
}

// ======== WriteToFile() ========
func (this *SuperBlock) WriteToFile(writer *os.File) (writedSize int64, err error) {
	writedSize = 0

	// Write Super Block Magic
	if _, err = writer.Write(this.Magic); err != nil {
		return
	}
	writedSize += int64(len(this.Magic))

	// Write Super Block Version
	if _, err = writer.Write([]byte{this.Version}); err != nil {
		return
	}
	writedSize += SUPERBLOCK_VERSION_SIZE

	// Write Super Block Padding
	if _, err = writer.Write(this.Padding); err != nil {
		return
	}
	writedSize += int64(len(this.Padding))

	return
}

// ======== ReadFromFile() ========
func (this *SuperBlock) ReadFromFile(reader *os.File) (err error) {
	var buf = make([]byte, SUPERBLOCK_SIZE)
	if _, err = reader.Read(buf[:SUPERBLOCK_SIZE]); err != nil {
		return
	}

	this.Magic = buf[SUPERBLOCK_MAGIC_OFFSET : SUPERBLOCK_MAGIC_OFFSET+SUPERBLOCK_MAGIC_SIZE]
	this.Version = buf[SUPERBLOCK_VERSION_OFFSET : SUPERBLOCK_VERSION_OFFSET+SUPERBLOCK_VERSION_SIZE][0]
	this.Padding = buf[SUPERBLOCK_PADDING_OFFSET : SUPERBLOCK_PADDING_OFFSET+SUPERBLOCK_PADDING_SIZE]

	if !bytes.Equal(this.Magic, superblockMagic) {
		return fmt.Errorf("SuperBlock magic number not match.")
	}
	if this.Version != superblockVersion[0] {
		return fmt.Errorf("SuperBlock Version not match.")
	}

	return
}
