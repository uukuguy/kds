package haystack

import (
	"bufio"
	"fmt"
	"github.com/uukuguy/kds/utils"
	"os"
	"strconv"
)

const (
	// 一个卷（volume）数据文件大小最大32GB。
	DATAFILE_MAXSIZE        = 4 * 1024 * 1024 * 1024 * NEEDLE_PADDINGSIZE
	DATAFILE_MAXOFFSET      = 4*1024*1024*1024 - 1 // 4294967295
	DATAFILE_MAX_CACHEWRITE = 1
)

// **************** Data ****************
type Data struct {
	vid           int32
	reader        *os.File
	writer        *os.File
	superblock    *SuperBlock
	Dir           string
	FileSize      uint64
	syncedSize    uint64
	AlignedOffset uint64 // FileSize / NEEDLE_PADDINGSIZE
	closed        bool
	cache_writed  int32
}

// ======== String() ========
func (this *Data) String() string {
	return fmt.Sprintf(`
-----------------------------
Data

vid:                  %d
Dir:                  %s
FileSize:             %d
syncedSize:           %d
AlignedOffset:        %d
closed:               %v
cache_writed:         %d
-----------------------------
`,
		this.vid,
		this.Dir,
		this.FileSize,
		this.syncedSize,
		this.AlignedOffset,
		this.closed,
		this.cache_writed,
	)
}

// ======== NewData() ========
func NewData(vid int32, store_dir string) (data *Data) {
	data = &Data{
		vid:        vid,
		Dir:        store_dir,
		superblock: NewSuperBlock(),
		closed:     false,
		FileSize:   0,
		syncedSize: 0,
	}

	return
}

// ======== Init() ========
func (this *Data) Init() (err error) {
	dataFileName := this.getDataFileName()
	if this.writer, err = os.OpenFile(dataFileName, os.O_WRONLY|os.O_CREATE|O_NOATIME, 0664); err != nil {
		utils.LogErrorf(err, "os.OpenFile(\"%s\")", dataFileName)
		this.Close()
		return
	}
	if this.reader, err = os.OpenFile(dataFileName, os.O_RDONLY|O_NOATIME, 0664); err != nil {
		utils.LogErrorf(err, "os.OpenFile(\"%s\")", dataFileName)
		this.Close()
		return
	}

	var filesize uint64
	if filesize, err = utils.GetFileSize(this.reader); err != nil {
		return
	}

	if filesize == 0 {
		// 为文件预分配物理空间，仅Linux适用。
		if err = Fallocate(this.writer.Fd(), FALLOC_FL_KEEP_SIZE, 0, DATAFILE_MAXSIZE); err != nil {
			utils.LogErrorf(err, "Data.Init() Fallocate() failed. vid:%d Dir:%s", this.vid, this.Dir)
			return
		}
		if err = this.writeSuperBlock(); err != nil {
			return
		}
		this.FileSize = SUPERBLOCK_SIZE
		this.syncedSize = this.FileSize
	} else {
		if err = this.loadSuperBlock(); err != nil {
			return
		}
		this.FileSize = filesize
		this.syncedSize = this.FileSize
		this.AlignedOffset = this.FileSize / NEEDLE_PADDINGSIZE
		this.writer.Seek(int64(this.FileSize), os.SEEK_SET)
	}

	return
}

// -------- writeSuperBlock() --------
func (this *Data) writeSuperBlock() (err error) {
	this.writer.Seek(0, os.SEEK_SET)

	var writedSize uint64
	if writedSize, err = this.superblock.WriteToFile(this.writer); err == nil {
		this.FileSize = writedSize
		this.AlignedOffset = SUPERBLOCK_SIZE / NEEDLE_PADDINGSIZE
	}
	return
}

// -------- loadSuperBlock() --------
func (this *Data) loadSuperBlock() (err error) {
	this.reader.Seek(0, os.SEEK_SET)
	err = this.superblock.ReadFromFile(this.reader)
	return
}

// -------- getFileSize() --------
func (this *Data) getFileSize() (filesize int64, err error) {
	var stat os.FileInfo
	if stat, err = this.reader.Stat(); err != nil {
		utils.LogErrorf(err, "")
		return 0, err
	}
	filesize = stat.Size()
	return filesize, nil
}

// -------- getDataFileName() --------
func (this *Data) getDataFileName() string {
	return this.Dir + "/" + strconv.Itoa(int(this.vid)) + ".dat"
}

// -------- flushFile() --------
// Change
//    this.cache_writed
//    this.syncedSize
//
func (this *Data) flushFile(force bool) (err error) {
	var (
		fd     uintptr
		offset uint64
		size   uint64
	)
	if this.cache_writed++; !force && this.cache_writed < DATAFILE_MAX_CACHEWRITE {
		return
	}
	this.cache_writed = 0
	offset = this.syncedSize
	size = this.FileSize - this.syncedSize
	if size == 0 {
		return
	}

	fd = this.writer.Fd()
	if err = Syncfilerange(fd, offset, size, SYNC_FILE_RANGE_WRITE); err != nil {
		utils.LogErrorf(err, "Syncfilerange() failed. %s.%d.dat %#v", this.Dir, this.vid)
		return
	}
	if err = Fdatasync(fd); err != nil {
		utils.LogErrorf(err, "Fdatasync() failed. %s.%d.dat", this.Dir, this.vid)
		return
	}
	if err = Fadvise(fd, offset, size, POSIX_FADV_DONTNEED); err == nil {
		this.syncedSize = this.FileSize
	} else {
		utils.LogErrorf(err, "Fadvise() failed. %s.%d.dat", this.Dir, this.vid)
	}
	return
}

// ======== Close() ========
func (this *Data) Close() {
	if this.writer != nil {
		if err := this.flushFile(true); err != nil {
		}
		if err := this.writer.Sync(); err != nil {
		}
		if err := this.writer.Close(); err != nil {
		}
	}
	if this.reader != nil {
		if err := this.reader.Close(); err != nil {
		}
	}
	this.closed = true
}

// ======== AppendNeedle() ========
// Keep write needles to the end of data file.
func (this *Data) AppendNeedle(needle *Needle) (region NeedleRegion, err error) {

	if DATAFILE_MAXSIZE-uint64(needle.WriteSize) < this.FileSize {
		err = fmt.Errorf("No more free space in data file.")
		return
	}
	needle.FillBuffer()
	if _, err = this.writer.Write(needle.Buffer()); err == nil {
		region.AlignedOffset = this.AlignedOffset
		region.Size = needle.WriteSize
		this.AlignedOffset += uint64(needle.AlignedSize)
		this.FileSize += uint64(needle.WriteSize)
		if err = this.flushFile(false); err != nil {
			this.FileSize -= uint64(needle.WriteSize)
			return
		}
	} else {
		return
	}

	return
}

// ======== UpdateNeedle() ========
func (this *Data) UpdateNeedle(needle *Needle) (region NeedleRegion, err error) {
	return NeedleRegion{}, nil
}

// ======== DeleteNeedle() ========
func (this *Data) DeleteNeedle(needle *Needle) (err error) {
	return nil
}

// ======== FindNeedle() ========
func (this *Data) FindNeedle(key int64) (needle *Needle, err error) {
	var cookie int32
	var size uint32
	needle = NewNeedle(key, cookie, size)
	return needle, nil
}

func (this *Data) GetNeedle(key int64, region NeedleRegion) (needle *Needle, err error) {
	offset := region.GetOffset()
	this.reader.Seek(int64(offset), os.SEEK_SET)
	utils.LogDebugf("reader.Seek() offset=%d region:%+v", offset, region)
	reader := bufio.NewReaderSize(this.reader, int(region.Size))
	var buf []byte
	if buf, err = reader.Peek(int(region.Size)); err != nil {
		return
	}
	//reader.Discard(int(region.Size))

	needle = new(Needle)
	if err = needle.BuildFrom(buf); err != nil {
		return
	}
	var n uint32 = 16
	if needle.Size < n {
		n = needle.Size
	}
	utils.LogDebugf("needle.Data(len=%d): %#v...", needle.Size, needle.Data[:n])

	return
}
