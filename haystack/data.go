package haystack

import (
	"bufio"
	"fmt"
	"os"
	"strconv"

	"github.com/uukuguy/kds/utils"
	log "github.com/uukuguy/kds/utils/logger"
)

const (
	// 一个卷（volume）数据文件大小最大32GB。
	DatafileMaxSize       = 4 * 1024 * 1024 * 1024 * NeedlePaddingSize
	DatafileMaxOffset     = 4*1024*1024*1024 - 1 // 4294967295
	DatafileMaxCacheWrite = 1
)

// Data -
// **************** Data ****************
type Data struct {
	vid           int32
	reader        *os.File
	writer        *os.File
	superblock    *SuperBlock
	Dir           string
	FileSize      uint64
	syncedSize    uint64
	AlignedOffset uint64 // FileSize / NeedlePaddingSize
	closed        bool
	cacheWrited   int32
}

// String -
// ======== String() ========
func (_this *Data) String() string {
	return fmt.Sprintf(`
-----------------------------
Data

vid:                  %d
Dir:                  %s
FileSize:             %d
syncedSize:           %d
AlignedOffset:        %d
closed:               %v
cacheWrited:         %d
-----------------------------
`,
		_this.vid,
		_this.Dir,
		_this.FileSize,
		_this.syncedSize,
		_this.AlignedOffset,
		_this.closed,
		_this.cacheWrited,
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

// Init -
// ======== Init() ========
func (_this *Data) Init() (err error) {
	dataFileName := _this.getDataFileName()
	if _this.writer, err = os.OpenFile(dataFileName, os.O_WRONLY|os.O_CREATE|O_NOATIME, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") %v", dataFileName, err)
		_this.Close()
		return
	}
	if _this.reader, err = os.OpenFile(dataFileName, os.O_RDONLY|O_NOATIME, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") %v", dataFileName, err)
		_this.Close()
		return
	}

	var filesize uint64
	if filesize, err = utils.GetFileSize(_this.reader); err != nil {
		return
	}

	if filesize == 0 {
		// 为文件预分配物理空间，仅Linux适用。
		if err = Fallocate(_this.writer.Fd(), FALLOC_FL_KEEP_SIZE, 0, DatafileMaxSize); err != nil {
			log.Errorf("Data.Init() Fallocate() failed. vid:%d Dir:%s. %v", _this.vid, _this.Dir, err)
			return
		}
		if err = _this.writeSuperBlock(); err != nil {
			return
		}
		_this.FileSize = SuperBlockSize
		_this.syncedSize = _this.FileSize
	} else {
		if err = _this.loadSuperBlock(); err != nil {
			return
		}
		_this.FileSize = filesize
		_this.syncedSize = _this.FileSize
		_this.AlignedOffset = _this.FileSize / NeedlePaddingSize
		_this.writer.Seek(int64(_this.FileSize), os.SEEK_SET)
	}

	return
}

// -------- writeSuperBlock() --------
func (_this *Data) writeSuperBlock() (err error) {
	_this.writer.Seek(0, os.SEEK_SET)

	var writedSize uint64
	if writedSize, err = _this.superblock.WriteToFile(_this.writer); err == nil {
		_this.FileSize = writedSize
		_this.AlignedOffset = SuperBlockSize / NeedlePaddingSize
	}
	return
}

// -------- loadSuperBlock() --------
func (_this *Data) loadSuperBlock() (err error) {
	_this.reader.Seek(0, os.SEEK_SET)
	err = _this.superblock.ReadFromFile(_this.reader)
	return
}

// -------- getFileSize() --------
func (_this *Data) getFileSize() (filesize int64, err error) {
	var stat os.FileInfo
	if stat, err = _this.reader.Stat(); err != nil {
		log.Errorf("Data.reader.Stat() failed. %v", err)
		return 0, err
	}
	filesize = stat.Size()
	return filesize, nil
}

// -------- getDataFileName() --------
func (_this *Data) getDataFileName() string {
	return _this.Dir + "/" + strconv.Itoa(int(_this.vid)) + ".dat"
}

// -------- flushFile() --------
// Change
//    _this.cacheWrited
//    _this.syncedSize
//
func (_this *Data) flushFile(force bool) (err error) {
	var (
		fd     uintptr
		offset uint64
		size   uint64
	)
	if _this.cacheWrited++; !force && _this.cacheWrited < DatafileMaxCacheWrite {
		return
	}
	_this.cacheWrited = 0
	offset = _this.syncedSize
	size = _this.FileSize - _this.syncedSize
	if size == 0 {
		return
	}

	fd = _this.writer.Fd()
	if err = Syncfilerange(fd, offset, size, SYNC_FILE_RANGE_WRITE); err != nil {
		log.Errorf("Syncfilerange() failed. %s.%d.dat %v", _this.Dir, _this.vid, err)
		return
	}
	if err = Fdatasync(fd); err != nil {
		log.Errorf("Fdatasync() failed. %s.%d.dat. %v", _this.Dir, _this.vid, err)
		return
	}
	if err = Fadvise(fd, offset, size, POSIX_FADV_DONTNEED); err == nil {
		_this.syncedSize = _this.FileSize
	} else {
		log.Errorf("Fadvise() failed. %s.%d.dat. %v", _this.Dir, _this.vid, err)
	}
	return
}

// Close -
// ======== Close() ========
func (_this *Data) Close() {
	if _this.writer != nil {
		if err := _this.flushFile(true); err != nil {
		}
		if err := _this.writer.Sync(); err != nil {
		}
		if err := _this.writer.Close(); err != nil {
		}
	}
	if _this.reader != nil {
		if err := _this.reader.Close(); err != nil {
		}
	}
	_this.closed = true
}

// AppendNeedle -
// ======== AppendNeedle() ========
// Keep write needles to the end of data file.
func (_this *Data) AppendNeedle(needle *Needle) (region NeedleRegion, err error) {

	if DatafileMaxSize-uint64(needle.WriteSize) < _this.FileSize {
		err = fmt.Errorf("No more free space in data file.")
		return
	}
	needle.FillBuffer()
	if _, err = _this.writer.Write(needle.Buffer()); err == nil {
		region.AlignedOffset = _this.AlignedOffset
		region.Size = needle.WriteSize
		_this.AlignedOffset += uint64(needle.AlignedSize)
		_this.FileSize += uint64(needle.WriteSize)
		if err = _this.flushFile(false); err != nil {
			_this.FileSize -= uint64(needle.WriteSize)
			return
		}
	} else {
		return
	}

	return
}

// UpdateNeedle -
// ======== UpdateNeedle() ========
func (_this *Data) UpdateNeedle(needle *Needle) (region NeedleRegion, err error) {
	return NeedleRegion{}, nil
}

// DeleteNeedle -
// ======== DeleteNeedle() ========
func (_this *Data) DeleteNeedle(needle *Needle) (err error) {
	return nil
}

// FindNeedle -
// ======== FindNeedle() ========
func (_this *Data) FindNeedle(key int64) (needle *Needle, err error) {
	var cookie int32
	var size uint32
	needle = NewNeedle(key, cookie, size)
	return needle, nil
}

// GetNeedle -
func (_this *Data) GetNeedle(key int64, region NeedleRegion) (needle *Needle, err error) {
	offset := region.GetOffset()
	_this.reader.Seek(int64(offset), os.SEEK_SET)
	log.Debugf("reader.Seek() offset=%d region:%+v", offset, region)
	reader := bufio.NewReaderSize(_this.reader, int(region.Size))
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
	log.Debugf("needle.Data(len=%d): %#v...", needle.Size, needle.Data[:n])

	return
}
