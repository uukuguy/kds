package haystack

import (
	"bufio"
	"fmt"
	"github.com/uukuguy/kds/utils"
	"io"
	"os"
	"strconv"
	"sync/atomic"
	"unsafe"
)

type needle_indices_t map[int64]int64 // needle id -> offset << 32 | size

const (
	// 一个卷（volume）最多3200万(33,554,432)个needle, 536,870,912 bytes。
	INDEXFILE_MAXSIZE        = int64((unsafe.Sizeof(int64(0)) + unsafe.Sizeof(int64(0))) * 32 * 1024 * 1024)
	INDEX_ENTRY_SIZE         = 16
	INDEXFILE_MAX_CACHEWRITE = 1
)

// **************** NeedleRegion ****************
type NeedleRegion struct {
	AlignedOffset uint32
	Size          uint32
}

func (this *NeedleRegion) to_int64() int64 {
	return int64(this.AlignedOffset)<<32 + int64(this.Size)
}

func (this *NeedleRegion) from_int64(value int64) {
	this.AlignedOffset = uint32(value >> 32)
	this.Size = uint32(value)
}

type IndexEntry struct {
	Key    int64
	Region NeedleRegion
}

// **************** Index ****************
type Index struct {
	vid          int32
	Dir          string
	idxFile      *os.File
	indices      needle_indices_t
	superblock   *SuperBlock
	closed       bool
	FileSize     int64
	syncedSize   int64
	cache_writed int32
	//outdated_regions []NeedleRegion
	outdated_keys uint32
	outdated_size uint64
	total_size    uint64
}

// ======== String() ========
func (this *Index) String() string {
	return fmt.Sprintf(`
-----------------------------
Index

vid:                  %d
Dir:                  %s
closed:               %v
FileSize:             %d
syncedSize:           %d
cache_writed          %d
outdated_keys         %d
total keys            %d
outdated_keys%%        %.3f%%
outdated_size         %d
total size            %d
outdated_size%%        %.3f%%
indices:              %#v
-----------------------------
`,
		this.vid,
		this.Dir,
		this.closed,
		this.FileSize,
		this.syncedSize,
		this.cache_writed,
		this.outdated_keys,
		len(this.indices),
		this.getOutdatedKeysRate()*100,
		this.outdated_size,
		this.total_size,
		this.getOutdatedSizeRate()*100,
		this.indices,
	)
}

// ======== NewIndex() ========
func NewIndex(vid int32, store_dir string) (index *Index) {
	index = &Index{
		vid:          vid,
		Dir:          store_dir,
		superblock:   NewSuperBlock(),
		idxFile:      nil,
		indices:      make(needle_indices_t),
		closed:       false,
		FileSize:     0,
		cache_writed: 0,
		//outdated_regions: []NeedleRegion{},
		outdated_keys: 0,
	}

	return
}

// ======== Init() ========
func (this *Index) Init() (err error) {
	idxFileName := this.getIndexFileName()
	if this.idxFile, err = os.OpenFile(idxFileName, os.O_RDWR|os.O_CREATE|O_NOATIME, 0664); err != nil {
		utils.LogErrorf(err, "Index.Init() open file %s failed.", idxFileName)
		this.Close()
		return
	}

	var filesize int64
	if filesize, err = utils.GetFileSize(this.idxFile); err != nil {
		return
	}

	if filesize == 0 {
		// 为文件预分配物理空间，仅Linux适用。
		if err = Fallocate(this.idxFile.Fd(), FALLOC_FL_KEEP_SIZE, 0, INDEXFILE_MAXSIZE); err != nil {
			utils.LogErrorf(err, "Index.Init() Fallocate() failed. vid:%d Dir:%s", this.vid, this.Dir)
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
	}

	if err = this.loadIndices(); err != nil {
		utils.LogErrorf(err, "Index.Init() call this.loadIndices() failed. vid:%d", this.vid)
		return
	}

	this.idxFile.Seek(this.FileSize, os.SEEK_SET)

	return
}

// ======== Close() ========
func (this *Index) Close() {
	this.closed = true
	if this.idxFile != nil {
		this.idxFile.Close()
		this.idxFile = nil
	}
}

// -------- getOutdatedSizeRate() --------
func (this *Index) getOutdatedSizeRate() float32 {
	return float32(float64(this.outdated_size) / float64(this.total_size))
}

// -------- getOutdatedKeysRate() --------
func (this *Index) getOutdatedKeysRate() float32 {
	return float32(this.outdated_keys) / float32(this.outdated_keys+uint32(len(this.indices)))
}

// -------- getIndexFileName() --------
func (this *Index) getIndexFileName() string {
	return this.Dir + "/" + strconv.Itoa(int(this.vid)) + ".idx"
}

// -------- loadIndices() --------
func (this *Index) loadIndices() (err error) {
	this.indices = make(needle_indices_t)

	this.idxFile.Seek(0, os.SEEK_SET)
	reader := bufio.NewReaderSize(this.idxFile, 1024*1024)
	var superblock []byte
	if superblock, err = reader.Peek(SUPERBLOCK_SIZE); err != nil {
		return
	}
	// Check superblock head.
	if len(superblock) != SUPERBLOCK_SIZE {
		utils.LogErrorf(err, "Readed superblock len = %d, not equal to %d", len(superblock), SUPERBLOCK_SIZE)
		return
	}
	reader.Discard(SUPERBLOCK_SIZE)

	for {
		var buf []byte
		if buf, err = reader.Peek(INDEX_ENTRY_SIZE); err != nil {
			break
		}
		if len(buf) != INDEX_ENTRY_SIZE {
			break
		}
		//utils.LogDebugf("len(buf)=%d %#v", len(buf), buf[0:8])
		key := utils.BigEndian.Int64(buf[0:8])
		offset := utils.BigEndian.Uint32(buf[8:12])
		size := utils.BigEndian.Uint32(buf[12:16])
		//utils.Debugf("key: %d offset: %d size %d", key, offset, size)

		region := NeedleRegion{offset, size}
		this.SetNeedleRegion(key, region)
		this.total_size += uint64(region.Size)

		if _, err = reader.Discard(INDEX_ENTRY_SIZE); err != nil {
			break
		}
	}

	if err == io.EOF {
		err = nil
	}

	utils.LogInfof("Index.loadIndices() done. %d index entries loaded.", len(this.indices))

	return
}

// -------- hasNeedle() --------
func (this *Index) hasNeedle(key int64) bool {
	_, ok := this.indices[key]
	return ok
}

// ======== SetNeedleRegion() ========
func (this *Index) SetNeedleRegion(key int64, region NeedleRegion) error {
	old_value, key_exist := this.indices[key]
	this.indices[key] = region.to_int64()

	if key_exist {
		old_region := NeedleRegion{}
		old_region.from_int64(old_value)
		//this.outdated_regions = append(this.outdated_regions, old_region)
		atomic.AddUint32(&this.outdated_keys, 1)
		atomic.AddUint64(&this.outdated_size, uint64(old_region.Size))
	}

	return nil
}

// ======== GetNeedleRegion() ========
func (this *Index) GetNeedleRegion(key int64) (NeedleRegion, bool) {
	if value, ok := this.indices[key]; ok {
		region := NeedleRegion{}
		region.from_int64(value)
		return region, true
	} else {
		return NeedleRegion{}, false
	}
}

// -------- flushFile() --------
// Change
//    this.cache_writed
//    this.syncedSize
//
func (this *Index) flushFile(force bool) (err error) {
	var (
		fd     uintptr
		offset int64
		size   int64
	)
	if this.cache_writed++; !force && this.cache_writed < INDEXFILE_MAX_CACHEWRITE {
		return
	}
	this.cache_writed = 0
	offset = this.syncedSize
	size = this.FileSize - this.syncedSize
	if size == 0 {
		return
	}

	fd = this.idxFile.Fd()
	if err = Syncfilerange(fd, offset, size, SYNC_FILE_RANGE_WRITE); err != nil {
		utils.LogErrorf(err, "Syncfilerange() failed. %s.%d.dat", this.Dir, this.vid)
		return
	}
	if err = Fdatasync(fd); err != nil {
		utils.LogErrorf(err, "Fdatasync() failed. %s", this.getIndexFileName())
		return
	}
	if err = Fadvise(fd, offset, size, POSIX_FADV_DONTNEED); err == nil {
		this.syncedSize = this.FileSize
	} else {
		utils.LogErrorf(err, "Fadvise() failed. %s", this.getIndexFileName())
	}
	return
}

// -------- writeSuperBlock() --------
func (this *Index) writeSuperBlock() (err error) {
	this.idxFile.Seek(0, os.SEEK_SET)

	var writedSize int64
	if writedSize, err = this.superblock.WriteToFile(this.idxFile); err == nil {
		this.FileSize = writedSize
	}
	return
}

// -------- AppendIndexEntry() --------
//func (this *Index) AppendIndexEntry(key int64, region NeedleRegion) (err error) {
func (this *Index) AppendIndexEntry(entry IndexEntry) (err error) {
	key := entry.Key
	region := entry.Region

	if INDEXFILE_MAXSIZE-int64(region.Size) < this.FileSize {
		err = fmt.Errorf("No more free space in data file.")
		return
	}

	buf := make([]byte, INDEX_ENTRY_SIZE)
	pos := 0
	utils.BigEndian.PutInt64(buf[pos:], key)
	pos += int(unsafe.Sizeof(key))
	utils.BigEndian.PutUint32(buf[pos:], region.AlignedOffset)
	pos += int(unsafe.Sizeof(region.AlignedOffset))
	utils.BigEndian.PutUint32(buf[pos:], region.Size)
	//pos += int(unsafe.Sizeof(region.Size))

	if _, err = this.idxFile.Write(buf); err == nil {
		this.FileSize += INDEX_ENTRY_SIZE
		if err = this.flushFile(false); err != nil {
			this.FileSize -= INDEX_ENTRY_SIZE
			return
		}
	} else {
		return
	}

	this.total_size += uint64(region.Size)
	this.SetNeedleRegion(key, region)

	return
}

// -------- loadSuperBlock() --------
func (this *Index) loadSuperBlock() (err error) {
	this.idxFile.Seek(0, os.SEEK_SET)
	err = this.superblock.ReadFromFile(this.idxFile)
	return
}
