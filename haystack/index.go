package haystack

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync/atomic"
	"unsafe"

	"github.com/uukuguy/kds/store/errors"
	"github.com/uukuguy/kds/utils"
	log "github.com/uukuguy/kds/utils/logger"
)

type needleIndicesType map[int64]uint64 // needle id -> offset << 32 | size

const (
	// 一个卷（volume）最多3200万(33,554,432)个needle, 536,870,912 bytes。If 100KB per file, max data file size is 3.4TB.

	// IndexfileMaxSize -
	IndexfileMaxSize = int64((unsafe.Sizeof(int64(0)) + unsafe.Sizeof(int64(0))) * 32 * 1024 * 1024)
	// IndexEntrySize -
	IndexEntrySize = 16
	// IndexfileMaxCacheWrite -
	IndexfileMaxCacheWrite = 1
)

// NeedleRegion -
// **************** NeedleRegion ****************
type NeedleRegion struct {
	// use late 40 bits.
	// Single data file max size is
	// 1TB (2^40=1,099,511,627,776) * NEEDLE_PADDINGSIZE (8 bytes aligned.)
	AlignedOffset uint64
	// use late 24 bits. single file size has to less 16MB (2^24).
	Size uint32
}

func (_this *NeedleRegion) toUint64() (v64 uint64) {
	v64 = _this.AlignedOffset<<24 + uint64(_this.Size)
	log.Debugf("AlignedOffset:%x Size:%x -> v64:%x", _this.AlignedOffset, _this.Size, v64)
	return
}

func (_this *NeedleRegion) fromUint64(v64 uint64) {
	_this.AlignedOffset = v64 >> 24
	_this.Size = uint32(v64 & 0x00FFFFFF)
	log.Debugf("v64:%x -> AlignedOffset:%x Size:%x", v64, _this.AlignedOffset, _this.Size)
}

// GetOffset ()
//  ======== NeedleRegion::GetOffset() ========
func (_this *NeedleRegion) GetOffset() uint64 {
	return _this.AlignedOffset * NeedlePaddingSize
}

// GetSize ()
// ======== NeedleRegion::GetSize() ========
func (_this *NeedleRegion) GetSize() uint32 {
	return _this.Size
}

// IndexEntry -
type IndexEntry struct {
	Key    int64
	Region NeedleRegion
}

// Index -
// **************** Index ****************
type Index struct {
	vid         int32
	Dir         string
	idxFile     *os.File
	indices     needleIndicesType
	superblock  *SuperBlock
	closed      bool
	FileSize    uint64
	syncedSize  uint64
	cacheWrited uint32
	//outdated_regions []NeedleRegion
	outdatedKeys uint32
	outdatedSize uint64
	totalSize    uint64
}

// ======== String() ========
func (_this *Index) String() string {
	return fmt.Sprintf(`
-----------------------------
Index

vid:                  %d
Dir:                  %s
closed:               %v
FileSize:             %d
syncedSize:           %d
cacheWrited          %d
outdatedKeys         %d
total keys            %d
outdatedKeys%%        %.3f%%
outdatedSize         %d
total size            %d
outdatedSize%%        %.3f%%
indices:              %#v
-----------------------------
`,
		_this.vid,
		_this.Dir,
		_this.closed,
		_this.FileSize,
		_this.syncedSize,
		_this.cacheWrited,
		_this.outdatedKeys,
		len(_this.indices),
		_this.getOutdatedKeysRate()*100,
		_this.outdatedSize,
		_this.totalSize,
		_this.getOutdatedSizeRate()*100,
		_this.indices,
	)
}

// NewIndex ()
// ======== NewIndex() ========
func NewIndex(vid int32, storeDir string) (index *Index) {
	index = &Index{
		vid:         vid,
		Dir:         storeDir,
		superblock:  NewSuperBlock(),
		idxFile:     nil,
		indices:     make(needleIndicesType),
		closed:      false,
		FileSize:    0,
		cacheWrited: 0,
		//outdated_regions: []NeedleRegion{},
		outdatedKeys: 0,
	}

	return
}

// Init ()
// ======== Index::Init() ========
func (_this *Index) Init() (err error) {
	idxFileName := _this.getIndexFileName()
	if _this.idxFile, err = os.OpenFile(idxFileName, os.O_RDWR|os.O_CREATE|O_NOATIME, 0664); err != nil {
		log.Errorf("Index.Init() open file %s failed. %v", idxFileName, err)
		_this.Close()
		return
	}

	var filesize uint64
	if filesize, err = utils.GetFileSize(_this.idxFile); err != nil {
		return
	}

	if filesize == 0 {
		// 为文件预分配物理空间，仅Linux适用。
		if err = Fallocate(_this.idxFile.Fd(), uint32(FALLOC_FL_KEEP_SIZE), 0, uint64(IndexfileMaxSize)); err != nil {
			log.Errorf("Index.Init() Fallocate() failed. vid:%d Dir:%s. %v", _this.vid, _this.Dir, err)
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
	}

	if err = _this.loadIndices(); err != nil {
		log.Errorf("Index.Init() call _this.loadIndices() failed. vid:%d. %v", _this.vid, err)
		return
	}

	_this.idxFile.Seek(int64(_this.FileSize), os.SEEK_SET)

	return
}

// Close ()
// ======== Index::Close() ========
func (_this *Index) Close() {
	_this.closed = true
	if _this.idxFile != nil {
		_this.idxFile.Close()
		_this.idxFile = nil
	}
}

// -------- Index::getOutdatedSizeRate() --------
func (_this *Index) getOutdatedSizeRate() float32 {
	return float32(float64(_this.outdatedSize) / float64(_this.totalSize))
}

// -------- Index::getOutdatedKeysRate() --------
func (_this *Index) getOutdatedKeysRate() float32 {
	return float32(_this.outdatedKeys) / float32(_this.outdatedKeys+uint32(len(_this.indices)))
}

// -------- Index::getIndexFileName() --------
func (_this *Index) getIndexFileName() string {
	return _this.Dir + "/" + strconv.Itoa(int(_this.vid)) + ".idx"
}

// -------- Index::loadIndices() --------
func (_this *Index) loadIndices() (err error) {
	_this.indices = make(needleIndicesType)

	_this.idxFile.Seek(0, os.SEEK_SET)
	reader := bufio.NewReaderSize(_this.idxFile, 1024*1024)
	var superblock []byte
	if superblock, err = reader.Peek(SuperBlockSize); err != nil {
		return
	}
	// Check superblock head.
	if len(superblock) != SuperBlockSize {
		log.Errorf("Readed superblock len = %d, not equal to %d. %v", len(superblock), SuperBlockSize, err)
		return
	}
	reader.Discard(SuperBlockSize)

	for {
		var buf []byte
		if buf, err = reader.Peek(IndexEntrySize); err != nil {
			break
		}
		if len(buf) != IndexEntrySize {
			break
		}
		//log.Debugf("len(buf)=%d %#v", len(buf), buf[0:8])
		key := utils.BigEndian.Int64(buf[0:8])

		h32 := utils.BigEndian.Uint32(buf[8:12])
		l32 := utils.BigEndian.Uint32(buf[12:16])
		v64 := uint64(h32)<<32 + uint64(l32)
		//log.Debugf("h32:%d l32: %d v64:%d", h32, l32, v64)

		region := NeedleRegion{}
		region.fromUint64(v64)

		log.Debugf("key: %d region.AlignedOffset: %d region.Size %d. %v", key, region.AlignedOffset, region.Size, err)

		_this.SetNeedleRegion(key, region)
		_this.totalSize += uint64(region.Size)

		if _, err = reader.Discard(IndexEntrySize); err != nil {
			break
		}
	}

	if err == io.EOF {
		err = nil
	}

	log.Infof("Index.loadIndices() done. %d index entries loaded. %v", len(_this.indices), err)

	return
}

// -------- Index::hasNeedle() --------
func (_this *Index) hasNeedle(key int64) bool {
	_, ok := _this.indices[key]
	return ok
}

// SetNeedleRegion ()
// ======== Index::SetNeedleRegion() ========
func (_this *Index) SetNeedleRegion(key int64, region NeedleRegion) error {
	oldValue, keyExist := _this.indices[key]
	_this.indices[key] = region.toUint64()

	if keyExist {
		oldRegion := NeedleRegion{}
		oldRegion.fromUint64(oldValue)
		//_this.outdated_regions = append(_this.outdated_regions, oldRegion)
		atomic.AddUint32(&_this.outdatedKeys, 1)
		atomic.AddUint64(&_this.outdatedSize, uint64(oldRegion.Size))
	}

	return nil
}

// GetNeedleRegion ()
// ======== Index::GetNeedleRegion() ========
func (_this *Index) GetNeedleRegion(key int64) (NeedleRegion, bool) {
	region := NeedleRegion{}
	value, ok := _this.indices[key]
	if ok {
		region.fromUint64(value)
	}
	return region, ok
}

// -------- Index::flushFile() --------
// Change
//    _this.cacheWrited
//    _this.syncedSize
//
func (_this *Index) flushFile(force bool) (err error) {
	var (
		fd     uintptr
		offset uint64
		size   uint64
	)
	if _this.cacheWrited++; !force && _this.cacheWrited < IndexfileMaxCacheWrite {
		return
	}
	_this.cacheWrited = 0
	offset = _this.syncedSize
	size = _this.FileSize - _this.syncedSize
	if size == 0 {
		return
	}

	fd = _this.idxFile.Fd()
	if err = Syncfilerange(fd, offset, size, SYNC_FILE_RANGE_WRITE); err != nil {
		log.Errorf("Syncfilerange() failed. %s.%d.dat. %v", _this.Dir, _this.vid, err)
		return
	}
	if err = Fdatasync(fd); err != nil {
		log.Errorf("Fdatasync() failed. %s. %v", _this.getIndexFileName(), err)
		return
	}
	if err = Fadvise(fd, offset, size, POSIX_FADV_DONTNEED); err == nil {
		_this.syncedSize = _this.FileSize
	} else {
		log.Errorf("Fadvise() failed. %s. %v", _this.getIndexFileName(), err)
	}
	return
}

// -------- Index::writeSuperBlock() --------
func (_this *Index) writeSuperBlock() (err error) {
	_this.idxFile.Seek(0, os.SEEK_SET)

	var writedSize uint64
	if writedSize, err = _this.superblock.WriteToFile(_this.idxFile); err == nil {
		_this.FileSize = writedSize
	}
	return
}

// AppendIndexEntry ()
//  ======== Index::AppendIndexEntry() ========
func (_this *Index) AppendIndexEntry(entry IndexEntry) (err error) {
	key := entry.Key
	region := entry.Region

	if uint64(IndexfileMaxSize)-uint64(region.Size) < _this.FileSize {
		err = errors.ErrIndexNomoreSpace
		log.Errorf("vid:%d, Dir:%s. %v", _this.vid, _this.Dir, err)
		return
	}

	buf := make([]byte, IndexEntrySize)
	pos := 0
	utils.BigEndian.PutInt64(buf[pos:], key)
	pos += int(unsafe.Sizeof(key))

	v64 := region.toUint64()
	h32 := uint32(v64 >> 32)
	l32 := uint32(v64)
	//log.Debugf("h32:%x l32: %x v64:%x", h32, l32, v64)

	utils.BigEndian.PutUint32(buf[pos:], h32)
	pos += int(unsafe.Sizeof(h32))
	utils.BigEndian.PutUint32(buf[pos:], l32)
	//pos += int(unsafe.Sizeof(l32))

	if _, err = _this.idxFile.Write(buf); err == nil {
		_this.FileSize += IndexEntrySize
		if err = _this.flushFile(false); err != nil {
			_this.FileSize -= IndexEntrySize
			return
		}
	} else {
		return
	}

	_this.totalSize += uint64(region.Size)
	_this.SetNeedleRegion(key, region)

	return
}

// -------- Index::loadSuperBlock() --------
func (_this *Index) loadSuperBlock() (err error) {
	_this.idxFile.Seek(0, os.SEEK_SET)
	err = _this.superblock.ReadFromFile(_this.idxFile)
	return
}
