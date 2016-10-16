package haystack

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uukuguy/kds/store/errors"
	log "github.com/uukuguy/kds/utils/logger"
)

// Metrics -
// **************** Metrics ****************
type Metrics struct {
	ReadCount   uint64
	ReadBytes   uint64
	ReadTime    uint64
	WriteCount  uint64
	WriteBytes  uint64
	WriteTime   uint64
	DeleteCount uint64
	DeleteBytes uint64
	DeleteTime  uint64
}

// Reset ()
// ======== Metrics::Reset() ========
func (_this *Metrics) Reset() {
	_this.ReadCount = 0
	_this.ReadBytes = 0
	_this.ReadTime = 0
	_this.WriteCount = 0
	_this.WriteBytes = 0
	_this.WriteTime = 0
	_this.DeleteCount = 0
	_this.DeleteBytes = 0
	_this.DeleteTime = 0
}

// String ()
// ======== String() ========
func (_this *Metrics) String() string {
	return fmt.Sprintf(`
-----------------------------
Metrics

WriteCount:           %d
WriteBytes:           %d
WriteTime:            %d
ReadCount:            %d
ReadBytes:            %d
ReadTime:             %d
DeleteCount:          %d
DeleteBytes:          %d
DeleteTime:           %d
-----------------------------
`,
		_this.WriteCount,
		_this.WriteBytes,
		_this.WriteTime,
		_this.ReadCount,
		_this.ReadBytes,
		_this.ReadTime,
		_this.DeleteCount,
		_this.DeleteBytes,
		_this.DeleteTime,
	)
}

// Volume -
// **************** Volume ****************
type Volume struct {
	ID      int32
	data    *Data
	index   *Index
	rwlock  sync.RWMutex
	metrics Metrics
}

// String ()
// ======== String() ========
func (_this *Volume) String() string {
	return fmt.Sprintf(`
-----------------------------
Volume

ID:                   %d
data:                 %s
index:                %s
Metrics:              %s
-----------------------------
`,
		_this.ID,
		_this.data.String(),
		_this.index.String(),
		_this.metrics.String(),
	)
}

// NewVolume ()
// ======== NewVolume() ========
func NewVolume(vid int32, storeDir string) (volume *Volume) {
	volume = &Volume{
		ID:    vid,
		data:  NewData(vid, storeDir),
		index: NewIndex(vid, storeDir),
	}

	return
}

// Init ()
// ======== Init() ========
func (_this *Volume) Init() (err error) {
	if _this.data == nil {
		return fmt.Errorf("Volume.data == nil")
	}
	if err = _this.data.Init(); err != nil {
		return
	}

	if _this.index == nil {
		return fmt.Errorf("Volume.index == nil")
	}
	if err = _this.index.Init(); err != nil {
		return
	}

	return
}

// Close ()
// ======== Close() ========
func (_this *Volume) Close() {
	if _this.data != nil {
		_this.data.Close()
	}
	if _this.index != nil {
		_this.index.Close()
	}
}

// WriteNeedle ()
// ======== WriteNeedle() ========
func (_this *Volume) WriteNeedle(needle *Needle) (err error) {
	// Just append new needle to the end of data file.

	now := time.Now().UnixNano()

	_this.rwlock.Lock()
	var region NeedleRegion
	if region, err = _this.data.AppendNeedle(needle); err == nil {
		if err = _this.index.AppendIndexEntry(IndexEntry{needle.Key, region}); err != nil {
			log.Errorf("Volume.WriteNeedle() _this.index.AppendIndexEntry() failed. %v", err)
		}
	} else {
		log.Errorf("Volume.WriteNeedle() _this.data.AppendNeedle() failed. %v", err)
	}
	_this.rwlock.Unlock()

	if err == nil {
		atomic.AddUint64(&_this.metrics.WriteCount, 1)
		atomic.AddUint64(&_this.metrics.WriteBytes, uint64(region.Size))
		atomic.AddUint64(&_this.metrics.WriteTime, uint64(time.Now().UnixNano()-now))
	}

	return
}

// ReadNeedle ()
// ======== ReadNeedle() ========
func (_this *Volume) ReadNeedle(key int64) (needle *Needle, err error) {
	now := time.Now().UnixNano()

	_this.rwlock.RLock()

	var region NeedleRegion
	var exist bool
	if region, exist = _this.index.GetNeedleRegion(key); !exist {
		err = errors.ErrNeedleNotExist
		log.Errorf("Volume.index.GetNeedleRegion() failed. key=%d. %v", key, err)
	} else {
		if needle, err = _this.data.GetNeedle(key, region); err != nil {
			log.Errorf("Volume.data.GetNeedle() failed. key=%d, region=%#v. %v", key, region, err)
		}
	}

	_this.rwlock.RUnlock()

	if err == nil {
		atomic.AddUint64(&_this.metrics.ReadCount, 1)
		atomic.AddUint64(&_this.metrics.ReadBytes, uint64(region.Size))
		atomic.AddUint64(&_this.metrics.ReadTime, uint64(time.Now().UnixNano()-now))
	}

	return
}
