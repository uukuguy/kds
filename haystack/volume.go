package haystack

import (
	"fmt"
	"github.com/uukuguy/kds/store/errors"
	"github.com/uukuguy/kds/utils"
	"sync"
	"sync/atomic"
	"time"
)

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

func (this *Metrics) Reset() {
	this.ReadCount = 0
	this.ReadBytes = 0
	this.ReadTime = 0
	this.WriteCount = 0
	this.WriteBytes = 0
	this.WriteTime = 0
	this.DeleteCount = 0
	this.DeleteBytes = 0
	this.DeleteTime = 0
}

// ======== String() ========
func (this *Metrics) String() string {
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
		this.WriteCount,
		this.WriteBytes,
		this.WriteTime,
		this.ReadCount,
		this.ReadBytes,
		this.ReadTime,
		this.DeleteCount,
		this.DeleteBytes,
		this.DeleteTime,
	)
}

// **************** Volume ****************
type Volume struct {
	Id      int32
	data    *Data
	index   *Index
	rwlock  sync.RWMutex
	metrics Metrics
}

// ======== String() ========
func (this *Volume) String() string {
	return fmt.Sprintf(`
-----------------------------
Volume

Id:                   %d
data:                 %s
index:                %s
Metrics:              %s
-----------------------------
`,
		this.Id,
		this.data.String(),
		this.index.String(),
		this.metrics.String(),
	)
}

// ======== NewVolume() ========
func NewVolume(vid int32, store_dir string) (volume *Volume) {
	volume = &Volume{
		Id:    vid,
		data:  NewData(vid, store_dir),
		index: NewIndex(vid, store_dir),
	}

	return
}

// ======== Init() ========
func (this *Volume) Init() (err error) {
	if this.data == nil {
		return fmt.Errorf("Volume.data == nil")
	}
	if err = this.data.Init(); err != nil {
		return
	}

	if this.index == nil {
		return fmt.Errorf("Volume.index == nil")
	}
	if err = this.index.Init(); err != nil {
		return
	}

	return
}

// ======== Close() ========
func (volume *Volume) Close() {
	if volume.data != nil {
		volume.data.Close()
	}
	if volume.index != nil {
		volume.index.Close()
	}
}

// ======== WriteNeedle() ========
func (this *Volume) WriteNeedle(needle *Needle) (err error) {
	// Just append new needle to the end of data file.

	now := time.Now().UnixNano()

	this.rwlock.Lock()
	var region NeedleRegion
	if region, err = this.data.AppendNeedle(needle); err == nil {
		if err = this.index.AppendIndexEntry(IndexEntry{needle.Key, region}); err != nil {
			utils.LogErrorf(err, "Volume.WriteNeedle() this.index.AppendIndexEntry() failed.")
		}
	} else {
		utils.LogErrorf(err, "Volume.WriteNeedle() this.data.AppendNeedle() failed.")
	}
	this.rwlock.Unlock()

	if err == nil {
		atomic.AddUint64(&this.metrics.WriteCount, 1)
		atomic.AddUint64(&this.metrics.WriteBytes, uint64(region.Size))
		atomic.AddUint64(&this.metrics.WriteTime, uint64(time.Now().UnixNano()-now))
	}

	return
}

// ======== ReadNeedle() ========
func (this *Volume) ReadNeedle(key int64) (needle *Needle, err error) {
	now := time.Now().UnixNano()

	this.rwlock.RLock()

	var region NeedleRegion
	var exist bool
	if region, exist = this.index.GetNeedleRegion(key); !exist {
		err = errors.ErrNeedleNotExist
		utils.LogErrorf(err, "key=%d", key)
	} else {
		if needle, err = this.data.GetNeedle(key, region); err != nil {
			utils.LogErrorf(err, "")
		}
	}

	this.rwlock.RUnlock()

	if err == nil {
		atomic.AddUint64(&this.metrics.ReadCount, 1)
		atomic.AddUint64(&this.metrics.ReadBytes, uint64(region.Size))
		atomic.AddUint64(&this.metrics.ReadTime, uint64(time.Now().UnixNano()-now))
	}

	return
}
