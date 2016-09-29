package haystack

import (
	"github.com/uukuguy/kds/utils"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
)

// **************** Store ****************
type Store struct {
	Volumes map[int32]*Volume
	Dir     string
}

// ======== NewStore() ========
func NewStore(store_dir string) (store *Store) {
	store = &Store{
		Volumes: make(map[int32]*Volume),
		Dir:     store_dir,
	}

	return
}

// ======== Init() ========
func (this *Store) Init() (err error) {
	utils.LogInfof("Init kds store in %s", this.Dir)

	if err = os.MkdirAll(this.Dir, os.ModeDir|0755); err != nil {
		utils.LogErrorf(err, "os.MkdirAll() failed. dir=%s err=%v", this.Dir, err)
		return
	}

	this.loadVolumes()

	return
}

// ======== Close() ========
func (this *Store) Close() {
	for _, volume := range this.Volumes {
		if volume != nil {
			volume.Close()
		}
	}
	this.Volumes = make(map[int32]*Volume)
}

// -------- loadVolumes() --------
func (this *Store) loadVolumes() (err error) {
	fileInfos, err := ioutil.ReadDir(this.Dir)
	for i, fi := range fileInfos {
		basename := fi.Name()
		if ext := filepath.Ext(basename); ext == ".dat" {
			var vid int64
			var volume *Volume
			if vid, err = strconv.ParseInt(basename[:len(basename)-4], 10, 64); err == nil {
				utils.LogDebugf("i=%d Loading volume %d from %s", i, vid, basename)
				volume = NewVolume(int32(vid), this.Dir)
				if err = volume.Init(); err != nil {
					// 初始化失败一个卷，整个Store就无法初始化。
					utils.LogErrorf(err, "volume %d in %s Init() failed. %v", vid, this.Dir, err)
					return err
				}
			}
			this.Volumes[int32(vid)] = volume
			utils.LogDebugf("Loaded volume info. %s", volume.String())
		}

	}

	utils.LogInfof("%d volumes loaded.", len(this.Volumes))

	return
}

// ======== GetVolume() ========
func (store *Store) GetVolume(vid int32) (*Volume, bool) {
	volume, ok := store.Volumes[vid]
	return volume, ok
}

// ======== CreateVolume() ========
func (this *Store) CreateVolume(vid int32) (volume *Volume, err error) {
	volume = NewVolume(vid, this.Dir)
	if err = volume.Init(); err != nil {
		utils.LogErrorf(err, "Store.CreateVolume() failed. vid:%d %v", vid, err)
		return
	}
	this.Volumes[vid] = volume
	return
}
