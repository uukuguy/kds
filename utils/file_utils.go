package utils

import (
	"os"

	log "github.com/Sirupsen/logrus"
)

// GetFileSize ()
// ======== GetFileSize() ========
func GetFileSize(file *os.File) (filesize uint64, err error) {
	var stat os.FileInfo
	if stat, err = file.Stat(); err != nil {
		log.Errorf("os.File.Stat() error(%v)", err)
		return
	}
	filesize = uint64(stat.Size())
	return
}

// FileExist ()
// ======== FileExist() ========
func FileExist(filename string) bool {
	var err error
	_, err = os.Stat(filename)
	return err == nil || os.IsExist(err)
}
