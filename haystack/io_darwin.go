// +build darwin

package haystack

const (
	O_NOATIME           = 0    // darwin no O_NOATIME set to O_LARGEFILE
	FALLOC_FL_KEEP_SIZE = 0x01 /* default is extend size */
)

func Fallocate(fd uintptr, mode uint32, off uint64, len uint64) (err error) {
	return
}

func Fdatasync(fd uintptr) (err error) {
	return
}

const (
	POSIX_FADV_NORMAL     = 0
	POSIX_FADV_SEQUENTIAL = 0
	POSIX_FADV_RANDOM     = 0
	POSIX_FADV_NOREUSE    = 0
	POSIX_FADV_WILLNEED   = 0
	POSIX_FADV_DONTNEED   = 0
)

func Fadvise(fd uintptr, off uint64, len uint64, advise int) (err error) {
	return
}

const (
	SYNC_FILE_RANGE_WAIT_BEFORE = 1
	SYNC_FILE_RANGE_WRITE       = 2
	SYNC_FILE_RANGE_WAIT_AFTER  = 4
)

func Syncfilerange(fd uintptr, off uint64, n uint64, flags int) (err error) {
	return
}
