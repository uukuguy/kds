// +build linux

package hystack

/*
#define _GNU_SOURCE
#include <fcntl.h>
#include <linux/falloc.h>
*/
import "C"

import (
	"syscall"
)

const (
	O_NOATIME           = syscall.O_NOATIME
	FALLOC_FL_KEEP_SIZE = uint32(C.FALLOC_FL_KEEP_SIZE)
)

func Fallocate(fd uintptr, mode uint32, off uint64, size uint64) error {
	return syscall.Fallocate(int(fd), mode, off, size)
}

func Fdatasync(fd uintptr) (err error) {
	return syscall.Fdatasync(int(fd))
}

const (
	POSIX_FADV_NORMAL     = int(C.POSIX_FADV_NORMAL)
	POSIX_FADV_SEQUENTIAL = int(C.POSIX_FADV_SEQUENTIAL)
	POSIX_FADV_RANDOM     = int(C.POSIX_FADV_RANDOM)
	POSIX_FADV_NOREUSE    = int(C.POSIX_FADV_NOREUSE)
	POSIX_FADV_WILLNEED   = int(C.POSIX_FADV_WILLNEED)
	POSIX_FADV_DONTNEED   = int(C.POSIX_FADV_DONTNEED)
)

func Fadvise(fd uintptr, off uint64, size uint64, advise int) (err error) {
	var errno int
	if errno = int(C.posix_fadvise(C.int(fd), C.__off_t(off), C.__off_t(size), C.int(advise))); errno != 0 {
		err = syscall.Errno(errno)
	}
	return
}

const (
	SYNC_FILE_RANGE_WAIT_BEFORE = 1
	SYNC_FILE_RANGE_WRITE       = 2
	SYNC_FILE_RANGE_WAIT_AFTER  = 4
)

func Syncfilerange(fd uintptr, off uint64, n uint64, flags int) (err error) {
	return syscall.SyncFileRange(int(fd), off, n, flags)
}
