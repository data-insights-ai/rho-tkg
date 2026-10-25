//go:build unix

package tiered

import (
	"io/fs"
	"syscall"
)

// fileDiskBytes is the blocks the file holds (what du counts), so badger's
// sparse preallocations count only what was written: an open value log is
// mmapped at 2 GB.
func fileDiskBytes(fi fs.FileInfo) int64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return st.Blocks * 512
	}
	return fi.Size()
}
