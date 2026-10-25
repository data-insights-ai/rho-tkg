//go:build !unix

package tiered

import "io/fs"

// fileDiskBytes is the file's size off unix (the supported platforms are
// unix), which counts a sparse preallocation in full.
func fileDiskBytes(fi fs.FileInfo) int64 { return fi.Size() }
