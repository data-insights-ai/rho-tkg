package tiered

import (
	"io/fs"
	"path/filepath"
)

// VolumeHot names the volume of DataDir (ADR-0012), the only one until the
// warm and cold volumes are configured.
const VolumeHot = "hot"

// shardDisk is the volume, directory and bytes on disk of the shard at rel,
// or nothing for an in-memory store. The bytes are -1 when the directory
// cannot be read (missing, or a file removed by a compaction mid-walk).
func (ts *Store) shardDisk(rel string) (volume, path string, bytes int64) {
	if ts.inMemory {
		return "", "", 0
	}
	path = ts.shardDir(rel)
	n, err := dirDiskBytes(path)
	if err != nil {
		return VolumeHot, path, -1
	}
	return VolumeHot, path, n
}

// dirDiskBytes sums the bytes on disk of the regular files under dir.
func dirDiskBytes(dir string) (int64, error) {
	var n int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		n += fileDiskBytes(fi)
		return nil
	})
	return n, err
}
