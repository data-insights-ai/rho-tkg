package tiered

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Volumes (ADR-0012): a volume is a directory on a device. hot is DataDir,
// warm Config.WarmDir, cold Config.ColdDir. A catalog entry names its
// shard's volume ("" = hot), resolved through the config at open, so a
// remount needs no catalog edit.
const (
	VolumeHot  = "hot"
	VolumeWarm = "warm"
	VolumeCold = "cold"
)

// catalogFormatSentinel is the catalog entry a store with a shard off the hot
// volume carries (kind catalog/2, no path): the releases before the volume
// minor refuse it at load (an empty path, an unknown kind) and so never open
// such a store, where they would look for that shard under DataDir.
const (
	catalogFormatSentinel = "__format__"
	catalogFormatKind     = ShardKind("catalog/2")
)

// volumeName is the listing's name of a catalog volume.
func volumeName(v string) string {
	if v == "" {
		return VolumeHot
	}
	return v
}

// parseVolume is the catalog form of a volume name ("" or hot = ""); false
// for an unknown one.
func parseVolume(v string) (string, bool) {
	switch v {
	case "", VolumeHot:
		return "", true
	case VolumeWarm, VolumeCold:
		return v, true
	}
	return "", false
}

// catalogVolume is parseVolume for a Config field.
func catalogVolume(field, v string) (string, error) {
	if cv, ok := parseVolume(v); ok {
		return cv, nil
	}
	return "", fmt.Errorf("graph: Config.%s %q is not a volume (hot, warm or cold)", field, v)
}

// volumeConfig validates the volume settings and stores them on ts.
func (ts *Store) volumeConfig(cfg Config) error {
	if cfg.InMemory {
		if cfg.WarmDir != "" || cfg.ColdDir != "" || cfg.ReferenceVolume != "" || cfg.ArchiveVolume != "" {
			return fmt.Errorf("graph: volumes (WarmDir, ColdDir, ReferenceVolume, ArchiveVolume) need a disk-backed store")
		}
		return nil
	}
	seen := map[string]string{}
	for _, d := range []struct{ field, dir string }{{"DataDir", cfg.DataDir}, {"WarmDir", cfg.WarmDir}, {"ColdDir", cfg.ColdDir}} {
		if d.dir == "" {
			continue
		}
		abs, err := filepath.Abs(d.dir)
		if err != nil {
			return fmt.Errorf("graph: Config.%s: %w", d.field, err)
		}
		if other, dup := seen[abs]; dup {
			return fmt.Errorf("graph: Config.%s is Config.%s; each volume needs its own directory", d.field, other)
		}
		seen[abs] = d.field
	}
	ts.warmDir, ts.coldDir = cfg.WarmDir, cfg.ColdDir
	var err error
	if ts.refVolume, err = catalogVolume("ReferenceVolume", cfg.ReferenceVolume); err != nil {
		return err
	}
	if ts.refVolume == VolumeCold {
		return fmt.Errorf("graph: Config.ReferenceVolume cannot be cold: every commit and unique check reads the reference shard")
	}
	if ts.archiveVolume, err = catalogVolume("ArchiveVolume", cfg.ArchiveVolume); err != nil {
		return err
	}
	for _, v := range []struct{ field, vol string }{{"ReferenceVolume", ts.refVolume}, {"ArchiveVolume", ts.archiveVolume}} {
		if _, ok := ts.volumeDir(v.vol); !ok {
			return fmt.Errorf("graph: Config.%s is %s, which the config does not map (Config.WarmDir, Config.ColdDir)", v.field, v.vol)
		}
	}
	return nil
}

// volumeDir is the directory of a catalog volume; false when the config does
// not map it.
func (ts *Store) volumeDir(v string) (string, bool) {
	switch v {
	case "":
		return ts.dataDir, true
	case VolumeWarm:
		return ts.warmDir, ts.warmDir != ""
	case VolumeCold:
		return ts.coldDir, ts.coldDir != ""
	}
	return "", false
}

// volumeOf is the catalog volume of the shard at rel: its catalog entry's,
// else (a shard being created) the configured one for the reference shard
// and the archive and hot for an event shard.
func (ts *Store) volumeOf(rel string) string {
	switch rel {
	case "reference":
		if e, ok := ts.catalog.GetShard("reference"); ok {
			return e.Volume
		}
		return ts.refVolume
	case "archive":
		if e, ok := ts.catalog.GetShard("archive"); ok {
			return e.Volume
		}
		return ts.archiveVolume
	}
	return ts.catalog.volumeOfPath(rel)
}

// checkCatalogVolumes fails New when the catalog places a shard on a volume
// the config does not map: it would otherwise open, or make, that shard
// under the wrong directory.
func (ts *Store) checkCatalogVolumes() error {
	for _, e := range ts.catalog.snapshotShards() {
		if _, ok := ts.volumeDir(e.Volume); !ok {
			return fmt.Errorf("graph: shard %q is on volume %s, which the config does not map (Config.WarmDir, Config.ColdDir)", e.Name, e.Volume)
		}
	}
	return nil
}

// requireVolume fails when the volume root of the shard at rel is missing
// (an unmounted device): a directory made there would land on the device
// underneath.
func (ts *Store) requireVolume(rel string) error {
	if ts.inMemory {
		return nil
	}
	root, _ := ts.volumeDir(ts.volumeOf(rel))
	if _, err := os.Stat(root); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("graph: volume directory %s is missing: %w", root, fs.ErrNotExist)
		}
		return fmt.Errorf("graph: volume directory %s: %w", root, err)
	}
	return nil
}
