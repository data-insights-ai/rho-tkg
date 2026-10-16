package graph

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrStoragePolicy reports a stored directory graph whose retention policy
// differs from the one Config.Storage asks for. The policy is fixed when the
// store is created (the raft log refuses a changed one); leave Storage zero to
// adopt the stored policy. It wraps ErrInvalidConfig.
var ErrStoragePolicy = errors.New("graph: storage policy differs from the stored one")

// StorageLimits sizes the retained-record quotas and the Pebble caches of a
// graph. A zero field means its default. RetainedRecords and RetainedBytes are
// quotas on every immutable record version the graph keeps, which for a
// create-heavy import is nearly every record written (about 70-90 records and
// 30 KB logical per op). They are fixed when a directory graph is created and
// persisted in it; CacheBytes and MemTableBytes are per-process and may change
// on every Open.
type StorageLimits struct {
	// RetainedRecords caps retained records: default 1<<30, at least 8200, at
	// most 1<<39.
	RetainedRecords uint64
	// RetainedBytes caps retained logical bytes: default 1<<38, at most 1<<38.
	RetainedBytes uint64
	// CacheBytes is the Pebble block cache: default 256 MiB, at most 1 GiB.
	CacheBytes int64
	// MemTableBytes is the Pebble memtable: default 32 MiB, from 64 KiB to 64 MiB.
	MemTableBytes uint64
}

const (
	defaultRetainedRecords uint64 = 1 << 30
	defaultRetainedBytes   uint64 = 1 << 38
	// legacyRetained* are the caps of stores created before StorageLimits.
	legacyRetainedRecords uint64 = 1 << 20
	legacyRetainedBytes   uint64 = 256 << 20
	minRetainedRecords    uint64 = 2 * (4096 + 3) // raftlog: 2*(MaxInstallWrites+3)
	policyFileName               = "rho-v5.policy"
)

func (s StorageLimits) resolve() StorageLimits {
	if s.RetainedRecords == 0 {
		s.RetainedRecords = defaultRetainedRecords
	}
	if s.RetainedBytes == 0 {
		s.RetainedBytes = defaultRetainedBytes
	}
	if s.CacheBytes == 0 {
		s.CacheBytes = 256 << 20
	}
	if s.MemTableBytes == 0 {
		s.MemTableBytes = 32 << 20
	}
	return s
}

// validate checks the resolved limits against raftlog's bounds.
func (s StorageLimits) validate() error {
	r := s.resolve()
	if r.RetainedRecords < minRetainedRecords || r.RetainedRecords > 1<<39 ||
		r.RetainedBytes < 1<<20 || r.RetainedBytes > 1<<38 ||
		r.CacheBytes < 1 || r.CacheBytes > 1<<30 || r.MemTableBytes < 64<<10 || r.MemTableBytes > 64<<20 {
		return fmt.Errorf("%w: storage limits out of range", ErrInvalidConfig)
	}
	return nil
}

type storedPolicy struct {
	RetainedRecords uint64 `json:"retained_records"`
	RetainedBytes   uint64 `json:"retained_bytes"`
}

// writePolicy persists the chosen policy next to the store (write, sync, rename).
func writePolicy(dir string, s StorageLimits) error {
	b, err := json.Marshal(storedPolicy{s.RetainedRecords, s.RetainedBytes})
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, policyFileName+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) // #nosec G304 -- Config.Dir is the caller's store.
	if err != nil {
		return err
	}
	_, werr := f.Write(b)
	serr := f.Sync()
	if err := errors.Join(werr, serr, f.Close()); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, policyFileName)); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil { // #nosec G304
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// readPolicy returns the stored caps; a missing file is a store of an older
// build, with the legacy caps.
func readPolicy(dir string) (retainedRecords, retainedBytes uint64, err error) {
	b, err := os.ReadFile(filepath.Join(dir, policyFileName)) // #nosec G304
	if errors.Is(err, os.ErrNotExist) {
		return legacyRetainedRecords, legacyRetainedBytes, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("%w: storage policy unreadable", ErrInvalidConfig)
	}
	var p storedPolicy
	if json.Unmarshal(b, &p) != nil || (StorageLimits{RetainedRecords: p.RetainedRecords, RetainedBytes: p.RetainedBytes}).validate() != nil || p.RetainedRecords == 0 || p.RetainedBytes == 0 {
		return 0, 0, fmt.Errorf("%w: storage policy file is corrupt", ErrInvalidConfig)
	}
	return p.RetainedRecords, p.RetainedBytes, nil
}

// reopenPolicy resolves the policy of an existing directory store: the stored
// caps win; an explicit different request is ErrStoragePolicy.
func reopenPolicy(dir string, want StorageLimits) (StorageLimits, error) {
	rec, byt, err := readPolicy(dir)
	if err != nil {
		return StorageLimits{}, err
	}
	if (want.RetainedRecords != 0 && want.RetainedRecords != rec) || (want.RetainedBytes != 0 && want.RetainedBytes != byt) {
		return StorageLimits{}, fmt.Errorf("%w: %w (stored %d records, %d bytes)", ErrInvalidConfig, ErrStoragePolicy, rec, byt)
	}
	want.RetainedRecords, want.RetainedBytes = rec, byt
	return want.resolve(), nil
}
