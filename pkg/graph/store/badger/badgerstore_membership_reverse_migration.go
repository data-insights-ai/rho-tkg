package badger

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// ErrHistoricalReverseMigrationRequired refuses implicit conversion of an
// existing directory. Use the explicit offline migration owner instead.
var ErrHistoricalReverseMigrationRequired = errors.New("graph: explicit offline historical reverse migration required")

// ErrHistoricalReverseMigrationIncomplete refuses ordinary opens until the
// explicit migration owner has finished both durable copy and legacy cleanup.
var ErrHistoricalReverseMigrationIncomplete = errors.New("graph: historical reverse migration incomplete; resume offline")

const historicalReverseProgressKey = "historical_reverse_progress_v2"

func historicalReverseProgressKeyBytes() []byte {
	return storepkg.MetaKey(historicalReverseProgressKey)
}

// HistoricalReverseMigrationProgress is durable auxiliary codec/cursor state.
// Copy and cleanup acknowledge only synced pages. Row codec and forward posting
// bytes are unchanged. Complete means legacy reverse conversion has finished;
// it does not imply forward membership bootstrap readiness.
type HistoricalReverseMigrationProgress struct {
	Version  int    `json:"version"`
	Codec    string `json:"codec"`
	Phase    string `json:"phase"`
	After    []byte `json:"after,omitempty"`
	Copied   int64  `json:"copied,omitzero"`
	Removed  int64  `json:"removed,omitzero"`
	Complete bool   `json:"complete,omitzero"`
}

func historicalReverseCompleteProgress() HistoricalReverseMigrationProgress {
	return HistoricalReverseMigrationProgress{Version: 2, Codec: "direct_crc32c", Phase: "complete", Complete: true}
}

func readHistoricalWriterVersion(db *badgerv4.DB) (uint16, error) {
	var version uint16
	err := db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.WireFormatVersionKey)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if item.ValueSize() != 2 {
			return fmt.Errorf("%w: writer marker size", ErrInvalidStoreMutation)
		}
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if len(raw) != 2 || binary.BigEndian.Uint16(raw) == 0 {
			return fmt.Errorf("%w: writer version marker", ErrInvalidStoreMutation)
		}
		version = binary.BigEndian.Uint16(raw)
		return nil
	})
	return version, err
}

func readHistoricalReverseProgress(db *badgerv4.DB) (HistoricalReverseMigrationProgress, error) {
	var p HistoricalReverseMigrationProgress
	err := db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(historicalReverseProgressKeyBytes())
		if err != nil {
			return err
		}
		if item.ValueSize() > 4096 {
			return fmt.Errorf("%w: reverse progress size", ErrInvalidStoreMutation)
		}
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		d := json.NewDecoder(bytes.NewReader(raw))
		d.DisallowUnknownFields()
		if err := d.Decode(&p); err != nil {
			return fmt.Errorf("%w: reverse progress JSON: %v", ErrInvalidStoreMutation, err)
		}
		var extra any
		if err := d.Decode(&extra); err != io.EOF {
			return fmt.Errorf("%w: reverse progress trailing JSON", ErrInvalidStoreMutation)
		}
		if p.Version != 2 || p.Codec != "direct_crc32c" || p.Copied < 0 || p.Removed < 0 || p.Removed > p.Copied || p.Complete != (p.Phase == "complete") {
			return fmt.Errorf("%w: reverse progress state", ErrInvalidStoreMutation)
		}
		switch p.Phase {
		case "copy":
			if p.Removed != 0 {
				return fmt.Errorf("%w: reverse copy removal", ErrInvalidStoreMutation)
			}
		case "cleanup":
		case "complete":
			if len(p.After) != 0 || p.Copied != p.Removed {
				return fmt.Errorf("%w: reverse completion", ErrInvalidStoreMutation)
			}
		default:
			return fmt.Errorf("%w: reverse migration phase", ErrInvalidStoreMutation)
		}
		if len(p.After) > 0 {
			if len(p.After) != 42 || p.After[0] != diskMembershipReversePrefix || p.After[1] < diskMemberNodeLabel || p.After[1] > diskMemberRelProperty {
				return fmt.Errorf("%w: reverse cursor shape", ErrInvalidStoreMutation)
			}
			id := binary.BigEndian.Uint64(p.After[2:10])
			if id == 0 || id > math.MaxInt64 {
				return fmt.Errorf("%w: reverse cursor owner", ErrInvalidStoreMutation)
			}
		}
		return nil
	})
	return p, err
}

func publishHistoricalReverseProgress(db *badgerv4.DB, p HistoricalReverseMigrationProgress, fence bool) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	err = db.Update(func(txn *badgerv4.Txn) error {
		if fence {
			marker := make([]byte, 2)
			binary.BigEndian.PutUint16(marker, compactHistoricalReverseWriterVersion)
			if err := txn.Set(storepkg.WireFormatVersionKey, marker); err != nil {
				return err
			}
		}
		return txn.Set(historicalReverseProgressKeyBytes(), raw)
	})
	if err != nil {
		return err
	}
	return db.Sync()
}

func verifyCompactHistoricalReverseOpen(db *badgerv4.DB, readOnly bool) error {
	version, err := readHistoricalWriterVersion(db)
	if err != nil {
		return err
	}
	if version > compactHistoricalReverseWriterVersion {
		return storecontract.ErrWireFormatVersionUnsupported
	}
	if version == compactHistoricalReverseWriterVersion {
		p, err := readHistoricalReverseProgress(db)
		if err != nil {
			return fmt.Errorf("%w: reverse descriptor: %w", ErrInvalidStoreMutation, err)
		}
		if !p.Complete {
			return ErrHistoricalReverseMigrationIncomplete
		}
		return nil
	}
	// Only an entirely empty directory may initialize directly. In particular
	// an empty-but-marked format5 directory still requires explicit migration.
	if version != 0 || readOnly {
		return ErrHistoricalReverseMigrationRequired
	}
	empty := true
	err = db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Rewind()
		empty = !it.Valid()
		return nil
	})
	if err != nil {
		return err
	}
	if !empty {
		return ErrHistoricalReverseMigrationRequired
	}
	return publishHistoricalReverseProgress(db, historicalReverseCompleteProgress(), true)
}

// HistoricalReverseMigration owns a directory exclusively through Badger's
// actual directory lock. It never creates a graph or hydrates entity maps.
// Close releases that ownership; a subsequent explicit open resumes progress.
type HistoricalReverseMigration struct {
	mu       sync.Mutex
	db       *badgerv4.DB
	closed   bool
	closeErr error
	testHook func(string)
}

// OpenHistoricalReverseMigration explicitly reserves an offline synced
// persistent directory and publishes writer fence6 before copying. All other
// graph owners must be closed first. Only format5 with complete forward
// membership or a resumable format6 migration is accepted. Calling this method
// is the opt-in transition; ordinary New never upgrades existing format5.
func OpenHistoricalReverseMigration(ctx context.Context, cfg Config) (*HistoricalReverseMigration, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.InMemory || cfg.ReadOnly || cfg.Dir == "" || !cfg.SyncWrites || !cfg.HistoricalMembershipOnDisk {
		return nil, fmt.Errorf("%w: migration requires offline persistent synced historical membership", ErrInvalidStoreMutation)
	}
	if err := validateTuningConfig(cfg); err != nil {
		return nil, err
	}
	if err := validateEncryptionConfig(cfg); err != nil {
		return nil, err
	}
	// Refuse oversized WAL rather than opening a second temporary owner or
	// implicitly migrating unrelated storage on this explicit auxiliary door.
	if err := guardReadOnlyOversizedWAL(cfg); err != nil {
		return nil, err
	}
	db, err := badgerv4.Open(buildBadgerOptions(cfg))
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*HistoricalReverseMigration, error) { return nil, errors.Join(err, db.Close()) }
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	version, err := readHistoricalWriterVersion(db)
	if err != nil {
		return fail(err)
	}
	switch version {
	case compactHistoricalReverseWriterVersion:
		if _, err := readHistoricalReverseProgress(db); err != nil {
			return fail(err)
		}
	case historicalMembershipWriterVersion:
		probe := &Store{db: db}
		p, err := probe.historicalMembershipProgress()
		if err != nil {
			return fail(err)
		}
		if !p.Complete {
			return fail(ErrIndexNotFound)
		}
		// Fence and initial descriptor are one small atomic, synced commit.
		p2 := HistoricalReverseMigrationProgress{Version: 2, Codec: "direct_crc32c", Phase: "copy"}
		if err := publishHistoricalReverseProgress(db, p2, true); err != nil {
			return fail(err)
		}
	default:
		return fail(storecontract.ErrWireFormatVersionUnsupported)
	}
	return &HistoricalReverseMigration{db: db}, nil
}

// Status returns independently owned durable progress. It performs no writes.
func (m *HistoricalReverseMigration) Status() (HistoricalReverseMigrationProgress, error) {
	if m == nil {
		return HistoricalReverseMigrationProgress{}, ErrNilStore
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.db == nil {
		return HistoricalReverseMigrationProgress{}, ErrStoreClosed
	}
	return readHistoricalReverseProgress(m.db)
}

// Advance copies or removes one bounded reverse page. Synced additions precede
// the copy cursor; all copying precedes destructive legacy cleanup. Cleanup
// cursors commit atomically with removals, in at most 1000 entries. Crash
// replay is idempotent in both phases.
func (m *HistoricalReverseMigration) Advance(ctx context.Context, limits HistoricalMembershipLimits) (HistoricalReverseMigrationProgress, error) {
	var zero HistoricalReverseMigrationProgress
	if m == nil {
		return zero, ErrNilStore
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if limits.MaxRows < 1 || limits.MaxRows > 10000 || limits.MaxBytes < 1 || limits.MaxBytes > 64<<20 {
		return zero, fmt.Errorf("%w: reverse migration page limits", ErrInvalidStoreMutation)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.db == nil {
		return zero, ErrStoreClosed
	}
	p, err := readHistoricalReverseProgress(m.db)
	if err != nil || p.Complete {
		return p, err
	}
	if p.Phase == "cleanup" {
		// Bound the small atomic delete+cursor transaction even when the caller
		// requests a larger copy page. A 1000-entry fixed42B-key page fits the
		// minimum supported memtable's transaction budget.
		limits.MaxRows = min(limits.MaxRows, 1000)
	}
	var ops []writeOp
	var rows int64
	var used int64
	exhausted := true
	err = m.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte{diskMembershipReversePrefix}
		it := txn.NewIterator(opts)
		defer it.Close()
		seek := opts.Prefix
		if len(p.After) > 0 {
			seek = p.After
		}
		it.Seek(seek)
		if len(p.After) > 0 && it.ValidForPrefix(opts.Prefix) && bytes.Equal(it.Item().Key(), p.After) {
			it.Next()
		}
		for ; it.ValidForPrefix(opts.Prefix); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := it.Item().KeyCopy(nil)
			if it.Item().ValueSize() < 20 || it.Item().ValueSize() > 52 {
				return fmt.Errorf("%w: legacy reverse value size", ErrInvalidStoreMutation)
			}
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(key) != 42 {
				return fmt.Errorf("%w: legacy reverse migration key", ErrInvalidStoreMutation)
			}
			rawID := binary.BigEndian.Uint64(key[2:10])
			if rawID == 0 || rawID > math.MaxInt64 {
				return fmt.Errorf("%w: legacy reverse migration owner", ErrInvalidStoreMutation)
			}
			forward, err := legacyReverseMembershipForward(key, value, key[1], types.EntityID(rawID)) // #nosec G115 — rawID checked in signed range
			if err != nil {
				return err
			}
			newOp := compactReverseMembershipOp(forward)
			cost := int64(len(key) + len(value) + len(newOp.key) + len(newOp.value))
			if rows >= int64(limits.MaxRows) || cost > limits.MaxBytes-used {
				exhausted = false
				break
			}
			if p.Phase == "copy" {
				ops = append(ops, newOp)
			} else {
				// A cleanup record must have its exact compact replacement.
				i, err := txn.Get(newOp.key)
				if err != nil {
					return err
				}
				v, err := i.ValueCopy(nil)
				if err != nil {
					return err
				}
				if !bytes.Equal(v, newOp.value) {
					return fmt.Errorf("%w: compact replacement differs", ErrInvalidStoreMutation)
				}
				ops = append(ops, writeOp{opType: writeOpDelete, key: key})
			}
			used += cost
			rows++
			p.After = key
		}
		return nil
	})
	if err != nil {
		return zero, err
	}
	if rows == 0 && !exhausted {
		return zero, fmt.Errorf("%w: reverse entry exceeds page byte budget", ErrInvalidStoreMutation)
	}
	if p.Phase == "copy" && p.Copied > math.MaxInt64-rows || p.Phase == "cleanup" && p.Removed > math.MaxInt64-rows {
		return zero, fmt.Errorf("%w: reverse migration counter overflow", ErrInvalidStoreMutation)
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	copyPhase := p.Phase == "copy"
	if copyPhase {
		wb := m.db.NewWriteBatch()
		defer wb.Cancel()
		for _, op := range ops {
			if err := wb.Set(op.key, op.value); err != nil {
				return zero, err
			}
		}
		if err := wb.Flush(); err != nil {
			return zero, err
		}
		if err := m.db.Sync(); err != nil {
			return zero, err
		}
		if m.testHook != nil {
			m.testHook("copy-data")
		}
		p.Copied += rows
	} else {
		p.Removed += rows
	}
	if exhausted {
		if p.Phase == "copy" {
			p.Phase = "cleanup"
		} else {
			p.Phase = "complete"
			p.Complete = true
		}
		p.After = nil
	}
	if p.Complete && p.Copied != p.Removed {
		return zero, fmt.Errorf("%w: reverse completion counts differ", ErrInvalidStoreMutation)
	}
	if copyPhase {
		if err := publishHistoricalReverseProgress(m.db, p, false); err != nil {
			return zero, err
		}
	} else {
		raw, err := json.Marshal(p)
		if err != nil {
			return zero, err
		}
		if m.testHook != nil {
			m.testHook("cleanup-before-commit")
		}
		err = m.db.Update(func(txn *badgerv4.Txn) error {
			for _, op := range ops {
				if err := txn.Delete(op.key); err != nil {
					return err
				}
			}
			return txn.Set(historicalReverseProgressKeyBytes(), raw)
		})
		if err != nil {
			return zero, err
		}
		if err := m.db.Sync(); err != nil {
			return zero, err
		}
		if m.testHook != nil {
			m.testHook("cleanup-data")
		}
	}
	if m.testHook != nil {
		m.testHook(p.Phase + "-cursor")
	}
	return p, nil
}

// Close releases exclusive directory ownership, idempotently. It serializes
// against Advance and Status. No callbacks run under this owner's lock.
func (m *HistoricalReverseMigration) Close() error {
	if m == nil {
		return ErrNilStore
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.db == nil {
		return ErrStoreClosed
	}
	if !m.closed {
		m.closed = true
		m.closeErr = m.db.Close()
	}
	return m.closeErr
}
