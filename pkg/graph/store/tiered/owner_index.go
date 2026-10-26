package tiered

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// The owner index (SHARD step 10, UNIQIDX; ADR-0012 "Global index"): for a
// declared (label, key) of a label whose rows live on event shards, the ID
// that claimed each value, in one badger under DataDir/index on the hot
// volume, so a unique check or a keyed lookup reads one key instead of every
// shard. It holds keys and IDs only, never rows. A claim is written before
// its row and every reader checks the row, so a claim the row does not back
// (a crash between the two, a rollback, a later change, a delete, a dropped
// shard) is ignored, and no claim is ever missing. The index opens at New
// when its directory exists and at the first declaration otherwise.
//
// Keys: 'D' label key -> "" (a declaration); 'R' label key -> "" (seeded
// from the rows that existed at the declaration: lookups may trust it);
// 'U' label len(key) key value -> the owner's ID (8 bytes, big-endian).
const ownerIndexDir = "index"

// ErrUniqueOwnerIndexNotDeclared is returned for a claim on a (label, key)
// without an owner index.
var ErrUniqueOwnerIndexNotDeclared = errors.New("graph: no owner index is declared for this label and key")

type ownerIndex struct {
	mu       sync.RWMutex
	db       *badgerv4.DB
	declared map[ownerDecl]struct{}
	ready    map[ownerDecl]struct{}
	// Claims are not synced one by one: a shard's durable flush syncs the
	// index first (syncOwnerIndexBeforeRows). syncMu orders that sync with
	// the dirty mark, so no flush skips a sync another one has not finished.
	syncMu sync.Mutex
	dirty  atomic.Bool
}

type ownerDecl struct {
	label uint16
	key   string
}

func ownerDeclKey(label uint16, key string) []byte { return ownerMarkKey('D', label, key) }

func ownerReadyKey(label uint16, key string) []byte { return ownerMarkKey('R', label, key) }

func ownerMarkKey(tag byte, label uint16, key string) []byte {
	b := binary.BigEndian.AppendUint16([]byte{tag}, label)
	return append(b, key...)
}

func ownerPrefix(label uint16, key string) []byte {
	b := binary.BigEndian.AppendUint16([]byte{'U'}, label)
	b = binary.BigEndian.AppendUint16(b, uint16(len(key))) // #nosec G115 -- property keys are bounded far below 64 KiB
	return append(b, key...)
}

func ownerKey(label uint16, key, valueKey string) []byte {
	return append(ownerPrefix(label, key), valueKey...)
}

// openOwnerIndex opens (creating when create is set) the owner index with the
// shards' badger options and loads its declarations; nil, nil when it does
// not exist and create is not set.
func (ts *Store) openOwnerIndex(create bool) (*ownerIndex, error) {
	cfg := ts.badgerCfg(ownerIndexDir, false)
	cfg.SyncWrites = false // claims are synced before the rows they back (syncOwnerIndexBeforeRows)
	if !ts.inMemory {
		cfg.Dir = filepath.Join(ts.dataDir, ownerIndexDir)
		if _, err := os.Stat(cfg.Dir); errors.Is(err, fs.ErrNotExist) && !create {
			return nil, nil
		} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("graph: owner index: %w", err)
		}
	}
	db, err := badgerv4.Open(badger.BadgerOptions(cfg))
	if err != nil {
		return nil, fmt.Errorf("graph: open owner index: %w", err)
	}
	idx := &ownerIndex{db: db, declared: make(map[ownerDecl]struct{}), ready: make(map[ownerDecl]struct{})}
	err = db.View(func(txn *badgerv4.Txn) error {
		for tag, into := range map[byte]map[ownerDecl]struct{}{'D': idx.declared, 'R': idx.ready} {
			opts := badgerv4.DefaultIteratorOptions
			opts.PrefetchValues = false
			opts.Prefix = []byte{tag}
			it := txn.NewIterator(opts)
			for it.Rewind(); it.Valid(); it.Next() {
				k := it.Item().Key()
				if len(k) < 3 {
					it.Close()
					return fmt.Errorf("graph: owner index: malformed mark key %x", k)
				}
				into[ownerDecl{binary.BigEndian.Uint16(k[1:3]), string(k[3:])}] = struct{}{}
			}
			it.Close()
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("graph: load owner index: %w", err)
	}
	return idx, nil
}

// ownerIndexLocked returns the open owner index, opening (creating) it when
// create is set. Caller holds ts.ownerIdxMu.
func (ts *Store) ownerIndexLocked(create bool) (*ownerIndex, error) {
	if ts.ownerIdx != nil || !create {
		return ts.ownerIdx, nil
	}
	idx, err := ts.openOwnerIndex(true)
	if err != nil {
		return nil, err
	}
	ts.ownerIdx = idx
	return idx, nil
}

// DeclareUniqueOwnerIndex declares the owner index of (label, key);
// idempotent. Claims follow with PutUniqueOwner.
func (ts *Store) DeclareUniqueOwnerIndex(label uint16, key string) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	ts.ownerIdxMu.Lock()
	defer ts.ownerIdxMu.Unlock()
	idx, err := ts.ownerIndexLocked(true)
	if err != nil {
		return err
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if _, ok := idx.declared[ownerDecl{label, key}]; ok {
		return nil
	}
	if err := idx.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(ownerDeclKey(label, key), nil) }); err != nil {
		return fmt.Errorf("graph: declare owner index: %w", err)
	}
	if err := idx.sync(); err != nil {
		return err
	}
	idx.declared[ownerDecl{label, key}] = struct{}{}
	return nil
}

// DropUniqueOwnerIndex removes the declaration of (label, key) and its claims.
func (ts *Store) DropUniqueOwnerIndex(label uint16, key string) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	ts.ownerIdxMu.Lock()
	defer ts.ownerIdxMu.Unlock()
	idx := ts.ownerIdx
	if idx == nil {
		return nil
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if err := idx.db.Update(func(txn *badgerv4.Txn) error {
		if err := txn.Delete(ownerReadyKey(label, key)); err != nil {
			return err
		}
		return txn.Delete(ownerDeclKey(label, key))
	}); err != nil {
		return fmt.Errorf("graph: drop owner index: %w", err)
	}
	delete(idx.ready, ownerDecl{label, key})
	delete(idx.declared, ownerDecl{label, key})
	if err := idx.db.DropPrefix(ownerPrefix(label, key)); err != nil {
		return fmt.Errorf("graph: drop owner index claims: %w", err)
	}
	return nil
}

// UniqueOwnerIndexDeclared reports whether (label, key) has an owner index.
func (ts *Store) UniqueOwnerIndexDeclared(label uint16, key string) bool {
	ts.ownerIdxMu.Lock()
	idx := ts.ownerIdx
	ts.ownerIdxMu.Unlock()
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	_, ok := idx.declared[ownerDecl{label, key}]
	return ok
}

// PutUniqueOwner records id as the claimant of valueKey under (label, key),
// replacing an earlier claim; durable as the store's writes are (SyncWrites).
// The caller writes it before the row it backs.
func (ts *Store) PutUniqueOwner(label uint16, key, valueKey string, id types.NodeID) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	idx, err := ts.declaredOwnerIndex(label, key)
	if err != nil {
		return err
	}
	v := binary.BigEndian.AppendUint64(nil, uint64(id)) // #nosec G115 -- an ID's bits round-trip
	if err := idx.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(ownerKey(label, key, valueKey), v) }); err != nil {
		return fmt.Errorf("graph: claim owner: %w", err)
	}
	idx.dirty.Store(true)
	return nil
}

// SeedUniqueOwner records id as the claimant of valueKey under (label, key)
// only when no claim exists: the seeding of a new declaration from the rows,
// beside writers whose claims (PutUniqueOwner) are newer and win.
func (ts *Store) SeedUniqueOwner(label uint16, key, valueKey string, id types.NodeID) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	idx, err := ts.declaredOwnerIndex(label, key)
	if err != nil {
		return err
	}
	k := ownerKey(label, key, valueKey)
	v := binary.BigEndian.AppendUint64(nil, uint64(id)) // #nosec G115 -- an ID's bits round-trip
	err = idx.db.Update(func(txn *badgerv4.Txn) error {
		if _, err := txn.Get(k); err == nil {
			return nil
		} else if !errors.Is(err, badgerv4.ErrKeyNotFound) {
			return err
		}
		return txn.Set(k, v)
	})
	if errors.Is(err, badgerv4.ErrConflict) {
		return nil // a writer claimed it meanwhile: its claim is the newer one
	}
	if err != nil {
		return fmt.Errorf("graph: seed owner: %w", err)
	}
	idx.dirty.Store(true)
	return nil
}

// MarkUniqueOwnerIndexReady records that (label, key)'s index holds a claim
// for every row that existed at its declaration (writers claim from the
// declaration on), so lookups may answer from it.
func (ts *Store) MarkUniqueOwnerIndexReady(label uint16, key string) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	idx, err := ts.declaredOwnerIndex(label, key)
	if err != nil {
		return err
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if err := idx.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(ownerReadyKey(label, key), nil) }); err != nil {
		return fmt.Errorf("graph: mark owner index ready: %w", err)
	}
	if err := idx.sync(); err != nil {
		return err
	}
	idx.ready[ownerDecl{label, key}] = struct{}{}
	return nil
}

// UniqueOwnerIndexReady reports whether (label, key)'s index may answer
// lookups (MarkUniqueOwnerIndexReady).
func (ts *Store) UniqueOwnerIndexReady(label uint16, key string) bool {
	ts.ownerIdxMu.Lock()
	idx := ts.ownerIdx
	ts.ownerIdxMu.Unlock()
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	_, ok := idx.ready[ownerDecl{label, key}]
	return ok
}

// UniqueOwner returns the last claimant of valueKey under (label, key); the
// caller checks that its row holds the value.
func (ts *Store) UniqueOwner(label uint16, key, valueKey string) (types.NodeID, bool, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, false, err
	}
	idx, err := ts.declaredOwnerIndex(label, key)
	if err != nil {
		return 0, false, err
	}
	var id types.NodeID
	found := false
	err = idx.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(ownerKey(label, key, valueKey))
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			if len(v) != 8 {
				return fmt.Errorf("graph: owner index: malformed claim of %d bytes", len(v))
			}
			id, found = types.NodeID(binary.BigEndian.Uint64(v)), true // #nosec G115 -- an ID's bits round-trip
			return nil
		})
	})
	if err != nil {
		return 0, false, fmt.Errorf("graph: read owner: %w", err)
	}
	return id, found, nil
}

// declaredOwnerIndex is the open owner index when (label, key) is declared.
func (ts *Store) declaredOwnerIndex(label uint16, key string) (*ownerIndex, error) {
	ts.ownerIdxMu.Lock()
	idx := ts.ownerIdx
	ts.ownerIdxMu.Unlock()
	if idx != nil {
		idx.mu.RLock()
		_, ok := idx.declared[ownerDecl{label, key}]
		idx.mu.RUnlock()
		if ok {
			return idx, nil
		}
	}
	return nil, fmt.Errorf("%w: label %d key %q", ErrUniqueOwnerIndexNotDeclared, label, key)
}

// clearOwnerIndex removes every declaration and claim (Clear).
func (ts *Store) clearOwnerIndex() error {
	ts.ownerIdxMu.Lock()
	defer ts.ownerIdxMu.Unlock()
	idx := ts.ownerIdx
	if idx == nil {
		return nil
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if err := idx.db.DropPrefix([]byte{'U'}, []byte{'R'}, []byte{'D'}); err != nil {
		return fmt.Errorf("graph: clear owner index: %w", err)
	}
	idx.declared = make(map[ownerDecl]struct{})
	idx.ready = make(map[ownerDecl]struct{})
	return nil
}

// sync makes every write so far durable (a no-op in memory).
func (idx *ownerIndex) sync() error {
	idx.syncMu.Lock()
	defer idx.syncMu.Unlock()
	idx.dirty.Store(false)
	if err := idx.db.Sync(); err != nil {
		idx.dirty.Store(true)
		return fmt.Errorf("graph: sync owner index: %w", err)
	}
	return nil
}

// syncOwnerIndexBeforeRows is every shard's Config.BeforeFlushRows: before a
// flush whose rows become durable (SyncWrites, DurableFlush) it syncs the
// claims written so far, which include the claims of those rows (a claim is
// written before its row enters the flush's snapshot).
func (ts *Store) syncOwnerIndexBeforeRows(durable bool) error {
	if !durable {
		return nil
	}
	ts.ownerIdxMu.Lock()
	idx := ts.ownerIdx
	ts.ownerIdxMu.Unlock()
	if idx == nil {
		return nil
	}
	idx.syncMu.Lock()
	defer idx.syncMu.Unlock()
	if !idx.dirty.Swap(false) {
		return nil
	}
	if err := idx.db.Sync(); err != nil {
		idx.dirty.Store(true)
		return fmt.Errorf("graph: sync owner index: %w", err)
	}
	ts.ownerIdxSyncs.Add(1)
	return nil
}

// closeOwnerIndex closes the owner index (Close).
func (ts *Store) closeOwnerIndex() error {
	ts.ownerIdxMu.Lock()
	defer ts.ownerIdxMu.Unlock()
	if ts.ownerIdx == nil {
		return nil
	}
	err := ts.ownerIdx.db.Close()
	ts.ownerIdx = nil
	return err
}

var _ storecontract.UniqueOwnerIndexCapability = (*Store)(nil)
