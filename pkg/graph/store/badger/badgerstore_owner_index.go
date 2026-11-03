package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// The owner index (UNIQUELOOK; the tiered store's is owner_index.go there):
// for a declared unique (label, key), the ID that last claimed each value,
// under KeyUniqueOwner (storeutil/unique_owner_key.go). The graph claims a
// value under its value lock before the row holding it is written, and every
// reader checks the claimant's row, so a claim the row does not back (a
// crash between the two, a rollback, a later change, a delete) is ignored
// and no holder is ever missing a claim.
//
// Claims, declarations and the ready mark are buffered writes (appendOps),
// like the rows: a claim enters the buffer before its row, a flush commits
// the buffer and the row cache's dirty rows from one snapshot taken while no
// writer is between them (idxMu), and a flush is all-or-nothing (FLUSHPART).
// So a claim is never less durable than its row, with or without SyncWrites,
// and needs no sync of its own (the tiered index lives in its own badger and
// syncs before each durable row flush). The first declaration raises the
// store format marker to 16 in its wbMu section.

var errUniqueOwnerIndexNotDeclared = errors.New("graph: no owner index is declared for this label and key")

// ownerDropWalkHook (tests) runs in DropUniqueOwnerIndex after owner.mu is
// released and before the claim walk.
var ownerDropWalkHook func()

type ownerDecl struct {
	label uint16
	key   string
}

type ownerIndexState struct {
	// ddlMu orders Declare and Drop: a drop's claim walk runs under it, not
	// under mu, so lookups and claims of other indexes never wait on the
	// walk, and a re-declaration cannot start while the walk could still
	// delete the new index's claims.
	ddlMu    sync.Mutex
	mu       sync.RWMutex // guards declared, ready
	declared map[ownerDecl]struct{}
	ready    map[ownerDecl]struct{}
	// claimMu orders a seed's read-then-write against claims.
	claimMu sync.Mutex
	// failNextClaim (tests) fails the next PutUniqueOwner once.
	failNextClaim error
}

// loadUniqueOwnerMarks loads the declarations and ready marks at open.
func (bs *Store) loadUniqueOwnerMarks() error {
	declared := make(map[ownerDecl]struct{})
	ready := make(map[ownerDecl]struct{})
	err := bs.view(func(txn *badgerv4.Txn) error {
		for tag, into := range map[byte]map[ownerDecl]struct{}{'D': declared, 'R': ready} {
			opts := badgerv4.DefaultIteratorOptions
			opts.PrefetchValues = false
			opts.Prefix = []byte{storepkg.KeyUniqueOwner, tag}
			it := txn.NewIterator(opts)
			for it.Rewind(); it.Valid(); it.Next() {
				k := it.Item().Key()
				if len(k) < 4 {
					it.Close()
					return fmt.Errorf("graph: owner index: %w: mark key %x", storecontract.ErrCorruptWire, k)
				}
				into[ownerDecl{binary.BigEndian.Uint16(k[2:4]), string(k[4:])}] = struct{}{}
			}
			it.Close()
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("graph: load owner index: %w", err)
	}
	for d := range ready {
		if _, ok := declared[d]; !ok {
			delete(ready, d) // a ready mark without its declaration trusts nothing
		}
	}
	bs.owner.mu.Lock()
	bs.owner.declared, bs.owner.ready = declared, ready
	bs.owner.mu.Unlock()
	return nil
}

// DeclareUniqueOwnerIndex declares the owner index of (label, key);
// idempotent. Claims follow with PutUniqueOwner.
func (bs *Store) DeclareUniqueOwnerIndex(label uint16, key string) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	bs.owner.ddlMu.Lock()
	defer bs.owner.ddlMu.Unlock()
	bs.owner.mu.Lock()
	defer bs.owner.mu.Unlock()
	d := ownerDecl{label, key}
	if _, ok := bs.owner.declared[d]; ok {
		return nil
	}
	bs.appendOpsRaising(storepkg.UniqueOwnerStoreFormatVersion,
		writeOp{opType: writeOpSet, key: storepkg.UniqueOwnerMarkKey('D', label, key), value: []byte{}})
	if bs.owner.declared == nil {
		bs.owner.declared = make(map[ownerDecl]struct{})
	}
	bs.owner.declared[d] = struct{}{}
	return nil
}

// DropUniqueOwnerIndex removes the declaration of (label, key) and its claims.
func (bs *Store) DropUniqueOwnerIndex(label uint16, key string) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	bs.owner.ddlMu.Lock()
	defer bs.owner.ddlMu.Unlock()
	bs.owner.mu.Lock()
	d := ownerDecl{label, key}
	if _, ok := bs.owner.declared[d]; !ok {
		bs.owner.mu.Unlock()
		return nil
	}
	// The marks go first, in the buffer: once they are gone nothing trusts
	// a claim, so the claims' deletion may land later or be cut by a crash.
	bs.appendOps(
		writeOp{opType: writeOpDelete, key: storepkg.UniqueOwnerMarkKey('R', label, key)},
		writeOp{opType: writeOpDelete, key: storepkg.UniqueOwnerMarkKey('D', label, key)},
	)
	delete(bs.owner.ready, d)
	delete(bs.owner.declared, d)
	// mu is released before the walk (the review's F6): the index is
	// undeclared from here, so PutUniqueOwner refuses its claims and no
	// reader trusts one, and ddlMu keeps a re-declaration out until the
	// walk ends.
	bs.owner.mu.Unlock()
	if hook := ownerDropWalkHook; hook != nil {
		hook()
	}
	// The claims are deleted through the buffer in chunks, not with
	// DropPrefix, which blocks every write of the store (docs/badger.md
	// section 2). A claim queued after the walk is a stray that a
	// re-declaration's seed checks against its row.
	prefix := storepkg.UniqueOwnerClaimPrefix(label, key)
	const chunk = 10_000
	var ops []writeOp
	bs.rangePendingClaims(prefix, func(k string) {
		ops = append(ops, writeOp{opType: writeOpDelete, key: []byte(k)})
	})
	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			ops = append(ops, writeOp{opType: writeOpDelete, key: it.Item().KeyCopy(nil)})
			if len(ops) >= chunk {
				bs.appendOps(ops...)
				ops = nil
				if err := bs.flush(); err != nil { // the buffer stays bounded
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("graph: drop owner index claims: %w", err)
	}
	if len(ops) > 0 {
		bs.appendOps(ops...)
	}
	return nil
}

// rangePendingClaims calls fn for every buffered key under prefix.
func (bs *Store) rangePendingClaims(prefix []byte, fn func(k string)) {
	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()
	p := string(prefix)
	for k := range bs.pending {
		if len(k) >= len(p) && k[:len(p)] == p {
			fn(k)
		}
	}
}

// UniqueOwnerIndexDeclared reports whether (label, key) has an owner index.
func (bs *Store) UniqueOwnerIndexDeclared(label uint16, key string) bool {
	bs.owner.mu.RLock()
	defer bs.owner.mu.RUnlock()
	_, ok := bs.owner.declared[ownerDecl{label, key}]
	return ok
}

// MarkUniqueOwnerIndexReady records that (label, key)'s index holds a claim
// for every row that existed at its declaration; buffered after the seed's
// claims, so it is never durable before them.
func (bs *Store) MarkUniqueOwnerIndexReady(label uint16, key string) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	bs.owner.mu.Lock()
	defer bs.owner.mu.Unlock()
	d := ownerDecl{label, key}
	if _, ok := bs.owner.declared[d]; !ok {
		return fmt.Errorf("%w: label %d key %q", errUniqueOwnerIndexNotDeclared, label, key)
	}
	bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.UniqueOwnerMarkKey('R', label, key), value: []byte{}})
	if bs.owner.ready == nil {
		bs.owner.ready = make(map[ownerDecl]struct{})
	}
	bs.owner.ready[d] = struct{}{}
	return nil
}

// UniqueOwnerIndexReady reports whether (label, key)'s index may answer
// lookups.
func (bs *Store) UniqueOwnerIndexReady(label uint16, key string) bool {
	bs.owner.mu.RLock()
	defer bs.owner.mu.RUnlock()
	_, ok := bs.owner.ready[ownerDecl{label, key}]
	return ok
}

// PutUniqueOwner records id as the claimant of valueKey under (label, key),
// replacing an earlier claim. The caller writes it before the row it backs.
func (bs *Store) PutUniqueOwner(label uint16, key, valueKey string, id types.NodeID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if !bs.UniqueOwnerIndexDeclared(label, key) {
		return fmt.Errorf("%w: label %d key %q", errUniqueOwnerIndexNotDeclared, label, key)
	}
	bs.owner.claimMu.Lock()
	defer bs.owner.claimMu.Unlock()
	if err := bs.owner.failNextClaim; err != nil {
		bs.owner.failNextClaim = nil
		return err
	}
	bs.appendOps(claimOp(label, key, valueKey, id))
	return nil
}

// SeedUniqueOwner records id as the claimant of valueKey under (label, key)
// only when no claim exists.
func (bs *Store) SeedUniqueOwner(label uint16, key, valueKey string, id types.NodeID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if !bs.UniqueOwnerIndexDeclared(label, key) {
		return fmt.Errorf("%w: label %d key %q", errUniqueOwnerIndexNotDeclared, label, key)
	}
	bs.owner.claimMu.Lock()
	defer bs.owner.claimMu.Unlock()
	_, found, err := bs.readClaim(storepkg.UniqueOwnerClaimKey(label, key, valueKey))
	if err != nil || found {
		return err
	}
	bs.appendOps(claimOp(label, key, valueKey, id))
	return nil
}

func claimOp(label uint16, key, valueKey string, id types.NodeID) writeOp {
	v := binary.BigEndian.AppendUint64(nil, uint64(id)) // #nosec G115 -- an ID's bits round-trip
	return writeOp{opType: writeOpSet, key: storepkg.UniqueOwnerClaimKey(label, key, valueKey), value: v}
}

// UniqueOwner returns the last claimant of valueKey under (label, key); the
// caller checks that its row holds the value.
func (bs *Store) UniqueOwner(label uint16, key, valueKey string) (types.NodeID, bool, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, false, err
	}
	if !bs.UniqueOwnerIndexDeclared(label, key) {
		return 0, false, fmt.Errorf("%w: label %d key %q", errUniqueOwnerIndexNotDeclared, label, key)
	}
	return bs.readClaim(storepkg.UniqueOwnerClaimKey(label, key, valueKey))
}

// readClaim reads a claim, the buffer before badger.
func (bs *Store) readClaim(k []byte) (types.NodeID, bool, error) {
	decode := func(v []byte) (types.NodeID, error) {
		if len(v) != 8 {
			return 0, fmt.Errorf("graph: owner index: %w: claim of %d bytes", storecontract.ErrCorruptWire, len(v))
		}
		return types.NodeID(binary.BigEndian.Uint64(v)), nil // #nosec G115 -- an ID's bits round-trip
	}
	if op, ok := bs.lookupPending(string(k)); ok {
		if op.opType == writeOpDelete {
			return 0, false, nil
		}
		id, err := decode(op.value)
		return id, err == nil, err
	}
	var id types.NodeID
	found := false
	err := bs.view(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(k)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			var derr error
			id, derr = decode(v)
			found = derr == nil
			return derr
		})
	})
	if err != nil {
		return 0, false, fmt.Errorf("graph: read owner: %w", err)
	}
	return id, found, nil
}

// resetUniqueOwner forgets every declaration (Clear, which drops the
// keyspace).
func (bs *Store) resetUniqueOwner() {
	bs.owner.mu.Lock()
	bs.owner.declared = make(map[ownerDecl]struct{})
	bs.owner.ready = make(map[ownerDecl]struct{})
	bs.owner.mu.Unlock()
}

var _ storecontract.UniqueOwnerIndexCapability = (*Store)(nil)
