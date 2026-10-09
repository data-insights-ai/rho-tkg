package badger

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

var _ storecontract.HistoryPresenceCapability = (*Store)(nil)

// historyPresence is one kind's RAM answer to "does this ID have a history
// row, and how high is its highest history version?"
// (store.HistoryPresenceCapability answers the first), equal at every moment to
// len(Get*History(id)) > 0 over the write buffer (pending + flushing) and the
// committed keys; the second is an upper bound used by the bulk as-of scans.
//
// State per ID: in has = at least one history row, with top, an upper bound of
// its highest history version (exact unless a SET raised it past a row that was
// later deleted; topUnbounded = no usable bound); in unknown = a history key of
// the ID was deleted since its state was last known (presence and top are
// resolved by one per-ID key probe on the next read); in neither = no history
// row (once built). Every history key passes noteHistoryKey under wbMu with its
// op and version, the single place history enters the write buffer: a SET into
// neither or has puts the ID in has and raises top to the SET's version; a SET
// into unknown keeps the ID unknown under a fresh stamp (rows it cannot see may
// sit above the SET's version); a DELETE moves the ID to unknown under a fresh
// stamp (other rows may remain). A probe installs its answer only if the ID
// still carries the stamp it read before the probe, so a write that lands
// during the probe is never overwritten (lessons 63 / 74, per-ID write
// generation).
//
// The set is built once, lazily: the build turns tracking on under wbMu, then
// scans the history keyspace for the highest version per ID (key-only: the
// version is in the key; pending and flushing writes included, overlay
// captured before the badger view), then installs the scanned IDs that no note
// touched since tracking began, raising the top of the ones a SET noted. A
// delete note always wins over the scan; a SET note is merged by max. Clear
// holds buildMu, so a scan never straddles a wipe, and drops the set with the
// write buffer; the next call rebuilds it.
//
// RAM: O(IDs with history), measured 34-56 B per ID (34 B at 10 K IDs, map
// growth; setB/id in BenchmarkRelHasHistory); not built until the first
// HasNodeHistory / HasRelHistory / bulk as-of call (HistoryPresenceStats
// reports it).
type historyPresence struct {
	buildMu sync.Mutex // serializes builds; Clear holds it for its whole run
	built   atomic.Bool

	// deletes counts the history-key deletes noted since the store opened (not
	// reset by Clear). A bulk as-of scan (bulkPresence) trusts the set's "no
	// history" only while it has not moved since the scan began.
	deletes atomic.Uint64

	mu       sync.RWMutex // guards has, unknown, stamp; taken inside wbMu by notes
	tracking bool         // written under wbMu AND mu; notes read it under wbMu
	has      map[snowflake.ID]uint32
	unknown  map[snowflake.ID]uint64
	stamp    uint64
}

// topUnbounded is the has value that carries no version bound (the highest
// uint32): a bulk reader reads the key. A history key whose version does not
// fit a uint32 maps to it too.
const topUnbounded = math.MaxUint32

func clampTop(version uint64) uint32 {
	if version >= topUnbounded {
		return topUnbounded
	}
	return uint32(version)
}

// note records one history-key op for id at version. Caller holds wbMu.
func (p *historyPresence) note(id snowflake.ID, version uint64, isDelete bool) {
	if !p.tracking {
		return
	}
	p.mu.Lock()
	switch {
	case isDelete:
		p.deletes.Add(1)
		delete(p.has, id)
		p.stamp++
		p.unknown[id] = p.stamp
	case len(p.unknown) > 0 && p.unknown[id] != 0:
		// Rows the delete left may sit above this version: stay unknown (the
		// next probe resolves presence and top), and move the stamp so a probe
		// in flight does not install a top that misses this row.
		p.stamp++
		p.unknown[id] = p.stamp
	default:
		if top, ok := p.has[id]; !ok || clampTop(version) > top {
			p.has[id] = clampTop(version)
		}
	}
	p.mu.Unlock()
}

// resetLocked drops the set (Clear): the next call rebuilds it from what the
// keyspace holds then. Clear resets before its drop, so a drop that fails
// leaves rows behind; an unbuilt set rebuilds over them instead of answering
// false for them forever. Calls during Clear block on buildMu, which Clear
// holds. Caller holds wbMu and buildMu.
func (p *historyPresence) resetLocked() {
	p.mu.Lock()
	p.built.Store(false)
	p.tracking = false
	p.has, p.unknown = nil, nil
	p.mu.Unlock()
}

// HistoryPresenceStats reports the RAM history-presence sets: whether each
// kind's set is built and how many IDs it holds (IDs with history plus IDs
// awaiting a probe after a history delete). The set costs 30-52 B per ID (measured).
type HistoryPresenceStats struct {
	NodesBuilt bool
	RelsBuilt  bool
	NodeIDs    int
	RelIDs     int
}

// HistoryPresenceStats returns the sizes of the RAM history-presence sets
// (zero on a nil or closed store).
func (bs *Store) HistoryPresenceStats() HistoryPresenceStats {
	if bs == nil || bs.isClosingOrClosed() {
		return HistoryPresenceStats{}
	}
	size := func(p *historyPresence) int {
		p.mu.RLock()
		defer p.mu.RUnlock()
		return len(p.has) + len(p.unknown)
	}
	return HistoryPresenceStats{
		NodesBuilt: bs.histNodePresence.built.Load(),
		RelsBuilt:  bs.histRelPresence.built.Load(),
		NodeIDs:    size(&bs.histNodePresence),
		RelIDs:     size(&bs.histRelPresence),
	}
}

// HasNodeHistory reports whether the node has at least one history row
// (store.HistoryPresenceCapability): len(GetNodeHistory(id)) > 0, pending and
// in-flight writes included, without reading a row. The first call builds the
// RAM set by one key-only scan of the node history keyspace; later calls are a
// map lookup. A store opened with Config.HistoryPresenceProbeOnly probes the
// ID's keys instead. An ID whose rows exist but fail to decode reads true here
// while GetNodeHistory returns the decode error.
func (bs *Store) HasNodeHistory(id types.NodeID) (bool, error) {
	if err := bs.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return false, err
	}
	return bs.hasHistory(&bs.histNodePresence, storepkg.KeyHistNode, id.SnowflakeID())
}

// HasRelHistory is HasNodeHistory for relationships.
func (bs *Store) HasRelHistory(id types.RelID) (bool, error) {
	if err := bs.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return false, err
	}
	return bs.hasHistory(&bs.histRelPresence, storepkg.KeyHistRel, id.SnowflakeID())
}

func (bs *Store) hasHistory(p *historyPresence, kind byte, id snowflake.ID) (bool, error) {
	if bs.historyPresenceProbeOnly {
		present, _, err := bs.probeHistoryPresence(kind, id, false)
		return present, err
	}
	if !p.built.Load() {
		if err := bs.buildHistoryPresence(p, kind); err != nil {
			return false, err
		}
	}
	p.mu.RLock()
	_, has := p.has[id]
	var stamp uint64
	var unresolved bool
	if !has && len(p.unknown) > 0 {
		stamp, unresolved = p.unknown[id]
	}
	p.mu.RUnlock()
	if has {
		return true, nil
	}
	if !unresolved {
		return false, nil
	}
	present, top, err := bs.probeHistoryPresence(kind, id, true)
	if err != nil {
		return false, err
	}
	if bs.historyPresenceProbeHook != nil {
		bs.historyPresenceProbeHook()
	}
	p.mu.Lock()
	if cur, ok := p.unknown[id]; ok && cur == stamp {
		delete(p.unknown, id)
		if present {
			p.has[id] = top
		}
	}
	p.mu.Unlock()
	return present, nil
}

// ensureHistoryPresenceBuilt builds p when it is not built yet, unless the
// store is probe-only. The bulk as-of doors call it BEFORE taking idxMu: a
// build scans the whole history keyspace, and nothing is held here, so the lock
// order flushMu -> idxMu -> buildMu -> wbMu is not touched.
func (bs *Store) ensureHistoryPresenceBuilt(p *historyPresence, kind byte) error {
	if bs.historyPresenceProbeOnly || p.built.Load() {
		return nil
	}
	return bs.buildHistoryPresence(p, kind)
}

// bulkPresence is a bulk as-of scan's view of one kind's presence set: it
// bounds the highest history version of an ID at the scan's snapshot from RAM,
// so the scan skips the key read at version current+1 when the bound is below
// it (no history at all is the bound -1).
//
// The set is live; the scan reads an older snapshot (the overlay captured once,
// then the shared badger transaction). A live bound holds for the snapshot as
// long as no history key was deleted since: without deletes the set only grows
// and tops only rise. bound therefore answers only if
//   - the set was built when the scan began (beginBulkPresence, under the
//     scan's idxMu.RLock; Clear resets the set under idxMu.Lock, so that
//     cannot change during the hold) and the store is not probe-only;
//   - the ID is not unknown (a delete removed some rows; presence and top are
//     unresolved) and its top is not topUnbounded;
//   - the delete counter still equals its value from before the overlay
//     capture (a delete noted earlier is in the overlay or already committed,
//     so the snapshot reflects it; a later one may have removed rows the
//     snapshot still holds, and a probe may since have resolved a LOWER top -
//     the trim doors take no idxMu).
//
// Every other case falls back to the snapshot key read (lessons 63, 64, 74).
// The zero value never answers.
type bulkPresence struct {
	p       *historyPresence
	deletes uint64
}

// beginBulkPresence captures the scan-start state. Caller holds idxMu.RLock and
// calls it BEFORE snapshotHistoryOverlay, so every delete the snapshot may not
// contain moves the counter past the value read here.
func (bs *Store) beginBulkPresence(p *historyPresence) bulkPresence {
	if bs.historyPresenceProbeOnly || !p.built.Load() {
		return bulkPresence{}
	}
	return bulkPresence{p: p, deletes: p.deletes.Load()}
}

// bound returns the highest history version an ID can hold at the scan's
// snapshot (-1: no history row), or ok == false when the set cannot tell.
func (b *bulkPresence) bound(id snowflake.ID) (limit int64, ok bool) {
	if b.p == nil {
		return 0, false
	}
	b.p.mu.RLock()
	top, has := b.p.has[id]
	unresolved := false
	if !has && len(b.p.unknown) > 0 {
		_, unresolved = b.p.unknown[id]
	}
	b.p.mu.RUnlock()
	switch {
	case unresolved || b.p.deletes.Load() != b.deletes:
		return 0, false
	case !has:
		return -1, true
	case top == topUnbounded:
		return 0, false
	}
	return int64(top), true
}

// buildHistoryPresence builds p once (see historyPresence).
func (bs *Store) buildHistoryPresence(p *historyPresence, kind byte) error {
	p.buildMu.Lock()
	defer p.buildMu.Unlock()
	if p.built.Load() {
		return nil
	}
	bs.wbMu.Lock()
	p.mu.Lock()
	if !p.tracking {
		p.has = make(map[snowflake.ID]uint32)
		p.unknown = make(map[snowflake.ID]uint64)
		p.tracking = true
	}
	p.mu.Unlock()
	bs.wbMu.Unlock()

	tops, err := bs.scanHistoryTops(kind)
	if err != nil {
		return fmt.Errorf("graph: build history presence: %w", err)
	}
	if bs.historyPresenceBuildHook != nil {
		bs.historyPresenceBuildHook()
	}

	p.mu.Lock()
	for _, e := range tops {
		if _, noted := p.unknown[e.id]; noted {
			continue
		}
		if cur, noted := p.has[e.id]; noted {
			if e.top > cur {
				p.has[e.id] = e.top
			}
			continue
		}
		p.has[e.id] = e.top
	}
	p.built.Store(true)
	p.mu.Unlock()
	return nil
}

// idTop is one scanned ID with its highest history version (clamped).
type idTop struct {
	id  snowflake.ID
	top uint32
}

// scanHistoryTops returns every ID with a history row and its highest version:
// the buffered writes (overlay captured BEFORE the badger view, lesson 64) and
// the committed keys, key-only. A pending delete masks its committed key.
// Single-row IDs cost one Next; for the others a reverse seek to the end of the
// ID's key range finds the top, and a seek to the next ID skips the rest.
func (bs *Store) scanHistoryTops(kind byte) ([]idTop, error) {
	sets := make(map[string]struct{})
	deletes := make(map[string]struct{})
	bs.rangePending(func(k string, op writeOp) {
		if len(k) != storepkg.SizeHistKey || k[0] != kind {
			return
		}
		if op.opType == writeOpDelete {
			deletes[k] = struct{}{}
			delete(sets, k)
			return
		}
		sets[k] = struct{}{}
		delete(deletes, k)
	})
	pending := make(map[snowflake.ID]uint32, len(sets))
	for k := range sets {
		id := storepkg.ParseIDFromKey([]byte(k), 1)
		if top := clampTop(historyVersionFromKey([]byte(k))); top >= pending[id] {
			pending[id] = top
		}
	}

	var out []idTop
	prefix := []byte{kind}
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		fopts := badgerv4.DefaultIteratorOptions
		fopts.PrefetchValues = false
		fit := txn.NewIterator(fopts)
		defer fit.Close()
		ropts := fopts
		ropts.Reverse = true
		rit := txn.NewIterator(ropts)
		defer rit.Close()

		idPrefix := make([]byte, 9)
		end := make([]byte, storepkg.SizeHistKey)
		for fit.Seek(prefix); fit.ValidForPrefix(prefix); {
			key := fit.Item().Key()
			if len(key) != storepkg.SizeHistKey {
				fit.Next()
				continue
			}
			id := storepkg.ParseIDFromKey(key, 1)
			first := historyVersionFromKey(key)
			_, firstDeleted := deletes[string(key)]
			copy(idPrefix, key[:9])
			fit.Next()
			var top uint64
			found := false
			if fit.ValidForPrefix(prefix) && len(fit.Item().Key()) == storepkg.SizeHistKey && bytes.Equal(fit.Item().Key()[:9], idPrefix) {
				top, found = committedHistoryTop(rit, idPrefix, end, deletes)
				next := historyIDSeekKey(kind, id)
				if next == nil {
					if found {
						out = append(out, idTop{id: id, top: clampTop(top)})
					}
					break
				}
				fit.Seek(next)
			} else {
				top, found = first, !firstDeleted
			}
			if found {
				out = append(out, idTop{id: id, top: clampTop(top)})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for i := range out {
		if pt, ok := pending[out[i].id]; ok {
			if pt > out[i].top {
				out[i].top = pt
			}
			delete(pending, out[i].id)
		}
	}
	for id, top := range pending {
		out = append(out, idTop{id: id, top: top})
	}
	return out, nil
}

// committedHistoryTop returns the highest committed history version under
// idPrefix (a 9-byte kind+ID prefix) that deletes does not mask, by a reverse
// seek to the largest possible key of the ID. rit is a Reverse iterator; end is
// a scratch key buffer.
func committedHistoryTop(rit *badgerv4.Iterator, idPrefix, end []byte, deletes map[string]struct{}) (uint64, bool) {
	copy(end, idPrefix)
	for i := len(idPrefix); i < len(end); i++ {
		end[i] = 0xFF
	}
	for rit.Seek(end); rit.ValidForPrefix(idPrefix); rit.Next() {
		k := rit.Item().Key()
		if len(k) != storepkg.SizeHistKey {
			continue
		}
		if _, deleted := deletes[string(k)]; deleted {
			continue
		}
		return historyVersionFromKey(k), true
	}
	return 0, false
}

// probeHistoryPresence answers for one ID from its keys: the write-buffer
// overlay (captured BEFORE the badger view, lesson 64) then a key-only,
// prefix-bounded seek. A buffered delete masks its committed key. With wantTop
// it also returns the highest version (clamped) of the surviving rows - a
// reverse seek instead of the first-key early exit.
func (bs *Store) probeHistoryPresence(kind byte, id snowflake.ID, wantTop bool) (bool, uint32, error) {
	prefix := storepkg.HistRelPrefix(id)
	if kind == storepkg.KeyHistNode {
		prefix = storepkg.HistNodePrefix(id)
	}
	overlay, deletes := bs.pendingHistoryVersionOverlay(prefix, 0)
	var top uint64
	found := false
	for k := range overlay {
		if v := historyVersionFromKey([]byte(k)); !found || v > top {
			top, found = v, true
		}
		if !wantTop {
			return true, 0, nil
		}
	}
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		if wantTop {
			opts.Reverse = true
			it := txn.NewIterator(opts)
			defer it.Close()
			if v, ok := committedHistoryTop(it, prefix, make([]byte, storepkg.SizeHistKey), deletes); ok && (!found || v > top) {
				top, found = v, true
			}
			return nil
		}
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			if _, deleted := deletes[string(key)]; deleted {
				continue
			}
			found = true
			return nil
		}
		return nil
	})
	if err != nil {
		return false, 0, fmt.Errorf("graph: probe history presence: %w", err)
	}
	return found, clampTop(top), nil
}
