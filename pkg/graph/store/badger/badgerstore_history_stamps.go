package badger

import (
	"fmt"
	"sync"
	"sync/atomic"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

var _ storecontract.HistoryStampsCapability = (*Store)(nil)

// historyStamps is one kind's RAM answer to "what are the newest transaction
// stamps over this ID's history rows" (store.HistoryStampsCapability, backlog
// 30): per ID the largest TxFrom and the largest TxTo or DeletedAt
// (store.FoldTxStamps) over the rows Get*History(id) returns at that moment,
// over the write buffer (pending + flushing) and the committed rows. It is the
// history presence set's protocol (badgerstore_history_presence.go) with
// values: the stamps live in the row values, so the build reads every history
// value (the temporal fields only: storeutil.HistoryValueTxStamps, no full
// decode) and every maintained SET decodes its own value.
//
// State per ID: in has = at least one history row, with the exact fold and
// top, the highest history version (exact; a SET above it is a new key); in
// unknown = a history key of the ID was deleted, or overwritten with stamps
// below the fold, or written while the build ran, since its state was last
// known (one per-ID read of the ID's rows resolves it on the next call); in
// neither = no history row (once built). Every history key passes
// noteHistoryKey under wbMu with its op, the single place history enters the
// write buffer:
//   - a DELETE moves the ID to unknown (a running max cannot be lowered,
//     lesson 57);
//   - a SET into neither starts the ID with the row's stamps;
//   - a SET into has above top, or one whose stamps reach the fold on both
//     halves, folds in (exact: a new key, or the new row is the max whatever
//     it replaced); any other SET may have replaced the row holding the max,
//     so the ID moves to unknown;
//   - a SET into unknown keeps it unknown under a fresh stamp;
//   - while the build runs every note moves the ID to unknown (the scan may or
//     may not have seen the row; overwrites make "merge by max" wrong).
//
// A probe installs its answer only if the ID still carries the stamp it read
// before the probe (per-ID write generation, lessons 63 / 74). The build turns
// tracking on under wbMu, scans (overlay captured before the badger view,
// lesson 64), and installs the scanned IDs no note touched since. Clear holds
// buildMu and drops the sidecar with the write buffer; the next call rebuilds.
// A store opened with Config.HistoryPresenceProbeOnly (tiered cold shards)
// never builds and reads per ID.
//
// RAM: O(IDs with history), one 24-byte entry per ID (see
// HistoryStampsStats; measured in BenchmarkLatestStamps).
type historyStamps struct {
	buildMu sync.Mutex // serializes builds; Clear holds it for its whole run
	built   atomic.Bool

	mu       sync.RWMutex // guards has, unknown, stamp; taken inside wbMu by notes
	tracking bool         // written under wbMu AND mu; notes read it under wbMu
	has      map[snowflake.ID]stampEntry
	unknown  map[snowflake.ID]uint64
	stamp    uint64
}

// stampEntry is one ID's fold over its history rows: the largest TxFrom, the
// largest TxTo or DeletedAt, and the highest history version (clamped).
type stampEntry struct {
	from, to types.Instant
	top      uint32
}

// note records one history-key op for id at version. node selects the delta
// decoder. Caller holds wbMu.
func (p *historyStamps) note(id snowflake.ID, version uint64, op writeOp, node bool) {
	if !p.tracking {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if op.opType == writeOpDelete || !p.built.Load() {
		p.markUnknownLocked(id)
		return
	}
	f, t, d, err := storepkg.HistoryValueTxStamps(op.value, node)
	if err != nil {
		p.markUnknownLocked(id) // the per-ID read reports the decode error
		return
	}
	if _, unresolved := p.unknown[id]; unresolved {
		p.markUnknownLocked(id)
		return
	}
	top := clampTop(version)
	e, ok := p.has[id]
	if !ok {
		from, to := storecontract.FoldTxStamps(0, 0, f, t, d)
		p.has[id] = stampEntry{from: from, to: to, top: top}
		return
	}
	if top > e.top || (f >= e.from && max(t, d) >= e.to) {
		e.from, e.to = storecontract.FoldTxStamps(e.from, e.to, f, t, d)
		e.top = max(e.top, top)
		p.has[id] = e
		return
	}
	p.markUnknownLocked(id)
}

// markUnknownLocked moves id to unknown under a fresh stamp. Caller holds mu.
func (p *historyStamps) markUnknownLocked(id snowflake.ID) {
	delete(p.has, id)
	p.stamp++
	p.unknown[id] = p.stamp
}

// resetLocked drops the sidecar (Clear); the next call rebuilds it. Caller
// holds wbMu and buildMu.
func (p *historyStamps) resetLocked() {
	p.mu.Lock()
	p.built.Store(false)
	p.tracking = false
	p.has, p.unknown = nil, nil
	p.mu.Unlock()
}

// HistoryStampsStats reports the RAM history-stamps sidecars: whether each
// kind's is built and how many IDs it holds (IDs with history plus IDs
// awaiting a per-ID read).
type HistoryStampsStats struct {
	NodesBuilt bool
	RelsBuilt  bool
	NodeIDs    int
	RelIDs     int
}

// HistoryStampsStats returns the sizes of the history-stamps sidecars (zero on
// a nil or closed store).
func (bs *Store) HistoryStampsStats() HistoryStampsStats {
	if bs == nil || bs.isClosingOrClosed() {
		return HistoryStampsStats{}
	}
	size := func(p *historyStamps) int {
		p.mu.RLock()
		defer p.mu.RUnlock()
		return len(p.has) + len(p.unknown)
	}
	return HistoryStampsStats{
		NodesBuilt: bs.histNodeStamps.built.Load(),
		RelsBuilt:  bs.histRelStamps.built.Load(),
		NodeIDs:    size(&bs.histNodeStamps),
		RelIDs:     size(&bs.histRelStamps),
	}
}

// NodeHistoryStamps returns the newest TxFrom and TxTo-or-DeletedAt over the
// node's history rows and whether it has any (store.HistoryStampsCapability),
// pending and in-flight writes included, without reading a row. The first call
// builds the RAM sidecar by one scan of the node history values (temporal
// fields only); later calls are a map lookup, or one read of the ID's rows
// after a delete or a lowering overwrite. Only each row's temporal block is
// decoded: a row whose temporal block fails to decode reports the error; a row
// whose body is corrupt but whose temporal block decodes is answered, while
// GetNodeHistory reports that row's decode error (HasNodeHistory has the same
// asymmetry).
func (bs *Store) NodeHistoryStamps(id types.NodeID) (types.Instant, types.Instant, bool, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, 0, false, err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return 0, 0, false, err
	}
	return bs.historyStampsOf(&bs.histNodeStamps, storepkg.KeyHistNode, id.SnowflakeID())
}

// RelHistoryStamps is NodeHistoryStamps for relationships.
func (bs *Store) RelHistoryStamps(id types.RelID) (types.Instant, types.Instant, bool, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, 0, false, err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return 0, 0, false, err
	}
	return bs.historyStampsOf(&bs.histRelStamps, storepkg.KeyHistRel, id.SnowflakeID())
}

func (bs *Store) historyStampsOf(p *historyStamps, kind byte, id snowflake.ID) (types.Instant, types.Instant, bool, error) {
	if bs.historyPresenceProbeOnly {
		e, present, err := bs.probeHistoryStamps(kind, id)
		return e.from, e.to, present, err
	}
	if !p.built.Load() {
		if err := bs.buildHistoryStamps(p, kind); err != nil {
			return 0, 0, false, err
		}
	}
	p.mu.RLock()
	e, has := p.has[id]
	var stamp uint64
	var unresolved bool
	if !has && len(p.unknown) > 0 {
		stamp, unresolved = p.unknown[id]
	}
	p.mu.RUnlock()
	if has {
		return e.from, e.to, true, nil
	}
	if !unresolved {
		return 0, 0, false, nil
	}
	e, present, err := bs.probeHistoryStamps(kind, id)
	if err != nil {
		return 0, 0, false, err
	}
	if bs.historyStampsProbeHook != nil {
		bs.historyStampsProbeHook()
	}
	p.mu.Lock()
	if cur, ok := p.unknown[id]; ok && cur == stamp {
		delete(p.unknown, id)
		if present {
			p.has[id] = e
		}
	}
	p.mu.Unlock()
	return e.from, e.to, present, nil
}

// buildHistoryStamps builds p once (see historyStamps).
func (bs *Store) buildHistoryStamps(p *historyStamps, kind byte) error {
	p.buildMu.Lock()
	defer p.buildMu.Unlock()
	if p.built.Load() {
		return nil
	}
	bs.wbMu.Lock()
	p.mu.Lock()
	if !p.tracking {
		p.has = make(map[snowflake.ID]stampEntry)
		p.unknown = make(map[snowflake.ID]uint64)
		p.tracking = true
	}
	p.mu.Unlock()
	bs.wbMu.Unlock()

	scanned, err := bs.scanHistoryStamps(kind)
	if err != nil {
		return fmt.Errorf("graph: build history stamps: %w", err)
	}
	if bs.historyStampsBuildHook != nil {
		bs.historyStampsBuildHook()
	}

	p.mu.Lock()
	for id, e := range scanned {
		if _, noted := p.unknown[id]; noted {
			continue
		}
		p.has[id] = e
	}
	p.built.Store(true)
	p.mu.Unlock()
	return nil
}

// scanHistoryStamps folds every history row of kind per ID: the buffered
// writes (overlay captured BEFORE the badger view, lesson 64; a buffered SET
// replaces and a buffered DELETE masks its committed key) and the committed
// rows, reading each value's temporal fields only.
func (bs *Store) scanHistoryStamps(kind byte) (map[snowflake.ID]stampEntry, error) {
	node := kind == storepkg.KeyHistNode
	sets := make(map[string][]byte)
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
		sets[k] = op.value
		delete(deletes, k)
	})
	if bs.historyScanTestHook != nil {
		bs.historyScanTestHook() // commit-window tests land a flush here
	}
	out := make(map[snowflake.ID]stampEntry)
	fold := func(key, raw []byte) error {
		f, t, d, err := storepkg.HistoryValueTxStamps(raw, node)
		if err != nil {
			return err
		}
		id := storepkg.ParseIDFromKey(key, 1)
		e := out[id]
		e.from, e.to = storecontract.FoldTxStamps(e.from, e.to, f, t, d)
		e.top = max(e.top, clampTop(historyVersionFromKey(key)))
		out[id] = e
		return nil
	}
	prefix := []byte{kind}
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false // decode inside Value(), no staging copy
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			if _, masked := deletes[string(key)]; masked {
				continue
			}
			if _, buffered := sets[string(key)]; buffered {
				continue
			}
			if err := item.Value(func(val []byte) error { return fold(key, val) }); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for k, raw := range sets {
		if err := fold([]byte(k), raw); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// probeHistoryStamps folds one ID's history rows (the temporal-meta reader:
// overlay before the badger view) and reports whether it has any.
func (bs *Store) probeHistoryStamps(kind byte, id snowflake.ID) (stampEntry, bool, error) {
	node := kind == storepkg.KeyHistNode
	prefix := storepkg.HistRelPrefix(id)
	if node {
		prefix = storepkg.HistNodePrefix(id)
	}
	metas, err := bs.historyTemporalMetaByPrefix(prefix, node)
	if err != nil {
		return stampEntry{}, false, fmt.Errorf("graph: probe history stamps: %w", err)
	}
	var e stampEntry
	for _, m := range metas {
		if m.Temporal != nil {
			e.from, e.to = storecontract.FoldTxStamps(e.from, e.to, m.Temporal.TxFrom, m.Temporal.TxTo, m.Temporal.DeletedAt)
		}
		e.top = max(e.top, m.Version)
	}
	return e, len(metas) > 0, nil
}
