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

var _ storecontract.HistoryPresenceCapability = (*Store)(nil)

// historyPresence is one kind's RAM answer to "does this ID have a history
// row?" (store.HistoryPresenceCapability), equal at every moment to
// len(Get*History(id)) > 0 over the write buffer (pending + flushing) and the
// committed keys.
//
// State per ID: in has = at least one history row; in unknown = a history key
// of the ID was deleted since its state was last known (resolved by a per-ID
// key probe on the next read); in neither = no history row (once built).
// Every history key passes noteHistoryKey under wbMu with its op, the single
// place history enters the write buffer: a SET puts the ID in has (the key
// exists), a DELETE moves it to unknown under a fresh stamp (other rows may
// remain). A probe installs its answer only if the ID still carries the stamp
// it read before the probe, so a write that lands during the probe is never
// overwritten (lessons 63 / 74, per-ID write generation).
//
// The set is built once, lazily: the build turns tracking on under wbMu, then
// scans the history IDs (ForEach*HistoryID: key-only, pending and flushing
// included, overlay captured before each badger view), then installs the
// scanned IDs that no note touched since tracking began. A note always wins
// over the scan: it is newer than everything the scan saw for that ID, and an
// ID without a note did not change while the scan ran. Clear holds buildMu, so
// a scan never straddles a wipe, and drops the set with the write buffer; the
// next call rebuilds it.
//
// RAM: O(IDs with history), about 25-40 B per ID in Go maps (map growth); not built until the
// first HasNodeHistory / HasRelHistory call (HistoryPresenceStats reports it).
type historyPresence struct {
	buildMu sync.Mutex // serializes builds; Clear holds it for its whole run
	built   atomic.Bool

	mu       sync.RWMutex // guards has, unknown, stamp; taken inside wbMu by notes
	tracking bool         // written under wbMu AND mu; notes read it under wbMu
	has      map[snowflake.ID]struct{}
	unknown  map[snowflake.ID]uint64
	stamp    uint64
}

// note records one history-key op for id. Caller holds wbMu.
func (p *historyPresence) note(id snowflake.ID, isDelete bool) {
	if !p.tracking {
		return
	}
	p.mu.Lock()
	if isDelete {
		delete(p.has, id)
		p.stamp++
		p.unknown[id] = p.stamp
	} else {
		p.has[id] = struct{}{}
		if len(p.unknown) > 0 {
			delete(p.unknown, id)
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
// awaiting a probe after a history delete). The set costs about 25-40 B per ID.
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
		return bs.probeHistoryPresence(kind, id)
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
	present, err := bs.probeHistoryPresence(kind, id)
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
			p.has[id] = struct{}{}
		}
	}
	p.mu.Unlock()
	return present, nil
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
		p.has = make(map[snowflake.ID]struct{})
		p.unknown = make(map[snowflake.ID]uint64)
		p.tracking = true
	}
	p.mu.Unlock()
	bs.wbMu.Unlock()

	var ids []snowflake.ID
	var err error
	if kind == storepkg.KeyHistNode {
		err = bs.ForEachNodeHistoryID(func(id types.NodeID) bool {
			ids = append(ids, id.SnowflakeID())
			return true
		})
	} else {
		err = bs.ForEachRelHistoryID(func(id types.RelID) bool {
			ids = append(ids, id.SnowflakeID())
			return true
		})
	}
	if err != nil {
		return fmt.Errorf("graph: build history presence: %w", err)
	}
	if bs.historyPresenceBuildHook != nil {
		bs.historyPresenceBuildHook()
	}

	p.mu.Lock()
	for _, id := range ids {
		if _, noted := p.has[id]; noted {
			continue
		}
		if _, noted := p.unknown[id]; noted {
			continue
		}
		p.has[id] = struct{}{}
	}
	p.built.Store(true)
	p.mu.Unlock()
	return nil
}

// probeHistoryPresence answers for one ID from its keys: the write-buffer
// overlay (captured BEFORE the badger view, lesson 64) then a key-only,
// prefix-bounded seek. A buffered delete masks its committed key.
func (bs *Store) probeHistoryPresence(kind byte, id snowflake.ID) (bool, error) {
	prefix := storepkg.HistRelPrefix(id)
	if kind == storepkg.KeyHistNode {
		prefix = storepkg.HistNodePrefix(id)
	}
	overlay, deletes := bs.pendingHistoryVersionOverlay(prefix, 0)
	if len(overlay) > 0 {
		return true, nil
	}
	found := false
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
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
		return false, fmt.Errorf("graph: probe history presence: %w", err)
	}
	return found, nil
}
