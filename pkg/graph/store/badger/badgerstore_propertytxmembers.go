package badger

import (
	"cmp"
	"fmt"
	"sync"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RelPropertyTxMembershipCapability   = (*Store)(nil)
	_ storecontract.NodePropertyTxMembershipCapability  = (*Store)(nil)
	_ storecontract.PropertyTxMembershipStatsCapability = (*Store)(nil)
)

// Property membership sidecars (backlog 8, badger arm): one
// indexpkg.PropertyTxMembers per declared property index a temporal lookup asked
// for (value key -> rels/nodes whose rows ever carried it, with the lowest
// TxFrom). RAM-only append-only supersets; the chain resolver stays the authority.
//
// Every door writing a current row records via maintain*IndexesAdd under
// idxMu.Lock, every history row next to its belief-watermark bump. History rows:
// the demoted row of a *WithHistory door, a delete tombstone, and
// PutRel/NodeVersion via enqueueVersionAgainstLazyBuilds. A sidecar in
// relPropTx / nodePropTx is tracking; built marks it readable.
//
// Lazy build without blocking writers (lessons 63, 64, 74): under a per-sidecar
// build mutex the build turns tracking on under idxMu.Lock, reads the invalidation
// generation, releases idxMu, captures the write-buffer overlay, then scans
// committed current and history rows in one view. A row written before tracking
// was on is in the overlay or the view (its door appended its op inside the same
// idxMu section); a row written after is recorded by its door. A scan straddling an
// invalidation is discarded and redone. The merge is a union with
// per-member minimum, so scan and concurrent records commute. The result installs
// only if generation and sidecar are unchanged; Clear, index drop, retention purge
// and exact erasure drop the sidecars and move the generation.
type propTxSidecar[ID cmp.Ordered] struct {
	members *indexpkg.PropertyTxMembers[ID]
	built   bool
}

// propTxBuildAttempts bounds rescans of a build that keeps losing to an
// invalidation (lesson 24); then the lookup reports ErrIndexNotFound (full fold).
const propTxBuildAttempts = 4

// recordRelRowLocked records one relationship row a door writes (current or
// history) into every built membership sidecar (K1 rel-type set and property
// sets). Every row, not only creates: a row's TxFrom lowers the member's bound.
// Caller holds idxMu (write).
func (bs *Store) recordRelRowLocked(r *types.Relationship) {
	bs.recordRelTypeMemberLocked(r)
	bs.recordRelPropTxLocked(r)
}

// recordNodeRowLocked is recordRelRowLocked for node rows (K1 label set and
// node property sets). Caller holds idxMu (write).
func (bs *Store) recordNodeRowLocked(n *types.Node) {
	bs.recordNodeLabelMembersLocked(n)
	bs.recordNodePropTxLocked(n)
}

// recordRelPropTxLocked records r's indexable values into the tracking rel
// sidecars of its type. Caller holds idxMu (write).
func (bs *Store) recordRelPropTxLocked(r *types.Relationship) {
	if len(bs.relPropTx) == 0 || r == nil {
		return
	}
	tok := r.TypeToken().Value()
	id := r.ID()
	tx := relTxFrom(r)
	r.ForEachIndexKey(indexpkg.RelElementsOf(bs.relPropertyIndexes, tok), func(propertyKey, valueKey string) bool {
		if sc := bs.relPropTx[indexpkg.RelPropertyIndexKey{RelTypeToken: tok, PropertyKey: propertyKey}]; sc != nil {
			sc.members.Record(valueKey, id, tx)
		}
		return true
	})
}

// recordNodePropTxLocked records n's indexable values into the tracking node
// sidecars of every label the row carries. Caller holds idxMu (write).
func (bs *Store) recordNodePropTxLocked(n *types.Node) {
	if len(bs.nodePropTx) == 0 || n == nil {
		return
	}
	id := n.ID()
	tx := nodeTxFrom(n)
	for i := 0; i < n.LabelTokenCount(); i++ {
		label := n.LabelTokenRawAt(i)
		n.ForEachIndexKey(indexpkg.ElementsOf(bs.propertyIndexes, label), func(propertyKey, valueKey string) bool {
			if sc := bs.nodePropTx[indexpkg.PropertyIndexKey{LabelToken: label, PropertyKey: propertyKey}]; sc != nil {
				sc.members.Record(valueKey, id, tx)
			}
			return true
		})
	}
}

// dropPropertyTxMembersLocked discards every property sidecar and moves the
// generation so an in-flight build discards its scan. Caller holds idxMu
// (write).
func (bs *Store) dropPropertyTxMembersLocked() {
	bs.relPropTx = nil
	bs.nodePropTx = nil
	bs.propTxGen++
}

// relPropIndexUsableLocked reports whether a rel property index is declared
// for key and finished creating. Caller holds idxMu.
func (bs *Store) relPropIndexUsableLocked(key indexpkg.RelPropertyIndexKey) bool {
	idx, ok := bs.relPropertyIndexes[key]
	return ok && idx != nil && idx.Mutated == nil
}

func (bs *Store) nodePropIndexUsableLocked(key indexpkg.PropertyIndexKey) bool {
	idx, ok := bs.propertyIndexes[key]
	return ok && idx != nil && idx.Mutated == nil
}

// ForEachRelPropertyTxMember implements store.RelPropertyTxMembershipCapability.
func (bs *Store) ForEachRelPropertyTxMember(relTypeToken uint16, propertyKey, valueKey string, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	for attempt := 0; attempt < propTxBuildAttempts; attempt++ {
		bs.idxMu.RLock()
		if !bs.relPropIndexUsableLocked(key) {
			bs.idxMu.RUnlock()
			return ErrIndexNotFound
		}
		if sc := bs.relPropTx[key]; sc != nil && sc.built {
			members := sc.members.Members(valueKey)
			bs.idxMu.RUnlock()
			// fn runs outside every store lock: it re-enters the store.
			for _, m := range members {
				if !fn(m.ID, m.FirstTx) {
					return nil
				}
			}
			return nil
		}
		bs.idxMu.RUnlock()
		if err := bs.buildRelPropTx(key); err != nil {
			return err
		}
	}
	return fmt.Errorf("graph: relationship property membership: lookup raced %d invalidations: %w", propTxBuildAttempts, ErrIndexNotFound)
}

// ForEachNodePropertyTxMember implements store.NodePropertyTxMembershipCapability.
func (bs *Store) ForEachNodePropertyTxMember(labelToken uint16, propertyKey, valueKey string, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	for attempt := 0; attempt < propTxBuildAttempts; attempt++ {
		bs.idxMu.RLock()
		if !bs.nodePropIndexUsableLocked(key) {
			bs.idxMu.RUnlock()
			return ErrIndexNotFound
		}
		if sc := bs.nodePropTx[key]; sc != nil && sc.built {
			members := sc.members.Members(valueKey)
			bs.idxMu.RUnlock()
			for _, m := range members {
				if !fn(m.ID, m.FirstTx) {
					return nil
				}
			}
			return nil
		}
		bs.idxMu.RUnlock()
		if err := bs.buildNodePropTx(key); err != nil {
			return err
		}
	}
	return fmt.Errorf("graph: node property membership: lookup raced %d invalidations: %w", propTxBuildAttempts, ErrIndexNotFound)
}

// propTxBuildKey names one sidecar for its build mutex.
type propTxBuildKey struct {
	node  bool
	token uint16
	key   string
}

// propTxBuildLock returns the build mutex of one sidecar (created on first use,
// bounded by the declared indexes).
func (bs *Store) propTxBuildLock(k propTxBuildKey) *sync.Mutex {
	if mu, ok := bs.propTxBuildMus.Load(k); ok {
		return mu.(*sync.Mutex)
	}
	mu, _ := bs.propTxBuildMus.LoadOrStore(k, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

// buildRelPropTx builds the sidecar for key (see the file comment): nil when built
// (by this call or a racing one), ErrIndexNotFound when the index went away.
func (bs *Store) buildRelPropTx(key indexpkg.RelPropertyIndexKey) error {
	mu := bs.propTxBuildLock(propTxBuildKey{token: key.RelTypeToken, key: key.PropertyKey})
	mu.Lock()
	defer mu.Unlock()
	for attempt := 0; attempt < propTxBuildAttempts; attempt++ {
		bs.idxMu.Lock()
		if !bs.relPropIndexUsableLocked(key) {
			bs.idxMu.Unlock()
			return ErrIndexNotFound
		}
		sc := bs.relPropTx[key]
		if sc != nil && sc.built {
			bs.idxMu.Unlock()
			return nil
		}
		if sc == nil {
			sc = &propTxSidecar[types.RelID]{members: indexpkg.NewPropertyTxMembers[types.RelID]()}
			if bs.relPropTx == nil {
				bs.relPropTx = make(map[indexpkg.RelPropertyIndexKey]*propTxSidecar[types.RelID])
			}
			bs.relPropTx[key] = sc // tracking from here on
		}
		gen := bs.propTxGen
		elements := bs.relPropertyIndexes[key].Elements()
		bs.idxMu.Unlock()

		start := time.Now()
		scratch := indexpkg.NewPropertyTxMembers[types.RelID]()
		err := bs.scanPropTxRows(storepkg.KeyRel, storepkg.KeyHistRel, func(id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) error {
			return bs.recordRelRowPropTx(scratch, key, elements, id, hist, version, raw, local)
		})
		if err != nil {
			return fmt.Errorf("graph: build relationship property membership: %w", err)
		}
		if bs.propTxBuildTestHook != nil {
			bs.propTxBuildTestHook()
		}
		bs.idxMu.Lock()
		if bs.propTxGen != gen || bs.relPropTx[key] != sc {
			bs.idxMu.Unlock()
			continue // dropped while scanning: the scan may hold dropped rows
		}
		sc.members.Merge(scratch)
		sc.built = true
		bs.idxMu.Unlock()
		bs.propTxBuilds.Add(1)
		bs.propTxBuildNanos.Add(int64(time.Since(start)))
		return nil
	}
	return fmt.Errorf("graph: build relationship property membership: raced %d invalidations: %w", propTxBuildAttempts, ErrIndexNotFound)
}

// buildNodePropTx is buildRelPropTx for a node property index.
func (bs *Store) buildNodePropTx(key indexpkg.PropertyIndexKey) error {
	mu := bs.propTxBuildLock(propTxBuildKey{node: true, token: key.LabelToken, key: key.PropertyKey})
	mu.Lock()
	defer mu.Unlock()
	for attempt := 0; attempt < propTxBuildAttempts; attempt++ {
		bs.idxMu.Lock()
		if !bs.nodePropIndexUsableLocked(key) {
			bs.idxMu.Unlock()
			return ErrIndexNotFound
		}
		sc := bs.nodePropTx[key]
		if sc != nil && sc.built {
			bs.idxMu.Unlock()
			return nil
		}
		if sc == nil {
			sc = &propTxSidecar[types.NodeID]{members: indexpkg.NewPropertyTxMembers[types.NodeID]()}
			if bs.nodePropTx == nil {
				bs.nodePropTx = make(map[indexpkg.PropertyIndexKey]*propTxSidecar[types.NodeID])
			}
			bs.nodePropTx[key] = sc
		}
		gen := bs.propTxGen
		elements := bs.propertyIndexes[key].Elements()
		bs.idxMu.Unlock()

		start := time.Now()
		scratch := indexpkg.NewPropertyTxMembers[types.NodeID]()
		err := bs.scanPropTxRows(storepkg.KeyNode, storepkg.KeyHistNode, func(id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) error {
			return bs.recordNodeRowPropTx(scratch, key, elements, id, hist, version, raw, local)
		})
		if err != nil {
			return fmt.Errorf("graph: build node property membership: %w", err)
		}
		if bs.propTxBuildTestHook != nil {
			bs.propTxBuildTestHook()
		}
		bs.idxMu.Lock()
		if bs.propTxGen != gen || bs.nodePropTx[key] != sc {
			bs.idxMu.Unlock()
			continue
		}
		sc.members.Merge(scratch)
		sc.built = true
		bs.idxMu.Unlock()
		bs.propTxBuilds.Add(1)
		bs.propTxBuildNanos.Add(int64(time.Since(start)))
		return nil
	}
	return fmt.Errorf("graph: build node property membership: raced %d invalidations: %w", propTxBuildAttempts, ErrIndexNotFound)
}

// recordRelRowPropTx decodes one stored rel row (current or history, full or delta)
// through the checked wire path, so the value key equals the index's, and records
// it when the row has key's type and the property.
func (bs *Store) recordRelRowPropTx(m *indexpkg.PropertyTxMembers[types.RelID], key indexpkg.RelPropertyIndexKey, elements bool, id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) error {
	var w storepkg.RelWire
	if hist && storepkg.HistoryValueKindOf(raw) == storepkg.HistoryDelta {
		d, err := storepkg.DecodeRelHistoryDelta(raw)
		if err != nil {
			return err
		}
		if d.Meta.RelType != int(key.RelTypeToken) {
			return nil
		}
		if w, err = bs.reconstructRelHistoryWire(id, version, raw, local); err != nil {
			return err
		}
	} else {
		if err := storepkg.SafeUnmarshal(raw, &w); err != nil {
			return err
		}
		if w.RelType != int(key.RelTypeToken) {
			return nil
		}
	}
	if err := bs.resolveRelWireKeys(&w); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidStoreMutation, err)
	}
	r, err := storepkg.WireToRelChecked(w)
	if err != nil {
		return err
	}
	if r.ID().SnowflakeID() != id {
		return fmt.Errorf("%w: relationship wire id %d does not match key %d", ErrInvalidStoreMutation, r.ID().SnowflakeID(), id)
	}
	r.ForEachPropertyIndexKey(key.PropertyKey, elements, func(vk string) bool {
		m.Record(vk, r.ID(), relTxFrom(r))
		return true
	})
	return nil
}

// recordNodeRowPropTx is recordRelRowPropTx for node rows; the row must carry
// key's label itself.
func (bs *Store) recordNodeRowPropTx(m *indexpkg.PropertyTxMembers[types.NodeID], key indexpkg.PropertyIndexKey, elements bool, id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) error {
	var w storepkg.NodeWire
	if hist && storepkg.HistoryValueKindOf(raw) == storepkg.HistoryDelta {
		d, err := storepkg.DecodeNodeHistoryDelta(raw)
		if err != nil {
			return err
		}
		if !nodeWireHasLabel(&d.Meta, key.LabelToken) {
			return nil
		}
		if w, err = bs.reconstructNodeHistoryWire(id, version, raw, local); err != nil {
			return err
		}
	} else {
		if err := storepkg.SafeUnmarshal(raw, &w); err != nil {
			return err
		}
		if !nodeWireHasLabel(&w, key.LabelToken) {
			return nil
		}
	}
	if err := bs.resolveNodeWireKeys(&w); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidStoreMutation, err)
	}
	n, err := storepkg.WireToNodeChecked(w)
	if err != nil {
		return err
	}
	if n.ID().SnowflakeID() != id {
		return fmt.Errorf("%w: node wire id %d does not match key %d", ErrInvalidStoreMutation, n.ID().SnowflakeID(), id)
	}
	n.ForEachPropertyIndexKey(key.PropertyKey, elements, func(vk string) bool {
		m.Record(vk, n.ID(), nodeTxFrom(n))
		return true
	})
	return nil
}

func nodeWireHasLabel(w *storepkg.NodeWire, label uint16) bool {
	if w.PrimaryLabel == int(label) {
		return true
	}
	for _, el := range w.ExtraLabels {
		if el == int(label) {
			return true
		}
	}
	return false
}

// scanPropTxRows visits every stored row of one entity kind: current rows (curKind)
// and history rows (histKind). The write-buffer overlay is captured FIRST, then the
// badger view opened (lessons 64, 74); a buffered op masks its committed key. A
// history delta resolves its anchor from the overlay, then earlier committed rows
// of the same entity, then the view.
func (bs *Store) scanPropTxRows(curKind, histKind byte, visit func(id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) error) error {
	overlay := make(map[string]writeOp)
	bs.rangePending(func(k string, op writeOp) {
		if len(k) == 0 {
			return
		}
		switch {
		case k[0] == curKind && len(k) == storepkg.SizeNodeKey:
		case k[0] == histKind && len(k) == storepkg.SizeHistKey:
		default:
			return
		}
		op.value = append([]byte(nil), op.value...)
		overlay[k] = op // flushing first, then pending: the newer op wins
	})
	histKey := func(id snowflake.ID, version uint64) string {
		if histKind == storepkg.KeyHistNode {
			return string(storepkg.HistNodeKey(id, version))
		}
		return string(storepkg.HistRelKey(id, version))
	}
	if bs.propTxScanTestHook != nil {
		bs.propTxScanTestHook()
	}
	return bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = true

		// Committed current rows the overlay does not resolve.
		prefix := []byte{curKind}
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			if len(k) != storepkg.SizeNodeKey {
				continue
			}
			if _, buffered := overlay[string(k)]; buffered {
				continue
			}
			id := storepkg.ParseIDFromKey(k, 1)
			if err := item.Value(func(val []byte) error { return visit(id, false, 0, val, nil) }); err != nil {
				it.Close()
				return err
			}
		}
		it.Close()

		// Committed history rows, one entity's versions after another.
		var groupID snowflake.ID
		anchors := make(map[uint64][]byte)
		committedLocal := func(id snowflake.ID) localAnchorFunc {
			return func(av uint64) ([]byte, bool) {
				if op, ok := overlay[histKey(id, av)]; ok {
					return op.value, op.opType != writeOpDelete
				}
				b, ok := anchors[av]
				return b, ok
			}
		}
		prefix = []byte{histKind}
		opts.Prefix = prefix
		it = txn.NewIterator(opts)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			if len(k) != storepkg.SizeHistKey {
				continue
			}
			id := storepkg.ParseIDFromKey(k, 1)
			if id != groupID {
				groupID = id
				clear(anchors)
			}
			if _, buffered := overlay[string(k)]; buffered {
				continue
			}
			version := historyVersionFromKey(k)
			if err := item.Value(func(val []byte) error {
				if storepkg.IsAnchorVersion(version, bs.historyAnchorInterval) && storepkg.HistoryValueKindOf(val) == storepkg.HistoryFull {
					anchors[version] = append([]byte(nil), val...)
				}
				return visit(id, true, version, val, committedLocal(id))
			}); err != nil {
				it.Close()
				return err
			}
		}
		it.Close()

		// Buffered rows (strictly newer than anything committed).
		for k, op := range overlay {
			if op.opType == writeOpDelete || len(op.value) == 0 {
				continue
			}
			kb := []byte(k)
			id := storepkg.ParseIDFromKey(kb, 1)
			if kb[0] == curKind {
				if err := visit(id, false, 0, op.value, nil); err != nil {
					return err
				}
				continue
			}
			local := func(av uint64) ([]byte, bool) {
				ak := histKey(id, av)
				if aop, ok := overlay[ak]; ok {
					return aop.value, aop.opType != writeOpDelete
				}
				item, err := txn.Get([]byte(ak))
				if err != nil {
					return nil, false
				}
				b, err := item.ValueCopy(nil)
				return b, err == nil
			}
			if err := visit(id, true, historyVersionFromKey(kb), op.value, local); err != nil {
				return err
			}
		}
		return nil
	})
}

// PropertyTxMembershipStats implements store.PropertyTxMembershipStatsCapability.
func (bs *Store) PropertyTxMembershipStats() (storecontract.PropertyTxMembershipStats, error) {
	if err := bs.checkOpen(); err != nil {
		return storecontract.PropertyTxMembershipStats{}, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	st := storecontract.PropertyTxMembershipStats{
		Builds:        bs.propTxBuilds.Load(),
		BuildDuration: time.Duration(bs.propTxBuildNanos.Load()),
	}
	for _, sc := range bs.relPropTx {
		if sc.built {
			st.RelSidecars++
		}
		st.RelPostings += sc.members.Postings()
	}
	for _, sc := range bs.nodePropTx {
		if sc.built {
			st.NodeSidecars++
		}
		st.NodePostings += sc.members.Postings()
	}
	return st, nil
}
