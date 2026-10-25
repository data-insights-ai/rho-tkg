package badger

import (
	"cmp"
	"fmt"
	"math"
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
// TxFrom). Append-only supersets; the chain resolver stays the authority. A
// clean close persists the built ones in the open image and a trusted reopen
// restores them (backlog 74, badgerstore_open_image.go).
//
// Every door writing a current row records via maintain*IndexesAdd under
// idxMu.Lock, every history row next to its belief-watermark bump. History rows:
// the demoted row of a *WithHistory door, a delete tombstone, and
// PutRel/NodeVersion via enqueueVersionAgainstLazyBuilds. A sidecar in
// relPropTx / nodePropTx is tracking; built marks it readable.
//
// Lazy build without blocking writers (lessons 63, 64): the first lookup of an
// unbuilt sidecar starts a build pass (propTxPass) that, under idxMu.Lock, claims
// every declared unbuilt sidecar of the kind and turns tracking on for each, reads
// the invalidation generation, releases idxMu, captures the write-buffer overlay,
// then scans committed current and history rows in one view, filling all of them
// (backlog 74: one scan, not one per (type, key)). A row written before tracking
// was on is in the overlay or the view (its door appended its op inside the same
// idxMu section); a row written after is recorded by its door. A scan straddling an
// invalidation is discarded and redone. The merge is a union with
// per-member minimum, so scan and concurrent records commute. The results install
// only if the generation is unchanged, each only if its sidecar is still mapped;
// Clear, index drop, retention purge and exact erasure drop the sidecars and move
// the generation.
type propTxSidecar[ID cmp.Ordered] struct {
	members *indexpkg.PropertyTxMembers[ID]
	built   bool
	pass    *propTxPass // the running pass that fills it; nil when none
}

// propTxBuildAttempts bounds rescans of a build that keeps losing to an
// invalidation (lesson 24); then the lookup reports ErrIndexNotFound (full fold).
const propTxBuildAttempts = 4

// recordRelRowLocked records one relationship row a door writes (current or
// history) into every built membership sidecar (K1 rel-type set and property
// sets). Every row, not only creates: a row's TxFrom lowers the member's bound,
// and its valid-time span joins the member's spans (ledger row 62). Caller
// holds idxMu (write).
func (bs *Store) recordRelRowLocked(r *types.Relationship, current bool) {
	bs.recordRelTypeMemberLocked(r)
	bs.recordRelPropTxLocked(r, current)
}

// recordNodeRowLocked is recordRelRowLocked for node rows (K1 label set and
// node property sets). Caller holds idxMu (write).
func (bs *Store) recordNodeRowLocked(n *types.Node, current bool) {
	bs.recordNodeLabelMembersLocked(n)
	bs.recordNodePropTxLocked(n, current)
}

// recordRelPropTxLocked records r's indexable values into the tracking rel
// sidecars of its type, as a current or a history row. Caller holds idxMu
// (write).
func (bs *Store) recordRelPropTxLocked(r *types.Relationship, current bool) {
	if len(bs.relPropTx) == 0 || r == nil {
		return
	}
	tok := r.TypeToken().Value()
	id := r.ID()
	ri := indexpkg.RelRowInfo(bs.validAxis.Get(), r)
	r.ForEachIndexKey(indexpkg.RelElementsOf(bs.relPropertyIndexes, tok), func(propertyKey, valueKey string) bool {
		if sc := bs.relPropTx[indexpkg.RelPropertyIndexKey{RelTypeToken: tok, PropertyKey: propertyKey}]; sc != nil {
			sc.members.RecordRow(valueKey, id, ri, current)
		}
		return true
	})
}

// recordNodePropTxLocked records n's indexable values into the tracking node
// sidecars of every label the row carries. Caller holds idxMu (write).
func (bs *Store) recordNodePropTxLocked(n *types.Node, current bool) {
	if len(bs.nodePropTx) == 0 || n == nil {
		return
	}
	id := n.ID()
	ri := indexpkg.NodeRowInfo(bs.validAxis.Get(), n)
	for i := 0; i < n.LabelTokenCount(); i++ {
		label := n.LabelTokenRawAt(i)
		n.ForEachIndexKey(indexpkg.ElementsOf(bs.propertyIndexes, label), func(propertyKey, valueKey string) bool {
			if sc := bs.nodePropTx[indexpkg.PropertyIndexKey{LabelToken: label, PropertyKey: propertyKey}]; sc != nil {
				sc.members.RecordRow(valueKey, id, ri, current)
			}
			return true
		})
	}
}

// recordRelReplacedLocked records prev, the copy of a current row an update
// door moves to history, replaced by next (ledger row 62): K1 as any row,
// and in the property sidecars prev ends at next (indexpkg.RecordReplaced).
func (bs *Store) recordRelReplacedLocked(prev, next *types.Relationship) {
	bs.recordRelTypeMemberLocked(prev)
	if len(bs.relPropTx) == 0 || prev == nil || next == nil {
		return
	}
	tok := prev.TypeToken().Value()
	id := prev.ID()
	old, nx := indexpkg.RelRowInfo(bs.validAxis.Get(), prev), indexpkg.RelRowInfo(bs.validAxis.Get(), next)
	prev.ForEachIndexKey(indexpkg.RelElementsOf(bs.relPropertyIndexes, tok), func(propertyKey, valueKey string) bool {
		if sc := bs.relPropTx[indexpkg.RelPropertyIndexKey{RelTypeToken: tok, PropertyKey: propertyKey}]; sc != nil {
			sc.members.RecordReplaced(valueKey, id, old, nx)
		}
		return true
	})
}

// recordNodeReplacedLocked is recordRelReplacedLocked for a node row.
func (bs *Store) recordNodeReplacedLocked(prev, next *types.Node) {
	bs.recordNodeLabelMembersLocked(prev)
	if len(bs.nodePropTx) == 0 || prev == nil || next == nil {
		return
	}
	id := prev.ID()
	old, nx := indexpkg.NodeRowInfo(bs.validAxis.Get(), prev), indexpkg.NodeRowInfo(bs.validAxis.Get(), next)
	for i := 0; i < prev.LabelTokenCount(); i++ {
		label := prev.LabelTokenRawAt(i)
		prev.ForEachIndexKey(indexpkg.ElementsOf(bs.propertyIndexes, label), func(propertyKey, valueKey string) bool {
			if sc := bs.nodePropTx[indexpkg.PropertyIndexKey{LabelToken: label, PropertyKey: propertyKey}]; sc != nil {
				sc.members.RecordReplaced(valueKey, id, old, nx)
			}
			return true
		})
	}
}

// forgetRelPropTxCurrentLocked notes that a delete door removed r's ID's
// current row: its members keep their history envelopes only (ledger row 62).
// Called once the delete is committed to the write path. Caller holds idxMu
// (write).
func (bs *Store) forgetRelPropTxCurrentLocked(r *types.Relationship) {
	if len(bs.relPropTx) == 0 || r == nil {
		return
	}
	tok := r.TypeToken().Value()
	id := r.ID()
	r.ForEachIndexKey(indexpkg.RelElementsOf(bs.relPropertyIndexes, tok), func(propertyKey, valueKey string) bool {
		if sc := bs.relPropTx[indexpkg.RelPropertyIndexKey{RelTypeToken: tok, PropertyKey: propertyKey}]; sc != nil {
			sc.members.ForgetCurrent(valueKey, id)
		}
		return true
	})
}

// forgetNodePropTxCurrentLocked is forgetRelPropTxCurrentLocked for a node.
func (bs *Store) forgetNodePropTxCurrentLocked(n *types.Node) {
	if len(bs.nodePropTx) == 0 || n == nil {
		return
	}
	id := n.ID()
	for i := 0; i < n.LabelTokenCount(); i++ {
		label := n.LabelTokenRawAt(i)
		n.ForEachIndexKey(indexpkg.ElementsOf(bs.propertyIndexes, label), func(propertyKey, valueKey string) bool {
			if sc := bs.nodePropTx[indexpkg.PropertyIndexKey{LabelToken: label, PropertyKey: propertyKey}]; sc != nil {
				sc.members.ForgetCurrent(valueKey, id)
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

// propTxPass is one running build pass (backlog 74). A pass claims every
// declared, usable sidecar of its kind that is neither built nor claimed, and
// fills them all from ONE store scan (ai-soc paid 29 scans for 29 types). done
// closes when the pass ends, installed or not: a lookup on a claimed sidecar
// waits for it instead of scanning again, a lookup on a built or unclaimed one
// never does.
type propTxPass struct{ done chan struct{} }

// propTxTarget is one sidecar a pass fills: the scan records into scratch,
// the install merges scratch into sc.
type propTxTarget[K comparable, ID cmp.Ordered] struct {
	key      K
	sc       *propTxSidecar[ID]
	elements bool
	scratch  *indexpkg.PropertyTxMembers[ID]
	// rows collects the scanned rows; the install records them into scratch
	// (indexpkg.RowScan: a replaced row ends at its replacing row, ledger
	// row 62).
	rows indexpkg.RowScan[ID]
}

// buildRelPropTx builds the sidecar for key (see the file comment): nil when built
// (by this call's pass or a running one), ErrIndexNotFound when the index went away.
func (bs *Store) buildRelPropTx(key indexpkg.RelPropertyIndexKey) error {
	return ensurePropTxBuilt(bs, "relationship", key, &bs.relPropTx, &bs.relPropertyIndexes, bs.scanRelPropTx)
}

// buildNodePropTx is buildRelPropTx for a node property index.
func (bs *Store) buildNodePropTx(key indexpkg.PropertyIndexKey) error {
	return ensurePropTxBuilt(bs, "node", key, &bs.nodePropTx, &bs.propertyIndexes, bs.scanNodePropTx)
}

// ensurePropTxBuilt is the body of both builds. sidecars and indexes point at
// the kind's sidecar map and declared indexes (both guarded by idxMu); scan
// runs one store pass filling every target. Lock order: idxMu is never held
// while waiting for a pass or scanning.
func ensurePropTxBuilt[K comparable, ID cmp.Ordered](bs *Store, what string, key K,
	sidecars *map[K]*propTxSidecar[ID], indexes *map[K]*indexpkg.PropertyIndex,
	scan func([]*propTxTarget[K, ID]) error) error {
	usable := func(idx *indexpkg.PropertyIndex) bool { return idx != nil && idx.Mutated == nil } // declared, finished creating
	for attempt := 0; attempt < propTxBuildAttempts; attempt++ {
		bs.idxMu.Lock()
		if !usable((*indexes)[key]) {
			bs.idxMu.Unlock()
			return ErrIndexNotFound
		}
		if sc := (*sidecars)[key]; sc != nil && sc.built {
			bs.idxMu.Unlock()
			return nil
		} else if sc != nil && sc.pass != nil {
			running := sc.pass
			bs.idxMu.Unlock()
			<-running.done // a running pass builds key: wait, then re-check
			continue
		}
		pass := &propTxPass{done: make(chan struct{})}
		var targets []*propTxTarget[K, ID]
		for k, idx := range *indexes {
			if !usable(idx) {
				continue
			}
			sc := (*sidecars)[k]
			if sc == nil {
				sc = &propTxSidecar[ID]{members: indexpkg.NewPropertyTxMembers[ID]()}
				if *sidecars == nil {
					*sidecars = make(map[K]*propTxSidecar[ID])
				}
				(*sidecars)[k] = sc // tracking from here on
			}
			if sc.built || sc.pass != nil {
				continue
			}
			sc.pass = pass
			targets = append(targets, &propTxTarget[K, ID]{key: k, sc: sc, elements: idx.Elements(), scratch: indexpkg.NewPropertyTxMembers[ID]()})
		}
		gen := bs.propTxGen
		bs.idxMu.Unlock()

		start := time.Now()
		installed, err := runPropTxPass(bs, pass, gen, sidecars, targets, scan)
		if err != nil {
			return fmt.Errorf("graph: build %s property membership: %w", what, err)
		}
		if installed {
			bs.propTxBuilds.Add(1)
			bs.propTxBuildNanos.Add(int64(time.Since(start)))
			return nil
		}
	}
	return fmt.Errorf("graph: build %s property membership: raced %d invalidations: %w", what, propTxBuildAttempts, ErrIndexNotFound)
}

// runPropTxPass scans for the claimed targets and installs them when the scan
// completed and the generation did not move (a drop while scanning: the scan
// may hold dropped rows, the caller rescans). However it ends, even by a
// panic, it releases the claims and closes pass.done, so no waiter hangs.
func runPropTxPass[K comparable, ID cmp.Ordered](bs *Store, pass *propTxPass, gen uint64,
	sidecars *map[K]*propTxSidecar[ID], targets []*propTxTarget[K, ID],
	scan func([]*propTxTarget[K, ID]) error) (installed bool, err error) {
	scanned := false
	defer func() {
		if scanned {
			// Record the scan into the pass's private scratch before the lock:
			// under idxMu only the merge into the live sidecar runs.
			for _, t := range targets {
				t.rows.Into(t.scratch)
			}
		}
		bs.idxMu.Lock()
		if bs.propTxInstallLockedTestHook != nil {
			n := 0
			for _, t := range targets {
				n += t.rows.Len()
			}
			bs.propTxInstallLockedTestHook(n)
		}
		installed = scanned && bs.propTxGen == gen
		for _, t := range targets {
			if (*sidecars)[t.key] != t.sc {
				continue // dropped while scanning
			}
			if installed {
				if t.sc.members.HasViews() {
					t.sc.members.Merge(t.scratch)
				} else {
					// The live sidecar holds only the writes recorded while the
					// pass ran: fold those into the scan and adopt it, so the
					// lock is held for them, not for the scan (unions commute).
					t.scratch.Merge(t.sc.members)
					t.sc.members = t.scratch
				}
				t.sc.built = true
			}
			t.sc.pass = nil
		}
		bs.idxMu.Unlock()
		close(pass.done)
	}()
	if err = scan(targets); err != nil {
		return false, err
	}
	if bs.propTxBuildTestHook != nil {
		bs.propTxBuildTestHook()
	}
	scanned = true
	return false, nil // the deferred install decides installed
}

type relPropTxTarget = propTxTarget[indexpkg.RelPropertyIndexKey, types.RelID]

type nodePropTxTarget = propTxTarget[indexpkg.PropertyIndexKey, types.NodeID]

// scanRelPropTx fills every rel target from one pass over the rel rows.
func (bs *Store) scanRelPropTx(targets []*relPropTxTarget) error {
	byType := make(map[int][]*relPropTxTarget, len(targets))
	for _, t := range targets {
		tok := int(t.key.RelTypeToken)
		byType[tok] = append(byType[tok], t)
	}
	return bs.scanPropTxRows(storepkg.KeyRel, storepkg.KeyHistRel, func(id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) error {
		return bs.recordRelRowPropTx(byType, id, hist, version, raw, local)
	})
}

// scanNodePropTx fills every node target from one pass over the node rows.
func (bs *Store) scanNodePropTx(targets []*nodePropTxTarget) error {
	byLabel := make(map[uint16][]*nodePropTxTarget, len(targets))
	for _, t := range targets {
		byLabel[t.key.LabelToken] = append(byLabel[t.key.LabelToken], t)
	}
	return bs.scanPropTxRows(storepkg.KeyNode, storepkg.KeyHistNode, func(id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) error {
		return bs.recordNodeRowPropTx(byLabel, id, hist, version, raw, local)
	})
}

// recordRelRowPropTx decodes one stored rel row (current or history, full or delta)
// through the checked wire path, so the value key equals the index's, and records
// it into every target of its type. A row of an untargeted type is skipped
// before the full decode.
func (bs *Store) recordRelRowPropTx(byType map[int][]*relPropTxTarget, id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) error {
	var w storepkg.RelWire
	var targets []*relPropTxTarget
	if hist && storepkg.HistoryValueKindOf(raw) == storepkg.HistoryDelta {
		d, err := storepkg.DecodeRelHistoryDelta(raw)
		if err != nil {
			return err
		}
		if targets = byType[d.Meta.RelType]; len(targets) == 0 {
			return nil
		}
		if w, err = bs.reconstructRelHistoryWire(id, version, raw, local); err != nil {
			return err
		}
	} else {
		if err := storepkg.SafeUnmarshal(raw, &w); err != nil {
			return err
		}
		if targets = byType[w.RelType]; len(targets) == 0 {
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
	rid, ri := r.ID(), indexpkg.RelRowInfo(bs.validAxis.Get(), r)
	for _, t := range targets {
		var vks []string
		r.ForEachPropertyIndexKey(t.key.PropertyKey, t.elements, func(vk string) bool {
			vks = append(vks, vk)
			return true
		})
		t.rows.Add(rid, ri, !hist, vks)
	}
	return nil
}

// recordNodeRowPropTx is recordRelRowPropTx for node rows: the row is recorded
// into the targets of every label it carries.
func (bs *Store) recordNodeRowPropTx(byLabel map[uint16][]*nodePropTxTarget, id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) error {
	var w storepkg.NodeWire
	if hist && storepkg.HistoryValueKindOf(raw) == storepkg.HistoryDelta {
		d, err := storepkg.DecodeNodeHistoryDelta(raw)
		if err != nil {
			return err
		}
		if !nodeWireHasTarget(&d.Meta, byLabel) {
			return nil
		}
		if w, err = bs.reconstructNodeHistoryWire(id, version, raw, local); err != nil {
			return err
		}
	} else {
		if err := storepkg.SafeUnmarshal(raw, &w); err != nil {
			return err
		}
		if !nodeWireHasTarget(&w, byLabel) {
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
	nid, ri := n.ID(), indexpkg.NodeRowInfo(bs.validAxis.Get(), n)
	for i := 0; i < n.LabelTokenCount(); i++ {
		for _, t := range byLabel[n.LabelTokenRawAt(i)] {
			var vks []string
			n.ForEachPropertyIndexKey(t.key.PropertyKey, t.elements, func(vk string) bool {
				vks = append(vks, vk)
				return true
			})
			t.rows.Add(nid, ri, !hist, vks)
		}
	}
	return nil
}

// nodeWireHasTarget reports whether the row carries a label some target indexes.
func nodeWireHasTarget(w *storepkg.NodeWire, byLabel map[uint16][]*nodePropTxTarget) bool {
	has := func(label int) bool {
		return label > 0 && label <= math.MaxUint16 && len(byLabel[uint16(label)]) > 0 // #nosec G115 -- range checked
	}
	if has(w.PrimaryLabel) {
		return true
	}
	for _, el := range w.ExtraLabels {
		if has(el) {
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
		opts.PrefetchValues = false // values are inline: no goroutine per row (PREFETCH)

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
