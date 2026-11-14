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

// Property membership sidecars (badger arm): one
// indexpkg.PropertyTxMembers per declared property index a temporal lookup asked
// for (value key -> rels/nodes whose rows ever carried it, with the lowest
// TxFrom). Append-only supersets; the chain resolver stays the authority. A
// clean close persists the built ones in the open image and a trusted reopen
// restores them (badgerstore_open_image.go).
//
// Every door writing a current row records via maintain*IndexesAdd under
// idxMu.Lock, every history row next to its belief-watermark bump. History rows:
// the demoted row of a *WithHistory door, a delete tombstone, and
// PutRel/NodeVersion via enqueueVersionAgainstLazyBuilds. A sidecar in
// relPropTx / nodePropTx is tracking; built marks it readable.
//
// Lazy build without blocking writers: the first lookup of an
// unbuilt sidecar starts a build pass (propTxPass) that, under idxMu.Lock, claims
// every declared unbuilt sidecar of the kind and turns tracking on for each, reads
// the invalidation generation, releases idxMu, captures the write-buffer overlay,
// then scans committed current and history rows in one view, filling all of them
// (one scan, not one per (type, key)). A row written before tracking
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
// invalidation; then the lookup reports ErrIndexNotFound (full fold).
const propTxBuildAttempts = 4

// recordRelRowLocked records one relationship row a door writes (current or
// history) into every built membership sidecar (K1 rel-type set and property
// sets). Every row, not only creates: a row's TxFrom lowers the member's bound,
// and its valid-time span joins the member's spans. Caller
// holds idxMu (write).
func (bs *Store) recordRelRowLocked(r *types.Relationship, current bool) {
	bs.recordRelTypeMemberLocked(r)
	bs.recordRelPropTxLocked(r, current)
}

// recordNodeRowLocked is recordRelRowLocked for node rows (K1 label set and
// node property sets). Caller holds idxMu (write).
func (bs *Store) recordNodeRowLocked(n *types.Node, current bool) {
	keep := n // a current row is a member through the label index
	if !current {
		keep = nil
	}
	bs.recordNodeLabelMembersLocked(n, keep)
	bs.recordNodePropTxLocked(n, current)
}

// recordRelPropTxLocked records r's indexable values into the tracking rel
// sidecars of its type, as a current or a history row. Caller holds idxMu
// (write).
func (bs *Store) recordRelPropTxLocked(r *types.Relationship, current bool) {
	if len(bs.relPropTx) == 0 || r == nil {
		return
	}
	id := r.ID()
	ri := indexpkg.RelRowInfo(bs.validAxis.Get(), r)
	bs.relIndexKeysLocked(r, func(key indexpkg.RelPropertyIndexKey, valueKey string) {
		if sc := bs.relPropTx[key]; sc != nil {
			sc.members.RecordRow(valueKey, id, ri, current)
		}
	})
}

// nodeIndexKeysLocked calls fn with each property index on a label n carries
// and each value key n has under it, built for indexed properties only
// (IDXKEYLAZY, as indexpkg's maintenance walks). Caller holds idxMu.
func (bs *Store) nodeIndexKeysLocked(n *types.Node, fn func(key indexpkg.PropertyIndexKey, valueKey string)) {
	for key, idx := range bs.propertyIndexes {
		if !n.HasLabelTokenRaw(key.LabelToken) {
			continue
		}
		n.ForEachPropertyIndexKey(key.PropertyKey, idx.Elements(), func(valueKey string) bool {
			fn(key, valueKey)
			return true
		})
	}
}

// relIndexKeysLocked is nodeIndexKeysLocked for a relationship: the indexes
// on its type.
func (bs *Store) relIndexKeysLocked(r *types.Relationship, fn func(key indexpkg.RelPropertyIndexKey, valueKey string)) {
	tok := r.TypeToken().Value()
	for key, idx := range bs.relPropertyIndexes {
		if key.RelTypeToken != tok {
			continue
		}
		r.ForEachPropertyIndexKey(key.PropertyKey, idx.Elements(), func(valueKey string) bool {
			fn(key, valueKey)
			return true
		})
	}
}

// recordNodePropTxLocked records n's indexable values into the tracking node
// sidecars of every label the row carries. Caller holds idxMu (write).
func (bs *Store) recordNodePropTxLocked(n *types.Node, current bool) {
	if len(bs.nodePropTx) == 0 || n == nil {
		return
	}
	id := n.ID()
	ri := indexpkg.NodeRowInfo(bs.validAxis.Get(), n)
	bs.nodeIndexKeysLocked(n, func(key indexpkg.PropertyIndexKey, valueKey string) {
		if sc := bs.nodePropTx[key]; sc != nil {
			sc.members.RecordRow(valueKey, id, ri, current)
		}
	})
}

// recordRelReplacedLocked records prev, the copy of a current row an update
// door moves to history, replaced by next: K1 as any row,
// and in the property sidecars prev ends at next (indexpkg.RecordReplaced).
func (bs *Store) recordRelReplacedLocked(prev, next *types.Relationship) {
	bs.recordRelTypeMemberLocked(prev)
	if len(bs.relPropTx) == 0 || prev == nil || next == nil {
		return
	}
	id := prev.ID()
	old, nx := indexpkg.RelRowInfo(bs.validAxis.Get(), prev), indexpkg.RelRowInfo(bs.validAxis.Get(), next)
	bs.relIndexKeysLocked(prev, func(key indexpkg.RelPropertyIndexKey, valueKey string) {
		if sc := bs.relPropTx[key]; sc != nil {
			sc.members.RecordReplaced(valueKey, id, old, nx)
		}
	})
}

// recordNodeReplacedLocked is recordRelReplacedLocked for a node row.
func (bs *Store) recordNodeReplacedLocked(prev, next *types.Node) {
	bs.recordNodeLabelMembersLocked(prev, next)
	if len(bs.nodePropTx) == 0 || prev == nil || next == nil {
		return
	}
	id := prev.ID()
	old, nx := indexpkg.NodeRowInfo(bs.validAxis.Get(), prev), indexpkg.NodeRowInfo(bs.validAxis.Get(), next)
	bs.nodeIndexKeysLocked(prev, func(key indexpkg.PropertyIndexKey, valueKey string) {
		if sc := bs.nodePropTx[key]; sc != nil {
			sc.members.RecordReplaced(valueKey, id, old, nx)
		}
	})
}

// forgetRelPropTxCurrentLocked notes that a delete door removed r's ID's
// current row: its members keep their history envelopes only.
// Called once the delete is committed to the write path. Caller holds idxMu
// (write).
func (bs *Store) forgetRelPropTxCurrentLocked(r *types.Relationship) {
	if len(bs.relPropTx) == 0 || r == nil {
		return
	}
	id := r.ID()
	bs.relIndexKeysLocked(r, func(key indexpkg.RelPropertyIndexKey, valueKey string) {
		if sc := bs.relPropTx[key]; sc != nil {
			sc.members.ForgetCurrent(valueKey, id)
		}
	})
}

// forgetNodePropTxCurrentLocked is forgetRelPropTxCurrentLocked for a node.
func (bs *Store) forgetNodePropTxCurrentLocked(n *types.Node) {
	if len(bs.nodePropTx) == 0 || n == nil {
		return
	}
	id := n.ID()
	bs.nodeIndexKeysLocked(n, func(key indexpkg.PropertyIndexKey, valueKey string) {
		if sc := bs.nodePropTx[key]; sc != nil {
			sc.members.ForgetCurrent(valueKey, id)
		}
	})
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
	return bs.forEachRelPropertyTxMember(relTypeToken, propertyKey, valueKey, fn, nil)
}

// memberAdmit is a read budget's admission of a RAM sidecar walk's collection of
// one member (nil for a nil tally): the member is counted as a candidate and
// its slot in the list charged as BufferBytes (txMemberBytes), before it is
// added, so the walk stops at the member that passes a limit.
func memberAdmit(t *storecontract.ReadTally) func() error {
	if t == nil {
		return nil
	}
	return func() error {
		if err := t.Candidates(1); err != nil {
			return err
		}
		return t.Buffer(txMemberBytes)
	}
}

// forEachRelPropertyTxMember is ForEachRelPropertyTxMember under a read
// budget's tally t (nil: none), which a definition in RAM charges before it
// copies the value's members, one counted as it is collected (memberAdmit).
func (bs *Store) forEachRelPropertyTxMember(relTypeToken uint16, propertyKey, valueKey string, fn func(id types.RelID, firstTxFrom types.Instant) bool, t *storecontract.ReadTally) error {
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
	if members, ok, err := bs.relDiskMembers(key, memberQuery{valueKeys: []string{valueKey}}); ok {
		return emitTxMembers(members, err, fn) // RELDISK: kept on disk
	}
	for attempt := 0; attempt < propTxBuildAttempts; attempt++ {
		bs.idxMu.RLock()
		if !bs.relPropIndexUsableLocked(key) {
			bs.idxMu.RUnlock()
			return ErrIndexNotFound
		}
		if sc := bs.relPropTx[key]; sc != nil && sc.built {
			members, err := sc.members.MembersAdmit(valueKey, memberAdmit(t))
			bs.idxMu.RUnlock()
			if err != nil {
				return err
			}
			// fn runs outside every store lock: it re-enters the store.
			for _, m := range members {
				if !fn(m.ID, m.FirstTx) {
					return nil
				}
			}
			return nil
		}
		bs.idxMu.RUnlock()
		if err := bs.buildRelPropTxUnder(key, t); err != nil {
			return err
		}
	}
	return fmt.Errorf("graph: relationship property membership: lookup raced %d invalidations: %w", propTxBuildAttempts, ErrIndexNotFound)
}

// ForEachNodePropertyTxMember implements store.NodePropertyTxMembershipCapability.
func (bs *Store) ForEachNodePropertyTxMember(labelToken uint16, propertyKey, valueKey string, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	return bs.forEachNodePropertyTxMember(labelToken, propertyKey, valueKey, fn, nil)
}

// forEachNodePropertyTxMember is ForEachNodePropertyTxMember under a read
// budget's tally t (nil: none), as forEachRelPropertyTxMember.
func (bs *Store) forEachNodePropertyTxMember(labelToken uint16, propertyKey, valueKey string, fn func(id types.NodeID, firstTxFrom types.Instant) bool, t *storecontract.ReadTally) error {
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
	if members, ok, err := bs.nodeDiskMembers(key, memberQuery{valueKeys: []string{valueKey}}); ok {
		return emitTxMembers(members, err, fn) // RELDISK: members on disk
	}
	for attempt := 0; attempt < propTxBuildAttempts; attempt++ {
		bs.idxMu.RLock()
		if !bs.nodePropIndexUsableLocked(key) {
			bs.idxMu.RUnlock()
			return ErrIndexNotFound
		}
		if sc := bs.nodePropTx[key]; sc != nil && sc.built {
			members, err := sc.members.MembersAdmit(valueKey, memberAdmit(t))
			bs.idxMu.RUnlock()
			if err != nil {
				return err
			}
			for _, m := range members {
				if !fn(m.ID, m.FirstTx) {
					return nil
				}
			}
			return nil
		}
		bs.idxMu.RUnlock()
		if err := bs.buildNodePropTxUnder(key, t); err != nil {
			return err
		}
	}
	return fmt.Errorf("graph: node property membership: lookup raced %d invalidations: %w", propTxBuildAttempts, ErrIndexNotFound)
}

// propTxPass is one running build pass. A pass claims every
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
	// (indexpkg.RowScan: a replaced row ends at its replacing row).
	rows indexpkg.RowScan[ID]
}

// buildRelPropTx builds the sidecar for key (see the file comment): nil when built
// (by this call's pass or a running one), ErrIndexNotFound when the index went away.
func (bs *Store) buildRelPropTx(key indexpkg.RelPropertyIndexKey) error {
	return bs.buildRelPropTxUnder(key, nil)
}

func (bs *Store) buildRelPropTxUnder(key indexpkg.RelPropertyIndexKey, t *storecontract.ReadTally) error {
	return ensurePropTxBuilt(bs, "relationship", key, &bs.relPropTx, &bs.relPropertyIndexes, bs.relMembersOnDiskLocked, t,
		func(targets []*relPropTxTarget) error { return bs.scanRelPropTx(targets, t) })
}

// buildNodePropTx is buildRelPropTx for a node property index.
func (bs *Store) buildNodePropTx(key indexpkg.PropertyIndexKey) error {
	return bs.buildNodePropTxUnder(key, nil)
}

func (bs *Store) buildNodePropTxUnder(key indexpkg.PropertyIndexKey, t *storecontract.ReadTally) error {
	return ensurePropTxBuilt(bs, "node", key, &bs.nodePropTx, &bs.propertyIndexes, bs.nodeMembersOnDiskLocked, t,
		func(targets []*nodePropTxTarget) error { return bs.scanNodePropTx(targets, t) })
}

// ensurePropTxBuilt is the body of both builds. sidecars and indexes point at
// the kind's sidecar map and declared indexes (both guarded by idxMu); a
// definition onDisk reports (RELDISK) answers from its member entries and gets
// no sidecar; scan runs one store pass filling every target. Lock order: idxMu
// is never held while waiting for a pass or scanning.
func ensurePropTxBuilt[K comparable, ID cmp.Ordered](bs *Store, what string, key K,
	sidecars *map[K]*propTxSidecar[ID], indexes *map[K]*indexpkg.PropertyIndex, onDisk func(K) bool, tally *storecontract.ReadTally,
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
			if err := tally.Candidates(1); err != nil {
				bs.idxMu.Unlock()
				return err
			}
			if !usable(idx) || onDisk(k) {
				continue
			}
			sc := (*sidecars)[k]
			if sc != nil && (sc.built || sc.pass != nil) {
				continue
			}
			// The temporary target, its slice slot and scan-group references.
			if err := tally.Buffer(128); err != nil {
				bs.idxMu.Unlock()
				return err
			}
			if sc == nil {
				sc = &propTxSidecar[ID]{members: indexpkg.NewPropertyTxMembers[ID]()}
			}
			targets = append(targets, &propTxTarget[K, ID]{key: k, sc: sc, elements: idx.Elements(), scratch: indexpkg.NewPropertyTxMembers[ID]()})
		}
		// Publish all claims only after admission succeeds. A refused claim
		// leaves no running pass for another reader to wait on.
		if *sidecars == nil {
			*sidecars = make(map[K]*propTxSidecar[ID])
		}
		for _, target := range targets {
			(*sidecars)[target.key] = target.sc
			target.sc.pass = pass
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
func (bs *Store) scanRelPropTx(targets []*relPropTxTarget, tally *storecontract.ReadTally) error {
	byType := make(map[int][]*relPropTxTarget, len(targets))
	for _, t := range targets {
		tok := int(t.key.RelTypeToken)
		byType[tok] = append(byType[tok], t)
	}
	return bs.scanPropTxRowsUnder(storepkg.KeyRel, storepkg.KeyHistRel, tally, func(id snowflake.ID, hist bool, version uint64, raw []byte, local propertyAnchorFunc) error {
		return bs.recordRelRowPropTx(byType, id, hist, version, raw, local, tally)
	})
}

// scanNodePropTx fills every node target from one pass over the node rows.
func (bs *Store) scanNodePropTx(targets []*nodePropTxTarget, tally *storecontract.ReadTally) error {
	byLabel := make(map[uint16][]*nodePropTxTarget, len(targets))
	for _, t := range targets {
		byLabel[t.key.LabelToken] = append(byLabel[t.key.LabelToken], t)
	}
	return bs.scanPropTxRowsUnder(storepkg.KeyNode, storepkg.KeyHistNode, tally, func(id snowflake.ID, hist bool, version uint64, raw []byte, local propertyAnchorFunc) error {
		return bs.recordNodeRowPropTx(byLabel, id, hist, version, raw, local, tally)
	})
}

// recordRelRowPropTx decodes one stored rel row (current or history, full or delta)
// through the checked wire path, so the value key equals the index's, and records
// it into every target of its type. A row of an untargeted type is skipped
// before the full decode.
func (bs *Store) recordRelRowPropTx(byType map[int][]*relPropTxTarget, id snowflake.ID, hist bool, version uint64, raw []byte, local propertyAnchorFunc, tally *storecontract.ReadTally) error {
	r, err := bs.decodeScannedRelUnder(func(relType int) bool { return len(byType[relType]) > 0 }, id, hist, version, raw, local, tally)
	if r == nil || err != nil {
		return err
	}
	targets := byType[int(r.TypeToken().Value())]
	rid, ri := r.ID(), indexpkg.RelRowInfo(bs.validAxis.Get(), r)
	for _, t := range targets {
		vks, err := indexpkg.PropertyValueKeysAdmit(func(fn func(string) bool) {
			r.ForEachPropertyIndexKey(t.key.PropertyKey, t.elements, fn)
		}, propertyKeyAdmit(tally))
		if err != nil {
			return err
		}
		if err := t.rows.AddAdmit(rid, ri, !hist, vks, tally.Buffer); err != nil {
			return err
		}
	}
	return nil
}

// decodeScannedRel decodes one stored rel row (current or history, full or
// delta) through the checked wire path, so its value keys equal the index's;
// nil, nil for a row of a type want refuses, before the full decode.
func (bs *Store) decodeScannedRel(want func(relType int) bool, id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) (*types.Relationship, error) {
	return bs.decodeScannedRelUnder(want, id, hist, version, raw, func(av uint64) ([]byte, error) {
		return bs.fetchAnchorRaw(av, local, func() ([]byte, error) { return bs.readHistoryRelRaw(id, av) })
	}, nil)
}

func (bs *Store) decodeScannedRelUnder(want func(relType int) bool, id snowflake.ID, hist bool, version uint64, raw []byte, anchor propertyAnchorFunc, tally *storecontract.ReadTally) (*types.Relationship, error) {
	if err := tally.Decoded(len(raw)); err != nil {
		return nil, err
	}
	notePropertyMemberDecoded()
	var w storepkg.RelWire
	if hist && storepkg.HistoryValueKindOf(raw) == storepkg.HistoryDelta {
		d, err := storepkg.DecodeRelHistoryDelta(raw)
		if err != nil {
			return nil, err
		}
		if !want(d.Meta.RelType) {
			return nil, nil
		}
		anchorRaw, err := anchor(storepkg.AnchorVersionFor(version, bs.historyAnchorInterval))
		if err != nil {
			return nil, err
		}
		if err := tally.Decoded(len(anchorRaw)); err != nil {
			return nil, err
		}
		if storepkg.HistoryValueKindOf(anchorRaw) != storepkg.HistoryFull {
			return nil, fmt.Errorf("%w: relationship history anchor is not a full snapshot", storecontract.ErrCorruptWire)
		}
		notePropertyMemberDecoded()
		if err := storepkg.SafeUnmarshal(anchorRaw, &w); err != nil {
			return nil, err
		}
		w = storepkg.ApplyRelHistory(w, d)
	} else {
		if err := storepkg.SafeUnmarshal(raw, &w); err != nil {
			return nil, err
		}
		if !want(w.RelType) {
			return nil, nil
		}
	}
	if err := bs.resolveRelWireKeys(&w); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidStoreMutation, err)
	}
	if hist && storepkg.HistoryValueKindOf(raw) == storepkg.HistoryDelta {
		storepkg.SortWirePropertiesByKey(w.Properties)
	}
	r, err := storepkg.WireToRelChecked(w)
	if err != nil {
		return nil, err
	}
	if r.ID().SnowflakeID() != id {
		return nil, fmt.Errorf("%w: relationship wire id %d does not match key %d", ErrInvalidStoreMutation, r.ID().SnowflakeID(), id)
	}
	return r, nil
}

// decodeScannedNode is decodeScannedRel for a node row; want sees the row's
// wire labels (nodeWireHasTarget).
func (bs *Store) decodeScannedNode(want func(w *storepkg.NodeWire) bool, id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) (*types.Node, error) {
	return bs.decodeScannedNodeUnder(want, id, hist, version, raw, func(av uint64) ([]byte, error) {
		return bs.fetchAnchorRaw(av, local, func() ([]byte, error) { return bs.readHistoryNodeRaw(id, av) })
	}, nil)
}

func (bs *Store) decodeScannedNodeUnder(want func(w *storepkg.NodeWire) bool, id snowflake.ID, hist bool, version uint64, raw []byte, anchor propertyAnchorFunc, tally *storecontract.ReadTally) (*types.Node, error) {
	if err := tally.Decoded(len(raw)); err != nil {
		return nil, err
	}
	notePropertyMemberDecoded()
	var w storepkg.NodeWire
	if hist && storepkg.HistoryValueKindOf(raw) == storepkg.HistoryDelta {
		d, err := storepkg.DecodeNodeHistoryDelta(raw)
		if err != nil {
			return nil, err
		}
		if !want(&d.Meta) {
			return nil, nil
		}
		anchorRaw, err := anchor(storepkg.AnchorVersionFor(version, bs.historyAnchorInterval))
		if err != nil {
			return nil, err
		}
		if err := tally.Decoded(len(anchorRaw)); err != nil {
			return nil, err
		}
		if storepkg.HistoryValueKindOf(anchorRaw) != storepkg.HistoryFull {
			return nil, fmt.Errorf("%w: node history anchor is not a full snapshot", storecontract.ErrCorruptWire)
		}
		notePropertyMemberDecoded()
		if err := storepkg.SafeUnmarshal(anchorRaw, &w); err != nil {
			return nil, err
		}
		w = storepkg.ApplyNodeHistory(w, d)
	} else {
		if err := storepkg.SafeUnmarshal(raw, &w); err != nil {
			return nil, err
		}
		if !want(&w) {
			return nil, nil
		}
	}
	if err := bs.resolveNodeWireKeys(&w); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidStoreMutation, err)
	}
	if hist && storepkg.HistoryValueKindOf(raw) == storepkg.HistoryDelta {
		storepkg.SortWirePropertiesByKey(w.Properties)
	}
	n, err := storepkg.WireToNodeChecked(w)
	if err != nil {
		return nil, err
	}
	if n.ID().SnowflakeID() != id {
		return nil, fmt.Errorf("%w: node wire id %d does not match key %d", ErrInvalidStoreMutation, n.ID().SnowflakeID(), id)
	}
	return n, nil
}

// recordNodeRowPropTx is recordRelRowPropTx for node rows: the row is recorded
// into the targets of every label it carries.
func (bs *Store) recordNodeRowPropTx(byLabel map[uint16][]*nodePropTxTarget, id snowflake.ID, hist bool, version uint64, raw []byte, local propertyAnchorFunc, tally *storecontract.ReadTally) error {
	n, err := bs.decodeScannedNodeUnder(func(w *storepkg.NodeWire) bool { return nodeWireHasTarget(w, byLabel) }, id, hist, version, raw, local, tally)
	if n == nil || err != nil {
		return err
	}
	nid, ri := n.ID(), indexpkg.NodeRowInfo(bs.validAxis.Get(), n)
	for i := 0; i < n.LabelTokenCount(); i++ {
		for _, t := range byLabel[n.LabelTokenRawAt(i)] {
			vks, err := indexpkg.PropertyValueKeysAdmit(func(fn func(string) bool) {
				n.ForEachPropertyIndexKey(t.key.PropertyKey, t.elements, fn)
			}, propertyKeyAdmit(tally))
			if err != nil {
				return err
			}
			if err := t.rows.AddAdmit(nid, ri, !hist, vks, tally.Buffer); err != nil {
				return err
			}
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
// and history rows (histKind; 0 reads none). The write-buffer overlay is captured FIRST, then the
// badger view opened; a buffered op masks its committed key. A
// history delta resolves its anchor from the overlay, then earlier committed rows
// of the same entity, then the view.
func (bs *Store) scanPropTxRows(curKind, histKind byte, visit func(id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) error) error {
	return bs.scanPropTxRowsUnder(curKind, histKind, nil, func(id snowflake.ID, hist bool, version uint64, raw []byte, local propertyAnchorFunc) error {
		var anchor localAnchorFunc
		var anchorErr error
		if local != nil {
			anchor = func(av uint64) ([]byte, bool) {
				var b []byte
				b, anchorErr = local(av)
				return b, anchorErr == nil
			}
		}
		err := visit(id, hist, version, raw, anchor)
		if anchorErr != nil {
			return anchorErr
		}
		return err
	})
}

// propertyAnchorFunc returns an admitted anchor or an error. A budget refusal
// must never become a cache miss followed by an unmetered point read.
type propertyAnchorFunc func(version uint64) ([]byte, error)

func (bs *Store) scanPropTxRowsUnder(curKind, histKind byte, tally *storecontract.ReadTally, visit func(id snowflake.ID, hist bool, version uint64, raw []byte, local propertyAnchorFunc) error) error {
	overlay := make(map[string]writeOp)
	var refused error
	bs.walkPending(func(k string, op writeOp) bool {
		if refused = tally.Candidates(1); refused != nil {
			return false
		}
		if len(k) == 0 {
			return true
		}
		switch {
		case k[0] == curKind && len(k) == storepkg.SizeNodeKey:
		case k[0] == histKind && len(k) == storepkg.SizeHistKey: // no key starts with 0, so histKind 0 matches none
			if refused = tally.History(1); refused != nil {
				return false
			}
		default:
			return true
		}
		// The overlay retains a logical key/writeOp slot and owns the value copy.
		if refused = tally.Buffer(72 + len(op.value)); refused != nil {
			return false
		}
		op.value = append([]byte(nil), op.value...)
		notePropertyMemberBytesCopied(len(op.value))
		overlay[k] = op // flushing first, then pending: the newer op wins
		return true
	})
	if refused != nil {
		return refused
	}
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
		anchorFor := func(id snowflake.ID, cached map[uint64][]byte) propertyAnchorFunc {
			return func(av uint64) ([]byte, error) {
				if err := tally.History(1); err != nil {
					return nil, err
				}
				ak := histKey(id, av)
				if op, ok := overlay[ak]; ok {
					if op.opType == writeOpDelete {
						return nil, ErrVersionNotFound
					}
					return op.value, nil
				}
				if raw, ok := cached[av]; ok {
					return raw, nil
				}
				if err := tally.Candidates(1); err != nil {
					return nil, err
				}
				item, err := txn.Get([]byte(ak))
				if err == badgerv4.ErrKeyNotFound {
					return nil, ErrVersionNotFound
				}
				if err != nil {
					return nil, err
				}
				var raw []byte
				err = item.Value(func(val []byte) error {
					if err := tally.Buffer(len(val)); err != nil {
						return err
					}
					raw = append([]byte(nil), val...)
					notePropertyMemberBytesCopied(len(val))
					return nil
				})
				return raw, err
			}
		}

		// Committed current rows the overlay does not resolve.
		prefix := []byte{curKind}
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err := tally.Candidates(1); err != nil {
				it.Close()
				return err
			}
			noteIndexEntryVisited()
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

		// Committed history rows, one entity's versions after another (none for
		// histKind 0).
		if histKind != 0 {
			var groupID snowflake.ID
			anchors := make(map[uint64][]byte)
			prefix = []byte{histKind}
			opts.Prefix = prefix
			it = txn.NewIterator(opts)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				if err := tally.Candidates(1); err != nil {
					it.Close()
					return err
				}
				if err := tally.History(1); err != nil {
					it.Close()
					return err
				}
				noteIndexEntryVisited()
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
						if err := tally.Buffer(32 + len(val)); err != nil {
							return err
						}
						anchors[version] = append([]byte(nil), val...)
						notePropertyMemberBytesCopied(len(val))
					}
					return visit(id, true, version, val, anchorFor(id, anchors))
				}); err != nil {
					it.Close()
					return err
				}
			}
			it.Close()
		}

		// Buffered rows (strictly newer than anything committed).
		for k, op := range overlay {
			if err := tally.Candidates(1); err != nil {
				return err
			}
			if k[0] == histKind {
				if err := tally.History(1); err != nil {
					return err
				}
			}
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
			if err := visit(id, true, historyVersionFromKey(kb), op.value, anchorFor(id, nil)); err != nil {
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
