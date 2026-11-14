package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Transaction-time label/rel-type membership sidecar (memory arm).
//
// A history-aware ByLabel/ByType scan (one carrying a temporal filter) must
// consider every node/rel whose PAST version could match the queried label/type
// at the pinned instant — not just the current index members. Without this
// sidecar the core layer folds ALL node/rel history into the candidate set (temporal.go
// forEachNodeCandidateID), so a pinned scan for a selective label cost
// O(everything that ever carried ANY label), not O(matches).
//
// These sidecars scope that fold to the label/type's ever-members. They are
// SOUND SUPERSETS (append-only; a removed/deleted member is retained so a pin
// before the removal still admits it) and the core chain resolver remains the
// correctness authority — an over-included candidate is rejected there, never
// mis-reported. firstTxFrom is a lower bound on the member's earliest
// acquisition transaction time, letting the core prune `pin < firstTxFrom`.
//
// LAZY: nil until the first ForEach*TxMember call builds it from the current +
// history state; afterwards every add-site keeps it fresh incrementally (removal
// sites are no-ops — append-only). Matches the OPT15 relValidIdx precedent, so a
// graph that never runs a pinned scan pays nothing.

// recordLabelMemberLocked records node id as an ever-member of tok with the
// given acquisition transaction time, keeping the earliest (lowest) firstTxFrom
// seen; 0 (an unstamped row, visible to a TxAt read at every pin) is "unknown"
// and is never raised (index.MergeFirstTx). No-op until the sidecar is built. Caller holds ms.mu.
func (ms *Store) recordLabelMemberLocked(tok uint16, id types.NodeID, txFrom types.Instant) {
	if ms.labelTxMembers == nil {
		return // not built yet — the lazy build will capture current state
	}
	set := ms.labelTxMembers[tok]
	if set == nil {
		set = make(map[types.NodeID]types.Instant)
		ms.labelTxMembers[tok] = set
	}
	if prev, ok := set[id]; ok {
		set[id] = indexpkg.MergeFirstTx(prev, txFrom)
	} else {
		set[id] = txFrom
	}
}

// recordNodeLabelMembersLocked records every label token n carries with n's
// transaction-time stamp. Called at every door where a node acquires a label
// (create, label-add, history-version insert). Caller holds ms.mu.
func (ms *Store) recordNodeLabelMembersLocked(n *types.Node) {
	if ms.labelTxMembers == nil || n == nil {
		return
	}
	tx := nodeTxFrom(n)
	id := n.ID()
	count := n.LabelTokenCount()
	for i := 0; i < count; i++ {
		ms.recordLabelMemberLocked(n.LabelTokenRawAt(i), id, tx)
	}
}

// recordRelTypeMemberLocked records rel id as an ever-member of its type token.
// Caller holds ms.mu.
func (ms *Store) recordRelTypeMemberLocked(r *types.Relationship) {
	if ms.relTypeTxMembers == nil || r == nil {
		return
	}
	tok := r.TypeToken().Value()
	if tok == 0 {
		return
	}
	tx := relTxFrom(r)
	id := r.ID()
	set := ms.relTypeTxMembers[tok]
	if set == nil {
		set = make(map[types.RelID]types.Instant)
		ms.relTypeTxMembers[tok] = set
	}
	if prev, ok := set[id]; ok {
		set[id] = indexpkg.MergeFirstTx(prev, tx)
	} else {
		set[id] = tx
	}
}

func nodeTxFrom(n *types.Node) types.Instant {
	if tm := n.Temporal(); tm != nil {
		return tm.TxFrom
	}
	return 0
}

func relTxFrom(r *types.Relationship) types.Instant {
	if tm := r.Temporal(); tm != nil {
		return tm.TxFrom
	}
	return 0
}

// ensureLabelTxMembersBuiltLocked builds the label sidecar from the current +
// history node state on first use. Caller holds ms.mu (write).
func (ms *Store) ensureLabelTxMembersBuiltLocked() {
	_ = ms.ensureLabelTxMembersBuiltUnderLocked(nil)
}

func (ms *Store) ensureLabelTxMembersBuiltUnderLocked(t *storecontract.ReadTally) (err error) {
	if ms.labelTxMembers != nil {
		return nil
	}
	ms.labelTxMembers = make(map[uint16]map[types.NodeID]types.Instant)
	defer func() {
		if err != nil {
			ms.labelTxMembers = nil
		}
	}()
	for _, n := range ms.nodes {
		if err := t.Candidates(1); err != nil {
			return err
		}
		noteTxMemberBuildRow()
		ms.recordNodeLabelMembersLocked(n)
	}
	for _, versions := range ms.nodeHistory {
		for _, n := range versions {
			if err := t.Candidates(1); err != nil {
				return err
			}
			if err := t.History(1); err != nil {
				return err
			}
			noteTxMemberBuildRow()
			ms.recordNodeLabelMembersLocked(n)
		}
	}
	return nil
}

// ensureRelTypeTxMembersBuiltLocked builds the rel-type sidecar from current +
// history relationship state on first use. Caller holds ms.mu (write).
func (ms *Store) ensureRelTypeTxMembersBuiltLocked() error {
	return ms.ensureRelTypeTxMembersBuiltUnderLocked(nil)
}

func (ms *Store) ensureRelTypeTxMembersBuiltUnderLocked(t *storecontract.ReadTally) (err error) {
	if ms.relTypeTxMembers != nil {
		return nil
	}
	ms.relTypeTxMembers = make(map[uint16]map[types.RelID]types.Instant)
	defer func() {
		if err != nil {
			ms.relTypeTxMembers = nil
		}
	}()
	// The sealed walker decodes pages before invoking its callback. Admit
	// its whole pass before entering it, including rows it rejects as dead.
	if err := t.Candidates(len(ms.rels)); err != nil {
		return err
	}
	for _, st := range ms.segTypes {
		if err := t.Candidates(int(st.sealedRows)); err != nil {
			return err
		}
		if err := t.Decoded(int(st.segBytes)); err != nil {
			return err
		}
	}
	// ADR-0011: every current row, sealed ones included. The sidecar must be
	// a superset, so a decode error leaves it unbuilt and fails the read.
	if err := ms.forEachCurrentRelLocked(func(r *types.Relationship) bool {
		noteTxMemberBuildRow()
		ms.recordRelTypeMemberLocked(r)
		return true
	}); err != nil {
		ms.relTypeTxMembers = nil
		return err
	}
	for _, versions := range ms.relHistory {
		for _, r := range versions {
			if err := t.Candidates(1); err != nil {
				return err
			}
			if err := t.History(1); err != nil {
				return err
			}
			noteTxMemberBuildRow()
			ms.recordRelTypeMemberLocked(r)
		}
	}
	return nil
}

// ForEachLabelTxMember implements store.LabelTxMembershipCapability. A built
// sidecar is read under ms.mu.RLock (readOrBuild); only the first call builds
// under the write lock.
func (ms *Store) ForEachLabelTxMember(token uint16, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	return ms.ForEachLabelTxMemberMetered(token, nil, fn)
}

// ForEachLabelTxMemberMetered implements
// store.TxMemberGatherMeteredCapability: ForEachLabelTxMember with the members
// of the native set charged as Candidates and its copy as BufferBytes,
// both before it is made.
func (ms *Store) ForEachLabelTxMemberMetered(token uint16, t *storecontract.ReadTally, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	return emitMemoryTxMemberCopy(ms, func() map[types.NodeID]types.Instant { return ms.labelTxMembers[token] },
		func() bool { return ms.labelTxMembers != nil },
		func() error { return ms.ensureLabelTxMembersBuiltUnderLocked(t) }, t, fn)
}

// ForEachRelTypeTxMember implements store.RelTypeTxMembershipCapability.
func (ms *Store) ForEachRelTypeTxMember(token uint16, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return ms.ForEachRelTypeTxMemberMetered(token, nil, fn)
}

// ForEachRelTypeTxMemberMetered is ForEachLabelTxMemberMetered for a
// relationship type.
func (ms *Store) ForEachRelTypeTxMemberMetered(token uint16, t *storecontract.ReadTally, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	return emitMemoryTxMemberCopy(ms, func() map[types.RelID]types.Instant { return ms.relTypeTxMembers[token] },
		func() bool { return ms.relTypeTxMembers != nil },
		func() error { return ms.ensureRelTypeTxMembersBuiltUnderLocked(t) }, t, fn)
}

// emitMemoryTxMemberCopy copies the set under its owning lock and invokes fn
// outside it. Every copied entry is native work, including current members
// repeated by this snapshot. Deducting a later current count would let a writer
// add members between the two snapshots without paying for them.
func emitMemoryTxMemberCopy[ID comparable](ms *Store, setOf func() map[ID]types.Instant, built func() bool, build func() error, t *storecontract.ReadTally, fn func(ID, types.Instant) bool) error {
	var members []indexpkg.TxMember[ID]
	snapshot := func() error {
		set := setOf()
		if err := t.Candidates(len(set)); err != nil {
			return err
		}
		if err := t.Buffer(storecontract.TxMemberCopyBytes * len(set)); err != nil {
			return err
		}
		members = make([]indexpkg.TxMember[ID], 0, len(set))
		for id, tx := range set {
			members = append(members, indexpkg.TxMember[ID]{ID: id, FirstTx: tx})
		}
		noteTxMembersCopied(len(members))
		return nil
	}
	err := ms.readOrBuild(func() (bool, error) {
		if !built() {
			return false, nil
		}
		return true, snapshot()
	}, func() error {
		if err := build(); err != nil {
			return err
		}
		return snapshot()
	})
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

// txMembersOf returns the members of set among ids (point lookups).
func txMembersOf[ID comparable](set map[ID]types.Instant, ids []ID) []indexpkg.TxMember[ID] {
	var out []indexpkg.TxMember[ID]
	for _, id := range ids {
		if tx, ok := set[id]; ok {
			out = append(out, indexpkg.TxMember[ID]{ID: id, FirstTx: tx})
		}
	}
	return out
}

// LabelTxMembersOf implements store.LabelTxMemberLookupCapability over the
// same sidecar as ForEachLabelTxMember.
func (ms *Store) LabelTxMembersOf(token uint16, ids []types.NodeID, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	var members []indexpkg.TxMember[types.NodeID]
	err := ms.readOrBuild(func() (bool, error) {
		if ms.labelTxMembers == nil {
			return false, nil
		}
		members = txMembersOf(ms.labelTxMembers[token], ids)
		return true, nil
	}, func() error {
		ms.ensureLabelTxMembersBuiltLocked()
		members = txMembersOf(ms.labelTxMembers[token], ids)
		return nil
	})
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

// RelTypeTxMembersOf implements store.RelTypeTxMemberLookupCapability.
func (ms *Store) RelTypeTxMembersOf(token uint16, ids []types.RelID, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	var members []indexpkg.TxMember[types.RelID]
	err := ms.readOrBuild(func() (bool, error) {
		if ms.relTypeTxMembers == nil {
			return false, nil
		}
		members = txMembersOf(ms.relTypeTxMembers[token], ids)
		return true, nil
	}, func() error {
		if err := ms.ensureRelTypeTxMembersBuiltLocked(); err != nil {
			return err
		}
		members = txMembersOf(ms.relTypeTxMembers[token], ids)
		return nil
	})
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
