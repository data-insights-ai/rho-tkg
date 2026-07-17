package memory

import (
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
// seen. No-op until the sidecar is built. Caller holds ms.mu.
func (ms *Store) recordLabelMemberLocked(tok uint16, id types.NodeID, txFrom types.Instant) {
	if ms.labelTxMembers == nil {
		return // not built yet — the lazy build will capture current state
	}
	set := ms.labelTxMembers[tok]
	if set == nil {
		set = make(map[types.NodeID]types.Instant)
		ms.labelTxMembers[tok] = set
	}
	if prev, ok := set[id]; !ok || (txFrom != 0 && (prev == 0 || txFrom < prev)) {
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
	if prev, ok := set[id]; !ok || (tx != 0 && (prev == 0 || tx < prev)) {
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
	if ms.labelTxMembers != nil {
		return
	}
	built := make(map[uint16]map[types.NodeID]types.Instant)
	ms.labelTxMembers = built
	for _, n := range ms.nodes {
		ms.recordNodeLabelMembersLocked(n)
	}
	for _, versions := range ms.nodeHistory {
		for _, n := range versions {
			ms.recordNodeLabelMembersLocked(n)
		}
	}
}

// ensureRelTypeTxMembersBuiltLocked builds the rel-type sidecar from current +
// history relationship state on first use. Caller holds ms.mu (write).
func (ms *Store) ensureRelTypeTxMembersBuiltLocked() {
	if ms.relTypeTxMembers != nil {
		return
	}
	ms.relTypeTxMembers = make(map[uint16]map[types.RelID]types.Instant)
	for _, r := range ms.rels {
		ms.recordRelTypeMemberLocked(r)
	}
	for _, versions := range ms.relHistory {
		for _, r := range versions {
			ms.recordRelTypeMemberLocked(r)
		}
	}
}

// ForEachLabelTxMember implements store.LabelTxMembershipCapability.
func (ms *Store) ForEachLabelTxMember(token uint16, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	ms.mu.Lock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.Unlock()
		return err
	}
	ms.ensureLabelTxMembersBuiltLocked()
	// Snapshot into a slice so fn runs OUTSIDE the store lock (fn re-enters the
	// store to resolve chains — holding ms.mu across it would deadlock).
	set := ms.labelTxMembers[token]
	type member struct {
		id types.NodeID
		tx types.Instant
	}
	members := make([]member, 0, len(set))
	for id, tx := range set {
		members = append(members, member{id: id, tx: tx})
	}
	ms.mu.Unlock()
	for _, m := range members {
		if !fn(m.id, m.tx) {
			return nil
		}
	}
	return nil
}

// ForEachRelTypeTxMember implements store.RelTypeTxMembershipCapability.
func (ms *Store) ForEachRelTypeTxMember(token uint16, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	ms.mu.Lock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.Unlock()
		return err
	}
	ms.ensureRelTypeTxMembersBuiltLocked()
	set := ms.relTypeTxMembers[token]
	type member struct {
		id types.RelID
		tx types.Instant
	}
	members := make([]member, 0, len(set))
	for id, tx := range set {
		members = append(members, member{id: id, tx: tx})
	}
	ms.mu.Unlock()
	for _, m := range members {
		if !fn(m.id, m.tx) {
			return nil
		}
	}
	return nil
}
