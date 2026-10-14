package tiered

import (
	"errors"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Transaction-time membership sidecars and valid-end indexes on the tiered
// store (backlog 58, sigma-tkgd R2). Every shard is a badger store keeping
// its own sidecars and valid-end index over its own rows, so the store-wide
// answer is the fan-out over every shard that can hold a row: the reference
// shard, the archive (when one exists) and every event shard. A closed cold
// shard is opened, as the DepthAll history fold the answers replace opens it;
// the core takes these answers for DepthAll reads only.
//
// An entity's rows can sit on more than one shard (ArchiveNode moves the
// current row to the archive and leaves the history on the reference shard;
// a re-imported entity's earlier lives stay on their shard), so per-entity
// answers are merged: each entity is emitted once, with the lowest first
// transaction time any shard knows (indexpkg.MergeFirstTx: 0, unknown,
// wins). Membership is append-only on every shard, so the union is the
// whole store's ever-membership. fn runs after every shard is checked back
// in, outside every lock.

var (
	_ storecontract.LabelTxMembershipCapability        = (*Store)(nil)
	_ storecontract.RelTypeTxMembershipCapability      = (*Store)(nil)
	_ storecontract.LabelTxMemberLookupCapability      = (*Store)(nil)
	_ storecontract.RelTypeTxMemberLookupCapability    = (*Store)(nil)
	_ storecontract.NodePropertyTxMembershipCapability = (*Store)(nil)
	_ storecontract.RelPropertyTxMembershipCapability  = (*Store)(nil)
	_ storecontract.RelValidEndCapability              = (*Store)(nil)
	_ storecontract.NodeValidEndCapability             = (*Store)(nil)
)

// allShards runs read on the reference shard, the archive (when one exists)
// and every event shard (open ones concurrently, cold ones one at a time,
// lazily opened), returning the reference shard's result first, then the
// others' (ref tells read which shard it reads). read must be safe for
// concurrent use.
func allShards[T any](ts *Store, read func(bs *BadgerStore, ref bool) (T, error)) (ref T, others []T, err error) {
	if err = ts.checkOpen(); err != nil {
		return ref, nil, err
	}
	refStore, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return ref, nil, err
	}
	ref, err = read(refStore, true)
	refCheckin()
	if err != nil {
		return ref, nil, err
	}
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return ref, nil, err
	}
	if archive != nil {
		v, err := read(archive, false)
		archiveCheckin()
		if err != nil {
			return ref, nil, err
		}
		others = append(others, v)
	}
	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()
	results := make([]T, len(shards))
	errs := make([]error, len(shards))
	queryEventShards(shards, func(i int, es *EventShard) {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			errs[i] = err
			return
		}
		defer release()
		results[i], errs[i] = read(store, false)
	})
	if err := errors.Join(errs...); err != nil {
		return ref, nil, err
	}
	return ref, append(others, results...), nil
}

// txMember is one (entity, first transaction time) pair of a shard.
type txMember[ID comparable] struct {
	id ID
	tx types.Instant
}

// txMemberUnion merges per-shard member lists: each entity once, in first-seen
// order, with the merged first transaction time.
type txMemberUnion[ID comparable] struct {
	tx    map[ID]types.Instant
	order []ID
}

func (u *txMemberUnion[ID]) add(ms []txMember[ID]) {
	if u.tx == nil {
		u.tx = make(map[ID]types.Instant)
	}
	for _, m := range ms {
		if prev, ok := u.tx[m.id]; ok {
			u.tx[m.id] = indexpkg.MergeFirstTx(prev, m.tx)
			continue
		}
		u.tx[m.id] = m.tx
		u.order = append(u.order, m.id)
	}
}

func (u *txMemberUnion[ID]) emit(fn func(ID, types.Instant) bool) {
	for _, id := range u.order {
		if !fn(id, u.tx[id]) {
			return
		}
	}
}

// unionTxMembers fans each shard's enumeration out and streams the union.
func unionTxMembers[ID comparable](ts *Store, each func(*BadgerStore, func(ID, types.Instant) bool) error, fn func(ID, types.Instant) bool) error {
	if fn == nil {
		return errNilIterationCallback()
	}
	read := func(bs *BadgerStore, _ bool) ([]txMember[ID], error) {
		var out []txMember[ID]
		err := each(bs, func(id ID, tx types.Instant) bool {
			out = append(out, txMember[ID]{id, tx})
			return true
		})
		return out, err
	}
	ref, others, err := allShards(ts, read)
	if err != nil {
		return err
	}
	var u txMemberUnion[ID]
	u.add(ref)
	for _, ms := range others {
		u.add(ms)
	}
	u.emit(fn)
	return nil
}

// ForEachLabelTxMember implements store.LabelTxMembershipCapability: the
// union of every shard's ever-members of token.
func (ts *Store) ForEachLabelTxMember(token uint16, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	return unionTxMembers(ts, func(bs *BadgerStore, f func(types.NodeID, types.Instant) bool) error {
		return bs.ForEachLabelTxMember(token, f)
	}, fn)
}

// ForEachRelTypeTxMember implements store.RelTypeTxMembershipCapability.
func (ts *Store) ForEachRelTypeTxMember(token uint16, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return unionTxMembers(ts, func(bs *BadgerStore, f func(types.RelID, types.Instant) bool) error {
		return bs.ForEachRelTypeTxMember(token, f)
	}, fn)
}

// LabelTxMembersOf implements store.LabelTxMemberLookupCapability: each ID of
// ids any shard knows as an ever-member of token, once.
func (ts *Store) LabelTxMembersOf(token uint16, ids []types.NodeID, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	return unionTxMembers(ts, func(bs *BadgerStore, f func(types.NodeID, types.Instant) bool) error {
		return bs.LabelTxMembersOf(token, ids, f)
	}, fn)
}

// RelTypeTxMembersOf implements store.RelTypeTxMemberLookupCapability.
func (ts *Store) RelTypeTxMembersOf(token uint16, ids []types.RelID, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return unionTxMembers(ts, func(bs *BadgerStore, f func(types.RelID, types.Instant) bool) error {
		return bs.RelTypeTxMembersOf(token, ids, f)
	}, fn)
}

// errLabelOffReference marks a shard other than the reference shard that
// holds an ever-member of the label (ForEachNodePropertyTxMember).
var errLabelOffReference = errors.New("label member off the reference shard")

// ForEachNodePropertyTxMember implements
// store.NodePropertyTxMembershipCapability. A node property index lives on
// the reference shard only (CreatePropertyIndex), so its sidecar sees only
// that shard's rows. It answers from that sidecar when no other shard (the
// archive, an event shard: an archived node, an event node carrying the
// label as a secondary one) ever held a node with the label; otherwise, or
// when the index is not declared or still being created on the reference
// shard, it declines with store.ErrIndexNotFound and the core takes the
// history fold.
func (ts *Store) ForEachNodePropertyTxMember(labelToken uint16, propertyKey, valueKey string, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	if fn == nil {
		return errNilIterationCallback()
	}
	ref, _, err := allShards(ts, func(bs *BadgerStore, isRef bool) ([]txMember[types.NodeID], error) {
		if isRef {
			var out []txMember[types.NodeID]
			err := bs.ForEachNodePropertyTxMember(labelToken, propertyKey, valueKey, func(id types.NodeID, tx types.Instant) bool {
				out = append(out, txMember[types.NodeID]{id, tx})
				return true
			})
			return out, err
		}
		held := false
		if err := bs.ForEachLabelTxMember(labelToken, func(types.NodeID, types.Instant) bool {
			held = true
			return false
		}); err != nil {
			return nil, err
		}
		if held {
			return nil, errLabelOffReference
		}
		return nil, nil
	})
	if errors.Is(err, errLabelOffReference) {
		return storecontract.ErrIndexNotFound
	}
	if err != nil {
		return err
	}
	var u txMemberUnion[types.NodeID]
	u.add(ref)
	u.emit(fn)
	return nil
}

// ForEachRelPropertyTxMember implements
// store.RelPropertyTxMembershipCapability. The tiered store never holds a
// relationship property index (CreateRelPropertyIndex is unsupported), so
// it always declines with store.ErrIndexNotFound, as a store without the
// declared index does.
func (ts *Store) ForEachRelPropertyTxMember(relTypeToken uint16, propertyKey, valueKey string, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	return storecontract.ErrIndexNotFound
}

// mergeValidEndCandidates merges per-shard valid-end candidates: an entity
// listed by several shards gets the highest stamp and bound (an unknown
// listing, Stamp ValidEndOpen, wins). The union is a sound superset: each
// shard's fold bounds the segments of its own rows (a delete tombstone caps
// only the rows of its own shard, which leaves the other shards' bounds
// looser, never tighter), and a shard lists an entity whenever one of its
// rows was stamped after the pin. Gen is 0: Refine is a no-op on tiered.
func mergeValidEndCandidates(ref []storecontract.ValidEndCandidate, others [][]storecontract.ValidEndCandidate) []storecontract.ValidEndCandidate {
	if len(others) == 0 {
		for i := range ref {
			ref[i].Gen = 0
		}
		return ref
	}
	pos := make(map[int64]int, len(ref))
	var out []storecontract.ValidEndCandidate
	add := func(cs []storecontract.ValidEndCandidate) {
		for _, c := range cs {
			c.Gen = 0
			i, ok := pos[c.ID]
			if !ok {
				pos[c.ID] = len(out)
				out = append(out, c)
				continue
			}
			m := &out[i]
			if m.Stamp == storecontract.ValidEndOpen || c.Stamp == storecontract.ValidEndOpen {
				m.Stamp, m.Bound = storecontract.ValidEndOpen, storecontract.ValidEndOpen
				continue
			}
			m.Stamp, m.Bound = max(m.Stamp, c.Stamp), max(m.Bound, c.Bound)
		}
	}
	add(ref)
	for _, cs := range others {
		add(cs)
	}
	return out
}

// RelValidEndCandidates implements store.RelValidEndCapability over every
// shard's valid-end index (each tracks the type on its first call, from its
// own membership sidecar).
func (ts *Store) RelValidEndCandidates(typeToken uint16, pin, validFrom types.Instant) ([]storecontract.ValidEndCandidate, error) {
	ref, others, err := allShards(ts, func(bs *BadgerStore, _ bool) ([]storecontract.ValidEndCandidate, error) {
		return bs.RelValidEndCandidates(typeToken, pin, validFrom)
	})
	if err != nil {
		return nil, err
	}
	return mergeValidEndCandidates(ref, others), nil
}

// RefineRelValidEnd implements store.RelValidEndCapability as a no-op: a
// shard's refined bound replaces its fold until the next write on that shard,
// but the entity's rows can span shards (ArchiveNode, a re-import), and a
// write on another shard would leave the refined bound standing for rows it
// no longer bounds. An entity with a loose fold (a valid-time close leaves
// the closed version's open predecessor in history) is read on every call.
func (ts *Store) RefineRelValidEnd(types.RelID, uint64, types.Instant) {}

// NodeValidEndCandidates implements store.NodeValidEndCapability (see
// RelValidEndCandidates).
func (ts *Store) NodeValidEndCandidates(labelTokens []uint16, pin, validFrom types.Instant) ([]storecontract.ValidEndCandidate, error) {
	ref, others, err := allShards(ts, func(bs *BadgerStore, _ bool) ([]storecontract.ValidEndCandidate, error) {
		return bs.NodeValidEndCandidates(labelTokens, pin, validFrom)
	})
	if err != nil {
		return nil, err
	}
	return mergeValidEndCandidates(ref, others), nil
}

// RefineNodeValidEnd implements store.NodeValidEndCapability as a no-op (see
// RefineRelValidEnd).
func (ts *Store) RefineNodeValidEnd(types.NodeID, uint64, types.Instant) {}
