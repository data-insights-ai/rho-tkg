package tiered

import (
	"errors"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Transaction-time membership sidecars (label, type, and since WP-31 node and
// relationship property) and valid-end indexes on the tiered store (backlog 58). Each shard keeps its own over its own rows, so the
// store-wide answer fans out over the reference shard, the archive (if any)
// and every event shard (closed cold shards are opened, as the DepthAll
// history fold does); the core uses these for DepthAll reads only.
//
// An entity's rows can sit on several shards (ArchiveNode leaves history on
// the reference shard), so per-entity answers are merged: emitted once with
// the lowest first tx time (indexpkg.MergeFirstTx: 0, unknown, wins).
// Membership is append-only, so the union is the ever-membership. fn runs
// after every shard is checked in, outside every lock.

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
// and every event shard (open ones concurrently, cold ones one at a time),
// returning the reference result first (ref tells read which shard it is).
// read must be safe for concurrent use.
//
// The reference and archive reads run under moveMu's read lock, which
// ArchiveNode and RestoreNode hold for their whole move. Without it a move
// between the two reads can hide a node from both (RestoreNode writes the
// reference shard, then hard-deletes from the archive without a tombstone).
func allShards[T any](ts *Store, read func(bs *BadgerStore, ref bool) (T, error)) (ref T, others []T, err error) {
	if err = ts.checkOpen(); err != nil {
		return ref, nil, err
	}
	ref, archived, hasArchive, err := refAndArchive(ts, read)
	if err != nil {
		return ref, nil, err
	}
	if hasArchive {
		others = append(others, archived)
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

// refAndArchive reads the reference shard, then the archive (when one
// exists), under moveMu's read lock (see allShards).
func refAndArchive[T any](ts *Store, read func(bs *BadgerStore, ref bool) (T, error)) (ref, archived T, hasArchive bool, err error) {
	ts.moveMu.RLock()
	defer ts.moveMu.RUnlock()
	refStore, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return ref, archived, false, err
	}
	ref, err = read(refStore, true)
	refCheckin()
	if err != nil {
		return ref, archived, false, err
	}
	ts.refArchiveGapHook()
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil || archive == nil {
		return ref, archived, false, err
	}
	defer archiveCheckin()
	archived, err = read(archive, false)
	return ref, archived, err == nil, err
}

// refArchiveGapHook runs the test-only fanoutRefReadHook between a door's
// reference and archive reads: the window a move could land in.
func (ts *Store) refArchiveGapHook() {
	if h := ts.fanoutRefReadHook; h != nil {
		h()
	}
}

// noteTypeScan reports a whole-label, whole-type or whole-history scan door to
// the test-only typeScanHook (a keyed read must never reach one).
func (ts *Store) noteTypeScan(door string) {
	if h := ts.typeScanHook; h != nil {
		h(door)
	}
}

// moveReadLock is a read hold on moveMu released by a door once its reference
// and archive reads are done (unlock is idempotent). Lock order: moveMu before
// ts.mu and archiveMu; a holder must not re-enter a door taking moveMu (a
// waiting move would block the second read lock).
type moveReadLock struct {
	ts   *Store
	held bool
}

// lockMoveRead read-locks moveMu, so an ArchiveNode or RestoreNode cannot run
// between the caller's reference shard read and its archive read.
func (ts *Store) lockMoveRead() moveReadLock {
	ts.moveMu.RLock()
	return moveReadLock{ts: ts, held: true}
}

func (l *moveReadLock) unlock() {
	if l.held {
		l.held = false
		l.ts.moveMu.RUnlock()
	}
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
	ts.noteTypeScan("ForEachLabelTxMember")
	return unionTxMembers(ts, func(bs *BadgerStore, f func(types.NodeID, types.Instant) bool) error {
		return bs.ForEachLabelTxMember(token, f)
	}, fn)
}

// ForEachRelTypeTxMember implements store.RelTypeTxMembershipCapability.
func (ts *Store) ForEachRelTypeTxMember(token uint16, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	ts.noteTypeScan("ForEachRelTypeTxMember")
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

// ForEachNodePropertyTxMember implements
// store.NodePropertyTxMembershipCapability over every shard's sidecar (see
// unionPropertyTxMembers). On decline the core takes the history fold.
func (ts *Store) ForEachNodePropertyTxMember(labelToken uint16, propertyKey, valueKey string, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	return unionPropertyTxMembers(ts, func(bs *BadgerStore) (storecontract.PropertyIndexOptions, bool, error) {
		return bs.PropertyIndexOptions(labelToken, propertyKey)
	}, func(bs *BadgerStore, f func(types.NodeID, types.Instant) bool) error {
		return bs.ForEachNodePropertyTxMember(labelToken, propertyKey, valueKey, f)
	}, fn)
}

// ForEachRelPropertyTxMember implements store.RelPropertyTxMembershipCapability
// (see unionPropertyTxMembers).
func (ts *Store) ForEachRelPropertyTxMember(relTypeToken uint16, propertyKey, valueKey string, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return unionPropertyTxMembers(ts, func(bs *BadgerStore) (storecontract.PropertyIndexOptions, bool, error) {
		return bs.RelPropertyIndexOptions(relTypeToken, propertyKey)
	}, func(bs *BadgerStore, f func(types.RelID, types.Instant) bool) error {
		return bs.ForEachRelPropertyTxMember(relTypeToken, propertyKey, valueKey, f)
	}, fn)
}

// unionPropertyTxMembers serves a property membership sidecar (WP-31): every
// shard keeps its own over its own rows, and an entity's rows can sit on
// several shards, so the answer is the union of every shard's members
// (txMemberUnion: each entity once, lowest first tx). It answers only when the
// anchor (reference shard) declares the index and EVERY other shard carries a
// finished copy with the anchor's options; otherwise store.ErrIndexNotFound
// (cheapest test first: the anchor) and the core takes the history fold —
// sound while a DDL, a repair or a backfill is in flight.
func unionPropertyTxMembers[ID comparable](ts *Store, options func(*BadgerStore) (storecontract.PropertyIndexOptions, bool, error), each func(*BadgerStore, func(ID, types.Instant) bool) error, fn func(ID, types.Instant) bool) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	_, anchored, err := options(ref)
	refCheckin()
	if err != nil {
		return err
	}
	if !anchored {
		return storecontract.ErrIndexNotFound
	}
	type answer struct {
		opts    storecontract.PropertyIndexOptions
		has     bool
		members []txMember[ID]
	}
	refAnswer, others, err := allShards(ts, func(bs *BadgerStore, _ bool) (answer, error) {
		var a answer
		var err error
		if a.opts, a.has, err = options(bs); err != nil || !a.has {
			return a, err
		}
		err = each(bs, func(id ID, tx types.Instant) bool {
			a.members = append(a.members, txMember[ID]{id, tx})
			return true
		})
		if errors.Is(err, storecontract.ErrIndexNotFound) {
			a.has = false // still building on this shard
			return a, nil
		}
		return a, err
	})
	if err != nil {
		return err
	}
	if !refAnswer.has {
		return storecontract.ErrIndexNotFound
	}
	for _, a := range others {
		if !a.has || a.opts != refAnswer.opts {
			return storecontract.ErrIndexNotFound
		}
	}
	var u txMemberUnion[ID]
	u.add(refAnswer.members)
	for _, a := range others {
		u.add(a.members)
	}
	u.emit(fn)
	return nil
}

// mergeValidEndCandidates merges per-shard candidates: an entity listed by
// several shards gets the highest stamp and bound (unknown, Stamp
// ValidEndOpen, wins). The union is a sound superset: a shard's fold bounds
// only its own rows, so other shards' bounds stay looser, never tighter. Gen
// is 0: Refine is a no-op on tiered.
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
// shard's valid-end index.
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
// shard's refined bound outlives writes on other shards, but an entity's rows
// can span shards (ArchiveNode, re-import), so it could stand for rows it no
// longer bounds. Loose-fold entities are read on every call.
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
