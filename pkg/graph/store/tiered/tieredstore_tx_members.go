package tiered

import (
	"errors"
	"slices"

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
	_ storecontract.LabelTxMembershipCapability          = (*Store)(nil)
	_ storecontract.RelTypeTxMembershipCapability        = (*Store)(nil)
	_ storecontract.LabelTxMemberLookupCapability        = (*Store)(nil)
	_ storecontract.RelTypeTxMemberLookupCapability      = (*Store)(nil)
	_ storecontract.NodePropertyTxMembershipCapability   = (*Store)(nil)
	_ storecontract.RelPropertyTxMembershipCapability    = (*Store)(nil)
	_ storecontract.NodePropertyTxMemberWindowCapability = (*Store)(nil) // ledger row 62
	_ storecontract.RelPropertyTxMemberWindowCapability  = (*Store)(nil)
	_ storecontract.RelValidEndCapability                = (*Store)(nil)
	_ storecontract.NodeValidEndCapability               = (*Store)(nil)
	_ storecontract.MemberGatherMeteredCapability        = (*Store)(nil) // sigma group B
)

// allShardsBut runs read on the reference shard, the archive (when one exists)
// and every event shard (open ones concurrently, cold ones one at a time),
// returning the reference result first (ref tells read which shard it is).
// read must be safe for concurrent use.
//
// The reference and archive reads run under moveMu's read lock, which
// ArchiveNode and RestoreNode hold for their whole move. Without it a move
// between the two reads can hide a node from both (RestoreNode writes the
// reference shard, then hard-deletes from the archive without a tombstone).
//
// It leaves out the event shards skip reports (nil:
// none). Under a read budget (opens set, READBUDGET step 6) the open of a
// closed archive or event shard is charged to opens before it; a worker
// checks opens' meter first, so once one refused the others stop at their
// next shard, and the errors join (errors.Is store.ErrReadBudget).
func allShardsBut[T any](ts *Store, skip func(*EventShard) bool, opens *storecontract.ReadTally, read func(bs *BadgerStore, ref bool) (T, error)) (ref T, others []T, err error) {
	if err = ts.checkOpen(); err != nil {
		return ref, nil, err
	}
	ref, archived, hasArchive, err := refAndArchive(ts, opens, read)
	if err != nil {
		return ref, nil, err
	}
	if hasArchive {
		others = append(others, archived)
	}
	ts.mu.RLock()
	shards := ts.eventShardsToRead(DepthAll)
	ts.mu.RUnlock()
	if skip != nil {
		shards = slices.DeleteFunc(shards, skip)
	}
	results := make([]T, len(shards))
	errs := make([]error, len(shards))
	queryEventShards(shards, func(i int, es *EventShard) {
		store, release, err := es.checkoutBudgeted(ts, opens)
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
// exists), under moveMu's read lock (see allShardsBut), charging a closed
// archive's open to opens before it (nil: none).
func refAndArchive[T any](ts *Store, opens *storecontract.ReadTally, read func(bs *BadgerStore, ref bool) (T, error)) (ref, archived T, hasArchive bool, err error) {
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
	archive, archiveCheckin, err := ts.checkoutArchiveCharged(opens)
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

// checkoutKeyRef and checkoutKeyArchive name the reference shard and the archive to
// the test-only checkoutHook; an event shard is named by its *EventShard.
const (
	checkoutKeyRef     = "ref"
	checkoutKeyArchive = "archive"
)

// noteCheckout reports a read pin of shard to the test-only checkoutHook
// (held +1) and returns release reporting its end (held -1); without the hook
// it returns release unchanged.
func (ts *Store) noteCheckout(shard any, release func()) func() {
	h := ts.checkoutHook
	if h == nil {
		return release
	}
	h(shard, 1)
	return func() {
		release()
		h(shard, -1)
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
	return unionTxMembersMetered(ts, nil, each, fn)
}

// unionTxMembersMetered is unionTxMembers with the open of each closed shard
// charged to opens before it (nil: none).
func unionTxMembersMetered[ID comparable](ts *Store, opens *storecontract.ReadTally, each func(*BadgerStore, func(ID, types.Instant) bool) error, fn func(ID, types.Instant) bool) error {
	return unionTxMembersSkip(ts, nil, opens, each, fn)
}

// unionTxMembersSkip is unionTxMembersMetered without the event shards skip
// reports (nil: none): a sealed shard proven to hold no member (Sigma RH-S7).
func unionTxMembersSkip[ID comparable](ts *Store, skip func(*EventShard) bool, opens *storecontract.ReadTally, each func(*BadgerStore, func(ID, types.Instant) bool) error, fn func(ID, types.Instant) bool) error {
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
	ref, others, err := allShardsBut(ts, skip, opens, read)
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
	return ts.ForEachLabelTxMemberMetered(token, nil, fn)
}

// ForEachLabelTxMemberMetered implements store.MemberGatherMeteredCapability:
// ForEachLabelTxMember with the open of each closed shard charged to t before
// it.
func (ts *Store) ForEachLabelTxMemberMetered(token uint16, t *storecontract.ReadTally, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	ts.noteTypeScan("ForEachLabelTxMember")
	skip := func(es *EventShard) bool { return es.sealedWithoutFormerLabel(token) }
	return unionTxMembersSkip(ts, skip, t, func(bs *BadgerStore, f func(types.NodeID, types.Instant) bool) error {
		return bs.ForEachLabelTxMember(token, f)
	}, fn)
}

// ForEachRelTypeTxMember implements store.RelTypeTxMembershipCapability.
func (ts *Store) ForEachRelTypeTxMember(token uint16, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return ts.ForEachRelTypeTxMemberMetered(token, nil, fn)
}

// ForEachRelTypeTxMemberMetered is ForEachLabelTxMemberMetered for a
// relationship type.
func (ts *Store) ForEachRelTypeTxMemberMetered(token uint16, t *storecontract.ReadTally, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	ts.noteTypeScan("ForEachRelTypeTxMember")
	return unionTxMembersMetered(ts, t, func(bs *BadgerStore, f func(types.RelID, types.Instant) bool) error {
		return bs.ForEachRelTypeTxMember(token, f)
	}, fn)
}

// LabelTxMembersOf implements store.LabelTxMemberLookupCapability: each ID of
// ids any shard knows as an ever-member of token, once.
func (ts *Store) LabelTxMembersOf(token uint16, ids []types.NodeID, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	skip := func(es *EventShard) bool { return es.sealedWithoutLabelMember(token) }
	return unionTxMembersSkip(ts, skip, nil, func(bs *BadgerStore, f func(types.NodeID, types.Instant) bool) error {
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
	return ts.ForEachNodePropertyTxMemberKeys(labelToken, propertyKey, []string{valueKey}, fn)
}

// ForEachRelPropertyTxMember implements store.RelPropertyTxMembershipCapability
// (see unionPropertyTxMembers).
func (ts *Store) ForEachRelPropertyTxMember(relTypeToken uint16, propertyKey, valueKey string, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return ts.ForEachRelPropertyTxMemberKeys(relTypeToken, propertyKey, []string{valueKey}, fn)
}

// ForEachNodePropertyTxMemberDuring implements
// store.NodePropertyTxMemberWindowCapability (ledger row 62): every shard's
// windowed answer, united like the full enumeration (unionPropertyTxMembers).
func (ts *Store) ForEachNodePropertyTxMemberDuring(labelToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	return ts.ForEachNodePropertyTxMemberKeysDuring(labelToken, propertyKey, []string{valueKey}, validStart, validEnd, txAt, fn)
}

// ForEachRelPropertyTxMemberDuring implements
// store.RelPropertyTxMemberWindowCapability (see
// ForEachNodePropertyTxMemberDuring).
func (ts *Store) ForEachRelPropertyTxMemberDuring(relTypeToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return ts.ForEachRelPropertyTxMemberKeysDuring(relTypeToken, propertyKey, []string{valueKey}, validStart, validEnd, txAt, fn)
}

var (
	_ storecontract.NodePropertyTxMemberKeysCapability = (*Store)(nil) // round 11
	_ storecontract.RelPropertyTxMemberKeysCapability  = (*Store)(nil)
)

// ForEachNodePropertyTxMemberKeys implements
// store.NodePropertyTxMemberKeysCapability: one fan-out over the shards, each
// shard enumerating every key (unionPropertyTxMembers unites across shards
// and keys).
func (ts *Store) ForEachNodePropertyTxMemberKeys(labelToken uint16, propertyKey string, valueKeys []string, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	return unionPropertyTxMembers(ts, nil, func(bs *BadgerStore) (storecontract.PropertyIndexOptions, bool, error) {
		return bs.PropertyIndexOptions(labelToken, propertyKey)
	}, eachKey(valueKeys, func(bs *BadgerStore, vk string, f func(types.NodeID, types.Instant) bool) error {
		return bs.ForEachNodePropertyTxMember(labelToken, propertyKey, vk, f)
	}), fn)
}

// ForEachRelPropertyTxMemberKeys implements
// store.RelPropertyTxMemberKeysCapability (see
// ForEachNodePropertyTxMemberKeys).
func (ts *Store) ForEachRelPropertyTxMemberKeys(relTypeToken uint16, propertyKey string, valueKeys []string, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return unionPropertyTxMembers(ts, nil, func(bs *BadgerStore) (storecontract.PropertyIndexOptions, bool, error) {
		return bs.RelPropertyIndexOptions(relTypeToken, propertyKey)
	}, eachKey(valueKeys, func(bs *BadgerStore, vk string, f func(types.RelID, types.Instant) bool) error {
		return bs.ForEachRelPropertyTxMember(relTypeToken, propertyKey, vk, f)
	}), fn)
}

// ForEachNodePropertyTxMemberKeysDuring implements
// store.NodePropertyTxMemberKeysCapability's windowed form.
func (ts *Store) ForEachNodePropertyTxMemberKeysDuring(labelToken uint16, propertyKey string, valueKeys []string, validStart, validEnd, txAt types.Instant, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	return unionPropertyTxMembers(ts, nil, func(bs *BadgerStore) (storecontract.PropertyIndexOptions, bool, error) {
		return bs.PropertyIndexOptions(labelToken, propertyKey)
	}, eachKey(valueKeys, func(bs *BadgerStore, vk string, f func(types.NodeID, types.Instant) bool) error {
		return bs.ForEachNodePropertyTxMemberDuring(labelToken, propertyKey, vk, validStart, validEnd, txAt, f)
	}), fn)
}

// ForEachRelPropertyTxMemberKeysDuring implements
// store.RelPropertyTxMemberKeysCapability's windowed form.
func (ts *Store) ForEachRelPropertyTxMemberKeysDuring(relTypeToken uint16, propertyKey string, valueKeys []string, validStart, validEnd, txAt types.Instant, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return unionPropertyTxMembers(ts, nil, func(bs *BadgerStore) (storecontract.PropertyIndexOptions, bool, error) {
		return bs.RelPropertyIndexOptions(relTypeToken, propertyKey)
	}, eachKey(valueKeys, func(bs *BadgerStore, vk string, f func(types.RelID, types.Instant) bool) error {
		return bs.ForEachRelPropertyTxMemberDuring(relTypeToken, propertyKey, vk, validStart, validEnd, txAt, f)
	}), fn)
}

var _ storecontract.PropertyTxMemberMeteredCapability = (*Store)(nil) // READBUDGET step 6

// ForEachNodePropertyTxMemberMetered implements
// store.PropertyTxMemberMeteredCapability: ForEachNodePropertyTxMemberKeys
// (window nil) or its windowed form, the open of each closed shard of the
// fan-out charged to t before it (allShardsBut) and, since R2, each shard's
// membership fold admitted under t's candidate room as it grows (the shard's
// own metered form), so a read past its limit stops at the member that passes
// it on every shard instead of folding each shard's whole value first.
func (ts *Store) ForEachNodePropertyTxMemberMetered(labelToken uint16, propertyKey string, valueKeys []string, window *storecontract.PropertyTxMemberWindow, t *storecontract.ReadTally, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	one := func(bs *BadgerStore, vk string, f func(types.NodeID, types.Instant) bool) error {
		return bs.ForEachNodePropertyTxMemberMetered(labelToken, propertyKey, []string{vk}, window, t, f) // each shard's fold admitted under the same tally
	}
	return unionPropertyTxMembers(ts, t, func(bs *BadgerStore) (storecontract.PropertyIndexOptions, bool, error) {
		return bs.PropertyIndexOptions(labelToken, propertyKey)
	}, eachKey(valueKeys, one), fn)
}

// ForEachRelPropertyTxMemberMetered is ForEachNodePropertyTxMemberMetered for
// a relationship (type, key) sidecar.
func (ts *Store) ForEachRelPropertyTxMemberMetered(relTypeToken uint16, propertyKey string, valueKeys []string, window *storecontract.PropertyTxMemberWindow, t *storecontract.ReadTally, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	one := func(bs *BadgerStore, vk string, f func(types.RelID, types.Instant) bool) error {
		return bs.ForEachRelPropertyTxMemberMetered(relTypeToken, propertyKey, []string{vk}, window, t, f)
	}
	return unionPropertyTxMembers(ts, t, func(bs *BadgerStore) (storecontract.PropertyIndexOptions, bool, error) {
		return bs.RelPropertyIndexOptions(relTypeToken, propertyKey)
	}, eachKey(valueKeys, one), fn)
}

// eachKey is one shard's enumeration over every key of valueKeys (the union
// is taken by unionPropertyTxMembers); the first error stops it.
func eachKey[ID comparable](valueKeys []string, one func(*BadgerStore, string, func(ID, types.Instant) bool) error) func(*BadgerStore, func(ID, types.Instant) bool) error {
	return func(bs *BadgerStore, f func(ID, types.Instant) bool) error {
		for _, vk := range valueKeys {
			if err := one(bs, vk, f); err != nil {
				return err
			}
		}
		return nil
	}
}

// unionPropertyTxMembers serves a property membership sidecar (WP-31): every
// shard keeps its own over its own rows, and an entity's rows can sit on
// several shards, so the answer is the union of every shard's members
// (txMemberUnion: each entity once, lowest first tx; also across the keys of
// a several-key enumeration, round 11). It answers only when the
// anchor (reference shard) declares the index and EVERY other shard carries a
// finished copy with the anchor's options; otherwise store.ErrIndexNotFound
// (cheapest test first: the anchor) and the core takes the history fold —
// sound while a DDL, a repair or a backfill is in flight.
func unionPropertyTxMembers[ID comparable](ts *Store, opens *storecontract.ReadTally, options func(*BadgerStore) (storecontract.PropertyIndexOptions, bool, error), each func(*BadgerStore, func(ID, types.Instant) bool) error, fn func(ID, types.Instant) bool) error {
	if fn == nil {
		if err := ts.checkOpen(); err != nil {
			return err
		}
		return errNilIterationCallback()
	}
	ref, others, err := propertyTxMemberAnswers(ts, opens, options, each)
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

// propertyTxMemberAnswers returns every shard's sidecar answer (each), the
// reference shard's first, under the anchor rule of unionPropertyTxMembers:
// store.ErrIndexNotFound unless the reference shard declares the index and
// every other shard carries a finished copy with its options.
func propertyTxMemberAnswers[ID comparable](ts *Store, opens *storecontract.ReadTally, options func(*BadgerStore) (storecontract.PropertyIndexOptions, bool, error), each func(*BadgerStore, func(ID, types.Instant) bool) error) ([]txMember[ID], [][]txMember[ID], error) {
	if err := ts.checkOpen(); err != nil {
		return nil, nil, err
	}
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, nil, err
	}
	_, anchored, err := options(ref)
	refCheckin()
	if err != nil {
		return nil, nil, err
	}
	if !anchored {
		return nil, nil, storecontract.ErrIndexNotFound
	}
	type answer struct {
		opts    storecontract.PropertyIndexOptions
		has     bool
		members []txMember[ID]
	}
	refAnswer, others, err := allShardsBut(ts, nil, opens, func(bs *BadgerStore, _ bool) (answer, error) {
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
		return nil, nil, err
	}
	if !refAnswer.has {
		return nil, nil, storecontract.ErrIndexNotFound
	}
	lists := make([][]txMember[ID], 0, len(others))
	for _, a := range others {
		if !a.has || a.opts != refAnswer.opts {
			return nil, nil, storecontract.ErrIndexNotFound
		}
		lists = append(lists, a.members)
	}
	return refAnswer.members, lists, nil
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
	return ts.RelValidEndCandidatesMetered(typeToken, pin, validFrom, nil)
}

var (
	_ storecontract.RelValidEndMeteredCapability  = (*Store)(nil) // R2-SCANS
	_ storecontract.NodeValidEndMeteredCapability = (*Store)(nil)
)

// RelValidEndCandidatesMetered implements store.RelValidEndMeteredCapability:
// RelValidEndCandidates charging each closed shard's open to t before it
// (allShardsBut) and counting every shard's first-call reads in t.
func (ts *Store) RelValidEndCandidatesMetered(typeToken uint16, pin, validFrom types.Instant, t *storecontract.ReadTally) ([]storecontract.ValidEndCandidate, error) {
	ref, others, err := allShardsBut(ts, nil, t, func(bs *BadgerStore, _ bool) ([]storecontract.ValidEndCandidate, error) {
		return bs.RelValidEndCandidatesMetered(typeToken, pin, validFrom, t)
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
	return ts.NodeValidEndCandidatesMetered(labelTokens, pin, validFrom, nil)
}

// NodeValidEndCandidatesMetered implements store.NodeValidEndMeteredCapability
// (see RelValidEndCandidatesMetered).
func (ts *Store) NodeValidEndCandidatesMetered(labelTokens []uint16, pin, validFrom types.Instant, t *storecontract.ReadTally) ([]storecontract.ValidEndCandidate, error) {
	ref, others, err := allShardsBut(ts, nil, t, func(bs *BadgerStore, _ bool) ([]storecontract.ValidEndCandidate, error) {
		return bs.NodeValidEndCandidatesMetered(labelTokens, pin, validFrom, t)
	})
	if err != nil {
		return nil, err
	}
	return mergeValidEndCandidates(ref, others), nil
}

// RefineNodeValidEnd implements store.NodeValidEndCapability as a no-op (see
// RefineRelValidEnd).
func (ts *Store) RefineNodeValidEnd(types.NodeID, uint64, types.Instant) {}
