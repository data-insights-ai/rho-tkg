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

// shardMemberBytes and unionEntryBytes are the logical sizes (R2, BufferBytes)
// of what a membership fan-out holds: a member a shard collected into its list
// (an ID and a transaction time, 8 + 8), and an entity in the union (a map entry
// of 24 bytes, the estimate every read budget uses, and its slot in the order
// list, 8).
const (
	shardMemberBytes = 16
	unionEntryBytes  = 32
)

// txMemberUnion merges per-shard member lists: each entity once, in first-seen
// order, with the merged first transaction time.
type txMemberUnion[ID comparable] struct {
	tx    map[ID]types.Instant
	order []ID
}

// add merges ms into the union, charging each new entity's entry as BufferBytes
// to t (nil: none) before it joins; the first refusal stops the merge. With
// dropDups (the label and type gathers, whose shards held every entry they
// copied: ReadTally.Hold) an entity a second shard hands on is examined and
// dropped here, so it is counted (ReadTally.Drop) before the skip: an entity
// that is a former member on two shards counts twice. The property doors'
// shards counted their members as they collected them, so they drop nothing.
func (u *txMemberUnion[ID]) add(ms []txMember[ID], t *storecontract.ReadTally, dropDups bool) error {
	if u.tx == nil {
		u.tx = make(map[ID]types.Instant)
	}
	for _, m := range ms {
		if prev, ok := u.tx[m.id]; ok {
			if dropDups {
				if err := t.Drop(1); err != nil {
					return err
				}
			}
			u.tx[m.id] = indexpkg.MergeFirstTx(prev, m.tx)
			continue
		}
		if err := t.Buffer(unionEntryBytes); err != nil {
			return err
		}
		u.tx[m.id] = m.tx
		u.order = append(u.order, m.id)
	}
	return nil
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
	return unionTxMembersSkip(ts, nil, nil, untalliedGather(each), fn)
}

// gatherEach is one shard's ever-member enumeration under the gather's tally
// (nil: unmetered).
type gatherEach[ID comparable] func(bs *BadgerStore, t *storecontract.ReadTally, f func(ID, types.Instant) bool) error

// untalliedGather adapts an enumeration that takes no tally.
func untalliedGather[ID comparable](each func(*BadgerStore, func(ID, types.Instant) bool) error) gatherEach[ID] {
	return func(bs *BadgerStore, _ *storecontract.ReadTally, f func(ID, types.Instant) bool) error {
		return each(bs, f)
	}
}

// unionTxMembersSkip fans each shard's enumeration out (without the event
// shards skip reports, nil: none: a sealed shard proven to hold no member,
// Sigma RH-S7) and streams the union. Under a budget (t) the work is paid for
// before it is done (R2 round 3): each closed shard's open is charged to t, each
// shard holds the set it copies on t (ReadTally.Hold: the read's one room, over
// every shard, and every label of the read when t carries its scope) and
// refuses before the copy once the shards together pass it; an entity a second
// shard hands on is counted where the union drops it (txMemberUnion.add); the
// members each shard keeps in its list (shardMemberBytes) and the union's
// entries (unionEntryBytes) are charged as BufferBytes before they are added,
// so a refusal stops the gather before the union is built and fn sees nothing.
func unionTxMembersSkip[ID comparable](ts *Store, skip func(*EventShard) bool, t *storecontract.ReadTally, each gatherEach[ID], fn func(ID, types.Instant) bool) error {
	if fn == nil {
		return errNilIterationCallback()
	}
	read := func(bs *BadgerStore, _ bool) ([]txMember[ID], error) {
		var out []txMember[ID]
		var refused error
		err := each(bs, t, func(id ID, tx types.Instant) bool {
			if refused = t.Buffer(shardMemberBytes); refused != nil { // charged before the list grows
				return false
			}
			out = append(out, txMember[ID]{id, tx})
			return true
		})
		if err == nil {
			err = refused
		}
		return out, err
	}
	ref, others, err := allShardsBut(ts, skip, t, read)
	if err != nil {
		return err
	}
	var u txMemberUnion[ID]
	if err := u.add(ref, t, true); err != nil {
		return err
	}
	for _, ms := range others {
		if err := u.add(ms, t, true); err != nil {
			return err
		}
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
// ForEachLabelTxMember with each closed shard's open charged to t before it, and
// each shard's copy held and charged in t before it is made
// (unionTxMembersSkip).
func (ts *Store) ForEachLabelTxMemberMetered(token uint16, t *storecontract.ReadTally, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	ts.noteTypeScan("ForEachLabelTxMember")
	skip := func(es *EventShard) bool { return es.sealedWithoutFormerLabel(token) }
	return unionTxMembersSkip(ts, skip, t, func(bs *BadgerStore, st *storecontract.ReadTally, f func(types.NodeID, types.Instant) bool) error {
		return bs.ForEachLabelTxMemberMetered(token, st, f)
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
	return unionTxMembersSkip(ts, nil, t, func(bs *BadgerStore, st *storecontract.ReadTally, f func(types.RelID, types.Instant) bool) error {
		return bs.ForEachRelTypeTxMemberMetered(token, st, f)
	}, fn)
}

// LabelTxMembersOf implements store.LabelTxMemberLookupCapability: each ID of
// ids any shard knows as an ever-member of token, once.
func (ts *Store) LabelTxMembersOf(token uint16, ids []types.NodeID, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	skip := func(es *EventShard) bool { return es.sealedWithoutLabelMember(token) }
	return unionTxMembersSkip(ts, skip, nil, untalliedGather(func(bs *BadgerStore, f func(types.NodeID, types.Instant) bool) error {
		return bs.LabelTxMembersOf(token, ids, f)
	}), fn)
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
	return unionPropertyTxMembers(ts, nil, nodePropertyIndexAnswers(labelToken, propertyKey), untallied(eachKey(valueKeys, func(bs *BadgerStore, vk string, f func(types.NodeID, types.Instant) bool) error {
		return bs.ForEachNodePropertyTxMember(labelToken, propertyKey, vk, f)
	})), fn)
}

// ForEachRelPropertyTxMemberKeys implements
// store.RelPropertyTxMemberKeysCapability (see
// ForEachNodePropertyTxMemberKeys).
func (ts *Store) ForEachRelPropertyTxMemberKeys(relTypeToken uint16, propertyKey string, valueKeys []string, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return unionPropertyTxMembers(ts, nil, relPropertyIndexAnswers(relTypeToken, propertyKey), untallied(eachKey(valueKeys, func(bs *BadgerStore, vk string, f func(types.RelID, types.Instant) bool) error {
		return bs.ForEachRelPropertyTxMember(relTypeToken, propertyKey, vk, f)
	})), fn)
}

// ForEachNodePropertyTxMemberKeysDuring implements
// store.NodePropertyTxMemberKeysCapability's windowed form.
func (ts *Store) ForEachNodePropertyTxMemberKeysDuring(labelToken uint16, propertyKey string, valueKeys []string, validStart, validEnd, txAt types.Instant, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	return unionPropertyTxMembers(ts, nil, nodePropertyIndexAnswers(labelToken, propertyKey), untallied(eachKey(valueKeys, func(bs *BadgerStore, vk string, f func(types.NodeID, types.Instant) bool) error {
		return bs.ForEachNodePropertyTxMemberDuring(labelToken, propertyKey, vk, validStart, validEnd, txAt, f)
	})), fn)
}

// ForEachRelPropertyTxMemberKeysDuring implements
// store.RelPropertyTxMemberKeysCapability's windowed form.
func (ts *Store) ForEachRelPropertyTxMemberKeysDuring(relTypeToken uint16, propertyKey string, valueKeys []string, validStart, validEnd, txAt types.Instant, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return unionPropertyTxMembers(ts, nil, relPropertyIndexAnswers(relTypeToken, propertyKey), untallied(eachKey(valueKeys, func(bs *BadgerStore, vk string, f func(types.RelID, types.Instant) bool) error {
		return bs.ForEachRelPropertyTxMemberDuring(relTypeToken, propertyKey, vk, validStart, validEnd, txAt, f)
	})), fn)
}

var _ storecontract.PropertyTxMemberMeteredCapability = (*Store)(nil) // READBUDGET step 6

// ForEachNodePropertyTxMemberMetered implements
// store.PropertyTxMemberMeteredCapability: ForEachNodePropertyTxMemberKeys
// (window nil) or its windowed form, the open of each closed shard of the
// fan-out charged to t before it (allShardsBut) and, since R2, every shard
// charging what it examines and holds through its own metered form on t, live
// (propertyTxMemberAnswers), so a read past its limit stops at the entry that
// passes it on every shard instead of folding each shard's whole value first.
// A shard that cannot answer and is open is found before any walk, so that
// decline costs nothing; a decline found mid-walk keeps what was walked.
func (ts *Store) ForEachNodePropertyTxMemberMetered(labelToken uint16, propertyKey string, valueKeys []string, window *storecontract.PropertyTxMemberWindow, t *storecontract.ReadTally, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	one := func(bs *BadgerStore, st *storecontract.ReadTally, vk string, f func(types.NodeID, types.Instant) bool) error {
		return bs.ForEachNodePropertyTxMemberMetered(labelToken, propertyKey, []string{vk}, window, st, f) // each shard counts on t
	}
	return unionPropertyTxMembers(ts, t, nodePropertyIndexAnswers(labelToken, propertyKey), eachKeyTallied(valueKeys, one), fn)
}

// ForEachRelPropertyTxMemberMetered is ForEachNodePropertyTxMemberMetered for
// a relationship (type, key) sidecar.
func (ts *Store) ForEachRelPropertyTxMemberMetered(relTypeToken uint16, propertyKey string, valueKeys []string, window *storecontract.PropertyTxMemberWindow, t *storecontract.ReadTally, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	one := func(bs *BadgerStore, st *storecontract.ReadTally, vk string, f func(types.RelID, types.Instant) bool) error {
		return bs.ForEachRelPropertyTxMemberMetered(relTypeToken, propertyKey, []string{vk}, window, st, f)
	}
	return unionPropertyTxMembers(ts, t, relPropertyIndexAnswers(relTypeToken, propertyKey), eachKeyTallied(valueKeys, one), fn)
}

// nodePropertyIndexAnswers is the options question of a node (label, key)
// membership fan-out: the shard's index options, carried only when the index
// is declared and finished (not being created: its lookups answer).
func nodePropertyIndexAnswers(labelToken uint16, propertyKey string) func(*BadgerStore) (storecontract.PropertyIndexOptions, bool, error) {
	return func(bs *BadgerStore) (storecontract.PropertyIndexOptions, bool, error) {
		o, has, err := bs.PropertyIndexOptions(labelToken, propertyKey)
		return o, has && bs.PropertyIndexUsable(labelToken, propertyKey), err
	}
}

// relPropertyIndexAnswers is nodePropertyIndexAnswers for a relationship
// (type, key) membership fan-out.
func relPropertyIndexAnswers(relTypeToken uint16, propertyKey string) func(*BadgerStore) (storecontract.PropertyIndexOptions, bool, error) {
	return func(bs *BadgerStore) (storecontract.PropertyIndexOptions, bool, error) {
		o, has, err := bs.RelPropertyIndexOptions(relTypeToken, propertyKey)
		return o, has && bs.RelPropertyIndexUsable(relTypeToken, propertyKey), err
	}
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

// eachKeyTallied is eachKey for the metered doors: one shard's enumeration
// under the tally the fan-out gives it.
func eachKeyTallied[ID comparable](valueKeys []string, one func(*BadgerStore, *storecontract.ReadTally, string, func(ID, types.Instant) bool) error) func(*BadgerStore, *storecontract.ReadTally, func(ID, types.Instant) bool) error {
	return func(bs *BadgerStore, t *storecontract.ReadTally, f func(ID, types.Instant) bool) error {
		for _, vk := range valueKeys {
			if err := one(bs, t, vk, f); err != nil {
				return err
			}
		}
		return nil
	}
}

// untallied adapts a shard enumeration that takes no tally to the form the
// fan-out calls.
func untallied[ID comparable](each func(*BadgerStore, func(ID, types.Instant) bool) error) func(*BadgerStore, *storecontract.ReadTally, func(ID, types.Instant) bool) error {
	return func(bs *BadgerStore, _ *storecontract.ReadTally, f func(ID, types.Instant) bool) error {
		return each(bs, f)
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
func unionPropertyTxMembers[ID comparable](ts *Store, opens *storecontract.ReadTally, options func(*BadgerStore) (storecontract.PropertyIndexOptions, bool, error), each func(*BadgerStore, *storecontract.ReadTally, func(ID, types.Instant) bool) error, fn func(ID, types.Instant) bool) error {
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
	if err := u.add(ref, opens, false); err != nil {
		return err
	}
	for _, ms := range others {
		if err := u.add(ms, opens, false); err != nil {
			return err
		}
	}
	u.emit(fn)
	return nil
}

// propertyTxMemberAnswers returns every shard's sidecar answer (each), the
// reference shard's first, under the anchor rule of unionPropertyTxMembers:
// store.ErrIndexNotFound unless the reference shard declares the index and
// every other shard carries a finished copy with its options (options reports
// a shard whose copy is still being created as not carrying it).
//
// Every shard that can be asked without an open (the reference shard, an open
// archive, every event shard that is not cold) is asked first, before any
// walk (openShardsAnswer), so a decline found there costs nothing. A closed
// shard is asked when the walk opens it, and a copy found still building
// mid-walk declines then: such a decline keeps what the shards walked (no
// charge is given back), and the caller's fallback is charged on top.
//
// Under a read budget (opens) the shards charge what they examine and the
// members each keeps in its list (shardMemberBytes, before the append) to the
// meter as they go: one allowance, live, for the shards and for every other
// read on the context; once one charge refused, the others are refused by
// their own add and the workers stop before their next shard
// (ReadMeter.Exceeded).
func propertyTxMemberAnswers[ID comparable](ts *Store, opens *storecontract.ReadTally, options func(*BadgerStore) (storecontract.PropertyIndexOptions, bool, error), each func(*BadgerStore, *storecontract.ReadTally, func(ID, types.Instant) bool) error) ([]txMember[ID], [][]txMember[ID], error) {
	if err := ts.checkOpen(); err != nil {
		return nil, nil, err
	}
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, nil, err
	}
	anchor, anchored, err := options(ref)
	refCheckin()
	if err != nil {
		return nil, nil, err
	}
	if !anchored {
		return nil, nil, storecontract.ErrIndexNotFound
	}
	if ok, err := openShardsAnswer(ts, opens, options, anchor); err != nil || !ok {
		if err == nil {
			err = storecontract.ErrIndexNotFound
		}
		return nil, nil, err
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
		var refused error
		err = each(bs, opens, func(id ID, tx types.Instant) bool {
			if refused = opens.Buffer(shardMemberBytes); refused != nil { // charged before the list grows
				return false
			}
			a.members = append(a.members, txMember[ID]{id, tx})
			return true
		})
		if refused != nil {
			return a, refused
		}
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

// openShardsAnswer asks every shard that is open now (an open archive, every
// event shard that is not cold; the caller asked the reference shard) whether
// it carries a finished copy of the index with the anchor's options, before
// any shard is walked: false when one does not. A cold shard is not opened for
// the question; it is asked when the walk opens it. A shard that turns cold in
// between is opened here, charged to opens as every open is.
func openShardsAnswer(ts *Store, opens *storecontract.ReadTally, options func(*BadgerStore) (storecontract.PropertyIndexOptions, bool, error), anchor storecontract.PropertyIndexOptions) (bool, error) {
	ask := func(bs *BadgerStore) (bool, error) {
		o, has, err := options(bs)
		return err == nil && has && o == anchor, err
	}
	if ts.refArchive.Load() != nil {
		archive, checkin, err := ts.checkoutArchiveCharged(opens)
		if err != nil {
			return false, err
		}
		if archive != nil {
			ok, err := ask(archive)
			checkin()
			if err != nil || !ok {
				return false, err
			}
		}
	}
	ts.mu.RLock()
	shards := ts.eventShardsToRead(DepthAll)
	ts.mu.RUnlock()
	for _, es := range shards {
		if es == nil || es.currentTier() == TierCold {
			continue
		}
		bs, release, err := es.checkoutBudgeted(ts, opens)
		if err != nil {
			return false, err
		}
		ok, err := ask(bs)
		release()
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
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
