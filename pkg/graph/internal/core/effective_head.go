package core

import (
	"cmp"
	"math"
	"reflect"
	"slices"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Head-pin effective timeline (sigma R10, backlog 68)
//
// At a pin at or above every TxFrom of a chain without deletes, the effective
// timeline no longer depends on the pin: every row is recorded and none is
// tombstone-normalized (nodeRowAtTx). That answer — the head answer — is kept
// per entity as pieces naming their winner by version (headEntry), so a head
// read costs the current row, the store's history token
// (store.HistoryEpochCapability) and one point read per winner: O(segments),
// not O(history).
//
// The entry is exact while the history token and the current row's version
// and temporal block are the ones it was computed with: any history write,
// overwrite or removal changes the token (the store's watch table), and the
// current row is compared directly. A head read that misses folds the chain
// as before (effectivePieces) and keeps the result. A correction
// (cascadeNode / cascadeRel, every SetVersionInterval and excise door) that
// finds a valid entry extends it by its appended rows (headOverlay) instead
// of dropping it, so sigma's read-then-correct cut stays O(segments) on the
// read; the write keeps its own cost.
//
// headOverlay is the sweep's answer for the grown chain only where the
// appended rows provably do not change how older rows resolve: no row of the
// chain carries a delete (no life ends, no retraction), the appended rows are
// the newest beliefs (versions above the chain's, TxFrom not below its newest)
// and replace nothing (no TxTo, TxFrom above every TxTo of the chain, so no
// supersession cap moves), and the chain keeps its arm: the own-bounds arm
// stays the own-bounds arm (an inversion in write order persists); the
// monotonic arm stays monotonic when each appended row starts at or after the
// newest row, whose positional end then moves only when it was open — allowed
// only when the appended row is open too (it then covers what the older row
// lost). Under those conditions each appended row wins exactly its own
// interval and every older piece keeps its winner outside it. Any other
// correction drops the entry; the next head read folds again. The
// differential test (effective_head_test.go) checks the cached answer against
// the fold at every pin on memory, badger, sharded and tiered.
// =============================================================================

// The cache is bounded by pieces, not entries (an entry weighs its pieces
// plus one): at most headCacheMaxPieces over the graph, split evenly over
// headCacheStripes stripes, each dropped whole when a put would pass its
// share. An answer of more than headMaxEntryPieces pieces is not kept (its
// reads fold, as before backlog 68).
const (
	headCacheStripes    = 64
	headCacheMaxPieces  = 1 << 20
	headMaxEntryPieces  = 512
	headStripeMaxWeight = headCacheMaxPieces / headCacheStripes
)

// headPiece is one piece [from, to) (to 0: open) of a head answer; version
// names the winning row (versions are unique in a cached chain).
type headPiece struct {
	from, to types.Instant
	version  uint32
}

// headEntry is one entity's head answer and what it was computed from.
type headEntry struct {
	epoch     uint64                 // store history token
	curVer    uint32                 // the current row it was computed with
	curTM     types.TemporalMetadata //
	maxTx     types.Instant          // newest TxFrom of the chain: valid at pins >= maxTx
	maxTxTo   types.Instant          // newest TxTo of the chain
	maxVer    uint32                 // highest version of the chain
	cascade   bool                   // the resolver's own-bounds arm
	lastStart types.Instant          // the newest row's start (resolver key)
	lastEnd   types.Instant          // the newest row's own ValidTo (0: open)
	pieces    []headPiece
}

type headKey struct {
	rel bool
	id  snowflake.ID
}

// headCache holds the head answers of one graph: striped by key under
// read-write locks, so concurrent head reads of different entities share no
// lock and a hit takes one stripe's read lock.
type headCache struct {
	stripes [headCacheStripes]headStripe
}

type headStripe struct {
	mu     sync.RWMutex
	m      map[headKey]*headEntry
	weight int // pieces + entries held
	_      [32]byte
}

func headWeight(e *headEntry) int { return len(e.pieces) + 1 }

func (h *headCache) stripe(k headKey) *headStripe {
	x := uint64(k.id) * 0x9E3779B97F4A7C15 // #nosec G115 — bit-pattern hash only; Fibonacci hash: snowflake IDs share low bits
	if k.rel {
		x ^= 1 << 63
	}
	return &h.stripes[x>>58] // top 6 bits: 64 stripes
}

func (h *headCache) get(k headKey) *headEntry {
	s := h.stripe(k)
	s.mu.RLock()
	e := s.m[k]
	s.mu.RUnlock()
	return e
}

// put keeps e under k (nil drops k's entry). An entry above
// headMaxEntryPieces is not kept: k's entry is dropped instead.
func (h *headCache) put(k headKey, e *headEntry) {
	s := h.stripe(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.m[k]; ok {
		s.weight -= headWeight(old)
		delete(s.m, k)
	}
	if e == nil || len(e.pieces) > headMaxEntryPieces {
		return
	}
	w := headWeight(e)
	if s.m == nil || s.weight+w > headStripeMaxWeight {
		s.m, s.weight = make(map[headKey]*headEntry), 0
	}
	s.m[k] = e
	s.weight += w
}

// size reports the entries and the weight held (tests).
func (h *headCache) size() (entries, weight int) {
	for i := range h.stripes {
		s := &h.stripes[i]
		s.mu.RLock()
		entries += len(s.m)
		weight += s.weight
		s.mu.RUnlock()
	}
	return entries, weight
}

// historyEpochCapability admits the history token unless a wrapper only
// inherits it from an embedded native store (the guard every cached
// capability handle uses: the wrapper's own history writes would not be noted).
func historyEpochCapability(store storepkg.MandatoryStore) storepkg.HistoryEpochCapability {
	cap, ok := store.(storepkg.HistoryEpochCapability)
	if !ok {
		return nil
	}
	if !isExactNativeStore(store) && embedsNativeCapability(store,
		reflect.TypeOf((*storepkg.HistoryEpochCapability)(nil)).Elem(), "NodeHistoryEpoch") {
		return nil
	}
	return cap
}

func (c *Core) nodeHistoryEpoch(id types.NodeID) uint64 {
	if c.historyEpochs == nil || c.headCacheOff {
		return 0
	}
	e, err := c.historyEpochs.NodeHistoryEpoch(id)
	if err != nil {
		return 0
	}
	return e
}

func (c *Core) relHistoryEpoch(id types.RelID) uint64 {
	if c.historyEpochs == nil || c.headCacheOff {
		return 0
	}
	e, err := c.historyEpochs.RelHistoryEpoch(id)
	if err != nil {
		return 0
	}
	return e
}

func temporalOf[T storeutil.TemporalRow](r T) types.TemporalMetadata {
	if tm := r.Temporal(); tm != nil {
		return *tm
	}
	return types.TemporalMetadata{}
}

// headValid returns k's entry when it answers for current at pin under the
// token epoch.
func (c *Core) headValid(k headKey, epoch uint64, curVer uint32, curTM types.TemporalMetadata, pin types.Instant) *headEntry {
	if epoch == 0 {
		return nil
	}
	e := c.heads.get(k)
	if e == nil || e.epoch != epoch || e.curVer != curVer || e.curTM != curTM || pin < e.maxTx {
		return nil
	}
	return e
}

// headFacts is what an entry needs from its chain, gathered by the fold
// (effectivePiecesFacts) from the rows it already walks.
type headFacts struct {
	ok        bool // the chain qualifies (see gatherHeadFacts)
	maxTx     types.Instant
	maxTxTo   types.Instant
	maxVer    uint32
	lastStart types.Instant
	lastEnd   types.Instant
	cascade   bool // set from the sweep
}

// gatherHeadFacts fills f from filtered, the fold's chain in write order when every
// row was recorded by the pin and none was normalized (the caller checks
// that no row was dropped). It qualifies when no row carries a delete (no
// life ends, no retraction) and versions are unique (write order is then
// version order); a chain stored before backlog 38 with a version shared by
// two rows does not.
func gatherHeadFacts[T interface {
	comparable
	storeutil.TemporalRow
}](f *headFacts, c *Core, k *chainKernel[T], filtered []T) {
	for i, r := range filtered {
		tm := r.Temporal()
		if tm == nil || tm.DeletedAt != 0 || tm.Retracted || (i > 0 && filtered[i-1].Version() >= r.Version()) {
			return
		}
		f.maxTx, f.maxTxTo = max(f.maxTx, tm.TxFrom), max(f.maxTxTo, tm.TxTo)
	}
	last := filtered[len(filtered)-1]
	f.maxVer = last.Version()
	f.lastStart, f.lastEnd = k.sortVF(c, last), last.Temporal().ValidTo
	f.ok = true
}

// headFromFold builds the entry for a fold's pieces and facts, or nil when
// the chain did not qualify or the answer is too large to keep.
func headFromFold[T interface {
	comparable
	storeutil.TemporalRow
}](epoch uint64, current T, pieces []effPiece[T], f *headFacts) *headEntry {
	if epoch == 0 || !f.ok || len(pieces) > headMaxEntryPieces {
		return nil
	}
	e := &headEntry{epoch: epoch, curVer: current.Version(), curTM: temporalOf(current),
		maxTx: f.maxTx, maxTxTo: f.maxTxTo, maxVer: f.maxVer, cascade: f.cascade,
		lastStart: f.lastStart, lastEnd: f.lastEnd, pieces: make([]headPiece, len(pieces))}
	for i, p := range pieces {
		e.pieces[i] = headPiece{from: p.from, to: p.to, version: p.row.Version()}
	}
	return e
}

// headOverlay is e extended by a correction's appended rows (ascending by
// version), or nil when the section comment's conditions do not hold.
func headOverlay[T interface {
	comparable
	storeutil.TemporalRow
}](c *Core, k *chainKernel[T], e *headEntry, appended []T) *headEntry {
	if e == nil || len(appended) == 0 {
		return nil
	}
	out := *e
	out.pieces = slices.Clone(e.pieces)
	for _, r := range appended {
		tm := r.Temporal()
		if tm == nil || tm.DeletedAt != 0 || tm.Retracted || tm.TxTo != 0 ||
			r.Version() <= out.maxVer || tm.TxFrom < out.maxTx || tm.TxFrom <= out.maxTxTo {
			return nil
		}
		s, end := k.sortVF(c, r), tm.ValidTo
		if end != 0 && end <= s {
			return nil
		}
		if !out.cascade {
			// Monotonic arm: the resolver's positional start equals the
			// classifier's key only once the store is migrated.
			if !c.bitemporalMigrated || s < out.lastStart || (out.lastEnd == 0 && end != 0) {
				return nil
			}
		}
		out.pieces = paintHead(out.pieces, s, end, r.Version(), isVoidRow(r))
		out.maxTx, out.maxVer = tm.TxFrom, r.Version()
		out.lastStart, out.lastEnd = s, end
	}
	return &out
}

// paintHead makes [s, end) (end 0: open) won by version (a gap when void).
func paintHead(pieces []headPiece, s, end types.Instant, version uint32, void bool) []headPiece {
	out := make([]headPiece, 0, len(pieces)+2)
	for _, p := range pieces {
		// The part of p before s.
		if p.from < s {
			q := p
			if q.to == 0 || q.to > s {
				q.to = s
			}
			out = append(out, q)
		}
		// The part of p after end.
		if end != 0 && (p.to == 0 || p.to > end) {
			q := p
			if q.from < end {
				q.from = end
			}
			out = append(out, q)
		}
	}
	if !void {
		out = append(out, headPiece{from: s, to: end, version: version})
	}
	slices.SortFunc(out, func(a, b headPiece) int { return cmp.Compare(a.from, b.from) })
	// Merge touching pieces of one winner (the sweep's normal form).
	merged := out[:0]
	for _, p := range out {
		if n := len(merged); n > 0 && merged[n-1].version == p.version && merged[n-1].to == p.from {
			merged[n-1].to = p.to
			continue
		}
		merged = append(merged, p)
	}
	return merged
}

// nodeHeadSegments answers a head read from a valid entry; ok false falls
// back to the fold (no entry, a stale one, or a winner gone).
func (c *Core) nodeHeadSegments(rd entityReads, id types.NodeID, current *types.Node, pin types.Instant, epoch uint64) ([]temporal.NodeSegment, bool) {
	e := c.headValid(headKey{id: id.SnowflakeID()}, epoch, current.Version(), temporalOf(current), pin)
	if e == nil {
		return nil, false
	}
	segs := make([]temporal.NodeSegment, 0, len(e.pieces))
	for _, p := range e.pieces {
		row := current
		if p.version != current.Version() {
			full, err := rd.nodeVersion(id, p.version)
			if err != nil {
				return nil, false
			}
			row = full
		}
		row, ok := nodeRowAtTx(row, pin)
		if !ok {
			return nil, false
		}
		segs = append(segs, nodeSegment(p.from, p.to, row))
	}
	c.headHits.Add(1)
	return segs, true
}

// relHeadSegments mirrors nodeHeadSegments.
func (c *Core) relHeadSegments(rd entityReads, id types.RelID, current *types.Relationship, pin types.Instant, epoch uint64) ([]temporal.RelSegment, bool) {
	e := c.headValid(headKey{rel: true, id: id.SnowflakeID()}, epoch, current.Version(), temporalOf(current), pin)
	if e == nil {
		return nil, false
	}
	segs := make([]temporal.RelSegment, 0, len(e.pieces))
	for _, p := range e.pieces {
		row := current
		if p.version != current.Version() {
			full, err := rd.relVersion(id, p.version)
			if err != nil {
				return nil, false
			}
			row = full
		}
		row, ok := relRowAtTx(row, pin)
		if !ok {
			return nil, false
		}
		segs = append(segs, relSegment(p.from, p.to, row))
	}
	c.headHits.Add(1)
	return segs, true
}

// noteNodeHead keeps the head answer a fold produced (no-op unless it qualifies).
func (c *Core) noteNodeHead(id types.NodeID, epoch uint64, current *types.Node, pieces []effPiece[*types.Node], f *headFacts) {
	if e := headFromFold(epoch, current, pieces, f); e != nil {
		c.heads.put(headKey{id: id.SnowflakeID()}, e)
	}
}

func (c *Core) noteRelHead(id types.RelID, epoch uint64, current *types.Relationship, pieces []effPiece[*types.Relationship], f *headFacts) {
	if e := headFromFold(epoch, current, pieces, f); e != nil {
		c.heads.put(headKey{rel: true, id: id.SnowflakeID()}, e)
	}
}

// nodeHeadBeforeWrite returns the node's valid entry at a correction's start
// (entity lock held, before any store write), or nil.
func (c *Core) nodeHeadBeforeWrite(id types.NodeID, current *types.Node) *headEntry {
	if current == nil {
		return nil
	}
	return c.headValid(headKey{id: id.SnowflakeID()}, c.nodeHistoryEpoch(id), current.Version(), temporalOf(current), math.MaxInt64)
}

func (c *Core) relHeadBeforeWrite(id types.RelID, current *types.Relationship) *headEntry {
	if current == nil {
		return nil
	}
	return c.headValid(headKey{rel: true, id: id.SnowflakeID()}, c.relHistoryEpoch(id), current.Version(), temporalOf(current), math.MaxInt64)
}

// nodeHeadAfterWrite extends before by a correction's appended rows once they
// are written (entity lock still held): the new current row is cur, the token
// is read now. Without a valid extension the entry is dropped.
func (c *Core) nodeHeadAfterWrite(id types.NodeID, before *headEntry, appended []*types.Node, cur *types.Node) {
	if c.historyEpochs == nil || c.headCacheOff {
		return
	}
	k := headKey{id: id.SnowflakeID()}
	e := headOverlay(c, nodeKernel, before, appended)
	if e == nil {
		c.heads.put(k, nil)
		return
	}
	e.epoch, e.curVer, e.curTM = c.nodeHistoryEpoch(id), cur.Version(), temporalOf(cur)
	c.heads.put(k, e)
	c.headOverlays.Add(1)
}

func (c *Core) relHeadAfterWrite(id types.RelID, before *headEntry, appended []*types.Relationship, cur *types.Relationship) {
	if c.historyEpochs == nil || c.headCacheOff {
		return
	}
	k := headKey{rel: true, id: id.SnowflakeID()}
	e := headOverlay(c, relKernel, before, appended)
	if e == nil {
		c.heads.put(k, nil)
		return
	}
	e.epoch, e.curVer, e.curTM = c.relHistoryEpoch(id), cur.Version(), temporalOf(cur)
	c.heads.put(k, e)
	c.headOverlays.Add(1)
}
