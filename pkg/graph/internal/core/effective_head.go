package core

import (
	"cmp"
	"math"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

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
// read; the write keeps its own cost. A plain Update, ReplaceVersion or
// CloseVersion that finds a valid entry extends it by its new current row
// the same way (headSupersede, RELHIST P2), and the plural door
// (NodeEffectiveTimelines) reads and fills the entry as the singular one.
//
// headOverlay is the sweep's answer for the grown chain only where the
// appended rows provably do not change how older rows resolve: no row of the
// chain carries a delete (no life ends, no retraction), the appended rows are
// the newest beliefs (versions above the chain's, TxFrom not below its newest)
// and replace nothing (no TxTo, TxFrom above every TxTo of the chain, so no
// supersession cap moves), and the arm is known: the own-bounds arm stays the
// own-bounds arm (an inversion in write order persists); the monotonic arm
// stays monotonic when the appended rows start at or after the newest row,
// in order, and the tiling keeps the paint (headTilingKept: a bounded row
// tiles its own interval, an open one loses what the rows after it start at,
// so the newest row's open end and an open appended row must be covered by
// the rows after them — the bounded correction of an open row with its open
// resumption, row 88); a monotonic chain the appended rows make non-monotonic
// takes the own-bounds arm, whose answer on the old rows equals the tiling
// when every old row is bounded, no row was replaced and TxFrom follows
// version (headEntry.armSwitches). Under those conditions each appended row
// wins exactly its own interval and every older piece keeps its winner
// outside it: the appended rows paint over the pieces in one pass
// (paintHeadAll). Any other correction drops the entry; the next head read
// folds again. The differential test (effective_head_test.go) checks the
// cached answer against the fold at every pin on memory, badger, sharded and
// tiered.
// =============================================================================

// The cache is bounded by pieces, not entries (an entry weighs its pieces
// plus its bounds plus one): at most headCacheMaxPieces over the graph, split
// evenly over headCacheStripes stripes, each dropped whole when a put would
// pass its share. One entry keeps at most headMaxEntryPieces pieces and
// headMaxEntryBounds bounds — a share of its stripe's budget, the cache's
// one memory bound (row 87: a cap of 512 below it was a second, tighter
// bound, and a consumer whose answer passed it lost the entry at both doors:
// a read needs a whole entry, and a correction from the chain's start lies
// below a trimmed entry's watermarks). Over the caps an entry keeps the
// NEWEST pieces and bounds, a suffix of the head answer (row 85), which
// answers the corrections at or above its watermarks (pieceFrom, boundFrom):
// where a consumer that appends versions at the head writes. Reads of such a
// chain fold, as before.
const (
	headCacheStripes    = 64
	headCacheMaxPieces  = 1 << 20
	headStripeMaxWeight = headCacheMaxPieces / headCacheStripes
	headMaxEntryPieces  = headStripeMaxWeight / 8 // 2,048: with its bounds, 3/8 of a stripe at most
	headMaxEntryBounds  = 2 * headMaxEntryPieces
)

// headWhole is the watermark of an entry that dropped nothing.
const headWhole = types.Instant(math.MinInt64)

// headCaps returns the entry caps (tests lower them).
func (c *Core) headCaps() (pieces, bounds int) {
	pieces, bounds = headMaxEntryPieces, headMaxEntryBounds
	if c.headPieceCap > 0 {
		pieces = c.headPieceCap
	}
	if c.headBoundCap > 0 {
		bounds = c.headBoundCap
	}
	return pieces, bounds
}

// headPiece is one piece [from, to) (to open by the graph's valid axis) of a
// head answer; version
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
	ownOpen   bool                   // some row's own interval is open (void or not)
	txOrdered bool                   // TxFrom never falls with version: the belief order is the version order
	lastStart types.Instant          // the newest row's start (resolver key)
	lastEnd   types.Instant          // the newest row's own ValidTo (open by the axis)
	pieces    []headPiece
	// What a correction needs besides the pieces (backlog 68b,
	// effective_head_write.go): every row's own valid bounds, sorted and
	// distinct, and the newest belief among the rows whose own interval is
	// open and not void.
	bounds []types.Instant
	open   openBelief
	// pieceFrom / boundFrom: the pieces and the bounds are exact from these
	// instants on (headWhole: nothing dropped). An entry over the caps keeps
	// the newest pieces and bounds (trim); a correction at or above both
	// watermarks is planned from it, a read needs a whole entry.
	pieceFrom, boundFrom types.Instant
	// void: some row of the chain is void (an excise). A whole-axis update
	// planned from the entry asks it as one planned from the history asks
	// the chain (wholeAxisSupersession.voidFree), so both stamp alike.
	void bool
}

// openBelief is the newest belief (nodeBeliefNewerThan: higher TxFrom, then
// higher version) among the rows noted; ok false: none.
type openBelief struct {
	tx      types.Instant
	version uint32
	ok      bool
}

// note keeps (tx, version) when it is a newer belief than the one kept.
func (o *openBelief) note(tx types.Instant, version uint32) {
	if !o.ok || tx > o.tx || (tx == o.tx && version > o.version) {
		*o = openBelief{tx: tx, version: version, ok: true}
	}
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
	weight int // pieces + bounds + entries held
	_      [32]byte
}

func headWeight(e *headEntry) int { return len(e.pieces) + len(e.bounds) + 1 }

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

// put keeps e under k (nil drops k's entry). An entry above the caps
// (headFromFold and headOverlay trim theirs) is not kept: k's entry is
// dropped instead.
func (h *headCache) put(k headKey, e *headEntry) {
	s := h.stripe(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.m[k]; ok {
		s.weight -= headWeight(old)
		delete(s.m, k)
	}
	if e == nil || len(e.pieces) > headMaxEntryPieces || len(e.bounds) > headMaxEntryBounds {
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
	e, _ := c.headLookup(k, epoch, curVer, curTM, pin)
	return e
}

// headLookup is headValid with the reason it found no valid entry (headHit
// when it found one), checked in this order: no token or no entry, the token
// moved, the current row changed, the pin below the entry's newest TxFrom.
func (c *Core) headLookup(k headKey, epoch uint64, curVer uint32, curTM types.TemporalMetadata, pin types.Instant) (*headEntry, headReason) {
	if epoch == 0 {
		return nil, headMissNoEntry
	}
	e := c.heads.get(k)
	switch {
	case e == nil:
		return nil, headMissNoEntry
	case e.epoch != epoch:
		return nil, headMissToken
	case e.curVer != curVer || e.curTM != curTM:
		return nil, headMissCurrent
	case pin < e.maxTx:
		return nil, headMissPinBelow
	}
	return e, headHit
}

// =============================================================================
// Head read counts (RELHIST P5 Phase 0)
//
// How each node history read was answered: per head door (the singular
// NodeEffectiveTimeline family and the plural NodeEffectiveTimelines family,
// the scans and page doors through them included) a hit or the reason the
// entry could not answer; the node-at doors (nodeAtFromCurrent past its
// current-row shortcut) and the correction's miss plan (nodeMissPlan), which
// never read an entry; and the skeleton lists (NodeHistoryTemporalMeta) all
// of them read. Tests and the RELHIST bench read the counts to choose among
// P5's options; nothing branches on them.
// =============================================================================

// headDoor names a head door family.
type headDoor int

const (
	headDoorOne  headDoor = iota // NodeEffectiveTimeline and its tails and scans
	headDoorMany                 // NodeEffectiveTimelines and the page doors
	headDoors
)

// headReason is how one head-door read was answered.
type headReason int

const (
	headHit               headReason = iota // answered from the entry
	headCurrentOnly                         // no history (presence bit): answered from the current row, no entry needed
	headMissNoEntry                         // no entry kept (never filled, dropped, a stripe cleared), or no token
	headMissToken                           // the entry's token moved: a history write it was not extended by
	headMissCurrent                         // the current row is not the one the entry was computed with
	headMissPinBelow                        // a pin below the entry's newest TxFrom (the plural door tests its lowest)
	headMissNotWhole                        // the entry was trimmed to its caps: it plans corrections, never answers reads
	headMissNotQualifying                   // the chain cannot have an entry: a delete, a retraction or a shared version (gatherHeadFacts), or no current row
	headMissScan                            // a scan or snapshot read: never reads or fills the entry
	headMissRowGone                         // a winner the entry names could not be read
	headReasons
)

// headReadStats holds the counts (Core.headStats).
type headReadStats struct {
	reads     [headDoors][headReasons]atomic.Uint64
	nodeAt    atomic.Uint64 // node-at reads that read history (past the current-row shortcut)
	missPlans atomic.Uint64 // node corrections planned on the history (no valid entry)
	metaLists atomic.Uint64 // node skeleton lists read
	metaRows  atomic.Uint64 // skeletons in them
}

// noteHeadRead counts one head-door read answered by r. A miss whose fold
// found the chain not qualifying (f.refused) counts as such whatever the
// lookup said: a stale or missing entry there is a consequence, not the
// cause. f nil: no fold facts (no token, or the fold did not gather them).
func (c *Core) noteHeadRead(door headDoor, r headReason, f *headFacts) {
	if f != nil && f.refused && r != headHit && r != headMissScan {
		r = headMissNotQualifying
	}
	c.headStats.reads[door][r].Add(1)
}

// noteNodeMeta counts one node skeleton list of n skeletons.
func (c *Core) noteNodeMeta(n int) {
	c.headStats.metaLists.Add(1)
	c.headStats.metaRows.Add(uint64(n)) // #nosec G115 -- a length, never negative
}

// headFacts is what an entry needs from its chain, gathered by the fold
// (effectivePiecesFacts) from the rows it already walks.
type headFacts struct {
	ok        bool // the chain qualifies (see gatherHeadFacts)
	refused   bool // gathered, and the chain does not qualify (head read counts)
	maxTx     types.Instant
	maxTxTo   types.Instant
	maxVer    uint32
	lastStart types.Instant
	lastEnd   types.Instant
	cascade   bool // set from the sweep
	ownOpen   bool // some row's own interval is open
	txOrdered bool // TxFrom never falls with version
	bounds    []types.Instant
	open      openBelief
	void      bool // some row is void
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
	bounds := make([]types.Instant, 0, 2*len(filtered))
	a := c.axis()
	f.txOrdered = true
	for i, r := range filtered {
		tm := r.Temporal()
		if tm == nil || tm.DeletedAt != 0 || tm.Retracted || (i > 0 && filtered[i-1].Version() >= r.Version()) {
			f.refused = true
			return
		}
		f.txOrdered = f.txOrdered && tm.TxFrom >= f.maxTx
		f.maxTx, f.maxTxTo = max(f.maxTx, tm.TxFrom), max(f.maxTxTo, tm.TxTo)
		f.void = f.void || tm.Void
		s, e := k.ownBounds(c, r)
		bounds = append(bounds, s)
		if !a.Open(e) {
			bounds = append(bounds, e)
		} else {
			f.ownOpen = true
			if !tm.Void {
				f.open.note(tm.TxFrom, r.Version())
			}
		}
	}
	last := filtered[len(filtered)-1]
	f.maxVer = last.Version()
	f.lastStart, f.lastEnd = k.sortVF(c, last), last.Temporal().ValidTo
	f.bounds = sortedBounds(bounds)
	f.ok = true
}

// sortedBounds sorts and de-duplicates b in place.
func sortedBounds(b []types.Instant) []types.Instant {
	slices.Sort(b)
	return slices.Clip(slices.Compact(b))
}

// headFromFold builds the entry for a fold's pieces and facts (trimmed to
// c's caps), or nil when the chain did not qualify.
func headFromFold[T interface {
	comparable
	storeutil.TemporalRow
}](c *Core, epoch uint64, current T, pieces []effPiece[T], f *headFacts) *headEntry {
	if epoch == 0 || !f.ok {
		return nil
	}
	e := &headEntry{epoch: epoch, curVer: current.Version(), curTM: temporalOf(current),
		maxTx: f.maxTx, maxTxTo: f.maxTxTo, maxVer: f.maxVer, cascade: f.cascade, ownOpen: f.ownOpen, txOrdered: f.txOrdered,
		lastStart: f.lastStart, lastEnd: f.lastEnd, pieces: make([]headPiece, len(pieces)),
		bounds: f.bounds, open: f.open, void: f.void, pieceFrom: headWhole, boundFrom: headWhole}
	for i, p := range pieces {
		e.pieces[i] = headPiece{from: p.from, to: p.to, version: p.row.Version()}
	}
	e.trim(c.headCaps())
	return e
}

// trim keeps the newest pieces and bounds within the caps and raises the
// watermarks to the first kept: the pieces are disjoint and ascending, so
// every dropped piece ends at or before the first kept one, and every
// dropped bound lies below the first kept one.
func (e *headEntry) trim(pieces, bounds int) {
	if n := len(e.pieces) - pieces; n > 0 {
		e.pieces = slices.Clone(e.pieces[n:])
		e.pieceFrom = max(e.pieceFrom, e.pieces[0].from)
	}
	if n := len(e.bounds) - bounds; n > 0 {
		e.bounds = slices.Clone(e.bounds[n:])
		e.boundFrom = max(e.boundFrom, e.bounds[0])
	}
}

// above and stamps answer, from the entry, what an append planned from the
// current row probes the store for (chainFromCurrent): a history row above
// a version, and the chain's newest TxFrom and TxTo (the current row's own
// are among them; it is checked apart).
func (e *headEntry) above(v uint32) (bool, error) { return e.maxVer > v, nil }

func (e *headEntry) stamps() (types.Instant, types.Instant, bool, error) {
	return e.maxTx, e.maxTxTo, true, nil
}

// whole reports an entry that dropped no piece: it answers reads.
func (e *headEntry) whole() bool { return e.pieceFrom == headWhole }

// plans reports whether a correction of [newVF, newVT) (newVT open by the
// valid axis a) lies at or above the entry's watermarks: its pieces and the
// resumption's bound are exact there.
func (e *headEntry) plans(a types.ValidAxis, newVF, newVT types.Instant) bool {
	return newVF >= e.pieceFrom && (a.Open(newVT) || newVT >= e.boundFrom)
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
	bounds := slices.Clone(e.bounds)
	spans := make([]headSpan, 0, len(appended))
	a := c.axis()
	monotonic := true // the grown chain stays in version order by start and tiles
	prevStart, prevEnd := e.lastStart, e.lastEnd
	for _, r := range appended {
		tm := r.Temporal()
		if tm == nil || tm.DeletedAt != 0 || tm.Retracted || tm.TxTo != 0 ||
			r.Version() <= out.maxVer || tm.TxFrom < out.maxTx || tm.TxFrom <= out.maxTxTo {
			return nil
		}
		s, end := k.sortVF(c, r), tm.ValidTo
		if (!a.Open(end) && end <= s) || s < out.pieceFrom {
			return nil // empty, or below the watermark: what it paints over is unknown
		}
		// A row sharing the previous row's start but ending earlier makes the
		// fold take the own-bounds arm (tilesWithNext, SAMESTART).
		monotonic = monotonic && s >= out.lastStart && (s != prevStart || endsNoEarlier(a, end, prevEnd))
		prevStart, prevEnd = s, end
		spans = append(spans, headSpan{headPiece: headPiece{from: s, to: end, version: r.Version()}, void: tm.Void})
		out.void = out.void || tm.Void
		out.maxTx, out.maxVer = tm.TxFrom, r.Version()
		out.lastStart, out.lastEnd = s, end
		bounds = append(bounds, s)
		if !a.Open(end) {
			bounds = append(bounds, end)
		} else {
			out.ownOpen = true
			if !tm.Void {
				out.open.note(tm.TxFrom, r.Version())
			}
		}
	}
	if !out.cascade {
		switch {
		case !c.bitemporalMigrated:
			// Monotonic arm: the resolver's positional start equals the
			// classifier's key only once the store is migrated.
			return nil
		case monotonic:
			if !headTilingKept(a, e.lastEnd, spans) {
				return nil
			}
		case e.armSwitches():
			out.cascade = true // the fold of the grown chain takes the own-bounds arm
		default:
			return nil
		}
	}
	out.pieces = paintHeadAll(a, e.pieces, resolveHeadSpans(a, spans))
	out.bounds = sortedBounds(bounds)
	out.trim(c.headCaps())
	return &out
}

// headSpan is an appended row's own interval: what it wins in the grown
// chain (a gap when void).
type headSpan struct {
	headPiece
	void bool
}

// headTilingKept reports whether the monotonic arm's tiling of the grown
// chain equals the paint of spans (ascending by version, each starting at or
// after the entry's newest row, whose own end is lastEnd). A bounded row
// tiles its own interval (an own ValidTo overrides the next row's start), so
// the paint and the tiling agree on it; an open row tiles only up to the
// next row's start and loses the rest, which the paint keeps unless the rows
// after it cover it: the newest row of the entry from the first span's start
// on (the bounded correction of an open row plus its open resumption, row
// 88); an open appended row from the next one's start on — allowed only as
// the last row (no door appends another).
func headTilingKept(a types.ValidAxis, lastEnd types.Instant, spans []headSpan) bool {
	for _, sp := range spans[:len(spans)-1] {
		if a.Open(sp.to) {
			return false
		}
	}
	if !a.Open(lastEnd) {
		return true
	}
	to := spans[0].from
	for _, sp := range spans {
		if sp.from > to {
			return false // a gap the newest row keeps in the paint and loses in the tiling
		}
		if a.Open(sp.to) {
			return true
		}
		to = max(to, sp.to)
	}
	return false // the newest row keeps [to, +inf) in the paint and loses it in the tiling
}

// armSwitches reports whether a monotonic entry's pieces are also the
// own-bounds arm's answer on its chain, so appended rows that make the chain
// non-monotonic (the fold then takes that arm) can paint over them: every
// row is bounded (its positional interval is its own interval), none was
// replaced (no supersession cap) and TxFrom follows version (the belief order
// is the positional order).
func (e *headEntry) armSwitches() bool {
	return !e.cascade && !e.ownOpen && e.txOrdered && e.maxTxTo == 0
}

// resolveHeadSpans returns spans (ascending by version) as disjoint spans in
// ascending order: where two overlap the higher version wins, as the newest
// belief and the highest position do. Disjoint ascending spans (the cascade's
// pieces and resumption) are returned as they are.
func resolveHeadSpans(a types.ValidAxis, spans []headSpan) []headSpan {
	disjoint := true
	for i := 1; i < len(spans) && disjoint; i++ {
		p := spans[i-1]
		disjoint = !a.Open(p.to) && p.to <= spans[i].from
	}
	if disjoint {
		return spans
	}
	void := make(map[uint32]bool, len(spans))
	var acc []headPiece
	for _, sp := range spans {
		void[sp.version] = sp.void
		acc = paintHead(a, acc, sp.from, sp.to, sp.version, false)
	}
	out := make([]headSpan, len(acc))
	for i, p := range acc {
		out[i] = headSpan{headPiece: p, void: void[p.version]}
	}
	return out
}

// paintHeadAll makes every span of spans (disjoint, ascending) won by its
// version (a gap when void) over pieces (disjoint, ascending) in one pass:
// O(pieces + spans), where a paint per span copied the pieces each time (a
// correction from the chain's start appends one row per piece, row 87).
func paintHeadAll(a types.ValidAxis, pieces []headPiece, spans []headSpan) []headPiece {
	out := make([]headPiece, 0, len(pieces)+2*len(spans))
	next := 0
	var carry headPiece // the rest of a piece a span cut, ahead of pieces[next]
	hasCarry := false
	take := func() (headPiece, bool) {
		if hasCarry {
			hasCarry = false
			return carry, true
		}
		if next < len(pieces) {
			next++
			return pieces[next-1], true
		}
		return headPiece{}, false
	}
	for _, sp := range spans {
		for {
			p, ok := take()
			if !ok {
				break
			}
			if !a.Open(p.to) && p.to <= sp.from {
				out = append(out, p) // wholly before the span
				continue
			}
			if p.from < sp.from {
				out = append(out, headPiece{from: p.from, to: sp.from, version: p.version})
			}
			if !a.Open(sp.to) && (a.Open(p.to) || p.to > sp.to) {
				carry, hasCarry = p, true // the part after the span
				if carry.from < sp.to {
					carry.from = sp.to
				}
				break
			}
			// The piece ends inside the span: painted over.
		}
		if !sp.void {
			out = append(out, sp.headPiece)
		}
	}
	for {
		p, ok := take()
		if !ok {
			break
		}
		out = append(out, p)
	}
	return mergeHeadPieces(out)
}

// paintHead makes [s, end) (end open by the valid axis a) won by version (a
// gap when void).
func paintHead(a types.ValidAxis, pieces []headPiece, s, end types.Instant, version uint32, void bool) []headPiece {
	out := make([]headPiece, 0, len(pieces)+2)
	for _, p := range pieces {
		// The part of p before s.
		if p.from < s {
			q := p
			if a.Open(q.to) || q.to > s {
				q.to = s
			}
			out = append(out, q)
		}
		// The part of p after end.
		if !a.Open(end) && (a.Open(p.to) || p.to > end) {
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
	return mergeHeadPieces(out)
}

// mergeHeadPieces merges, in place, touching pieces of one winner among
// ascending pieces (the sweep's normal form).
func mergeHeadPieces(out []headPiece) []headPiece {
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

// nodeHeadSegments answers a head read from a valid entry, the segments that
// reach validFrom, reading only their winners; a reason other than headHit
// falls back to the fold (no entry, a stale one, or a winner gone).
func (c *Core) nodeHeadSegments(rd entityReads, id types.NodeID, current *types.Node, validFrom, pin types.Instant, epoch uint64) ([]temporal.NodeSegment, headReason) {
	e, r := c.headLookup(headKey{id: id.SnowflakeID()}, epoch, current.Version(), temporalOf(current), pin)
	if e == nil {
		return nil, r
	}
	if !e.whole() {
		return nil, headMissNotWhole
	}
	segs := make([]temporal.NodeSegment, 0, len(e.pieces))
	for _, p := range e.pieces {
		if !segmentReaches(c.axis(), p.to, validFrom) {
			continue
		}
		row := current
		if p.version != current.Version() {
			full, err := rd.lentNodeVersion(id, p.version) // handed out frozen: no copy needed
			if err != nil {
				return nil, headMissRowGone
			}
			row = full
		}
		row, ok := nodeRowAtTx(c.axis(), row, pin)
		if !ok {
			return nil, headMissRowGone
		}
		segs = append(segs, nodeSegment(p.from, p.to, row))
	}
	c.headHits.Add(1)
	return segs, headHit
}

// relHeadSegments mirrors nodeHeadSegments.
func (c *Core) relHeadSegments(rd entityReads, id types.RelID, current *types.Relationship, pin types.Instant, epoch uint64) ([]temporal.RelSegment, bool) {
	e := c.headValid(headKey{rel: true, id: id.SnowflakeID()}, epoch, current.Version(), temporalOf(current), pin)
	if e == nil || !e.whole() {
		return nil, false
	}
	segs := make([]temporal.RelSegment, 0, len(e.pieces))
	for _, p := range e.pieces {
		row := current
		if p.version != current.Version() {
			full, err := rd.lentRelVersion(id, p.version) // handed out frozen: no copy needed
			if err != nil {
				return nil, false
			}
			row = full
		}
		row, ok := relRowAtTx(c.axis(), row, pin)
		if !ok {
			return nil, false
		}
		segs = append(segs, relSegment(p.from, p.to, row))
	}
	c.headHits.Add(1)
	return segs, true
}

// nodeHeadTimelines answers the plural door (NodeEffectiveTimelines) from a
// valid entry when every pin is at or above its newest TxFrom (RELHIST P2):
// each winner is read once and normalized per pin as the fold normalizes
// it; a reason other than headHit falls back to the fold.
func (c *Core) nodeHeadTimelines(rd entityReads, id types.NodeID, current *types.Node, pins []types.Instant, epoch uint64) ([][]temporal.NodeSegment, headReason) {
	e, r := c.headLookup(headKey{id: id.SnowflakeID()}, epoch, current.Version(), temporalOf(current), slices.Min(pins))
	if e == nil {
		return nil, r
	}
	if !e.whole() {
		return nil, headMissNotWhole
	}
	out, ok := headTimelinesOf(c, nodeKernel, e, current, pins,
		func(v uint32) (*types.Node, error) { return rd.lentNodeVersion(id, v) }, nodeSegment) // lent frozen: no copy needed
	if !ok {
		return nil, headMissRowGone
	}
	c.headHits.Add(1)
	return out, headHit
}

// relHeadTimelines mirrors nodeHeadTimelines.
func (c *Core) relHeadTimelines(rd entityReads, id types.RelID, current *types.Relationship, pins []types.Instant, epoch uint64) ([][]temporal.RelSegment, bool) {
	e := c.headValid(headKey{rel: true, id: id.SnowflakeID()}, epoch, current.Version(), temporalOf(current), slices.Min(pins))
	if e == nil || !e.whole() {
		return nil, false
	}
	out, ok := headTimelinesOf(c, relKernel, e, current, pins,
		func(v uint32) (*types.Relationship, error) { return rd.lentRelVersion(id, v) }, relSegment)
	if ok {
		c.headHits.Add(1)
	}
	return out, ok
}

// headTimelinesOf is e's answer at every pin: the winner of each piece read
// once (the current row, or read), then per pin rowAtTx'd into a segment.
// A pin's answer with no piece is nil, as the plural fold's.
func headTimelinesOf[T interface {
	comparable
	storeutil.TemporalRow
}, S any](c *Core, k *chainKernel[T], e *headEntry, current T, pins []types.Instant, read func(uint32) (T, error), seg func(from, to types.Instant, row T) S) ([][]S, bool) {
	rows := make([]T, len(e.pieces))
	for i, p := range e.pieces {
		if p.version == current.Version() {
			rows[i] = current
			continue
		}
		r, err := read(p.version)
		if err != nil {
			return nil, false
		}
		rows[i] = r
	}
	out := make([][]S, len(pins))
	if len(e.pieces) == 0 {
		return out, true
	}
	for j, pin := range pins {
		segs := make([]S, 0, len(e.pieces))
		for i, p := range e.pieces {
			row, ok := k.rowAtTx(c.axis(), rows[i], pin)
			if !ok {
				return nil, false
			}
			segs = append(segs, seg(p.from, p.to, row))
		}
		out[j] = segs
	}
	return out, true
}

// noteNodeHead keeps the head answer a fold produced (no-op unless it qualifies).
func (c *Core) noteNodeHead(id types.NodeID, epoch uint64, current *types.Node, pieces []effPiece[*types.Node], f *headFacts) {
	if e := headFromFold(c, epoch, current, pieces, f); e != nil {
		c.heads.put(headKey{id: id.SnowflakeID()}, e)
	}
}

func (c *Core) noteRelHead(id types.RelID, epoch uint64, current *types.Relationship, pieces []effPiece[*types.Relationship], f *headFacts) {
	if e := headFromFold(c, epoch, current, pieces, f); e != nil {
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

// headSupersede is e extended by a superseding write's new current row n
// (RELHIST P2: a plain Update, ReplaceVersion or CloseVersion moves the
// current row e was computed with into history with TxTo = superTx and
// writes n at TxFrom = superTx), or nil where it is not provably the fold's
// answer. It is headOverlay of n plus the one thing the old current row's
// TxTo changes: when the grown chain resolves in the own-bounds arm, n is a
// replacing row (chain_supersession.go) and caps every row starting at or
// before n at n's start. Where n's own interval is open, n covers all of
// [start, open) anyway, so the paint stands. Where it ends, the instants
// after its end lose every row that starts at or before n; when no row of
// the chain has a bound after n's start, no row is left there and those
// instants are a gap; otherwise the answer there is not known from the
// entry and the entry is dropped. The monotonic arm (headOverlay's tiling
// check) never reads TxTo.
func headSupersede[T interface {
	comparable
	storeutil.TemporalRow
}](c *Core, k *chainKernel[T], e *headEntry, n T, superTx types.Instant) *headEntry {
	if e == nil {
		return nil
	}
	tm := n.Temporal()
	if tm == nil || tm.TxFrom != superTx {
		return nil
	}
	out := headOverlay(c, k, e, []T{n})
	if out == nil {
		return nil
	}
	if a := c.axis(); out.cascade && !a.Open(tm.ValidTo) {
		s := k.sortVF(c, n)
		// n replaces (and so caps) only when the superseded row's TxTo
		// lies after its TxFrom; then nothing may start after s.
		if superTx <= e.curTM.TxFrom || e.curTM.DeletedAt != 0 || len(e.bounds) == 0 || e.bounds[len(e.bounds)-1] > s {
			return nil
		}
		end := tm.ValidTo
		out.pieces = slices.DeleteFunc(out.pieces, func(p headPiece) bool { return p.from >= end })
	}
	out.maxTxTo = max(out.maxTxTo, superTx)
	return out
}

// nodeHeadAfterSupersede extends before (the node's valid entry read before
// the write, nil: none) by a superseding write's new current row cur, once
// written (entity lock still held), or drops the entry.
func (c *Core) nodeHeadAfterSupersede(id types.NodeID, before *headEntry, cur *types.Node, superTx types.Instant) {
	if before == nil || c.historyEpochs == nil || c.headCacheOff {
		return // no entry was valid: whatever is kept is stale by the token
	}
	k := headKey{id: id.SnowflakeID()}
	e := headSupersede(c, nodeKernel, before, cur, superTx)
	if e == nil {
		c.heads.put(k, nil)
		return
	}
	e.epoch, e.curVer, e.curTM = c.nodeHistoryEpoch(id), cur.Version(), temporalOf(cur)
	c.heads.put(k, e)
	c.headOverlays.Add(1)
}

// relHeadAfterSupersede mirrors nodeHeadAfterSupersede.
func (c *Core) relHeadAfterSupersede(id types.RelID, before *headEntry, cur *types.Relationship, superTx types.Instant) {
	if before == nil || c.historyEpochs == nil || c.headCacheOff {
		return
	}
	k := headKey{rel: true, id: id.SnowflakeID()}
	e := headSupersede(c, relKernel, before, cur, superTx)
	if e == nil {
		c.heads.put(k, nil)
		return
	}
	e.epoch, e.curVer, e.curTM = c.relHistoryEpoch(id), cur.Version(), temporalOf(cur)
	c.heads.put(k, e)
	c.headOverlays.Add(1)
}
