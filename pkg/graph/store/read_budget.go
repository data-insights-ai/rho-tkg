package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Read budgets (RH list R2, sigma's selective doors): a caller bounds the
// internal work of one selective read, not only the rows it returns, by
// putting a ReadBudget in the call's context. A door that supports budgets
// counts its work in the context's ReadMeter and, at a limit, returns a
// *ReadBudgetError (errors.Is ErrReadBudget) carrying the counters, before it
// starts the refused work. Slice doors return all or an error; streaming
// callbacks delivered before refusal remain valid. A context without a budget
// reads as before: general callers keep their contract.

// ErrReadBudget is the sentinel of a selective read stopped at its budget.
var ErrReadBudget = errors.New("graph: read budget exceeded")

// ReadBudget bounds one selective read. A zero field is unbounded.
type ReadBudget struct {
	// Candidates bounds the entries a door examines before filtering: the
	// members an index lookup yields (each read as a timeline afterwards), the
	// IDs a by-ID or history probe looks at.
	Candidates uint64
	// Segments bounds the timeline segments the doors return.
	Segments uint64
	// ShardOpens bounds the closed shards the reads open (tiered). For a meter
	// that holds its opens (HoldShardOpens) a shard counts once however often
	// it is opened; Work.ShardOpens counts every open.
	ShardOpens uint64
	// DecodedBytes bounds stored bytes handed to a decoder, charged before
	// decoding: current rows, history skeletons, winners and delta anchors,
	// encoded memory relationship segments, and property-index bounds.
	// Cache hits and already decoded RAM rows add no decode bytes. A partial
	// decode counts its whole input; repeated decodes count again. It measures
	// logical decoder input, not physical I/O, block decompression or live heap.
	// Covered doors and native tests are listed in docs/api.md.
	DecodedBytes uint64
	// HistoryExamined bounds the history versions the doors' reads look at:
	// each history row the store hands to a reader (a skeleton entry, a
	// temporal peek of an as-of walk, a decode), counted before it is looked
	// at, so a long history stops at the version that passes the limit. A
	// version read twice (skeleton, then winner) counts twice. The memory
	// store counts the versions of an entity's chain before it walks them.
	HistoryExamined uint64
	// BufferBytes bounds the bytes a read holds in its temporary buffers
	// (candidate sets and member lists, the rows and segments it collects
	// before it returns them), a logical estimate per entry (8 per ID or row
	// pointer, 24 per set entry, a segment's struct size), charged before each
	// entry is added, so a read stops before the buffer grows past the limit.
	// It is not the Go heap: the stores' own buffers (badger blocks, caches)
	// are not counted.
	BufferBytes uint64
}

// ReadWork is what the reads made with one context did, counted by the doors
// that support budgets (fields a door does not count stay 0).
type ReadWork struct {
	Candidates      uint64
	Segments        uint64
	ShardOpens      uint64
	DecodedBytes    uint64
	HistoryExamined uint64
	BufferBytes     uint64
}

// ReadBudgetError is a read stopped by Door at Limit ("candidates",
// "segments", "shard opens", "shards held", "decoded bytes", "history
// examined", "buffer bytes" or "unsupported") of Budget. Unsupported refuses work that cannot be
// charged, before it starts; other limits name the bound it passed. Work is
// that budget's meter's counters at refusal. For a numeric limit the charge
// has just been added: Work.<Limit> is above Budget.<Limit>, and every field
// is at most the meter's Work read after the door returned. They are equal when one
// goroutine charges the meter and nothing is charged after the refusal; the
// parallel shards of a tiered fan-out may each make their own refusal, joined
// (errors.As gives the first in shard order, ReadMeter.Exceeded the first in
// time).
type ReadBudgetError struct {
	Door   string
	Limit  string
	Budget ReadBudget
	Work   ReadWork
	// ShardOpensLimited and ShardWindows are the refusing meter's shard-open
	// scope at a "shard opens" refusal (LimitShardOpens, the number of
	// AllowShardWindows): with the limit a Budget.ShardOpens of 0 allows none.
	ShardOpensLimited bool
	ShardWindows      int
	// ShardsHeldBound is the store's bound at a "shards held" refusal
	// (ShardHoldStrict): the shards one meter keeps open at once.
	ShardsHeldBound int
}

func (e *ReadBudgetError) Error() string {
	scope := ""
	if e.ShardOpensLimited {
		scope = fmt.Sprintf("; shard opens are limited to the budget's %d beyond the shards of %d windows", e.Budget.ShardOpens, e.ShardWindows)
	}
	if e.Limit == "shards held" {
		scope = fmt.Sprintf("; the read needs more than the %d closed shards the store keeps open for one caller at once", e.ShardsHeldBound)
	}
	return fmt.Sprintf("%v: %s at %s (budget %+v, work %+v%s)", ErrReadBudget, e.Door, e.Limit, e.Budget, e.Work, scope)
}

// Is makes errors.Is(err, ErrReadBudget) true.
func (e *ReadBudgetError) Is(target error) bool { return target == ErrReadBudget }

// ReadMeter counts the work of every read made with its context, cumulative
// over the calls; safe for concurrent use. A meter made inside another
// budget's context adds its work to the outer meter too, and each meter checks
// its own limits, so the tighter one stops first.
type ReadMeter struct {
	budget                                         ReadBudget
	parent                                         *ReadMeter
	candidateMu                                    sync.Mutex // the outermost meter orders candidate admissions for its whole tree
	candidates, segments, shardOpens, decodedBytes atomic.Uint64
	historyExamined, bufferBytes                   atomic.Uint64
	exceeded                                       atomic.Pointer[ReadBudgetError]    // the first refusal, for Exceeded
	shardOpensLimited                              atomic.Bool                        // LimitShardOpens
	shardWindows                                   atomic.Pointer[[][2]types.Instant] // AllowShardWindows; the slice is never changed
	askedOpens                                     atomic.Uint64                      // the opens outside shardWindows: what ShardOpens limits
	// Shard holds (HoldShardOpens): the shards a store opened for this meter's
	// reads and keeps open until Release, oldest first. holder is the meter
	// whose holds a meter made beside it joins (ScopeLike).
	holdMode atomic.Int32
	holdMu   sync.Mutex
	holds    []heldShard
	holder   *ReadMeter
	paid     map[[2]types.Instant]struct{} // the windowed shards the limit counted (paidShard); under holdMu
}

// LimitShardOpens makes this meter's ShardOpens a limit at any value, 0
// included: the first closed shard a read would open past it is refused before
// the open (limit "shard opens"). A caller whose reads stay on the shards its
// keys and window keep open calls it once after WithReadBudget and names in
// ReadBudget.ShardOpens every older shard it may open (CUTCOST: a read that
// fans out over closed shards is then a refusal, not a cost). The budget
// itself is unchanged, so a refusal carries the caller's ReadBudget.
func (m *ReadMeter) LimitShardOpens() {
	if m != nil {
		m.shardOpensLimited.Store(true)
	}
}

// AllowShardWindows names windows [from, to] (both inclusive) of the time a
// tiered store places its event shards by (valid time under
// tiered.Config.MintAtValidFrom): this meter's reads open a closed shard whose
// own window meets one of them without drawing on ShardOpens. Such an open
// still counts in Work.ShardOpens; every other closed shard draws on the limit
// as before. With LimitShardOpens a caller thereby opens the shards its keys'
// instants name, one window an instant, and no shard between them, and asks
// for older history with ReadBudget.ShardOpens. A later call replaces the
// windows; none names no shard.
func (m *ReadMeter) AllowShardWindows(windows ...[2]types.Instant) {
	if m == nil {
		return
	}
	kept := make([][2]types.Instant, 0, len(windows))
	for _, w := range windows {
		if w[0] <= w[1] {
			kept = append(kept, w)
		}
	}
	m.shardWindows.Store(&kept)
}

// ShardHoldMode says how a store keeps the closed shards it opens for a
// meter's reads (ReadMeter.HoldShardOpens).
type ShardHoldMode int32

const (
	// ShardHoldNone: the store's own rule (tiered: a cold shard stays open
	// while fewer than Config.MaxOpenColdShards are, else closes after each
	// read).
	ShardHoldNone ShardHoldMode = iota
	// ShardHoldStrict: each shard is opened read-only for a read and stays
	// open until Release; the days the caller names for its write are opened
	// writable, or upgraded, before it writes (tiered: HoldShardsAt). At most
	// the store's bound of them at once (tiered: Config.MaxOpenColdShards): one
	// more is refused, limit "shards held".
	ShardHoldStrict
	// ShardHoldLRU: each shard is opened read-only and stays open until
	// Release or until the bound is reached, when the one held longest is
	// closed first. A shard closed so and read again is opened, and counted,
	// again.
	ShardHoldLRU
)

type heldShard struct {
	key     any
	release func()
}

// HoldShardOpens makes the stores this meter's reads reach keep each closed
// shard they open for it open until Release, in mode. So a shard a caller
// reads many times is opened, and counted, once; no handle is left behind
// when the caller is done; and the same reads open the same shards whatever
// was open before. Meters made inside this meter's context, and one scoped
// like it (ScopeLike), hold on it. The caller must call Release.
func (m *ReadMeter) HoldShardOpens(mode ShardHoldMode) {
	if m != nil {
		m.holdMode.Store(int32(mode))
	}
}

// Release closes every shard held for m (HoldShardOpens), the latest first,
// and ends the holding: a shard opened for m afterwards is not held, so none
// is kept that no Release would close. Calling it again, or on a meter that
// holds none, does nothing.
func (m *ReadMeter) Release() {
	if m == nil {
		return
	}
	m.holdMode.Store(int32(ShardHoldNone))
	m.holdMu.Lock()
	holds := m.holds
	m.holds = nil
	m.holdMu.Unlock()
	for i := len(holds) - 1; i >= 0; i-- {
		holds[i].release()
	}
}

// holdRoot is the meter m's opened shards are held on: the one it was scoped
// like, else the nearest of m and its outer meters that holds; nil: none.
func (m *ReadMeter) holdRoot() *ReadMeter {
	if m == nil {
		return nil
	}
	if m.holder != nil {
		return m.holder
	}
	for r := m; r != nil; r = r.parent {
		if ShardHoldMode(r.holdMode.Load()) != ShardHoldNone {
			return r
		}
	}
	return nil
}

// HoldMode is the mode the tally's opens are held in (ShardHoldNone: not held).
// For a store.
func (t *ReadTally) HoldMode() ShardHoldMode {
	if t == nil {
		return ShardHoldNone
	}
	if r := t.meter.holdRoot(); r != nil {
		return ShardHoldMode(r.holdMode.Load())
	}
	return ShardHoldNone
}

// HoldsShard reports whether key is held for the tally's meter, HeldShards how
// many are. For a store.
func (t *ReadTally) HoldsShard(key any) bool {
	r := t.root()
	if r == nil {
		return false
	}
	r.holdMu.Lock()
	defer r.holdMu.Unlock()
	for _, h := range r.holds {
		if h.key == key {
			return true
		}
	}
	return false
}

func (t *ReadTally) HeldShards() int {
	r := t.root()
	if r == nil {
		return 0
	}
	r.holdMu.Lock()
	defer r.holdMu.Unlock()
	return len(r.holds)
}

// HoldShard records that the store keeps key open for the tally's meter until
// release is called, which Release or ReleaseOldestShard does. For a store.
func (t *ReadTally) HoldShard(key any, release func()) {
	r := t.root()
	if r == nil {
		release()
		return
	}
	r.holdMu.Lock()
	r.holds = append(r.holds, heldShard{key, release})
	r.holdMu.Unlock()
}

// NoteShardOpen counts one shard open in the work of the tally's meter and its
// outer meters without admitting it: for a store whose write opens or
// upgrades a shard the meter holds. The write is not a read the budget could
// refuse; the open is counted where the shard's other opens are. A nil tally
// counts nothing.
func (t *ReadTally) NoteShardOpen() {
	if t == nil {
		return
	}
	for r := t.meter; r != nil; r = r.parent {
		r.shardOpens.Add(1)
	}
}

// RekeyHeld names the hold the tally's meter has under key old by key from now
// on (HoldsShard); false when it has none. For a store that held a place for a
// shard before the shard existed.
func (t *ReadTally) RekeyHeld(old, key any) bool {
	r := t.root()
	if r == nil {
		return false
	}
	r.holdMu.Lock()
	defer r.holdMu.Unlock()
	for i := range r.holds {
		if r.holds[i].key == old {
			r.holds[i].key = key
			return true
		}
	}
	return false
}

// ReleaseOldestShard closes the shard held longest for the tally's meter;
// false when none is held. For a store in ShardHoldLRU.
func (t *ReadTally) ReleaseOldestShard() bool {
	r := t.root()
	if r == nil {
		return false
	}
	r.holdMu.Lock()
	if len(r.holds) == 0 {
		r.holdMu.Unlock()
		return false
	}
	h := r.holds[0]
	r.holds = r.holds[1:]
	r.holdMu.Unlock()
	h.release()
	return true
}

// RefuseHeld refuses one more held shard past bound (ShardHoldStrict): limit
// "shards held", the meter's refusal from then on. For a store.
func (t *ReadTally) RefuseHeld(bound int) error {
	if t == nil {
		return nil
	}
	e := &ReadBudgetError{Door: t.door, Limit: "shards held", Budget: t.meter.budget, Work: t.meter.Work(), ShardsHeldBound: bound}
	t.meter.exceeded.CompareAndSwap(nil, e)
	return t.meter.exceeded.Load()
}

func (t *ReadTally) root() *ReadMeter {
	if t == nil {
		return nil
	}
	return t.meter.holdRoot()
}

// ScopeLike gives m the shard-open scope of other: its LimitShardOpens and
// its AllowShardWindows, and holds its opened shards where other's are held. A meter made beside a refused one from that meter's
// ReadBudget (a repair after the refusal) calls it, so the repair opens what
// the refused read could have opened and no more. A nil other changes nothing.
func (m *ReadMeter) ScopeLike(other *ReadMeter) {
	if m == nil || other == nil {
		return
	}
	m.shardOpensLimited.Store(other.shardOpensLimited.Load())
	m.shardWindows.Store(other.shardWindows.Load())
	m.holder = other.holdRoot()
}

// paidShard reports whether m's limit already counted a shard of this window
// and, when not, notes it. For opens that are held (HoldShardOpens) the limit
// counts a shard once per meter: ShardOpens then names shards, not how often
// a hold that keeps few of them open at once opens each (CUTCOST P3). A shard
// without a window (an archive) and every open that is not held count each
// time, as before.
func (m *ReadMeter) paidShard(shard *[2]types.Instant) bool {
	if shard == nil {
		return false
	}
	m.holdMu.Lock()
	defer m.holdMu.Unlock()
	if _, ok := m.paid[*shard]; ok {
		return true
	}
	if m.paid == nil {
		m.paid = map[[2]types.Instant]struct{}{}
	}
	m.paid[*shard] = struct{}{}
	return false
}

// allowsShard reports whether a shard of window [start, end) meets one of m's
// windows.
func (m *ReadMeter) allowsShard(shard *[2]types.Instant) bool {
	ws := m.shardWindows.Load()
	if ws == nil || shard == nil {
		return false
	}
	for _, w := range *ws {
		if shard[0] <= w[1] && w[0] < shard[1] {
			return true
		}
	}
	return false
}

type readMeterKey struct{}

// WithReadBudget returns ctx carrying budget and the meter that counts the
// work of the reads made with it. Read the counters with Work, under or over
// the limit.
func WithReadBudget(ctx context.Context, budget ReadBudget) (context.Context, *ReadMeter) {
	m := &ReadMeter{budget: budget, parent: ReadMeterFrom(ctx)}
	return context.WithValue(ctx, readMeterKey{}, m), m
}

// ReadMeterFrom returns ctx's meter, or nil when ctx carries no budget.
func ReadMeterFrom(ctx context.Context) *ReadMeter {
	if ctx == nil {
		return nil
	}
	m, _ := ctx.Value(readMeterKey{}).(*ReadMeter)
	return m
}

// BoundsStoreWork reports whether this meter or an outer one bounds work only
// the store itself can count (DecodedBytes, ShardOpens): a store that cannot
// count it must refuse such a budget rather than read unbounded.
func (m *ReadMeter) BoundsStoreWork() bool {
	for r := m; r != nil; r = r.parent {
		if r.budget.DecodedBytes != 0 || r.budget.ShardOpens != 0 || r.shardOpensLimited.Load() {
			return true
		}
	}
	return false
}

// BoundsShardOpens reports whether this meter or an outer one bounds
// ShardOpens.
func (m *ReadMeter) BoundsShardOpens() bool {
	for r := m; r != nil; r = r.parent {
		if r.budget.ShardOpens != 0 || r.shardOpensLimited.Load() {
			return true
		}
	}
	return false
}

// BoundsOutput reports whether this meter or an outer one bounds what a read
// keeps (Segments, BufferBytes): a read that decodes ahead of what it keeps
// then reads one entity at a time, so a refusal stops it before the next
// entity's decode.
func (m *ReadMeter) BoundsOutput() bool {
	for r := m; r != nil; r = r.parent {
		if r.budget.Segments != 0 || r.budget.BufferBytes != 0 {
			return true
		}
	}
	return false
}

// Work returns the counters so far.
func (m *ReadMeter) Work() ReadWork {
	if m == nil {
		return ReadWork{}
	}
	return ReadWork{Candidates: m.candidates.Load(), Segments: m.segments.Load(), ShardOpens: m.shardOpens.Load(), DecodedBytes: m.decodedBytes.Load(), HistoryExamined: m.historyExamined.Load(), BufferBytes: m.bufferBytes.Load()}
}

// AddCandidates counts n candidates door examined, here and in every outer
// meter, and returns the budget error of the innermost meter whose candidate
// limit the total passes. A nil meter counts nothing.
func (m *ReadMeter) AddCandidates(door string, n int) error {
	if m == nil {
		return nil
	}
	root := m
	for root.parent != nil {
		root = root.parent
	}
	root.candidateMu.Lock()
	defer root.candidateMu.Unlock()
	if err := m.Exceeded(); err != nil {
		return err
	}
	return m.add(door, "candidates", n, func(r *ReadMeter) (*atomic.Uint64, uint64) { return &r.candidates, r.budget.Candidates })
}

// AddSegments counts n returned segments, as AddCandidates.
func (m *ReadMeter) AddSegments(door string, n int) error {
	return m.add(door, "segments", n, func(r *ReadMeter) (*atomic.Uint64, uint64) { return &r.segments, r.budget.Segments })
}

// AddShardOpens counts n closed shards opened, as AddCandidates.
func (m *ReadMeter) AddShardOpens(door string, n int) error {
	return m.addShardOpens(door, n, nil)
}

// AddShardOpensIn is AddShardOpens for closed shards whose own window is
// [start, end): a meter whose AllowShardWindows meet it counts them outside
// its limit.
func (m *ReadMeter) AddShardOpensIn(door string, n int, start, end types.Instant) error {
	return m.addShardOpens(door, n, &[2]types.Instant{start, end})
}

// addShardOpens counts n opens of a shard of window shard (nil: none known) in
// m and its outer meters; each meter checks its own limit on the opens outside
// its allowed windows (askedOpens, counted on their own so parallel shards
// never see a sum of two counters), a zero limit too once LimitShardOpens was
// called.
func (m *ReadMeter) addShardOpens(door string, n int, shard *[2]types.Instant) error {
	if err := m.Exceeded(); err != nil {
		return err
	}
	var over error
	held := m.holdRoot() != nil
	for r := m; r != nil; r = r.parent {
		r.shardOpens.Add(uint64(n)) // #nosec G115 -- a count, never negative
		if r.allowsShard(shard) {
			continue
		}
		if held && r.paidShard(shard) {
			continue // held opens: a shard this meter was allowed before counts, opened again, in Work only
		}
		asked, bound := r.askedOpens.Add(uint64(n)), r.budget.ShardOpens // #nosec G115 -- a count, never negative
		if (bound != 0 || r.shardOpensLimited.Load()) && asked > bound {
			e := &ReadBudgetError{Door: door, Limit: "shard opens", Budget: r.budget, Work: r.Work(), ShardOpensLimited: r.shardOpensLimited.Load()}
			if ws := r.shardWindows.Load(); ws != nil {
				e.ShardWindows = len(*ws)
			}
			r.exceeded.CompareAndSwap(nil, e)
			if over == nil {
				over = r.exceeded.Load()
			}
		}
	}
	return over
}

// AddDecodedBytes counts n stored bytes door hands to a row decoder, as
// AddCandidates. A store calls it (through a ReadTally) before it decodes the
// bytes, so a read past the limit stops before it decodes the row that passes
// it.
func (m *ReadMeter) AddDecodedBytes(door string, n int) error {
	return m.add(door, "decoded bytes", n, func(r *ReadMeter) (*atomic.Uint64, uint64) { return &r.decodedBytes, r.budget.DecodedBytes })
}

// AddHistoryExamined counts n history versions door looks at, as
// AddCandidates.
func (m *ReadMeter) AddHistoryExamined(door string, n int) error {
	return m.add(door, "history examined", n, func(r *ReadMeter) (*atomic.Uint64, uint64) { return &r.historyExamined, r.budget.HistoryExamined })
}

// AddBufferBytes counts n bytes door is about to add to a temporary buffer,
// as AddCandidates.
func (m *ReadMeter) AddBufferBytes(door string, n int) error {
	return m.add(door, "buffer bytes", n, func(r *ReadMeter) (*atomic.Uint64, uint64) { return &r.bufferBytes, r.budget.BufferBytes })
}

// Exceeded returns the first budget error this meter or an outer one gave
// (nil while every count is within its limits): parallel workers of one read
// check it before their next unit, so a refusal stops them all. A nil meter
// returns nil.
func (m *ReadMeter) Exceeded() error {
	for r := m; r != nil; r = r.parent {
		if e := r.exceeded.Load(); e != nil {
			return e
		}
	}
	return nil
}

// ReadTally is a meter bound to the door that reads: what a store receives
// (store.ReadSnapshotMeterCapability, QueryOpts, the metered lend doors) to
// count the work it does: the bytes it decodes, the entries it examines, the
// members it collects, the buffers it grows. A nil tally counts nothing and
// refuses nothing: a read without a budget pays one nil check per decoded row.
// The layer that does a piece of work charges it, once (a store that takes a
// tally counts what it walks and collects; the graph counts only what a store
// without the metered form hands it). No charge is ever given back: every
// counter of a meter only grows, so the room a read sees only shrinks.
type ReadTally struct {
	meter *ReadMeter
	door  string
	hold  *holdScope // set by Held: this gather admits its entries before copying them
}

// holdScope marks a gather that charges candidates at admission. It keeps no
// private reservation: every read sharing the meter or an outer meter sees
// that charge before another copy starts. Drop and Settle never charge again.
type holdScope struct{}

// Tally binds m to door. A nil meter gives a nil tally.
func (m *ReadMeter) Tally(door string) *ReadTally {
	if m == nil {
		return nil
	}
	return &ReadTally{meter: m, door: door}
}

// Candidates counts n entries a door is about to examine before it filters or
// reads them (an adjacency entry, an index match), returning the budget error
// past a limit. A nil tally counts nothing.
func (t *ReadTally) Candidates(n int) error {
	if t == nil {
		return nil
	}
	return t.meter.AddCandidates(t.door, n)
}

// Decoded counts n bytes about to be decoded, returning the budget error past
// a limit. A nil tally counts nothing.
func (t *ReadTally) Decoded(n int) error {
	if t == nil {
		return nil
	}
	return t.meter.AddDecodedBytes(t.door, n)
}

// History counts n history versions about to be looked at, returning the
// budget error past a limit. A nil tally counts nothing.
func (t *ReadTally) History(n int) error {
	if t == nil {
		return nil
	}
	return t.meter.AddHistoryExamined(t.door, n)
}

// Buffer counts n bytes about to be added to a temporary buffer the store
// holds for the read (BufferBytes), returning the budget error past a limit,
// before the buffer grows. A nil tally counts nothing.
func (t *ReadTally) Buffer(n int) error {
	if t == nil {
		return nil
	}
	return t.meter.AddBufferBytes(t.door, n)
}

// ShardOpen counts one closed shard about to be opened (ShardOpens),
// returning the budget error past a limit, before the open. A nil tally
// counts nothing.
func (t *ReadTally) ShardOpen() error {
	if t == nil {
		return nil
	}
	return t.meter.AddShardOpens(t.door, 1)
}

// ShardOpenIn is ShardOpen for a shard whose own window is [start, end)
// (ReadMeter.AddShardOpensIn).
func (t *ReadTally) ShardOpenIn(start, end types.Instant) error {
	if t == nil {
		return nil
	}
	return t.meter.AddShardOpensIn(t.door, 1, start, end)
}

// CandidateRoom returns how many more candidates reads on t's meter may
// admit before a candidate limit of it or an outer meter refuses (0: the
// next one does), including entries other gathers already admitted. The room
// only shrinks; admission must still charge before work starts. A nil tally
// or a meter without a candidate limit returns math.MaxUint64.
func (t *ReadTally) CandidateRoom() uint64 {
	if t == nil {
		return math.MaxUint64
	}
	return t.meter.candidateRoom()
}

// candidateRoom is the least room of m and its outer meters. All accepted
// holds are already counted; atomic loads let a diagnostic read sample the
// room without taking the admission lock. It is never a reservation.
func (m *ReadMeter) candidateRoom() uint64 {
	room := uint64(math.MaxUint64)
	for r := m; r != nil; r = r.parent {
		if bound := r.budget.Candidates; bound != 0 {
			left := uint64(0)
			if n := r.candidates.Load(); n < bound {
				left = bound - n
			}
			room = min(room, left)
		}
	}
	return room
}

// Held marks a gather that pays for its entries through Hold before copying
// them. It returns t itself when nil or already marked. Unlimited controls
// use the same admission and counting path as bounded reads.
func (t *ReadTally) Held() *ReadTally {
	if t == nil || t.hold != nil {
		return t
	}
	return &ReadTally{meter: t.meter, door: t.door, hold: &holdScope{}}
}

// Hold charges n candidates before a gather copies them. The charge is
// immediately visible to every read sharing any enclosing budget. Refused
// admissions do no copy, and a spent context returns its original refusal.
// A tally without Held counts nothing here: its caller owns admission.
func (t *ReadTally) Hold(n int) error {
	if t == nil || t.hold == nil {
		return nil
	}
	return t.meter.AddCandidates(t.door, max(n, 0))
}

// CandidatesAhead refuses a read about to do work on n entries that it will
// count as it examines them later, when n exceeds CandidateRoom. This is a
// preflight, not a reservation: the native walk must still charge its work.
// Past the room it charges the refused demand and returns the typed error;
// within the room it counts nothing. Already admitted holds stay counted.
func (t *ReadTally) CandidatesAhead(n int) error {
	if t == nil {
		return nil
	}
	if err := t.meter.Exceeded(); err != nil {
		return err
	}
	if uint64(max(n, 0)) <= t.CandidateRoom() { // #nosec G115 -- a count
		return nil
	}
	return t.meter.AddCandidates(t.door, max(n, 0))
}

// Drop finishes rejected entries already charged by Hold. It returns a
// shared refusal if the context was spent and never refunds or charges again.
func (t *ReadTally) Drop(_ int) error {
	if t == nil || t.hold == nil {
		return nil
	}
	return t.Exceeded()
}

// Settle checks the gather before it returns its list. Every entry was paid
// for at admission, including entries a caller's result limit will skip.
func (t *ReadTally) Settle() error { return t.Drop(0) }

// Exceeded is the tally's meter's Exceeded. A nil tally returns nil.
func (t *ReadTally) Exceeded() error {
	if t == nil {
		return nil
	}
	return t.meter.Exceeded()
}

// RefuseUnsupported stops before work that cannot be charged to this read.
// Every enclosing meter keeps the refusal, so sibling reads sharing an outer
// allowance stop too. A nil tally is an unmetered read and refuses nothing.
func (t *ReadTally) RefuseUnsupported() error {
	if t == nil || t.meter == nil {
		return nil
	}
	if err := t.Exceeded(); err != nil {
		return err
	}
	var first *ReadBudgetError
	for m := t.meter; m != nil; m = m.parent {
		e := &ReadBudgetError{Door: t.door, Limit: "unsupported", Budget: m.budget, Work: m.Work()}
		m.exceeded.CompareAndSwap(nil, e)
		if first == nil {
			first = m.exceeded.Load()
		}
	}
	return first
}

// DecodedHistory counts one history version and its n stored bytes about to
// be decoded (History(1), then Decoded(n)).
func (t *ReadTally) DecodedHistory(n int) error {
	if t == nil {
		return nil
	}
	if err := t.History(1); err != nil {
		return err
	}
	return t.Decoded(n)
}

func (m *ReadMeter) add(door, limit string, n int, field func(*ReadMeter) (*atomic.Uint64, uint64)) error {
	if err := m.Exceeded(); err != nil {
		return err
	}
	var over error
	for r := m; r != nil; r = r.parent {
		c, bound := field(r)
		total := c.Add(uint64(n)) // #nosec G115 -- a count, never negative
		if bound != 0 && total > bound {
			e := &ReadBudgetError{Door: door, Limit: limit, Budget: r.budget, Work: r.Work()}
			r.exceeded.CompareAndSwap(nil, e)
			if over == nil {
				over = r.exceeded.Load()
			}
		}
	}
	return over
}
