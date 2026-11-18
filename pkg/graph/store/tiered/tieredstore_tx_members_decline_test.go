package tiered

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The property membership fan-out of the tiered store under a read budget (R2
// meter, design-meter.txt sections 1 and 7: D1, D2, B1, and the main session's
// O8). There is no give-back: Candidates and BufferBytes never decrease, so the
// shards charge the meter directly, a read that declines keeps what its shards
// walked, and the caller's fallback is new work on top. A shard that cannot
// answer (no index, or an index still being created) is asked before any walk,
// so that decline costs nothing.

const (
	declineSignal = uint16(3) // the Signal label of installDefaultTestLabelRegistry (an event label)
	declineKey    = "sev"
	declinePer    = 4 // members on each of the three event shards
	declineShards = 3
	declineTotal  = declinePer * declineShards
	// declineMemberBytes is what a shard charges for a member of a RAM index:
	// the sidecar snapshot's slot (16) and the member in the shard's list (16).
	declineMemberBytes = 16 + 16
)

// declineProbe reads the members of sev=1 through unionPropertyTxMembers with
// each shard's enumeration wrapped: yielded counts the members the shards
// appended to their lists (the native count, which the meter does not keep),
// declineOn makes that shard's enumeration report store.ErrIndexNotFound
// without walking (an index found still building mid-walk), and afterYield runs
// after each append with the total so far (a second read inside the fan-out).
type declineProbe struct {
	ts         *Store
	yielded    atomic.Int64
	declineOn  *BadgerStore
	afterYield func(n int64)
}

func newDeclineProbe(t *testing.T, disk bool) *declineProbe {
	t.Helper()
	ts := newCumulativeTieredStore(t, disk)
	installDefaultTestLabelRegistry(t, ts)
	_ = spreadMembers(t, ts, "node", declinePer, declineShards)
	return &declineProbe{ts: ts}
}

func (p *declineProbe) read(tally *storecontract.ReadTally) (handed int, err error) {
	p.yielded.Store(0)
	vk := types.IndexablePropertyValueKey(int64(1))
	inner := eachKeyTallied([]string{vk}, func(bs *BadgerStore, st *storecontract.ReadTally, vk string, f func(types.NodeID, types.Instant) bool) error {
		return bs.ForEachNodePropertyTxMemberMetered(declineSignal, declineKey, []string{vk}, nil, st, f)
	})
	each := func(bs *BadgerStore, st *storecontract.ReadTally, f func(types.NodeID, types.Instant) bool) error {
		if bs == p.declineOn {
			return storecontract.ErrIndexNotFound
		}
		return inner(bs, st, func(id types.NodeID, tx types.Instant) bool {
			if !f(id, tx) {
				return false
			}
			n := p.yielded.Add(1)
			if p.afterYield != nil {
				p.afterYield(n)
			}
			return true
		})
	}
	options := func(bs *BadgerStore) (storecontract.PropertyIndexOptions, bool, error) {
		return bs.PropertyIndexOptions(declineSignal, declineKey)
	}
	var mu sync.Mutex
	err = unionPropertyTxMembers(p.ts, tally, options, each, func(types.NodeID, types.Instant) bool {
		mu.Lock()
		handed++
		mu.Unlock()
		return true
	})
	return handed, err
}

func declineBudget(b storecontract.ReadBudget) (*storecontract.ReadMeter, *storecontract.ReadTally) {
	_, m := storecontract.WithReadBudget(context.Background(), b)
	return m, m.Tally("a")
}

// TestTieredMemberDeclineAsksEveryShardFirst (O8, the main session's decision):
// a membership read on a tiered store answers only when every shard carries
// the anchor's index. The shards were asked only as each was walked, so a read
// that declined because one shard lacked the index had walked the others; with
// no give-back that walk is charged. Every shard that can be asked without an
// open is now asked first, and the read declines before any walk: 0
// Candidates, 0 BufferBytes and 0 index entries visited (the native count of
// every disk index walk, which the meter does not keep). The fallback the
// caller then takes is charged as its own work. The control, with every shard
// carrying the index, answers the 12 members and visits their entries.
func TestTieredMemberDeclineAsksEveryShardFirst(t *testing.T) {
	ts := newCumulativeTieredStore(t, true)
	installDefaultTestLabelRegistry(t, ts)
	read := spreadMembers(t, ts, "node", declinePer, declineShards)
	visited, stop := badger.CountIndexEntriesVisitedForTest()
	defer stop()
	vk := types.IndexablePropertyValueKey(int64(1))

	m, tally := declineBudget(storecontract.ReadBudget{Candidates: 1 << 20, BufferBytes: 1 << 20})
	members := 0
	before := visited()
	if err := read(vk, nil, tally, func() { members++ }); err != nil || members != declineTotal || m.Work().Candidates != declineTotal || visited()-before < declineTotal {
		t.Fatalf("control: %d members, %v, work %+v, %d entries visited; want the %d members counted and walked", members, err, m.Work(), visited()-before, declineTotal)
	}

	lost := ""
	for name, bs := range allShardStoresForTest(t, ts) {
		if name != "reference" && name != "archive" && lost == "" {
			if err := bs.DropPropertyIndex(declineSignal, declineKey); err != nil {
				t.Fatal(err)
			}
			lost = name
		}
	}
	if lost == "" {
		t.Fatal("fixture: no event shard")
	}
	m, tally = declineBudget(storecontract.ReadBudget{Candidates: 1 << 20, BufferBytes: 1 << 20})
	members = 0
	before = visited()
	err := read(vk, nil, tally, func() { members++ })
	if !errors.Is(err, storecontract.ErrIndexNotFound) || members != 0 || m.Work() != (storecontract.ReadWork{}) || visited() != before {
		t.Fatalf("decline (shard %s without the index): %d members, %v, work %+v, %d entries visited; want ErrIndexNotFound before any walk: nothing counted, 0 visited", lost, members, err, m.Work(), visited()-before)
	}
	const fallback = 5
	if err := tally.Candidates(fallback); err != nil || m.Work().Candidates != fallback {
		t.Fatalf("the fallback's %d candidates: %v, work %+v; want them counted alone", fallback, err, m.Work())
	}
}

// TestTieredMemberDeclineAsksAnOpenColdShard: the question before any walk
// goes to every event shard whose store is open now, a cold one included, and
// opens no closed shard for it. On disk (a closed cold shard reopens with its
// data): three event shards of 4 members, the two older demoted to cold (their
// handles stay open), the oldest's handle then closed. The control, with every
// shard carrying the index, reopens the closed shard once (one ShardOpens, one
// native ColdOpens), admits the native open probe and answers the 12 members.
// Then the oldest is closed again and the open cold shard loses the index:
// the read declines before any walk,
// with nothing counted, no index entry visited, no shard opened (ColdOpens and
// the closed handle unchanged) and no member handed on. Skipping cold shards in
// the question would leave the open cold shard to the walk: the closed one
// opened, it and the hot shard walked, 8 members counted, then the decline.
func TestTieredMemberDeclineAsksAnOpenColdShard(t *testing.T) {
	testtier.Slow(t)
	ts, err := New(Config{
		DataDir:             t.TempDir(),
		RefLabels:           []string{"Case", "User"},
		ShardWindow:         7 * 24 * time.Hour,
		FlushInterval:       1<<63 - 1,
		PropertyIndexOnDisk: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	ts.SetPropertyKeyRegistry(registrypkg.NewPropertyKeyRegistry())
	installDefaultTestLabelRegistry(t, ts)
	read := spreadMembers(t, ts, "node", declinePer, declineShards)
	hot := ts.HotShardForTest().Name()
	var older []string
	for name := range ts.EventShardsForTest() {
		if name != hot {
			older = append(older, name)
		}
	}
	if len(older) != declineShards-1 {
		t.Fatalf("fixture: %d older event shards, want %d", len(older), declineShards-1)
	}
	closed, open := older[0], older[1]
	demoteToCold(ts, closed)
	demoteToCold(ts, open)
	closeEventShardStore(t, ts, closed)
	visited, stop := badger.CountIndexEntriesVisitedForTest()
	defer stop()
	vk := types.IndexablePropertyValueKey(int64(1))
	coldOpens := func() uint64 { return ts.ShardAccessStats().ColdOpens }
	closedOpen := func() bool {
		es := ts.EventShardsForTest()[closed]
		es.LockShardMuForTest()
		defer es.UnlockShardMuForTest()
		return es.Store() != nil
	}

	m, tally := declineBudget(storecontract.ReadBudget{Candidates: 1 << 20, BufferBytes: 1 << 20})
	const wantCandidates = declineTotal + 1 // member visits plus the closed shard's native open probe
	members := 0
	before, opened := visited(), coldOpens()
	if err := read(vk, nil, tally, func() { members++ }); err != nil || members != declineTotal || m.Work().Candidates != wantCandidates ||
		m.Work().ShardOpens != 1 || coldOpens()-opened != 1 || visited()-before != declineTotal {
		t.Fatalf("control: %d members, %v, work %+v, %d cold opens, %d entries visited; want %d members walked, %d candidates and the closed shard opened once",
			members, err, m.Work(), coldOpens()-opened, visited()-before, declineTotal, wantCandidates)
	}
	for _, limit := range []uint64{wantCandidates, wantCandidates - 1} {
		closeEventShardStore(t, ts, closed)
		m, tally = declineBudget(storecontract.ReadBudget{Candidates: limit})
		members = 0
		before, opened = visited(), coldOpens()
		err := read(vk, nil, tally, func() { members++ })
		if limit == wantCandidates {
			if err != nil || members != declineTotal || visited()-before != declineTotal {
				t.Fatalf("exact Candidates %d: %d members, %v, %d native visits; want all %d members", limit, members, err, visited()-before, declineTotal)
			}
		} else {
			var rb *storecontract.ReadBudgetError
			// The native visit observer includes the rejected iterator entry;
			// admission refuses before its value is read or a fold slot allocated.
			if !errors.As(err, &rb) || rb.Limit != "candidates" || members != 0 || visited()-before != declineTotal || rb.Work != m.Work() {
				t.Fatalf("Candidates %d: %d members, %v, %d native visits, work %+v; want refusal at the last examined member and no result", limit, members, err, visited()-before, m.Work())
			}
		}
		if m.Work().Candidates != wantCandidates || m.Work().ShardOpens != 1 || coldOpens()-opened != 1 {
			t.Fatalf("Candidates %d: work %+v, %d native opens; want %d candidates and one admitted open", limit, m.Work(), coldOpens()-opened, wantCandidates)
		}
	}

	closeEventShardStore(t, ts, closed)
	es := ts.EventShardsForTest()[open]
	es.LockShardMuForTest()
	openStore := es.Store()
	es.UnlockShardMuForTest()
	if openStore == nil {
		t.Fatal("fixture: the open cold shard has no store")
	}
	if err := openStore.DropPropertyIndex(declineSignal, declineKey); err != nil {
		t.Fatal(err)
	}
	m, tally = declineBudget(storecontract.ReadBudget{Candidates: 1 << 20, BufferBytes: 1 << 20})
	members = 0
	before, opened = visited(), coldOpens()
	err = read(vk, nil, tally, func() { members++ })
	if !errors.Is(err, storecontract.ErrIndexNotFound) || members != 0 || m.Work() != (storecontract.ReadWork{}) || visited() != before ||
		coldOpens() != opened || closedOpen() {
		t.Fatalf("decline (open cold shard without the index): %d members, %v, work %+v, %d entries visited, %d cold opens, closed shard open %v; want ErrIndexNotFound before any walk: nothing counted, visited or opened",
			members, err, m.Work(), visited()-before, coldOpens()-opened, closedOpen())
	}
}

// TestTieredMemberDeclineKeepsItsWork (D1, replaces
// TestTieredMemberDeclineCostsNothing): a read that declines after its shards
// walked (a shard found still building mid-walk; here the reference shard's
// enumeration reports it) keeps what the shards examined and the lists they
// kept: the 12 members of the three event shards are counted and their 32
// bytes each charged, on the meter as they are made. Before, a staged tally
// held them back and the decline gave them back. The fallback the caller takes
// on the same meter counts on top: a Candidates limit of 12 + 5 answers it, one
// less refuses it. Native count: the members the shards appended.
func TestTieredMemberDeclineKeepsItsWork(t *testing.T) {
	p := newDeclineProbe(t, false)
	p.declineOn = p.ts.refShard
	const fallback = 5

	m, tally := declineBudget(storecontract.ReadBudget{Candidates: 1 << 20, BufferBytes: 1 << 20})
	handed, err := p.read(tally)
	want := storecontract.ReadWork{Candidates: declineTotal, BufferBytes: declineTotal * declineMemberBytes}
	if !errors.Is(err, storecontract.ErrIndexNotFound) || handed != 0 || p.yielded.Load() != declineTotal || m.Work() != want {
		t.Fatalf("control decline: %d handed, %v, %d yielded, work %+v; want ErrIndexNotFound, none handed, %d yielded and work %+v kept", handed, err, p.yielded.Load(), m.Work(), declineTotal, want)
	}
	if err := tally.Candidates(fallback); err != nil || m.Work().Candidates != declineTotal+fallback {
		t.Fatalf("the fallback on the same meter: %v, work %+v; want %d candidates", err, m.Work(), declineTotal+fallback)
	}

	for _, c := range []struct {
		limit  uint64
		answer bool
	}{{declineTotal + fallback, true}, {declineTotal + fallback - 1, false}} {
		m, tally := declineBudget(storecontract.ReadBudget{Candidates: c.limit})
		if _, err := p.read(tally); !errors.Is(err, storecontract.ErrIndexNotFound) {
			t.Fatalf("Candidates %d: the read %v; want the decline", c.limit, err)
		}
		err := tally.Candidates(fallback)
		if c.answer != (err == nil) {
			t.Fatalf("Candidates %d: the fallback %v, work %+v; want answered %v", c.limit, err, m.Work(), c.answer)
		}
	}
}

// TestTieredMemberFanoutBesideAnotherRead (D2): the meter is one allowance for
// every read on a context, live. Read A declines after its shards walked (the
// reference shard reports still building); read B (3 candidates, 48 bytes,
// charged directly) runs inside A's fan-out after A's shards appended 4
// members. The control, with no limit, keeps both: 15 candidates. Under a
// Candidates limit of 6, B sees A's charges and is refused at 7 or more, A's
// shards stop at their next charge (fewer than the 12 appended), and the meter
// keeps the work after A returns (nothing is given back). Before, A's charges
// were staged: B saw none and answered.
func TestTieredMemberFanoutBesideAnotherRead(t *testing.T) {
	p := newDeclineProbe(t, false)
	p.declineOn = p.ts.refShard
	run := func(b storecontract.ReadBudget) (m *storecontract.ReadMeter, errA, errB error) {
		m, tally := declineBudget(b)
		var once sync.Once
		p.afterYield = func(n int64) {
			if n >= 4 {
				once.Do(func() {
					tb := m.Tally("b")
					if errB = tb.Candidates(3); errB == nil {
						errB = tb.Buffer(48)
					}
				})
			}
		}
		defer func() { p.afterYield = nil }()
		_, errA = p.read(tally)
		return m, errA, errB
	}

	m, errA, errB := run(storecontract.ReadBudget{Segments: 1 << 20})
	want := storecontract.ReadWork{Candidates: declineTotal + 3, BufferBytes: declineTotal*declineMemberBytes + 48}
	if !errors.Is(errA, storecontract.ErrIndexNotFound) || errB != nil || m.Work() != want || p.yielded.Load() != declineTotal {
		t.Fatalf("unlimited control: A %v, B %v, %d yielded, work %+v; want A's decline, B answered, work %+v", errA, errB, p.yielded.Load(), m.Work(), want)
	}

	m, errA, errB = run(storecontract.ReadBudget{Candidates: 6})
	var rb *storecontract.ReadBudgetError
	if !errors.As(errB, &rb) || rb.Door != "b" || rb.Limit != "candidates" || rb.Work.Candidates < 7 || rb.Work.Candidates > m.Work().Candidates {
		t.Fatalf("Candidates 6: B %v, meter %+v; want B refused at 7 or more, its Work at most the meter's", errB, m.Work())
	}
	if !errors.Is(errA, storecontract.ErrReadBudget) || p.yielded.Load() >= declineTotal || m.Work().Candidates < 7 {
		t.Fatalf("Candidates 6: A %v, %d yielded, meter %+v after A returned; want A refused, fewer than %d yielded, the meter at 7 or more (nothing given back)", errA, p.yielded.Load(), m.Work(), declineTotal)
	}
}

// TestTieredMemberBufferAcrossShards (B1): BufferBytes is one allowance over the
// shards of a fan-out and over every read on the context, live. Three event
// shards of 4 members: the control appends 12; under 128 bytes the fan-out is
// refused at "buffer bytes" with every append after its own nil charge
// (appended x 16 <= 128). Then (a) a second read's direct Buffer(96) inside the
// fan-out, after 4 appends (A has charged 128 bytes or more by then), under
// 128 + 95: refused; (b) a read that declines after its shards walked keeps
// their 384 bytes, so a fallback's Buffer(64) under 384 + 63 is refused. Before,
// the fan-out's charges were staged and both answered.
func TestTieredMemberBufferAcrossShards(t *testing.T) {
	p := newDeclineProbe(t, false)

	m, tally := declineBudget(storecontract.ReadBudget{BufferBytes: 1 << 20})
	if handed, err := p.read(tally); err != nil || handed != declineTotal || p.yielded.Load() != declineTotal || m.Work().BufferBytes == 0 {
		t.Fatalf("control: %d handed, %v, %d appended, work %+v; want the %d members", handed, err, p.yielded.Load(), m.Work(), declineTotal)
	}
	m, tally = declineBudget(storecontract.ReadBudget{BufferBytes: 128})
	_, err := p.read(tally)
	var rb *storecontract.ReadBudgetError
	if !errors.As(err, &rb) || rb.Limit != "buffer bytes" || p.yielded.Load()*16 > 128 {
		t.Fatalf("BufferBytes 128: %v, %d appended, work %+v; want the buffer refusal with at most 8 appended", err, p.yielded.Load(), m.Work())
	}

	t.Run("second read inside the fan-out", func(t *testing.T) {
		m, tally := declineBudget(storecontract.ReadBudget{BufferBytes: 128 + 95})
		var errB error
		var once sync.Once
		p.afterYield = func(n int64) {
			if n >= 4 {
				once.Do(func() { errB = m.Tally("b").Buffer(96) })
			}
		}
		defer func() { p.afterYield = nil }()
		_, _ = p.read(tally)
		if !errors.As(errB, &rb) || rb.Limit != "buffer bytes" || rb.Door != "b" {
			t.Fatalf("B's Buffer(96) after A's 4 appends under %d: %v, meter %+v; want refused", 128+95, errB, m.Work())
		}
	})

	t.Run("fallback after a decline", func(t *testing.T) {
		p.declineOn = p.ts.refShard
		defer func() { p.declineOn = nil }()
		const kept = declineTotal * declineMemberBytes
		m, tally := declineBudget(storecontract.ReadBudget{BufferBytes: kept + 63})
		if _, err := p.read(tally); !errors.Is(err, storecontract.ErrIndexNotFound) || m.Work().BufferBytes != kept {
			t.Fatalf("decline: %v, work %+v; want ErrIndexNotFound with %d bytes kept", err, m.Work(), kept)
		}
		if err := tally.Buffer(64); !errors.As(err, &rb) || rb.Limit != "buffer bytes" {
			t.Fatalf("the fallback's Buffer(64) after the decline under %d: %v, work %+v; want refused", kept+63, err, m.Work())
		}
	})
}

// TestTieredMemberFanoutChargesItsBuffers (R2 cumulative, Fable review 6): the
// buffers a membership fan-out holds are charged as BufferBytes before they
// grow: each shard's own list (the sidecar's snapshot slot of 16 bytes, or on
// disk the fold's 24-byte entry and the list's 16), the member each shard keeps
// in the slice it returns (16) and the entity in the union (32). 9 members over
// 3 shards: a RAM index costs 16 + 16 + 32 = 64 bytes a member, a disk index
// 24 + 16 + 16 + 32 = 88. The limit at that count answers the 9 members; one
// byte under it refuses with the typed error and hands none on.
func TestTieredMemberFanoutChargesItsBuffers(t *testing.T) {
	const perShard, shards = 3, 3
	const total = perShard * shards
	value := types.IndexablePropertyValueKey(int64(1))
	for _, c := range []struct {
		name      string
		disk      bool
		perMember uint64
	}{{"ram", false, 16 + 16 + 32}, {"disk", true, 24 + 16 + 16 + 32}} {
		t.Run(c.name, func(t *testing.T) {
			ts := newCumulativeTieredStore(t, c.disk)
			installDefaultTestLabelRegistry(t, ts)
			read := spreadMembers(t, ts, "node", perShard, shards)
			budget := func(limit uint64) (*storecontract.ReadMeter, *storecontract.ReadTally) {
				_, m := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{BufferBytes: limit})
				return m, m.Tally("d")
			}
			want := total * c.perMember
			members := 0
			m, tally := budget(1 << 20)
			if err := read(value, nil, tally, func() { members++ }); err != nil || members != total || m.Work().BufferBytes != want {
				t.Fatalf("control: %d members, %v, %d buffer bytes; want the %d members and exactly %d bytes", members, err, m.Work().BufferBytes, total, want)
			}
			members = 0
			_, tally = budget(want)
			if err := read(value, nil, tally, func() { members++ }); err != nil || members != total {
				t.Fatalf("BufferBytes %d: %d members, %v; want the %d members", want, members, err, total)
			}
			members = 0
			m, tally = budget(want - 1)
			err := read(value, nil, tally, func() { members++ })
			var rb *storecontract.ReadBudgetError
			if !errors.As(err, &rb) || rb.Limit != "buffer bytes" || members != 0 {
				t.Fatalf("BufferBytes %d: %d members, %v, work %+v; want the buffer bytes refusal and no member handed on", want-1, members, err, m.Work())
			}
		})
	}
}
