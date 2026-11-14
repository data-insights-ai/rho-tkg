package core

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A tiered property membership read that one shard declines, followed by the
// graph's real fallback on the same meter (sigma's read-budget handoff). Three
// event shards each hold one Sig node with k = int64(1); the hot shard loses
// its property index either before the read (early: every open shard is asked
// before any walk, so the decline costs nothing) or inside the read, after the
// reference shard's read and before the event shards walk (late: a DDL drop
// landing mid-read). A late decline keeps what the two other shards walked,
// and the fallback is charged on top: the Candidates budget that answers is
// the fallback's plus the walk's, one less refuses with no row.
//
// Native counter: the disk index entries visited
// (badger.CountIndexEntriesVisitedForTest), which the meter does not keep; the
// visits the index drop itself makes inside the hook are subtracted. The
// tests use that process-wide counter: none runs in parallel.

const (
	dfCandidates = dfShards * (1 + 5) // Cf: one member and five owner/current/proof/liveness probes per node
	dfVisits     = 0                  // Vf: the fallback uses label membership, not property index entries
	dfLate       = 2                  // N: the member entries the two shards with the index walk before the late decline
	dfShards     = 3
)

// declineFallback is one fresh fixture: a tiered store with three event
// shards, the Sig nodes' IDs and the pin every read uses.
type declineFallback struct {
	ts    *tiered.Store
	g     *Core
	pin   types.Instant
	ids   []types.NodeID
	label uint16
}

func newDeclineFallback(t *testing.T) *declineFallback {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		DataDir:             t.TempDir(),
		RefLabels:           []string{"Ref"},
		ShardWindow:         7 * 24 * time.Hour,
		FlushInterval:       1<<63 - 1,
		PropertyIndexOnDisk: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Config{Store: ts, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		_ = ts.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = ts.Close() })
	ctx := context.Background()
	if err := g.Index.CreateProperty("Sig", "k"); err != nil {
		t.Fatal(err)
	}
	f := &declineFallback{ts: ts, g: g}
	for s := range dfShards {
		if s > 0 {
			time.Sleep(2 * time.Millisecond)
			if err := ts.RotateHotShard(); err != nil {
				t.Fatal(err)
			}
		}
		n, err := g.Nodes.Add(ctx, []string{"Sig"}, map[string]any{"k": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		f.ids = append(f.ids, n.ID())
	}
	slices.Sort(f.ids)
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	if f.pin, err = g.Temporal.NowTx(); err != nil {
		t.Fatal(err)
	}
	var ok bool
	if f.label, ok = g.labels.Lookup("Sig"); !ok {
		t.Fatal("fixture: no Sig token")
	}
	ts.MuForTest().RLock()
	shards := ts.EventShardsForTest()
	n, cold := len(shards), 0
	for _, es := range shards {
		if es.Tier() == tiered.TierCold {
			cold++
		}
	}
	ts.MuForTest().RUnlock()
	if n != dfShards || cold != 0 {
		t.Fatalf("fixture: %d event shards, %d cold; want %d, none cold (every shard asked before the walk)", n, cold, dfShards)
	}
	// Isolate the declined property walk from the label sidecar's first build.
	// The first-build admission has separate native/public regressions; both
	// the control and each exact/refused arm here start from the same cache state.
	warmGatherMembership(t, g)
	return f
}

// dropHot drops the hot shard's (Sig, k) index.
func (f *declineFallback) dropHot(t *testing.T) {
	t.Helper()
	if err := f.ts.HotShardForTest().Store().DropPropertyIndex(f.label, "k"); err != nil {
		t.Fatal(err)
	}
}

// armLate installs the fan-out hook that drops the hot shard's index on its
// first call only, and returns how often it dropped and the index entries the
// drop itself visited (subtracted from the read's count).
func (f *declineFallback) armLate(t *testing.T, visited func() int64) (acted *atomic.Int64, dropVisits *atomic.Int64) {
	t.Helper()
	acted, dropVisits = new(atomic.Int64), new(atomic.Int64)
	var calls atomic.Int64
	var dropErr atomic.Pointer[error]
	f.ts.SetFanoutRefReadHookForTest(func() {
		if calls.Add(1) != 1 {
			return
		}
		before := visited()
		if err := f.ts.HotShardForTest().Store().DropPropertyIndex(f.label, "k"); err != nil {
			dropErr.Store(&err) // reported by the cleanup: the hook may run off the test goroutine
			return
		}
		dropVisits.Add(visited() - before)
		acted.Add(1)
	})
	t.Cleanup(func() {
		f.ts.SetFanoutRefReadHookForTest(nil)
		if p := dropErr.Load(); p != nil {
			t.Errorf("the hook's index drop: %v", *p)
		}
	})
	return acted, dropVisits
}

// door is the public read: Sig with k = int64(1) at the pin.
func (f *declineFallback) door(ctx context.Context) ([]*types.Node, error) {
	return f.g.Nodes.ByLabelAndPropertyCtx(ctx, "Sig", "k", int64(1), storepkg.QueryOpts{TxPin: f.pin})
}

// helper is the tiered store's native metered membership read on its own
// meter with a wide budget: the members it handed on and the meter.
func (f *declineFallback) helper() (handed int, m *storepkg.ReadMeter, err error) {
	_, m = storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 1 << 20, BufferBytes: 1 << 20})
	vk := types.IndexablePropertyValueKey(int64(1))
	err = f.ts.ForEachNodePropertyTxMemberMetered(f.label, "k", []string{vk}, nil, m.Tally("helper"), func(types.NodeID, types.Instant) bool {
		handed++
		return true
	})
	return handed, m, err
}

// exact fails unless rows are the three Sig nodes, each with k = int64(1).
func (f *declineFallback) exact(t *testing.T, what string, rows []*types.Node, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v; want the %d rows", what, err, dfShards)
	}
	got := make([]types.NodeID, 0, len(rows))
	for _, n := range rows {
		got = append(got, n.ID())
		if v, ok := n.GetProperty("k"); !ok || v != int64(1) {
			t.Fatalf("%s: node %v k = %#v (%v); want int64(1)", what, n.ID(), v, ok)
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, f.ids) {
		t.Fatalf("%s: IDs %v; want %v", what, got, f.ids)
	}
}

// refusedAt fails unless err is the typed candidates refusal at limit with no row.
func refusedAt(t *testing.T, what string, rows []*types.Node, err error, limit uint64) {
	t.Helper()
	var rb *storepkg.ReadBudgetError
	if !errors.As(err, &rb) || rb.Limit != "candidates" || rb.Budget.Candidates != limit || rb.Work.Candidates <= limit || rows != nil {
		t.Fatalf("%s: %d rows (nil %v), %v; want the candidates refusal at budget %d with work above it and no rows", what, len(rows), rows == nil, err, limit)
	}
}

// TestTieredPropertyDeclineFallbackEarly: the hot shard lacks the index before
// the read. The native read declines before any walk (nothing counted, no
// entry visited); the door's fallback is the read's whole count: Cf candidates
// and Vf visits. Cf answers exactly, Cf-1 refuses.
func TestTieredPropertyDeclineFallbackEarly(t *testing.T) {
	f := newDeclineFallback(t)
	f.dropHot(t)
	visited, stop := badger.CountIndexEntriesVisitedForTest()
	defer stop()

	t.Run("helper", func(t *testing.T) {
		before := visited()
		handed, m, err := f.helper()
		if v := visited() - before; !errors.Is(err, storepkg.ErrIndexNotFound) || handed != 0 || v != 0 || m.Work() != (storepkg.ReadWork{}) {
			t.Fatalf("early helper: %d handed, %v, %d visited, work %+v; want ErrIndexNotFound before any walk: 0 handed, 0 visited, no work", handed, err, v, m.Work())
		}
	})

	t.Run("door control", func(t *testing.T) {
		before := visited()
		uctx, um := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
		rows, err := f.door(uctx)
		v := visited() - before
		t.Logf("early door control: %d rows, work %+v, %d visited", len(rows), um.Work(), v)
		f.exact(t, "early control", rows, err)
		if v != dfVisits || um.Work().Candidates != dfCandidates {
			t.Fatalf("early control: %d visited, work %+v; want %d visited, %d candidates (the fallback alone)", v, um.Work(), dfVisits, dfCandidates)
		}
	})

	t.Run("door Cf", func(t *testing.T) {
		ectx, em := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: dfCandidates})
		rows, err := f.door(ectx)
		f.exact(t, "early Candidates Cf", rows, err)
		if em.Work().Candidates != dfCandidates {
			t.Fatalf("early Candidates %d: work %+v; want %d candidates", dfCandidates, em.Work(), dfCandidates)
		}
	})

	t.Run("door Cf-1", func(t *testing.T) {
		lctx, _ := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: dfCandidates - 1})
		rows, err := f.door(lctx)
		refusedAt(t, "early Candidates Cf-1", rows, err, dfCandidates-1)
	})
}

// TestTieredPropertyDeclineFallbackLate: the hot shard loses the index inside
// the read, after the reference shard's read and before the event shards
// walk. The native read walks the two other shards (N entries, N candidates,
// their buffers) and then declines, keeping that work. The door's real
// fallback runs on top on the same meter: Vf + N visits, Cf + N candidates.
// Cf + N answers exactly; Cf + N - 1 refuses.
// A fresh fixture for every read: after the hook the store is the early case.
func TestTieredPropertyDeclineFallbackLate(t *testing.T) {
	visited, stop := badger.CountIndexEntriesVisitedForTest()
	defer stop()

	t.Run("helper", func(t *testing.T) {
		f := newDeclineFallback(t)
		acted, dropVisits := f.armLate(t, visited)
		before := visited()
		handed, m, err := f.helper()
		n := visited() - before - dropVisits.Load()
		t.Logf("late helper: %d handed, %v, hook acted %d, %d visited (%d by the drop), work %+v", handed, err, acted.Load(), n, dropVisits.Load(), m.Work())
		if acted.Load() != 1 || n != dfLate {
			t.Fatalf("late helper precondition: hook acted %d, %d entries visited before the decline; want 1 and %d (a real walk first)", acted.Load(), n, dfLate)
		}
		if !errors.Is(err, storepkg.ErrIndexNotFound) || handed != 0 || m.Work().Candidates != dfLate || m.Work().BufferBytes == 0 {
			t.Fatalf("late helper: %d handed, %v, work %+v; want ErrIndexNotFound, none handed, %d candidates and the buffers kept", handed, err, m.Work(), dfLate)
		}
	})

	t.Run("door control", func(t *testing.T) {
		f := newDeclineFallback(t)
		acted, dropVisits := f.armLate(t, visited)
		before := visited()
		uctx, um := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
		rows, err := f.door(uctx)
		v := visited() - before - dropVisits.Load()
		t.Logf("late door control: %d rows, work %+v, hook acted %d, %d visited (%d by the drop)", len(rows), um.Work(), acted.Load(), v, dropVisits.Load())
		if acted.Load() != 1 {
			t.Fatalf("late door control: hook acted %d; want 1", acted.Load())
		}
		f.exact(t, "late control", rows, err)
		if v != dfVisits+dfLate || um.Work().Candidates != dfCandidates+dfLate {
			t.Fatalf("late control: %d visited, work %+v; want %d visited and %d candidates (the walk kept, the fallback on top)", v, um.Work(), dfVisits+dfLate, dfCandidates+dfLate)
		}
	})

	t.Run("door Cf+N", func(t *testing.T) {
		f := newDeclineFallback(t)
		acted, _ := f.armLate(t, visited)
		ectx, em := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: dfCandidates + dfLate})
		rows, err := f.door(ectx)
		if acted.Load() != 1 {
			t.Fatalf("late Candidates %d: hook acted %d; want 1", dfCandidates+dfLate, acted.Load())
		}
		f.exact(t, "late Candidates Cf+N", rows, err)
		if em.Work().Candidates != dfCandidates+dfLate {
			t.Fatalf("late Candidates %d: work %+v; want %d candidates", dfCandidates+dfLate, em.Work(), dfCandidates+dfLate)
		}
	})

	t.Run("door Cf+N-1", func(t *testing.T) {
		f := newDeclineFallback(t)
		acted, _ := f.armLate(t, visited)
		lctx, _ := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: dfCandidates + dfLate - 1})
		rows, err := f.door(lctx)
		if acted.Load() != 1 {
			t.Fatalf("late Candidates %d: hook acted %d; want 1", dfCandidates+dfLate-1, acted.Load())
		}
		refusedAt(t, "late Candidates Cf+N-1", rows, err, dfCandidates+dfLate-1)
	})
}
