package graph_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type publicCASTarget struct {
	node types.NodeID
	rel  types.RelID
}

func (x publicCASTarget) compare(ctx context.Context, g *graph.Graph, expected, replacement any) (bool, error) {
	if x.node != 0 {
		return g.Nodes().CompareAndSetProperty(ctx, x.node, "state", expected, replacement)
	}
	return g.Rels().CompareAndSetProperty(ctx, x.rel, "state", expected, replacement)
}

type publicCASRow struct {
	version    uint32
	temporal   types.TemporalMetadata
	properties map[string]any
	hash       string
}

type publicCASState struct {
	head    publicCASRow
	history []publicCASRow
}

func (x publicCASTarget) state(t *testing.T, g *graph.Graph) publicCASState {
	t.Helper()
	var state publicCASState
	if x.node != 0 {
		n, err := g.Nodes().GetCtx(t.Context(), x.node)
		if err != nil {
			t.Fatal(err)
		}
		row := func(n *types.Node) publicCASRow {
			return publicCASRow{n.Version(), *n.Temporal(), n.PropertiesMap(), n.Integrity().Hash}
		}
		state.head = row(n)
		history, err := g.Nodes().History(x.node)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range history {
			state.history = append(state.history, row(n))
		}
	} else {
		r, err := g.Rels().GetCtx(t.Context(), x.rel)
		if err != nil {
			t.Fatal(err)
		}
		row := func(r *types.Relationship) publicCASRow {
			return publicCASRow{r.Version(), *r.Temporal(), r.PropertiesMap(), r.Integrity().Hash}
		}
		state.head = row(r)
		history, err := g.Rels().History(x.rel)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range history {
			state.history = append(state.history, row(r))
		}
	}
	return state
}

type publicCASFixture struct {
	g       *graph.Graph
	st      *tiered.Store
	cfg     tiered.Config
	targets []publicCASTarget
	owners  []*badger.Store
}

func newPublicCASFixture(t *testing.T, kind string) *publicCASFixture {
	t.Helper()
	return newPublicCASFixtureCuts(t, kind, 3)
}

func newPublicCASFixtureCuts(t *testing.T, kind string, cuts byte) *publicCASFixture {
	t.Helper()
	f := &publicCASFixture{cfg: tiered.Config{
		DataDir: t.TempDir(), RefLabels: []string{"Case"}, RefTypes: []string{"REF"},
		ShardWindow: time.Hour, FlushInterval: time.Hour,
		CacheCapacity: 1, CacheBudgetBytes: 1, MemTableSize: 8 << 20,
	}}
	f.open(t)
	t.Cleanup(func() { _ = f.g.Close() })
	add := func(label string) *types.Node {
		n, err := f.g.Nodes().Add(t.Context(), []string{label}, map[string]any{"state": bytes.Repeat([]byte{0}, 512), "guard": "unchanged"})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	a, z, event := add("Case"), add("Case"), add("Signal")
	f.targets = []publicCASTarget{{node: a.ID()}, {node: event.ID()}}
	if kind == "rel" {
		f.targets = nil
		for i, start := range []*types.Node{a, event} {
			r, err := f.g.Rels().Add(t.Context(), []string{"REF", "EVENT"}[i], start, z,
				map[string]any{"state": bytes.Repeat([]byte{0}, 512), "guard": "unchanged"})
			if err != nil {
				t.Fatal(err)
			}
			f.targets = append(f.targets, publicCASTarget{rel: r.ID()})
		}
	}
	for revision := byte(1); revision <= cuts; revision++ {
		for _, target := range f.targets {
			matched, err := target.compare(t.Context(), f.g, bytes.Repeat([]byte{revision - 1}, 512), bytes.Repeat([]byte{revision}, 512))
			if err != nil || !matched {
				t.Fatalf("publication %d: matched %v/error %v", revision, matched, err)
			}
		}
	}
	if err := f.st.DurableFlush(); err != nil {
		t.Fatal(err)
	}
	f.owners = []*badger.Store{f.st.RefShardForTest(), f.st.HotShardForTest().Store()}
	if f.owners[0] == f.owners[1] {
		t.Fatal("fixture needs separate reference and event shard owners")
	}
	for i, target := range f.targets {
		state := target.state(t, f.g)
		if state.head.version != uint32(cuts) || len(state.history) != int(cuts) || state.head.temporal.ValidFrom != types.UnboundedValidFrom {
			t.Fatalf("target %d is not a whole-axis head with %d historical revisions: version %d/history %d/valid-from %d", i, cuts, state.head.version, len(state.history), state.head.temporal.ValidFrom)
		}
	}
	// PeekTx includes wall time. A public HLC floor ahead of it makes the
	// unchanged-clock assertions observe reservations rather than elapsed time.
	if _, err := f.g.Temporal().AdvanceClock(types.Instant(time.Now().Add(time.Hour).UnixMilli())); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *publicCASFixture) open(t *testing.T) {
	t.Helper()
	var err error
	f.st, err = tiered.New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.g, err = graph.New(graph.Config{Store: f.st, UntimedValidStart: graph.UntimedWholeAxis})
	if err != nil {
		_ = f.st.Close()
		t.Fatal(err)
	}
}

func publicCASClock(t *testing.T, g *graph.Graph) types.Instant {
	t.Helper()
	at, err := g.Temporal().PeekTx()
	if err != nil {
		t.Fatal(err)
	}
	return at
}

// Whole-axis comparison must admit both the current row and its history proof.
// A one-byte cache budget cannot retain a clean row, so neither repeated reads
// nor prior assertions can hide a native decode. Both shard owners are exercised.
func TestReadBudgetPublicCASComparison(t *testing.T) {
	for _, kind := range []string{"node", "rel"} {
		t.Run(kind, func(t *testing.T) {
			f := newPublicCASFixture(t, kind)
			for i, target := range f.targets {
				before := target.state(t, f.g)
				clock := publicCASClock(t, f.g)
				controlCtx, controlMeter := store.WithReadBudget(t.Context(), store.ReadBudget{})
				var matched bool
				var err error
				decoded := badger.CountRowDecodesForTest(func() {
					matched, err = target.compare(controlCtx, f.g, bytes.Repeat([]byte{9}, 512), []byte("replacement"))
				})
				if err != nil || matched || decoded == 0 {
					t.Fatalf("owner %d mismatch control: matched %v/decodes %d/work %+v/error %v", i, matched, decoded, controlMeter.Work(), err)
				}
				for _, expected := range [][]byte{bytes.Repeat([]byte{9}, 512), bytes.Repeat([]byte{3}, 512)} {
					budget := store.ReadBudget{DecodedBytes: 1}
					ctx, meter := store.WithReadBudget(t.Context(), budget)
					decoded = badger.CountRowDecodesForTest(func() {
						matched, err = target.compare(ctx, f.g, expected, []byte("replacement"))
					})
					var refusal *store.ReadBudgetError
					if matched || !errors.Is(err, store.ErrReadBudget) || !errors.As(err, &refusal) || refusal.Limit != "decoded bytes" || refusal.Budget != budget || refusal.Work != meter.Work() || decoded != 0 {
						t.Fatalf("owner %d comparison bypassed admission: matched %v/decodes %d/work %+v/error %v", i, matched, decoded, meter.Work(), err)
					}
					if after := target.state(t, f.g); !reflect.DeepEqual(before, after) || publicCASClock(t, f.g) != clock {
						t.Fatalf("owner %d refusal changed head, properties, history, integrity or commit clock", i)
					}
				}
			}
		})
	}
}

type publicCASObservation struct {
	work    store.ReadWork
	decoded int64
	meta    [2]int64
	calls   int
	err     error
}

func (f *publicCASFixture) mismatch(t *testing.T, budget store.ReadBudget, targets []publicCASTarget) publicCASObservation {
	t.Helper()
	return f.observeComparison(t, budget, targets, bytes.Repeat([]byte{9}, 512), []byte("replacement"), false)
}

func (f *publicCASFixture) observeComparison(t *testing.T, budget store.ReadBudget, targets []publicCASTarget, expected, replacement any, wantMatched bool) publicCASObservation {
	t.Helper()
	ctx, meter := store.WithReadBudget(t.Context(), budget)
	var got publicCASObservation
	for i, owner := range f.owners {
		got.meta[i] = -owner.HistoryMetaDecodesForTest()
	}
	got.decoded = badger.CountRowDecodesForTest(func() {
		for _, target := range targets {
			var matched bool
			matched, got.err = target.compare(ctx, f.g, expected, replacement)
			if matched != (wantMatched && got.err == nil) {
				t.Fatalf("comparison matched %v, want %v with a nil error; error %v", matched, wantMatched, got.err)
			}
			if got.err != nil {
				return
			}
			got.calls++
		}
	})
	for i, owner := range f.owners {
		got.meta[i] += owner.HistoryMetaDecodesForTest()
	}
	got.work = meter.Work()
	return got
}

func TestReadBudgetPublicCASFallbackCumulative(t *testing.T) {
	for _, kind := range []string{"node", "rel"} {
		t.Run(kind, func(t *testing.T) {
			f := newPublicCASFixture(t, kind)
			f.invalidateHistoryProof(t)
			before := []publicCASState{f.targets[0].state(t, f.g), f.targets[1].state(t, f.g)}
			clock := publicCASClock(t, f.g)
			control := f.proofMismatch(t, store.ReadBudget{}, f.targets)
			w := control.work
			// The invalid proof first pays owner liveness and revalidation;
			// the admitted fallback then pays its own metadata liveness. Nodes
			// decode four rows per owner; relationship routing adds two more.
			wantDecoded := int64(8)
			if kind == "rel" {
				wantDecoded = 12
			}
			// Each owner retains and decodes the one-byte invalid proof, then
			// visits exactly its three ordinary historical metadata rows.
			if control.err != nil || control.calls != 2 || control.decoded != wantDecoded || control.meta != [2]int64{3, 3} || control.proofProbes != 2 || control.proofDecodes != 2 || control.proofCopies != 2 || control.historyCopies != 0 || control.visits != 6 || control.pending != 0 || w.Candidates != 16 || w.HistoryExamined != 6 || w.DecodedBytes <= 1 || w.BufferBytes != 440 {
				t.Fatalf("nonempty two-shard comparison control: %+v", control)
			}
			exact := store.ReadBudget{Candidates: w.Candidates, HistoryExamined: w.HistoryExamined, DecodedBytes: w.DecodedBytes, BufferBytes: w.BufferBytes}
			got := f.proofMismatch(t, exact, f.targets)
			if got.err != nil || got != control {
				t.Fatalf("exact allowance changed comparison work: got %+v; control %+v", got, control)
			}
			for _, dim := range []struct {
				name    string
				budget  store.ReadBudget
				decoded int64
				meta    [2]int64
				visits  int64
			}{
				{"candidates", store.ReadBudget{Candidates: w.Candidates - 1}, wantDecoded - 1, [2]int64{3, 0}, 3},
				{"history examined", store.ReadBudget{HistoryExamined: w.HistoryExamined - 1}, wantDecoded, [2]int64{3, 2}, 5},
				{"decoded bytes", store.ReadBudget{DecodedBytes: w.DecodedBytes - 1}, wantDecoded, [2]int64{3, 2}, 6},
				{"buffer bytes", store.ReadBudget{BufferBytes: w.BufferBytes - 1}, wantDecoded, [2]int64{3, 3}, 6},
			} {
				got = f.proofMismatch(t, dim.budget, f.targets)
				var refusal *store.ReadBudgetError
				if !errors.As(got.err, &refusal) || refusal.Limit != dim.name || refusal.Budget != dim.budget || refusal.Work != got.work || got.calls != 1 || got.decoded != dim.decoded || got.meta != dim.meta || got.proofProbes != 2 || got.proofDecodes != 2 || got.proofCopies != 2 || got.historyCopies != 0 || got.visits != dim.visits || got.pending != 0 {
					t.Fatalf("one under %s: %+v; error %v", dim.name, got, got.err)
				}
			}
			// Each owner fits this allowance by itself; their probes must share
			// it when the caller uses one context for both logical keys.
			first := f.proofMismatch(t, store.ReadBudget{}, f.targets[:1])
			second := f.proofMismatch(t, store.ReadBudget{}, f.targets[1:])
			shared := store.ReadBudget{DecodedBytes: max(first.work.DecodedBytes, second.work.DecodedBytes)}
			for i, target := range f.targets {
				got = f.proofMismatch(t, shared, []publicCASTarget{target})
				if got.err != nil || got.calls != 1 || got.decoded == 0 {
					t.Fatalf("single owner does not fit shared allowance: %+v", got)
				}
				// A matching no-op still pays the same comparison cost and leaves
				// the version and clock unchanged under its exact allowance.
				single := []publicCASProofObservation{first, second}[i]
				sw := single.work
				ctx, meter := store.WithReadBudget(t.Context(), store.ReadBudget{Candidates: sw.Candidates, HistoryExamined: sw.HistoryExamined, DecodedBytes: sw.DecodedBytes, BufferBytes: sw.BufferBytes})
				var matched bool
				var err error
				decoded := badger.CountRowDecodesForTest(func() {
					matched, err = target.compare(ctx, f.g, bytes.Repeat([]byte{3}, 512), bytes.Repeat([]byte{3}, 512))
				})
				if err != nil || !matched || decoded != single.decoded || meter.Work() != sw {
					t.Fatalf("owner %d exact matching no-op: matched %v/decoded %d/work %+v/error %v; want the mismatch control %+v", i, matched, decoded, meter.Work(), err, single)
				}
			}
			got = f.proofMismatch(t, shared, f.targets)
			var refusal *store.ReadBudgetError
			if !errors.As(got.err, &refusal) || refusal.Limit != "decoded bytes" || got.calls != 1 || got.decoded >= control.decoded || got.work.DecodedBytes <= shared.DecodedBytes {
				t.Fatalf("caller allowance was reset across owners: %+v; error %v", got, got.err)
			}
			for i, target := range f.targets {
				after := target.state(t, f.g)
				if !reflect.DeepEqual(before[i], after) {
					t.Fatalf("owner %d mismatch/refusal changed state: head equal %v/history equal %v; versions %d/%d, history lengths %d/%d", i, reflect.DeepEqual(before[i].head, after.head), reflect.DeepEqual(before[i].history, after.history), before[i].head.version, after.head.version, len(before[i].history), len(after.history))
				}
				if afterClock := publicCASClock(t, f.g); afterClock != clock {
					t.Fatalf("owner %d mismatch/refusal advanced commit clock from %d to %d", i, clock, afterClock)
				}
			}
			t.Logf("comparison control: %+v; shared-byte refusal: %+v", control, got)
		})
	}
}

func TestReadBudgetPublicCASAtomicReopen(t *testing.T) {
	for _, kind := range []string{"node", "rel"} {
		t.Run(kind, func(t *testing.T) {
			f := newPublicCASFixture(t, kind)
			target := f.targets[0]
			before := target.state(t, f.g)
			clock := publicCASClock(t, f.g)
			control := f.mismatch(t, store.ReadBudget{}, []publicCASTarget{target})
			if control.err != nil || control.decoded == 0 || control.work.DecodedBytes == 0 {
				t.Fatalf("comparison control: %+v", control)
			}
			// Two publishers observe the same parent. Entity locking must allow
			// exactly one admitted compare-and-write to publish the next state.
			start := make(chan struct{})
			var wg sync.WaitGroup
			matched, errs := [2]bool{}, [2]error{}
			for i := range 2 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					ctx, _ := store.WithReadBudget(t.Context(), store.ReadBudget{DecodedBytes: 1 << 20, HistoryExamined: 100, Candidates: 100, BufferBytes: 1 << 20})
					matched[i], errs[i] = target.compare(ctx, f.g, bytes.Repeat([]byte{3}, 512), bytes.Repeat([]byte{[]byte{4, 5}[i]}, 512))
				}()
			}
			close(start)
			wg.Wait()
			if errs[0] != nil || errs[1] != nil || matched[0] == matched[1] {
				t.Fatalf("atomic publication: matched %v/errors %v; want exactly one successful writer", matched, errs)
			}
			winner := byte(4)
			if matched[1] {
				winner = 5
			}
			after := target.state(t, f.g)
			if after.head.version != before.head.version+1 || len(after.history) != len(before.history)+1 || !types.PropertyEqual(after.head.properties["state"], bytes.Repeat([]byte{winner}, 512)) || after.head.properties["guard"] != "unchanged" || publicCASClock(t, f.g) <= clock {
				t.Fatalf("successful CAS did not publish one whole revision: version %d/history %d/state %v", after.head.version, len(after.history), after.head.properties["state"])
			}
			previous := before.head
			previous.temporal.TxTo = after.head.temporal.TxFrom
			if !reflect.DeepEqual(after.history[:len(before.history)], before.history) || !reflect.DeepEqual(after.history[len(before.history)], previous) {
				t.Fatal("successful CAS changed an older revision or failed to archive the exact prior head")
			}
			if err := f.st.DurableFlush(); err != nil {
				t.Fatal(err)
			}
			if err := f.g.Close(); err != nil {
				t.Fatal(err)
			}
			f.open(t)
			if got := target.state(t, f.g); !reflect.DeepEqual(after, got) {
				t.Fatal("durable reopen changed the admitted CAS head or historical revisions")
			}
		})
	}
}

func TestReadBudgetPublicCASSegmentedPrecedence(t *testing.T) {
	for _, kind := range []string{"node", "rel"} {
		t.Run(kind, func(t *testing.T) {
			f := newPublicCASFixture(t, kind)
			target := f.targets[0]
			var err error
			props := map[string]any{"state": []byte("segment")}
			if target.node != 0 {
				_, err = f.g.Temporal().SetNodeVersionInterval(t.Context(), target.node, 100, 200, props)
			} else {
				_, err = f.g.Temporal().SetRelVersionInterval(t.Context(), target.rel, 100, 200, props)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := f.st.DurableFlush(); err != nil {
				t.Fatal(err)
			}
			before := target.state(t, f.g)
			clock := publicCASClock(t, f.g)
			current := before.head.properties["state"]
			for _, pair := range [][2]any{{current, []byte("replacement")}, {[]byte("mismatch"), current}, {current, current}} {
				for _, metered := range []bool{false, true} {
					ctx := t.Context()
					if metered {
						ctx, _ = store.WithReadBudget(ctx, store.ReadBudget{DecodedBytes: 1 << 20, HistoryExamined: 100, Candidates: 100, BufferBytes: 1 << 20})
					}
					matched, err := target.compare(ctx, f.g, pair[0], pair[1])
					if matched || !errors.Is(err, graph.ErrUntimedModeMismatch) {
						t.Fatalf("segmented comparison (metered %v): matched %v/error %v; want WholeAxis refusal before match/no-op decisions", metered, matched, err)
					}
				}
			}
			if !reflect.DeepEqual(before, target.state(t, f.g)) || publicCASClock(t, f.g) != clock {
				t.Fatal("segmented CAS refusal changed state or commit clock")
			}
		})
	}
}
