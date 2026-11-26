package graph_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// nullProbeBackends are every store backend, and badger and tiered with the
// type's definition kept on disk (its 0x0C entries carry the null postings),
// as ai-soc keeps it; mut (nil = unchanged) applies to every Config.
func nullProbeBackends(mut func(*graphpkg.Config)) []storeBackend {
	out := append(allStoreBackendsWith(mut), storeBackend{name: "tiered-disk", tiered: true, open: func(t *testing.T) *graphpkg.Graph {
		ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Ref"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: noAutoFlush,
			RelPropertyIndexOnDiskTypes: []string{"LOGON"}})
		if err != nil {
			t.Fatalf("tiered.New: %v", err)
		}
		cfg := graphpkg.Config{Validation: graphpkg.ValidationLimits{AllowSelfLoops: true}, Store: ts}
		if mut != nil {
			mut(&cfg)
		}
		g, err := graphpkg.New(cfg)
		if err != nil {
			t.Fatalf("graph.New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		return g
	}})
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) {
		if mut != nil {
			mut(c)
		}
		c.RelPropertyIndexOnDiskTypes = []string{"LOGON"}
	}) {
		if b.name == "badger" {
			b.name = "badger-disk"
			out = append(out, b)
		}
	}
	return out
}

// addLogon adds a LOGON edge valid over [from, to), with dev when dev is
// non-empty.
func addLogon(t *testing.T, g *graphpkg.Graph, a, z *types.Node, dev string, from, to int64) types.RelID {
	t.Helper()
	props := map[string]any{"tkg_valid_from": types.Instant(from), "tkg_valid_to": types.Instant(to)}
	if dev != "" {
		props["dev"] = dev
	}
	r, err := g.Rels().Add(context.Background(), "LOGON", a, z, props)
	if err != nil {
		t.Fatal(err)
	}
	return r.ID()
}

// TestNullProbeReadsTheWindowsNullRows: types.Null reads through the
// relationship index exactly the rows whose property is absent, at a valid
// instant, at the transaction pin and current, with the index built before
// and after rows exist, read twice (the first temporal read builds the
// sidecar); nil still matches nothing and a value probe never returns a null
// row. Its cost is TestNullProbeWorkIsFlatOverDays.
func TestNullProbeReadsTheWindowsNullRows(t *testing.T) {
	t.Parallel()
	for _, b := range nullProbeBackends(nil) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, err := g.Nodes().Add(ctx, []string{"D"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes().Add(ctx, []string{"D"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			withDev := addLogon(t, g, a, z, "d1", 100, 200)
			early := addLogon(t, g, a, z, "", 100, 200) // backfilled by the index build
			if err := g.Index().CreateRelProperty("LOGON", "dev"); err != nil {
				t.Fatal(err)
			}
			late := addLogon(t, g, a, z, "", 300, 400) // written with the index
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for range 2 { // the first temporal read builds the sidecar, the second reads it
				for _, c := range []struct {
					name  string
					value any
					opts  store.QueryOpts
					want  []types.RelID
				}{
					{"null current", types.Null, store.QueryOpts{}, []types.RelID{early, late}},
					{"null at 150", types.Null, store.QueryOpts{ValidAt: 150, ValidAtSet: true}, []types.RelID{early}},
					{"null at 350", types.Null, store.QueryOpts{ValidAt: 350, ValidAtSet: true}, []types.RelID{late}},
					{"null at the pin", types.Null, store.QueryOpts{TxAt: pin, ValidAt: 150, ValidAtSet: true}, []types.RelID{early}},
					{"value at 150", "d1", store.QueryOpts{ValidAt: 150, ValidAtSet: true}, []types.RelID{withDev}},
					{"nil", nil, store.QueryOpts{}, nil},
				} {
					mctx, _ := store.WithReadBudget(ctx, store.ReadBudget{})
					got, err := g.Rels().ByTypeAndPropertyCtx(mctx, "LOGON", "dev", c.value, c.opts)
					if err != nil {
						t.Fatalf("%s: %v", c.name, err)
					}
					ids := relIDs(got)
					slices.Sort(ids)
					var want []int64
					for _, id := range c.want {
						want = append(want, int64(id))
					}
					slices.Sort(want)
					if !slices.Equal(ids, want) {
						t.Errorf("%s: %v, want %v", c.name, ids, want)
					}
				}
			}
		})
	}
}

// TestNullProbeWorkIsFlatOverDays: on a feed adding each day 20 edges with
// dev and 20 without, a Null probe at the day's instant does the work of a
// value probe of as many rows (the same valid-window seek), and where the
// value probe is flat over days, day 8 at most 1.2 times day 1, so is Null.
// badger-disk and tiered-disk hold no window seek for either (a value probe
// there grows with history too).
func TestNullProbeWorkIsFlatOverDays(t *testing.T) {
	t.Parallel()
	const day = int64(86_400_000)
	for _, b := range nullProbeBackends(nil) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			if err := g.Index().CreateRelProperty("LOGON", "dev"); err != nil {
				t.Fatal(err)
			}
			a, err := g.Nodes().Add(ctx, []string{"D"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes().Add(ctx, []string{"D"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			work := func(d int64, value any) uint64 {
				t.Helper()
				mctx, meter := store.WithReadBudget(ctx, store.ReadBudget{})
				got, err := g.Rels().ByTypeAndPropertyCtx(mctx, "LOGON", "dev", value, store.QueryOpts{ValidAt: types.Instant(d*day + 500), ValidAtSet: true})
				if err != nil || len(got) != 20 {
					t.Fatalf("day %d, %v: %d rows, %v; want 20", d, value, len(got), err)
				}
				w := meter.Work()
				return w.Candidates + w.HistoryExamined
			}
			var nullFirst, valueFirst uint64
			for d := int64(1); d <= 8; d++ {
				for i := range int64(20) {
					addLogon(t, g, a, z, "d1", d*day+i, d*day+i+1000)
					addLogon(t, g, a, z, "", d*day+i, d*day+i+1000)
				}
				work(d, types.Null) // a first temporal read may build the sidecar
				null, value := work(d, types.Null), work(d, "d1")
				if null > value {
					t.Errorf("day %d: Null work %d, value work %d of as many rows", d, null, value)
				}
				if d == 1 {
					nullFirst, valueFirst = null, value
				}
				if d == 8 && float64(value) <= 1.2*float64(valueFirst) && float64(null) > 1.2*float64(nullFirst) {
					t.Errorf("day 8 Null work %d, day 1 %d: grows with history where a value probe is flat", null, nullFirst)
				}
			}
		})
	}
}

// TestNullPostingFormatRefusesAnOlderDiskIndex: a relationship definition kept
// on disk raises the marker to NullPostingStoreFormatVersion; the same store at
// the marker before it (a directory built without null postings) is refused at
// open with ErrWireFormatVersionUnsupported, naming the rebuild, since a Null
// probe would miss its rows.
func TestNullPostingFormatRefusesAnOlderDiskIndex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := graphpkg.Config{BadgerDir: dir, RelPropertyIndexOnDiskTypes: []string{"LOGON"}}
	g, err := graphpkg.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Index().CreateRelProperty("LOGON", "dev"); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	var marker []byte
	err = db.Update(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storeutil.WireFormatVersionKey)
		if err != nil {
			return err
		}
		if marker, err = item.ValueCopy(nil); err != nil {
			return err
		}
		return txn.Set(storeutil.WireFormatVersionKey, []byte{0, storeutil.NullPostingStoreFormatVersion - 1})
	})
	if err := errors.Join(err, db.Close()); err != nil {
		t.Fatal(err)
	}
	if len(marker) != 2 || marker[1] != storeutil.NullPostingStoreFormatVersion {
		t.Fatalf("marker after the definition %v, want %d", marker, storeutil.NullPostingStoreFormatVersion)
	}
	if g, err := graphpkg.New(cfg); !errors.Is(err, store.ErrWireFormatVersionUnsupported) || !strings.Contains(fmt.Sprint(err), "rebuild") {
		if g != nil {
			_ = g.Close()
		}
		t.Fatalf("open at marker %d: %v, want ErrWireFormatVersionUnsupported naming the rebuild", storeutil.NullPostingStoreFormatVersion-1, err)
	}
}

// TestNullPostingCostPerEdge: on an ai-soc-shaped feed (LOGON edges, half
// without dev), an edge without dev costs the index (kept on disk) the
// entries and bytes of one with a value: a constant per edge (per version),
// the same as a value's.
func TestNullPostingCostPerEdge(t *testing.T) {
	t.Parallel()
	const edges = 200
	measure := func(t *testing.T, withDev func(i int) bool) (entries, bytes int) {
		dir := t.TempDir()
		g, err := graphpkg.New(graphpkg.Config{BadgerDir: dir, RelPropertyIndexOnDiskTypes: []string{"LOGON"}, Validation: graphpkg.ValidationLimits{AllowSelfLoops: true}})
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Index().CreateRelProperty("LOGON", "dev"); err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		a, err := g.Nodes().Add(ctx, []string{"D"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for i := range edges {
			dev := ""
			if withDev(i) {
				dev = fmt.Sprintf("dev-%04d", i)
			}
			addLogon(t, g, a, a, dev, int64(1000+i), int64(2000+i))
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		err = db.View(func(txn *badgerv4.Txn) error {
			it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: []byte{storeutil.KeyEntityIndex}})
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				entries++
				bytes += len(it.Item().Key()) + int(it.Item().ValueSize())
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return entries, bytes
	}
	allEntries, allBytes := measure(t, func(int) bool { return true })
	halfEntries, halfBytes := measure(t, func(i int) bool { return i%2 == 0 })
	t.Logf("all with dev: %d entries, %d bytes (%.1f B/edge); half without: %d entries, %d bytes (%.1f B/edge)",
		allEntries, allBytes, float64(allBytes)/edges, halfEntries, halfBytes, float64(halfBytes)/edges)
	if halfEntries != allEntries {
		t.Errorf("entries: half without dev %d, all with dev %d: a null edge must cost a value edge's entries", halfEntries, allEntries)
	}
	if float64(halfBytes) > 1.1*float64(allBytes) {
		t.Errorf("bytes: half without dev %d, all with dev %d", halfBytes, allBytes)
	}
}

// TestWindowedProbeWorkIsFlatWithTheIndexOnDisk: on a feed adding each day 20
// LOGON edges, with badger and tiered opened as ai-soc opens them (SyncWrites,
// DurableCommit, the type's index on disk, no explicit flush), a value probe at
// the day's instant reads on day 30 at most 1.2 times the entries of day 4 and
// answers the rows of the in-RAM index.
func TestWindowedProbeWorkIsFlatWithTheIndexOnDisk(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	const day, days = int64(86_400_000), 30
	feed := func(t *testing.T, g *graphpkg.Graph) (work []uint64, answers [][]int64) {
		ctx := context.Background()
		if err := g.Index().CreateRelProperty("LOGON", "dev"); err != nil {
			t.Fatal(err)
		}
		a, err := g.Nodes().Add(ctx, []string{"D"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		z, err := g.Nodes().Add(ctx, []string{"D"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for d := int64(1); d <= days; d++ {
			for i := range int64(20) {
				addLogon(t, g, a, z, "d1", d*day+i, d*day+i+1000)
			}
			mctx, meter := store.WithReadBudget(ctx, store.ReadBudget{})
			got, err := g.Rels().ByTypeAndPropertyCtx(mctx, "LOGON", "dev", "d1", store.QueryOpts{ValidAt: types.Instant(d*day + 500), ValidAtSet: true})
			if err != nil {
				t.Fatalf("day %d: %v", d, err)
			}
			ids := relIDs(got)
			slices.Sort(ids)
			w := meter.Work()
			work, answers = append(work, w.Candidates+w.HistoryExamined), append(answers, ids)
		}
		return work, answers
	}
	open := func(t *testing.T, cfg graphpkg.Config) *graphpkg.Graph {
		cfg.Validation = graphpkg.ValidationLimits{AllowSelfLoops: true}
		cfg.DurableCommit = cfg.Store != nil || cfg.BadgerDir != ""
		g, err := graphpkg.New(cfg)
		if err != nil {
			t.Fatalf("graph.New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		return g
	}
	_, want := feed(t, open(t, graphpkg.Config{}))
	for _, b := range []struct {
		name string
		open func(t *testing.T) *graphpkg.Graph
	}{
		{"badger-disk", func(t *testing.T) *graphpkg.Graph {
			return open(t, graphpkg.Config{BadgerDir: t.TempDir(), SyncWrites: true, RelPropertyIndexOnDiskTypes: []string{"LOGON"}})
		}},
		{"tiered-disk", func(t *testing.T) *graphpkg.Graph {
			ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), RefLabels: []string{"Ref"}, ShardWindow: 7 * 24 * time.Hour, SyncWrites: true,
				RelPropertyIndexOnDiskTypes: []string{"LOGON"}})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			return open(t, graphpkg.Config{Store: ts})
		}},
	} {
		t.Run(b.name, func(t *testing.T) {
			work, answers := feed(t, b.open(t))
			for d := range answers {
				if len(answers[d]) != 20 || len(answers[d]) != len(want[d]) {
					t.Fatalf("day %d: %d rows, the in-RAM index %d", d+1, len(answers[d]), len(want[d]))
				}
			}
			if float64(work[days-1]) > 1.2*float64(work[3]) {
				t.Errorf("day %d read %d entries, day 4 %d: grows with history", days, work[days-1], work[3])
			}
			t.Logf("entries read on day 1, 4, 8, %d: %d %d %d %d", days, work[0], work[3], work[7], work[days-1])
		})
	}
}
