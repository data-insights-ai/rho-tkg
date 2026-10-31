package graph_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// WHOLEROW (ADR-0014): sigma-tkgd's paged-row writer rewrites a grown page
// row whole as one version with its full validity, on a closed row, with an
// equal or earlier start, and reads it by TxPin plus the current row. Sigma's
// five tests, each on badger and tiered (the page label a reference label,
// its property indexes on disk, ov_h* bounding valid ends: sigma's shape).

const (
	pageLabel = "Page"
	pageOldVF = types.Instant(1000)
	pageOldVT = types.Instant(2000)
	pageNewVF = types.Instant(500)
	pageNewVT = types.Instant(3000)
)

var pageValidEnd = []storepkg.PropertyIndexValidEnd{{Label: pageLabel, KeyPrefix: "ov_h"}}

func pageNewProps() map[string]any {
	return map[string]any{"ov_k": "k1", "ov_h3": "h-new", "pages": int64(2)}
}

// replaceVersionBackends opens badger and tiered graphs with Page listed for
// ReplaceVersion and its property indexes on disk with ov_h* bounds.
func replaceVersionBackends() []storeBackend {
	newGraph := func(t *testing.T, cfg graphpkg.Config) *graphpkg.Graph {
		t.Helper()
		cfg.ReplaceVersionLabels = []string{pageLabel}
		g, err := graphpkg.New(cfg)
		if err != nil {
			t.Fatalf("graph.New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		return g
	}
	return []storeBackend{
		{name: "badger", open: func(t *testing.T) *graphpkg.Graph {
			return newGraph(t, graphpkg.Config{BadgerInMemory: true, PropertyIndexOnDiskLabels: []string{pageLabel}, PropertyIndexValidEnd: pageValidEnd})
		}},
		{name: "tiered", tiered: true, open: func(t *testing.T) *graphpkg.Graph {
			ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), RefLabels: []string{pageLabel}, ShardWindow: time.Hour,
				FlushInterval: 1<<63 - 1, PropertyIndexOnDiskLabels: []string{pageLabel}, PropertyIndexValidEnd: pageValidEnd})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			return newGraph(t, graphpkg.Config{Store: ts})
		}},
	}
}

func forReplaceVersionBackends(t *testing.T, fn func(t *testing.T, g *graphpkg.Graph)) {
	t.Helper()
	for _, b := range replaceVersionBackends() {
		t.Run(b.name, func(t *testing.T) { fn(t, b.open(t)) })
	}
}

// pageFixture is one closed page row [1000, 2000) with ov_k k1, ov_h3 h-old
// and size 1, read before the replace: the row, a pin and the timeline at it.
type pageFixture struct {
	id      types.NodeID
	old     *types.Node
	before  types.Instant
	oldSegs []temporal.NodeSegment
}

func writePage(t *testing.T, g *graphpkg.Graph) pageFixture {
	t.Helper()
	ctx := context.Background()
	for _, k := range []string{"ov_k", "ov_h3"} {
		if err := g.Index().CreateProperty(pageLabel, k); err != nil {
			t.Fatal(err)
		}
	}
	n, err := g.Nodes().Add(ctx, []string{pageLabel}, map[string]any{"ov_k": "k1", "ov_h3": "h-old", "size": int64(1), "tkg_valid_from": pageOldVF})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes().CloseVersion(ctx, n.ID(), pageOldVT); err != nil {
		t.Fatal(err)
	}
	f := pageFixture{id: n.ID()}
	if f.old, err = g.Nodes().Get(ctx, n.ID()); err != nil {
		t.Fatal(err)
	}
	if f.before, err = g.Temporal().NowTx(); err != nil {
		t.Fatal(err)
	}
	if f.oldSegs, err = g.Temporal().NodeEffectiveTimeline(n.ID(), f.before); err != nil {
		t.Fatal(err)
	}
	return f
}

// replacePage replaces the fixture's row whole: [500, 3000), ov_h3 h-new,
// size dropped, pages added.
func replacePage(t *testing.T, g *graphpkg.Graph, f pageFixture) *types.Node {
	t.Helper()
	n, err := g.Nodes().ReplaceVersion(context.Background(), f.id, pageNewProps(), pageNewVF, pageNewVT)
	if err != nil {
		t.Fatalf("ReplaceVersion on a closed row with an earlier start and a grown end: %v", err)
	}
	return n
}

func wantRow(t *testing.T, what string, got *types.Node, version uint32, vf, vt types.Instant, props map[string]any) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s: nil row", what)
	}
	tm := got.Temporal()
	if got.Version() != version || tm == nil || tm.ValidFrom != vf || tm.ValidTo != vt {
		t.Fatalf("%s: version %d [%d, %d), want version %d [%d, %d)", what, got.Version(), tm.ValidFrom, tm.ValidTo, version, vf, vt)
	}
	if props != nil && !reflect.DeepEqual(got.PropertiesMap(), props) {
		t.Fatalf("%s: properties %v, want %v", what, got.PropertiesMap(), props)
	}
}

func onlyNode(t *testing.T, what string, ns []*types.Node, err error, id types.NodeID) *types.Node {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if len(ns) != 1 || ns[0].ID() != id {
		t.Fatalf("%s: %d rows, want exactly the node", what, len(ns))
	}
	return ns[0]
}

// Test 1: a read pinned at or after the replace returns that version's
// validity only: no older piece or version beside it, on the record doors
// (NodeAsOf, TxPin) and on the effective timeline (a replace covering the old
// interval leaves nothing older).
func TestReplaceVersionPinAtOrAfterReadsTheNewVersionOnly(t *testing.T) {
	forReplaceVersionBackends(t, func(t *testing.T, g *graphpkg.Graph) {
		f := writePage(t, g)
		rep := replacePage(t, g, f)
		after := rep.Temporal().TxFrom
		now, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		if after <= f.old.Temporal().TxFrom {
			t.Fatalf("new TxFrom %d not above the replaced row's %d", after, f.old.Temporal().TxFrom)
		}
		for _, pin := range []types.Instant{after, now} {
			n, err := g.Temporal().NodeAsOf(f.id, pin)
			if err != nil {
				t.Fatalf("NodeAsOf at %d: %v", pin, err)
			}
			wantRow(t, fmt.Sprintf("NodeAsOf at %d", pin), n, rep.Version(), pageNewVF, pageNewVT, pageNewProps())
			ns, err := g.Nodes().ByLabel(pageLabel, storepkg.QueryOpts{TxPin: pin})
			wantRow(t, fmt.Sprintf("ByLabel TxPin %d", pin), onlyNode(t, "ByLabel TxPin", ns, err, f.id), rep.Version(), pageNewVF, pageNewVT, pageNewProps())
			ns, err = g.Temporal().NodesAsOf(pin)
			wantRow(t, fmt.Sprintf("NodesAsOf %d", pin), onlyNode(t, "NodesAsOf", ns, err, f.id), rep.Version(), pageNewVF, pageNewVT, nil)
			segs, err := g.Temporal().NodeEffectiveTimeline(f.id, pin)
			if err != nil {
				t.Fatal(err)
			}
			if len(segs) != 1 || segs[0].ValidFrom != pageNewVF || segs[0].ValidTo != pageNewVT || segs[0].Node.Version() != rep.Version() {
				t.Fatalf("timeline at %d: %d segments, want one [%d, %d) of version %d: %+v", pin, len(segs), pageNewVF, pageNewVT, rep.Version(), segs)
			}
		}
	})
}

// Test 2: a read pinned before the replace returns what it returned before.
func TestReplaceVersionPinBeforeReadsAsBefore(t *testing.T) {
	forReplaceVersionBackends(t, func(t *testing.T, g *graphpkg.Graph) {
		f := writePage(t, g)
		replacePage(t, g, f)
		n, err := g.Temporal().NodeAsOf(f.id, f.before)
		if err != nil {
			t.Fatal(err)
		}
		wantRow(t, "NodeAsOf before", n, f.old.Version(), pageOldVF, pageOldVT, f.old.PropertiesMap())
		ns, err := g.Nodes().ByLabel(pageLabel, storepkg.QueryOpts{TxPin: f.before})
		wantRow(t, "ByLabel TxPin before", onlyNode(t, "ByLabel TxPin before", ns, err, f.id), f.old.Version(), pageOldVF, pageOldVT, f.old.PropertiesMap())
		segs, err := g.Temporal().NodeEffectiveTimeline(f.id, f.before)
		if err != nil {
			t.Fatal(err)
		}
		if len(segs) != len(f.oldSegs) {
			t.Fatalf("timeline before: %d segments, want %d", len(segs), len(f.oldSegs))
		}
		for i := range segs {
			if segs[i].ValidFrom != f.oldSegs[i].ValidFrom || segs[i].ValidTo != f.oldSegs[i].ValidTo || segs[i].Node.Version() != f.oldSegs[i].Node.Version() {
				t.Fatalf("timeline before changed: %+v, want %+v", segs[i], f.oldSegs[i])
			}
		}
	})
}

// Test 3: the current row from an index read is the newest version and
// carries its TxFrom: one row decides a node, no history read.
func TestReplaceVersionIndexReadIsTheNewestVersionWithItsTxFrom(t *testing.T) {
	forReplaceVersionBackends(t, func(t *testing.T, g *graphpkg.Graph) {
		f := writePage(t, g)
		rep := replacePage(t, g, f)
		now, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		for _, opts := range []storepkg.QueryOpts{{}, {TxPin: now}} {
			ns, err := g.Nodes().ByLabelAndProperty(pageLabel, "ov_k", "k1", opts)
			n := onlyNode(t, fmt.Sprintf("ByLabelAndProperty ov_k %+v", opts), ns, err, f.id)
			wantRow(t, "ov_k row", n, rep.Version(), pageNewVF, pageNewVT, pageNewProps())
			if tx, _, _ := n.TxStamps(); tx != rep.Temporal().TxFrom || tx <= f.old.Temporal().TxFrom {
				t.Fatalf("ov_k row TxFrom %d, want the replace's %d above the old %d", tx, rep.Temporal().TxFrom, f.old.Temporal().TxFrom)
			}
			ns, err = g.Nodes().ByLabelAndProperty(pageLabel, "ov_h3", "h-new", opts)
			onlyNode(t, "ov_h3 h-new", ns, err, f.id)
			if ns, err = g.Nodes().ByLabelAndProperty(pageLabel, "ov_h3", "h-old", opts); err != nil || len(ns) != 0 {
				t.Fatalf("ov_h3 h-old %+v: %d rows, %v; want none", opts, len(ns), err)
			}
		}
	})
}

// Test 4: TxFrom rises with version on these nodes across a crash; the
// replace takes a closed row and an equal or earlier start. The child writes
// under SyncWrites and exits without Close; the parent reopens the directory.
const (
	rvEnvMode    = "RHO_TKG_WHOLEROW_CRASH_CHILD"
	rvEnvDir     = "RHO_TKG_WHOLEROW_CRASH_DIR"
	rvEnvBackend = "RHO_TKG_WHOLEROW_CRASH_BACKEND"
)

func rvCrashConfig(backend, dir string) (graphpkg.Config, error) {
	cfg := graphpkg.Config{ReplaceVersionLabels: []string{pageLabel}}
	switch backend {
	case "badger":
		bs, err := badger.New(badger.Config{Dir: dir, SyncWrites: true})
		if err != nil {
			return cfg, err
		}
		cfg.Store = bs
	case "tiered":
		ts, err := tiered.New(tiered.Config{DataDir: dir, RefLabels: []string{pageLabel}, SyncWrites: true})
		if err != nil {
			return cfg, err
		}
		cfg.Store = ts
	default:
		return cfg, fmt.Errorf("unknown backend %q", backend)
	}
	return cfg, nil
}

func TestReplaceVersionCrashChild(t *testing.T) {
	if os.Getenv(rvEnvMode) != "1" {
		t.Skip("crash child: run only by TestReplaceVersionTxFromRisesWithVersionAcrossCrash")
	}
	cfg, err := rvCrashConfig(os.Getenv(rvEnvBackend), os.Getenv(rvEnvDir))
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(2)
	}
	g, err := graphpkg.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "graph.New:", err)
		os.Exit(3)
	}
	ctx := context.Background()
	n, err := g.Nodes().Add(ctx, []string{pageLabel}, map[string]any{"ov_k": "k1", "size": int64(1), "tkg_valid_from": pageOldVF})
	if err == nil {
		err = g.Nodes().CloseVersion(ctx, n.ID(), pageOldVT) // closed row
	}
	if err == nil { // an equal start, a grown end
		_, err = g.Nodes().ReplaceVersion(ctx, n.ID(), map[string]any{"ov_k": "k1", "pages": int64(2)}, pageOldVF, pageNewVT)
	}
	if err == nil { // an earlier start, a grown end, on the closed replaced row
		_, err = g.Nodes().ReplaceVersion(ctx, n.ID(), map[string]any{"ov_k": "k1", "pages": int64(3)}, pageNewVF, pageNewVT+1000)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "write:", err)
		os.Exit(4)
	}
	fmt.Printf("\n%s%d\n", rvIDMarker, int64(n.ID())) // the store logs to stdout too
	os.Exit(0)                                        // crash: no Close
}

const rvIDMarker = "WHOLEROW_ID="

func TestReplaceVersionTxFromRisesWithVersionAcrossCrash(t *testing.T) {
	for _, backend := range []string{"badger", "tiered"} {
		t.Run(backend, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestReplaceVersionCrashChild$", "-test.count=1")
			cmd.Env = append(os.Environ(), rvEnvMode+"=1", rvEnvDir+"="+dir, rvEnvBackend+"="+backend)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("crash child failed: %v\n%s", err, out)
			}
			var raw int64
			_, marker, found := strings.Cut(string(out), rvIDMarker)
			if !found {
				t.Fatalf("child output without %s:\n%s", rvIDMarker, out)
			}
			if _, err := fmt.Sscan(marker, &raw); err != nil {
				t.Fatalf("child output %q: %v", marker, err)
			}
			id := types.NodeID(raw)
			cfg, err := rvCrashConfig(backend, dir)
			if err != nil {
				t.Fatal(err)
			}
			g, err := graphpkg.New(cfg)
			if err != nil {
				t.Fatalf("reopen after crash: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })

			current, err := g.Nodes().Get(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			// Versions start at 0: the create, the close, the two replaces.
			wantRow(t, "current after crash", current, 3, pageNewVF, pageNewVT+1000, map[string]any{"ov_k": "k1", "pages": int64(3)})
			hist, err := g.Nodes().History(id)
			if err != nil {
				t.Fatal(err)
			}
			if len(hist) != 3 {
				t.Fatalf("history after crash: %d rows, want 3 (versions 0-2)", len(hist))
			}
			rows := append(hist, current)
			for i, r := range rows {
				if r.Version() != uint32(i) {
					t.Fatalf("row %d has version %d, want %d (dense, rising)", i, r.Version(), i)
				}
				tx, txTo, _ := r.TxStamps()
				if i > 0 {
					prevTx, prevTo, _ := rows[i-1].TxStamps()
					if tx < prevTx {
						t.Fatalf("version %d TxFrom %d below version %d's %d", r.Version(), tx, rows[i-1].Version(), prevTx)
					}
					if prevTo != tx {
						t.Fatalf("version %d TxTo %d, want the next version's TxFrom %d", rows[i-1].Version(), prevTo, tx)
					}
				}
				if i == len(rows)-1 && txTo != 0 {
					t.Fatalf("current row carries TxTo %d", txTo)
				}
			}
			now, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			asOf, err := g.Temporal().NodeAsOf(id, now)
			if err != nil {
				t.Fatal(err)
			}
			wantRow(t, "NodeAsOf now after crash", asOf, 3, pageNewVF, pageNewVT+1000, nil)
			if ok, err := g.Hash().VerifyNodeChain(id); err != nil || !ok {
				t.Fatalf("VerifyNodeChain after crash = %v, %v", ok, err)
			}
		})
	}
}

// Test 5: ov_k, ov_h<mask> and the validity-end index follow the new
// version; a pin before it still finds the node by its old values.
func TestReplaceVersionIndexesFollowTheNewVersion(t *testing.T) {
	forReplaceVersionBackends(t, func(t *testing.T, g *graphpkg.Graph) {
		f := writePage(t, g)
		pastOldEnd := storepkg.QueryOpts{ValidEndAfter: pageOldVT + 500, ValidEndAfterSet: true}
		pruned := func() uint64 {
			t.Helper()
			st, err := g.Index().PropertyIndexValidEndStats()
			if err != nil {
				t.Fatal(err)
			}
			return st.Pruned
		}
		for range 2 { // the first read refines the stored bound to the old end
			if ns, err := g.Nodes().ByLabelAndProperty(pageLabel, "ov_h3", "h-old", pastOldEnd); err != nil || len(ns) != 0 {
				t.Fatalf("closed at %d, ValidEndAfter %d: %d rows, %v; want none", pageOldVT, pastOldEnd.ValidEndAfter, len(ns), err)
			}
		}
		if pruned() != 1 {
			t.Fatalf("the second read pruned %d entries by the refined bound, want 1", pruned())
		}
		rep := replacePage(t, g, f)
		after := rep.Temporal().TxFrom

		// The replace wrote the entry unknown again: the read resolves the row
		// instead of pruning it by the old bound.
		ns, err := g.Nodes().ByLabelAndProperty(pageLabel, "ov_h3", "h-new", pastOldEnd)
		wantRow(t, "ov_h3 h-new past the old end", onlyNode(t, "ov_h3 h-new ValidEndAfter", ns, err, f.id), rep.Version(), pageNewVF, pageNewVT, nil)
		if pruned() != 1 {
			t.Fatalf("a read after the replace pruned by the old bound (%d pruned, want 1)", pruned())
		}
		ns, err = g.Nodes().ByLabelAndProperty(pageLabel, "ov_k", "k1", pastOldEnd)
		onlyNode(t, "ov_k past the old end", ns, err, f.id)
		pastNewEnd := storepkg.QueryOpts{ValidEndAfter: pageNewVT + 1, ValidEndAfterSet: true}
		if ns, err = g.Nodes().ByLabelAndProperty(pageLabel, "ov_k", "k1", pastNewEnd); err != nil || len(ns) != 0 {
			t.Fatalf("ValidEndAfter past the new end: %d rows, %v; want none", len(ns), err)
		}

		for _, c := range []struct {
			key, value string
			pin        types.Instant
			want       bool
		}{
			{"ov_h3", "h-old", f.before, true}, {"ov_h3", "h-new", f.before, false}, {"ov_k", "k1", f.before, true},
			{"ov_h3", "h-old", after, false}, {"ov_h3", "h-new", after, true}, {"ov_k", "k1", after, true},
		} {
			ns, err := g.Nodes().ByLabelAndProperty(pageLabel, c.key, c.value, storepkg.QueryOpts{TxPin: c.pin})
			if err != nil {
				t.Fatal(err)
			}
			if (len(ns) == 1 && ns[0].ID() == f.id) != c.want || len(ns) > 1 {
				t.Fatalf("%s=%s at pin %d: %d rows, want found=%v", c.key, c.value, c.pin, len(ns), c.want)
			}
		}
	})
}

// Refusals write nothing; a replace that changes nothing writes nothing; the
// hash chain verifies across the replace; the GraphTx twin rolls back.
func TestReplaceVersionRefusalsAndTxTwin(t *testing.T) {
	forReplaceVersionBackends(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		f := writePage(t, g)
		other, err := g.Nodes().Add(ctx, []string{"Other"}, map[string]any{"ov_k": "o1", "tkg_valid_from": pageOldVF})
		if err != nil {
			t.Fatal(err)
		}
		refused := []struct {
			name  string
			id    types.NodeID
			props map[string]any
			vf    types.Instant
			vt    types.Instant
			want  error
		}{
			{"unlisted label", other.ID(), map[string]any{"ov_k": "o1"}, pageNewVF, pageNewVT, graphpkg.ErrReplaceVersionNotListed},
			{"unset start", f.id, pageNewProps(), 0, pageNewVT, graphpkg.ErrInvalidTimeRange},
			{"end at start", f.id, pageNewProps(), pageNewVF, pageNewVF, graphpkg.ErrInvalidTimeRange},
			{"shadow key", f.id, map[string]any{"ov_k": "k1", "tkg_valid_from": pageNewVF}, pageNewVF, pageNewVT, types.ErrReservedPrefix},
		}
		for _, c := range refused {
			if _, err := g.Nodes().ReplaceVersion(ctx, c.id, c.props, c.vf, c.vt); !errors.Is(err, c.want) {
				t.Fatalf("%s: %v, want %v", c.name, err, c.want)
			}
		}
		// A unique ov_k: the replace may not take another page's key.
		if err := g.Constraints().CreateUnique(ctx, pageLabel, "ov_k"); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes().Add(ctx, []string{pageLabel}, map[string]any{"ov_k": "k2", "tkg_valid_from": pageOldVF}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes().ReplaceVersion(ctx, f.id, map[string]any{"ov_k": "k2"}, pageNewVF, pageNewVT); !errors.Is(err, graphpkg.ErrUniqueViolation) {
			t.Fatalf("another page's ov_k: %v, want ErrUniqueViolation", err)
		}
		// A relationship constrained within its endpoints: the new validity
		// may not leave the relationship outside the page.
		if err := g.Constraints().Add(temporal.TemporalConstraint{Kind: temporal.ConstraintRelWithinEndpoints}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels().AddByID(ctx, "LINKS", f.id, other.ID(), map[string]any{"tkg_valid_from": types.Instant(1200), "tkg_valid_to": types.Instant(1800)}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes().ReplaceVersion(ctx, f.id, pageNewProps(), pageNewVF, 1500); !errors.Is(err, temporal.ErrTemporalConstraint) {
			t.Fatalf("an end inside the relationship's validity: %v, want ErrTemporalConstraint", err)
		}
		got, err := g.Nodes().Get(ctx, f.id)
		if err != nil {
			t.Fatal(err)
		}
		wantRow(t, "row after refusals", got, f.old.Version(), pageOldVF, pageOldVT, f.old.PropertiesMap())

		// A no-op: equal properties and validity, no event.
		bus := eventspkg.NewEventBus()
		var updates atomic.Int64
		bus.Subscribe(func(e eventspkg.Event) {
			if e.Type == eventspkg.EventNodeUpdate && e.EntityID == types.EntityID(f.id) {
				updates.Add(1)
			}
		})
		if err := g.Events().SetSync(bus); err != nil {
			t.Fatal(err)
		}
		same, err := g.Nodes().ReplaceVersion(ctx, f.id, f.old.PropertiesMap(), pageOldVF, pageOldVT)
		if err != nil {
			t.Fatal(err)
		}
		wantRow(t, "no-op replace", same, f.old.Version(), pageOldVF, pageOldVT, f.old.PropertiesMap())
		if updates.Load() != 0 {
			t.Fatalf("a no-op replace published %d update events", updates.Load())
		}

		// The tx twin: rolled back, every read is as before.
		rollback := errors.New("roll back")
		err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
			n, err := tx.ReplaceNodeVersion(f.id, pageNewProps(), pageNewVF, pageNewVT)
			if err != nil {
				return err
			}
			wantRow(t, "in tx", n, f.old.Version()+1, pageNewVF, pageNewVT, pageNewProps())
			return rollback
		})
		if !errors.Is(err, rollback) {
			t.Fatalf("tx: %v", err)
		}
		if got, err = g.Nodes().Get(ctx, f.id); err != nil {
			t.Fatal(err)
		}
		wantRow(t, "row after rollback", got, f.old.Version(), pageOldVF, pageOldVT, f.old.PropertiesMap())
		hist, err := g.Nodes().History(f.id)
		if err != nil || len(hist) != 1 {
			t.Fatalf("history after rollback: %d rows, %v; want 1", len(hist), err)
		}

		// Committed, the twin is the standalone door.
		if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
			_, err := tx.ReplaceNodeVersion(f.id, pageNewProps(), pageNewVF, pageNewVT)
			return err
		}); err != nil {
			t.Fatalf("tx replace of a closed row: %v", err)
		}
		if got, err = g.Nodes().Get(ctx, f.id); err != nil {
			t.Fatal(err)
		}
		wantRow(t, "row after tx commit", got, f.old.Version()+1, pageNewVF, pageNewVT, pageNewProps())
		if updates.Load() != 1 {
			t.Fatalf("the committed replace published %d update events, want 1 (the rolled-back one none)", updates.Load())
		}
		// An end alone, then a dropped property alone, each is a new version
		// with an event (standalone).
		if n, err := g.Nodes().ReplaceVersion(ctx, f.id, pageNewProps(), pageNewVF, pageNewVT+100); err != nil || n.Version() != f.old.Version()+2 {
			t.Fatalf("end-only replace: version %d, %v; want %d", n.Version(), err, f.old.Version()+2)
		}
		fewer := pageNewProps()
		delete(fewer, "pages")
		if n, err := g.Nodes().ReplaceVersion(ctx, f.id, fewer, pageNewVF, pageNewVT+100); err != nil || n.Version() != f.old.Version()+3 || n.PropertyCount() != len(fewer) {
			t.Fatalf("dropped-property replace: version %d with %d properties, %v; want %d with %d", n.Version(), n.PropertyCount(), err, f.old.Version()+3, len(fewer))
		}
		if n, err := g.Nodes().ReplaceVersion(ctx, f.id, fewer, pageNewVF-100, pageNewVT+100); err != nil || n.Version() != f.old.Version()+4 {
			t.Fatalf("start-only replace: version %d, %v; want %d", n.Version(), err, f.old.Version()+4)
		}
		if updates.Load() != 4 {
			t.Fatalf("%d update events after three standalone replaces, want 4", updates.Load())
		}
		if ok, err := g.Hash().VerifyNodeChain(f.id); err != nil || !ok {
			t.Fatalf("VerifyNodeChain = %v, %v", ok, err)
		}
		// A deleted node is refused as not found.
		if err := g.Nodes().Delete(ctx, f.id); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes().ReplaceVersion(ctx, f.id, pageNewProps(), pageNewVF, pageNewVT); !errors.Is(err, storepkg.ErrNodeNotFound) {
			t.Fatalf("deleted: %v, want ErrNodeNotFound", err)
		}
	})
}

// segment is one expected piece of an effective timeline.
type segment struct {
	vf, vt types.Instant
	props  string // ov_h3 of the version answering there
}

func wantTimeline(t *testing.T, what string, g *graphpkg.Graph, id types.NodeID, pin types.Instant, want []segment) {
	t.Helper()
	segs, err := g.Temporal().NodeEffectiveTimeline(id, pin)
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	got := make([]segment, 0, len(segs))
	for _, s := range segs {
		h3, _ := s.Node.PropertiesMap()["ov_h3"].(string)
		got = append(got, segment{s.ValidFrom, s.ValidTo, h3})
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s: timeline %+v, want %+v", what, got, want)
	}
}

// exciseThenReplace is the correction of a row whose range shrinks: every
// piece of the current range the new one leaves is excised first, then the new
// version replaces the row, all in one transaction.
func exciseThenReplace(g *graphpkg.Graph, id types.NodeID, left [][2]types.Instant, props map[string]any, vf, vt types.Instant) (*types.Node, error) {
	var rep *types.Node
	err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		for _, p := range left {
			if err := tx.ExciseNodeValidity(id, p[0], p[1]); err != nil {
				return err
			}
		}
		var err error
		rep, err = tx.ReplaceNodeVersion(id, props, vf, vt)
		return err
	})
	return rep, err
}

// The old row is [1000, 2000). shrinkShapes names each new range, what the
// older version keeps answering when the replace runs alone, and the pieces a
// correction excises for what left.
var shrinkShapes = []struct {
	name   string
	vf, vt types.Instant
	alone  []segment
	left   [][2]types.Instant
}{
	{"later start earlier end", 1200, 1800,
		[]segment{{1000, 1200, "h-old"}, {1200, 1800, "h-new"}},
		[][2]types.Instant{{1000, 1200}, {1800, 2000}}},
	{"later start same end", 1200, 2000,
		[]segment{{1000, 1200, "h-old"}, {1200, 2000, "h-new"}},
		[][2]types.Instant{{1000, 1200}}},
	{"same start earlier end", 1000, 1800,
		[]segment{{1000, 1800, "h-new"}},
		[][2]types.Instant{{1800, 2000}}},
	{"earlier start earlier end", 500, 1800,
		[]segment{{500, 1800, "h-new"}, {1800, 2000, "h-old"}},
		[][2]types.Instant{{1800, 2000}}},
	{"later start later end", 1200, 3000,
		[]segment{{1000, 1200, "h-old"}, {1200, 3000, "h-new"}},
		[][2]types.Instant{{1000, 1200}}},
	{"disjoint after", 3000, 4000,
		[]segment{{1000, 2000, "h-old"}, {3000, 4000, "h-new"}},
		[][2]types.Instant{{1000, 2000}}},
	{"disjoint before", 100, 500,
		[]segment{{100, 500, "h-new"}, {1000, 2000, "h-old"}},
		[][2]types.Instant{{1000, 2000}}},
}

// A replace alone keeps the older version where it does not cover (ADR-0014).
func TestReplaceVersionShrunkAloneKeepsTheOlderVersion(t *testing.T) {
	for _, c := range shrinkShapes {
		t.Run(c.name, func(t *testing.T) {
			forReplaceVersionBackends(t, func(t *testing.T, g *graphpkg.Graph) {
				f := writePage(t, g)
				rep, err := g.Nodes().ReplaceVersion(context.Background(), f.id, pageNewProps(), c.vf, c.vt)
				if err != nil {
					t.Fatal(err)
				}
				wantTimeline(t, "alone", g, f.id, rep.Temporal().TxFrom, c.alone)
			})
		})
	}
}

// Excise for what left, then replace, in one transaction: every read pinned at
// or after it sees the new version with its validity only; a pin before reads
// as before.
func TestReplaceVersionShrunkWithExciseReadsTheNewVersionOnly(t *testing.T) {
	for _, c := range shrinkShapes {
		t.Run(c.name, func(t *testing.T) {
			forReplaceVersionBackends(t, func(t *testing.T, g *graphpkg.Graph) {
				f := writePage(t, g)
				rep, err := exciseThenReplace(g, f.id, c.left, pageNewProps(), c.vf, c.vt)
				if err != nil {
					t.Fatal(err)
				}
				now, err := g.Temporal().NowTx()
				if err != nil {
					t.Fatal(err)
				}
				for _, pin := range []types.Instant{rep.Temporal().TxFrom, now} {
					n, err := g.Temporal().NodeAsOf(f.id, pin)
					if err != nil {
						t.Fatal(err)
					}
					wantRow(t, "NodeAsOf", n, rep.Version(), c.vf, c.vt, pageNewProps())
					ns, err := g.Nodes().ByLabel(pageLabel, storepkg.QueryOpts{TxPin: pin})
					wantRow(t, "ByLabel TxPin", onlyNode(t, "ByLabel TxPin", ns, err, f.id), rep.Version(), c.vf, c.vt, pageNewProps())
					ns, err = g.Temporal().NodesAsOf(pin)
					wantRow(t, "NodesAsOf", onlyNode(t, "NodesAsOf", ns, err, f.id), rep.Version(), c.vf, c.vt, pageNewProps())
					wantTimeline(t, "pin at or after", g, f.id, pin, []segment{{c.vf, c.vt, "h-new"}})
					opts := storepkg.QueryOpts{TxPin: pin}
					ns, err = g.Nodes().ByLabelAndProperty(pageLabel, "ov_h3", "h-new", opts)
					wantRow(t, "index h-new", onlyNode(t, "index h-new", ns, err, f.id), rep.Version(), c.vf, c.vt, pageNewProps())
					if ns, err = g.Nodes().ByLabelAndProperty(pageLabel, "ov_h3", "h-old", opts); err != nil || len(ns) != 0 {
						t.Fatalf("index h-old at pin %d: %d rows, err %v", pin, len(ns), err)
					}
				}
				n, err := g.Temporal().NodeAsOf(f.id, f.before)
				if err != nil {
					t.Fatal(err)
				}
				wantRow(t, "NodeAsOf before", n, f.old.Version(), pageOldVF, pageOldVT, f.old.PropertiesMap())
				wantTimeline(t, "pin before", g, f.id, f.before, []segment{{pageOldVF, pageOldVT, "h-old"}})
			})
		})
	}
}

// Repeated corrections on one row: each sees the last row's range, and the
// timeline stays one piece.
func TestReplaceVersionShrunkRepeatedCorrections(t *testing.T) {
	forReplaceVersionBackends(t, func(t *testing.T, g *graphpkg.Graph) {
		f := writePage(t, g)
		for i, step := range []struct {
			vf, vt types.Instant
			left   [][2]types.Instant
		}{
			{1200, 1800, [][2]types.Instant{{1000, 1200}, {1800, 2000}}},
			{1000, 2000, nil},
			{1300, 1700, [][2]types.Instant{{1000, 1300}, {1700, 2000}}},
			{3000, 4000, [][2]types.Instant{{1300, 1700}}},
			{1400, 1600, [][2]types.Instant{{3000, 4000}}},
		} {
			rep, err := exciseThenReplace(g, f.id, step.left, pageNewProps(), step.vf, step.vt)
			if err != nil {
				t.Fatalf("step %d: %v", i, err)
			}
			wantTimeline(t, fmt.Sprintf("step %d", i), g, f.id, rep.Temporal().TxFrom, []segment{{step.vf, step.vt, "h-new"}})
		}
	})
}

// The other order refuses when the replace ended before the old end: nothing
// answers past it any more. The transaction rolls back whole.
func TestReplaceVersionShrunkThenExciseRefusesAndRollsBack(t *testing.T) {
	forReplaceVersionBackends(t, func(t *testing.T, g *graphpkg.Graph) {
		f := writePage(t, g)
		err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
			if _, err := tx.ReplaceNodeVersion(f.id, pageNewProps(), 1200, 1800); err != nil {
				return err
			}
			return tx.ExciseNodeValidity(f.id, 1800, 2000)
		})
		if !errors.Is(err, graphpkg.ErrNoVersionValidAt) {
			t.Fatalf("err %v, want ErrNoVersionValidAt", err)
		}
		cur, err := g.Nodes().Get(context.Background(), f.id)
		if err != nil {
			t.Fatal(err)
		}
		wantRow(t, "after rollback", cur, f.old.Version(), pageOldVF, pageOldVT, f.old.PropertiesMap())
	})
}

// writeOpenPage is writePage without the close: the row is [1000, open).
func writeOpenPage(t *testing.T, g *graphpkg.Graph) pageFixture {
	t.Helper()
	ctx := context.Background()
	for _, k := range []string{"ov_k", "ov_h3"} {
		if err := g.Index().CreateProperty(pageLabel, k); err != nil {
			t.Fatal(err)
		}
	}
	n, err := g.Nodes().Add(ctx, []string{pageLabel}, map[string]any{"ov_k": "k1", "ov_h3": "h-old", "tkg_valid_from": pageOldVF})
	if err != nil {
		t.Fatal(err)
	}
	f := pageFixture{id: n.ID(), old: n}
	if f.before, err = g.Temporal().NowTx(); err != nil {
		t.Fatal(err)
	}
	return f
}

// An open row corrected: closed by the new version, moved, or kept open. The
// tail the new range leaves is excised with an open end.
func TestReplaceVersionOpenRowWithExcise(t *testing.T) {
	const open = types.Instant(0)
	for _, c := range []struct {
		name   string
		vf, vt types.Instant
		left   [][2]types.Instant
	}{
		{"closes at same start", 1000, 1800, [][2]types.Instant{{1800, open}}},
		{"closes at earlier start", 500, 1800, [][2]types.Instant{{1800, open}}},
		{"closes at later start", 1200, 1800, [][2]types.Instant{{1000, 1200}, {1800, open}}},
		{"stays open at later start", 1200, open, [][2]types.Instant{{1000, 1200}}},
		{"stays open at earlier start", 500, open, nil},
		{"moves wholly before", 100, 500, [][2]types.Instant{{1000, open}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			forReplaceVersionBackends(t, func(t *testing.T, g *graphpkg.Graph) {
				f := writeOpenPage(t, g)
				rep, err := exciseThenReplace(g, f.id, c.left, pageNewProps(), c.vf, c.vt)
				if err != nil {
					t.Fatal(err)
				}
				pin := rep.Temporal().TxFrom
				n, err := g.Temporal().NodeAsOf(f.id, pin)
				if err != nil {
					t.Fatal(err)
				}
				wantRow(t, "NodeAsOf", n, rep.Version(), c.vf, c.vt, pageNewProps())
				ns, err := g.Nodes().ByLabel(pageLabel, storepkg.QueryOpts{TxPin: pin})
				wantRow(t, "ByLabel TxPin", onlyNode(t, "ByLabel TxPin", ns, err, f.id), rep.Version(), c.vf, c.vt, pageNewProps())
				ns, err = g.Nodes().ByLabelAndProperty(pageLabel, "ov_h3", "h-new", storepkg.QueryOpts{TxPin: pin})
				wantRow(t, "index h-new", onlyNode(t, "index h-new", ns, err, f.id), rep.Version(), c.vf, c.vt, pageNewProps())
				if ns, err = g.Nodes().ByLabelAndProperty(pageLabel, "ov_h3", "h-old", storepkg.QueryOpts{TxPin: pin}); err != nil || len(ns) != 0 {
					t.Fatalf("index h-old: %d rows, err %v", len(ns), err)
				}
				wantTimeline(t, "pin at", g, f.id, pin, []segment{{c.vf, c.vt, "h-new"}})
				wantTimeline(t, "pin before", g, f.id, f.before, []segment{{pageOldVF, open, "h-old"}})
			})
		})
	}
	// Sigma closes its open run in place: tkg_valid_to through UpdateNode in a
	// transaction, keeping the row's stamp.
	closeInPlace := func(tx *graphpkg.GraphTx, id types.NodeID, vt types.Instant) error {
		_, err := tx.UpdateNode(id, map[string]any{"tkg_valid_to": vt})
		return err
	}
	t.Run("closed in place then shrunk in one tx", func(t *testing.T) {
		forReplaceVersionBackends(t, func(t *testing.T, g *graphpkg.Graph) {
			f := writeOpenPage(t, g)
			var rep *types.Node
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				if err := closeInPlace(tx, f.id, pageOldVT); err != nil {
					return err
				}
				if err := tx.ExciseNodeValidity(f.id, 1800, pageOldVT); err != nil {
					return err
				}
				var err error
				rep, err = tx.ReplaceNodeVersion(f.id, pageNewProps(), 1000, 1800)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			wantTimeline(t, "pin at", g, f.id, rep.Temporal().TxFrom, []segment{{1000, 1800, "h-new"}})
		})
	})
	// After a replace with an earlier start, a close in place shortens the new
	// version and the older belief answers past it on the timeline (ADR-0014).
	// An excise of the tail alone closes the timeline, not the record doors'
	// row. Excise and replace again closes both.
	t.Run("replaced open at earlier start then closed", func(t *testing.T) {
		for _, c := range []struct {
			name  string
			close func(tx *graphpkg.GraphTx, id types.NodeID) error
			want  []segment
			rowVT types.Instant
		}{
			{"in place", func(tx *graphpkg.GraphTx, id types.NodeID) error { return closeInPlace(tx, id, 1800) },
				[]segment{{500, 1800, "h-new"}, {1800, open, "h-old"}}, 1800},
			{"by excise", func(tx *graphpkg.GraphTx, id types.NodeID) error { return tx.ExciseNodeValidity(id, 1800, open) },
				[]segment{{500, 1800, "h-new"}}, open},
			{"by excise and replace", func(tx *graphpkg.GraphTx, id types.NodeID) error {
				if err := tx.ExciseNodeValidity(id, 1800, open); err != nil {
					return err
				}
				_, err := tx.ReplaceNodeVersion(id, pageNewProps(), 500, 1800)
				return err
			}, []segment{{500, 1800, "h-new"}}, 1800},
		} {
			t.Run(c.name, func(t *testing.T) {
				forReplaceVersionBackends(t, func(t *testing.T, g *graphpkg.Graph) {
					f := writeOpenPage(t, g)
					if _, err := exciseThenReplace(g, f.id, nil, pageNewProps(), 500, open); err != nil {
						t.Fatal(err)
					}
					if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error { return c.close(tx, f.id) }); err != nil {
						t.Fatal(err)
					}
					pin, err := g.Temporal().NowTx()
					if err != nil {
						t.Fatal(err)
					}
					wantTimeline(t, "pin after the close", g, f.id, pin, c.want)
					n, err := g.Temporal().NodeAsOf(f.id, pin)
					if err != nil {
						t.Fatal(err)
					}
					tm := n.Temporal()
					if tm.ValidFrom != 500 || tm.ValidTo != c.rowVT || n.PropertiesMap()["ov_h3"] != "h-new" {
						t.Fatalf("NodeAsOf [%d, %d) %v, want the new version [500, %d)", tm.ValidFrom, tm.ValidTo, n.PropertiesMap(), c.rowVT)
					}
				})
			})
		}
	})
	// An open row closed by CloseVersion first, then shrunk: the excise sees
	// the closed range.
	t.Run("closed then shrunk", func(t *testing.T) {
		forReplaceVersionBackends(t, func(t *testing.T, g *graphpkg.Graph) {
			f := writeOpenPage(t, g)
			if err := g.Nodes().CloseVersion(context.Background(), f.id, pageOldVT); err != nil {
				t.Fatal(err)
			}
			rep, err := exciseThenReplace(g, f.id, [][2]types.Instant{{1800, pageOldVT}}, pageNewProps(), 1000, 1800)
			if err != nil {
				t.Fatal(err)
			}
			wantTimeline(t, "pin at", g, f.id, rep.Temporal().TxFrom, []segment{{1000, 1800, "h-new"}})
		})
	})
}
