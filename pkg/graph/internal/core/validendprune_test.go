package core

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	badgerv4 "github.com/dgraph-io/badger/v4"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// validEndStores opens a graph on memory, badger and tiered (label D on the
// reference shard); badger and tiered keep D's property indexes on disk, those
// of keys starting with "ov_h" bounding their nodes' valid ends (VALIDENDPRUNE).
func validEndStores(t *testing.T) map[string]*Core {
	t.Helper()
	listing := []storepkg.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_h"}}
	out := map[string]*Core{}
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	out["memory"] = g
	g, err = New(Config{BadgerInMemory: true, PropertyIndexOnDiskLabels: []string{"D"}, PropertyIndexValidEnd: listing})
	if err != nil {
		t.Fatal(err)
	}
	out["badger"] = g
	ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), ShardWindow: time.Hour, RefLabels: []string{"D"},
		PropertyIndexOnDiskLabels: []string{"D"}, PropertyIndexValidEnd: listing})
	if err != nil {
		t.Fatal(err)
	}
	g, err = New(Config{Store: ts})
	if err != nil {
		_ = ts.Close()
		t.Fatal(err)
	}
	out["tiered"] = g
	for _, g := range out {
		t.Cleanup(func() { _ = g.Close() })
	}
	return out
}

// validEndOracle is the answer ValidEndAfter must give: the current matches
// whose effective timeline has a segment ending at or after bound, or an open
// one.
func validEndOracle(t *testing.T, g *Core, values []any, bound types.Instant) []types.NodeID {
	t.Helper()
	all, err := g.Nodes.ByLabelAndPropertyValues("D", "ov_h1", values, storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	var want []types.NodeID
	for _, n := range all {
		segs, err := g.Temporal.NodeEffectiveTimeline(n.ID(), pin)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range segs {
			if s.ValidTo == 0 || s.ValidTo >= bound {
				want = append(want, n.ID())
				break
			}
		}
	}
	return want
}

// TestValidEndAfter (VALIDENDPRUNE): a current property lookup with
// ValidEndAfter returns exactly the matches with a segment ending at or after
// the bound, or an open one, on every store, paged or not, as the timelines
// change: closes, an interval that extends a closed node, an excise that cuts
// an open one, updates. A listed on-disk definition prunes the nodes it bounded
// below the bound from its entries once a read resolved them, and the read
// writes no row, stamp or change record.
func TestValidEndAfter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, g := range validEndStores(t) {
		for _, k := range []string{"ov_h1", "other"} {
			if err := g.Index.CreateProperty("D", k); err != nil {
				t.Fatal(err)
			}
		}
		var ids []types.NodeID
		for i := range 24 {
			n, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"ov_h1": fmt.Sprint("v", i%3), "other": int64(i),
				"tkg_valid_from": types.Instant(1000)})
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, n.ID())
		}
		const bound = types.Instant(5000)
		values := []any{"v0", "v1"}
		check := func(step string) {
			t.Helper()
			want := validEndOracle(t, g, values, bound)
			opts := storepkg.QueryOpts{ValidEndAfter: bound, ValidEndAfterSet: true}
			got, err := g.Nodes.ByLabelAndPropertyValues("D", "ov_h1", values, opts)
			if err != nil {
				t.Fatalf("%s %s: %v", name, step, err)
			}
			if !slices.Equal(nodeIDs(got), want) {
				t.Errorf("%s %s: got %v, want %v", name, step, nodeIDs(got), want)
			}
			var paged []types.NodeID
			for page := opts; ; {
				page.Limit = 3
				nodes, err := g.Nodes.ByLabelAndPropertyValues("D", "ov_h1", values, page)
				if err != nil {
					t.Fatal(err)
				}
				paged = append(paged, nodeIDs(nodes)...)
				if len(nodes) < 3 {
					break
				}
				page.After = types.EntityID(nodes[len(nodes)-1].ID())
			}
			if !slices.Equal(paged, want) {
				t.Errorf("%s %s: paged %v, want %v", name, step, paged, want)
			}
			one, err := g.Nodes.ByLabelAndProperty("D", "ov_h1", "v0", opts)
			if err != nil {
				t.Fatal(err)
			}
			wantOne := validEndOracle(t, g, []any{"v0"}, bound)
			if !slices.Equal(nodeIDs(one), wantOne) {
				t.Errorf("%s %s: one value %v, want %v", name, step, nodeIDs(one), wantOne)
			}
		}
		check("all open")
		for i, id := range ids { // a third ends before the bound, a third after
			switch i % 4 {
			case 0:
				if err := g.Nodes.CloseVersion(ctx, id, 2000); err != nil {
					t.Fatal(err)
				}
			case 1:
				if err := g.Nodes.CloseVersion(ctx, id, 6000); err != nil {
					t.Fatal(err)
				}
			}
		}
		check("closed")
		lsn, _ := g.Repl.LastCommittedLSN()
		version := func(id types.NodeID) uint32 {
			n, err := g.Nodes.Get(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			return n.Version()
		}
		before := version(ids[0])
		check("closed, read again")
		if after, _ := g.Repl.LastCommittedLSN(); after != lsn || version(ids[0]) != before {
			t.Errorf("%s: a read moved the change log (%d to %d) or a version (%d to %d)", name, lsn, after, before, version(ids[0]))
		}
		if pr, ok := g.store.(storepkg.PropertyIndexValidEndCapability); name != "memory" {
			if !ok {
				t.Fatalf("%s: no PropertyIndexValidEndCapability", name)
			}
			if st := pr.PropertyIndexValidEndStats(); st.Pruned == 0 || st.Refined == 0 {
				t.Errorf("%s: %+v, want entries pruned after the reads refined them", name, st)
			}
		}
		// ids[0] ended at 2000: an interval to 8000 extends it past the bound.
		if _, err := g.Temporal.SetNodeVersionInterval(ctx, ids[0], 2000, 8000, nil); err != nil {
			t.Fatal(err)
		}
		check("extended")
		// ids[1] ends at 6000: an excise from 4000 cuts it below the bound.
		if err := g.Temporal.ExciseNodeValidity(ctx, ids[1], 4000, 0); err != nil {
			t.Fatal(err)
		}
		check("excised")
		// ids[2] and ids[6] are open: an update of another key, of the value.
		if _, err := g.Nodes.Update(ctx, ids[2], map[string]any{"other": int64(100)}); err != nil {
			t.Fatal(err)
		}
		check("other key updated")
		if _, err := g.Nodes.Update(ctx, ids[6], map[string]any{"ov_h1": "v1"}); err != nil {
			t.Fatal(err)
		}
		check("value updated")
		if _, err := g.Nodes.ByLabelAndProperty("D", "ov_h1", "v0", storepkg.QueryOpts{ValidEndAfter: bound, ValidEndAfterSet: true, TxPin: 1}); !errors.Is(err, storepkg.ErrValidEndAfterUnsupported) {
			t.Errorf("%s: ValidEndAfter with a pin: %v, want ErrValidEndAfterUnsupported", name, err)
		}
		if _, err := g.Nodes.ByLabel("D", storepkg.QueryOpts{ValidEndAfter: bound}); !errors.Is(err, storepkg.ErrValidEndAfterUnsupported) {
			t.Errorf("%s: ByLabel with ValidEndAfter: %v, want ErrValidEndAfterUnsupported", name, err)
		}
	}
}

// TestValidEndAfterAcrossOpens (VALIDENDPRUNE): bounds persist and are trusted
// at the next open (pruned before any row is read), the first raises the
// format marker to 14; an open without the listing keeps nothing trusted, and
// the next listing resets what that open did not maintain, so a validity it
// extended is not pruned; a read-only open prunes and refines nothing.
func TestValidEndAfterAcrossOpens(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	listing := []storepkg.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_h"}}
	open := func(listed bool) *Core {
		t.Helper()
		cfg := Config{BadgerDir: dir, PropertyIndexOnDiskLabels: []string{"D"}}
		if listed {
			cfg.PropertyIndexValidEnd = listing
		}
		g, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	const bound = types.Instant(5000)
	probe := func(g *Core, step string) storepkg.PropertyIndexValidEndStats {
		t.Helper()
		want := validEndOracle(t, g, []any{"v0"}, bound)
		got, err := g.Nodes.ByLabelAndPropertyValues("D", "ov_h1", []any{"v0"}, storepkg.QueryOpts{ValidEndAfter: bound, ValidEndAfterSet: true})
		if err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		if !slices.Equal(nodeIDs(got), want) {
			t.Errorf("%s: got %v, want %v", step, nodeIDs(got), want)
		}
		return g.store.(storepkg.PropertyIndexValidEndCapability).PropertyIndexValidEndStats()
	}
	closeGraph := func(g *Core) {
		t.Helper()
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
	}
	g := open(true)
	if err := g.Index.CreateProperty("D", "ov_h1"); err != nil {
		t.Fatal(err)
	}
	var ids []types.NodeID
	for i := range 12 {
		n, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"ov_h1": "v0", "tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, n.ID())
		if i%2 == 0 {
			if err := g.Nodes.CloseVersion(ctx, n.ID(), 2000); err != nil {
				t.Fatal(err)
			}
		}
	}
	probe(g, "listed")
	if st := probe(g, "listed, read again"); st.Pruned == 0 {
		t.Errorf("listed: %+v, nothing pruned", st)
	}
	closeGraph(g)
	if v := rawFormatMarker(t, dir); v < storeutil.ValidEndStoreFormatVersion {
		t.Errorf("format marker %d, want %d", v, storeutil.ValidEndStoreFormatVersion)
	}

	g = open(true)
	if st := probe(g, "reopened"); st.Pruned != 6 || st.UnknownRead != 0 {
		t.Errorf("reopened: %+v, want 6 pruned from their entries and no bound unknown", st)
	}
	closeGraph(g)

	g = open(false) // ids[0] ended at 2000: extended past the bound while nothing is listed
	if _, err := g.Temporal.SetNodeVersionInterval(ctx, ids[0], 2000, 8000, nil); err != nil {
		t.Fatal(err)
	}
	probe(g, "unlisted")
	closeGraph(g)

	g = open(true)
	if st := probe(g, "listed again"); st.Pruned != 0 || st.UnknownRead != 12 {
		t.Errorf("listed again: %+v, want every bound reset by the open", st)
	}
	closeGraph(g)

	bs, err := badger.New(badger.Config{Dir: dir, ReadOnly: true, PropertyIndexOnDiskLabels: []string{"D"}, PropertyIndexValidEnd: listing})
	if err != nil {
		t.Fatal(err)
	}
	g, err = New(Config{Store: bs})
	if err != nil {
		_ = bs.Close()
		t.Fatal(err)
	}
	probe(g, "read-only")
	if st := probe(g, "read-only, read again"); st.Refined != 0 || st.Pruned == 0 {
		t.Errorf("read-only: %+v, want pruned from the bounds the last open refined, none refined", st)
	}
	_ = g.Close() // a graph over a read-only store cannot save its registries
}

// rawFormatMarker reads dir's store-level format marker (0: none).
func rawFormatMarker(t *testing.T, dir string) uint16 {
	t.Helper()
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var v uint16
	err = db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storeutil.WireFormatVersionKey)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) == 2 {
				v = binary.BigEndian.Uint16(val)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TestValidEndAfterConcurrent (VALIDENDPRUNE): readers probing in parallel,
// refining the same entries, while a writer extends closed nodes past the
// bound: no refine computed before an extension survives it, so every
// extended node is read at the end. Run under -race.
func TestValidEndAfterConcurrent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, g := range validEndStores(t) {
		if err := g.Index.CreateProperty("D", "ov_h1"); err != nil {
			t.Fatal(err)
		}
		var closed []types.NodeID
		for i := range 40 {
			n, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"ov_h1": "v0", "tkg_valid_from": types.Instant(1000)})
			if err != nil {
				t.Fatal(err)
			}
			if i%2 == 0 {
				if err := g.Nodes.CloseVersion(ctx, n.ID(), 2000); err != nil {
					t.Fatal(err)
				}
				closed = append(closed, n.ID())
			}
		}
		const bound = types.Instant(5000)
		opts := storepkg.QueryOpts{ValidEndAfter: bound, ValidEndAfterSet: true}
		done := make(chan struct{})
		errs := make(chan error, 4)
		for range 4 {
			go func() {
				for {
					select {
					case <-done:
						errs <- nil
						return
					default:
					}
					if _, err := g.Nodes.ByLabelAndPropertyValues("D", "ov_h1", []any{"v0"}, opts); err != nil {
						errs <- err
						return
					}
				}
			}()
		}
		for _, id := range closed {
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, id, 2000, 9000, nil); err != nil {
				t.Fatal(err)
			}
		}
		close(done)
		for range 4 {
			if err := <-errs; err != nil {
				t.Fatalf("%s: reader: %v", name, err)
			}
		}
		got, err := g.Nodes.ByLabelAndPropertyValues("D", "ov_h1", []any{"v0"}, opts)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 40 {
			t.Errorf("%s: %d nodes after every closed one was extended past the bound, want 40", name, len(got))
		}
	}
}
