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
// reference shard, E on event shards); badger and tiered keep D's and E's
// property indexes on disk, those of keys starting with "ov_h" bounding their
// nodes' valid ends (VALIDENDPRUNE).
func validEndStores(t *testing.T) map[string]*Core {
	t.Helper()
	listing := []storepkg.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_h"}, {Label: "E", KeyPrefix: "ov_h"}}
	onDisk := []string{"D", "E"}
	out := map[string]*Core{}
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	out["memory"] = g
	g, err = New(Config{BadgerInMemory: true, PropertyIndexOnDiskLabels: onDisk, PropertyIndexValidEnd: listing})
	if err != nil {
		t.Fatal(err)
	}
	out["badger"] = g
	ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), ShardWindow: time.Hour, RefLabels: []string{"D"},
		PropertyIndexOnDiskLabels: onDisk, PropertyIndexValidEnd: listing})
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
				if len(nodes) > 3 {
					t.Errorf("%s %s: a page of %d nodes, limit 3", name, step, len(nodes))
				}
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
		for i, id := range ids { // a quarter ends before the bound, at it, after it; a quarter stays open
			if end := []types.Instant{2000, 6000, bound, 0}[i%4]; end != 0 {
				if err := g.Nodes.CloseVersion(ctx, id, end); err != nil {
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
		st, err := g.Index.PropertyIndexValidEndStats()
		if name == "memory" {
			if !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
				t.Errorf("memory: stats %v, want ErrCapabilityNotSupported", err)
			}
		} else if err != nil || st.Pruned == 0 || st.Refined == 0 {
			t.Errorf("%s: %+v, %v: want entries pruned after the reads refined them", name, st, err)
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
		// ids[3] and ids[7] are open: an update of another key, of the value.
		if _, err := g.Nodes.Update(ctx, ids[3], map[string]any{"other": int64(100)}); err != nil {
			t.Fatal(err)
		}
		check("other key updated")
		if _, err := g.Nodes.Update(ctx, ids[7], map[string]any{"ov_h1": "v1"}); err != nil {
			t.Fatal(err)
		}
		check("value updated")
		// E is listed too; on tiered its nodes live on event shards: read,
		// never refined into the reference shard.
		if err := g.Index.CreateProperty("E", "ov_h1"); err != nil {
			t.Fatal(err)
		}
		for range 4 {
			n, err := g.Nodes.Add(ctx, []string{"E"}, map[string]any{"ov_h1": "e", "tkg_valid_from": types.Instant(1000)})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes.CloseVersion(ctx, n.ID(), 2000); err != nil {
				t.Fatal(err)
			}
		}
		eBefore, err := g.Index.PropertyIndexValidEndStats()
		pruning := err == nil
		for range 2 {
			got, err := g.Nodes.ByLabelAndProperty("E", "ov_h1", "e", storepkg.QueryOpts{ValidEndAfter: bound, ValidEndAfterSet: true})
			if err != nil || len(got) != 0 {
				t.Errorf("%s: E ended at 2000: %d nodes, %v", name, len(got), err)
			}
		}
		if pruning {
			eAfter, _ := g.Index.PropertyIndexValidEndStats()
			refined := eAfter.Refined - eBefore.Refined
			if want := map[string]uint64{"badger": 4, "tiered": 0}[name]; refined != want {
				t.Errorf("%s: E's nodes refined %d times, want %d", name, refined, want)
			}
		}
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
		st, err := g.Index.PropertyIndexValidEndStats()
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	closeGraph := func(g *Core) {
		t.Helper()
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
	}
	g := open(false) // built before the listing: the open that lists it raises the marker
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
	closeGraph(g)
	if v := rawFormatMarker(t, dir); v >= storeutil.ValidEndStoreFormatVersion {
		t.Errorf("never listed: format marker %d, want below %d", v, storeutil.ValidEndStoreFormatVersion)
	}

	g = open(true)
	probe(g, "listed")
	if st := probe(g, "listed, read again"); st.Pruned == 0 {
		t.Errorf("listed: %+v, nothing pruned", st)
	}
	closeGraph(g)
	if v := rawFormatMarker(t, dir); v < storeutil.ValidEndStoreFormatVersion {
		t.Errorf("format marker %d, want %d", v, storeutil.ValidEndStoreFormatVersion)
	}

	for _, step := range []string{"reopened", "reopened again"} {
		g = open(true)
		if st := probe(g, step); st.Pruned != 6 || st.UnknownRead != 0 || st.Refined != 0 {
			t.Errorf("%s: %+v, want 6 pruned from their entries, no bound unknown, none refined", step, st)
		}
		closeGraph(g)
	}

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
	if ok, err := g.Index.PropertyIndexValidEndListed("D", "ov_h1"); err != nil || !ok {
		t.Errorf("read-only over a marked definition: listed %v, %v, want true", ok, err)
	}
	if st := probe(g, "read-only, read again"); st.Refined != 0 || st.Pruned == 0 {
		t.Errorf("read-only: %+v, want pruned from the bounds the last open refined, none refined", st)
	}
	_ = g.Close() // a graph over a read-only store cannot save its registries

	closeGraph(open(false)) // unmarks D's definition
	bs, err = badger.New(badger.Config{Dir: dir, ReadOnly: true, PropertyIndexOnDiskLabels: []string{"D"}, PropertyIndexValidEnd: listing})
	if err != nil {
		t.Fatal(err)
	}
	g, err = New(Config{Store: bs})
	if err != nil {
		_ = bs.Close()
		t.Fatal(err)
	}
	if st := probe(g, "read-only after an unlisted open"); st.Pruned != 0 {
		t.Errorf("read-only after an unlisted open: %+v, want nothing pruned (the bounds are not trusted)", st)
	}
	if ok, err := g.Index.PropertyIndexValidEndListed("D", "ov_h1"); err != nil || ok {
		t.Errorf("read-only after an unlisted open: listed %v, %v, want false", ok, err)
	}
	_ = g.Close()
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

// TestTieredValidEndStoreWide (VALIDENDPRUNE): with every property index on
// disk (PropertyIndexOnDisk), not a label list, tiered still names the listed
// labels when its shards open, so the bounds a read refined prune after a
// reopen.
func TestTieredValidEndStoreWide(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	open := func() *Core {
		t.Helper()
		ts, err := tiered.New(tiered.Config{DataDir: dir, ShardWindow: time.Hour, RefLabels: []string{"D"}, PropertyIndexOnDisk: true,
			PropertyIndexValidEnd: []storepkg.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_h"}}})
		if err != nil {
			t.Fatal(err)
		}
		g, err := New(Config{Store: ts})
		if err != nil {
			_ = ts.Close()
			t.Fatal(err)
		}
		return g
	}
	const bound = types.Instant(5000)
	probe := func(g *Core) storepkg.PropertyIndexValidEndStats {
		t.Helper()
		want := validEndOracle(t, g, []any{"v0"}, bound)
		got, err := g.Nodes.ByLabelAndPropertyValues("D", "ov_h1", []any{"v0"}, storepkg.QueryOpts{ValidEndAfter: bound, ValidEndAfterSet: true})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(nodeIDs(got), want) {
			t.Errorf("got %v, want %v", nodeIDs(got), want)
		}
		st, err := g.Index.PropertyIndexValidEndStats()
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	g := open()
	if err := g.Index.CreateProperty("D", "ov_h1"); err != nil {
		t.Fatal(err)
	}
	for i := range 8 {
		n, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"ov_h1": "v0", "tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			if err := g.Nodes.CloseVersion(ctx, n.ID(), 2000); err != nil {
				t.Fatal(err)
			}
		}
	}
	probe(g)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g = open()
	defer func() { _ = g.Close() }()
	if st := probe(g); st.Pruned != 4 || st.UnknownRead != 0 {
		t.Errorf("reopened: %+v, want the 4 closed nodes pruned from the bounds the last open refined", st)
	}
}

// TestTieredValidEndEventShardNode (VALIDENDPRUNE): a node of a listed label
// on an event shard (its primary label E, an event label) is read whole, and
// its end is not sent to the reference shard, which does not hold it (there
// its zero probe would count as a skipped refine).
func TestTieredValidEndEventShardNode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), ShardWindow: time.Hour, RefLabels: []string{"D"},
		PropertyIndexOnDiskLabels: []string{"D", "E"}, PropertyIndexValidEnd: []storepkg.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_h"}}})
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Config{Store: ts})
	if err != nil {
		_ = ts.Close()
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	if err := g.Index.CreateProperty("D", "ov_h1"); err != nil {
		t.Fatal(err)
	}
	for _, labels := range [][]string{{"D"}, {"D"}, {"E", "D"}} {
		n, err := g.Nodes.Add(ctx, labels, map[string]any{"ov_h1": "v0", "tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Nodes.CloseVersion(ctx, n.ID(), 2000); err != nil {
			t.Fatal(err)
		}
	}
	if all, err := g.Nodes.ByLabelAndProperty("D", "ov_h1", "v0", storepkg.QueryOpts{}); err != nil || len(all) != 3 {
		t.Fatalf("D's matches: %d, %v, want 3 (one on an event shard)", len(all), err)
	}
	for range 2 {
		got, err := g.Nodes.ByLabelAndProperty("D", "ov_h1", "v0", storepkg.QueryOpts{ValidEndAfter: 5000, ValidEndAfterSet: true})
		if err != nil || len(got) != 0 {
			t.Fatalf("%d nodes, %v: want none (all ended at 2000)", len(got), err)
		}
	}
	want := storepkg.PropertyIndexValidEndStats{Pruned: 2, UnknownRead: 2, Refined: 2}
	if st, err := g.Index.PropertyIndexValidEndStats(); err != nil || st != want {
		t.Errorf("%+v, %v: want %+v (the 2 reference nodes read, refined, then pruned; the event shard's node never refined there)", st, err, want)
	}
}

// TestPropertyIndexValidEndListed (VALIDENDLISTED): the check answers true
// exactly where a ValidEndAfter lookup prunes from the index entries: a listed
// definition on disk whose bounds are trusted; on tiered, a reference label's.
// Everywhere else false, without an error.
func TestPropertyIndexValidEndListed(t *testing.T) {
	t.Parallel()
	listing := []storepkg.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_h"}, {Label: "E", KeyPrefix: "ov_h"}, {Label: "R", KeyPrefix: "ov_h"}}
	onDisk := []string{"D", "E"} // R's indexes stay in RAM
	open := map[string]func() *Core{
		"memory": func() *Core {
			g, err := New(Config{Store: memory.New()})
			if err != nil {
				t.Fatal(err)
			}
			return g
		},
		"badger": func() *Core {
			g, err := New(Config{BadgerInMemory: true, PropertyIndexOnDiskLabels: onDisk, PropertyIndexValidEnd: listing})
			if err != nil {
				t.Fatal(err)
			}
			return g
		},
		"badger unlisted": func() *Core {
			g, err := New(Config{BadgerInMemory: true, PropertyIndexOnDiskLabels: onDisk})
			if err != nil {
				t.Fatal(err)
			}
			return g
		},
		"tiered": func() *Core { // E is an event label
			ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), ShardWindow: time.Hour, RefLabels: []string{"D", "R"},
				PropertyIndexOnDiskLabels: onDisk, PropertyIndexValidEnd: listing})
			if err != nil {
				t.Fatal(err)
			}
			if ts.PropertyIndexValidEndListed(1, "ov_h1") { // no label names before a graph installs them
				t.Error("tiered without label names: listed")
			}
			g, err := New(Config{Store: ts})
			if err != nil {
				_ = ts.Close()
				t.Fatal(err)
			}
			return g
		},
	}
	for name, mk := range open {
		g := mk()
		t.Cleanup(func() { _ = g.Close() })
		listed := func(label, key string) bool {
			t.Helper()
			ok, err := g.Index.PropertyIndexValidEndListed(label, key)
			if err != nil {
				t.Fatalf("%s %s/%s: %v", name, label, key, err)
			}
			return ok
		}
		if listed("D", "ov_h1") || listed("Nope", "ov_h1") {
			t.Errorf("%s: listed before any index or label exists", name)
		}
		for _, def := range [][2]string{{"D", "ov_h1"}, {"D", "other"}, {"E", "ov_h1"}, {"R", "ov_h1"}} {
			if err := g.Index.CreateProperty(def[0], def[1]); err != nil {
				t.Fatal(err)
			}
		}
		want := map[string]bool{"badger": true, "tiered": true}[name]
		if got := listed("D", "ov_h1"); got != want {
			t.Errorf("%s: D/ov_h1 listed %v, want %v", name, got, want)
		}
		if got := listed("E", "ov_h1"); got != (name == "badger") {
			t.Errorf("%s: E/ov_h1 listed %v, want %v (on tiered E's nodes live on event shards)", name, got, name == "badger")
		}
		if listed("D", "other") || listed("R", "ov_h1") {
			t.Errorf("%s: a key not listed, or a listed label's index in RAM, reported listed", name)
		}
		if err := g.Index.DeleteProperty("D", "ov_h1"); err != nil {
			t.Fatal(err)
		}
		if listed("D", "ov_h1") {
			t.Errorf("%s: dropped D/ov_h1 still listed", name)
		}
		if _, err := g.Index.PropertyIndexValidEndListed("", "ov_h1"); err == nil {
			t.Errorf("%s: empty label: no error", name)
		}
		if _, err := g.Index.PropertyIndexValidEndListed("D", "tkg_valid_from"); !errors.Is(err, types.ErrReservedPrefix) {
			t.Errorf("%s: reserved key: %v, want ErrReservedPrefix", name, err)
		}
	}
}
