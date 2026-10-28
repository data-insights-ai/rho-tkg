package core

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// idxStatsStores opens a graph on each in-tree backend: memory, badger,
// sharded and tiered; badger and tiered keep label E and relationship type S on
// disk.
func idxStatsStores(t *testing.T) map[string]*Core {
	t.Helper()
	out := map[string]*Core{}
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	out["memory"] = g
	g, err = New(Config{BadgerInMemory: true, PropertyIndexOnDiskLabels: []string{"E"}, RelPropertyIndexOnDiskTypes: []string{"S"}})
	if err != nil {
		t.Fatal(err)
	}
	out["badger"] = g
	ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), ShardWindow: time.Hour,
		PropertyIndexOnDiskLabels: []string{"E"}, RelPropertyIndexOnDiskTypes: []string{"S"}})
	if err != nil {
		t.Fatal(err)
	}
	g, err = New(Config{Store: ts})
	if err != nil {
		_ = ts.Close()
		t.Fatal(err)
	}
	out["tiered"] = g
	ss, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	g, err = New(Config{Store: ss})
	if err != nil {
		_ = ss.Close()
		t.Fatal(err)
	}
	out["sharded"] = g
	for _, g := range out {
		t.Cleanup(func() { _ = g.Close() })
	}
	return out
}

// TestPropertyIndexStats (IDXSTATS): every in-tree backend reports what each
// declared property index holds in RAM: its distinct values (a composite's
// tuples), postings, its membership sidecar's postings once a pinned read
// built it, an estimate of the bytes, nothing for one kept on disk; the rows
// named, in one order, per shard on tiered, summed per label or type.
func TestPropertyIndexStats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, g := range idxStatsStores(t) {
		if _, ok := g.store.(storepkg.PropertyIndexStatsCapability); !ok {
			t.Fatalf("%s: no PropertyIndexStatsCapability", name)
		}
		for _, l := range []string{"D", "E"} {
			if err := g.Index.CreateProperty(l, "k"); err != nil {
				t.Fatal(err)
			}
		}
		if err := g.Index.CreateComposite("D", []string{"k", "n"}); err != nil {
			t.Fatal(err)
		}
		for _, typ := range []string{"R", "S"} {
			if err := g.Index.CreateRelProperty(typ, "k"); err != nil {
				t.Fatal(err)
			}
		}
		var ds []*types.Node
		for i := range 30 { // 3 values of k, 6 tuples of (k, n)
			n, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"k": fmt.Sprint("v", i%3), "n": int64(i % 2)})
			if err != nil {
				t.Fatal(err)
			}
			ds = append(ds, n)
		}
		for i := range 10 {
			if _, err := g.Nodes.Add(ctx, []string{"E"}, map[string]any{"k": fmt.Sprint("e", i)}); err != nil {
				t.Fatal(err)
			}
		}
		for i := range 20 {
			if _, err := g.Rels.Add(ctx, "R", ds[i], ds[i+1], map[string]any{"k": fmt.Sprint("r", i%4)}); err != nil {
				t.Fatal(err)
			}
		}
		for i := range 5 {
			if _, err := g.Rels.Add(ctx, "S", ds[i], ds[i+1], map[string]any{"k": fmt.Sprint("s", i)}); err != nil {
				t.Fatal(err)
			}
		}
		unbuilt, err := g.Index.PropertyIndexStats()
		if err != nil {
			t.Fatal(err)
		}
		pin, err := g.Temporal.NowTx()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes.ByLabelAndProperty("D", "k", "v0", storepkg.QueryOpts{TxPin: pin}); err != nil { // builds D's sidecar
			t.Fatal(err)
		}
		if _, err := g.Rels.ByTypeAndProperty("R", "k", "r0", storepkg.QueryOpts{TxPin: pin}); err != nil { // builds R's
			t.Fatal(err)
		}
		stats, err := g.Index.PropertyIndexStats()
		if err != nil {
			t.Fatal(err)
		}
		bytesOf := func(stats []storepkg.PropertyIndexStat, label, relType string) (b int64) {
			for _, st := range stats {
				if st.Label == label && st.RelType == relType && len(st.Keys) == 1 {
					b += st.RAMBytes
				}
			}
			return b
		}
		for _, sc := range [][2]string{{"D", ""}, {"", "R"}} {
			if bytesOf(stats, sc[0], sc[1]) <= bytesOf(unbuilt, sc[0], sc[1]) {
				t.Errorf("%s %v: %d bytes with the sidecar built, %d without", name, sc, bytesOf(stats, sc[0], sc[1]), bytesOf(unbuilt, sc[0], sc[1]))
			}
		}
		type sum struct {
			values, postings, members, bytes int64
			onDisk, composite                bool
		}
		got := map[string]sum{}
		var order []string
		for _, st := range stats {
			if (st.Shard != "") != (name == "tiered" || name == "sharded") {
				t.Errorf("%s: row %+v: Shard %q", name, st, st.Shard)
			}
			k := keyOf(st)
			if len(order) == 0 || order[len(order)-1] != k {
				order = append(order, k)
			}
			s := got[k]
			s.values += st.Values
			s.postings += st.Postings
			s.members += st.MemberPostings
			s.bytes += st.RAMBytes
			s.onDisk = s.onDisk || st.OnDisk
			s.composite = s.composite || st.Composite
			got[k] = s
		}
		disk := name == "badger" || name == "tiered"
		want := map[string]sum{
			"D k":     {values: 3, postings: 30, members: 30},
			"D k,n":   {values: 6, postings: 30, composite: true},
			"E k":     {values: 10, postings: 10},
			"rel R k": {values: 4, postings: 20, members: 20},
			"rel S k": {values: 5, postings: 5},
		}
		for _, k := range []string{"E k", "rel S k"} {
			if disk {
				want[k] = sum{onDisk: true}
			}
		}
		if name == "sharded" { // a badger pass builds every sidecar of its kind in RAM: E's with D's, S's with R's
			want["E k"] = sum{values: 10, postings: 10, members: 10}
			want["rel S k"] = sum{values: 5, postings: 5, members: 5}
		}
		for k, w := range want {
			g := got[k]
			if name == "sharded" {
				g.values = w.values // per slot: a value on both slots counts twice
			}
			if g.values != w.values || g.postings != w.postings || g.members != w.members || g.onDisk != w.onDisk || g.composite != w.composite {
				t.Errorf("%s %s: %+v, want %+v", name, k, g, w)
			}
			if (g.bytes > 0) != (w.postings > 0) {
				t.Errorf("%s %s: %d bytes for %d postings", name, k, g.bytes, w.postings)
			}
		}
		if len(got) != len(want) {
			t.Errorf("%s: rows %v, want %d definitions", name, got, len(want))
		}
		if (name == "memory" || name == "badger") && fmt.Sprint(order) != "[D k D k,n E k rel R k rel S k]" {
			t.Errorf("%s: order %v, want nodes by label (single key, then composite), then relationships", name, order)
		}
		rank := map[string]int{"D k": 1, "D k,n": 2, "E k": 3, "rel R k": 4, "rel S k": 5}
		for i := 1; i < len(stats); i++ { // shards in name order, each shard's rows in that one
			a, b := stats[i-1], stats[i]
			if a.Shard > b.Shard || (a.Shard == b.Shard && rank[keyOf(a)] >= rank[keyOf(b)]) {
				t.Errorf("%s: row %d (%s %q) after (%s %q)", name, i, keyOf(b), b.Shard, keyOf(a), a.Shard)
			}
		}
		var scopes []string
		var dBytes int64
		for _, st := range stats {
			if st.Label == "D" {
				dBytes += st.RAMBytes
			}
		}
		for _, sc := range storepkg.PropertyIndexStatsByScope(stats) {
			if sc.Label == "D" && sc.RAMBytes != dBytes {
				t.Errorf("%s: D summed %d bytes, its rows %d", name, sc.RAMBytes, dBytes)
			}
			scopes = append(scopes, sc.Label+sc.RelType)
			if sc.Keys != nil || sc.Shard != "" {
				t.Errorf("%s: scope row %+v names keys or a shard", name, sc)
			}
			if sc.Label == "D" && ((sc.Values != 9 && name != "sharded") || sc.Postings != 60 || sc.MemberPostings != 30 || !sc.Composite || sc.OnDisk) {
				t.Errorf("%s: D summed %+v, want 9 values, 60 postings, 30 member postings, composite", name, sc)
			}
			if sc.RelType == "R" && (sc.Postings != 20 || sc.MemberPostings != 20 || sc.Composite || sc.OnDisk) {
				t.Errorf("%s: R summed %+v, want 20 postings and members", name, sc)
			}
			if sc.RelType == "S" && sc.OnDisk != disk {
				t.Errorf("%s: S summed on disk %v, want %v", name, sc.OnDisk, disk)
			}
		}
		if slices.Sort(scopes); fmt.Sprint(scopes) != "[D E R S]" {
			t.Errorf("%s: scopes %v, want D, E, R and S", name, scopes)
		}
	}
}

// keyOf names st's definition: "D k,n", or "rel R k" for a relationship type.
func keyOf(st storepkg.PropertyIndexStat) string {
	scope := st.Label
	if st.RelTypeToken != 0 {
		scope = "rel " + st.RelType
	}
	return scope + " " + strings.Join(st.Keys, ",")
}
