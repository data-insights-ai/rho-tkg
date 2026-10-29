package core

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// compositeOracle is the answer ByLabelAndProperties must give: the current
// nodes of label carrying every pair of values, in ID order.
func compositeOracle(t *testing.T, g *Core, label string, values map[string]any) []types.NodeID {
	t.Helper()
	all, err := g.Nodes.ByLabel(label, storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	var out []types.NodeID
	for _, n := range all {
		match := true
		for k, v := range values {
			if got, ok := n.GetProperty(k); !ok || got != v {
				match = false
			}
		}
		if match {
			out = append(out, n.ID())
		}
	}
	slices.Sort(out)
	return out
}

// compositeStat is the IDXSTATS row of label's composite on keys (summed over
// tiered's shards: on disk only when every shard keeps it there).
func compositeStat(t *testing.T, g *Core, label string, keys []string) (onDisk bool, ramBytes int64) {
	t.Helper()
	stats, err := g.Index.PropertyIndexStats()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	onDisk = true
	for _, st := range stats {
		if st.Composite && st.Label == label && slices.Equal(st.Keys, keys) {
			found = true
			onDisk = onDisk && st.OnDisk
			ramBytes += st.RAMBytes
		}
	}
	if !found {
		t.Fatalf("no IDXSTATS row for %s %v", label, keys)
	}
	return onDisk, ramBytes
}

// TestCompositeIndexOnDisk (COMPDISK): a composite node index of a label kept
// on disk (PropertyIndexOnDiskLabels) holds its entries on disk, not in RAM,
// and answers current ByLabelAndProperties, paged by After and Limit, as the
// RAM index does: across creates, updates of the tuple and of other keys,
// deletes, reopens, the label leaving the list and joining again. A label not
// listed keeps its composite in RAM.
func TestCompositeIndexOnDisk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	keys := []string{"ov_namespace", "ov_rel"}
	type store struct {
		open func(onDisk []string) *Core
	}
	stores := map[string]store{}
	badgerDir := t.TempDir()
	stores["badger"] = store{open: func(onDisk []string) *Core {
		g, err := New(Config{BadgerDir: badgerDir, PropertyIndexOnDiskLabels: onDisk})
		if err != nil {
			t.Fatal(err)
		}
		return g
	}}
	tieredDir := t.TempDir()
	stores["tiered"] = store{open: func(onDisk []string) *Core {
		ts, err := tiered.New(tiered.Config{DataDir: tieredDir, ShardWindow: time.Hour, RefLabels: []string{"D", "R"},
			PropertyIndexOnDiskLabels: onDisk})
		if err != nil {
			t.Fatal(err)
		}
		g, err := New(Config{Store: ts})
		if err != nil {
			_ = ts.Close()
			t.Fatal(err)
		}
		return g
	}}
	for name, st := range stores {
		t.Run(name, func(t *testing.T) {
			listed := []string{"D"}
			g := st.open(listed)
			closeGraph := func() {
				t.Helper()
				if err := g.Close(); err != nil {
					t.Fatal(err)
				}
			}
			for _, label := range []string{"D", "R"} {
				if err := g.Index.CreateComposite(label, keys); err != nil {
					t.Fatal(err)
				}
			}
			var ids []types.NodeID
			for i := range 30 {
				for _, label := range []string{"D", "R"} {
					n, err := g.Nodes.Add(ctx, []string{label}, map[string]any{"ov_namespace": fmt.Sprint("ns", i%3), "ov_rel": "rel", "other": int64(i)})
					if err != nil {
						t.Fatal(err)
					}
					if label == "D" {
						ids = append(ids, n.ID())
					}
				}
			}
			check := func(step string) {
				t.Helper()
				for _, label := range []string{"D", "R"} {
					for _, ns := range []string{"ns0", "ns1", "ns2", "ns9"} {
						values := map[string]any{"ov_namespace": ns, "ov_rel": "rel"}
						want := compositeOracle(t, g, label, values)
						got, err := g.Nodes.ByLabelAndProperties(label, values, storepkg.QueryOpts{})
						if err != nil {
							t.Fatalf("%s %s %s: %v", step, label, ns, err)
						}
						if !slices.Equal(nodeIDs(got), want) {
							t.Errorf("%s %s %s: got %v, want %v", step, label, ns, nodeIDs(got), want)
						}
						var paged []types.NodeID
						for page := (storepkg.QueryOpts{Limit: 4}); ; {
							nodes, err := g.Nodes.ByLabelAndProperties(label, values, page)
							if err != nil {
								t.Fatal(err)
							}
							paged = append(paged, nodeIDs(nodes)...)
							if len(nodes) < 4 {
								break
							}
							page.After = types.EntityID(nodes[len(nodes)-1].ID())
						}
						if !slices.Equal(paged, want) {
							t.Errorf("%s %s %s: paged %v, want %v", step, label, ns, paged, want)
						}
					}
				}
			}
			disk := func(step string, want bool) {
				t.Helper()
				onDisk, ram := compositeStat(t, g, "D", keys)
				if onDisk != want || (want && ram != 0) {
					t.Errorf("%s: D's composite on disk %v with %d B in RAM, want on disk %v", step, onDisk, ram, want)
				}
				if onDisk, _ := compositeStat(t, g, "R", keys); onDisk {
					t.Errorf("%s: R's composite on disk, R is not listed", step)
				}
			}
			check("created")
			disk("created", true)
			if _, err := g.Nodes.Update(ctx, ids[0], map[string]any{"ov_namespace": "ns2"}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes.Update(ctx, ids[1], map[string]any{"other": int64(100)}); err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes.Delete(ctx, ids[2]); err != nil {
				t.Fatal(err)
			}
			check("updated, deleted")
			closeGraph()

			g = st.open(listed)
			check("reopened")
			disk("reopened", true)
			closeGraph()

			g = st.open(nil) // D's composite in RAM; a write there is not on disk
			disk("unlisted", false)
			if _, err := g.Nodes.Update(ctx, ids[3], map[string]any{"ov_namespace": "ns9"}); err != nil {
				t.Fatal(err)
			}
			check("unlisted")
			closeGraph()

			g = st.open(listed) // built again from the rows
			check("listed again")
			disk("listed again", true)
			closeGraph()
			if name == "badger" {
				if v := rawFormatMarker(t, badgerDir); v < storeutil.CompositeDiskStoreFormatVersion {
					t.Errorf("format marker %d, want %d", v, storeutil.CompositeDiskStoreFormatVersion)
				}
			}
		})
	}
}
