package core

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// currentOnlyGraphs opens badger and tiered (D on the reference shard) with
// D's and E's property indexes on disk and D's current-only (CURRENTONLY).
func currentOnlyGraphs(t *testing.T) map[string]*Core {
	t.Helper()
	onDisk, only := []string{"D", "E"}, []string{"D"}
	out := map[string]*Core{}
	g, err := New(Config{BadgerInMemory: true, PropertyIndexOnDiskLabels: onDisk, PropertyIndexCurrentOnlyLabels: only})
	if err != nil {
		t.Fatal(err)
	}
	out["badger"] = g
	ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), ShardWindow: time.Hour, RefLabels: []string{"D"},
		PropertyIndexOnDiskLabels: onDisk, PropertyIndexCurrentOnlyLabels: only})
	if err != nil {
		t.Fatal(err)
	}
	if g, err = New(Config{Store: ts}); err != nil {
		_ = ts.Close()
		t.Fatal(err)
	}
	out["tiered"] = g
	for _, g := range out {
		t.Cleanup(func() { _ = g.Close() })
	}
	return out
}

// TestCurrentOnlyIndexOnThePublicDoors: on a current-only definition a
// current lookup answers, every pinned or windowed lookup is refused with
// ErrPropertyIndexCurrentOnly, and Index.PropertyIndexServesPinned says so; a
// definition of a label not listed answers both and serves pinned reads.
func TestCurrentOnlyIndexOnThePublicDoors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, g := range currentOnlyGraphs(t) {
		t.Run(name, func(t *testing.T) {
			for _, label := range []string{"D", "E"} {
				if err := g.Index.CreateProperty(label, "k"); err != nil {
					t.Fatal(err)
				}
			}
			var want []types.NodeID
			for range 3 {
				n, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"k": "v", "tkg_valid_from": types.Instant(1000)})
				if err != nil {
					t.Fatal(err)
				}
				want = append(want, n.ID())
				if _, err := g.Nodes.Add(ctx, []string{"E"}, map[string]any{"k": "v", "tkg_valid_from": types.Instant(1000)}); err != nil {
					t.Fatal(err)
				}
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			got, err := g.Nodes.ByLabelAndProperty("D", "k", "v", storepkg.QueryOpts{})
			if err != nil {
				t.Fatalf("current read: %v", err)
			}
			if ids := nodeIDs(got); !slices.Equal(ids, want) {
				t.Errorf("current read: %v, want %v", ids, want)
			}
			pinned := map[string]storepkg.QueryOpts{
				"tx pin":       {TxPin: pin, TxPinSet: true},
				"tx at":        {TxAt: pin, TxAtSet: true},
				"valid at":     {ValidAt: 1500, ValidAtSet: true},
				"valid window": {ValidStart: 1000, ValidEnd: 2000, ValidWindowSet: true},
			}
			for door, opts := range pinned {
				if _, err := g.Nodes.ByLabelAndProperty("D", "k", "v", opts); !errors.Is(err, storepkg.ErrPropertyIndexCurrentOnly) {
					t.Errorf("D, %s: %v, want ErrPropertyIndexCurrentOnly", door, err)
				}
				if got, err := g.Nodes.ByLabelAndProperty("E", "k", "v", opts); err != nil || len(got) != 3 {
					t.Errorf("E, %s: %d nodes, %v, want 3", door, len(got), err)
				}
			}
			for _, c := range []struct {
				label, key string
				want       bool
			}{{"D", "k", false}, {"E", "k", true}, {"D", "absent", false}} {
				if ok, err := g.Index.PropertyIndexServesPinned(c.label, c.key); err != nil || ok != c.want {
					t.Errorf("PropertyIndexServesPinned(%s, %s) = %v, %v, want %v", c.label, c.key, ok, err, c.want)
				}
			}
		})
	}
}
