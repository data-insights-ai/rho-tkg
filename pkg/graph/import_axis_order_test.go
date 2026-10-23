package graph_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// axisOrderExport is an UntimedWholeAxis export of nodes labelled T, each
// valid [0, 10) with k = 1 and u = "x".
func axisOrderExport(t *testing.T, nodes int) []byte {
	t.Helper()
	src := allStoreBackendsWith(r11ZeroCfg)[0].open(t)
	for range nodes {
		if _, err := src.Nodes().Add(context.Background(), []string{"T"}, map[string]any{"k": int64(1), "u": "x", types.ShadowValidFrom: int64(0), types.ShadowValidTo: int64(10)}); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if err := src.IO().Export(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// axisOrderCheck: g answers the one imported node at valid 5 through the
// temporal index, the property index with a time filter and the label scan.
func axisOrderCheck(t *testing.T, g *graphpkg.Graph, what string) {
	t.Helper()
	if ax, err := g.Admin().ValidAxis(); err != nil || !ax.Literal() {
		t.Fatalf("%s: valid axis %+v, %v; want the literal axis", what, ax, err)
	}
	at := types.Instant(5)
	if got, err := g.Temporal().NodesByLabelAt("T", at); err != nil || len(got) != 1 {
		t.Errorf("%s: NodesByLabelAt(T, 5) = %d rows, %v; want 1", what, len(got), err)
	}
	if got, err := g.Temporal().NodesByLabelPropertyDuring("T", "k", int64(1), at, at+1); err != nil || len(got) != 1 {
		t.Errorf("%s: NodesByLabelPropertyDuring(T, k=1, [5,6)) = %d rows, %v; want 1", what, len(got), err)
	}
	if got, err := g.Nodes().ByLabel("T", storepkg.QueryOpts{ValidAt: at, ValidAtSet: true}); err != nil || len(got) != 1 {
		t.Errorf("%s: ByLabel(T, valid 5) = %d rows, %v; want 1", what, len(got), err)
	}
}

// TestImportAdoptsAxisBeforeRows (row 81): an empty default-mode target with
// a temporal and a property index adopts an UntimedWholeAxis export's axis
// before it writes the first row, so the store's envelopes and buckets are
// built on the literal axis (they were built on the legacy one and answered
// 0 rows at valid 5); a replica (no index) following the import's feed does
// the same; an
// import rolled back after the header leaves the target's mode as it was.
func TestImportAdoptsAxisBeforeRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	one := axisOrderExport(t, 1)
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			dst := b.open(t)
			if err := dst.Index().CreateTemporal("T"); err != nil {
				t.Fatal(err)
			}
			if err := dst.Index().CreateProperty("T", "k"); err != nil {
				t.Fatal(err)
			}
			if err := dst.IO().Import(bytes.NewReader(one), tkgio.ImportOptions{}); err != nil {
				t.Fatalf("import: %v", err)
			}
			axisOrderCheck(t, dst, b.name)
		})
	}

	t.Run("replica", func(t *testing.T) {
		primary, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, ChangeLog: true, Store: memory.New(memory.WithChangeLog())})
		if err != nil {
			t.Fatal(err)
		}
		defer primary.Close()
		// The replica's indexes are created on its empty store before it
		// opens read-only (a replica refuses Index().Create*).
		rdir := t.TempDir()
		pre, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2, BadgerDir: rdir})
		if err != nil {
			t.Fatal(err)
		}
		if err := pre.Index().CreateTemporal("T"); err != nil {
			t.Fatal(err)
		}
		if err := pre.Index().CreateProperty("T", "k"); err != nil {
			t.Fatal(err)
		}
		if err := pre.Close(); err != nil {
			t.Fatal(err)
		}
		replica, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2, ReadOnlyReplica: true, BadgerDir: rdir})
		if err != nil {
			t.Fatal(err)
		}
		defer replica.Close()
		replica.SetReplicationSource(primary.Replication())
		if err := primary.IO().Import(bytes.NewReader(one), tkgio.ImportOptions{}); err != nil {
			t.Fatal(err)
		}
		var recs []storepkg.ChangeRecord
		if err := primary.Replication().ForEachChange(0, func(rec storepkg.ChangeRecord) bool {
			recs = append(recs, rec)
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := replica.Replication().ApplyChanges(recs); err != nil {
			t.Fatalf("replica applies the import's feed: %v", err)
		}
		axisOrderCheck(t, replica, "replica")
	})

	t.Run("rollback", func(t *testing.T) {
		dir := t.TempDir()
		dst, err := graphpkg.New(graphpkg.Config{BadgerDir: dir})
		if err != nil {
			t.Fatal(err)
		}
		if err := dst.Constraints().CreateUnique(ctx, "T", "u"); err != nil {
			t.Fatal(err)
		}
		if err := dst.IO().Import(bytes.NewReader(axisOrderExport(t, 2)), tkgio.ImportOptions{}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
			t.Fatalf("import of two u=x nodes: %v, want ErrUniqueViolation", err)
		}
		check := func(g *graphpkg.Graph, what string) {
			t.Helper()
			if ax, err := g.Admin().ValidAxis(); err != nil || ax.Literal() {
				t.Errorf("%s: valid axis %+v, %v; want the legacy axis", what, ax, err)
			}
			if got, _, err := g.Admin().UntimedValidStart(); err != nil || got != graphpkg.UntimedUnbounded {
				t.Errorf("%s: mode %d, %v; want UntimedUnbounded (the target's)", what, got, err)
			}
		}
		check(dst, "rolled back")
		if err := dst.Close(); err != nil {
			t.Fatal(err)
		}
		re, err := graphpkg.New(graphpkg.Config{BadgerDir: dir})
		if err != nil {
			t.Fatalf("reopen after the rolled-back import: %v", err)
		}
		defer re.Close()
		check(re, "reopened")
	})
}
