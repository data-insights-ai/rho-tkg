package graph_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 11 (sigma-tkgd, 2026-10-17): a consumer asks the graph which untimed
// mode its store's rows are written under, by a call, before it relies on a
// door whose answer depends on it (the v4.60.0 window doors: under
// UntimedUnbounded an untimed entity holds from -inf, under UntimedFromTxFrom
// from its TxFrom). Admin().UntimedValidStart() answers the mode the store
// persisted (not the Config value: UntimedDefault resolves to it) and whether
// it is complete (false while a conversion to UntimedUnbounded is unfinished:
// the store then holds rows of both rules).

// TestRound11UntimedModeDoor: on every backend the door answers the mode the
// graph writes by (a new store: UntimedUnbounded; Config UntimedFromTxFrom:
// UntimedFromTxFrom), and its answer agrees with what a create stores (an
// untimed node's start: -inf or its TxFrom). After a conversion it answers
// UntimedUnbounded. A closed graph answers ErrGraphClosed.
func TestRound11UntimedModeDoor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, mode := range []graphpkg.UntimedValidStart{graphpkg.UntimedDefault, graphpkg.UntimedFromTxFrom, graphpkg.UntimedUnbounded} {
		for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.UntimedValidStart = mode }) {
			t.Run(fmt.Sprintf("%s/config=%d", b.name, mode), func(t *testing.T) {
				g := b.open(t)
				got, complete, err := g.Admin().UntimedValidStart()
				want := mode
				if want == graphpkg.UntimedDefault {
					want = graphpkg.UntimedUnbounded // a new store
				}
				if err != nil || got != want || !complete {
					t.Fatalf("config %d: UntimedValidStart = %d, %v, %v; want %d, true", mode, got, complete, err, want)
				}
				n, err := g.Nodes().Add(ctx, []string{"U"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				tx, _, _ := n.TxStamps()
				start := n.EffectiveValidFrom()
				if (got == graphpkg.UntimedUnbounded) != (start == types.UnboundedValidFrom) || (got == graphpkg.UntimedFromTxFrom) != (start == tx) {
					t.Errorf("config %d: the door says %d, an untimed create starts at %d (TxFrom %d)", mode, got, start, tx)
				}
				if got == graphpkg.UntimedFromTxFrom {
					if err := g.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedUnbounded); err != nil {
						t.Fatal(err)
					}
					if got, complete, err := g.Admin().UntimedValidStart(); err != nil || got != graphpkg.UntimedUnbounded || !complete {
						t.Errorf("after the conversion: %d, %v, %v; want UntimedUnbounded, true", got, complete, err)
					}
				}
				if err := g.Close(); err != nil {
					t.Fatal(err)
				}
				if _, _, err := g.Admin().UntimedValidStart(); !errors.Is(err, graphpkg.ErrGraphClosed) {
					t.Errorf("closed graph: %v, want ErrGraphClosed", err)
				}
			})
		}
	}
}

// TestRound11UntimedModeDoorPersisted: on the durable backends the door
// answers the persisted mode after a reopen under UntimedDefault (a store
// written under UntimedFromTxFrom stays one), and a conversion cut short
// answers UntimedUnbounded, not complete, until it finishes.
func TestRound11UntimedModeDoorPersisted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, b := range uvsDurable {
		t.Run(b.name, func(t *testing.T) {
			dir := t.TempDir()
			g, _ := uvsMustOpen(t, b, dir, graphpkg.UntimedFromTxFrom)
			for i := 0; i < 8; i++ {
				if _, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"i": int64(i)}); err != nil {
					t.Fatal(err)
				}
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g, _ = uvsMustOpen(t, b, dir, graphpkg.UntimedDefault)
			if got, complete, err := g.Admin().UntimedValidStart(); err != nil || got != graphpkg.UntimedFromTxFrom || !complete {
				t.Fatalf("reopened under UntimedDefault: %d, %v, %v; want UntimedFromTxFrom, true", got, complete, err)
			}
			if err := g.Admin().SetUntimedValidStart(newCutCtx(3), graphpkg.UntimedUnbounded); !errors.Is(err, context.Canceled) {
				t.Fatalf("cut-short conversion: %v, want context.Canceled", err)
			}
			if got, complete, err := g.Admin().UntimedValidStart(); err != nil || got != graphpkg.UntimedUnbounded || complete {
				t.Errorf("cut short: %d, %v, %v; want UntimedUnbounded, false", got, complete, err)
			}
			if err := g.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedUnbounded); err != nil {
				t.Fatal(err)
			}
			if got, complete, err := g.Admin().UntimedValidStart(); err != nil || got != graphpkg.UntimedUnbounded || !complete {
				t.Errorf("finished: %d, %v, %v; want UntimedUnbounded, true", got, complete, err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g, _ = uvsMustOpen(t, b, dir, graphpkg.UntimedDefault)
			defer g.Close()
			if got, complete, err := g.Admin().UntimedValidStart(); err != nil || got != graphpkg.UntimedUnbounded || !complete {
				t.Errorf("reopened after the conversion: %d, %v, %v; want UntimedUnbounded, true", got, complete, err)
			}
		})
	}
}

// TestRound11UntimedModeDoorReplica: a replica answers the primary's mode as
// replicated: after it applies the primary's conversion it answers
// UntimedUnbounded.
func TestRound11UntimedModeDoorReplica(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	primary, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, ChangeLog: true, Store: memory.New(memory.WithChangeLog()), UntimedValidStart: graphpkg.UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	replica, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2, ReadOnlyReplica: true, Store: memory.New(), UntimedValidStart: graphpkg.UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replica.SetReplicationSource(primary.Replication())
	if _, err := primary.Nodes().Add(ctx, []string{"U"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := primary.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedUnbounded); err != nil {
		t.Fatal(err)
	}
	var recs []storepkg.ChangeRecord
	if err := primary.Replication().ForEachChange(0, func(rec storepkg.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if got, _, err := replica.Admin().UntimedValidStart(); err != nil || got != graphpkg.UntimedFromTxFrom {
		t.Fatalf("replica before the feed: %d, %v; want UntimedFromTxFrom", got, err)
	}
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatal(err)
	}
	if got, complete, err := replica.Admin().UntimedValidStart(); err != nil || got != graphpkg.UntimedUnbounded || !complete {
		t.Errorf("replica after the primary's conversion: %d, %v, %v; want UntimedUnbounded, true", got, complete, err)
	}
}

// TestRound11UntimedModeDoorLegacyAndImport: a store with rows and no
// persisted mode (written before v4.56.0) answers UntimedFromTxFrom after a
// reopen under UntimedDefault, on the durable backends; an empty default
// store that imports a UntimedFromTxFrom export adopts and answers it.
func TestRound11UntimedModeDoorLegacyAndImport(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, b := range uvsDurable {
		t.Run(b.name, func(t *testing.T) {
			dir := t.TempDir()
			g, st := uvsMustOpen(t, b, dir, graphpkg.UntimedFromTxFrom)
			if _, err := g.Nodes().Add(ctx, []string{"Ref"}, nil); err != nil {
				t.Fatal(err)
			}
			if err := st.(storepkg.MetaKVCapability).MetaSet("untimed_valid_start", nil); err != nil {
				t.Fatal(err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g, _ = uvsMustOpen(t, b, dir, graphpkg.UntimedDefault)
			defer g.Close()
			if got, complete, err := g.Admin().UntimedValidStart(); err != nil || got != graphpkg.UntimedFromTxFrom || !complete {
				t.Errorf("legacy store under UntimedDefault: %d, %v, %v; want UntimedFromTxFrom, true", got, complete, err)
			}
		})
	}
	src, err := graphpkg.New(graphpkg.Config{Store: memory.New(), UntimedValidStart: graphpkg.UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if _, err := src.Nodes().Add(ctx, []string{"U"}, nil); err != nil {
		t.Fatal(err)
	}
	var full bytes.Buffer
	if err := src.IO().Export(&full); err != nil {
		t.Fatal(err)
	}
	dst, err := graphpkg.New(graphpkg.Config{Store: memory.New(), SnowflakeNodeID: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if got, _, err := dst.Admin().UntimedValidStart(); err != nil || got != graphpkg.UntimedUnbounded {
		t.Fatalf("empty default store: %d, %v; want UntimedUnbounded", got, err)
	}
	if err := dst.IO().Import(bytes.NewReader(full.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatal(err)
	}
	if got, complete, err := dst.Admin().UntimedValidStart(); err != nil || got != graphpkg.UntimedFromTxFrom || !complete {
		t.Errorf("after importing a UntimedFromTxFrom export: %d, %v, %v; want UntimedFromTxFrom, true", got, complete, err)
	}
}

// TestRound11UntimedModeDoorReplicaMidConversion: a replica that applied the
// primary's "converting" record but not its final one answers
// UntimedUnbounded, not complete; after the rest of the feed, complete.
func TestRound11UntimedModeDoorReplicaMidConversion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	primary, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, ChangeLog: true, Store: memory.New(memory.WithChangeLog()), UntimedValidStart: graphpkg.UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	replica, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2, ReadOnlyReplica: true, Store: memory.New(), UntimedValidStart: graphpkg.UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replica.SetReplicationSource(primary.Replication())
	for i := 0; i < 8; i++ {
		if _, err := primary.Nodes().Add(ctx, []string{"U"}, map[string]any{"i": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	feed := func(from uint64) []storepkg.ChangeRecord {
		t.Helper()
		var recs []storepkg.ChangeRecord
		if err := primary.Replication().ForEachChange(from, func(rec storepkg.ChangeRecord) bool {
			recs = append(recs, rec)
			return true
		}); err != nil {
			t.Fatal(err)
		}
		return recs
	}
	if err := primary.Admin().SetUntimedValidStart(newCutCtx(3), graphpkg.UntimedUnbounded); !errors.Is(err, context.Canceled) {
		t.Fatalf("cut-short conversion: %v, want context.Canceled", err)
	}
	recs := feed(0)
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatal(err)
	}
	if got, complete, err := replica.Admin().UntimedValidStart(); err != nil || got != graphpkg.UntimedUnbounded || complete {
		t.Errorf("replica mid-conversion: %d, %v, %v; want UntimedUnbounded, false", got, complete, err)
	}
	if err := primary.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedUnbounded); err != nil {
		t.Fatal(err)
	}
	if _, err := replica.Replication().ApplyChanges(feed(recs[len(recs)-1].LSN)); err != nil {
		t.Fatal(err)
	}
	if got, complete, err := replica.Admin().UntimedValidStart(); err != nil || got != graphpkg.UntimedUnbounded || !complete {
		t.Errorf("replica after the conversion: %d, %v, %v; want UntimedUnbounded, true", got, complete, err)
	}
}
