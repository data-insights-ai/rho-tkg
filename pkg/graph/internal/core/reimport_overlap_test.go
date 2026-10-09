package core

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestAtTxReImportOverlapsEarlierLife: a deleted ID re-imported with a valid
// start BEFORE the end of its first life's rows (the first life updated
// without a valid-from, so its later row starts at the update's write time).
// The re-import is the newer belief: from its valid start on, every point
// door at a pin after it answers the re-imported row; before it, the first
// life's row. Found by the effective-timeline oracle: the current-row shortcut
// (memory, badger) answered the re-imported row while the full chain fold
// (tiered, sharded, and memory / badger whenever the shortcut declines)
// interleaved the two lives by version number — the re-imported row restarts
// at version 0 — and tiled the first life's version 1 after it. Every backend,
// node and rel, NodeAtTx at a pin and NodeAt.
func TestAtTxReImportOverlapsEarlierLife(t *testing.T) {
	t.Parallel()
	ccRun(t, false, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		id := e.add("T", 1000, nil)
		e.mustUpdate(id, map[string]any{"x": int64(1)})
		var upd types.Instant
		for _, r := range e.chain(id) {
			if r.current {
				upd = r.tm.UpdatedAt
			}
		}
		e.mustDel(id)
		var d types.Instant
		for _, r := range e.chain(id) {
			d = max(d, r.tm.DeletedAt)
		}
		props := map[string]any{"tkg_valid_from": types.Instant(5000), "x": int64(9)}
		if e.rel {
			s, _ := e.g.Nodes.Get(e.ctx, e.start)
			en, _ := e.g.Nodes.Get(e.ctx, e.end)
			if _, err := e.g.Rels.Import(e.ctx, types.RelID(id), ccType, s, en, props); err != nil {
				t.Fatalf("Rels.Import: %v", err)
			}
		} else if _, err := e.g.Nodes.Import(e.ctx, types.NodeID(id), []string{ccLabel}, props); err != nil {
			t.Fatalf("Nodes.Import: %v", err)
		}
		pin := e.pin()
		x := func(va, p types.Instant) any {
			var row interface{ Properties() types.PropertySlice }
			var err error
			if e.rel {
				row, err = e.g.Temporal.RelAtTx(types.RelID(id), va, p)
			} else {
				row, err = e.g.Temporal.NodeAtTx(types.NodeID(id), va, p)
			}
			if errors.Is(err, storepkg.ErrNoVersionValidAt) {
				return "none"
			}
			if err != nil {
				t.Fatalf("AtTx(%d, %d): %v", va, p, err)
			}
			v, _ := row.Properties().Get("x")
			return v
		}
		var bad []string
		for _, c := range []struct {
			va   types.Instant
			want any
		}{
			{999, "none"}, {1000, int64(0)}, {4999, int64(0)},
			{5000, int64(9)}, {upd - 1, int64(9)}, {upd, int64(9)}, {d - 1, int64(9)}, {d, int64(9)}, {d + 1_000_000, int64(9)},
		} {
			for _, p := range []types.Instant{pin, 0} {
				if got := x(c.va, p); got != c.want {
					bad = append(bad, fmt.Sprintf("valid %d pin %d: x=%v, want %v", c.va, p, got, c.want))
				}
			}
		}
		if len(bad) > 0 {
			t.Fatalf("%s (update at %d, delete at %d):\n  %s\nchain:%s", e.kind(), upd, d, strings.Join(bad, "\n  "), e.chainString(id))
		}
	})
}
