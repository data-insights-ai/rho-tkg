package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReImportDoesNotAdvanceTheClock: a delete of an entity whose valid start
// lies decades ahead is stamped after that start (validInstantAfter: caller
// valid time turned into a transaction stamp). A plain re-import of the ID
// must follow that stamp on its own row only; it must not move the graph's
// commit clock (valid time must never pull the floor forward, context.go
// maxCommitStamp). Afterwards an unrelated create is stamped at the wall
// clock, NowTx stays at the wall clock, and — badger on disk, closed and
// reopened — the unrelated row is visible at a pin taken after the reopen.
//
// Catches: a re-import that raises the commit-clock floor to the chain's
// largest stamp (every later write of every entity stamped decades ahead; on
// reopen the floor seed is refused as implausible and those rows are invisible
// at a current pin — the lesson-71 anachronism).
func TestReImportDoesNotAdvanceTheClock(t *testing.T) {
	t.Parallel()
	const fifty = 50 * 365 * 24 * time.Hour
	run := func(t *testing.T, g *Core, rel bool) (unrelated types.NodeID) {
		t.Helper()
		e := newCCEnt(t, g, rel)
		wall := types.Instant(time.Now().UnixMilli())
		id := e.add("T", wall+types.Instant(fifty.Milliseconds()), nil)
		e.mustDel(id)
		d := e.tombstoneAt(id)
		e.mustReimport(id, map[string]any{"tkg_valid_from": types.Instant(5000), "x": int64(9)})
		for _, r := range e.chain(id) {
			if r.current && r.tm.TxFrom <= d {
				t.Fatalf("[%s] re-import TxFrom %d is not after the delete %d", e.kind(), r.tm.TxFrom, d)
			}
		}
		n, err := g.Nodes.Add(context.Background(), []string{"Other"}, map[string]any{"k": int64(1)})
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
		limit := types.Instant(time.Now().Add(time.Minute).UnixMilli())
		if tf := n.Temporal().TxFrom; tf > limit {
			t.Errorf("[%s] an unrelated create after the re-import is stamped %d, wall %d: the re-import advanced the commit clock", e.kind(), tf, limit)
		}
		if p := e.pin(); p > limit {
			t.Errorf("[%s] NowTx %d after the re-import is ahead of the wall %d", e.kind(), p, limit)
		}
		return n.ID()
	}
	for _, be := range txbBackends() {
		for _, rel := range []bool{false, true} {
			t.Run(be.name+"/"+map[bool]string{false: "node", true: "rel"}[rel], func(t *testing.T) {
				run(t, be.open(t, false), rel)
			})
		}
	}
	for _, rel := range []bool{false, true} {
		t.Run("badger-disk-reopen/"+map[bool]string{false: "node", true: "rel"}[rel], func(t *testing.T) {
			dir := t.TempDir()
			g, err := New(Config{BadgerDir: dir})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			other := run(t, g, rel)
			if err := g.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			g2, err := New(Config{BadgerDir: dir})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer g2.Close()
			pin, err := g2.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			if _, err := g2.Temporal.NodeAsOf(other, pin); err != nil {
				t.Fatalf("after reopen the unrelated node is invisible at pin %d: %v", pin, err)
			}
		})
	}
}
