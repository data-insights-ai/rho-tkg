package graph

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// engineRows returns every application row of the engine's latest commit.
func engineRows(t *testing.T, g *Graph) []raftlog.KV {
	t.Helper()
	e := g.eng.(*pebbleEngine)
	idx, _, err := e.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	v, err := e.store.ApplicationView(idx)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	var out []raftlog.KV
	var after []byte
	for {
		pg, err := v.Scan(context.Background(), nil, nil, after, raftlog.ReadBudget{Rows: 256, Bytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, pg.Rows...)
		if pg.Complete {
			return out
		}
		after = pg.Next
	}
}

// TestEndLivesOnlyInCellsAndSurvivesReopen: End is stored only as LifeEnd cells
// (no engine row), read back after reopen, and a Retract is never mistaken for
// it or able to erase it. That the cells travel with a range move or an import
// is covered by WP-28 (dist codec) and WP-10b (importer), not here.
func TestEndLivesOnlyInCellsAndSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	cfg := engineConfig(t, t.TempDir())
	g, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	all := scopeAll(t)
	var id types.NodeID
	update(t, g, func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(1)}, Scope: all})
		return
	})
	ref, _ := NodeRef(id)
	update(t, g, func(tx Tx) error { return tx.End(End{Target: ref, Scope: spanMS(t, 0, 200)}) })
	for _, kv := range engineRows(t, g) {
		if bytes.HasPrefix(kv.Key, []byte("rho-tkg:v5/pkg/graph:end:v1/")) {
			t.Fatalf("per-entity End engine row still written: %q", kv.Key)
		}
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g, err = Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	n9 := map[string]Value{"n": Int(9)}
	if _, err := g.Update(ctx, func(tx Tx) error {
		return tx.Correct(Correct{Target: ref, Set: n9, Scope: spanMS(t, 250, 300)})
	}); !errors.Is(err, ErrInvalidScope) {
		t.Fatalf("Correct after the End, after reopen: %v", err)
	}
	// A Retract over the tail must not erase the End.
	update(t, g, func(tx Tx) error { return tx.Retract(Retract{Target: ref, Scope: all}) })
	if _, err := g.Update(ctx, func(tx Tx) error {
		return tx.Correct(Correct{Target: ref, Set: n9, Scope: spanMS(t, 250, 300)})
	}); !errors.Is(err, ErrInvalidScope) {
		t.Fatalf("Retract erased the End: %v", err)
	}
	// An id that was only retracted has no End.
	var other types.NodeID
	update(t, g, func(tx Tx) (err error) {
		other, err = tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(1)}, Scope: all})
		return
	})
	oref, _ := NodeRef(other)
	update(t, g, func(tx Tx) error { return tx.Retract(Retract{Target: oref, Scope: spanMS(t, 100, 400)}) })
	update(t, g, func(tx Tx) error { return tx.Correct(Correct{Target: oref, Set: n9, Scope: spanMS(t, 250, 300)}) })
}

// An End carrying Recorded evidence commits (the evidence codec knows the kind).
func TestEndWithRecordedEvidenceCommits(t *testing.T) {
	g := openMem(t)
	all := scopeAll(t)
	var id types.NodeID
	update(t, g, func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Scope: all})
		return
	})
	ref, _ := NodeRef(id)
	update(t, g, func(tx Tx) error {
		return tx.End(End{Target: ref, Scope: spanMS(t, 0, 200), Recorded: recAt(t, knowledgeAxis(t), 1000)})
	})
}

// A later End than the standing one is not dropped: its Recorded evidence is
// committed and the earliest End still rules.
func TestLaterEndKeepsEvidenceAndEarliestEndRules(t *testing.T) {
	g := openMem(t)
	all := scopeAll(t)
	var id types.NodeID
	update(t, g, func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(1)}, Scope: all})
		return
	})
	ref, _ := NodeRef(id)
	update(t, g, func(tx Tx) error { return tx.End(End{Target: ref, Scope: spanMS(t, 0, 200)}) })
	update(t, g, func(tx Tx) error {
		return tx.End(End{Target: ref, Scope: spanMS(t, 0, 400), Recorded: recAt(t, knowledgeAxis(t), 1000)})
	})
	if _, err := g.Update(context.Background(), func(tx Tx) error {
		return tx.Correct(Correct{Target: ref, Set: map[string]Value{"n": Int(9)}, Scope: spanMS(t, 250, 300)})
	}); !errors.Is(err, ErrInvalidScope) {
		t.Fatalf("earliest End lost: %v", err)
	}
}
