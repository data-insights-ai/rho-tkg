package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// §4.1 — Rückdatierbare Transaktionszeit beim Ingest (backfill mode).
// Break-oriented: gate enforcement across the whole create-door family, verbatim
// TxFrom reproduction (the integrity hash is BLIND to TxFrom — assert Temporal()
// directly), and the bitemporal "is a backdated Erkenntniszeit addressable via
// AS OF SYSTEM TIME" contract with a no-backfill control that must NOT match.

func newBackfillGraph(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{Store: memory.New(), AllowTxBackfill: true})
	if err != nil {
		t.Fatalf("New(AllowTxBackfill): %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func mkNode(t *testing.T, g *Core, labels []string, props map[string]any) *types.Node {
	t.Helper()
	n, err := g.Nodes.Add(context.Background(), labels, props)
	if err != nil {
		t.Fatalf("Add(%v): %v", labels, err)
	}
	return n
}

// The canonical documented knowledge time from the study.
func erkenntnisZeit() types.Instant {
	return types.Instant(time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC).UnixMilli())
}

// -----------------------------------------------------------------------------
// Gate OFF (default) rejects backfill through EVERY create door (lesson 58).
// -----------------------------------------------------------------------------

func TestBackfill_GateOff_RejectsEveryCreateDoor(t *testing.T) {
	g := newTestGraph(t) // default Config → AllowTxBackfill false
	ctx := context.Background()
	tx := erkenntnisZeit()

	// Endpoint nodes for the rel doors (created WITHOUT tkg_tx_from — allowed).
	a := mkNode(t, g, []string{"A"}, nil)
	b := mkNode(t, g, []string{"B"}, nil)

	t.Run("Nodes.Add property", func(t *testing.T) {
		_, err := g.Nodes.Add(ctx, []string{"N"}, map[string]any{"tkg_tx_from": tx})
		if !errors.Is(err, ErrTxBackfillDisabled) {
			t.Fatalf("Add w/ tkg_tx_from gate off = %v, want ErrTxBackfillDisabled", err)
		}
	})
	t.Run("Nodes.AddWithTx", func(t *testing.T) {
		_, err := g.Nodes.AddWithTx(ctx, []string{"N"}, nil, tx)
		if !errors.Is(err, ErrTxBackfillDisabled) {
			t.Fatalf("AddWithTx gate off = %v, want ErrTxBackfillDisabled", err)
		}
	})
	t.Run("Nodes.Import property", func(t *testing.T) {
		_, err := g.Nodes.Import(ctx, g.nextNodeID(), []string{"N"}, map[string]any{"tkg_tx_from": tx})
		if !errors.Is(err, ErrTxBackfillDisabled) {
			t.Fatalf("Import w/ tkg_tx_from gate off = %v, want ErrTxBackfillDisabled", err)
		}
	})
	t.Run("Rels.Add property", func(t *testing.T) {
		_, err := g.Rels.Add(ctx, "R", a, b, map[string]any{"tkg_tx_from": tx})
		if !errors.Is(err, ErrTxBackfillDisabled) {
			t.Fatalf("Rels.Add w/ tkg_tx_from gate off = %v, want ErrTxBackfillDisabled", err)
		}
	})
	t.Run("Rels.AddWithTx", func(t *testing.T) {
		_, err := g.Rels.AddWithTx(ctx, "R", a, b, nil, tx)
		if !errors.Is(err, ErrTxBackfillDisabled) {
			t.Fatalf("Rels.AddWithTx gate off = %v, want ErrTxBackfillDisabled", err)
		}
	})
	t.Run("Batch.AddNode property", func(t *testing.T) {
		bb, _ := NewBatchBuilder(g)
		_, err := bb.AddNode([]string{"N"}, map[string]any{"tkg_tx_from": tx})
		if !errors.Is(err, ErrTxBackfillDisabled) {
			t.Fatalf("Batch.AddNode w/ tkg_tx_from gate off = %v, want ErrTxBackfillDisabled", err)
		}
	})
	t.Run("Batch.AddRelationship property", func(t *testing.T) {
		bb, _ := NewBatchBuilder(g)
		_, err := bb.AddRelationship("R", a, b, map[string]any{"tkg_tx_from": tx})
		if !errors.Is(err, ErrTxBackfillDisabled) {
			t.Fatalf("Batch.AddRelationship w/ tkg_tx_from gate off = %v, want ErrTxBackfillDisabled", err)
		}
	})
}

// -----------------------------------------------------------------------------
// Malformed override — invalid-value error takes precedence over the gate error.
// -----------------------------------------------------------------------------

func TestBackfill_InvalidTxFrom(t *testing.T) {
	ctx := context.Background()

	t.Run("AddWithTx zero", func(t *testing.T) {
		g := newBackfillGraph(t)
		if _, err := g.Nodes.AddWithTx(ctx, []string{"N"}, nil, 0); !errors.Is(err, ErrInvalidTxFrom) {
			t.Fatalf("AddWithTx(0) = %v, want ErrInvalidTxFrom", err)
		}
	})
	t.Run("AddWithTx negative", func(t *testing.T) {
		g := newBackfillGraph(t)
		if _, err := g.Nodes.AddWithTx(ctx, []string{"N"}, nil, -5); !errors.Is(err, ErrInvalidTxFrom) {
			t.Fatalf("AddWithTx(-5) = %v, want ErrInvalidTxFrom", err)
		}
	})
	t.Run("Rels.AddWithTx non-positive (parity)", func(t *testing.T) {
		g := newBackfillGraph(t)
		a := mkNode(t, g, []string{"A"}, nil)
		b := mkNode(t, g, []string{"B"}, nil)
		if _, err := g.Rels.AddWithTx(ctx, "R", a, b, nil, 0); !errors.Is(err, ErrInvalidTxFrom) {
			t.Fatalf("Rels.AddWithTx(0) = %v, want ErrInvalidTxFrom", err)
		}
		if _, err := g.Rels.AddWithTx(ctx, "R", a, b, nil, -1); !errors.Is(err, ErrInvalidTxFrom) {
			t.Fatalf("Rels.AddWithTx(-1) = %v, want ErrInvalidTxFrom", err)
		}
	})
	t.Run("negative property (gate ON) is invalid, not disabled", func(t *testing.T) {
		g := newBackfillGraph(t)
		_, err := g.Nodes.Add(ctx, []string{"N"}, map[string]any{"tkg_tx_from": int64(-5)})
		if !errors.Is(err, ErrInvalidTxFrom) {
			t.Fatalf("Add tkg_tx_from=-5 = %v, want ErrInvalidTxFrom", err)
		}
	})
	t.Run("negative property (gate OFF) is invalid, precedence over disabled", func(t *testing.T) {
		g := newTestGraph(t)
		_, err := g.Nodes.Add(ctx, []string{"N"}, map[string]any{"tkg_tx_from": int64(-5)})
		if !errors.Is(err, ErrInvalidTxFrom) {
			t.Fatalf("Add tkg_tx_from=-5 gate off = %v, want ErrInvalidTxFrom (precedence)", err)
		}
	})
}

// -----------------------------------------------------------------------------
// A FUTURE tkg_tx_from is incoherent (a knowledge time cannot exceed wall-clock
// at write; the feature is BACKfill) and would produce an inverted TX interval
// on the version once superseded — reject it at every create door, and never let
// it corrupt bitemporal state.
// -----------------------------------------------------------------------------

func TestBackfill_FutureTxFromRejected(t *testing.T) {
	ctx := context.Background()
	future := types.Instant(time.Now().Add(365 * 24 * time.Hour).UnixMilli()) // ~1yr ahead

	t.Run("Nodes.AddWithTx", func(t *testing.T) {
		g := newBackfillGraph(t)
		if _, err := g.Nodes.AddWithTx(ctx, []string{"N"}, nil, future); !errors.Is(err, ErrInvalidTxFrom) {
			t.Fatalf("AddWithTx(future) = %v, want ErrInvalidTxFrom", err)
		}
	})
	t.Run("Nodes.Add property", func(t *testing.T) {
		g := newBackfillGraph(t)
		if _, err := g.Nodes.Add(ctx, []string{"N"}, map[string]any{"tkg_tx_from": future}); !errors.Is(err, ErrInvalidTxFrom) {
			t.Fatalf("Add(future) = %v, want ErrInvalidTxFrom", err)
		}
	})
	t.Run("Rels.AddWithTx (parity)", func(t *testing.T) {
		g := newBackfillGraph(t)
		a := mkNode(t, g, []string{"A"}, nil)
		b := mkNode(t, g, []string{"B"}, nil)
		if _, err := g.Rels.AddWithTx(ctx, "R", a, b, nil, future); !errors.Is(err, ErrInvalidTxFrom) {
			t.Fatalf("Rels.AddWithTx(future) = %v, want ErrInvalidTxFrom", err)
		}
	})
	t.Run("Batch.AddNode property", func(t *testing.T) {
		g := newBackfillGraph(t)
		bb, _ := NewBatchBuilder(g)
		if _, err := bb.AddNode([]string{"N"}, map[string]any{"tkg_tx_from": future}); !errors.Is(err, ErrInvalidTxFrom) {
			t.Fatalf("Batch.AddNode(future) = %v, want ErrInvalidTxFrom", err)
		}
	})
	t.Run("no inverted interval reaches the store", func(t *testing.T) {
		// The rejection is the whole guard: a future backfill can never be
		// created, so the superseded-version inverted-TX-interval path (genesis
		// TxFrom > TxTo, invisible at every txAt, passing Verify*Chain) is
		// unreachable. Confirm no node was created.
		g := newBackfillGraph(t)
		if _, err := g.Nodes.AddWithTx(ctx, []string{"Person"}, map[string]any{"pid": int64(1)}, future); err == nil {
			t.Fatal("future backfill unexpectedly created a node")
		}
		if c, _ := g.Nodes.Count(); c != 0 {
			t.Fatalf("node count = %d after rejected future backfill, want 0", c)
		}
	})
	t.Run("a past backfill still works (no over-rejection)", func(t *testing.T) {
		g := newBackfillGraph(t)
		n, err := g.Nodes.AddWithTx(ctx, []string{"Person"}, nil, erkenntnisZeit())
		if err != nil {
			t.Fatalf("past AddWithTx = %v, want ok", err)
		}
		if n.Temporal().TxFrom != erkenntnisZeit() {
			t.Fatalf("past backfill TxFrom = %d, want %d", n.Temporal().TxFrom, erkenntnisZeit())
		}
	})
}

// -----------------------------------------------------------------------------
// Gate ON: TxFrom is reproduced VERBATIM (assert Temporal directly — the hash
// oracle is blind to TxFrom), and the integrity chain still verifies.
// -----------------------------------------------------------------------------

func TestBackfill_StampsTxFromVerbatim_NodeAndRel(t *testing.T) {
	g := newBackfillGraph(t)
	ctx := context.Background()
	tx := erkenntnisZeit()

	n, err := g.Nodes.AddWithTx(ctx, []string{"Person"}, map[string]any{"pid": int64(1)}, tx)
	if err != nil {
		t.Fatalf("AddWithTx: %v", err)
	}
	// Re-fetch to prove it round-tripped through the store, not just the reply.
	got, err := g.Nodes.Get(ctx, n.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Temporal().TxFrom != tx {
		t.Fatalf("stored node TxFrom = %d, want backfilled %d", got.Temporal().TxFrom, tx)
	}
	if ok, err := g.Hash.VerifyNodeChain(n.ID()); err != nil || !ok {
		t.Fatalf("VerifyNodeChain after backfill = (%v,%v), want (true,nil)", ok, err)
	}

	a := mkNode(t, g, []string{"A"}, nil)
	b := mkNode(t, g, []string{"B"}, nil)
	r, err := g.Rels.AddWithTx(ctx, "KNOWS", a, b, map[string]any{"w": int64(2)}, tx)
	if err != nil {
		t.Fatalf("Rels.AddWithTx: %v", err)
	}
	gotR, err := g.Rels.Get(ctx, r.ID())
	if err != nil {
		t.Fatalf("Rels.Get: %v", err)
	}
	if gotR.Temporal().TxFrom != tx {
		t.Fatalf("stored rel TxFrom = %d, want backfilled %d", gotR.Temporal().TxFrom, tx)
	}
	if ok, err := g.Hash.VerifyRelChain(r.ID()); err != nil || !ok {
		t.Fatalf("VerifyRelChain after backfill = (%v,%v), want (true,nil)", ok, err)
	}
}

// -----------------------------------------------------------------------------
// Batch backfill: per-node distinct backfill instants, each reproduced verbatim
// (pins the separate backfillTxFrom field vs. the execute-time reset-to-0).
// -----------------------------------------------------------------------------

func TestBackfill_Batch_PerNodeDistinctTxFrom(t *testing.T) {
	g := newBackfillGraph(t)
	ctx := context.Background()
	tx1 := erkenntnisZeit()
	tx2 := tx1 + 86_400_000 // one day later

	bb, _ := NewBatchBuilder(g)
	n1, err := bb.AddNode([]string{"P"}, map[string]any{"tkg_tx_from": tx1, "i": int64(1)})
	if err != nil {
		t.Fatalf("AddNode 1: %v", err)
	}
	n2, err := bb.AddNode([]string{"P"}, map[string]any{"tkg_tx_from": tx2, "i": int64(2)})
	if err != nil {
		t.Fatalf("AddNode 2: %v", err)
	}
	if _, err := bb.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	g1, _ := g.Nodes.Get(ctx, n1.ID())
	g2, _ := g.Nodes.Get(ctx, n2.ID())
	if g1.Temporal().TxFrom != tx1 {
		t.Fatalf("batch node1 TxFrom = %d, want %d", g1.Temporal().TxFrom, tx1)
	}
	if g2.Temporal().TxFrom != tx2 {
		t.Fatalf("batch node2 TxFrom = %d, want %d", g2.Temporal().TxFrom, tx2)
	}
}

// -----------------------------------------------------------------------------
// Flagship: the backdated Erkenntniszeit is addressable via AS OF SYSTEM TIME,
// a no-backfill control is NOT — the whole point of §4.1.
// -----------------------------------------------------------------------------

func TestBackfill_KnowledgeTimeAddressableViaAsOf(t *testing.T) {
	g := newBackfillGraph(t)
	ctx := context.Background()
	stichtag := types.Instant(time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC).UnixMilli()) // valid-from
	erkenntnis := erkenntnisZeit()                                                       // tx-from

	// Backfilled: ValidFrom = Stichtag (world time), TxFrom = Erkenntniszeit.
	bf, err := g.Nodes.AddWithTx(ctx, []string{"Person"},
		map[string]any{"pid": int64(1), "tkg_valid_from": stichtag}, erkenntnis)
	if err != nil {
		t.Fatalf("AddWithTx: %v", err)
	}
	// Control: normal Add → TxFrom = wall-clock now (≫ erkenntnis).
	ctrl := mkNode(t, g, []string{"Person"},
		map[string]any{"pid": int64(2), "tkg_valid_from": stichtag})
	if ctrl.Temporal().TxFrom <= erkenntnis {
		t.Fatalf("control TxFrom %d should be ~now, ≫ erkenntnis %d", ctrl.Temporal().TxFrom, erkenntnis)
	}

	// (a) NodeAsOf at the knowledge time: backfilled visible, control invisible.
	if _, err := g.Temporal.NodeAsOf(bf.ID(), erkenntnis); err != nil {
		t.Fatalf("NodeAsOf(backfilled, erkenntnis) = %v, want visible", err)
	}
	if _, err := g.Temporal.NodeAsOf(ctrl.ID(), erkenntnis); !errors.Is(err, ErrNoVersionAsOf) {
		t.Fatalf("NodeAsOf(control, erkenntnis) = %v, want ErrNoVersionAsOf (not yet recorded)", err)
	}
	// (b) One ms before the knowledge time: backfilled not yet recorded.
	if _, err := g.Temporal.NodeAsOf(bf.ID(), erkenntnis-1); !errors.Is(err, ErrNoVersionAsOf) {
		t.Fatalf("NodeAsOf(backfilled, erkenntnis-1) = %v, want ErrNoVersionAsOf", err)
	}

	// (c) Full bitemporal point query: (validAt, txAt) grid.
	if got, err := g.Temporal.NodeAtTx(bf.ID(), stichtag, erkenntnis); err != nil || got == nil {
		t.Fatalf("NodeAtTx(stichtag, erkenntnis) = (%v,%v), want the node", got, err)
	}
	if got, _ := g.Temporal.NodeAtTx(bf.ID(), stichtag, erkenntnis-1); got != nil {
		t.Fatalf("NodeAtTx(stichtag, erkenntnis-1) = %v, want nil (unknown then)", got)
	}
	if got, _ := g.Temporal.NodeAtTx(bf.ID(), stichtag-1, erkenntnis); got != nil {
		t.Fatalf("NodeAtTx(stichtag-1, erkenntnis) = %v, want nil (not valid yet)", got)
	}
}

// -----------------------------------------------------------------------------
// A batch node that survives a partial-failure (partial-live) create must still
// emit EventNodeCreate with the REAL commit clock, not the backdated backfill
// TxFrom (which the persisted row carries). Reachable only via a wrapper store
// whose PutNodesBatch installs the rows then errors and whose DeleteNode fails.
// -----------------------------------------------------------------------------

// installThenFailBatchStore installs the batch rows into the inner store, then
// reports a post-write error, and fails DeleteNode — forcing the batch
// partial-live path (nodesCommitted via partialLive=true).
type installThenFailBatchStore struct {
	storepkg.Store
	failBatch  error
	failDelete error
}

func (s *installThenFailBatchStore) PutNodesBatch(nodes []*types.Node) error {
	_ = s.Store.PutNodesBatch(nodes) // rows go live in the inner store
	return s.failBatch
}

func (s *installThenFailBatchStore) DeleteNode(id types.NodeID) error {
	return s.failDelete
}

func TestBackfill_BatchPartialLiveEventUsesRealClock(t *testing.T) {
	ctx := context.Background()
	store := &installThenFailBatchStore{
		Store:      memory.New(),
		failBatch:  errors.New("post-write batch failure"),
		failDelete: errors.New("delete cleanup failure"),
	}
	g, err := New(Config{Store: store, AllowTxBackfill: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	// Register label "Existing" via a normal Add (PutNode is delegated + succeeds)
	// so the batch below finds an EXISTING label → labelsLocked=false → the
	// cleanup-failure partial-live path.
	if _, err := g.Nodes.Add(ctx, []string{"Existing"}, nil); err != nil {
		t.Fatalf("register label: %v", err)
	}

	var mu sync.Mutex
	var createTimestamps []types.Instant
	bus := eventspkg.NewEventBus()
	bus.Subscribe(func(e eventspkg.Event) {
		if e.Type == eventspkg.EventNodeCreate {
			mu.Lock()
			createTimestamps = append(createTimestamps, e.Timestamp)
			mu.Unlock()
		}
	})
	if err := g.Events.SetSync(bus); err != nil {
		t.Fatalf("SetSync: %v", err)
	}

	bf := erkenntnisZeit()
	bb, _ := NewBatchBuilder(g)
	if _, err := bb.AddNode([]string{"Existing"}, map[string]any{"tkg_tx_from": bf, "pid": int64(1)}); err != nil {
		t.Fatalf("Batch.AddNode: %v", err)
	}
	// Partial-live: Execute reports failure but the row is live and its event fires.
	_, _ = bb.Execute()

	mu.Lock()
	defer mu.Unlock()
	if len(createTimestamps) == 0 {
		t.Fatal("no EventNodeCreate captured — partial-live branch not reached")
	}
	for _, ts := range createTimestamps {
		if ts == bf {
			t.Fatalf("EventNodeCreate timestamp = %d (backdated backfill), want the real commit clock", ts)
		}
		if ts <= bf {
			t.Fatalf("EventNodeCreate timestamp = %d, want > backfill instant %d (real clock)", ts, bf)
		}
	}
}

// -----------------------------------------------------------------------------
// The transaction create path is a create-door family member too — it funnels
// through addNodeInternal / the rel kernel, so backfill via tkg_tx_from must work
// inside a tx, and be gated off outside it (lesson 58 family completeness).
// -----------------------------------------------------------------------------

func TestBackfill_TxPath(t *testing.T) {
	tx1 := erkenntnisZeit()

	t.Run("gate on: tx create backfills verbatim", func(t *testing.T) {
		g := newBackfillGraph(t)
		tx, err := g.BeginTx()
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		n, err := tx.AddNode([]string{"Person"}, map[string]any{"tkg_tx_from": tx1, "pid": int64(1)})
		if err != nil {
			t.Fatalf("tx.AddNode: %v", err)
		}
		a, _ := tx.AddNode([]string{"A"}, nil)
		r, err := tx.AddRelationship("KNOWS", n, a, map[string]any{"tkg_tx_from": tx1})
		if err != nil {
			t.Fatalf("tx.AddRelationship: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		gotN, _ := g.Nodes.Get(context.Background(), n.ID())
		if gotN.Temporal().TxFrom != tx1 {
			t.Fatalf("tx node TxFrom = %d, want %d", gotN.Temporal().TxFrom, tx1)
		}
		gotR, _ := g.Rels.Get(context.Background(), r.ID())
		if gotR.Temporal().TxFrom != tx1 {
			t.Fatalf("tx rel TxFrom = %d, want %d", gotR.Temporal().TxFrom, tx1)
		}
	})

	t.Run("gate off: tx create rejects", func(t *testing.T) {
		g := newTestGraph(t)
		tx, err := g.BeginTx()
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.AddNode([]string{"N"}, map[string]any{"tkg_tx_from": tx1}); !errors.Is(err, ErrTxBackfillDisabled) {
			t.Fatalf("tx.AddNode w/ tkg_tx_from gate off = %v, want ErrTxBackfillDisabled", err)
		}
	})
}

// -----------------------------------------------------------------------------
// Backfill is CREATE-only: Update must not accept tkg_tx_from (per-version
// TxFrom is always the monotonic system clock, lesson 20/46).
// -----------------------------------------------------------------------------

func TestBackfill_UpdateRejectsTxFrom(t *testing.T) {
	g := newBackfillGraph(t) // even with the gate ON
	ctx := context.Background()
	n, err := g.Nodes.AddWithTx(ctx, []string{"N"}, map[string]any{"x": int64(1)}, erkenntnisZeit())
	if err != nil {
		t.Fatalf("AddWithTx: %v", err)
	}
	before := n.Temporal().TxFrom

	if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"tkg_tx_from": erkenntnisZeit() + 1000}); err == nil {
		t.Fatal("Update with tkg_tx_from should be rejected (reserved key), got nil error")
	}
	got, _ := g.Nodes.Get(ctx, n.ID())
	if got.Temporal().TxFrom != before {
		t.Fatalf("failed Update mutated TxFrom %d -> %d", before, got.Temporal().TxFrom)
	}
}
