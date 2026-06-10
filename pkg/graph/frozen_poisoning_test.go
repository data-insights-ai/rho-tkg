package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerstore "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Break-the-system tests for the v4.5.0 frozen-row contract. The frozen flag
// guards entity METHODS, but Temporal()/Integrity() historically return the
// shared internal pointer — on a frozen SCAN row that pointer aliases the
// store's canonical cache entry. These tests try to poison the cache through
// every reachable reference and assert the canonical row stays intact for
// subsequent readers. A failing assertion here means one hostile (or merely
// careless) reader silently corrupts query results for the whole process.

func frozenTestBackends(t *testing.T) map[string]func(t *testing.T) *graphpkg.Graph {
	t.Helper()
	return map[string]func(t *testing.T) *graphpkg.Graph{
		"memory": func(t *testing.T) *graphpkg.Graph {
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 6})
			if err != nil {
				t.Fatalf("graph.New(memory): %v", err)
			}
			return g
		},
		"badger": func(t *testing.T) *graphpkg.Graph {
			bs, err := badgerstore.New(badgerstore.Config{InMemory: true})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 7, Store: bs})
			if err != nil {
				t.Fatalf("graph.New(badger): %v", err)
			}
			return g
		},
	}
}

// A scan reader writing through row.Temporal() must NOT change what any
// other reader sees afterwards — neither point reads, nor re-scans, nor
// temporal queries that derive effective valid-time from the poisoned field.
func TestFrozenScanRowTemporalPoisoningDoesNotCorruptStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for name, open := range frozenTestBackends(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g := open(t)
			defer g.Close()

			n, err := g.Nodes().Add(ctx, []string{"Victim"}, map[string]any{
				"tkg_valid_from": types.Instant(1000),
				"tkg_valid_to":   types.Instant(2000),
			})
			if err != nil {
				t.Fatalf("add: %v", err)
			}

			rows, err := g.Nodes().ByLabel("Victim", storepkg.QueryOpts{})
			if err != nil || len(rows) != 1 {
				t.Fatalf("ByLabel: %v (%d rows)", err, len(rows))
			}
			row := rows[0]
			if !row.IsFrozen() {
				t.Fatalf("scan row not frozen — test no longer exercises the frozen path")
			}

			// The attack: write every temporal field through the accessor.
			if tm := row.Temporal(); tm != nil {
				tm.ValidFrom = 1
				tm.ValidTo = 2
				tm.TxFrom = 3
				tm.TxTo = 4
				tm.DeletedAt = 5
				tm.CreatedBy = "poisoned"
			} else {
				t.Fatalf("victim has no temporal metadata; setup broken")
			}

			// Point read must see the original assertion, not the poison.
			fresh, err := g.Nodes().Get(ctx, n.ID())
			if err != nil {
				t.Fatalf("Get after poisoning attempt: %v", err)
			}
			tm := fresh.Temporal()
			if tm == nil || tm.ValidFrom != 1000 || tm.ValidTo != 2000 || tm.DeletedAt != 0 || tm.CreatedBy == "poisoned" {
				t.Fatalf("canonical row corrupted through a frozen scan row's Temporal(): got %+v", tm)
			}

			// Re-scan must also be clean.
			rows2, err := g.Nodes().ByLabel("Victim", storepkg.QueryOpts{})
			if err != nil || len(rows2) != 1 {
				t.Fatalf("re-scan: %v (%d rows)", err, len(rows2))
			}
			tm2 := rows2[0].Temporal()
			if tm2 == nil || tm2.ValidFrom != 1000 || tm2.ValidTo != 2000 {
				t.Fatalf("re-scan sees poisoned temporal metadata: %+v", tm2)
			}

			// And the temporal resolver must still honour the ORIGINAL
			// world-time interval — the poison set ValidTo=2, which would
			// make the entity invisible at t=1500 if it reached the store.
			at, err := g.Temporal().NodeAt(n.ID(), 1500)
			if err != nil {
				t.Fatalf("NodeAt(1500) after poisoning attempt: %v — the poison reached the temporal resolver", err)
			}
			if at == nil {
				t.Fatalf("NodeAt(1500) returned no version")
			}
		})
	}
}

// Same attack through Integrity(): overwriting the hash fields and flipping
// shared Signature bytes must not break hash-chain verification for anyone
// else.
func TestFrozenScanRowIntegrityPoisoningDoesNotCorruptStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for name, open := range frozenTestBackends(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g := open(t)
			defer g.Close()

			n, err := g.Nodes().Add(ctx, []string{"Victim"}, map[string]any{
				"tkg_signature": []byte{0xAA, 0xBB, 0xCC},
			})
			if err != nil {
				t.Fatalf("add: %v", err)
			}

			rows, err := g.Nodes().ByLabel("Victim", storepkg.QueryOpts{})
			if err != nil || len(rows) != 1 {
				t.Fatalf("ByLabel: %v (%d rows)", err, len(rows))
			}
			row := rows[0]
			if !row.IsFrozen() {
				t.Fatalf("scan row not frozen — test no longer exercises the frozen path")
			}

			ig := row.Integrity()
			if ig == nil || ig.Hash == "" {
				t.Fatalf("victim has no integrity state; setup broken")
			}
			// The attack: forge the hash chain and flip signature bytes
			// through the shared references.
			ig.Hash = "forged"
			ig.PrevHash = "forged-prev"
			if len(ig.Signature) > 0 {
				ig.Signature[0] ^= 0xFF
			}

			// Verification must still pass for the canonical row.
			valid, err := g.Hash().VerifyNodeChain(n.ID())
			if err != nil || !valid {
				t.Fatalf("hash chain broken by frozen-row Integrity() poisoning: valid=%v err=%v", valid, err)
			}

			fresh, err := g.Nodes().Get(ctx, n.ID())
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			fig := fresh.Integrity()
			if fig == nil || fig.Hash == "forged" || (len(fig.Signature) > 0 && fig.Signature[0] != 0xAA) {
				t.Fatalf("canonical integrity state corrupted: %+v", fig)
			}
		})
	}
}

// Door symmetry: AddByIDIfAbsent must hand back a usable result on BOTH
// branches. The created branch returns a fresh mutable relationship; the
// found branch must not surprise the caller with a frozen shared row whose
// mutators panic.
func TestAddByIDIfAbsentFoundBranchNotFrozen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for name, open := range frozenTestBackends(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g := open(t)
			defer g.Close()

			a, _ := g.Nodes().Add(ctx, []string{"N"}, nil)
			b, _ := g.Nodes().Add(ctx, []string{"N"}, nil)
			if a == nil || b == nil {
				t.Fatalf("node setup failed")
			}

			created, wasCreated, err := g.Rels().AddByIDIfAbsent(ctx, "R", a.ID(), b.ID(), nil)
			if err != nil || !wasCreated {
				t.Fatalf("create branch: created=%v err=%v", wasCreated, err)
			}
			if created.IsFrozen() {
				t.Fatalf("created branch returned a frozen relationship")
			}

			found, wasCreated, err := g.Rels().AddByIDIfAbsent(ctx, "R", a.ID(), b.ID(), nil)
			if err != nil || wasCreated {
				t.Fatalf("found branch: created=%v err=%v", wasCreated, err)
			}
			if found.IsFrozen() {
				t.Fatalf("found branch returned a FROZEN relationship — asymmetric with the created branch; caller mutators would panic")
			}
			// Mutating the returned object must be safe (local-only, like
			// every other returned entity) — and must not corrupt the store.
			if err := found.SetProperty("scratch", "v"); err != nil {
				t.Fatalf("found-branch result rejects local mutation: %v", err)
			}
			again, _, err := g.Rels().AddByIDIfAbsent(ctx, "R", a.ID(), b.ID(), nil)
			if err != nil {
				t.Fatalf("third call: %v", err)
			}
			if _, ok := again.GetProperty("scratch"); ok {
				t.Fatalf("local mutation of the found-branch result leaked into the store")
			}
		})
	}
}
