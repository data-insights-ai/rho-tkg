package graph_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// backendCase drives every scenario against both in-tree persistence backends.
type backendCase struct {
	name string
	open func(t *testing.T) *graphpkg.Graph
}

func uniqueBackends(t *testing.T) []backendCase {
	return []backendCase{
		{
			name: "memory",
			open: func(t *testing.T) *graphpkg.Graph {
				g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1})
				if err != nil {
					t.Fatalf("new memory graph: %v", err)
				}
				return g
			},
		},
		{
			name: "badger",
			open: func(t *testing.T) *graphpkg.Graph {
				g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2, BadgerDir: t.TempDir()})
				if err != nil {
					t.Fatalf("new badger graph: %v", err)
				}
				return g
			},
		},
	}
}

func mustAddUser(t *testing.T, g *graphpkg.Graph, email string) *types.Node {
	t.Helper()
	n, err := g.Nodes().Add(context.Background(), []string{"User"}, map[string]any{"email": email})
	if err != nil {
		t.Fatalf("add User{email:%q}: %v", email, err)
	}
	return n
}

func mustCreateUnique(t *testing.T, g *graphpkg.Graph) {
	t.Helper()
	if err := g.Constraints().CreateUnique(context.Background(), "User", "email"); err != nil {
		t.Fatalf("CreateUnique(User,email): %v", err)
	}
}

func TestUnique_CreateThenViolate(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)
			mustAddUser(t, g, "a@x.com")

			_, err := g.Nodes().Add(context.Background(), []string{"User"}, map[string]any{"email": "a@x.com"})
			if !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("duplicate create err = %v, want ErrUniqueViolation", err)
			}
			// A different value is fine.
			mustAddUser(t, g, "b@x.com")
			// A node WITHOUT the constrained key is unconstrained.
			if _, err := g.Nodes().Add(context.Background(), []string{"User"}, map[string]any{"name": "no-email"}); err != nil {
				t.Fatalf("add keyless User: %v", err)
			}
		})
	}
}

func TestUnique_UpdateIntoViolation(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)
			mustAddUser(t, g, "a@x.com")
			b := mustAddUser(t, g, "b@x.com")

			// Update b's email onto a's value → violation.
			_, err := g.Nodes().Update(context.Background(), b.ID(), map[string]any{"email": "a@x.com"})
			if !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("update-into-violation err = %v, want ErrUniqueViolation", err)
			}
			// Update b to a fresh value succeeds.
			if _, err := g.Nodes().Update(context.Background(), b.ID(), map[string]any{"email": "c@x.com"}); err != nil {
				t.Fatalf("update b -> c: %v", err)
			}
			// Updating b's non-constrained property does not falsely reject.
			if _, err := g.Nodes().Update(context.Background(), b.ID(), map[string]any{"name": "Bob"}); err != nil {
				t.Fatalf("update b name: %v", err)
			}
		})
	}
}

func TestUnique_CASIntoViolation(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)
			mustAddUser(t, g, "a@x.com")
			b := mustAddUser(t, g, "b@x.com")

			_, err := g.Nodes().CompareAndSetProperty(context.Background(), b.ID(), "email", "b@x.com", "a@x.com")
			if !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("CAS-into-violation err = %v, want ErrUniqueViolation", err)
			}
			// CAS to a fresh value succeeds.
			ok, err := g.Nodes().CompareAndSetProperty(context.Background(), b.ID(), "email", "b@x.com", "z@x.com")
			if err != nil || !ok {
				t.Fatalf("CAS b -> z = (%v,%v), want (true,nil)", ok, err)
			}
		})
	}
}

func TestUnique_LabelAddIntoViolation(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)
			mustAddUser(t, g, "shared@x.com")

			// A node carrying the offending value but NOT yet the constrained
			// label. Adding the label must be rejected.
			other, err := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"email": "shared@x.com"})
			if err != nil {
				t.Fatalf("add Person: %v", err)
			}
			err = g.Nodes().AddLabel(context.Background(), other.ID(), "User")
			if !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("label-add-into-violation err = %v, want ErrUniqueViolation", err)
			}
			// Adding a constrained label to a node with a unique value succeeds.
			free, err := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"email": "free@x.com"})
			if err != nil {
				t.Fatalf("add free Person: %v", err)
			}
			if err := g.Nodes().AddLabel(context.Background(), free.ID(), "User"); err != nil {
				t.Fatalf("add label to free node: %v", err)
			}
		})
	}
}

func TestUnique_DeleteFreesValue(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)
			a := mustAddUser(t, g, "a@x.com")

			if err := g.Nodes().Delete(context.Background(), a.ID()); err != nil {
				t.Fatalf("delete a: %v", err)
			}
			// Value is free again — reuse succeeds.
			mustAddUser(t, g, "a@x.com")
		})
	}
}

func TestUnique_SupersessionFrees(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)
			a := mustAddUser(t, g, "a@x.com")

			// Update a away from the value; the value is now free.
			if _, err := g.Nodes().Update(context.Background(), a.ID(), map[string]any{"email": "a2@x.com"}); err != nil {
				t.Fatalf("update a away: %v", err)
			}
			// A second node may now claim the freed value.
			mustAddUser(t, g, "a@x.com")
		})
	}
}

func TestUnique_ExistingDataValidationFails(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			// Two nodes already share a value BEFORE the constraint.
			mustAddUser(t, g, "dup@x.com")
			mustAddUser(t, g, "dup@x.com")

			err := g.Constraints().CreateUnique(context.Background(), "User", "email")
			if !errors.Is(err, graphpkg.ErrUniqueViolationExisting) {
				t.Fatalf("CreateUnique over dup data err = %v, want ErrUniqueViolationExisting", err)
			}
			// No constraint installed.
			if cs := g.Constraints().UniqueConstraints(); len(cs) != 0 {
				t.Fatalf("UniqueConstraints after failed create = %v, want empty", cs)
			}
			// Writes are NOT enforced (a third dup is accepted).
			mustAddUser(t, g, "dup@x.com")
		})
	}
}

func TestUnique_FloatRejected(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			// Existing float value → unsupported.
			if _, err := g.Nodes().Add(context.Background(), []string{"Sensor"}, map[string]any{"reading": 1.5}); err != nil {
				t.Fatalf("add Sensor: %v", err)
			}
			err := g.Constraints().CreateUnique(context.Background(), "Sensor", "reading")
			if !errors.Is(err, graphpkg.ErrUniqueUnsupportedType) {
				t.Fatalf("CreateUnique over float err = %v, want ErrUniqueUnsupportedType", err)
			}
			if cs := g.Constraints().UniqueConstraints(); len(cs) != 0 {
				t.Fatalf("constraint installed over float data: %v", cs)
			}

			// Constraint on an int key, then a write introducing a float value.
			if err := g.Constraints().CreateUnique(context.Background(), "Item", "sku"); err != nil {
				t.Fatalf("CreateUnique(Item,sku): %v", err)
			}
			_, err = g.Nodes().Add(context.Background(), []string{"Item"}, map[string]any{"sku": 3.14})
			if !errors.Is(err, graphpkg.ErrUniqueUnsupportedType) {
				t.Fatalf("float write on constrained key err = %v, want ErrUniqueUnsupportedType", err)
			}
		})
	}
}

func TestUnique_DropAndDoubleCreate(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)

			// Double-create → exists.
			if err := g.Constraints().CreateUnique(context.Background(), "User", "email"); !errors.Is(err, graphpkg.ErrUniqueConstraintExists) {
				t.Fatalf("double CreateUnique err = %v, want ErrUniqueConstraintExists", err)
			}
			// Introspection sees exactly one, with scope current.
			cs := g.Constraints().UniqueConstraints()
			if len(cs) != 1 || cs[0].Label != "User" || cs[0].PropertyKey != "email" || cs[0].Scope != constraints.UniqueCurrent {
				t.Fatalf("UniqueConstraints = %+v, want [{User email current}]", cs)
			}
			// Drop, then writes no longer enforced.
			if err := g.Constraints().DropUnique(context.Background(), "User", "email"); err != nil {
				t.Fatalf("DropUnique: %v", err)
			}
			mustAddUser(t, g, "same@x.com")
			mustAddUser(t, g, "same@x.com") // no longer rejected
			// Drop again → not found.
			if err := g.Constraints().DropUnique(context.Background(), "User", "email"); !errors.Is(err, graphpkg.ErrUniqueConstraintNotFound) {
				t.Fatalf("second DropUnique err = %v, want ErrUniqueConstraintNotFound", err)
			}
		})
	}
}

// Two-phase temporal sanity: history rows carrying a duplicate value do NOT
// violate the CURRENT-scope constraint — only current nodes count.
func TestUnique_HistoryDuplicatesAreLegal(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)

			// (1) Node in state X (email=old) at t0.
			a := mustAddUser(t, g, "old@x.com")
			// (2) Mutate away — old@x.com now lives ONLY in a's history.
			if _, err := g.Nodes().Update(context.Background(), a.ID(), map[string]any{"email": "new@x.com"}); err != nil {
				t.Fatalf("update a: %v", err)
			}
			// (3) A second node claims the historical value — both alive, legal.
			b := mustAddUser(t, g, "old@x.com")

			// Both current rows exist with distinct current values.
			ga, _ := g.Nodes().Get(context.Background(), a.ID())
			gb, _ := g.Nodes().Get(context.Background(), b.ID())
			if ga == nil || gb == nil {
				t.Fatalf("expected both nodes alive")
			}
			va, _ := ga.GetProperty("email")
			vb, _ := gb.GetProperty("email")
			if va != "new@x.com" || vb != "old@x.com" {
				t.Fatalf("current emails = (%v,%v), want (new,old)", va, vb)
			}
		})
	}
}

// 100 goroutines create the SAME value under an active constraint: exactly one
// winner, N-1 ErrUniqueViolation.
func TestUnique_ConcurrentStormOneWinner(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)

			const goroutines = 100
			var wins atomic.Int32
			var violations atomic.Int32
			var others atomic.Int32
			var wg sync.WaitGroup
			wg.Add(goroutines)
			start := make(chan struct{})
			for i := 0; i < goroutines; i++ {
				go func() {
					defer wg.Done()
					<-start
					_, err := g.Nodes().Add(context.Background(), []string{"User"}, map[string]any{"email": "storm@x.com"})
					switch {
					case err == nil:
						wins.Add(1)
					case errors.Is(err, graphpkg.ErrUniqueViolation):
						violations.Add(1)
					default:
						others.Add(1)
					}
				}()
			}
			close(start)
			wg.Wait()

			if wins.Load() != 1 {
				t.Fatalf("winners = %d, want exactly 1", wins.Load())
			}
			if others.Load() != 0 {
				t.Fatalf("unexpected non-violation errors = %d", others.Load())
			}
			if violations.Load() != goroutines-1 {
				t.Fatalf("violations = %d, want %d", violations.Load(), goroutines-1)
			}
		})
	}
}

// Durability across a Close/reopen of the same badger dir: the constraint
// survives and continues to enforce.
func TestUnique_ReopenDurability(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 3, BadgerDir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	if err := g.Constraints().CreateUnique(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUnique: %v", err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "persist@x.com"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	g2, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 3, BadgerDir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer g2.Close()

	// Constraint is still registered.
	if cs := g2.Constraints().UniqueConstraints(); len(cs) != 1 || cs[0].Label != "User" || cs[0].PropertyKey != "email" {
		t.Fatalf("after reopen UniqueConstraints = %+v, want [{User email}]", cs)
	}
	// And still enforced.
	_, err = g2.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "persist@x.com"})
	if !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("post-reopen duplicate err = %v, want ErrUniqueViolation", err)
	}
}

// Admin().Reset reaps the durable constraint registry.
func TestUnique_ResetReaps(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)
			mustAddUser(t, g, "a@x.com")

			if err := g.Admin().Reset(); err != nil {
				t.Fatalf("Reset: %v", err)
			}
			if cs := g.Constraints().UniqueConstraints(); len(cs) != 0 {
				t.Fatalf("after Reset UniqueConstraints = %v, want empty", cs)
			}
			// Enforcement gone: duplicates now accepted.
			mustAddUser(t, g, "a@x.com")
			mustAddUser(t, g, "a@x.com")
		})
	}
}

// AddByIDIfAbsent (a caller-ID create door) is enforced.
func TestUnique_AddByIDIfAbsentEnforced(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)
			mustAddUser(t, g, "byid@x.com")

			_, _, err := g.Nodes().AddByIDIfAbsent(context.Background(), types.NodeID(999999), []string{"User"}, map[string]any{"email": "byid@x.com"})
			if !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("AddByIDIfAbsent-into-violation err = %v, want ErrUniqueViolation", err)
			}
		})
	}
}

// Sentinel identity: the graph alias equals the store-independent core sentinel
// and the violation error carries the label/key detail.
func TestUnique_ViolationErrorDetail(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 5})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer g.Close()
	mustCreateUnique(t, g)
	mustAddUser(t, g, "detail@x.com")
	_, err = g.Nodes().Add(context.Background(), []string{"User"}, map[string]any{"email": "detail@x.com"})
	if !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("err = %v, want ErrUniqueViolation", err)
	}
	msg := err.Error()
	if want := "email"; !strings.Contains(msg, want) {
		t.Fatalf("violation message %q missing key %q", msg, want)
	}
}

// A backend without MetaKV declines with ErrCapabilityNotSupported. The shim
// embeds the MandatoryStore INTERFACE (not the concrete memory.Store), so the
// optional MetaKVCapability methods are not promoted and the type assertion in
// uniqueMetaKV misses.
func TestUnique_DeclinesWithoutMetaKV(t *testing.T) {
	shim := noMetaKVStore{MandatoryStore: memory.New()}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 7, Store: shim})
	if err != nil {
		t.Fatalf("new over shim: %v", err)
	}
	defer g.Close()
	if _, ok := interface{}(shim).(storepkg.MetaKVCapability); ok {
		t.Fatalf("shim still advertises MetaKVCapability; test would not exercise the decline path")
	}
	err = g.Constraints().CreateUnique(context.Background(), "User", "email")
	if !errors.Is(err, graphpkg.ErrCapabilityNotSupported) {
		t.Fatalf("CreateUnique without MetaKV err = %v, want ErrCapabilityNotSupported", err)
	}
}

// noMetaKVStore wraps a MandatoryStore hiding every optional capability that is
// not part of the mandatory contract — in particular MetaKVCapability.
type noMetaKVStore struct {
	storepkg.MandatoryStore
}

// The tiered backend now supports unique constraints on REFERENCE labels
// (ADR-0005 §3.5 supersedes ADR-0002 Decision 5) and rejects only EVENT labels
// with ErrUniqueEventLabelUnsupported. Those behaviors are covered end-to-end in
// unique_tiered_test.go.
