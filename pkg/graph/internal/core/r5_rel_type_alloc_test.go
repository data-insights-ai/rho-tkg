// Tests in this file pin R5-F6 from the 2026-05-09 maintainability
// review: rel-type token allocation must happen AFTER every
// endpoint-fetch failure path, not before. Round 4 (R4-F14) deferred
// allocation past cheap validation gates (self-loop, ID==0, duplicate
// ID); R5-F6 pushes it past the operational store-error paths so a
// missing endpoint, store error, or context cancellation that occurs
// after the cheap gates does not leave a permanent rel-type
// registration.
package core

import (
	"context"
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
)

// R5-F6: when a store error makes the live-endpoint fetch fail in
// Rels.Add, the rel-type token must not be registered.
func TestR5_RelAdd_EndpointFetchError_DoesNotAllocateRelTypeToken(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic endpoint fetch fault")
	fs := &nodeProbeFaultStore{Store: memory.New(), err: injected}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Arm the fault store to fail on the next GetNode for `a`.
	// addRelationshipInternal calls GetNode(startID) under the
	// endpoint lock; the fault triggers there and the function must
	// abort BEFORE relTypes.GetOrCreate runs.
	fs.target = a.ID()
	fs.enabled = true

	if _, err := g.Rels.Add("FETCH_FAULT_TYPE", a, b, nil); !errors.Is(err, injected) {
		t.Fatalf("expected wrapped fault, got %v", err)
	}

	if _, ok := g.relTypes.Lookup("FETCH_FAULT_TYPE"); ok {
		t.Errorf("FETCH_FAULT_TYPE registered despite endpoint-fetch failure (R5-F6)")
	}
}

// R5-F6: same protection on Rels.Import. A live-endpoint fetch
// failure must abort before the rel-type token is allocated.
func TestR5_RelImport_EndpointFetchError_DoesNotAllocateRelTypeToken(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic endpoint fetch fault")
	fs := &nodeProbeFaultStore{Store: memory.New(), err: injected}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	relID := g.Rels.NextID()
	fs.target = a.ID()
	fs.enabled = true

	if _, err := g.Rels.Import(context.Background(), relID, "IMPORT_FETCH_FAULT", a, b, nil); !errors.Is(err, injected) {
		t.Fatalf("expected wrapped fault, got %v", err)
	}

	if _, ok := g.relTypes.Lookup("IMPORT_FETCH_FAULT"); ok {
		t.Errorf("IMPORT_FETCH_FAULT registered despite endpoint-fetch failure (R5-F6)")
	}
}

func TestR5_RelCreate_PutFailureDoesNotKeepNewRelTypeToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*Core, string) error
	}{
		{
			name: "Add",
			run: func(g *Core, typ string) error {
				a, err := g.Nodes.Add([]string{"A"}, nil)
				if err != nil {
					return err
				}
				b, err := g.Nodes.Add([]string{"B"}, nil)
				if err != nil {
					return err
				}
				_, err = g.Rels.Add(typ, a, b, nil)
				return err
			},
		},
		{
			name: "AddByID",
			run: func(g *Core, typ string) error {
				a, err := g.Nodes.Add([]string{"A"}, nil)
				if err != nil {
					return err
				}
				b, err := g.Nodes.Add([]string{"B"}, nil)
				if err != nil {
					return err
				}
				_, err = g.Rels.AddByID(typ, a.ID(), b.ID(), nil)
				return err
			},
		},
		{
			name: "AddByIDIfAbsent",
			run: func(g *Core, typ string) error {
				a, err := g.Nodes.Add([]string{"A"}, nil)
				if err != nil {
					return err
				}
				b, err := g.Nodes.Add([]string{"B"}, nil)
				if err != nil {
					return err
				}
				_, _, err = g.Rels.AddByIDIfAbsent(typ, a.ID(), b.ID(), nil)
				return err
			},
		},
		{
			name: "Import",
			run: func(g *Core, typ string) error {
				a, err := g.Nodes.Add([]string{"A"}, nil)
				if err != nil {
					return err
				}
				b, err := g.Nodes.Add([]string{"B"}, nil)
				if err != nil {
					return err
				}
				_, err = g.Rels.Import(context.Background(), g.nextRelID(), typ, a, b, nil)
				return err
			},
		},
		{
			name: "BatchExecute",
			run: func(g *Core, typ string) error {
				bb, err := NewBatchBuilder(g)
				if err != nil {
					return err
				}
				a, err := bb.AddNode([]string{"A"}, nil)
				if err != nil {
					return err
				}
				b, err := bb.AddNode([]string{"B"}, nil)
				if err != nil {
					return err
				}
				if _, err := bb.AddRelationship(typ, a, b, nil); err != nil {
					return err
				}
				result, err := bb.Execute()
				if err == nil {
					return nil
				}
				if result != nil && len(result.Errors) > 0 {
					return result.Errors[0].Err
				}
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			injected := errors.New("synthetic PutRelationship fault")
			g, err := New(Config{Store: &failPutRelationshipStore{Store: memory.New(), err: injected}})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()

			typ := "PUT_FAIL_" + tc.name
			if err := tc.run(g, typ); !errors.Is(err, injected) {
				t.Fatalf("%s error = %v, want injected PutRelationship fault", tc.name, err)
			}
			if _, ok := g.relTypes.Lookup(typ); ok {
				t.Fatalf("%s kept rel type token %q after PutRelationship failure", tc.name, typ)
			}
		})
	}
}

// R5-F6: the AddByIDIfAbsent path must not allocate the rel-type
// token when the duplicate-existence check finds nothing in the
// registry — there's nothing to look up, so we know there are no
// duplicates without registering. Allocation must only happen on
// the actual create path. (Verifying no token is registered AFTER a
// successful create requires the type token to BE there afterwards;
// this test instead checks the negative case where IfAbsent finds
// the type already absent, then a forced store-failure aborts the
// create before allocation. Use a custom store that fails
// PutRelationship — the IfAbsent path then tries to allocate before
// PutRelationship, so verify allocation only happens after the
// "type unknown → skip OutgoingRelationships" branch.)
//
// Implementation note: PutRelationship still needs a typed relationship object,
// so token allocation happens before the final store call. If that final store
// call fails, the newly allocated token must be rolled back; the separate
// PutRelationship-failure test above pins that post-allocation window.
func TestR5_RelAddByIDIfAbsent_VacuousDupCheck_StillAllocatesOnCreate(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	defer g.Close()

	a, err := g.Nodes.Add([]string{"X"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add([]string{"X"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Type was never registered before this call.
	if _, ok := g.relTypes.Lookup("FRESH_TYPE"); ok {
		t.Fatal("FRESH_TYPE pre-existed; test premise broken")
	}

	r, created, err := g.Rels.AddByIDIfAbsentWithContext(context.Background(), "FRESH_TYPE", a.ID(), b.ID(), nil)
	if err != nil {
		t.Fatalf("AddByIDIfAbsent: %v", err)
	}
	if !created {
		t.Fatal("expected created=true on first call")
	}
	if r == nil {
		t.Fatal("expected non-nil rel")
	}

	// On the create path, the token IS expected to be registered now.
	if _, ok := g.relTypes.Lookup("FRESH_TYPE"); !ok {
		t.Errorf("FRESH_TYPE should be registered after successful create")
	}
}
