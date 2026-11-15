package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var errOwnerChanged = errors.New("test publication: owner changed")

// These helpers are a host protocol built from public APIs, not a new graph
// isolation guarantee. Every writer of the owner property must participate.
func ownerProtocolCAS(ctx context.Context, g *graphpkg.Graph, head types.NodeID, expected, replacement int64) (bool, error) {
	var matched bool
	err := g.Tx().RunContext(ctx, func(*graphpkg.GraphTx) error {
		var err error
		matched, err = g.Nodes().CompareAndSetProperty(ctx, head, "epoch", expected, replacement)
		return err
	})
	return matched, err
}

func ownerProtocolPart(ctx context.Context, g *graphpkg.Graph, head types.NodeID, expected int64, apply func(*graphpkg.GraphTx) error) error {
	return g.Tx().RunContext(ctx, func(tx *graphpkg.GraphTx) error {
		node, err := g.Nodes().GetCtx(ctx, head)
		if err != nil {
			return err
		}
		if found, equal := node.PropertyValueEqual("epoch", expected); !found || !equal {
			return errOwnerChanged
		}
		return apply(tx)
	})
}

// Deliberately bypass the protocol synchronously: a raw CAS can hand over
// ownership between a tx's guard and its payload. This negative control has
// no scheduler or timeout dependency and pins the documented isolation limit.
func TestTxOwnerProtocolRequiresParticipatingCAS(t *testing.T) {
	t.Parallel()
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		ctx := t.Context()
		head, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"epoch": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		var payload types.NodeID
		err = ownerProtocolPart(ctx, g, head.ID(), 1, func(tx *graphpkg.GraphTx) error {
			matched, err := g.Nodes().CompareAndSetProperty(ctx, head.ID(), "epoch", int64(1), int64(2))
			if err != nil {
				return err
			}
			if !matched {
				return errors.New("raw CAS control did not match")
			}
			node, err := tx.AddNode([]string{"Derived"}, map[string]any{"epoch": int64(1)})
			if node != nil {
				payload = node.ID()
			}
			return err
		})
		if err != nil || payload == 0 {
			t.Fatalf("raw CAS bypass control: err=%v payload=%v", err, payload)
		}
		node, err := g.Nodes().Get(ctx, payload)
		if err != nil {
			t.Fatal(err)
		}
		if found, equal := node.PropertyValueEqual("epoch", int64(1)); !found || !equal {
			t.Fatal("control did not commit the stale epoch")
		}
	})
}

func TestTxOwnerProtocolFenceOrdering(t *testing.T) {
	t.Parallel()
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		ctx := t.Context()
		head, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"epoch": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		var output types.NodeID
		var matched bool
		var fenceErr error
		attempted, fenced := make(chan struct{}), make(chan struct{})
		err = ownerProtocolPart(ctx, g, head.ID(), 1, func(tx *graphpkg.GraphTx) error {
			go func() {
				close(attempted)
				matched, fenceErr = ownerProtocolCAS(ctx, g, head.ID(), 1, 2)
				close(fenced)
			}()
			<-attempted
			select {
			case <-fenced:
				return fmt.Errorf("owner fence completed between check and payload: matched=%v error=%v", matched, fenceErr)
			case <-time.After(25 * time.Millisecond):
			}

			// A normal standalone mutation still completes while the part owns
			// transaction serialization. It is not pulled into tx rollback.
			standalone := make(chan error, 1)
			go func() {
				_, err := g.Nodes().Add(ctx, []string{"Independent"}, nil)
				standalone <- err
			}()
			select {
			case err := <-standalone:
				if err != nil {
					return err
				}
			case <-time.After(time.Second):
				return errors.New("unrelated standalone mutation blocked behind owner part")
			}
			node, err := tx.AddNode([]string{"Derived"}, map[string]any{"epoch": int64(1)})
			if node != nil {
				output = node.ID()
			}
			return err
		})
		select {
		case <-fenced:
		case <-time.After(time.Second):
			t.Fatal("fence did not finish after part released transaction serialization")
		}
		if err != nil || !matched || fenceErr != nil {
			t.Fatalf("part=%v, fence=(%v, %v)", err, matched, fenceErr)
		}
		if output == 0 {
			t.Fatal("non-empty control did not write its payload")
		}
		if _, err := g.Nodes().Get(ctx, output); err != nil {
			t.Fatalf("committed payload: %v", err)
		}
		called := false
		err = ownerProtocolPart(ctx, g, head.ID(), 1, func(*graphpkg.GraphTx) error {
			called = true
			return nil
		})
		if !errors.Is(err, errOwnerChanged) || called {
			t.Fatalf("stale part: err=%v, payload called=%v", err, called)
		}
		nodes, err := g.Nodes().ByLabel("Derived", storepkg.QueryOpts{})
		if err != nil || len(nodes) != 1 || nodes[0].ID() != output {
			t.Fatalf("stale part changed the exact output set: nodes=%v, err=%v", nodes, err)
		}
	})
}

func TestTxOwnerProtocolPartRollback(t *testing.T) {
	t.Parallel()
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		head, err := g.Nodes().Add(t.Context(), []string{"Ref"}, map[string]any{"epoch": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		for _, mode := range []string{"error", "cancel", "panic"} {
			t.Run(mode, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var payload types.NodeID
				cause := errors.New("part failed")
				var panicValue any
				err := func() (err error) {
					defer func() { panicValue = recover() }()
					return ownerProtocolPart(ctx, g, head.ID(), 1, func(tx *graphpkg.GraphTx) error {
						node, err := tx.AddNode([]string{"Derived"}, map[string]any{"epoch": int64(1)})
						if err != nil {
							return err
						}
						payload = node.ID()
						switch mode {
						case "error":
							return cause
						case "cancel":
							cancel()
						case "panic":
							panic(cause)
						}
						return nil
					})
				}()
				if mode == "error" && !errors.Is(err, cause) || mode == "cancel" && !errors.Is(err, context.Canceled) || mode == "panic" && panicValue != cause {
					t.Fatalf("part error=%v, panic=%v", err, panicValue)
				}
				if payload == 0 {
					t.Fatal("rollback control did not reach a payload write")
				}
				if _, err := g.Nodes().Get(t.Context(), payload); !errors.Is(err, graphpkg.ErrNodeNotFound) {
					t.Fatalf("rolled-back payload survived: %v", err)
				}
				if matched, err := ownerProtocolCAS(t.Context(), g, head.ID(), 1, 1); !matched || err != nil {
					t.Fatalf("owner changed or transaction serialization leaked: %v, %v", matched, err)
				}
			})
		}
	})
}

// A finite read allowance is shared by the work before the part and its head
// check. Count real native decodes independently of that allowance. GraphTx's
// unmetered GetNode is deliberately not used by the protocol helper.
func TestTxOwnerProtocolReadAdmission(t *testing.T) {
	bs, err := badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: bs})
	if err != nil {
		_ = bs.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	var heads []types.NodeID
	for range 2 {
		node, err := g.Nodes().Add(t.Context(), []string{"Ref"}, map[string]any{"epoch": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		heads = append(heads, node.ID())
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []uint64{0, 2, 1} {
		t.Run(fmt.Sprintf("candidates-%d", limit), func(t *testing.T) {
			bs.NodeCacheForTest().ResetForTest()
			ctx, meter := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{Candidates: limit})
			var payload types.NodeID
			var gotErr error
			decoded := badger.CountRowDecodesForTest(func() {
				if _, gotErr = g.Nodes().GetCtx(ctx, heads[0]); gotErr != nil {
					return
				}
				gotErr = ownerProtocolPart(ctx, g, heads[1], 1, func(tx *graphpkg.GraphTx) error {
					n, err := tx.AddNode([]string{"Derived"}, map[string]any{"epoch": int64(1)})
					if n != nil {
						payload = n.ID()
					}
					return err
				})
			})
			if meter.Work().Candidates != 2 {
				t.Fatalf("allowance restarted at transaction: %+v", meter.Work())
			}
			if limit == 1 {
				var refusal *storepkg.ReadBudgetError
				if !errors.As(gotErr, &refusal) || refusal.Limit != "candidates" || payload != 0 || decoded != 1 {
					t.Fatalf("refusal=%v, output=%v, native decodes=%d; want refusal before second decode and payload", gotErr, payload, decoded)
				}
			} else if gotErr != nil || payload == 0 || decoded != 2 {
				t.Fatalf("control: error=%v, output=%v, native decodes=%d; want one payload after two decodes", gotErr, payload, decoded)
			}
		})
	}
}

func TestTxOwnerProtocolCASIsNotRolledBack(t *testing.T) {
	t.Parallel()
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		head, err := g.Nodes().Add(t.Context(), []string{"Ref"}, map[string]any{"epoch": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		matched := false
		err = g.Tx().RunContext(ctx, func(*graphpkg.GraphTx) error {
			var err error
			matched, err = g.Nodes().CompareAndSetProperty(ctx, head.ID(), "epoch", int64(1), int64(2))
			cancel()
			return err
		})
		if !matched || !errors.Is(err, context.Canceled) {
			t.Fatalf("CAS result=%v, wrapper error=%v", matched, err)
		}
		node, err := g.Nodes().Get(t.Context(), head.ID())
		if err != nil {
			t.Fatal(err)
		}
		if found, equal := node.PropertyValueEqual("epoch", int64(2)); !found || !equal {
			t.Fatal("standalone CAS was unexpectedly rolled back")
		}
		if matched, err := ownerProtocolCAS(t.Context(), g, head.ID(), 1, 3); matched || err != nil {
			t.Fatalf("old owner reused after ambiguous outcome: matched=%v error=%v", matched, err)
		}
	})
}

func TestTxOwnerProtocolCASCommitNotDurable(t *testing.T) {
	t.Parallel()
	g, bs := newDurableFailGraph(t, true)
	head, err := g.Nodes().Add(t.Context(), []string{"Ref"}, map[string]any{"epoch": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.DurableFlush(); err != nil {
		t.Fatal(err)
	}
	bs.FailNextDurableSyncForTest(errInjectedFlush)
	matched, err := ownerProtocolCAS(t.Context(), g, head.ID(), 1, 2)
	if !matched || !errors.Is(err, graphpkg.ErrCommitNotDurable) || !errors.Is(err, errInjectedFlush) {
		t.Fatalf("CAS=%v, commit=%v; want successful CAS and uncertain durability", matched, err)
	}
	if matched, err := ownerProtocolCAS(t.Context(), g, head.ID(), 1, 3); matched || err != nil {
		t.Fatalf("failed flush reinstated old owner: matched=%v, err=%v", matched, err)
	}
	if err := bs.DurableFlush(); err != nil {
		t.Fatal(err)
	}
	changes, err := g.Replication().ChangeFeed(0, 100)
	if err != nil {
		t.Fatal(err)
	}
	puts := 0
	for _, change := range changes {
		if change.Tag == storepkg.ChangeNodePut {
			puts++
		}
	}
	if puts != 3 { // seed Signal, initial head, and the standalone head CAS
		t.Fatalf("CAS record lost to transaction scope: node puts=%d, want 3", puts)
	}
}
