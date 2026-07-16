package core

import (
	"context"
	"errors"
	"sync"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// N concurrent sessions (§14 concurrent mode) self-apply creates + rels in
// parallel: every write is visible after its Submit returns, every chain
// verifies, counts add up, and tokens carry a nonzero lane with WaitApplied
// already resolved. Run under -race.
func TestIngestConcurrentParallelSessions(t *testing.T) {
	t.Parallel()
	c := newIngestGraph(t)
	ctx := context.Background()

	const sessions = 6
	const perSession = 25
	var wg sync.WaitGroup
	ids := make([][]types.NodeID, sessions)
	lanes := make([]uint16, sessions)
	errCh := make(chan error, sessions)
	for w := 0; w < sessions; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			sess, err := c.Ingest.NewSession(IngestOptions{Concurrent: true})
			if err != nil {
				errCh <- err
				return
			}
			defer sess.Close()
			var mine []types.NodeID
			var prev *types.Node
			for i := 0; i < perSession; i++ {
				n, err := sess.AddNode([]string{"Event"}, map[string]any{"w": int64(w), "i": int64(i)})
				if err != nil {
					errCh <- err
					return
				}
				mine = append(mine, n.ID())
				if prev != nil {
					if _, err := sess.AddRelationship("NEXT", prev, n, nil); err != nil {
						errCh <- err
						return
					}
				}
				prev = n
				if i%5 == 4 { // submit in sub-groups to exercise multiple applies
					token, err := sess.Submit()
					if err != nil {
						errCh <- err
						return
					}
					if token.Lane == 0 {
						errCh <- errors.New("concurrent token has lane 0")
						return
					}
					lanes[w] = token.Lane
					if err := c.Ingest.WaitApplied(token); err != nil {
						errCh <- err
						return
					}
					// Read-your-writes immediately after a concurrent Submit.
					if _, err := c.Nodes.Get(context.Background(), mine[len(mine)-1]); err != nil {
						errCh <- err
						return
					}
				}
			}
			ids[w] = mine
			errCh <- nil
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("session: %v", err)
		}
	}

	// Distinct sessions got distinct lanes.
	seen := map[uint16]bool{}
	for _, l := range lanes {
		if l == 0 {
			t.Fatal("session never submitted / lane 0")
		}
		if seen[l] {
			t.Fatalf("lane %d reused across live sessions", l)
		}
		seen[l] = true
	}

	total := 0
	for w := range ids {
		total += len(ids[w])
		for _, id := range ids[w] {
			n, err := c.Nodes.Get(ctx, id)
			if err != nil {
				t.Fatalf("Get(%d): %v", id, err)
			}
			if tm := n.Temporal(); tm == nil || tm.TxFrom == 0 {
				t.Fatalf("node %d missing TxFrom stamp", id)
			}
			ok, err := c.Hash.VerifyNodeChain(id)
			if !ok || err != nil {
				t.Fatalf("VerifyNodeChain(%d) = (%v, %v)", id, ok, err)
			}
		}
	}
	if want := sessions * perSession; total != want {
		t.Fatalf("created %d nodes, want %d", total, want)
	}
}

// TestIngestConcurrentBulkAddNodes exercises the write-only bulk door
// (Session.AddNodes) across concurrent sessions: each queues count-per-chunk
// nodes with NO caller-visible skeleton (so prepare skips the isolation
// DeepCopy — lever #2), submits, and every created node must be persisted,
// TxFrom-stamped, and hash-valid. The exact total guards against the bulk door
// silently dropping or over-creating nodes.
func TestIngestConcurrentBulkAddNodes(t *testing.T) {
	t.Parallel()
	c := newIngestGraph(t)

	const sessions = 6
	const chunks = 4
	const perChunk = 25
	var wg sync.WaitGroup
	errCh := make(chan error, sessions)
	for w := 0; w < sessions; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			sess, err := c.Ingest.NewSession(IngestOptions{Concurrent: true})
			if err != nil {
				errCh <- err
				return
			}
			defer sess.Close()
			for ch := 0; ch < chunks; ch++ {
				if err := sess.AddNodes([]string{"Bulk"}, map[string]any{"w": int64(w), "ch": int64(ch)}, perChunk); err != nil {
					errCh <- err
					return
				}
				if _, err := sess.Submit(); err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("bulk session: %v", err)
		}
	}

	got, err := c.Nodes.ByLabel("Bulk", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabel: %v", err)
	}
	if want := sessions * chunks * perChunk; len(got) != want {
		t.Fatalf("bulk created %d nodes, want %d", len(got), want)
	}
	for _, n := range got {
		if tm := n.Temporal(); tm == nil || tm.TxFrom == 0 {
			t.Fatalf("bulk node %d missing TxFrom stamp", n.ID())
		}
		ok, err := c.Hash.VerifyNodeChain(n.ID())
		if !ok || err != nil {
			t.Fatalf("VerifyNodeChain(%d) = (%v, %v)", n.ID(), ok, err)
		}
	}
}

// A unique-constraint storm across concurrent sessions: every session races the
// SAME constrained value — exactly one wins, every loser's Submit surfaces
// ErrUniqueViolation directly (Submit is the truth channel in concurrent mode).
func TestIngestConcurrentUniqueStorm(t *testing.T) {
	t.Parallel()
	c := newIngestGraph(t)
	ctx := context.Background()

	if err := c.Constraints.CreateUnique(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUnique: %v", err)
	}

	const racers = 8
	var wg sync.WaitGroup
	outcomes := make([]error, racers)
	for w := 0; w < racers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			sess, err := c.Ingest.NewSession(IngestOptions{Concurrent: true})
			if err != nil {
				outcomes[w] = err
				return
			}
			defer sess.Close()
			if _, err := sess.AddNode([]string{"User"}, map[string]any{"email": "x@y.z", "w": int64(w)}); err != nil {
				outcomes[w] = err
				return
			}
			_, outcomes[w] = sess.Submit()
		}(w)
	}
	wg.Wait()

	winners, losers := 0, 0
	for w, err := range outcomes {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, ErrUniqueViolation):
			losers++
		default:
			t.Fatalf("racer %d: unexpected outcome %v", w, err)
		}
	}
	if winners != 1 || losers != racers-1 {
		t.Fatalf("winners=%d losers=%d, want 1/%d", winners, losers, racers-1)
	}
	nodes, err := c.Nodes.ByLabel("User", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabel: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("%d User nodes persisted, want exactly 1", len(nodes))
	}
}

// Two-phase bitemporal through a concurrent session (Testing Rule 15): create
// state X, mutate it, pin knowledge time between the two — the pin must reflect
// X, not the post-mutation state. Also covers update + delete intents through
// the concurrent apply path (history queryable after deletion).
func TestIngestConcurrentBitemporalTwoPhase(t *testing.T) {
	t.Parallel()
	c := newIngestGraph(t)
	ctx := context.Background()

	sess, err := c.Ingest.NewSession(IngestOptions{Concurrent: true})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer sess.Close()

	n, err := sess.AddNode([]string{"Doc"}, map[string]any{"state": "X"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	victim, err := sess.AddNode([]string{"Doc"}, map[string]any{"state": "V"})
	if err != nil {
		t.Fatalf("AddNode victim: %v", err)
	}
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit create: %v", err)
	}

	created, err := c.Nodes.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	pin := created.Temporal().TxFrom

	if err := sess.UpdateNode(id, map[string]any{"state": "Y"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	if err := sess.DeleteNode(victim.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit mutate: %v", err)
	}

	// Pinned at creation knowledge time: state X, not Y.
	asof, err := c.Temporal.NodeAsOf(id, pin)
	if err != nil {
		t.Fatalf("NodeAsOf: %v", err)
	}
	if got, _ := asof.GetProperty("state"); got != "X" {
		t.Fatalf("as-of pin state = %v, want X (pre-update belief)", got)
	}
	cur, err := c.Nodes.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get current: %v", err)
	}
	if got, _ := cur.GetProperty("state"); got != "Y" {
		t.Fatalf("current state = %v, want Y", got)
	}

	// Deleted entity: gone from live state, history remains queryable.
	if _, err := c.Nodes.Get(ctx, victim.ID()); err == nil {
		t.Fatal("victim still live after delete")
	}
	hist, err := c.Nodes.History(victim.ID())
	if err != nil || len(hist) == 0 {
		t.Fatalf("victim history after delete: %d rows, err=%v", len(hist), err)
	}
}

// Submit racing Close: a concurrent session's Submit either applies fully
// (the ack is synchronous — by the time Submit returns nil the data is in the
// store) or fails cleanly with a closed-graph sentinel — never a hang, never a
// panic, never an accepted-then-dropped group (there is no queue to drop from).
// Run under -race.
func TestIngestConcurrentSubmitVsClose(t *testing.T) {
	t.Parallel()
	for iter := 0; iter < 30; iter++ {
		g, err := New(Config{Store: memory.New()})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		const racers = 4
		var wg sync.WaitGroup
		for w := 0; w < racers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				sess, err := g.Ingest.NewSession(IngestOptions{Concurrent: true})
				if err != nil {
					return // graph already closed — fine
				}
				for i := 0; i < 10; i++ {
					if _, err := sess.AddNode([]string{"E"}, map[string]any{"w": int64(w), "i": int64(i)}); err != nil {
						return // closed mid-prepare — fine
					}
					if _, err := sess.Submit(); err != nil {
						if errors.Is(err, ErrGraphClosed) || errors.Is(err, ErrIngestClosed) {
							return
						}
						t.Errorf("iter %d racer %d: unexpected Submit error: %v", iter, w, err)
						return
					}
				}
			}(w)
		}
		g.Close() // race the submitters
		wg.Wait()
	}
}
