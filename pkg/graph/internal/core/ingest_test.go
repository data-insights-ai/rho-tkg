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

func newIngestGraph(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

func newIngestGraphWithLog(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{Store: memory.New(memory.WithChangeLog())})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

// TestIngestSyncSubmitVisibleToAnyGoroutine — the §4.6 SYNC freshness contract:
// once a sync Submit returns, ANY subsequent read on ANY goroutine observes the
// write.
func TestIngestSyncSubmitVisibleToAnyGoroutine(t *testing.T) {
	t.Parallel()
	c := newIngestGraph(t)

	sess, err := c.Ingest.NewSession(IngestOptions{Sync: true})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	n, err := sess.AddNode([]string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Reader on this goroutine.
	got, err := c.Nodes.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get on submitter goroutine: %v", err)
	}
	if got.ID() != id {
		t.Fatalf("wrong node")
	}

	// Reader on a DIFFERENT goroutine, started after the ack.
	errc := make(chan error, 1)
	go func() {
		_, e := c.Nodes.Get(context.Background(), id)
		errc <- e
	}()
	if e := <-errc; e != nil {
		t.Fatalf("Get on concurrent goroutine after ack: %v", e)
	}
}

// TestIngestAsyncTokenWaitApplied — the §4.6 ASYNC contract: Submit returns a
// (lane, seq) token without blocking on apply; WaitApplied(token) then
// guarantees visibility, and AppliedSeq advances to at least token.Seq.
func TestIngestAsyncTokenWaitApplied(t *testing.T) {
	t.Parallel()
	c := newIngestGraph(t)

	sess, err := c.Ingest.NewSession(IngestOptions{Sync: false})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	n, err := sess.AddNode([]string{"Event"}, map[string]any{"kind": "click"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	token, err := sess.Submit()
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if token.Seq == 0 {
		t.Fatalf("async submit returned zero token")
	}

	if err := c.Ingest.WaitApplied(token); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}
	if c.Ingest.AppliedSeq() < token.Seq {
		t.Fatalf("AppliedSeq %d < token.Seq %d", c.Ingest.AppliedSeq(), token.Seq)
	}
	if _, err := c.Nodes.Get(context.Background(), id); err != nil {
		t.Fatalf("Get after WaitApplied: %v", err)
	}
}

// TestIngestParallelProducersChainIntegrity — many producer goroutines prepare
// in parallel; every pipeline-written node passes VerifyNodeChain and the total
// count is exact (invariant: per-entity chain integrity + no lost/duplicated
// writes).
func TestIngestParallelProducersChainIntegrity(t *testing.T) {
	t.Parallel()
	c := newIngestGraph(t)

	const producers = 8
	const perProducer = 200
	var wg sync.WaitGroup
	idsMu := sync.Mutex{}
	var ids []types.NodeID

	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			sess, err := c.Ingest.NewSession(IngestOptions{Sync: true, DeclareLabels: []string{"Row"}})
			if err != nil {
				t.Errorf("NewSession: %v", err)
				return
			}
			var local []types.NodeID
			for i := 0; i < perProducer; i++ {
				n, err := sess.AddNode([]string{"Row"}, map[string]any{"p": int64(p), "i": int64(i)})
				if err != nil {
					t.Errorf("AddNode: %v", err)
					return
				}
				local = append(local, n.ID())
			}
			if _, err := sess.Submit(); err != nil {
				t.Errorf("Submit: %v", err)
				return
			}
			idsMu.Lock()
			ids = append(ids, local...)
			idsMu.Unlock()
		}(p)
	}
	wg.Wait()

	if len(ids) != producers*perProducer {
		t.Fatalf("prepared %d ids, want %d", len(ids), producers*perProducer)
	}
	count, err := c.Nodes.Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != producers*perProducer {
		t.Fatalf("Count = %d, want %d", count, producers*perProducer)
	}
	// Sample chain verification (exhaustive would be slow; check every 37th).
	for i := 0; i < len(ids); i += 37 {
		ok, err := c.Hash.VerifyNodeChain(ids[i])
		if err != nil || !ok {
			t.Fatalf("VerifyNodeChain(%d) = (%v, %v)", ids[i], ok, err)
		}
	}
}

// TestIngestTxFromStampedAndMonotonicAcrossGroups — every pipeline-written node
// carries an applier-owned TxFrom stamped from the shared monotonic clock
// (lesson 20). Within one commit group the distinct fresh-ID entities share the
// group's commit instant (each is its own genesis chain — per-entity
// monotonicity is unaffected, lesson 62); across separate groups TxFrom is
// strictly increasing (single applier, one clock).
func TestIngestTxFromStampedAndMonotonicAcrossGroups(t *testing.T) {
	t.Parallel()
	c := newIngestGraph(t)

	sess, err := c.Ingest.NewSession(IngestOptions{Sync: true})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	var groupTx []types.Instant
	for g := 0; g < 6; g++ {
		var ids []types.NodeID
		for i := 0; i < 5; i++ {
			n, err := sess.AddNode([]string{"N"}, map[string]any{"g": int64(g), "i": int64(i)})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			ids = append(ids, n.ID())
		}
		if _, err := sess.Submit(); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		var tx types.Instant
		for _, id := range ids {
			n, err := c.Nodes.Get(context.Background(), id)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if n.Temporal().TxFrom == 0 {
				t.Fatalf("node %d has unstamped TxFrom", id)
			}
			if tx == 0 {
				tx = n.Temporal().TxFrom
			}
		}
		groupTx = append(groupTx, tx)
	}
	for i := 1; i < len(groupTx); i++ {
		if groupTx[i] <= groupTx[i-1] {
			t.Fatalf("group TxFrom not increasing: %v", groupTx)
		}
	}
}

// TestIngestLSNGapless — with a change-log enabled, the pipeline mints one
// gapless, monotonic LSN per committed mutation via the existing co-commit
// (invariant: LSN gaplessness).
func TestIngestLSNGapless(t *testing.T) {
	t.Parallel()
	c := newIngestGraphWithLog(t)

	sess, err := c.Ingest.NewSession(IngestOptions{Sync: true})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	const n = 30
	for i := 0; i < n; i++ {
		if _, err := sess.AddNode([]string{"L"}, map[string]any{"i": int64(i)}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	last, err := c.Repl.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if last != n {
		t.Fatalf("LastCommittedLSN = %d, want %d", last, n)
	}
	recs, err := c.Repl.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != n {
		t.Fatalf("feed has %d records, want %d", len(recs), n)
	}
	for i, r := range recs {
		if r.LSN != uint64(i+1) {
			t.Fatalf("record %d has LSN %d, want %d (gap)", i, r.LSN, i+1)
		}
	}
}

// TestIngestTxCoexistenceNoDeadlock — an interactive tx and the ingest pipeline
// coexist: both complete under a storm with no deadlock (§4.3 / §14 — they
// serialize at c.txMu group granularity).
func TestIngestTxCoexistenceNoDeadlock(t *testing.T) {
	t.Parallel()
	c := newIngestGraph(t)

	var wg sync.WaitGroup
	// Interactive tx storm.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			tx, err := c.BeginTx()
			if err != nil {
				t.Errorf("BeginTx: %v", err)
				return
			}
			if _, err := tx.AddNode([]string{"TxNode"}, map[string]any{"i": int64(i)}); err != nil {
				t.Errorf("tx.AddNode: %v", err)
				_ = tx.Rollback()
				return
			}
			if err := tx.Commit(); err != nil {
				t.Errorf("tx.Commit: %v", err)
				return
			}
		}
	}()
	// Ingest storm.
	wg.Add(1)
	go func() {
		defer wg.Done()
		sess, err := c.Ingest.NewSession(IngestOptions{Sync: true})
		if err != nil {
			t.Errorf("NewSession: %v", err)
			return
		}
		for i := 0; i < 30; i++ {
			if _, err := sess.AddNode([]string{"IngestNode"}, map[string]any{"i": int64(i)}); err != nil {
				t.Errorf("AddNode: %v", err)
				return
			}
			if _, err := sess.Submit(); err != nil {
				t.Errorf("Submit: %v", err)
				return
			}
		}
	}()
	wg.Wait()

	tx, _ := c.Nodes.CountByLabel("TxNode")
	ing, _ := c.Nodes.CountByLabel("IngestNode")
	if tx != 30 || ing != 30 {
		t.Fatalf("counts: TxNode=%d IngestNode=%d, want 30/30", tx, ing)
	}
}

// TestIngestRelCreate — nodes plus a relationship through the pipeline; the rel
// exists and its chain verifies (endpoint-hash ladder ran apply-side).
func TestIngestRelCreate(t *testing.T) {
	t.Parallel()
	c := newIngestGraph(t)

	sess, err := c.Ingest.NewSession(IngestOptions{Sync: true, DeclareRelTypes: []string{"KNOWS"}})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	a, err := sess.AddNode([]string{"P"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := sess.AddNode([]string{"P"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := sess.AddRelationship("KNOWS", a, b, map[string]any{"since": int64(2020)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	got, err := c.Rels.Get(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("Rels.Get: %v", err)
	}
	if got.StartNodeID() != a.ID() || got.EndNodeID() != b.ID() {
		t.Fatalf("endpoints wrong")
	}
	ok, err := c.Hash.VerifyRelChain(r.ID())
	if err != nil || !ok {
		t.Fatalf("VerifyRelChain = (%v, %v)", ok, err)
	}
}

// TestIngestBackpressureBounded — a tiny queue bound with many small submits
// never drops or OOMs; every write lands (§4.8: entity writes are never
// dropped, a full queue blocks the producer).
func TestIngestBackpressureBounded(t *testing.T) {
	t.Parallel()
	c := newIngestGraph(t)

	sess, err := c.Ingest.NewSession(IngestOptions{Sync: false, QueueBound: 1})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	const n = 500
	var lastTok SubmitToken
	for i := 0; i < n; i++ {
		if _, err := sess.AddNode([]string{"B"}, map[string]any{"i": int64(i)}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		tok, err := sess.Submit()
		if err != nil {
			t.Fatalf("Submit: %v", err)
		}
		lastTok = tok
	}
	if err := c.Ingest.WaitApplied(lastTok); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}
	count, err := c.Nodes.Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != n {
		t.Fatalf("Count = %d, want %d (dropped writes)", count, n)
	}
}

// TestIngestUpdateAndDelete — the pipeline carries updates and deletes through
// the same apply doors; history is preserved and chains verify.
func TestIngestUpdateAndDelete(t *testing.T) {
	t.Parallel()
	c := newIngestGraph(t)

	// Seed a node through the pipeline.
	sess, err := c.Ingest.NewSession(IngestOptions{Sync: true})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	n, err := sess.AddNode([]string{"Doc"}, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit seed: %v", err)
	}

	// Update it through the pipeline.
	if err := sess.UpdateNode(id, map[string]any{"v": int64(2)}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit update: %v", err)
	}
	got, err := c.Nodes.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v, _ := got.Properties().Get("v"); v != int64(2) {
		t.Fatalf("update not applied: v=%v", v)
	}
	ok, err := c.Hash.VerifyNodeChain(id)
	if err != nil || !ok {
		t.Fatalf("VerifyNodeChain after update = (%v, %v)", ok, err)
	}

	// Delete it through the pipeline.
	if err := sess.DeleteNode(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit delete: %v", err)
	}
	if _, err := c.Nodes.Get(context.Background(), id); err == nil {
		t.Fatalf("node still present after pipeline delete")
	}
}

// TestIngestBitemporalTwoPhase — the CHOSEN bounded oracle option (a focused
// two-phase battery, not a full pipeline-op generator arm): a node written and
// then superseded THROUGH THE PIPELINE is read at an earlier transaction-time
// pin and reflects its original belief state, not the post-update state. This
// exercises that pipeline-written rows carry correct bitemporal metadata
// (applier-stamped TxFrom, version chain, PrevHash splice) end-to-end through
// the shared resolver (rule 15).
func TestIngestBitemporalTwoPhase(t *testing.T) {
	t.Parallel()
	c := newIngestGraph(t)

	sess, err := c.Ingest.NewSession(IngestOptions{Sync: true})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	// Phase 1: create in state X, capture the tx pin AFTER it is applied.
	n, err := sess.AddNode([]string{"Doc"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit create: %v", err)
	}
	pin, err := c.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}

	// Phase 2: supersede with state Y through the pipeline.
	if err := sess.UpdateNode(id, map[string]any{"status": "published"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit update: %v", err)
	}

	// Current state is Y.
	cur, err := c.Nodes.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v, _ := cur.Properties().Get("status"); v != "published" {
		t.Fatalf("current status = %v, want published", v)
	}

	// As-of the pin (before the update) the belief state is X.
	asof, err := c.Temporal.NodeAsOf(id, pin)
	if err != nil {
		t.Fatalf("NodeAsOf: %v", err)
	}
	if v, _ := asof.Properties().Get("status"); v != "draft" {
		t.Fatalf("as-of pin status = %v, want draft (pipeline lost bitemporal history)", v)
	}
}

// TestIngestUniqueConstraintStorm — a unique constraint is enforced identically
// through the pipeline: many producers racing to create nodes with the SAME
// constrained value converge to exactly ONE winner (invariant 8 — the batch
// pre-check under the applier's exclusive lock arbitrates, prepare is advisory).
func TestIngestUniqueConstraintStorm(t *testing.T) {
	t.Parallel()
	c := newIngestGraph(t)
	if err := c.Constraints.CreateUnique(context.Background(), "User", "email"); err != nil {
		t.Fatalf("CreateUnique: %v", err)
	}

	const producers = 8
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sess, err := c.Ingest.NewSession(IngestOptions{Sync: true, DeclareLabels: []string{"User"}})
			if err != nil {
				t.Errorf("NewSession: %v", err)
				return
			}
			// Every producer tries to claim the same email.
			if _, err := sess.AddNode([]string{"User"}, map[string]any{"email": "dup@example.com"}); err != nil {
				t.Errorf("AddNode: %v", err)
				return
			}
			// A violation surfaces as the submit's apply error; that is expected
			// for all but one winner, so we tolerate it here.
			_, _ = sess.Submit()
		}()
	}
	wg.Wait()

	users, err := c.Nodes.ByLabelAndProperty("User", "email", "dup@example.com", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty: %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("unique constraint admitted %d nodes with the same email, want exactly 1", len(users))
	}
}

// TestIngestClosedRejectsNewSession — after Close the pipeline refuses new work.
func TestIngestClosedRejectsNewSession(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Start the applier so Close exercises the drain/stop path.
	sess, err := g.Ingest.NewSession(IngestOptions{Sync: true})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := sess.AddNode([]string{"N"}, nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := g.Ingest.NewSession(IngestOptions{}); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("NewSession after Close = %v, want ErrGraphClosed", err)
	}
}

// TestIngestNilSafety — nil receivers fail closed, never panic.
func TestIngestNilSafety(t *testing.T) {
	t.Parallel()
	var s *Session
	if err := s.DeleteNode(types.NodeID(1)); !errors.Is(err, ErrNilSession) {
		t.Fatalf("nil session DeleteNode = %v, want ErrNilSession", err)
	}
	if got := s.Pending(); got != 0 {
		t.Fatalf("nil session Pending = %d", got)
	}
	var i *IngestOps
	if _, err := i.NewSession(IngestOptions{}); !errors.Is(err, ErrNilGraph) {
		t.Fatalf("nil IngestOps NewSession = %v", err)
	}
	if got := i.AppliedSeq(); got != 0 {
		t.Fatalf("nil IngestOps AppliedSeq = %d", got)
	}
}
