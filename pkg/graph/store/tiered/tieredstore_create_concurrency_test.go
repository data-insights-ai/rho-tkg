package tiered

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 19q: nodeCreateMu/relCreateMu (whole-store mutexes held across the
// ENTIRE create operation — duplicate-ID check AND the physical shard write)
// were replaced with a 256-shard per-raw-ID striped lock (internal/locks.Manager,
// the same primitive used elsewhere in this codebase for entity-level
// serialization). The invariant these tests exist to prove: the per-ID lock is
// STILL held continuously from check through commit (so two concurrent
// creates for the SAME id remain fully serialized, closing the cross-shard/
// cross-class TOCTOU the old whole-store mutex closed), while two creates for
// DIFFERENT ids can now proceed concurrently.

// TestTieredPutNode_ConcurrentSameIDCrossClassRace is the primary adversarial
// safety net: N goroutines race to create the SAME externally-supplied node
// ID, half asserting a REFERENCE-class primary label and half an EVENT-class
// one (routing to entirely different shards under a naive design). Exactly
// one must win; the store must end up with the ID in exactly one shard, node
// counters must fold to exactly 1 (not double-counted), and the change-log
// must carry exactly one ChangeNodePut for that ID (a second record would
// poison a replica with a phantom duplicate). Run under `go test -race`.
func TestTieredPutNode_ConcurrentSameIDCrossClassRace(t *testing.T) {
	const iterations = 20
	const goroutines = 200

	for iter := 0; iter < iterations; iter++ {
		ts, caseTok, _, signalTok := newChangeLogTieredStore(t)
		id := types.NodeID(tieredNodeGen(t).Generate())

		var wg sync.WaitGroup
		var successes int32
		errs := make([]error, goroutines)
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func(i int) {
				defer wg.Done()
				tok := caseTok // reference class
				if i%2 == 1 {
					tok = signalTok // event class
				}
				n := types.NewNode(id, tok, nil)
				err := ts.PutNode(n)
				errs[i] = err
				if err == nil {
					atomic.AddInt32(&successes, 1)
				}
			}(i)
		}
		wg.Wait()

		if successes != 1 {
			t.Fatalf("iter %d: %d/%d goroutines succeeded creating id %d concurrently, want exactly 1",
				iter, successes, goroutines, id.SnowflakeID())
		}
		for i, err := range errs {
			if err != nil && !errors.Is(err, ErrNodeExists) {
				t.Fatalf("iter %d: goroutine %d error = %v, want nil or ErrNodeExists", iter, i, err)
			}
		}

		if _, err := ts.GetNode(id); err != nil {
			t.Fatalf("iter %d: GetNode after race: %v", iter, err)
		}

		count, err := ts.NodeCount()
		if err != nil {
			t.Fatalf("iter %d: NodeCount: %v", iter, err)
		}
		if count != 1 {
			t.Fatalf("iter %d: NodeCount = %d, want 1 (torn/double-counted create)", iter, count)
		}

		putCount := 0
		if err := ts.ForEachChange(0, func(rec storecontract.ChangeRecord) bool {
			if rec.Tag == storecontract.ChangeNodePut {
				putCount++
			}
			return true
		}); err != nil {
			t.Fatalf("iter %d: ForEachChange: %v", iter, err)
		}
		if putCount != 1 {
			t.Fatalf("iter %d: change-log has %d ChangeNodePut records for one winning create, want exactly 1 "+
				"(a second record would replicate a phantom duplicate)", iter, putCount)
		}
	}
}

// TestTieredPutNode_GeneratedThenSuppliedDuplicateDetected locks in a
// SEQUENTIAL-ordering invariant: a generated-ID create (checkGlobalDuplicate
// skipped — safe ONLY because a graph-generated id is unique by construction,
// per generatedcreate.Proof's own contract) followed by a supplied-ID create
// for the SAME id must have the second call detect the duplicate and fail.
//
// NOTE: a genuinely CONCURRENT race between a generated-ID create and a
// supplied-ID create for the identical id (as opposed to this sequential
// check) is NOT a scenario this store needs to defend against — confirmed
// pre-existing and unrelated to BACKLOG 19q by testing against the
// unmodified store: PutNodeGeneratedID's checkGlobalDuplicate=false is a
// deliberate optimization resting on the caller's contract that the id was
// JUST minted by this graph's own snowflake generator (hence provably unique
// against every other concurrent caller, since nothing else could already
// know or be racing to create that exact id) — internal/generatedcreate.Proof
// is unexported specifically so no external caller can pair a stale/reused id
// with that skip. Deliberately reusing the same id for both a generated and a
// supplied create, concurrently, violates that precondition and is out of
// scope here (not a regression: reproduced identically against the
// pre-19q code).
func TestTieredPutNode_GeneratedThenSuppliedDuplicateDetected(t *testing.T) {
	ts, caseTok, _, signalTok := newChangeLogTieredStore(t)
	id := types.NodeID(tieredNodeGen(t).Generate())

	first := types.NewNode(id, caseTok, nil)
	if err := ts.PutNodeGeneratedID(first, generatedcreate.FreshGraphID()); err != nil {
		t.Fatalf("generated-ID create: %v", err)
	}

	second := types.NewNode(id, signalTok, nil)
	if err := ts.PutNode(second); !errors.Is(err, ErrNodeExists) {
		t.Fatalf("supplied-ID create for an id already committed via the generated path = %v, want ErrNodeExists", err)
	}

	count, err := ts.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if count != 1 {
		t.Fatalf("NodeCount = %d, want 1", count)
	}
}

// TestTieredPutNode_ConcurrentDistinctIDsAcrossShards proves the striped lock
// still lets DISTINCT ids proceed concurrently and end up fully consistent:
// every create for a distinct id must succeed, and every store-wide counter
// fold must be exact. This is the throughput-side counterpart to the
// same-ID race tests above — it would also catch a broken reservation
// (e.g. a stuck per-ID lock blocking an unrelated id).
func TestTieredPutNode_ConcurrentDistinctIDsAcrossShards(t *testing.T) {
	ts, caseTok, _, signalTok := newChangeLogTieredStore(t)
	nodeGen := tieredNodeGen(t)

	const n = 500
	ids := make([]types.NodeID, n)
	for i := range ids {
		ids[i] = types.NodeID(nodeGen.Generate())
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			tok := caseTok
			if i%2 == 1 {
				tok = signalTok
			}
			node := types.NewNode(ids[i], tok, nil)
			errs[i] = ts.PutNode(node)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("PutNode(%d) (distinct id, concurrent) = %v, want nil", ids[i].SnowflakeID(), err)
		}
	}

	count, err := ts.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if count != n {
		t.Fatalf("NodeCount = %d, want %d", count, n)
	}

	putCount := 0
	if err := ts.ForEachChange(0, func(rec storecontract.ChangeRecord) bool {
		if rec.Tag == storecontract.ChangeNodePut {
			putCount++
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if putCount != n {
		t.Fatalf("change-log has %d ChangeNodePut records, want %d", putCount, n)
	}
}

// TestTieredPutNode_LockNotStuckAfterFailedCreate proves a failed create (a
// duplicate-ID rejection) does not leave that id's stripe permanently locked
// — the striped lock is released via defer regardless of outcome, but this
// pins the observable behavior: retrying a DIFFERENT id immediately after a
// failure must not block, and the original id must still be usable for reads.
func TestTieredPutNode_LockNotStuckAfterFailedCreate(t *testing.T) {
	ts, caseTok, _, _ := newChangeLogTieredStore(t)
	nodeGen := tieredNodeGen(t)

	id := types.NodeID(nodeGen.Generate())
	first := types.NewNode(id, caseTok, nil)
	if err := ts.PutNode(first); err != nil {
		t.Fatalf("first PutNode: %v", err)
	}

	dup := types.NewNode(id, caseTok, nil)
	if err := ts.PutNode(dup); !errors.Is(err, ErrNodeExists) {
		t.Fatalf("duplicate PutNode = %v, want ErrNodeExists", err)
	}

	// The SAME id's lock must be immediately reusable (no stuck lock) — retry
	// a duplicate-detecting call again to prove LockEntity/UnlockEntity for
	// this id round-tripped cleanly.
	dup2 := types.NewNode(id, caseTok, nil)
	if err := ts.PutNode(dup2); !errors.Is(err, ErrNodeExists) {
		t.Fatalf("second duplicate PutNode = %v, want ErrNodeExists (lock may be stuck)", err)
	}

	// A DIFFERENT id must not be blocked by the failed create above.
	otherID := types.NodeID(nodeGen.Generate())
	other := types.NewNode(otherID, caseTok, nil)
	if err := ts.PutNode(other); err != nil {
		t.Fatalf("PutNode for unrelated id after a failed create = %v, want nil", err)
	}

	count, err := ts.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if count != 2 {
		t.Fatalf("NodeCount = %d, want 2", count)
	}
}

// TestTieredRelationship_ConcurrentSameIDSameShardRace mirrors
// TestTieredPutNode_ConcurrentSameIDCrossClassRace for relationships: N
// goroutines race to create the SAME externally-supplied relationship ID
// between two reference-shard (same-shard) nodes. Exactly one must win, and
// the change-log must carry exactly one ChangeRelPut record (same-shard rel
// creates delegate to badger's own PutRelationship, which logs normally).
func TestTieredRelationship_ConcurrentSameIDSameShardRace(t *testing.T) {
	const iterations = 20
	const goroutines = 200

	for iter := 0; iter < iterations; iter++ {
		ts, caseTok, _, _ := newChangeLogTieredStore(t)
		nodeGen := tieredNodeGen(t)
		relGen := tieredRelGen(t)

		start := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
		end := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
		if err := ts.PutNode(start); err != nil {
			t.Fatalf("iter %d: PutNode start: %v", iter, err)
		}
		if err := ts.PutNode(end); err != nil {
			t.Fatalf("iter %d: PutNode end: %v", iter, err)
		}

		relID := types.RelID(relGen.Generate())
		const relType = uint16(7)

		var wg sync.WaitGroup
		var successes int32
		errs := make([]error, goroutines)
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func(i int) {
				defer wg.Done()
				r := types.NewRelationship(relID, relType, start.ID(), end.ID())
				err := ts.PutRelationship(r)
				errs[i] = err
				if err == nil {
					atomic.AddInt32(&successes, 1)
				}
			}(i)
		}
		wg.Wait()

		if successes != 1 {
			t.Fatalf("iter %d: %d/%d goroutines succeeded creating rel %d concurrently, want exactly 1",
				iter, successes, goroutines, relID.SnowflakeID())
		}
		for i, err := range errs {
			if err != nil && !errors.Is(err, ErrRelExists) {
				t.Fatalf("iter %d: goroutine %d error = %v, want nil or ErrRelExists", iter, i, err)
			}
		}

		if _, err := ts.GetRelationship(relID); err != nil {
			t.Fatalf("iter %d: GetRelationship after race: %v", iter, err)
		}

		putCount := 0
		if err := ts.ForEachChange(0, func(rec storecontract.ChangeRecord) bool {
			if rec.Tag == storecontract.ChangeRelPut {
				putCount++
			}
			return true
		}); err != nil {
			t.Fatalf("iter %d: ForEachChange: %v", iter, err)
		}
		if putCount != 1 {
			t.Fatalf("iter %d: change-log has %d ChangeRelPut records for one winning create, want exactly 1",
				iter, putCount)
		}

		out, err := ts.OutgoingRelationships(start.ID(), 0)
		if err != nil {
			t.Fatalf("iter %d: OutgoingRelationships: %v", iter, err)
		}
		if len(out) != 1 {
			t.Fatalf("iter %d: start node has %d outgoing rels, want 1", iter, len(out))
		}
	}
}

// TestTieredRelationship_ConcurrentSameIDCrossShardRace mirrors the same-shard
// race above but between a reference-shard start node and an event-shard end
// node, forcing the cross-shard split-write path in putRelationshipLocked.
// Exactly one create must win and BOTH shards' adjacency legs must end up
// consistent (no torn half-write from a loser that raced past the endpoint
// checks). Deliberately does NOT assert on the change-log: cross-shard tiered
// relationship creates route through badger's PutRelEntityAndOut/PutRelIncoming
// split-write doors, which are documented (badgerstore_rel.go) as record-free
// — confirmed (against the unmodified pre-19q store too) that a cross-shard
// tiered relationship create emits NO change-log record at all today. That is
// a genuine, separate gap in tiered's change-log coverage, unrelated to and
// out of scope for BACKLOG 19q (a create-mutex/concurrency fix) — flagged here
// rather than silently worked around.
func TestTieredRelationship_ConcurrentSameIDCrossShardRace(t *testing.T) {
	const iterations = 20
	const goroutines = 200

	for iter := 0; iter < iterations; iter++ {
		ts, caseTok, _, signalTok := newTestTieredStoreForRelRace(t)
		nodeGen := tieredNodeGen(t)
		relGen := tieredRelGen(t)

		start := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil) // reference shard
		end := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil) // event (hot) shard
		if err := ts.PutNode(start); err != nil {
			t.Fatalf("iter %d: PutNode start: %v", iter, err)
		}
		if err := ts.PutNode(end); err != nil {
			t.Fatalf("iter %d: PutNode end: %v", iter, err)
		}

		relID := types.RelID(relGen.Generate())
		const relType = uint16(7)

		var wg sync.WaitGroup
		var successes int32
		errs := make([]error, goroutines)
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func(i int) {
				defer wg.Done()
				r := types.NewRelationship(relID, relType, start.ID(), end.ID())
				err := ts.PutRelationship(r)
				errs[i] = err
				if err == nil {
					atomic.AddInt32(&successes, 1)
				}
			}(i)
		}
		wg.Wait()

		if successes != 1 {
			t.Fatalf("iter %d: %d/%d goroutines succeeded creating cross-shard rel %d concurrently, want exactly 1",
				iter, successes, goroutines, relID.SnowflakeID())
		}
		for i, err := range errs {
			if err != nil && !errors.Is(err, ErrRelExists) {
				t.Fatalf("iter %d: goroutine %d error = %v, want nil or ErrRelExists", iter, i, err)
			}
		}

		if _, err := ts.GetRelationship(relID); err != nil {
			t.Fatalf("iter %d: GetRelationship after race: %v", iter, err)
		}

		// Adjacency must be consistent on BOTH shards — exactly one outgoing
		// edge from start and one incoming edge to end, never a torn cross-shard
		// half-write from a loser that raced past the endpoint checks.
		out, err := ts.OutgoingRelationships(start.ID(), 0)
		if err != nil {
			t.Fatalf("iter %d: OutgoingRelationships: %v", iter, err)
		}
		if len(out) != 1 {
			t.Fatalf("iter %d: start node has %d outgoing rels, want 1", iter, len(out))
		}
		in, err := ts.IncomingRelationships(end.ID(), 0)
		if err != nil {
			t.Fatalf("iter %d: IncomingRelationships: %v", iter, err)
		}
		if len(in) != 1 {
			t.Fatalf("iter %d: end node has %d incoming rels, want 1", iter, len(in))
		}
	}
}

// newTestTieredStoreForRelRace is newChangeLogTieredStore without the
// change-log enabled — the cross-shard race test above does not assert on the
// feed (see its doc comment), so a plain store keeps the test focused.
func newTestTieredStoreForRelRace(t *testing.T) (*Store, uint16, uint16, uint16) {
	t.Helper()
	ts := newTestTieredStore(t)
	caseTok, userTok, signalTok := installDefaultTestLabelRegistry(t, ts)
	return ts, caseTok, userTok, signalTok
}

// TestTieredPutNodesBatch_ConcurrentOverlappingBatches races two concurrent
// PutNodesBatch calls whose id sets OVERLAP by one id: batch A =
// [shared, onlyA...], batch B = [shared, onlyB...]. nodeCreateLocks.LockMany
// must still serialize the two batches against each other on the shared id
// (they contend on that id's stripe even though most of each batch's ids are
// disjoint), so exactly one batch wins the shared id while every OTHER,
// non-overlapping id in BOTH batches still ends up created — a batch losing
// on its one shared id must not corrupt or drop its unrelated ids.
func TestTieredPutNodesBatch_ConcurrentOverlappingBatches(t *testing.T) {
	const iterations = 20
	const perBatch = 20

	for iter := 0; iter < iterations; iter++ {
		ts, caseTok, _, _ := newTestTieredStoreForRelRace(t)
		nodeGen := tieredNodeGen(t)

		shared := types.NodeID(nodeGen.Generate())
		buildBatch := func() []*types.Node {
			nodes := make([]*types.Node, 0, perBatch)
			nodes = append(nodes, types.NewNode(shared, caseTok, nil))
			for i := 1; i < perBatch; i++ {
				nodes = append(nodes, types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil))
			}
			return nodes
		}
		batchA := buildBatch()
		batchB := buildBatch()

		var wg sync.WaitGroup
		var errA, errB error
		wg.Add(2)
		go func() {
			defer wg.Done()
			errA = ts.PutNodesBatch(batchA)
		}()
		go func() {
			defer wg.Done()
			errB = ts.PutNodesBatch(batchB)
		}()
		wg.Wait()

		if errA == nil && errB == nil {
			t.Fatalf("iter %d: both overlapping batches succeeded (shared id %d created twice)", iter, shared.SnowflakeID())
		}
		if errA != nil && !errors.Is(errA, ErrNodeExists) {
			t.Fatalf("iter %d: batch A error = %v, want nil or ErrNodeExists", iter, errA)
		}
		if errB != nil && !errors.Is(errB, ErrNodeExists) {
			t.Fatalf("iter %d: batch B error = %v, want nil or ErrNodeExists", iter, errB)
		}

		// The shared id must exist exactly once regardless of which batch won.
		if _, err := ts.GetNode(shared); err != nil {
			t.Fatalf("iter %d: shared id missing after both batch attempts: %v", iter, err)
		}

		// The WINNING batch's non-shared ids must ALL be present (a batch that
		// lost only on the shared id, but partially wrote unrelated ids before
		// failing, would corrupt the all-or-nothing batch contract).
		var winner []*types.Node
		switch {
		case errA == nil:
			winner = batchA
		case errB == nil:
			winner = batchB
		default:
			t.Fatalf("iter %d: neither batch succeeded (errA=%v errB=%v)", iter, errA, errB)
		}
		for _, n := range winner {
			if _, err := ts.GetNode(n.ID()); err != nil {
				t.Fatalf("iter %d: winning batch's node %d missing: %v", iter, n.ID().SnowflakeID(), err)
			}
		}

		// The LOSING batch's non-shared ids must ALL be absent (rolled back
		// cleanly, not left as a partial write).
		var loser []*types.Node
		if errA != nil {
			loser = batchA
		} else {
			loser = batchB
		}
		for _, n := range loser {
			if n.ID() == shared {
				continue
			}
			if _, err := ts.GetNode(n.ID()); !errors.Is(err, ErrNodeNotFound) {
				t.Fatalf("iter %d: losing batch's unrelated node %d = (found, err=%v), want ErrNodeNotFound (partial write leaked)",
					iter, n.ID().SnowflakeID(), err)
			}
		}

		count, err := ts.NodeCount()
		if err != nil {
			t.Fatalf("iter %d: NodeCount: %v", iter, err)
		}
		if count != perBatch {
			t.Fatalf("iter %d: NodeCount = %d, want %d (exactly one batch's ids, shared id counted once)", iter, count, perBatch)
		}
	}
}

// TestTieredPutRelationshipsBatch_ConcurrentOverlappingBatches mirrors
// TestTieredPutNodesBatch_ConcurrentOverlappingBatches for relationships:
// two concurrent PutRelationshipsBatch calls share one rel id. Exactly one
// batch must win the shared id; both batches' node endpoints already exist
// (created up front), so only the relationship layer is under test.
func TestTieredPutRelationshipsBatch_ConcurrentOverlappingBatches(t *testing.T) {
	const iterations = 20
	const perBatch = 10

	for iter := 0; iter < iterations; iter++ {
		ts, caseTok, _, _ := newTestTieredStoreForRelRace(t)
		nodeGen := tieredNodeGen(t)
		relGen := tieredRelGen(t)

		start := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
		end := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
		if err := ts.PutNode(start); err != nil {
			t.Fatalf("iter %d: PutNode start: %v", iter, err)
		}
		if err := ts.PutNode(end); err != nil {
			t.Fatalf("iter %d: PutNode end: %v", iter, err)
		}

		sharedID := types.RelID(relGen.Generate())
		buildBatch := func() []*types.Relationship {
			rels := make([]*types.Relationship, 0, perBatch)
			rels = append(rels, types.NewRelationship(sharedID, 7, start.ID(), end.ID()))
			for i := 1; i < perBatch; i++ {
				rels = append(rels, types.NewRelationship(types.RelID(relGen.Generate()), 7, start.ID(), end.ID()))
			}
			return rels
		}
		batchA := buildBatch()
		batchB := buildBatch()

		var wg sync.WaitGroup
		var errA, errB error
		wg.Add(2)
		go func() {
			defer wg.Done()
			errA = ts.PutRelationshipsBatch(batchA)
		}()
		go func() {
			defer wg.Done()
			errB = ts.PutRelationshipsBatch(batchB)
		}()
		wg.Wait()

		if errA == nil && errB == nil {
			t.Fatalf("iter %d: both overlapping rel batches succeeded (shared rel %d created twice)", iter, sharedID.SnowflakeID())
		}
		if errA != nil && !errors.Is(errA, ErrRelExists) {
			t.Fatalf("iter %d: batch A error = %v, want nil or ErrRelExists", iter, errA)
		}
		if errB != nil && !errors.Is(errB, ErrRelExists) {
			t.Fatalf("iter %d: batch B error = %v, want nil or ErrRelExists", iter, errB)
		}

		if _, err := ts.GetRelationship(sharedID); err != nil {
			t.Fatalf("iter %d: shared rel missing after both batch attempts: %v", iter, err)
		}

		var winner []*types.Relationship
		switch {
		case errA == nil:
			winner = batchA
		case errB == nil:
			winner = batchB
		default:
			t.Fatalf("iter %d: neither batch succeeded (errA=%v errB=%v)", iter, errA, errB)
		}
		for _, r := range winner {
			if _, err := ts.GetRelationship(r.ID()); err != nil {
				t.Fatalf("iter %d: winning batch's rel %d missing: %v", iter, r.ID().SnowflakeID(), err)
			}
		}

		var loser []*types.Relationship
		if errA != nil {
			loser = batchA
		} else {
			loser = batchB
		}
		for _, r := range loser {
			if r.ID() == sharedID {
				continue
			}
			if _, err := ts.GetRelationship(r.ID()); !errors.Is(err, ErrRelNotFound) {
				t.Fatalf("iter %d: losing batch's unrelated rel %d = (found, err=%v), want ErrRelNotFound (partial write leaked)",
					iter, r.ID().SnowflakeID(), err)
			}
		}

		out, err := ts.OutgoingRelationships(start.ID(), 0)
		if err != nil {
			t.Fatalf("iter %d: OutgoingRelationships: %v", iter, err)
		}
		if len(out) != perBatch {
			t.Fatalf("iter %d: start node has %d outgoing rels, want %d", iter, len(out), perBatch)
		}
	}
}
