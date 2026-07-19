package tiered

import (
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredColdShardFastDrop_ForeignLabelDuringDrainBlocksTheDrop is the
// BACKLOG 19e regression: dropOneShard's single-label eligibility check runs
// TWICE — once before the drain-unlink (a cheap early-out) and once AFTER the
// drain completes (the AUTHORITATIVE check, since the drain is what makes the
// shard's on-disk state quiescent/trustworthy). The second call's onlyLabel
// result used to be silently discarded (`_, nodeIDs, rels, cerr := ...`), so
// a request that started before the unlink but only PUBLISHED its effect (a
// foreign AddNodeLabelToken) during the drain window was invisible to the
// eligibility decision — the shard was dropped anyway, physically destroying
// the foreign-labeled node's directory-resident data.
//
// This test simulates that exact window deterministically (no timing races):
// it holds the candidate shard's activeReqs artificially non-zero via
// CheckoutStoreForTest so dropOneShard's drain loop must spin, then — while
// it is provably spinning (the shard is already unlinked from ts.eventShards
// but the purge goroutine has not progressed past the drain) — writes a
// foreign-labeled node DIRECTLY to the checked-out shard store, simulating
// the in-flight request's effect landing before it completes. Releasing the
// checkout lets the drain finish and the second (authoritative) check run.
func TestTieredColdShardFastDrop_ForeignLabelDuringDrainBlocksTheDrop(t *testing.T) {
	ts, err := New(Config{
		DataDir:       t.TempDir(),
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	for _, l := range []string{"Case", "User", "Signal"} {
		if _, err := reg.GetOrCreate(l); err != nil {
			t.Fatalf("registry: %v", err)
		}
	}
	otherTok, err := reg.GetOrCreate("Other")
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	const signalTok = uint16(3)

	gen := tieredNodeGen(t)
	var signalIDs []types.NodeID
	for i := 0; i < 5; i++ {
		id := types.NodeID(gen.Generate())
		if err := ts.PutNode(types.NewNode(id, signalTok, nil)); err != nil {
			t.Fatalf("put signal node: %v", err)
		}
		signalIDs = append(signalIDs, id)
	}
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}

	// Find the rotated (non-hot) candidate shard holding the signal nodes.
	ts.MuForTest().RLock()
	var candidate *EventShard
	for _, es := range ts.EventShardsForTest() {
		if es != ts.HotShardForTest() {
			candidate = es
		}
	}
	ts.MuForTest().RUnlock()
	if candidate == nil {
		t.Fatal("test setup: no rotated candidate shard found")
	}
	eventShardsBefore := len(ts.CatalogForTest().EventShards())

	// Hold the shard "in use" so dropOneShard's post-unlink drain must spin
	// until we release it below.
	held, err := candidate.CheckoutStoreForTest(ts)
	if err != nil {
		t.Fatalf("CheckoutStoreForTest: %v", err)
	}

	purgeDone := make(chan struct{})
	var purgeErr error
	go func() {
		defer close(purgeDone)
		_, purgeErr = ts.PurgeNodesByLabelBefore(signalTok, types.Instant(1<<50), 8)
	}()

	// Wait until the shard is unlinked from routing — proof the drop has
	// passed its FIRST (pre-drain) eligibility check and is now blocked in
	// the drain, spinning on our held checkout.
	unlinkDeadline := time.Now().Add(5 * time.Second)
	for {
		ts.MuForTest().RLock()
		_, stillLinked := ts.EventShardsForTest()[candidate.name]
		ts.MuForTest().RUnlock()
		if !stillLinked {
			break
		}
		if time.Now().After(unlinkDeadline) {
			candidate.CheckinStoreForTest()
			<-purgeDone
			t.Fatal("candidate shard was never unlinked — test setup did not reach the drain window")
		}
		time.Sleep(time.Millisecond)
	}

	// Simulate the in-flight request's effect landing during the drain: a
	// foreign-labeled node written directly to the (still open) shard store.
	foreignID := types.NodeID(gen.Generate())
	if err := held.PutNode(types.NewNode(foreignID, otherTok, nil)); err != nil {
		candidate.CheckinStoreForTest()
		<-purgeDone
		t.Fatalf("PutNode(foreign) on checked-out shard: %v", err)
	}

	// Release the held checkout — the drain can now complete and the SECOND,
	// authoritative eligibility check runs and must see the foreign label.
	candidate.CheckinStoreForTest()
	<-purgeDone
	if purgeErr != nil {
		t.Fatalf("PurgeNodesByLabelBefore: %v", purgeErr)
	}

	// The shard must NOT have been physically dropped — the foreign node's
	// data must survive. Pre-fix, the shard directory (and its BadgerStore)
	// would already be gone by this point, taking the foreign node with it.
	// Read directly from the candidate shard's own store rather than through
	// ts.GetNode: foreignID's snowflake timestamp falls in the CURRENT hot
	// shard's window (it was minted after ForceRotate), so ts-level routing
	// would look in the wrong shard regardless of whether the drop happened —
	// the discriminator this test needs is whether the ROTATED shard (and the
	// row we wrote straight into it) still physically exists.
	if got, err := held.GetNode(foreignID); err != nil {
		t.Fatalf("foreign-labeled node lost — BACKLOG 19e regression (shard dropped despite a foreign label): %v", err)
	} else if !got.HasLabelTokenRaw(otherTok) {
		t.Fatalf("foreign node label = %v, want token %d", got.AllLabelTokens(), otherTok)
	}
	if got := len(ts.CatalogForTest().EventShards()); got != eventShardsBefore {
		t.Fatalf("event shards after purge = %d, want %d unchanged (the candidate shard must still exist, just re-linked)", got, eventShardsBefore)
	}

	// The signal nodes must still have been purged — just via the safe
	// per-node row-scan fallback (purgeNodesFanOut), not the shard-drop
	// shortcut. The overall purge result must remain correct even though the
	// fast path declined.
	for _, id := range signalIDs {
		if _, err := ts.GetNode(id); err == nil {
			t.Fatalf("signal node %v survived the purge (row-scan fallback should still have removed it)", id)
		}
	}
}
