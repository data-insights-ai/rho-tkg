package store_test

import (
	"bytes"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// feedStore is any backend that both mutates and exposes a change-feed.
type feedStore interface {
	storecontract.MandatoryStore
	storecontract.ChangeFeedCapability
}

func pnode(id int64, primary uint16) *types.Node {
	return types.NewNode(types.NodeID(snowflake.ID(id)), primary, nil)
}

func prel(id, start, end int64, relType uint16) *types.Relationship {
	return types.NewRelationship(types.RelID(snowflake.ID(id)), relType, types.NodeID(snowflake.ID(start)), types.NodeID(snowflake.ID(end)))
}

// applyParityScenario runs an identical, PROPERTY-FREE lifecycle through a store.
// Property-free entities are used deliberately: the badger backend tokenizes
// property KEYS on the wire (PropertyWire.KeyToken) against its own registry
// while memory keeps raw key strings, so property-bearing payloads are
// semantically equal but not byte-identical. With no properties the two wire
// encodings are byte-for-byte identical, which lets this test assert exact feed
// parity across the backends (same LSN, tag, and payload bytes).
func applyParityScenario(t *testing.T, s feedStore) {
	t.Helper()
	must := func(err error, ctx string) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", ctx, err)
		}
	}
	for id := int64(1); id <= 5; id++ {
		must(s.PutNode(pnode(id, 10)), "PutNode")
	}
	// Node 1 fans out to four relationships so the cascade below removes a
	// MULTI-element set — exercising the deterministic-ordering contract on
	// CascadedRelIDs (a single-rel cascade is order-invariant and would mask a
	// nondeterministic / divergent ordering bug).
	must(s.PutRelationship(prel(100, 1, 2, 5)), "PutRel100")
	must(s.PutRelationship(prel(101, 1, 3, 5)), "PutRel101")
	must(s.PutRelationship(prel(102, 1, 4, 5)), "PutRel102")
	must(s.PutRelationship(prel(103, 1, 5, 5)), "PutRel103")

	// Add a label to node 2 (no history, no version bump).
	n2, err := s.GetNode(types.NodeID(2))
	must(err, "GetNode2")
	upd := n2.DeepCopy()
	upd.AddLabelTokenRaw(20)
	must(s.AddNodeLabelToken(types.NodeID(2), 20, upd), "AddNodeLabelToken")

	must(s.DeleteRelationship(types.RelID(100)), "DeleteRel100")

	// Explicit-version history write on node 3, then truncate it away.
	n3v := pnode(3, 10)
	n3v.SetVersion(7)
	must(s.PutNodeVersion(types.NodeID(3), 7, n3v), "PutNodeVersion")
	must(s.TruncateNodeHistory(types.NodeID(3), 0), "TruncateNodeHistory")

	// Cascade-delete node 1 — still connected to nodes 3, 4, 5 via rels 101/102/103
	// (rel 100 was deleted above). The record's CascadedRelIDs is the sorted set
	// {101, 102, 103}.
	must(s.DeleteNodeCascade(types.NodeID(1)), "DeleteNodeCascade")
}

// newParityTieredStore builds a single-shard-deterministic tiered fixture with
// the change-log enabled. The scenario uses label token 10 (an EVENT label — not
// in RefLabels) and rel token 5, so every node/rel routes to the ONE hot event
// shard; the reference shard stays empty. LSN interleaving is therefore
// deterministic and the store-global feed byte-matches the single-shard backends.
func newParityTieredStore(t *testing.T) *tiered.Store {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		InMemory:      true,
		RefLabels:     []string{"Ref"}, // not used by the scenario → all rows on hot shard
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
		ChangeLog:     true,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	return ts
}

func TestChangeFeedParity_BadgerVsMemory(t *testing.T) {
	bs, err := badger.New(badger.Config{InMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	defer bs.Close()
	ms := memory.New(memory.WithChangeLog())
	defer ms.Close()
	ts := newParityTieredStore(t)
	defer ts.Close()

	applyParityScenario(t, bs)
	applyParityScenario(t, ms)
	applyParityScenario(t, ts)

	if err := bs.Flush(); err != nil {
		t.Fatalf("badger Flush: %v", err)
	}

	bf, err := bs.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("badger ChangeFeed: %v", err)
	}
	mf, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("memory ChangeFeed: %v", err)
	}
	tf, err := ts.ChangeFeed(0, 0) // barriers internally
	if err != nil {
		t.Fatalf("tiered ChangeFeed: %v", err)
	}

	if len(bf) != len(mf) || len(bf) != len(tf) {
		t.Fatalf("feed length: badger=%d memory=%d tiered=%d", len(bf), len(mf), len(tf))
	}
	if len(bf) == 0 {
		t.Fatalf("empty feed — scenario produced no records")
	}
	for i := range bf {
		if bf[i].LSN != mf[i].LSN || bf[i].LSN != tf[i].LSN {
			t.Errorf("record[%d] LSN: badger=%d memory=%d tiered=%d", i, bf[i].LSN, mf[i].LSN, tf[i].LSN)
		}
		if bf[i].Tag != mf[i].Tag || bf[i].Tag != tf[i].Tag {
			t.Errorf("record[%d] Tag: badger=%v memory=%v tiered=%v", i, bf[i].Tag, mf[i].Tag, tf[i].Tag)
		}
		if !bytes.Equal(bf[i].Payload, mf[i].Payload) {
			t.Errorf("record[%d] (tag %v) payload bytes differ: badger=%x memory=%x", i, bf[i].Tag, bf[i].Payload, mf[i].Payload)
		}
		if !bytes.Equal(bf[i].Payload, tf[i].Payload) {
			t.Errorf("record[%d] (tag %v) payload bytes differ: badger=%x tiered=%x", i, bf[i].Tag, bf[i].Payload, tf[i].Payload)
		}
	}

	// Sanity: the scenario covers the full tag taxonomy we expect, in order.
	wantTags := []storecontract.ChangeTag{
		storecontract.ChangeNodePut, storecontract.ChangeNodePut, storecontract.ChangeNodePut,
		storecontract.ChangeNodePut, storecontract.ChangeNodePut, // 5 nodes
		storecontract.ChangeRelPut, storecontract.ChangeRelPut,
		storecontract.ChangeRelPut, storecontract.ChangeRelPut, // 4 rels
		storecontract.ChangeNodePut,             // add label
		storecontract.ChangeRelDelete,           // delete rel 100
		storecontract.ChangeNodeHistoryVersion,  // put node version
		storecontract.ChangeNodeHistoryTruncate, // truncate
		storecontract.ChangeNodeDelete,          // cascade (3 rels)
	}
	if len(bf) != len(wantTags) {
		t.Fatalf("feed has %d records, scenario expects %d", len(bf), len(wantTags))
	}
	for i, want := range wantTags {
		if bf[i].Tag != want {
			t.Errorf("record[%d] tag = %v, want %v", i, bf[i].Tag, want)
		}
	}

	// The cascade record (last) carries the sorted multi-element rel set —
	// the exact case that masks an ordering/divergence bug when it has 0/1 elems.
	cascade := bf[len(bf)-1]
	body, err := storeutil.DecodeNodeDelete(cascade.Payload)
	if err != nil {
		t.Fatalf("DecodeNodeDelete(cascade): %v", err)
	}
	if got := body.CascadedRelIDs; len(got) != 3 || got[0] != 101 || got[1] != 102 || got[2] != 103 {
		t.Fatalf("cascade CascadedRelIDs = %v, want sorted [101 102 103]", got)
	}
}
