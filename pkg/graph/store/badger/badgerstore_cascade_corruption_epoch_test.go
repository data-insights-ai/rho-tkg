package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 18h: cascadeDeleteInner's corruption-fallback branch (a node
// present in bs.nodeIDs but whose row can't be loaded — data corruption)
// purged label/property/temporal/vector indexes but never bumped
// nodeEpochSalt, unlike purgeNodesByLabel's identical bulk-delete
// belt-and-braces invalidation (BACKLOG 4b precedent). Without the bump, a
// columnar DocValues query could keep answering from a per-label column
// snapshot still containing this node's now-deleted row.
//
// Reproduces the corruption directly (package-internal test): seed
// bs.nodeIDs with an entry that has NO underlying badger row at all (never
// written), forcing getNodeLocked to fail exactly like a corrupted/missing
// row would, and drives cascadeDeleteLocked (the idxMu-locked wrapper)
// straight into the fallback branch.
func TestCascadeDeleteInner_CorruptionFallback_BumpsNodeEpochSalt(t *testing.T) {
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })

	nid := types.NodeID(snowflake.ID(999_000_001))
	bs.idxMu.Lock()
	bs.nodeIDs[nid] = struct{}{}
	bs.idxMu.Unlock()

	before := bs.nodeEpochSalt.Load()

	_, corruptErr, fatalErr := bs.cascadeDeleteLocked(nid, cascadeDeletePrefetch{})
	if fatalErr != nil {
		t.Fatalf("cascadeDeleteLocked fatal error: %v", fatalErr)
	}
	if corruptErr == nil {
		t.Fatal("cascadeDeleteLocked = nil corruption error, want the corrupt-node-data error — test setup did not reach the fallback branch")
	}

	after := bs.nodeEpochSalt.Load()
	if after != before+1 {
		t.Fatalf("nodeEpochSalt = %d, want %d (bumped by exactly 1) — BACKLOG 18h regression", after, before+1)
	}

	// The node must genuinely be gone from the in-memory liveness map too.
	bs.idxMu.RLock()
	_, stillPresent := bs.nodeIDs[nid]
	bs.idxMu.RUnlock()
	if stillPresent {
		t.Fatal("node still present in bs.nodeIDs after corruption-fallback cascade delete")
	}
}
