package badger

import (
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestNodesAsOf_SingleSnapshotConsistencyUnderConcurrentWrite proves the
// documented contract (BACKLOG 18k): NodesAsOf's single shared transaction PLUS
// its whole-scan idxMu.RLock hold gives a bulk scan ONE consistent snapshot,
// never a per-entity-torn view.
//
// For a PAST or NowTx pin this is unobservable by construction — a concurrent
// write's TxFrom is always > such a pin, so classifyVersionAtTxTime excludes it
// regardless of snapshot/lock timing (see the doc comment on NodesAsOf). The
// interesting, newly-documented case is a FUTURE pin: a concurrent WRITER
// goroutine independently commits N separate single-entity ReplaceNode calls
// (touched[0], touched[1], ..., touched[N-1], STRICTLY in that order — each its
// own atomic idxMu-protected commit, never a multi-entity atomic group) racing a
// concurrent SCAN goroutine's one bs.NodesAsOf(futurePin) call.
//
// The correct invariant is NOT "the scan sees either none or all of the
// writer's N writes" — that would be wrong: the writer's N writes were never
// atomic as a GROUP to begin with (any other concurrent reader could already
// observe an arbitrary partial subset applied, with or without this fix), so
// asserting all-or-nothing tests a property the system never promised. The
// REAL invariant idxMu's mutual exclusion between the scan's one long RLock
// hold and the writer's many individual Lock() calls delivers is: the scan's
// view corresponds to EXACTLY ONE cut point k in the writer's KNOWN commit
// order — touched[0..k-1] visible (new), touched[k..N-1] not yet applied at
// scan-open time (old) — a monotonic PREFIX with NO GAPS. A gap (e.g.
// touched[5] new but touched[3] old, when idxMu guarantees touched[3] commits
// strictly before touched[5]) is the actual torn-read signature this test
// checks for.
func TestNodesAsOf_SingleSnapshotConsistencyUnderConcurrentWrite(t *testing.T) {
	bs := newTestBadgerStore(t)

	const population = 4000
	const touchedStride = 7 // every 7th node gets concurrently rewritten
	ids := make([]types.NodeID, population)
	for i := 0; i < population; i++ {
		nid := types.NodeID(snowflake.ID(9_000_000 + int64(i)))
		ids[i] = nid
		n := types.NewNode(nid, 1, nil)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: 100})
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", nid, err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	var touched []types.NodeID
	for i := 0; i < population; i += touchedStride {
		touched = append(touched, ids[i])
	}

	const trials = 60
	const futurePin = types.Instant(1_000_000) // always "future" relative to each trial's write TxFrom
	for trial := 0; trial < trials; trial++ {
		writeTxFrom := types.Instant(200 + trial) // strictly increasing, always < futurePin

		// Reset every touched node back to TxFrom=100 BEFORE the race starts (a
		// synchronous, non-concurrent step) so each trial's "old" value is always
		// exactly 100 — otherwise a touched node would carry forward the PREVIOUS
		// trial's writeTxFrom as its pre-race state, breaking the fixed old/new
		// value check below.
		for _, nid := range touched {
			reset := types.NewNode(nid, 1, nil)
			reset.SetVersion(0)
			reset.SetTemporal(&types.TemporalMetadata{TxFrom: 100})
			if err := bs.ReplaceNode(reset); err != nil {
				t.Fatalf("trial %d: reset ReplaceNode(%d): %v", trial, nid, err)
			}
		}
		if err := bs.Flush(); err != nil {
			t.Fatalf("trial %d: reset Flush: %v", trial, err)
		}

		var wg sync.WaitGroup
		var got []*types.Node
		var scanErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			got, scanErr = bs.NodesAsOf(futurePin)
		}()
		go func() {
			defer wg.Done()
			for _, nid := range touched {
				n := types.NewNode(nid, 1, nil)
				n.SetVersion(uint32(trial + 1))
				n.SetTemporal(&types.TemporalMetadata{TxFrom: writeTxFrom})
				if err := bs.ReplaceNode(n); err != nil {
					t.Errorf("trial %d: ReplaceNode(%d): %v", trial, nid, err)
					return
				}
			}
		}()
		wg.Wait()
		if scanErr != nil {
			t.Fatalf("trial %d: NodesAsOf: %v", trial, scanErr)
		}

		byID := make(map[snowflake.ID]*types.Node, len(got))
		for _, n := range got {
			byID[n.ID().SnowflakeID()] = n
		}

		// isNew[i] records whether touched[i] shows the post-write value.
		isNew := make([]bool, len(touched))
		for i, nid := range touched {
			n, ok := byID[nid.SnowflakeID()]
			if !ok {
				t.Fatalf("trial %d: touched node %d missing from NodesAsOf(%d) result entirely", trial, nid, futurePin)
			}
			switch n.Temporal().TxFrom {
			case 100:
				isNew[i] = false
			case writeTxFrom:
				isNew[i] = true
			default:
				t.Fatalf("trial %d: node %d has unexpected TxFrom %d (want 100 or %d)", trial, nid, n.Temporal().TxFrom, writeTxFrom)
			}
		}

		// The writer commits touched[0..N-1] STRICTLY in order, each its own
		// idxMu-protected commit; the scan holds idxMu.RLock for its whole
		// duration. Together this guarantees the "new" entries form a PREFIX —
		// once index i flips to old after some earlier index was new, that is a
		// genuine torn/non-monotonic view.
		sawOldAfterNew := false
		firstOldIdx := -1
		for i, n := range isNew {
			if !n && firstOldIdx == -1 {
				firstOldIdx = i
			}
			if n && firstOldIdx != -1 {
				sawOldAfterNew = true
				break
			}
		}
		if sawOldAfterNew {
			t.Fatalf("trial %d: BACKLOG 18k regression — NodesAsOf(%d) returned a NON-MONOTONIC (torn) view of the writer's ordered commit sequence: isNew=%v (touched[%d] is old but a LATER-indexed entry is new, even though idxMu guarantees touched[%d] committed strictly before it)", trial, futurePin, isNew, firstOldIdx, firstOldIdx)
		}

		// Flush before the next trial's writes so ForEachDeletedNodeID-style
		// badger-persisted scans (not used by this specific path, but keeping the
		// store's async buffer drained avoids compounding pending-write depth
		// across 60 trials) stay representative of steady-state operation.
		if err := bs.Flush(); err != nil {
			t.Fatalf("trial %d: Flush: %v", trial, err)
		}
	}
}
