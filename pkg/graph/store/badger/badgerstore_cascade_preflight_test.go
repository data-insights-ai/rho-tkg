package badger

import (
	"errors"
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// cpfState is everything a cascade delete mutates, as seen through the store:
// the node row, the orphans' in-memory adjacency/type entries, the counters,
// the node's history, and the write buffers (entity ops and change-log
// records).
func cpfState(t *testing.T, bs *Store, nid types.NodeID, orphans []types.RelID) string {
	t.Helper()
	_, nodeErr := bs.GetNode(nid)
	hist, err := bs.GetNodeHistory(nid)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	bs.idxMu.RLock()
	idx := ""
	for _, o := range orphans {
		_, out := bs.outIdx[nid][o]
		_, typ := bs.typeIdx[7][o]
		_, rel := bs.relIDs[o]
		idx += fmt.Sprintf(" %d:out=%v,type=%v,relIDs=%v", o, out, typ, rel)
	}
	_, nodeID := bs.nodeIDs[nid]
	bs.idxMu.RUnlock()
	bs.wbMu.Lock()
	pending, logs := len(bs.pending), len(bs.pendingLog)
	bs.wbMu.Unlock()
	return fmt.Sprintf("node=%v nodeIDs=%v history=%d%s relCount=%d type7=%d pending=%d log=%d",
		nodeErr == nil, nodeID, len(hist), idx, bs.relCount.Load(), bs.getOrCreateTypeCounter(7).Load(), pending, logs)
}

// TestCascadeDeleteFatalPreflightAppliesNothing fails the second orphan
// relationship's index-key read inside a node delete's preflight and expects
// the delete to return that error with NOTHING applied: node row, history
// (no tombstone), adjacency and type entries of both orphans, counters, the
// pending buffer and the change-log. Breaks: a cascade that purges each orphan
// as it reads its keys (the order before backlog 32: the first orphan is
// purged before the second read fails); both delete doors.
func TestCascadeDeleteFatalPreflightAppliesNothing(t *testing.T) {
	for _, door := range []string{"DeleteNodeCascade", "DeleteNodeWithHistory"} {
		t.Run(door, func(t *testing.T) {
			bs := newChangeLogStore(t, false)
			const nid, end = types.NodeID(10), types.NodeID(20)
			putTestNode(t, bs, 10, 1, nil)
			putTestNode(t, bs, 20, 1, nil)
			orphans := []types.RelID{types.RelID(snowflake.ID(998)), types.RelID(snowflake.ID(999))}
			bs.idxMu.Lock()
			bs.outIdx[nid] = map[types.RelID]types.NodeID{}
			bs.inIdx[end] = map[types.RelID]inEdge{}
			bs.typeIdx[7] = map[types.RelID]struct{}{}
			for _, o := range orphans {
				bs.relIDs[o] = struct{}{}
				bs.outIdx[nid][o] = end
				bs.inIdx[end][o] = inEdge{start: nid, typ: 7}
				bs.typeIdx[7][o] = struct{}{}
				bs.appendOps(
					writeOp{opType: writeOpSet, key: storepkg.OutKey(10, 7, 20, o.SnowflakeID())},
					writeOp{opType: writeOpSet, key: storepkg.InKey(20, 7, 10, o.SnowflakeID())},
					writeOp{opType: writeOpSet, key: storepkg.RelTypeIndexKey(7, o.SnowflakeID())},
				)
			}
			bs.getOrCreateTypeCounter(7).Store(2)
			bs.relCount.Store(2)
			bs.idxMu.Unlock()
			if err := bs.Flush(); err != nil {
				t.Fatalf("Flush setup: %v", err)
			}

			before := cpfState(t, bs, nid, orphans)
			injected := errors.New("injected orphan key read failure")
			reads := 0
			bs.relIndexKeysTestErr = func() error {
				reads++
				if reads == 2 {
					return injected
				}
				return nil
			}
			var err error
			if door == "DeleteNodeCascade" {
				err = bs.DeleteNodeCascade(nid)
			} else {
				n, gerr := bs.GetNode(nid)
				if gerr != nil {
					t.Fatal(gerr)
				}
				tomb := n.DeepCopy()
				tomb.SetTemporal(&types.TemporalMetadata{DeletedAt: 5, ValidTo: 5, TxFrom: 5, TxTo: 5})
				err = bs.DeleteNodeWithHistory(nid, n.Version(), tomb, nil)
			}
			bs.relIndexKeysTestErr = nil
			if !errors.Is(err, injected) {
				t.Fatalf("%s = %v, want the injected preflight error (reads: %d)", door, err, reads)
			}
			if after := cpfState(t, bs, nid, orphans); after != before {
				t.Fatalf("%s applied mutations before its fatal preflight error:\nbefore %s\nafter  %s", door, before, after)
			}
		})
	}
}
