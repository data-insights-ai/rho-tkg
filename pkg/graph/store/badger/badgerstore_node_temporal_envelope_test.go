package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The node temporal envelope prunes, so once it covers an ID it
// must cover every row of that ID carrying the label. Each test prunes at an
// instant only an old row covers; the node must be kept.

func envNode(id types.NodeID, version uint32, from, to types.Instant, primary uint16, extra ...uint16) *types.Node {
	n := types.NewNode(id, primary, extra)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: from, ValidTo: to})
	n.SetVersion(version)
	return n
}

// (a) PutNodeVersion on a covered node: the imported past row [100,200)
// must join the envelope.
func TestNodeTemporalEnvelope_PutNodeVersionCovered(t *testing.T) {
	t.Parallel()
	bs := newFlushParkStore(t, nil)
	nid := types.NodeID(snowflake.ID(8030))
	if err := bs.PutNode(envNode(nid, 5, 5000, 6000, 7)); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateTemporalIndex(7); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNodeVersion(nid, 1, envNode(nid, 1, 100, 200, 7)); err != nil {
		t.Fatal(err)
	}
	nodeMustKeep(t, bs, nid, 150, 5500)
}

// (b) A prune between the create's current-row backfill and its history fold
// must not see the current row alone.
func TestNodeTemporalEnvelope_CreatePruneBeforeHistoryFold(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	nid := types.NodeID(snowflake.ID(8031))
	old := envNode(nid, 0, 100, 200, 7)
	if err := bs.PutNode(old); err != nil {
		t.Fatal(err)
	}
	if err := bs.ReplaceNodeWithHistory(envNode(nid, 1, 5000, 6000, 7), 0, old); err != nil {
		t.Fatal(err)
	}
	fired := false
	temporalCreateBeforeFoldHook = func() {
		fired = true
		if kept, ok := bs.PruneTemporalCandidates(7, []types.NodeID{nid}, QueryOpts{ValidAt: 150}); len(kept) != 1 {
			t.Errorf("prune at 150 before the history fold kept %v (ok=%v), want [%d]", kept, ok, nid)
		}
	}
	defer func() { temporalCreateBeforeFoldHook = nil }()
	if err := bs.CreateTemporalIndex(7); err != nil {
		t.Fatal(err)
	}
	if !fired {
		t.Fatal("hook did not fire")
	}
	nodeMustKeep(t, bs, nid, 150, 5500)
}

// A live node that carried label 7 in [100,200), lost it, and gets it back
// after the index exists: the envelope must cover the old row too, through the
// add-label door with history and through the plain one (rollback path, no
// history row), which folds history as well.
func TestNodeTemporalEnvelope_LabelReAddOnLiveNode(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name string
		id   int64
		add  func(bs *Store, nid types.NodeID, v1 *types.Node) error
		want []types.Instant // valid times the label answers at
	}{
		{"with history", 8032, func(bs *Store, nid types.NodeID, v1 *types.Node) error {
			return bs.AddNodeLabelTokenWithHistory(nid, 7, envNode(nid, 2, 7000, 8000, 8, 7), 1, v1)
		}, []types.Instant{150, 7500}},
		{"plain", 8041, func(bs *Store, nid types.NodeID, _ *types.Node) error {
			return bs.AddNodeLabelToken(nid, 7, envNode(nid, 1, 5000, 6000, 8, 7))
		}, []types.Instant{150, 5500}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			bs := newFlushParkStore(t, nil)
			nid := types.NodeID(snowflake.ID(c.id))
			v0 := envNode(nid, 0, 100, 200, 8, 7)
			if err := bs.PutNode(v0); err != nil {
				t.Fatal(err)
			}
			v1 := envNode(nid, 1, 5000, 6000, 8)
			if err := bs.RemoveNodeLabelTokenWithHistory(nid, 7, v1, 0, v0); err != nil {
				t.Fatal(err)
			}
			if err := bs.CreateTemporalIndex(7); err != nil {
				t.Fatal(err)
			}
			if err := c.add(bs, nid, v1); err != nil {
				t.Fatal(err)
			}
			nodeMustKeep(t, bs, nid, c.want...)
		})
	}
}
