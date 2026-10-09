package badger

import (
	"fmt"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 32: a with-history door moves the entity's current row into history
// inside one idxMu.Lock section, but the point readers take neither idxMu nor
// one snapshot: GetNode / GetRelationship answer from the entity cache, the
// history readers from the pending-buffer overlay plus a badger View. So the
// ORDER in which the door publishes the two halves is what a reader sees
// mid-move. The faulty order (cache first, history second) lets a reader see
// the new current row and a history without the moved row: the row the reader
// is pinned to is in neither read, and NodeAsOf / NodeAtTx at a pin before the
// move answer "absent". The test pauses every with-history door between its
// two halves (moveTestHook) and reads the entity through the lock-free point
// doors there.

const (
	mpoValidFrom types.Instant = 1000
	mpoTxBefore  types.Instant = 100  // the moved row's recording
	mpoPin       types.Instant = 1500 // a pin before the move
	mpoTxMove    types.Instant = 2000 // the move's recording
)

func mpoTemporal() *types.TemporalMetadata {
	return &types.TemporalMetadata{ValidFrom: mpoValidFrom, TxFrom: mpoTxBefore}
}

// mpoSuperseded is the moved row as a door stores it in history: superseded at
// the move (TxTo), a delete also stamps DeletedAt / ValidTo.
func mpoSuperseded(deleted bool) *types.TemporalMetadata {
	tm := mpoTemporal()
	tm.TxTo = mpoTxMove
	if deleted {
		tm.DeletedAt = mpoTxMove
		tm.ValidTo = mpoTxMove
	}
	return tm
}

func mpoNext() *types.TemporalMetadata {
	return &types.TemporalMetadata{ValidFrom: mpoValidFrom, TxFrom: mpoTxMove, UpdatedAt: mpoTxMove}
}

type mpoEntity interface {
	Version() uint32
	Temporal() *types.TemporalMetadata
}

// mpoView is what a lock-free reader sees of one entity: the current row, the
// history rows and their temporal skeletons, and the native as-of answer at a
// pin before the move.
type mpoView struct {
	current  mpoEntity
	history  []mpoEntity
	skeleton []uint32
	asOf     mpoEntity
	asOfErr  error
}

// moved reports what is wrong with the view: the moved row (version 0) must be
// the current row or a history row, in the full history AND in the skeletons,
// and the as-of answer at the pin must be that row.
func (v mpoView) moved() error {
	inCurrent := v.current != nil && v.current.Version() == 0
	inHistory := false
	for _, h := range v.history {
		if h.Version() == 0 {
			inHistory = true
		}
	}
	inSkeleton := false
	for _, s := range v.skeleton {
		if s == 0 {
			inSkeleton = true
		}
	}
	switch {
	case !inCurrent && !inHistory:
		return fmt.Errorf("moved row v0 in neither the current row nor GetHistory")
	case !inCurrent && !inSkeleton:
		return fmt.Errorf("moved row v0 in neither the current row nor the temporal skeletons")
	case v.asOfErr != nil:
		return fmt.Errorf("AsOf(pin before the move): %w", v.asOfErr)
	case v.asOf.Version() != 0:
		return fmt.Errorf("AsOf(pin before the move) = v%d, want v0", v.asOf.Version())
	}
	return nil
}

func mpoNodeView(bs *Store, id types.NodeID) mpoView {
	var v mpoView
	if n, err := bs.GetNode(id); err == nil {
		v.current = n
	}
	if hist, err := bs.GetNodeHistory(id); err == nil {
		for _, h := range hist {
			v.history = append(v.history, h)
		}
	}
	if metas, err := bs.NodeHistoryTemporalMeta(id); err == nil {
		for _, m := range metas {
			v.skeleton = append(v.skeleton, m.Version)
		}
	}
	n, err := bs.NodeAsOf(id, mpoPin)
	v.asOf, v.asOfErr = n, err
	return v
}

func mpoRelView(bs *Store, id types.RelID) mpoView {
	var v mpoView
	if r, err := bs.GetRelationship(id); err == nil {
		v.current = r
	}
	if hist, err := bs.GetRelHistory(id); err == nil {
		for _, h := range hist {
			v.history = append(v.history, h)
		}
	}
	if metas, err := bs.RelHistoryTemporalMeta(id); err == nil {
		for _, m := range metas {
			v.skeleton = append(v.skeleton, m.Version)
		}
	}
	r, err := bs.RelAsOf(id, mpoPin)
	v.asOf, v.asOfErr = r, err
	return v
}

// TestWithHistoryDoorsPublishHistoryBeforeCurrent pauses each with-history
// door between the two halves of its move and reads the moved entity there:
// the moved row must be visible (current or history) and the as-of answer at
// a pin before the move must be that row. Breaks: cache published before the
// history row (the fault), on every door; per backend flavour (in-memory, on
// disk, rows flushed to badger before the move).
func TestWithHistoryDoorsPublishHistoryBeforeCurrent(t *testing.T) {
	const (
		nodeID  = types.NodeID(1)
		otherID = types.NodeID(2)
		relID   = types.RelID(100)
	)
	type door struct {
		name string
		rel  bool // the moved entity is relationship relID, else node nodeID
		run  func(bs *Store) error
	}
	node := func(bs *Store) *types.Node {
		n, err := bs.GetNode(nodeID)
		if err != nil {
			panic(err)
		}
		return n
	}
	rel := func(bs *Store) *types.Relationship {
		r, err := bs.GetRelationship(relID)
		if err != nil {
			panic(err)
		}
		return r
	}
	doors := []door{
		{"ReplaceNodeWithHistory", false, func(bs *Store) error {
			cur := node(bs)
			next, prev := cur.DeepCopy(), cur.DeepCopy()
			next.SetVersion(1)
			next.SetTemporal(mpoNext())
			if err := next.SetProperty("x", int64(1)); err != nil {
				return err
			}
			prev.SetTemporal(mpoSuperseded(false))
			return bs.ReplaceNodeWithHistory(next, 0, prev)
		}},
		{"AddNodeLabelTokenWithHistory", false, func(bs *Store) error {
			cur := node(bs)
			next, prev := cur.DeepCopy(), cur.DeepCopy()
			next.AddLabelTokenRaw(30)
			next.SetVersion(1)
			next.SetTemporal(mpoNext())
			prev.SetTemporal(mpoSuperseded(false))
			return bs.AddNodeLabelTokenWithHistory(nodeID, 30, next, 0, prev)
		}},
		{"RemoveNodeLabelTokenWithHistory", false, func(bs *Store) error {
			cur := node(bs)
			next, prev := cur.DeepCopy(), cur.DeepCopy()
			next.RemoveLabelTokenRaw(20)
			next.SetVersion(1)
			next.SetTemporal(mpoNext())
			prev.SetTemporal(mpoSuperseded(false))
			return bs.RemoveNodeLabelTokenWithHistory(nodeID, 20, next, 0, prev)
		}},
		{"DeleteNodeWithHistory/node", false, func(bs *Store) error {
			tomb := node(bs).DeepCopy()
			tomb.SetTemporal(mpoSuperseded(true))
			rt := rel(bs).DeepCopy()
			rt.SetTemporal(mpoSuperseded(true))
			return bs.DeleteNodeWithHistory(nodeID, 0, tomb, []RelTombstone{{ID: relID, PrevVersion: 0, Tombstone: rt}})
		}},
		{"DeleteNodeWithHistory/rel", true, func(bs *Store) error {
			tomb := node(bs).DeepCopy()
			tomb.SetTemporal(mpoSuperseded(true))
			rt := rel(bs).DeepCopy()
			rt.SetTemporal(mpoSuperseded(true))
			return bs.DeleteNodeWithHistory(nodeID, 0, tomb, []RelTombstone{{ID: relID, PrevVersion: 0, Tombstone: rt}})
		}},
		{"ReplaceRelWithHistory", true, func(bs *Store) error {
			cur := rel(bs)
			next, prev := cur.DeepCopy(), cur.DeepCopy()
			next.SetVersion(1)
			next.SetTemporal(mpoNext())
			if err := next.SetProperty("x", int64(1)); err != nil {
				return err
			}
			prev.SetTemporal(mpoSuperseded(false))
			return bs.ReplaceRelWithHistory(next, 0, prev)
		}},
		{"DeleteRelWithHistory", true, func(bs *Store) error {
			tomb := rel(bs).DeepCopy()
			tomb.SetTemporal(mpoSuperseded(true))
			return bs.DeleteRelWithHistory(relID, 0, tomb)
		}},
	}
	flavours := []struct {
		name  string
		open  func(t *testing.T) *Store
		flush bool
	}{
		{"in-memory", func(t *testing.T) *Store { return mpoOpen(t, Config{InMemory: true, FlushInterval: time.Hour}) }, false},
		{"on-disk", func(t *testing.T) *Store { return mpoOpen(t, Config{Dir: t.TempDir(), FlushInterval: time.Hour}) }, false},
		{"flushed", func(t *testing.T) *Store { return mpoOpen(t, Config{InMemory: true, FlushInterval: time.Hour}) }, true},
	}
	for _, fl := range flavours {
		for _, d := range doors {
			t.Run(fl.name+"/"+d.name, func(t *testing.T) {
				bs := fl.open(t)
				n := types.NewNode(nodeID, 10, []uint16{20})
				n.SetTemporal(mpoTemporal())
				if err := n.SetProperty("x", int64(0)); err != nil {
					t.Fatal(err)
				}
				other := types.NewNode(otherID, 10, nil)
				other.SetTemporal(mpoTemporal())
				r := types.NewRelationship(relID, 5, nodeID, otherID)
				r.SetTemporal(mpoTemporal())
				if err := r.SetProperty("x", int64(0)); err != nil {
					t.Fatal(err)
				}
				for _, err := range []error{bs.PutNode(n), bs.PutNode(other), bs.PutRelationship(r)} {
					if err != nil {
						t.Fatalf("setup: %v", err)
					}
				}
				if fl.flush {
					if err := bs.Flush(); err != nil {
						t.Fatalf("Flush: %v", err)
					}
				}
				view := func() mpoView {
					if d.rel {
						return mpoRelView(bs, relID)
					}
					return mpoNodeView(bs, nodeID)
				}
				// Warm the lazy builds (history presence) outside the hook:
				// they take idxMu, which the paused door holds.
				if err := view().moved(); err != nil {
					t.Fatalf("before the move: %v", err)
				}

				fired := 0
				var mid error
				bs.SetMoveTestHookForTest(func() {
					fired++
					if fired == 1 {
						mid = view().moved()
					}
				})
				if err := d.run(bs); err != nil {
					t.Fatalf("%s: %v", d.name, err)
				}
				bs.SetMoveTestHookForTest(nil)
				if fired == 0 {
					t.Fatalf("%s never reached the move hook", d.name)
				}
				if mid != nil {
					t.Errorf("mid-move read of %s: %v", d.name, mid)
				}
				if err := view().moved(); err != nil {
					t.Errorf("after the move: %v", err)
				}
			})
		}
	}
}

func mpoOpen(t *testing.T, cfg Config) *Store {
	t.Helper()
	bs, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	return bs
}
