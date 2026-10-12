package badger

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// snapReads is the read surface a store.ReadSnapshot shares with the store.
type snapReads interface {
	GetNode(types.NodeID) (*types.Node, error)
	GetRelationship(types.RelID) (*types.Relationship, error)
	HasNodeHistory(types.NodeID) (bool, error)
	HasRelHistory(types.RelID) (bool, error)
	NodeHistoryTemporalMeta(types.NodeID) ([]storecontract.VersionTemporalMeta, error)
	RelHistoryTemporalMeta(types.RelID) ([]storecontract.VersionTemporalMeta, error)
	GetNodeVersion(types.NodeID, uint32) (*types.Node, error)
	GetRelVersion(types.RelID, uint32) (*types.Relationship, error)
	GetNodeHistory(types.NodeID) ([]*types.Node, error)
	GetRelHistory(types.RelID) ([]*types.Relationship, error)
}

// rsRender renders every read of node 1 and relationship 7 through r.
func rsRender(r snapReads) string {
	var b strings.Builder
	errStr := func(err error) string { return "err(" + err.Error() + ")" }
	row := func(v uint32, tm *types.TemporalMetadata, x any) string {
		if tm == nil {
			return fmt.Sprintf("v%d x=%v", v, x)
		}
		return fmt.Sprintf("v%d tx%d-%d del%d r%v x=%v", v, tm.TxFrom, tm.TxTo, tm.DeletedAt, tm.Retracted, x)
	}
	node := func(n *types.Node, err error) string {
		if err != nil {
			return errStr(err)
		}
		x, _ := n.GetProperty("x")
		return row(n.Version(), n.Temporal(), x)
	}
	rel := func(r *types.Relationship, err error) string {
		if err != nil {
			return errStr(err)
		}
		x, _ := r.GetProperty("x")
		return row(r.Version(), r.Temporal(), x)
	}
	metas := func(ms []storecontract.VersionTemporalMeta, err error) string {
		if err != nil {
			return errStr(err)
		}
		parts := make([]string, len(ms))
		for i, m := range ms {
			parts[i] = row(m.Version, m.Temporal, nil)
		}
		return "[" + strings.Join(parts, " ") + "]"
	}
	has := func(h bool, err error) string {
		if err != nil {
			return errStr(err)
		}
		return fmt.Sprint(h)
	}
	nid, rid := types.NodeID(1), types.RelID(7)
	fmt.Fprintf(&b, "node %s has=%s meta=%s", node(r.GetNode(nid)), has(r.HasNodeHistory(nid)), metas(r.NodeHistoryTemporalMeta(nid)))
	for v := uint32(0); v < 3; v++ {
		fmt.Fprintf(&b, " v%d=%s", v, node(r.GetNodeVersion(nid, v)))
	}
	hn, err := r.GetNodeHistory(nid)
	if err != nil {
		fmt.Fprintf(&b, " hist=%s", errStr(err))
	}
	for _, n := range hn {
		fmt.Fprintf(&b, " hist:%s", node(n, nil))
	}
	fmt.Fprintf(&b, "\nrel %s has=%s meta=%s", rel(r.GetRelationship(rid)), has(r.HasRelHistory(rid)), metas(r.RelHistoryTemporalMeta(rid)))
	for v := uint32(0); v < 3; v++ {
		fmt.Fprintf(&b, " v%d=%s", v, rel(r.GetRelVersion(rid, v)))
	}
	hr, err := r.GetRelHistory(rid)
	if err != nil {
		fmt.Fprintf(&b, " hist=%s", errStr(err))
	}
	for _, x := range hr {
		fmt.Fprintf(&b, " hist:%s", rel(x, nil))
	}
	return b.String()
}

// TestReadSnapshotAnswersAsTheStore (backlog 55): one read snapshot, kept
// across steps as an effective scan keeps it within a chunk (released where a
// chunk ends), answers each read of node 1 and relationship 7 exactly as the
// store's own method does at that moment — rows only in the write buffer,
// after a flush, a flush between two reads of one entity (the history memo
// must not outlive the transaction), current rows evicted from a one-row
// cache, a history truncation buffered and flushed, deleted entities with
// history rows only, a re-imported life, a retracted life, Clear with the
// transaction open — and opens one transaction per commit it has to see, not
// one per read. The oracle is the store itself, so the snapshot's copy of the
// read paths cannot drift from them unseen.
func TestReadSnapshotAnswersAsTheStore(t *testing.T) {
	bs, err := New(Config{InMemory: true, CacheCapacity: 1, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	snap := bs.NewReadSnapshot()
	defer snap.Close()

	putTestNode(t, bs, 2, 10, nil)
	write := func(v uint32, x int64) func(t *testing.T) {
		return func(t *testing.T) {
			t.Helper()
			n := types.NewNode(types.NodeID(1), 10, nil)
			r := types.NewRelationship(types.RelID(7), 5, types.NodeID(1), types.NodeID(2))
			for _, e := range []interface {
				SetVersion(uint32)
				SetTemporal(*types.TemporalMetadata)
				SetProperty(string, any) error
			}{n, r} {
				e.SetVersion(v)
				e.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(100 * (v + 1)), ValidFrom: 1})
				if err := e.SetProperty("x", x); err != nil {
					t.Fatal(err)
				}
			}
			if v > 0 { // the previous version moves to history
				prevN, err := bs.GetNode(types.NodeID(1))
				if err != nil {
					t.Fatal(err)
				}
				prevR, err := bs.GetRelationship(types.RelID(7))
				if err != nil {
					t.Fatal(err)
				}
				if err := bs.PutNodeVersion(types.NodeID(1), v-1, prevN); err != nil {
					t.Fatal(err)
				}
				if err := bs.PutRelVersion(types.RelID(7), v-1, prevR); err != nil {
					t.Fatal(err)
				}
				if err := bs.ReplaceNode(n); err != nil {
					t.Fatal(err)
				}
				if err := bs.ReplaceRelationship(r); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err := bs.PutNode(n); err != nil {
				t.Fatal(err)
			}
			if err := bs.PutRelationship(r); err != nil {
				t.Fatal(err)
			}
		}
	}
	putCurrent := func(t *testing.T, v uint32, x int64) { // a new life of both IDs
		t.Helper()
		tm := types.TemporalMetadata{TxFrom: types.Instant(100 * (v + 1)), ValidFrom: 1}
		n := types.NewNode(types.NodeID(1), 10, nil)
		r := types.NewRelationship(types.RelID(7), 5, types.NodeID(1), types.NodeID(2))
		n.SetVersion(v)
		r.SetVersion(v)
		tmN, tmR := tm, tm
		n.SetTemporal(&tmN)
		r.SetTemporal(&tmR)
		if err := n.SetProperty("x", x); err != nil {
			t.Fatal(err)
		}
		if err := r.SetProperty("x", x); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	flush := func(t *testing.T) {
		t.Helper()
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	steps := []struct {
		name   string
		before func() // reads through the snapshot before the change
		change func(t *testing.T)
		txns   int64 // most transactions the snapshot may open for the check
	}{
		{"rows only in the write buffer", nil, func(t *testing.T) { write(0, 10)(t); write(1, 11)(t) }, 1},
		{"after a flush", nil, flush, 1},
		{"flush between two reads of one entity", func() {
			_, _ = snap.RelHistoryTemporalMeta(types.RelID(7))
			_, _ = snap.NodeHistoryTemporalMeta(types.NodeID(1))
		}, func(t *testing.T) { write(2, 12)(t); flush(t) }, 1},
		{"current rows evicted", nil, func(t *testing.T) { putTestNode(t, bs, 3, 10, nil); flush(t); putTestNode(t, bs, 4, 10, nil) }, 1},
		{"history truncated, buffered", nil, func(t *testing.T) {
			if err := bs.TruncateRelHistory(types.RelID(7), 1); err != nil {
				t.Fatal(err)
			}
			if err := bs.TruncateNodeHistory(types.NodeID(1), 1); err != nil {
				t.Fatal(err)
			}
		}, 0},
		{"history truncated, flushed", nil, flush, 1},
		{"released between runs", func() { snap.Release() }, func(*testing.T) {}, 1},
		{"deleted, history rows only, buffered", nil, func(t *testing.T) {
			prevN, err := bs.GetNode(types.NodeID(1))
			if err != nil {
				t.Fatal(err)
			}
			prevR, err := bs.GetRelationship(types.RelID(7))
			if err != nil {
				t.Fatal(err)
			}
			if err := bs.PutRelVersion(types.RelID(7), prevR.Version(), prevR); err != nil {
				t.Fatal(err)
			}
			if err := bs.PutNodeVersion(types.NodeID(1), prevN.Version(), prevN); err != nil {
				t.Fatal(err)
			}
			if err := bs.DeleteRelationship(types.RelID(7)); err != nil {
				t.Fatal(err)
			}
			if err := bs.DeleteNode(types.NodeID(1)); err != nil {
				t.Fatal(err)
			}
		}, 1},
		{"deleted, flushed", nil, flush, 1},
		{"re-imported, buffered", nil, func(t *testing.T) { putCurrent(t, 3, 13) }, 0},
		{"re-imported, flushed", nil, flush, 1},
		{"retracted life, buffered", nil, func(t *testing.T) {
			tm := &types.TemporalMetadata{TxFrom: 50, TxTo: 60, ValidFrom: 1, DeletedAt: 60, Retracted: true}
			n := types.NewNode(types.NodeID(1), 10, nil)
			r := types.NewRelationship(types.RelID(7), 5, types.NodeID(1), types.NodeID(2))
			n.SetVersion(5)
			r.SetVersion(5)
			n.SetTemporal(tm)
			tmR := *tm
			r.SetTemporal(&tmR)
			if err := bs.PutNodeVersion(types.NodeID(1), 5, n); err != nil {
				t.Fatal(err)
			}
			if err := bs.PutRelVersion(types.RelID(7), 5, r); err != nil {
				t.Fatal(err)
			}
		}, 0},
		{"retracted life, flushed", nil, flush, 1},
		{"after Clear", nil, func(t *testing.T) {
			if err := bs.Clear(); err != nil {
				t.Fatal(err)
			}
		}, 1},
	}
	for _, st := range steps {
		if st.before != nil {
			st.before()
		}
		st.change(t)
		want := rsRender(bs)
		start := ReadTxnsTotalForTest()
		got := rsRender(snap)
		if opened := ReadTxnsTotalForTest() - start; opened > st.txns {
			t.Errorf("%s: the snapshot opened %d transactions for one round of reads, want <= %d", st.name, opened, st.txns)
		}
		if got != want {
			t.Fatalf("%s: snapshot answers differ from the store's:\n got  %s\n want %s", st.name, got, want)
		}
	}

	// Close of the store first, then of the snapshot: no hang, no panic; a
	// read after the close fails as the store's does.
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := snap.GetRelationship(types.RelID(7)); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("read after Close: %v, want ErrStoreClosed", err)
	}
	snap.Close()
	snap.Close()
}
