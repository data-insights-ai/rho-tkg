package badger

import (
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 44: the commit floor persisted with the rows
// (badgerstore_commit_floor.go). Each case writes rows with chosen stamps,
// reopens the directory and checks StoredCommitFloor.

func cfNodeRow(id int64, ver uint32, tf, tt, da types.Instant) *types.Node {
	n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
	n.SetVersion(ver)
	n.SetTemporal(stampTM(tf, tt, da))
	return n
}

func TestStoredCommitFloor(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	put := func(t *testing.T, bs *Store, id int64, tf types.Instant) {
		t.Helper()
		if err := bs.PutNode(cfNodeRow(id, 1, tf, 0, 0)); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	flush := func(t *testing.T, bs *Store) {
		t.Helper()
		if err := bs.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	cases := []struct {
		name     string
		delta    bool
		write    func(t *testing.T, bs *Store)
		readOnly bool // reopen ReadOnly
		want     types.Instant
		live     types.Instant // the in-session answer when it differs (0: want)
	}{
		{name: "empty store", write: func(*testing.T, *Store) {}, want: 0},
		{name: "current node row", write: func(t *testing.T, bs *Store) { put(t, bs, 1, 100) }, want: 100},
		{name: "history tombstone only", write: func(t *testing.T, bs *Store) {
			put(t, bs, 1, 100)
			if err := bs.PutNodeVersion(types.NodeID(2), 1, cfNodeRow(2, 1, 50, 0, 300)); err != nil {
				t.Fatal(err)
			}
		}, want: 300},
		{name: "history rel row", write: func(t *testing.T, bs *Store) {
			r := types.NewRelationship(types.RelID(9), 5, types.NodeID(1), types.NodeID(2))
			r.SetVersion(1)
			r.SetTemporal(stampTM(40, 400, 0))
			if err := bs.PutRelVersion(r.ID(), 1, r); err != nil {
				t.Fatal(err)
			}
		}, want: 400},
		{name: "an older row never lowers it", write: func(t *testing.T, bs *Store) {
			put(t, bs, 1, 500)
			flush(t, bs)
			put(t, bs, 2, 100)
		}, want: 500},
		{name: "delta-encoded history row", delta: true, write: func(t *testing.T, bs *Store) {
			for v := uint32(0); v < 5; v++ {
				tt := types.Instant(0)
				if v == 4 {
					tt = 900 // newest stamp on a delta (non-anchor) version
				}
				if err := bs.PutNodeVersion(types.NodeID(3), v, cfNodeRow(3, v, types.Instant(10+v), tt, 0)); err != nil {
					t.Fatal(err)
				}
			}
		}, want: 900},
		{name: "Clear then rows: re-persisted, not lowered", write: func(t *testing.T, bs *Store) {
			put(t, bs, 1, 900)
			flush(t, bs)
			if err := bs.Clear(); err != nil {
				t.Fatal(err)
			}
			put(t, bs, 2, 50)
		}, want: 900},
		{name: "Clear without rows after: nothing stored", write: func(t *testing.T, bs *Store) {
			put(t, bs, 1, 900)
			flush(t, bs)
			if err := bs.Clear(); err != nil {
				t.Fatal(err)
			}
		}, want: 0, live: 900}, // in session the floor is never lowered; the wiped key reopens empty
		{name: "read-only reopen", readOnly: true, write: func(t *testing.T, bs *Store) { put(t, bs, 1, 700) }, want: 700},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bs, err := New(Config{Dir: dir, HistoryDeltaEncoding: tc.delta})
			if err != nil {
				t.Fatal(err)
			}
			tc.write(t, bs)
			flush(t, bs)
			live := tc.want
			if tc.live != 0 {
				live = tc.live
			}
			if got, err := bs.StoredCommitFloor(); err != nil || got != live {
				t.Fatalf("after flush: StoredCommitFloor = (%d, %v), want %d", got, err, live)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			re, err := New(Config{Dir: dir, ReadOnly: tc.readOnly, HistoryDeltaEncoding: tc.delta})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = re.Close() }()
			if got, err := re.StoredCommitFloor(); err != nil || got != tc.want {
				t.Fatalf("after reopen: StoredCommitFloor = (%d, %v), want %d", got, err, tc.want)
			}
		})
	}
}

// A malformed persisted floor fails the open (as a malformed counter does);
// a closed store answers ErrStoreClosed-class errors.
func TestStoredCommitFloor_MalformedAndClosed(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.MetaSet("tx_commit_floor", []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := bs.StoredCommitFloor(); err == nil {
		t.Fatal("StoredCommitFloor on a closed store: want an error")
	}
	if re, err := New(Config{Dir: dir}); err == nil {
		_ = re.Close()
		t.Fatal("open over a 3-byte commit floor: want an error")
	}
}

// The flush-side cost of the floor: one stamp decode per entity and history
// row of the batch (530 current rows plus 530 history rows, the ai-soc group).
func BenchmarkBatchCommitFloor(b *testing.B) {
	bs, err := New(Config{Dir: b.TempDir(), FlushInterval: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = bs.Close() }()
	for i := int64(1); i <= 530; i++ {
		if err := bs.PutNode(cfNodeRow(i, 2, types.Instant(1000+i), 0, 0)); err != nil {
			b.Fatal(err)
		}
		if err := bs.PutNodeVersion(types.NodeID(snowflake.ID(i)), 1, cfNodeRow(i, 1, types.Instant(i), types.Instant(1000+i), 0)); err != nil {
			b.Fatal(err)
		}
	}
	bs.wbMu.Lock()
	ops := bs.pending
	bs.wbMu.Unlock()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := bs.batchCommitFloor(ops); err != nil {
			b.Fatal(err)
		}
	}
}
