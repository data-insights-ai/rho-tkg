package tiered

import (
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestColdOpen_TransientHandleBuildsNoLabelMapAtOpen: the label index in RAM is
// built at every open from one walk of the shard's label keys (2.3 MB of the
// 7 MB and 34 % of the CPU of an open at 50,000 rows,
// tasks/evidence/R3/coldread.txt), and a handle that closes when its read ends
// (MaxOpenColdShards 0) mostly never reads a label. Such a handle opens with
// the trusted image's per-label counts and no label map: a label's members are
// read from its keys when a read first asks, once (R3 cold open,
// badger.Config.TransientRead). Label maps built by the open, by the shard's
// handle: none for a transient read-only handle, one for the label the first
// read asks of and no second build for the same label; a handle a cap keeps
// builds its maps at the open as every other shard does. (LabelIndexOnDisk is
// not the way: it makes the doc-values doors decline, so a tiered
// ForEachDocValues that reached a cold shard declined as a whole,
// TestTieredDocValues_ColdShardParticipation.)
func TestColdOpen_TransientHandleBuildsNoLabelMapAtOpen(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name       string
		maxOpen    int
		labelsDisk bool // Config.LabelIndexOnDisk
		wantLazy   bool
		wantAtOpen int // label maps in RAM right after the open (the fixture's node carries one label)
	}{
		{"cap 0", 0, false, true, 0},
		{"cap 0, label index on disk: no map either way", 0, true, false, 0},
		{"cap 2 retains: the maps are built at the open", 2, false, false, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ts, name, n, _ := coldOpenFixture(t, func(cfg *Config) {
				cfg.MaxOpenColdShards = c.maxOpen
				cfg.LabelIndexOnDisk = c.labelsDisk
			})
			store, release := readOnlyColdHandle(t, ts, name)
			defer release()
			lazy, atOpen := store.LabelMapStateForTest()
			if lazy != c.wantLazy || atOpen != c.wantAtOpen {
				t.Fatalf("right after the open: lazy label maps %v with %d maps in RAM, want %v with %d", lazy, atOpen, c.wantLazy, c.wantAtOpen)
			}
			// A label read builds the label's map once, in either mode, and answers the same.
			for read := 1; read <= 2; read++ {
				ids, err := store.NodeIDsByLabel(mintSignalTok)
				if err != nil || !slices.Equal(ids, []types.NodeID{n}) {
					t.Fatalf("NodeIDsByLabel read %d = %v, %v, want [%d]", read, ids, err, n)
				}
				wantBuilt := c.wantAtOpen
				if c.wantLazy {
					wantBuilt = 1
				}
				if _, built := store.LabelMapStateForTest(); built != wantBuilt {
					t.Fatalf("after label read %d: %d label maps in RAM, want %d", read, built, wantBuilt)
				}
			}
		})
	}
}

// TestColdOpen_WritableHandleKeepsTheLabelMap: the writable open a late write
// makes builds the label map at the open: the lazy maps are for a handle that
// only reads and closes.
func TestColdOpen_WritableHandleKeepsTheLabelMap(t *testing.T) {
	t.Parallel()
	ts, name, _, late := coldOpenFixture(t, nil)
	if err := ts.PutNode(types.NewNode(late, mintSignalTok, nil)); err != nil {
		t.Fatalf("late write: %v", err)
	}
	store, ro := ts.EventShardsForTest()[name].StoreStateForTest()
	if store == nil || ro {
		t.Fatalf("the late write left store %v read-only %v, want a writable handle", store != nil, ro)
	}
	if lazy, maps := store.LabelMapStateForTest(); lazy || maps != 1 {
		t.Fatalf("the writable cold handle has lazy label maps %v with %d maps in RAM, want the map built with 1", lazy, maps)
	}
}
