package badger

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Ledger row 62: the windowed property membership on badger answers the same
// from the write path's records, from an open image after a clean close, and
// from a build scan after an untrusted reopen.

func winRel(id int64, seat int64, version uint32, txFrom, txTo, vf, vt types.Instant) *types.Relationship {
	r := types.NewRelationship(types.RelID(id), ptxT, types.NodeID(1), types.NodeID(2))
	_ = r.SetProperty("seat", seat)
	r.SetVersion(version)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom, TxTo: txTo, ValidFrom: vf, ValidTo: vt})
	return r
}

// winDump renders the windowed members of seat=1 for a fixed set of windows
// and pins.
func winDump(t *testing.T, bs *Store) string {
	t.Helper()
	var lines []string
	for _, w := range [][2]types.Instant{{1000, math.MaxInt64}, {100, 101}, {250, 270}, {types.UnboundedValidFrom, math.MaxInt64}} {
		for _, pin := range []types.Instant{0, 150, 250, 350, 450, 1 << 40} {
			var ids []int64
			if err := bs.ForEachRelPropertyTxMemberDuring(ptxT, "seat", winVK(t), w[0], w[1], pin, func(id types.RelID, _ types.Instant) bool {
				ids = append(ids, int64(id))
				return true
			}); err != nil {
				t.Fatalf("window: %v", err)
			}
			slices.Sort(ids)
			lines = append(lines, fmt.Sprintf("[%d,%d)@%d=%v", w[0], w[1], pin, ids))
		}
	}
	return strings.Join(lines, " ")
}

func winVK(t *testing.T) string {
	t.Helper()
	vk := types.IndexablePropertyValueKey(int64(1))
	if vk == "" {
		t.Fatal("no index key for 1")
	}
	return vk
}

// TestPropTxWindow_WritePathImageScanAgree — members of seat=1 (valid and
// transaction times on one clock): (11) open since 100, replaced at 200 by a
// row valid [210, 260); (12) open since 100, deleted at 300 (the delete
// closes it there); (13) valid [100, 120); (14) open since 100, current;
// (15) open since 100, replaced at 400 by a row of another seat. The late
// window [1000, inf) holds 14 only at the newest pin, and before each
// replacement or delete the open row answers too. The sidecar the write path
// maintains, the one restored from the open image and the one a scan
// rebuilds answer alike; the restore scans nothing.
func TestPropTxWindow_WritePathImageScanAgree(t *testing.T) {
	dir := t.TempDir()
	open := func() *Store {
		t.Helper()
		bs, err := New(Config{Dir: dir})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return bs
	}
	must := func(what string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	bs := open()
	putTestNode(t, bs, 1, 9, nil)
	putTestNode(t, bs, 2, 9, nil)
	must("index", bs.CreateRelPropertyIndex(ptxT, "seat"))
	_ = winDump(t, bs) // builds the (empty) sidecar: the writes below are recorded by the write path

	must("put 11", bs.PutRelationship(winRel(11, 1, 0, 100, 0, 100, 0)))
	must("replace 11", bs.ReplaceRelWithHistory(winRel(11, 1, 1, 200, 0, 210, 260), 0, winRel(11, 1, 0, 100, 200, 100, 0)))
	must("put 12", bs.PutRelationship(winRel(12, 1, 0, 100, 0, 100, 0)))
	tomb := winRel(12, 1, 0, 100, 300, 100, 300)
	tomb.Temporal().DeletedAt = 300
	must("delete 12", bs.DeleteRelWithHistory(types.RelID(12), 0, tomb))
	must("put 13", bs.PutRelationship(winRel(13, 1, 0, 100, 0, 100, 120)))
	must("put 14", bs.PutRelationship(winRel(14, 1, 0, 100, 0, 100, 0)))
	must("put 15", bs.PutRelationship(winRel(15, 1, 0, 100, 0, 100, 0)))
	must("replace 15", bs.ReplaceRelWithHistory(winRel(15, 2, 1, 400, 0, 410, 0), 0, winRel(15, 1, 0, 100, 400, 100, 0)))
	must("flush", bs.Flush())

	live := winDump(t, bs)
	for _, want := range []string{
		"[1000,9223372036854775807)@0=[14]",
		"[1000,9223372036854775807)@1099511627776=[14]",
		"[1000,9223372036854775807)@450=[14]",
		"[1000,9223372036854775807)@350=[14 15]",
		"[1000,9223372036854775807)@250=[12 14 15]",
		"[1000,9223372036854775807)@150=[11 12 14 15]",
		"[100,101)@0=[11 12 13 14 15]",
		"[250,270)@0=[11 12 14 15]",
	} {
		if !strings.Contains(live, want) {
			t.Fatalf("write path: missing %s in\n%s", want, live)
		}
	}
	must("close", bs.Close())

	bs = open()
	scans := opCountScans(bs)
	if got := winDump(t, bs); got != live {
		t.Fatalf("after a clean reopen\n got %s\nwant %s", got, live)
	}
	if n := scans.Load(); n != 0 {
		t.Fatalf("the restored sidecar scanned the store %d times", n)
	}
	must("close", bs.Close())

	// A commit after the clean close: the image is not trusted, a scan rebuilds.
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	must("raw open", err)
	must("foreign commit", db.Update(func(txn *badgerv4.Txn) error { return txn.Set([]byte("\xfeforeign"), nil) }))
	must("raw close", db.Close())
	bs = open()
	defer bs.Close()
	scans = opCountScans(bs)
	if got := winDump(t, bs); got != live {
		t.Fatalf("after a scan rebuild\n got %s\nwant %s", got, live)
	}
	if n := scans.Load(); n != 1 {
		t.Fatalf("rebuild scanned %d times, want 1", n)
	}
}
