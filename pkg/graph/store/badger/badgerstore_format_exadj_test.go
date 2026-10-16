package badger

import (
	"encoding/binary"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Backlog 50: this release keeps the ex-adjacency index (0x0D) at every
// relationship delete; a release before it would delete relationships without
// it, and the adjacency doors would then miss those relationships. Releases up
// to v4.51.1 refuse a store marker above 2 (their CurrentWireFormatVersion), so
// a store this release opened — including one stamped 2 before — must carry a
// higher marker.
func TestStoreMarkerRefusedByReleasesWithoutExAdjacency(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	const lastMarkerWithoutExAdjacency = 2
	dir := t.TempDir()
	newDiskStoreWithNode(t, dir, 400)
	if v, found := readRawMarker(t, dir); !found || v <= lastMarkerWithoutExAdjacency {
		t.Fatalf("marker = (%d, %v), want above %d", v, found, lastMarkerWithoutExAdjacency)
	}
	old := make([]byte, 2)
	binary.BigEndian.PutUint16(old, lastMarkerWithoutExAdjacency)
	updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.WireFormatVersionKey, old)
	})
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open a store stamped %d: %v", lastMarkerWithoutExAdjacency, err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if v, _ := readRawMarker(t, dir); v <= lastMarkerWithoutExAdjacency {
		t.Fatalf("marker after reopen = %d, want re-stamped above %d", v, lastMarkerWithoutExAdjacency)
	}
}
