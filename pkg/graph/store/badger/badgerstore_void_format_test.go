package badger

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 64: a store raises its format marker to VoidStoreFormatVersion (6)
// in the batch that writes its first void piece (node or relationship
// history row), and only then; a binary that reads markers up to 5 refuses
// the store at open; this binary reopens it and reads the piece back marked.
// Catches: a marker never raised (an older binary opens the store and reads
// the piece as a valid version), raised on every store (older binaries refuse
// stores without an excise), a marker lost on a later plain write, and the
// void flag dropped by the store's encode path.
// Not parallel: it moves maxReadableStoreFormatVersion.
func TestVoidRowRaisesStoreFormatMarker(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	for _, kind := range []string{"node", "rel"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			nid := types.NodeID(snowflake.ID(300))
			rid := types.RelID(snowflake.ID(301))
			row := func(version uint32, void bool) (*types.Node, *types.Relationship) {
				tm := &types.TemporalMetadata{ValidFrom: 10, TxFrom: types.Instant(100 + version), UpdatedAt: types.Instant(100 + version)}
				if void {
					tm.ValidFrom, tm.ValidTo, tm.Void = 20, 30, true
				}
				n := types.NewNode(nid, 1, nil)
				n.SetVersion(version)
				n.SetTemporal(tm)
				r := types.NewRelationship(rid, 1, nid, nid)
				r.SetVersion(version)
				r.SetTemporal(&types.TemporalMetadata{ValidFrom: tm.ValidFrom, ValidTo: tm.ValidTo, TxFrom: tm.TxFrom, UpdatedAt: tm.UpdatedAt, Void: tm.Void})
				return n, r
			}
			put := func(bs *Store, version uint32, void bool) {
				t.Helper()
				n, r := row(version, void)
				var err error
				if kind == "node" {
					err = bs.PutNodeVersion(nid, version, n)
				} else {
					err = bs.PutRelVersion(rid, version, r)
				}
				if err != nil {
					t.Fatalf("put version %d void=%v: %v", version, void, err)
				}
			}
			open := func() *Store {
				t.Helper()
				bs, err := New(Config{Dir: dir})
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				return bs
			}
			marker := func(want uint16) {
				t.Helper()
				if v, ok := readRawMarker(t, dir); !ok || v != want {
					t.Fatalf("marker = (%d, %v); want %d", v, ok, want)
				}
			}

			bs := open()
			put(bs, 0, false)
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			marker(openStampVersion()) // 3, or 4 under the binary-hash writer

			bs = open()
			put(bs, 1, true)
			put(bs, 2, false)
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			marker(storepkg.VoidStoreFormatVersion)

			prev := maxReadableStoreFormatVersion
			maxReadableStoreFormatVersion = storepkg.VoidStoreFormatVersion - 1
			old, err := New(Config{Dir: dir})
			maxReadableStoreFormatVersion = prev
			if err == nil {
				_ = old.Close()
				t.Fatal("a reader of markers up to 5 opened a store holding a void row")
			}
			if !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
				t.Fatalf("older reader: %v; want ErrWireFormatVersionUnsupported", err)
			}

			bs = open()
			defer func() { _ = bs.Close() }()
			put(bs, 3, false) // a later plain write keeps the marker
			var tm *types.TemporalMetadata
			if kind == "node" {
				got, err := bs.GetNodeVersion(nid, 1)
				if err != nil {
					t.Fatal(err)
				}
				tm = got.Temporal()
			} else {
				got, err := bs.GetRelVersion(rid, 1)
				if err != nil {
					t.Fatal(err)
				}
				tm = got.Temporal()
			}
			if !tm.Void || tm.ValidFrom != 20 || tm.ValidTo != 30 {
				t.Fatalf("void piece read back as %+v", tm)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			marker(storepkg.VoidStoreFormatVersion)
			bs = open()
		})
	}
}
