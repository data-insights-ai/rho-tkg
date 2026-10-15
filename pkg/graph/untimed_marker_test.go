package graph_test

// Backlog 73: an UntimedUnbounded store raises the store format marker to
// UntimedUnboundedStoreFormatVersion (7) with its mode key, so a release that
// reads markers up to 6 (it ignores the key and would write TxFrom-start
// creates into the store) refuses it at open. A TxFrom store keeps its
// marker.

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Not parallel: it moves the badger package's readable-marker cap.
func TestUntimedUnboundedRaisesStoreFormatMarker(t *testing.T) {
	ctx := context.Background()
	open := map[string]func(dir string) (storepkg.MandatoryStore, error){
		"badger": func(dir string) (storepkg.MandatoryStore, error) { return badger.New(badger.Config{Dir: dir}) },
		"tiered": func(dir string) (storepkg.MandatoryStore, error) {
			return tiered.New(tiered.Config{DataDir: dir, ShardWindow: 7 * 24 * time.Hour, RefLabels: []string{"Ref"}})
		},
		"sharded": func(dir string) (storepkg.MandatoryStore, error) {
			return sharded.New(sharded.Config{Dir: dir, BaseSlot: 0, SlotCount: 2})
		},
	}
	for _, name := range []string{"badger", "tiered", "sharded"} {
		openStore := open[name]
		t.Run(name, func(t *testing.T) {
			write := func(dir string, mode graphpkg.UntimedValidStart, convert bool) {
				t.Helper()
				st, err := openStore(dir)
				if err != nil {
					t.Fatal(err)
				}
				g, err := graphpkg.New(graphpkg.Config{Store: st, UntimedValidStart: mode})
				if err != nil {
					_ = st.Close()
					t.Fatal(err)
				}
				if _, err := g.Nodes().Add(ctx, []string{"U"}, nil); err != nil {
					t.Fatal(err)
				}
				if convert {
					if err := g.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedUnbounded); err != nil {
						t.Fatal(err)
					}
				}
				if err := g.Close(); err != nil {
					t.Fatal(err)
				}
			}
			// olderOpens opens dir as a release reading markers up to 6.
			olderOpens := func(dir string) error {
				t.Helper()
				restore := badger.SetMaxReadableStoreFormatVersionForTest(storeutil.VoidStoreFormatVersion)
				defer restore()
				st, err := openStore(dir)
				if err != nil {
					return err
				}
				return st.Close()
			}

			txDir := t.TempDir()
			write(txDir, graphpkg.UntimedFromTxFrom, false)
			if err := olderOpens(txDir); err != nil {
				t.Fatalf("a TxFrom store must stay readable by a marker-6 release: %v", err)
			}

			unbDir := t.TempDir()
			write(unbDir, graphpkg.UntimedUnbounded, false)
			if err := olderOpens(unbDir); !errors.Is(err, storepkg.ErrWireFormatVersionUnsupported) {
				t.Fatalf("a marker-6 release opened a store created unbounded: err = %v, want ErrWireFormatVersionUnsupported", err)
			}

			convDir := t.TempDir()
			write(convDir, graphpkg.UntimedFromTxFrom, true)
			if err := olderOpens(convDir); !errors.Is(err, storepkg.ErrWireFormatVersionUnsupported) {
				t.Fatalf("a marker-6 release opened a converted store: err = %v, want ErrWireFormatVersionUnsupported", err)
			}

			if name != "badger" {
				return
			}
			for dir, want := range map[string]uint16{txDir: storeutil.BinaryHashStoreFormatVersion, unbDir: storeutil.UntimedUnboundedStoreFormatVersion, convDir: storeutil.UntimedUnboundedStoreFormatVersion} {
				if got := uvsRawMarker(t, dir); got != want {
					t.Errorf("%s: marker %d, want %d", dir, got, want)
				}
			}
		})
	}
}

func uvsRawMarker(t *testing.T, dir string) uint16 {
	t.Helper()
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var m []byte
	if err := db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storeutil.WireFormatVersionKey)
		if err != nil {
			return err
		}
		m, err = item.ValueCopy(nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(m) != 2 {
		t.Fatalf("marker %v", m)
	}
	return uint16(m[0])<<8 | uint16(m[1])
}
