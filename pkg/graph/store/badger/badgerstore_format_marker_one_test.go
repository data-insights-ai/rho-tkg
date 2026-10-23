package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// One store-level marker, raised monotonically by every feature that needs it
// (binary hashes 4, Elements definitions 5, void rows 6). Whatever order the
// features land in within one flush, and across a Clear, the disk marker is
// the highest the store's contents need. Catches two raise trackers that each
// think the marker is theirs (an Elements raise after a void row in the same
// pending batch overwrote 6 with 5, so a release reading markers up to 5
// opened a store with an excise), and a Clear that forgets a raise.
func TestFormatMarkerOneTrackerAcrossFeatures(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	elements := storecontract.PropertyIndexOptions{Elements: true}
	nid := types.NodeID(snowflake.ID(400))
	void := func(bs *Store, version uint32) {
		t.Helper()
		n := types.NewNode(nid, 1, nil)
		n.SetVersion(version)
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: 20, ValidTo: 30, TxFrom: types.Instant(100 + version), UpdatedAt: types.Instant(100 + version), Void: true})
		if err := bs.PutNodeVersion(nid, version, n); err != nil {
			t.Fatalf("void row: %v", err)
		}
	}
	for _, c := range []struct {
		name string
		run  func(t *testing.T, bs *Store)
		want uint16
	}{
		{"void then elements in one flush", func(t *testing.T, bs *Store) {
			void(bs, 1)
			if err := bs.CreatePropertyIndexWithOptions(1, "v", elements); err != nil {
				t.Fatal(err)
			}
		}, storepkg.VoidStoreFormatVersion},
		{"elements then void in one flush", func(t *testing.T, bs *Store) {
			if err := bs.CreatePropertyIndexWithOptions(1, "v", elements); err != nil {
				t.Fatal(err)
			}
			void(bs, 1)
		}, storepkg.VoidStoreFormatVersion},
		{"void then rel elements", func(t *testing.T, bs *Store) {
			void(bs, 1)
			if err := bs.CreateRelPropertyIndexWithOptions(1, "v", elements); err != nil {
				t.Fatal(err)
			}
		}, storepkg.VoidStoreFormatVersion},
		{"clear, then void", func(t *testing.T, bs *Store) {
			void(bs, 1)
			if err := bs.Clear(); err != nil {
				t.Fatal(err)
			}
			void(bs, 1)
		}, storepkg.VoidStoreFormatVersion},
		{"void, then clear", func(t *testing.T, bs *Store) {
			void(bs, 1)
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := bs.Clear(); err != nil {
				t.Fatal(err)
			}
		}, openStampVersion()},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			bs, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			c.run(t, bs)
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			if got, ok := readRawMarker(t, dir); !ok || got != c.want {
				t.Fatalf("disk marker = %d (present %v); want %d", got, ok, c.want)
			}
		})
	}
}
