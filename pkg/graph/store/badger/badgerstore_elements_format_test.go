package badger

import (
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Backlog 63: an Elements definition is a field an older binary skips, which
// would load the index plain and key scalars only. A store that persists one
// is stamped ElementsStoreFormatVersion, so a reader that predates it refuses
// the open; a store without Elements keeps StoreFormatVersion and still opens
// there. Node and relationship definitions both raise it.
func TestElementsIndexStampsFormatMarker(t *testing.T) {
	elements := storecontract.PropertyIndexOptions{Elements: true}
	for _, c := range []struct {
		name   string
		create func(*Store) error
		want   uint16
	}{
		{"plain", func(bs *Store) error {
			return bs.CreatePropertyIndexWithOptions(1, "v", storecontract.PropertyIndexOptions{RangeCounts: true})
		}, openStampVersion()},
		{"node elements", func(bs *Store) error { return bs.CreatePropertyIndexWithOptions(1, "v", elements) }, storepkg.ElementsStoreFormatVersion},
		{"rel elements", func(bs *Store) error { return bs.CreateRelPropertyIndexWithOptions(1, "v", elements) }, storepkg.ElementsStoreFormatVersion},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			bs, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.create(bs); err != nil {
				t.Fatal(err)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			if got, ok := readRawMarker(t, dir); !ok || got != c.want {
				t.Fatalf("marker = %d (present %v), want %d", got, ok, c.want)
			}
			// This binary reopens it and keeps the marker.
			bs, err = New(Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			if got, _ := readRawMarker(t, dir); got != c.want {
				t.Fatalf("marker after reopen = %d, want %d", got, c.want)
			}
			// A reader that predates Elements.
			prev := maxReadableStoreFormatVersion
			maxReadableStoreFormatVersion = openStampVersion()
			defer func() { maxReadableStoreFormatVersion = prev }()
			bs, err = New(Config{Dir: dir})
			if c.want == openStampVersion() {
				if err != nil {
					t.Fatalf("older reader on a store without Elements: %v", err)
				}
				_ = bs.Close()
				return
			}
			if err == nil {
				_ = bs.Close()
				t.Fatal("older reader opened an Elements store")
			}
			if !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
				t.Fatalf("older reader: %v, want ErrWireFormatVersionUnsupported", err)
			}
		})
	}
}

// An Elements definition reopened under PropertyIndexOnDisk would be served by
// the shared on-disk keyspace, which keys scalars only (equality answers):
// the open is refused.
func TestElementsIndexRefusedOnDiskReopen(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.CreatePropertyIndexWithOptions(1, "v", storecontract.PropertyIndexOptions{Elements: true}); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs, err = New(Config{Dir: dir, PropertyIndexOnDisk: true})
	if err == nil {
		_ = bs.Close()
		t.Fatal("PropertyIndexOnDisk opened a store with an Elements index")
	}
	if !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatalf("open: %v, want ErrCapabilityNotSupported", err)
	}
}
