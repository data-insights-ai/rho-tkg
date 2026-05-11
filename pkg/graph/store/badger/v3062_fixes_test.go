package badger

import (
	"errors"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// ─── Issue 1: Store — DropTemporalIndex ─────────────────────────────────

func TestBadgerStore_DropTemporalIndex(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Create a label token's index.
	putTestNode(t, bs, 100, 1, nil)
	if err := bs.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	// Drop it.
	if err := bs.DropTemporalIndex(1); err != nil {
		t.Fatalf("DropTemporalIndex: %v", err)
	}

	// Double-drop returns ErrTemporalIndexNotFound.
	err := bs.DropTemporalIndex(1)
	if !errors.Is(err, ErrTemporalIndexNotFound) {
		t.Errorf("double DropTemporalIndex err = %v, want ErrTemporalIndexNotFound", err)
	}
}

// ─── Issue 1: Store — CreateHighFrequencyIndex ──────────────────────────

func TestBadgerStore_CreateHighFrequencyIndex(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Succeed first time.
	if err := bs.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	// Duplicate returns ErrTemporalIndexExists.
	err := bs.CreateHighFrequencyIndex(1, time.Hour)
	if !errors.Is(err, ErrTemporalIndexExists) {
		t.Errorf("duplicate CreateHighFrequencyIndex err = %v, want ErrTemporalIndexExists", err)
	}

	// Conflict with existing temporal index on different label.
	putTestNode(t, bs, 200, 2, nil)
	if err := bs.CreateTemporalIndex(2); err != nil {
		t.Fatalf("CreateTemporalIndex(2): %v", err)
	}
	err = bs.CreateHighFrequencyIndex(2, time.Hour)
	if !errors.Is(err, ErrTemporalIndexExists) {
		t.Errorf("HF conflict with temporal err = %v, want ErrTemporalIndexExists", err)
	}
}

func TestBadgerStore_CreateHighFrequencyIndexRejectsInvalidBucket(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.CreateHighFrequencyIndex(1, 0); !errors.Is(err, ErrInvalidTemporalIndexConfig) {
		t.Fatalf("CreateHighFrequencyIndex(0) err = %v, want ErrInvalidTemporalIndexConfig", err)
	}
	if err := bs.CreateHighFrequencyIndex(1, -time.Second); !errors.Is(err, ErrInvalidTemporalIndexConfig) {
		t.Fatalf("CreateHighFrequencyIndex(-1s) err = %v, want ErrInvalidTemporalIndexConfig", err)
	}
	if err := bs.CreateHighFrequencyIndex(1, time.Nanosecond); !errors.Is(err, ErrInvalidTemporalIndexConfig) {
		t.Fatalf("CreateHighFrequencyIndex(1ns) err = %v, want ErrInvalidTemporalIndexConfig", err)
	}
	if err := bs.CreateHighFrequencyIndex(1, 1500*time.Microsecond); !errors.Is(err, ErrInvalidTemporalIndexConfig) {
		t.Fatalf("CreateHighFrequencyIndex(1.5ms) err = %v, want ErrInvalidTemporalIndexConfig", err)
	}
	if err := bs.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex valid bucket after rejects: %v", err)
	}
}

func TestBadgerStoreIndexAPIsRejectZeroLabelToken(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	cases := []struct {
		name string
		run  func() error
	}{
		{name: "create property", run: func() error { return bs.CreatePropertyIndex(0, "name") }},
		{name: "drop property", run: func() error { return bs.DropPropertyIndex(0, "name") }},
		{name: "create temporal", run: func() error { return bs.CreateTemporalIndex(0) }},
		{name: "drop temporal", run: func() error { return bs.DropTemporalIndex(0) }},
		{name: "create high frequency", run: func() error { return bs.CreateHighFrequencyIndex(0, time.Hour) }},
		{name: "drop high frequency", run: func() error { return bs.DropHighFrequencyIndex(0) }},
		{name: "create vector", run: func() error { return bs.CreateVectorIndex(0, "vec", 2, DistanceCosine) }},
		{name: "drop vector", run: func() error { return bs.DropVectorIndex(0, "vec") }},
		{name: "search vector", run: func() error {
			_, err := bs.SearchNearestNodes(0, "vec", []float32{1, 0}, 1, QueryOpts{})
			return err
		}},
		{name: "search filtered vector", run: func() error {
			_, err := bs.SearchNearestFiltered(0, "vec", []float32{1, 0}, 1, nil)
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("%s err = %v, want ErrInvalidStoreMutation", tc.name, err)
			}
		})
	}
}

func TestBadgerStoreIndexAPIsRejectReservedPropertyKey(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	cases := []struct {
		name string
		run  func() error
	}{
		{name: "create property", run: func() error { return bs.CreatePropertyIndex(1, "tkg_hash") }},
		{name: "drop property", run: func() error { return bs.DropPropertyIndex(1, "tkg_hash") }},
		{name: "query property", run: func() error {
			_, err := bs.NodesByLabelAndProperty(1, "tkg_hash", "x", QueryOpts{})
			return err
		}},
		{name: "create vector", run: func() error { return bs.CreateVectorIndex(1, "tkg_hash", 2, DistanceCosine) }},
		{name: "drop vector", run: func() error { return bs.DropVectorIndex(1, "tkg_hash") }},
		{name: "search vector", run: func() error {
			_, err := bs.SearchNearestNodes(1, "tkg_hash", []float32{1, 0}, 1, QueryOpts{})
			return err
		}},
		{name: "search filtered vector", run: func() error {
			_, err := bs.SearchNearestFiltered(1, "tkg_hash", []float32{1, 0}, 1, nil)
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, types.ErrReservedPrefix) {
				t.Fatalf("%s err = %v, want ErrReservedPrefix", tc.name, err)
			}
		})
	}
}

// ─── Issue 1: Store — DropHighFrequencyIndex ────────────────────────────

func TestBadgerStore_DropHighFrequencyIndex(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	// Drop.
	if err := bs.DropHighFrequencyIndex(1); err != nil {
		t.Fatalf("DropHighFrequencyIndex: %v", err)
	}

	// Double-drop.
	err := bs.DropHighFrequencyIndex(1)
	if !errors.Is(err, ErrTemporalIndexNotFound) {
		t.Errorf("double DropHighFrequencyIndex err = %v, want ErrTemporalIndexNotFound", err)
	}

	// Re-create after drop.
	if err := bs.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("re-create after drop: %v", err)
	}
}

// ─── Issue 1 + 6: Store — RemoveNodeLabelTokenWithHistory ───────────────

func TestBadgerStore_RemoveNodeLabelTokenWithHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	primary := uint16(1)
	extra := uint16(2)
	n := putTestNode(t, bs, 100, primary, []uint16{extra})

	// Build updated node with label removed.
	prevVersion := n.Version()
	prevState := n.DeepCopy()
	updated := n.DeepCopy()
	updated.RemoveLabelTokenRaw(extra)
	updated.SetVersion(prevVersion + 1)

	if err := bs.RemoveNodeLabelTokenWithHistory(
		types.NodeID(100), extra, updated, prevVersion, prevState,
	); err != nil {
		t.Fatalf("RemoveNodeLabelTokenWithHistory: %v", err)
	}

	// Verify history entry exists.
	hist, err := bs.GetNodeVersion(types.NodeID(100), prevVersion)
	if err != nil {
		t.Fatalf("GetNodeVersion(%d): %v", prevVersion, err)
	}
	if hist.Version() != prevVersion {
		t.Errorf("history version = %d, want %d", hist.Version(), prevVersion)
	}

	// Verify current node has updated version.
	got, err := bs.GetNode(types.NodeID(100))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Version() != prevVersion+1 {
		t.Errorf("current version = %d, want %d", got.Version(), prevVersion+1)
	}

	// Verify label index updated — node should not appear for the removed label.
	byLabel, err := bs.NodesByLabel(extra, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel(%d): %v", extra, err)
	}
	for _, node := range byLabel {
		if node.ID() == types.NodeID(100) {
			t.Error("node still in label index after RemoveNodeLabelTokenWithHistory")
		}
	}
}

// ─── Issue 1: Store — CreateTemporalIndex (direct store test) ───────────

func TestBadgerStore_CreateTemporalIndex(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, nil)

	if err := bs.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	// Duplicate.
	err := bs.CreateTemporalIndex(1)
	if !errors.Is(err, ErrTemporalIndexExists) {
		t.Errorf("duplicate CreateTemporalIndex err = %v, want ErrTemporalIndexExists", err)
	}
}
