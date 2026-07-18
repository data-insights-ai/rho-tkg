package badger

import (
	"bytes"
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// deltaTestStore builds an in-memory badger store with delta encoding on/off and
// no periodic flush (writes stay in the pending buffer, exercising the overlay
// read paths).
func deltaTestStore(t *testing.T, delta bool) *Store {
	t.Helper()
	bs, err := New(Config{
		InMemory:             true,
		FlushInterval:        1<<63 - 1,
		HistoryDeltaEncoding: delta,
	})
	if err != nil {
		t.Fatalf("New(delta=%v): %v", delta, err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	return bs
}

const deltaBlob = "a large, unchanging free-text field standing in for real entity data that " +
	"a full snapshot would otherwise re-serialize on every single version bump, " +
	"which is exactly the duplication anchor+delta history storage eliminates."

// deltaVersionNode builds version v of a node: a big unchanging blob, a changing
// scalar, several stable attributes, and increasing transaction time.
func deltaVersionNode(id snowflake.ID, v uint32) *types.Node {
	n := types.NewNode(types.NodeID(id), 10, []uint16{20, 21})
	_ = n.SetProperty("blob", deltaBlob)
	_ = n.SetProperty("status", []string{"active", "pending", "held", "closed"}[v%4])
	_ = n.SetProperty("counter", int64(v))
	_ = n.SetProperty("region", "eu-west")
	_ = n.SetProperty("tier", "gold")
	n.SetVersion(v)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(1000 + int64(v)*100)})
	return n
}

func nodeWireBytes(t *testing.T, n *types.Node) []byte {
	t.Helper()
	b, err := storepkg.MarshalNodeWire(n)
	if err != nil {
		t.Fatalf("MarshalNodeWire: %v", err)
	}
	return b
}

func relWireBytes(t *testing.T, r *types.Relationship) []byte {
	t.Helper()
	b, err := storepkg.MarshalRelWire(r)
	if err != nil {
		t.Fatalf("MarshalRelWire: %v", err)
	}
	return b
}

// TestBadgerHistoryDeltaDifferential drives an identical version chain into a
// delta-ON store and a delta-OFF (full-snapshot oracle) store and asserts every
// history read door returns byte-identical entities — reconstruction must be
// transparent. It also white-box-checks that deltas were actually written.
func TestBadgerHistoryDeltaDifferential(t *testing.T) {
	const versions = 21 // crosses the anchor boundary at 16
	id := snowflake.ID(1)

	on := deltaTestStore(t, true)
	off := deltaTestStore(t, false)

	inputs := make([]*types.Node, versions)
	for v := uint32(0); v < versions; v++ {
		inputs[v] = deltaVersionNode(id, v)
		for _, bs := range []*Store{on, off} {
			// Each store gets its OWN copy (PutNodeVersion may retain references).
			if err := bs.PutNodeVersion(types.NodeID(id), v, deltaVersionNode(id, v)); err != nil {
				t.Fatalf("PutNodeVersion v%d: %v", v, err)
			}
		}
	}

	// White-box: a non-anchor version on the delta store must be stored as a delta,
	// and an anchor version as a full snapshot.
	if raw, err := on.readHistoryNodeRaw(id, 5); err != nil {
		t.Fatalf("read raw v5: %v", err)
	} else if storepkg.HistoryValueKindOf(raw) != storepkg.HistoryDelta {
		t.Fatalf("v5 not stored as a delta (kind=%d) — delta encoding did not engage", storepkg.HistoryValueKindOf(raw))
	}
	if raw, err := on.readHistoryNodeRaw(id, 16); err != nil {
		t.Fatalf("read raw v16: %v", err)
	} else if storepkg.HistoryValueKindOf(raw) != storepkg.HistoryFull {
		t.Fatalf("anchor v16 not stored full")
	}

	// GetNodeVersion: every version reconstructs identically on both stores AND
	// equals the original input.
	for v := uint32(0); v < versions; v++ {
		gotOn, err := on.GetNodeVersion(types.NodeID(id), v)
		if err != nil {
			t.Fatalf("delta GetNodeVersion v%d: %v", v, err)
		}
		gotOff, err := off.GetNodeVersion(types.NodeID(id), v)
		if err != nil {
			t.Fatalf("full GetNodeVersion v%d: %v", v, err)
		}
		wantB := nodeWireBytes(t, inputs[v])
		if !bytes.Equal(nodeWireBytes(t, gotOn), wantB) {
			t.Fatalf("v%d: delta reconstruction != input", v)
		}
		if !bytes.Equal(nodeWireBytes(t, gotOff), wantB) {
			t.Fatalf("v%d: full != input", v)
		}
	}

	// GetNodeHistory: whole-chain reconstruction identical.
	assertNodeHistoriesEqual(t, on, off, id, versions)

	// NodeHistoryVersionsFrom: paged/offset scans identical (incl. a start that is
	// mid-interval so its anchor precedes the window).
	for _, tc := range []struct {
		start uint32
		limit int
	}{{0, 0}, {3, 5}, {17, 10}, {16, 3}} {
		gotOn, err := on.NodeHistoryVersionsFrom(types.NodeID(id), tc.start, tc.limit)
		if err != nil {
			t.Fatalf("delta NodeHistoryVersionsFrom(%d,%d): %v", tc.start, tc.limit, err)
		}
		gotOff, err := off.NodeHistoryVersionsFrom(types.NodeID(id), tc.start, tc.limit)
		if err != nil {
			t.Fatalf("full NodeHistoryVersionsFrom(%d,%d): %v", tc.start, tc.limit, err)
		}
		assertNodeSlicesEqual(t, gotOn, gotOff, "NodeHistoryVersionsFrom")
	}

	// NodeAsOf: as-of resolution identical across a range of transaction times.
	for _, txAt := range []int64{900, 1000, 1350, 1900, 3000, 5000} {
		gotOn, errOn := on.NodeAsOf(types.NodeID(id), types.Instant(txAt))
		gotOff, errOff := off.NodeAsOf(types.NodeID(id), types.Instant(txAt))
		if (errOn == nil) != (errOff == nil) {
			t.Fatalf("NodeAsOf(%d) error mismatch: delta=%v full=%v", txAt, errOn, errOff)
		}
		if errOn == nil && !bytes.Equal(nodeWireBytes(t, gotOn), nodeWireBytes(t, gotOff)) {
			t.Fatalf("NodeAsOf(%d): delta != full", txAt)
		}
	}
}

func assertNodeHistoriesEqual(t *testing.T, on, off *Store, id snowflake.ID, wantLen int) {
	t.Helper()
	gotOn, err := on.GetNodeHistory(types.NodeID(id))
	if err != nil {
		t.Fatalf("delta GetNodeHistory: %v", err)
	}
	gotOff, err := off.GetNodeHistory(types.NodeID(id))
	if err != nil {
		t.Fatalf("full GetNodeHistory: %v", err)
	}
	if wantLen >= 0 && len(gotOn) != wantLen {
		t.Fatalf("delta history len = %d, want %d", len(gotOn), wantLen)
	}
	assertNodeSlicesEqual(t, gotOn, gotOff, "GetNodeHistory")
}

func assertNodeSlicesEqual(t *testing.T, a, b []*types.Node, ctx string) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("%s: len %d != %d", ctx, len(a), len(b))
	}
	for i := range a {
		if !bytes.Equal(nodeWireBytes(t, a[i]), nodeWireBytes(t, b[i])) {
			t.Fatalf("%s: element %d (version %d) differs", ctx, i, a[i].Version())
		}
	}
}

// TestBadgerHistoryDeltaTruncateAnchorSafety proves a keep-newest-N truncation
// does not orphan a kept delta from a removed anchor: the reads after truncation
// still match the full-snapshot oracle truncated the same way.
func TestBadgerHistoryDeltaTruncateAnchorSafety(t *testing.T) {
	const versions = 20
	id := snowflake.ID(7)
	on := deltaTestStore(t, true)
	off := deltaTestStore(t, false)
	for v := uint32(0); v < versions; v++ {
		for _, bs := range []*Store{on, off} {
			if err := bs.PutNodeVersion(types.NodeID(id), v, deltaVersionNode(id, v)); err != nil {
				t.Fatalf("PutNodeVersion v%d: %v", v, err)
			}
		}
	}
	// Keep the newest 5 (versions 15..19): on the delta store, deltas 17,18,19
	// reference anchor 16 (kept) but 15 references anchor 0 (removed) — 15 must be
	// re-anchored to full.
	for _, bs := range []*Store{on, off} {
		if err := bs.TruncateNodeHistory(types.NodeID(id), 5); err != nil {
			t.Fatalf("TruncateNodeHistory: %v", err)
		}
	}
	assertNodeHistoriesEqual(t, on, off, id, 5)
	// The re-anchored lowest kept version must now be reconstructable in isolation.
	got, err := on.GetNodeVersion(types.NodeID(id), 15)
	if err != nil {
		t.Fatalf("GetNodeVersion v15 after truncate: %v", err)
	}
	if !bytes.Equal(nodeWireBytes(t, got), nodeWireBytes(t, deltaVersionNode(id, 15))) {
		t.Fatalf("re-anchored v15 != input")
	}
}

// TestBadgerHistoryDeltaCompactAnchorSafety proves history compaction (trim
// oldest, keep newest N) does not orphan a kept delta from a trimmed anchor: the
// reads after compaction still match the full-snapshot oracle compacted the same
// way. Same hazard as truncate, via the compactHistoryByPrefix path.
func TestBadgerHistoryDeltaCompactAnchorSafety(t *testing.T) {
	const versions = 20
	id := snowflake.ID(9)
	on := deltaTestStore(t, true)
	off := deltaTestStore(t, false)
	for v := uint32(0); v < versions; v++ {
		for _, bs := range []*Store{on, off} {
			if err := bs.PutNodeVersion(types.NodeID(id), v, deltaVersionNode(id, v)); err != nil {
				t.Fatalf("PutNodeVersion v%d: %v", v, err)
			}
		}
	}
	for _, bs := range []*Store{on, off} {
		if err := bs.CompactNodeHistory(types.NodeID(id), 5, nil); err != nil {
			t.Fatalf("CompactNodeHistory: %v", err)
		}
	}
	assertNodeHistoriesEqual(t, on, off, id, 5)
	// The lowest kept version (15, referencing trimmed anchor 0) must reconstruct.
	got, err := on.GetNodeVersion(types.NodeID(id), 15)
	if err != nil {
		t.Fatalf("GetNodeVersion v15 after compact: %v", err)
	}
	if !bytes.Equal(nodeWireBytes(t, got), nodeWireBytes(t, deltaVersionNode(id, 15))) {
		t.Fatalf("re-anchored v15 != input after compact")
	}
}

// TestBadgerHistoryDeltaPersistence proves deltas + anchors survive a close/reopen
// on disk and still reconstruct.
func TestBadgerHistoryDeltaPersistence(t *testing.T) {
	dir := t.TempDir()
	id := snowflake.ID(3)
	const versions = 18
	func() {
		bs, err := New(Config{Dir: dir, HistoryDeltaEncoding: true})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer func() { _ = bs.Close() }()
		for v := uint32(0); v < versions; v++ {
			if err := bs.PutNodeVersion(types.NodeID(id), v, deltaVersionNode(id, v)); err != nil {
				t.Fatalf("PutNodeVersion v%d: %v", v, err)
			}
		}
	}()

	bs, err := New(Config{Dir: dir, HistoryDeltaEncoding: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	for v := uint32(0); v < versions; v++ {
		got, err := bs.GetNodeVersion(types.NodeID(id), v)
		if err != nil {
			t.Fatalf("post-reopen GetNodeVersion v%d: %v", v, err)
		}
		if !bytes.Equal(nodeWireBytes(t, got), nodeWireBytes(t, deltaVersionNode(id, v))) {
			t.Fatalf("post-reopen v%d != input", v)
		}
	}
}

// --- relationship mirror ---

func deltaVersionRel(id snowflake.ID, v uint32) *types.Relationship {
	r := types.NewRelationship(types.RelID(id), 5, types.NodeID(snowflake.ID(100)), types.NodeID(snowflake.ID(200)))
	_ = r.SetProperty("blob", deltaBlob)
	_ = r.SetProperty("status", []string{"open", "ack", "closed"}[v%3])
	_ = r.SetProperty("weight", int64(v))
	r.SetVersion(v)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(1000 + int64(v)*100)})
	return r
}

func TestBadgerHistoryDeltaRelDifferential(t *testing.T) {
	const versions = 20
	id := snowflake.ID(2)
	on := deltaTestStore(t, true)
	off := deltaTestStore(t, false)
	for v := uint32(0); v < versions; v++ {
		for _, bs := range []*Store{on, off} {
			if err := bs.PutRelVersion(types.RelID(id), v, deltaVersionRel(id, v)); err != nil {
				t.Fatalf("PutRelVersion v%d: %v", v, err)
			}
		}
	}
	if raw, err := on.readHistoryRelRaw(id, 7); err != nil {
		t.Fatalf("read raw rel v7: %v", err)
	} else if storepkg.HistoryValueKindOf(raw) != storepkg.HistoryDelta {
		t.Fatalf("rel v7 not stored as a delta")
	}

	for v := uint32(0); v < versions; v++ {
		gotOn, err := on.GetRelVersion(types.RelID(id), v)
		if err != nil {
			t.Fatalf("delta GetRelVersion v%d: %v", v, err)
		}
		gotOff, err := off.GetRelVersion(types.RelID(id), v)
		if err != nil {
			t.Fatalf("full GetRelVersion v%d: %v", v, err)
		}
		if !bytes.Equal(relWireBytes(t, gotOn), relWireBytes(t, gotOff)) {
			t.Fatalf("rel v%d: delta != full", v)
		}
		if !bytes.Equal(relWireBytes(t, gotOn), relWireBytes(t, deltaVersionRel(id, v))) {
			t.Fatalf("rel v%d: delta != input", v)
		}
	}

	histOn, err := on.GetRelHistory(types.RelID(id))
	if err != nil {
		t.Fatalf("delta GetRelHistory: %v", err)
	}
	histOff, err := off.GetRelHistory(types.RelID(id))
	if err != nil {
		t.Fatalf("full GetRelHistory: %v", err)
	}
	if len(histOn) != len(histOff) || len(histOn) != versions {
		t.Fatalf("rel history len delta=%d full=%d want=%d", len(histOn), len(histOff), versions)
	}
	for i := range histOn {
		if !bytes.Equal(relWireBytes(t, histOn[i]), relWireBytes(t, histOff[i])) {
			t.Fatalf("rel history element %d differs", i)
		}
	}

	for _, txAt := range []int64{900, 1000, 1550, 2900} {
		gotOn, errOn := on.RelAsOf(types.RelID(id), types.Instant(txAt))
		gotOff, errOff := off.RelAsOf(types.RelID(id), types.Instant(txAt))
		if (errOn == nil) != (errOff == nil) {
			t.Fatalf("RelAsOf(%d) error mismatch: delta=%v full=%v", txAt, errOn, errOff)
		}
		if errOn == nil && !bytes.Equal(relWireBytes(t, gotOn), relWireBytes(t, gotOff)) {
			t.Fatalf("RelAsOf(%d): delta != full", txAt)
		}
	}
}

// TestHistoryAnchorInterval_ConfiguredRoundTrips proves a non-default anchor interval
// round-trips: versions written at interval 4 (anchors at 0,4,8,12,16) reconstruct
// exactly after reopen at the same interval.
func TestHistoryAnchorInterval_ConfiguredRoundTrips(t *testing.T) {
	dir := t.TempDir()
	id := snowflake.ID(7)
	const versions = 18
	func() {
		bs, err := New(Config{Dir: dir, HistoryDeltaEncoding: true, HistoryAnchorInterval: 4})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer func() { _ = bs.Close() }()
		for v := uint32(0); v < versions; v++ {
			if err := bs.PutNodeVersion(types.NodeID(id), v, deltaVersionNode(id, v)); err != nil {
				t.Fatalf("PutNodeVersion v%d: %v", v, err)
			}
		}
	}()

	bs, err := New(Config{Dir: dir, HistoryDeltaEncoding: true, HistoryAnchorInterval: 4})
	if err != nil {
		t.Fatalf("reopen at same interval: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	for v := uint32(0); v < versions; v++ {
		got, err := bs.GetNodeVersion(types.NodeID(id), v)
		if err != nil {
			t.Fatalf("GetNodeVersion v%d: %v", v, err)
		}
		if !bytes.Equal(nodeWireBytes(t, got), nodeWireBytes(t, deltaVersionNode(id, v))) {
			t.Fatalf("v%d != input at interval 4", v)
		}
	}
}

// TestHistoryAnchorInterval_MismatchFailsClosed is the silent-misread guard: a delta
// store written at interval 4 must FAIL CLOSED when reopened at a different interval.
func TestHistoryAnchorInterval_MismatchFailsClosed(t *testing.T) {
	dir := t.TempDir()
	id := snowflake.ID(9)
	func() {
		bs, err := New(Config{Dir: dir, HistoryDeltaEncoding: true, HistoryAnchorInterval: 4})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer func() { _ = bs.Close() }()
		for v := uint32(0); v < 6; v++ {
			if err := bs.PutNodeVersion(types.NodeID(id), v, deltaVersionNode(id, v)); err != nil {
				t.Fatalf("PutNodeVersion v%d: %v", v, err)
			}
		}
	}()

	_, err := New(Config{Dir: dir, HistoryDeltaEncoding: true, HistoryAnchorInterval: 8})
	if !errors.Is(err, storecontract.ErrHistoryAnchorIntervalMismatch) {
		t.Fatalf("reopen at mismatched interval err = %v, want ErrHistoryAnchorIntervalMismatch", err)
	}
	// Reopening at the DEFAULT (16) after writing at 4 must also fail.
	if _, err := New(Config{Dir: dir, HistoryDeltaEncoding: true}); !errors.Is(err, storecontract.ErrHistoryAnchorIntervalMismatch) {
		t.Fatalf("reopen at default interval err = %v, want mismatch", err)
	}
}

// TestHistoryAnchorInterval_Validation rejects an out-of-range interval at New.
func TestHistoryAnchorInterval_Validation(t *testing.T) {
	for _, bad := range []int{1, -1, 4097} {
		if _, err := New(Config{Dir: t.TempDir(), HistoryAnchorInterval: bad}); err == nil {
			t.Fatalf("New with HistoryAnchorInterval=%d succeeded, want validation error", bad)
		}
	}
}

// TestHistoryAnchorInterval_NoDeltaNoMarker proves the interval is NOT pinned when
// delta encoding is off (no deltas written → no interval-dependent layout), so such a
// store reopens at any interval without failing.
func TestHistoryAnchorInterval_NoDeltaNoMarker(t *testing.T) {
	dir := t.TempDir()
	func() {
		bs, err := New(Config{Dir: dir, HistoryAnchorInterval: 4}) // delta OFF
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		_ = bs.Close()
	}()
	// A non-delta store never stamped a marker → reopen at a different interval is fine.
	bs, err := New(Config{Dir: dir, HistoryAnchorInterval: 8})
	if err != nil {
		t.Fatalf("reopen non-delta store at different interval failed: %v", err)
	}
	_ = bs.Close()
}
