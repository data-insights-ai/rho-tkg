package badger

import (
	"bytes"
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// preEncodeBattery returns a battery of node shapes that exercise every
// pre-encode-relevant NodeWire field: empty, single/multi-property, unicode
// keys/values, multi-label, and a fully-populated temporal + integrity node.
// Each node carries a non-zero finalized TxFrom so the patched-tail path is real.
func preEncodeBattery() []*types.Node {
	mk := func(id int64, primary uint16, extras []uint16, props map[string]any, tm *types.TemporalMetadata) *types.Node {
		n := types.NewNode(types.NodeID(snowflake.ID(id)), primary, extras)
		if len(props) > 0 {
			ps, err := types.NewPropertySlice(props)
			if err != nil {
				panic(err)
			}
			_ = n.SetProperties(ps)
		}
		if tm != nil {
			n.SetTemporal(tm)
		}
		n.SetIntegrity(&types.NodeIntegrity{Hash: "h", PrevHash: ""})
		return n
	}
	return []*types.Node{
		mk(1001, 1, nil, nil, &types.TemporalMetadata{TxFrom: 555}),
		mk(1002, 2, nil, map[string]any{"name": "Alice", "age": int64(30)}, &types.TemporalMetadata{TxFrom: 777, ValidFrom: 100, ValidTo: 200, CreatedAt: 50}),
		mk(1003, 3, []uint16{4, 5}, map[string]any{"名前": "太郎", "emoji": "🚀"}, &types.TemporalMetadata{TxFrom: 999}),
		mk(1004, 6, []uint16{7}, map[string]any{"k": int64(1), "z": "last", "a": true}, &types.TemporalMetadata{TxFrom: 1234, CreatedBy: "admin", UpdatedBy: "sys"}),
		mk(1005, 8, nil, nil, &types.TemporalMetadata{}), // zero-temporal (TxFrom 0)
	}
}

func rawNodeBytes(t *testing.T, bs *Store, id snowflake.ID) []byte {
	t.Helper()
	var out []byte
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storeutil.NodeKey(id))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			out = append([]byte(nil), v...)
			return nil
		})
	})
	if err != nil {
		t.Fatalf("raw node %d: %v", id, err)
	}
	return out
}

// TestPreEncodedPutByteIdentity is the store-level half of the ingest divergence
// battery (constraint B): PutNodesBatchPreEncoded with a prepare-side pre-encoded
// buffer (tail patched with the finalized TxFrom) persists the EXACT same entity
// bytes as the encode-at-flush PutNodesBatch — over a battery of node shapes, and
// with byte-equal change feeds. Two fresh stores share one property-key registry
// so tokenization is identical; the only difference between them is which write
// door each node took.
func TestPreEncodedPutByteIdentity(t *testing.T) {
	t.Parallel()

	reg := registrypkg.NewPropertyKeyRegistry()
	encodeStore, err := New(Config{InMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatalf("New encode store: %v", err)
	}
	t.Cleanup(func() { _ = encodeStore.Close() })
	encodeStore.SetPropertyKeyRegistry(reg)

	preStore, err := New(Config{InMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatalf("New pre-encode store: %v", err)
	}
	t.Cleanup(func() { _ = preStore.Close() })
	preStore.SetPropertyKeyRegistry(reg)

	battery := preEncodeBattery()

	// Encode-at-flush baseline.
	if err := encodeStore.PutNodesBatch(battery); err != nil {
		t.Fatalf("PutNodesBatch: %v", err)
	}
	// Pre-encoded path: build the zero-tail buffer, patch the finalized TxFrom.
	wireBodies := make([][]byte, len(battery))
	for i, n := range battery {
		buf, err := storeutil.PreEncodeNodeWireV2WithKeys(n, reg)
		if err != nil {
			t.Fatalf("PreEncodeNodeWireV2WithKeys[%d]: %v", i, err)
		}
		tm := n.Temporal()
		var tf, tt int64
		if tm != nil {
			tf, tt = int64(tm.TxFrom), int64(tm.TxTo)
		}
		if err := storeutil.PatchWireTemporalTail(buf, tf, tt); err != nil {
			t.Fatalf("PatchWireTemporalTail[%d]: %v", i, err)
		}
		wireBodies[i] = buf
	}
	if err := preStore.PutNodesBatchPreEncoded(battery, wireBodies); err != nil {
		t.Fatalf("PutNodesBatchPreEncoded: %v", err)
	}

	if err := encodeStore.Flush(); err != nil {
		t.Fatalf("Flush encode: %v", err)
	}
	if err := preStore.Flush(); err != nil {
		t.Fatalf("Flush pre: %v", err)
	}

	// Every persisted entity row must be byte-identical.
	for _, n := range battery {
		id := n.ID().SnowflakeID()
		a := rawNodeBytes(t, encodeStore, id)
		b := rawNodeBytes(t, preStore, id)
		if !bytes.Equal(a, b) {
			t.Fatalf("node %d bytes diverge:\n encode=%x\n pre   =%x", id, a, b)
		}
		// And the persisted row must decode to the finalized TxFrom (proving the
		// patch landed, not merely that the two stores agree on a zero tail).
		w, err := storeutil.UnmarshalNodeWireWithKeys(b, reg)
		if err != nil {
			t.Fatalf("decode pre node %d: %v", id, err)
		}
		want := int64(0)
		if tm := n.Temporal(); tm != nil {
			want = int64(tm.TxFrom)
		}
		if w.TxFrom != want {
			t.Fatalf("node %d persisted TxFrom = %d, want %d", id, w.TxFrom, want)
		}
	}

	// Change feeds must be byte-identical (the put body is the UNTOKENIZED
	// encode-at-flush form on both paths — unaffected by this change).
	feed := func(bs *Store) [][]byte {
		var out [][]byte
		if err := bs.ForEachChange(0, func(r storepkg.ChangeRecord) bool {
			rec := append([]byte{byte(r.Tag)}, r.Payload...)
			out = append(out, rec)
			return true
		}); err != nil {
			t.Fatalf("ForEachChange: %v", err)
		}
		return out
	}
	ef, pf := feed(encodeStore), feed(preStore)
	if len(ef) != len(pf) {
		t.Fatalf("change feed length diverge: encode=%d pre=%d", len(ef), len(pf))
	}
	for i := range ef {
		if !bytes.Equal(ef[i], pf[i]) {
			t.Fatalf("change feed record %d diverge:\n encode=%x\n pre   =%x", i, ef[i], pf[i])
		}
	}
}

// TestPreEncodedPutNilFallsBackToEncode proves a nil wireBodies element makes
// PutNodesBatchPreEncoded encode that row exactly as PutNodesBatch would — the
// applier's conservative fallback (probe-restamp / malformed buffer) is
// byte-identical to encode-at-flush.
func TestPreEncodedPutNilFallsBackToEncode(t *testing.T) {
	t.Parallel()

	reg := registrypkg.NewPropertyKeyRegistry()
	a, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New a: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	a.SetPropertyKeyRegistry(reg)
	b, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New b: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	b.SetPropertyKeyRegistry(reg)

	battery := preEncodeBattery()
	if err := a.PutNodesBatch(battery); err != nil {
		t.Fatalf("PutNodesBatch: %v", err)
	}
	// All-nil bodies → every row re-encoded internally.
	if err := b.PutNodesBatchPreEncoded(battery, make([][]byte, len(battery))); err != nil {
		t.Fatalf("PutNodesBatchPreEncoded(nil bodies): %v", err)
	}
	if err := a.Flush(); err != nil {
		t.Fatalf("Flush a: %v", err)
	}
	if err := b.Flush(); err != nil {
		t.Fatalf("Flush b: %v", err)
	}
	for _, n := range battery {
		id := n.ID().SnowflakeID()
		if !bytes.Equal(rawNodeBytes(t, a, id), rawNodeBytes(t, b, id)) {
			t.Fatalf("node %d fallback bytes diverge", id)
		}
	}
}

// TestPutNodesBatchOwnedPreEncoded_FreezesInPlaceAndRejectsMutation (BACKLOG
// 18t) is the adversarial proof the frozen-row guard actually fires on an
// OWNERSHIP-TRANSFERRED (owned=true) cache entry — the ingest bulk-apply
// path (PutNodesBatchOwnedPreEncoded had zero direct badger-package tests
// before this one; only indirect coverage via the ingest package, which
// violates Rule 1). Unlike the default owned=false doors (which deep-copy,
// leaving the caller's own object mutable — see
// TestPreEncodedPutNilFallsBackToEncode's dual-store reuse of one battery),
// owned=true freezes the caller's node IN PLACE: this test proves that after
// the call, the CALLER'S OWN object — not just some separate cached copy —
// rejects further mutation, matching the documented "UNDEFINED BEHAVIOR if
// the caller touches a node afterward" contract by making touching it fail
// fast instead of silently corrupting the cache.
func TestPutNodesBatchOwnedPreEncoded_FreezesInPlaceAndRejectsMutation(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(2001)), 1, nil)
	if err := n.SetProperty("k", "v"); err != nil {
		t.Fatalf("SetProperty before transfer: %v", err)
	}

	if err := bs.PutNodesBatchOwnedPreEncoded([]*types.Node{n}, nil, nil); err != nil {
		t.Fatalf("PutNodesBatchOwnedPreEncoded: %v", err)
	}

	// The caller's OWN object is now frozen in place — not a separate copy.
	if err := n.SetProperty("k2", "v2"); !errors.Is(err, types.ErrFrozenNode) {
		t.Fatalf("SetProperty on the ownership-transferred node = %v, want ErrFrozenNode", err)
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("AddLabelTokenRaw on the ownership-transferred node did not panic")
			}
		}()
		n.AddLabelTokenRaw(99)
	}()

	// The cached entry the store now serves is the SAME frozen object and
	// also rejects mutation — not just the caller's dangling reference.
	bs.idxMu.RLock()
	cached, status := bs.nodeCache.Get(2001)
	bs.idxMu.RUnlock()
	if status != indexpkg.CacheHit {
		t.Fatalf("cache status = %v, want CacheHit", status)
	}
	if err := cached.SetProperty("k3", "v3"); !errors.Is(err, types.ErrFrozenNode) {
		t.Fatalf("SetProperty on the cached entry = %v, want ErrFrozenNode", err)
	}

	// GetNode still serves a correct, independently-mutable deep copy — the
	// frozen guard protects the SHARED cache entry, not every reader.
	got, err := bs.GetNode(types.NodeID(snowflake.ID(2001)))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	v, ok := got.GetProperty("k")
	if !ok || v != "v" {
		t.Fatalf("GetNode property = %v, %v, want \"v\", true", v, ok)
	}
	if err := got.SetProperty("k4", "v4"); err != nil {
		t.Fatalf("SetProperty on GetNode's own deep copy: %v", err)
	}
}
