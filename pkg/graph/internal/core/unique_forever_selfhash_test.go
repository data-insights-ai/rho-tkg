package core

import (
	"context"
	"errors"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

// A tampered UniqueForever ownership blob must fail closed at open: the durable
// envelope carries a self-hash over its records, so flipping a byte of the
// stored self-hash (records intact, msgpack still well-formed) makes the
// recomputed hash disagree and loadUniqueForeverOwners returns ErrCorruptWire —
// never silently granting or revoking ownership.
func TestUniqueForever_SelfHashTamperFailsClosed(t *testing.T) {
	ms := memory.New()
	g, err := New(Config{Store: ms})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if err := g.Constraints.CreateUniqueForever(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUniqueForever: %v", err)
	}
	// Persist a real ownership claim.
	if _, err := g.Nodes.Add(ctx, []string{"User"}, map[string]any{"email": "own@x.com"}); err != nil {
		t.Fatalf("add owner: %v", err)
	}

	raw, err := ms.MetaGet(uniqueForeverOwnersMeta)
	if err != nil {
		t.Fatalf("MetaGet: %v", err)
	}
	if len(raw) == 0 {
		t.Fatalf("ownership blob empty after claim — nothing persisted to tamper")
	}
	var blob foreverOwnersBlob
	if err := msgpack.Unmarshal(raw, &blob); err != nil {
		t.Fatalf("decode persisted blob: %v", err)
	}
	if blob.SelfHash == "" || len(blob.Owners) == 0 {
		t.Fatalf("unexpected blob shape: %+v", blob)
	}
	// Bit-flip one byte of the stored self-hash; records stay intact so the
	// recomputed hash mismatches (the self-hash branch, not the decode branch).
	hb := []byte(blob.SelfHash)
	hb[0] ^= 0xff
	blob.SelfHash = string(hb)
	tampered, err := msgpack.Marshal(blob)
	if err != nil {
		t.Fatalf("re-marshal tampered blob: %v", err)
	}
	if err := ms.MetaSet(uniqueForeverOwnersMeta, tampered); err != nil {
		t.Fatalf("MetaSet tampered: %v", err)
	}

	// Reopen over the same store: load must fail closed with ErrCorruptWire.
	if _, err := New(Config{Store: ms}); !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("reopen over tampered self-hash = %v, want ErrCorruptWire", err)
	}

	// The blob is genuinely decodable msgpack (so the failure is the self-hash
	// branch, not a malformed-bytes decode failure).
	var check foreverOwnersBlob
	if err := storeutil.SafeUnmarshal(tampered, &check); err != nil {
		t.Fatalf("tampered blob is not decodable (%v) — test would exercise the decode branch, not the self-hash branch", err)
	}
}

// foreverRecords decodes the persisted UniqueForever ownership records.
func foreverRecords(t *testing.T, ms *memory.Store) map[string]int64 {
	t.Helper()
	raw, err := ms.MetaGet(uniqueForeverOwnersMeta)
	if err != nil {
		t.Fatalf("MetaGet: %v", err)
	}
	var blob foreverOwnersBlob
	if err := storeutil.SafeUnmarshal(raw, &blob); err != nil {
		t.Fatalf("decode: %v", err)
	}
	out := make(map[string]int64, len(blob.Owners))
	for _, r := range blob.Owners {
		out[r.Label+"/"+r.PropKey+"/"+r.ValueKey] = r.Owner
	}
	return out
}

// The ownership blob keeps the stored value's own width key, byte-identical to
// what v4.57.0 writes (an older binary reads it): int(3) persists "i:3",
// uint8(4) "u8:4", never the canonical "i64:" key.
func TestUniqueForever_PersistsStoredWidthKey(t *testing.T) {
	ms := memory.New()
	g, err := New(Config{Store: ms})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := g.Constraints.CreateUniqueForever(ctx, "User", "k"); err != nil {
		t.Fatal(err)
	}
	a, err := g.Nodes.Add(ctx, []string{"User"}, map[string]any{"k": int(3)})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(ctx, []string{"User"}, map[string]any{"k": uint8(4)})
	if err != nil {
		t.Fatal(err)
	}
	got := foreverRecords(t, ms)
	want := map[string]int64{"User/k/i:3": int64(a.ID()), "User/k/u8:4": int64(b.ID())}
	if len(got) != len(want) || got["User/k/i:3"] != want["User/k/i:3"] || got["User/k/u8:4"] != want["User/k/u8:4"] {
		t.Fatalf("persisted records = %v, want %v", got, want)
	}
}

// A store written before v4.57.1 may hold one value's ownership under two
// widths (i:3 owned by A, i64:3 owned by B). It loads (lenient), the smaller
// owner ID owns the value: A keeps writing it, B's claim of it is refused, and
// a later persist keeps both raw records (B's is not silently dropped).
func TestUniqueForever_LegacyWidthTwinsLoad(t *testing.T) {
	ms := memory.New()
	g, err := New(Config{Store: ms})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := g.Constraints.CreateUniqueForever(ctx, "User", "k"); err != nil {
		t.Fatal(err)
	}
	a, err := g.Nodes.Add(ctx, []string{"User"}, map[string]any{"k": int(3)})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(ctx, []string{"User"}, map[string]any{"name": "b"})
	if err != nil {
		t.Fatal(err)
	}
	// Forge the legacy twin record i64:3 -> B (what an older binary could write).
	raw, err := ms.MetaGet(uniqueForeverOwnersMeta)
	if err != nil {
		t.Fatal(err)
	}
	var blob foreverOwnersBlob
	if err := storeutil.SafeUnmarshal(raw, &blob); err != nil {
		t.Fatal(err)
	}
	blob.Owners = append(blob.Owners, foreverOwnerRecord{Label: "User", PropKey: "k", ValueKey: "i64:3", Owner: int64(b.ID())})
	blob.SelfHash = hashForeverOwners(blob.Owners)
	forged, err := msgpack.Marshal(blob)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.MetaSet(uniqueForeverOwnersMeta, forged); err != nil {
		t.Fatal(err)
	}

	// Reload the registry from the blob, as an open does (the memory store
	// keeps no label registry across a reopen).
	if err := g.loadUniqueForeverOwners(); err != nil {
		t.Fatalf("load legacy twins: %v", err)
	}
	g2 := g
	if _, err := g2.Nodes.Update(ctx, b.ID(), map[string]any{"k": int64(3)}); !errors.Is(err, ErrUniqueViolation) {
		t.Errorf("loser B claims int64(3): %v, want ErrUniqueViolation", err)
	}
	if _, err := g2.Nodes.Update(ctx, a.ID(), map[string]any{"k": int64(3)}); err != nil {
		t.Errorf("winner A rewrites the value as int64(3): %v", err)
	}
	if _, err := g2.Nodes.Add(ctx, []string{"User"}, map[string]any{"k": int(9)}); err != nil {
		t.Fatal(err)
	}
	got := foreverRecords(t, ms)
	if got["User/k/i:3"] != int64(a.ID()) || got["User/k/i64:3"] != int64(b.ID()) || len(got) != 3 {
		t.Errorf("records after a persist = %v, want both raw twins kept and i:9", got)
	}
}
