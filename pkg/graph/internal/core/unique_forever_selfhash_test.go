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
