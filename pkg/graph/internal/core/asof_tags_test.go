package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// §4.2 — Named knowledge-time (Erkenntniszeit) marks. Break-oriented: durability
// across restart (the whole point of "cleaner as graph metadata"), fail-closed
// on a corrupt blob, capacity bound, and end-to-end use as an AS OF SYSTEM TIME
// argument together with a §4.1 backfill.

func TestAsOfTags_RoundTripOverwriteRemove(t *testing.T) {
	g := newTestGraph(t)
	at := erkenntnisZeit()

	if err := g.Temporal.TagAsOf("oetk-statistik-2025", at); err != nil {
		t.Fatalf("TagAsOf: %v", err)
	}
	got, ok, err := g.Temporal.ResolveAsOf("oetk-statistik-2025")
	if err != nil || !ok || got != at {
		t.Fatalf("ResolveAsOf = (%d,%v,%v), want (%d,true,nil)", got, ok, err, at)
	}
	// Unknown tag: ok=false, nil error (not an error condition).
	if _, ok, err := g.Temporal.ResolveAsOf("nope"); ok || err != nil {
		t.Fatalf("ResolveAsOf(unknown) = (_,%v,%v), want (false,nil)", ok, err)
	}
	// Overwrite wins.
	if err := g.Temporal.TagAsOf("oetk-statistik-2025", at+1000); err != nil {
		t.Fatalf("TagAsOf overwrite: %v", err)
	}
	if got, _, _ := g.Temporal.ResolveAsOf("oetk-statistik-2025"); got != at+1000 {
		t.Fatalf("after overwrite ResolveAsOf = %d, want %d", got, at+1000)
	}
	// List returns a copy.
	all, err := g.Temporal.AsOfTags()
	if err != nil || len(all) != 1 || all["oetk-statistik-2025"] != at+1000 {
		t.Fatalf("AsOfTags = (%v,%v), want single tag", all, err)
	}
	all["mutate-me"] = 1 // must not affect the stored registry
	if all2, _ := g.Temporal.AsOfTags(); len(all2) != 1 {
		t.Fatalf("AsOfTags returned a live map — external mutation leaked: %v", all2)
	}
	// Idempotent remove.
	if err := g.Temporal.RemoveAsOfTag("oetk-statistik-2025"); err != nil {
		t.Fatalf("RemoveAsOfTag: %v", err)
	}
	if _, ok, _ := g.Temporal.ResolveAsOf("oetk-statistik-2025"); ok {
		t.Fatal("tag still resolvable after remove")
	}
	if err := g.Temporal.RemoveAsOfTag("oetk-statistik-2025"); err != nil {
		t.Fatalf("RemoveAsOfTag (absent) = %v, want nil (idempotent)", err)
	}
}

func TestAsOfTags_InvalidInput(t *testing.T) {
	g := newTestGraph(t)
	for _, name := range []string{"", "   ", "\t"} {
		if err := g.Temporal.TagAsOf(name, erkenntnisZeit()); !errors.Is(err, ErrInvalidAsOfTag) {
			t.Fatalf("TagAsOf(%q) = %v, want ErrInvalidAsOfTag", name, err)
		}
	}
	for _, at := range []types.Instant{0, -1} {
		if err := g.Temporal.TagAsOf("x", at); !errors.Is(err, ErrInvalidAsOfTag) {
			t.Fatalf("TagAsOf(x,%d) = %v, want ErrInvalidAsOfTag", at, err)
		}
	}
	if _, _, err := g.Temporal.ResolveAsOf(""); !errors.Is(err, ErrInvalidAsOfTag) {
		t.Fatalf("ResolveAsOf(blank) = %v, want ErrInvalidAsOfTag", err)
	}
	// Over-length name.
	long := make([]byte, maxAsOfTagNameLen+1)
	for i := range long {
		long[i] = 'a'
	}
	if err := g.Temporal.TagAsOf(string(long), erkenntnisZeit()); !errors.Is(err, ErrInvalidAsOfTag) {
		t.Fatalf("TagAsOf(overlong) = %v, want ErrInvalidAsOfTag", err)
	}
}

// Durability: tags survive a Close/reopen of the same badger data dir — the
// reason the plan prefers this over an app-side alias map.
func TestAsOfTags_PersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	at := erkenntnisZeit()

	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	if err := g1.Temporal.TagAsOf("release", at); err != nil {
		t.Fatalf("TagAsOf: %v", err)
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2 (reopen): %v", err)
	}
	defer g2.Close()
	got, ok, err := g2.Temporal.ResolveAsOf("release")
	if err != nil || !ok || got != at {
		t.Fatalf("after reopen ResolveAsOf = (%d,%v,%v), want (%d,true,nil)", got, ok, err, at)
	}
}

// A corrupt on-disk blob must fail closed (SafeUnmarshal discipline), never panic.
func TestAsOfTags_CorruptBlobFailsClosed(t *testing.T) {
	ms := memory.New()
	g, err := New(Config{Store: ms})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	garbage, _ := msgpack.Marshal([]int{1, 2, 3}) // valid msgpack, wrong shape
	if err := ms.MetaSet("asof_tags", garbage); err != nil {
		t.Fatalf("seed corrupt meta: %v", err)
	}
	if _, _, err := g.Temporal.ResolveAsOf("release"); err == nil {
		t.Fatal("ResolveAsOf over corrupt blob = nil error, want fail-closed")
	}
	if _, err := g.Temporal.AsOfTags(); err == nil {
		t.Fatal("AsOfTags over corrupt blob = nil error, want fail-closed")
	}
}

// A persisted non-positive instant (corrupt disk row or foreign writer) must
// fail closed on LOAD, mirroring the write door. Instant(0) is especially
// dangerous — it aliases the "no TX filter" sentinel, so a resolved tag would
// silently return CURRENT belief instead of the named knowledge time.
func TestAsOfTags_NonPositiveInstantFailsClosed(t *testing.T) {
	for _, bad := range []int64{0, -1, -1000} {
		ms := memory.New()
		g, err := New(Config{Store: ms})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		b, _ := msgpack.Marshal(map[string]int64{"oetk-statistik-2025": bad})
		if err := ms.MetaSet("asof_tags", b); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if _, _, err := g.Temporal.ResolveAsOf("oetk-statistik-2025"); !errors.Is(err, storepkg.ErrCorruptWire) {
			t.Fatalf("ResolveAsOf over instant=%d = %v, want ErrCorruptWire (fail closed)", bad, err)
		}
		if _, err := g.Temporal.AsOfTags(); !errors.Is(err, storepkg.ErrCorruptWire) {
			t.Fatalf("AsOfTags over instant=%d = %v, want ErrCorruptWire", bad, err)
		}
		_ = g.Close()
	}
}

// noMetaStore embeds only the MandatoryStore interface, so MetaGet/MetaSet are
// NOT part of its method set — every as-of-tag door must decline.
type noMetaStore struct{ storepkg.MandatoryStore }

func TestAsOfTags_DeclinesWithoutMetaKV(t *testing.T) {
	g, err := New(Config{Store: &noMetaStore{MandatoryStore: memory.New()}, AllowReset: true})
	if err != nil {
		t.Fatalf("New(no-MetaKV store): %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	if err := g.Temporal.TagAsOf("x", erkenntnisZeit()); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("TagAsOf without MetaKV = %v, want ErrCapabilityNotSupported", err)
	}
	if _, _, err := g.Temporal.ResolveAsOf("x"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("ResolveAsOf without MetaKV = %v, want ErrCapabilityNotSupported", err)
	}
	if _, err := g.Temporal.AsOfTags(); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("AsOfTags without MetaKV = %v, want ErrCapabilityNotSupported", err)
	}
	if err := g.Temporal.RemoveAsOfTag("x"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("RemoveAsOfTag without MetaKV = %v, want ErrCapabilityNotSupported", err)
	}
	// Reset's tag reap is a no-op when the backend has no MetaKV.
	if err := g.Admin.Reset(); err != nil {
		t.Fatalf("Reset without MetaKV = %v, want nil (reap no-op)", err)
	}
}

// failMetaStore delegates to an inner MetaKV store but can inject a failure on
// the asof_tags key, exercising the persist/read error branches. Scoped to the
// asof_tags key so New()/epoch/other meta paths still work.
type failMetaStore struct {
	storepkg.MandatoryStore
	inner   storepkg.MetaKVCapability
	failGet error
	failSet error
}

func (s *failMetaStore) MetaGet(key string) ([]byte, error) {
	if s.failGet != nil && key == "asof_tags" {
		return nil, s.failGet
	}
	return s.inner.MetaGet(key)
}

func (s *failMetaStore) MetaSet(key string, v []byte) error {
	if s.failSet != nil && key == "asof_tags" {
		return s.failSet
	}
	return s.inner.MetaSet(key, v)
}

func TestAsOfTags_MetaStoreErrorsSurface(t *testing.T) {
	t.Run("MetaSet failure on write", func(t *testing.T) {
		ms := memory.New()
		store := &failMetaStore{MandatoryStore: ms, inner: ms, failSet: errors.New("meta write boom")}
		g, err := New(Config{Store: store, AllowReset: true})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		if err := g.Temporal.TagAsOf("x", erkenntnisZeit()); err == nil || errors.Is(err, ErrInvalidAsOfTag) {
			t.Fatalf("TagAsOf with failing MetaSet = %v, want the store write error", err)
		}
		// Reset's reap also surfaces the persist error.
		if err := g.Admin.Reset(); err == nil {
			t.Fatal("Reset with failing MetaSet = nil, want the reap persist error")
		}
	})
	t.Run("MetaGet failure on read", func(t *testing.T) {
		ms := memory.New()
		store := &failMetaStore{MandatoryStore: ms, inner: ms, failGet: errors.New("meta read boom")}
		g, err := New(Config{Store: store})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		if _, _, err := g.Temporal.ResolveAsOf("x"); err == nil {
			t.Fatal("ResolveAsOf with failing MetaGet = nil, want the store read error")
		}
		if _, err := g.Temporal.AsOfTags(); err == nil {
			t.Fatal("AsOfTags with failing MetaGet = nil, want the store read error")
		}
	})
}

// Capacity is bounded; overwriting an existing tag at capacity is still allowed.
func TestAsOfTags_CapacityBound(t *testing.T) {
	ms := memory.New()
	g, err := New(Config{Store: ms})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	// Seed a full registry directly (cheap — no 4096 RMW cycles).
	full := make(map[string]int64, maxAsOfTags)
	for i := 0; i < maxAsOfTags; i++ {
		full[fmt.Sprintf("t%d", i)] = int64(i + 1)
	}
	b, _ := msgpack.Marshal(full)
	if err := ms.MetaSet("asof_tags", b); err != nil {
		t.Fatalf("seed full registry: %v", err)
	}

	if err := g.Temporal.TagAsOf("one-too-many", erkenntnisZeit()); !errors.Is(err, ErrTooManyAsOfTags) {
		t.Fatalf("TagAsOf at cap = %v, want ErrTooManyAsOfTags", err)
	}
	// Overwriting an existing name at cap must succeed (does not grow the set).
	if err := g.Temporal.TagAsOf("t0", erkenntnisZeit()); err != nil {
		t.Fatalf("TagAsOf overwrite existing at cap = %v, want nil", err)
	}
}

// A read-only replica tags on its primary: writes reject, reads pass.
func TestAsOfTags_ReadOnlyReplicaRejectsWrites(t *testing.T) {
	g, err := New(Config{Store: memory.New(), ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("New(ReadOnlyReplica): %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if err := g.Temporal.TagAsOf("x", erkenntnisZeit()); !errors.Is(err, ErrReadOnlyReplica) {
		t.Fatalf("TagAsOf on replica = %v, want ErrReadOnlyReplica", err)
	}
	if err := g.Temporal.RemoveAsOfTag("x"); !errors.Is(err, ErrReadOnlyReplica) {
		t.Fatalf("RemoveAsOfTag on replica = %v, want ErrReadOnlyReplica", err)
	}
	// Reads stay open.
	if _, ok, err := g.Temporal.ResolveAsOf("x"); ok || err != nil {
		t.Fatalf("ResolveAsOf on replica = (_,%v,%v), want (false,nil)", ok, err)
	}
}

// Clear()/Reset reaps the durable tag registry (lesson 53 scan-based meta reap
// covers this new key).
func TestAsOfTags_ResetReapsTags(t *testing.T) {
	g := newTestGraph(t)
	if err := g.Temporal.TagAsOf("x", erkenntnisZeit()); err != nil {
		t.Fatalf("TagAsOf: %v", err)
	}
	if err := g.Admin.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if all, err := g.Temporal.AsOfTags(); err != nil || len(all) != 0 {
		t.Fatalf("after Reset AsOfTags = (%v,%v), want empty", all, err)
	}
}

// Concurrent distinct tags must not lose updates (asofMu serializes the RMW).
// Run under -race to catch a missing lock.
func TestAsOfTags_ConcurrentWritesNoLostUpdate(t *testing.T) {
	g := newTestGraph(t)
	const n = 24
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			if err := g.Temporal.TagAsOf(fmt.Sprintf("tag-%d", i), types.Instant(i+1)); err != nil {
				t.Errorf("TagAsOf %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	all, err := g.Temporal.AsOfTags()
	if err != nil {
		t.Fatalf("AsOfTags: %v", err)
	}
	if len(all) != n {
		t.Fatalf("after %d concurrent tags, registry has %d (lost update)", n, len(all))
	}
}

// End-to-end: a §4.1 backfilled node addressed via a §4.2 named mark — the OETK
// "AS OF SYSTEM TIME $tag against a documented Erkenntniszeit" loop.
func TestAsOfTags_ResolvedTagDrivesBitemporalQuery(t *testing.T) {
	g, err := New(Config{Store: memory.New(), AllowTxBackfill: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()

	stichtag := types.Instant(time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC).UnixMilli())
	erkenntnis := erkenntnisZeit()

	n, err := g.Nodes.AddWithTx(ctx, []string{"Person"},
		map[string]any{"pid": int64(1), "tkg_valid_from": stichtag}, erkenntnis)
	if err != nil {
		t.Fatalf("AddWithTx: %v", err)
	}
	if err := g.Temporal.TagAsOf("oetk-statistik-2025", erkenntnis); err != nil {
		t.Fatalf("TagAsOf: %v", err)
	}

	txAt, ok, err := g.Temporal.ResolveAsOf("oetk-statistik-2025")
	if err != nil || !ok {
		t.Fatalf("ResolveAsOf = (_,%v,%v)", ok, err)
	}
	got, err := g.Temporal.NodeAtTx(n.ID(), stichtag, txAt)
	if err != nil || got == nil {
		t.Fatalf("NodeAtTx(stichtag, $tag) = (%v,%v), want the backfilled node", got, err)
	}
	if v, _ := got.GetProperty("pid"); v != int64(1) {
		t.Fatalf("resolved node pid = %v, want 1", v)
	}
}
