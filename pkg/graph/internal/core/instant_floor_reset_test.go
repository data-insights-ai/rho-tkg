package core

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

// reapFailStore fails MetaSet for ONE chosen key, so a single Reap step inside
// reapCoreStateForClear can be made to fail while everything else works.
// Close is a no-op so the shared memory store survives Core.Close().
type reapFailStore struct {
	*memory.Store
	failSetKey string
	err        error
}

func (s *reapFailStore) MetaSet(key string, v []byte) error {
	if s.err != nil && key == s.failSetKey {
		return s.err
	}
	return s.Store.MetaSet(key, v)
}
func (s *reapFailStore) Close() error { return nil }

// Admin.Reset() wipes the ENTIRE MetaKV keyspace via store.Clear() and then
// re-persists the Preserve-classified keys (instantFloorMeta, idSlotLeaseMeta,
// and the integrity-mode, untimed-mode and schema-version keys). Those restores
// run FIRST in reapCoreStateForClear, before the Reap steps, and no step
// short-circuits the others.
//
// Were they to run last, any one Reap step failing would mean the Preserve keys
// are never restored — and Clear has ALREADY destroyed them. A transient MetaKV
// write fault during a Reset would then permanently destroy durable state that
// was explicitly classified as must-survive: the commit-clock floor (transaction
// time can run BACKWARDS across the Reset) and the id-slot
// lease (two nodes can collide on one snowflake slot after a failover).
//
// The ordering is the contract. Restoring what Clear just destroyed is the FIRST
// obligation after Clear, not the last; the Reap steps are only clearing more
// state and cannot be harmed by running after.
func TestReset_PreserveKeysSurviveAFailingReapStep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	base := memory.New()
	st := &reapFailStore{Store: base}

	g, err := New(Config{Store: st, AllowReset: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if _, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"i": 1}); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	floorBefore := g.lastInstant.Load()
	if floorBefore <= 0 {
		t.Fatalf("precondition: no floor minted (%d)", floorBefore)
	}

	// One Reap step now fails; the two Preserve restores sit AFTER it.
	st.failSetKey = asofTagsMeta
	st.err = errors.New("transient meta write fault")

	resetErr := g.Admin.Reset()
	if resetErr == nil {
		t.Fatal("precondition: Reset did not surface the injected fault")
	}
	st.err = nil // let the assertion read/write freely

	v, err := base.MetaGet(instantFloorMeta)
	if err != nil || len(v) != 8 {
		t.Fatalf("PRESERVE KEY NOT RESTORED: instantFloorMeta is %d bytes (err %v) after a Reset whose "+
			"Reap step failed — reapCoreStateForClear returned early and never reached "+
			"restoreInstantFloorAfterReset (nor restoreIDSlotLeaseAfterReset). The live floor was %d. "+
			"On a backend whose Clear() genuinely wipes MetaKV (badger), the pre-Reset watermark is "+
			"gone AND the restore is skipped, so transaction time can run backwards across the Reset. "+
			"Restoring a Preserve key must not be gated on unrelated Reap steps succeeding.",
			len(v), err, floorBefore)
	}
}

// A session that could not READ the watermark must still RESTORE it after a
// Reset that destroyed it, and must persist its later floor at Close.
//
// persistInstantFloor deliberately no-ops when floorSeedUnreadable is set: at
// Close, the durable value may be intact-but-unreadable, and overwriting it
// with this session's (possibly lower) floor would silently downgrade a
// monotone high-water mark. That reasoning is correct at Close.
//
// It is exactly WRONG after Reset, so restoreInstantFloorAfterReset writes with
// writeInstantFloor, not persistInstantFloor: by then store.Clear() has already
// destroyed the key — there is no durable value left to protect. Declining to
// write would leave the watermark simply GONE, so the next open falls back to the
// wall clock and, if this session's floor sat above the wall (a >1 write/ms
// burst), fresh writes are stamped BELOW records the change log already emitted.
//
// After Reset re-wrote the watermark the flag is stale too: this session now
// KNOWS the durable value because it wrote it. Leaving the flag set makes the
// following Close decline to persist a strictly higher floor, stranding every
// post-Reset write above the durable watermark.
func TestReset_RestoresTheFloorEvenWhenTheSeedWasUnreadable(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()

	g, err := New(Config{BadgerDir: dir, AllowReset: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = g.Close()
		}
	}()

	mk := g.store.(storepkg.MetaKVCapability) // badger always offers MetaKV
	if _, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"i": 1}); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	// A durable watermark exists before the Reset, as it would after any clean
	// previous shutdown.
	if err := g.persistInstantFloor(); err != nil {
		t.Fatalf("persist: %v", err)
	}
	if v, err := mk.MetaGet(instantFloorMeta); err != nil || len(v) != 8 {
		t.Fatalf("precondition: watermark not persisted (%v, %d bytes)", err, len(v))
	}

	// This session could not READ the watermark at open. persistInstantFloor
	// honours that at Close (it must not overwrite an intact-but-unread value)
	// — the question is whether restoreInstantFloorAfterReset wrongly inherits it.
	g.floorSeedUnreadable = true
	floor := g.lastInstant.Load()

	if err := g.Admin.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	v, err := mk.MetaGet(instantFloorMeta)
	if err != nil || len(v) != 8 {
		t.Fatalf("WATERMARK LOST ACROSS RESET: instantFloorMeta is %d bytes (err %v). Clear() "+
			"destroyed it and restoreInstantFloorAfterReset declined to rewrite it because the SEED "+
			"had faulted — but after Clear there is no durable value left to protect, so the "+
			"no-overwrite rule costs exactly the thing it exists to preserve. The live floor was %d; "+
			"the next open now falls back to the wall clock and can stamp below records the change "+
			"log already emitted.", len(v), err, floor)
	}
	if got := int64(binary.BigEndian.Uint64(v)); got < floor { // #nosec G115 -- test decode
		t.Fatalf("watermark restored after Reset = %d, want >= the live floor %d", got, floor)
	}
	if g.floorSeedUnreadable {
		t.Fatal("floorSeedUnreadable is still set after Reset re-wrote the watermark — the following " +
			"Close will decline to persist a floor this session already wrote, so every post-Reset " +
			"write is stranded above the durable watermark")
	}

	// Post-Reset writes push the floor higher; Close must persist that.
	for i := 0; i < 5; i++ {
		if _, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"post": i}); err != nil {
			t.Fatalf("post-reset add: %v", err)
		}
	}
	postFloor := g.lastInstant.Load()
	closed = true
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()
	v, err = g2.store.(storepkg.MetaKVCapability).MetaGet(instantFloorMeta)
	if err != nil || len(v) != 8 {
		t.Fatalf("watermark absent after Close: %v, %d bytes", err, len(v))
	}
	if got := int64(binary.BigEndian.Uint64(v)); got < postFloor { // #nosec G115 -- test decode
		t.Fatalf("persisted watermark %d < the live post-Reset floor %d", got, postFloor)
	}
}
