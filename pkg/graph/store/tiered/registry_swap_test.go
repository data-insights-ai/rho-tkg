package tiered

import (
	"context"
	"sync"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
)

// TestRegistrySetDuringHandleSwapReachesNewHandle pins R3 T-i: the registry
// setters read each event shard's store under its shardMu, so a set racing a
// warm shard's handle swap (MoveShard closes the handle and reopens it, as
// the sweep does) reaches the handle installed after the swap. A set held
// inside the move lands on the reopened handle; sets looping beside moves
// back and forth race no handle write. Catches the loops reading es.store
// under ts.mu alone (the race detector reports the read against the swap's
// write) and a set that leaves the new handle with the old registry.
func TestRegistrySetDuringHandleSwapReachesNewHandle(t *testing.T) {
	t.Parallel()
	ts, _, _, _, old, _ := warmMoveFixture(t)
	defer func() { _ = ts.Close() }()
	es := ts.EventShardsForTest()[old]

	// A set while the move holds the shard (gate taken, old handle still
	// installed) reaches the handle the move installs.
	held := registrypkg.NewPropertyKeyRegistry()
	release := make(chan struct{})
	unhold := sync.OnceFunc(func() { close(release) })
	defer unhold()
	reached := holdMoveAt(ts, "gated", release)
	moved := make(chan error, 1)
	go func() { moved <- ts.MoveShard(context.Background(), old, VolumeWarm) }()
	awaitHold(t, reached, moved)
	ts.SetNameRegistries(registrypkg.NewLabelRegistry(), registrypkg.NewRelTypeRegistry())
	ts.SetPropertyKeyRegistry(held)
	unhold()
	if err := <-moved; err != nil {
		t.Fatalf("MoveShard: %v", err)
	}
	ts.SetMoveFaultForTest(nil)
	if es.Store() == nil || es.Tier() != TierWarm {
		t.Fatalf("after the move the shard is %s, open %v; want warm and open", es.Tier(), es.Store() != nil)
	}
	if got := es.Store().PropertyKeyRegistry(); got != held {
		t.Fatalf("the reopened handle holds registry %p, want the one set during the move %p", got, held)
	}

	// Sets beside handle swaps: each move closes and reopens the warm
	// handle (es.store written under shardMu) while the setters read it.
	stop := make(chan struct{})
	var last *registrypkg.PropertyKeyRegistry
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		set := func() {
			last = registrypkg.NewPropertyKeyRegistry()
			ts.SetNameRegistries(registrypkg.NewLabelRegistry(), registrypkg.NewRelTypeRegistry())
			ts.SetPropertyKeyRegistry(last)
		}
		for {
			select {
			case <-stop:
				set() // one set after every move
				return
			default:
				set()
			}
		}
	}()
	vols := []string{VolumeHot, VolumeWarm}
	for i := range 6 {
		if err := ts.MoveShard(context.Background(), old, vols[i%2]); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("MoveShard %d: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()
	if got := es.Store().PropertyKeyRegistry(); got != last {
		t.Fatalf("after the swaps the handle holds registry %p, want the last one set %p", got, last)
	}
}
