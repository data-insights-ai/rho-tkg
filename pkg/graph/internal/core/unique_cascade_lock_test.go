package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Lock protocol of the cascade unique check (tasks/backlog.md item 12): the
// cascade holds the value stripe of every constrained value it introduces
// from the check through its LAST store write, and an open-ended cascade that
// moves the node off a value also holds the old value's stripe (ADR-0002
// Decision 3, as the update door does). Both are white-box: a cascade that
// took only the new stripe, or that released its stripe before the write,
// gives the same answers on an idle machine; only the lock interleaving
// tells them apart.

// cascadeGateStore parks ReplaceNode for one node id until released, so a
// test can act while the cascade is inside its store write.
type cascadeGateStore struct {
	*memory.Store
	mu      sync.Mutex
	gateID  types.NodeID
	entered chan struct{}
	release chan struct{}
}

func (s *cascadeGateStore) arm(id types.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gateID = id
	s.entered = make(chan struct{})
	s.release = make(chan struct{})
}

func (s *cascadeGateStore) ReplaceNode(n *types.Node) error {
	s.mu.Lock()
	gated := n != nil && s.gateID != 0 && n.ID() == s.gateID
	var entered, release chan struct{}
	if gated {
		entered, release = s.entered, s.release
		s.gateID = 0 // one shot
	}
	s.mu.Unlock()
	if gated {
		close(entered)
		<-release
	}
	return s.Store.ReplaceNode(n)
}

// NodesByLabelAndProperty is declared (not merely promoted) so the wrapper
// keeps the property-index capability (BACKLOG 14c guard, store_capabilities.go).
func (s *cascadeGateStore) NodesByLabelAndProperty(labelToken uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return s.Store.NodesByLabelAndProperty(labelToken, key, value, opts)
}

const lockTestT = types.Instant(1_000_000)

func newUniqueCascadeCore(t *testing.T, st *cascadeGateStore) *Core {
	t.Helper()
	cfg := Config{}
	if st != nil {
		cfg.Store = st
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Constraints.CreateUnique(context.Background(), "Ref", "k"); err != nil {
		t.Fatalf("CreateUnique: %v", err)
	}
	return c
}

func addRefK(t *testing.T, c *Core, v string) *types.Node {
	t.Helper()
	n, err := c.Nodes.Add(context.Background(), []string{"Ref"}, map[string]any{"k": v, "tkg_valid_from": lockTestT})
	if err != nil {
		t.Fatalf("Add k=%s: %v", v, err)
	}
	return n
}

// stripeOf is the value stripe of Ref.k = v.
func stripeOf(t *testing.T, c *Core, like *types.Node, v string) uint8 {
	t.Helper()
	tok, ok := c.labels.Lookup("Ref")
	if !ok {
		t.Fatal("label Ref not registered")
	}
	cp := like.DeepCopy()
	if err := cp.SetProperty("k", v); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	vk, found := cp.IndexablePropertyValueKey("k")
	if !found {
		t.Fatalf("no value key for %q", v)
	}
	return uniqueValueStripe(tok, "k", vk)
}

func waitDone(ch <-chan error, d time.Duration) (error, bool) {
	select {
	case err := <-ch:
		return err, true
	case <-time.After(d):
		return nil, false
	}
}

// An open-ended cascade that moves A off "a" must wait for the old value's
// stripe. Catches: a cascade that takes only the new value's stripe.
func TestCascadeUnique_HoldsOldValueStripe(t *testing.T) {
	c := newUniqueCascadeCore(t, nil)
	a := addRefK(t, c, "a")
	old := stripeOf(t, c, a, "a")
	newV := ""
	for _, cand := range []string{"w0", "w1", "w2", "w3", "w4", "w5", "w6", "w7"} {
		if stripeOf(t, c, a, cand) != old {
			newV = cand
			break
		}
	}
	if newV == "" {
		t.Fatal("no candidate value on a different stripe")
	}

	held := c.valueLocks.LockStripes([]uint8{old})
	done := make(chan error, 1)
	go func() {
		_, err := c.Temporal.SetNodeVersionInterval(context.Background(), a.ID(), lockTestT+100, 0, map[string]any{"k": newV})
		done <- err
	}()
	if err, finished := waitDone(done, 150*time.Millisecond); finished {
		c.valueLocks.UnlockStripes(held)
		t.Fatalf("cascade finished (err=%v) while the OLD value's stripe was held: it must hold both stripes", err)
	}
	c.valueLocks.UnlockStripes(held)
	if err, finished := waitDone(done, 5*time.Second); !finished || err != nil {
		t.Fatalf("cascade after release: finished=%v err=%v", finished, err)
	}
}

// A bounded cascade leaves the current row's value, so it takes no stripe for
// a UniqueCurrent value (no check binds it) — it must not block on one.
// Catches: a fix that locks (and checks) every patch value under
// UniqueCurrent, which would also refuse legal history duplicates.
func TestCascadeUnique_BoundedCurrentScopeTakesNoStripe(t *testing.T) {
	c := newUniqueCascadeCore(t, nil)
	a := addRefK(t, c, "a")
	held := c.valueLocks.LockStripes([]uint8{stripeOf(t, c, a, "p")})
	defer c.valueLocks.UnlockStripes(held)
	done := make(chan error, 1)
	go func() {
		_, err := c.Temporal.SetNodeVersionInterval(context.Background(), a.ID(), lockTestT+10, lockTestT+20, map[string]any{"k": "p"})
		done <- err
	}()
	if err, finished := waitDone(done, 5*time.Second); !finished || err != nil {
		t.Fatalf("bounded UniqueCurrent cascade: finished=%v err=%v, want nil without waiting on the value stripe", finished, err)
	}
}

// The stripe is held across the store write: while A's open-ended cascade
// onto "v" is parked inside ReplaceNode, an Update of B onto "v" must wait,
// and once A's write lands B is refused. Catches: a cascade that releases the
// stripe after the check but before the write (B passes the index check
// against A's old row and both end up holding "v").
func TestCascadeUnique_StripeHeldAcrossStoreWrite(t *testing.T) {
	st := &cascadeGateStore{Store: memory.New()}
	c := newUniqueCascadeCore(t, st)
	a := addRefK(t, c, "a")
	b := addRefK(t, c, "b")
	ctx := context.Background()

	st.arm(a.ID())
	entered := st.entered
	cascadeDone := make(chan error, 1)
	go func() {
		_, err := c.Temporal.SetNodeVersionInterval(ctx, a.ID(), lockTestT+100, 0, map[string]any{"k": "v"})
		cascadeDone <- err
	}()
	select {
	case <-entered:
	case err := <-cascadeDone:
		t.Fatalf("cascade finished (err=%v) without reaching ReplaceNode", err)
	case <-time.After(5 * time.Second):
		t.Fatal("cascade never reached ReplaceNode")
	}

	updDone := make(chan error, 1)
	go func() {
		_, err := c.Nodes.Update(ctx, b.ID(), map[string]any{"k": "v"})
		updDone <- err
	}()
	if err, finished := waitDone(updDone, 150*time.Millisecond); finished {
		close(st.release)
		<-cascadeDone
		t.Fatalf("Update onto v finished (err=%v) while the cascade claiming v was inside its store write", err)
	}
	close(st.release)
	if err := <-cascadeDone; err != nil {
		t.Fatalf("cascade: %v", err)
	}
	if err, finished := waitDone(updDone, 5*time.Second); !finished || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("Update after the cascade landed: finished=%v err=%v, want ErrUniqueViolation", finished, err)
	}
	rows, err := c.Nodes.ByLabelAndProperty("Ref", "k", "v", storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("holders of v = %d, want 1", len(rows))
	}
}
