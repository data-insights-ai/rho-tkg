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
func TestUniqueCascade_HoldsOldValueStripe(t *testing.T) {
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
func TestUniqueCascade_BoundedCurrentScopeTakesNoStripe(t *testing.T) {
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

// A patch value the row builder rejects is refused with the kernel's own
// error (the unique check steps aside), and nothing is appended. Catches: a
// check that reports the bad value as a unique violation or lets it reach a
// store write.
func TestUniqueCascade_InvalidPatchValueKeepsKernelError(t *testing.T) {
	c := newUniqueCascadeCore(t, nil)
	a := addRefK(t, c, "a")
	before, err := c.Nodes.History(a.ID())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Temporal.SetNodeVersionInterval(context.Background(), a.ID(), lockTestT+100, 0, map[string]any{"k": make(chan int)})
	if err == nil || errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("invalid patch value: err = %v, want the kernel's property error", err)
	}
	after, err := c.Nodes.History(a.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("History %d -> %d rows after a refused patch", len(before), len(after))
	}
}

// The stripe is held across the store write: while A's open-ended cascade
// onto "v" is parked inside ReplaceNode, an Update of B onto "v" must wait,
// and once A's write lands B is refused. Catches: a cascade that releases the
// stripe after the check but before the write (B passes the index check
// against A's old row and both end up holding "v").
func TestUniqueCascade_StripeHeldAcrossStoreWrite(t *testing.T) {
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

// errInjectedWrite is the store failure cascadeFailStore injects.
var errInjectedWrite = errors.New("injected store write failure")

// cascadeFailStore fails the next PutNodeVersion or ReplaceNode for one node
// id (one shot), so a test can make the cascade fail at a chosen write.
type cascadeFailStore struct {
	*memory.Store
	mu          sync.Mutex
	failID      types.NodeID
	failPut     bool
	failReplace bool
}

func (s *cascadeFailStore) NodesByLabelAndProperty(labelToken uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return s.Store.NodesByLabelAndProperty(labelToken, key, value, opts)
}

func (s *cascadeFailStore) arm(id types.NodeID, put, replace bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failID, s.failPut, s.failReplace = id, put, replace
}

func (s *cascadeFailStore) take(id types.NodeID, put bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failID == 0 || s.failID != id || (put && !s.failPut) || (!put && !s.failReplace) {
		return false
	}
	s.failID = 0
	return true
}

func (s *cascadeFailStore) PutNodeVersion(id types.NodeID, version uint32, n *types.Node) error {
	if s.take(id, true) {
		return errInjectedWrite
	}
	return s.Store.PutNodeVersion(id, version, n)
}

func (s *cascadeFailStore) ReplaceNode(n *types.Node) error {
	if n != nil && s.take(n.ID(), false) {
		return errInjectedWrite
	}
	return s.Store.ReplaceNode(n)
}

func newForeverFailCore(t *testing.T) (*Core, *cascadeFailStore) {
	t.Helper()
	st := &cascadeFailStore{Store: memory.New()}
	c, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Constraints.CreateUniqueForever(context.Background(), "Ref", "k"); err != nil {
		t.Fatalf("CreateUniqueForever: %v", err)
	}
	return c, st
}

// RED before the fix. An open-ended cascade onto "z" whose first store write
// fails has written no row carrying "z", so its UniqueForever claim must be
// withdrawn: a later create with "z" passes. Catches: a kernel that claims
// under the stripe and keeps the claim when the write that would have carried
// the value fails (the value is owned forever by a node that never held it).
func TestUniqueCascade_StoreWriteFailureWithdrawsUnwrittenClaim(t *testing.T) {
	c, st := newForeverFailCore(t)
	ctx := context.Background()
	a := addRefK(t, c, "a")
	st.arm(a.ID(), true, true) // the first write of the open-ended cascade is the demotion put
	if _, err := c.Temporal.SetNodeVersionInterval(ctx, a.ID(), lockTestT+100, 0, map[string]any{"k": "z"}); !errors.Is(err, errInjectedWrite) {
		t.Fatalf("cascade with a failing write: err = %v, want the injected failure", err)
	}
	if _, err := c.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"k": "z"}); err != nil {
		t.Fatalf("Add with the value of a cascade that wrote nothing: %v (claim must be withdrawn)", err)
	}
}

// cascadeMetaFailStore is a cascadeFailStore whose MetaSet fails once armed,
// so the withdrawal's registry persist fails.
type cascadeMetaFailStore struct {
	*cascadeFailStore
	failMeta bool
}

func (s *cascadeMetaFailStore) NodesByLabelAndProperty(labelToken uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return s.Store.NodesByLabelAndProperty(labelToken, key, value, opts)
}

func (s *cascadeMetaFailStore) MetaSet(key string, value []byte) error {
	s.mu.Lock()
	fail := s.failMeta
	s.mu.Unlock()
	if fail {
		return errInjectedWrite
	}
	return s.Store.MetaSet(key, value)
}

// GUARD on the withdrawal's persist-failure branch (written with it): when the
// registry cannot be persisted, the withdrawal keeps the claim in memory too
// (memory never diverges from disk) and the cascade returns both failures.
// Catches: a withdrawal that drops the in-memory claim although the persisted
// registry still holds it (after reopen the value would be owned again).
func TestUniqueCascade_StoreWriteFailureWithdrawPersistFailureKeepsClaim(t *testing.T) {
	st := &cascadeMetaFailStore{cascadeFailStore: &cascadeFailStore{Store: memory.New()}}
	c, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	if err := c.Constraints.CreateUniqueForever(ctx, "Ref", "k"); err != nil {
		t.Fatalf("CreateUniqueForever: %v", err)
	}
	a := addRefK(t, c, "a")
	st.arm(a.ID(), true, true)
	tok, _ := c.labels.Lookup("Ref")
	cp := a.DeepCopy()
	if err := cp.SetProperty("k", "z"); err != nil {
		t.Fatal(err)
	}
	vk, _ := cp.IndexablePropertyValueKey("k")
	// Fail the registry persist only after the claim was made: claim, then
	// fail the write, then the withdrawal's persist fails.
	if _, err := c.claimForever(tok, "k", vk, a.ID()); err != nil {
		t.Fatalf("claimForever: %v", err)
	}
	st.mu.Lock()
	st.failMeta = true
	st.mu.Unlock()
	hold := &uniqueHold{c: c, id: a.ID(), claims: []uniqueCheckTuple{{labelTok: tok, key: "k", valueKey: vk}}}
	err = hold.writeFailed(nil, errInjectedWrite)
	if !errors.Is(err, errInjectedWrite) || err.Error() == errInjectedWrite.Error() {
		t.Fatalf("writeFailed with a failing persist: err = %v, want the write failure joined with the withdrawal failure", err)
	}
	if err := c.checkForeverOwnership(tok, "k", vk, a.ID()+1); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("after a failed withdrawal persist, the in-memory claim must stay: err = %v", err)
	}
}

// GUARD (passes before the fix too): when the write that fails comes AFTER a
// correction row carrying "z" was stored, "z" was written by the node, so its
// UniqueForever claim stays. Catches: a withdrawal that ignores which rows
// were already written (it would free a value present in the node's history).
func TestUniqueCascade_StoreWriteFailureKeepsWrittenClaim(t *testing.T) {
	c, st := newForeverFailCore(t)
	ctx := context.Background()
	a := addRefK(t, c, "a")
	st.arm(a.ID(), false, true) // corrections and the demotion are put first; ReplaceNode fails
	if _, err := c.Temporal.SetNodeVersionInterval(ctx, a.ID(), lockTestT+10, lockTestT+20, map[string]any{"k": "z"}); !errors.Is(err, errInjectedWrite) {
		t.Fatalf("cascade with a failing ReplaceNode: err = %v, want the injected failure", err)
	}
	hist, err := c.Nodes.History(a.ID())
	if err != nil {
		t.Fatal(err)
	}
	wrote := false
	for _, h := range hist {
		if v, _ := h.GetProperty("k"); v == "z" {
			wrote = true
		}
	}
	if !wrote {
		t.Fatal("precondition: no stored row carries z")
	}
	if _, err := c.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"k": "z"}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("Add with a value the node already wrote: err = %v, want ErrUniqueViolation", err)
	}
}
