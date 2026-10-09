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

// Lock protocol of the claim withdrawal (tasks/backlog.md item 29): a door
// whose store write fails withdraws its UniqueForever claims while it still
// holds the value stripe. White-box: a door that released the stripe first
// and withdrew afterwards gives the same end state on an idle machine; only
// the interleaving shows it — a concurrent writer of the value, admitted by
// the released stripe, is refused as "permanently owned" by a node that
// never stored the value.

// withdrawGateStore fails the first node write carrying k == value, then
// parks the next GetNode of that node (the withdrawal re-reads the stored
// row) until released.
type withdrawGateStore struct {
	*memory.Store
	mu      sync.Mutex
	value   any
	parkID  types.NodeID
	entered chan struct{}
	release chan struct{}
}

func (s *withdrawGateStore) arm(v any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.value = v
	s.entered = make(chan struct{})
	s.release = make(chan struct{})
}

func (s *withdrawGateStore) fail(n *types.Node) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.value == nil || n == nil {
		return false
	}
	if v, ok := n.GetProperty("k"); !ok || v != s.value {
		return false
	}
	s.value = nil
	s.parkID = n.ID()
	return true
}

func (s *withdrawGateStore) PutNode(n *types.Node) error {
	if s.fail(n) {
		return errInjectedWrite
	}
	return s.Store.PutNode(n)
}

func (s *withdrawGateStore) ReplaceNodeWithHistory(n *types.Node, pv uint32, prev *types.Node) error {
	if s.fail(n) {
		return errInjectedWrite
	}
	return s.Store.ReplaceNodeWithHistory(n, pv, prev)
}

func (s *withdrawGateStore) GetNode(id types.NodeID) (*types.Node, error) {
	s.mu.Lock()
	park := s.parkID != 0 && s.parkID == id
	var entered, release chan struct{}
	if park {
		s.parkID = 0
		entered, release = s.entered, s.release
	}
	s.mu.Unlock()
	if park {
		close(entered)
		<-release
	}
	return s.Store.GetNode(id)
}

func (s *withdrawGateStore) NodesByLabelAndProperty(labelToken uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return s.Store.NodesByLabelAndProperty(labelToken, key, value, opts)
}

// RED before the fix (no withdrawal: B is refused as "permanently owned").
// While the failed writer A withdraws (parked in its stored-row re-read),
// writer B of the same value must still wait on the stripe; once A is done, B
// wins. Catches: withdraw nothing (B refused), release the stripe before the
// withdrawal (B runs while A still owns the value and is refused).
func TestUniqueClaims_StripeHeldAcrossWithdrawal(t *testing.T) {
	type door struct {
		name string
		// target prepares the node a writer moves onto v (0 for a create).
		target func(t *testing.T, c *Core, i int) types.NodeID
		write  func(c *Core, id types.NodeID, v string) error
	}
	ctx := context.Background()
	doors := []door{
		{"Update", func(t *testing.T, c *Core, i int) types.NodeID {
			n := addRefK(t, c, []string{"a", "b"}[i])
			return n.ID()
		}, func(c *Core, id types.NodeID, v string) error {
			_, err := c.Nodes.Update(ctx, id, map[string]any{"k": v})
			return err
		}},
		{"session-concurrent-AddNode", func(*testing.T, *Core, int) types.NodeID { return 0 }, func(c *Core, _ types.NodeID, v string) error {
			s, err := c.Ingest.NewSession(IngestOptions{Concurrent: true})
			if err != nil {
				return err
			}
			defer s.Close()
			if _, err := s.AddNode([]string{"Ref"}, map[string]any{"k": v}); err != nil {
				return err
			}
			_, err = s.Submit()
			return err
		}},
	}
	for _, d := range doors {
		d := d
		t.Run(d.name, func(t *testing.T) {
			st := &withdrawGateStore{Store: memory.New()}
			c, err := New(Config{Store: st})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			if err := c.Constraints.CreateUniqueForever(ctx, "Ref", "k"); err != nil {
				t.Fatalf("CreateUniqueForever: %v", err)
			}
			a, b := d.target(t, c, 0), d.target(t, c, 1)
			st.arm("v")
			entered := st.entered
			aDone := make(chan error, 1)
			go func() { aDone <- d.write(c, a, "v") }()

			parked := false
			select {
			case <-entered:
				parked = true
			case err := <-aDone:
				aDone <- err // A finished without re-reading its stored row
			case <-time.After(5 * time.Second):
				t.Fatal("writer A neither finished nor re-read its stored row")
			}
			bDone := make(chan error, 1)
			go func() { bDone <- d.write(c, b, "v") }()
			if parked {
				if err, finished := waitDone(bDone, 150*time.Millisecond); finished {
					close(st.release)
					<-aDone
					t.Fatalf("writer B finished (err=%v) while failed writer A was still withdrawing its claim: the stripe must be held across the withdrawal", err)
				}
				close(st.release)
			}
			if err := <-aDone; !errors.Is(err, errInjectedWrite) {
				t.Fatalf("writer A: err = %v, want the injected write failure", err)
			}
			if err, finished := waitDone(bDone, 5*time.Second); !finished || err != nil {
				t.Fatalf("writer B after A's failed write: finished=%v err=%v, want nil (A never stored v)", finished, err)
			}
			rows, err := c.Nodes.ByLabelAndProperty("Ref", "k", "v", storepkg.QueryOpts{})
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 {
				t.Fatalf("holders of v = %d, want 1", len(rows))
			}
		})
	}
}
