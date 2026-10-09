package graph_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Fault-injecting Config.Store decorators for the unique-claim tests
// (tasks/backlog.md item 29). Each embeds one in-tree backend and overrides
// the node write doors the claiming paths reach (PutNode, PutNodesBatch,
// ReplaceNode, ReplaceNodeWithHistory, AddNodeLabelTokenWithHistory), the
// partial-create cleanup (DeleteNode) and the registry persist (MetaSet).
// NodesByLabelAndProperty is declared so the wrapper keeps the
// property-index capability (BACKLOG 14c guard).

// errClaimFault is the failure every armed fault returns.
var errClaimFault = errors.New("injected unique-claims fault")

// claimOwnersMeta is the MetaKV key of the UniqueForever ownership registry
// (unique_forever.go uniqueForeverOwnersMeta).
const claimOwnersMeta = "unique_forever_owners"

// claimFault is the shared trigger state of one decorated store.
type claimFault struct {
	mu sync.Mutex
	// writeValue != nil: the next node write whose node carries this value on
	// ucKey or ucKey2 fails (one shot). writeAfter: the write is performed
	// first, then the failure is returned (a store that reports an error after
	// installing the row).
	writeValue any
	writeAfter bool
	// deleteFail: the next DeleteNode fails (one shot), so a partial create
	// stays live.
	deleteFail bool
	// metaArmed: after metaSkip successful ownership-registry persists, the
	// next one fails (one shot).
	metaArmed bool
	metaSkip  int
	fired     int // node-write faults fired
}

func (f *claimFault) armWrite(v any, after, failDelete bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeValue, f.writeAfter, f.deleteFail = v, after, failDelete
}

func (f *claimFault) armMeta(skip int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.metaArmed, f.metaSkip = true, skip
}

func (f *claimFault) disarm() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeValue, f.writeAfter, f.deleteFail, f.metaArmed, f.metaSkip = nil, false, false, false, 0
}

func (f *claimFault) firedWrites() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fired
}

func claimCarries(nodes []*types.Node, want any) bool {
	for _, n := range nodes {
		if n == nil {
			continue
		}
		for _, k := range []string{ucKey, ucKey2} {
			if v, ok := n.GetProperty(k); ok && v == want {
				return true
			}
		}
	}
	return false
}

func (f *claimFault) write(nodes []*types.Node, do func() error) error {
	f.mu.Lock()
	fire := f.writeValue != nil && claimCarries(nodes, f.writeValue)
	after := f.writeAfter
	if fire {
		f.writeValue = nil
		f.fired++
	}
	f.mu.Unlock()
	if !fire {
		return do()
	}
	if after {
		if err := do(); err != nil {
			return err
		}
	}
	return errClaimFault
}

func (f *claimFault) del(do func() error) error {
	f.mu.Lock()
	fire := f.deleteFail
	f.deleteFail = false
	f.mu.Unlock()
	if fire {
		return errClaimFault
	}
	return do()
}

func (f *claimFault) meta(key string, do func() error) error {
	if key == claimOwnersMeta {
		f.mu.Lock()
		fire := false
		if f.metaArmed {
			if f.metaSkip > 0 {
				f.metaSkip--
			} else {
				f.metaArmed = false
				fire = true
			}
		}
		f.mu.Unlock()
		if fire {
			return errClaimFault
		}
	}
	return do()
}

func one(n *types.Node) []*types.Node { return []*types.Node{n} }

// ---- memory -----------------------------------------------------------------

type claimFaultMemory struct {
	*memory.Store
	f *claimFault
}

func (s *claimFaultMemory) PutNode(n *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.PutNode(n) })
}
func (s *claimFaultMemory) PutNodesBatch(ns []*types.Node) error {
	return s.f.write(ns, func() error { return s.Store.PutNodesBatch(ns) })
}
func (s *claimFaultMemory) ReplaceNode(n *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.ReplaceNode(n) })
}
func (s *claimFaultMemory) ReplaceNodeWithHistory(n *types.Node, pv uint32, prev *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.ReplaceNodeWithHistory(n, pv, prev) })
}
func (s *claimFaultMemory) AddNodeLabelTokenWithHistory(id types.NodeID, tok uint16, n *types.Node, pv uint32, prev *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.AddNodeLabelTokenWithHistory(id, tok, n, pv, prev) })
}
func (s *claimFaultMemory) DeleteNode(id types.NodeID) error {
	return s.f.del(func() error { return s.Store.DeleteNode(id) })
}
func (s *claimFaultMemory) MetaSet(key string, value []byte) error {
	return s.f.meta(key, func() error { return s.Store.MetaSet(key, value) })
}
func (s *claimFaultMemory) NodesByLabelAndProperty(l uint16, k string, v any, o storepkg.QueryOpts) ([]*types.Node, error) {
	return s.Store.NodesByLabelAndProperty(l, k, v, o)
}

// ---- badger -----------------------------------------------------------------

type claimFaultBadger struct {
	*badger.Store
	f *claimFault
}

func (s *claimFaultBadger) PutNode(n *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.PutNode(n) })
}
func (s *claimFaultBadger) PutNodesBatch(ns []*types.Node) error {
	return s.f.write(ns, func() error { return s.Store.PutNodesBatch(ns) })
}
func (s *claimFaultBadger) ReplaceNode(n *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.ReplaceNode(n) })
}
func (s *claimFaultBadger) ReplaceNodeWithHistory(n *types.Node, pv uint32, prev *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.ReplaceNodeWithHistory(n, pv, prev) })
}
func (s *claimFaultBadger) AddNodeLabelTokenWithHistory(id types.NodeID, tok uint16, n *types.Node, pv uint32, prev *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.AddNodeLabelTokenWithHistory(id, tok, n, pv, prev) })
}
func (s *claimFaultBadger) DeleteNode(id types.NodeID) error {
	return s.f.del(func() error { return s.Store.DeleteNode(id) })
}
func (s *claimFaultBadger) MetaSet(key string, value []byte) error {
	return s.f.meta(key, func() error { return s.Store.MetaSet(key, value) })
}
func (s *claimFaultBadger) NodesByLabelAndProperty(l uint16, k string, v any, o storepkg.QueryOpts) ([]*types.Node, error) {
	return s.Store.NodesByLabelAndProperty(l, k, v, o)
}

// ---- tiered -----------------------------------------------------------------

type claimFaultTiered struct {
	*tiered.Store
	f *claimFault
}

func (s *claimFaultTiered) PutNode(n *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.PutNode(n) })
}
func (s *claimFaultTiered) PutNodesBatch(ns []*types.Node) error {
	return s.f.write(ns, func() error { return s.Store.PutNodesBatch(ns) })
}
func (s *claimFaultTiered) ReplaceNode(n *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.ReplaceNode(n) })
}
func (s *claimFaultTiered) ReplaceNodeWithHistory(n *types.Node, pv uint32, prev *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.ReplaceNodeWithHistory(n, pv, prev) })
}
func (s *claimFaultTiered) AddNodeLabelTokenWithHistory(id types.NodeID, tok uint16, n *types.Node, pv uint32, prev *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.AddNodeLabelTokenWithHistory(id, tok, n, pv, prev) })
}
func (s *claimFaultTiered) DeleteNode(id types.NodeID) error {
	return s.f.del(func() error { return s.Store.DeleteNode(id) })
}
func (s *claimFaultTiered) MetaSet(key string, value []byte) error {
	return s.f.meta(key, func() error { return s.Store.MetaSet(key, value) })
}
func (s *claimFaultTiered) NodesByLabelAndProperty(l uint16, k string, v any, o storepkg.QueryOpts) ([]*types.Node, error) {
	return s.Store.NodesByLabelAndProperty(l, k, v, o)
}

// ---- sharded ----------------------------------------------------------------

type claimFaultSharded struct {
	*sharded.Store
	f *claimFault
}

func (s *claimFaultSharded) PutNode(n *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.PutNode(n) })
}
func (s *claimFaultSharded) PutNodesBatch(ns []*types.Node) error {
	return s.f.write(ns, func() error { return s.Store.PutNodesBatch(ns) })
}
func (s *claimFaultSharded) ReplaceNode(n *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.ReplaceNode(n) })
}
func (s *claimFaultSharded) ReplaceNodeWithHistory(n *types.Node, pv uint32, prev *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.ReplaceNodeWithHistory(n, pv, prev) })
}
func (s *claimFaultSharded) AddNodeLabelTokenWithHistory(id types.NodeID, tok uint16, n *types.Node, pv uint32, prev *types.Node) error {
	return s.f.write(one(n), func() error { return s.Store.AddNodeLabelTokenWithHistory(id, tok, n, pv, prev) })
}
func (s *claimFaultSharded) DeleteNode(id types.NodeID) error {
	return s.f.del(func() error { return s.Store.DeleteNode(id) })
}
func (s *claimFaultSharded) MetaSet(key string, value []byte) error {
	return s.f.meta(key, func() error { return s.Store.MetaSet(key, value) })
}
func (s *claimFaultSharded) NodesByLabelAndProperty(l uint16, k string, v any, o storepkg.QueryOpts) ([]*types.Node, error) {
	return s.Store.NodesByLabelAndProperty(l, k, v, o)
}

// claimBackend opens a fresh graph over one decorated backend.
type claimBackend struct {
	name string
	open func(t *testing.T) (*graphpkg.Graph, *claimFault)
}

func claimBackends() []claimBackend {
	newGraph := func(t *testing.T, st storepkg.MandatoryStore) *graphpkg.Graph {
		t.Helper()
		g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0, Store: st, AllowTxBackfill: true})
		if err != nil {
			t.Fatalf("graph.New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		return g
	}
	return []claimBackend{
		{"memory", func(t *testing.T) (*graphpkg.Graph, *claimFault) {
			f := &claimFault{}
			return newGraph(t, &claimFaultMemory{Store: memory.New(), f: f}), f
		}},
		{"badger", func(t *testing.T) (*graphpkg.Graph, *claimFault) {
			bs, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			f := &claimFault{}
			return newGraph(t, &claimFaultBadger{Store: bs, f: f}), f
		}},
		{"tiered", func(t *testing.T) (*graphpkg.Graph, *claimFault) {
			ts, err := tiered.New(tiered.Config{
				InMemory:      true,
				RefLabels:     []string{"Ref", "Plain"},
				ShardWindow:   7 * 24 * time.Hour,
				FlushInterval: 1<<63 - 1,
			})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			f := &claimFault{}
			return newGraph(t, &claimFaultTiered{Store: ts, f: f}), f
		}},
		{"sharded", func(t *testing.T) (*graphpkg.Graph, *claimFault) {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			f := &claimFault{}
			return newGraph(t, &claimFaultSharded{Store: st, f: f}), f
		}},
	}
}
