// Tests in this file pin the F5 fix from the 2026-05-08 maintainability
// review: relationship endpoint hash refresh must surface store errors.
// Before the fix, both the standalone update path
// (updateRelationshipInternal) and the batch path (BatchBuilder.runRels)
// silently swallowed GetNode errors and wrote the relationship with empty
// FromNodeHash / ToNodeHash, making operational faults indistinguishable
// from a broken endpoint row.

package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/generatedcreate"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// endpointFaultStore wraps a Store and, once enabled, returns a fixed
// non-sentinel error from GetNode for a single target node. Used to drive
// the F5 endpoint hash-refresh paths into their error branches without
// disturbing any other store interaction.
type endpointFaultStore struct {
	storepkg.Store
	target  types.NodeID
	err     error
	enabled atomic.Bool
}

func (s *endpointFaultStore) GetNode(id types.NodeID) (*types.Node, error) {
	if s.enabled.Load() && id == s.target {
		return nil, s.err
	}
	return s.Store.GetNode(id)
}

type concreteEndpointFaultStore struct {
	*memory.Store
	target  types.NodeID
	err     error
	enabled atomic.Bool
}

func (s *concreteEndpointFaultStore) GetNode(id types.NodeID) (*types.Node, error) {
	if s.enabled.Load() && id == s.target {
		return nil, s.err
	}
	return s.Store.GetNode(id)
}

type endpointHashStub struct {
	from string
	to   string
	err  error
}

func (s endpointHashStub) EndpointIntegrityHashes(types.NodeID, types.NodeID) (string, string, error) {
	if s.err != nil {
		return "", "", s.err
	}
	return s.from, s.to, nil
}

type nodeHashStub struct {
	hashes map[types.NodeID]string
	errs   map[types.NodeID]error
	calls  []types.NodeID
}

func (s *nodeHashStub) NodeIntegrityHash(id types.NodeID) (string, error) {
	s.calls = append(s.calls, id)
	if err := s.errs[id]; err != nil {
		return "", err
	}
	return s.hashes[id], nil
}

func TestLiveEndpointHashes_EndpointCapability(t *testing.T) {
	t.Parallel()

	c := &Core{endpointHash: endpointHashStub{from: "from-hash", to: "to-hash"}}
	from, to, err := c.liveEndpointHashes(types.NodeID(1), types.NodeID(2))
	if err != nil {
		t.Fatalf("liveEndpointHashes endpoint capability: %v", err)
	}
	if from != "from-hash" || to != "to-hash" {
		t.Fatalf("liveEndpointHashes = %q, %q; want from-hash, to-hash", from, to)
	}

	c.endpointHash = endpointHashStub{from: "self-hash", to: "stale-other-hash"}
	from, to, err = c.liveEndpointHashes(types.NodeID(1), types.NodeID(1))
	if err != nil {
		t.Fatalf("liveEndpointHashes self endpoint capability: %v", err)
	}
	if from != "self-hash" || to != "self-hash" {
		t.Fatalf("liveEndpointHashes self = %q, %q; want self-hash twice", from, to)
	}

	injected := errors.New("synthetic endpoint hash failure")
	c.endpointHash = endpointHashStub{err: injected}
	if _, _, err := c.liveEndpointHashes(types.NodeID(1), types.NodeID(2)); !errors.Is(err, injected) {
		t.Fatalf("liveEndpointHashes endpoint error = %v, want injected fault", err)
	}
}

func TestLiveEndpointHashes_NodeHashCapability(t *testing.T) {
	t.Parallel()

	startID := types.NodeID(1)
	endID := types.NodeID(2)
	stub := &nodeHashStub{
		hashes: map[types.NodeID]string{
			startID: "start-hash",
			endID:   "end-hash",
		},
		errs: make(map[types.NodeID]error),
	}
	c := &Core{nodeHash: stub}

	from, to, err := c.liveEndpointHashes(startID, endID)
	if err != nil {
		t.Fatalf("liveEndpointHashes node hash: %v", err)
	}
	if from != "start-hash" || to != "end-hash" {
		t.Fatalf("liveEndpointHashes = %q, %q; want start-hash, end-hash", from, to)
	}

	stub.calls = nil
	from, to, err = c.liveEndpointHashes(startID, startID)
	if err != nil {
		t.Fatalf("liveEndpointHashes self node hash: %v", err)
	}
	if from != "start-hash" || to != "start-hash" {
		t.Fatalf("liveEndpointHashes self = %q, %q; want start-hash twice", from, to)
	}
	if len(stub.calls) != 1 || stub.calls[0] != startID {
		t.Fatalf("self endpoint hash calls = %v, want one start-node call", stub.calls)
	}

	injectedStart := errors.New("synthetic start hash failure")
	stub.errs[startID] = injectedStart
	if _, _, err := c.liveEndpointHashes(startID, endID); !errors.Is(err, injectedStart) {
		t.Fatalf("liveEndpointHashes start error = %v, want injected start fault", err)
	}
	delete(stub.errs, startID)

	injectedEnd := errors.New("synthetic end hash failure")
	stub.errs[endID] = injectedEnd
	if _, _, err := c.liveEndpointHashes(startID, endID); !errors.Is(err, injectedEnd) {
		t.Fatalf("liveEndpointHashes end error = %v, want injected end fault", err)
	}
}

func TestRefreshRelationshipEndpointHashes_NormalizesSelfEndpointBatchHash(t *testing.T) {
	t.Parallel()

	id := types.NodeID(1)
	rel := types.NewRelationship(types.RelID(2), 1, id, id)
	ig := &types.RelIntegrity{}
	rel.SetIntegrity(ig)
	c := &Core{endpointHash: endpointHashStub{from: "current-hash", to: "stale-other-hash"}}

	if err := c.refreshRelationshipEndpointHashes(rel, ig); err != nil {
		t.Fatalf("refreshRelationshipEndpointHashes self endpoint capability: %v", err)
	}
	if ig.FromNodeHash != "current-hash" || ig.ToNodeHash != "current-hash" {
		t.Fatalf("refreshRelationshipEndpointHashes self = %q, %q; want current-hash twice", ig.FromNodeHash, ig.ToNodeHash)
	}
}

func TestRefreshRelationshipEndpointHashes_NodeHashCapability(t *testing.T) {
	t.Parallel()

	startID := types.NodeID(1)
	endID := types.NodeID(2)
	stub := &nodeHashStub{
		hashes: map[types.NodeID]string{
			startID: "start-hash",
			endID:   "end-hash",
		},
		errs: make(map[types.NodeID]error),
	}
	c := &Core{nodeHash: stub}
	rel := types.NewRelationship(types.RelID(3), 1, startID, endID)
	ig := &types.RelIntegrity{}

	if err := c.refreshRelationshipEndpointHashes(rel, ig); err != nil {
		t.Fatalf("refreshRelationshipEndpointHashes node hash: %v", err)
	}
	if ig.FromNodeHash != "start-hash" || ig.ToNodeHash != "end-hash" {
		t.Fatalf("endpoint hashes = %q, %q; want start-hash, end-hash", ig.FromNodeHash, ig.ToNodeHash)
	}

	stub.errs[endID] = storepkg.ErrNodeNotFound
	ig = &types.RelIntegrity{}
	if err := c.refreshRelationshipEndpointHashes(rel, ig); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("refreshRelationshipEndpointHashes missing endpoint = %v, want ErrNodeNotFound", err)
	}
}

func TestRefreshRelationshipEndpointHashes_EndpointCapabilityNotFoundFallsBack(t *testing.T) {
	t.Parallel()

	startID := types.NodeID(1)
	endID := types.NodeID(2)
	stub := &nodeHashStub{
		hashes: map[types.NodeID]string{
			startID: "start-hash",
			endID:   "end-hash",
		},
		errs: make(map[types.NodeID]error),
	}
	c := &Core{
		endpointHash: endpointHashStub{err: storepkg.ErrNodeNotFound},
		nodeHash:     stub,
	}
	rel := types.NewRelationship(types.RelID(3), 1, startID, endID)
	ig := &types.RelIntegrity{}

	if err := c.refreshRelationshipEndpointHashes(rel, ig); err != nil {
		t.Fatalf("refreshRelationshipEndpointHashes endpoint fallback: %v", err)
	}
	if ig.FromNodeHash != "start-hash" || ig.ToNodeHash != "end-hash" {
		t.Fatalf("endpoint fallback hashes = %q, %q; want start-hash, end-hash", ig.FromNodeHash, ig.ToNodeHash)
	}
}

func TestAddByIDIfAbsentUsesEndpointHashWriteCapability(t *testing.T) {
	t.Parallel()

	injected := errors.New("unexpected endpoint GetNode")
	fs := &concreteEndpointFaultStore{Store: memory.New(), err: injected}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}

	cap, ok := any(fs).(generatedcreate.RelationshipEndpointHashCapability)
	if !ok {
		t.Fatal("test wrapper must inherit RelationshipEndpointHashCapability from memory.Store")
	}
	g.endpointHashWrite = cap

	fs.target = a.ID()
	fs.enabled.Store(true)

	rel, created, err := g.Rels.AddByIDIfAbsentWithContext(context.Background(), "KNOWS", a.ID(), b.ID(), nil)
	if err != nil {
		t.Fatalf("AddByIDIfAbsentWithContext: %v", err)
	}
	if !created {
		t.Fatal("AddByIDIfAbsentWithContext returned existing relationship, want create")
	}
	ig := rel.Integrity()
	if ig == nil || ig.FromNodeHash == "" || ig.ToNodeHash == "" {
		t.Fatalf("endpoint hashes not captured: %#v", ig)
	}
}

func TestBatchAddRelationshipUsesEndpointHashWriteCapability(t *testing.T) {
	t.Parallel()

	injected := errors.New("unexpected endpoint GetNode")
	fs := &concreteEndpointFaultStore{Store: memory.New(), err: injected}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}

	cap, ok := any(fs).(generatedcreate.RelationshipEndpointHashCapability)
	if !ok {
		t.Fatal("test wrapper must inherit RelationshipEndpointHashCapability from memory.Store")
	}
	g.endpointHashWrite = cap

	bb, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	rel, err := bb.AddRelationship("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	fs.target = a.ID()
	fs.enabled.Store(true)

	res, err := bb.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Created != 1 || res.Failed != 0 {
		t.Fatalf("result = %+v, want Created=1 Failed=0", res)
	}
	ig := rel.Integrity()
	if ig == nil || ig.FromNodeHash == "" || ig.ToNodeHash == "" {
		t.Fatalf("endpoint hashes not captured: %#v", ig)
	}
}

func TestRelImportUsesEndpointHashCapability(t *testing.T) {
	t.Parallel()

	injected := errors.New("unexpected endpoint GetNode")
	fs := &concreteEndpointFaultStore{Store: memory.New(), err: injected}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.endpointHash != nil {
		t.Fatal("concrete wrapper must not enable the endpoint hash fast path")
	}

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}

	cap, ok := any(fs).(storepkg.EndpointIntegrityHashCapability)
	if !ok {
		t.Fatal("test wrapper must inherit EndpointIntegrityHashCapability from memory.Store")
	}
	g.endpointHash = cap

	fs.target = a.ID()
	fs.enabled.Store(true)

	rel, err := g.Rels.Import(context.Background(), g.Rels.NextID(), "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	ig := rel.Integrity()
	if ig == nil || ig.FromNodeHash == "" || ig.ToNodeHash == "" {
		t.Fatalf("endpoint hashes not captured: %#v", ig)
	}
}

func TestRelAdd_ConcreteMemoryWrapperEndpointReadFailure_Propagates(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic wrapper endpoint read fault")
	fs := &concreteEndpointFaultStore{Store: memory.New(), err: injected}
	if _, ok := any(fs).(storepkg.EndpointIntegrityHashCapability); !ok {
		t.Fatal("test wrapper must inherit EndpointIntegrityHashCapability from memory.Store")
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.endpointHash != nil {
		t.Fatal("concrete wrapper must not enable the endpoint hash fast path")
	}

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}

	fs.target = b.ID()
	fs.enabled.Store(true)

	const typ = "CONCRETE_WRAPPER_ENDPOINT_FAULT"
	rel, err := g.Rels.AddWithContext(context.Background(), typ, a, b, nil)
	if !errors.Is(err, injected) {
		t.Fatalf("AddWithContext err = %v, want injected endpoint read fault", err)
	}
	if rel != nil {
		t.Fatalf("AddWithContext rel = %v, want nil on endpoint read failure", rel)
	}
	if _, ok := g.relTypes.Lookup(typ); ok {
		t.Fatalf("relationship type %q remained registered after endpoint read failure", typ)
	}
}

func TestRelUpdate_ConcreteMemoryWrapperEndpointReadFailure_Propagates(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic wrapper endpoint read fault")
	fs := &concreteEndpointFaultStore{Store: memory.New(), err: injected}
	if _, ok := any(fs).(storepkg.EndpointIntegrityHashCapability); !ok {
		t.Fatal("test wrapper must inherit EndpointIntegrityHashCapability from memory.Store")
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.endpointHash != nil {
		t.Fatal("concrete wrapper must not enable the endpoint hash fast path")
	}

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.Rels.AddWithContext(context.Background(), "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	fs.target = b.ID()
	fs.enabled.Store(true)

	updated, err := g.Rels.UpdateWithContext(context.Background(), r.ID(), map[string]any{"since": 2025})
	if !errors.Is(err, injected) {
		t.Fatalf("UpdateWithContext err = %v, want injected endpoint read fault", err)
	}
	if updated != nil {
		t.Fatalf("updated rel = %v, want nil on endpoint read failure", updated)
	}
}

func TestRelUpdate_EndpointReadFailure_Propagates(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic disk fault")
	fs := &endpointFaultStore{Store: memory.New(), err: injected}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.Rels.AddWithContext(context.Background(), "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	fs.target = b.ID()
	fs.enabled.Store(true)

	updated, err := g.Rels.UpdateWithContext(context.Background(), r.ID(), map[string]any{"since": 2025})
	if err == nil {
		t.Fatalf("UpdateWithContext: got nil error, want non-nil (synthetic GetNode failure must propagate)")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("err = %v, want errors.Is(err, injected)", err)
	}
	if updated != nil {
		t.Fatalf("updated rel = %v, want nil on failure", updated)
	}

	fs.enabled.Store(false)
	got, err := g.Rels.GetWithContext(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("GetRelationship after failed update: %v", err)
	}
	if v, ok := got.GetProperty("since"); ok {
		t.Fatalf("rel acquired property 'since' = %v after failed update", v)
	}
	if got.Version() != r.Version() {
		t.Fatalf("rel version = %d, want %d (no version bump on failed update)", got.Version(), r.Version())
	}
}

func TestRelUpdate_EndpointNotFound_Propagates(t *testing.T) {
	t.Parallel()
	fs := &endpointFaultStore{Store: memory.New(), err: storepkg.ErrNodeNotFound}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.Rels.AddWithContext(context.Background(), "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	fs.target = b.ID()
	fs.enabled.Store(true)

	updated, err := g.Rels.UpdateWithContext(context.Background(), r.ID(), map[string]any{"since": 2025})
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("UpdateWithContext err = %v, want ErrNodeNotFound", err)
	}
	if updated != nil {
		t.Fatalf("updated rel = %v, want nil on missing endpoint", updated)
	}

	fs.enabled.Store(false)
	got, err := g.Rels.GetWithContext(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("GetRelationship after failed update: %v", err)
	}
	if v, ok := got.GetProperty("since"); ok {
		t.Fatalf("rel acquired property 'since' = %v after failed update", v)
	}
	if got.Version() != r.Version() {
		t.Fatalf("rel version = %d, want %d (no version bump on failed update)", got.Version(), r.Version())
	}
}

func TestRelCAS_EndpointNotFound_Propagates(t *testing.T) {
	t.Parallel()
	fs := &endpointFaultStore{Store: memory.New(), err: storepkg.ErrNodeNotFound}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.Rels.AddWithContext(context.Background(), "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	fs.target = b.ID()
	fs.enabled.Store(true)

	ok, err := g.Rels.CompareAndSetPropertyWithContext(context.Background(), r.ID(), "since", nil, 2025)
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("CompareAndSetPropertyWithContext err = %v, want ErrNodeNotFound", err)
	}
	if ok {
		t.Fatal("CompareAndSetPropertyWithContext ok = true, want false on missing endpoint")
	}

	fs.enabled.Store(false)
	got, err := g.Rels.GetWithContext(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("GetRelationship after failed CAS: %v", err)
	}
	if v, ok := got.GetProperty("since"); ok {
		t.Fatalf("rel acquired property 'since' = %v after failed CAS", v)
	}
	if got.Version() != r.Version() {
		t.Fatalf("rel version = %d, want %d (no version bump on failed CAS)", got.Version(), r.Version())
	}
}

func TestBatch_EndpointReadFailure_RecordsBatchError(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic disk fault")
	fs := &endpointFaultStore{Store: memory.New(), err: injected}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	c, err := g.Nodes.Add([]string{"C"}, nil)
	if err != nil {
		t.Fatalf("AddNode C: %v", err)
	}

	bb, _ := NewBatchBuilder(g)
	rFail, err := bb.AddRelationship("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("queue rFail: %v", err)
	}
	if _, err := bb.AddRelationship("KNOWS", a, c, nil); err != nil {
		t.Fatalf("queue rOK: %v", err)
	}

	fs.target = b.ID()
	fs.enabled.Store(true)

	res, err := bb.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", err)
	}

	if res.Failed != 1 {
		t.Fatalf("res.Failed = %d, want 1 (only the b-endpointed rel should fail)", res.Failed)
	}
	if res.Created < 1 {
		t.Fatalf("res.Created = %d, want >= 1 (the c-endpointed rel must still commit)", res.Created)
	}

	var matched *BatchError
	for i := range res.Errors {
		if res.Errors[i].ID == types.EntityID(rFail.ID()) {
			matched = &res.Errors[i]
			break
		}
	}
	if matched == nil {
		t.Fatalf("res.Errors did not include the failed rel; got %+v", res.Errors)
	}
	if matched.Op != "AddRelationship" {
		t.Errorf("Op = %q, want %q", matched.Op, "AddRelationship")
	}
	if !errors.Is(matched.Err, injected) {
		t.Errorf("Err = %v, want errors.Is(err, injected)", matched.Err)
	}
	if tm := rFail.Temporal(); tm != nil && tm.TxFrom != 0 {
		t.Fatalf("failed rel TxFrom = %d, want queue-time zero", tm.TxFrom)
	}
	if ig := rFail.Integrity(); ig == nil {
		t.Fatal("failed rel integrity = nil, want queue-time integrity")
	} else if ig.FromNodeHash != "" || ig.ToNodeHash != "" {
		t.Fatalf("failed rel endpoint hashes = (%q, %q), want empty queue-time hashes", ig.FromNodeHash, ig.ToNodeHash)
	}
}
