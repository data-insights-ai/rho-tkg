package core

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/io"
)

func TestR9_RegistryPersistsAfterSuccessfulWritesWithoutGraphClose(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	g, err := New(Config{BadgerDir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	a, err := g.Nodes.Add(context.Background(), []string{"CrashSafeLabel"}, nil)
	if err != nil {
		t.Fatalf("Add node a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"CrashSafeLabel"}, nil)
	if err != nil {
		t.Fatalf("Add node b: %v", err)
	}
	if _, err := g.Rels.Add(context.Background(), "CRASH_SAFE_REL", a, b, nil); err != nil {
		t.Fatalf("Add relationship: %v", err)
	}

	// Bypass Core.Close to model a process that completed successful writes
	// but did not get the graph-level close hook that used to be the only
	// registry persistence point.
	if err := g.store.Close(); err != nil {
		t.Fatalf("direct store Close: %v", err)
	}

	reopened, err := New(Config{BadgerDir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("reopen graph: %v", err)
	}
	defer reopened.Close()

	if _, ok := reopened.labels.Lookup("CrashSafeLabel"); !ok {
		t.Fatal("label registry lost CrashSafeLabel across direct store restart")
	}
	if _, ok := reopened.relTypes.Lookup("CRASH_SAFE_REL"); !ok {
		t.Fatal("reltype registry lost CRASH_SAFE_REL across direct store restart")
	}

	nodes, err := reopened.Nodes.ByLabel("CrashSafeLabel", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabel after reopen: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("ByLabel after reopen returned %d nodes, want 2", len(nodes))
	}
	rels, err := reopened.Rels.ByType("CRASH_SAFE_REL", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType after reopen: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("ByType after reopen returned %d relationships, want 1", len(rels))
	}
}

func TestR9_ResolveTokenCreationPersistsWithoutGraphClose(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	g, err := New(Config{BadgerDir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	labelTok, err := g.Resolve.GetOrCreateLabel("LookupOnlyLabel")
	if err != nil {
		t.Fatalf("GetOrCreateLabel: %v", err)
	}
	relTok, err := g.Resolve.GetOrCreateRelType("LOOKUP_ONLY_REL")
	if err != nil {
		t.Fatalf("GetOrCreateRelType: %v", err)
	}

	if err := g.store.Close(); err != nil {
		t.Fatalf("direct store Close: %v", err)
	}

	reopened, err := New(Config{BadgerDir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("reopen graph: %v", err)
	}
	defer reopened.Close()

	gotLabelTok, ok := reopened.Resolve.LookupLabel("LookupOnlyLabel")
	if !ok || gotLabelTok != labelTok {
		t.Fatalf("LookupLabel after direct store restart = %d, %v; want %d, true", gotLabelTok, ok, labelTok)
	}
	gotRelTok, ok := reopened.Resolve.LookupRelType("LOOKUP_ONLY_REL")
	if !ok || gotRelTok != relTok {
		t.Fatalf("LookupRelType after direct store restart = %d, %v; want %d, true", gotRelTok, ok, relTok)
	}
}

func TestR9_ResolveTokenCreationPersistFailureRollsBack(t *testing.T) {
	t.Parallel()

	labelStore := &registryPersistFailStore{Store: memory.New(), err: errInjectedRegistryPersist}
	g, err := New(Config{Store: labelStore})
	if err != nil {
		t.Fatalf("New label graph: %v", err)
	}
	if tok, err := g.Resolve.GetOrCreateLabel("LookupPersistFailLabel"); tok != 0 || !errors.Is(err, errInjectedRegistryPersist) {
		t.Fatalf("GetOrCreateLabel = (%d, %v), want (0, injected registry persist error)", tok, err)
	}
	if _, ok := g.Resolve.LookupLabel("LookupPersistFailLabel"); ok {
		t.Fatal("failed resolver label token creation kept in-memory registry token")
	}
	if labelStore.saveCalls == 0 {
		t.Fatal("GetOrCreateLabel did not attempt registry checkpoint")
	}

	relStore := &registryPersistFailStore{Store: memory.New(), err: errInjectedRegistryPersist}
	g, err = New(Config{Store: relStore})
	if err != nil {
		t.Fatalf("New rel graph: %v", err)
	}
	if tok, err := g.Resolve.GetOrCreateRelType("LOOKUP_PERSIST_FAIL_REL"); tok != 0 || !errors.Is(err, errInjectedRegistryPersist) {
		t.Fatalf("GetOrCreateRelType = (%d, %v), want (0, injected registry persist error)", tok, err)
	}
	if _, ok := g.Resolve.LookupRelType("LOOKUP_PERSIST_FAIL_REL"); ok {
		t.Fatal("failed resolver reltype token creation kept in-memory registry token")
	}
	if relStore.saveCalls == 0 {
		t.Fatal("GetOrCreateRelType did not attempt registry checkpoint")
	}
}

func TestR9_ImportedRegistryPersistsWithoutGraphClose(t *testing.T) {
	t.Parallel()

	src := newTestGraph(t)
	defer src.Close()
	a, err := src.Nodes.Add(context.Background(), []string{"ImportedCrashSafeLabel"}, nil)
	if err != nil {
		t.Fatalf("source add node a: %v", err)
	}
	b, err := src.Nodes.Add(context.Background(), []string{"ImportedCrashSafeLabel"}, nil)
	if err != nil {
		t.Fatalf("source add node b: %v", err)
	}
	if _, err := src.Rels.Add(context.Background(), "IMPORTED_CRASH_SAFE_REL", a, b, nil); err != nil {
		t.Fatalf("source add relationship: %v", err)
	}
	var stream bytes.Buffer
	if err := src.IO.Export(&stream); err != nil {
		t.Fatalf("Export: %v", err)
	}

	dir := t.TempDir()
	dst, err := New(Config{BadgerDir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("New dst: %v", err)
	}
	if err := dst.IO.Import(bytes.NewReader(stream.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	if err := dst.store.Close(); err != nil {
		t.Fatalf("direct dst store Close: %v", err)
	}

	reopened, err := New(Config{BadgerDir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("reopen imported graph: %v", err)
	}
	defer reopened.Close()

	nodes, err := reopened.Nodes.ByLabel("ImportedCrashSafeLabel", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabel after imported reopen: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("ByLabel after imported reopen returned %d nodes, want 2", len(nodes))
	}
	rels, err := reopened.Rels.ByType("IMPORTED_CRASH_SAFE_REL", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType after imported reopen: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("ByType after imported reopen returned %d relationships, want 1", len(rels))
	}
}

func TestR9_RegistryPersistFailureSurfacesAfterTokenCommit(t *testing.T) {
	t.Parallel()

	st := &registryPersistFailStore{Store: memory.New(), err: errInjectedRegistryPersist}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New label graph: %v", err)
	}
	node, err := g.Nodes.Add(context.Background(), []string{"PersistFailLabel"}, nil)
	if !errors.Is(err, errInjectedRegistryPersist) {
		t.Fatalf("Add node with registry persist failure = %v, want injected error", err)
	}
	if node == nil {
		t.Fatal("Add node returned nil node after committed registry persist failure")
	}
	if _, err := g.store.GetNode(node.ID()); err != nil {
		t.Fatalf("GetNode after committed registry persist failure: %v", err)
	}
	if stats, _ := g.Stats.Get(); stats.NodesAdded != 1 {
		t.Fatalf("NodesAdded after committed registry persist failure = %d, want 1", stats.NodesAdded)
	}

	st = &registryPersistFailStore{Store: memory.New()}
	g, err = New(Config{Store: st})
	if err != nil {
		t.Fatalf("New rel graph: %v", err)
	}
	a, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint b: %v", err)
	}
	st.err = errInjectedRegistryPersist
	rel, err := g.Rels.Add(context.Background(), "PERSIST_FAIL_REL", a, b, nil)
	if !errors.Is(err, errInjectedRegistryPersist) {
		t.Fatalf("Add rel with registry persist failure = %v, want injected error", err)
	}
	if rel == nil {
		t.Fatal("Add rel returned nil relationship after committed registry persist failure")
	}
	if _, err := g.store.GetRelationship(rel.ID()); err != nil {
		t.Fatalf("GetRelationship after committed registry persist failure: %v", err)
	}
	if stats, _ := g.Stats.Get(); stats.RelsAdded != 1 {
		t.Fatalf("RelsAdded after committed registry persist failure = %d, want 1", stats.RelsAdded)
	}
}

func TestR9_StandaloneCreateRegistryPersistFailurePublishesCreateEvents(t *testing.T) {
	t.Parallel()

	t.Run("node add", func(t *testing.T) {
		g, st, events := newRegistryPersistEventGraph(t)
		st.err = errInjectedRegistryPersist

		before := len(drain(events))
		n, err := g.Nodes.Add(context.Background(), []string{"EventPersistFailNodeAdd"}, nil)
		if !errors.Is(err, errInjectedRegistryPersist) {
			t.Fatalf("AddNode error = %v, want injected registry persist error", err)
		}
		if n == nil {
			t.Fatal("AddNode returned nil node after committed Store write")
		}
		requireOneNewCreateEvent(t, events, before, eventspkg.EventNodeCreate, types.EntityID(n.ID()))
	})

	t.Run("node import", func(t *testing.T) {
		g, st, events := newRegistryPersistEventGraph(t)
		st.err = errInjectedRegistryPersist

		id := g.nextNodeID()
		before := len(drain(events))
		n, err := g.Nodes.Import(context.Background(), id, []string{"EventPersistFailNodeImport"}, nil)
		if !errors.Is(err, errInjectedRegistryPersist) {
			t.Fatalf("ImportNode error = %v, want injected registry persist error", err)
		}
		if n == nil {
			t.Fatal("ImportNode returned nil node after committed Store write")
		}
		requireOneNewCreateEvent(t, events, before, eventspkg.EventNodeCreate, types.EntityID(n.ID()))
	})

	t.Run("relationship add", func(t *testing.T) {
		g, st, events := newRegistryPersistEventGraph(t)
		a, b := addRegistryPersistEventEndpoints(t, g)
		st.err = errInjectedRegistryPersist

		before := len(drain(events))
		r, err := g.Rels.Add(context.Background(), "EVENT_PERSIST_FAIL_REL_ADD", a, b, nil)
		if !errors.Is(err, errInjectedRegistryPersist) {
			t.Fatalf("AddRelationship error = %v, want injected registry persist error", err)
		}
		if r == nil {
			t.Fatal("AddRelationship returned nil relationship after committed Store write")
		}
		requireOneNewCreateEvent(t, events, before, eventspkg.EventRelCreate, types.EntityID(r.ID()))
	})

	t.Run("relationship add by id", func(t *testing.T) {
		g, st, events := newRegistryPersistEventGraph(t)
		a, b := addRegistryPersistEventEndpoints(t, g)
		st.err = errInjectedRegistryPersist

		before := len(drain(events))
		r, err := g.Rels.AddByID(context.Background(), "EVENT_PERSIST_FAIL_REL_BY_ID", a.ID(), b.ID(), nil)
		if !errors.Is(err, errInjectedRegistryPersist) {
			t.Fatalf("AddRelationshipByID error = %v, want injected registry persist error", err)
		}
		if r == nil {
			t.Fatal("AddRelationshipByID returned nil relationship after committed Store write")
		}
		requireOneNewCreateEvent(t, events, before, eventspkg.EventRelCreate, types.EntityID(r.ID()))
	})

	t.Run("relationship add by id if absent", func(t *testing.T) {
		g, st, events := newRegistryPersistEventGraph(t)
		a, b := addRegistryPersistEventEndpoints(t, g)
		st.err = errInjectedRegistryPersist

		before := len(drain(events))
		r, created, err := g.Rels.AddByIDIfAbsent(context.Background(), "EVENT_PERSIST_FAIL_REL_IF_ABSENT", a.ID(), b.ID(), nil)
		if !errors.Is(err, errInjectedRegistryPersist) {
			t.Fatalf("AddRelationshipByIDIfAbsent error = %v, want injected registry persist error", err)
		}
		if r == nil || !created {
			t.Fatalf("AddRelationshipByIDIfAbsent returned rel=%v created=%v after committed Store write", r, created)
		}
		requireOneNewCreateEvent(t, events, before, eventspkg.EventRelCreate, types.EntityID(r.ID()))
	})

	t.Run("relationship import", func(t *testing.T) {
		g, st, events := newRegistryPersistEventGraph(t)
		a, b := addRegistryPersistEventEndpoints(t, g)
		st.err = errInjectedRegistryPersist

		before := len(drain(events))
		r, err := g.Rels.Import(context.Background(), g.nextRelID(), "EVENT_PERSIST_FAIL_REL_IMPORT", a, b, nil)
		if !errors.Is(err, errInjectedRegistryPersist) {
			t.Fatalf("ImportRelationship error = %v, want injected registry persist error", err)
		}
		if r == nil {
			t.Fatal("ImportRelationship returned nil relationship after committed Store write")
		}
		requireOneNewCreateEvent(t, events, before, eventspkg.EventRelCreate, types.EntityID(r.ID()))
	})
}

func newRegistryPersistEventGraph(t *testing.T) (*Core, *registryPersistFailStore, *[]eventspkg.Event) {
	t.Helper()
	st := &registryPersistFailStore{Store: memory.New()}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}
	if err := g.Events.SetSync(eventspkg.NewEventBus()); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	events := collectEvents(g, eventspkg.EventNodeCreate)
	t.Cleanup(func() { _ = g.Close() })
	return g, st, events
}

func addRegistryPersistEventEndpoints(t *testing.T, g *Core) (*types.Node, *types.Node) {
	t.Helper()
	a, err := g.Nodes.Add(context.Background(), []string{"EventPersistFailEndpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"EventPersistFailEndpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint b: %v", err)
	}
	return a, b
}

func requireOneNewCreateEvent(t *testing.T, events *[]eventspkg.Event, before int, typ eventspkg.EventType, id types.EntityID) {
	t.Helper()
	got := drain(events)
	if len(got) != before+1 {
		t.Fatalf("new events = %d, want 1; all events: %+v", len(got)-before, got)
	}
	ev := got[before]
	if ev.Type != typ || ev.EntityID != id {
		t.Fatalf("event = %+v, want type=%v id=%d", ev, typ, id)
	}
}

func TestR9_DirtyRegistryRetryBeforeExistingTokenWriteSuccess(t *testing.T) {
	t.Parallel()

	t.Run("node add with existing dirty label", func(t *testing.T) {
		st := &registryPersistFailStore{Store: memory.New(), err: errInjectedRegistryPersist}
		g, err := New(Config{Store: st})
		if err != nil {
			t.Fatalf("New graph: %v", err)
		}
		defer g.Close()

		first, err := g.Nodes.Add(context.Background(), []string{"DirtyRetryLabel"}, nil)
		if !errors.Is(err, errInjectedRegistryPersist) {
			t.Fatalf("first AddNode error = %v, want injected registry persist error", err)
		}
		if first == nil {
			t.Fatal("first AddNode returned nil node after committed Store write")
		}
		callsAfterFailure := st.saveCalls

		st.err = nil
		second, err := g.Nodes.Add(context.Background(), []string{"DirtyRetryLabel"}, nil)
		if err != nil {
			t.Fatalf("second AddNode with existing dirty label: %v", err)
		}
		if second == nil {
			t.Fatal("second AddNode returned nil node")
		}
		if st.saveCalls <= callsAfterFailure {
			t.Fatalf("SaveRegistries calls = %d, want retry after dirty failure > %d", st.saveCalls, callsAfterFailure)
		}
	})

	t.Run("add label with existing dirty label", func(t *testing.T) {
		st := &registryPersistFailStore{Store: memory.New()}
		g, err := New(Config{Store: st})
		if err != nil {
			t.Fatalf("New graph: %v", err)
		}
		defer g.Close()

		target, err := g.Nodes.Add(context.Background(), []string{"DirtyRetryBase"}, nil)
		if err != nil {
			t.Fatalf("Add target node: %v", err)
		}

		st.err = errInjectedRegistryPersist
		source, err := g.Nodes.Add(context.Background(), []string{"DirtyRetryAttached"}, nil)
		if !errors.Is(err, errInjectedRegistryPersist) {
			t.Fatalf("AddNode dirty label error = %v, want injected registry persist error", err)
		}
		if source == nil {
			t.Fatal("AddNode dirty label returned nil node after committed Store write")
		}
		callsAfterFailure := st.saveCalls

		st.err = nil
		if err := g.Nodes.AddLabel(target.ID(), "DirtyRetryAttached"); err != nil {
			t.Fatalf("AddLabel with existing dirty label: %v", err)
		}
		if st.saveCalls <= callsAfterFailure {
			t.Fatalf("SaveRegistries calls = %d, want retry before AddLabel success > %d", st.saveCalls, callsAfterFailure)
		}
	})

	t.Run("remove label with existing dirty label", func(t *testing.T) {
		st := &registryPersistFailStore{Store: memory.New(), err: errInjectedRegistryPersist}
		g, err := New(Config{Store: st})
		if err != nil {
			t.Fatalf("New graph: %v", err)
		}
		defer g.Close()

		n, err := g.Nodes.Add(context.Background(), []string{"DirtyRetryRemoveBase", "DirtyRetryRemoveAttached"}, nil)
		if !errors.Is(err, errInjectedRegistryPersist) {
			t.Fatalf("AddNode dirty label error = %v, want injected registry persist error", err)
		}
		if n == nil {
			t.Fatal("AddNode dirty label returned nil node after committed Store write")
		}
		callsAfterFailure := st.saveCalls

		st.err = nil
		if err := g.Nodes.RemoveLabel(n.ID(), "DirtyRetryRemoveAttached"); err != nil {
			t.Fatalf("RemoveLabel with existing dirty label: %v", err)
		}
		if st.saveCalls <= callsAfterFailure {
			t.Fatalf("SaveRegistries calls = %d, want retry before RemoveLabel success > %d", st.saveCalls, callsAfterFailure)
		}
		updated, err := g.Nodes.Get(context.Background(), n.ID())
		if err != nil {
			t.Fatalf("Get updated node: %v", err)
		}
		if g.Nodes.HasLabel(updated, "DirtyRetryRemoveAttached") {
			t.Fatal("RemoveLabel left removed label attached")
		}
		if !g.Nodes.HasLabel(updated, "DirtyRetryRemoveBase") {
			t.Fatal("RemoveLabel removed the wrong label")
		}
	})

	t.Run("relationship add with existing dirty type", func(t *testing.T) {
		st := &registryPersistFailStore{Store: memory.New()}
		g, err := New(Config{Store: st})
		if err != nil {
			t.Fatalf("New graph: %v", err)
		}
		defer g.Close()

		a, err := g.Nodes.Add(context.Background(), []string{"DirtyRetryEndpoint"}, nil)
		if err != nil {
			t.Fatalf("Add endpoint a: %v", err)
		}
		b, err := g.Nodes.Add(context.Background(), []string{"DirtyRetryEndpoint"}, nil)
		if err != nil {
			t.Fatalf("Add endpoint b: %v", err)
		}

		st.err = errInjectedRegistryPersist
		first, err := g.Rels.Add(context.Background(), "DIRTY_RETRY_REL", a, b, nil)
		if !errors.Is(err, errInjectedRegistryPersist) {
			t.Fatalf("first AddRelationship error = %v, want injected registry persist error", err)
		}
		if first == nil {
			t.Fatal("first AddRelationship returned nil relationship after committed Store write")
		}
		callsAfterFailure := st.saveCalls

		st.err = nil
		second, err := g.Rels.Add(context.Background(), "DIRTY_RETRY_REL", a, b, nil)
		if err != nil {
			t.Fatalf("second AddRelationship with existing dirty type: %v", err)
		}
		if second == nil {
			t.Fatal("second AddRelationship returned nil relationship")
		}
		if st.saveCalls <= callsAfterFailure {
			t.Fatalf("SaveRegistries calls = %d, want retry after dirty reltype failure > %d", st.saveCalls, callsAfterFailure)
		}
	})
}

func TestR9_RemoveLabelDirtyRegistryCheckpointFailureDoesNotMutate(t *testing.T) {
	t.Parallel()

	st := &registryPersistFailStore{Store: memory.New(), err: errInjectedRegistryPersist}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}
	defer g.Close()

	n, err := g.Nodes.Add(context.Background(), []string{"DirtyRemoveFailureBase", "DirtyRemoveFailureAttached"}, nil)
	if !errors.Is(err, errInjectedRegistryPersist) {
		t.Fatalf("AddNode dirty label error = %v, want injected registry persist error", err)
	}
	if n == nil {
		t.Fatal("AddNode dirty label returned nil node after committed Store write")
	}
	callsAfterFailure := st.saveCalls
	beforeStats, _ := g.Stats.Get()

	err = g.Nodes.RemoveLabel(n.ID(), "DirtyRemoveFailureAttached")
	if !errors.Is(err, errInjectedRegistryPersist) {
		t.Fatalf("RemoveLabel dirty registry failure = %v, want injected registry persist error", err)
	}
	if st.saveCalls <= callsAfterFailure {
		t.Fatalf("SaveRegistries calls = %d, want retry before RemoveLabel failure > %d", st.saveCalls, callsAfterFailure)
	}
	updated, err := g.Nodes.Get(context.Background(), n.ID())
	if err != nil {
		t.Fatalf("Get updated node: %v", err)
	}
	if !g.Nodes.HasLabel(updated, "DirtyRemoveFailureAttached") {
		t.Fatal("RemoveLabel mutated node after registry checkpoint failure")
	}
	afterStats, _ := g.Stats.Get()
	if afterStats.NodesUpdated != beforeStats.NodesUpdated {
		t.Fatalf("NodesUpdated after failed RemoveLabel = %d, want %d", afterStats.NodesUpdated, beforeStats.NodesUpdated)
	}
}

func TestR9_DirtyRegistryCheckpointFailureBlocksExistingTokenMutations(t *testing.T) {
	t.Parallel()

	nodeCases := []struct {
		name   string
		mutate func(*Core, *types.Node) error
	}{
		{
			name: "node update",
			mutate: func(g *Core, n *types.Node) error {
				_, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"p": "new"})
				return err
			},
		},
		{
			name: "node update in place",
			mutate: func(g *Core, n *types.Node) error {
				_, err := g.Nodes.UpdateInPlace(context.Background(), n.ID(), map[string]any{"p": "new"})
				return err
			},
		},
		{
			name: "node property cas",
			mutate: func(g *Core, n *types.Node) error {
				ok, err := g.Nodes.CompareAndSetProperty(context.Background(), n.ID(), "p", "old", "new")
				if err == nil && !ok {
					return errors.New("node CAS did not match")
				}
				return err
			},
		},
		{
			name: "node close version",
			mutate: func(g *Core, n *types.Node) error {
				return g.Nodes.CloseVersion(n.ID(), g.now()+1000)
			},
		},
		{
			name: "node delete",
			mutate: func(g *Core, n *types.Node) error {
				return g.Nodes.Delete(context.Background(), n.ID())
			},
		},
	}

	for _, tc := range nodeCases {
		t.Run(tc.name, func(t *testing.T) {
			g, st, n := newDirtyNodeForExistingTokenMutation(t)
			defer g.Close()
			callsAfterFailure := st.saveCalls

			err := tc.mutate(g, n)
			if !errors.Is(err, errInjectedRegistryPersist) {
				t.Fatalf("%s dirty registry failure = %v, want injected registry persist error", tc.name, err)
			}
			if st.saveCalls <= callsAfterFailure {
				t.Fatalf("SaveRegistries calls = %d, want retry before %s failure > %d", st.saveCalls, tc.name, callsAfterFailure)
			}
			assertDirtyNodeMutationBlocked(t, g, n.ID())
		})
	}

	relCases := []struct {
		name   string
		mutate func(*Core, *types.Relationship) error
	}{
		{
			name: "relationship update",
			mutate: func(g *Core, r *types.Relationship) error {
				_, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"p": "new"})
				return err
			},
		},
		{
			name: "relationship update in place",
			mutate: func(g *Core, r *types.Relationship) error {
				_, err := g.Rels.UpdateInPlace(context.Background(), r.ID(), map[string]any{"p": "new"})
				return err
			},
		},
		{
			name: "relationship property cas",
			mutate: func(g *Core, r *types.Relationship) error {
				ok, err := g.Rels.CompareAndSetProperty(context.Background(), r.ID(), "p", "old", "new")
				if err == nil && !ok {
					return errors.New("relationship CAS did not match")
				}
				return err
			},
		},
		{
			name: "relationship close version",
			mutate: func(g *Core, r *types.Relationship) error {
				return g.Rels.CloseVersion(r.ID(), g.now()+1000)
			},
		},
		{
			name: "relationship delete",
			mutate: func(g *Core, r *types.Relationship) error {
				return g.Rels.Delete(context.Background(), r.ID())
			},
		},
	}

	for _, tc := range relCases {
		t.Run(tc.name, func(t *testing.T) {
			g, st, r := newDirtyRelForExistingTokenMutation(t)
			defer g.Close()
			callsAfterFailure := st.saveCalls

			err := tc.mutate(g, r)
			if !errors.Is(err, errInjectedRegistryPersist) {
				t.Fatalf("%s dirty registry failure = %v, want injected registry persist error", tc.name, err)
			}
			if st.saveCalls <= callsAfterFailure {
				t.Fatalf("SaveRegistries calls = %d, want retry before %s failure > %d", st.saveCalls, tc.name, callsAfterFailure)
			}
			assertDirtyRelMutationBlocked(t, g, r.ID())
		})
	}
}

func TestR9_TxDirtyRegistryCheckpointFailureBlocksLaterMutation(t *testing.T) {
	t.Parallel()

	st := &registryPersistFailStore{Store: memory.New(), err: errInjectedRegistryPersist}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}
	defer g.Close()

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	n, err := tx.AddNode([]string{"TxDirtyLaterMutationLabel"}, map[string]any{"p": "old"})
	if !errors.Is(err, errInjectedRegistryPersist) {
		t.Fatalf("tx AddNode dirty label error = %v, want injected registry persist error", err)
	}
	if n == nil {
		t.Fatal("tx AddNode dirty label returned nil node after committed Store write")
	}
	callsAfterFailure := st.saveCalls

	_, err = tx.UpdateNode(n.ID(), map[string]any{"p": "new"})
	if !errors.Is(err, errInjectedRegistryPersist) {
		t.Fatalf("tx UpdateNode dirty registry failure = %v, want injected registry persist error", err)
	}
	if st.saveCalls <= callsAfterFailure {
		t.Fatalf("SaveRegistries calls = %d, want retry before tx UpdateNode failure > %d", st.saveCalls, callsAfterFailure)
	}
	got, err := tx.GetNode(n.ID())
	if err != nil {
		t.Fatalf("tx GetNode after blocked update: %v", err)
	}
	val, ok := got.GetProperty("p")
	if !ok || val != "old" {
		t.Fatalf("node property after blocked tx update = (%v, %v), want (old, true)", val, ok)
	}

	st.err = nil
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if _, err := g.store.GetNode(n.ID()); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("GetNode after rollback = %v, want ErrNodeNotFound", err)
	}
	if _, ok := g.labels.Lookup("TxDirtyLaterMutationLabel"); ok {
		t.Fatal("rollback kept dirty label token")
	}
}

func TestR9_BatchDirtyRegistryCheckpointFailureBlocksLaterMutation(t *testing.T) {
	t.Parallel()

	st := &registryPersistFailStore{Store: memory.New(), err: errInjectedRegistryPersist}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}
	defer g.Close()

	batch, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	n, err := batch.AddNode([]string{"BatchDirtyLaterMutationLabel"}, map[string]any{"p": "old"})
	if err != nil {
		t.Fatalf("AddNode queue: %v", err)
	}
	if err := batch.UpdateNode(n.ID(), map[string]any{"p": "new"}); err != nil {
		t.Fatalf("UpdateNode queue: %v", err)
	}

	result, err := batch.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", err)
	}
	if result == nil || result.Failed != 2 || len(result.Errors) != 2 {
		t.Fatalf("Execute result = %+v, want two failed operations", result)
	}
	var sawAdd, sawUpdate bool
	for _, batchErr := range result.Errors {
		switch batchErr.Op {
		case "AddNode":
			sawAdd = true
			if !errors.Is(batchErr.Err, errInjectedRegistryPersist) {
				t.Fatalf("AddNode batch error = %v, want injected registry persist error", batchErr.Err)
			}
		case "UpdateNode":
			sawUpdate = true
			if !errors.Is(batchErr.Err, errInjectedRegistryPersist) {
				t.Fatalf("UpdateNode batch error = %v, want injected registry persist error", batchErr.Err)
			}
		default:
			t.Fatalf("unexpected batch error op %q: %v", batchErr.Op, batchErr.Err)
		}
	}
	if !sawAdd || !sawUpdate {
		t.Fatalf("batch errors saw AddNode=%v UpdateNode=%v, want both", sawAdd, sawUpdate)
	}
	assertDirtyNodeMutationBlocked(t, g, n.ID())
}

func TestR9_BatchNodeRegistryPersistFailureKeepsCommittedNodeShape(t *testing.T) {
	t.Parallel()

	st := &registryPersistFailStore{Store: memory.New(), err: errInjectedRegistryPersist}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}
	defer g.Close()
	if err := g.Events.SetSync(eventspkg.NewEventBus()); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	events := collectEvents(g, eventspkg.EventNodeCreate)

	b, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	n, err := b.AddNode([]string{"BatchPersistFailLabel"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	before := len(drain(events))
	result, err := b.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", err)
	}
	if result == nil || result.Created != 1 || result.Failed != 1 || len(result.Errors) != 1 {
		t.Fatalf("Execute result = %+v, want Created=1 Failed=1", result)
	}
	if !errors.Is(result.Errors[0].Err, errInjectedRegistryPersist) {
		t.Fatalf("batch node error = %v, want injected registry persist error", result.Errors[0].Err)
	}
	if n.Temporal() == nil || n.Temporal().TxFrom == 0 {
		t.Fatal("batch node pointer lost committed TxFrom after post-write registry persist failure")
	}
	stored, err := g.store.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode after post-write registry persist failure: %v", err)
	}
	if stored.Temporal() == nil || stored.Temporal().TxFrom == 0 {
		t.Fatal("stored batch node lost committed TxFrom after post-write registry persist failure")
	}
	if stats, _ := g.Stats.Get(); stats.NodesAdded != 1 {
		t.Fatalf("NodesAdded after batch registry checkpoint failure = %d, want 1", stats.NodesAdded)
	}
	requireOneNewCreateEvent(t, events, before, eventspkg.EventNodeCreate, types.EntityID(n.ID()))
}

func TestR9_BatchRelationshipUsesCommittedNodesAfterTransientNodeRegistryPersistFailure(t *testing.T) {
	t.Parallel()

	st := &registryPersistFailFirstStore{Store: memory.New(), err: errInjectedRegistryPersist}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}
	defer g.Close()

	batch, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	a, err := batch.AddNode([]string{"BatchTransientPersistEndpoint"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := batch.AddNode([]string{"BatchTransientPersistEndpoint"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	rel, err := batch.AddRelationship("BATCH_TRANSIENT_PERSIST_REL", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	result, err := batch.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed from node registry checkpoint", err)
	}
	if result == nil || result.Created != 3 || result.Failed != 2 {
		t.Fatalf("Execute result = %+v, want Created=3 Failed=2", result)
	}
	for _, batchErr := range result.Errors {
		if batchErr.Op == "AddRelationship" {
			t.Fatalf("relationship was skipped despite committed endpoints: %v", batchErr.Err)
		}
	}
	if rel.Temporal() == nil || rel.Temporal().TxFrom == 0 {
		t.Fatal("batch relationship was not finalised after committed endpoints")
	}
	if _, err := g.store.GetRelationship(rel.ID()); err != nil {
		t.Fatalf("GetRelationship after transient node registry checkpoint failure: %v", err)
	}
	if st.saveCalls < 2 {
		t.Fatalf("SaveRegistries calls = %d, want retry during relationship type checkpoint", st.saveCalls)
	}
}

func TestR9_BatchRelRegistryPersistFailureKeepsCommittedRelationshipShape(t *testing.T) {
	t.Parallel()

	st := &registryPersistFailStore{Store: memory.New()}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint a: %v", err)
	}
	bNode, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint b: %v", err)
	}
	if err := g.Events.SetSync(eventspkg.NewEventBus()); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	events := collectEvents(g, eventspkg.EventRelCreate)

	st.err = errInjectedRegistryPersist
	batch, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	rel, err := batch.AddRelationship("BATCH_PERSIST_FAIL_REL", a, bNode, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	before := len(drain(events))
	result, err := batch.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", err)
	}
	if result == nil || result.Created != 1 || result.Failed != 1 || len(result.Errors) != 1 {
		t.Fatalf("Execute result = %+v, want Created=1 Failed=1", result)
	}
	if !errors.Is(result.Errors[0].Err, errInjectedRegistryPersist) {
		t.Fatalf("batch rel error = %v, want injected registry persist error", result.Errors[0].Err)
	}
	if rel.Temporal() == nil || rel.Temporal().TxFrom == 0 {
		t.Fatal("batch relationship pointer lost committed TxFrom after post-write registry persist failure")
	}
	relIntegrity := rel.Integrity()
	if relIntegrity == nil || relIntegrity.FromNodeHash == "" || relIntegrity.ToNodeHash == "" {
		t.Fatal("batch relationship pointer lost committed endpoint hashes after post-write registry persist failure")
	}
	stored, err := g.store.GetRelationship(rel.ID())
	if err != nil {
		t.Fatalf("GetRelationship after post-write registry persist failure: %v", err)
	}
	if stored.Temporal() == nil || stored.Temporal().TxFrom == 0 {
		t.Fatal("stored batch relationship lost committed TxFrom after post-write registry persist failure")
	}
	storedIntegrity := stored.Integrity()
	if storedIntegrity == nil || storedIntegrity.FromNodeHash == "" || storedIntegrity.ToNodeHash == "" {
		t.Fatal("stored batch relationship lost endpoint hashes after post-write registry persist failure")
	}
	if stats, _ := g.Stats.Get(); stats.RelsAdded != 1 {
		t.Fatalf("RelsAdded after batch registry checkpoint failure = %d, want 1", stats.RelsAdded)
	}
	requireOneNewCreateEvent(t, events, before, eventspkg.EventRelCreate, types.EntityID(rel.ID()))
}

func TestR9_TxNodeRegistryPersistFailureStillRollsBackCommittedRows(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		create func(*GraphTx) (*types.Node, error)
		label  string
	}{
		{
			name: "add node",
			create: func(tx *GraphTx) (*types.Node, error) {
				return tx.AddNode([]string{"TxPersistFailNode"}, nil)
			},
			label: "TxPersistFailNode",
		},
		{
			name: "import node",
			create: func(tx *GraphTx) (*types.Node, error) {
				return tx.ImportNodeWithID(context.Background(), types.NodeID(987654321), []string{"TxPersistFailImportNode"}, nil)
			},
			label: "TxPersistFailImportNode",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &registryPersistFailStore{Store: memory.New()}
			g, err := New(Config{Store: st})
			if err != nil {
				t.Fatalf("New graph: %v", err)
			}
			defer g.Close()

			tx, err := g.BeginTx()
			if err != nil {
				t.Fatalf("BeginTx: %v", err)
			}
			st.err = errInjectedRegistryPersist
			n, err := tc.create(tx)
			if !errors.Is(err, errInjectedRegistryPersist) {
				t.Fatalf("%s error = %v, want injected registry persist error", tc.name, err)
			}
			if n == nil {
				t.Fatalf("%s returned nil node after committed Store write", tc.name)
			}
			if got := tx.CreatedNodeIDs(); len(got) != 1 || got[0] != n.ID() {
				t.Fatalf("CreatedNodeIDs after %s = %v, want [%d]", tc.name, got, n.ID())
			}

			st.err = nil
			if err := tx.Rollback(); err != nil {
				t.Fatalf("Rollback: %v", err)
			}
			if _, err := g.store.GetNode(n.ID()); !errors.Is(err, storepkg.ErrNodeNotFound) {
				t.Fatalf("GetNode after rollback = %v, want ErrNodeNotFound", err)
			}
			if _, ok := g.labels.Lookup(tc.label); ok {
				t.Fatalf("rollback kept label token %q after failed tx create", tc.label)
			}
		})
	}
}

func TestR9_TxRelRegistryPersistFailureStillRollsBackCommittedRows(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		create func(*GraphTx, *types.Node, *types.Node) (*types.Relationship, error)
		typ    string
	}{
		{
			name: "add relationship",
			create: func(tx *GraphTx, a, b *types.Node) (*types.Relationship, error) {
				return tx.AddRelationship("TX_PERSIST_FAIL_REL", a, b, nil)
			},
			typ: "TX_PERSIST_FAIL_REL",
		},
		{
			name: "add relationship by id",
			create: func(tx *GraphTx, a, b *types.Node) (*types.Relationship, error) {
				return tx.AddRelationshipByID("TX_PERSIST_FAIL_REL_BY_ID", a.ID(), b.ID(), nil)
			},
			typ: "TX_PERSIST_FAIL_REL_BY_ID",
		},
		{
			name: "add relationship by id if absent",
			create: func(tx *GraphTx, a, b *types.Node) (*types.Relationship, error) {
				r, created, err := tx.AddRelationshipByIDIfAbsent("TX_PERSIST_FAIL_REL_IF_ABSENT", a.ID(), b.ID(), nil)
				if !created && err == nil {
					return nil, errors.New("expected relationship creation")
				}
				return r, err
			},
			typ: "TX_PERSIST_FAIL_REL_IF_ABSENT",
		},
		{
			name: "import relationship",
			create: func(tx *GraphTx, a, b *types.Node) (*types.Relationship, error) {
				return tx.ImportRelationshipWithID(context.Background(), types.RelID(a.ID().SnowflakeID()), "TX_PERSIST_FAIL_REL_IMPORT", a, b, nil)
			},
			typ: "TX_PERSIST_FAIL_REL_IMPORT",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &registryPersistFailStore{Store: memory.New()}
			g, err := New(Config{Store: st})
			if err != nil {
				t.Fatalf("New graph: %v", err)
			}
			defer g.Close()

			a, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
			if err != nil {
				t.Fatalf("Add endpoint a: %v", err)
			}
			b, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
			if err != nil {
				t.Fatalf("Add endpoint b: %v", err)
			}
			tx, err := g.BeginTx()
			if err != nil {
				t.Fatalf("BeginTx: %v", err)
			}
			st.err = errInjectedRegistryPersist
			r, err := tc.create(tx, a, b)
			if !errors.Is(err, errInjectedRegistryPersist) {
				t.Fatalf("%s error = %v, want injected registry persist error", tc.name, err)
			}
			if r == nil {
				t.Fatalf("%s returned nil relationship after committed Store write", tc.name)
			}
			if got := tx.CreatedRelIDs(); len(got) != 1 || got[0] != r.ID() {
				t.Fatalf("CreatedRelIDs after %s = %v, want [%d]", tc.name, got, r.ID())
			}

			st.err = nil
			if err := tx.Rollback(); err != nil {
				t.Fatalf("Rollback: %v", err)
			}
			if _, err := g.store.GetRelationship(r.ID()); !errors.Is(err, storepkg.ErrRelNotFound) {
				t.Fatalf("GetRelationship after rollback = %v, want ErrRelNotFound", err)
			}
			if _, ok := g.relTypes.Lookup(tc.typ); ok {
				t.Fatalf("rollback kept relationship type token %q after failed tx create", tc.typ)
			}
		})
	}
}

func TestR9_TxCommitRetriesRegistryCheckpointAfterCreateError(t *testing.T) {
	t.Parallel()

	st := &registryPersistFailStore{Store: memory.New()}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}
	defer g.Close()

	beforeStats, _ := g.Stats.Get()
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	st.err = errInjectedRegistryPersist
	n, err := tx.AddNode([]string{"TxCommitRetryLabel"}, nil)
	if !errors.Is(err, errInjectedRegistryPersist) {
		t.Fatalf("tx AddNode error = %v, want injected registry persist error", err)
	}
	if n == nil {
		t.Fatal("tx AddNode returned nil node after committed Store write")
	}
	saveCalls := st.saveCalls

	st.err = nil
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit after registry recovery: %v", err)
	}
	if st.saveCalls != saveCalls+1 {
		t.Fatalf("Commit SaveRegistries calls = %d, want %d", st.saveCalls, saveCalls+1)
	}
	if _, err := g.store.GetNode(n.ID()); err != nil {
		t.Fatalf("GetNode after Commit: %v", err)
	}
	if _, ok := g.labels.Lookup("TxCommitRetryLabel"); !ok {
		t.Fatal("Commit lost label token after retrying registry checkpoint")
	}
	afterStats, _ := g.Stats.Get()
	if afterStats.NodesAdded != beforeStats.NodesAdded+1 {
		t.Fatalf("NodesAdded after Commit retry = %d, want %d", afterStats.NodesAdded, beforeStats.NodesAdded+1)
	}
}

func TestR9_TxRelCommitRetryCountsCommittedCreateAfterRegistryCheckpointError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		create func(*GraphTx, *types.Node, *types.Node, types.RelID) (*types.Relationship, error)
	}{
		{
			name: "add relationship",
			create: func(tx *GraphTx, a, b *types.Node, _ types.RelID) (*types.Relationship, error) {
				return tx.AddRelationship("TX_COMMIT_RETRY_REL", a, b, nil)
			},
		},
		{
			name: "add relationship by id",
			create: func(tx *GraphTx, a, b *types.Node, _ types.RelID) (*types.Relationship, error) {
				return tx.AddRelationshipByID("TX_COMMIT_RETRY_REL_BY_ID", a.ID(), b.ID(), nil)
			},
		},
		{
			name: "add relationship by id if absent",
			create: func(tx *GraphTx, a, b *types.Node, _ types.RelID) (*types.Relationship, error) {
				r, created, err := tx.AddRelationshipByIDIfAbsent("TX_COMMIT_RETRY_REL_IF_ABSENT", a.ID(), b.ID(), nil)
				if err == nil && !created {
					return nil, errors.New("expected relationship creation")
				}
				return r, err
			},
		},
		{
			name: "import relationship",
			create: func(tx *GraphTx, a, b *types.Node, id types.RelID) (*types.Relationship, error) {
				return tx.ImportRelationshipWithID(context.Background(), id, "TX_COMMIT_RETRY_REL_IMPORT", a, b, nil)
			},
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &registryPersistFailStore{Store: memory.New()}
			g, err := New(Config{Store: st})
			if err != nil {
				t.Fatalf("New graph: %v", err)
			}
			defer g.Close()

			a, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
			if err != nil {
				t.Fatalf("Add endpoint a: %v", err)
			}
			b, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
			if err != nil {
				t.Fatalf("Add endpoint b: %v", err)
			}
			beforeStats, _ := g.Stats.Get()

			tx, err := g.BeginTx()
			if err != nil {
				t.Fatalf("BeginTx: %v", err)
			}
			st.err = errInjectedRegistryPersist
			r, err := tc.create(tx, a, b, types.RelID(900000000+i))
			if !errors.Is(err, errInjectedRegistryPersist) {
				t.Fatalf("%s error = %v, want injected registry persist error", tc.name, err)
			}
			if r == nil {
				t.Fatalf("%s returned nil relationship after committed Store write", tc.name)
			}

			st.err = nil
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit after registry recovery: %v", err)
			}
			if _, err := g.store.GetRelationship(r.ID()); err != nil {
				t.Fatalf("GetRelationship after Commit: %v", err)
			}
			afterStats, _ := g.Stats.Get()
			if afterStats.RelsAdded != beforeStats.RelsAdded+1 {
				t.Fatalf("RelsAdded after Commit retry = %d, want %d", afterStats.RelsAdded, beforeStats.RelsAdded+1)
			}
		})
	}
}

func TestR9_TxCommitRegistryPersistFailureLeavesTransactionRollbackable(t *testing.T) {
	t.Parallel()

	st := &registryPersistFailStore{Store: memory.New()}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}
	defer g.Close()

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	st.err = errInjectedRegistryPersist
	n, err := tx.AddNode([]string{"TxCommitFailRollbackLabel"}, nil)
	if !errors.Is(err, errInjectedRegistryPersist) {
		t.Fatalf("tx AddNode error = %v, want injected registry persist error", err)
	}
	if n == nil {
		t.Fatal("tx AddNode returned nil node after committed Store write")
	}
	if err := tx.Commit(); !errors.Is(err, errInjectedRegistryPersist) {
		t.Fatalf("Commit with registry failure = %v, want injected registry persist error", err)
	}

	st.err = nil
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback after failed Commit: %v", err)
	}
	if _, err := g.store.GetNode(n.ID()); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("GetNode after rollback = %v, want ErrNodeNotFound", err)
	}
	if _, ok := g.labels.Lookup("TxCommitFailRollbackLabel"); ok {
		t.Fatal("rollback kept label token after failed Commit")
	}
}

func TestR9_RegistryRollbackPersistsRestoredSnapshots(t *testing.T) {
	t.Parallel()

	nodeStore := &putNodeFailRegistryStore{Store: memory.New()}
	g, err := New(Config{Store: nodeStore})
	if err != nil {
		t.Fatalf("New node graph: %v", err)
	}
	if _, err := g.Nodes.Add(context.Background(), []string{"RollbackPersistLabel"}, nil); !errors.Is(err, errInjectedNodePut) {
		t.Fatalf("Add node with failing store = %v, want injected put error", err)
	}
	if nodeStore.saveCalls != 1 {
		t.Fatalf("node rollback SaveRegistries calls = %d, want 1", nodeStore.saveCalls)
	}
	if _, ok := g.labels.Lookup("RollbackPersistLabel"); ok {
		t.Fatal("failed node create kept rolled-back label")
	}

	relStore := &putRelFailRegistryStore{Store: memory.New()}
	g, err = New(Config{Store: relStore})
	if err != nil {
		t.Fatalf("New rel graph: %v", err)
	}
	a, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint b: %v", err)
	}
	relStore.saveCalls = 0
	if _, err := g.Rels.Add(context.Background(), "ROLLBACK_PERSIST_REL", a, b, nil); !errors.Is(err, errInjectedRelPut) {
		t.Fatalf("Add rel with failing store = %v, want injected put error", err)
	}
	if relStore.saveCalls != 1 {
		t.Fatalf("rel rollback SaveRegistries calls = %d, want 1", relStore.saveCalls)
	}
	if _, ok := g.relTypes.Lookup("ROLLBACK_PERSIST_REL"); ok {
		t.Fatal("failed rel create kept rolled-back reltype")
	}
}

func TestR9_RegistryRollbackReportsPersistFailure(t *testing.T) {
	t.Parallel()

	nodeStore := &putNodeFailRegistryStore{Store: memory.New(), saveErr: errInjectedRegistryPersist}
	g, err := New(Config{Store: nodeStore})
	if err != nil {
		t.Fatalf("New node graph: %v", err)
	}
	_, err = g.Nodes.Add(context.Background(), []string{"RollbackPersistFailureLabel"}, nil)
	if !errors.Is(err, errInjectedNodePut) {
		t.Fatalf("Add node with failing store = %v, want injected put error", err)
	}
	if !strings.Contains(err.Error(), "failed to persist restored label registry") {
		t.Fatalf("Add node error %q does not report restored registry persist failure", err)
	}

	relStore := &putRelFailRegistryStore{Store: memory.New()}
	g, err = New(Config{Store: relStore})
	if err != nil {
		t.Fatalf("New rel graph: %v", err)
	}
	a, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint b: %v", err)
	}
	relStore.saveErr = errInjectedRegistryPersist
	_, err = g.Rels.Add(context.Background(), "ROLLBACK_PERSIST_FAILURE_REL", a, b, nil)
	if !errors.Is(err, errInjectedRelPut) {
		t.Fatalf("Add rel with failing store = %v, want injected put error", err)
	}
	if !strings.Contains(err.Error(), "failed to persist restored reltype registry") {
		t.Fatalf("Add rel error %q does not report restored registry persist failure", err)
	}
}

func TestR9_RegistryRollbackChangedSnapshotLeavesRegistryUntouched(t *testing.T) {
	t.Parallel()

	g := newTestGraph(t)
	defer g.Close()
	labelSnapshot := g.labels.ExportNames()
	if _, err := g.labels.GetOrCreate("ActualLabel"); err != nil {
		t.Fatalf("GetOrCreate label: %v", err)
	}
	g.registryMu.Lock()
	err := g.restoreNewLabelsOnError(labelSnapshot, []string{"DifferentLabel"}, errInjectedNodePut)
	if !errors.Is(err, errInjectedNodePut) {
		t.Fatalf("restoreNewLabelsOnError = %v, want original error", err)
	}
	if _, ok := g.labels.Lookup("ActualLabel"); !ok {
		t.Fatal("mismatched label rollback removed unrelated registry change")
	}

	relSnapshot := g.relTypes.ExportNames()
	if _, err := g.relTypes.GetOrCreate("ACTUAL_REL"); err != nil {
		t.Fatalf("GetOrCreate reltype: %v", err)
	}
	g.registryMu.Lock()
	err = g.restoreNewRelTypeOnError(relSnapshot, true, "DIFFERENT_REL", errInjectedRelPut)
	if !errors.Is(err, errInjectedRelPut) {
		t.Fatalf("restoreNewRelTypeOnError = %v, want original error", err)
	}
	if _, ok := g.relTypes.Lookup("ACTUAL_REL"); !ok {
		t.Fatal("mismatched reltype rollback removed unrelated registry change")
	}
}

func newDirtyNodeForExistingTokenMutation(t *testing.T) (*Core, *registryPersistFailStore, *types.Node) {
	t.Helper()

	st := &registryPersistFailStore{Store: memory.New(), err: errInjectedRegistryPersist}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}

	n, err := g.Nodes.Add(context.Background(), []string{"DirtyExistingNodeLabel"}, map[string]any{"p": "old"})
	if !errors.Is(err, errInjectedRegistryPersist) {
		t.Fatalf("AddNode dirty label error = %v, want injected registry persist error", err)
	}
	if n == nil {
		t.Fatal("AddNode dirty label returned nil node after committed Store write")
	}
	return g, st, n
}

func assertDirtyNodeMutationBlocked(t *testing.T, g *Core, id types.NodeID) {
	t.Helper()

	got, err := g.Nodes.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get node after blocked mutation: %v", err)
	}
	val, ok := got.GetProperty("p")
	if !ok || val != "old" {
		t.Fatalf("node property after blocked mutation = (%v, %v), want (old, true)", val, ok)
	}
	if tm := got.Temporal(); tm != nil && (tm.ValidTo != 0 || tm.DeletedAt != 0) {
		t.Fatalf("node temporal after blocked mutation = ValidTo %d DeletedAt %d, want zeroes", tm.ValidTo, tm.DeletedAt)
	}
}

func newDirtyRelForExistingTokenMutation(t *testing.T) (*Core, *registryPersistFailStore, *types.Relationship) {
	t.Helper()

	st := &registryPersistFailStore{Store: memory.New()}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}
	a, err := g.Nodes.Add(context.Background(), []string{"DirtyExistingRelEndpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"DirtyExistingRelEndpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint b: %v", err)
	}

	st.err = errInjectedRegistryPersist
	r, err := g.Rels.Add(context.Background(), "DIRTY_EXISTING_REL_TYPE", a, b, map[string]any{"p": "old"})
	if !errors.Is(err, errInjectedRegistryPersist) {
		t.Fatalf("AddRelationship dirty type error = %v, want injected registry persist error", err)
	}
	if r == nil {
		t.Fatal("AddRelationship dirty type returned nil relationship after committed Store write")
	}
	return g, st, r
}

func assertDirtyRelMutationBlocked(t *testing.T, g *Core, id types.RelID) {
	t.Helper()

	got, err := g.Rels.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get relationship after blocked mutation: %v", err)
	}
	val, ok := got.GetProperty("p")
	if !ok || val != "old" {
		t.Fatalf("relationship property after blocked mutation = (%v, %v), want (old, true)", val, ok)
	}
	if tm := got.Temporal(); tm != nil && (tm.ValidTo != 0 || tm.DeletedAt != 0) {
		t.Fatalf("relationship temporal after blocked mutation = ValidTo %d DeletedAt %d, want zeroes", tm.ValidTo, tm.DeletedAt)
	}
}

var (
	errInjectedRegistryPersist = errors.New("injected registry persist failure")
	errInjectedNodePut         = errors.New("injected node put failure")
	errInjectedRelPut          = errors.New("injected relationship put failure")
)

type registryPersistFailStore struct {
	*memory.Store
	err       error
	saveCalls int
}

func (s *registryPersistFailStore) SaveRegistries(*registrypkg.LabelRegistry, *registrypkg.RelTypeRegistry) error {
	s.saveCalls++
	return s.err
}

type registryPersistFailFirstStore struct {
	*memory.Store
	err       error
	saveCalls int
}

func (s *registryPersistFailFirstStore) SaveRegistries(*registrypkg.LabelRegistry, *registrypkg.RelTypeRegistry) error {
	s.saveCalls++
	if s.saveCalls == 1 {
		return s.err
	}
	return nil
}

type putNodeFailRegistryStore struct {
	*memory.Store
	saveCalls int
	saveErr   error
}

func (s *putNodeFailRegistryStore) PutNode(*types.Node) error {
	return errInjectedNodePut
}

func (s *putNodeFailRegistryStore) SaveRegistries(*registrypkg.LabelRegistry, *registrypkg.RelTypeRegistry) error {
	s.saveCalls++
	return s.saveErr
}

type putRelFailRegistryStore struct {
	*memory.Store
	saveCalls int
	saveErr   error
}

func (s *putRelFailRegistryStore) PutRelationship(*types.Relationship) error {
	return errInjectedRelPut
}

func (s *putRelFailRegistryStore) SaveRegistries(*registrypkg.LabelRegistry, *registrypkg.RelTypeRegistry) error {
	s.saveCalls++
	return s.saveErr
}
