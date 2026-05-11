package core

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestR9_RegistryPersistsAfterSuccessfulWritesWithoutGraphClose(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	g, err := New(Config{BadgerDir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	a, err := g.Nodes.Add([]string{"CrashSafeLabel"}, nil)
	if err != nil {
		t.Fatalf("Add node a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"CrashSafeLabel"}, nil)
	if err != nil {
		t.Fatalf("Add node b: %v", err)
	}
	if _, err := g.Rels.Add("CRASH_SAFE_REL", a, b, nil); err != nil {
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

func TestR9_ImportedRegistryPersistsWithoutGraphClose(t *testing.T) {
	t.Parallel()

	src := newTestGraph(t)
	defer src.Close()
	a, err := src.Nodes.Add([]string{"ImportedCrashSafeLabel"}, nil)
	if err != nil {
		t.Fatalf("source add node a: %v", err)
	}
	b, err := src.Nodes.Add([]string{"ImportedCrashSafeLabel"}, nil)
	if err != nil {
		t.Fatalf("source add node b: %v", err)
	}
	if _, err := src.Rels.Add("IMPORTED_CRASH_SAFE_REL", a, b, nil); err != nil {
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
	if err := dst.IO.Import(bytes.NewReader(stream.Bytes())); err != nil {
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
	node, err := g.Nodes.Add([]string{"PersistFailLabel"}, nil)
	if !errors.Is(err, errInjectedRegistryPersist) {
		t.Fatalf("Add node with registry persist failure = %v, want injected error", err)
	}
	if node == nil {
		t.Fatal("Add node returned nil node after committed registry persist failure")
	}
	if _, err := g.store.GetNode(node.ID()); err != nil {
		t.Fatalf("GetNode after committed registry persist failure: %v", err)
	}

	st = &registryPersistFailStore{Store: memory.New()}
	g, err = New(Config{Store: st})
	if err != nil {
		t.Fatalf("New rel graph: %v", err)
	}
	a, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint b: %v", err)
	}
	st.err = errInjectedRegistryPersist
	rel, err := g.Rels.Add("PERSIST_FAIL_REL", a, b, nil)
	if !errors.Is(err, errInjectedRegistryPersist) {
		t.Fatalf("Add rel with registry persist failure = %v, want injected error", err)
	}
	if rel == nil {
		t.Fatal("Add rel returned nil relationship after committed registry persist failure")
	}
	if _, err := g.store.GetRelationship(rel.ID()); err != nil {
		t.Fatalf("GetRelationship after committed registry persist failure: %v", err)
	}
}

func TestR9_BatchNodeRegistryPersistFailureKeepsCommittedNodeShape(t *testing.T) {
	t.Parallel()

	st := &registryPersistFailStore{Store: memory.New(), err: errInjectedRegistryPersist}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}
	defer g.Close()

	b, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	n, err := b.AddNode([]string{"BatchPersistFailLabel"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	result, err := b.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", err)
	}
	if result == nil || result.Failed != 1 || len(result.Errors) != 1 {
		t.Fatalf("Execute result = %+v, want one failed operation", result)
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
}

func TestR9_BatchRelRegistryPersistFailureKeepsCommittedRelationshipShape(t *testing.T) {
	t.Parallel()

	st := &registryPersistFailStore{Store: memory.New()}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint a: %v", err)
	}
	bNode, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint b: %v", err)
	}

	st.err = errInjectedRegistryPersist
	batch, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	rel, err := batch.AddRelationship("BATCH_PERSIST_FAIL_REL", a, bNode, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	result, err := batch.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", err)
	}
	if result == nil || result.Failed != 1 || len(result.Errors) != 1 {
		t.Fatalf("Execute result = %+v, want one failed operation", result)
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

			a, err := g.Nodes.Add([]string{"Endpoint"}, nil)
			if err != nil {
				t.Fatalf("Add endpoint a: %v", err)
			}
			b, err := g.Nodes.Add([]string{"Endpoint"}, nil)
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
	if _, err := g.Nodes.Add([]string{"RollbackPersistLabel"}, nil); !errors.Is(err, errInjectedNodePut) {
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
	a, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint b: %v", err)
	}
	relStore.saveCalls = 0
	if _, err := g.Rels.Add("ROLLBACK_PERSIST_REL", a, b, nil); !errors.Is(err, errInjectedRelPut) {
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
	_, err = g.Nodes.Add([]string{"RollbackPersistFailureLabel"}, nil)
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
	a, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint b: %v", err)
	}
	relStore.saveErr = errInjectedRegistryPersist
	_, err = g.Rels.Add("ROLLBACK_PERSIST_FAILURE_REL", a, b, nil)
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
