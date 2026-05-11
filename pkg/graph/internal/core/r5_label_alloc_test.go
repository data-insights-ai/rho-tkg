package core

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestR5_NodeCreate_PutFailureDoesNotKeepNewLabelToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*Core, string) error
	}{
		{
			name: "Add",
			run: func(g *Core, label string) error {
				_, err := g.Nodes.Add([]string{label}, nil)
				return err
			},
		},
		{
			name: "Import",
			run: func(g *Core, label string) error {
				_, err := g.Nodes.Import(context.Background(), g.nextNodeID(), []string{label}, nil)
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			injected := errors.New("synthetic PutNode fault")
			g, err := New(Config{Store: &failPutNodeStore{Store: memory.New(), err: injected}})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()

			label := "PUT_FAIL_" + tc.name
			if err := tc.run(g, label); !errors.Is(err, injected) {
				t.Fatalf("%s error = %v, want injected PutNode fault", tc.name, err)
			}
			if _, ok := g.labels.Lookup(label); ok {
				t.Fatalf("%s kept label token %q after PutNode failure", tc.name, label)
			}
		})
	}
}

func TestR5_AddNodeLabel_WriteFailureDoesNotKeepNewLabelToken(t *testing.T) {
	t.Parallel()

	injected := errors.New("synthetic AddNodeLabelTokenWithHistory fault")
	g, err := New(Config{Store: &failAddNodeLabelStore{Store: memory.New(), err: injected}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	n, err := g.Nodes.Add([]string{"Existing"}, nil)
	if err != nil {
		t.Fatalf("Add existing node: %v", err)
	}

	const label = "ADD_LABEL_FAIL"
	if err := g.Nodes.AddLabel(n.ID(), label); !errors.Is(err, injected) {
		t.Fatalf("AddLabel error = %v, want injected AddNodeLabelTokenWithHistory fault", err)
	}
	if _, ok := g.labels.Lookup(label); ok {
		t.Fatalf("kept label token %q after AddNodeLabelTokenWithHistory failure", label)
	}
}

func TestBatchBuilderAddNodeDefersNewLabelUntilExecute(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bb, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	n, err := bb.AddNode([]string{"DeferredBatchLabel", "DeferredBatchExtra", "DeferredBatchLabel"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	for _, label := range []string{"DeferredBatchLabel", "DeferredBatchExtra"} {
		if _, ok := g.labels.Lookup(label); ok {
			t.Fatalf("queued AddNode registered label %q before Execute", label)
		}
	}

	result, err := bb.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Created != 1 || result.Failed != 0 {
		t.Fatalf("result created=%d failed=%d, want created=1 failed=0", result.Created, result.Failed)
	}
	for _, label := range []string{"DeferredBatchLabel", "DeferredBatchExtra"} {
		if _, ok := g.labels.Lookup(label); !ok {
			t.Fatalf("executed AddNode did not register label %q", label)
		}
	}
	labels := g.Nodes.Labels(n)
	if len(labels) != 2 || labels[0] != "DeferredBatchLabel" || labels[1] != "DeferredBatchExtra" {
		t.Fatalf("returned node labels after Execute = %v, want [DeferredBatchLabel DeferredBatchExtra]", labels)
	}
	ok, err := g.Hash.VerifyNodeChain(n.ID())
	if err != nil {
		t.Fatalf("VerifyNodeChain: %v", err)
	}
	if !ok {
		t.Fatal("batch-created node hash did not verify after execute-time retokenization")
	}
}

func TestBatchBuilderAddNodePutNodesBatchFailureDoesNotKeepNewLabelToken(t *testing.T) {
	t.Parallel()

	injected := errors.New("synthetic PutNodesBatch fault")
	g, err := New(Config{Store: &failPutNodesBatchStore{Store: memory.New(), err: injected}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bb, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	n, err := bb.AddNode([]string{"BatchPutFailLabel"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if _, ok := g.labels.Lookup("BatchPutFailLabel"); ok {
		t.Fatal("queued AddNode registered BatchPutFailLabel before Execute")
	}

	result, err := bb.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", err)
	}
	if result.Failed != 1 || result.Created != 0 {
		t.Fatalf("result failed=%d created=%d, want failed=1 created=0", result.Failed, result.Created)
	}
	if _, ok := g.labels.Lookup("BatchPutFailLabel"); ok {
		t.Fatal("BatchPutFailLabel remained registered after PutNodesBatch failure")
	}
	if tm := n.Temporal(); tm != nil && tm.TxFrom != 0 {
		t.Fatalf("returned node TxFrom = %d, want 0 after failed Execute", tm.TxFrom)
	}
}

func TestR5_NodeCreateCapacityFailureDoesNotKeepPartiallyAllocatedLabelToken(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	importLabelRegistryNames(t, g, int(registrypkg.TokenCapacityMax)-1)

	if _, err := g.Nodes.Add([]string{"CapacityPrimary", "CapacityExtra"}, nil); err == nil {
		t.Fatal("Nodes.Add error = nil, want label registry capacity failure")
	}
	if _, ok := g.labels.Lookup("CapacityPrimary"); ok {
		t.Fatal("CapacityPrimary remained registered after later extra label hit capacity")
	}
	if _, ok := g.labels.Lookup("CapacityExtra"); ok {
		t.Fatal("CapacityExtra registered despite capacity failure")
	}
	if got := g.labels.Len(); got != int(registrypkg.TokenCapacityMax)-1 {
		t.Fatalf("label registry length = %d, want %d", got, int(registrypkg.TokenCapacityMax)-1)
	}
}

func TestBatchBuilderAddNodeCapacityFailureDoesNotKeepPartiallyAllocatedLabelToken(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bb, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	n, err := bb.AddNode([]string{"BatchCapacityPrimary", "BatchCapacityExtra"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	importLabelRegistryNames(t, g, int(registrypkg.TokenCapacityMax)-1)

	result, err := bb.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", err)
	}
	if result.Failed != 1 || result.Created != 0 {
		t.Fatalf("result failed=%d created=%d, want failed=1 created=0", result.Failed, result.Created)
	}
	if _, ok := g.labels.Lookup("BatchCapacityPrimary"); ok {
		t.Fatal("BatchCapacityPrimary remained registered after later extra label hit capacity")
	}
	if _, ok := g.labels.Lookup("BatchCapacityExtra"); ok {
		t.Fatal("BatchCapacityExtra registered despite capacity failure")
	}
	if tm := n.Temporal(); tm != nil && tm.TxFrom != 0 {
		t.Fatalf("returned node TxFrom = %d, want 0 after failed Execute", tm.TxFrom)
	}
}

func TestBatchBuilderAddNodePanicDoesNotLeakRegistryLock(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: &panicStore{Store: memory.New()}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bb, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if _, err := bb.AddNode([]string{"BatchPanicLabel"}, nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic from PutNodesBatch")
			}
		}()
		_, _ = bb.Execute()
	}()

	if _, ok := g.labels.Lookup("BatchPanicLabel"); ok {
		t.Fatal("BatchPanicLabel remained registered after PutNodesBatch panic")
	}

	var addErr error
	completed := withDeadline(t, 2*time.Second, func() {
		_, addErr = g.Nodes.Add([]string{"AfterBatchPanic"}, nil)
	})
	if !completed {
		t.Fatal("follow-up Nodes.Add deadlocked; registry rollback leaked its mutex")
	}
	if addErr != nil {
		t.Fatalf("follow-up Nodes.Add: %v", addErr)
	}
}

type failPutNodeStore struct {
	storepkg.Store
	err error
}

func (s *failPutNodeStore) PutNode(n *types.Node) error {
	return s.err
}

type failAddNodeLabelStore struct {
	storepkg.Store
	err error
}

func (s *failAddNodeLabelStore) AddNodeLabelTokenWithHistory(
	id types.NodeID,
	tok uint16,
	updatedNode *types.Node,
	prevVersion uint32,
	prevState *types.Node,
) error {
	return s.err
}

func importLabelRegistryNames(t *testing.T, g *Core, count int) {
	t.Helper()

	names := make([]string, count+1)
	for i := range names {
		if i == 0 {
			continue
		}
		names[i] = fmt.Sprintf("SeedLabel%05d", i)
	}
	if err := g.labels.ImportNames(names); err != nil {
		t.Fatalf("ImportNames(%d labels): %v", count, err)
	}
}
