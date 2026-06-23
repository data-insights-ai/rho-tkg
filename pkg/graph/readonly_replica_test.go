package graph_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
)

// A read-only replica rejects every user-mutation door with ErrReadOnlyReplica,
// while reads and the bootstrap importer stay open.
func TestReadOnlyReplica_GatesWritesNotReads(t *testing.T) {
	ctx := context.Background()

	// Seed a primary and capture an export to bootstrap the replica with.
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	a, err := primary.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("seed Add: %v", err)
	}
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}

	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()

	// Bootstrap import is NOT gated — a replica must be seedable.
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("replica Import (bootstrap must be allowed): %v", err)
	}

	// Reads work.
	if cnt, err := replica.Nodes().Count(); err != nil || cnt != 1 {
		t.Fatalf("replica read Count = (%d, %v), want (1, nil)", cnt, err)
	}
	if _, err := replica.Nodes().ByLabel("Person", graph.QueryOpts{}); err != nil {
		t.Fatalf("replica read ByLabel: %v", err)
	}

	// Every user-mutation door is rejected with ErrReadOnlyReplica.
	roErr := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, graph.ErrReadOnlyReplica) {
			t.Errorf("%s on replica = %v, want ErrReadOnlyReplica", name, err)
		}
	}
	_, err = replica.Nodes().Add(ctx, []string{"X"}, nil)
	roErr("Nodes().Add", err)
	_, err = replica.Nodes().Update(ctx, a.ID(), map[string]any{"k": int64(1)})
	roErr("Nodes().Update", err)
	roErr("Nodes().Delete", replica.Nodes().Delete(ctx, a.ID()))
	roErr("Nodes().SetProperty", replica.Nodes().SetProperty(ctx, a.ID(), "k", int64(1)))
	roErr("Nodes().AddLabel", replica.Nodes().AddLabel(ctx, a.ID(), "Y"))
	_, err = replica.Rels().AddByID(ctx, "KNOWS", a.ID(), a.ID(), nil)
	roErr("Rels().AddByID", err)
	_, err = replica.Temporal().SetNodeVersionInterval(ctx, a.ID(), 0, 0, nil)
	roErr("Temporal().SetNodeVersionInterval", err)
	_, err = replica.Nodes().CompareAndSetProperty(ctx, a.ID(), "name", "Alice", "Bob")
	roErr("Nodes().CompareAndSetProperty", err)
	_, err = replica.Rels().CompareAndSetProperty(ctx, 1, "w", nil, int64(1))
	roErr("Rels().CompareAndSetProperty", err)
	roErr("Nodes().CloseVersion", replica.Nodes().CloseVersion(ctx, a.ID(), 1))
	roErr("Rels().CloseVersion", replica.Rels().CloseVersion(ctx, 1, 1))
	roErr("Index().CreateProperty", replica.Index().CreateProperty("Person", "name"))
	roErr("Admin().Reset", replica.Admin().Reset())
	_, err = replica.Tx().Begin()
	roErr("Tx().Begin", err)

	bb, err := replica.Batch().New()
	if err != nil {
		t.Fatalf("Batch().New: %v", err)
	}
	_, _ = bb.AddNode([]string{"Z"}, nil)
	_, err = bb.Execute()
	roErr("Batch().Execute", err)

	// A normal (non-replica) graph still writes fine — the gate is opt-in.
	if _, err := primary.Nodes().Add(ctx, []string{"Q"}, nil); err != nil {
		t.Fatalf("primary write after gating replica: %v", err)
	}
}
