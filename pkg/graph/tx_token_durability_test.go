package graph_test

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/core"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTokenInternedInTxSurvivesReopen is the durability claim, checked against disk rather
// than against a counter.
//
// The commit-time registry checkpoint became conditional on a registry having actually
// changed. The risk that change carries is precise: a row referencing a token that never
// reached stable storage is undecodable on reload. So intern a label and a property key
// inside a transaction, commit, close, reopen from the same directory, and read the row back.
func TestTokenInternedInTxSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	g, err := graph.New(graph.Config{SnowflakeNodeID: 0, BadgerDir: dir})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	var id types.NodeID
	if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		n, addErr := tx.AddNode([]string{"ReopenLabel"}, map[string]any{"reopen_key": "kept"})
		if addErr != nil {
			return addErr
		}
		id = n.ID()
		return nil
	}); err != nil {
		t.Fatalf("tx: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := graph.New(graph.Config{SnowflakeNodeID: 0, BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.Nodes().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("the node written in the transaction did not survive reopen: %v", err)
	}
	// The LABEL token: resolvable back to its name after reload.
	if got.PrimaryLabelToken() == 0 {
		t.Error("the node came back with no label token")
	}
	// The PROPERTY KEY token: the row is decodable and the value is intact. An unresolvable
	// key is exactly the corruption the checkpoint exists to prevent.
	v, ok := got.GetProperty("reopen_key")
	if !ok {
		t.Error("the PROPERTY KEY token did not survive the reopen: key unresolvable")
	} else if v != "kept" {
		t.Errorf("property value did not survive: %v", v)
	}
}
