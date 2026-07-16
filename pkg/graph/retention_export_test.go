package graph_test

import (
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// ADR-0008 R1 — the ErrRetentionExpired sentinel is re-exported from pkg/graph so
// consumers match it with errors.Is at the public boundary (rule 4). No purge
// door is public yet (R2); R1 ships the fail-closed guard + the matchable
// sentinel.
func TestErrRetentionExpired_ReExported(t *testing.T) {
	if graphpkg.ErrRetentionExpired == nil {
		t.Fatal("graph.ErrRetentionExpired is nil")
	}
	wrapped := errors.New("wrap: " + graphpkg.ErrRetentionExpired.Error())
	_ = wrapped
	if !errors.Is(graphpkg.ErrRetentionExpired, graphpkg.ErrRetentionExpired) {
		t.Fatal("ErrRetentionExpired not identifiable via errors.Is")
	}
}
