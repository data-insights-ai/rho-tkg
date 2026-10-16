package graphstate

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// The page-read budget is ONE type, owned by raftlog: a ReadView takes it
// without conversion (compile-time check; fails to build if the types differ).
func TestReadViewTakesRaftlogReadBudget(t *testing.T) {
	var v ReadView = &fixtureView{}
	_, _ = v.ComponentKeys(context.Background(), KeyPredicate{}, 0, raftlog.ReadBudget{Rows: 1, Bytes: 1})
}
