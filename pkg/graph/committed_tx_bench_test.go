package graph_test

import (
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// BenchmarkCommittedTx: the pin's cost with no transaction open (a NowTx) and
// with one open (its start instant), against NowTx.
func BenchmarkCommittedTx(b *testing.B) {
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()
	b.Run("NowTx", func(b *testing.B) {
		for b.Loop() {
			if _, err := g.Temporal().NowTx(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("CommittedTx/idle", func(b *testing.B) {
		for b.Loop() {
			if _, err := g.Temporal().CommittedTx(); err != nil {
				b.Fatal(err)
			}
		}
	})
	tx, err := g.Tx().Begin()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	b.Run("CommittedTx/openTx", func(b *testing.B) {
		for b.Loop() {
			if pin, err := g.Temporal().CommittedTx(); err != nil || pin != tx.StartInstant() {
				b.Fatal(pin, err)
			}
		}
	})
}
