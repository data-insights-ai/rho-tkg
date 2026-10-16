package core

import (
	"math"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkWindowedEqualitySeek (ledger row 62): one relationship value with
// 50,000 members (every fifth revised: created open, then closed by an
// update), each valid in its own slot, and a late window [a, MaxInt64) at the
// newest pin that 10 of them meet — sigma's late-row refold shape. Reports the
// lookup's time, allocations and chains loaded.
func BenchmarkWindowedEqualitySeek(b *testing.B) {
	const members = 50_000
	for _, be := range txbBackends() {
		b.Run(be.name, func(b *testing.B) {
			fx := newWEFixture(b, be.open(b, false), true)
			x0 := fx.x - members*100 - 10_000
			for i := 0; i < members; i++ {
				from := x0 + types.Instant(i)*100
				props := map[string]any{weKey: "big", "tkg_valid_from": from, "tkg_valid_to": from + 50}
				if i%5 != 0 {
					fx.add(b, props)
					continue
				}
				delete(props, "tkg_valid_to")
				id := fx.add(b, props)
				props["tkg_valid_from"], props["tkg_valid_to"] = from+10, from+50
				if err := fx.update(id, props); err != nil {
					b.Fatal(err)
				}
			}
			if f, ok := fx.g.store.(interface{ Flush() error }); ok {
				if err := f.Flush(); err != nil { // the rows as written out, not a write buffer
					b.Fatal(err)
				}
			}
			fx.tick(1000)
			tx, err := fx.g.Temporal.NowTx()
			if err != nil {
				b.Fatal(err)
			}
			opts := storepkg.QueryOpts{TxAt: tx, ValidStart: x0 + (members-10)*100, ValidEnd: math.MaxInt64}
			got, err := fx.query(weKey, "big", opts) // builds the sidecar, opens the view
			if err != nil || len(got) != 10 {
				b.Fatalf("lookup: %d matches, %v", len(got), err)
			}
			loads := countChainLoads(fx.g, func() { _, _ = fx.query(weKey, "big", opts) })
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := fx.query(weKey, "big", opts); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(loads), "chains/op")
		})
	}
}

// BenchmarkWindowedEqualityWritePath (ledger row 62): the write cost the
// windowed sidecar adds — an untimed Update of an indexed relationship
// (same value) after a windowed lookup built the sidecar and opened the
// value's view, 1,000 members, memory and badger. Runs on the base too.
func BenchmarkWindowedEqualityWritePath(b *testing.B) {
	for _, be := range txbBackends()[:2] {
		b.Run(be.name, func(b *testing.B) {
			fx := newWEFixture(b, be.open(b, false), true)
			ids := make([]int64, 1000)
			for i := range ids {
				ids[i] = fx.add(b, map[string]any{weKey: "v"})
			}
			if _, err := fx.query(weKey, "v", storepkg.QueryOpts{ValidStart: fx.x, ValidEnd: math.MaxInt64}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				fx.tick(1)
				if err := fx.update(ids[i%len(ids)], map[string]any{weKey: "v", "n": i}); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	}
}
