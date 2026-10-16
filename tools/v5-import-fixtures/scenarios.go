package fixtures

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// All times are fixed instants (ms). Every write goes through a door that
// takes an explicit transaction time (Config.AllowTxBackfill), so no wall
// clock or minted ID reaches the store.
const (
	t0         types.Instant = 1_700_000_000_000 // 2023-11-14
	farPast    types.Instant = -4_000_000_000_000
	farFuture  types.Instant = 4_000_000_000_000
	pinFarPast types.Instant = -4_500_000_000_000
	pinFarFut  types.Instant = 4_500_000_000_000
)

func tx(i int) types.Instant { return t0 + types.Instant(i)*1000 }

// Fixed entity IDs.
const (
	idAlice types.NodeID = 1001 + iota
	idBob
	idAcme
	idDoc1
	idDoc2
	idFuture
)

const (
	relKnows types.RelID = 2001 + iota
	relWorks
	relCites
	relFar
)

type builder struct {
	g *graph.Graph
	s Scenario
}

func (b *builder) query() query {
	q := query{
		nodeIDs:    []int64{int64(idAlice), int64(idBob), int64(idAcme), int64(idDoc1), int64(idDoc2), int64(idFuture)},
		relIDs:     []int64{int64(relKnows), int64(relWorks), int64(relCites), int64(relFar)},
		timelineTx: []int64{int64(tx(5)), int64(tx(10)), int64(tx(20))},
	}
	valids := []types.Instant{pinFarPast, -5_000, t0, t0 + 2_500, t0 + 3_200, t0 + 6_000, t0 + 30_000, pinFarFut}
	txs := []types.Instant{t0 + 500, tx(2), tx(3), tx(5), tx(7), tx(10), tx(20)}
	for _, v := range valids {
		for _, t := range txs {
			q.pins = append(q.pins, [2]int64{int64(v), int64(t)})
		}
	}
	str := func(v string) *Typed { t := typed(v); return &t }
	num := func(v int64) *Typed { t := typed(v); return &t }
	for _, v := range []types.Instant{t0, t0 + 2_500, t0 + 6_000, pinFarPast} {
		for _, name := range []string{"alice", "bob"} {
			q.lookups = append(q.lookups, Lookup{Kind: "node_prop_at", Label: "Person", Key: "name", Value: str(name), Valid: int64(v)})
		}
		for _, w := range []int64{1, 2} {
			q.lookups = append(q.lookups, Lookup{Kind: "rel_prop_at", Label: "KNOWS", Key: "w", Value: num(w), Valid: int64(v)})
		}
	}
	for _, r := range [][2]types.Instant{{t0, t0 + 10_000}, {pinFarPast, t0}, {t0 + 1_800, t0 + 1_850}, {t0, pinFarFut}} {
		q.lookups = append(q.lookups,
			Lookup{Kind: "nodes_during", Valid: int64(r[0]), End: int64(r[1])},
			Lookup{Kind: "rels_during", Valid: int64(r[0]), End: int64(r[1])})
	}
	return q
}

func with(m map[string]any, at types.Instant, extra map[string]any) map[string]any {
	out := map[string]any{types.ShadowTxFrom: at}
	for k, v := range m {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

// buildStandard writes the store every scenario shares; the mode and hash
// switches of the Scenario decide what v4 stores from it.
func buildStandard(ctx context.Context, b *builder) error {
	g, s := b.g, b.s
	var err error
	n := 0
	step := func(e error) {
		n++
		if err == nil && e != nil {
			err = fmt.Errorf("step %d: %w", n, e)
		}
	}
	idx := g.Index()
	step(idx.CreateProperty("Person", "name"))
	step(idx.CreateRelProperty("KNOWS", "w"))
	step(idx.CreateComposite("Person", []string{"name", "age"}))
	step(idx.CreateTemporal("Person"))
	step(idx.CreateRelTemporal("KNOWS"))
	if err != nil {
		return err
	}

	allKinds := map[string]any{
		"tag": Marker + "alice", "name": "alice", "age": int64(30),
		"b": true, "i": int64(-42), "u": uint64(math.MaxUint64), "f": 3.25, "s": "héllo 世界",
		"i8": int8(-8), "u16": uint16(65535), "f32": float32(0.1), "nothing": nil,
		"by": []byte{0, 1, 2, 255}, "sl": []string{"a", "b"}, "il": []int64{1, -2}, "fl32": []float32{0.5, 1.5},
		"fl64": []float64{2.5, -0.125}, "bl": []bool{true, false},
		"any": []any{int64(1), "x", true, 2.5}, // Maps hold ONE key each: v4 writes a map property in Go map order, so a
		// multi-key map would make the export differ from run to run.
		"m": map[string]any{"sub": map[string]any{"z": true}}, "m2": map[string]any{"n": int64(7)},
		"date":     types.TemporalValue{Kind: types.TemporalDate, Value: "2024-01-02"},
		"datetime": types.TemporalValue{Kind: types.TemporalDateTime, Value: "2024-01-02T03:04:05Z"},
		"dur":      types.TemporalValue{Kind: types.TemporalDuration, Value: "P1DT2H"},
		"when":     time.Date(2024, 3, 14, 9, 26, 53, 0, time.UTC),
	}
	node := func(id types.NodeID, labels []string, p map[string]any, at types.Instant, extra map[string]any) *types.Node {
		n, e := g.Nodes().Import(ctx, id, labels, with(p, at, extra))
		step(e)
		return n
	}
	rel := func(id types.RelID, typ string, from, to *types.Node, p map[string]any, at types.Instant, extra map[string]any) {
		_, e := g.Rels().Import(ctx, id, typ, from, to, with(p, at, extra))
		step(e)
	}

	// tx 1: nodes. alice and doc1 are untimed; bob starts far in the past,
	// a second org only starts far in the future; doc2 is untimed and closed later.
	alice := node(idAlice, []string{"Person", "Admin"}, allKinds, tx(1), nil)
	bob := node(idBob, []string{"Person"}, map[string]any{"tag": Marker + "bob", "name": "bob", "age": int64(41)}, tx(1),
		map[string]any{types.ShadowValidFrom: farPast})
	acme := node(idAcme, []string{"Org"}, map[string]any{"tag": Marker + "acme", "name": "acme"}, tx(1), nil)
	future := node(idFuture, []string{"Org"}, map[string]any{"tag": Marker + "future", "name": "later"}, tx(1),
		map[string]any{types.ShadowValidFrom: farFuture})
	doc1 := node(idDoc1, []string{"Doc"}, map[string]any{"tag": Marker + "doc1", "title": "first"}, tx(1), nil)
	doc2 := node(idDoc2, []string{"Doc"}, map[string]any{"tag": Marker + "doc2", "title": "second"}, tx(1), nil)
	_ = future
	if err != nil {
		return err
	}
	// tx 2: relationships.
	rel(relKnows, "KNOWS", alice, bob, map[string]any{"tag": Marker + "r1", "w": int64(1)}, tx(2), nil)
	rel(relWorks, "WORKS_AT", bob, acme, map[string]any{"tag": Marker + "r2", "role": "dev"}, tx(2),
		map[string]any{types.ShadowValidFrom: tx(2)})
	rel(relCites, "CITES", alice, doc2, map[string]any{"tag": Marker + "r3"}, tx(2), nil)
	// tx 3: update alice.
	_, e := g.Nodes().UpdateWithTx(ctx, idAlice, map[string]any{"age": int64(31), "nick": "al"}, tx(3))
	step(e)
	// tx 4: update a rel, delete doc1.
	_, e = g.Rels().UpdateWithTx(ctx, relKnows, map[string]any{"w": int64(2)}, tx(4))
	step(e)
	step(g.Nodes().DeleteWithTx(ctx, idDoc1, tx(4)))
	// tx 5: patch a bounded valid interval into bob.
	_, e = g.Temporal().SetNodeVersionIntervalWithTx(ctx, idBob, t0+1_500, t0+2_500, map[string]any{"mood": "patched"}, tx(5))
	step(e)
	// tx 6: a later life of doc1 (re-import of a deleted ID); close doc2's validity.
	doc1 = node(idDoc1, []string{"Doc"}, map[string]any{"tag": Marker + "doc1b", "title": "first again"}, tx(6), nil)
	_, e = g.Nodes().UpdateWithTx(ctx, idDoc2, map[string]any{types.ShadowValidTo: t0 + 20_000}, tx(6))
	step(e)
	// tx 7: excise validity of a node and of a rel (void rows).
	step(g.Temporal().ExciseNodeValidityWithTx(ctx, idBob, t0+1_700, t0+1_900, tx(7)))
	step(g.Temporal().ExciseRelValidityWithTx(ctx, relKnows, t0+3_000, t0+3_500, tx(7)))
	if s.Unbounded {
		// Only an untimed first version starting at -inf has validity before 3000.
		step(g.Temporal().ExciseNodeValidityWithTx(ctx, idDoc2, 2_000, 3_000, tx(7)+1))
	}
	// tx 8: retract acme (cascades to WORKS_AT).
	step(g.Nodes().RetractWithTx(ctx, idAcme, tx(8)))
	// tx 9: retract the CITES rel.
	step(g.Rels().RetractWithTx(ctx, relCites, tx(9)))
	// tx 10: delete alice (cascades to KNOWS).
	step(g.Nodes().DeleteWithTx(ctx, idAlice, tx(10)))
	// tx 11: more history on bob and the re-lived doc1.
	_, e = g.Nodes().UpdateWithTx(ctx, idBob, map[string]any{"mood": "final"}, tx(11))
	step(e)
	// tx 12: a rel valid across the whole range, then updated.
	rel(relFar, "KNOWS", bob, doc1, map[string]any{"tag": Marker + "r4", "w": int64(9)}, tx(12),
		map[string]any{types.ShadowValidFrom: farPast, types.ShadowValidTo: farFuture})
	_, e = g.Temporal().SetRelVersionIntervalWithTx(ctx, relFar, t0+4_000, t0+5_000, map[string]any{"w": int64(10)}, tx(13))
	step(e)
	return err
}
