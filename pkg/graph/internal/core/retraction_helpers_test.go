package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Shared fixtures of the retraction tests (backlog 43, tasks/evidence/retraction/).
//
// The belief definition every retraction test checks, stated without the
// resolver: a retraction at transaction instant T of an entity E (and, for a
// node, of every relationship it has) leaves every answer at a pin p < T
// byte-identical to the answer before the retraction, and makes every answer
// at a pin p >= T equal to the answer before the retraction with E removed —
// at EVERY valid time, not only at valid times from T on (that is Delete).
// Bystanders answer exactly as before at every pin.

// retractDoor is one way to retract: a door family (standalone, GraphTx,
// Batch, ingest strong/concurrent), with or without a caller instant.
type retractDoor struct {
	name   string
	withTx bool
	node   func(t *testing.T, g *Core, id types.NodeID, at types.Instant) error
	rel    func(t *testing.T, g *Core, id types.RelID, at types.Instant) error
}

func retractDoors() []retractDoor {
	ctx := context.Background()
	ingest := func(name string, opts IngestOptions) []retractDoor {
		return []retractDoor{
			{name: name, node: func(t *testing.T, g *Core, id types.NodeID, _ types.Instant) error {
				return txbIngestDo(t, g, opts, func(s *Session) error { return s.RetractNode(id) })
			}, rel: func(t *testing.T, g *Core, id types.RelID, _ types.Instant) error {
				return txbIngestDo(t, g, opts, func(s *Session) error { return s.RetractRelationship(id) })
			}},
			{name: name + "WithTx", withTx: true, node: func(t *testing.T, g *Core, id types.NodeID, at types.Instant) error {
				return txbIngestDo(t, g, opts, func(s *Session) error { return s.RetractNodeWithTx(id, at) })
			}, rel: func(t *testing.T, g *Core, id types.RelID, at types.Instant) error {
				return txbIngestDo(t, g, opts, func(s *Session) error { return s.RetractRelationshipWithTx(id, at) })
			}},
		}
	}
	out := []retractDoor{
		{name: "standalone", node: func(t *testing.T, g *Core, id types.NodeID, _ types.Instant) error {
			return g.Nodes.Retract(ctx, id)
		}, rel: func(t *testing.T, g *Core, id types.RelID, _ types.Instant) error {
			return g.Rels.Retract(ctx, id)
		}},
		{name: "standaloneWithTx", withTx: true, node: func(t *testing.T, g *Core, id types.NodeID, at types.Instant) error {
			return g.Nodes.RetractWithTx(ctx, id, at)
		}, rel: func(t *testing.T, g *Core, id types.RelID, at types.Instant) error {
			return g.Rels.RetractWithTx(ctx, id, at)
		}},
		{name: "graphtx", node: func(t *testing.T, g *Core, id types.NodeID, _ types.Instant) error {
			return txbTxDo(t, g, func(tx *GraphTx) error { return tx.RetractNode(id) })
		}, rel: func(t *testing.T, g *Core, id types.RelID, _ types.Instant) error {
			return txbTxDo(t, g, func(tx *GraphTx) error { return tx.RetractRelationship(id) })
		}},
		{name: "graphtxWithTx", withTx: true, node: func(t *testing.T, g *Core, id types.NodeID, at types.Instant) error {
			return txbTxDo(t, g, func(tx *GraphTx) error { return tx.RetractNodeWithTx(id, at) })
		}, rel: func(t *testing.T, g *Core, id types.RelID, at types.Instant) error {
			return txbTxDo(t, g, func(tx *GraphTx) error { return tx.RetractRelationshipWithTx(id, at) })
		}},
		{name: "batch", node: func(t *testing.T, g *Core, id types.NodeID, _ types.Instant) error {
			return txbBatchDo(t, g, func(b *BatchBuilder) error { return b.RetractNode(id) })
		}, rel: func(t *testing.T, g *Core, id types.RelID, _ types.Instant) error {
			return txbBatchDo(t, g, func(b *BatchBuilder) error { return b.RetractRelationship(id) })
		}},
		{name: "batchWithTx", withTx: true, node: func(t *testing.T, g *Core, id types.NodeID, at types.Instant) error {
			return txbBatchDo(t, g, func(b *BatchBuilder) error { return b.RetractNodeWithTx(id, at) })
		}, rel: func(t *testing.T, g *Core, id types.RelID, at types.Instant) error {
			return txbBatchDo(t, g, func(b *BatchBuilder) error { return b.RetractRelationshipWithTx(id, at) })
		}},
	}
	out = append(out, ingest("ingestSync", IngestOptions{Sync: true})...)
	out = append(out, ingest("ingestConcurrent", IngestOptions{Concurrent: true})...)
	return out
}

// rtWorld is the retraction scenario: an anchor A ("Ref": the reference
// shard on tiered, so its relationships cross shards), a target node X and a
// bystander node Y ("Pers"), relationships RX = A->X, RY = A->Y, RXY = X->Y
// ("LINK"). Every entity is created valid from v0 and updated valid from v1,
// so each has a history row and a past valid span a Delete keeps readable.
type rtWorld struct {
	a, x, y       types.NodeID
	rx, ry, rxy   types.RelID
	v0, v1, built types.Instant
}

func buildRetractWorld(t *testing.T, g *Core) *rtWorld {
	t.Helper()
	ctx := context.Background()
	wall := txbWall()
	w := &rtWorld{v0: wall - 600_000, v1: wall - 300_000}
	// The declared property indexes put the pinned property lookups on their
	// tx-membership sidecars where the backend has them.
	if err := g.Index.CreateProperty("Pers", "k"); err != nil && !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("CreateProperty: %v", err)
	}
	addNode := func(label, k string, n int64) types.NodeID {
		nd, err := g.Nodes.Add(ctx, []string{label}, map[string]any{"k": k, "n": n, "tkg_valid_from": w.v0})
		if err != nil {
			t.Fatalf("Add(%s): %v", k, err)
		}
		return nd.ID()
	}
	w.a = addNode("Ref", "a", 1)
	w.x = addNode("Pers", "x", 10)
	w.y = addNode("Pers", "y", 20)
	if err := g.Index.CreateRelProperty("LINK", "k"); err != nil && !errors.Is(err, storepkg.ErrRelPropertyIndexUnsupported) {
		t.Fatalf("CreateRelProperty: %v", err)
	}
	addRel := func(s, e types.NodeID, k string, wt int64) types.RelID {
		r, err := g.Rels.AddByID(ctx, "LINK", s, e, map[string]any{"k": k, "w": wt, "tkg_valid_from": w.v0})
		if err != nil {
			t.Fatalf("AddByID(%s): %v", k, err)
		}
		return r.ID()
	}
	w.rx = addRel(w.a, w.x, "rx", 1)
	w.ry = addRel(w.a, w.y, "ry", 2)
	w.rxy = addRel(w.x, w.y, "rxy", 3)
	for _, id := range []types.NodeID{w.a, w.x, w.y} {
		if _, err := g.Nodes.Update(ctx, id, map[string]any{"n": int64(99), "tkg_valid_from": w.v1}); err != nil {
			t.Fatalf("Update node: %v", err)
		}
	}
	for _, id := range []types.RelID{w.rx, w.ry, w.rxy} {
		if _, err := g.Rels.Update(ctx, id, map[string]any{"w": int64(99), "tkg_valid_from": w.v1}); err != nil {
			t.Fatalf("Update rel: %v", err)
		}
	}
	built, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}
	w.built = built
	return w
}

// validGrid is the valid instants every probe visits: around both version
// starts (the past a Delete keeps readable) and the present.
func (w *rtWorld) validGrid() []types.Instant {
	return []types.Instant{w.v0 - 1, w.v0, w.v0 + 1, (w.v0 + w.v1) / 2, w.v1 - 1, w.v1, w.v1 + 1, txbWall()}
}

func nodeKey(id types.NodeID) string { return "n:" + strconv.FormatInt(int64(id), 10) }
func relKey(id types.RelID) string   { return "r:" + strconv.FormatInt(int64(id), 10) }

// The oracles render every answer row of two worlds, so these helpers write
// with strconv instead of fmt (4 Nov: fmt and its garbage were half of the
// retraction oracle's CPU under race). Their output is byte for byte what
// fmt's %+v and %v print, which the goldens pin; TestRowRenderMatchesFmt
// holds them to it, a new TemporalMetadata field included.

func fmtTemporal(tm *types.TemporalMetadata) string {
	if tm == nil {
		return "<nil>"
	}
	b := make([]byte, 0, 200)
	b = appendTemporal(b, tm)
	return string(b)
}

func appendTemporal(b []byte, tm *types.TemporalMetadata) []byte {
	i := func(name string, v types.Instant) {
		b = append(b, name...)
		b = strconv.AppendInt(b, int64(v), 10)
	}
	i("{ValidFrom:", tm.ValidFrom)
	i(" ValidTo:", tm.ValidTo)
	i(" TxFrom:", tm.TxFrom)
	i(" TxTo:", tm.TxTo)
	i(" CreatedAt:", tm.CreatedAt)
	i(" UpdatedAt:", tm.UpdatedAt)
	i(" DeletedAt:", tm.DeletedAt)
	b = append(append(b, " CreatedBy:"...), tm.CreatedBy...)
	b = append(append(b, " UpdatedBy:"...), tm.UpdatedBy...)
	b = strconv.AppendInt(append(b, " baseEntityID:"...), int64(tm.BaseEntityID()), 10)
	b = strconv.AppendBool(append(b, " Retracted:"...), tm.Retracted)
	b = strconv.AppendBool(append(b, " Void:"...), tm.Void)
	b = strconv.AppendBool(append(b, " DeleteKeptValidTo:"...), tm.DeleteKeptValidTo)
	return append(b, '}')
}

// fmtValue is %v of a property value: strconv for the kinds the worlds
// write, fmt for anything else.
func fmtValue(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return strconv.FormatInt(x, 10)
	case int:
		return strconv.Itoa(x)
	case bool:
		return strconv.FormatBool(x)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	}
	return fmt.Sprint(v)
}

func fmtProps(ps types.PropertySlice) string {
	parts := make([]string, 0, len(ps))
	for _, p := range ps {
		parts = append(parts, p.Key+"="+fmtValue(p.Value))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func rowString(version int64, tm *types.TemporalMetadata, ps types.PropertySlice) string {
	b := make([]byte, 0, 256)
	b = strconv.AppendInt(append(b, 'v'), version, 10)
	b = append(b, ' ')
	if tm == nil {
		b = append(b, "<nil>"...)
	} else {
		b = appendTemporal(b, tm)
	}
	b = append(append(append(b, " {"...), fmtProps(ps)...), '}')
	return string(b)
}

// nodeRowString is everything a reader sees of a row: version, every temporal
// stamp (the retraction marker included) and the properties.
func nodeRowString(n *types.Node) string {
	return rowString(int64(n.Version()), n.Temporal(), n.Properties())
}

func relRowString(r *types.Relationship) string {
	return rowString(int64(r.Version()), r.Temporal(), r.Properties())
}

// answer is a door's answer: entity key -> what the reader sees of it.
type answer map[string]string

func (a answer) String() string {
	keys := make([]string, 0, len(a))
	for k := range a {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString("\n    ")
		b.WriteString(k)
		b.WriteString(": ")
		b.WriteString(a[k])
	}
	return b.String()
}

// without drops every entry naming a key of keys: the entry's key itself, or
// one component of a composite key ("n:1<-n:2", a neighbor seen from a node).
func (a answer) without(keys map[string]bool) answer {
	out := answer{}
	for k, v := range a {
		drop := false
		for _, part := range strings.Split(k, "<-") {
			if keys[part] {
				drop = true
			}
		}
		if !drop {
			out[k] = v
		}
	}
	return out
}

func (a answer) equal(b answer) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

func nodesAnswer(ns []*types.Node) answer {
	out := answer{}
	for _, n := range ns {
		out[nodeKey(n.ID())] = nodeRowString(n)
	}
	return out
}

func relsAnswer(rs []*types.Relationship) answer {
	out := answer{}
	for _, r := range rs {
		out[relKey(r.ID())] = relRowString(r)
	}
	return out
}

// TestRowRenderMatchesFmt holds the strconv row rendering to fmt's: every
// TemporalMetadata field set to a distinct value (a new field changes %+v and
// fails here), each property kind the worlds write, and one fmt falls back on.
func TestRowRenderMatchesFmt(t *testing.T) {
	t.Parallel()
	tm := &types.TemporalMetadata{ValidFrom: 1, ValidTo: 2, TxFrom: 3, TxTo: 4, CreatedAt: 5, UpdatedAt: 6, DeletedAt: -7,
		CreatedBy: "c b", UpdatedBy: "u", Retracted: true, Void: true, DeleteKeptValidTo: true}
	tm.SetBaseEntityID(8)
	if got, want := fmtTemporal(tm), fmt.Sprintf("%+v", *tm); got != want {
		t.Fatalf("fmtTemporal = %s\nfmt %%+v    = %s", got, want)
	}
	var zero types.TemporalMetadata
	if got, want := fmtTemporal(&zero), fmt.Sprintf("%+v", zero); got != want {
		t.Fatalf("fmtTemporal(zero) = %s, fmt = %s", got, want)
	}
	for _, v := range []any{"s", int64(-9), 10, true, 1.5, 1e21, 0.1, []any{int64(1), "a"}, nil} {
		if got, want := fmtValue(v), fmt.Sprintf("%v", v); got != want {
			t.Errorf("fmtValue(%#v) = %q, fmt %q", v, got, want)
		}
	}
	ps := types.PropertySlice{{Key: "k", Value: "x"}, {Key: "n", Value: int64(3)}}
	if got, want := rowString(4, tm, ps), fmt.Sprintf("v%d %+v {%s}", 4, *tm, "k=x,n=3"); got != want {
		t.Fatalf("rowString = %s\nfmt       = %s", got, want)
	}
	if got, want := nodeKey(types.NodeID(12)), fmt.Sprintf("n:%d", types.NodeID(12)); got != want {
		t.Fatalf("nodeKey = %s, fmt %s", got, want)
	}
}
