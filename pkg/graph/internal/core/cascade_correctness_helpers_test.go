package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Fixtures for the cascade-correctness tests (backlog 14, 18, 19 and the 2a
// half of 20; tasks/handover-effective-read-cost-20261009.md §2). One adapter
// drives a node or a relationship through the same scenario, so every test runs
// node AND relationship (rule 2) on the four backends of txbBackends (memory,
// badger in-memory, sharded, tiered). Every read is rendered as the exact set
// of tracked entities a door returns ("B:v0 T:v3"), so a door that over- or
// under-reports, or answers another version, fails the comparison (rule 16), and
// the named and generic doors are rendered side by side (rule 17).

const (
	ccLabel = "C" // label of the tracked nodes
	ccType  = "L" // type of the tracked relationships
)

// ccEnt is the node-or-relationship adapter. Entity IDs are carried as int64.
type ccEnt struct {
	t     *testing.T
	g     *Core
	ctx   context.Context
	rel   bool
	start types.NodeID // relationship endpoints (valid since 1)
	end   types.NodeID
	names map[int64]string
}

func newCCEnt(t *testing.T, g *Core, rel bool) *ccEnt {
	t.Helper()
	e := &ccEnt{t: t, g: g, ctx: context.Background(), rel: rel, names: map[int64]string{}}
	if rel {
		s, err := g.Nodes.Add(e.ctx, []string{"Ref"}, map[string]any{"tkg_valid_from": types.Instant(1)})
		if err != nil {
			t.Fatalf("Add(Ref): %v", err)
		}
		en, err := g.Nodes.Add(e.ctx, []string{"Ev"}, map[string]any{"tkg_valid_from": types.Instant(1)})
		if err != nil {
			t.Fatalf("Add(Ev): %v", err)
		}
		e.start, e.end = s.ID(), en.ID()
	}
	return e
}

func (e *ccEnt) kind() string {
	if e.rel {
		return "rel"
	}
	return "node"
}

// add creates a tracked entity named name valid from vf (plus extra props).
func (e *ccEnt) add(name string, vf types.Instant, extra map[string]any) int64 {
	e.t.Helper()
	props := map[string]any{"tkg_valid_from": vf, "x": int64(0)}
	for k, v := range extra {
		props[k] = v
	}
	var id int64
	if e.rel {
		r, err := e.g.Rels.AddByID(e.ctx, ccType, e.start, e.end, props)
		if err != nil {
			e.t.Fatalf("AddByID(%s): %v", name, err)
		}
		id = int64(r.ID())
	} else {
		n, err := e.g.Nodes.Add(e.ctx, []string{ccLabel}, props)
		if err != nil {
			e.t.Fatalf("Add(%s): %v", name, err)
		}
		id = int64(n.ID())
	}
	e.names[id] = name
	return id
}

func (e *ccEnt) cascade(id int64, vf, vt types.Instant, props map[string]any) error {
	if e.rel {
		_, err := e.g.Temporal.SetRelVersionInterval(e.ctx, types.RelID(id), vf, vt, props)
		return err
	}
	_, err := e.g.Temporal.SetNodeVersionInterval(e.ctx, types.NodeID(id), vf, vt, props)
	return err
}

func (e *ccEnt) mustCascade(id int64, vf, vt types.Instant, props map[string]any) {
	e.t.Helper()
	if err := e.cascade(id, vf, vt, props); err != nil {
		e.t.Fatalf("%s cascade(%d,[%d,%d)): %v", e.kind(), id, vf, vt, err)
	}
}

func (e *ccEnt) update(id int64, props map[string]any) error {
	if e.rel {
		_, err := e.g.Rels.Update(e.ctx, types.RelID(id), props)
		return err
	}
	_, err := e.g.Nodes.Update(e.ctx, types.NodeID(id), props)
	return err
}

func (e *ccEnt) mustUpdate(id int64, props map[string]any) {
	e.t.Helper()
	if err := e.update(id, props); err != nil {
		e.t.Fatalf("%s update(%d): %v", e.kind(), id, err)
	}
}

func (e *ccEnt) closeAt(id int64, at types.Instant) error {
	if e.rel {
		return e.g.Rels.CloseVersion(e.ctx, types.RelID(id), at)
	}
	return e.g.Nodes.CloseVersion(e.ctx, types.NodeID(id), at)
}

func (e *ccEnt) del(id int64) error {
	if e.rel {
		return e.g.Rels.Delete(e.ctx, types.RelID(id))
	}
	return e.g.Nodes.Delete(e.ctx, types.NodeID(id))
}

func (e *ccEnt) mustDel(id int64) {
	e.t.Helper()
	if err := e.del(id); err != nil {
		e.t.Fatalf("%s delete(%d): %v", e.kind(), id, err)
	}
}

func (e *ccEnt) notFound() error {
	if e.rel {
		return storepkg.ErrRelNotFound
	}
	return storepkg.ErrNodeNotFound
}

// pin reserves a transaction instant (every earlier write is at or below it,
// every later one above).
func (e *ccEnt) pin() types.Instant {
	e.t.Helper()
	p, err := e.g.Temporal.NowTx()
	if err != nil {
		e.t.Fatalf("NowTx: %v", err)
	}
	return p
}

// ccRow is one stored row of a chain.
type ccRow struct {
	version uint32
	tm      types.TemporalMetadata
	current bool
	x       any
}

// chain returns history ‖ current, ascending by version.
func (e *ccEnt) chain(id int64) []ccRow {
	e.t.Helper()
	var rows []ccRow
	if e.rel {
		hist, err := e.g.Rels.History(types.RelID(id))
		if err != nil {
			e.t.Fatalf("Rels.History: %v", err)
		}
		for _, h := range hist {
			x, _ := h.Properties().Get("x")
			rows = append(rows, ccRow{version: h.Version(), tm: relTemporalCopy(h), x: x})
		}
		cur, err := e.g.Rels.Get(e.ctx, types.RelID(id))
		if err == nil {
			x, _ := cur.Properties().Get("x")
			rows = append(rows, ccRow{version: cur.Version(), tm: relTemporalCopy(cur), current: true, x: x})
		} else if !errors.Is(err, storepkg.ErrRelNotFound) {
			e.t.Fatalf("Rels.Get: %v", err)
		}
	} else {
		hist, err := e.g.Nodes.History(types.NodeID(id))
		if err != nil {
			e.t.Fatalf("Nodes.History: %v", err)
		}
		for _, h := range hist {
			x, _ := h.Properties().Get("x")
			rows = append(rows, ccRow{version: h.Version(), tm: nodeTemporalCopy(h), x: x})
		}
		cur, err := e.g.Nodes.Get(e.ctx, types.NodeID(id))
		if err == nil {
			x, _ := cur.Properties().Get("x")
			rows = append(rows, ccRow{version: cur.Version(), tm: nodeTemporalCopy(cur), current: true, x: x})
		} else if !errors.Is(err, storepkg.ErrNodeNotFound) {
			e.t.Fatalf("Nodes.Get: %v", err)
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].version < rows[j].version })
	return rows
}

func (e *ccEnt) chainString(id int64) string {
	var b strings.Builder
	for _, r := range e.chain(id) {
		fmt.Fprintf(&b, "\n  v%d vf=%d vt=%d tx=%d..%d del=%d cur=%v x=%v", r.version, r.tm.ValidFrom, r.tm.ValidTo, r.tm.TxFrom, r.tm.TxTo, r.tm.DeletedAt, r.current, r.x)
	}
	return b.String()
}

// maxVersion is the highest version on the chain.
func (e *ccEnt) maxVersion(id int64) uint32 {
	var m uint32
	for _, r := range e.chain(id) {
		m = max(m, r.version)
	}
	return m
}

// render is the exact tracked-entity set of a door: "name:vN" sorted; an
// entity the test does not track is rendered "?<id>" so over-reporting shows.
func (e *ccEnt) render(m map[int64]uint32) string {
	out := make([]string, 0, len(m))
	for id, v := range m {
		name, ok := e.names[id]
		if !ok {
			name = fmt.Sprintf("?%d", id)
		}
		out = append(out, fmt.Sprintf("%s:v%d", name, v))
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

func (e *ccEnt) nodes(ns []*types.Node, err error) string {
	e.t.Helper()
	if err != nil {
		e.t.Fatalf("set door: %v", err)
	}
	m := map[int64]uint32{}
	for _, n := range ns {
		if !e.rel || e.names[int64(n.ID())] != "" {
			m[int64(n.ID())] = n.Version()
		}
	}
	return e.render(m)
}

func (e *ccEnt) rels(rs []*types.Relationship, err error) string {
	e.t.Helper()
	if err != nil {
		e.t.Fatalf("set door: %v", err)
	}
	m := map[int64]uint32{}
	for _, r := range rs {
		m[int64(r.ID())] = r.Version()
	}
	return e.render(m)
}

// tracked returns the tracked IDs, sorted.
func (e *ccEnt) tracked() []int64 {
	ids := make([]int64, 0, len(e.names))
	for id := range e.names {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// point renders a per-ID named door over every tracked entity; absent IDs
// (absentErr) are left out, any other error fails.
func (e *ccEnt) point(door string, one func(id int64) (uint32, error), absent ...error) string {
	e.t.Helper()
	m := map[int64]uint32{}
	for _, id := range e.tracked() {
		v, err := one(id)
		if err != nil {
			ok := false
			for _, a := range absent {
				if errors.Is(err, a) {
					ok = true
				}
			}
			if !ok {
				e.t.Fatalf("%s(%s): %v", door, e.names[id], err)
			}
			continue
		}
		m[id] = v
	}
	return e.render(m)
}

// asOfDoors renders the belief-state doors at pin: the named point door, the
// named set door and the generic TxPin door.
func (e *ccEnt) asOfDoors(pin types.Instant) map[string]string {
	e.t.Helper()
	if e.rel {
		return map[string]string{
			"RelAsOf": e.point("RelAsOf", func(id int64) (uint32, error) {
				r, err := e.g.Temporal.RelAsOf(types.RelID(id), pin)
				if err != nil {
					return 0, err
				}
				return r.Version(), nil
			}, ErrNoVersionAsOf),
			"RelsAsOf":        e.rels(e.g.Temporal.RelsAsOf(pin)),
			"ByType{TxPin}":   e.rels(e.g.Rels.ByType(ccType, storepkg.QueryOpts{TxPin: pin})),
			"Rels.All{TxPin}": e.rels(e.g.Rels.All(storepkg.QueryOpts{TxPin: pin})),
		}
	}
	return map[string]string{
		"NodeAsOf": e.point("NodeAsOf", func(id int64) (uint32, error) {
			n, err := e.g.Temporal.NodeAsOf(types.NodeID(id), pin)
			if err != nil {
				return 0, err
			}
			return n.Version(), nil
		}, ErrNoVersionAsOf),
		"NodesAsOf":        e.nodes(e.g.Temporal.NodesAsOf(pin)),
		"ByLabel{TxPin}":   e.nodes(e.g.Nodes.ByLabel(ccLabel, storepkg.QueryOpts{TxPin: pin})),
		"Nodes.All{TxPin}": e.nodes(e.g.Nodes.All(storepkg.QueryOpts{TxPin: pin})),
	}
}

// atTxDoors renders the bitemporal point doors at (validAt, txAt): named point,
// named set, generic ValidAt+TxAt.
func (e *ccEnt) atTxDoors(validAt, txAt types.Instant) map[string]string {
	e.t.Helper()
	if e.rel {
		return map[string]string{
			"RelAtTx": e.point("RelAtTx", func(id int64) (uint32, error) {
				r, err := e.g.Temporal.RelAtTx(types.RelID(id), validAt, txAt)
				if err != nil {
					return 0, err
				}
				return r.Version(), nil
			}, storepkg.ErrNoVersionValidAt, storepkg.ErrRelNotFound),
			"RelsAtTx":             e.rels(e.g.Temporal.RelsAtTx(validAt, txAt)),
			"ByType{ValidAt,TxAt}": e.rels(e.g.Rels.ByType(ccType, storepkg.QueryOpts{ValidAt: validAt, TxAt: txAt})),
		}
	}
	return map[string]string{
		"NodeAtTx": e.point("NodeAtTx", func(id int64) (uint32, error) {
			n, err := e.g.Temporal.NodeAtTx(types.NodeID(id), validAt, txAt)
			if err != nil {
				return 0, err
			}
			return n.Version(), nil
		}, storepkg.ErrNoVersionValidAt, storepkg.ErrNodeNotFound),
		"NodesAtTx":             e.nodes(e.g.Temporal.NodesAtTx(validAt, txAt)),
		"ByLabel{ValidAt,TxAt}": e.nodes(e.g.Nodes.ByLabel(ccLabel, storepkg.QueryOpts{ValidAt: validAt, TxAt: txAt})),
	}
}

// validDoors renders the declared valid-time doors at validAt (no pin) and,
// for relationships, the effective doors (Snapshot, OutgoingRelsAt; the
// endpoints are valid since 1, so they mask nothing).
func (e *ccEnt) validDoors(validAt types.Instant) map[string]string {
	e.t.Helper()
	if e.rel {
		snap, err := e.g.Temporal.Snapshot(validAt)
		if err != nil {
			e.t.Fatalf("Snapshot: %v", err)
		}
		return map[string]string{
			"RelAt": e.point("RelAt", func(id int64) (uint32, error) {
				r, err := e.g.Temporal.RelAt(types.RelID(id), validAt)
				if err != nil {
					return 0, err
				}
				return r.Version(), nil
			}, storepkg.ErrNoVersionValidAt, storepkg.ErrRelNotFound),
			"RelsAt":          e.rels(e.g.Temporal.RelsAt(validAt)),
			"ByType{ValidAt}": e.rels(e.g.Rels.ByType(ccType, storepkg.QueryOpts{ValidAt: validAt})),
			"Snapshot":        e.rels(snap.Relationships, nil),
			"OutgoingRelsAt":  e.rels(e.g.Temporal.OutgoingRelsAt(e.start, validAt)),
		}
	}
	return map[string]string{
		"NodeAt": e.point("NodeAt", func(id int64) (uint32, error) {
			n, err := e.g.Temporal.NodeAt(types.NodeID(id), validAt)
			if err != nil {
				return 0, err
			}
			return n.Version(), nil
		}, storepkg.ErrNoVersionValidAt, storepkg.ErrNodeNotFound),
		"NodesAt":          e.nodes(e.g.Temporal.NodesAt(validAt)),
		"ByLabel{ValidAt}": e.nodes(e.g.Nodes.ByLabel(ccLabel, storepkg.QueryOpts{ValidAt: validAt})),
	}
}

// expect asserts every door of doors renders want.
func (e *ccEnt) expect(phase string, doors map[string]string, want string, ids ...int64) {
	e.t.Helper()
	keys := make([]string, 0, len(doors))
	for k := range doors {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var bad []string
	for _, k := range keys {
		if doors[k] != want {
			bad = append(bad, fmt.Sprintf("%s = [%s]", k, doors[k]))
		}
	}
	if len(bad) > 0 {
		var chains strings.Builder
		for _, id := range ids {
			fmt.Fprintf(&chains, "\n chain %s:%s", e.names[id], e.chainString(id))
		}
		e.t.Fatalf("[%s %s] want [%s] on every door; got:\n %s%s", e.kind(), phase, want, strings.Join(bad, "\n "), chains.String())
	}
}

// same asserts every door of doors renders the same set and returns it.
func (e *ccEnt) same(phase string, doors map[string]string, ids ...int64) string {
	e.t.Helper()
	var first string
	for _, v := range doors {
		first = v
		break
	}
	e.expect(phase, doors, first, ids...)
	return first
}

// views is a recorded set of answers at fixed coordinates, for the rule-15
// "the past is remembered" comparison.
type ccViews map[string]string

func (e *ccEnt) record(pins []types.Instant, valids []types.Instant) ccViews {
	e.t.Helper()
	out := ccViews{}
	for _, p := range pins {
		for k, v := range e.asOfDoors(p) {
			out[fmt.Sprintf("pin=%d %s", p, k)] = v
		}
		for _, va := range valids {
			for k, v := range e.atTxDoors(va, p) {
				out[fmt.Sprintf("pin=%d valid=%d %s", p, va, k)] = v
			}
		}
	}
	return out
}

func (e *ccEnt) unchanged(phase string, before, after ccViews, ids ...int64) {
	e.t.Helper()
	keys := make([]string, 0, len(before))
	for k := range before {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var bad []string
	for _, k := range keys {
		if before[k] != after[k] {
			bad = append(bad, fmt.Sprintf("%s: [%s] -> [%s]", k, before[k], after[k]))
		}
	}
	if len(bad) > 0 {
		var chains strings.Builder
		for _, id := range ids {
			fmt.Fprintf(&chains, "\n chain %s:%s", e.names[id], e.chainString(id))
		}
		e.t.Fatalf("[%s %s] the past changed (rule 15):\n %s%s", e.kind(), phase, strings.Join(bad, "\n "), chains.String())
	}
}

// ccRun runs body for node and relationship on every txbBackend; mk opens a
// fresh graph (test clock attached) for each subtest, so the exact-set
// assertions see only that subtest's entities.
func ccRun(t *testing.T, allowBackfill bool, body func(t *testing.T, mk func(t *testing.T) *ccEnt)) {
	t.Helper()
	for _, be := range txbBackends() {
		for _, rel := range []bool{false, true} {
			kind := "node"
			if rel {
				kind = "rel"
			}
			t.Run(be.name+"/"+kind, func(t *testing.T) {
				body(t, func(t *testing.T) *ccEnt {
					t.Helper()
					g := be.open(t, allowBackfill)
					useTestClock(t, g)
					return newCCEnt(t, g, rel)
				})
			})
		}
	}
}

// ccVer renders "name:vN".
func ccVer(name string, v uint32) string { return fmt.Sprintf("%s:v%d", name, v) }

// ccSet joins rendered members in sorted order.
func ccSet(members ...string) string {
	sort.Strings(members)
	return strings.Join(members, " ")
}
