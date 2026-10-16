package importmap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Write-path ambiguity kinds (a value the graph's typed schema cannot hold).
const (
	KindKeyKindConflict = "key_kind_conflict"    // a later value of another kind than the key's first
	KindNullOnlyKey     = "null_only_key"        // every value of the key is null: nothing can declare it
	KindRelClipped      = "rel_clipped_to_nodes" // the engine keeps a relationship inside the valid time of its two nodes
	KindRelOutsideNodes = "rel_outside_nodes"    // nothing of the relationship lies inside both nodes
)

// The import marker is one node holding the import's progress. It is written
// in the same Update as every batch, so "how far did the import get" is never
// a guess: there is no window between a commit and its bookkeeping.
const (
	markerLabel   = "rho.import"
	markerApplied = "rho.import.applied"
	markerPrint   = "rho.import.fingerprint"
)

// Options tunes Import. The zero value is fine.
type Options struct {
	// MaxOps caps the ops of one Update (default 256). A batch the store refuses as
	// too large is halved until it fits.
	MaxOps int
	// AfterBatch runs after each committed batch with the number of ops applied
	// so far; an error stops the import (the graph keeps what is committed).
	AfterBatch func(applied int) error
}

type idKey struct {
	kind string
	id   int64
	life int
}

// IDMap relates v4 entities to the v5 IDs they were minted: the engine mints
// IDs itself (sequentially above Config.Allocation.Seed), so a v4 ID is not
// kept but looked up here. Each life of a v4 ID (after a delete) is its own v5 entity.
type IDMap struct {
	v5     map[idKey]int64
	v4     map[[2]any]idKey
	latest map[[2]any]int64
	// Dropped lists the property values the typed schema could not take.
	Dropped []Ambiguity
}

func newIDMap() *IDMap {
	return &IDMap{v5: map[idKey]int64{}, v4: map[[2]any]idKey{}, latest: map[[2]any]int64{}}
}

func (m *IDMap) set(k idKey, v5 int64) {
	m.v5[k] = v5
	m.v4[[2]any{k.kind, v5}] = k
	m.latest[[2]any{k.kind, k.id}] = v5
}

// V5 is the v5 ID of life of the v4 entity.
func (m *IDMap) V5(kind string, id int64, life int) (int64, bool) {
	v, ok := m.v5[idKey{kind, id, life}]
	return v, ok
}

// V4 is the v4 ID and life a v5 ID stands for.
func (m *IDMap) V4(kind string, v5 int64) (id int64, life int, ok bool) {
	k, ok := m.v4[[2]any{kind, v5}]
	return k.id, k.life, ok
}

// Latest is the v5 ID of the newest life of a v4 entity.
func (m *IDMap) Latest(kind string, id int64) (int64, bool) {
	v, ok := m.latest[[2]any{kind, id}]
	return v, ok
}

func isCreate(op string) bool { return op == "create_node" || op == "create_rel" }

// check refuses a stream the write path cannot apply: an ID that is zero or
// negative, a life created twice, an op on an entity nothing created, a
// relationship to a node nothing created.
func (r *Result) check() error {
	if r == nil {
		return errors.New("importmap: nil result")
	}
	made := map[idKey]bool{}
	nodes := map[int64]bool{}
	for i, o := range r.Ops {
		if o.Entity != "node" && o.Entity != "rel" {
			return fmt.Errorf("importmap: op %d: entity %q", i, o.Entity)
		}
		if o.ID <= 0 || o.ID >= math.MaxInt64 {
			return fmt.Errorf("importmap: op %d: %s id %d is not a positive v4 ID", i, o.Entity, o.ID)
		}
		if o.Life < 1 {
			return fmt.Errorf("importmap: op %d: life %d", i, o.Life)
		}
		k := idKey{o.Entity, o.ID, o.Life}
		switch o.Op {
		case "create_node", "create_rel":
			if (o.Op == "create_node") != (o.Entity == "node") {
				return fmt.Errorf("importmap: op %d: %s on a %s", i, o.Op, o.Entity)
			}
			if made[k] {
				return fmt.Errorf("importmap: op %d: %s %d life %d is created twice", i, o.Entity, o.ID, o.Life)
			}
			if o.Op == "create_rel" && (!nodes[o.Start] || !nodes[o.End]) {
				return fmt.Errorf("importmap: op %d: rel %d runs between nodes %d and %d, not both created", i, o.ID, o.Start, o.End)
			}
			made[k] = true
			if o.Entity == "node" {
				nodes[o.ID] = true
			}
		case "correct", "unset", "end", "retract":
			if !made[k] {
				return fmt.Errorf("importmap: op %d: %s on %s %d life %d, which nothing created", i, o.Op, o.Entity, o.ID, o.Life)
			}
		case "add_label", "remove_label":
			if o.Entity != "node" || !made[k] {
				return fmt.Errorf("importmap: op %d: %s on %s %d life %d, which is not a created node", i, o.Op, o.Entity, o.ID, o.Life)
			}
		default:
			return fmt.Errorf("importmap: op %d: unknown op %q", i, o.Op)
		}
	}
	return nil
}

// Seed is the largest v4 ID of the stream: the allocator is seeded here so no
// new entity is ever minted on a v4 ID.
func (r *Result) Seed() uint64 {
	var m int64
	for _, o := range r.Ops {
		m = max(m, o.ID)
	}
	return uint64(m)
}

// plan is the stream as written: values the typed schema cannot hold are
// dropped (and reported), and the schema seeds the graph at genesis so a null
// first value or an import order never decides a key's kind.
type plan struct {
	ops     []Op
	node    []graph.PropertySchema
	rel     []graph.PropertySchema
	dropped []Ambiguity
}

func (r *Result) plan() (*plan, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	p := &plan{}
	kinds := map[[2]string]graph.ValueKind{}
	var order, nullOrder [][2]string
	nulls := map[[2]string]bool{}
	for _, o := range r.Ops {
		for _, k := range sortedPropKeys(o.Props) {
			v := o.Props[k]
			sk := [2]string{o.Entity, k}
			if v.Kind() == graph.KindNull {
				// A null cannot declare a key; a key that is only ever null is declared
				// as a string so the null stays present.
				if _, ok := nulls[sk]; !ok {
					nulls[sk] = true
					nullOrder = append(nullOrder, sk)
				}
				continue
			}
			if _, ok := kinds[sk]; !ok {
				kinds[sk] = v.Kind()
				order = append(order, sk)
			}
		}
	}
	for _, sk := range nullOrder {
		if _, ok := kinds[sk]; !ok {
			kinds[sk] = graph.KindString
			order = append(order, sk)
		}
	}
	for _, sk := range order {
		ps := graph.PropertySchema{Key: sk[1], Kind: kinds[sk]}
		if sk[0] == "node" {
			p.node = append(p.node, ps)
		} else {
			p.rel = append(p.rel, ps)
		}
	}
	for _, o := range r.Ops {
		if len(o.Props) > 0 {
			props := map[string]PropValue{}
			for k, v := range o.Props {
				want, ok := kinds[[2]string{o.Entity, k}]
				switch {
				case !ok:
					return nil, fmt.Errorf("importmap: key %q has no kind", k)
				case v.Kind() == graph.KindNull || fits(want, v.Value):
					props[k] = v
				default:
					p.dropped = append(p.dropped, Ambiguity{Entity: o.Entity, ID: o.ID, Version: o.Version, Key: k, Kind: KindKeyKindConflict})
				}
			}
			if len(props) == 0 {
				props = nil
			}
			if o.Op == "correct" && props == nil {
				continue // nothing left to correct
			}
			o.Props = props
		}
		p.ops = append(p.ops, o)
	}
	return p, nil
}

func sortedPropKeys(m map[string]PropValue) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// fits reports whether the engine takes v under a key of kind want: the same
// kind, or an exact numeric (an integral float into an int key, an int a float holds exactly).
func fits(want graph.ValueKind, v graph.Value) bool {
	if v.Kind() == want {
		return true
	}
	switch {
	case want == graph.KindInt && v.Kind() == graph.KindFloat:
		f, _ := v.AsFloat()
		return f == math.Trunc(f) && f >= -(1<<63) && f < 1<<63
	case want == graph.KindFloat && v.Kind() == graph.KindInt:
		n, _ := v.AsInt()
		f := float64(n)
		return f < 1<<63 && int64(f) == n
	}
	return false
}

// Config returns base with the allocator seeded above every v4 ID and the
// property schema of the stream. A graph written by Import must be opened with it
// (and reopened with it: the seed is stored, the schema is checked).
func (r *Result) Config(base graph.Config) (graph.Config, error) {
	p, err := r.plan()
	if err != nil {
		return graph.Config{}, err
	}
	merge := func(have, add []graph.PropertySchema) []graph.PropertySchema {
		out := slices.Clone(have)
		for _, a := range add {
			if !slices.ContainsFunc(out, func(h graph.PropertySchema) bool { return h.Key == a.Key }) {
				out = append(out, a)
			}
		}
		return out
	}
	base.Allocation.Seed = r.Seed()
	base.NodeProperties = merge(base.NodeProperties, append([]graph.PropertySchema{{Key: markerApplied, Kind: graph.KindInt}, {Key: markerPrint, Kind: graph.KindString}}, p.node...))
	base.RelProperties = merge(base.RelProperties, p.rel)
	return base, base.Validate()
}

// Import applies the op stream to g in batches (each one Update, under the
// store's stage-record limit) and returns the ID map. cfg must be the Config
// the graph was opened with (Result.Config). An import that stopped is
// resumed by calling Import again with the same stream: progress lives in a
// marker node written inside every batch, so nothing is applied twice.
func (r *Result) Import(ctx context.Context, g *graph.Graph, cfg graph.Config, o Options) (*IDMap, error) {
	p, err := r.plan()
	if err != nil {
		return nil, err
	}
	if cfg.Allocation.Seed < r.Seed() {
		return nil, fmt.Errorf("importmap: allocator seed %d is not above the largest v4 ID %d (use Result.Config)", cfg.Allocation.Seed, r.Seed())
	}
	if o.MaxOps <= 0 {
		o.MaxOps = 256
	}
	w := &writer{ctx: ctx, g: g, cfg: cfg, p: p, ids: newIDMap(), models: map[int64]*overlay{}, ends: map[int64][2]int64{}, skipped: map[idKey]bool{}}
	w.ids.Dropped = slices.Clone(p.dropped)
	sum := sha256.Sum256((&Result{Ops: p.ops}).OpsJSON())
	w.print = hex.EncodeToString(sum[:])
	if err := w.resume(); err != nil {
		return nil, err
	}
	for w.applied < len(p.ops) {
		n := w.cut(min(o.MaxOps, len(p.ops)-w.applied))
		for {
			err := w.batch(w.applied, n)
			if err == nil {
				break
			}
			if errors.Is(err, graph.ErrInvalidRequest) && n > 1 {
				n = w.cut((n + 1) / 2)
				continue
			}
			return nil, err
		}
		w.applied += n
		if o.AfterBatch != nil {
			if err := o.AfterBatch(w.applied); err != nil {
				return nil, fmt.Errorf("importmap: stopped after %d ops: %w", w.applied, err)
			}
		}
	}
	return w.ids, nil
}

type writer struct {
	ctx     context.Context
	g       *graph.Graph
	cfg     graph.Config
	p       *plan
	ids     *IDMap
	print   string
	applied int
	marker  int64 // the marker node's ID (0 = not written yet)
	next    int64 // the ID the next create is minted
	// What the engine now holds, as far as the writer needs it: the valid-time
	// support of every written entity (a relationship may only live where both
	// of its nodes do), the endpoints of every relationship, and the
	// relationships left unwritten.
	models  map[int64]*overlay
	ends    map[int64][2]int64
	skipped map[idKey]bool
	undo    []func()
}

// cut shortens a batch of n ops from applied so that no entity gets ops
// recorded at two different instants in one commit: a commit is one revision
// of the entity, and its evidence must be one instant.
func (w *writer) cut(n int) int {
	rec := map[idKey]int64{}
	for i := 0; i < n; i++ {
		o := w.p.ops[w.applied+i]
		k := idKey{o.Entity, o.ID, o.Life}
		if at, ok := rec[k]; ok && at != o.Recorded {
			return max(i, 1)
		}
		rec[k] = o.Recorded
	}
	return n
}

func (w *writer) rollback() {
	for i := len(w.undo) - 1; i >= 0; i-- {
		w.undo[i]()
	}
	w.undo = nil
}

// model returns the overlay of v5 entity id, journaling a copy for rollback.
func (w *writer) model(id int64) *overlay {
	m, ok := w.models[id]
	if !ok {
		m = &overlay{}
		w.models[id] = m
		w.undo = append(w.undo, func() { delete(w.models, id) })
		return m
	}
	old := slices.Clone(m.segs)
	for i := range old {
		old[i] = old[i].clone(old[i].lo, old[i].hi)
	}
	w.undo = append(w.undo, func() { m.segs = old })
	return m
}

func (w *writer) setID(k idKey, v5 int64) {
	prev, had := w.ids.latest[[2]any{k.kind, k.id}]
	w.ids.set(k, v5)
	w.undo = append(w.undo, func() {
		delete(w.ids.v5, k)
		delete(w.ids.v4, [2]any{k.kind, v5})
		if had {
			w.ids.latest[[2]any{k.kind, k.id}] = prev
		} else {
			delete(w.ids.latest, [2]any{k.kind, k.id})
		}
	})
}

// coverage is the valid time both nodes of a relationship hold, as ordered disjoint [lo, hi) pairs.
func (w *writer) coverage(a, b int64) [][2]int64 {
	var out [][2]int64
	for _, x := range w.models[a].segs {
		for _, y := range w.models[b].segs {
			lo, hi := max(x.lo, y.lo), min(x.hi, y.hi)
			if lo < hi {
				out = append(out, [2]int64{lo, hi})
			}
		}
	}
	slices.SortFunc(out, func(p, q [2]int64) int { return cmpInt(p[0], q[0]) })
	return out
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func edgeOf(c int64) Edge {
	switch c {
	case math.MinInt64:
		return NegInf
	case math.MaxInt64:
		return PosInf
	}
	return At(c)
}

// stage turns a stream op into the op the engine gets: v5 IDs for v4 IDs, a
// relationship clipped to the valid time of its two nodes (the engine refuses
// a relationship beyond them; v4 declared it and masked it in the effective
// view). ok is false when nothing is left to write. Create IDs are minted in
// stream order, so stage predicts them; the caller checks the engine agrees.
func (w *writer) stage(op Op) (out Op, parts []Scope, ok bool) {
	k := idKey{op.Entity, op.ID, op.Life}
	if w.skipped[k] {
		return op, nil, false
	}
	parts = []Scope{op.Scope}
	if op.Entity == "rel" && (op.Op == "create_rel" || op.Op == "correct") {
		var a, b int64
		if op.Op == "create_rel" {
			a, _ = w.ids.Latest("node", op.Start)
			b, _ = w.ids.Latest("node", op.End)
			op.Start, op.End = a, b
		} else {
			e := w.ends[w.ids.v5[k]]
			a, b = e[0], e[1]
		}
		cov := w.coverage(a, b)
		lo, hi := op.Scope.From.coord(), op.Scope.To.coord()
		parts = parts[:0]
		for _, c := range cov {
			if l, h := max(lo, c[0]), min(hi, c[1]); l < h {
				parts = append(parts, Scope{From: edgeOf(l), To: edgeOf(h)})
			}
		}
		if len(parts) == 0 {
			w.ids.Dropped = append(w.ids.Dropped, Ambiguity{Entity: op.Entity, ID: op.ID, Version: op.Version, Kind: KindRelOutsideNodes})
			if op.Op == "create_rel" {
				w.skipped[k] = true
				w.undo = append(w.undo, func() { delete(w.skipped, k) })
			}
			return op, nil, false
		}
		if len(parts) != 1 || parts[0] != op.Scope {
			w.ids.Dropped = append(w.ids.Dropped, Ambiguity{Entity: op.Entity, ID: op.ID, Version: op.Version, Kind: KindRelClipped})
		}
		op.Scope = parts[0]
		if len(parts) > 1 {
			op.region = parts
		}
	}
	return op, parts, true
}

// record notes the engine state after op (id is the v5 ID of a create).
func (w *writer) record(op Op, v5 int64, parts []Scope) {
	if isCreate(op.Op) {
		w.setID(idKey{op.Entity, op.ID, op.Life}, v5)
		if op.Op == "create_rel" {
			w.ends[v5] = [2]int64{op.Start, op.End}
			w.undo = append(w.undo, func() { delete(w.ends, v5) })
		}
	}
	m := w.model(v5)
	for _, p := range parts {
		o := op
		o.Scope = p
		m.apply(o)
	}
}

// resume reads the marker: where the import stopped and which ID comes next.
func (w *writer) resume() error {
	s, err := w.g.OpenSnapshot(w.ctx, graph.Fresh())
	if err != nil {
		return err
	}
	defer s.Close()
	all, err := temporal.All(w.cfg.DefaultAxis.Axis())
	if err != nil {
		return err
	}
	known, err := temporal.All(w.cfg.KnowledgeAxis.Axis())
	if err != nil {
		return err
	}
	sel := graph.Selector{View: graph.ViewDeclared, Valid: all, Known: known, Labels: []string{markerLabel}}
	pg, err := s.Scan(w.ctx, graph.ScanRequest{Selector: sel, Projection: graph.Projection{Properties: []string{markerApplied, markerPrint}}, Paging: graph.PageLimits{Rows: 2, MaxBytes: graph.DefaultMaxBytes}})
	if err != nil {
		return err
	}
	if len(pg.Rows) == 0 {
		any, err := s.Scan(w.ctx, graph.ScanRequest{Selector: graph.Selector{View: graph.ViewDeclared, Valid: all, Known: known}, Projection: graph.Projection{}, Paging: graph.PageLimits{Rows: 1, MaxBytes: graph.DefaultMaxBytes}})
		if err != nil {
			return err
		}
		if len(any.Rows) > 0 {
			return errors.New("importmap: the graph holds data and no import marker; Import needs a new graph")
		}
		return nil
	}
	row := pg.Rows[0]
	n, _ := row.Props[markerApplied].AsInt()
	fp, _ := row.Props[markerPrint].AsString()
	if fp != w.print {
		return errors.New("importmap: the graph holds an import of another op stream")
	}
	if n < 0 || int(n) > len(w.p.ops) {
		return fmt.Errorf("importmap: marker says %d ops applied of %d", n, len(w.p.ops))
	}
	w.marker = int64(row.Ref.Node())
	w.next = w.marker + 1
	// Replay the committed prefix without a transaction: IDs and clipping are a
	// function of the stream, so the writer's picture is rebuilt exactly.
	for _, op := range w.p.ops[:int(n)] {
		w.step(op, nil)
	}
	w.undo = nil
	w.applied = int(n)
	return nil
}

// step stages one op and, with a transaction, writes it.
func (w *writer) step(op Op, tx graph.Tx) error {
	k := idKey{op.Entity, op.ID, op.Life}
	if !isCreate(op.Op) {
		v5, ok := w.ids.V5(k.kind, k.id, k.life)
		if !ok && !w.skipped[k] {
			return fmt.Errorf("importmap: %s %d life %d has no v5 entity", op.Entity, op.ID, op.Life)
		}
		op.ID = v5
	}
	if op.Op == "create_rel" {
		_, ok1 := w.ids.Latest("node", op.Start)
		_, ok2 := w.ids.Latest("node", op.End)
		if !ok1 || !ok2 {
			return fmt.Errorf("importmap: rel %d runs between nodes %d and %d, not both written", op.ID, op.Start, op.End)
		}
	}
	// stage reads the v4 identity of a rel op; restore it for the skip check.
	staged := op
	if !isCreate(op.Op) {
		staged.ID = k.id
	}
	staged, parts, ok := w.stage(staged)
	if !ok {
		return nil
	}
	if !isCreate(op.Op) {
		staged.ID = op.ID
	}
	v5 := staged.ID
	if isCreate(op.Op) {
		v5 = w.next
		w.next++
		w.undo = append(w.undo, func() { w.next-- })
	}
	if tx != nil {
		gop, err := staged.Graph(w.cfg)
		if err != nil {
			return err
		}
		id, err := applyOp(tx, gop)
		if err != nil {
			return fmt.Errorf("importmap: %s on %s (v4 %d): %w", op.Op, op.Entity, k.id, err)
		}
		if isCreate(op.Op) && id != v5 {
			return fmt.Errorf("importmap: the allocator minted %d, expected %d", id, v5)
		}
	}
	w.record(staged, v5, parts)
	return nil
}

// batch applies ops [from, from+n) and the marker in one Update.
func (w *writer) batch(from, n int) error {
	w.undo = nil
	marker0, next0 := w.marker, w.next
	dropped0 := len(w.ids.Dropped)
	_, err := w.g.Update(w.ctx, func(tx graph.Tx) error {
		w.rollback()
		w.marker, w.next = marker0, next0
		w.ids.Dropped = w.ids.Dropped[:dropped0]
		all, err := temporal.All(w.cfg.DefaultAxis.Axis())
		if err != nil {
			return err
		}
		if w.marker == 0 {
			id, err := tx.CreateNode(graph.CreateNode{Labels: []string{markerLabel}, Props: map[string]graph.Value{markerApplied: graph.Int(0), markerPrint: graph.String(w.print)}, Scope: all})
			if err != nil {
				return err
			}
			w.marker, w.next = int64(id), int64(id)+1
		}
		for _, op := range w.p.ops[from : from+n] {
			if err := w.step(op, tx); err != nil {
				return err
			}
		}
		ref, err := graph.NodeRef(types.NodeID(w.marker))
		if err != nil {
			return err
		}
		return tx.Correct(graph.Correct{Target: ref, Set: map[string]graph.Value{markerApplied: graph.Int(int64(from + n))}, Scope: all})
	})
	if err != nil {
		w.rollback()
		w.marker, w.next = marker0, next0
		w.ids.Dropped = w.ids.Dropped[:dropped0]
		return err
	}
	w.undo = nil
	return nil
}

func applyOp(tx graph.Tx, op graph.Op) (int64, error) {
	switch x := op.(type) {
	case graph.CreateNode:
		id, err := tx.CreateNode(x)
		return int64(id), err
	case graph.CreateRel:
		id, err := tx.CreateRel(x)
		return int64(id), err
	case graph.Correct:
		return 0, tx.Correct(x)
	case graph.Unset:
		return 0, tx.Unset(x)
	case graph.AddLabel:
		return 0, tx.AddLabel(x)
	case graph.RemoveLabel:
		return 0, tx.RemoveLabel(x)
	case graph.End:
		return 0, tx.End(x)
	case graph.Retract:
		return 0, tx.Retract(x)
	}
	return 0, fmt.Errorf("importmap: op %T", op)
}
