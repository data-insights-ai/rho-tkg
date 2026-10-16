// Package v5adapter plugs the native v5/pkg/graph engine into the conformance
// suite (native profile only). The suite speaks v4-shaped neutral types; this
// file is the one place that maps them onto typed ops:
//
//   - instants map to temporal.Scope on the graph default axis (0 is "unset":
//     [from,+inf), (-inf,to), or All);
//   - a pin is an index into the cuts the adapter has read (CommittedTx);
//   - the engine schema is declared at genesis, so the adapter declares a fixed
//     key list, one property per (key, value kind) named "key#kind", and the
//     unique claims of GetOrCreateByKey as "key#kind@label";
//   - "deleted" and "retracted" are a reserved boolean property plus the engine
//     End/Retract op, because the v4 record doors treat a deleted entity as
//     absent while the valid-time doors still see its past.
//
// Doors that need label or equality index pages (Scan, WP-08) skip the running
// case with that reason; they never pretend.
package v5adapter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	conformance "github.com/data-insights-ai/rho-tkg/tools/v5-conformance"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Adapter opens v5 graphs.
type Adapter struct{}

var _ conformance.Adapter = Adapter{}
var _ conformance.ProfileAdapter = Adapter{}

// Profiles declares native only: caller stamps, ms pins and ID-keyset cursors
// belong to the compat facade (PLAN 8a).
func (Adapter) Profiles() []conformance.Profile { return []conformance.Profile{conformance.Native} }

var kindTags = []string{"s", "i", "f", "b"}

const deletedProp = "~deleted#b"

var nodeKeys = strings.Fields("v w est k idx a z s keep b id i gen extra n other x y p q r name")
var relKeys = strings.Fields("w est idx v k")

// uniqueClaims are the (label, key, kind) triples GetOrCreateByKey may claim.
var uniqueClaims = [][3]string{{"U", "id", "s"}, {"W", "id", "s"}, {"U", "other", "s"}, {"N", "k", "s"}, {"N", "k", "i"}, {"C", "id", "s"}, {"K", "k", "i"}}

func schema(keys []string) []graph.PropertySchema {
	kinds := map[string]graph.ValueKind{"s": graph.KindString, "i": graph.KindInt, "f": graph.KindFloat, "b": graph.KindBool}
	var out []graph.PropertySchema
	for _, k := range keys {
		for _, t := range kindTags {
			out = append(out, graph.PropertySchema{Key: k + "#" + t, Kind: kinds[t]})
		}
	}
	return append(out, graph.PropertySchema{Key: deletedProp, Kind: graph.KindBool})
}

func nodeSchema() []graph.PropertySchema {
	out := schema(nodeKeys)
	for _, u := range uniqueClaims {
		k := graph.KindString
		if u[2] == "i" {
			k = graph.KindInt
		}
		out = append(out, graph.PropertySchema{Key: u[1] + "#" + u[2] + "@" + u[0], Kind: k, Unique: true})
	}
	return out
}

// Config builds the graph.Config for the suite options. Exported so a test can
// check the mapping without opening a store.
func Config(t testing.TB, o conformance.Options) (graph.Config, error) {
	gid := types.GraphID{1}
	b, err := types.NewDefaultAxisBinding(gid, temporal.AxisID{1}, temporal.Limits{})
	if err != nil {
		return graph.Config{}, err
	}
	k, err := types.NewDefaultAxisBinding(gid, temporal.AxisID{2}, temporal.Limits{})
	if err != nil {
		return graph.Config{}, err
	}
	c := graph.Config{GraphID: gid, DefaultAxis: b, KnowledgeAxis: k, Durability: graph.DurabilityCommit,
		NodeProperties: nodeSchema(), RelProperties: schema(relKeys)}
	if o.Dir == "" {
		c.Memory, c.Durability = true, graph.DurabilityBuffered
	} else {
		c.Dir = o.Dir
	}
	return c, nil
}

// notImplemented joins both sentinels so errors.Is matches either.
type notImplemented struct{ err error }

func (e notImplemented) Error() string { return e.err.Error() }
func (e notImplemented) Unwrap() []error {
	return []error{e.err, conformance.ErrNotImplemented}
}

// Open implements conformance.Adapter.
func (Adapter) Open(t testing.TB, o conformance.Options) (conformance.Graph, error) {
	cfg, err := Config(t, o)
	if err != nil {
		return nil, err
	}
	g, err := graph.Open(context.Background(), cfg)
	if errors.Is(err, graph.ErrNotImplemented) {
		if os.Getenv("V5_CONFORMANCE_STRICT") != "" { // red proof: a missing engine fails instead of skipping
			return nil, err
		}
		return nil, notImplemented{err}
	}
	if err != nil {
		return nil, err
	}
	w := &Graph{t: t, g: g, cfg: cfg}
	t.Cleanup(func() { _ = w.g.Close() })
	return w, nil
}

// Graph maps the suite doors onto *graph.Graph.
type Graph struct {
	t    testing.TB
	mu   sync.Mutex
	g    *graph.Graph
	cfg  graph.Config
	pins []graph.Cut
}

var _ conformance.Graph = (*Graph)(nil)

func (w *Graph) Close() error { return mapErr(w.g.Close()) }

func (w *Graph) Reopen() error {
	if err := w.g.Close(); err != nil {
		return mapErr(err)
	}
	g, err := graph.Open(context.Background(), w.cfg)
	if err != nil {
		return mapErr(err)
	}
	w.g = g
	return nil
}

// skipScan ends the running case: the door needs index pages the engine lacks.
func (w *Graph) skipScan(door string) {
	w.t.Helper()
	w.t.Skipf("door %s needs the label/equality index pages (WP-08): %v", door, graph.ErrNotImplemented)
}

// ---- errors ----

type joined struct{ class, err error }

func (e joined) Error() string   { return e.err.Error() }
func (e joined) Unwrap() []error { return []error{e.class, e.err} }

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	for _, c := range []struct{ from, to error }{
		{graph.ErrNotFound, conformance.ErrNotFound}, {graph.ErrClosed, conformance.ErrClosed},
		{graph.ErrInvalidID, conformance.ErrInvalidArgument}, {graph.ErrInvalidRequest, conformance.ErrInvalidArgument},
		{graph.ErrInvalidValue, conformance.ErrInvalidArgument}, {graph.ErrInvalidOp, conformance.ErrInvalidArgument},
		{graph.ErrInvalidScope, conformance.ErrInvalidArgument}, {graph.ErrUniqueViolation, conformance.ErrInvalidArgument},
	} {
		if errors.Is(err, c.from) {
			return joined{c.to, err}
		}
	}
	return err
}

func bad(format string, a ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{conformance.ErrInvalidArgument}, a...)...)
}

// ---- time ----

func (w *Graph) axis() temporal.Axis { return w.cfg.DefaultAxis.Axis() }

func (w *Graph) bound(ms int64, inclusive bool) temporal.Bound {
	p, err := types.InstantPosition(w.axis(), types.Instant(ms), w.cfg.Limits)
	if err != nil {
		panic(err) // an int64 millisecond always fits the configured axis
	}
	b, err := temporal.FiniteBound(p, inclusive)
	if err != nil {
		panic(err)
	}
	return b
}

func (w *Graph) span(lo, hi temporal.Bound) temporal.Scope {
	s, err := temporal.Span(w.axis(), lo, hi, w.cfg.Limits)
	if err != nil {
		panic(err)
	}
	return s
}

func (w *Graph) all() temporal.Scope {
	s, err := temporal.All(w.axis())
	if err != nil {
		panic(err)
	}
	return s
}

func (w *Graph) known() temporal.Scope {
	s, err := temporal.All(w.cfg.KnowledgeAxis.Axis())
	if err != nil {
		panic(err)
	}
	return s
}

func (w *Graph) point(ms int64) temporal.Scope {
	p, err := types.InstantPosition(w.axis(), types.Instant(ms), w.cfg.Limits)
	if err != nil {
		panic(err)
	}
	s, err := temporal.Point(p)
	if err != nil {
		panic(err)
	}
	return s
}

// interval maps the suite's Interval (0 = unset) to a scope; from > 0 and to
// must follow it.
func (w *Graph) interval(iv conformance.Interval) (temporal.Scope, error) {
	switch {
	case iv.From == 0 && iv.To == 0:
		return w.all(), nil
	case iv.To != 0 && (iv.From == 0 || iv.To <= iv.From) && iv.From != 0:
		return temporal.Scope{}, fmt.Errorf("%w: valid from %d to %d", conformance.ErrInvalidTimeRange, iv.From, iv.To)
	case iv.From == 0:
		return w.span(temporal.NegativeInfinity(), w.bound(iv.To, false)), nil
	case iv.To == 0:
		return w.span(w.bound(iv.From, true), temporal.PositiveInfinity()), nil
	}
	return w.span(w.bound(iv.From, true), w.bound(iv.To, false)), nil
}

func (w *Graph) ms(b temporal.Bound) int64 {
	p, ok := b.Position()
	if !ok {
		return 0
	}
	i, err := types.InstantFromPosition(p, w.axis(), w.cfg.Limits)
	if err != nil {
		return 0
	}
	return int64(i)
}

// ---- values ----

func kindTag(v graph.Value) string {
	switch v.Kind() {
	case graph.KindInt:
		return "i"
	case graph.KindFloat:
		return "f"
	case graph.KindBool:
		return "b"
	}
	return "s"
}

func toValue(x any) (graph.Value, error) {
	switch v := x.(type) {
	case nil:
		return graph.Null(), nil
	case string:
		return graph.String(v), nil
	case bool:
		return graph.Bool(v), nil
	case int:
		return graph.Int(int64(v)), nil
	case int8:
		return graph.Int(int64(v)), nil
	case int16:
		return graph.Int(int64(v)), nil
	case int32:
		return graph.Int(int64(v)), nil
	case int64:
		return graph.Int(v), nil
	case uint:
		return uintValue(uint64(v))
	case uint8:
		return graph.Int(int64(v)), nil
	case uint16:
		return graph.Int(int64(v)), nil
	case uint32:
		return graph.Int(int64(v)), nil
	case uint64:
		return uintValue(v)
	case float32:
		return floatValue(float64(v))
	case float64:
		return floatValue(v)
	}
	return graph.Value{}, bad("unsupported property value %T", x)
}

func uintValue(u uint64) (graph.Value, error) {
	if u > math.MaxInt64 {
		return graph.Value{}, bad("unsigned value above int64")
	}
	return graph.Int(int64(u)), nil
}

func floatValue(f float64) (graph.Value, error) {
	v, err := graph.Float(f)
	if err != nil {
		return graph.Value{}, bad("%v", err)
	}
	return v, nil
}

func fromValue(v graph.Value) any {
	switch v.Kind() {
	case graph.KindString:
		s, _ := v.AsString()
		return s
	case graph.KindBool:
		b, _ := v.AsBool()
		return b
	case graph.KindInt:
		i, _ := v.AsInt()
		return i
	case graph.KindFloat:
		f, _ := v.AsFloat()
		return f
	}
	return nil
}

func propName(key string, v graph.Value) string { return key + "#" + kindTag(v) }

func (w *Graph) props(in map[string]any) (map[string]graph.Value, error) {
	out := make(map[string]graph.Value, len(in))
	for k, x := range in {
		if k == "" || strings.ContainsAny(k, "#@~") {
			return nil, bad("property key %q", k)
		}
		v, err := toValue(x)
		if err != nil {
			return nil, err
		}
		out[propName(k, v)] = v
	}
	return out, nil
}

// readNames are the engine property names a read projects.
func (w *Graph) readNames(rel bool) []string {
	ps := w.cfg.NodeProperties
	if rel {
		ps = w.cfg.RelProperties
	}
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.Key
	}
	return out
}

func userKey(name string) (string, bool) {
	if strings.HasPrefix(name, "~") {
		return "", false
	}
	k, _, _ := strings.Cut(name, "#")
	return k, true
}

func deleted(e graph.Entity) bool {
	v, ok := e.Props[deletedProp]
	b, _ := v.AsBool()
	return ok && b
}

func (w *Graph) node(e graph.Entity) conformance.Node {
	n := conformance.Node{ID: int64(e.Ref.Node()), Labels: slices.Clone(e.Labels), Props: map[string]any{}}
	for name, v := range e.Props {
		if k, ok := userKey(name); ok && !v.IsNull() {
			n.Props[k] = fromValue(v)
		}
	}
	lo, hi, _ := e.Valid.Bounds()
	n.ValidFrom, n.ValidTo = w.ms(lo), w.ms(hi)
	return n
}

func (w *Graph) rel(e graph.Entity) conformance.Rel {
	n := w.node(e)
	return conformance.Rel{ID: int64(e.Ref.Rel()), Type: e.RelType, From: int64(e.From), To: int64(e.To), Props: n.Props, ValidFrom: n.ValidFrom, ValidTo: n.ValidTo}
}

// ---- reads ----

func (w *Graph) ref(id int64, rel bool) (graph.Ref, error) {
	if id <= 0 {
		return graph.Ref{}, bad("ID %d", id)
	}
	if rel {
		return graph.RelRef(types.RelID(id))
	}
	return graph.NodeRef(types.NodeID(id))
}

func (w *Graph) selector(valid temporal.Scope) graph.Selector {
	return graph.Selector{View: graph.ViewDeclared, Valid: valid, Known: w.known()}
}

func (w *Graph) lookup(ctx context.Context, id int64, rel bool, spec graph.SnapshotSpec, valid temporal.Scope) ([]graph.Entity, error) {
	ref, err := w.ref(id, rel)
	if err != nil {
		return nil, err
	}
	s, err := w.g.OpenSnapshot(ctx, spec)
	if err != nil {
		return nil, mapErr(err)
	}
	defer s.Close()
	req := graph.LookupRequest{Selector: w.selector(valid), Projection: graph.Projection{Properties: w.readNames(rel)}, Paging: graph.PageLimits{Rows: graph.MaxPageRows, MaxBytes: 8 << 20}}
	if rel {
		req.Rels = []types.RelID{ref.Rel()}
	} else {
		req.Nodes = []types.NodeID{ref.Node()}
	}
	return w.drain(s.Lookup(ctx, req))(s, ctx)
}

func (w *Graph) drain(p graph.Page, err error) func(*graph.Snapshot, context.Context) ([]graph.Entity, error) {
	return func(s *graph.Snapshot, ctx context.Context) ([]graph.Entity, error) {
		var rows []graph.Entity
		for {
			if err != nil {
				return nil, mapErr(err)
			}
			rows = append(rows, p.Rows...)
			if p.Next.IsZero() {
				return rows, nil
			}
			p, err = s.Resume(ctx, p.Next, graph.PageLimits{Rows: graph.MaxPageRows, MaxBytes: 8 << 20})
		}
	}
}

// current is the live entity: the newest row at the newest cut, not deleted.
func (w *Graph) current(ctx context.Context, id int64, rel bool) (graph.Entity, error) {
	rows, err := w.lookup(ctx, id, rel, graph.Fresh(), w.all())
	if err != nil {
		return graph.Entity{}, err
	}
	return liveRow(rows)
}

func liveRow(rows []graph.Entity) (graph.Entity, error) {
	if len(rows) == 0 || deleted(rows[len(rows)-1]) {
		return graph.Entity{}, conformance.ErrNotFound
	}
	return rows[len(rows)-1], nil
}

func (w *Graph) cutOf(pin int64) (graph.Cut, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if pin < 1 || pin > int64(len(w.pins)) {
		return graph.Cut{}, bad("unknown pin %d", pin)
	}
	return w.pins[pin-1], nil
}

func (w *Graph) CommittedTx() (int64, error) {
	s, err := w.g.OpenSnapshot(context.Background(), graph.Fresh())
	if err != nil {
		return 0, mapErr(err)
	}
	defer s.Close()
	c := s.Cut()
	w.mu.Lock()
	defer w.mu.Unlock()
	if n := len(w.pins); n > 0 && string(w.pins[n-1].Bytes()) == string(c.Bytes()) {
		return int64(n), nil
	}
	w.pins = append(w.pins, c)
	return int64(len(w.pins)), nil
}

func (w *Graph) GetNode(ctx context.Context, id int64) (conformance.Node, error) {
	e, err := w.current(ctx, id, false)
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	return w.node(e), nil
}

func (w *Graph) GetRel(ctx context.Context, id int64) (conformance.Rel, error) {
	e, err := w.current(ctx, id, true)
	if err != nil {
		return conformance.Rel{}, mapErr(err)
	}
	return w.rel(e), nil
}

func (w *Graph) asOf(id, pin int64, rel bool) (graph.Entity, error) {
	c, err := w.cutOf(pin)
	if err != nil {
		return graph.Entity{}, err
	}
	rows, err := w.lookup(context.Background(), id, rel, graph.At(c), w.all())
	if err != nil {
		return graph.Entity{}, err
	}
	e, err := liveRow(rows)
	if err != nil {
		return graph.Entity{}, fmt.Errorf("%w: %v", conformance.ErrNoVersionAsOf, err)
	}
	return e, nil
}

func (w *Graph) NodeAsOf(id int64, pin int64) (conformance.Node, error) {
	e, err := w.asOf(id, pin, false)
	if err != nil {
		return conformance.Node{}, err
	}
	return w.node(e), nil
}

func (w *Graph) RelAsOf(id int64, pin int64) (conformance.Rel, error) {
	e, err := w.asOf(id, pin, true)
	if err != nil {
		return conformance.Rel{}, err
	}
	return w.rel(e), nil
}

func (w *Graph) at(id int64, rel bool, spec graph.SnapshotSpec, valid int64) (graph.Entity, error) {
	rows, err := w.lookup(context.Background(), id, rel, spec, w.all())
	if err != nil {
		return graph.Entity{}, err
	}
	for _, r := range rows {
		lo, hi, _ := r.Valid.Bounds()
		_, loFin := lo.Position()
		_, hiFin := hi.Position()
		if (!loFin || w.ms(lo) <= valid) && (!hiFin || valid < w.ms(hi)) {
			return r, nil
		}
	}
	return graph.Entity{}, conformance.ErrNoVersionValidAt
}

func (w *Graph) NodeAt(id int64, validAt int64) (conformance.Node, error) {
	e, err := w.at(id, false, graph.Fresh(), validAt)
	if err != nil {
		return conformance.Node{}, err
	}
	return w.node(e), nil
}

func (w *Graph) NodeAtTx(id int64, validAt, tx int64) (conformance.Node, error) {
	c, err := w.cutOf(tx)
	if err != nil {
		return conformance.Node{}, err
	}
	e, err := w.at(id, false, graph.At(c), validAt)
	if err != nil {
		return conformance.Node{}, err
	}
	return w.node(e), nil
}

func (w *Graph) RelAtTx(id int64, validAt, tx int64) (conformance.Rel, error) {
	c, err := w.cutOf(tx)
	if err != nil {
		return conformance.Rel{}, err
	}
	e, err := w.at(id, true, graph.At(c), validAt)
	if err != nil {
		return conformance.Rel{}, err
	}
	return w.rel(e), nil
}

// history is every revision except the newest, oldest first.
func (w *Graph) history(id int64, rel bool) ([]graph.Entity, error) {
	ref, err := w.ref(id, rel)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	s, err := w.g.OpenSnapshot(ctx, graph.Fresh())
	if err != nil {
		return nil, mapErr(err)
	}
	defer s.Close()
	rows, err := w.drain(s.Timeline(ctx, graph.TimelineRequest{Ref: ref, Selector: w.selector(w.all()), Projection: graph.Projection{Properties: w.readNames(rel), Revisions: true},
		Paging: graph.PageLimits{Rows: graph.MaxPageRows, MaxBytes: 8 << 20}}))(s, ctx)
	if err != nil {
		return nil, err
	}
	// One version per revision: the last row of each revision's rows.
	var versions []graph.Entity
	for i, r := range rows {
		if i+1 == len(rows) || rows[i+1].Revision != r.Revision {
			versions = append(versions, r)
		}
	}
	if len(versions) == 0 {
		return nil, conformance.ErrNotFound
	}
	return versions[:len(versions)-1], nil
}

func (w *Graph) NodeHistory(id int64) ([]conformance.Node, error) {
	rows, err := w.history(id, false)
	out := make([]conformance.Node, len(rows))
	for i, r := range rows {
		out[i] = w.node(r)
	}
	return out, err
}

func (w *Graph) RelHistory(id int64) ([]conformance.Rel, error) {
	rows, err := w.history(id, true)
	out := make([]conformance.Rel, len(rows))
	for i, r := range rows {
		out[i] = w.rel(r)
	}
	return out, err
}

// ---- writes ----

func (w *Graph) update(ctx context.Context, fn func(graph.Tx) error) error {
	_, err := w.g.Update(ctx, fn)
	return mapErr(err)
}

func (w *Graph) writeNode(tx graph.Tx, s conformance.NodeSpec) (types.NodeID, graph.Entity, error) {
	if s.TxFrom != 0 {
		return 0, graph.Entity{}, fmt.Errorf("%w: caller instants are not part of the native profile", conformance.ErrTxOrder)
	}
	if len(s.Labels) == 0 || slices.Contains(s.Labels, "") {
		return 0, graph.Entity{}, bad("a node needs non-empty labels")
	}
	scope, err := w.interval(s.Valid)
	if err != nil {
		return 0, graph.Entity{}, err
	}
	props, err := w.props(s.Props)
	if err != nil {
		return 0, graph.Entity{}, err
	}
	id, err := tx.CreateNode(graph.CreateNode{Labels: s.Labels, Props: props, Scope: scope})
	if err != nil {
		return 0, graph.Entity{}, err
	}
	ref, _ := graph.NodeRef(id)
	return id, graph.Entity{Ref: ref, Labels: slices.Clone(s.Labels), Props: props, Valid: scope}, nil
}

func (w *Graph) AddNode(ctx context.Context, s conformance.NodeSpec) (conformance.Node, error) {
	var ent graph.Entity
	err := w.update(ctx, func(tx graph.Tx) (err error) {
		_, ent, err = w.writeNode(tx, s)
		return
	})
	if err != nil {
		return conformance.Node{}, err
	}
	return w.node(ent), nil
}

// retarget replaces the properties of kinds that the new values supersede.
func supersede(cur graph.Entity, props map[string]graph.Value) []string {
	var drop []string
	for name := range cur.Props {
		k, ok := userKey(name)
		if !ok || cur.Props[name].IsNull() {
			continue
		}
		if _, same := props[name]; same {
			continue
		}
		for n := range props {
			if nk, _ := userKey(n); nk == k && n != name {
				drop = append(drop, name)
			}
		}
	}
	return drop
}

func (w *Graph) updateEntity(ctx context.Context, id int64, rel bool, in map[string]any, txFrom int64) (graph.Entity, error) {
	if txFrom != 0 {
		return graph.Entity{}, fmt.Errorf("%w: caller instants are not part of the native profile", conformance.ErrTxOrder)
	}
	props, err := w.props(in)
	if err != nil {
		return graph.Entity{}, err
	}
	ref, err := w.ref(id, rel)
	if err != nil {
		return graph.Entity{}, mapErr(err)
	}
	err = w.update(ctx, func(tx graph.Tx) error {
		cur, err := w.current(ctx, id, rel)
		if err != nil {
			return err
		}
		scope := w.fromNow(cur.Valid)
		for _, name := range supersede(cur, props) {
			if err := tx.Unset(graph.Unset{Target: ref, Key: name, Scope: scope}); err != nil {
				return err
			}
		}
		for _, name := range sortedNames(props) {
			if err := tx.Set(graph.Set{Target: ref, Key: name, Value: props[name], Scope: scope}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return graph.Entity{}, err
	}
	return w.current(ctx, id, rel)
}

// fromNow narrows a version's valid span to start at the wall clock, so an update
// is a new version that leaves the earlier valid time to the earlier version.
func (w *Graph) fromNow(v temporal.Scope) temporal.Scope {
	lo, hi, _ := v.Bounds()
	now := time.Now().UnixMilli()
	_, loFin := lo.Position()
	_, hiFin := hi.Position()
	if (loFin && w.ms(lo) >= now) || (hiFin && w.ms(hi) <= now) {
		return v
	}
	return w.span(w.bound(now, true), hi)
}

func sortedNames(m map[string]graph.Value) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	return ks
}

func (w *Graph) UpdateNode(ctx context.Context, id int64, props map[string]any, txFrom int64) (conformance.Node, error) {
	e, err := w.updateEntity(ctx, id, false, props, txFrom)
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	return w.node(e), nil
}

func (w *Graph) UpdateRel(ctx context.Context, id int64, props map[string]any, txFrom int64) (conformance.Rel, error) {
	e, err := w.updateEntity(ctx, id, true, props, txFrom)
	if err != nil {
		return conformance.Rel{}, mapErr(err)
	}
	return w.rel(e), nil
}

// end deletes (End at the wall clock) or retracts (Retract over all time) and
// marks the entity so the record doors treat it as absent.
func (w *Graph) end(ctx context.Context, id int64, rel, retract bool, txTo int64) error {
	if txTo != 0 {
		return fmt.Errorf("%w: caller instants are not part of the native profile", conformance.ErrTxOrder)
	}
	ref, err := w.ref(id, rel)
	if err != nil {
		return mapErr(err)
	}
	return w.update(ctx, func(tx graph.Tx) error {
		cur, err := w.current(ctx, id, rel)
		if err != nil {
			return err
		}
		if !retract {
			// The flag is written while the entity is still present, then the End closes it.
			if err = tx.Set(graph.Set{Target: ref, Key: deletedProp, Value: graph.Bool(true), Scope: cur.Valid}); err != nil {
				return err
			}
			return tx.End(graph.End{Target: ref, Scope: w.span(temporal.NegativeInfinity(), w.bound(time.Now().UnixMilli(), false))})
		}
		return tx.Retract(graph.Retract{Target: ref, Scope: w.all()})
	})
}

func (w *Graph) DeleteNode(ctx context.Context, id int64, txTo int64) error {
	return w.end(ctx, id, false, false, txTo)
}
func (w *Graph) DeleteRel(ctx context.Context, id int64, txTo int64) error {
	return w.end(ctx, id, true, false, txTo)
}
func (w *Graph) RetractNode(ctx context.Context, id int64, txTo int64) error {
	return w.end(ctx, id, false, true, txTo)
}
func (w *Graph) RetractRel(ctx context.Context, id int64, txTo int64) error {
	return w.end(ctx, id, true, true, txTo)
}

func (w *Graph) AddRel(ctx context.Context, s conformance.RelSpec) (conformance.Rel, error) {
	var ent graph.Entity
	err := w.update(ctx, func(tx graph.Tx) error {
		var err error
		ent, err = w.writeRel(tx, s)
		return err
	})
	if err != nil {
		return conformance.Rel{}, err
	}
	return w.rel(ent), nil
}

func (w *Graph) writeRel(tx graph.Tx, s conformance.RelSpec) (graph.Entity, error) {
	if s.TxFrom != 0 {
		return graph.Entity{}, fmt.Errorf("%w: caller instants are not part of the native profile", conformance.ErrTxOrder)
	}
	if s.Type == "" || s.From <= 0 || s.To <= 0 || s.From == s.To {
		return graph.Entity{}, bad("relationship %q %d->%d", s.Type, s.From, s.To)
	}
	scope, err := w.interval(s.Valid)
	if err != nil {
		return graph.Entity{}, err
	}
	props, err := w.props(s.Props)
	if err != nil {
		return graph.Entity{}, err
	}
	id, err := tx.CreateRel(graph.CreateRel{Type: s.Type, From: types.NodeID(s.From), To: types.NodeID(s.To), Props: props, Scope: scope})
	if err != nil {
		return graph.Entity{}, err
	}
	ref, _ := graph.RelRef(id)
	return graph.Entity{Ref: ref, RelType: s.Type, From: types.NodeID(s.From), To: types.NodeID(s.To), Props: props, Valid: scope}, nil
}

func (w *Graph) GetOrCreateByKey(ctx context.Context, label, key string, value any, extra map[string]any) (conformance.Node, bool, error) {
	if label == "" || key == "" {
		return conformance.Node{}, false, bad("label and key are required")
	}
	v, err := toValue(value)
	if err != nil || v.IsNull() {
		return conformance.Node{}, false, bad("claim value %T", value)
	}
	props, err := w.props(extra)
	if err != nil {
		return conformance.Node{}, false, err
	}
	var id types.NodeID
	var created bool
	err = w.update(ctx, func(tx graph.Tx) (err error) {
		id, created, err = tx.GetOrCreate(graph.GetOrCreate{Label: label, Key: key + "#" + kindTag(v) + "@" + label, Value: v, Props: props, Scope: w.span(w.bound(time.Now().UnixMilli(), true), temporal.PositiveInfinity())})
		return
	})
	if err != nil {
		return conformance.Node{}, false, err
	}
	n, err := w.GetNode(ctx, int64(id))
	return n, created, err
}

func (w *Graph) correct(ctx context.Context, id int64, rel bool, from, to int64, in map[string]any, txFrom int64) (graph.Entity, error) {
	if txFrom != 0 {
		return graph.Entity{}, fmt.Errorf("%w: caller instants are not part of the native profile", conformance.ErrTxOrder)
	}
	if from <= 0 || (to != 0 && to <= from) {
		return graph.Entity{}, fmt.Errorf("%w: [%d,%d)", conformance.ErrInvalidTimeRange, from, to)
	}
	props, err := w.props(in)
	if err != nil {
		return graph.Entity{}, err
	}
	if len(props) == 0 {
		props = map[string]graph.Value{deletedProp: graph.Bool(false)}
	}
	ref, err := w.ref(id, rel)
	if err != nil {
		return graph.Entity{}, mapErr(err)
	}
	hi := temporal.PositiveInfinity()
	if to != 0 {
		hi = w.bound(to, false)
	}
	err = w.update(ctx, func(tx graph.Tx) error {
		if _, err := w.current(ctx, id, rel); err != nil {
			return err
		}
		return tx.Correct(graph.Correct{Target: ref, Set: props, Scope: w.span(w.bound(from, true), hi)})
	})
	if err != nil {
		return graph.Entity{}, err
	}
	return w.at(id, rel, graph.Fresh(), from)
}

func (w *Graph) SetNodeVersionInterval(ctx context.Context, id int64, from, to int64, props map[string]any, txFrom int64) (conformance.Node, error) {
	e, err := w.correct(ctx, id, false, from, to, props, txFrom)
	if err != nil {
		return conformance.Node{}, err
	}
	return w.node(e), nil
}

func (w *Graph) SetRelVersionInterval(ctx context.Context, id int64, from, to int64, props map[string]any, txFrom int64) (conformance.Rel, error) {
	e, err := w.correct(ctx, id, true, from, to, props, txFrom)
	if err != nil {
		return conformance.Rel{}, err
	}
	return w.rel(e), nil
}

func (w *Graph) closeVersion(ctx context.Context, id int64, rel bool, t int64) error {
	ref, err := w.ref(id, rel)
	if err != nil {
		return mapErr(err)
	}
	return w.update(ctx, func(tx graph.Tx) error {
		cur, err := w.current(ctx, id, rel)
		if err != nil {
			return err
		}
		lo, hi, _ := cur.Valid.Bounds()
		if _, fin := hi.Position(); fin {
			return conformance.ErrVersionClosed
		}
		if from := w.ms(lo); t <= from || t <= 0 {
			return fmt.Errorf("%w: close at %d", conformance.ErrInvalidTimeRange, t)
		}
		return tx.End(graph.End{Target: ref, Scope: w.span(temporal.NegativeInfinity(), w.bound(t, false))})
	})
}

func (w *Graph) CloseNodeVersion(ctx context.Context, id int64, t int64) error {
	return w.closeVersion(ctx, id, false, t)
}
func (w *Graph) CloseRelVersion(ctx context.Context, id int64, t int64) error {
	return w.closeVersion(ctx, id, true, t)
}

func (w *Graph) excise(ctx context.Context, id int64, rel bool, a, b, txFrom int64) error {
	if txFrom != 0 {
		return fmt.Errorf("%w: caller instants are not part of the native profile", conformance.ErrTxOrder)
	}
	if a <= 0 || b <= a {
		return fmt.Errorf("%w: [%d,%d)", conformance.ErrInvalidTimeRange, a, b)
	}
	ref, err := w.ref(id, rel)
	if err != nil {
		return mapErr(err)
	}
	return w.update(ctx, func(tx graph.Tx) error {
		if _, err := w.current(ctx, id, rel); err != nil {
			return err
		}
		scope := w.span(w.bound(a, true), w.bound(b, false))
		rows, err := w.lookup(ctx, id, rel, graph.Fresh(), scope)
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return conformance.ErrNoVersionValidAt
		}
		lo, hi, _ := rows[0].Valid.Bounds()
		if w.ms(lo) > a || (w.ms(hi) != 0 && w.ms(hi) < b) {
			return conformance.ErrNoVersionValidAt
		}
		return tx.Retract(graph.Retract{Target: ref, Scope: scope})
	})
}

func (w *Graph) ExciseNodeValidity(ctx context.Context, id int64, a, b, txFrom int64) error {
	return w.excise(ctx, id, false, a, b, txFrom)
}
func (w *Graph) ExciseRelValidity(ctx context.Context, id int64, a, b, txFrom int64) error {
	return w.excise(ctx, id, true, a, b, txFrom)
}

// ---- transactions ----

type txWrap struct {
	w      *Graph
	ctx    context.Context
	tx     graph.Tx
	shadow map[int64]*conformance.Node
}

var errStamp = fmt.Errorf("%w: caller instants are not part of the native profile", conformance.ErrTxOrder)

func (w *Graph) Tx(ctx context.Context, fn func(conformance.Tx) error) error {
	if fn == nil {
		return bad("nil callback")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return w.update(ctx, func(tx graph.Tx) error {
		return fn(&txWrap{w: w, ctx: ctx, tx: tx, shadow: map[int64]*conformance.Node{}})
	})
}

func (t *txWrap) AddNode(s conformance.NodeSpec) (conformance.Node, error) {
	id, ent, err := t.w.writeNode(t.tx, s)
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	n := t.w.node(ent)
	n.ID = int64(id)
	t.shadow[n.ID] = &n
	return n, nil
}

func (t *txWrap) AddRel(s conformance.RelSpec) (conformance.Rel, error) {
	ent, err := t.w.writeRel(t.tx, s)
	if err != nil {
		return conformance.Rel{}, mapErr(err)
	}
	return t.w.rel(ent), nil
}

func (t *txWrap) UpdateNode(id int64, props map[string]any) (conformance.Node, error) {
	p, err := t.w.props(props)
	if err != nil {
		return conformance.Node{}, err
	}
	ref, err := t.w.ref(id, false)
	if err != nil {
		return conformance.Node{}, mapErr(err)
	}
	scope := t.w.all()
	if sh := t.shadow[id]; sh == nil {
		cur, err := t.w.current(t.ctx, id, false)
		if err != nil {
			return conformance.Node{}, mapErr(err)
		}
		scope = t.w.fromNow(cur.Valid)
	}
	for _, name := range sortedNames(p) {
		if err := t.tx.Set(graph.Set{Target: ref, Key: name, Value: p[name], Scope: scope}); err != nil {
			return conformance.Node{}, mapErr(err)
		}
	}
	if sh := t.shadow[id]; sh != nil {
		for k, v := range props {
			sh.Props[k] = v
		}
		return *sh, nil
	}
	return conformance.Node{ID: id}, nil
}

func (t *txWrap) DeleteNode(id int64) error {
	ref, err := t.w.ref(id, false)
	if err != nil {
		return mapErr(err)
	}
	scope := t.w.all()
	if t.shadow[id] == nil {
		cur, err := t.w.current(t.ctx, id, false)
		if err != nil {
			return mapErr(err)
		}
		scope = cur.Valid
	}
	if err := t.tx.Set(graph.Set{Target: ref, Key: deletedProp, Value: graph.Bool(true), Scope: scope}); err != nil {
		return mapErr(err)
	}
	delete(t.shadow, id)
	return mapErr(t.tx.End(graph.End{Target: ref, Scope: t.w.span(temporal.NegativeInfinity(), t.w.bound(time.Now().UnixMilli(), false))}))
}

func (t *txWrap) GetNode(id int64) (conformance.Node, error) {
	if sh := t.shadow[id]; sh != nil {
		return *sh, nil
	}
	return t.w.GetNode(t.ctx, id)
}

func (t *txWrap) UpdateNodeWithTx(int64, map[string]any, int64) (conformance.Node, error) {
	return conformance.Node{}, errStamp
}
func (t *txWrap) DeleteNodeWithTx(int64, int64) error { return errStamp }
func (t *txWrap) SetNodeVersionIntervalWithTx(int64, int64, int64, map[string]any, int64) (conformance.Node, error) {
	return conformance.Node{}, errStamp
}
func (t *txWrap) ExciseNodeValidityWithTx(int64, int64, int64, int64) error { return errStamp }
func (t *txWrap) UpdateRelWithTx(int64, map[string]any, int64) (conformance.Rel, error) {
	return conformance.Rel{}, errStamp
}

type batchWrap struct {
	w   *Graph
	ops []func(*txWrap) error
}

func (w *Graph) BatchRun(ctx context.Context, fn func(conformance.Batch) error) error {
	if fn == nil {
		return bad("nil callback")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b := &batchWrap{w: w}
	if err := fn(b); err != nil {
		return err
	}
	return w.Tx(ctx, func(tx conformance.Tx) error {
		for _, op := range b.ops {
			if err := op(tx.(*txWrap)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *batchWrap) AddNode(s conformance.NodeSpec) error {
	b.ops = append(b.ops, func(t *txWrap) error { _, err := t.AddNode(s); return err })
	return nil
}
func (b *batchWrap) UpdateNodeWithTx(int64, map[string]any, int64) error { return errStamp }
func (b *batchWrap) DeleteNodeWithTx(int64, int64) error                 { return errStamp }
func (b *batchWrap) SetNodeVersionIntervalWithTx(int64, int64, int64, map[string]any, int64) error {
	return errStamp
}
func (b *batchWrap) ExciseNodeValidityWithTx(int64, int64, int64, int64) error { return errStamp }
func (b *batchWrap) UpdateRelWithTx(int64, map[string]any, int64) error        { return errStamp }

// ---- doors that need the WP-08 index pages ----

func (w *Graph) NodesByLabel(string, conformance.Query) ([]conformance.Node, error) {
	w.skipScan("NodesByLabel")
	return nil, nil
}
func (w *Graph) RelsByType(string, conformance.Query) ([]conformance.Rel, error) {
	w.skipScan("RelsByType")
	return nil, nil
}
func (w *Graph) NodesByLabelAndProperty(string, string, any, conformance.Query) ([]conformance.Node, error) {
	w.skipScan("NodesByLabelAndProperty")
	return nil, nil
}
func (w *Graph) RelsEffectivePage(context.Context, conformance.Pin, conformance.Filter, int64, int) ([]conformance.RelSegment, int64, error) {
	w.skipScan("RelsEffectivePage")
	return nil, 0, nil
}
func (w *Graph) NodesEffectivePage(context.Context, conformance.Pin, conformance.Filter, int64, int) ([]conformance.NodeSegment, int64, error) {
	w.skipScan("NodesEffectivePage")
	return nil, 0, nil
}
func (w *Graph) CountByLabelPropertyValue(string, string, any) (int, error) {
	w.skipScan("CountByLabelPropertyValue")
	return 0, nil
}
