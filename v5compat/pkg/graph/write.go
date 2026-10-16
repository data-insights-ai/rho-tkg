package graph

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"

	v5 "github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	v5t "github.com/data-insights-ai/rho-tkg/v5/pkg/types"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"
)

// ent is the write-side picture of one entity: the newest version and the
// bookkeeping the v4 doors need. It is loaded from the committed state once per
// transaction and then mutated by the doors, so reads inside a Tx see the Tx's
// own writes.
type ent struct {
	rel      bool
	id       int64
	ref      v5.Ref
	labels   []string
	relType  string
	from, to int64
	vals     map[string]v5.Value // shadow name -> value, newest version
	ver, tf  int64
	vf, vt   int64
	spans    spanSet // visible valid time of the entity
	nspans   spanSet // visible valid time of the newest version
	dt       int64
	dr       bool
	xl       []voidRec
	last     int64 // latest instant written to this entity
}

type entKey struct {
	rel bool
	id  int64
}

func (e *ent) ended() bool { return e.dt != 0 }

func (e *ent) row() row {
	return row{id: e.id, rel: e.rel, labels: e.labels, relType: e.relType, from: e.from, to: e.to, vals: e.vals,
		ver: e.ver, tf: e.tf, vf: e.vf, vt: e.vt, dt: e.dt, dr: e.dr}
}

// countTx counts the ops a compat transaction applies, so an empty one commits
// nothing.
type countTx struct {
	v5.Tx
	n int
}

func (c *countTx) CreateNode(o v5.CreateNode) (v5t.NodeID, error) { c.n++; return c.Tx.CreateNode(o) }
func (c *countTx) CreateRel(o v5.CreateRel) (v5t.RelID, error)    { c.n++; return c.Tx.CreateRel(o) }
func (c *countTx) Correct(o v5.Correct) error                     { c.n++; return c.Tx.Correct(o) }
func (c *countTx) End(o v5.End) error                             { c.n++; return c.Tx.End(o) }
func (c *countTx) AddLabel(o v5.AddLabel) error                   { c.n++; return c.Tx.AddLabel(o) }
func (c *countTx) Set(o v5.Set) error                             { c.n++; return c.Tx.Set(o) }
func (c *countTx) Retract(o v5.Retract) error                     { c.n++; return c.Tx.Retract(o) }

// wtx is one compat write transaction: every door applies its op to the one live
// v5 Update at call time (validated against the overlay of the entities it
// touched), stamped with Recorded = the op's instant. The Update commits when the
// callback returns nil, as one v5 commit.
type wtx struct {
	g        *Graph
	ctx      context.Context
	live     *countTx
	ents     map[entKey]*ent
	used     map[int64]bool // instants of non-create ops
	plain    int64          // the clock instant of this transaction, once a clock op asks
	clock    int64          // the instant the commit takes at most (see Graph.next)
	creates  int
	createAt int64
	claims   map[string]*ent
	oneWrite bool // Batch: a second write to one entity is refused
	written  map[entKey]bool
	newNodes []int64
	newRels  []int64
	late     []func(*wtx) error // Batch creates: applied last, at the clock instant
}

func newWtx(g *Graph, ctx context.Context) *wtx {
	return &wtx{g: g, ctx: ctx, ents: map[entKey]*ent{}, used: map[int64]bool{}, claims: map[string]*ent{}, written: map[entKey]bool{}}
}

// instant picks the commit instant of one write: the caller's stamp on the
// backfill doors, else the transaction's clock instant.
func (w *wtx) instant(caller int64, e *ent) (int64, error) {
	if caller == 0 {
		if w.plain == 0 {
			w.plain = w.clock
		}
		if e != nil && w.plain < e.last {
			return 0, fmt.Errorf("%w: instant %d below the entity's latest %d", ErrTxOrder, w.plain, e.last)
		}
		return w.plain, nil
	}
	if !w.g.cfg.AllowTxBackfill {
		return 0, ErrTxBackfillDisabled
	}
	if caller < 0 || caller > w.clock {
		return 0, fmt.Errorf("%w: %d", ErrInvalidTxFrom, caller)
	}
	if e != nil && caller <= e.last {
		return 0, fmt.Errorf("%w: %d is not above the entity's latest %d", ErrTxOrder, caller, e.last)
	}
	return caller, nil
}

func (w *wtx) touch(e *ent) error {
	k := entKey{e.rel, e.id}
	if w.oneWrite && w.written[k] {
		return fmt.Errorf("%w: one write per entity per unit", ErrTxOrder)
	}
	w.written[k] = true
	return nil
}

// ent loads (or returns the overlay of) an entity; an unknown ID is NotFound.
func (w *wtx) ent(id int64, rel bool) (*ent, error) {
	if err := idErr(id); err != nil {
		return nil, err
	}
	k := entKey{rel, id}
	if e := w.ents[k]; e != nil {
		return e, nil
	}
	rows, last, err := w.g.rowsOfLast(w.ctx, w.g.ax.known(), id, rel)
	if err != nil {
		return nil, err
	}
	nw, ok := newest(rows)
	if !ok {
		return nil, notFound(rel)
	}
	ref, _ := w.g.ref(id, rel)
	e := &ent{rel: rel, id: id, ref: ref, labels: nw.labels, relType: nw.relType, from: nw.from, to: nw.to, vals: nw.vals,
		ver: nw.ver, tf: nw.tf, vf: nw.vf, vt: nw.vt}
	var all, newer []span
	for _, r := range rows {
		all = append(all, r.sp)
		if r.ver == nw.ver {
			newer = append(newer, r.sp)
		}
		if r.ended {
			e.dt, e.dr = r.dt, r.dr
		}
		if len(r.xl) > len(formatXl(e.xl)) {
			e.xl = parseXl(r.xl)
		}
	}
	e.last = last
	e.spans, e.nspans = normSpans(all), normSpans(newer)
	w.ents[k] = e
	return e, nil
}

// live entity: loaded and not deleted or retracted.
func (w *wtx) liveEnt(id int64, rel bool) (*ent, error) {
	e, err := w.ent(id, rel)
	if err != nil {
		return nil, err
	}
	if e.ended() {
		return nil, notFound(rel)
	}
	return e, nil
}

// ---- span of a write ----

func validSpan(vf, vt int64) span {
	s := fullSpan()
	if vf != 0 && vf != negInf {
		s.lo = vf
	}
	if vt != 0 {
		s.hi = vt
	}
	return s
}

func checkInterval(vf, vt int64) error {
	if vf != 0 && vf != negInf && vt != 0 && vt <= vf {
		return fmt.Errorf("%w: valid [%d,%d)", ErrInvalidTimeRange, vf, vt)
	}
	if vf == 0 && vt != 0 && vt < 0 {
		return fmt.Errorf("%w: valid to %d", ErrInvalidTimeRange, vt)
	}
	return nil
}

// fromInstant narrows a version's valid time to start at the write instant, so an
// update is a new version that leaves the earlier valid time to the earlier one.
func fromInstant(s spanSet, t int64) spanSet {
	h := s.hull()
	if (h.lo != negInf && h.lo >= t) || (h.hi != posInf && h.hi <= t) {
		return s
	}
	return s.intersect(span{t, posInf})
}

func reserved(ver, vf, vt int64) map[string]v5.Value {
	return map[string]v5.Value{kVer: v5.Int(ver), kVf: v5.Int(vf), kVt: v5.Int(vt)}
}

// ---- creates ----

func (w *wtx) registerCreate(t int64) error {
	if w.creates == 0 {
		for u := range w.used {
			if u != t {
				return fmt.Errorf("%w: a transaction that creates entities has one instant", ErrTxOrder)
			}
		}
		w.createAt = t
	} else if t != w.createAt {
		return fmt.Errorf("%w: a transaction that creates entities has one instant (%d), not %d", ErrTxOrder, w.createAt, t)
	}
	w.creates++
	return nil
}

func (w *wtx) createNode(caller int64, labels []string, raw map[string]any) (*ent, error) {
	if len(labels) == 0 {
		return nil, ErrNoLabels
	}
	if slices.Contains(labels, "") {
		return nil, ErrEmptyName
	}
	ps, err := toProps(raw, true)
	if err != nil {
		return nil, err
	}
	if err := checkInterval(ps.vf, ps.vt); err != nil {
		return nil, err
	}
	t, err := w.instant(caller, nil)
	if err != nil {
		return nil, err
	}
	if err := w.registerCreate(t); err != nil {
		return nil, err
	}
	sp := validSpan(ps.vf, ps.vt)
	vf, vt := ps.vf, ps.vt
	if vf == negInf {
		vf = 0
	}
	props := ps.vals
	for k, v := range reserved(0, vf, vt) {
		props[k] = v
	}
	id, err := w.live.CreateNode(v5.CreateNode{Labels: labels, Props: props, Scope: w.g.ax.scope(sp), Recorded: w.g.ax.recorded(t)})
	if err != nil {
		return nil, err
	}
	e := &ent{id: int64(id), ref: mustRef(v5.NodeRef(id)), labels: slices.Clone(labels), vals: ps.vals, tf: t, vf: vf, vt: vt,
		spans: spanSet{sp}, nspans: spanSet{sp}, last: t}
	e.vals = map[string]v5.Value{}
	for k, v := range ps.vals {
		if _, ok := splitShadow(k); ok {
			e.vals[k] = v
		}
	}
	w.ents[entKey{false, e.id}] = e
	w.written[entKey{false, e.id}] = true
	w.newNodes = append(w.newNodes, e.id)
	return e, nil
}

func mustRef(r v5.Ref, err error) v5.Ref {
	if err != nil {
		panic(err)
	}
	return r
}

func (w *wtx) createRel(caller int64, typ string, from, to int64, raw map[string]any) (*ent, error) {
	if typ == "" {
		return nil, ErrEmptyName
	}
	if err := idErr(from); err != nil {
		return nil, err
	}
	if err := idErr(to); err != nil {
		return nil, err
	}
	if from == to {
		return nil, ErrSelfLoop
	}
	ps, err := toProps(raw, true)
	if err != nil {
		return nil, err
	}
	if err := checkInterval(ps.vf, ps.vt); err != nil {
		return nil, err
	}
	for _, id := range []int64{from, to} {
		if _, err := w.liveEnt(id, false); err != nil {
			return nil, err
		}
	}
	t, err := w.instant(caller, nil)
	if err != nil {
		return nil, err
	}
	if err := w.registerCreate(t); err != nil {
		return nil, err
	}
	sp := validSpan(ps.vf, ps.vt)
	vf, vt := ps.vf, ps.vt
	if vf == negInf {
		vf = 0
	}
	props := maps(ps.vals)
	for k, v := range reserved(0, vf, vt) {
		props[k] = v
	}
	id, err := w.live.CreateRel(v5.CreateRel{Type: typ, From: v5t.NodeID(from), To: v5t.NodeID(to), Props: props, Scope: w.g.ax.scope(sp), Recorded: w.g.ax.recorded(t)})
	if err != nil {
		return nil, err
	}
	e := &ent{rel: true, id: int64(id), ref: mustRef(v5.RelRef(id)), relType: typ, from: from, to: to, vals: maps(ps.vals),
		tf: t, vf: vf, vt: vt, spans: spanSet{sp}, nspans: spanSet{sp}, last: t}
	w.ents[entKey{true, e.id}] = e
	w.written[entKey{true, e.id}] = true
	w.newRels = append(w.newRels, e.id)
	return e, nil
}

func maps(m map[string]v5.Value) map[string]v5.Value {
	out := make(map[string]v5.Value, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// ---- version writes ----

// nullOthers returns the shadow names of e's current values that a write of
// (vals, clear) replaces by another kind or removes.
func nullOthers(cur map[string]v5.Value, vals map[string]v5.Value, clear []string) []string {
	written := map[string]bool{}
	for name := range vals {
		k, _ := splitShadow(name)
		written[k] = true
	}
	for _, k := range clear {
		written[k] = true
	}
	var out []string
	for name := range cur {
		k, ok := splitShadow(name)
		if !ok || !written[k] {
			continue
		}
		if _, same := vals[name]; !same {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func sortedNames(m map[string]v5.Value) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// writeVersion applies a new version of e over pieces (an update) or, with
// correction, a Correct over the one piece (it may extend the entity's valid time).
func (w *wtx) writeVersion(e *ent, t int64, pieces spanSet, ps propSet, vf, vt int64, correction bool) error {
	ver := e.ver + 1
	if e.tf == t && !correction {
		ver = e.ver // same commit instant: one version
	}
	nulls := nullOthers(e.vals, ps.vals, ps.clear)
	res := reserved(ver, vf, vt)
	rec := w.g.ax.recorded(t)
	vals := maps(ps.vals)
	w.used[t] = true
	tx := w.live
	for _, p := range pieces {
		sc := w.g.ax.scope(p)
		if correction {
			set := maps(vals)
			for k, v := range res {
				set[k] = v
			}
			for _, n := range nulls {
				set[n] = v5.Null()
			}
			if err := tx.Correct(v5.Correct{Target: e.ref, Set: set, Scope: sc, Recorded: rec}); err != nil {
				return err
			}
			if !e.rel {
				for _, l := range e.labels {
					if err := tx.AddLabel(v5.AddLabel{Target: e.ref.Node(), Label: l, Scope: sc, Recorded: rec}); err != nil {
						return err
					}
				}
			}
			continue
		}
		for _, name := range sortedNames(vals) {
			if err := tx.Set(v5.Set{Target: e.ref, Key: name, Value: vals[name], Scope: sc, Recorded: rec}); err != nil {
				return err
			}
		}
		for _, n := range nulls {
			if err := tx.Set(v5.Set{Target: e.ref, Key: n, Value: v5.Null(), Scope: sc, Recorded: rec}); err != nil {
				return err
			}
		}
		for _, name := range sortedNames(res) {
			if err := tx.Set(v5.Set{Target: e.ref, Key: name, Value: res[name], Scope: sc, Recorded: rec}); err != nil {
				return err
			}
		}
	}
	for _, n := range nulls {
		delete(e.vals, n)
	}
	for k, v := range vals {
		e.vals[k] = v
	}
	e.ver, e.tf, e.vf, e.vt, e.last = ver, t, vf, vt, t
	e.nspans = normSpans(pieces)
	if correction {
		e.spans = e.spans.add(pieces[0])
	}
	return nil
}

func (w *wtx) update(caller int64, e *ent, raw map[string]any) error {
	if e.ended() {
		return notFound(e.rel)
	}
	ps, err := toProps(raw, false)
	if err != nil {
		return err
	}
	if err := w.touch(e); err != nil {
		return err
	}
	t, err := w.instant(caller, e)
	if err != nil {
		return err
	}
	if w.creates > 0 && t != w.createAt {
		return fmt.Errorf("%w: a transaction that creates entities has one instant", ErrTxOrder)
	}
	pieces := fromInstant(e.nspans, t)
	if len(pieces) == 0 {
		pieces = e.nspans
	}
	vf := e.vf
	if h := pieces.hull(); h.lo != negInf {
		vf = h.lo
	}
	return w.writeVersion(e, t, pieces, ps, vf, e.vt, false)
}

func (w *wtx) correct(caller int64, e *ent, from, to int64, raw map[string]any) error {
	if e.ended() {
		return notFound(e.rel)
	}
	if from <= 0 || (to != 0 && to <= from) {
		return fmt.Errorf("%w: [%d,%d)", ErrInvalidTimeRange, from, to)
	}
	ps, err := toProps(raw, false)
	if err != nil {
		return err
	}
	if err := w.touch(e); err != nil {
		return err
	}
	t, err := w.instant(caller, e)
	if err != nil {
		return err
	}
	if w.creates > 0 && t != w.createAt {
		return fmt.Errorf("%w: a transaction that creates entities has one instant", ErrTxOrder)
	}
	hi := int64(posInf)
	if to != 0 {
		hi = to
	}
	return w.writeVersion(e, t, spanSet{{from, hi}}, ps, from, to, true)
}

// end deletes (validity ends at the instant, the past stays readable) or
// retracts (the belief never held: every valid instant answers nothing).
func (w *wtx) end(caller int64, e *ent, retract bool) error {
	if e.ended() {
		return notFound(e.rel)
	}
	if err := w.touch(e); err != nil {
		return err
	}
	t, err := w.instant(caller, e)
	if err != nil {
		return err
	}
	if w.creates > 0 && t != w.createAt {
		return fmt.Errorf("%w: a transaction that creates entities has one instant", ErrTxOrder)
	}
	dr := int64(0)
	if retract {
		dr = 1
	}
	rec := w.g.ax.recorded(t)
	w.used[t] = true
	for _, p := range e.spans {
		if err := w.live.Set(v5.Set{Target: e.ref, Key: kDr, Value: v5.Int(dr), Scope: w.g.ax.scope(p), Recorded: rec}); err != nil {
			return err
		}
	}
	if !retract {
		if err := w.live.End(v5.End{Target: e.ref, Scope: w.g.ax.scope(span{negInf, t}), Recorded: rec}); err != nil {
			return err
		}
	}
	e.dt, e.dr, e.last = t, retract, t
	if !retract {
		e.spans = e.spans.subtract(span{t, posInf})
		e.nspans = e.nspans.subtract(span{t, posInf})
	}
	return nil
}

func (w *wtx) excise(caller int64, e *ent, a, b int64) error {
	if e.ended() {
		return notFound(e.rel)
	}
	if a <= 0 || b <= a {
		return fmt.Errorf("%w: [%d,%d)", ErrInvalidTimeRange, a, b)
	}
	if err := w.touch(e); err != nil {
		return err
	}
	t, err := w.instant(caller, e)
	if err != nil {
		return err
	}
	if w.creates > 0 && t != w.createAt {
		return fmt.Errorf("%w: a transaction that creates entities has one instant", ErrTxOrder)
	}
	hole := span{a, b}
	if !e.spans.overlaps(hole) {
		return fmt.Errorf("%w: nothing valid in [%d,%d)", ErrNoVersionValidAt, a, b)
	}
	log := append(slices.Clone(e.xl), voidRec{a, b, t})
	rec := w.g.ax.recorded(t)
	w.used[t] = true
	for _, p := range e.spans {
		if err := w.live.Set(v5.Set{Target: e.ref, Key: kXl, Value: v5.String(formatXl(log)), Scope: w.g.ax.scope(p), Recorded: rec}); err != nil {
			return err
		}
	}
	if err := w.live.Retract(v5.Retract{Target: e.ref, Scope: w.g.ax.scope(hole), Recorded: rec}); err != nil {
		return err
	}
	e.xl, e.last = log, t
	e.spans, e.nspans = e.spans.subtract(hole), e.nspans.subtract(hole)
	return nil
}

func (w *wtx) closeVersion(e *ent, at int64) error {
	if e.ended() {
		return notFound(e.rel)
	}
	if e.vt != 0 {
		return ErrAlreadyClosed
	}
	if at <= 0 || (e.vf != 0 && at <= e.vf) {
		return fmt.Errorf("%w: close at %d", ErrInvalidTimeRange, at)
	}
	if err := w.touch(e); err != nil {
		return err
	}
	t, err := w.instant(0, e)
	if err != nil {
		return err
	}
	pieces := e.nspans.intersect(span{negInf, at})
	if len(pieces) == 0 {
		return fmt.Errorf("%w: close at %d", ErrInvalidTimeRange, at)
	}
	if err := w.writeVersion(e, t, pieces, propSet{vals: map[string]v5.Value{}}, e.vf, at, false); err != nil {
		return err
	}
	if err := w.live.End(v5.End{Target: e.ref, Scope: w.g.ax.scope(span{negInf, at}), Recorded: w.g.ax.recorded(t)}); err != nil {
		return err
	}
	e.spans = e.spans.subtract(span{at, posInf})
	e.nspans = e.nspans.subtract(span{at, posInf})
	return nil
}

// ---- GetOrCreateByKey ----

func (w *wtx) getOrCreate(label, key string, value any, extra map[string]any) (*ent, bool, error) {
	if label == "" || key == "" {
		return nil, false, ErrEmptyName
	}
	if err := checkKey(key); err != nil {
		return nil, false, err
	}
	ps, err := toProps(map[string]any{key: value}, false)
	if err != nil {
		return nil, false, err
	}
	if len(ps.vals) != 1 {
		return nil, false, fmt.Errorf("%w: nil claim value", ErrUniqueUnsupportedType)
	}
	var name string
	var cv v5.Value
	for n, v := range ps.vals {
		name, cv = n, v
	}
	if cv.Kind() == v5.KindFloat || cv.Kind() == v5.KindList || cv.Kind() == v5.KindBytes {
		return nil, false, fmt.Errorf("%w: %v claim value", ErrUniqueUnsupportedType, cv.Kind())
	}
	ck := fmt.Sprintf("%s\x00%s\x00%s\x00%v", label, name, "", cv)
	if e := w.claims[ck]; e != nil && !e.ended() {
		return e, false, nil
	}
	hits, err := w.g.claimHolders(w.ctx, label, key, cv)
	if err != nil {
		return nil, false, err
	}
	for _, id := range hits {
		if e, err := w.liveEnt(id, false); err == nil {
			w.claims[ck] = e
			return e, false, nil
		}
	}
	props := map[string]any{}
	for k, v := range extra {
		props[k] = v
	}
	props[key] = value
	e, err := w.createNode(0, []string{label}, props)
	if err != nil {
		return nil, false, err
	}
	w.claims[ck] = e
	return e, true, nil
}

// ---- running a transaction ----

var errNothing = errors.New("graph: empty transaction")

// run executes fn as one compat transaction: one v5 Update, one commit. Every
// door applies its op to the live Update; Batch creates run last, at the clock
// instant. A callback error, an op error or a cancelled ctx rolls everything back.
func (g *Graph) run(ctx context.Context, oneWrite bool, fn func(*wtx) error) (*wtx, error) {
	if err := g.alive(); err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.alive(); err != nil {
		return nil, err
	}
	w := newWtx(g, ctx)
	w.oneWrite = oneWrite
	w.clock = g.next() // read before the Update opens: never above the engine's own
	rcpt, err := g.g.Update(ctx, func(tx v5.Tx) error {
		w.live = &countTx{Tx: tx}
		if err := fn(w); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(w.late) > 0 {
			w.creates, w.createAt, w.used = 0, 0, map[int64]bool{}
			for _, f := range w.late {
				if err := f(w); err != nil {
					return err
				}
			}
		}
		if w.live.n == 0 {
			return errNothing
		}
		return nil
	})
	if errors.Is(err, errNothing) {
		return w, nil
	}
	if err != nil {
		return nil, mapV5(err)
	}
	g.clk.note(rcpt)
	return w, nil
}

var _ = types.Instant(0)
