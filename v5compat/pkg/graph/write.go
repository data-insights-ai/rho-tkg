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

// wtx is one compat write transaction. Creates run on the live v5 Update (they
// need the ID now); every other op is validated against the overlay at call time
// and queued per caller instant, then flushed when the callback returns nil.
type wtx struct {
	g        *Graph
	ctx      context.Context
	live     v5.Tx
	ents     map[entKey]*ent
	groups   map[int64][]func(v5.Tx) error
	plain    int64
	creates  int
	createAt int64
	claims   map[string]*ent
	oneWrite bool // Batch: a second write to one entity is refused
	written  map[entKey]bool
	newNodes []int64
	newRels  []int64
	late     []func(*wtx) error // Batch creates: committed last, at the clock instant
}

func newWtx(g *Graph, ctx context.Context) *wtx {
	return &wtx{g: g, ctx: ctx, ents: map[entKey]*ent{}, groups: map[int64][]func(v5.Tx) error{}, claims: map[string]*ent{}, written: map[entKey]bool{}}
}

// instant picks the commit instant of one write: the caller's stamp on the
// backfill doors, else the transaction's clock instant.
func (w *wtx) instant(caller int64, e *ent) (int64, error) {
	if caller == 0 {
		if w.plain == 0 {
			w.plain = w.g.led.next()
		}
		if e != nil && w.plain < e.last {
			return 0, fmt.Errorf("%w: instant %d below the entity's latest %d", ErrTxOrder, w.plain, e.last)
		}
		return w.plain, nil
	}
	if !w.g.cfg.AllowTxBackfill {
		return 0, ErrTxBackfillDisabled
	}
	if caller < 0 || caller > nowMs()+maxSkewMs {
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

func (w *wtx) queue(t int64, step func(v5.Tx) error) error {
	if w.creates > 0 && t != w.createAt {
		return fmt.Errorf("%w: a transaction that creates entities has one instant (%d), not %d", ErrTxOrder, w.createAt, t)
	}
	w.groups[t] = append(w.groups[t], step)
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
	rows, err := w.g.rowsOf(w.ctx, v5.Fresh(), id, rel)
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
		e.last = max(e.last, r.tf, r.dt)
		if r.dt != 0 {
			e.dt, e.dr = r.dt, r.dr
		}
		if len(r.xl) > len(formatXl(e.xl)) {
			e.xl = parseXl(r.xl)
		}
	}
	for _, v := range e.xl {
		e.last = max(e.last, v.tx)
	}
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

func reserved(ver, tf, vf, vt int64) map[string]v5.Value {
	return map[string]v5.Value{kVer: v5.Int(ver), kTf: v5.Int(tf), kVf: v5.Int(vf), kVt: v5.Int(vt)}
}

// ---- creates ----

func (w *wtx) registerCreate(t int64) error {
	if w.creates == 0 {
		for gt, steps := range w.groups {
			if gt != t && len(steps) > 0 {
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
	for k, v := range reserved(0, t, vf, vt) {
		props[k] = v
	}
	id, err := w.live.CreateNode(v5.CreateNode{Labels: labels, Props: props, Scope: w.g.ax.scope(sp)})
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
	for k, v := range reserved(0, t, vf, vt) {
		props[k] = v
	}
	id, err := w.live.CreateRel(v5.CreateRel{Type: typ, From: v5t.NodeID(from), To: v5t.NodeID(to), Props: props, Scope: w.g.ax.scope(sp)})
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

// writeVersion queues a new version of e over pieces (an update) or, with
// correction, a Correct over the one piece (it may extend the entity's valid time).
func (w *wtx) writeVersion(e *ent, t int64, pieces spanSet, ps propSet, vf, vt int64, correction bool) {
	ver := e.ver + 1
	if e.tf == t && !correction {
		ver = e.ver // same commit instant: one version
	}
	nulls := nullOthers(e.vals, ps.vals, ps.clear)
	res := reserved(ver, t, vf, vt)
	ref := e.ref
	labels := slices.Clone(e.labels)
	rel := e.rel
	vals := maps(ps.vals)
	step := func(tx v5.Tx) error {
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
				if err := tx.Correct(v5.Correct{Target: ref, Set: set, Scope: sc}); err != nil {
					return err
				}
				if !rel {
					for _, l := range labels {
						if err := tx.AddLabel(v5.AddLabel{Target: ref.Node(), Label: l, Scope: sc}); err != nil {
							return err
						}
					}
				}
				continue
			}
			for _, name := range sortedNames(vals) {
				if err := tx.Set(v5.Set{Target: ref, Key: name, Value: vals[name], Scope: sc}); err != nil {
					return err
				}
			}
			for _, n := range nulls {
				if err := tx.Set(v5.Set{Target: ref, Key: n, Value: v5.Null(), Scope: sc}); err != nil {
					return err
				}
			}
			for _, name := range sortedNames(res) {
				if err := tx.Set(v5.Set{Target: ref, Key: name, Value: res[name], Scope: sc}); err != nil {
					return err
				}
			}
		}
		return nil
	}
	w.groups[t] = append(w.groups[t], step)
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
	w.writeVersion(e, t, pieces, ps, vf, e.vt, false)
	return nil
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
	w.writeVersion(e, t, spanSet{{from, hi}}, ps, from, to, true)
	return nil
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
	ref, spans := e.ref, slices.Clone(e.spans)
	step := func(tx v5.Tx) error {
		dr := int64(0)
		if retract {
			dr = 1
		}
		for _, p := range spans {
			sc := w.g.ax.scope(p)
			if err := tx.Set(v5.Set{Target: ref, Key: kDt, Value: v5.Int(t), Scope: sc}); err != nil {
				return err
			}
			if err := tx.Set(v5.Set{Target: ref, Key: kDr, Value: v5.Int(dr), Scope: sc}); err != nil {
				return err
			}
		}
		if retract {
			return nil
		}
		return tx.End(v5.End{Target: ref, Scope: w.g.ax.scope(span{negInf, t})})
	}
	w.groups[t] = append(w.groups[t], step)
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
	ref, spans := e.ref, slices.Clone(e.spans)
	step := func(tx v5.Tx) error {
		for _, p := range spans {
			if err := tx.Set(v5.Set{Target: ref, Key: kXl, Value: v5.String(formatXl(log)), Scope: w.g.ax.scope(p)}); err != nil {
				return err
			}
		}
		return tx.Retract(v5.Retract{Target: ref, Scope: w.g.ax.scope(hole)})
	}
	w.groups[t] = append(w.groups[t], step)
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
	w.writeVersion(e, t, pieces, propSet{vals: map[string]v5.Value{}}, e.vf, at, false)
	ref := e.ref
	w.groups[t] = append(w.groups[t], func(tx v5.Tx) error {
		return tx.End(v5.End{Target: ref, Scope: w.g.ax.scope(span{negInf, at})})
	})
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
	if cv.Kind() == v5.KindFloat {
		return nil, false, fmt.Errorf("%w: float claim value", ErrUniqueUnsupportedType)
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

// run executes fn as one compat transaction: a live v5 Update for the first
// instant (creates and the group at the creates' instant), then one further v5
// commit per remaining caller instant, ascending. Every op is validated before
// the first commit.
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
	var liveT int64
	committed := true
	rcpt, err := g.g.Update(ctx, func(tx v5.Tx) error {
		w.live = tx
		if err := fn(w); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		stamps := w.stamps()
		switch {
		case w.creates > 0:
			liveT = w.createAt
		case len(stamps) > 0:
			liveT = stamps[0]
		default:
			return errNothing
		}
		return w.flush(tx, liveT)
	})
	if errors.Is(err, errNothing) {
		committed, err = false, nil
	}
	if err != nil {
		return nil, mapV5(err)
	}
	if committed {
		if err := g.record(ctx, liveT, rcpt); err != nil {
			return nil, err
		}
	}
	for _, t := range w.stamps() {
		rcpt, err := g.g.Update(ctx, func(tx v5.Tx) error { return w.flush(tx, t) })
		if err != nil {
			return nil, mapV5(err)
		}
		if err := g.record(ctx, t, rcpt); err != nil {
			return nil, err
		}
	}
	if len(w.late) > 0 {
		w.creates, w.createAt = 0, 0
		rcpt, err := g.g.Update(ctx, func(tx v5.Tx) error {
			w.live = tx
			for _, f := range w.late {
				if err := f(w); err != nil {
					return err
				}
			}
			return w.flush(tx, w.createAt)
		})
		if err != nil {
			return nil, mapV5(err)
		}
		if err := g.record(ctx, w.createAt, rcpt); err != nil {
			return nil, err
		}
	}
	return w, nil
}

// stamps are the instants still holding queued steps, ascending.
func (w *wtx) stamps() []int64 {
	var out []int64
	for t, steps := range w.groups {
		if len(steps) > 0 {
			out = append(out, t)
		}
	}
	slices.Sort(out)
	return out
}

func (w *wtx) flush(tx v5.Tx, t int64) error {
	steps := w.groups[t]
	w.groups[t] = nil
	for _, s := range steps {
		if err := s(tx); err != nil {
			return err
		}
	}
	return nil
}

// record writes the commit's cut to the ledger.
func (g *Graph) record(ctx context.Context, t int64, r v5.Receipt) error {
	s, err := g.g.OpenSnapshot(ctx, v5.After(r))
	if err != nil {
		return mapV5(err)
	}
	cut := s.Cut()
	_ = s.Close()
	return g.led.add(t, cut)
}

var _ = types.Instant(0)
