package importmap

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"

	fixtures "github.com/data-insights-ai/rho-tkg/tools/v5-import-fixtures"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
)

// Edge is one end of a Scope in milliseconds: finite (Inf 0), -inf (-1) or +inf (1).
type Edge struct {
	Inf int8
	MS  int64
}

// Edges.
var (
	NegInf = Edge{Inf: -1}
	PosInf = Edge{Inf: 1}
)

// At is a finite edge.
func At(ms int64) Edge { return Edge{MS: ms} }

// MarshalJSON writes "-inf", "+inf" or the millisecond number.
func (e Edge) MarshalJSON() ([]byte, error) {
	switch e.Inf {
	case -1:
		return []byte(`"-inf"`), nil
	case 1:
		return []byte(`"+inf"`), nil
	}
	return []byte(strconv.FormatInt(e.MS, 10)), nil
}

// Scope is the explicit valid-time support [From, To) of an op, in milliseconds.
type Scope struct {
	From Edge `json:"from"`
	To   Edge `json:"to"`
}

// Empty reports a scope that holds no instant.
func (s Scope) Empty() bool {
	if s.From.Inf == 1 || s.To.Inf == -1 {
		return true
	}
	return s.From.Inf == 0 && s.To.Inf == 0 && s.From.MS >= s.To.MS
}

// PropValue is a mapped property value.
type PropValue struct{ graph.Value }

// MarshalJSON writes {"k": kind, "v": text}; bytes are hex, a list is {"k":"list","e":[...]}.
func (p PropValue) MarshalJSON() ([]byte, error) {
	var k, v string
	var elems []PropValue
	switch p.Kind() {
	case graph.KindNull:
		k = "null"
	case graph.KindString:
		k = "string"
		v, _ = p.AsString()
	case graph.KindBool:
		k = "bool"
		b, _ := p.AsBool()
		v = strconv.FormatBool(b)
	case graph.KindInt:
		k = "i64"
		n, _ := p.AsInt()
		v = strconv.FormatInt(n, 10)
	case graph.KindFloat:
		k = "f64"
		f, _ := p.AsFloat()
		v = strconv.FormatFloat(f, 'g', -1, 64)
	case graph.KindBytes:
		k = "bytes"
		b, _ := p.AsBytes()
		v = hex.EncodeToString(b)
	case graph.KindList:
		k = "list"
		els, _ := p.AsList()
		elems = make([]PropValue, len(els))
		for i, e := range els {
			elems[i] = PropValue{e}
		}
	default:
		return nil, fmt.Errorf("importmap: invalid value")
	}
	return json.Marshal(struct {
		K string      `json:"k"`
		V string      `json:"v,omitempty"`
		E []PropValue `json:"e,omitempty"`
	}{k, v, elems})
}

// Op is one v5 write, in plain data. Op is create_node, create_rel, correct,
// unset, add_label, remove_label, end or retract.
type Op struct {
	Op       string               `json:"op"`
	Entity   string               `json:"entity"` // node | rel
	ID       int64                `json:"id"`     // the v4 ID, to be kept (WP-10b seeds the allocator above it)
	Life     int                  `json:"life"`   // 1, 2, ... after each delete or retract of the ID
	Version  uint32               `json:"version"`
	Labels   []string             `json:"labels,omitempty"`
	Type     string               `json:"type,omitempty"`
	Start    int64                `json:"start,omitempty"`
	End      int64                `json:"end,omitempty"`
	Label    string               `json:"label,omitempty"` // add_label, remove_label
	Key      string               `json:"key,omitempty"`   // unset
	Props    map[string]PropValue `json:"props,omitempty"`
	Scope    Scope                `json:"scope"`
	Recorded int64                `json:"recorded"` // v4 TxFrom (DeletedAt for end and retract)
	Hash     string               `json:"hash,omitempty"`
	PrevHash string               `json:"prev_hash,omitempty"`

	region []Scope // writer only: a scope of several parts (Scope holds the first)
}

// Ambiguity is one thing the importer did not map. Key is empty for a row-level finding.
type Ambiguity struct {
	Entity  string `json:"entity"`
	ID      int64  `json:"id"`
	Version uint32 `json:"version"`
	Key     string `json:"key,omitempty"`
	Kind    string `json:"kind"`
}

// Row-level ambiguity kinds.
const (
	KindVoidRow            = "void_row"
	KindReimportAfterDel   = "reimport_after_delete"
	KindEmptyCorrection    = "empty_correction"
	KindEmptyScope         = "empty_scope"
	KindUnboundedInTxFrom  = "unbounded_start_in_txfrom_store"
	KindDeleteInstantSplit = "delete_instant_mismatch"
	KindRelIdentityChanged = "rel_identity_changed"
)

// Result is the op stream and the ambiguity report of one export.
type Result struct {
	Ops         []Op
	Ambiguities []Ambiguity
}

// Map maps every row of src. It refuses a nil source, an unknown untimed mode,
// an ID that is zero, negative or listed twice, and a row whose ID is not its entity's.
func Map(src *fixtures.Source) (*Result, error) {
	if src == nil {
		return nil, fmt.Errorf("importmap: nil source")
	}
	switch src.Config.UntimedValidStart {
	case "unbounded", "tx_from":
	default:
		return nil, fmt.Errorf("importmap: unknown untimed_valid_start %q", src.Config.UntimedValidStart)
	}
	m := &mapper{mode: src.Config.UntimedValidStart}
	for _, kind := range []struct {
		name string
		ents []fixtures.Entity
	}{{"node", src.Nodes}, {"rel", src.Rels}} {
		seen := map[int64]bool{}
		for _, en := range kind.ents {
			if en.ID <= 0 || seen[en.ID] {
				return nil, fmt.Errorf("importmap: %s id %d is not positive and unique", kind.name, en.ID)
			}
			seen[en.ID] = true
			if err := m.entity(kind.name, en); err != nil {
				return nil, err
			}
		}
	}
	sort.SliceStable(m.ops, func(i, j int) bool {
		a, b := m.ops[i], m.ops[j]
		if a.Recorded != b.Recorded {
			return a.Recorded < b.Recorded
		}
		if a.Entity != b.Entity {
			return a.Entity == "node"
		}
		return a.ID < b.ID
	})
	sort.SliceStable(m.amb, func(i, j int) bool {
		a, b := m.amb[i], m.amb[j]
		switch {
		case a.Entity != b.Entity:
			return a.Entity == "node"
		case a.ID != b.ID:
			return a.ID < b.ID
		case a.Version != b.Version:
			return a.Version < b.Version
		case a.Key != b.Key:
			return a.Key < b.Key
		}
		return a.Kind < b.Kind
	})
	return &Result{Ops: m.ops, Ambiguities: m.amb}, nil
}

type mapper struct {
	mode string
	ops  []Op
	amb  []Ambiguity
}

func (m *mapper) report(entity string, r fixtures.Row, key, kind string) {
	m.amb = append(m.amb, Ambiguity{Entity: entity, ID: r.ID, Version: r.Version, Key: key, Kind: kind})
}

func (m *mapper) entity(kind string, en fixtures.Entity) error {
	rows := append([]fixtures.Row(nil), en.History...)
	if en.Current != nil {
		rows = append(rows, *en.Current)
	}
	for _, r := range rows {
		if r.ID != en.ID {
			return fmt.Errorf("importmap: %s %d holds a row of id %d", kind, en.ID, r.ID)
		}
		if kind == "rel" && (r.Start <= 0 || r.End <= 0) {
			return fmt.Errorf("importmap: rel %d has endpoints %d -> %d", en.ID, r.Start, r.End)
		}
	}
	// Recording order, whatever order v4 returned the rows in.
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].TxFrom != rows[j].TxFrom {
			return rows[i].TxFrom < rows[j].TxFrom
		}
		return rows[i].Version < rows[j].Version
	})
	e := &entityMap{m: m, kind: kind}
	for i := range rows {
		e.row(rows[i])
	}
	e.flush()
	return nil
}

// entityMap walks the rows of one entity in recording order. The model holds
// what the ops emitted so far make true over valid time, so each row is
// turned into the ops that move the model to the state the row asserts, not
// into a copy of the row.
type entityMap struct {
	m      *mapper
	kind   string
	model  overlay
	done   []doneRow // non-void rows already mapped
	first  *fixtures.Row
	life   int
	open   bool
	closed int64 // the instant the open life was deleted or retracted (0 = not)
	pend   *Op   // the end or retract at that instant, emitted once a row is recorded at or after it
}

type doneRow struct {
	row   fixtures.Row
	scope Scope
}

func (e *entityMap) emit(o Op) {
	e.m.ops = append(e.m.ops, o)
	e.model.apply(o)
}

// flush emits a pending end or retract and closes the life.
func (e *entityMap) flush() {
	if e.pend != nil {
		e.emit(*e.pend)
	}
	if e.closed != 0 {
		e.open, e.closed, e.pend = false, 0, nil
	}
}

func (e *entityMap) row(r fixtures.Row) {
	m, kind := e.m, e.kind
	// v4 stamps a delete or retract IN PLACE on the newest row, so rows recorded
	// before that instant still belong to the life; a row recorded at or after
	// it starts the next life.
	if e.closed != 0 && r.TxFrom >= e.closed {
		e.flush()
	}
	scope, ok := m.scopeOf(kind, r)
	base := Op{Entity: kind, ID: r.ID, Version: r.Version, Scope: scope, Recorded: r.TxFrom, Hash: r.Hash, PrevHash: r.PrevHash}
	if r.Void {
		// An excise piece: that valid time was never lived. Reported as well, since
		// a retract of valid time is v5's reading, not v4's word.
		m.report(kind, r, "", KindVoidRow)
		if e.open && ok {
			o := base
			o.Op, o.Life = "retract", e.life
			e.emit(o)
		}
		return
	}
	if !ok {
		m.report(kind, r, "", KindEmptyScope)
	}
	props, dropped := m.props(kind, r)
	if ok {
		if !e.open {
			e.life++
			if e.life > 1 {
				m.report(kind, r, "", KindReimportAfterDel)
			}
			e.open, e.first, e.model = true, &r, overlay{}
			o := base
			o.Life, o.Props = e.life, props
			if kind == "node" {
				o.Op, o.Labels = "create_node", r.Labels
			} else {
				o.Op, o.Type, o.Start, o.End = "create_rel", r.Type, r.Start, r.End
			}
			e.emit(o)
		} else {
			e.follow(base, r, props, dropped)
		}
	}
	e.done = append(e.done, doneRow{r, scope})
	if e.open && e.closed == 0 && (r.Retracted || r.DeletedAt != 0) {
		e.close(base, r, scope, ok)
	}
}

// follow emits the ops that move the model to r's state over r's scope.
func (e *entityMap) follow(base Op, r fixtures.Row, props map[string]PropValue, dropped int) {
	m, kind := e.m, e.kind
	base.Life = e.life
	if kind == "rel" && e.first != nil && (r.Type != e.first.Type || r.Start != e.first.Start || r.End != e.first.End) {
		m.report(kind, r, "", KindRelIdentityChanged)
	}
	lo, hi := base.Scope.From.coord(), base.Scope.To.coord()
	if len(props) > 0 {
		o := base
		o.Op, o.Props = "correct", props
		e.emit(o)
	} else if dropped > 0 {
		m.report(kind, r, "", KindEmptyCorrection)
	}
	// What the model holds over the scope and r does not have: unset it.
	gone := map[string]bool{}
	lab := map[string]bool{}
	for _, s := range e.model.over(lo, hi) {
		for k := range s.props {
			gone[k] = true
		}
		for l := range s.labels {
			lab[l] = true
		}
	}
	for _, k := range sortedStrings(gone) {
		if _, still := r.Props[k]; !still {
			o := base
			o.Op, o.Key = "unset", k
			e.emit(o)
		}
	}
	if kind == "node" {
		want := map[string]bool{}
		for _, l := range r.Labels {
			want[l] = true
		}
		for _, l := range sortedStrings(want) {
			if !lab[l] || e.lacks(lo, hi, l) {
				o := base
				o.Op, o.Label = "add_label", l
				e.emit(o)
			}
		}
		for _, l := range sortedStrings(lab) {
			if !want[l] {
				o := base
				o.Op, o.Label = "remove_label", l
				e.emit(o)
			}
		}
	}
	// r replaced an earlier row at this very instant (TxTo == TxFrom): what that row
	// held beyond r's upper bound is gone.
	if base.Scope.To.Inf == 0 {
		for _, d := range e.done {
			if d.row.TxTo != 0 && d.row.TxTo == r.TxFrom && (d.scope.To.Inf == 1 || d.scope.To.Inf == 0 && d.scope.To.MS > base.Scope.To.MS) {
				o := base
				o.Op, o.Props = "end", nil
				e.emit(o)
				break
			}
		}
	}
}

func (e *entityMap) lacks(lo, hi int64, l string) bool {
	for _, s := range e.model.over(lo, hi) {
		if !s.labels[l] {
			return true
		}
	}
	return false
}

// close records the delete or retract a row carries. A tombstone ends validity
// at DeletedAt; a retract withdraws the entity for all valid time (v4: "no
// valid time"). Both are recorded at DeletedAt and take effect once a row
// recorded at or after that instant shows the life is over.
func (e *entityMap) close(base Op, r fixtures.Row, scope Scope, scopeOK bool) {
	base.Life = e.life
	base.Recorded = r.DeletedAt
	if base.Recorded == 0 {
		base.Recorded = r.TxFrom
	}
	e.closed = base.Recorded
	if r.Retracted {
		base.Op, base.Scope = "retract", Scope{From: NegInf, To: PosInf}
		e.pend = &base
		return
	}
	if r.ValidTo != 0 && r.ValidTo != r.DeletedAt {
		e.m.report(e.kind, r, "", KindDeleteInstantSplit)
	}
	end := scope
	end.To = At(r.DeletedAt)
	if !scopeOK || (end.From.Inf == 0 && end.From.MS >= r.DeletedAt) {
		// the row never held valid time before the delete
		e.m.report(e.kind, r, "", KindEmptyScope)
		return
	}
	base.Op, base.Scope = "end", end
	e.pend = &base
}

// scopeOf resolves a row's valid interval. ok is false for an empty scope.
// A delete stamps its instant into valid_to of the row it hits; that is the
// delete, not the row's own interval, so the row itself stays open ended.
func (m *mapper) scopeOf(kind string, r fixtures.Row) (Scope, bool) {
	var s Scope
	switch {
	case r.ValidFrom == math.MinInt64:
		s.From = NegInf
		if m.mode != "unbounded" {
			m.report(kind, r, "", KindUnboundedInTxFrom)
		}
	case r.ValidFrom == 0:
		s.From = At(r.TxFrom) // v4 reads 0 as unset in both modes
	default:
		s.From = At(r.ValidFrom)
	}
	s.To = PosInf
	if r.ValidTo != 0 && !(r.DeletedAt != 0 && r.ValidTo == r.DeletedAt && !r.Void) {
		s.To = At(r.ValidTo)
	}
	return s, !s.Empty()
}

func (m *mapper) props(kind string, r fixtures.Row) (map[string]PropValue, int) {
	out := map[string]PropValue{}
	dropped := 0
	for _, k := range sortedKeys(r.Props) {
		v, bad := MapValue(r.Props[k])
		if bad != "" {
			m.report(kind, r, k, bad)
			dropped++
			continue
		}
		out[k] = PropValue{v}
	}
	if len(out) == 0 {
		out = nil
	}
	return out, dropped
}

func sortedKeys(m map[string]fixtures.Typed) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedStrings(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
