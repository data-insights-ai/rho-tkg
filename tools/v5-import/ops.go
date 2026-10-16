package importmap

import (
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

// MarshalJSON writes {"k": kind, "v": text}.
func (p PropValue) MarshalJSON() ([]byte, error) {
	var k, v string
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
	default:
		return nil, fmt.Errorf("importmap: invalid value")
	}
	return json.Marshal(struct {
		K string `json:"k"`
		V string `json:"v,omitempty"`
	}{k, v})
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
	unbounded := false
	switch src.Config.UntimedValidStart {
	case "unbounded":
		unbounded = true
	case "tx_from":
	default:
		return nil, fmt.Errorf("importmap: unknown untimed_valid_start %q", src.Config.UntimedValidStart)
	}
	m := &mapper{unbounded: unbounded}
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
	unbounded bool
	ops       []Op
	amb       []Ambiguity
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
	var (
		open, seenAny bool
		life          int
		closedAt      int64 // the instant the current life was deleted or retracted (0 = not)
		prev          *fixtures.Row
		first         *fixtures.Row
	)
	for i := range rows {
		r := rows[i]
		if r.Void {
			m.report(kind, r, "", KindVoidRow)
			continue
		}
		// v4 stamps a delete or retract IN PLACE on the newest row, so rows recorded
		// before that instant still belong to the life; a row recorded at or after it
		// starts the next life.
		if open && closedAt != 0 && r.TxFrom >= closedAt {
			open, closedAt = false, 0
		}
		scope, ok := m.scopeOf(kind, r, !seenAny)
		seenAny = true
		if !ok {
			m.report(kind, r, "", KindEmptyScope)
		}
		base := Op{Entity: kind, ID: r.ID, Version: r.Version, Scope: scope, Recorded: r.TxFrom, Hash: r.Hash, PrevHash: r.PrevHash}
		props, dropped := m.props(kind, r)
		if ok {
			switch {
			case !open:
				life++
				if life > 1 {
					m.report(kind, r, "", KindReimportAfterDel)
				}
				first, prev, open = &rows[i], nil, true
				o := base
				o.Life, o.Props = life, props
				if kind == "node" {
					o.Op, o.Labels = "create_node", r.Labels
				} else {
					o.Op, o.Type, o.Start, o.End = "create_rel", r.Type, r.Start, r.End
				}
				m.ops = append(m.ops, o)
			default:
				m.follow(kind, base, life, first, prev, r, props, dropped)
			}
		}
		if ok || open {
			prev = &rows[i]
		}
		if open && closedAt == 0 && (r.Retracted || r.DeletedAt != 0) {
			closedAt = m.close(kind, base, life, r, scope, ok)
		}
	}
	return nil
}

// follow emits the ops that turn the previous row's state into r's, over r's scope.
func (m *mapper) follow(kind string, base Op, life int, first, prev *fixtures.Row, r fixtures.Row, props map[string]PropValue, dropped int) {
	base.Life = life
	if kind == "rel" && first != nil && (r.Type != first.Type || r.Start != first.Start || r.End != first.End) {
		m.report(kind, r, "", KindRelIdentityChanged)
	}
	if len(props) > 0 {
		o := base
		o.Op, o.Props = "correct", props
		m.ops = append(m.ops, o)
	} else if dropped > 0 {
		m.report(kind, r, "", KindEmptyCorrection)
	}
	if prev == nil {
		return
	}
	for _, k := range sortedKeys(prev.Props) {
		if _, still := r.Props[k]; !still {
			o := base
			o.Op, o.Key = "unset", k
			m.ops = append(m.ops, o)
		}
	}
	if kind != "node" {
		return
	}
	for _, l := range diff(r.Labels, prev.Labels) {
		o := base
		o.Op, o.Label = "add_label", l
		m.ops = append(m.ops, o)
	}
	for _, l := range diff(prev.Labels, r.Labels) {
		o := base
		o.Op, o.Label = "remove_label", l
		m.ops = append(m.ops, o)
	}
}

// close ends or retracts the life a row closes. A tombstone ends validity at
// DeletedAt; a retracted row withdraws what the row asserted. Both are
// recorded at DeletedAt.
// It returns the instant the life closed.
func (m *mapper) close(kind string, base Op, life int, r fixtures.Row, scope Scope, scopeOK bool) int64 {
	base.Life = life
	base.Recorded = r.DeletedAt
	if base.Recorded == 0 {
		base.Recorded = r.TxFrom
	}
	at := base.Recorded
	if r.Retracted {
		if scopeOK {
			base.Op = "retract"
			m.ops = append(m.ops, base)
		}
		return at
	}
	if r.ValidTo != 0 && r.ValidTo != r.DeletedAt {
		m.report(kind, r, "", KindDeleteInstantSplit)
	}
	end := scope
	end.To = At(r.DeletedAt)
	if end.Empty() {
		m.report(kind, r, "", KindEmptyScope)
		return at
	}
	base.Op, base.Scope = "end", end
	m.ops = append(m.ops, base)
	return at
}

// scopeOf resolves a row's valid interval. ok is false for an empty scope.
func (m *mapper) scopeOf(kind string, r fixtures.Row, firstEver bool) (Scope, bool) {
	var s Scope
	switch {
	case r.ValidFrom == math.MinInt64:
		s.From = NegInf
		if !m.unbounded {
			m.report(kind, r, "", KindUnboundedInTxFrom)
		}
	case r.ValidFrom == 0 && m.unbounded && firstEver:
		s.From = At(0) // an unset first row would be MinInt64 in this mode: this 0 is explicit
	case r.ValidFrom == 0:
		s.From = At(r.TxFrom)
	default:
		s.From = At(r.ValidFrom)
	}
	s.To = PosInf
	if r.ValidTo != 0 {
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

// diff returns the labels in a that are not in b, sorted.
func diff(a, b []string) []string {
	have := map[string]bool{}
	for _, l := range b {
		have[l] = true
	}
	var out []string
	for _, l := range a {
		if !have[l] {
			out = append(out, l)
		}
	}
	sort.Strings(out)
	return out
}
