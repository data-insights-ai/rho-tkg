// Package fixtures builds small, deterministic v4 stores, exports them with
// v4's IO().Export and records the answers v4 gives about them (as-of reads,
// histories, effective timelines, index inventory). The export is the input of
// a v5 importer; the expected file is the oracle it must reproduce.
package fixtures

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Typed is a property value with its Go kind, so int64 5 and uint64 5 and
// float64 5 stay distinguishable. V is a string for scalars (floats in the
// shortest exact form, bytes as hex), a list for slices, a map for maps.
type Typed struct {
	T string `json:"t"`
	V any    `json:"v,omitempty"`
}

// Row is one stored version of a node or relationship, as v4 returns it.
type Row struct {
	ID        int64            `json:"id"`
	Labels    []string         `json:"labels,omitempty"`
	Type      string           `json:"type,omitempty"`
	Start     int64            `json:"start,omitempty"`
	End       int64            `json:"end,omitempty"`
	Props     map[string]Typed `json:"props,omitempty"`
	ValidFrom int64            `json:"valid_from"`
	ValidTo   int64            `json:"valid_to"`
	TxFrom    int64            `json:"tx_from"`
	TxTo      int64            `json:"tx_to"`
	DeletedAt int64            `json:"deleted_at,omitempty"`
	Retracted bool             `json:"retracted,omitempty"`
	Void      bool             `json:"void,omitempty"`
	Version   uint32           `json:"version"`
	Hash      string           `json:"hash,omitempty"`
	PrevHash  string           `json:"prev_hash,omitempty"`
}

// Entity is one node or relationship as v4 stores it: Current is the row in
// the current slot (nil once deleted or retracted), History the superseded
// rows and tombstones as Nodes().History / Rels().History return them (void
// pieces of an excise included). History does not repeat Current.
type Entity struct {
	ID      int64 `json:"id"`
	Current *Row  `json:"current"`
	History []Row `json:"history"`
}

// Pin is the answer of NodesAtTx / RelsAtTx at (Valid, Tx), sorted by ID.
type Pin struct {
	Valid int64 `json:"valid"`
	Tx    int64 `json:"tx"`
	Nodes []Row `json:"nodes"`
	Rels  []Row `json:"rels"`
}

// Segment is one effective-timeline segment: the row that answers over
// [ValidFrom, ValidTo) (ValidTo 0 = open).
type Segment struct {
	ValidFrom int64 `json:"valid_from"`
	ValidTo   int64 `json:"valid_to"`
	Row       Row   `json:"row"`
}

// EntityTimeline is one entity's effective timeline at a transaction pin.
type EntityTimeline struct {
	ID       int64     `json:"id"`
	Segments []Segment `json:"segments"`
}

// Timeline holds the effective timelines of every node and rel at one tx pin.
type Timeline struct {
	Tx    int64            `json:"tx"`
	Nodes []EntityTimeline `json:"nodes"`
	Rels  []EntityTimeline `json:"rels"`
}

// Lookup is one index-backed read. Kind is node_prop_at (NodesByLabelPropertyAt),
// rel_prop_at (RelsByTypePropertyAt), nodes_during (NodesDuring) or rels_during
// (RelsDuring); IDs are the sorted result. Valid is the instant (the interval
// start for *_during, End its end).
type Lookup struct {
	Kind  string  `json:"kind"`
	Label string  `json:"label,omitempty"` // label or relationship type
	Key   string  `json:"key,omitempty"`
	Value *Typed  `json:"value,omitempty"`
	Valid int64   `json:"valid"`
	End   int64   `json:"end,omitempty"`
	IDs   []int64 `json:"ids"`
}

// Indexes is the inventory v4 reports, probed for every label/type and
// property key that occurs in the data. The export stream does NOT carry index
// definitions: they are what the source store had, to be recreated on the
// target (Verify recreates them, then checks Lookups). Entries are "Label.key",
// "Label.k1+k2" (composite) or "Label" (temporal).
type Indexes struct {
	Property    []string `json:"property"`
	RelProperty []string `json:"rel_property"`
	Composite   []string `json:"composite"`
	Temporal    []string `json:"temporal"`
	RelTemporal []string `json:"rel_temporal"`
}

// Config is the store configuration the export came from; an importer must be
// told the same untimed mode, and hashes decide whether rows carry them.
type Config struct {
	UntimedValidStart string `json:"untimed_valid_start"` // "tx_from" | "unbounded"
	IntegrityHashes   bool   `json:"integrity_hashes"`
}

// Expected is the oracle file.
type Expected struct {
	Scenario  string     `json:"scenario"`
	Config    Config     `json:"config"`
	Indexes   Indexes    `json:"indexes"`
	Nodes     []Entity   `json:"nodes"`
	Rels      []Entity   `json:"rels"`
	Pins      []Pin      `json:"pins"`
	Timelines []Timeline `json:"timelines"`
	Lookups   []Lookup   `json:"lookups"`
}

// Output is a generated fixture pair.
type Output struct {
	Export   []byte // v4 IO().Export stream, header timestamp pinned
	Expected []byte // JSON of Expected, one trailing newline
}

func typed(v any) Typed {
	switch x := v.(type) {
	case nil:
		return Typed{T: "nil"}
	case bool:
		return Typed{T: "bool", V: strconv.FormatBool(x)}
	case string:
		return Typed{T: "string", V: x}
	case int:
		return Typed{T: "int", V: strconv.FormatInt(int64(x), 10)}
	case int8:
		return Typed{T: "int8", V: strconv.FormatInt(int64(x), 10)}
	case int16:
		return Typed{T: "int16", V: strconv.FormatInt(int64(x), 10)}
	case int32:
		return Typed{T: "int32", V: strconv.FormatInt(int64(x), 10)}
	case int64:
		return Typed{T: "int64", V: strconv.FormatInt(x, 10)}
	case uint:
		return Typed{T: "uint", V: strconv.FormatUint(uint64(x), 10)}
	case uint8:
		return Typed{T: "uint8", V: strconv.FormatUint(uint64(x), 10)}
	case uint16:
		return Typed{T: "uint16", V: strconv.FormatUint(uint64(x), 10)}
	case uint32:
		return Typed{T: "uint32", V: strconv.FormatUint(uint64(x), 10)}
	case uint64:
		return Typed{T: "uint64", V: strconv.FormatUint(x, 10)}
	case float32:
		return Typed{T: "float32", V: strconv.FormatFloat(float64(x), 'g', -1, 32)}
	case float64:
		if math.IsNaN(x) {
			return Typed{T: "float64", V: "NaN"}
		}
		return Typed{T: "float64", V: strconv.FormatFloat(x, 'g', -1, 64)}
	case []byte:
		return Typed{T: "bytes", V: fmt.Sprintf("%x", x)}
	case types.TemporalValue:
		return Typed{T: fmt.Sprintf("temporal%d", x.Kind), V: x.Value}
	case time.Time:
		return Typed{T: "time", V: x.UTC().Format(time.RFC3339Nano)}
	case map[string]any:
		m := make(map[string]Typed, len(x))
		for k, e := range x {
			m[k] = typed(e)
		}
		return Typed{T: "map", V: m}
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice {
		l := make([]Typed, rv.Len())
		for i := range l {
			l[i] = typed(rv.Index(i).Interface())
		}
		return Typed{T: "[]" + rv.Type().Elem().String(), V: l}
	}
	return Typed{T: "unsupported:" + rv.Type().String(), V: fmt.Sprint(v)}
}

func props(m map[string]any) map[string]Typed {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]Typed, len(m))
	for k, v := range m {
		out[k] = typed(v)
	}
	return out
}

func tmRow(r *Row, tm *types.TemporalMetadata) {
	r.ValidFrom, r.ValidTo = int64(tm.ValidFrom), int64(tm.ValidTo)
	r.TxFrom, r.TxTo = int64(tm.TxFrom), int64(tm.TxTo)
	r.DeletedAt = int64(tm.DeletedAt)
	r.Retracted, r.Void = tm.Retracted, tm.Void
}

func nodeRow(g *graph.Graph, n *types.Node, hashes bool) Row {
	r := Row{ID: int64(n.ID()), Labels: g.Nodes().Labels(n), Props: props(n.PropertiesMap()), Version: n.Version()}
	tmRow(&r, n.Temporal())
	if ig := n.Integrity(); hashes && ig != nil {
		r.Hash, r.PrevHash = ig.Hash, ig.PrevHash
	}
	return r
}

func relRow(g *graph.Graph, x *types.Relationship, hashes bool) Row {
	r := Row{ID: int64(x.ID()), Type: g.Rels().Type(x), Start: int64(x.StartNodeID()), End: int64(x.EndNodeID()),
		Props: props(x.PropertiesMap()), Version: x.Version()}
	tmRow(&r, x.Temporal())
	if ig := x.Integrity(); hashes && ig != nil {
		r.Hash, r.PrevHash = ig.Hash, ig.PrevHash
	}
	return r
}

// query is what the expected file asks of a graph: the IDs, pins and
// timeline pins to answer. Generate fills it from the scenario, Verify reads
// it back from the file under test.
type query struct {
	nodeIDs, relIDs []int64
	pins            [][2]int64 // valid, tx
	timelineTx      []int64
	lookups         []Lookup // IDs ignored on input
	declare         bool     // recreate indexes from Expected.Indexes before answering (import side)
	indexes         Indexes
}

func queryOf(e *Expected) query {
	q := query{}
	for _, n := range e.Nodes {
		q.nodeIDs = append(q.nodeIDs, n.ID)
	}
	for _, r := range e.Rels {
		q.relIDs = append(q.relIDs, r.ID)
	}
	for _, p := range e.Pins {
		q.pins = append(q.pins, [2]int64{p.Valid, p.Tx})
	}
	for _, t := range e.Timelines {
		q.timelineTx = append(q.timelineTx, t.Tx)
	}
	q.lookups = e.Lookups
	q.indexes = e.Indexes
	return q
}

// answer asks g every question of q and returns the Expected it gives.
func answer(ctx context.Context, g *graph.Graph, name string, cfg Config, q query) (*Expected, error) {
	e := &Expected{Scenario: name, Config: cfg}
	if q.declare {
		if err := declareIndexes(g, q.indexes); err != nil {
			return nil, err
		}
	}
	tmp := g.Temporal()
	byID := func(rows []Row) { sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID }) }

	labels, relTypes := map[string]bool{}, map[string]bool{}
	nodeKeys, relKeys := map[string]map[string]bool{}, map[string]map[string]bool{}
	note := func(m map[string]map[string]bool, k string, p map[string]Typed) {
		if m[k] == nil {
			m[k] = map[string]bool{}
		}
		for pk := range p {
			m[k][pk] = true
		}
	}
	noteRow := func(labels map[string]bool, keys map[string]map[string]bool, r Row) {
		for _, l := range r.Labels {
			labels[l] = true
			note(keys, l, r.Props)
		}
	}
	for _, id := range q.nodeIDs {
		hist, err := g.Nodes().History(types.NodeID(id))
		if err != nil {
			return nil, fmt.Errorf("node %d history: %w", id, err)
		}
		en := Entity{ID: id, History: []Row{}}
		cur, err := g.Nodes().Get(ctx, types.NodeID(id))
		switch {
		case err == nil:
			r := nodeRow(g, cur, cfg.IntegrityHashes)
			en.Current = &r
			noteRow(labels, nodeKeys, r)
		case errors.Is(err, graph.ErrNodeNotFound):
		default:
			return nil, fmt.Errorf("node %d: %w", id, err)
		}
		for _, h := range hist {
			r := nodeRow(g, h, cfg.IntegrityHashes)
			en.History = append(en.History, r)
			for _, l := range r.Labels {
				labels[l] = true
				note(nodeKeys, l, r.Props)
			}
		}
		e.Nodes = append(e.Nodes, en)
	}
	for _, id := range q.relIDs {
		hist, err := g.Rels().History(types.RelID(id))
		if err != nil {
			return nil, fmt.Errorf("rel %d history: %w", id, err)
		}
		en := Entity{ID: id, History: []Row{}}
		cur, err := g.Rels().Get(ctx, types.RelID(id))
		switch {
		case err == nil:
			r := relRow(g, cur, cfg.IntegrityHashes)
			en.Current = &r
			relTypes[r.Type] = true
			note(relKeys, r.Type, r.Props)
		case errors.Is(err, graph.ErrRelNotFound):
		default:
			return nil, fmt.Errorf("rel %d: %w", id, err)
		}
		for _, h := range hist {
			r := relRow(g, h, cfg.IntegrityHashes)
			en.History = append(en.History, r)
			relTypes[r.Type] = true
			note(relKeys, r.Type, r.Props)
		}
		e.Rels = append(e.Rels, en)
	}
	for _, p := range q.pins {
		valid, tx := types.Instant(p[0]), types.Instant(p[1])
		ns, err := tmp.NodesAtTx(valid, tx)
		if err != nil {
			return nil, fmt.Errorf("NodesAtTx(%d,%d): %w", valid, tx, err)
		}
		rs, err := tmp.RelsAtTx(valid, tx)
		if err != nil {
			return nil, fmt.Errorf("RelsAtTx(%d,%d): %w", valid, tx, err)
		}
		pin := Pin{Valid: p[0], Tx: p[1], Nodes: []Row{}, Rels: []Row{}}
		for _, n := range ns {
			pin.Nodes = append(pin.Nodes, nodeRow(g, n, false))
		}
		for _, r := range rs {
			pin.Rels = append(pin.Rels, relRow(g, r, false))
		}
		byID(pin.Nodes)
		byID(pin.Rels)
		e.Pins = append(e.Pins, pin)
	}
	for _, tx := range q.timelineTx {
		tl := Timeline{Tx: tx, Nodes: []EntityTimeline{}, Rels: []EntityTimeline{}}
		for _, id := range q.nodeIDs {
			segs, err := tmp.NodeEffectiveTimeline(types.NodeID(id), types.Instant(tx))
			if err != nil {
				return nil, fmt.Errorf("NodeEffectiveTimeline(%d,%d): %w", id, tx, err)
			}
			et := EntityTimeline{ID: id, Segments: []Segment{}}
			for _, s := range segs {
				et.Segments = append(et.Segments, Segment{int64(s.ValidFrom), int64(s.ValidTo), nodeRow(g, s.Node, false)})
			}
			tl.Nodes = append(tl.Nodes, et)
		}
		for _, id := range q.relIDs {
			segs, err := tmp.RelEffectiveTimeline(types.RelID(id), types.Instant(tx))
			if err != nil {
				return nil, fmt.Errorf("RelEffectiveTimeline(%d,%d): %w", id, tx, err)
			}
			et := EntityTimeline{ID: id, Segments: []Segment{}}
			for _, s := range segs {
				et.Segments = append(et.Segments, Segment{int64(s.ValidFrom), int64(s.ValidTo), relRow(g, s.Rel, false)})
			}
			tl.Rels = append(tl.Rels, et)
		}
		e.Timelines = append(e.Timelines, tl)
	}

	e.Lookups = []Lookup{}
	for _, l := range q.lookups {
		l.IDs = []int64{}
		var err error
		switch l.Kind {
		case "node_prop_at":
			var ns []*types.Node
			if ns, err = tmp.NodesByLabelPropertyAt(l.Label, l.Key, untyped(*l.Value), types.Instant(l.Valid)); err == nil {
				for _, n := range ns {
					l.IDs = append(l.IDs, int64(n.ID()))
				}
			}
		case "rel_prop_at":
			var rs []*types.Relationship
			if rs, err = tmp.RelsByTypePropertyAt(l.Label, l.Key, untyped(*l.Value), types.Instant(l.Valid)); err == nil {
				for _, r := range rs {
					l.IDs = append(l.IDs, int64(r.ID()))
				}
			}
		case "nodes_during":
			var ns []*types.Node
			if ns, err = tmp.NodesDuring(types.Instant(l.Valid), types.Instant(l.End)); err == nil {
				for _, n := range ns {
					l.IDs = append(l.IDs, int64(n.ID()))
				}
			}
		case "rels_during":
			var rs []*types.Relationship
			if rs, err = tmp.RelsDuring(types.Instant(l.Valid), types.Instant(l.End)); err == nil {
				for _, r := range rs {
					l.IDs = append(l.IDs, int64(r.ID()))
				}
			}
		default:
			err = fmt.Errorf("unknown lookup kind %q", l.Kind)
		}
		if err != nil {
			return nil, fmt.Errorf("lookup %s %s.%s: %w", l.Kind, l.Label, l.Key, err)
		}
		sort.Slice(l.IDs, func(i, j int) bool { return l.IDs[i] < l.IDs[j] })
		e.Lookups = append(e.Lookups, l)
	}

	idx := g.Index()
	ix := Indexes{Property: []string{}, RelProperty: []string{}, Composite: []string{}, Temporal: []string{}, RelTemporal: []string{}}
	for _, l := range sortedKeys(labels) {
		for _, k := range sortedKeys(nodeKeys[l]) {
			if ok, err := idx.HasProperty(l, k); err != nil {
				return nil, err
			} else if ok {
				ix.Property = append(ix.Property, l+"."+k)
			}
		}
		comps, err := idx.ListComposites(l)
		if err != nil {
			return nil, err
		}
		for _, c := range comps {
			s := l + "."
			for i, k := range c {
				if i > 0 {
					s += "+"
				}
				s += k
			}
			ix.Composite = append(ix.Composite, s)
		}
		if ok, err := idx.HasTemporal(l); err != nil {
			return nil, err
		} else if ok {
			ix.Temporal = append(ix.Temporal, l)
		}
	}
	for _, rt := range sortedKeys(relTypes) {
		for _, k := range sortedKeys(relKeys[rt]) {
			if ok, err := idx.HasRelProperty(rt, k); err != nil {
				return nil, err
			} else if ok {
				ix.RelProperty = append(ix.RelProperty, rt+"."+k)
			}
		}
		if ok, err := idx.HasRelTemporal(rt); err != nil {
			return nil, err
		} else if ok {
			ix.RelTemporal = append(ix.RelTemporal, rt)
		}
	}
	e.Indexes = ix
	return e, nil
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func marshal(e *Expected) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(e); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// Observed reports which kinds of rows and values the expected file holds.
func Observed(e *Expected) map[string]bool {
	f := map[string]bool{}
	if e.Config.UntimedValidStart == "unbounded" {
		f["untimed_unbounded"] = true
	} else {
		f["untimed_tx_from"] = true
	}
	if e.Config.IntegrityHashes {
		f["hash_on"] = true
	} else {
		f["hash_off"] = true
	}
	if len(e.Indexes.Property) > 0 {
		f["index_property"] = true
	}
	if len(e.Indexes.RelProperty) > 0 {
		f["index_rel_property"] = true
	}
	if len(e.Indexes.Composite) > 0 {
		f["index_composite"] = true
	}
	if len(e.Indexes.Temporal) > 0 {
		f["index_temporal"] = true
	}
	if len(e.Indexes.RelTemporal) > 0 {
		f["index_rel_temporal"] = true
	}
	var walk func(t Typed)
	walk = func(t Typed) {
		f["prop_"+t.T] = true
		switch v := t.V.(type) {
		case []any:
			for _, x := range v {
				b, _ := json.Marshal(x)
				var tt Typed
				_ = json.Unmarshal(b, &tt)
				walk(tt)
			}
		case map[string]any:
			for _, x := range v {
				b, _ := json.Marshal(x)
				var tt Typed
				_ = json.Unmarshal(b, &tt)
				walk(tt)
			}
		case []Typed:
			for _, x := range v {
				walk(x)
			}
		case map[string]Typed:
			for _, x := range v {
				walk(x)
			}
		}
	}
	rows := func(kind string, ents []Entity) {
		for _, en := range ents {
			if len(en.History) > 1 {
				f[kind+"_history"] = true
			}
			chain := en.History
			if en.Current != nil {
				chain = append(append([]Row(nil), en.History...), *en.Current)
			}
			for i, r := range chain {
				for _, p := range r.Props {
					walk(p)
				}
				switch {
				case r.Void:
					f["void"] = true
				case r.Retracted:
					f["retracted"] = true
				case r.DeletedAt != 0:
					f["deleted"] = true
				case r.ValidTo != 0:
					f["closed_interval"] = true
				}
				if r.ValidFrom == math.MinInt64 {
					f["valid_from_unbounded"] = true
				} else if r.ValidFrom != 0 && r.ValidFrom < r.TxFrom-1_000_000_000_000 {
					f["valid_far_past"] = true
				}
				if r.ValidFrom > r.TxFrom+1_000_000_000_000 {
					f["valid_far_future"] = true
				}
				if i > 0 && chain[i-1].DeletedAt != 0 && r.DeletedAt == 0 && !r.Void {
					f["reimport_after_delete"] = true
				}
			}
		}
	}
	rows("node", e.Nodes)
	rows("rel", e.Rels)
	return f
}

// RequiredFeatures is what the fixture set as a whole must cover.
var RequiredFeatures = []string{
	"node_history", "rel_history", "closed_interval", "deleted", "retracted", "void",
	"untimed_tx_from", "untimed_unbounded", "valid_from_unbounded", "valid_far_past", "valid_far_future",
	"reimport_after_delete", "hash_on", "hash_off",
	"index_property", "index_rel_property", "index_composite", "index_temporal", "index_rel_temporal",
	"prop_bool", "prop_int64", "prop_uint64", "prop_float64", "prop_string", "prop_bytes",
	"prop_[]string", "prop_[]int64", "prop_[]float64", "prop_[]float32", "prop_[]bool", "prop_[]interface {}",
	"prop_map", "prop_temporal0", "prop_temporal4", "prop_nil",
}

// untyped is the Go value a lookup's Typed scalar stands for (string, int64, bool).
func untyped(t Typed) any {
	v, _ := t.V.(string)
	switch t.T {
	case "int64":
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	case "bool":
		return v == "true"
	}
	return v
}

// declareIndexes recreates the indexes an inventory lists.
func declareIndexes(g *graph.Graph, ix Indexes) error {
	idx := g.Index()
	split := func(s string) (string, string) {
		i := strings.IndexByte(s, '.')
		return s[:i], s[i+1:]
	}
	for _, s := range ix.Property {
		l, k := split(s)
		if err := idx.CreateProperty(l, k); err != nil {
			return err
		}
	}
	for _, s := range ix.RelProperty {
		l, k := split(s)
		if err := idx.CreateRelProperty(l, k); err != nil {
			return err
		}
	}
	for _, s := range ix.Composite {
		l, k := split(s)
		if err := idx.CreateComposite(l, strings.Split(k, "+")); err != nil {
			return err
		}
	}
	for _, l := range ix.Temporal {
		if err := idx.CreateTemporal(l); err != nil {
			return err
		}
	}
	for _, l := range ix.RelTemporal {
		if err := idx.CreateRelTemporal(l); err != nil {
			return err
		}
	}
	return nil
}
