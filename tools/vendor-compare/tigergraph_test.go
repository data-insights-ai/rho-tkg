package vendorcompare

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
)

func intCell(value string) Cell {
	return Cell{Type: "i64", Value: json.RawMessage(fmt.Sprintf("%q", value))}
}
func boolCell(value bool) Cell {
	if value {
		return Cell{Type: "bool", Value: json.RawMessage("true")}
	}
	return Cell{Type: "bool", Value: json.RawMessage("false")}
}
func textCell(value string) Cell {
	data, _ := json.Marshal(value)
	return Cell{Type: "text", Value: data}
}
func nodeID(i int) string { return fmt.Sprintf("n:%016x", i) }
func edgeID(i int) string { return fmt.Sprintf("e:%016x", i) }
func literalNodes() []Entity {
	var rows []Entity
	for i, value := range []string{"-9223372036854775808", "9007199254740992", "9007199254740993"} {
		rows = append(rows, Entity{Kind: "node", ID: nodeID(i), Labels: []string{"Entity"}, Properties: map[string]Cell{"p_i64": intCell(value), "p_bool": boolCell(i == 1), "p_text": textCell(""), "p_f64": {Type: "f64", Bits: "0000000000000000"}}})
	}
	return rows
}
func literalEdges() []Entity {
	pairs := [][2]int{{0, 1}, {0, 1}, {0, 0}, {1, 2}, {1, 0}}
	var rows []Entity
	for i, pair := range pairs {
		rows = append(rows, Entity{Kind: "edge", ID: edgeID(i), Source: nodeID(pair[0]), Target: nodeID(pair[1]), Type: "HOP", Properties: map[string]Cell{"p_i64": intCell("1"), "p_bool": boolCell(false), "p_text": textCell("")}})
	}
	return rows
}

type fakeTiger struct {
	mu          sync.Mutex
	nodes       map[string]Entity
	edges       map[string]Entity
	refuse      bool
	skip        bool
	dropReverse bool
}

func newFake(t *testing.T) (*fakeTiger, *TigerGraph) {
	t.Helper()
	state := &fakeTiger{nodes: map[string]Entity{}, edges: map[string]Entity{}}
	server := httptest.NewServer(http.HandlerFunc(state.serve))
	t.Cleanup(server.Close)
	client, err := NewTigerGraph(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return state, client
}
func plainAttrs(row Entity) map[string]json.RawMessage {
	values, _ := attributes(row)
	out := map[string]json.RawMessage{}
	for key, value := range values {
		data, _ := json.Marshal(value.Value)
		out[key] = data
	}
	return out
}
func (f *fakeTiger) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	reply := func(errorFlag bool, results any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"error": errorFlag, "results": results, "code": "REST-0000"})
	}
	if f.refuse {
		reply(true, []any{})
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "graph" || parts[1] != GraphName {
		w.WriteHeader(404)
		return
	}
	if r.Method == http.MethodPost {
		if r.Header.Get("gsql-atomic-level") != "atomic" || r.URL.Query().Get("ack") != "all" {
			reply(true, []any{})
			return
		}
		type rawAttribute struct {
			Value json.RawMessage `json:"value"`
		}
		var payload struct {
			Vertices map[string]map[string]map[string]rawAttribute                                    `json:"vertices"`
			Edges    map[string]map[string]map[string]map[string]map[string][]map[string]rawAttribute `json:"edges"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			reply(true, []any{})
			return
		}
		if f.skip {
			reply(false, []any{map[string]int{"accepted_vertices": 0, "accepted_edges": 0, "skipped_vertices": 1}})
			return
		}
		n, e := 0, 0
		unwrap := func(values map[string]rawAttribute) map[string]json.RawMessage {
			attrs := map[string]json.RawMessage{}
			for key, value := range values {
				attrs[key] = value.Value
			}
			return attrs
		}
		for kind, rows := range payload.Vertices {
			for id, values := range rows {
				row, err := decodeNode(nativeNode{ID: id, Type: kind, Attributes: unwrap(values)})
				if err != nil {
					reply(true, []any{})
					return
				}
				_, exists := f.nodes[id]
				if r.URL.Query().Get("new_vertex_only") == "true" && exists || r.URL.Query().Get("update_vertex_only") == "true" && !exists {
					reply(true, []any{})
					return
				}
				f.nodes[id] = row
				n++
			}
		}
		for sourceType, sources := range payload.Edges {
			for source, kinds := range sources {
				for kind, targetTypes := range kinds {
					for targetType, targets := range targetTypes {
						for target, items := range targets {
							for _, values := range items {
								attrs := unwrap(values)
								row, reverse, err := decodeEdge(nativeEdge{Type: kind, Source: source, SourceType: sourceType, Target: target, TargetType: targetType, Directed: true, Attributes: attrs})
								if err != nil || reverse || r.URL.Query().Get("vertex_must_exist") != "true" {
									reply(true, []any{})
									return
								}
								if _, ok := f.nodes[source]; !ok {
									reply(true, []any{})
									return
								}
								if _, ok := f.nodes[target]; !ok {
									reply(true, []any{})
									return
								}
								f.edges[row.ID] = row
								e++
							}
						}
					}
				}
			}
		}
		reply(false, []any{map[string]int{"accepted_vertices": n, "accepted_edges": e}})
		return
	}
	if len(parts) < 4 {
		reply(true, []any{})
		return
	}
	if parts[2] == "vertices" {
		if r.Method == http.MethodDelete && len(parts) == 5 {
			delete(f.nodes, parts[4])
			reply(false, []any{map[string]int{"deleted_vertices": 1}})
			return
		}
		var rows []nativeNode
		for _, row := range f.nodes {
			rows = append(rows, nativeNode{ID: row.ID, Type: NodeType, Attributes: plainAttrs(row)})
		}
		if rows == nil {
			rows = []nativeNode{}
		}
		reply(false, rows)
		return
	}
	if parts[2] != "edges" || len(parts) < 6 {
		reply(true, []any{})
		return
	}
	if r.Method == http.MethodDelete {
		if len(parts) != 9 {
			reply(true, []any{})
			return
		}
		delete(f.edges, parts[8])
		reply(false, []any{map[string]int{"deleted_edges": 1}})
		return
	}
	kind, reverse, err := logicalType(parts[5])
	if err != nil {
		reply(true, []any{})
		return
	}
	var rows []nativeEdge
	if !(reverse && f.dropReverse) {
		for _, row := range f.edges {
			source, target := row.Source, row.Target
			if reverse {
				source, target = target, source
			}
			if row.Type == kind && source == parts[4] {
				rows = append(rows, nativeEdge{Type: parts[5], Source: source, SourceType: NodeType, Target: target, TargetType: NodeType, Directed: true, Attributes: plainAttrs(row)})
			}
		}
	}
	if rows == nil {
		rows = []nativeEdge{}
	}
	reply(false, rows)
}
func loadLiteral(t *testing.T, client *TigerGraph) {
	t.Helper()
	for _, row := range append(literalNodes(), literalEdges()...) {
		if err := client.Load(t.Context(), row); err != nil {
			t.Fatal(err)
		}
	}
}
func collect(t *testing.T, client *TigerGraph, request Request) []any {
	t.Helper()
	var rows []any
	if err := client.Query(t.Context(), request, func(row any) error { rows = append(rows, row); return nil }); err != nil {
		t.Fatal(err)
	}
	return rows
}
func TestTigerGraphExactNativeRowsAndRepeatedWalks(t *testing.T) {
	_, client := newFake(t)
	loadLiteral(t, client)
	exact := collect(t, client, Request{Op: "range", Kind: "node", Key: "p_i64", Low: intCell("9007199254740993"), High: intCell("9007199254740993")})
	if len(exact) != 1 || exact[0].(IDRow).ID != nodeID(2) {
		t.Fatal(exact)
	}
	for _, request := range []Request{{Op: "label", Label: "Entity"}, {Op: "type", Type: "HOP"}, {Op: "equality", Kind: "node", Key: "p_bool", Value: boolCell(false)}, {Op: "scan", Kind: "edge"}, {Op: "projection", Kind: "edge", Keys: []string{"p_bool", "p_text", "p_optional", "p_never"}}} {
		if len(collect(t, client, request)) == 0 {
			t.Fatal(request)
		}
	}
	out := collect(t, client, Request{Op: "adjacency", ID: nodeID(0), Direction: "out"})
	var ids []string
	for _, row := range out {
		ids = append(ids, row.(Adjacency).ID)
	}
	if !slices.Equal(ids, []string{edgeID(0), edgeID(1), edgeID(2)}) {
		t.Fatal(ids)
	}
	in := collect(t, client, Request{Op: "adjacency", ID: nodeID(0), Direction: "in"})
	ids = nil
	for _, row := range in {
		ids = append(ids, row.(Adjacency).ID)
	}
	if !slices.Equal(ids, []string{edgeID(2), edgeID(4)}) {
		t.Fatal(ids)
	}
	walks := collect(t, client, Request{Op: "expand", ID: nodeID(0), MaxDepth: 2})
	expected := []string{"0", "0,3", "0,4", "1", "1,3", "1,4", "2", "2,0", "2,1", "2,2"}
	var got []string
	for _, row := range walks {
		walk := row.(Walk)
		var sequence []string
		for _, id := range walk.EdgeIDs {
			sequence = append(sequence, strings.TrimLeft(id[2:], "0"))
			if sequence[len(sequence)-1] == "" {
				sequence[len(sequence)-1] = "0"
			}
		}
		got = append(got, strings.Join(sequence, ","))
	}
	slices.Sort(got)
	slices.Sort(expected)
	if !slices.Equal(got, expected) {
		t.Fatal(got)
	}
	if len(collect(t, client, Request{Op: "lookup", Kind: "node", ID: nodeID(99)})) != 0 {
		t.Fatal("phantom")
	}
	inventory, err := client.PhysicalInventory(t.Context())
	if err != nil || inventory.ForwardRows != 5 || inventory.ReverseRows != 5 {
		t.Fatal(inventory, err)
	}
	state, other := newFake(t)
	loadLiteral(t, other)
	state.mu.Lock()
	state.dropReverse = true
	state.mu.Unlock()
	if _, err := other.PhysicalInventory(t.Context()); !errors.Is(err, ErrContract) {
		t.Fatal("missing reverse inventory", err)
	}
}
func TestTigerGraphMutationPresenceAndParallelIdentity(t *testing.T) {
	_, client := newFake(t)
	loadLiteral(t, client)
	if err := client.Apply(t.Context(), Event{Revision: 1, Op: "update", Kind: "node", ID: nodeID(0), Set: map[string]Cell{"p_i64": intCell("9223372036854775807")}, Remove: []string{"p_bool"}}); err != nil {
		t.Fatal(err)
	}
	rows := collect(t, client, Request{Op: "projection", Kind: "node", Keys: []string{"p_bool", "p_text", "p_never"}})
	projection := rows[0].(Projection)
	if projection.Columns["p_bool"].Presence != "absent" || projection.Columns["p_never"].Presence != "absent" || projection.Columns["p_text"].Presence != "present" || string(projection.Columns["p_text"].Value.Value) != "\"\"" {
		t.Fatal(projection)
	}
	replacement := literalNodes()[0]
	replacement.Properties["p_bool"] = boolCell(false)
	if err := client.Apply(t.Context(), Event{Revision: 2, Op: "upsert", Row: replacement}); err != nil {
		t.Fatal(err)
	}
	if err := client.Apply(t.Context(), Event{Revision: 3, Op: "delete", Kind: "edge", ID: edgeID(1)}); err != nil {
		t.Fatal(err)
	}
	if len(collect(t, client, Request{Op: "lookup", Kind: "edge", ID: edgeID(1)})) != 0 || len(collect(t, client, Request{Op: "lookup", Kind: "edge", ID: edgeID(0)})) != 1 {
		t.Fatal("parallel deletion collapsed or phantom")
	}
	if err := client.Apply(t.Context(), Event{Revision: 3, Op: "delete", Kind: "node", ID: nodeID(0)}); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	traffic, err := client.Traffic()
	if err != nil || traffic.Requests == 0 {
		t.Fatal(err)
	}
	traffic.ByMethod["GET"] = 0
	again, _ := client.Traffic()
	if again.ByMethod["GET"] == 0 {
		t.Fatal("aliased traffic")
	}
}
func TestTigerGraphRefusalsArePrecise(t *testing.T) {
	var nilClient *TigerGraph
	for _, err := range []error{nilClient.Load(t.Context(), Entity{}), nilClient.Apply(t.Context(), Event{}), nilClient.Query(t.Context(), Request{}, func(any) error { return nil })} {
		if !errors.Is(err, ErrContract) {
			t.Fatal(err)
		}
	}
	if _, err := nilClient.Traffic(); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	if _, err := nilClient.PhysicalInventory(t.Context()); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"https://127.0.0.1:19000", "http://localhost:19000", "http://example.com:19000", "http://127.0.0.1:19000/path"} {
		if _, err := NewTigerGraph(endpoint, nil); !errors.Is(err, ErrContract) {
			t.Fatal(endpoint, err)
		}
	}
	state, client := newFake(t)
	if err := client.Query(t.Context(), Request{Op: "history"}, func(any) error { return nil }); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	if err := client.Query(t.Context(), Request{}, nil); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	if err := client.Load(nil, literalNodes()[0]); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := client.Load(ctx, literalNodes()[0]); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	state.mu.Lock()
	state.skip = true
	state.mu.Unlock()
	if err := client.Load(t.Context(), literalNodes()[0]); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	state.mu.Lock()
	state.skip = false
	state.refuse = true
	state.mu.Unlock()
	if err := client.Query(t.Context(), Request{Op: "scan", Kind: "node"}, func(any) error { return nil }); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
}
func TestExactAttributesAndScalarBranches(t *testing.T) {
	for _, kind := range []string{"node", "edge"} {
		row := literalNodes()[0]
		if kind == "edge" {
			row = literalEdges()[0]
		}
		row.Properties["p_i64"] = intCell("-9223372036854775808")
		attrs, err := attributes(row)
		if err != nil {
			t.Fatal(err)
		}
		wire, _ := json.Marshal(attrs)
		if !strings.Contains(string(wire), "\"value\":-9223372036854775808") || attrs["has_p_bool"].Value != true || attrs["p_bool"].Value != false || attrs["has_p_optional"].Value != false || attrs["p_text"].Value != "" {
			t.Fatal(string(wire))
		}
		row.Properties["p_i64"] = intCell("9223372036854775807")
		attrs, err = attributes(row)
		if err != nil {
			t.Fatal(err)
		}
		props, err := nativeProperties(plainAttrs(row))
		if err != nil {
			t.Fatal(err)
		}
		n, err := props["p_i64"].Native()
		if err != nil || n != int64(9223372036854775807) {
			t.Fatal(n, err)
		}
		row.Properties["p_optional"] = Cell{Type: "null", Value: json.RawMessage("null")}
		if _, err := attributes(row); !errors.Is(err, ErrUnsupported) {
			t.Fatal(err)
		}
	}
	for _, cell := range []Cell{intCell("9007199254740993"), boolCell(false), textCell(""), {Type: "f64", Bits: "8000000000000000"}} {
		value, err := cell.Native()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cellFromNative(value); err != nil {
			t.Fatal(err)
		}
	}
	for _, cell := range []Cell{intCell("9223372036854775808"), intCell("01"), {Type: "i64", Value: json.RawMessage("true")}, {Type: "f64", Bits: "7ff0000000000000"}, {Type: "null", Value: json.RawMessage("null")}} {
		if _, err := cell.Native(); !errors.Is(err, ErrContract) {
			t.Fatal(err)
		}
	}
}
