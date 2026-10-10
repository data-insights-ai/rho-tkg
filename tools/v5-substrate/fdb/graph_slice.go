package main

import (
	"bytes"
	"cmp"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// IDs are opaque fixture declarations qualified by config.Graph. This fixed
// two-node mapping does not allocate IDs or implement arbitrary graph routing.
type endpoint struct {
	Owner      uint8
	Node, Life uint64
}
type graphEdge struct {
	ID             uint64
	Source, Target endpoint
	Value          int64
}
type graphCommand struct {
	Kind string
	Node endpoint
	Edge graphEdge
}
type graphView struct {
	Graph                       string
	Nodes                       []endpoint
	IdentityEdges, VisibleEdges []graphEdge
	Outgoing, Incoming          []graphEdge
}

func validGraphConfig(c config) bool {
	return c.Nodes == [2]uint64{} || c.Nodes[0] != 0 && c.Nodes[1] != 0 && c.Nodes[0] != c.Nodes[1]
}
func (c config) validEndpoint(e endpoint) bool {
	return c.Nodes != [2]uint64{} && e.Owner < 2 && e.Node == c.Nodes[e.Owner] && e.Life != 0
}
func lifeKey(e endpoint) string { return fmt.Sprintf("node/%016x/life/%016x", e.Node, e.Life) }
func edgeKey(kind string, e graphEdge) string {
	return fmt.Sprintf("%s/%016x/%016x/%016x/%016x/%016x", kind, e.ID, e.Source.Node, e.Source.Life, e.Target.Node, e.Target.Life)
}
func graphEffects(command graphCommand) [2][]effect {
	var effects [2][]effect
	if command.Kind == "node-create" || command.Kind == "node-close" {
		e := effect{Key: lifeKey(command.Node), Value: 1}
		if command.Kind == "node-close" {
			e.Value = 0
			e.Delete = true
		}
		effects[command.Node.Owner] = []effect{e}
		return effects
	}
	for _, kind := range []string{"edge", "out", "in"} {
		g := uint8(0)
		if kind == "in" {
			g = 1
		}
		e := effect{Key: edgeKey(kind, command.Edge), Value: command.Edge.Value}
		if command.Kind == "edge-close" {
			e.Value = 0
			e.Delete = true
		}
		effects[g] = append(effects[g], e)
	}
	for g := range effects {
		slices.SortFunc(effects[g], func(a, b effect) int { return strings.Compare(a.Key, b.Key) })
	}
	return effects
}
func graphReadKeys(command graphCommand) [2][]string {
	var keys [2][]string
	if command.Kind == "node-create" || command.Kind == "node-close" {
		keys[command.Node.Owner] = []string{lifeKey(command.Node)}
		return keys
	}
	keys[0] = []string{edgeKey("edge", command.Edge)}
	if command.Kind != "edge-close" {
		keys[0] = append(keys[0], lifeKey(command.Edge.Source))
		keys[1] = []string{lifeKey(command.Edge.Target)}
	}
	if command.Kind != "edge-create" {
		keys[0] = append(keys[0], edgeKey("out", command.Edge))
		keys[1] = append(keys[1], edgeKey("in", command.Edge))
	}
	for g := range keys {
		slices.Sort(keys[g])
	}
	return keys
}
func (h *harness) validateGraphShape(x transaction) error {
	if x.GraphChange == nil {
		if h.config.Nodes != [2]uint64{} {
			for _, p := range x.Participants {
				for _, e := range p.Effects {
					if strings.HasPrefix(e.Key, "node/") || strings.HasPrefix(e.Key, "edge/") || strings.HasPrefix(e.Key, "out/") || strings.HasPrefix(e.Key, "in/") {
						return errInvalid
					}
				}
			}
		}
		return nil
	}
	command := *x.GraphChange
	switch command.Kind {
	case "node-create", "node-close":
		if !h.config.validEndpoint(command.Node) || command.Edge != (graphEdge{}) || len(x.Participants) != 1 || x.Participants[0].Group != command.Node.Owner {
			return errInvalid
		}
	case "edge-create", "edge-update", "edge-close":
		if command.Node != (endpoint{}) || command.Edge.ID == 0 || !h.config.validEndpoint(command.Edge.Source) || !h.config.validEndpoint(command.Edge.Target) || command.Edge.Source.Owner != 0 || command.Edge.Target.Owner != 1 || len(x.Participants) != 2 || command.Kind == "edge-close" && command.Edge.Value != 0 {
			return errInvalid
		}
	default:
		return errInvalid
	}
	expected, keys := graphEffects(command), graphReadKeys(command)
	for _, p := range x.Participants {
		if !slices.Equal(p.Effects, expected[p.Group]) || len(p.Reads) != len(keys[p.Group]) {
			return errInvalid
		}
		for i, r := range p.Reads {
			if r.Key != keys[p.Group][i] {
				return errInvalid
			}
			absent := command.Kind == "node-create" || command.Kind == "edge-create" && r.Key == edgeKey("edge", command.Edge)
			if absent && r.Version != 0 || !absent && r.Version == 0 {
				return errInvalid
			}
		}
	}
	return nil
}
func (h *harness) logicalRange(g uint8, prefix string) fdb.KeyRange {
	begin := []byte(h.prefix(g) + "current/" + hex.EncodeToString([]byte(prefix)))
	end := bytes.Clone(begin)
	end[len(end)-1]++ // Hex ASCII suffix, never 0xff.
	return fdb.KeyRange{Begin: fdb.Key(begin), End: fdb.Key(end)}
}
func (h *harness) graphRows(tr fdb.ReadTransaction, g uint8, prefix string) ([]fdb.KeyValue, error) {
	rows, err := tr.GetRange(h.logicalRange(g, prefix), fdb.RangeOptions{Limit: 129}).GetSliceWithError()
	if err != nil {
		return nil, err
	}
	if len(rows) > 128 {
		return nil, errLimit
	}
	return rows, nil
}
func (h *harness) nativeLive(tr fdb.ReadTransaction, e endpoint) (bool, error) {
	live, matched, err := h.nativeLives(tr, e)
	return live == 1 && matched, err
}
func (h *harness) nativeLives(tr fdb.ReadTransaction, e endpoint) (int, bool, error) {
	rows, err := h.graphRows(tr, e.Owner, fmt.Sprintf("node/%016x/life/", e.Node))
	if err != nil {
		return 0, false, err
	}
	live := 0
	matched := false
	for _, row := range rows {
		v, err := decode[value](row.Value)
		if err != nil {
			return 0, false, err
		}
		if !validGraphValue(v) {
			return 0, false, errInvalid
		}
		encoded, err := hex.DecodeString(string(row.Key[len(h.prefix(e.Owner)+"current/"):]))
		parts := strings.Split(string(encoded), "/")
		if err != nil || len(parts) != 4 {
			return 0, false, errInvalid
		}
		life, err := strconv.ParseUint(parts[3], 16, 64)
		identity := endpoint{e.Owner, e.Node, life}
		if err != nil || !h.config.validEndpoint(identity) || !bytes.Equal(row.Key, h.key(e.Owner, "current", lifeKey(identity))) {
			return 0, false, errInvalid
		}
		if !v.Deleted {
			if v.Value != 1 {
				return 0, false, errInvalid
			}
			live++
			matched = bytes.Equal(row.Key, h.key(e.Owner, "current", lifeKey(e)))
		}
	}
	if live > 1 {
		return 0, false, errInvalid
	}
	return live, matched, nil
}
func (h *harness) checkGraphNative(tr fdb.ReadTransaction, x transaction) (string, error) {
	command := *x.GraphChange
	if command.Kind == "node-create" {
		live, _, err := h.nativeLives(tr, command.Node)
		if err != nil {
			return "", err
		}
		if live != 0 {
			return "endpoint-life", nil
		}
		return "", nil
	}
	if command.Kind == "node-close" {
		live, err := h.nativeLive(tr, command.Node)
		if err != nil {
			return "", err
		}
		if !live {
			return "endpoint-life", nil
		}
		return "", nil
	}
	if command.Kind == "edge-create" || command.Kind == "edge-update" {
		for _, e := range []endpoint{command.Edge.Source, command.Edge.Target} {
			live, err := h.nativeLive(tr, e)
			if err != nil {
				return "", err
			}
			if !live {
				return "endpoint-life", nil
			}
		}
	}
	rows, err := h.graphRows(tr, 0, fmt.Sprintf("edge/%016x/", command.Edge.ID))
	if err != nil {
		return "", err
	}
	if command.Kind == "edge-create" {
		if len(rows) != 0 {
			return "edge-identity", nil
		}
		for _, posting := range []struct {
			group uint8
			kind  string
		}{{0, "out"}, {1, "in"}} {
			orphans, err := h.graphRows(tr, posting.group, fmt.Sprintf("%s/%016x/", posting.kind, command.Edge.ID))
			if err != nil {
				return "", err
			}
			if len(orphans) != 0 {
				return "graph-shape", nil
			}
		}
		return "", nil
	}
	if len(rows) != 1 || !bytes.Equal(rows[0].Key, h.key(0, "current", edgeKey("edge", command.Edge))) {
		return "edge-identity", nil
	}
	prior, err := decode[value](rows[0].Value)
	if err != nil {
		return "", err
	}
	if !validGraphValue(prior) {
		return "", errInvalid
	}
	if prior.Deleted {
		return "edge-identity", nil
	}
	for _, entry := range []struct {
		group uint8
		kind  string
	}{{0, "out"}, {1, "in"}} {
		rows, err := h.graphRows(tr, entry.group, fmt.Sprintf("%s/%016x/", entry.kind, command.Edge.ID))
		if err != nil {
			return "", err
		}
		if len(rows) != 1 || !bytes.Equal(rows[0].Key, h.key(entry.group, "current", edgeKey(entry.kind, command.Edge))) {
			return "graph-shape", nil
		}
		posting, err := decode[value](rows[0].Value)
		if err != nil {
			return "", err
		}
		if posting != prior {
			return "graph-shape", nil
		}
	}
	return "", nil
}
func (h *harness) graphTransaction(id string, coordinator uint8, command graphCommand, s snapshot) transaction {
	x := transaction{Graph: h.config.Graph, Topology: h.config.Topology, ID: id, Request: "request/" + id, Coordinator: coordinator, GraphChange: new(command)}
	// A malformed descriptor remains an invalid transaction, never an array panic.
	if command.Node.Owner > 1 {
		return x
	}
	effects, keys := graphEffects(command), graphReadKeys(command)
	for g := uint8(0); g < 2; g++ {
		if len(effects[g]) == 0 {
			continue
		}
		p := participant{Group: g, Epoch: h.config.Epochs[g], Generation: s.Generations[g], Effects: effects[g]}
		for _, key := range keys[g] {
			p.Reads = append(p.Reads, read{Key: key, Version: s.Values[g][key].Version})
		}
		x.Participants = append(x.Participants, p)
	}
	return x
}
func parseGraphEdge(kind, key string, v int64, c config) (graphEdge, error) {
	parts := strings.Split(key, "/")
	if len(parts) != 6 || parts[0] != kind {
		return graphEdge{}, errInvalid
	}
	var ids [5]uint64
	for i, p := range parts[1:] {
		n, err := strconv.ParseUint(p, 16, 64)
		if err != nil || n == 0 || fmt.Sprintf("%016x", n) != p {
			return graphEdge{}, errInvalid
		}
		ids[i] = n
	}
	e := graphEdge{ID: ids[0], Source: endpoint{0, ids[1], ids[2]}, Target: endpoint{1, ids[3], ids[4]}, Value: v}
	if !c.validEndpoint(e.Source) || !c.validEndpoint(e.Target) {
		return graphEdge{}, errInvalid
	}
	return e, nil
}
func (h *harness) projectGraph(s snapshot) (graphView, error) {
	if h == nil {
		return graphView{}, errInvalid
	}
	view := graphView{Graph: h.config.Graph}
	if h.config.Nodes == [2]uint64{} {
		return view, errInvalid
	}
	lives := map[endpoint]bool{}
	heads := map[uint8]bool{}
	edges := map[string]graphEdge{}
	rows := map[string]value{}
	identities := map[uint64]bool{}
	for g, values := range s.Values {
		for key, v := range values {
			if !validGraphValue(v) || v.Deleted {
				return view, errInvalid
			}
			if strings.HasPrefix(key, "node/") {
				parts := strings.Split(key, "/")
				if len(parts) != 4 || parts[2] != "life" || v.Value != 1 || v.Deleted {
					return view, errInvalid
				}
				node, e1 := strconv.ParseUint(parts[1], 16, 64)
				life, e2 := strconv.ParseUint(parts[3], 16, 64)
				e := endpoint{uint8(g), node, life}
				if e1 != nil || e2 != nil || !h.config.validEndpoint(e) || lifeKey(e) != key || heads[e.Owner] {
					return view, errInvalid
				}
				heads[e.Owner] = true
				lives[e] = true
				view.Nodes = append(view.Nodes, e)
			}
			for _, kind := range []string{"edge", "out", "in"} {
				if strings.HasPrefix(key, kind+"/") {
					if kind == "in" && g != 1 || kind != "in" && g != 0 {
						return view, errInvalid
					}
					e, err := parseGraphEdge(kind, key, v.Value, h.config)
					if err != nil {
						return view, err
					}
					rows[key] = v
					if kind == "edge" {
						if identities[e.ID] {
							return view, errInvalid
						}
						identities[e.ID] = true
						edges[key] = e
					}
				}
			}
		}
	}
	for key, e := range edges {
		owner := rows[key]
		for _, kind := range []string{"out", "in"} {
			posting, ok := rows[edgeKey(kind, e)]
			if !ok || posting != owner {
				return view, errInvalid
			}
		}
		view.IdentityEdges = append(view.IdentityEdges, e)
		if lives[e.Source] && lives[e.Target] {
			view.VisibleEdges = append(view.VisibleEdges, e)
			view.Outgoing = append(view.Outgoing, e)
			view.Incoming = append(view.Incoming, e)
		}
	}
	if len(rows) != len(edges)*3 {
		return view, errInvalid
	}
	slices.SortFunc(view.Nodes, func(a, b endpoint) int {
		if n := cmp.Compare(a.Owner, b.Owner); n != 0 {
			return n
		}
		return cmp.Compare(a.Life, b.Life)
	})
	for _, list := range [][]graphEdge{view.IdentityEdges, view.VisibleEdges, view.Outgoing, view.Incoming} {
		slices.SortFunc(list, func(a, b graphEdge) int { return cmp.Compare(a.ID, b.ID) })
	}
	return view, nil
}

func validGraphValue(v value) bool {
	return v.Version != 0 && v.Round != 0 && validName(v.TxID) && (!v.Deleted || v.Value == 0)
}
func (h *harness) graphAt(c cut) (graphView, error) {
	if h == nil {
		return graphView{}, errInvalid
	}
	if !slices.Equal(c.record.Scope, []uint8{0, 1}) {
		return graphView{}, errUnavailable
	}
	s, err := h.at(c, 1, nil)
	if err != nil {
		return graphView{}, err
	}
	return h.projectGraph(s)
}
