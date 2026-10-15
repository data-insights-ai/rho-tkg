package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphEdgeUniqueFamily = "cross-edge-endpoint-uniqueness"
const graphEdgeUniqueFormat = "graph-edge-endpoint-unique-v1"

// Version4 has an independent immutable schema/binding. Versions1/2/3 retain
// their own ordinary/contention/selector meanings and exact command bytes.
func graphEdgeUniqueValid(p graphCostProfile) bool {
	return p.Version == 4 && p.Facts >= 20 && p.Facts <= 1_000_000 && p.Facts%20 == 0 && (p.Writers == 1 || p.Writers == 8 || p.Writers == 32) && (p.CrossPercent == 0 || p.CrossPercent == 10 || p.CrossPercent == 50 || p.CrossPercent == 100)
}

func graphEdgeUniqueHomeCount(p graphCostProfile) uint64 { return 5*p.Facts + 7 }
func graphEdgeUniqueNodes(p graphCostProfile) [3]endpoint {
	home := graphEdgeUniqueHomeCount(p)
	return [3]endpoint{{0, 1, 2}, {0, 3, 4}, {1, home + 1, home + 2}}
}
func graphEdgeUniqueNode(p graphCostProfile, remote, reopened bool) endpoint {
	nodes := graphEdgeUniqueNodes(p)
	node := nodes[1]
	if remote {
		node = nodes[2]
	}
	if reopened {
		node.Life = 4*p.Facts + 7
		if remote {
			node.Life = graphEdgeUniqueHomeCount(p) + 3
		}
	}
	return node
}
func graphEdgeUniqueEndpoint(c config, e endpoint) bool {
	if !validGraphEdgeUniqueConfig(c) {
		return false
	}
	if e == graphEdgeUniqueNodes(c.Cost)[0] {
		return true
	}
	for _, remote := range []bool{false, true} {
		for _, reopened := range []bool{false, true} {
			if e == graphEdgeUniqueNode(c.Cost, remote, reopened) {
				return true
			}
		}
	}
	return false
}
func graphEdgeUniqueEdge(p graphCostProfile, fact, candidate uint64, reopened bool) (graphEdge, error) {
	if !graphEdgeUniqueValid(p) || fact >= p.Facts || candidate > 1 {
		return graphEdge{}, errInvalid
	}
	return graphEdge{ID: 7 + 4*fact + 2*candidate, Source: graphEdgeUniqueNodes(p)[0], Target: graphEdgeUniqueNode(p, p.cross(fact), reopened), Value: int64(fact + 5)}, nil // #nosec G115 -- fact<1M.
}
func graphEdgeUniqueIdentity(p graphCostProfile, id uint64) (fact, candidate uint64, err error) {
	if !graphEdgeUniqueValid(p) || id < 7 || (id-7)%2 != 0 {
		return 0, 0, errInvalid
	}
	fact, candidate = (id-7)/4, ((id-7)%4)/2
	if fact >= p.Facts {
		return 0, 0, errInvalid
	}
	return fact, candidate, nil
}
func graphEdgeUniqueValueID(p graphCostProfile, fact uint64) uint64 { return 4*p.Facts + 8 + fact }
func graphEdgeUniqueKey(v int64) string                             { return fmt.Sprintf("unique/p/i64/%016x", uint64(v)) } // #nosec G115 -- admitted positive fact+5.

func graphEdgeUniqueLimit(p graphCostProfile) limits {
	if !graphEdgeUniqueValid(p) {
		return limits{}
	}
	// Two contenders, one replacement and one old close each admit at most64
	// distinct originals; one old-life and one frozen-close refusal add two.
	// Seven uncontended lifecycle operations use one original each. These are
	// finite application admission grants, not allocation/RSS/capacity evidence.
	originals := 7 + 258*p.Facts
	return limits{Bytes: (64 << 20) + p.Facts*(256<<10) + originals*2048, Transactions: originals, Versions: 7 + 11*p.Facts, Certificates: 1024}
}

type graphEdgeUniqueSchedule struct{ Groups, Versions, Originals uint64 }

func graphEdgeUniqueCounts(p graphCostProfile) (graphEdgeUniqueSchedule, error) {
	if !graphEdgeUniqueValid(p) {
		return graphEdgeUniqueSchedule{}, errInvalid
	}
	// The admitted Facts<=1M makes every following operation nonoverflowing.
	// setup3 + close/reopen4 + three successful edge operations per fact;
	// create/replacement four physical writes each; old close three.
	return graphEdgeUniqueSchedule{7 + 3*p.Facts, 7 + 11*p.Facts, 7 + 258*p.Facts}, nil
}

func graphEdgeUniqueColumnPolicy(p graphCostProfile) (graphColumnPolicy, error) {
	n, err := graphEdgeUniqueCounts(p)
	if err != nil {
		return graphColumnPolicy{}, err
	}
	// Each chunk holds at least one admitted original; the byte cap includes
	// one complete <=64KiB encoded original per group, column cells/header/key,
	// and two independently retained old/final generations. Work is per walk:
	// delivered chunk plus exact CDC, paired originals and Before/After reads.
	// Native upload/walk still debit actual bytes; these finite grants are not
	// fabricated usage or guarantees that an arbitrary larger envelope fits.
	chunks := 2 * n.Groups
	policy := graphColumnPolicy{Version: 1, ChunkRows: 32, ChunkBytes: 64 << 10, MaxChunks: chunks, MaxGenerations: 2, MaxRetainedBytes: (64 << 20) + chunks*((64<<10)+1024), MaxWorkRows: n.Groups, MaxWorkBytes: (64 << 20) + n.Groups*(256<<10)}
	// A chunk cannot contain arbitrarily many originals. At most32 per chunk;
	// at1M the complete schedule needs fewer than1M chunks if each original
	// fits its separately admitted 8KiB family envelope (enforced by runner).
	policy.MaxChunks = 2 * ((n.Groups + 6) / 7)
	if policy.validate() != nil {
		return graphColumnPolicy{}, errLimit
	}
	return policy, nil
}
func graphEdgeUniqueConfig(p graphCostProfile, layout, run string) config {
	c := defaults()
	n := graphEdgeUniqueNodes(p)
	c.Graph, c.Nodes, c.Cost = "graph-workload-edge-unique-v1-"+layout+"-"+run, [2]uint64{n[0].Node, n[2].Node}, p
	c.CostScope, c.Limits = lifecycleConfig("validation").Lifecycle.Scope, graphEdgeUniqueLimit(p)
	return c
}
func validGraphEdgeUniqueConfig(c config) bool {
	if !graphEdgeUniqueValid(c.Cost) || c.Lifecycle != (lifecycleProfile{}) || c.CostScope != lifecycleConfig("validation").Lifecycle.Scope || c.Limits != graphEdgeUniqueLimit(c.Cost) {
		return false
	}
	n := graphEdgeUniqueNodes(c.Cost)
	if c.Nodes != [2]uint64{n[0].Node, n[2].Node} {
		return false
	}
	for _, layout := range []string{"kv", "columns"} {
		run, found := strings.CutPrefix(c.Graph, "graph-workload-edge-unique-v1-"+layout+"-")
		if !found {
			run, found = strings.CutPrefix(c.Graph, "graph-workload-edge-unique-v2-"+layout+"-")
		}
		raw, err := hex.DecodeString(run)
		if found && err == nil && len(raw) == 16 && hex.EncodeToString(raw) == run && !bytes.Equal(raw, make([]byte, 16)) {
			return true
		}
	}
	return false
}

// The complete physical mutation/read footprint is fresh-profile specific.
// Its extra claim is committed with primary/out/in and the same CDC/original;
// ordinary graphEffects/graphReadKeys and all their callers remain unchanged.
func graphEdgeUniqueEffects(command graphCommand, release bool) [2][]effect {
	effects := graphEffects(command)
	if command.Kind == "edge-create" || command.Kind == "edge-close" && release {
		unique := effect{Key: graphEdgeUniqueKey(command.Edge.Value), Value: int64(command.Edge.ID)} // #nosec G115 -- admitted ID<=4M+6.
		if command.Kind == "edge-close" {
			// Edge close's scalar payload is zero; recover the fixed unique value
			// from this declared original identity, never from current winner data.
			unique.Key = graphEdgeUniqueKey(int64((command.Edge.ID-7)/4 + 5)) // #nosec G115 -- admitted fact<1M.
			unique.Value, unique.Delete = 0, true
		}
		effects[0] = append(effects[0], unique)
		slices.SortFunc(effects[0], func(a, b effect) int { return strings.Compare(a.Key, b.Key) })
	}
	return effects
}
func graphEdgeUniqueReadKeys(command graphCommand) [2][]string {
	keys := graphReadKeys(command)
	if command.Kind == "edge-create" || command.Kind == "edge-close" {
		value := command.Edge.Value
		if command.Kind == "edge-close" {
			value = int64((command.Edge.ID-7)/4 + 5) // #nosec G115 -- admitted fact<1M.
		}
		keys[0] = append(keys[0], graphEdgeUniqueKey(value))
		slices.Sort(keys[0])
	}
	return keys
}
func (h *harness) graphEdgeUniqueTransaction(id string, command graphCommand, s snapshot) transaction {
	x := transaction{Graph: h.config.Graph, Topology: h.config.Topology, ID: id, Request: "request/" + id, GraphChange: new(command)}
	if command.Kind == "node-create" || command.Kind == "node-close" {
		x.Coordinator = command.Node.Owner
	}
	release := false
	if command.Kind == "edge-close" {
		key := graphEdgeUniqueKey(int64((command.Edge.ID-7)/4 + 5)) // #nosec G115 -- admitted fact<1M.
		head := s.Values[0][key]
		release = head.Version != 0 && !head.Deleted && head.Value == int64(command.Edge.ID) // #nosec G115 -- admitted ID<=4M+6.
	}
	effects, keys := graphEdgeUniqueEffects(command, release), graphEdgeUniqueReadKeys(command)
	for g := uint8(0); g < 2; g++ {
		if len(effects[g]) == 0 {
			continue
		}
		p := participant{Group: g, Epoch: h.config.Epochs[g], Generation: s.Generations[g], Effects: effects[g]}
		for _, key := range keys[g] {
			p.Reads = append(p.Reads, read{key, s.Values[g][key].Version})
		}
		x.Participants = append(x.Participants, p)
	}
	return x
}

func (h *harness) validateGraphEdgeUniqueShape(x transaction) error {
	if h == nil || !validGraphEdgeUniqueConfig(h.config) || x.GraphChange == nil || x.Lifecycle == nil || x.Lifecycle.Provenance == 0 {
		return errInvalid
	}
	c := *x.GraphChange
	switch c.Kind {
	case "node-create", "node-close":
		if !graphEdgeUniqueEndpoint(h.config, c.Node) || c.Edge != (graphEdge{}) || x.Coordinator != c.Node.Owner || x.Lifecycle.EdgeLife != 0 || x.Lifecycle.Definition != nil {
			return errInvalid
		}
	case "edge-create", "edge-close":
		fact, candidate, err := graphEdgeUniqueIdentity(h.config.Cost, c.Edge.ID)
		if err != nil || c.Node != (endpoint{}) || x.Coordinator != 0 {
			return errInvalid
		}
		old, e1 := graphEdgeUniqueEdge(h.config.Cost, fact, candidate, false)
		current, e2 := graphEdgeUniqueEdge(h.config.Cost, fact, candidate, true)
		if c.Kind == "edge-close" {
			old.Value, current.Value = 0, 0
		}
		if e1 != nil || e2 != nil || c.Edge != old && c.Edge != current || x.Lifecycle.EdgeLife != c.Edge.ID+1 {
			return errInvalid
		}
		if c.Kind == "edge-create" && (x.Lifecycle.Definition == nil || *x.Lifecycle.Definition != (lifecycleDefinition{graphEdgeUniqueValueID(h.config.Cost, fact), "i64", c.Edge.Value})) || c.Kind == "edge-close" && x.Lifecycle.Definition != nil {
			return errInvalid
		}
	default:
		return errInvalid
	}
	release := false
	if c.Kind == "edge-close" && len(x.Participants) > 0 {
		// This chooses only one of two exact legal frozen shapes. Native apply
		// independently checks whether the actual head still belongs to this ID.
		for _, effect := range x.Participants[0].Effects {
			if strings.HasPrefix(effect.Key, "unique/") {
				release = true
			}
		}
	}
	effects, keys := graphEdgeUniqueEffects(c, release), graphEdgeUniqueReadKeys(c)
	n := 0
	for g := uint8(0); g < 2; g++ {
		if len(effects[g]) == 0 {
			continue
		}
		if n == len(x.Participants) {
			return errInvalid
		}
		p := x.Participants[n]
		n++
		if p.Group != g || !slices.Equal(p.Effects, effects[g]) || len(p.Reads) != len(keys[g]) {
			return errInvalid
		}
		for i, r := range p.Reads {
			if r.Key != keys[g][i] {
				return errInvalid
			}
			absent := c.Kind == "node-create" || c.Kind == "edge-create" && r.Key == edgeKey("edge", c.Edge)
			// Unique head can be a real inactive predecessor after endpoint ABA;
			// its exact revision is retained rather than rewritten as absence.
			if absent && r.Version != 0 || !absent && c.Kind != "edge-create" && !strings.HasPrefix(r.Key, "unique/") && r.Version == 0 {
				return errInvalid
			}
		}
	}
	if n != len(x.Participants) {
		return errInvalid
	}
	return nil
}

type graphEdgeUniqueBinding struct {
	Version             byte
	Family              string
	Config              config
	Window, MaxAttempts uint64
	Policy              string
	Admission           [32]byte `json:",omitzero"`
}

func graphEdgeUniquePolicy(c config) graphEdgeUniqueBinding {
	return graphEdgeUniqueBinding{1, graphEdgeUniqueFamily, c, 32, 64, "effective relationship p:UniqueScalar; exact endpoint lives; distinct contenders; retained canonical old relations and originals; unknown never authorizes replan", [32]byte{}}
}

func graphEdgeUniquePolicyAdmission(c config, p *graphTransportPolicy) graphEdgeUniqueBinding {
	b := graphEdgeUniquePolicy(c)
	if p != nil {
		b.Version = 2
		b.Admission = graphTransportDigest(p)
	}
	return b
}
func (h *harness) graphEdgeUniqueBindingIn(tr fdb.ReadTransaction) error {
	p, err := h.graphTransportNative(tr)
	if err != nil {
		return err
	}
	if h == nil || !validGraphEdgeUniqueConfig(h.config) {
		return errInvalid
	}
	raw, err := tr.Get(h.key(0, graphTransportBindingKind(h.config, "workload-edge-unique-v1"), "binding")).Get()
	if err != nil {
		return err
	}
	expected, err := wire(graphEdgeUniquePolicyAdmission(h.config, p))
	if err != nil || !bytes.Equal(raw, expected) {
		return errors.Join(errMismatch, err)
	}
	return nil
}

// Native scalar uniqueness is EFFECTIVE. An immutable old primary still
// exists after an endpoint close/reopen, but its bound old life has no support.
// Inspecting that actual primary and both native life ranges is mandatory before
// replacing the head; a copied head value alone proves neither conflict nor room.
func (h *harness) graphEdgeUniqueIncumbent(tr fdb.ReadTransaction, edge graphEdge) (bool, error) {
	raw, found, err := get[value](tr, h.key(0, "current", graphEdgeUniqueKey(edge.Value)))
	if err != nil || !found || raw.Deleted {
		return false, err
	}
	if !validGraphValue(raw) || raw.Value <= 0 {
		return false, errInvalid
	}
	id := uint64(raw.Value) // #nosec G115 -- positive admitted above.
	fact, _, err := graphEdgeUniqueIdentity(h.config.Cost, id)
	if err != nil || int64(fact+5) != edge.Value {
		return false, errInvalid
	} // #nosec G115 -- fact<1M.
	rows, err := h.graphRows(tr, 0, fmt.Sprintf("edge/%016x/", id))
	if err != nil || len(rows) != 1 {
		return false, errors.Join(errInvalid, err)
	}
	prior, err := decode[value](rows[0].Value)
	if err != nil || !validGraphValue(prior) || prior.Deleted || prior.Value != edge.Value {
		return false, errors.Join(errInvalid, err)
	}
	encoded := strings.TrimPrefix(string(rows[0].Key), h.prefix(0)+"current/")
	logical, err := hex.DecodeString(encoded)
	if err != nil {
		return false, err
	}
	incumbent, err := parseGraphEdge("edge", string(logical), prior.Value, h.config)
	if err != nil || incumbent.ID != id || !bytes.Equal(rows[0].Key, h.key(0, "current", edgeKey("edge", incumbent))) {
		return false, errors.Join(errInvalid, err)
	}
	for _, e := range []endpoint{incumbent.Source, incumbent.Target} {
		live, err := h.nativeLive(tr, e)
		if err != nil || !live {
			return false, err
		}
	}
	return incumbent.ID != edge.ID, nil
}

// This hook is called INSIDE h.execute's original native transaction, before
// effects. Native read conflicts, request originals, CDC and the unique head
// are one atomic commit, never a preliminary advisory uniqueness query.
func (h *harness) checkGraphEdgeUniqueNative(tr fdb.ReadTransaction, x transaction) (string, error) {
	if err := h.validateGraphEdgeUniqueShape(x); err != nil {
		return "", err
	}
	if err := h.graphEdgeUniqueBindingIn(tr); err != nil {
		return "", err
	}
	c := *x.GraphChange
	if c.Kind == "node-create" {
		count, _, err := h.nativeLives(tr, c.Node)
		if err != nil {
			return "", err
		}
		if count != 0 {
			return "endpoint-life", nil
		}
		return "", nil
	}
	if c.Kind == "node-close" {
		live, err := h.nativeLive(tr, c.Node)
		if err != nil {
			return "", err
		}
		if !live {
			return "endpoint-life", nil
		}
		return "", nil
	}
	if c.Kind == "edge-close" {
		fact, _, err := graphEdgeUniqueIdentity(h.config.Cost, c.Edge.ID)
		if err != nil {
			return "", err
		}
		key := graphEdgeUniqueKey(int64(fact + 5)) // #nosec G115 -- admitted fact<1M.
		head, found, err := get[value](tr, h.key(0, "current", key))
		if err != nil {
			return "", err
		}
		actualRelease := found && !head.Deleted && head.Value == int64(c.Edge.ID) // #nosec G115 -- admitted ID<=4M+6.
		proposedRelease := false
		for _, effect := range x.Participants[0].Effects {
			if effect.Key == key {
				proposedRelease = true
			}
		}
		if actualRelease != proposedRelease {
			return "revision", nil
		}
		var primary value
		for _, entry := range []struct {
			group uint8
			kind  string
		}{{0, "edge"}, {0, "out"}, {c.Edge.Target.Owner, "in"}} {
			rows, err := h.graphRows(tr, entry.group, fmt.Sprintf("%s/%016x/", entry.kind, c.Edge.ID))
			if err != nil {
				return "", err
			}
			if len(rows) != 1 || !bytes.Equal(rows[0].Key, h.key(entry.group, "current", edgeKey(entry.kind, c.Edge))) {
				return "edge-identity", nil
			}
			prior, err := decode[value](rows[0].Value)
			if err != nil || !validGraphValue(prior) {
				return "", errors.Join(errInvalid, err)
			}
			if prior.Deleted {
				return "edge-identity", nil
			}
			if entry.kind == "edge" {
				primary = prior
			} else if prior != primary {
				return "graph-shape", nil
			}
		}
		return "", nil
	}
	for _, e := range []endpoint{c.Edge.Source, c.Edge.Target} {
		live, err := h.nativeLive(tr, e)
		if err != nil {
			return "", err
		}
		if !live {
			return "endpoint-life", nil
		}
	}
	for _, entry := range []struct {
		group uint8
		kind  string
	}{{0, "edge"}, {0, "out"}, {c.Edge.Target.Owner, "in"}} {
		rows, err := h.graphRows(tr, entry.group, fmt.Sprintf("%s/%016x/", entry.kind, c.Edge.ID))
		if err != nil {
			return "", err
		}
		if len(rows) != 0 {
			return "edge-identity", nil
		}
	}
	conflict, err := h.graphEdgeUniqueIncumbent(tr, c.Edge)
	if err != nil {
		return "", err
	}
	if conflict {
		return "unique", nil
	}
	return "", nil
}

func graphEdgeUniqueOriginalDigest(x transaction) ([32]byte, error) {
	body, err := wire(x)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(body), nil
}

func (h *harness) graphEdgeUniqueValidateRaw(row graphCostRawRow, history bool, round uint64) error {
	if h == nil || !validGraphEdgeUniqueConfig(h.config) || row.Group > 1 {
		return errInvalid
	}
	v, err := decode[value](row.Value)
	if err != nil || !validGraphValue(v) || v.Round > round {
		return errors.Join(errInvalid, err)
	}
	prefix := h.prefix(row.Group) + "current/"
	if history {
		prefix = fmt.Sprintf("%shistory/%016x/", h.prefix(row.Group), v.Round)
	}
	encoded, found := strings.CutPrefix(string(row.Key), prefix)
	logical, err := hex.DecodeString(encoded)
	if !found || err != nil || !validName(string(logical)) || hex.EncodeToString(logical) != encoded {
		return errInvalid
	}
	key := string(logical)
	for _, remote := range []bool{false, true} {
		for _, reopened := range []bool{false, true} {
			e := graphEdgeUniqueNode(h.config.Cost, remote, reopened)
			if key == lifeKey(e) {
				if row.Group != e.Owner || !v.Deleted && v.Value != 1 || reopened && v.Deleted {
					return errInvalid
				}
				return nil
			}
		}
	}
	source := graphEdgeUniqueNodes(h.config.Cost)[0]
	if key == lifeKey(source) {
		if row.Group != 0 || v.Deleted || v.Value != 1 {
			return errInvalid
		}
		return nil
	}
	if suffix, ok := strings.CutPrefix(key, "unique/p/i64/"); ok {
		u, err := strconv.ParseUint(suffix, 16, 64)
		if err != nil || u < 5 || u-5 >= h.config.Cost.Facts || graphEdgeUniqueKey(int64(u)) != key || row.Group != 0 || v.Deleted {
			return errInvalid
		} // #nosec G115 -- u<=1M+4.
		if v.Value <= 0 {
			return errInvalid
		}
		fact, _, err := graphEdgeUniqueIdentity(h.config.Cost, uint64(v.Value)) // #nosec G115 -- positive above.
		if err != nil || fact != u-5 {
			return errInvalid
		}
		return nil
	}
	kind, _, ok := strings.Cut(key, "/")
	if !ok || kind != "edge" && kind != "out" && kind != "in" {
		return errInvalid
	}
	e, err := parseGraphEdge(kind, key, v.Value, h.config)
	if err != nil {
		return errInvalid
	}
	fact, candidate, err := graphEdgeUniqueIdentity(h.config.Cost, e.ID)
	if err != nil || kind != "in" && row.Group != 0 || kind == "in" && row.Group != e.Target.Owner {
		return errInvalid
	}
	for _, reopened := range []bool{false, true} {
		expected, err := graphEdgeUniqueEdge(h.config.Cost, fact, candidate, reopened)
		if err != nil {
			return err
		}
		if v.Deleted {
			expected.Value = 0
		}
		if e == expected && (!v.Deleted || !reopened) {
			return nil
		}
	}
	return errInvalid
}
