package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const lifecycleFixture = "graph-lifecycle-seven-v1"

type traceIdentity struct {
	Logical, Role, Owner, Graph string
	Raw                         uint64
}
type traceOperation struct {
	Logical              string
	Coordinator          uint8
	Request, Transaction string
	Round                uint64
}
type traceCell struct {
	Present, Null     bool
	Kind, Reference   string
	I64               int64
	RevisionOperation string
	Provenance        uint64
}
type traceEntity struct {
	ID, Kind, Mode, Type, Source, Target string
	Presence                             traceCell
	Effective                            bool
	EffectiveLife                        string
	Labels                               []string
	Property                             traceCell
}
type traceLife struct{ Owner, Life, SourceLife, TargetLife string }
type tracePosting struct {
	Endpoint, Life      string
	Declared, Effective []string
}
type traceView struct {
	ObservationScope string
	CensusBounds     [2]uint64
	CensusIDs        []string
	Entities         []traceEntity
	Lives            []traceLife
	Postings         []tracePosting
	PhantomCount     int
	PhantomFound     []bool
}
type traceChange struct {
	Owner, Life, Component, Name string
	Scope                        []byte
	AllScope                     lifecycleScope
	Before, After                traceCell
}
type traceValue struct {
	Reference, Kind string
	I64             int64
}
type traceKV struct {
	Group      uint8
	Key, Value []byte
}
type traceRawObservation struct {
	ObservationScope string
	History, Config  []traceKV
	Groups           []changeGroup
	Census           [32]bool
}
type traceStep struct {
	Operation, Coordinator string
	Before, After          traceView
	Cut                    certificate
	RawRecords             []traceKV
	LogicalCDC             []traceChange
	NewEntities            []traceEntity
	NewLives               []traceLife
	NewValues              []traceValue
	RawObservation         traceRawObservation
}
type traceOldRead struct {
	Operation, Mode string
	Cut             certificate
	View            traceView
	RawObservation  traceRawObservation
}
type fdbLifecycleTrace struct {
	SchemaVersion   int              `json:"schemaVersion"`
	Fixture         string           `json:"fixtureID"`
	Engine          string           `json:"engine"`
	Normalization   []string         `json:"normalizationRules"`
	IdentityMap     []traceIdentity  `json:"identityMapping"`
	OperationMap    []traceOperation `json:"operationMapping"`
	InitialCut      certificate      `json:"initialCut"`
	Steps           []traceStep      `json:"steps"`
	OldReads        []traceOldRead   `json:"oldReads"`
	RawCounts       map[string]any   `json:"rawCounts"`
	FinalRawCurrent []traceKV        `json:"finalRawCurrent"`
	Scope           string           `json:"scope"`
}

func traceIDs(c config) []traceIdentity {
	p := c.Lifecycle
	return []traceIdentity{{"node-a", "entity", "", c.Graph, c.Nodes[0]}, {"life-a1", "life", "node-a", c.Graph, p.NodeLives[0]}, {"node-b", "entity", "", c.Graph, c.Nodes[1]}, {"life-b1", "life", "node-b", c.Graph, p.NodeLives[1]}, {"edge", "entity", "", c.Graph, p.Edge}, {"life-edge", "life", "edge", c.Graph, p.EdgeLife}, {"value5", "value", "", c.Graph, p.Values[0].ID}, {"value6", "value", "", c.Graph, p.Values[1].ID}, {"life-a2", "life", "node-a", c.Graph, p.ReopenedLife}}
}
func traceID(ids []traceIdentity, role string, raw uint64) (string, error) {
	for _, id := range ids {
		if id.Role == role && id.Raw == raw {
			return id.Logical, nil
		}
	}
	return "", errInvalid
}
func tracePresent(kind, reference, operation string, provenance uint64) traceCell {
	return traceCell{Present: true, Kind: kind, Reference: reference, RevisionOperation: operation, Provenance: provenance}
}
func traceAbsent(operation string, provenance uint64) traceCell {
	return traceCell{Kind: "absent", RevisionOperation: operation, Provenance: provenance}
}
func (h *harness) lifecycleRawRows(c cut, kind string) ([]traceKV, error) {
	var result []traceKV
	for g := uint8(0); g < 2; g++ {
		begin := fdb.Key(h.prefix(g) + kind + "/")
		end := prefixRange(h.prefix(g) + kind + "/").End
		if kind == "history" {
			end = fdb.Key(fmt.Sprintf("%shistory/%016x0", h.prefix(g), c.record.Round))
		}
		for {
			got, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
				if err := h.verifyCut(tr, c); err != nil {
					return nil, err
				}
				if err := h.checkConfig(tr, g); err != nil {
					return nil, err
				}
				return tr.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Limit: 128}).GetSliceWithError()
			})
			if err != nil {
				return nil, err
			}
			rows := got.([]fdb.KeyValue)
			for _, row := range rows {
				result = append(result, traceKV{g, bytes.Clone(row.Key), bytes.Clone(row.Value)})
			}
			if uint64(len(result)) > 2*h.config.Limits.Versions {
				return nil, errLimit
			} // validated <=65,536.
			if len(rows) < 128 {
				break
			}
			begin = append(bytes.Clone(rows[len(rows)-1].Key), 0)
		}
	}
	return result, nil
}
func (h *harness) lifecycleConfigRows(c cut) ([]traceKV, error) {
	var rows []traceKV
	for g := uint8(0); g < 2; g++ {
		key := h.key(g, "meta", "config")
		got, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
			if err := h.verifyCut(tr, c); err != nil {
				return nil, err
			}
			if err := h.checkConfig(tr, g); err != nil {
				return nil, err
			}
			return tr.Get(key).Get()
		})
		if err != nil {
			return nil, err
		}
		rows = append(rows, traceKV{g, bytes.Clone(key), bytes.Clone(got.([]byte))})
	}
	return rows, nil
}
func (h *harness) lifecycleRecordRows(c cut, group changeGroup) ([]traceKV, error) {
	o := group.Outcome
	g := o.Coordinator
	var rows []traceKV
	for _, key := range []fdb.Key{h.changeKey(g, o.Sequence, o.Request), h.key(g, "request", o.Request), h.key(g, "id", o.ID)} {
		got, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
			if err := h.verifyCut(tr, c); err != nil {
				return nil, err
			}
			return tr.Get(key).Get()
		})
		if err != nil {
			return nil, err
		}
		raw := got.([]byte)
		var expected []byte
		if bytes.Equal(key, h.changeKey(g, o.Sequence, o.Request)) {
			expected, err = wire(group)
		} else {
			expected, err = wire(o)
		}
		if err != nil || !bytes.Equal(raw, expected) {
			return nil, errUnavailable
		}
		rows = append(rows, traceKV{g, bytes.Clone(key), bytes.Clone(raw)})
	}
	return rows, nil
}

func (h *harness) lifecycleValidateCurrent(rows []traceKV, groups []changeGroup) error {
	ledger := map[string]value{}
	for _, group := range groups {
		for _, write := range group.Writes {
			key := fmt.Sprintf("%d/%s", write.Group, h.key(write.Group, "current", write.Key))
			ledger[key] = write.After
		}
	}
	if len(rows) != len(ledger) {
		return errUnavailable
	}
	for _, row := range rows {
		key := fmt.Sprintf("%d/%s", row.Group, row.Key)
		want, found := ledger[key]
		got, err := decode[value](row.Value)
		if err != nil || !found || got != want {
			return errUnavailable
		}
		delete(ledger, key)
	}
	if len(ledger) != 0 {
		return errUnavailable
	}
	return nil
}

// The fold consumes only actual history and complete committed groups. The
// seven-step literal oracle is deliberately absent from this projection path.
func (h *harness) lifecycleFold(history []traceKV, groups []changeGroup) (traceView, []traceStep, [32]bool, error) {
	p := h.config.Lifecycle
	ids := traceIDs(h.config)
	view := traceView{ObservationScope: "graph-complete-in-fixture-domain", CensusBounds: [2]uint64{1, 32}, CensusIDs: []string{}, Entities: []traceEntity{}, Lives: []traceLife{}, Postings: []tracePosting{}, PhantomCount: 2, PhantomFound: []bool{false, false}}
	var census [32]bool
	historyValues := map[string]value{}
	for _, row := range history {
		prefix := h.prefix(row.Group) + "history/"
		if row.Group > 1 || !strings.HasPrefix(string(row.Key), prefix) {
			return view, nil, census, errInvalid
		}
		tail := string(row.Key[len(prefix):])
		parts := strings.SplitN(tail, "/", 2)
		if len(parts) != 2 || len(parts[0]) != 16 {
			return view, nil, census, errInvalid
		}
		round, err := strconv.ParseUint(parts[0], 16, 64)
		if err != nil {
			return view, nil, census, errInvalid
		}
		key, err := hex.DecodeString(parts[1])
		if err != nil || !validName(string(key)) {
			return view, nil, census, errInvalid
		}
		v, err := decode[value](row.Value)
		if err != nil || !validGraphValue(v) || v.Round != round || fmt.Sprintf("%016x", round) != parts[0] || hex.EncodeToString(key) != parts[1] {
			return view, nil, census, errInvalid
		}
		name := fmt.Sprintf("%d/%016x/%s", row.Group, round, key)
		if _, found := historyValues[name]; found {
			return view, nil, census, errInvalid
		}
		historyValues[name] = v
	}
	physical := [2]map[string]value{{}, {}}
	entities := map[string]traceEntity{}
	lives := map[string]traceLife{}
	values := map[uint64]lifecycleDefinition{}
	var steps []traceStep
	seen := map[string]bool{}
	scope, err := wire(p.Scope)
	if err != nil {
		return view, nil, census, err
	}
	for _, group := range groups {
		if err := h.validateChange(group); err != nil {
			return view, nil, census, err
		}
		x, o := group.Tx, group.Outcome
		m := x.Lifecycle
		if seen[x.ID] || m == nil {
			return view, nil, census, errInvalid
		}
		seen[x.ID] = true
		for _, write := range group.Writes {
			before, found := physical[write.Group][write.Key]
			if found != (write.Before != nil) || found && before != *write.Before {
				return view, nil, census, errInvalid
			}
			name := fmt.Sprintf("%d/%016x/%s", write.Group, o.Round, write.Key)
			v, found := historyValues[name]
			if !found || v != write.After {
				return view, nil, census, errUnavailable
			}
			delete(historyValues, name)
		}
		g := *x.GraphChange
		if strings.HasPrefix(g.Kind, "edge-") {
			if len(group.Writes) != 3 {
				return view, nil, census, errInvalid
			}
			for _, w := range group.Writes[1:] {
				if w.After != group.Writes[0].After || !reflect.DeepEqual(w.Before, group.Writes[0].Before) {
					return view, nil, census, errInvalid
				}
			}
		}
		for _, write := range group.Writes {
			physical[write.Group][write.Key] = write.After
		}
		step := traceStep{Operation: x.ID, Coordinator: []string{"A", "B"}[x.Coordinator]}
		change := func(owner, life, component, name string, before, after traceCell) {
			step.LogicalCDC = append(step.LogicalCDC, traceChange{owner, life, component, name, scope, p.Scope, before, after})
		}
		if g.Kind == "node-create" || g.Kind == "node-close" {
			name, err := traceID(ids, "entity", g.Node.Node)
			if err != nil {
				return view, nil, census, err
			}
			life, err := traceID(ids, "life", g.Node.Life)
			if err != nil {
				return view, nil, census, err
			}
			e, found := entities[name]
			before := e.Presence
			if !found {
				if g.Kind != "node-create" {
					return view, nil, census, errInvalid
				}
				e = traceEntity{ID: name, Kind: "node", Labels: []string{}}
				before = traceAbsent("", 0)
				step.NewEntities = append(step.NewEntities, traceEntity{ID: name, Kind: "node"})
			}
			if g.Kind == "node-create" {
				if e.Presence.Present {
					return view, nil, census, errInvalid
				}
				l := traceLife{Owner: name, Life: life}
				if _, found := lives[life]; found {
					return view, nil, census, errInvalid
				}
				lives[life] = l
				step.NewLives = append(step.NewLives, l)
				e.Presence = tracePresent("life", life, x.ID, m.Provenance)
			} else {
				if !e.Presence.Present || e.Presence.Reference != life {
					return view, nil, census, errInvalid
				}
				e.Presence = traceAbsent(x.ID, m.Provenance)
			}
			entities[name] = e
			change(name, "", "presence", "", before, e.Presence)
		} else {
			e, found := entities["edge"]
			switch g.Kind {
			case "edge-create":
				if found {
					return view, nil, census, errInvalid
				}
				source, err := traceID(ids, "entity", g.Edge.Source.Node)
				if err != nil {
					return view, nil, census, err
				}
				target, err := traceID(ids, "entity", g.Edge.Target.Node)
				if err != nil {
					return view, nil, census, err
				}
				sourceLife, err := traceID(ids, "life", g.Edge.Source.Life)
				if err != nil {
					return view, nil, census, err
				}
				targetLife, err := traceID(ids, "life", g.Edge.Target.Life)
				if err != nil {
					return view, nil, census, err
				}
				edgeLife, err := traceID(ids, "life", m.EdgeLife)
				if err != nil {
					return view, nil, census, err
				}
				if entities[source].Presence.Reference != sourceLife || !entities[source].Presence.Present || entities[target].Presence.Reference != targetLife || !entities[target].Presence.Present {
					return view, nil, census, errInvalid
				}
				e = traceEntity{ID: "edge", Kind: "relationship", Mode: p.Mode, Type: p.Type, Source: source, Target: target, Labels: []string{}, Presence: tracePresent("life", edgeLife, x.ID, m.Provenance)}
				l := traceLife{"edge", edgeLife, sourceLife, targetLife}
				lives[edgeLife] = l
				step.NewLives = append(step.NewLives, l)
				step.NewEntities = append(step.NewEntities, traceEntity{ID: e.ID, Kind: e.Kind, Mode: e.Mode, Type: e.Type, Source: e.Source, Target: e.Target})
				change("edge", "", "presence", "", traceAbsent("", 0), e.Presence)
			case "edge-update":
				if !found || !e.Presence.Present {
					return view, nil, census, errInvalid
				}
			case "edge-close":
				if !found || !e.Presence.Present {
					return view, nil, census, errInvalid
				}
				before := e.Presence
				e.Presence = traceAbsent(x.ID, m.Provenance)
				change("edge", "", "presence", "", before, e.Presence)
			default:
				return view, nil, census, errInvalid
			}
			if g.Kind != "edge-close" {
				definition := *m.Definition
				if old, found := values[definition.ID]; found && old != definition {
					return view, nil, census, errInvalid
				}
				ref, err := traceID(ids, "value", definition.ID)
				if err != nil {
					return view, nil, census, err
				}
				if _, found := values[definition.ID]; !found {
					step.NewValues = append(step.NewValues, traceValue{ref, definition.Kind, definition.I64})
				}
				values[definition.ID] = definition
				before := e.Property
				if !before.Present {
					before = traceAbsent("", 0)
				}
				e.Property = traceCell{Present: true, Kind: definition.Kind, Reference: ref, I64: definition.I64, RevisionOperation: x.ID, Provenance: m.Provenance}
				change("edge", "life-edge", "property", p.Property, before, e.Property)
			}
			entities["edge"] = e
		}
		steps = append(steps, step)
	}
	if len(historyValues) != 0 {
		return view, nil, census, errUnavailable
	}
	// Stable fixture ordering is presentation only; membership comes from reads.
	for _, name := range []string{"node-a", "node-b", "edge"} {
		e, found := entities[name]
		if !found {
			continue
		}
		if e.Presence.Present {
			e.Effective = true
			e.EffectiveLife = e.Presence.Reference
		}
		if name == "edge" && e.Presence.Present {
			l := lives[e.Presence.Reference]
			e.Effective = entities[e.Source].Presence.Present && entities[e.Source].Presence.Reference == l.SourceLife && entities[e.Target].Presence.Present && entities[e.Target].Presence.Reference == l.TargetLife
		}
		view.Entities = append(view.Entities, e)
		view.CensusIDs = append(view.CensusIDs, name)
		for _, id := range ids {
			if id.Role == "entity" && id.Logical == name {
				census[id.Raw-1] = true
			}
		}
	}
	slices.Sort(view.CensusIDs)
	for _, name := range []string{"life-a1", "life-a2", "life-b1", "life-edge"} {
		if l, found := lives[name]; found {
			view.Lives = append(view.Lives, l)
		}
	}
	for _, key := range [][2]string{{"node-a", ""}, {"node-a", "life-a1"}, {"node-a", "life-a2"}, {"node-b", ""}, {"node-b", "life-b1"}} {
		posting := tracePosting{Endpoint: key[0], Life: key[1], Declared: []string{}, Effective: []string{}}
		if e, found := entities["edge"]; found {
			l := lives["life-edge"]
			matches := key[0] == e.Source && (key[1] == "" || key[1] == l.SourceLife) || key[0] == e.Target && (key[1] == "" || key[1] == l.TargetLife)
			if matches && e.Presence.Present {
				posting.Declared = append(posting.Declared, "edge")
			}
			for _, observed := range view.Entities {
				if observed.ID == "edge" && observed.Effective && matches {
					posting.Effective = append(posting.Effective, "edge")
				}
			}
		}
		view.Postings = append(view.Postings, posting)
	}
	return view, steps, census, nil
}

func (h *harness) lifecycleObserve(initial, c cut, compact bool) (traceRawObservation, traceView, []traceStep, error) {
	var raw traceRawObservation
	rows, err := h.lifecycleRawRows(c, "history")
	if err != nil {
		return raw, traceView{}, nil, err
	}
	groups, err := h.changes(initial, c, 1)
	if err != nil {
		return raw, traceView{}, nil, err
	}
	raw.History, raw.Groups = rows, groups
	raw.Config, err = h.lifecycleConfigRows(c)
	if err != nil {
		return raw, traceView{}, nil, err
	}
	view, steps, census, err := h.lifecycleFold(rows, groups)
	if err != nil {
		return raw, traceView{}, nil, err
	}
	raw.Census = census
	raw.ObservationScope = view.ObservationScope
	if compact {
		raw.ObservationScope = "edge-only"
		view.ObservationScope = "edge-only"
		view.CensusBounds = [2]uint64{}
		view.CensusIDs = nil
		view.Lives = slices.DeleteFunc(view.Lives, func(l traceLife) bool { return l.Owner != "edge" })
		view.Postings = []tracePosting{}
		view.PhantomCount = 0
		view.PhantomFound = []bool{}
		view.Entities = slices.DeleteFunc(view.Entities, func(e traceEntity) bool { return e.ID != "edge" })
	}
	return raw, view, steps, nil
}

func runLifecycleTrace(h *harness) error {
	if h == nil || h.config.Lifecycle.Version != 1 {
		return errInvalid
	}
	initial, err := h.fresh([]uint8{0, 1}, nil)
	if err != nil {
		return err
	}
	raw, view, _, err := h.lifecycleObserve(initial, initial, false)
	if err != nil {
		return err
	}
	if len(view.Entities) != 0 || len(raw.History) != 0 {
		return errInvalid
	}
	trace := fdbLifecycleTrace{SchemaVersion: 1, Fixture: lifecycleFixture, Engine: "foundationdb", IdentityMap: traceIDs(h.config), InitialCut: initial.record,
		Normalization: []string{"Declared bijective graph-qualified IDs; raw keys/values/cuts/groups retained", "Only AllScope descriptor and explicitly stored caller provenance/value definitions are compared", "Immutable declarations, presence and retained property are separate history projections", "Three edge/out/in physical revisions including tombstones are checked before logical collapse", "Complete-fixture census has declared fixture-domain authority, not allocator-issued-range authority", "Raw row/write counts include duplicated edge/out/in values; no physical storage-write count"},
		Scope:         "Fixed sequential lifecycle functional comparison; no generic rho backend, allocator, non-All temporal access, GC, cost/multi-host or engine selection"}
	ids := []string{"node-a", "node-b", "edge-create", "edge-update", "node-close", "edge-close", "node-reopen"}
	coords := []uint8{0, 1, 1, 0, 0, 1, 0}
	cuts := make([]cut, 7)
	var heldFirst traceView
	var last outcome
	for i, id := range ids {
		c, err := h.fresh([]uint8{0, 1}, nil)
		if err != nil {
			return err
		}
		_, before, _, err := h.lifecycleObserve(initial, c, false)
		if err != nil {
			return err
		}
		current, err := h.current()
		if err != nil {
			return err
		}
		command := scenarioCommand(i + 1)
		// Inputs are declared by the caller before submission, never inferred while
		// observing the stored result. Existing graph smoke/fault commands stay nil.
		x := h.graphTransaction(id, coords[i], command, current)
		x.Dependency = last.Round
		x.Lifecycle = &lifecycleMetadata{Provenance: uint64(i + 1)}
		if strings.HasPrefix(command.Kind, "edge-") {
			x.Lifecycle.EdgeLife = h.config.Lifecycle.EdgeLife
		}
		switch i {
		case 2:
			x.Lifecycle.Definition = new(h.config.Lifecycle.Values[0])
		case 3:
			x.Lifecycle.Definition = new(h.config.Lifecycle.Values[1])
		}
		o, err := h.submit(x)
		if err != nil {
			return err
		}
		if !o.Commit {
			return errInvalid
		}
		last = o
		c, err = h.fresh([]uint8{0, 1}, &o)
		if err != nil {
			return err
		}
		cuts[i] = c
		raw, after, steps, err := h.lifecycleObserve(initial, c, false)
		if err != nil {
			return err
		}
		if len(steps) != i+1 {
			return errUnavailable
		}
		step := steps[len(steps)-1]
		step.Before, step.After, step.Cut, step.RawObservation = before, after, c.record, raw
		step.RawRecords, err = h.lifecycleRecordRows(c, raw.Groups[len(raw.Groups)-1])
		if err != nil {
			return err
		}
		trace.Steps = append(trace.Steps, step)
		trace.OperationMap = append(trace.OperationMap, traceOperation{o.ID, o.Coordinator, o.Request, o.ID, o.Round})
		if i == 2 {
			_, heldFirst, _, err = h.lifecycleObserve(initial, c, true)
			if err != nil {
				return err
			}
		}
	}
	raw, held, _, err := h.lifecycleObserve(initial, cuts[2], true)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(held, heldFirst) {
		return errInvalid
	}
	trace.OldReads = append(trace.OldReads, traceOldRead{"edge-create", "held-old", cuts[2].record, held, raw})
	for _, i := range []int{2, 3, 4} {
		raw, v, _, err := h.lifecycleObserve(initial, cuts[i], false)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(v, trace.Steps[i].After) {
			return errInvalid
		}
		trace.OldReads = append(trace.OldReads, traceOldRead{ids[i], "reopened-at", cuts[i].record, v, raw})
	}
	trace.FinalRawCurrent, err = h.lifecycleRawRows(cuts[6], "current")
	if err != nil {
		return err
	}
	if err = h.lifecycleValidateCurrent(trace.FinalRawCurrent, trace.Steps[6].RawObservation.Groups); err != nil {
		return err
	}
	effects, historyBytes, groupBytes := 0, 0, 0
	for _, g := range trace.Steps[len(trace.Steps)-1].RawObservation.Groups {
		effects += len(g.Writes)
		b, err := wire(g)
		if err != nil {
			return err
		}
		groupBytes += len(b)
	}
	for _, r := range trace.Steps[len(trace.Steps)-1].RawObservation.History {
		historyBytes += len(r.Key) + len(r.Value)
	}
	trace.RawCounts = map[string]any{"completeGroups": len(trace.Steps), "graphEffectWrites": effects, "retainedHistoryRows": len(trace.Steps[6].RawObservation.History), "retainedHistoryKVBytes": historyBytes, "completeGroupBytes": groupBytes, "exactInstallWriteCount": nil, "representation": "I64 duplicated in edge/out/in; profile and caller metadata in config/complete groups"}
	b, err := json.MarshalIndent(trace, "", "  ")
	if err != nil {
		return err
	}
	if len(b) > 2<<20 {
		return errLimit
	}
	_, err = os.Stdout.Write(append(b, '\n'))
	return err
}
