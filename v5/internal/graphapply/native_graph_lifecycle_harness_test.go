package graphapply

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

const nativeLifecycleFixture = "graph-lifecycle-seven-v1"
const nativeLifecycleBytes = 64 << 10

// These declarations configure only fresh lifecycle test stores. They do not
// alter the accepted E15/E18 schema or an existing store's immutable genesis.
func nativeLifecycleSchemas() []graphstate.PropertyDefinition {
	return []graphstate.PropertyDefinition{{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}, {Name: "p", Owner: graphstate.Relationship, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}}
}

// This is a client command factory, not an observation/oracle. Each complete
// edge-create command includes its initial property in the SAME transaction.
func nativeLifecycleProposal(o *certifiedProcessOwners, r processRequest, id requestID) (allocationProposal, error) {
	if o == nil || o.recipient == nil || os.Getenv("RHO_GRAPH_LIFECYCLE_PROCESS") != "1" || r.index < 1 || r.index > 7 || len(r.data) != 0 {
		return allocationProposal{}, errInvalid
	}
	cfg, err := o.host.currentConfiguration()
	if err != nil {
		return allocationProposal{}, err
	}
	scope, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return allocationProposal{}, err
	}
	grant := grantReference{session: o.recipient.session, sequence: 1}
	var ops []graphstate.Operation
	var claims []freshBinding
	switch r.index {
	case 1, 2:
		owner, life := graphstate.EntityID(1), graphstate.LifeID(2)
		if r.index == 2 {
			owner, life = 17, 18
		}
		ops = []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: owner, Life: life, Scope: scope, Record: graphstate.EntityRecord{Interpretation: graphstate.InterpretationObservation, TemporalRole: graphstate.TemporalRoleValidity}}}
		claims = []freshBinding{{role: entityBinding, id: uint64(owner), grant: grant}, {role: lifeBinding, owner: owner, id: uint64(life), grant: grant}}
	case 3:
		ops = []graphstate.Operation{{Kind: graphstate.CreateRelationship, Owner: 3, Life: 4, Scope: scope, Record: graphstate.EntityRecord{Type: "edge", Source: 1, Target: 17, Mode: graphstate.LifeBound, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleOccurrenceTime}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: 18}}, {Kind: graphstate.Set, Owner: 3, Life: 4, Scope: scope, Name: "p", Value: graphstate.I64(5), ValueID: 5}}
		claims = []freshBinding{{role: entityBinding, id: 3, grant: grant}, {role: lifeBinding, owner: 3, id: 4, grant: grant}, {role: valueBinding, id: 5, grant: grant}}
	case 4:
		ops = []graphstate.Operation{{Kind: graphstate.Set, Owner: 3, Life: 4, Scope: scope, Name: "p", Value: graphstate.I64(6), ValueID: 6}}
		claims = []freshBinding{{role: valueBinding, id: 6, grant: grant}}
	case 5:
		ops = []graphstate.Operation{{Kind: graphstate.Close, Owner: 1, Life: 2, Scope: scope}}
	case 6:
		ops = []graphstate.Operation{{Kind: graphstate.Close, Owner: 3, Life: 4, Scope: scope}}
	case 7:
		ops = []graphstate.Operation{{Kind: graphstate.Reopen, Owner: 1, Life: 7, Scope: scope}}
		claims = []freshBinding{{role: lifeBinding, owner: 1, id: 7, grant: grant}}
	}
	return o.host.PreparePartitionGraphOperations(id, ops, r.index, claims)
}

type nativeLifecycleCell struct {
	Present    bool   `json:"present"`
	Null       bool   `json:"null"`
	Handle     uint64 `json:"handle"`
	Revision   uint64 `json:"revision"`
	Provenance uint64 `json:"provenance"`
	Kind       string `json:"kind"`
	I64        int64  `json:"i64"`
}
type nativeLifecycleEntity struct {
	ID            uint64              `json:"id"`
	Found         bool                `json:"found"`
	Kind          byte                `json:"kind"`
	Source        uint64              `json:"source"`
	Target        uint64              `json:"target"`
	Mode          byte                `json:"mode"`
	Type          string              `json:"type"`
	Presence      nativeLifecycleCell `json:"presence"`
	Effective     bool                `json:"effective"`
	EffectiveLife uint64              `json:"effectiveLife"`
	Labels        []string            `json:"labels"`
	Property      nativeLifecycleCell `json:"property"`
}
type nativeLifecycleLife struct {
	Owner      uint64 `json:"owner"`
	Life       uint64 `json:"life"`
	Found      bool   `json:"found"`
	SourceLife uint64 `json:"sourceLife"`
	TargetLife uint64 `json:"targetLife"`
}
type nativeLifecyclePosting struct {
	Endpoint   uint64   `json:"endpoint"`
	Life       uint64   `json:"life"`
	Candidates []uint64 `json:"candidates"`
	Declared   []uint64 `json:"declared"`
	Effective  []uint64 `json:"effective"`
}
type nativeLifecycleObservation struct {
	Graph            string                   `json:"graph"`
	ObservationScope string                   `json:"observationScope"`
	Census           []nativeLifecycleEntity  `json:"entityCensus"`
	Entities         []nativeLifecycleEntity  `json:"entities"`
	Lives            []nativeLifecycleLife    `json:"lives"`
	Postings         []nativeLifecyclePosting `json:"postings"`
	PhantomEntities  []nativeLifecycleEntity  `json:"phantomEntities"`
}

func nativeLifecycleCellRead(ctx context.Context, v graphstate.ReadView, key graphstate.ComponentKey, scope temporal.Scope, point temporal.Position, l state.Limits) (nativeLifecycleCell, error) {
	page, err := v.ComponentPage(ctx, graphstate.ComponentQuery{Key: key, Window: scope}, 0, graphstate.ReadBudget{Rows: 64, Bytes: 64 << 10})
	if err != nil {
		return nativeLifecycleCell{}, err
	}
	if !page.Complete || page.Next != 0 {
		return nativeLifecycleCell{}, errLimit
	}
	cell, err := page.Data.At(point, l)
	if err != nil {
		return nativeLifecycleCell{}, err
	}
	return nativeLifecycleCellFromState(ctx, v, key, cell)
}
func nativeLifecycleCellFromState(ctx context.Context, v graphstate.ReadView, key graphstate.ComponentKey, c state.Cell) (nativeLifecycleCell, error) {
	out := nativeLifecycleCell{Present: c.Present(), Null: c.Value().IsNull(), Handle: c.Value().ID(), Revision: c.Revision().ID(), Provenance: c.Revision().Provenance()}
	if !c.Present() {
		out.Kind = "absent"
		return out, nil
	}
	if c.Value().IsNull() {
		out.Kind = "null"
		return out, nil
	}
	if key.Kind == graphstate.Presence {
		out.Kind = "life"
		return out, nil
	}
	value, err := v.Value(ctx, graphstate.ValueID(out.Handle))
	if err != nil {
		return nativeLifecycleCell{}, err
	}
	if !value.Found {
		return nativeLifecycleCell{}, errCorrupt
	}
	var ok bool
	out.I64, ok = value.Value.Int64Value()
	if !ok {
		return nativeLifecycleCell{}, errCorrupt
	}
	out.Kind = "i64"
	return out, nil
}

// Everything below is read from one actual child-owned certified ReadView.
// It neither consults the literal oracle nor receives a parent-created handle.
func nativeLifecycleObserve(ctx context.Context, s *graphProcessCertifiedSessions, r processRequest) ([]byte, error) {
	if ctx == nil || s == nil || len(r.data) != 0 || r.entity > 2 {
		return nil, errInvalid
	}
	s.mu.Lock()
	owner, ok := s.owners[r.index]
	s.mu.Unlock()
	if !ok {
		return nil, errInvalid
	}
	v, err := owner.read.ReadView()
	if err != nil {
		return nil, err
	}
	s.coordinator.host.mu.Lock()
	cfg, err := s.coordinator.host.currentConfiguration()
	s.coordinator.host.mu.Unlock()
	if err != nil {
		return nil, err
	}
	scope, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return nil, err
	}
	point, err := temporal.RationalPosition(cfg.defaultAxis.Axis(), temporal.RationalInt64(0))
	if err != nil {
		return nil, err
	}
	l := s.coordinator.host.machine.limits.graph.Planner
	g := v.Graph()
	out := nativeLifecycleObservation{Graph: hex.EncodeToString(g[:])}
	out.ObservationScope = "graph-complete-in-issued-range"
	if r.entity == 1 {
		out.ObservationScope = "edge-only"
	}
	if r.entity == 2 {
		out.ObservationScope = "entity-census-1-through-32"
		for id := uint64(1); id <= 32; id++ {
			e, err := v.Entity(ctx, graphstate.EntityID(id))
			if err != nil {
				return nil, err
			}
			out.Census = append(out.Census, nativeLifecycleEntity{ID: id, Found: e.Found, Kind: byte(e.Record.Kind), Source: uint64(e.Record.Source), Target: uint64(e.Record.Target), Mode: byte(e.Record.Mode), Type: e.Record.Type})
		}
		wire, err := json.Marshal(out)
		if err != nil {
			return nil, err
		}
		if len(wire) > nativeLifecycleBytes {
			return nil, errLimit
		}
		return wire, nil
	}

	ids := []uint64{1, 17, 3}
	if r.entity == 1 {
		ids = []uint64{3}
	}
	for _, id := range ids {
		e, err := v.Entity(ctx, graphstate.EntityID(id))
		if err != nil {
			return nil, err
		}
		item := nativeLifecycleEntity{ID: id, Found: e.Found, Labels: []string{}}
		if e.Found {
			item.Kind, item.Source, item.Target, item.Mode, item.Type = byte(e.Record.Kind), uint64(e.Record.Source), uint64(e.Record.Target), byte(e.Record.Mode), e.Record.Type
			item.Presence, err = nativeLifecycleCellRead(ctx, v, graphstate.ComponentKey{Owner: graphstate.EntityID(id), Kind: graphstate.Presence}, scope, point, l.Component)
			if err != nil {
				return nil, err
			}
			projection, err := graphstate.Project(ctx, v, graphstate.EntityID(id), point, graphstate.Effective, l)
			if err != nil {
				return nil, err
			}
			item.Effective, item.EffectiveLife, item.Labels = projection.Active, uint64(projection.Life), slices.Clone(projection.Labels)
			// Enumerate stored label components even when endpoint masking hides the
			// projection. The fixture's empty labels are observations, never constants.
			keys, err := v.ComponentKeys(ctx, graphstate.KeyPredicate{Owner: graphstate.EntityID(id), Kind: graphstate.Label}, 0, graphstate.ReadBudget{Rows: 64, Bytes: 64 << 10})
			if err != nil {
				return nil, err
			}
			if !keys.Complete || keys.Next != 0 || len(keys.Keys) != 0 {
				return nil, errCorrupt
			}
			if id == 3 {
				item.Property, err = nativeLifecycleCellRead(ctx, v, graphstate.ComponentKey{Owner: 3, Life: 4, Kind: graphstate.ScalarProperty, Name: "p"}, scope, point, l.Component)
				if err != nil {
					return nil, err
				}
			}
		}
		out.Entities = append(out.Entities, item)
	}
	if r.entity == 1 {
		life, err := v.Life(ctx, 3, 4)
		if err != nil {
			return nil, err
		}
		out.Lives = append(out.Lives, nativeLifecycleLife{Owner: 3, Life: 4, Found: life.Found, SourceLife: uint64(life.Record.SourceLife), TargetLife: uint64(life.Record.TargetLife)})
	}
	if r.entity == 0 {
		for _, key := range [][2]uint64{{1, 2}, {1, 7}, {17, 18}, {3, 4}} {
			life, err := v.Life(ctx, graphstate.EntityID(key[0]), graphstate.LifeID(key[1]))
			if err != nil {
				return nil, err
			}
			out.Lives = append(out.Lives, nativeLifecycleLife{Owner: key[0], Life: key[1], Found: life.Found, SourceLife: uint64(life.Record.SourceLife), TargetLife: uint64(life.Record.TargetLife)})
		}
		for _, key := range [][2]uint64{{1, 0}, {1, 2}, {1, 7}, {17, 0}, {17, 18}} {
			page, err := v.IncidentRelationships(ctx, graphstate.IncidentPredicate{Endpoint: graphstate.EntityID(key[0]), Life: graphstate.LifeID(key[1]), Window: scope}, 0, graphstate.ReadBudget{Rows: 64, Bytes: 64 << 10})
			if err != nil {
				return nil, err
			}
			if !page.Complete || page.Next != 0 || len(page.Entities) > 1 {
				return nil, errLimit
			}
			p := nativeLifecyclePosting{Endpoint: key[0], Life: key[1], Candidates: []uint64{}, Declared: []uint64{}, Effective: []uint64{}}
			for _, id := range page.Entities {
				p.Candidates = append(p.Candidates, uint64(id))
				var edge *nativeLifecycleEntity
				for i := range out.Entities {
					if out.Entities[i].ID == uint64(id) {
						edge = &out.Entities[i]
					}
				}
				if edge == nil || !edge.Found {
					return nil, errCorrupt
				}
				if edge.Presence.Present {
					p.Declared = append(p.Declared, uint64(id))
				}
				if edge.Effective {
					p.Effective = append(p.Effective, uint64(id))
				}
			}
			out.Postings = append(out.Postings, p)
		}
		for _, id := range []uint64{15, 31} {
			e, err := v.Entity(ctx, graphstate.EntityID(id))
			if err != nil {
				return nil, err
			}
			out.PhantomEntities = append(out.PhantomEntities, nativeLifecycleEntity{ID: id, Found: e.Found})
		}
	}
	wire, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	if len(wire) > nativeLifecycleBytes {
		return nil, errLimit
	}
	return wire, nil
}
func decodeNativeLifecycleObservation(wire []byte) (nativeLifecycleObservation, error) {
	if len(wire) == 0 || len(wire) > nativeLifecycleBytes {
		return nativeLifecycleObservation{}, errLimit
	}
	var out nativeLifecycleObservation
	d := json.NewDecoder(bytes.NewReader(wire))
	d.DisallowUnknownFields()
	if err := d.Decode(&out); err != nil {
		return nativeLifecycleObservation{}, errors.Join(errCorrupt, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nativeLifecycleObservation{}, errors.Join(errCorrupt, err)
	}
	if len(out.Entities) > 3 || len(out.Lives) > 4 || len(out.Postings) > 5 || len(out.PhantomEntities) > 2 || len(out.Census) > 32 {
		return nativeLifecycleObservation{}, errCorrupt
	}
	return out, nil
}

// Identity and operation maps are external correspondence, not observed graph
// content. Canonicalization must fail on an unrecognized native reference.
type lifecycleIDMap struct {
	Logical, Role, Owner string
	Graph                string
	Raw                  uint64
}
type lifecycleOperationMap struct {
	Logical              string
	Coordinator          uint64
	Request, Transaction string
	Round                uint64
}
type lifecycleCell struct {
	Present, Null     bool
	Kind, Reference   string
	I64               int64
	RevisionOperation string
	Provenance        uint64
}
type lifecycleEntity struct {
	ID, Kind, Mode, Type, Source, Target string
	Presence                             lifecycleCell
	Effective                            bool
	EffectiveLife                        string
	Labels                               []string
	Property                             lifecycleCell
}
type lifecycleLife struct{ Owner, Life, SourceLife, TargetLife string }
type lifecyclePosting struct {
	Endpoint, Life      string
	Declared, Effective []string
}
type lifecycleView struct {
	ObservationScope string
	CensusBounds     [2]uint64
	CensusIDs        []string
	Entities         []lifecycleEntity
	Lives            []lifecycleLife
	Postings         []lifecyclePosting
	PhantomCount     int
	PhantomFound     []bool
}
type lifecycleRawRecord struct {
	Partition, Index uint64
	Outcome, CDC     []byte
}
type lifecycleRawCut struct{ Certificates [][]byte }
type lifecycleChange struct {
	Owner, Life, Component, Name string
	Scope                        []byte
	AllScope                     lifecycleAllScope
	Before, After                lifecycleCell
}
type lifecycleValue struct {
	Reference, Kind string
	I64             int64
}
type lifecycleStep struct {
	Operation, Coordinator string
	Before, After          lifecycleView
	Cut                    lifecycleRawCut
	RawRecords             []lifecycleRawRecord
	LogicalCDC             []lifecycleChange
	NewEntities            []lifecycleEntity
	NewLives               []lifecycleLife
	NewValues              []lifecycleValue
	RawObservation         nativeLifecycleObservation
}
type lifecycleOldRead struct {
	Operation, Mode string
	Cut             lifecycleRawCut
	View            lifecycleView
	RawObservation  nativeLifecycleObservation
}
type lifecycleTrace struct {
	RawCounts     lifecycleTraceCounts    `json:"rawCounts"`
	SchemaVersion int                     `json:"schemaVersion"`
	Fixture       string                  `json:"fixtureID"`
	Engine        string                  `json:"engine"`
	Normalization []string                `json:"normalizationRules"`
	IdentityMap   []lifecycleIDMap        `json:"identityMapping"`
	OperationMap  []lifecycleOperationMap `json:"operationMapping"`
	InitialCut    lifecycleRawCut         `json:"initialCut"`
	Steps         []lifecycleStep         `json:"steps"`
	OldReads      []lifecycleOldRead      `json:"oldReads"`
	Scope         string                  `json:"scope"`
}

func lifecycleRawCertificate(c graphCut) (lifecycleRawCut, error) {
	out := lifecycleRawCut{}
	for _, cert := range c.certificates {
		wire, err := encodeGraphCertificate(cert)
		if err != nil {
			return out, err
		}
		out.Certificates = append(out.Certificates, wire)
	}
	return out, nil
}
func lifecycleIDs() []lifecycleIDMap {
	graph := hex.EncodeToString([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	return []lifecycleIDMap{{"node-a", "entity", "", graph, 1}, {"life-a1", "life", "node-a", graph, 2}, {"node-b", "entity", "", graph, 17}, {"life-b1", "life", "node-b", graph, 18}, {"edge", "entity", "", graph, 3}, {"life-edge", "life", "edge", graph, 4}, {"value5", "value", "", graph, 5}, {"value6", "value", "", graph, 6}, {"life-a2", "life", "node-a", graph, 7}}
}
func lifecycleID(ids []lifecycleIDMap, role string, raw, owner uint64) (string, error) {
	if raw == 0 {
		return "", nil
	}
	ownerName := ""
	if owner != 0 {
		var err error
		ownerName, err = lifecycleID(ids, "entity", owner, 0)
		if err != nil {
			return "", err
		}
	}
	for _, id := range ids {
		if id.Role == role && id.Raw == raw && id.Owner == ownerName {
			return id.Logical, nil
		}
	}
	return "", errCorrupt
}
func lifecycleNormalizeCell(c nativeLifecycleCell, key graphstate.ComponentKey, ids []lifecycleIDMap, ops []lifecycleOperationMap) (lifecycleCell, error) {
	out := lifecycleCell{Present: c.Present, Null: c.Null, Kind: c.Kind, I64: c.I64, Provenance: c.Provenance}
	if c.Revision != 0 {
		for _, op := range ops {
			if op.Round == c.Revision {
				out.RevisionOperation = op.Logical
			}
		}
		if out.RevisionOperation == "" {
			return lifecycleCell{}, errCorrupt
		}
	}
	if c.Present && !c.Null {
		role, owner := "value", uint64(0)
		if key.Kind == graphstate.Presence {
			role, owner = "life", uint64(key.Owner)
		}
		var err error
		out.Reference, err = lifecycleID(ids, role, c.Handle, owner)
		if err != nil {
			return lifecycleCell{}, err
		}
	}
	return out, nil
}
func lifecycleNormalizeView(raw nativeLifecycleObservation, ids []lifecycleIDMap, ops []lifecycleOperationMap) (lifecycleView, error) {
	for _, id := range ids {
		if id.Graph != raw.Graph {
			return lifecycleView{}, errCorrupt
		}
	}
	out := lifecycleView{ObservationScope: raw.ObservationScope, Entities: []lifecycleEntity{}, Lives: []lifecycleLife{}, Postings: []lifecyclePosting{}, PhantomFound: []bool{}}
	for _, e := range raw.Entities {
		if !e.Found {
			continue
		}
		id, err := lifecycleID(ids, "entity", e.ID, 0)
		if err != nil {
			return out, err
		}
		if e.Kind != byte(graphstate.Node) && e.Kind != byte(graphstate.Relationship) {
			return out, errCorrupt
		}
		n := lifecycleEntity{ID: id, Kind: "node", Labels: slices.Clone(e.Labels)}
		if e.Kind == byte(graphstate.Relationship) {
			if e.Mode != byte(graphstate.LifeBound) {
				return out, errCorrupt
			}
			n.Kind, n.Mode, n.Type = "relationship", "life-bound", e.Type
			n.Source, err = lifecycleID(ids, "entity", e.Source, 0)
			if err != nil {
				return out, err
			}
			n.Target, err = lifecycleID(ids, "entity", e.Target, 0)
			if err != nil {
				return out, err
			}
		}
		n.Presence, err = lifecycleNormalizeCell(e.Presence, graphstate.ComponentKey{Owner: graphstate.EntityID(e.ID), Kind: graphstate.Presence}, ids, ops)
		if err != nil {
			return out, err
		}
		n.Effective = e.Effective
		n.EffectiveLife, err = lifecycleID(ids, "life", e.EffectiveLife, e.ID)
		if err != nil {
			return out, err
		}
		if e.ID == 3 {
			n.Property, err = lifecycleNormalizeCell(e.Property, graphstate.ComponentKey{Owner: 3, Life: 4, Kind: graphstate.ScalarProperty, Name: "p"}, ids, ops)
			if err != nil {
				return out, err
			}
		}
		out.Entities = append(out.Entities, n)
	}
	for _, l := range raw.Lives {
		if !l.Found {
			continue
		}
		n := lifecycleLife{}
		var err error
		n.Owner, err = lifecycleID(ids, "entity", l.Owner, 0)
		if err != nil {
			return out, err
		}
		n.Life, err = lifecycleID(ids, "life", l.Life, l.Owner)
		if err != nil {
			return out, err
		}
		if l.Owner == 3 {
			n.SourceLife, err = lifecycleID(ids, "life", l.SourceLife, 1)
			if err != nil {
				return out, err
			}
			n.TargetLife, err = lifecycleID(ids, "life", l.TargetLife, 17)
			if err != nil {
				return out, err
			}
		}
		out.Lives = append(out.Lives, n)
	}
	for _, p := range raw.Postings {
		n := lifecyclePosting{Declared: []string{}, Effective: []string{}}
		var err error
		n.Endpoint, err = lifecycleID(ids, "entity", p.Endpoint, 0)
		if err != nil {
			return out, err
		}
		n.Life, err = lifecycleID(ids, "life", p.Life, p.Endpoint)
		if err != nil {
			return out, err
		}
		for _, r := range []struct {
			ids []uint64
			to  *[]string
		}{{p.Declared, &n.Declared}, {p.Effective, &n.Effective}} {
			for _, id := range r.ids {
				v, err := lifecycleID(ids, "entity", id, 0)
				if err != nil {
					return out, err
				}
				*r.to = append(*r.to, v)
			}
		}
		out.Postings = append(out.Postings, n)
	}
	if raw.ObservationScope == "graph-complete-in-issued-range" {
		if len(raw.Census) != 32 {
			return out, errCorrupt
		}
		out.CensusBounds = [2]uint64{1, 32}
		out.CensusIDs = []string{}
		for i, e := range raw.Census {
			if e.ID != uint64(i+1) {
				return out, errCorrupt
			}
			if e.Found {
				name, err := lifecycleID(ids, "entity", e.ID, 0)
				if err != nil {
					return out, err
				}
				out.CensusIDs = append(out.CensusIDs, name)
			}
		}
		slices.Sort(out.CensusIDs)
	}
	out.PhantomCount = len(raw.PhantomEntities)
	for _, p := range raw.PhantomEntities {
		out.PhantomFound = append(out.PhantomFound, p.Found)
	}
	return out, nil
}

type nativeLifecycleChange struct {
	Owner, Life   uint64
	Component     byte
	Name          string
	AllScope      lifecycleAllScope
	Scope         []byte
	Before, After nativeLifecycleCell
}
type nativeLifecycleValue struct {
	ID   uint64
	Kind string
	I64  int64
}
type nativeLifecycleRecord struct {
	Partition, Index uint64
	Outcome, CDC     []byte
	Changes          []nativeLifecycleChange
	Entities         []nativeLifecycleEntity
	Lives            []nativeLifecycleLife
	Values           []nativeLifecycleValue
}

// This is independent retained-record inspection on an actual voter. It does
// not certify a cut, submit a batch, or use expected operations to invent CDC.
func nativeLifecycleRecordRead(ctx context.Context, h *allocationHost, index uint64) (nativeLifecycleRecord, error) {
	return nativeGraphRecordRead(ctx, h, index, false)
}
func nativeGraphRecordRead(ctx context.Context, h *allocationHost, index uint64, allowLocal bool) (out nativeLifecycleRecord, err error) {
	if ctx == nil || h == nil || index == 0 {
		return out, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if index > h.driver.Applied() {
		return out, errInvalid
	}
	out.Partition, out.Index = h.machine.ns.partition, index
	out.Outcome, err = h.machine.store.ApplicationRecord(ctx, index, true, 64<<10)
	if err != nil {
		return out, err
	}
	out.CDC, err = h.machine.store.ApplicationRecord(ctx, index, false, 1<<20)
	if err != nil {
		return out, err
	}
	if len(out.CDC) == 0 {
		return out, nil
	}
	status, statusErr := decodeCrossStatus(out.Outcome)
	var original outcome
	if statusErr == nil {
		if status.phase != crossResolve || !status.commit {
			return out, errCorrupt
		}
		original = outcome{ns: status.ns, kind: graphOperations, identity: status.tx, hash: status.digest, index: status.index, disposition: applied}
	} else if allowLocal {
		original, err = decodePartitionGraphOutcome(out.Outcome, h.machine.ns)
		if err != nil || original.reason != reasonNone || original.disposition != applied {
			return out, errors.Join(errCorrupt, err)
		}
	} else {
		return out, errors.Join(errCorrupt, statusErr)
	}
	logical, err := decodeTypedChangeEnvelope(out.CDC, original, h.machine.limits, true)
	if err != nil {
		return out, err
	}
	cfg, err := h.currentConfiguration()
	if err != nil {
		return out, err
	}
	digest, err := cfg.digest()
	if err != nil {
		return out, err
	}
	backend, err := h.machine.store.ApplicationView(index)
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, backend.Close()) }()
	l := h.machine.limits
	view, _, err := graphstore.OpenPartitionGraphReadView(ctx, backend, h.machine.declaration, cfg.defaultAxis, digest, l.catalog, l.graph, graphstore.OwnershipBudget{SourceRows: l.sourceRows, SourceBytes: l.sourceBytes, OutputBytes: l.outputBytes})
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, view.Close()) }()
	for _, e := range logical.entities {
		out.Entities = append(out.Entities, nativeLifecycleEntity{ID: uint64(e.ID), Found: true, Kind: byte(e.Kind), Type: e.Type, Mode: byte(e.Mode), Source: uint64(e.Source), Target: uint64(e.Target)})
	}
	for _, life := range logical.lives {
		out.Lives = append(out.Lives, nativeLifecycleLife{Owner: uint64(life.Owner), Life: uint64(life.Life), Found: true, SourceLife: uint64(life.SourceLife), TargetLife: uint64(life.TargetLife)})
	}
	for _, value := range logical.values {
		i64, ok := value.Value.Int64Value()
		if !ok {
			return out, errCorrupt
		}
		out.Values = append(out.Values, nativeLifecycleValue{ID: uint64(value.ID), Kind: "i64", I64: i64})
	}
	for _, group := range logical.groups {
		for _, change := range group.Changes {
			scope, err := temporal.AppendScope(nil, change.Scope(), l.catalog.Temporal)
			if err != nil {
				return out, err
			}
			allScope, err := lifecycleCanonicalScope(scope, change.Scope().Axis(), l.catalog.Temporal)
			if err != nil {
				return out, err
			}
			before, err := nativeLifecycleCellFromState(ctx, view, group.Key, change.Before())
			if err != nil {
				return out, err
			}
			after, err := nativeLifecycleCellFromState(ctx, view, group.Key, change.After())
			if err != nil {
				return out, err
			}
			out.Changes = append(out.Changes, nativeLifecycleChange{Owner: uint64(group.Key.Owner), Life: uint64(group.Key.Life), Component: byte(group.Key.Kind), Name: group.Key.Name, Scope: scope, AllScope: allScope, Before: before, After: after})
		}
	}
	return out, nil
}
func nativeLifecycleRecordWire(ctx context.Context, h *allocationHost, index uint64) ([]byte, error) {
	out, err := nativeLifecycleRecordRead(ctx, h, index)
	if err != nil {
		return nil, err
	}
	wire, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	if len(wire) > nativeLifecycleBytes {
		return nil, errLimit
	}
	return wire, nil
}

// These are measured raw artifact/IPC counts, not inferred physical install
// writes. Native Host exposes retained records/stats, not a write-count hook.
type lifecycleTraceCounts struct {
	CensusOperations        int                          `json:"censusOperations"`
	CensusTraffic           map[uint64][2]int            `json:"censusGetScanTraffic"`
	CertifiedReadOperations int                          `json:"certifiedReadOperations"`
	ApplicationRecords      int                          `json:"applicationRecords"`
	OutcomeBytes            int                          `json:"outcomeBytes"`
	CDCBytes                int                          `json:"cdcBytes"`
	ComponentChanges        int                          `json:"componentChanges"`
	ExactInstallWriteCount  *uint64                      `json:"exactInstallWriteCount"`
	QueryTraffic            map[uint64][2]int            `json:"queryGetScanTraffic"`
	VoterStats              map[uint64][statCount]uint64 `json:"voterStats"`
}

type lifecycleAllScope struct {
	Kind          string `json:"kind"`
	AxisProfile   string `json:"axisProfile"`
	AxisVersion   uint16 `json:"axisVersion"`
	CanonicalUnit string `json:"canonicalUnit"`
	Reference     string `json:"reference"`
}

func lifecycleCanonicalScope(raw []byte, axis temporal.Axis, l temporal.Limits) (lifecycleAllScope, error) {
	decoded, err := temporal.DecodeScope(raw, axis, l)
	if err != nil {
		return lifecycleAllScope{}, errors.Join(errCorrupt, err)
	}
	if decoded.Kind() != temporal.ScopeAll {
		return lifecycleAllScope{}, errCorrupt
	}
	d := decoded.Axis().Descriptor()
	profile := ""
	switch d.Profile {
	case temporal.ProfileIntegerZ:
		profile = "Z"
	case temporal.ProfileRationalQ:
		profile = "Q"
	case temporal.ProfileLexicographicQN:
		profile = "QxN"
	default:
		return lifecycleAllScope{}, errCorrupt
	}
	return lifecycleAllScope{Kind: "all", AxisProfile: profile, AxisVersion: d.Version, CanonicalUnit: d.CanonicalUnit, Reference: d.Reference}, nil
}
