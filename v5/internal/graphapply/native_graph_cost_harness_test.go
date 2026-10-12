package graphapply

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Fresh test-support profile only. Facts means relationship identities, not
// RPCs/history rows; three anchor nodes and allocator/setup are reported apart.
// No ordinary fixture limit or production storage/materializer changes here.
type nativeGraphCostSpec struct {
	Version      int    `json:"version"`
	Facts        uint64 `json:"facts"`
	CrossPercent uint64 `json:"crossPercent"`
	Writers      int    `json:"writers"`
}

func (s nativeGraphCostSpec) validate() error {
	if s.Version != 1 || s.Facts < 1 || s.Facts > 1_000_000 || s.Writers != 1 || s.CrossPercent != 0 && s.CrossPercent != 10 && s.CrossPercent != 50 && s.CrossPercent != 100 {
		return errInvalid
	}
	return nil
}
func nativeGraphCostSpecFromEnv() (*nativeGraphCostSpec, error) {
	text := os.Getenv("RHO_GRAPH_COST_SPEC")
	if text == "" {
		return nil, nil
	}
	if len(text) > 1024 {
		return nil, errLimit
	}
	var s nativeGraphCostSpec
	d := json.NewDecoder(bytes.NewBufferString(text))
	d.DisallowUnknownFields()
	if err := d.Decode(&s); err != nil {
		return nil, errors.Join(errInvalid, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.Join(errInvalid, err)
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	return &s, nil
}
func (s nativeGraphCostSpec) homeCount() uint64  { return 6 + 2*s.Facts }
func (s nativeGraphCostSpec) remoteNode() uint64 { return s.homeCount() + 1 }
func (s nativeGraphCostSpec) cross(i uint64) bool {
	return (i+1)*s.CrossPercent/100 > i*s.CrossPercent/100
}
func (s nativeGraphCostSpec) edge(i uint64) (uint64, uint64, uint64, uint64) {
	target, targetLife := uint64(3), uint64(4)
	if s.cross(i) {
		target, targetLife = s.remoteNode(), s.remoteNode()+1
	}
	return 7 + 2*i, 8 + 2*i, target, targetLife
}
func (s nativeGraphCostSpec) deliveries() int { return int((2*s.Facts + 128) * 4096) } // #nosec G115 -- selected darwin/linux arm64 fixture, Facts<=1M, allowance<9Gi.

func nativeGraphCostProfile(s *nativeGraphCostSpec, init *declaredInitCommand, l *raftlog.Limits, p *raftlog.ApplicationPolicy, generations *raftlog.ApplicationGenerationLimits) error {
	if s == nil {
		return nil
	}
	if err := s.validate(); err != nil {
		return err
	}
	if s.homeCount() > idalloc.MaxBlockSize {
		return fmt.Errorf("single-grant cost profile needs %d IDs, allocator MaxBlockSize=%d: %w", s.homeCount(), idalloc.MaxBlockSize, errLimit)
	}
	init.maxBlock = s.homeCount()
	// Logical retained quotas are explicit per dataset, never resident/RSS limits.
	// Every old root, graph version, original outcome, CDC and index remains.
	retainedBytes, retainedRecords := uint64(64<<20)+s.Facts*(64<<10), uint64(65536)+s.Facts*512
	l.MaxRetainedBytes, l.MaxRetainedEntries = retainedBytes, 1_000_000
	p.RetainedApplicationBytes, p.RetainedApplicationRecords = retainedBytes, retainedRecords
	*generations = raftlog.ApplicationGenerationLimits{MaxBytes: 2 * retainedBytes, MaxRecords: 2 * retainedRecords}
	return nil
}
func (o *certifiedProcessOwners) grantParameters(data []byte) (uint64, uint64, error) {
	if o.cost == nil {
		return graphE17GrantParameters(o.host, data)
	}
	if len(data) != 16 {
		return 0, 0, errInvalid
	}
	sequence, count := binary.BigEndian.Uint64(data[:8]), binary.BigEndian.Uint64(data[8:])
	if sequence != 1 || count == 0 || count > o.cost.homeCount() {
		return 0, 0, errInvalid
	}
	return sequence, count, nil
}

// Inputs select declared typed operations; the actual Host authenticates grants,
// checks dependencies and creates the original proposal. This builds no outcome.
// index1/2/3: three anchors; index4/5 with data=i: edge create/property update.
func nativeGraphCostProposal(o *certifiedProcessOwners, r processRequest, id requestID) (allocationProposal, error) {
	if o == nil || o.cost == nil || o.recipient == nil || r.index < 1 || r.index > 5 {
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
	provenance := r.index
	if r.index <= 3 {
		if len(r.data) != 0 {
			return allocationProposal{}, errInvalid
		}
		owner, life := uint64(1), uint64(2)
		if r.index == 2 {
			owner, life = 3, 4
		}
		if r.index == 3 {
			owner, life = o.cost.remoteNode(), o.cost.remoteNode()+1
		}
		ops = []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: graphstate.EntityID(owner), Life: graphstate.LifeID(life), Scope: scope, Record: graphstate.EntityRecord{Interpretation: graphstate.InterpretationObservation, TemporalRole: graphstate.TemporalRoleValidity}}}
		claims = []freshBinding{{role: entityBinding, id: owner, grant: grant}, {role: lifeBinding, owner: graphstate.EntityID(owner), id: life, grant: grant}}
	} else {
		if len(r.data) != 8 {
			return allocationProposal{}, errInvalid
		}
		i := binary.BigEndian.Uint64(r.data)
		if i >= o.cost.Facts {
			return allocationProposal{}, errInvalid
		}
		edge, life, target, targetLife := o.cost.edge(i)
		value, valueID := int64(5), uint64(5)
		provenance = 4 + i
		if r.index == 4 {
			ops = append(ops, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: graphstate.EntityID(edge), Life: graphstate.LifeID(life), Scope: scope, Record: graphstate.EntityRecord{Type: "edge", Source: 1, Target: graphstate.EntityID(target), Mode: graphstate.LifeBound, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleOccurrenceTime}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: graphstate.LifeID(targetLife)}})
			claims = append(claims, freshBinding{role: entityBinding, id: edge, grant: grant}, freshBinding{role: lifeBinding, owner: graphstate.EntityID(edge), id: life, grant: grant})
		} else {
			value, valueID = 6, 6
			provenance = 4 + o.cost.Facts + i
		}
		ops = append(ops, graphstate.Operation{Kind: graphstate.Set, Owner: graphstate.EntityID(edge), Life: graphstate.LifeID(life), Scope: scope, Name: "p", Value: graphstate.I64(value), ValueID: graphstate.ValueID(valueID)})
		if i == 0 {
			claims = append(claims, freshBinding{role: valueBinding, id: valueID, grant: grant})
		}
	}
	return o.host.PreparePartitionGraphOperations(id, ops, provenance, claims)
}

// Exactly one entity's retained presence/property/life is observed from the real
// child-owned certified view. High-degree endpoint scans use separate paging;
// this never widens the old32-ID lifecycle census or resets its source ledger.
func nativeGraphCostObserve(ctx context.Context, s *graphProcessCertifiedSessions, r processRequest) ([]byte, error) {
	if ctx == nil || s == nil || len(r.data) != 0 || r.entity == 0 {
		return nil, errInvalid
	}
	s.mu.Lock()
	owner, ok := s.owners[r.index]
	s.mu.Unlock()
	if !ok {
		return nil, errInvalid
	}
	view, err := owner.read.ReadView()
	if err != nil {
		return nil, err
	}
	s.coordinator.host.mu.Lock()
	cfg, err := s.coordinator.host.currentConfiguration()
	s.coordinator.host.mu.Unlock()
	if err != nil {
		return nil, err
	}
	entity, err := view.Entity(ctx, graphstate.EntityID(r.entity))
	if err != nil {
		return nil, err
	}
	out := nativeLifecycleEntity{ID: r.entity, Found: entity.Found, Labels: []string{}}
	if entity.Found {
		point, err := temporal.RationalPosition(cfg.defaultAxis.Axis(), temporal.RationalInt64(0))
		if err != nil {
			return nil, err
		}
		scope, err := temporal.All(cfg.defaultAxis.Axis())
		if err != nil {
			return nil, err
		}
		l := s.coordinator.host.machine.limits.graph.Planner
		out.Kind, out.Type, out.Mode, out.Source, out.Target = byte(entity.Record.Kind), entity.Record.Type, byte(entity.Record.Mode), uint64(entity.Record.Source), uint64(entity.Record.Target)
		out.Presence, err = nativeLifecycleCellRead(ctx, view, graphstate.ComponentKey{Owner: graphstate.EntityID(r.entity), Kind: graphstate.Presence}, scope, point, l.Component)
		if err != nil {
			return nil, err
		}
		projection, err := graphstate.Project(ctx, view, graphstate.EntityID(r.entity), point, graphstate.Effective, l)
		if err != nil {
			return nil, err
		}
		out.Effective, out.EffectiveLife, out.Labels = projection.Active, uint64(projection.Life), projection.Labels
		if entity.Record.Kind == graphstate.Relationship {
			out.Property, err = nativeLifecycleCellRead(ctx, view, graphstate.ComponentKey{Owner: graphstate.EntityID(r.entity), Life: projection.Life, Kind: graphstate.ScalarProperty, Name: "p"}, scope, point, l.Component)
			if err != nil {
				return nil, err
			}
		}
	}
	return json.Marshal(out)
}

func nativeGraphCostRecordWire(ctx context.Context, h *allocationHost, index uint64) ([]byte, error) {
	out, err := nativeGraphRecordRead(ctx, h, index, true)
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
