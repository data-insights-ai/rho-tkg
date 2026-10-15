package graphapply

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

const nativeEdgeObserve = "GEI1"

type nativeGraphEdgeUniqueObservedEntity struct {
	Entity nativeLifecycleEntity
	Life   nativeLifecycleLife
	Scalar nativeLifecycleCell
}
type nativeGraphEdgeUniqueObservation struct {
	Fact                         uint64
	Entities                     [2]nativeGraphEdgeUniqueObservedEntity
	Source, OldTarget, NewTarget nativeLifecycleLife
	Claims                       []graphstate.UniqueClaim
	Complete                     bool
	ValueFound                   bool
	ValueID                      uint64
	PhantomFound                 bool
}

// Every entity, exact old/new life, unique candidate and scalar is read through
// ONE genuine certified owner. Candidate presence is descriptive; effective
// projection at that same cut supplies actual endpoint/life support.
func nativeGraphEdgeUniqueObserve(ctx context.Context, sessions *graphProcessCertifiedSessions, spec nativeGraphCostSpec, r processRequest) ([]byte, error) {
	if ctx == nil || sessions == nil || nativeGraphEdgeUniqueValidate(spec) != nil || len(r.data) != 12 || !bytes.Equal(r.data[:4], []byte(nativeEdgeObserve)) || r.entity != 0 {
		return nil, errInvalid
	}
	fact := binary.BigEndian.Uint64(r.data[4:])
	if fact >= spec.Facts {
		return nil, errInvalid
	}
	sessions.mu.Lock()
	owner, found := sessions.owners[r.index]
	sessions.mu.Unlock()
	if !found {
		return nil, errInvalid
	}
	view, err := owner.read.ReadView()
	if err != nil {
		return nil, err
	}
	h := sessions.coordinator.host
	if h == nil || h.machine.SemanticContractID() != durableColumnGraphSemanticContractID() {
		return nil, errInvalid
	}
	h.mu.Lock()
	cfg, err := h.currentConfiguration()
	h.mu.Unlock()
	if err != nil {
		return nil, err
	}
	window, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return nil, err
	}
	point, err := temporal.RationalPosition(cfg.defaultAxis.Axis(), temporal.RationalInt64(0))
	if err != nil {
		return nil, err
	}
	out := nativeGraphEdgeUniqueObservation{Fact: fact, Claims: []graphstate.UniqueClaim{}}
	readLife := func(entity, life uint64) (nativeLifecycleLife, error) {
		actual, e := view.Life(ctx, graphstate.EntityID(entity), graphstate.LifeID(life))
		return nativeLifecycleLife{Owner: entity, Life: life, Found: actual.Found, SourceLife: uint64(actual.Record.SourceLife), TargetLife: uint64(actual.Record.TargetLife)}, e
	}
	out.Source, err = readLife(1, 2)
	if err != nil {
		return nil, err
	}
	target, oldLife, newLife := uint64(3), uint64(4), 4*spec.Facts+7
	if spec.cross(fact) {
		target, oldLife, newLife = spec.remoteNode(), spec.remoteNode()+1, spec.remoteNode()+2
	}
	out.OldTarget, err = readLife(target, oldLife)
	if err != nil {
		return nil, err
	}
	out.NewTarget, err = readLife(target, newLife)
	if err != nil {
		return nil, err
	}
	for candidate := range 2 {
		id := 7 + 4*fact + 2*uint64(candidate)
		x := &out.Entities[candidate]
		x.Entity.ID = id
		x.Entity.Labels = []string{}
		entity, e := view.Entity(ctx, graphstate.EntityID(id))
		if e != nil {
			return nil, e
		}
		x.Entity.Found = entity.Found
		if !entity.Found {
			continue
		}
		x.Entity.Kind, x.Entity.Type, x.Entity.Mode = byte(entity.Record.Kind), entity.Record.Type, byte(entity.Record.Mode)
		x.Entity.Source, x.Entity.Target = uint64(entity.Record.Source), uint64(entity.Record.Target)
		x.Entity.Presence, err = nativeLifecycleCellRead(ctx, view, graphstate.ComponentKey{Owner: graphstate.EntityID(id), Kind: graphstate.Presence}, window, point, h.machine.limits.graph.Planner.Component)
		if err != nil {
			return nil, err
		}
		projection, e := graphstate.Project(ctx, view, graphstate.EntityID(id), point, graphstate.Effective, h.machine.limits.graph.Planner)
		if e != nil {
			return nil, e
		}
		x.Entity.Effective, x.Entity.EffectiveLife = projection.Active, uint64(projection.Life)
		x.Life, err = readLife(id, id+1)
		if err != nil {
			return nil, err
		}
		x.Scalar, err = nativeLifecycleCellRead(ctx, view, graphstate.ComponentKey{Owner: graphstate.EntityID(id), Life: graphstate.LifeID(id + 1), Kind: graphstate.ScalarProperty, Name: "p"}, window, point, h.machine.limits.graph.Planner.Component)
		if err != nil {
			return nil, err
		}
	}
	definition, err := view.Property(ctx, graphstate.Relationship, "p")
	if err != nil || !definition.Found || definition.Record.Unique != graphstate.UniqueScalar {
		return nil, errors.Join(errCorrupt, err)
	}
	page, err := view.UniqueCandidates(ctx, graphstate.UniquePredicate{Definition: definition.Record, Value: graphstate.I64(int64(fact + 5)), Window: window}, 0, graphstate.ReadBudget{Rows: 4, Bytes: 16 << 10}) // #nosec G115 -- admitted fact<1M.
	if err != nil || page.View != view.Identity() || !page.Complete || page.Next != 0 || len(page.Claims) > 2 {
		return nil, errors.Join(errCorrupt, err)
	}
	out.Claims = append(out.Claims, page.Claims...)
	out.Complete = true
	value, err := view.ValueIdentity(ctx, graphstate.I64(int64(fact+5)))
	if err != nil {
		return nil, err
	} // #nosec G115 -- admitted fact<1M.
	out.ValueFound, out.ValueID = value.Found, uint64(value.ID)
	phantom, err := view.ValueIdentity(ctx, graphstate.I64(int64(spec.Facts+47)))
	if err != nil {
		return nil, err
	} // #nosec G115 -- Facts<=1M.
	out.PhantomFound = phantom.Found
	body, err := json.Marshal(out)
	if len(body) > 32<<10 {
		return nil, errLimit
	}
	return body, err
}
