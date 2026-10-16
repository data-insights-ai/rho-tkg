package graphapply

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Only child-local Host factories create commands. This IPC carries fixed
// fixture parameters, never planned effects or certificate authority.
func graphE18Proposal(towner *certifiedProcessOwners, request processRequest, id requestID) (allocationProposal, error) {
	if towner == nil || towner.recipient == nil {
		return allocationProposal{}, errInvalid
	}
	machine := towner.host.machine
	cfg, err := towner.host.currentConfiguration()
	if err != nil {
		return allocationProposal{}, err
	}
	window, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return allocationProposal{}, err
	}
	grant := grantReference{session: towner.recipient.session, sequence: 1}
	var ops []graphstate.Operation
	var claims []freshBinding
	switch request.entity {
	case certifiedE18Property:
		if len(request.data) != 16 || request.index != 1 && request.index != 17 && request.index != 19 {
			return allocationProposal{}, errInvalid
		}
		ref, value := binary.BigEndian.Uint64(request.data[:8]), binary.BigEndian.Uint64(request.data[8:])
		if value > 100 || ref == 0 {
			return allocationProposal{}, errInvalid
		}
		label := "old-label"
		if request.index != 1 {
			label = "remote-control"
		}
		ops = []graphstate.Operation{{Kind: graphstate.Set, Owner: graphstate.EntityID(request.index), Life: graphstate.LifeID(request.index + 1), Name: "p", Value: graphstate.I64(int64(value)), ValueID: graphstate.ValueID(ref), Scope: window}, {Kind: graphstate.AddLabel, Owner: graphstate.EntityID(request.index), Life: graphstate.LifeID(request.index + 1), Name: label, Scope: window}}
		claims = []freshBinding{{role: valueBinding, id: ref, grant: grant}}
	case certifiedE18Correction:
		if machine.ns.partition != 3 || len(request.data) != 0 {
			return allocationProposal{}, errInvalid
		}
		ops = []graphstate.Operation{{Kind: graphstate.Set, Owner: 1, Life: 2, Name: "p", Value: graphstate.I64(10), ValueID: 6, Scope: window}, {Kind: graphstate.RemoveLabel, Owner: 1, Life: 2, Name: "old-label", Scope: window}, {Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "new-label", Scope: window}, {Kind: graphstate.Correct, Owner: 17, Life: 18, Scope: window, Present: false}}
		claims = []freshBinding{{role: valueBinding, id: 6, grant: grant}}
	default:
		return allocationProposal{}, errInvalid
	}
	return towner.host.PreparePartitionGraphOperations(id, ops, 31, claims)
}

type graphE18Row struct {
	owner, life, effectiveLife             uint64
	source, target, sourceLife, targetLife uint64
	active                                 bool
	labels                                 []string
	ref, revision, provenance              uint64
	value                                  int64
	component                              []byte
}

// A selector has no supplied candidate IDs: tag1 starts at the declared unique
// index, tag2 at endpoint postings (canonical Life0 or declared Life-qualified).
func graphE18IndexInput(kind byte, value uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{kind}, value)
}
func graphE18IndexObserve(ctx context.Context, sessions *graphProcessCertifiedSessions, request processRequest) (out []byte, err error) {
	if ctx == nil || sessions == nil || len(request.data) != 9 || request.data[0] < 1 || request.data[0] > 2 {
		return nil, errInvalid
	}
	sessions.mu.Lock()
	owner, found := sessions.owners[request.index]
	sessions.mu.Unlock()
	if !found {
		return nil, errInvalid
	}
	defer func() {
		if err != nil {
			for _, source := range owner.read.sources {
				if remote, ok := source.(*remoteGraphSource); ok {
					b := remote.endpoint.budget
					b.mu.Lock()
					calls, rows, size := b.calls, b.rows, b.bytes
					remaining := b.output.Remaining()
					b.mu.Unlock()
					err = fmt.Errorf("%w (operation calls=%d/%d rows=%d/%d bytes=%d/%d outputRemaining=%d)", err, calls, b.maxCalls, rows, b.maxRows, size, b.maxBytes, remaining)
					break
				}
			}
		}
	}()
	view, err := owner.read.ReadView()
	if err != nil {
		return nil, err
	}
	sessions.coordinator.host.mu.Lock()
	cfg, err := sessions.coordinator.host.currentConfiguration()
	sessions.coordinator.host.mu.Unlock()
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
	budget := raftlog.ReadBudget{Rows: 64, Bytes: 64 << 10}
	var claims []graphstate.UniqueClaim
	if request.data[0] == 1 {
		if request.entity != 0 || binary.BigEndian.Uint64(request.data[1:]) > 1000 {
			return nil, errInvalid
		}
		page, e := view.UniqueCandidates(ctx, graphstate.UniquePredicate{Definition: partitionWriteSchemas()[1], Value: graphstate.I64(int64(binary.BigEndian.Uint64(request.data[1:]))), Window: window}, 0, budget)
		if e != nil {
			return nil, e
		}
		if !page.Complete || page.Next != 0 || len(page.Claims) > 16 {
			return nil, errLimit
		}
		claims = page.Claims
	} else {
		if request.entity != 1 && request.entity != 17 {
			return nil, errInvalid
		}
		page, e := view.IncidentRelationships(ctx, graphstate.IncidentPredicate{Endpoint: graphstate.EntityID(request.entity), Life: graphstate.LifeID(binary.BigEndian.Uint64(request.data[1:])), Window: window}, 0, budget)
		if e != nil {
			return nil, e
		}
		if !page.Complete || page.Next != 0 || len(page.Entities) > 16 {
			return nil, errLimit
		}
		for _, id := range page.Entities {
			claims = append(claims, graphstate.UniqueClaim{Owner: id})
		}
	}
	rows := make([]graphE18Row, len(claims))
	for i, claim := range claims {
		row := &rows[i]
		row.owner = uint64(claim.Owner)
		row.life = uint64(claim.Life)
		entity, e := view.Entity(ctx, claim.Owner)
		if e != nil {
			return nil, e
		}
		if !entity.Found {
			return nil, errCorrupt
		}
		row.source, row.target = uint64(entity.Record.Source), uint64(entity.Record.Target)
		projection, e := graphstate.Project(ctx, view, claim.Owner, point, graphstate.Effective, sessions.coordinator.host.machine.limits.graph.Planner)
		if e != nil {
			return nil, e
		}
		row.active, row.effectiveLife, row.labels = projection.Active, uint64(projection.Life), projection.Labels
		key := claim.Key
		if request.data[0] == 2 {
			key = graphstate.ComponentKey{Owner: claim.Owner, Kind: graphstate.Presence}
		}
		page, e := view.ComponentPage(ctx, graphstate.ComponentQuery{Key: key, Window: window}, 0, budget)
		if e != nil {
			return nil, e
		}
		if !page.Complete || page.Next != 0 {
			return nil, errLimit
		}
		row.component, e = state.AppendState(nil, page.Data, state.CodecLimits{State: sessions.coordinator.host.machine.limits.graph.Planner.Component, MaxEncodedBytes: 64 << 10})
		if e != nil {
			return nil, e
		}
		cell, e := page.Data.At(point, sessions.coordinator.host.machine.limits.graph.Planner.Component)
		if e != nil {
			return nil, e
		}
		if cell.Present() {
			row.ref, row.revision, row.provenance = cell.Value().ID(), cell.Revision().ID(), cell.Revision().Provenance()
		}
		if request.data[0] == 1 {
			if !cell.Present() {
				return nil, errCorrupt
			}
			value, e := view.Value(ctx, graphstate.ValueID(row.ref))
			if e != nil {
				return nil, e
			}
			if !value.Found {
				return nil, errCorrupt
			}
			var ok bool
			row.value, ok = value.Value.Int64Value()
			if !ok {
				return nil, errCorrupt
			}
		} else {
			row.life = row.ref
		}
		life, e := view.Life(ctx, claim.Owner, graphstate.LifeID(row.life))
		if e != nil {
			return nil, e
		}
		if !life.Found {
			return nil, errCorrupt
		}
		row.sourceLife, row.targetLife = uint64(life.Record.SourceLife), uint64(life.Record.TargetLife)
	}
	return encodeGraphE18Rows(rows)
}
func encodeGraphE18Rows(rows []graphE18Row) ([]byte, error) {
	if len(rows) > 16 {
		return nil, errLimit
	}
	for _, row := range rows {
		if len(row.labels) > 16 || len(row.component) > 64<<10 {
			return nil, errLimit
		}
	}
	return processEncoding(func(w *boundedWriter) {
		w.u32(len(rows))
		for _, r := range rows {
			for _, v := range []uint64{r.owner, r.life, r.effectiveLife, r.source, r.target, r.sourceLife, r.targetLife, r.ref, r.revision, r.provenance} {
				w.u64(v)
			}
			if r.active {
				w.tag(1)
			} else {
				w.tag(0)
			}
			w.u64(uint64(r.value))
			w.u32(len(r.labels))
			for _, label := range r.labels {
				w.text(label)
			}
			w.field(r.component)
		}
	})
}
func decodeGraphE18Rows(wire []byte) ([]graphE18Row, error) {
	c := graphCursor{b: wire}
	n := c.count(16, 97)
	if c.err != nil {
		return nil, c.err
	}
	rows := make([]graphE18Row, n)
	for i := range rows {
		r := &rows[i]
		r.owner, r.life, r.effectiveLife = c.u64(), c.u64(), c.u64()
		r.source, r.target, r.sourceLife, r.targetLife = c.u64(), c.u64(), c.u64(), c.u64()
		r.ref, r.revision, r.provenance = c.u64(), c.u64(), c.u64()
		flag := c.tag()
		if flag > 1 {
			c.err = errCorrupt
		}
		r.active = flag == 1
		r.value = int64(c.u64())
		n := c.count(16, 4)
		if c.err != nil {
			return nil, c.err
		}
		r.labels = make([]string, n)
		for j := range n {
			r.labels[j] = string(c.field(128))
		}
		r.component = bytes.Clone(c.field(64 << 10))
	}
	if c.err != nil || len(c.b) != 0 {
		return nil, errors.Join(errCorrupt, c.err)
	}
	return rows, nil
}
