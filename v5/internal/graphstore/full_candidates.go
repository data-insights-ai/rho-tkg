package graphstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func (q *pageReader) queryScope(scope temporal.Scope) ([]byte, error) {
	limit := min(q.q.c.limits.MaxReadBytes, q.limits.MaxWorkBytes)
	if q.q.maxBytes > 0 {
		limit = min(limit, q.q.maxBytes)
	}
	available := limit - q.q.bytes
	if available < 256 {
		return nil, ErrResourceLimit
	}
	l := q.q.c.limits.Temporal
	l.MaxValueBytes = min(l.MaxValueBytes, (available-128)/2)
	wire, err := temporal.AppendScope(nil, scope, l)
	if err != nil {
		return nil, callerError(err)
	}
	if err := q.q.materialize(128 + 2*cap(wire)); err != nil {
		return nil, err
	}
	return wire, nil
}
func claimPageAppend(q *pageReader, out *graphstate.ClaimPage, key graphstate.ComponentKey, output *int, limit, maxRows int) error {
	capacity := cap(out.Claims)
	if len(out.Claims) == capacity {
		capacity = min(max(1, 2*capacity), maxRows, max(0, (limit-*output-len(key.Name))/96))
	}
	cost := 96*(capacity-cap(out.Claims)) + len(key.Name)
	if capacity < len(out.Claims)+1 || cost > limit-*output {
		return ErrResourceLimit
	}
	if err := q.q.materialize(cost); err != nil {
		return err
	}
	if capacity > cap(out.Claims) {
		claims := make([]graphstate.UniqueClaim, len(out.Claims), capacity)
		copy(claims, out.Claims)
		out.Claims = claims
	}
	key.Name = strings.Clone(key.Name)
	out.Claims = append(out.Claims, graphstate.UniqueClaim{Owner: key.Owner, Life: key.Life, Key: key})
	*output += cost
	return nil
}
func entityPageAppend(q *pageReader, out *graphstate.EntityPage, id graphstate.EntityID, output *int, limit, maxRows int) error {
	capacity := cap(out.Entities)
	if len(out.Entities) == capacity {
		capacity = min(max(1, 2*capacity), maxRows, max(0, (limit-*output)/8))
	}
	cost := 8 * (capacity - cap(out.Entities))
	if capacity < len(out.Entities)+1 || cost > limit-*output {
		return ErrResourceLimit
	}
	if err := q.q.materialize(cost); err != nil {
		return err
	}
	if capacity > cap(out.Entities) {
		entries := make([]graphstate.EntityID, len(out.Entities), capacity)
		copy(entries, out.Entities)
		out.Entities = entries
	}
	out.Entities = append(out.Entities, id)
	*output += cost
	return nil
}

// UniqueCandidates requires the exact registered declared-unique definition.
// Its retained raw superset includes closed/retracted/masked historical claims;
// Plan rechecks final effective support. Canonical ID normalization uses exact
// typed dictionary equality, including aliases and hash collision candidates.
func (v *ReadView) UniqueCandidates(ctx context.Context, predicate graphstate.UniquePredicate, token graphstate.Cursor, budget graphstate.ReadBudget) (graphstate.ClaimPage, error) {
	if v == nil {
		return graphstate.ClaimPage{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if budget.Rows < 1 || budget.Bytes < 1 || !validProperty(predicate.Definition, v.c.limits) || predicate.Definition.Unique == graphstate.UniqueNone || predicate.Value.Kind() != predicate.Definition.Type && predicate.Value.Kind() != graphstate.ScalarNull || predicate.Window.Kind() == temporal.ScopeInvalid || predicate.Window.Kind() == temporal.ScopeUnplaced {
		return graphstate.ClaimPage{}, ErrInvalid
	}
	q, err := v.begin(ctx, budget)
	if err != nil {
		return graphstate.ClaimPage{}, err
	}
	if err := q.q.materialize(256 + 2*v.c.limits.MaxNameBytes); err != nil {
		return graphstate.ClaimPage{}, v.finish(q, err)
	}
	definition, found, err := q.q.property(predicate.Definition.Owner, predicate.Definition.Name)
	if err == nil && (!found || definition != predicate.Definition) {
		err = callerError(graphstate.ErrSchemaMismatch)
	}
	if err != nil {
		return graphstate.ClaimPage{}, v.finish(q, err)
	}
	equality, err := q.equalityKey(predicate.Value)
	if err != nil {
		return graphstate.ClaimPage{}, v.finish(q, err)
	}
	window, err := q.queryScope(predicate.Window)
	if err != nil {
		return graphstate.ClaimPage{}, v.finish(q, err)
	}
	// Preflight fingerprint copies before allocating an equality/window buffer.
	if err := q.q.materialize(256 + 2*len(equality) + 2*len(definition.Name)); err != nil {
		return graphstate.ClaimPage{}, v.finish(q, err)
	}
	data := append([]byte("full:unique:v1\x00"), v.id[:]...)
	schema, err := encodeProperty(v.c.root.namespace, definition, v.c.limits)
	if err != nil {
		return graphstate.ClaimPage{}, v.finish(q, err)
	}
	data = appendField(data, schema)
	data = appendField(data, []byte(equality))
	data = appendField(data, window)
	hash := sha256.Sum256(data)
	old, err := v.oldCursor(token, uniquePostingRecord, hash)
	if err != nil {
		return graphstate.ClaimPage{}, v.finish(q, err)
	}
	canonical, found, err := q.q.local(equality)
	if err != nil {
		return graphstate.ClaimPage{}, v.finish(q, err)
	}
	out := graphstate.ClaimPage{View: v.id, Version: v.version(), Complete: true}
	output := 128
	if !found || predicate.Value.Kind() == graphstate.ScalarNull || predicate.Window.Kind() == temporal.ScopeEmpty {
		if err := v.finish(q, nil); err != nil {
			return graphstate.ClaimPage{}, err
		}
		if err := v.output(output); err != nil {
			return graphstate.ClaimPage{}, err
		}
		v.advanceCursor(token, 0, old, fullContinuation{})
		return out, nil
	}
	prefix := postingKey{family: uniquePostingRecord, owner: definition.Owner, name: definition.Name, scalar: definition.Type, value: canonical.Ref.ID}
	after := prefix
	inclusive := true
	if token != 0 {
		after = old.posting
		inclusive = false
	}
	it, err := newPostingIterator(q, v.descriptor.unique, prefix, after, inclusive)
	if err != nil {
		return graphstate.ClaimPage{}, v.finish(q, err)
	}
	last := postingKey{}
	visited := 0
	for {
		key, more, e := it.next(0)
		if e != nil {
			err = e
			break
		}
		if !more {
			out.Complete = true
			break
		}
		if e := q.validateRawCandidate(key, definition, canonical.Ref.ID); e != nil {
			err = e
			break
		}
		if e := claimPageAppend(q, &out, key.component, &output, min(budget.Bytes, v.limits.MaxOutputBytes-v.outputBytes), budget.Rows); e != nil {
			err = e
			break
		}
		last = key
		visited++
	}
	if err != nil {
		if !errors.Is(err, ErrResourceLimit) || visited == 0 {
			return graphstate.ClaimPage{}, v.finish(q, err)
		}
		out.Complete = false
	}
	next := fullContinuation{}
	if !out.Complete {
		next = fullContinuation{kind: uniquePostingRecord, query: hash, posting: last, bytes: 256 + len(last.name) + len(last.component.Name)}

		out.Next, err = v.storeCursor(token, old, next)
		if err != nil {
			return graphstate.ClaimPage{}, v.finish(q, err)
		}
	}
	if err := v.finish(q, nil); err != nil {
		return graphstate.ClaimPage{}, err
	}
	if err := v.output(output); err != nil {
		return graphstate.ClaimPage{}, err
	}
	v.advanceCursor(token, out.Next, old, next)
	return out, nil
}

// IncidentRelationships returns the complete declared candidate superset for
// one endpoint/life, including ended relationships and immutable life bindings.
// It merges declared LifeBound and canonical IdentityReference postings. Sorted
// relationship IDs preserve parallel edges; one exclusive-ID continuation skips
// all self-loop roles and repeated relationship-life entries across pages.
func (v *ReadView) IncidentRelationships(ctx context.Context, predicate graphstate.IncidentPredicate, token graphstate.Cursor, budget graphstate.ReadBudget) (graphstate.EntityPage, error) {
	if v == nil {
		return graphstate.EntityPage{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if predicate.Endpoint == 0 || budget.Rows < 1 || budget.Bytes < 1 || predicate.Window.Kind() == temporal.ScopeInvalid || predicate.Window.Kind() == temporal.ScopeUnplaced {
		return graphstate.EntityPage{}, ErrInvalid
	}
	q, err := v.begin(ctx, budget)
	if err != nil {
		return graphstate.EntityPage{}, err
	}
	if err := q.q.materialize(256); err != nil {
		return graphstate.EntityPage{}, v.finish(q, err)
	}
	window, err := q.queryScope(predicate.Window)
	if err != nil {
		return graphstate.EntityPage{}, v.finish(q, err)
	}
	if err := q.q.materialize(256); err != nil {
		return graphstate.EntityPage{}, v.finish(q, err)
	}
	data := append([]byte("full:incident:v1\x00"), v.id[:]...)
	data = appendComponent(data, graphstate.ComponentKey{Owner: predicate.Endpoint, Life: predicate.Life})
	data = appendField(data, window)
	hash := sha256.Sum256(data)
	old, err := v.oldCursor(token, canonicalIncidentRecord, hash)
	if err != nil {
		return graphstate.EntityPage{}, v.finish(q, err)
	}
	out := graphstate.EntityPage{View: v.id, Version: v.version(), Complete: true}
	output := 128
	if predicate.Window.Kind() == temporal.ScopeEmpty {
		if err := v.finish(q, nil); err != nil {
			return graphstate.EntityPage{}, err
		}
		if err := v.output(output); err != nil {
			return graphstate.EntityPage{}, err
		}
		v.advanceCursor(token, 0, old, fullContinuation{})
		return out, nil
	}
	// A zero Life requests both canonical modes. A qualified Life requests its
	// declared LifeBound range plus all identity references, which may use another
	// axis and never depend on endpoint life/visibility.
	roots := [2]postingTreeRoot{v.descriptor.canonical, v.descriptor.canonical}
	prefixes := [2]postingKey{{family: canonicalIncidentRecord, endpoint: predicate.Endpoint, mode: graphstate.LifeBound}, {family: canonicalIncidentRecord, endpoint: predicate.Endpoint, mode: graphstate.IdentityReference}}
	if predicate.Life != 0 {
		roots[0] = v.descriptor.declared
		prefixes[0] = postingKey{family: declaredIncidentRecord, endpoint: predicate.Endpoint, bound: predicate.Life}
	}
	var iterators [2]*postingIterator
	var heads [2]postingKey
	var have [2]bool
	for i := range iterators {
		it, e := newPostingIterator(q, roots[i], prefixes[i], prefixes[i], true)
		if e != nil {
			return graphstate.EntityPage{}, v.finish(q, e)
		}
		iterators[i] = it
		heads[i], have[i], err = it.next(uint64(old.relationship))
		if err != nil {
			return graphstate.EntityPage{}, v.finish(q, err)
		}
	}
	last := old.relationship
	visited := 0
	for have[0] || have[1] {
		i := 0
		if !have[0] || have[1] && heads[1].relationship < heads[0].relationship {
			i = 1
		}
		key := heads[i]
		if err = q.validateIncident(key); err != nil {
			break
		}
		if err = entityPageAppend(q, &out, key.relationship, &output, min(budget.Bytes, v.limits.MaxOutputBytes-v.outputBytes), budget.Rows); err != nil {
			break
		}
		last = key.relationship
		visited++
		// Both source ranges are advanced beyond the WHOLE ID run, not only the
		// selected life/role row. No transaction-local lifetime-sized dedup map.
		for j := range iterators {
			if have[j] && heads[j].relationship <= last {
				heads[j], have[j], err = iterators[j].next(uint64(last))
				if err != nil {
					break
				}
			}
		}
		if err != nil {
			break
		}
	}
	if err != nil {
		if !errors.Is(err, ErrResourceLimit) || visited == 0 {
			return graphstate.EntityPage{}, v.finish(q, err)
		}
		out.Complete = false
	} else {
		out.Complete = !have[0] && !have[1]
	}
	next := fullContinuation{}
	if !out.Complete {
		next = fullContinuation{kind: canonicalIncidentRecord, query: hash, relationship: last, bytes: 256}

		out.Next, err = v.storeCursor(token, old, next)
		if err != nil {
			return graphstate.EntityPage{}, v.finish(q, err)
		}
	}
	if err := v.finish(q, nil); err != nil {
		return graphstate.EntityPage{}, err
	}
	if err := v.output(output); err != nil {
		return graphstate.EntityPage{}, err
	}
	v.advanceCursor(token, out.Next, old, next)
	return out, nil
}
