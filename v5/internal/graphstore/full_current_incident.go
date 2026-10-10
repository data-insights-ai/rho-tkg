package graphstore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"slices"
)

// IncidentDirection selects endpoint roles, independently of lifecycle visibility.
type IncidentDirection uint8

// Incident directions preserve both roles of self-loops without duplicate lives.
const (
	IncidentSource IncidentDirection = 1 << iota
	IncidentTarget
	IncidentBoth = IncidentSource | IncidentTarget
)

// IncidentAtQuery selects exact presence at an explicit axis/position in this
// immutable view. Life restricts LifeBound only. Type is an exact residual.
type IncidentAtQuery struct {
	Endpoint  graphstate.EntityID
	Life      graphstate.LifeID
	At        temporal.Position
	Mode      graphstate.ReferenceMode
	Direction IncidentDirection
	Type      string
	Visible   graphstate.Visibility
}

// IncidentAtCandidate identifies one relationship life and its matching roles.
type IncidentAtCandidate struct {
	Relationship graphstate.EntityID
	Life         graphstate.LifeID
	Roles        IncidentDirection
}

// IncidentAtPage owns exact candidates in native index order, not entity-ID
// order. Empty advancing pages are valid; only Complete finishes the query.
type IncidentAtPage struct {
	View       graphstate.ViewID
	Version    graphstate.ReadVersion
	Candidates []IncidentAtCandidate
	Next       graphstate.Cursor
	Complete   bool
}

func nativeIncidentDirection(direction IncidentDirection) byte {
	var out byte
	if direction&IncidentSource != 0 {
		out |= cpSource
	}
	if direction&IncidentTarget != 0 {
		out |= cpTarget
	}
	return out
}
func incidentRoles(roles byte) IncidentDirection {
	var out IncidentDirection
	if roles&cpSource != 0 {
		out |= IncidentSource
	}
	if roles&cpTarget != 0 {
		out |= IncidentTarget
	}
	return out
}

type fullCurrentCursor struct {
	iterator     *currentPresenceIterator
	endpointLife graphstate.LifeID
}
type currentIteratorCheckpoint struct {
	frames        []cpIteratorFrame
	cache         map[uint64]currentPresencePage
	last          currentPresenceAtom
	cursor        graphstate.Cursor
	started, done bool
	output        int
}

func checkpointCurrent(it *currentPresenceIterator) currentIteratorCheckpoint {
	return currentIteratorCheckpoint{it.frames, it.op.cache, it.last, it.cursor, it.started, it.done, it.outputBytes}
}
func (s currentIteratorCheckpoint) restore(it *currentPresenceIterator) {
	it.frames, it.op.cache, it.last, it.cursor, it.started, it.done, it.outputBytes = s.frames, s.cache, s.last, s.cursor, s.started, s.done, s.output
	it.op.witness = currentPresenceWitness{}
}
func incidentAtHash(v *ReadView, q *pageReader, query IncidentAtQuery) ([32]byte, error) {
	// At most three integer magnitudes are encoded (Q×N). Reserve validation
	// scratch and the complete hash buffer before AppendPosition can allocate.
	l := v.c.limits.Temporal
	coordinateBytes, scratch := cpPositionBudget(query.At, l)
	prefixBytes := len("rho-tkg:incident-at:v1\x00") + len(v.id) + 16 + 3 + 4 + len(query.Type)
	if err := q.q.materialize(prefixBytes + coordinateBytes + scratch); err != nil {
		return [32]byte{}, err
	}
	b := make([]byte, 0, prefixBytes+coordinateBytes)
	b = append(b, "rho-tkg:incident-at:v1\x00"...)
	b = append(b, v.id[:]...)
	b = binary.BigEndian.AppendUint64(b, uint64(query.Endpoint))
	b = binary.BigEndian.AppendUint64(b, uint64(query.Life))
	b = append(b, byte(query.Mode), byte(query.Direction), byte(query.Visible))
	b = appendField(b, []byte(query.Type))
	b, err := temporal.AppendPosition(b, query.At, l)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(b), nil
}

func (q *pageReader) presenceAt(id graphstate.EntityID, at temporal.Position) (graphstate.LifeID, error) {
	point, err := temporal.Point(at)
	if err != nil {
		return 0, callerError(err)
	}
	key := graphstate.ComponentKey{Owner: id, Kind: graphstate.Presence}
	m, found, err := q.readMeta(key)
	if err != nil || !found {
		return 0, err
	}
	if m.Axis.Descriptor() != at.Axis().Descriptor() || m.Axis.DefinitionHash() != at.Axis().DefinitionHash() {
		return 0, ErrCorrupt
	}
	leaf, err := q.findLeaf(m, point)
	if err != nil {
		return 0, err
	}
	s, err := q.materialize(leaf)
	if err != nil {
		return 0, err
	}
	cell, err := s.At(at, q.limits.stateLimits(q.q.c))
	if err != nil {
		return 0, err
	}
	if !cell.Present() {
		return 0, nil
	}
	return graphstate.LifeID(cell.Value().ID()), nil
}

// IncidentAt selects exact own/effective presence at an explicit position.
// It is independent of IncidentRelationships' retained historical superset.
// Pages contain a bounded local batch or advance empty after residual filtering;
// their row/byte budget bounds output, with source bounded by GraphLimits.
// Type and opposite-endpoint masks remain charged residuals, not index columns.
func (v *ReadView) IncidentAt(ctx context.Context, query IncidentAtQuery, token graphstate.Cursor, budget graphstate.ReadBudget) (IncidentAtPage, error) {
	if v == nil {
		return IncidentAtPage{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return IncidentAtPage{}, err
	}
	if v.descriptor.format != 3 {
		return IncidentAtPage{}, ErrTopologyUnsupported
	}
	if query.Endpoint == 0 || query.Visible != graphstate.Declared && query.Visible != graphstate.Effective || query.Mode != 0 && query.Mode != graphstate.LifeBound && query.Mode != graphstate.IdentityReference || query.Direction == 0 || query.Direction & ^IncidentBoth != 0 || query.Type != "" && !validName(query.Type, v.c.limits) || budget.Rows < 1 || budget.Bytes < fullPageOutputBytes {
		return IncidentAtPage{}, ErrInvalid
	}
	q, err := v.begin(ctx, graphstate.ReadBudget{})
	if err != nil {
		return IncidentAtPage{}, err
	}
	if err := q.q.materialize(512 + 2*len(query.Type)); err != nil {
		return IncidentAtPage{}, v.finish(q, err)
	}
	hash, err := incidentAtHash(v, q, query)
	if err != nil {
		return IncidentAtPage{}, v.finish(q, callerError(err))
	}
	old, err := v.oldCursor(token, currentPresenceRecord, hash)
	if err != nil {
		return IncidentAtPage{}, v.finish(q, err)
	}
	// Registration/definition identity is independent of endpoint visibility
	// and reference mode. Check it before an Effective LifeBound empty shortcut.
	axis, found, err := q.q.axis(query.At.Axis().Descriptor().ID)
	if err != nil {
		return IncidentAtPage{}, v.finish(q, err)
	}
	if !found || axis.Descriptor() != query.At.Axis().Descriptor() || axis.DefinitionHash() != query.At.Axis().DefinitionHash() {
		return IncidentAtPage{}, v.finish(q, errors.Join(ErrInvalid, temporal.ErrAxisMismatch))
	}
	state := old.current
	if state == nil {
		state = &fullCurrentCursor{}
		mode, life := query.Mode, query.Life
		if query.Visible == graphstate.Effective && mode != graphstate.IdentityReference {
			node, found, e := q.q.entity(EntityRef{v.c.root.namespace.Graph, query.Endpoint})
			if e != nil {
				return IncidentAtPage{}, v.finish(q, e)
			}
			if found && node.Kind != graphstate.Node {
				return IncidentAtPage{}, v.finish(q, ErrInvalid)
			}
			compatible := found && node.Axis.Descriptor() == query.At.Axis().Descriptor() && node.Axis.DefinitionHash() == query.At.Axis().DefinitionHash()
			if compatible {
				state.endpointLife, e = q.presenceAt(query.Endpoint, query.At)
				if e != nil {
					return IncidentAtPage{}, v.finish(q, e)
				}
				// Only a known, compatible endpoint proves these masks. An
				// absent/foreign-axis endpoint must leave matching native rows
				// available to ordinary declared-binding corruption checks.
				if state.endpointLife == 0 || life != 0 && life != state.endpointLife {
					if mode == graphstate.LifeBound {
						return v.emptyIncidentAt(q, token, old)
					}
					mode = graphstate.IdentityReference
				} else {
					life = state.endpointLife
				}
			}
		}
		if err := v.finish(q, q.budget()); err != nil {
			return IncidentAtPage{}, err
		}
		rows, bytes := v.remaining()
		limits := fullPresenceLimits(q)
		limits.pages.MaxWorkRecords = min(limits.pages.MaxWorkRecords, rows)
		limits.pages.MaxWorkBytes = min(limits.pages.MaxWorkBytes, bytes)
		// Resolve rejects an exhausted allowance; never turn zero into defaults.
		if rows < 1 || bytes < limits.pages.MaxCheckpointBytes+128 {
			return IncidentAtPage{}, ErrResourceLimit
		}
		it, constructorWork, e := newCurrentPresenceIteratorWork(ctx, v.c, v.descriptor.own, currentPresenceQuery{endpoint: query.Endpoint, life: life, at: query.At, mode: mode, direction: nativeIncidentDirection(query.Direction), typ: query.Type}, limits)
		v.work = addWork(v.work, constructorWork)
		if e != nil {
			return IncidentAtPage{}, e
		}
		state.iterator = it
		q, err = v.begin(ctx, graphstate.ReadBudget{})
		if err != nil {
			_ = it.close()
			return IncidentAtPage{}, err
		}
	}
	it := state.iterator
	checkpoint := checkpointCurrent(it)
	success := false
	defer func() {
		if !success {
			checkpoint.restore(it)
			if token == 0 {
				_ = it.close()
			}
		}
	}()
	it.op.pageStage.pageReader = q
	it.op.q.fullView = &v.descriptor
	it.op.limits.pages = q.limits
	it.op.limits.pages.MaxCursorBytes = v.limits.Pages.MaxCursorBytes - (v.cursorBytes + v.pages.cursorBytes - old.bytes)
	// Native output is operation-local scratch, not a second published quota.
	it.outputBytes = 0
	scratch, err := newIncidentScratch(q, &v.descriptor)
	if err != nil {
		return IncidentAtPage{}, v.finish(q, err)
	}
	// Bound visits even when every candidate is masked. Each native attempt
	// itself visits at most one codec page of rows, and shares this call's source
	// ledger. Caller output limits independently bound the retained slice.
	attempts := min(8, budget.Rows)
	outputRows := min(attempts, (min(budget.Bytes, v.limits.MaxOutputBytes-v.outputBytes)-fullPageOutputBytes)/cpCandidateOwned)
	if outputRows < 1 {
		return IncidentAtPage{}, v.finish(q, ErrResourceLimit)
	}
	if err := q.q.materialize(cpCandidateOwned); err != nil {
		return IncidentAtPage{}, v.finish(q, err)
	}
	out := IncidentAtPage{View: v.id, Version: v.version(), Candidates: make([]IncidentAtCandidate, 0, 1)}
	progress := 0
	for range attempts {
		attemptCheckpoint := checkpointCurrent(it)
		// A native row is private scratch until its residuals succeed. Reuse
		// the bounded one-row witness rather than retaining per-fact metadata.
		it.outputBytes = 0
		page, attemptErr := it.next(ctx, it.cursor, graphstate.ReadBudget{Rows: 1, Bytes: 4096})
		v.currentWork = it.op.counters
		include := false
		var candidate currentPresenceCandidate
		if attemptErr == nil && len(page.candidates) > 0 {
			candidate = page.candidates[0]
			include, attemptErr = q.includeCurrentCandidate(state, candidate, query, scratch)
		}
		it.op.witness = currentPresenceWitness{}
		if attemptErr == nil && include && len(out.Candidates) == cap(out.Candidates) {
			capacity := min(outputRows, 2*cap(out.Candidates))
			attemptErr = q.q.materialize(cpCandidateOwned * capacity)
			if attemptErr == nil {
				grown := make([]IncidentAtCandidate, len(out.Candidates), capacity)
				copy(grown, out.Candidates)
				out.Candidates = grown
			}
		}
		if attemptErr != nil {
			attemptCheckpoint.restore(it)
			if ctx.Err() == nil && errors.Is(attemptErr, ErrResourceLimit) && progress > 0 {
				// Keep earlier completed advances only. The refused attempt's
				// actual source work remains consumed; finish still enforces
				// the whole-call limits before allowing this continuation.
				break
			}
			return IncidentAtPage{}, v.finish(q, attemptErr)
		}
		progress++
		out.Complete = page.complete
		if include {
			out.Candidates = append(out.Candidates, IncidentAtCandidate{candidate.relationship, candidate.life, incidentRoles(candidate.roles)})
		}
		if out.Complete || len(out.Candidates) == outputRows {
			break
		}
	}
	next := fullContinuation{}
	if !out.Complete {
		cost := it.retainedBytes(it.op.cache, &it.last) + 512
		next = fullContinuation{kind: currentPresenceRecord, query: hash, bytes: cost, current: state}
		out.Next, err = v.storeCursor(token, old, next)
		if err != nil {
			return IncidentAtPage{}, v.finish(q, err)
		}
	}
	if err := v.finish(q, q.budget()); err != nil {
		return IncidentAtPage{}, err
	}
	if err := v.output(fullPageOutputBytes + cpCandidateOwned*cap(out.Candidates)); err != nil {
		return IncidentAtPage{}, err
	}
	v.advanceCursor(token, out.Next, old, next)
	success = true
	if out.Complete {
		_ = it.close()
	}
	return out, nil
}
func (v *ReadView) emptyIncidentAt(q *pageReader, token graphstate.Cursor, old fullContinuation) (IncidentAtPage, error) {
	if err := v.finish(q, q.budget()); err != nil {
		return IncidentAtPage{}, err
	}
	if err := v.output(fullPageOutputBytes); err != nil {
		return IncidentAtPage{}, err
	}
	v.advanceCursor(token, 0, old, fullContinuation{})
	return IncidentAtPage{View: v.id, Version: v.version(), Complete: true}, nil
}

func (q *pageReader) includeCurrentCandidate(cursor *fullCurrentCursor, candidate currentPresenceCandidate, query IncidentAtQuery, scratch *incidentScratch) (bool, error) {
	witness := cursor.iterator.op.witness
	if witness.entity.ID != candidate.relationship || witness.life.Life != candidate.life {
		return false, ErrCorrupt
	}
	if err := q.checkDeclaredWithLookup(witness.entity, witness.life, scratch.hasPosting); err != nil {
		return false, err
	}
	if query.Visible != graphstate.Effective || witness.entity.Mode != graphstate.LifeBound {
		return true, nil
	}
	for _, endpoint := range [2]struct {
		id   graphstate.EntityID
		life graphstate.LifeID
	}{{witness.entity.Source, witness.life.SourceLife}, {witness.entity.Target, witness.life.TargetLife}} {
		active := cursor.endpointLife
		if endpoint.id != query.Endpoint {
			var err error
			active, err = scratch.presenceAt(endpoint.id, query.At)
			if err != nil {
				return false, err
			}
		}
		if active == 0 || active != endpoint.life {
			return false, nil
		}
	}
	return true, nil
}

const incidentScratchOwned = 2048
const incidentPostingOwnedLimit = 128 << 10

type incidentPostingSlot struct {
	node  postingTreeNode
	age   uint64
	valid bool
}
type incidentPresenceSlot struct {
	id    graphstate.EntityID
	life  graphstate.LifeID
	valid bool
}

// This scratch is reachable only from the current call's stack. In particular,
// neither q nor the iterator/cursor contains a backpointer to it.
type incidentScratch struct {
	q            *pageReader
	descriptor   *fullIndexDescriptor
	posting      [6]incidentPostingSlot
	presence     [4]incidentPresenceSlot
	clock        uint64
	nextPresence int
	postingBytes int
}

func newIncidentScratch(q *pageReader, d *fullIndexDescriptor) (*incidentScratch, error) {
	if q.q.full != nil || q.q.stage != nil || q.q.pending != nil || q.q.fullView != d {
		return nil, ErrInvalid
	}
	if err := q.q.materialize(incidentScratchOwned); err != nil {
		return nil, err
	}
	return &incidentScratch{q: q, descriptor: d}, nil
}
func incidentPostingBytes(n postingTreeNode) int { return 128 + 320*(len(n.keys)+len(n.children)) }
func (s *incidentScratch) postingNode(id uint64, kind recordKind, l postingTreeLimits, root bool) (postingTreeNode, error) {
	if s.q.q.full != nil || s.q.q.stage != nil || s.q.q.pending != nil || s.q.q.fullView != s.descriptor {
		return postingTreeNode{}, ErrInvalid
	}
	if err := s.q.q.ctx.Err(); err != nil {
		return postingTreeNode{}, err
	}
	if err := l.validate(); err != nil {
		return postingTreeNode{}, err
	}
	if kind != canonicalIncidentRecord && kind != declaredIncidentRecord {
		return postingTreeNode{}, ErrInvalid
	}
	s.clock++
	for i := range s.posting {
		slot := &s.posting[i]
		if slot.valid && slot.node.id == id && slot.node.kind == kind {
			slot.age = s.clock
			return slot.node, nil
		}
	}
	n, err := s.q.postingTreeNode(id, kind, l)
	if err != nil {
		return postingTreeNode{}, err
	}
	index := 0
	if root {
		if kind == declaredIncidentRecord {
			index = 1
		}
	} else {
		index = 2
		for i := 2; i < len(s.posting); i++ {
			if !s.posting[i].valid {
				index = i
				break
			}
			if s.posting[i].age < s.posting[index].age {
				index = i
			}
		}
	}
	bytes := incidentPostingBytes(n)
	old := 0
	if s.posting[index].valid {
		old = incidentPostingBytes(s.posting[index].node)
	}
	if bytes > incidentPostingOwnedLimit-(s.postingBytes-old) {
		return postingTreeNode{}, ErrResourceLimit
	}
	s.postingBytes += bytes - old
	s.posting[index] = incidentPostingSlot{node: n, age: s.clock, valid: true}
	return n, nil
}
func (s *incidentScratch) hasPosting(root postingTreeRoot, key postingKey, l postingTreeLimits) (bool, error) {
	if key.family != root.kind || !validPostingKey(key, s.q.q.c.limits) {
		return false, ErrInvalid
	}
	n, err := s.postingNode(root.id, root.kind, l, true)
	if err != nil {
		return false, err
	}
	if n.level != root.level || n.summary().count != root.count {
		return false, ErrCorrupt
	}
	for n.level > 0 {
		expected := n.children[postingKeyChildIndex(n, key)]
		level := n.level - 1
		n, err = s.postingNode(expected.id, root.kind, l, false)
		if err != nil {
			return false, err
		}
		actual := n.summary()
		if n.level != level || actual.count != expected.count || actual.first != expected.first || actual.last != expected.last {
			return false, ErrCorrupt
		}
	}
	i, found := slices.BinarySearchFunc(n.keys, key, comparePostingKeys)
	if found && n.keys[i] != key {
		return false, ErrCorrupt
	}
	return found, nil
}
func (s *incidentScratch) presenceAt(id graphstate.EntityID, at temporal.Position) (graphstate.LifeID, error) {
	if err := s.q.q.ctx.Err(); err != nil {
		return 0, err
	}
	for _, slot := range s.presence {
		if slot.valid && slot.id == id {
			return slot.life, nil
		}
	}
	life, err := s.q.presenceAt(id, at)
	if err != nil {
		return 0, err
	}
	s.presence[s.nextPresence] = incidentPresenceSlot{id: id, life: life, valid: true}
	s.nextPresence = (s.nextPresence + 1) % len(s.presence)
	return life, nil
}
