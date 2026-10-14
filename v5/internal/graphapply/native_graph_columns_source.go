package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Only this constructor acquires source ownership, through the actual native
// Host certificate check. Captured roots/counters grant no new cut authority.
// This fixed-slice owner is in-process; restart acquires the ORIGINAL At cuts
// anew or refuses lost retention. Durable cut/feed pins are a separate seam.
type nativeColumnSource struct {
	mu                       sync.Mutex
	ctx                      context.Context
	cancel                   context.CancelFunc
	closed                   bool
	stop                     func() bool
	closeErr                 error
	owner                    [16]byte
	coordinator              *crossProcessCoordinator
	meter                    *nativeColumnTransportMeter
	hosts                    [2]*allocationHost
	views                    [2]*raftlog.ApplicationView
	initial, final           [2]graphCertificate
	roots                    [2]raftlog.ApplicationRoot
	configurations           [2]genesisAllocationConfig
	binding                  nativeColumnBinding
	digest                   [32]byte
	openWork, grant, charged nativeColumnWork
}

// A failed admitted capture retires its full prepaid allowance. This receipt
// carries no live view, token, secret or graph authority.
type nativeColumnSourceFailure struct {
	Charged nativeColumnWork
	Cause   error
}

func (e *nativeColumnSourceFailure) Error() string {
	return "native-column: admitted source capture failed"
}
func (e *nativeColumnSourceFailure) Unwrap() error { return e.Cause }

func nativeColumnLimitAllowance(policy raftlog.ApplicationPolicy, l materializerLimits, p nativeColumnPolicy) (open, grant nativeColumnWork, err error) {
	if !policy.Enabled() || l.validate() != nil || p.validate() != nil {
		return open, grant, errInvalid
	}

	// Application/decoder limits are immutable. Read metadata only; no
	// historical view, Get, Root or control inspection precedes admission.
	capture := nativeColumnWork{Records: 32 + 64*uint64(l.allocation.readRows), Bytes: 16384 + 64*uint64(l.allocation.readBytes) + 8*uint64(l.outputBytes) + 64*uint64(policy.MaxPageBytes) + 16*uint64(policy.MaxImageBytes), DecodedRows: uint64(8*l.maxAxes + 8*l.maxSchemas), OutputBytes: 16384 + 8*uint64(l.outputBytes)}
	// All native decoder traversal/temporary ownership is funded BEFORE
	// decoding and BEFORE this layout's ChunkRows refusal. Delivered wire
	// bounds total decoded entries; the declared limits additionally bound
	// per-group changes and all declaration tables. Catalog calls retain
	// their own ordinary caps and can inspect collision/absent candidates.
	catalogDefaults, graphDefaults := graphstore.DefaultLimits(), graphstore.DefaultGraphLimits()
	catalogRows, catalogBytes := max(l.catalog.MaxReadRows, catalogDefaults.MaxReadRows), max(l.catalog.MaxReadBytes, catalogDefaults.MaxReadBytes)
	declared := uint64(l.maxAxes+l.maxSchemas+3*l.maxClaims) + uint64(max(l.graph.Pages.MaxPatches, graphDefaults.Pages.MaxPatches))*(1+2*uint64(max(l.graph.Planner.Component.MaxChangePieces, state.DefaultLimits().MaxChangePieces)))
	decoded := 8 * min(declared, uint64(p.FrameBytes))
	count := uint64(p.ChunkRecords + 1)
	page := nativeColumnWork{Records: count * (16 + uint64(p.ChunkRows*5+3)*uint64(catalogRows) + uint64(l.allocation.readRows)), Bytes: count * (8192 + 8*uint64(policy.MaxImageBytes) + 8*uint64(policy.MaxOutcomeBytes) + 8*uint64(policy.MaxChangeBytes) + uint64(p.ChunkRows*5+3)*uint64(catalogBytes) + 4*uint64(l.outputBytes) + uint64(16*p.ChunkBytes)), DecodedRows: count * decoded, OutputBytes: count * (4*uint64(l.outputBytes) + uint64(16*p.ChunkBytes))}
	return capture, page, nil
}
func nativeColumnSourceAllowance(hosts [2]*allocationHost, p nativeColumnPolicy) (open, grant nativeColumnWork, err error) {
	for _, h := range hosts {
		if h == nil || h.machine == nil || h.machine.store == nil {
			return open, grant, errInvalid
		}
		capture, page, e := nativeColumnLimitAllowance(h.machine.store.ApplicationLimits(), h.machine.limits, p)
		if e != nil {
			return open, grant, e
		}
		open, err = open.plus(capture)
		if err != nil {
			return open, grant, err
		}
		grant.Records = max(grant.Records, page.Records)
		grant.Bytes = max(grant.Bytes, page.Bytes)
		grant.DecodedRows = max(grant.DecodedRows, page.DecodedRows)
		grant.OutputBytes = max(grant.OutputBytes, page.OutputBytes)
	}
	if !open.admit(grant, p) {
		return open, grant, errLimit
	}
	return open, grant, nil
}
func nativeColumnPeerOpening(open nativeColumnWork, l materializerLimits) (nativeColumnWork, error) {
	doubled, err := open.scaled(2)
	if err != nil {
		return nativeColumnWork{}, err
	}
	return doubled.plus(nativeColumnWork{Records: 2 * uint64(l.sourceRows), Bytes: 2 * uint64(l.sourceBytes), OutputBytes: 2 * uint64(l.outputBytes), RPCs: 2 * 1024, WireBytes: 4 * 1024 * uint64(graphSourceFrameBytes)})
}

func nativeColumnCutPair(c graphCut, hosts [2]*allocationHost) ([2]graphCertificate, error) {
	var pair [2]graphCertificate
	if len(c.certificates) != 2 || hosts[0] == nil || hosts[1] == nil || hosts[0].machine == nil || hosts[1].machine == nil || hosts[0].machine.ns.partition >= hosts[1].machine.ns.partition {
		return pair, errInvalid
	}
	for _, cert := range c.certificates {
		if _, err := encodeGraphCertificate(cert); err != nil {
			return pair, err
		}
		matched := false
		for g, h := range hosts {
			if cert.scope == h.machine.scope() {
				if pair[g].index != 0 {
					return pair, errInvalid
				}
				pair[g] = cert
				matched = true
			}
		}
		if !matched {
			return pair, errInvalid
		}
	}
	if pair[0].index == 0 || pair[1].index == 0 || pair[0].round != pair[1].round || pair[0].configuration != pair[1].configuration {
		return pair, errInvalid
	}
	return pair, nil
}

func newNativeColumnSource(ctx context.Context, hosts [2]*allocationHost, initial, final graphCut, policy nativeColumnPolicy) (source *nativeColumnSource, err error) {
	if ctx == nil || policy.validate() != nil {
		return nil, errInvalid
	}
	first, err := nativeColumnCutPair(initial, hosts)
	if err != nil {
		return nil, err
	}
	last, err := nativeColumnCutPair(final, hosts)
	if err != nil {
		return nil, err
	}
	open, grant, err := nativeColumnSourceAllowance(hosts, policy)
	if err != nil {
		return nil, err
	}
	owner, cancel := context.WithTimeout(ctx, policy.Lifetime)
	s := &nativeColumnSource{ctx: owner, cancel: cancel, hosts: hosts, initial: first, final: last, openWork: open, grant: grant, charged: open}
	defer func() {
		if err != nil {
			err = &nativeColumnSourceFailure{Charged: open, Cause: errors.Join(err, s.Close())}
		}
	}()
	s.owner, err = graphReadNonce()
	if err != nil {
		return nil, err
	}
	s.binding = nativeColumnBinding{Version: 1, Layout: nativeColumnLayout, Coverage: 3, Agreement: sha256.Sum256([]byte("rho-tkg:V2:native-derived-change-columns:v1\x00complete-native-source;typed-i64;exact-native-scope;retain-all;no-authority\x00")), Policy: policy}
	for g, h := range hosts {
		if first[g].configuration != last[g].configuration || first[g].scope != last[g].scope || first[g].round > last[g].round || first[g].sourceIndex > last[g].sourceIndex {
			return nil, errInvalid
		}
		// Authenticate initial before holding the final view; no extra live lease
		// is needed per chunk, and no source ledger is reset after an ID.
		v, e := h.AtGraphCertificate(owner, first[g])
		if e != nil {
			return nil, e
		}
		initialRoot, e := v.RootBounded(owner, 172)
		e = errors.Join(e, v.Close())
		if e != nil {
			return nil, e
		}
		s.views[g], err = h.AtGraphCertificate(owner, last[g])
		if err != nil {
			return nil, err
		}
		s.roots[g], err = s.views[g].RootBounded(owner, 172)
		if err != nil {
			return nil, err
		}
		retained, e := s.views[g].RootAt(owner, first[g].sourceIndex, 172)
		if e != nil || retained.Generation != s.roots[g].Generation || !bytes.Equal(retained.Image, initialRoot.Image) {
			return nil, errors.Join(errInvalid, e)
		}
		binding, e := s.views[g].ApplicationBinding(owner)
		if e != nil || binding != h.machine.store.ApplicationBinding() || binding.SemanticContractID != last[g].scope.semantic {
			return nil, errors.Join(errInvalid, e)
		}
		h.mu.Lock()
		cfg, e := h.currentConfiguration()
		h.mu.Unlock()
		if e != nil {
			return nil, e
		}
		actual, e := cfg.digest()
		if e != nil || actual != last[g].configuration {
			return nil, errors.Join(errInvalid, e)
		}
		s.binding.Configurations[g], err = encodeGraphGenesisConfig(cfg)
		if err != nil {
			return nil, err
		}
		stored, found, e := s.views[g].Get(owner, genesisConfigurationKey(h.machine.ns), partitionGenesisConfigBytes)
		if e != nil || !found || stored.Deleted || !bytes.Equal(stored.Value, s.binding.Configurations[g]) {
			return nil, errors.Join(errCorrupt, e)
		}
		s.configurations[g] = cfg
	}
	if !bytes.Equal(s.binding.Configurations[0], s.binding.Configurations[1]) {
		return nil, errInvalid
	}
	body, err := nativeColumnMetadata(s.binding)
	if err != nil {
		return nil, err
	}
	s.digest = sha256.Sum256(body)
	s.charged = s.openWork
	// Publish teardown ownership only after every view assignment is complete.
	// An already-fired context callback cannot race constructor assignments.
	s.mu.Lock()
	err = owner.Err()
	if err == nil {
		s.stop = context.AfterFunc(owner, func() { _ = s.Close() })
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s, nil
}
func (s *nativeColumnSource) generation(id [16]byte) (nativeColumnGeneration, error) {
	if s == nil || id == ([16]byte{}) {
		return nativeColumnGeneration{}, errInvalid
	}
	g := nativeColumnGeneration{ID: id, Binding: s.digest, Coverage: s.binding.Coverage}
	for n := range 2 {
		var err error
		g.Sources[n].Initial, err = encodeGraphCertificate(s.initial[n])
		if err != nil {
			return g, err
		}
		g.Sources[n].Final, err = encodeGraphCertificate(s.final[n])
		if err != nil {
			return g, err
		}
		g.Sources[n].Generation = s.roots[n].Generation
	}
	return g, nil
}
func (s *nativeColumnSource) Close() error {
	if s == nil {
		return errInvalid
	}
	s.cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	if s.stop != nil {
		s.stop()
		s.stop = nil
	}
	for i, v := range s.views {
		if v != nil {
			s.closeErr = errors.Join(s.closeErr, v.Close())
			s.views[i] = nil
		}
	}
	return s.closeErr
}
func (s *nativeColumnSource) check(ctx context.Context) error {
	if ctx == nil {
		return errInvalid
	}
	if s.closed {
		return errInvalid
	}
	return errors.Join(ctx.Err(), s.ctx.Err())
}
func (s *nativeColumnSource) operation(ctx context.Context) (context.Context, func()) {
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	return call, func() { stop(); cancel() }
}

// A page attempt is charged BEFORE reading. The caller persists its matching
// grant before calling this seam. At most one frame is hydrated at a time.
func (s *nativeColumnSource) page(ctx context.Context, positions [2]uint64) (frames []nativeColumnFrame, next [2]uint64, err error) {
	if s == nil {
		return nil, next, errInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.check(ctx); err != nil {
		return nil, next, err
	}
	p := s.binding.Policy
	if !s.charged.admit(s.grant, p) {
		return nil, next, errLimit
	}
	s.charged = s.charged.add(s.grant)
	call, done := s.operation(ctx)
	defer done()
	next = positions
	for g := range 2 {
		if !s.covers(g) {
			if next[g] != s.initial[g].sourceIndex {
				return nil, [2]uint64{}, errInvalid
			}
			continue
		}
		if next[g] < s.initial[g].sourceIndex || next[g] > s.final[g].sourceIndex {
			return nil, [2]uint64{}, errInvalid
		}
		for next[g] < s.final[g].sourceIndex && len(frames) < p.ChunkRecords {
			frame, e := s.frame(call, g, next[g]+1)
			if e != nil {
				return nil, [2]uint64{}, e
			}
			candidate := append(frames, frame)
			generation, e := s.generation([16]byte{1})
			if e != nil {
				return nil, [2]uint64{}, e
			}
			if _, e = encodeNativeColumnChunk(generation, 0, [32]byte{}, candidate, p); e != nil {
				if len(frames) > 0 && errors.Is(e, errLimit) {
					return frames, next, nil
				}
				return nil, [2]uint64{}, e
			}
			frames = candidate
			next[g]++
		}
		if len(frames) == p.ChunkRecords {
			break
		}
	}
	if e := errors.Join(call.Err(), s.ctx.Err()); e != nil {
		return nil, [2]uint64{}, e
	}
	if len(frames) == 0 {
		// Empty/end-of-prefix is still an actual live source operation. Closing
		// the underlying store cannot turn cached endpoints into successful EOF.
		for g, view := range s.views {
			if !s.covers(g) {
				continue
			}
			root, e := view.RootBounded(call, 172)
			if e != nil || root.Generation != s.roots[g].Generation || root.Index != s.final[g].sourceIndex || root.ImageHash != s.roots[g].ImageHash || !bytes.Equal(root.Image, s.final[g].image[:]) {
				return nil, [2]uint64{}, errors.Join(errCorrupt, e)
			}
		}
	}
	return frames, next, nil
}

func (s *nativeColumnSource) frame(ctx context.Context, group int, index uint64) (out nativeColumnFrame, err error) {
	v, h, p := s.views[group], s.hosts[group], s.binding.Policy
	root, err := v.RootAt(ctx, index, 172)
	if err != nil {
		return out, err
	}
	if root.Generation != s.roots[group].Generation || root.Index != index || len(root.Image) != 172 {
		return out, errCorrupt
	}
	out.Partition, out.Index = h.machine.ns.partition, index
	copy(out.Root[:], root.Image)
	out.Outcome, err = v.ApplicationRecord(ctx, index, true, min(p.FrameBytes, h.machine.store.ApplicationLimits().MaxPageBytes))
	if err != nil {
		return out, err
	}
	out.CDC, err = v.ApplicationRecord(ctx, index, false, min(p.FrameBytes, h.machine.store.ApplicationLimits().MaxPageBytes))
	if err != nil {
		return out, err
	}
	if out.Outcome == nil || out.CDC == nil {
		return out, errCorrupt
	}
	var bound outcome
	switch {
	case len(out.Outcome) == 0:
		if len(out.CDC) != 0 || index <= 1 {
			return out, errCorrupt
		}
		before, e := v.RootAt(ctx, index-1, 172)
		if e != nil || !bytes.Equal(before.Image, root.Image) {
			return out, errors.Join(errCorrupt, e)
		}
	case bytes.HasPrefix(out.Outcome, []byte("GSO\x01")):
		r, e := decodeGraphCutResult(out.Outcome)
		if e != nil || r.index != index || len(out.CDC) != 0 {
			return out, errors.Join(errCorrupt, e)
		}
	case bytes.HasPrefix(out.Outcome, []byte("GCS\x01")):
		st, e := decodeCrossStatus(out.Outcome)
		if e != nil || st.ns != h.machine.ns || st.index > index {
			return out, errors.Join(errCorrupt, e)
		}
		arena, e := graphstate.NewOutputBudget(h.machine.limits.outputBytes)
		if e != nil {
			return out, e
		}
		q := reader{ctx: ctx, view: v, ns: h.machine.ns, base: s.roots[group], limits: h.machine.limits.allocation, arena: arena}
		registration, found, e := crossControl(&q, crossControlKey(q.ns, st.tx, 'R'), p.FrameBytes)
		if e != nil || !found || registration.Index > index || sha256.Sum256(registration.Value) != st.digest {
			return out, errors.Join(errCorrupt, e)
		}
		manifest, e := decodeCrossManifest(registration.Value, h.machine.limits, arena)
		if e != nil || manifest.tx != st.tx || manifest.digest != st.digest || h.machine.validateCrossManifestBinding(manifest, s.configurations[group]) != nil {
			return out, errors.Join(errCorrupt, e)
		}
		out.Registration = registration.Value
		if st.index < index {
			if len(out.CDC) != 0 {
				return out, errCorrupt
			}
			if err = s.replay(ctx, group, &out, st.index); err != nil {
				return out, err
			}
			if !bytes.Equal(out.OriginalOutcome, out.Outcome) {
				return out, errCorrupt
			}
			if st.phase == crossResolve && st.commit {
				if len(out.OriginalCDC) == 0 {
					return out, errCorrupt
				}
				original := outcome{ns: st.ns, kind: graphOperations, identity: st.tx, hash: st.digest, index: st.index, disposition: applied}
				if _, e := decodeTypedChangeEnvelope(out.OriginalCDC, original, h.machine.limits, true); e != nil {
					return out, e
				}
			} else if len(out.OriginalCDC) != 0 {
				return out, errCorrupt
			}
			return out, nil
		}
		if len(out.CDC) > 0 {
			if st.phase != crossResolve || !st.commit {
				return out, errCorrupt
			}
			bound = outcome{ns: st.ns, kind: graphOperations, identity: st.tx, hash: st.digest, index: index, disposition: applied}
		} else if st.phase == crossResolve && st.commit {
			return out, errCorrupt
		}
	default:
		o, e := decodePartitionMappedOutcome(out.Outcome, h.machine.ns)
		if e != nil || o.index > index {
			return out, errors.Join(errCorrupt, e)
		}
		if o.kind == graphOperations && o.index == index && o.reason == reasonNone && o.disposition == applied && len(out.CDC) == 0 {
			return out, errCorrupt
		}
		if o.kind == graphOperations && o.index < index {
			if o.disposition != requestReplay || len(out.CDC) != 0 {
				return out, errCorrupt
			}
			if err = s.replay(ctx, group, &out, o.index); err != nil {
				return out, err
			}
			original, e := decodePartitionGraphOutcome(out.OriginalOutcome, h.machine.ns)
			if e != nil || original.identity != o.identity || original.hash != o.hash || original.kind != o.kind || original.index != o.index || original.reason != o.reason || original.disposition != applied {
				return out, errors.Join(errCorrupt, e)
			}
			if original.reason == reasonNone {
				if len(out.OriginalCDC) == 0 {
					return out, errCorrupt
				}
				if _, e := decodeTypedChangeEnvelope(out.OriginalCDC, original, h.machine.limits, true); e != nil {
					return out, e
				}
			} else if len(out.OriginalCDC) != 0 {
				return out, errCorrupt
			}
			return out, nil
		}
		if len(out.CDC) > 0 {
			if o.kind != graphOperations || o.index != index || o.disposition != applied || o.reason != reasonNone {
				return out, errCorrupt
			}
			bound = o
		}
	}
	if len(out.CDC) == 0 {
		return out, nil
	}
	changes, err := decodeTypedChangeEnvelope(out.CDC, bound, h.machine.limits, true)
	if err != nil {
		return out, err
	}
	if changes.initialized {
		return out, errInvalid
	} // Initial cut must follow graph genesis.
	l := h.machine.limits
	catalog, _, err := graphstore.OpenPartitionCatalogWithDefaultAxis(ctx, v, graphstore.Namespace{Graph: graphstate.GraphID(h.machine.ns.graph), Partition: h.machine.ns.partition}, h.machine.scope().ownership, s.configurations[group].defaultAxis, l.catalog, l.graph, graphstore.OwnershipBudget{SourceRows: l.sourceRows, SourceBytes: l.sourceBytes, OutputBytes: l.outputBytes})
	if err != nil {
		return out, err
	}
	for _, component := range changes.groups {
		if component.Key.Kind != graphstate.Presence && (component.Key.Kind != graphstate.ScalarProperty || component.Key.Name != "p") {
			return out, errInvalid
		}
		entity, found, e := catalog.Entity(ctx, graphstore.EntityRef{Graph: graphstate.GraphID(h.machine.ns.graph), ID: component.Key.Owner})
		if e != nil || !found {
			return out, errors.Join(errCorrupt, e)
		}
		if entity.Kind == graphstate.Relationship && entity.Mode != graphstate.LifeBound {
			return out, errInvalid
		}
		if entity.Axis.DefinitionHash() != component.Owned.Axis().DefinitionHash() {
			return out, errCorrupt
		}
		if component.Key.Kind == graphstate.ScalarProperty {
			definition, found, e := catalog.Property(ctx, entity.Kind, component.Key.Name)
			if e != nil || !found || definition.Type != graphstate.ScalarI64 || definition.Cardinality != graphstate.ScalarCardinality {
				return out, errors.Join(errInvalid, e)
			}
		}
		for _, change := range component.Changes {
			if len(out.Rows) == p.ChunkRows {
				return out, errLimit
			}
			scope, e := temporal.AppendScope(nil, change.Scope(), h.machine.limits.graph.Planner.Component.Temporal)
			if e != nil || len(scope) > p.FrameBytes {
				return out, errors.Join(errLimit, e)
			}
			row := nativeColumnRow{Owner: uint64(component.Key.Owner), Life: uint64(component.Key.Life), Component: byte(component.Key.Kind), Kind: byte(entity.Kind), Mode: byte(entity.Mode), Interpretation: byte(entity.Interpretation), Role: byte(entity.TemporalRole), Name: component.Key.Name, Type: entity.Type, Source: uint64(entity.Source), Target: uint64(entity.Target), Scope: scope}
			row.Axis, e = boundedEncoding(p.FrameBytes, func(w *boundedWriter) { writeAxis(w, change.Scope().Axis(), l) })
			if e != nil {
				return out, e
			}
			row.Before, e = nativeColumnCellFromSource(ctx, catalog, graphstate.GraphID(h.machine.ns.graph), component.Key.Kind, change.Before())
			if e != nil {
				return out, e
			}
			row.After, e = nativeColumnCellFromSource(ctx, catalog, graphstate.GraphID(h.machine.ns.graph), component.Key.Kind, change.After())
			if e != nil {
				return out, e
			}
			life := row.Life
			if component.Key.Kind == graphstate.Presence {
				life = row.After.Handle
				if life == 0 {
					life = row.Before.Handle
				}
			}
			if life != 0 {
				actual, found, e := catalog.Life(ctx, graphstore.LifeRef{Graph: graphstate.GraphID(h.machine.ns.graph), Owner: entity.ID, ID: graphstate.LifeID(life)})
				if e != nil || !found {
					return out, errors.Join(errCorrupt, e)
				}
				row.SourceLife, row.TargetLife = uint64(actual.SourceLife), uint64(actual.TargetLife)
			}
			out.Rows = append(out.Rows, row)
		}
	}
	return out, nil
}

func (s *nativeColumnSource) replay(ctx context.Context, group int, out *nativeColumnFrame, index uint64) error {
	v, p := s.views[group], s.binding.Policy
	var err error
	out.OriginalOutcome, err = v.ApplicationRecord(ctx, index, true, min(p.FrameBytes, s.hosts[group].machine.store.ApplicationLimits().MaxPageBytes))
	if err != nil {
		return err
	}
	out.OriginalCDC, err = v.ApplicationRecord(ctx, index, false, min(p.FrameBytes, s.hosts[group].machine.store.ApplicationLimits().MaxPageBytes))
	if err != nil {
		return err
	}
	if out.OriginalOutcome == nil || out.OriginalCDC == nil || out.Index <= 1 {
		return errCorrupt
	}
	previous, err := v.RootAt(ctx, out.Index-1, 172)
	if err != nil || !bytes.Equal(previous.Image, out.Root[:]) {
		return errors.Join(errCorrupt, err)
	}
	return nil
}

func nativeColumnCellFromSource(ctx context.Context, c *graphstore.Catalog, graph graphstate.GraphID, kind graphstate.ComponentKind, cell state.Cell) (nativeColumnCell, error) {
	out := nativeColumnCell{Present: cell.Present(), Revision: cell.Revision().ID(), Provenance: cell.Revision().Provenance()}
	if !out.Present {
		return out, nil
	}
	out.Null, out.Handle = cell.Value().IsNull(), cell.Value().ID()
	if kind == graphstate.Presence {
		if out.Null || out.Handle == 0 {
			return nativeColumnCell{}, errCorrupt
		}
		return out, nil
	}
	if out.Null {
		return out, nil
	}
	value, found, err := c.Value(ctx, graphstore.ValueRef{Graph: graph, ID: graphstate.ValueID(out.Handle)})
	if err != nil || !found {
		return nativeColumnCell{}, errors.Join(errCorrupt, err)
	}
	var ok bool
	out.Value, ok = value.Value.Int64Value()
	if !ok {
		return nativeColumnCell{}, errInvalid
	}
	return out, nil
}

// Seal is restricted to a quiescent comparative source. Current native state is
// inspected under each COVERED Host event lock through the derived commit. A
// local mask makes no simultaneous global quiescence claim. Old held views stay
// valid after later writes; reads do not require publication quiescence.
func (s *nativeColumnSource) quiescent(ctx context.Context, commit func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	if !s.charged.admit(s.openWork, s.binding.Policy) {
		return errLimit
	}
	s.charged = s.charged.add(s.openWork)
	call, done := s.operation(ctx)
	defer done()
	for g, h := range s.hosts {
		if s.covers(g) {
			h.mu.Lock()
			defer h.mu.Unlock()
		}
	}
	for g, h := range s.hosts {
		if !s.covers(g) {
			continue
		}
		if h.unavailableLocked() {
			return errNativeColumnChanged
		}
		v, e := h.machine.store.ApplicationView(h.driver.Applied())
		if e != nil {
			return e
		}
		root, e := v.RootBounded(call, 172)
		e = errors.Join(e, v.Close())
		if e != nil {
			return e
		}
		if root.Generation != s.roots[g].Generation || !bytes.Equal(root.Image, s.final[g].image[:]) {
			return errNativeColumnChanged
		}
	}
	if err := call.Err(); err != nil {
		return err
	}
	return commit()
}

func (s *nativeColumnSource) at(ctx context.Context, at graphCut) ([2]graphCertificate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return [2]graphCertificate{}, err
	}
	pair, err := s.cutPair(at)
	if err != nil {
		return pair, err
	}
	for g := range 2 {
		if pair[g].scope != s.final[g].scope || pair[g].configuration != s.final[g].configuration || pair[g].round < s.initial[g].round || pair[g].round > s.final[g].round || pair[g].sourceIndex < s.initial[g].sourceIndex || pair[g].sourceIndex > s.final[g].sourceIndex {
			return pair, errInvalid
		}
	}
	if !s.charged.admit(s.openWork, s.binding.Policy) {
		return pair, errLimit
	}
	s.charged = s.charged.add(s.openWork)
	call, done := s.operation(ctx)
	defer done()
	if s.coordinator != nil {
		if err = s.authenticateCut(call, at, false); err != nil {
			return pair, err
		}
	}
	for g, h := range s.hosts {
		if !s.covers(g) {
			continue
		}
		v, e := h.AtGraphCertificate(call, pair[g])
		if e != nil {
			return pair, e
		}
		if e = v.Close(); e != nil {
			return pair, e
		}
	}
	return pair, nil
}

func (s *nativeColumnSource) covers(group int) bool {
	return group >= 0 && group < 2 && s.binding.Coverage&(1<<group) != 0
}
func (s *nativeColumnSource) finalPositions() [2]uint64 {
	var out [2]uint64
	for g := range 2 {
		out[g] = s.initial[g].sourceIndex
		if s.covers(g) {
			out[g] = s.final[g].sourceIndex
		}
	}
	return out
}
func (s *nativeColumnSource) cutPair(cut graphCut) ([2]graphCertificate, error) {
	if s.coordinator == nil {
		return nativeColumnCutPair(cut, s.hosts)
	}
	return nativeColumnPeerCutPair(s.coordinator, cut)
}
func nativeColumnPeerCutPair(c *crossProcessCoordinator, cut graphCut) (pair [2]graphCertificate, err error) {
	if c == nil || c.host == nil || c.host.machine == nil || len(cut.certificates) != 2 || c.host.machine.declaration.Len() != 2 || len(c.transports) != 2 {
		return pair, errInvalid
	}
	for g := range 2 {
		part, ok := c.host.machine.declaration.PartitionAt(g)
		if !ok {
			return pair, errInvalid
		}
		actual := c.transports[part.Partition]
		if actual == nil {
			return pair, errInvalid
		}
		peer := actual.Peer()
		if peer.voter == 0 || peer.scope.partition != part.Partition {
			return pair, errInvalid
		}
		for _, cert := range cut.certificates {
			if cert.scope == peer.scope {
				if pair[g].index != 0 {
					return pair, errInvalid
				}
				pair[g] = cert
			}
		}
		if pair[g].index == 0 {
			return pair, errInvalid
		}
		if _, err = encodeGraphCertificate(pair[g]); err != nil {
			return pair, err
		}
	}
	if pair[0].round != pair[1].round || pair[0].configuration != pair[1].configuration {
		return pair, errInvalid
	}
	return pair, nil
}

type nativeColumnTransportMeter struct {
	mu                                                        sync.Mutex
	fundedCalls, fundedBytes, calls, requestBytes, replyBytes uint64
	failed                                                    bool
}
type nativeColumnMeteredTransport struct {
	actual graphPeerWireTransport
	meter  *nativeColumnTransportMeter
}

func (t *nativeColumnMeteredTransport) Peer() graphPeerIdentity { return t.actual.Peer() }
func (m *nativeColumnTransportMeter) fund(work nativeColumnWork) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failed || !nativeColumnAdd(m.fundedCalls, work.RPCs) || !nativeColumnAdd(m.fundedBytes, work.WireBytes) {
		return errLimit
	}
	m.fundedCalls += work.RPCs
	m.fundedBytes += work.WireBytes
	return nil
}
func (m *nativeColumnTransportMeter) snapshot() (uint64, uint64, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls, m.requestBytes, m.replyBytes
}
func (t *nativeColumnMeteredTransport) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	m := t.meter
	m.mu.Lock()
	if m.failed || m.requestBytes > m.fundedBytes || m.replyBytes > m.fundedBytes-m.requestBytes {
		m.mu.Unlock()
		return nil, errLimit
	}
	used := m.requestBytes + m.replyBytes
	if m.calls >= m.fundedCalls || len(wire) > graphSourceFrameBytes || uint64(len(wire))+uint64(graphSourceFrameBytes) > m.fundedBytes-used {
		m.mu.Unlock()
		return nil, errLimit
	}
	m.calls++
	m.requestBytes += uint64(len(wire))
	m.mu.Unlock()
	reply, err := t.actual.RoundTrip(ctx, wire)
	m.mu.Lock()
	if !nativeColumnAdd(m.replyBytes, uint64(len(reply))) {
		m.failed = true
		err = errors.Join(err, errLimit)
	} else {
		m.replyBytes += uint64(len(reply))
		if len(reply) > graphSourceFrameBytes || m.requestBytes > m.fundedBytes || m.replyBytes > m.fundedBytes-m.requestBytes {
			m.failed = true
			err = errors.Join(err, errLimit)
		}
	}
	m.mu.Unlock()
	return reply, err
}

// Capture authenticates the ENTIRE actual cut/configuration through existing
// source transport. It binds omitted peers but grants no EOF for their rows.
// Each capture has ordinary finite RPC limits, prepaid on the SAME source owner.
func (s *nativeColumnSource) authenticateCut(ctx context.Context, cut graphCut, capture bool) (err error) {
	if s.coordinator == nil || s.meter == nil {
		return errInvalid
	}
	if err = s.meter.fund(nativeColumnWork{RPCs: 1024, WireBytes: 2 * 1024 * uint64(graphSourceFrameBytes)}); err != nil {
		return err
	}
	pair, err := s.cutPair(cut)
	if err != nil {
		return err
	}
	read, err := s.coordinator.OpenCertified(ctx, graphReadRequest{mode: graphReadAt, cut: cut}, nil)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, read.Close()) }()
	actual, err := read.Cut()
	if err != nil || len(actual.certificates) != 2 || len(read.sources) != 2 {
		return errors.Join(errInvalid, err)
	}
	for g, source := range read.sources {
		remote, ok := source.(*remoteGraphSource)
		if !ok || remote.certified == nil || remote.certified.certificate != pair[g] || actual.certificates[g] != pair[g] {
			return errInvalid
		}
		info := remote.certified
		cfg, err := encodeGraphGenesisConfig(info.configuration)
		if err != nil {
			return err
		}
		scope, err := encodeAllocationScope(remote.endpoint.peer.scope)
		if err != nil {
			return err
		}
		peer, err := boundedEncoding(256, func(w *boundedWriter) { w.add([]byte("NCP1")); w.field(scope); w.u64(remote.endpoint.peer.voter) })
		if err != nil {
			return err
		}
		if s.binding.Peers[g] == nil {
			s.binding.Peers[g] = peer
		} else if !bytes.Equal(s.binding.Peers[g], peer) {
			return errInvalid
		}
		if s.binding.Configurations[g] == nil {
			s.binding.Configurations[g] = cfg
		} else if !bytes.Equal(s.binding.Configurations[g], cfg) {
			return errInvalid
		}
		if s.roots[g].Generation != 0 && info.root.Generation != s.roots[g].Generation {
			return errInvalid
		}
		if capture {
			s.roots[g] = info.root
			s.configurations[g] = info.configuration
		}
	}
	if !bytes.Equal(s.binding.Configurations[0], s.binding.Configurations[1]) {
		return errInvalid
	}
	return nil
}

// A genuine local publisher owns one native Host, not a reconstructed remote
// Host. Coverage declares which complete raw prefix this generation contains.
// Full-cut authentication precedes ownership; the separate peer generation must
// independently reach its own genuine source EOF before a parent can join both.
func newPartitionNativeColumnSource(ctx context.Context, c *crossProcessCoordinator, initial, final graphCut, p nativeColumnPolicy) (source *nativeColumnSource, err error) {
	if ctx == nil || p.validate() != nil || p.MaxRPCs == 0 || p.MaxWireBytes == 0 {
		return nil, errInvalid
	}
	first, err := nativeColumnPeerCutPair(c, initial)
	if err != nil {
		return nil, err
	}
	last, err := nativeColumnPeerCutPair(c, final)
	if err != nil {
		return nil, err
	}
	h := c.host
	group := -1
	for g := range 2 {
		if last[g].scope == h.machine.scope() {
			group = g
		}
		if first[g].scope != last[g].scope || first[g].configuration != last[g].configuration || first[g].round > last[g].round || first[g].sourceIndex > last[g].sourceIndex {
			return nil, errInvalid
		}
	}
	if group < 0 {
		return nil, errInvalid
	}
	// Static native ceilings plus BOTH complete cut captures and local historical
	// hydration are funded before any At/Get/Root/control/RPC effect. A failed
	// capture retires the entire grant, even without trustworthy reply counters.
	open, grant, err := nativeColumnSourceAllowance([2]*allocationHost{h, h}, p)
	if err != nil {
		return nil, err
	}
	open, err = nativeColumnPeerOpening(open, h.machine.limits)
	if err != nil {
		return nil, err
	}
	if !open.admit(grant, p) {
		return nil, errLimit
	}
	owner, cancel := context.WithTimeout(ctx, p.Lifetime)
	s := &nativeColumnSource{ctx: owner, cancel: cancel, initial: first, final: last, openWork: open, grant: grant, charged: open, meter: new(nativeColumnTransportMeter)}
	s.hosts[group] = h
	s.binding = nativeColumnBinding{Version: 1, Layout: nativeColumnLayout, Coverage: 1 << group, Policy: p, Agreement: sha256.Sum256([]byte("rho-tkg:V2:partition-local-native-change-columns:v1\x00authenticated-full-cuts;explicit-local-prefix-coverage;complete-native-originals;typed-i64;exact-native-scope;local-quiescence;retain-all;no-authority\x00"))}
	wrapped := make(map[uint64]graphPeerWireTransport, 2)
	for partition, actual := range c.transports {
		wrapped[partition] = &nativeColumnMeteredTransport{actual, s.meter}
	}
	s.coordinator = &crossProcessCoordinator{host: h, transports: wrapped, streams: c.streams}
	defer func() {
		if err != nil {
			err = &nativeColumnSourceFailure{Charged: open, Cause: errors.Join(err, s.Close())}
		}
	}()
	s.owner, err = graphReadNonce()
	if err != nil {
		return nil, err
	}
	if err = s.authenticateCut(owner, initial, false); err != nil {
		return nil, err
	}
	if err = s.authenticateCut(owner, final, true); err != nil {
		return nil, err
	}
	v, err := h.AtGraphCertificate(owner, first[group])
	if err != nil {
		return nil, err
	}
	initialRoot, e := v.RootBounded(owner, 172)
	e = errors.Join(e, v.Close())
	if e != nil {
		return nil, e
	}
	s.views[group], err = h.AtGraphCertificate(owner, last[group])
	if err != nil {
		return nil, err
	}
	root, err := s.views[group].RootBounded(owner, 172)
	if err != nil {
		return nil, err
	}
	retained, err := s.views[group].RootAt(owner, first[group].sourceIndex, 172)
	if err != nil || retained.Generation != root.Generation || root.Generation != s.roots[group].Generation || root.Index != last[group].sourceIndex || !bytes.Equal(root.Image, last[group].image[:]) || !bytes.Equal(retained.Image, initialRoot.Image) {
		return nil, errors.Join(errInvalid, err)
	}
	binding, err := s.views[group].ApplicationBinding(owner)
	if err != nil || binding != h.machine.store.ApplicationBinding() || binding.SemanticContractID != last[group].scope.semantic {
		return nil, errors.Join(errInvalid, err)
	}
	actual, found, err := s.views[group].Get(owner, genesisConfigurationKey(h.machine.ns), partitionGenesisConfigBytes)
	if err != nil || !found || actual.Deleted || !bytes.Equal(actual.Value, s.binding.Configurations[group]) {
		return nil, errors.Join(errCorrupt, err)
	}
	body, err := nativeColumnMetadata(s.binding)
	if err != nil {
		return nil, err
	}
	s.digest = sha256.Sum256(body)
	s.mu.Lock()
	err = owner.Err()
	if err == nil {
		s.stop = context.AfterFunc(owner, func() { _ = s.Close() })
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s, nil
}
