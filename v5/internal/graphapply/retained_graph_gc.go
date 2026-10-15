package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// This agreement extends every retained/P05/range/certified/cross capability.
// Transport and retained-source handoff keep the original keeper authority.
// These native floor commands do not retire graph logical key families.
func collectedGraphSemanticContractID() raftlog.ApplicationSemanticContractID {
	base := retainedGraphSemanticContractID()
	return raftlog.ApplicationSemanticContractID(sha256.Sum256(append(base[:], []byte("native-retention=authenticated-sparse-plan-v1\x00source=current-plan-anchor-v1\x00originals=immutable-controls-v1\x00handoff=resolved-fence-retained-lineage-v1\x00")...)))
}
func retainedGraphAgreement(id raftlog.ApplicationSemanticContractID) bool {
	return id == retainedGraphSemanticContractID() || collectedGraphAgreement(id)
}

// A fresh AS7 native store is required; old directories/agreements are never
// reopened or silently upgraded. Transport and retained-source handoff share
// this exact immutable capability chain.
func newCollectedGraphMaterializer(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits) (*declaredMaterializer, error) {
	if s == nil || s.ApplicationRetention().Version != 1 || s.ApplicationRecovery().Version != 2 || s.ApplicationControls().Version != 1 {
		return nil, errInvalid
	}
	return newDeclaredMaterializerWithAgreement(s, n, d, l, collectedGraphSemanticContractID())
}
func (m *declaredMaterializer) collectsHistory() bool {
	return m != nil && collectedGraphAgreement(m.SemanticContractID())
}

var errGraphHistoryExpired = errors.New("graphapply: authenticated graph history below retained floor")
var errGraphRetentionPending = errors.New("graphapply: retention protected by unresolved or retained owners")

const graphRetentionCommandBytes = 512
const (
	graphRetentionApplied byte = iota + 1
	graphRetentionStale
	graphRetentionBlocked
)

// Native index dimensions and graph certificate installation coordinates remain
// distinct. These inert requested floors are not proof that any data is retained.
type graphRetentionFloors struct{ root, data, feed, outcome, recovery, certificate uint64 }
type graphRetentionCommand struct {
	scope                   allocationScope
	issuer, request         [16]byte
	configuration, previous [32]byte
	floors                  graphRetentionFloors
}
type graphRetentionResult struct {
	id, sourceHash       [32]byte
	index, source, round uint64
	status               byte
}

func graphRetentionOriginalKey(n namespace, issuer, request [16]byte) []byte {
	return append(append(recordKey(n, 25), issuer[:]...), request[:]...)
}
func graphRetentionPlanKey(n namespace, id [32]byte) []byte {
	return append(recordKey(n, 26), id[:]...)
}
func isGraphRetentionWire(b []byte) bool {
	return len(b) >= 4 && bytes.Equal(b[:4], []byte{'G', 'G', 'C', 1})
}
func encodeGraphRetentionCommand(c graphRetentionCommand) ([]byte, error) {
	if !c.scope.valid() || !collectedGraphAgreement(c.scope.semantic) || c.issuer == ([16]byte{}) || c.request == ([16]byte{}) || c.configuration == ([32]byte{}) {
		return nil, errInvalid
	}
	for _, floor := range []uint64{c.floors.root, c.floors.data, c.floors.feed, c.floors.outcome, c.floors.recovery, c.floors.certificate} {
		if floor >= math.MaxUint64-1 {
			return nil, errInvalid
		}
	}
	return boundedEncoding(graphRetentionCommandBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'G', 'C', 1})
		w.add(appendAllocationScope(nil, c.scope))
		w.add(c.issuer[:])
		w.add(c.request[:])
		w.add(c.configuration[:])
		w.add(c.previous[:])
		for _, floor := range []uint64{c.floors.root, c.floors.data, c.floors.feed, c.floors.outcome, c.floors.recovery, c.floors.certificate} {
			w.u64(floor)
		}
	})
}
func decodeGraphRetentionCommand(wire []byte) (graphRetentionCommand, error) {
	body, err := graphBody(wire, "GGC\x01", graphRetentionCommandBytes)
	if err != nil {
		return graphRetentionCommand{}, err
	}
	d := graphCursor{b: body}
	scope := d.take(allocationScopeBytes - 36)
	c := graphRetentionCommand{issuer: d.array(), request: d.array()}
	copy(c.configuration[:], d.take(32))
	copy(c.previous[:], d.take(32))
	c.floors = graphRetentionFloors{d.u64(), d.u64(), d.u64(), d.u64(), d.u64(), d.u64()}
	if d.err != nil || len(d.b) != 0 {
		return graphRetentionCommand{}, errors.Join(errCorrupt, d.err)
	}
	c.scope = readAllocationScope(&decoder{b: scope})
	canonical, err := encodeGraphRetentionCommand(c)
	if err != nil || !bytes.Equal(canonical, wire) {
		return graphRetentionCommand{}, errors.Join(errCorrupt, err)
	}
	return c, nil
}
func encodeGraphRetentionResult(r graphRetentionResult) ([]byte, error) {
	if r.id == ([32]byte{}) || r.sourceHash == ([32]byte{}) || r.index == 0 || r.source == 0 || r.source >= r.index || r.status < graphRetentionApplied || r.status > graphRetentionBlocked {
		return nil, errInvalid
	}
	return boundedEncoding(256, func(w *boundedWriter) {
		w.add([]byte{'G', 'G', 'O', 1})
		w.tag(r.status)
		w.add(r.id[:])
		w.add(r.sourceHash[:])
		w.u64(r.index)
		w.u64(r.source)
		w.u64(r.round)
	})
}
func decodeGraphRetentionResult(wire []byte) (graphRetentionResult, error) {
	body, err := graphBody(wire, "GGO\x01", 256)
	if err != nil {
		return graphRetentionResult{}, err
	}
	d := graphCursor{b: body}
	r := graphRetentionResult{status: d.tag()}
	copy(r.id[:], d.take(32))
	copy(r.sourceHash[:], d.take(32))
	r.index, r.source, r.round = d.u64(), d.u64(), d.u64()
	if d.err != nil || len(d.b) != 0 {
		return graphRetentionResult{}, errors.Join(errCorrupt, d.err)
	}
	canonical, err := encodeGraphRetentionResult(r)
	if err != nil || !bytes.Equal(canonical, wire) {
		return graphRetentionResult{}, errors.Join(errCorrupt, err)
	}
	return r, nil
}
func graphRetentionRecord(q *reader, key []byte) (graphRetentionCommand, graphRetentionResult, []byte, bool, error) {
	row, found, err := crossControl(q, key, 1024)
	if err != nil || !found {
		return graphRetentionCommand{}, graphRetentionResult{}, nil, found, err
	}
	if err := reserveComposer(q, 2048+8*len(row.Value)); err != nil {
		return graphRetentionCommand{}, graphRetentionResult{}, nil, false, err
	}
	body, err := graphBody(row.Value, "GGX\x01", 1024)
	if err != nil {
		return graphRetentionCommand{}, graphRetentionResult{}, nil, false, err
	}
	d := graphCursor{b: body}
	wire, result := d.field(graphRetentionCommandBytes), d.field(256)
	if d.err != nil || len(d.b) != 0 {
		return graphRetentionCommand{}, graphRetentionResult{}, nil, false, errors.Join(errCorrupt, d.err)
	}
	c, err := decodeGraphRetentionCommand(wire)
	if err != nil {
		return c, graphRetentionResult{}, nil, false, err
	}
	r, err := decodeGraphRetentionResult(result)
	if err != nil || r.index != row.Index || r.index > q.base.Index || r.id != sha256.Sum256(wire) {
		return c, r, nil, false, errors.Join(errCorrupt, err)
	}
	return c, r, wire, true, nil
}
func graphRetentionOriginal(q *reader, c graphRetentionCommand, wire []byte) (graphRetentionResult, bool, error) {
	actual, result, original, found, err := graphRetentionRecord(q, graphRetentionOriginalKey(q.ns, c.issuer, c.request))
	if err != nil || !found {
		return result, found, err
	}
	if actual != c || !bytes.Equal(original, wire) {
		return graphRetentionResult{}, false, errInvalid
	}
	return result, true, nil
}

// The native helper owns the captured plan, including prepared-bank lifetime.
// Its offered grant is capped by THIS reader's source/output remainder before
// allocation; actual ownership is charged back without a fresh/default ledger.
func capturedGraphRetention(q *reader) (raftlog.ApplicationRetentionState, error) {
	if q == nil || q.view == nil || q.ctx == nil || q.arena == nil {
		return raftlog.ApplicationRetentionState{}, errInvalid
	}
	rows := min(1, q.limits.readRows-q.rows)
	n := min(q.view.ReadLimits().Bytes, q.limits.readBytes-q.bytes, q.arena.Remaining())
	if rows < 1 || n < 512 {
		return raftlog.ApplicationRetentionState{}, errLimit
	}
	plan, enabled, work, err := q.view.RetentionPlan(q.ctx, raftlog.ReadBudget{Rows: rows, Bytes: n})
	q.rows += work.Records
	q.bytes += work.Bytes
	charge := reserveComposer(q, work.Bytes)
	if err != nil || charge != nil {
		return raftlog.ApplicationRetentionState{}, errors.Join(err, charge)
	}
	if !enabled {
		return raftlog.ApplicationRetentionState{}, errInvalid
	}
	return plan, nil
}
func activeGraphRetention(q *reader) (raftlog.ApplicationRetentionState, graphRetentionCommand, graphRetentionResult, error) {
	plan, err := capturedGraphRetention(q)
	if err != nil || plan.ID == ([32]byte{}) {
		return plan, graphRetentionCommand{}, graphRetentionResult{}, err
	}
	c, r, _, found, err := graphRetentionRecord(q, graphRetentionPlanKey(q.ns, plan.ID))
	if err != nil {
		return plan, c, r, err
	}
	f := c.floors
	if !found || c.scope.partition != q.ns.partition || c.scope.graph != q.ns.graph || r.status != graphRetentionApplied || r.id != plan.ID || r.index != plan.InstalledIndex || r.source != plan.SourceIndex || r.sourceHash != plan.SourceHash || c.previous != plan.PreviousID || f.root != plan.RootFloor || f.data != plan.DataFloor || f.feed != plan.FeedFloor || f.outcome != plan.OutcomeFloor || f.recovery != plan.RecoveryFloor {
		return plan, c, r, errors.Join(errCorrupt, err)
	}
	if err := chargeGraphWork(q, graphstore.PageWork{Records: 1, Bytes: 256}); err != nil {
		return plan, c, r, err
	}
	binding, err := q.view.ApplicationBinding(q.ctx)
	if err != nil {
		return plan, c, r, err
	}
	if binding.Identity.Graph != [16]byte(c.scope.graph) || binding.Identity.Partition != c.scope.partition || binding.Identity.Group != c.scope.group || binding.SemanticContractID != c.scope.semantic {
		return plan, c, r, errors.Join(errCorrupt, err)
	}
	// The retained original's source round is authenticated by its immutable
	// PLM, not inferred from a later root or supplied result/hash alone.
	wire, present, err := q.getBounded(partitionRoundMapKey(q.ns, r.round), 256)
	if err != nil {
		return plan, c, r, err
	}
	if !present {
		return plan, c, r, errors.Join(errCorrupt, err)
	}
	body, err := graphBody(wire, "PLM\x01", 256)
	if err != nil || len(body) != 220 || !bytes.Equal(body[:32], c.configuration[:]) || binary.BigEndian.Uint64(body[32:40]) != r.round || binary.BigEndian.Uint64(body[40:48]) == 0 || binary.BigEndian.Uint64(body[40:48]) > r.source || sha256.Sum256(body[48:]) != plan.SourceHash {
		return plan, c, r, errors.Join(errCorrupt, err)
	}
	installed, err := readPartitionRoundFloor(q, c.configuration)
	if err != nil {
		return plan, c, r, err
	}
	if installed < r.round {
		return plan, c, r, errors.Join(errCorrupt, err)
	}
	return plan, c, r, nil
}

func graphSemanticAnchor(q *reader, index uint64, maxBytes int) (raftlog.ApplicationRoot, error) {
	plan, _, _, err := activeGraphRetention(q)
	if err != nil {
		return raftlog.ApplicationRoot{}, err
	}
	if plan.ID == ([32]byte{}) {
		return q.view.RootAt(q.ctx, index, maxBytes)
	}
	if err := chargeGraphWork(q, graphstore.PageWork{Records: 1, Bytes: 1024}); err != nil {
		return raftlog.ApplicationRoot{}, err
	}
	if err := reserveComposer(q, 1024); err != nil {
		return raftlog.ApplicationRoot{}, err
	}
	return q.view.RetentionAnchor(q.ctx, plan.ID, plan.InstalledIndex, index, maxBytes)
}

// Only the actual CURRENT installed PLM may select a new native source index.
// An original PLM/certificate remains immutable; a later business round uses its
// genuine newer original location, and older cuts never borrow this anchor.
func retainedGraphRoundSource(q *reader, configuration [32]byte, round, index uint64, image [172]byte) (uint64, raftlog.ApplicationRoot, error) {
	plan, c, result, err := activeGraphRetention(q)
	if err != nil {
		return 0, raftlog.ApplicationRoot{}, err
	}
	if plan.ID != ([32]byte{}) && c.configuration == configuration && result.round == round && plan.SourceHash == sha256.Sum256(image[:]) {
		installed, err := readPartitionRoundFloor(q, configuration)
		if err != nil {
			return 0, raftlog.ApplicationRoot{}, err
		}
		if installed == round {
			root, err := q.view.RootAt(q.ctx, plan.InstalledIndex, 172)
			if err != nil {
				return 0, root, err
			}
			if root.ImageHash != plan.SourceHash || !bytes.Equal(root.Image, image[:]) {
				return 0, root, errors.Join(errCorrupt, err)
			}
			return plan.InstalledIndex, root, nil
		}
	}
	root, err := q.view.RootAt(q.ctx, index, 172)
	if errors.Is(err, raftlog.ErrApplicationExpired) {
		return 0, root, errors.Join(errGraphHistoryExpired, err)
	}
	return index, root, err
}

// The caller first authenticated the complete immutable GSC row. Caller-provided
// source indexes alone must never classify an unknown certificate as expired.
func checkGraphCertificateRetention(q *reader, certificate graphCertificate) error {
	plan, c, _, err := activeGraphRetention(q)
	if err != nil {
		return err
	}
	if plan.ID != ([32]byte{}) && (certificate.index < c.floors.certificate || certificate.sourceIndex < max(plan.RootFloor, plan.DataFloor)) {
		return errors.Join(errGraphHistoryExpired, raftlog.ErrApplicationExpired)
	}
	return nil
}

// All actual retained owners protect their source even after policy time passes:
// only their durably installed terminal command removes ownership/bank backing.
func (m *declaredMaterializer) graphRetentionCeiling(q *reader, configuration [32]byte) (graphRetentionFloors, error) {
	cap := graphRetentionFloors{q.base.Index, q.base.Index, q.base.Index + 1, q.base.Index, q.base.Index, q.base.Index}
	if err := m.checkRetainedPinRoster(q, true); err != nil {
		return cap, err
	}
	if active, err := readRangeMoveActive(q); err != nil {
		return cap, err
	} else if active != ([16]byte{}) {
		return cap, errGraphRetentionPending
	}
	if lock, err := crossLock(q); err != nil {
		return cap, err
	} else if lock != ([16]byte{}) {
		return cap, errGraphRetentionPending
	}
	if _, found, err := m.readCrossResourceOwner(q); err != nil {
		return cap, err
	} else if found {
		return cap, errGraphRetentionPending
	}
	rows, n := min(1, q.limits.readRows-q.rows), min(q.view.ReadLimits().Bytes, q.limits.readBytes-q.bytes, q.arena.Remaining())
	if rows < 1 || n < 1024 {
		return cap, errLimit
	}
	owner, work, err := q.view.RecoveryOwner(q.ctx, raftlog.ReadBudget{Rows: rows, Bytes: n})
	q.rows += work.Records
	q.bytes += work.Bytes
	charge := reserveComposer(q, work.Bytes)
	if err != nil || charge != nil {
		return cap, errors.Join(err, charge)
	}
	if owner != (raftlog.ApplicationRecoveryOwner{}) {
		return cap, errGraphRetentionPending
	}
	head, err := readPinHead(q)
	if err != nil {
		return cap, err
	}
	for _, id := range head.owners {
		owner, found, err := readPinOwner(q, id)
		if err != nil {
			return cap, err
		}
		if !found || owner.terminal != 0 {
			return cap, errors.Join(errCorrupt, err)
		}
		original, _, found, err := readPinOriginal(q, id)
		if err != nil {
			return cap, err
		}
		if !found {
			return cap, errors.Join(errCorrupt, err)
		}
		part, found := pinPart(original, m.ns.partition)
		if !found || part.certificate.configuration != configuration {
			return cap, errCorrupt
		}
		source := part.certificate.sourceIndex
		cap.root = min(cap.root, source)
		cap.data = min(cap.data, source)
		cap.feed = min(cap.feed, owner.feedStart)
		cap.outcome = min(cap.outcome, source)
		if m.SemanticContractID() == durableColumnGraphSemanticContractID() {
			cap.outcome = min(cap.outcome, owner.feedStart)
		}
		cap.recovery = min(cap.recovery, source)
		cap.certificate = min(cap.certificate, part.certificate.index)
	}
	return cap, nil
}
func graphRetentionWithin(f, cap graphRetentionFloors) bool {
	return f.root <= cap.root && f.data <= cap.data && f.feed <= cap.feed && f.outcome <= cap.outcome && f.recovery <= cap.recovery && f.certificate <= cap.certificate
}
func graphRetentionMonotone(f graphRetentionFloors, p raftlog.ApplicationRetentionState, old graphRetentionCommand) bool {
	return f.root >= p.RootFloor && f.data >= p.DataFloor && f.feed >= p.FeedFloor && f.outcome >= p.OutcomeFloor && f.recovery >= p.RecoveryFloor && f.certificate >= old.floors.certificate
}
func (m *declaredMaterializer) graphRetentionCheck(q *reader, c graphRetentionCommand) (raftlog.ApplicationRetentionState, uint64, error) {
	if !m.collectsHistory() || c.scope != m.scope() {
		return raftlog.ApplicationRetentionState{}, 0, errInvalid
	}
	cfg, err := m.initialized(q)
	if err != nil {
		return raftlog.ApplicationRetentionState{}, 0, err
	}
	configuration, err := cfg.digest()
	if err != nil {
		return raftlog.ApplicationRetentionState{}, 0, err
	}
	plan, old, _, err := activeGraphRetention(q)
	if err != nil {
		return plan, 0, err
	}
	if configuration != c.configuration || c.previous != plan.ID || !graphRetentionMonotone(c.floors, plan, old) {
		return plan, 0, errInvalid
	}
	ceiling, err := m.graphRetentionCeiling(q, configuration)
	if err != nil {
		return plan, 0, err
	}
	if !graphRetentionWithin(c.floors, ceiling) {
		return plan, 0, errGraphRetentionPending
	}
	installed, err := readPartitionRoundFloor(q, configuration)
	return plan, installed, err
}
func graphRetentionReader(h *allocationHost, ctx context.Context) (*reader, error) {
	if h == nil || ctx == nil || h.machine == nil || h.driver == nil || !h.machine.collectsHistory() {
		return nil, errInvalid
	}
	v, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		return nil, err
	}
	base, err := v.RootBounded(ctx, 172)
	if err != nil {
		return nil, errors.Join(err, v.Close())
	}
	l := h.machine.limits.allocation
	l.readRows = min(l.readRows, h.machine.limits.sourceRows)
	l.readBytes = min(l.readBytes, h.machine.limits.sourceBytes)
	q := &reader{ctx: ctx, view: v, ns: h.machine.ns, base: base, limits: l, bytes: cap(base.Image)}
	if err := joinGraphOutputBudget(q, graphRequest{}, 4096, h.machine.limits); err != nil {
		return nil, errors.Join(err, v.Close())
	}
	return q, nil
}
func (h *allocationHost) PrepareGraphRetention(ctx context.Context, id requestID, floors graphRetentionFloors) (proposal allocationProposal, err error) {
	if h == nil || ctx == nil || id == (requestID{}) {
		return proposal, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return proposal, replica.ErrStopped
	}
	q, err := graphRetentionReader(h, ctx)
	if err != nil {
		return proposal, err
	}
	defer func() { err = errors.Join(err, q.view.Close()) }()
	cfg, err := h.machine.initialized(q)
	if err != nil {
		return proposal, err
	}
	configuration, err := cfg.digest()
	if err != nil {
		return proposal, err
	}
	plan, _, _, err := activeGraphRetention(q)
	if err != nil {
		return proposal, err
	}
	c := graphRetentionCommand{scope: h.machine.scope(), issuer: h.session, request: [16]byte(id), configuration: configuration, previous: plan.ID, floors: floors}
	wire, err := encodeGraphRetentionCommand(c)
	if err != nil {
		return proposal, err
	}
	if _, _, err := h.machine.graphRetentionCheck(q, c); err != nil {
		return proposal, err
	}
	return allocationProposal{target: c.scope, wire: wire}, nil
}
func (h *allocationHost) preflightGraphRetention(c graphRetentionCommand, wire []byte) (err error) {
	q, err := graphRetentionReader(h, context.Background())
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, q.view.Close()) }()
	if _, found, err := graphRetentionOriginal(q, c, wire); err != nil || found {
		return err
	}
	_, _, err = h.machine.graphRetentionCheck(q, c)
	return err
}

func (m *declaredMaterializer) stageGraphRetention(q *reader, entry replica.Entry, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	if !m.collectsHistory() {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	if err := joinGraphOutputBudget(q, graphRequest{}, 4096+8*len(entry.Data), m.limits); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	c, err := decodeGraphRetentionCommand(entry.Data)
	if err != nil || c.scope != m.scope() {
		return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
	}
	if original, found, err := graphRetentionOriginal(q, c, entry.Data); err != nil {
		return raftlog.ApplicationBatch{}, err
	} else if found {
		out, err := encodeGraphRetentionResult(original)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		b := batchAt(q.base)
		b.Outcome = out
		return b, m.preflight(b, budget)
	}
	result := graphRetentionResult{id: sha256.Sum256(entry.Data), index: entry.Index, source: q.base.Index, sourceHash: q.base.ImageHash, status: graphRetentionApplied}
	_, round, err := m.graphRetentionCheck(q, c)
	result.round = round
	if err != nil {
		if errors.Is(err, errGraphRetentionPending) {
			result.status = graphRetentionBlocked
		} else if errors.Is(err, errInvalid) {
			result.status = graphRetentionStale
		} else {
			return raftlog.ApplicationBatch{}, err
		}
		return m.finishGraphRetention(q, c, entry.Data, result, nil, budget)
	}
	// Semantic genesis is authenticated from the actual immutable PLM0, never an
	// arbitrary caller hash. Image-only protection does not bless an old data cut.
	wire, found, err := q.getBounded(partitionRoundMapKey(m.ns, 0), 256)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if !found {
		return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, err)
	}
	body, err := graphBody(wire, "PLM\x01", 256)
	if err != nil || len(body) != 220 || !bytes.Equal(body[:32], c.configuration[:]) || binary.BigEndian.Uint64(body[32:40]) != 0 {
		return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, err)
	}
	genesis := binary.BigEndian.Uint64(body[40:48])
	if genesis == 0 || genesis > q.base.Index {
		return raftlog.ApplicationBatch{}, errCorrupt
	}
	if err := chargeGraphWork(q, graphstore.PageWork{Records: 1, Bytes: 128}); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if err := reserveComposer(q, 128); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	publicationID, _, err := m.store.RetentionPublication()
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	f := c.floors
	next := raftlog.ApplicationRetentionState{ID: result.id, PreviousID: c.previous, SourceIndex: q.base.Index, InstalledIndex: entry.Index, SourceHash: q.base.ImageHash, RetiredCutID: publicationID, RootFloor: f.root, DataFloor: f.data, FeedFloor: f.feed, OutcomeFloor: f.outcome, RecoveryFloor: f.recovery, Roots: []raftlog.ApplicationRetentionRoot{{Index: genesis, ImageHash: sha256.Sum256(body[48:])}}}
	rows, n := min(q.view.ReadLimits().Rows, q.limits.readRows-q.rows), min(q.view.ReadLimits().Bytes, q.limits.readBytes-q.bytes, q.arena.Remaining())
	if rows < 1 || n < 1024 {
		return raftlog.ApplicationBatch{}, errLimit
	}
	admission, work, err := q.view.PrepareApplicationRetention(q.ctx, next, entry.Data, raftlog.ReadBudget{Rows: rows, Bytes: n})
	q.rows += work.Records
	q.bytes += work.Bytes
	if charge := reserveComposer(q, work.Bytes); err != nil || charge != nil {
		return raftlog.ApplicationBatch{}, errors.Join(err, charge)
	}
	return m.finishGraphRetention(q, c, entry.Data, result, admission, budget)
}

// Every durably installed refusal is an immutable first result too. Clearing a
// blocker cannot turn that exact original request/body into an applied plan;
// the caller must use a new request identity. Local preflight failures and IO/
// authority errors that were not installed do not fabricate an original result.
func (m *declaredMaterializer) finishGraphRetention(q *reader, c graphRetentionCommand, command []byte, result graphRetentionResult, admission *raftlog.ApplicationRetentionAdmission, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	if (result.status == graphRetentionApplied) != (admission != nil) {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	if err := reserveComposer(q, 4096+8*(len(command)+256)); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	out, err := encodeGraphRetentionResult(result)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	record, err := boundedEncoding(1024, func(w *boundedWriter) { w.add([]byte{'G', 'G', 'X', 1}); w.field(command); w.field(out) })
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	b := batchAt(q.base)
	b.RetentionAdmission = admission
	b.Outcome = out
	b.ControlPuts = []raftlog.ApplicationControlPut{{Key: graphRetentionOriginalKey(m.ns, c.issuer, c.request), Value: record}}
	if admission != nil {
		b.ControlPuts = append(b.ControlPuts, raftlog.ApplicationControlPut{Key: graphRetentionPlanKey(m.ns, result.id), Value: owned(record)})
	}
	return b, m.preflight(b, budget)
}

// GraphRetentionOriginal authenticates an immutable installed result from the
// actual active native prefix. It requires neither new quorum/log credit nor a
// live former issuer. Completed local absence is not global absence or abort.
func (h *allocationHost) GraphRetentionOriginal(ctx context.Context, proposal allocationProposal) (result graphRetentionResult, found bool, err error) {
	if h == nil || ctx == nil {
		return result, false, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return result, false, replica.ErrStopped
	}
	c, err := decodeGraphRetentionCommand(proposal.wire)
	if err != nil || h.machine == nil || proposal.target != h.machine.scope() || c.scope != proposal.target {
		return result, false, errors.Join(errInvalid, err)
	}
	q, err := graphRetentionReader(h, ctx)
	if err != nil {
		return result, false, err
	}
	defer func() {
		err = errors.Join(err, q.view.Close())
		if err != nil {
			result, found = graphRetentionResult{}, false
		}
	}()
	return graphRetentionOriginal(q, c, proposal.wire)
}
