package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// materializer is an internal singleton ApplicationMachine, not a Host or a
// transaction door based on prior client reads. Operations plan at the current
// serialized applied root. PLAN5.2 footprint validation for historical client
// transactions and distributed prepare/decision remain separate obligations.
// Stage never installs; errors after Raft commit stop the driver, not abort it.
// Distributed application traffic requires separately agreed semantic command
// policy plus the complete replicated admission/ownership protocol; this private
// materializer does not enable it or certify V0-V7 acceptance.
type materializer struct {
	store  *raftlog.Store
	ns     namespace
	owner  uint64
	limits materializerLimits
}

var _ replica.ApplicationMachine = (*materializer)(nil)

func newMaterializer(s *raftlog.Store, n namespace, owner uint64, l materializerLimits) (*materializer, error) {
	if s == nil || !n.valid() || owner == 0 {
		return nil, errInvalid
	}
	if err := l.validate(); err != nil {
		return nil, err
	}
	if !s.ApplicationLimits().Enabled() {
		return nil, errInvalid
	}
	m := &materializer{s, n, owner, l}
	index, image, err := s.Checkpoint()
	if err != nil {
		return nil, err
	}
	if err := m.Restore(index, image); err != nil {
		return nil, err
	}
	return m, nil
}
func (m *materializer) catalog(v *raftlog.ApplicationView) (*graphstore.Catalog, graphstore.Root, error) {
	c, err := graphstore.OpenCatalog(v, graphstore.Namespace{Graph: graphstate.GraphID(m.ns.graph), Partition: m.ns.partition}, m.owner, m.limits.catalog)
	if err != nil {
		return nil, graphstore.Root{}, err
	}
	r, err := c.Root()
	if err != nil {
		return nil, graphstore.Root{}, err
	}
	t, err := r.SinglePartition()
	if err != nil {
		return nil, graphstore.Root{}, err
	}
	if t.TopologyEpoch != 1 || t.SchemaVersion != 1 {
		return nil, graphstore.Root{}, graphstore.ErrTopologyUnsupported
	}
	return c, r, nil
}

// IndexVersion is a physical format, never a Full coverage claim. Zero only
// classifies the bootstrap door; supported formats belong to graphstore.
func isBootstrapRoot(r graphstore.Root) bool {
	t, err := r.SinglePartition()
	return err == nil && t.IndexVersion == 0
}
func (m *materializer) Restore(index uint64, image []byte) (err error) {
	if m == nil || m.store == nil || index == 0 {
		return errInvalid
	}
	v, err := m.store.ApplicationView(index)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, v.Close()) }()
	base, err := v.Root()
	if err != nil {
		return err
	}
	if !bytes.Equal(image, base.Image) {
		return errInvalid
	}
	c, r, err := m.catalog(v)
	if err != nil {
		return err
	}
	l := m.limits.allocation
	l.readRows = min(l.readRows, m.limits.sourceRows)
	l.readBytes = min(l.readBytes, m.limits.sourceBytes)
	q := reader{ctx: context.Background(), view: v, ns: m.ns, base: base, limits: l, bytes: cap(base.Image)}
	if q.bytes > l.readBytes {
		return errLimit
	}
	if isBootstrapRoot(r) {
		seed, err := graphstore.NewRoot(r.Namespace(), m.owner)
		if err != nil {
			return err
		}
		if r.SemanticEpoch() != 0 || r.NextPhysicalID() != 1 || r.EffectDigest() != seed.EffectDigest() {
			return errCorrupt
		}
		return q.emptyProof()
	}
	return m.checkInitialized(&q, c, r)
}

// Guard metadata includes fixed decoded command/reader state, empty cursor maps
// and descriptor/fingerprint scratch. Four image copies are charged separately.
// Variable tree-root decode backing is bounded by the source ledger within the
// remaining output headroom. This is conservative representation, not heap/RSS.
const readinessMetadataBytes = 4096

func (m *materializer) checkInitialized(q *reader, c *graphstore.Catalog, root graphstore.Root) error {
	retained := graphResultMetadataBytes + readinessMetadataBytes + 4*cap(q.base.Image)
	if retained >= m.limits.outputBytes {
		return errLimit
	}
	l, err := m.graphBudget(q)
	if err != nil {
		return err
	}
	// Source/decode accounting includes temporary variable root pages as well as
	// conservative fixed reader costs. Counting it additionally is deliberate;
	// it prevents a large physical root from evading the outer retention cap.
	l.MaxSourceBytes = min(l.MaxSourceBytes, m.limits.outputBytes-retained)
	v, err := graphstore.OpenReadView(q.ctx, c, l)
	if err != nil {
		return err
	}
	work := v.Work()
	if err := v.Close(); err != nil {
		return err
	}
	if work.Records > q.limits.readRows-q.rows || work.Bytes > q.limits.readBytes-q.bytes {
		return errLimit
	}
	q.rows += work.Records
	q.bytes += work.Bytes
	// Full is checked BEFORE logical co-initialization. No physical version is
	// interpreted as a ready graph, and no control outcome masks corrupt state.
	if root.SemanticEpoch() == 0 {
		return errCorrupt
	}
	if _, found, err := q.allocator(); err != nil {
		return err
	} else if !found {
		return errCorrupt
	}
	return nil
}
func sameBase(a, b raftlog.ApplicationRoot) bool {
	return a.Generation == b.Generation && a.Index == b.Index && a.ImageHash == b.ImageHash && bytes.Equal(a.Image, b.Image)
}
func materializerOutputCost(b raftlog.ApplicationBatch) int {
	n := graphResultMetadataBytes + cap(b.Image) + cap(b.Changes) + cap(b.Outcome) + 64*cap(b.Writes)
	for _, w := range b.Writes {
		n += cap(w.Key) + cap(w.Value)
	}
	return n
}
func (m *materializer) preflight(b raftlog.ApplicationBatch, budget raftlog.ApplicationBudget) error {
	stageBytes := 128
	for _, w := range b.Writes {
		stageBytes += 2*len(w.Key) + len(w.Value) + 64
	}
	if len(b.Writes) > m.limits.stageRows || stageBytes > m.limits.stageBytes || materializerOutputCost(b) > m.limits.outputBytes || len(b.Writes) > budget.Writes || len(b.Image) > budget.ImageBytes || len(b.Changes) > budget.ChangeBytes || len(b.Outcome) > budget.OutcomeBytes {
		return errLimit
	}
	policy := m.store.ApplicationLimits()
	policy.MaxInstallWrites = min(policy.MaxInstallWrites, budget.Writes)
	policy.MaxInstallBytes = min(policy.MaxInstallBytes, budget.Bytes)
	policy.MaxImageBytes = min(policy.MaxImageBytes, budget.ImageBytes)
	policy.MaxChangeBytes = min(policy.MaxChangeBytes, budget.ChangeBytes)
	policy.MaxOutcomeBytes = min(policy.MaxOutcomeBytes, budget.OutcomeBytes)
	// Pure framing is not reservation. The serialized driver's admission and
	// synchronous InstallApplication remain the actual authority and failure point.
	if _, err := policy.Preflight(b, m.store.Limits()); err != nil {
		if errors.Is(err, raftlog.ErrLimit) {
			return errors.Join(errLimit, err)
		}
		return err
	}
	return nil
}
func batchAt(base raftlog.ApplicationRoot) raftlog.ApplicationBatch {
	return raftlog.ApplicationBatch{BaseGeneration: base.Generation, BaseIndex: base.Index, BaseImageHash: base.ImageHash, Image: base.Image}
}
func sharedReplay(q *reader, r request, hash [32]byte, index uint64) (outcome, bool, error) {
	b, found, err := q.get(outcomeKey(r))
	if err != nil || !found {
		return outcome{}, false, err
	}
	old, err := decodeAnyOutcome(b, q.ns)
	if err != nil {
		return outcome{}, false, err
	}
	if old.identity != r.identity() || old.index > q.base.Index || old.disposition == requestReplay {
		return outcome{}, false, errCorrupt
	}
	// Payload mismatch precedes current command-kind/state checks, across ARQ1
	// and GRQ2. The immutable original mapping is never overwritten.
	if old.hash != hash {
		return outcome{ns: q.ns, kind: r.kind, identity: r.identity(), hash: hash, index: index, disposition: applied, reason: reasonMismatch}, true, nil
	}
	if old.kind != r.kind {
		return outcome{}, false, errCorrupt
	}
	old.disposition = requestReplay
	return old, true, nil
}
func commandIdentity(b []byte) (request, bool, error) {
	if len(b) < requestHeaderBytes || len(b) > 4<<20 {
		return request{}, false, errCorrupt
	}
	graph := bytes.Equal(b[:4], []byte{'G', 'R', 'Q', 2})
	control := bytes.Equal(b[:4], []byte{'A', 'R', 'Q', 1})
	kind := commandKind(b[4])
	if !graph && !control || graph && kind != initGraph && kind != graphOperations || control && requestSize(kind) == 0 {
		return request{}, false, errCorrupt
	}
	c := graphCursor{b: b[5:]}
	r := request{ns: namespace{graph: c.array(), partition: c.u64()}, kind: kind}
	id := c.array()
	if kind == initGraph || kind == initAllocator {
		r.attempt = bootstrapAttemptID(id)
	} else {
		r.id = requestID(id)
	}
	if c.err != nil || !r.ns.valid() || id == ([16]byte{}) {
		return request{}, false, errCorrupt
	}
	return r, graph, nil
}

func businessGraphReason(err error) (reason, bool) {
	if errors.Is(err, graphstore.ErrCorrupt) || errors.Is(err, graphstore.ErrPoisoned) || errors.Is(err, graphstore.ErrClosed) || errors.Is(err, raftlog.ErrClosed) || errors.Is(err, raftlog.ErrCorrupt) || errors.Is(err, raftlog.ErrPoisoned) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return reasonNone, false
	}
	// Storage/Plan/source/output/staging limits depend on local physical layout
	// and policy. They are operational, never durable replicated rejections.
	if errors.Is(err, graphstate.ErrResourceLimit) || errors.Is(err, graphstore.ErrResourceLimit) || errors.Is(err, temporal.ErrResourceLimit) || errors.Is(err, state.ErrResourceLimit) || errors.Is(err, errLimit) || errors.Is(err, raftlog.ErrLimit) {
		return reasonNone, false
	}
	for _, expected := range []error{graphstate.ErrInvalidInput, graphstate.ErrValidityRequired, graphstate.ErrEmptyMutation, graphstate.ErrUnsupported, graphstate.ErrNotFound, graphstate.ErrAlreadyExists, graphstate.ErrOwnerValidity, graphstate.ErrLifecycleOverlap, graphstate.ErrSchemaMismatch, graphstate.ErrTypeMismatch, graphstate.ErrUniqueOverlap} {
		if errors.Is(err, expected) {
			return reasonInvalid, true
		}
	}
	return reasonNone, false
}

// Simultaneous composition includes returned graph effects and newly owned
// outer buffers. Graph KV payloads are shared with the merged batch and charged
// once; graph and merged header arrays are both charged. Fixed scratch/typed
// metadata is independently tested; no allocator/RSS claim is made.
const compositionMetadataBytes = 2048

func retainedCompositionBytes(q *reader, g *graphstore.GraphEffects, changes graphChanges) int {
	total := compositionMetadataBytes + cap(q.base.Image) + 64*cap(q.writes) + 64*cap(changes.schemas)
	for _, w := range q.writes {
		total += cap(w.Key) + cap(w.Value)
	}
	for _, d := range changes.schemas {
		total += len(d.Name)
	}
	if g != nil {
		total += g.OwnedBytes
	}
	return total
}
func reserveComposition(total *int, n, limit int) error {
	if n < 0 || n > limit-*total {
		return errLimit
	}
	*total += n
	return nil
}
func (m *materializer) finish(q *reader, o outcome, hash [32]byte, mapRecord bool, budget raftlog.ApplicationBudget, graph *graphstore.GraphEffects, changes graphChanges) (raftlog.ApplicationBatch, error) {
	// Drop incidental physical effects before composing a logical no-op.
	if !changes.nonempty() {
		graph = nil
	}
	retained := 0
	if graph != nil {
		if !sameBase(q.base, graph.Base) {
			return raftlog.ApplicationBatch{}, errInvalid
		}
		if graph.OwnedBytes <= 0 {
			return raftlog.ApplicationBatch{}, errCorrupt
		}
		retained = retainedCompositionBytes(q, graph, changes)
		size := outcomeBytes
		if o.kind == initGraph || o.kind == graphOperations {
			size = graphOutcomeBytes
		}
		// Encoding owns private body+sealed copy. Mapping additionally owns its
		// exact key/value and a conservative replacement touched-array allowance.
		extra := 2 * size
		if mapRecord {
			keyBytes := 43
			extra += 2*keyBytes + size + 128*(len(q.writes)+1)
		}
		if err := reserveComposition(&retained, extra, m.limits.outputBytes); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
	}
	e, err := collectEffects(q, o, hash, mapRecord)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	b := batchAt(q.base)
	b.Writes, b.Outcome, b.Changes = e.writes, e.envelope, e.changes
	if graph != nil {
		// The axis-table backing is bounded before its count-sized allocation.
		axisSlots := min(m.limits.maxAxes, len(changes.entities)+len(changes.values)+len(changes.groups))
		if err := reserveComposition(&retained, 256*axisSlots, m.limits.outputBytes); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		available := m.limits.outputBytes - retained
		if available < 4*36 {
			return raftlog.ApplicationBatch{}, errLimit
		}
		encodingLimits := m.limits
		encodingLimits.changeBytes = min(encodingLimits.changeBytes, available/4)
		logical, err := encodeGraphChanges(changes, encodingLimits)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		// Four copies cover returned logical bytes plus bounded scope/CC codec
		// staging during the sizing/final pass. Exact variable magnitudes are bounded
		// independently by temporal/state limits before their decoders allocate.
		if err := reserveComposition(&retained, 4*cap(logical), m.limits.outputBytes); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if err := reserveComposition(&retained, 2*(len(logical)+97)+4*140, m.limits.outputBytes); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		digest := graphEffectDigest(graph.Root.EffectDigest(), logical)
		root, err := graph.Root.AdvanceEffects(digest)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		b.Image, err = graphstore.EncodeRoot(root)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		b.Changes, err = encodeChangeEnvelope(e.outcome, logical, m.limits)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if len(graph.Writes) > m.limits.stageRows-len(b.Writes) {
			return raftlog.ApplicationBatch{}, errLimit
		}
		count := len(b.Writes) + len(graph.Writes)
		if err := reserveComposition(&retained, 64*count, m.limits.outputBytes); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		writes := make([]raftlog.KV, count)
		copy(writes, b.Writes)
		copy(writes[len(b.Writes):], graph.Writes)
		slices.SortFunc(writes, func(a, b raftlog.KV) int { return bytes.Compare(a.Key, b.Key) })
		for i := 1; i < len(writes); i++ {
			if bytes.Equal(writes[i-1].Key, writes[i].Key) {
				return raftlog.ApplicationBatch{}, errCorrupt
			}
		}
		b.Writes = writes
	}

	// No-op/rejection/replay discard ALL graph effects, including incidental
	// descriptor writes/physical reservations from the otherwise checked stager.
	if err := m.preflight(b, budget); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	return b, nil
}
func (m *materializer) reject(q *reader, o outcome, hash [32]byte, mapRecord bool, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	q.writes = nil
	q.stageBytes = 128
	return m.finish(q, o, hash, mapRecord, budget, nil, graphChanges{})
}
func (m *materializer) graphBudget(q *reader) (graphstore.GraphLimits, error) {
	l := m.limits.graph
	l.MaxSourceRows = min(l.MaxSourceRows, q.limits.readRows-q.rows)
	l.MaxSourceBytes = min(l.MaxSourceBytes, q.limits.readBytes-q.bytes)
	l.MaxOutputBytes = min(l.MaxOutputBytes, m.limits.outputBytes-graphResultMetadataBytes-cap(q.base.Image))
	if l.MaxSourceRows < 1 || l.MaxSourceBytes < 1 || l.MaxOutputBytes < 1 {
		return l, errLimit
	}
	return l, nil
}
func (m *materializer) chargeGraph(q *reader, g graphstore.GraphEffects) error {
	if !sameBase(q.base, g.Base) {
		return errInvalid
	}
	if g.Work.Records > q.limits.readRows-q.rows || g.Work.Bytes > q.limits.readBytes-q.bytes {
		return errLimit
	}
	q.rows += g.Work.Records
	q.bytes += g.Work.Bytes
	return nil
}
func findBinding(claims []freshBinding, role bindingRole, owner graphstate.EntityID, id uint64) (freshBinding, bool) {
	for _, c := range claims {
		if c.role == role && c.owner == owner && c.id == id {
			return c, true
		}
	}
	return freshBinding{}, false
}
func admitBinding(q *reader, c freshBinding) (reason, error) {
	grant, found, err := q.grant(request{ns: q.ns, session: c.grant.session, sequence: c.grant.sequence})
	if err != nil {
		return reasonNone, err
	}
	if !found {
		return reasonInvalid, nil
	}
	recipient, found, err := q.recipient(c.grant.session.ID)
	if err != nil {
		return reasonNone, err
	}
	if !found || recipient.session != c.grant.session || recipient.home != q.ns.partition {
		return reasonStale, nil
	}
	rangeRecord := grant.grant.Reservation
	if !q.allocatorFound || grant.index < recipient.index || grant.grant.Request.Sequence > recipient.sequence || rangeRecord.Last > q.allocatorInfo.HighWater || rangeRecord.Request.Authority.Epoch() > q.allocatorInfo.Authority.Epoch() || rangeRecord.Request.Authority.Epoch() == q.allocatorInfo.Authority.Epoch() && rangeRecord.Request.Authority != q.allocatorInfo.Authority {
		return reasonNone, errCorrupt
	}
	if c.id < rangeRecord.First || c.id > rangeRecord.Last {
		return reasonInvalid, nil
	}
	return reasonNone, nil
}
func admitDelta(q *reader, r graphRequest, d graphstate.Delta) (reason, error) {
	for _, e := range d.Entities {
		c, found := findBinding(r.claims, entityBinding, 0, uint64(e.ID))
		if !found {
			return reasonInvalid, nil
		}
		if why, err := admitBinding(q, c); why != reasonNone || err != nil {
			return why, err
		}
	}
	for _, life := range d.Lives {
		c, found := findBinding(r.claims, lifeBinding, life.Owner, uint64(life.Life))
		if !found {
			return reasonInvalid, nil
		}
		if why, err := admitBinding(q, c); why != reasonNone || err != nil {
			return why, err
		}
	}
	for _, value := range d.Values {
		c, found := findBinding(r.claims, valueBinding, 0, uint64(value.ID))
		if !found {
			return reasonInvalid, nil
		}
		if why, err := admitBinding(q, c); why != reasonNone || err != nil {
			return why, err
		}
	}
	return reasonNone, nil
}
func (m *materializer) Stage(entry replica.Entry, budget raftlog.ApplicationBudget) (batch raftlog.ApplicationBatch, err error) {
	if m == nil || m.store == nil || budget.Writes < 1 || budget.Bytes < 1 || budget.ImageBytes < 1 || budget.ChangeBytes < 1 || budget.OutcomeBytes < 1 {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	if len(entry.Data) > 4<<20 {
		return raftlog.ApplicationBatch{}, errLimit
	}
	index, _, err := m.store.Checkpoint()
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	v, err := m.store.ApplicationView(index)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	defer func() {
		if closeErr := v.Close(); closeErr != nil {
			batch = raftlog.ApplicationBatch{}
			err = errors.Join(err, closeErr)
		}
	}()
	base, err := v.Root()
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if base.Index == math.MaxUint64 || entry.Index != base.Index+1 || entry.Generation != base.Generation {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	c, root, err := m.catalog(v)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if len(entry.Data) == 0 {
		b := batchAt(base)
		if err := m.preflight(b, budget); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		return b, nil
	}
	l := m.limits.allocation
	l.readRows = min(l.readRows, m.limits.sourceRows)
	l.readBytes = min(l.readBytes, m.limits.sourceBytes)
	l.stageRows = min(l.stageRows, m.limits.stageRows)
	l.stageBytes = min(l.stageBytes, m.limits.stageBytes)
	q := reader{ctx: context.Background(), view: v, ns: m.ns, limits: l, base: base, bytes: cap(base.Image), stageBytes: 128}
	if q.bytes > l.readBytes {
		return raftlog.ApplicationBatch{}, errLimit
	}
	// Read only the bounded identity header, then recover immutable outcomes
	// BEFORE full decode/local policy and current state checks. The 4MiB format
	// ceiling bounds hashing work even when exact retry exceeds current policy.
	control, isGraph, err := commandIdentity(entry.Data)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if control.ns != m.ns {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	hash := sha256.Sum256(entry.Data)
	o, replay, err := sharedReplay(&q, control, hash, entry.Index)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if replay {
		return m.finish(&q, o, hash, false, budget, nil, graphChanges{})
	}
	var r graphRequest
	var decodeErr error
	if isGraph {
		r, decodeErr = decodeGraphRequest(entry.Data, m.limits)
	} else {
		_, decodeErr = decodeRequest(entry.Data, l)
	}
	if errors.Is(decodeErr, errLimit) || errors.Is(decodeErr, temporal.ErrResourceLimit) || errors.Is(decodeErr, graphstate.ErrResourceLimit) {
		return raftlog.ApplicationBatch{}, decodeErr
	}
	if decodeErr == nil && !isGraph {
		control, decodeErr = decodeRequest(entry.Data, l)
	}

	bootstrap := (control.kind == initGraph || control.kind == initAllocator) && isBootstrapRoot(root)
	if !isBootstrapRoot(root) && control.kind != graphOperations {
		// Post-init attempts never use their schemas. Release that decoded backing
		// before the guarded reader, including recognized malformed init records.
		r.schemas = nil
		if err := m.checkInitialized(&q, c, root); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
	}
	o = outcome{ns: m.ns, kind: control.kind, identity: control.identity(), index: entry.Index, hash: hash, disposition: applied, reason: reasonInvalid}
	// Reserve no quota here: prove exact minimum deterministic rejection framing
	// before expensive Plan. Driver admission is the serialized reservation.
	minimum := q
	minimum.writes = nil
	minimum.stageBytes = 128
	if _, err := m.finish(&minimum, o, hash, !bootstrap, budget, nil, graphChanges{}); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if decodeErr != nil || control.kind == initAllocator && bootstrap {
		if decodeErr != nil && control.kind != initGraph && control.kind != initAllocator {
			return raftlog.ApplicationBatch{}, decodeErr
		}
		if bootstrap {
			if err := q.emptyProof(); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
		}

		return m.reject(&q, o, hash, !bootstrap, budget)
	}
	if !isGraph {
		if isBootstrapRoot(root) {
			return raftlog.ApplicationBatch{}, errNotInitialized
		}
		o, _, err = q.transition(control, entry.Index)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		b, err := m.finish(&q, o, hash, true, budget, nil, graphChanges{})

		return b, err
	}
	var effects graphstore.GraphEffects
	if r.kind == initGraph {
		if !isBootstrapRoot(root) {
			o.reason = reasonAlreadyInitialized
			return m.reject(&q, o, hash, true, budget)
		}
		// Fresh graph initialization proves the entire backend empty even if a
		// partial/misrouted allocator record exists under a bootstrap-shaped image.
		if err := q.emptyProof(); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		o, _, err = q.transition(request{ns: m.ns, kind: initAllocator, attempt: r.attempt, authority: r.authority, maxBlock: r.maxBlock}, entry.Index)
		o.kind = initGraph
		o.identity = r.identity()
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		} else if o.reason == reasonNone {
			gl, limitErr := m.graphBudget(&q)
			if limitErr != nil {
				err = limitErr
			} else {
				effects, err = graphstore.InitializeGraphIndexes(q.ctx, c, r.schemas, gl)
			}
		}
	} else {
		if isBootstrapRoot(root) {
			return raftlog.ApplicationBatch{}, errNotInitialized
		}
		if root.SemanticEpoch() == 0 {
			return raftlog.ApplicationBatch{}, errCorrupt
		}
		if _, found, err := q.allocator(); err != nil {
			return raftlog.ApplicationBatch{}, err
		} else if !found {
			return raftlog.ApplicationBatch{}, errCorrupt
		}
		gl, limitErr := m.graphBudget(&q)
		if limitErr != nil {
			err = limitErr
		} else {
			effects, err = graphstore.StageOperations(q.ctx, c, r.operations, r.revision, gl)
		}
		o.reason = reasonNone
	}
	if err != nil {
		why, recognized := businessGraphReason(err)
		if !recognized {
			return raftlog.ApplicationBatch{}, err
		}
		o.reason = why
		return m.reject(&q, o, hash, !bootstrap, budget)
	}
	if o.reason != reasonNone {
		return m.reject(&q, o, hash, !bootstrap, budget)
	}
	if err := m.chargeGraph(&q, effects); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if r.kind == graphOperations {
		why, err := admitDelta(&q, r, effects.Delta)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if why != reasonNone {
			o.reason = why
			return m.reject(&q, o, hash, true, budget)
		}
	}
	changes := graphChanges{ns: m.ns, entities: effects.Delta.Entities, lives: effects.Delta.Lives, values: effects.Delta.Values, groups: effects.Groups}
	if r.kind == initGraph {
		topology, err := effects.Root.SinglePartition()
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		changes.initialized = true
		changes.topology, changes.schema = topology.TopologyEpoch, topology.SchemaVersion
		changes.schemas = r.schemas
	}
	// The decoded command arrays are no longer retained during composition.
	r = graphRequest{}
	var graphEffects *graphstore.GraphEffects
	if changes.nonempty() {
		graphEffects = &effects
	} else {
		effects = graphstore.GraphEffects{}
	}
	b, err := m.finish(&q, o, hash, true, budget, graphEffects, changes)
	if err != nil {
		why, recognized := businessGraphReason(err)
		if !recognized {
			return raftlog.ApplicationBatch{}, err
		}
		o.reason = why
		return m.reject(&q, o, hash, !bootstrap, budget)
	}
	return b, nil
}
