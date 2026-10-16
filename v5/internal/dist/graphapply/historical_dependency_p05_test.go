package graphapply

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Missing-feature guard: the original unconditional Run has no historical-read
// condition. These inert codec checks do not claim a runtime RED or a Run bug.
func TestHistoricalDependencyP05ConditionCodecAndAgreement(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	var missing *crossProcessCoordinator
	if _, err := missing.OpenForUpdate(t.Context(), graphReadRequest{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil conditional open", err)
	}
	if _, err := missing.RunIfUnchanged(t.Context(), requestID{1}, nil, 1, nil, graphReadCondition{}); !errors.Is(err, errInvalid) {
		t.Fatal("decoded/absent condition admitted", err)
	}
	if _, err := missing.ResumeConditional(t.Context(), crossProcessResume{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil conditional resume", err)
	}
	if _, err := (&crossProcessCoordinator{host: &allocationHost{}}).ResumeConditional(t.Context(), crossProcessResume{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil conditional resume materializer", err)
	}
	var absent *conditionalGraphRead
	if _, err := absent.Condition(); !errors.Is(err, errInvalid) {
		t.Fatal("nil conditional read", err)
	}
	if _, err := absent.ReadView(); !errors.Is(err, errInvalid) {
		t.Fatal("nil conditional view", err)
	}
	if _, err := absent.Cut(); !errors.Is(err, errInvalid) {
		t.Fatal("nil conditional cut", err)
	}
	if err := absent.Close(); !errors.Is(err, errInvalid) {
		t.Fatal("nil conditional close", err)
	}
	// A real exhausted consumptive ledger refuses before a defensive copy.
	// These inert owner slots create no authenticated condition or native lease.
	exhausted, err := graphstate.NewOutputBudget(0)
	if err != nil {
		t.Fatal(err)
	}
	inert := &conditionalGraphRead{ctx: t.Context(), read: &certifiedGraphRead{sources: []certifiedGraphSource{&remoteGraphSource{ctx: t.Context()}}}, budget: newGraphRemoteBudget(defaultMaterializerLimits(), exhausted)}
	if _, err := inert.Condition(); !errors.Is(err, graphstate.ErrResourceLimit) || !errors.Is(err, errLimit) || exhausted.Used() != 0 {
		t.Fatal("condition copy revived exhausted shared allowance", err, exhausted.Used())
	}
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	c := &certifiedReadCoordinator{declaration: n.d, transport: &certifiedTestTransport{n: n}}
	read, err := c.Open(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	cut, err := read.Cut()
	if err != nil {
		t.Fatal(err)
	}
	value := graphReadConditionValue{parts: make([]graphReadConditionPart, len(cut.certificates))}
	for i, cert := range cut.certificates {
		// Inert representation fixtures are not authenticated conditions.
		cert.scope.semantic = conditionalGraphSemanticContractID()
		value.parts[i] = graphReadConditionPart{certificate: cert, prefix: cert.index, authority: [32]byte{1}}
	}
	l := defaultMaterializerLimits()
	if _, err := genesisSchemaDigestVersion(n.hosts[0][0].machine.ns.graph, p05Schemas(), l, true); err != nil {
		t.Fatal("conditional profile schema canonical ordering", err)
	}
	wire, err := encodeGraphReadConditionValue(value, l)
	if err != nil {
		t.Fatal(err)
	}
	arena, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeGraphReadConditionValue(wire, l, arena)
	if err != nil {
		t.Fatal(err)
	}
	again, err := encodeGraphReadConditionValue(got, l)
	if err != nil || !bytes.Equal(wire, again) {
		t.Fatal("condition lost exact bytes", err)
	}
	if (graphReadCondition{value: got}).validSeal() {
		t.Fatal("decoded caller bytes became authenticated evidence")
	}
	sources := []allocationProof{n.read(0, allocationQuery{kind: observeConfiguration}), n.read(1, allocationQuery{kind: observeConfiguration})}
	tx, _, err := n.hosts[0][0].PrepareCrossPartitionGraphOperations(t.Context(), n.requestID(), ops, 23, claims, sources)
	if err != nil {
		t.Fatal(err)
	}
	m, err := decodeCrossManifest(tx.manifest, l, arena)
	if err != nil {
		t.Fatal(err)
	}
	old, err := encodeCrossManifest(m, l)
	if err != nil || !bytes.Equal(old, tx.manifest) {
		t.Fatal("GCM1 bytes changed", err)
	}
	conditional, err := encodeConditionalCrossManifest(m, got, l)
	if err != nil {
		t.Fatal(err)
	}
	decoded, condition, err := decodeConditionalCrossManifest(conditional, l, arena)
	if err != nil {
		t.Fatal(err)
	}
	oldAgain, err := encodeCrossManifest(decoded, l)
	if err != nil || !bytes.Equal(old, oldAgain) {
		t.Fatal("conditional envelope changed unconditional original", err)
	}
	conditionAgain, err := encodeGraphReadConditionValue(condition, l)
	if err != nil || !bytes.Equal(wire, conditionAgain) {
		t.Fatal("manifest lost condition", err)
	}
	live, err := decodeCrossManifest(conditional, l, arena)
	if err != nil || live.condition == nil {
		t.Fatal("conditional envelope not retained", err)
	}
	configuration, err := n.hosts[0][0].currentConfiguration()
	if err != nil {
		t.Fatal(err)
	}
	if err := n.hosts[0][0].machine.validateCrossManifestBinding(live, configuration); !errors.Is(err, errInvalid) {
		t.Fatal("old profile admitted new conditional envelope", err)
	}

	bad := bytes.Clone(wire)
	bad[len(bad)-1] ^= 1
	if _, err := decodeGraphReadConditionValue(bad, l, arena); !errors.Is(err, errCorrupt) {
		t.Fatal("bad condition checksum", err)
	}
	for _, length := range []int{0, 4, len(wire) - 1} {
		if _, err := decodeGraphReadConditionValue(wire[:length], l, arena); !errors.Is(err, errCorrupt) {
			t.Fatal("truncated condition", length, err)
		}
	}
	value.parts[1].certificate.scope.partition = value.parts[0].certificate.scope.partition
	if _, err := encodeGraphReadConditionValue(value, l); !errors.Is(err, errInvalid) {
		t.Fatal("duplicate participant", err)
	}
	if !certifiedGraphAgreement(conditionalGraphSemanticContractID()) || !rangeMoveGraphAgreement(conditionalGraphSemanticContractID()) || rangeMoveGraphAgreement(certifiedGraphSemanticContractID()) {
		t.Fatal("conditional agreement must explicitly preserve both native capabilities")
	}
	// Inert encoder sizing at the admitted large participant bound: these
	// copied kernels/certificates are not valid participant authority or effects.
	sized := m
	sized.parts = make([]crossPart, 16)
	sizedValue := graphReadConditionValue{parts: make([]graphReadConditionPart, 16)}
	proofs := make([]crossStatus, 16)
	for i := range 16 {
		sized.parts[i] = m.parts[i%len(m.parts)]
		sized.parts[i].binding.Partition = uint64(i + 1)
		sizedValue.parts[i] = got.parts[i%len(got.parts)]
		sizedValue.parts[i].certificate.scope.partition = uint64(i + 1)
		proofs[i] = crossStatus{ns: namespace{m.request.ns.graph, uint64(i + 1)}, tx: m.tx, digest: m.digest, phase: crossPrepare, index: 1, yes: true}
	}
	oldCredit, err := n.hosts[0][0].machine.crossReservation(sized, crossRegister)
	if err != nil {
		t.Fatal(err)
	}
	oldSized, err := encodeCrossManifest(sized, l)
	if err != nil {
		t.Fatal(err)
	}
	sized.condition = &sizedValue
	newSized, err := encodeCrossManifest(sized, l)
	if err != nil || len(newSized)-len(oldSized) != 84+436*16 {
		t.Fatal("exact conditional envelope size", err, len(newSized)-len(oldSized))
	}
	newCredit, err := n.hosts[0][0].machine.crossReservation(sized, crossRegister)
	if err != nil {
		t.Fatal(err)
	}
	command, err := encodeCrossCommand(crossCommand{ns: m.request.ns, phase: crossDecide, manifest: newSized, proofs: proofs}, l)
	if err != nil {
		t.Fatal(err)
	}
	for i, credit := range newCredit.Credits {
		if credit.CommandBytes < len(command) || credit.CommandBytes-oldCredit.Credits[i].CommandBytes != len(newSized)-len(oldSized) {
			t.Fatal("conditional credit underfunded encoded completion", i, len(command), credit)
		}
	}
	if newCredit.Credits[0].RetainedBytes-oldCredit.Credits[0].RetainedBytes != 2*uint64(len(newSized)-len(oldSized)) {
		t.Fatal("conditional prepared-original retention omitted wrapper")
	}
	// Inert diagnostic frames exercise only bounded ownership/correlation. They
	// grant no certificate, condition, or native original authority.
	question := graphSourceRequest{kind: sourceCertifiedOpen, sequence: 1, nonce: [16]byte{1}}
	request, err := encodeGraphSourceRequest(question)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := encodeGraphWireResponse(graphWireResponse{peer: graphPeerIdentity{scope: n.hosts[0][0].machine.scope(), voter: 1}, issuer: [16]byte{2}, kind: question.kind, sequence: question.sequence, nonce: question.nonce})
	if err != nil {
		t.Fatal(err)
	}
	capture := &p05CampaignCapture{}
	if err := capture.after(t.Context(), 1, 3, request, reply); err != nil || capture.charge.Candidates != 1 || capture.charge.Attempts != 1 || capture.charge.Work == 0 || capture.charge.Output == 0 || capture.charge.Retained != uint64(64+len(request)+len(reply)) || len(capture.frames) != 1 {
		t.Fatal("bounded complete P05 frame ownership", err, capture.charge)
	}
	request[0] ^= 1
	reply[0] ^= 1
	if capture.frames[0].Request[0] != 'G' || capture.frames[0].Reply[0] != 'G' {
		t.Fatal("P05 diagnostic frame retained caller-owned bytes")
	}
	canceled, cancelCapture := context.WithCancel(t.Context())
	cancelCapture()
	unspent := &p05CampaignCapture{}
	if err := unspent.after(canceled, 1, 3, nil, nil); !errors.Is(err, context.Canceled) || unspent.charge != (p05CampaignCaptureCharge{}) || len(unspent.frames) != 0 {
		t.Fatal("canceled diagnostic capture admitted work", err, unspent.charge)
	}
	const candidates = 2 * (processDeliveries + 96*4*1024)
	full := &p05CampaignCapture{charge: p05CampaignCaptureCharge{Candidates: candidates}}
	if err := full.after(t.Context(), 1, 3, nil, nil); !errors.Is(err, errLimit) || full.charge.Candidates != candidates || full.charge.Work != 0 || len(full.frames) != 0 {
		t.Fatal("capture renewed its exhausted candidate grant", err, full.charge)
	}
	if err := full.prepayGate(t.Context(), nil); !errors.Is(err, errLimit) || full.charge.Work != 0 || full.charge.Output != 0 {
		t.Fatal("Register hold bypassed the same exhausted capture ledger", err, full.charge)
	}
}

func p05Schemas() []graphstate.PropertyDefinition {
	// Native genesis ordering is Owner then Name; the two original p schemas
	// remain unchanged and both unique-u schemas belong only to this profile.
	return []graphstate.PropertyDefinition{
		{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality},
		{Name: "u", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueScalar},
		{Name: "p", Owner: graphstate.Relationship, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality},
		{Name: "u", Owner: graphstate.Relationship, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueScalar},
	}
}

type p05ProcessOwner struct {
	read      *conditionalGraphRead
	condition graphReadCondition
	cancel    context.CancelFunc
}
type p05ProcessSessions struct {
	mu        sync.Mutex
	ctx       context.Context
	certified *graphProcessCertifiedSessions
	business  *certifiedProcessOwners
	owners    map[uint64]p05ProcessOwner
}
type p05OpenReply struct{ Token, Condition []byte }
type p05Attempt struct {
	Result                                          []byte
	Conflict, Closed, Unavailable, Expired, Invalid bool
	Error                                           string
	NativePending                                   bool `json:",omitzero"` // Fresh campaign diagnostic; ordinary bytes unchanged.
}
type p05ObservedCell struct {
	Owner, Life uint64
	Name        string
	Point       int64
	Cell        nativeLifecycleCell
	State       []byte
}
type p05ObservedPresence struct {
	Owner     uint64
	Point     int64
	Cell      nativeLifecycleCell
	Effective bool
	Life      uint64
}
type p05Observation struct {
	Native, Census                        []byte
	Cells                                 []p05ObservedCell
	Presence                              []p05ObservedPresence
	Keys                                  []graphstate.ComponentKey
	NodeUnique, RelUnique                 []graphstate.UniqueClaim
	AbsentEntity, AbsentValue, AbsentLife bool
}

func newP05ProcessSessions(ctx context.Context, certified *graphProcessCertifiedSessions, o *certifiedProcessOwners) *p05ProcessSessions {
	return &p05ProcessSessions{ctx: ctx, certified: certified, business: o, owners: make(map[uint64]p05ProcessOwner)}
}
func (s *p05ProcessSessions) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	for _, owner := range s.owners {
		owner.cancel()
		err = errors.Join(err, owner.read.Close())
	}
	clear(s.owners)
	return err
}
func (s *p05ProcessSessions) serve(ctx context.Context, r processRequest) ([]byte, error) {
	c := s.certified.coordinator
	if c.host.machine.SemanticContractID() != conditionalGraphSemanticContractID() {
		return nil, errInvalid
	}
	switch r.op {
	case processP05Open:
		if r.entity != 0 && r.entity != 5 {
			return nil, errInvalid
		}
		request, err := decodeGraphE15ReadRequest(r.data)
		if err != nil || request.mode == graphReadAfter {
			return nil, errors.Join(errInvalid, err)
		}
		s.certified.mu.Lock()
		if len(s.certified.owners)+s.certified.pending >= 2 {
			s.certified.mu.Unlock()
			return nil, errLimit
		}
		s.certified.next++
		id := s.certified.next
		s.certified.pending++
		s.certified.mu.Unlock()
		owner, cancel := context.WithCancel(s.ctx)
		if r.entity != 0 {
			cancel()
			owner, cancel = context.WithTimeout(s.ctx, 5*time.Second)
		}
		stop := context.AfterFunc(ctx, cancel)
		read, err := c.OpenForUpdate(owner, graphReadRequest{mode: request.mode, cut: request.cut})
		stop()
		var condition graphReadCondition
		var token, conditionWire []byte
		if err == nil {
			condition, err = read.Condition()
		}
		if err == nil {
			var cut graphCut
			cut, err = read.Cut()
			if err == nil {
				token, err = encodeGraphE15ReadToken(id, cut)
			}
		}
		if err == nil {
			conditionWire, err = encodeGraphReadConditionValue(condition.value, c.host.machine.limits)
		}
		if err == nil {
			err = ctx.Err()
		}
		s.certified.mu.Lock()
		s.certified.pending--
		if err == nil {
			s.certified.owners[id] = graphProcessCertifiedOwner{read.read, cancel}
		}
		s.certified.mu.Unlock()
		if err != nil {
			cancel()
			if read != nil {
				err = errors.Join(err, read.Close())
			}
			return nil, err
		}
		s.mu.Lock()
		s.owners[id] = p05ProcessOwner{read, condition, cancel}
		s.mu.Unlock()
		return json.Marshal(p05OpenReply{token, conditionWire})
	case processP05Close:
		s.mu.Lock()
		owner, ok := s.owners[r.index]
		s.mu.Unlock()
		if !ok {
			return nil, errInvalid
		}
		if r.entity == 1 {
			select {
			case <-owner.read.ctx.Done():
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		s.certified.mu.Lock()
		delete(s.certified.owners, r.index)
		s.certified.mu.Unlock()
		owner.cancel()
		return nil, owner.read.Close()
	case processP05Observe:
		return s.observe(ctx, r)
	case processP05Coordinate, processP05Resume:
		var result crossProcessResult
		var err error
		if r.op == processP05Resume {
			var resume crossProcessResume
			resume, err = decodeP05Resume(r.data)
			if err == nil {
				result, err = c.ResumeConditional(ctx, resume)
			}
		} else {
			s.mu.Lock()
			owner, ok := s.owners[r.index]
			s.mu.Unlock()
			if !ok {
				return nil, errInvalid
			}
			var business partitionGraphCommand
			business, err = decodePartitionGraphCommand(r.data, c.host.machine.limits)
			if err == nil {
				result, err = c.RunIfUnchanged(ctx, business.request.id, business.request.operations, business.request.revision.Provenance(), business.request.claims, owner.condition)
			}
		}
		wire, encodeErr := encodeGraphProcessCoordinatorResult(result, err)
		if encodeErr != nil {
			return nil, encodeErr
		}
		// Diagnostic classification from this genuine child-owned API error; raw
		// records independently prove no Prepare/CDC and original result identity.
		message := ""
		if err != nil {
			message = err.Error()
		}
		pendingLoss := false
		if err != nil && os.Getenv("RHO_GRAPH_PROTOCOL_CAMPAIGN") == nativeProtocolCampaignProfile {
			pendingLoss = nativeProtocolPendingLoss(err)
		}
		return json.Marshal(p05Attempt{Result: wire, Conflict: errors.Is(err, errGraphReadConflict), Closed: errors.Is(err, graphstore.ErrClosed), Unavailable: errors.Is(err, replica.ErrUnavailable) || errors.Is(err, errCrossProcessPending), Expired: errors.Is(err, context.DeadlineExceeded), Invalid: errors.Is(err, errInvalid), Error: message, NativePending: pendingLoss})
	}
	return nil, errInvalid
}
func encodeP05Resume(r crossProcessResume) ([]byte, error) {
	return boundedEncoding(512, func(w *boundedWriter) {
		w.add([]byte{'G', 'U', 'R', 1})
		w.add(appendAllocationScope(nil, r.scope))
		w.add(r.request[:])
		w.add(r.tx[:])
		w.add(r.business[:])
	})
}
func decodeP05Resume(wire []byte) (crossProcessResume, error) {
	body, err := graphBody(wire, "GUR\x01", 512)
	if err != nil {
		return crossProcessResume{}, err
	}
	c := graphCursor{b: body}
	scope := c.take(120)
	r := crossProcessResume{request: requestID(c.array()), tx: c.array()}
	copy(r.business[:], c.take(32))
	if c.err != nil || len(c.b) != 0 {
		return crossProcessResume{}, errors.Join(errCorrupt, c.err)
	}
	r.scope = readAllocationScope(&decoder{b: scope})
	return r, nil
}
func p05Span(axis temporal.Axis) (temporal.Scope, error) {
	lo, err := temporal.RationalPosition(axis, temporal.RationalInt64(2))
	if err != nil {
		return temporal.Scope{}, err
	}
	hi, err := temporal.RationalPosition(axis, temporal.RationalInt64(3))
	if err != nil {
		return temporal.Scope{}, err
	}
	lower, err := temporal.FiniteBound(lo, true)
	if err != nil {
		return temporal.Scope{}, err
	}
	upper, err := temporal.FiniteBound(hi, true)
	if err != nil {
		return temporal.Scope{}, err
	}
	return temporal.Span(axis, lower, upper, temporal.Limits{})
}
func (s *p05ProcessSessions) proposal(ctx context.Context, r processRequest) (allocationProposal, error) {
	o := s.business
	h := o.host
	if h.machine.SemanticContractID() != conditionalGraphSemanticContractID() || o.recipient == nil {
		return allocationProposal{}, errInvalid
	}
	id := requestID(o.incarnation)
	binary.BigEndian.PutUint64(id[8:], r.sequence)
	if r.entity == 101 || r.entity == 102 {
		var wire []byte
		var err error
		if r.entity == 101 {
			var intent rangeMoveIntent
			intent, err = h.PrepareRangeMove(ctx, id, 17, 3, 16, 16, 64<<10)
			if err == nil {
				wire, err = encodeRangeMoveIntent(intent)
			}
		} else {
			wire = bytes.Clone(r.data)
		}
		if err != nil {
			return allocationProposal{}, err
		}
		arena, err := graphstate.NewOutputBudget(h.machine.limits.outputBytes)
		if err != nil {
			return allocationProposal{}, err
		}
		phase := rangeMoveFence
		if r.entity == 102 {
			phase = rangeMoveAbort
		}
		return h.prepareRangeMoveCommand(wire, phase, h.machine.ns.partition, 0, nil, nil, arena)
	}
	cfg, err := h.currentConfiguration()
	if err != nil {
		return allocationProposal{}, err
	}
	scope, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return allocationProposal{}, err
	}
	if r.entity >= 4 && r.entity <= 7 {
		scope, err = p05Span(cfg.defaultAxis.Axis())
		if err != nil {
			return allocationProposal{}, err
		}
	}
	grant := grantReference{session: o.recipient.session, sequence: 1}
	var ops []graphstate.Operation
	var claims []freshBinding
	valueClaim := func(id uint64) { claims = append(claims, freshBinding{role: valueBinding, id: id, grant: grant}) }
	switch r.entity {
	case 1:
		if r.index != 1 && r.index != 17 {
			return allocationProposal{}, errInvalid
		}
		valueID := uint64(5)
		if h.machine.ns.partition == 8 {
			valueID = 21
		}
		ops = []graphstate.Operation{{Kind: graphstate.Set, Owner: graphstate.EntityID(r.index), Life: graphstate.LifeID(r.index + 1), Name: "p", Scope: scope, Value: graphstate.I64(5), ValueID: graphstate.ValueID(valueID)}}
		valueClaim(valueID)
	case 2:
		ops = []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 9, Life: 10, Scope: scope, Record: graphstate.EntityRecord{Interpretation: graphstate.InterpretationObservation, TemporalRole: graphstate.TemporalRoleValidity}}, {Kind: graphstate.Set, Owner: 9, Life: 10, Name: "u", Scope: scope, Value: graphstate.I64(5), ValueID: 11}, {Kind: graphstate.CreateRelationship, Owner: 3, Life: 4, Scope: scope, Record: graphstate.EntityRecord{Type: "edge", Source: 1, Target: 17, Mode: graphstate.LifeBound, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleOccurrenceTime}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: 18}}, {Kind: graphstate.Set, Owner: 3, Life: 4, Name: "u", Scope: scope, Value: graphstate.I64(6), ValueID: 12}}
		claims = []freshBinding{{role: entityBinding, id: 9, grant: grant}, {role: lifeBinding, owner: 9, id: 10, grant: grant}, {role: entityBinding, id: 3, grant: grant}, {role: lifeBinding, owner: 3, id: 4, grant: grant}}
		valueClaim(11)
		valueClaim(12)
	case 4, 5:
		value := int64(6)
		ref := graphstate.ValueID(6)
		if r.entity == 5 {
			value, ref = 5, 5
		}
		ops = []graphstate.Operation{{Kind: graphstate.Set, Owner: 3, Life: 4, Name: "p", Scope: scope, Value: graphstate.I64(value), ValueID: ref}}
		if r.entity == 4 {
			valueClaim(6)
		}
	case 6:
		ops = []graphstate.Operation{{Kind: graphstate.Close, Owner: 1, Life: 2, Scope: scope}}
	case 7:
		ops = []graphstate.Operation{{Kind: graphstate.Reopen, Owner: 1, Life: 7, Scope: scope}}
		claims = []freshBinding{{role: lifeBinding, owner: 1, id: 7, grant: grant}}
	default:
		return allocationProposal{}, errInvalid
	}
	return h.PreparePartitionGraphOperations(id, ops, 100+r.entity, claims)
}
func (s *p05ProcessSessions) observe(ctx context.Context, r processRequest) (wire []byte, cause error) {
	s.certified.mu.Lock()
	owner, ok := s.certified.owners[r.index]
	s.certified.mu.Unlock()
	if !ok {
		return nil, errInvalid
	}
	var view graphstate.ReadView
	var err error
	s.mu.Lock()
	conditional, conditionalOK := s.owners[r.index]
	s.mu.Unlock()
	stage := "read-view"
	var observedOwner, observedLife uint64
	var observedPoint int64
	var observedName string
	defer func() {
		if cause == nil || os.Getenv("RHO_GRAPH_PROTOCOL_CAMPAIGN") == "" {
			return
		}
		// Failure-only context for the actual retained owner. These counters do
		// not renew its allowance or claim the remote server's private work.
		work := owner.read.Work()
		context := fmt.Sprintf("P05 observe handle=%d conditional=%t stage=%s owner=%d life=%d point=%d name=%q work=%+v sourceUnavailable=%t", r.index, conditionalOK, stage, observedOwner, observedLife, observedPoint, observedName, work, errors.Is(cause, graphstore.ErrSourceUnavailable))
		if remote, ok := errors.AsType[graphWireError](cause); ok {
			context += fmt.Sprintf(" remoteMask=%x", remote.mask)
		}
		owner.read.mu.Lock()
		for _, source := range owner.read.sources {
			remote, ok := source.(*remoteGraphSource)
			if !ok || remote.endpoint == nil || remote.endpoint.budget == nil {
				continue
			}
			b := remote.endpoint.budget
			b.mu.Lock()
			context += fmt.Sprintf(" [source partition=%d voter=%d calls=%d/%d rows=%d/%d bytes=%d/%d outputRemaining=%d]", remote.endpoint.peer.scope.partition, remote.endpoint.peer.voter, b.calls, b.maxCalls, b.rows, b.maxRows, b.bytes, b.maxBytes, b.output.Remaining())
			b.mu.Unlock()
		}
		owner.read.mu.Unlock()
		cause = fmt.Errorf("%s: %w", context, cause)
	}()
	if conditionalOK {
		view, err = conditional.read.ReadView()
	} else {
		view, err = owner.read.ReadView()
	}
	if err != nil {
		return nil, err
	}
	// Retain actual immutable results only until this observation returns. Both
	// temporal observations still use this SAME conditional handle; neither its
	// RPC grant nor its cut is renewed.
	owner.read.mu.Lock()
	var shared *graphRemoteBudget
	var lifetimes [16]context.Context
	sourceCount := 0
	for _, source := range owner.read.sources {
		remote, ok := source.(*remoteGraphSource)
		if !ok || remote.endpoint == nil || remote.endpoint.budget == nil || shared != nil && shared != remote.endpoint.budget || sourceCount == len(lifetimes) {
			owner.read.mu.Unlock()
			return nil, errInvalid
		}
		shared = remote.endpoint.budget
		lifetimes[sourceCount] = remote.ctx
		sourceCount++
	}
	owner.read.mu.Unlock()
	if shared == nil {
		return nil, errInvalid
	}
	arena, ok := shared.output.(*graphstate.OutputBudget)
	if !ok {
		return nil, errInvalid
	}
	view, err = newNativeObservationRead(view, arena, s.certified.coordinator.host.machine.limits.graph.Planner.Component, lifetimes[:sourceCount])
	if err != nil {
		return nil, err
	}
	h := s.certified.coordinator.host
	cfg, err := h.currentConfiguration()
	if err != nil {
		return nil, err
	}
	window, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return nil, err
	}
	out := p05Observation{}
	stage = "lifecycle"
	out.Native, err = nativeLifecycleObserveView(ctx, s.certified, view, processRequest{index: r.index})
	if err != nil {
		return nil, err
	}
	stage = "entity-census"
	out.Census, err = nativeLifecycleObserveView(ctx, s.certified, view, processRequest{index: r.index, entity: 2})
	if err != nil {
		return nil, err
	}
	budget := graphstate.ReadBudget{Rows: 64, Bytes: 64 << 10}
	for _, ownerLife := range [][2]uint64{{1, 2}, {17, 18}, {3, 4}, {9, 10}} {
		observedOwner, observedLife = ownerLife[0], ownerLife[1]
		for _, at := range []int64{0, 2} {
			observedPoint, observedName = at, ""
			point, err := temporal.RationalPosition(cfg.defaultAxis.Axis(), temporal.RationalInt64(at))
			if err != nil {
				return nil, err
			}
			stage = "presence"
			cell, err := nativeLifecycleCellRead(ctx, view, graphstate.ComponentKey{Owner: graphstate.EntityID(ownerLife[0]), Kind: graphstate.Presence}, window, point, h.machine.limits.graph.Planner.Component)
			if err != nil {
				return nil, err
			}
			stage = "effective-projection"
			projection, err := graphstate.Project(ctx, view, graphstate.EntityID(ownerLife[0]), point, graphstate.Effective, h.machine.limits.graph.Planner)
			if err != nil {
				return nil, err
			}
			out.Presence = append(out.Presence, p05ObservedPresence{ownerLife[0], at, cell, projection.Active, uint64(projection.Life)})
		}
		for _, name := range []string{"p", "u"} {
			stage, observedName, observedPoint = "component-page", name, 0
			key := graphstate.ComponentKey{Owner: graphstate.EntityID(ownerLife[0]), Life: graphstate.LifeID(ownerLife[1]), Kind: graphstate.ScalarProperty, Name: name}
			page, err := view.ComponentPage(ctx, graphstate.ComponentQuery{Key: key, Window: window}, 0, budget)
			if err != nil {
				return nil, err
			}
			if !page.Complete || page.Next != 0 {
				return nil, errLimit
			}
			stage = "component-encode"
			wire, err := state.AppendState(nil, page.Data, state.CodecLimits{State: h.machine.limits.graph.Planner.Component, MaxEncodedBytes: 64 << 10})
			if err != nil {
				return nil, err
			}
			for _, at := range []int64{0, 2} {
				stage, observedPoint = "component-value", at
				point, err := temporal.RationalPosition(cfg.defaultAxis.Axis(), temporal.RationalInt64(at))
				if err != nil {
					return nil, err
				}
				cell, err := page.Data.At(point, h.machine.limits.graph.Planner.Component)
				if err != nil {
					return nil, err
				}
				actual, err := nativeLifecycleCellFromState(ctx, view, key, cell)
				if err != nil {
					return nil, err
				}
				out.Cells = append(out.Cells, p05ObservedCell{ownerLife[0], ownerLife[1], name, at, actual, wire})
			}
		}
	}
	stage, observedOwner, observedLife, observedPoint, observedName = "absent-component-keys", 9, 10, 0, ""
	keys, err := view.ComponentKeys(ctx, graphstate.KeyPredicate{Owner: 9, Life: 10, Kind: graphstate.ScalarProperty}, 0, budget)
	if err != nil || !keys.Complete || keys.Next != 0 {
		return nil, errors.Join(errLimit, err)
	}
	out.Keys = keys.Keys
	for i, kind := range []graphstate.EntityKind{graphstate.Node, graphstate.Relationship} {
		stage, observedOwner, observedName = "unique-candidates", uint64(kind), "u"
		value := int64(5 + i)
		page, err := view.UniqueCandidates(ctx, graphstate.UniquePredicate{Definition: graphstate.PropertyDefinition{Name: "u", Owner: kind, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueScalar}, Value: graphstate.I64(value), Window: window}, 0, budget)
		if err != nil || !page.Complete || page.Next != 0 {
			return nil, errors.Join(errLimit, err)
		}
		if i == 0 {
			out.NodeUnique = page.Claims
		} else {
			out.RelUnique = page.Claims
		}
	}
	stage, observedOwner, observedLife, observedName = "absent-entity", 9, 0, ""
	entity, err := view.Entity(ctx, 9)
	if err != nil {
		return nil, err
	}
	out.AbsentEntity = !entity.Found
	stage, observedOwner = "absent-value", 11
	value, err := view.Value(ctx, 11)
	if err != nil {
		return nil, err
	}
	out.AbsentValue = !value.Found
	stage, observedOwner, observedLife = "absent-life", 9, 10
	life, err := view.Life(ctx, 9, 10)
	if err != nil {
		return nil, err
	}
	out.AbsentLife = !life.Found
	stage = "observation-encode"
	return json.Marshal(out)
}

func p05Open(t *testing.T, f *pairedGraphFaultNetwork, actor uint64, mode graphReadMode, cut graphCut, lifetime ...uint64) (uint64, graphCut, []byte) {
	t.Helper()
	input, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: mode, cut: cut})
	if err != nil {
		t.Fatal(err)
	}
	seconds := uint64(0)
	if len(lifetime) > 0 {
		seconds = lifetime[0]
	}
	r := f.call(f.n.all[actor], processRequest{op: processP05Open, entity: seconds, data: input})
	var opened p05OpenReply
	if err := json.Unmarshal(r.aux, &opened); err != nil {
		t.Fatal(err)
	}
	handle, actual, err := decodeGraphE15ReadToken(opened.Token)
	if err != nil {
		t.Fatal(err)
	}
	return handle, actual, opened.Condition
}
func p05Observe(t *testing.T, f *pairedGraphFaultNetwork, actor, handle uint64) p05Observation {
	t.Helper()
	completed := false
	defer func() {
		if !completed && f.parent != nil {
			t.Logf("P05 observation interrupted: actual actor=%d handle=%d", actor, handle)
		}
	}()
	r := f.call(f.n.all[actor], processRequest{op: processP05Observe, index: handle})
	completed = true
	var observed p05Observation
	if err := json.Unmarshal(r.aux, &observed); err != nil {
		t.Fatal(err)
	}
	return observed
}
func p05Current(t *testing.T, f *pairedGraphFaultNetwork, actor uint64, mode graphReadMode, cut graphCut) (p05Observation, graphCut) {
	t.Helper()
	handle, actual := f.open(actor, mode, cut)
	observed := p05Observe(t, f, actor, handle)
	f.call(f.n.all[actor], processRequest{op: processSourceCertifiedClose, index: handle})
	return observed, actual
}
func p05AttemptRun(t *testing.T, f *pairedGraphFaultNetwork, actor, handle uint64, p allocationProposal) (p05Attempt, crossProcessResult) {
	t.Helper()
	r := f.call(f.n.all[actor], processRequest{op: processP05Coordinate, index: handle, data: p.wire})
	var attempt p05Attempt
	if err := json.Unmarshal(r.aux, &attempt); err != nil {
		t.Fatal(err)
	}
	result, err := decodeGraphProcessCoordinatorResult(processResponse{aux: attempt.Result})
	if err != nil && !attempt.Unavailable {
		t.Fatal("actual conditional request result", err, attempt.Error)
	}
	return attempt, result
}
func p05Prepare(t *testing.T, f *pairedGraphFaultNetwork, actor, kind, owner uint64, data []byte) allocationProposal {
	t.Helper()
	r := f.call(f.n.all[actor], processRequest{op: processP05Proposal, entity: kind, index: owner, data: data})
	p, err := decodeCertifiedProcessProposal(r.aux)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func p05Records(t *testing.T, f *pairedGraphFaultNetwork, from [2]uint64) []lifecycleRawRecord {
	t.Helper()
	f.drain()
	records := p06Records(f, from, f.indexes())
	for _, record := range records {
		members := []uint64{1, 2, 3}
		if record.Partition == 8 {
			members = []uint64{4, 5, 6}
		}
		for _, id := range members {
			r := f.call(f.n.all[id], processRequest{op: processEntryRecord, index: record.Index})
			if !bytes.Equal(r.outcome, record.Outcome) || !bytes.Equal(r.changes, record.CDC) {
				t.Fatal("P05 actual voter original/CDC mismatch", id, record.Index)
			}
		}
	}
	return records
}
func p05Cell(t *testing.T, o p05Observation, owner uint64, name string, point int64) nativeLifecycleCell {
	t.Helper()
	for _, c := range o.Cells {
		if c.Owner == owner && c.Name == name && c.Point == point {
			return c.Cell
		}
	}
	t.Fatal("missing observed cell", owner, name, point)
	return nativeLifecycleCell{}
}
func p05Presence(t *testing.T, o p05Observation, owner uint64, point int64) p05ObservedPresence {
	t.Helper()
	for _, c := range o.Presence {
		if c.Owner == owner && c.Point == point {
			return c
		}
	}
	t.Fatal("missing observed presence", owner, point)
	return p05ObservedPresence{}
}
func p05NoEffects(t *testing.T, records []lifecycleRawRecord, tx [16]byte, preflight bool) {
	t.Helper()
	for _, r := range records {
		if len(r.CDC) != 0 {
			t.Fatal("refusal/duplicate emitted CDC", r.Partition, r.Index)
		}
		if status, err := decodeCrossStatus(r.Outcome); err == nil && status.tx == tx && preflight {
			t.Fatal("preflight stale condition registered/prepared", status)
		}
	}
}
func p05CommitCDC(t *testing.T, records []lifecycleRawRecord, result crossProcessResult, expected []graphstate.ComponentKey, entities, lives, values int) {
	t.Helper()
	var keys []graphstate.ComponentKey
	resolved, newEntities, newLives, newValues := 0, 0, 0, 0
	for _, r := range records {
		status, err := decodeCrossStatus(r.Outcome)
		if err != nil || status.tx != result.resume.tx {
			if len(r.CDC) != 0 {
				t.Fatal("unbound actual CDC", r.Partition, r.Index)
			}
			continue
		}
		if status.digest != result.decision.digest {
			t.Fatal("condition/original digest changed", status)
		}
		if status.phase == crossPrepare && !status.yes {
			t.Fatal("unchanged condition voted NO", status)
		}
		if status.phase != crossResolve {
			if len(r.CDC) != 0 {
				t.Fatal("business CDC before Resolve")
			}
			continue
		}
		if !status.commit || status.round != result.decision.round || status.index != r.Index {
			t.Fatal("actual COMMIT original mismatch")
		}
		resolved++
		logical, err := decodeTypedChangeEnvelope(r.CDC, outcome{ns: status.ns, kind: graphOperations, identity: status.tx, hash: status.digest, index: status.index, disposition: applied}, defaultMaterializerLimits(), true)
		if err != nil {
			t.Fatal(err)
		}
		newEntities += len(logical.entities)
		newLives += len(logical.lives)
		newValues += len(logical.values)
		for _, g := range logical.groups {
			keys = append(keys, g.Key)
			if len(g.Changes) == 0 {
				t.Fatal("empty business change")
			}
			for _, change := range g.Changes {
				if change.After().Revision().ID() != status.round {
					t.Fatal("CDC revision not actual COMMIT round")
				}
			}
		}
	}
	slices.SortFunc(keys, func(a, b graphstate.ComponentKey) int {
		if a.Owner != b.Owner {
			return cmp.Compare(a.Owner, b.Owner)
		}
		if a.Life != b.Life {
			return cmp.Compare(a.Life, b.Life)
		}
		if a.Kind != b.Kind {
			return cmp.Compare(a.Kind, b.Kind)
		}
		return strings.Compare(a.Name, b.Name)
	})
	slices.SortFunc(expected, func(a, b graphstate.ComponentKey) int {
		if a.Owner != b.Owner {
			return cmp.Compare(a.Owner, b.Owner)
		}
		if a.Life != b.Life {
			return cmp.Compare(a.Life, b.Life)
		}
		if a.Kind != b.Kind {
			return cmp.Compare(a.Kind, b.Kind)
		}
		return strings.Compare(a.Name, b.Name)
	})
	if resolved != 2 || !slices.Equal(keys, expected) || newEntities != entities || newLives != lives || newValues != values {
		t.Fatal("complete actual CDC declarations/groups", resolved, keys, expected, newEntities, newLives, newValues)
	}
}

type p05Step struct {
	Cut           lifecycleRawCut
	Name          string
	Original      []byte
	Result        []byte
	Records       []lifecycleRawRecord
	Before, After p05Observation
}
type p05Artifact struct {
	Schedule               string
	Condition              []byte
	Cut                    lifecycleRawCut
	Old, Retained, Current p05Observation
	Attempts               []p05Attempt
	Steps                  []p05Step
	Phases                 []p06Phase
	Controls               []p06LocalRecord
	Processes              []pairedFaultProcess
	Records                []lifecycleRawRecord
}

func TestHistoricalDependencyP05ConditionalCrossWriteActualProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, schedule := range []string{"write-skew-A-first", "write-skew-B-first", "empty-range-and-absent-key-insertion", "temporal-life-value-ABA", "authority-fence-before-prepare"} {
		if !t.Run(schedule, func(t *testing.T) { p05ConditionalCase(t, schedule, nil) }) {
			return
		}
	}
}

// Hooks observe the same genuine conditional scenarios; they grant no source,
// condition, or proposal authority. Parent is fixed before any child starts.
type p05CampaignHooks struct {
	Parent        context.Context
	Start         func(*pairedGraphFaultNetwork) // Optional fresh program, before any captured source.
	Begin         func(*pairedGraphFaultNetwork, uint64, graphCut)
	BeforeRecords func(*pairedGraphFaultNetwork)
	BeforeReopen  func(*pairedGraphFaultNetwork, uint64) // Actual cumulative capture ordinal before the sole cohort stop.
	Finish        func(*pairedGraphFaultNetwork, p05Artifact, [2]uint64, p05CampaignCaptureCharge)
}

type p05CampaignCaptureCharge struct {
	Candidates, Attempts, Work, Output, Retained uint64
}

func p05ConditionalCase(t *testing.T, schedule string, hooks *p05CampaignHooks) {
	t.Helper()
	t.Setenv("RHO_GRAPH_CONDITIONAL_PROCESS", "1")
	var f *pairedGraphFaultNetwork
	var finishCapture func() []p06Phase
	var capture *p05CampaignCapture
	var charge p05CampaignCaptureCharge
	if hooks == nil {
		f = newPairedGraphFaultNetwork(t)
		finishCapture = p05Capture(f)
	} else {
		if hooks.Parent == nil || hooks.Begin == nil || hooks.BeforeRecords == nil || hooks.Finish == nil {
			t.Fatal("incomplete bounded P05 campaign hooks")
		}
		t.Setenv("RHO_GRAPH_PROTOCOL_CAMPAIGN", nativeProtocolCampaignProfile)
		f = newPairedGraphFaultNetworkWithParent(t, hooks.Parent)
		if hooks.Start != nil {
			hooks.Start(f)
		}
		capture = newP05CampaignCapture(f)
		t.Cleanup(capture.close)
		finishCapture = func() []p06Phase {
			var rows []p06Phase
			rows, charge = capture.finish()
			return rows
		}
	}
	proofRecords := func(from [2]uint64) []lifecycleRawRecord {
		if hooks != nil {
			hooks.BeforeRecords(f)
		}
		return p05Records(t, f, from)
	}
	base := f.indexes()
	for stage := 1; stage <= 2; stage++ {
		actor := uint64(1)
		if stage == 2 {
			actor = 4
		}
		p := f.proposal(stage)
		result := f.run(actor, p)
		if !result.decision.commit {
			t.Fatal("unconditional fixture node creation failed")
		}
	}
	if schedule == "temporal-life-value-ABA" {
		if result := f.run(1, f.proposal(3)); !result.decision.commit {
			t.Fatal("unconditional edge creation failed")
		}
	}
	actor := uint64(1)
	if schedule == "write-skew-B-first" {
		actor = 4
	}
	handle, cut, condition := p05Open(t, f, actor, graphReadFresh, graphCut{})
	old := p05Observe(t, f, actor, handle)
	cutWire, err := lifecycleRawCertificate(cut)
	if err != nil {
		t.Fatal(err)
	}
	artifact := p05Artifact{Schedule: schedule, Condition: condition, Cut: cutWire, Old: old}
	if hooks != nil {
		hooks.Begin(f, handle, cut)
	}
	var committed crossProcessResult
	var original allocationProposal
	switch schedule {
	case "write-skew-A-first", "write-skew-B-first":
		other := uint64(4)
		owner, otherOwner := uint64(1), uint64(17)
		if actor == 4 {
			other, owner, otherOwner = 1, 17, 1
		}
		otherHandle, _, _ := p05Open(t, f, other, graphReadAt, cut)
		if p05Cell(t, old, 1, "p", 0).Present || p05Cell(t, old, 17, "p", 0).Present {
			t.Fatal("skew read predicate not genuinely absent")
		}
		original = p05Prepare(t, f, actor, 1, owner, nil)
		from := f.indexes()
		attempt, result := p05AttemptRun(t, f, actor, handle, original)
		artifact.Attempts = append(artifact.Attempts, attempt)
		if attempt.Error != "" || !result.decision.commit {
			t.Fatal("unchanged condition must genuinely COMMIT", attempt, result)
		}
		committed = result
		records := proofRecords(from)
		p05CommitCDC(t, records, result, []graphstate.ComponentKey{{Owner: graphstate.EntityID(owner), Life: graphstate.LifeID(owner + 1), Kind: graphstate.ScalarProperty, Name: "p"}}, 0, 0, 1)
		loser := p05Prepare(t, f, other, 1, otherOwner, nil)
		from = f.indexes()
		refused, result := p05AttemptRun(t, f, other, otherHandle, loser)
		artifact.Attempts = append(artifact.Attempts, refused)
		if !refused.Conflict || refused.Unavailable || result.decision != (crossStatus{}) {
			t.Fatal("historical skew branch admitted", refused, result)
		}
		p05NoEffects(t, proofRecords(from), result.resume.tx, true)
		if retained := p05Observe(t, f, other, otherHandle); !reflect.DeepEqual(retained, old) {
			t.Fatal("held old skew read became current")
		}
		f.call(f.n.all[other], processRequest{op: processP05Close, index: otherHandle})
		artifact.Steps = append(artifact.Steps, p05Step{Name: "conditional-winner", Original: original.wire, Result: p05Result(t, committed), Records: records, Before: old})
	case "empty-range-and-absent-key-insertion":
		if !old.AbsentEntity || !old.AbsentValue || !old.AbsentLife || len(old.Keys) != 0 || len(old.NodeUnique) != 0 || len(old.RelUnique) != 0 {
			t.Fatal("absence/range footprint not empty", old)
		}
		p := p05Prepare(t, f, 1, 2, 0, nil)
		from := f.indexes()
		result := f.run(1, p)
		if !result.decision.commit {
			t.Fatal("actual node/relationship unique insert failed")
		}
		records := proofRecords(from)
		p05CommitCDC(t, records, result, []graphstate.ComponentKey{{Owner: 9, Kind: graphstate.Presence}, {Owner: 9, Life: 10, Kind: graphstate.ScalarProperty, Name: "u"}, {Owner: 3, Kind: graphstate.Presence}, {Owner: 3, Life: 4, Kind: graphstate.ScalarProperty, Name: "u"}}, 2, 2, 2)
		inserted := result
		original = p05Prepare(t, f, 1, 1, 17, nil)
		from = f.indexes()
		refused, result := p05AttemptRun(t, f, 1, handle, original)
		artifact.Attempts = append(artifact.Attempts, refused)
		if !refused.Conflict || result.decision != (crossStatus{}) {
			t.Fatal("old empty/absent footprint admitted", refused)
		}
		p05NoEffects(t, proofRecords(from), result.resume.tx, true)
		artifact.Steps = append(artifact.Steps, p05Step{Name: "unique-insertion", Original: p.wire, Result: p05Result(t, inserted), Records: records, Before: old})
		// Restart owns a NEW complete authenticated cut and evaluates its
		// actual positive/negative footprint before constructing new work.
		restarted, restartedCut, _ := p05Open(t, f, actor, graphReadFresh, graphCut{})
		reevaluated := p05Observe(t, f, actor, restarted)
		if reevaluated.AbsentEntity || reevaluated.AbsentValue || reevaluated.AbsentLife || len(reevaluated.Keys) != 1 || len(reevaluated.NodeUnique) != 1 || reevaluated.NodeUnique[0].Owner != 9 || len(reevaluated.RelUnique) != 1 || reevaluated.RelUnique[0].Owner != 3 {
			t.Fatal("restart did not evaluate the complete actual footprint", reevaluated)
		}
		artifact.Steps[0].Cut, err = lifecycleRawCertificate(restartedCut)
		if err != nil {
			t.Fatal(err)
		}
		artifact.Steps[0].After = reevaluated
		original = p05Prepare(t, f, actor, 1, 17, nil)
		from = f.indexes()
		admitted, result := p05AttemptRun(t, f, actor, restarted, original)
		artifact.Attempts = append(artifact.Attempts, admitted)
		if admitted.Error != "" || !result.decision.commit {
			t.Fatal("genuine re-evaluation/new request must COMMIT", admitted, result)
		}
		committed = result
		records = proofRecords(from)
		// ValueIdentity(5) was genuinely installed as11 by the insertion.
		// Re-evaluation reuses it rather than minting requested fresh5.
		p05CommitCDC(t, records, result, []graphstate.ComponentKey{{Owner: 17, Life: 18, Kind: graphstate.ScalarProperty, Name: "p"}}, 0, 0, 0)
		artifact.Steps = append(artifact.Steps, p05Step{Name: "conditional-re-evaluation", Original: original.wire, Result: p05Result(t, result), Records: records, Before: reevaluated})
		f.call(f.n.all[actor], processRequest{op: processP05Close, index: restarted})
	case "temporal-life-value-ABA":
		previous := old
		for _, kind := range []uint64{4, 5, 6, 7} {
			p := p05Prepare(t, f, 1, kind, 0, nil)
			from := f.indexes()
			result := f.run(1, p)
			if !result.decision.commit {
				t.Fatal("real correction/Close/Reopen failed", kind)
			}
			records := proofRecords(from)
			key := graphstate.ComponentKey{Owner: 3, Life: 4, Kind: graphstate.ScalarProperty, Name: "p"}
			lives, values := 0, 0
			if kind == 4 {
				values = 1
			}
			if kind >= 6 {
				key = graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}
			}
			if kind == 7 {
				lives = 1
			}
			p05CommitCDC(t, records, result, []graphstate.ComponentKey{key}, 0, lives, values)
			after, afterCut := p05Current(t, f, 1, graphReadFresh, graphCut{})
			cutEvidence, err := lifecycleRawCertificate(afterCut)
			if err != nil {
				t.Fatal(err)
			}
			artifact.Steps = append(artifact.Steps, p05Step{Cut: cutEvidence, Name: fmt.Sprint("native-ABA-", kind), Original: p.wire, Result: p05Result(t, result), Records: records, Before: previous, After: after})
			previous = after
		}
		original = p05Prepare(t, f, 1, 1, 17, nil)
		from := f.indexes()
		refused, result := p05AttemptRun(t, f, 1, handle, original)
		artifact.Attempts = append(artifact.Attempts, refused)
		if !refused.Conflict || result.decision != (crossStatus{}) {
			t.Fatal("equal point/scalar masked history/life ABA", refused)
		}
		p05NoEffects(t, proofRecords(from), result.resume.tx, true)
	case "authority-fence-before-prepare":
		original = p05Prepare(t, f, 1, 1, 17, nil)
		from := f.indexes()
		var entered <-chan struct{}
		var release func()
		if capture == nil {
			entered, release = p05GateRegister(t, f, 1)
		} else {
			entered, release = p05CampaignGateRegister(f, 1, capture)
		}
		defer release()
		answers := make(chan graphProcessAnswer, 1)
		runCtx, cancelRun := f.operationContext(30 * time.Second)
		defer cancelRun()
		go func() {
			response, err := f.n.all[1].exchange(runCtx, processRequest{op: processP05Coordinate, index: handle, data: original.wire})
			answers <- graphProcessAnswer{response: response, err: err}
		}()

		if f.parent == nil {
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("conditional preflight/Register boundary missing")
			}
		} else {
			waitCtx, cancelWait := f.operationContext(10 * time.Second)
			defer cancelWait()
			select {
			case <-entered:
				f.requireActive()
			case <-waitCtx.Done():
				t.Fatal("conditional Register hold exceeded original deadline", waitCtx.Err())
			}
		}
		before := f.call(f.n.all[4], processRequest{op: processStateOp})
		fence := p05Prepare(t, f, 4, 101, 0, nil)
		f.groupEvent(f.n.group(4), 4, processRequest{op: processPropose, data: mustP05Proposal(t, fence)})
		command, err := decodeRangeMoveCommand(fence.wire)
		if err != nil {
			t.Fatal(err)
		}
		abort := p05Prepare(t, f, 4, 102, 0, command.intent)
		f.groupEvent(f.n.group(4), 4, processRequest{op: processPropose, data: mustP05Proposal(t, abort)})
		after := f.call(f.n.all[4], processRequest{op: processStateOp})
		if !bytes.Equal(before.image, after.image) || after.applied <= before.applied {
			t.Fatal("authority control fixture must change actual prefix while graph image stays identical")
		}
		for index := before.applied + 1; index <= after.applied; index++ {
			r := f.call(f.n.all[4], processRequest{op: processEntryRecord, index: index})
			status, err := decodeRangeMoveStatus(r.outcome)
			if err != nil || status.result != rangeMoveApplied || status.phase != rangeMoveFence && status.phase != rangeMoveAbort || len(r.changes) != 0 {
				t.Fatal("actual range fence/abort evidence", err, status)
			}
			artifact.Controls = append(artifact.Controls, p06LocalRecord{Partition: 8, Index: index, Outcome: r.outcome, CDC: r.changes})
		}
		release()
		var answer graphProcessAnswer
		if f.parent == nil {
			answer = <-answers
		} else {
			select {
			case answer = <-answers:
				f.requireActive()
			case <-runCtx.Done():
				t.Fatal("conditional original exceeded its admitted deadline", runCtx.Err())
			}
		}
		if answer.err != nil {
			t.Fatal("original conditional caller", answer.err)
		}
		var attempt p05Attempt
		if err := json.Unmarshal(answer.response.aux, &attempt); err != nil {
			t.Fatal(err)
		}
		result, err := decodeGraphProcessCoordinatorResult(processResponse{aux: attempt.Result})
		if err != nil {
			t.Fatal(err)
		}
		artifact.Attempts = append(artifact.Attempts, attempt)
		if attempt.Error != "" || result.decision.commit || result.decision.phase != crossDecide {
			t.Fatal("intervening native authority prefix obtained YES/COMMIT", attempt, result)
		}
		noVotes, resolves := 0, 0
		for _, record := range proofRecords(from) {
			if len(record.CDC) != 0 {
				t.Fatal("authority refusal installed business effects")
			}
			status, err := decodeCrossStatus(record.Outcome)
			if err != nil || status.tx != result.resume.tx {
				continue
			}
			if status.digest != result.decision.digest {
				t.Fatal("authority refusal changed original condition identity")
			}
			if status.phase == crossPrepare && status.ns.partition == 8 {
				if status.yes || status.why != reasonReadConflict {
					t.Fatal("changed participant did not retain exact prefix-conflict NO", status)
				}
				noVotes++
			}
			if status.phase == crossResolve {
				if status.commit || status.round != result.decision.round {
					t.Fatal("authority refusal changed durable ABORT", status)
				}
				resolves++
			}
		}
		if noVotes != 1 || resolves != 2 {
			t.Fatal("authority refusal omitted exact participant/decision closure", noVotes, resolves)
		}
		committed = result

	}
	retained := p05Observe(t, f, actor, handle)
	if !reflect.DeepEqual(retained, old) {
		t.Fatal("historical read changed after concurrent work")
	}
	artifact.Retained = retained
	f.call(f.n.all[actor], processRequest{op: processP05Close, index: handle})
	// Closed handles refuse NEW admission, while registered originals use
	// their retained exact manifests/conditions and need no live read owner.
	closedProposal := p05Prepare(t, f, actor, 1, 17, nil)
	from := f.indexes()
	closed, result := p05AttemptRun(t, f, actor, handle, closedProposal)
	if !closed.Closed || result.decision != (crossStatus{}) {
		t.Fatal("closed read admitted new work", closed)
	}
	p05NoEffects(t, proofRecords(from), result.resume.tx, true)
	current, currentCut := p05Current(t, f, actor, graphReadFresh, graphCut{})
	artifact.Current = current
	if schedule == "write-skew-A-first" || schedule == "write-skew-B-first" {
		winner, loser, valueID := uint64(1), uint64(17), uint64(5)
		if actor == 4 {
			winner, loser, valueID = 17, 1, 21
		}
		for _, at := range []int64{0, 2} {
			cell := p05Cell(t, current, winner, "p", at)
			if !cell.Present || cell.I64 != 5 || cell.Handle != valueID || cell.Revision != committed.decision.round || cell.Provenance != 101 || p05Cell(t, current, loser, "p", at) != p05Cell(t, old, loser, "p", at) {
				t.Fatal("skew winner scalar/original revision or unchanged loser", at, cell)
			}
		}
	}
	if schedule == "temporal-life-value-ABA" {
		oldCell, newCell := p05Cell(t, old, 3, "p", 2), p05Cell(t, current, 3, "p", 2)
		if oldCell.I64 != 5 || newCell.I64 != 5 || oldCell.Handle != newCell.Handle || oldCell.Revision == newCell.Revision || p05Cell(t, old, 3, "p", 0) != p05Cell(t, current, 3, "p", 0) {
			t.Fatal("ABA must retain point equality while changing actual interval revision")
		}
		if p05Presence(t, old, 1, 2).Life != 2 || p05Presence(t, current, 1, 2).Life != 7 || !p05Presence(t, current, 3, 0).Effective || p05Presence(t, current, 3, 2).Effective {
			t.Fatal("life ABA/partial-overlap effective edge proof")
		}
	}
	if schedule == "empty-range-and-absent-key-insertion" && (current.AbsentEntity || current.AbsentValue || current.AbsentLife || len(current.Keys) != 1 || len(current.NodeUnique) != 1 || current.NodeUnique[0].Owner != 9 || len(current.RelUnique) != 1 || current.RelUnique[0].Owner != 3) {
		t.Fatal("insertion omitted native negative/range footprint", current)
	}
	for i := range artifact.Steps {
		if i == len(artifact.Steps)-1 {
			artifact.Steps[i].Cut, err = lifecycleRawCertificate(currentCut)
			if err != nil {
				t.Fatal(err)
			}
			artifact.Steps[i].After = current
		}
	}
	if committed.resume.request != (requestID{}) {
		resume, err := encodeP05Resume(committed.resume)
		if err != nil {
			t.Fatal(err)
		}
		from = f.indexes()
		r := f.call(f.n.all[actor], processRequest{op: processP05Resume, data: resume})
		var replay p05Attempt
		if err := json.Unmarshal(r.aux, &replay); err != nil {
			t.Fatal(err)
		}
		replayed, err := decodeGraphProcessCoordinatorResult(processResponse{aux: replay.Result})
		if err != nil || replayed != committed || replay.Error != "" {
			t.Fatal("closed-handle original identity/recovery", err, replay, replayed, committed)
		}
		p05NoEffects(t, proofRecords(from), committed.resume.tx, false)
		artifact.Attempts = append(artifact.Attempts, replay)
		if schedule == "write-skew-B-first" {
			changedHandle, _, _ := p05Open(t, f, actor, graphReadAt, currentCut)
			from = f.indexes()
			changed, result := p05AttemptRun(t, f, actor, changedHandle, original)
			if !changed.Invalid || result.decision != (crossStatus{}) {
				t.Fatal("same original request accepted another authenticated condition", changed)
			}
			p05NoEffects(t, proofRecords(from), committed.resume.tx, false)
			artifact.Attempts = append(artifact.Attempts, changed)
			f.call(f.n.all[actor], processRequest{op: processP05Close, index: changedHandle})
		}
		if schedule == "write-skew-A-first" {
			if hooks != nil && hooks.BeforeReopen != nil {
				capture.mu.Lock()
				ordinal := uint64(len(capture.frames))
				capture.mu.Unlock()
				hooks.BeforeReopen(f, ordinal)
			}
			artifact.Processes = append(artifact.Processes, f.stopAll()...)
			if hooks != nil && hooks.Start != nil {
				// The genuine same-volume constructor below elects1/4. Do not
				// retain earlier2/6 routes across replacement of all six children.
				f.n.leaders = [2]uint64{1, 4}
			}
			artifact.Processes = append(artifact.Processes, p05Reopen(t, f)...)
			if capture != nil {
				capture.attach(f)
			}
			from = f.indexes()
			r = f.call(f.n.all[actor], processRequest{op: processP05Resume, data: resume})
			if err := json.Unmarshal(r.aux, &replay); err != nil {
				t.Fatal(err)
			}
			recovered, err := decodeGraphProcessCoordinatorResult(processResponse{aux: replay.Result})
			if err != nil || recovered != committed || replay.Error != "" {
				t.Fatal("same-directory original condition/decision changed", err, replay)
			}
			p05NoEffects(t, proofRecords(from), committed.resume.tx, false)
			artifact.Attempts = append(artifact.Attempts, replay)
		}
	}
	if schedule == "write-skew-B-first" {
		expiring, _, _ := p05Open(t, f, actor, graphReadAt, currentCut, 5)
		f.call(f.n.all[actor], processRequest{op: processP05Close, index: expiring, entity: 1})
		freshProposal := p05Prepare(t, f, actor, 1, 1, nil)
		from = f.indexes()
		expired, result := p05AttemptRun(t, f, actor, expiring, freshProposal)
		if !expired.Expired || result.decision != (crossStatus{}) {
			t.Fatal("actual expired ownership admitted new work", expired)
		}
		p05NoEffects(t, proofRecords(from), result.resume.tx, true)
		artifact.Attempts = append(artifact.Attempts, expired)
		f.call(f.n.all[actor], processRequest{op: processP05Close, index: expiring})
	}
	oldAfter, _ := p05Current(t, f, actor, graphReadAt, cut)
	if !reflect.DeepEqual(oldAfter, old) {
		t.Fatal("reopened old certified cut substituted current data")
	}
	unchanged, _ := p05Current(t, f, actor, graphReadAt, currentCut)
	if !reflect.DeepEqual(unchanged, current) {
		t.Fatal("duplicate/original recovery changed graph/postings/census")
	}
	artifact.Phases = finishCapture()
	artifact.Records = proofRecords(base)
	if hooks != nil {
		hooks.Finish(f, artifact, base, charge)
		return
	}
	wire, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.n.dir, "p05-"+schedule+".json"), wire, 0600); err != nil {
		t.Fatal(err)
	}
}

func mustP05Proposal(t *testing.T, p allocationProposal) []byte {
	t.Helper()
	wire, err := encodeCertifiedProcessProposal(p)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}
func p05GateRegister(t *testing.T, f *pairedGraphFaultNetwork, actor uint64) (<-chan struct{}, func()) {
	entered := make(chan struct{}, 1)
	released := make(chan struct{})
	var once, end sync.Once
	hub := f.n.all[actor].mux.hub
	hub.mu.Lock()
	hub.beforeRelay = func(ctx context.Context, origin, partition uint64, wire []byte) error {
		q, err := decodeGraphSourceRequest(wire)
		if err != nil {
			return err
		}
		if origin != actor || partition != 3 || q.kind != sourceSubmit {
			return nil
		}
		p, err := decodeGraphSourceProposal(q.command)
		if err != nil {
			return err
		}
		if !isCrossPartitionWire(p.wire) {
			return nil
		}
		command, _, err := decodeCrossCommand(p.wire, defaultMaterializerLimits())
		if err != nil {
			return err
		}
		if command.phase != crossRegister {
			return nil
		}
		once.Do(func() { entered <- struct{}{} })
		select {
		case <-released:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	hub.mu.Unlock()
	return entered, func() { end.Do(func() { close(released); hub.mu.Lock(); hub.beforeRelay = nil; hub.mu.Unlock() }) }
}
func p05Reopen(t *testing.T, f *pairedGraphFaultNetwork) []pairedFaultProcess {
	t.Helper()
	old := f.n.all
	fresh := make(map[uint64]*graphProcess)
	var events []pairedFaultProcess
	for id := uint64(1); id <= 6; id++ {
		if old[id].cmd.ProcessState == nil || !old[id].closed {
			t.Fatal("P05 reopen before actual child reap")
		}
		f.requireActive()
		fresh[id] = startGraphProcess(t, filepath.Join(f.n.dir, fmt.Sprint(id)), id, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_LIFECYCLE_PROCESS=1", "RHO_GRAPH_PAIRED_PROCESS=1", "RHO_GRAPH_CONDITIONAL_PROCESS=1", "RHO_GRAPH_PROCESS_REOPEN=1")
		f.incarnations = append(f.incarnations, fresh[id])
	}
	f.retiredBootstrap += f.n.groups[0].delivered + f.n.groups[1].delivered
	attachGraphProcessMuxes(t, fresh)
	f.n.all = fresh
	f.n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: fresh[1], 2: fresh[2], 3: fresh[3]}}
	f.n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: fresh[4], 5: fresh[5], 6: fresh[6]}}
	f.mu.Lock()
	clear(f.down)
	f.mu.Unlock()
	f.installFilter()
	for _, leader := range []uint64{1, 4} {
		f.groupEvent(f.n.group(leader), leader, processRequest{op: processCampaign})
		f.bootstrapRead(leader, observeConfiguration, 0)
		f.heal(leader)
	}
	for id := uint64(1); id <= 6; id++ {
		raw, err := encodeProcessResponse(f.call(fresh[id], processRequest{op: processStateOp}))
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, pairedFaultProcess{EventID: f.event(), Action: "reopen", Role: "replica", Member: id, Instance: pairedGraphFaultInstance(id, fresh[id]), PreviousInstance: pairedGraphFaultInstance(id, old[id]), Volume: filepath.Join(f.n.dir, fmt.Sprint(id)), Ready: true, RecoveryRaw: raw})
	}
	return events
}

func p05Result(t *testing.T, result crossProcessResult) []byte {
	t.Helper()
	wire, err := encodeGraphProcessCoordinatorResult(result, nil)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}
func p05Capture(f *pairedGraphFaultNetwork) func() []p06Phase {
	var mu sync.Mutex
	var frames []p06Phase
	hub := f.n.all[1].mux.hub
	hub.mu.Lock()
	hub.afterRelay = func(ctx context.Context, origin, partition uint64, request, reply []byte) error {
		q, err := decodeGraphSourceRequest(request)
		if err != nil {
			return err
		}
		if q.kind != sourceSubmit && q.kind != sourceCertifiedOpen {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if len(frames) >= 128 {
			return errLimit
		}
		frames = append(frames, p06Phase{origin, partition, bytes.Clone(request), bytes.Clone(reply)})
		return nil
	}
	hub.mu.Unlock()
	return func() []p06Phase {
		hub.mu.Lock()
		hub.afterRelay = nil
		hub.mu.Unlock()
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(frames)
	}
}

// The fresh capture uses the fixture's existing whole-cohort IPC ceiling and
// the legacy128 retained-frame ceiling. Delivered bytes are charged BEFORE
// decoding or taking ownership; diagnostic refusal never fabricates a reply.
type p05CampaignCapture struct {
	mu      sync.Mutex
	f       *pairedGraphFaultNetwork
	charge  p05CampaignCaptureCharge
	frames  []p06Phase
	slots   []p05CampaignCaptureSlot
	closed  bool
	refused bool
}

type p05CampaignCaptureSlot struct {
	hub   *graphProcessHub
	prior func(context.Context, uint64, uint64, []byte, []byte) error
}

func newP05CampaignCapture(f *pairedGraphFaultNetwork) *p05CampaignCapture {
	// Prepay the fixed selected-row and two-hub directory headers before their
	// allocations. Exact capacity prevents append growth from hiding ownership.
	const headers = 128*64 + 2*64
	c := &p05CampaignCapture{f: f, charge: p05CampaignCaptureCharge{Output: headers, Retained: headers}}
	c.frames = make([]p06Phase, 0, 128)
	c.slots = make([]p05CampaignCaptureSlot, 0, 2)
	c.attach(f)
	return c
}

func (c *p05CampaignCapture) attach(f *pairedGraphFaultNetwork) {
	// A cohort reopen has already reaped/drained the old hub. Continue this SAME
	// consumptive owner on the new actual pipes, without resetting its ledger.
	hub := f.n.all[1].mux.hub
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || len(c.slots) >= 2 {
		f.t.Fatal("P05 capture admits only original and same-volume reopened hubs")
	}
	for _, slot := range c.slots {
		if slot.hub == hub {
			f.t.Fatal("P05 capture already owns this actual hub")
		}
	}
	hub.mu.Lock()
	prior := hub.afterRelay
	hub.afterRelay = func(ctx context.Context, origin, partition uint64, request, reply []byte) error {
		if prior != nil {
			if err := prior(ctx, origin, partition, request, reply); err != nil {
				return err
			}
		}
		return c.after(ctx, origin, partition, request, reply)
	}
	hub.mu.Unlock()
	c.slots = append(c.slots, p05CampaignCaptureSlot{hub: hub, prior: prior})
}

func (c *p05CampaignCapture) after(ctx context.Context, origin, partition uint64, request, reply []byte) error {
	const candidates = 2 * (processDeliveries + 96*4*1024) // At most before+after per admitted fixture RPC.
	const retainedFrames = 128                             // Same selected-row ceiling as p05Capture.
	const frameWork = 4096 + 64*processFrameBytes
	const frameOutput = 8192 + 16*processFrameBytes
	const frameRetained = 64 + 2*processFrameBytes
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		c.refused = true
		return err
	}
	if c.closed || c.charge.Candidates >= candidates || len(request) > processFrameBytes || len(reply) > processFrameBytes {
		c.refused = true
		return errLimit
	}
	work := uint64(4096 + 32*(len(request)+len(reply)))
	if work > uint64(candidates)*frameWork-c.charge.Work {
		c.refused = true
		return errLimit
	}
	c.charge.Candidates++
	c.charge.Work += work
	q, err := decodeGraphSourceRequest(request)
	if err != nil {
		c.refused = true
		return err
	}
	if q.kind != sourceSubmit && q.kind != sourceCertifiedOpen {
		return nil
	}
	output := uint64(8192 + 8*(len(request)+len(reply)))
	retained := uint64(64 + len(request) + len(reply))
	if c.charge.Attempts >= retainedFrames || output > uint64(candidates)*frameOutput-c.charge.Output || retained > retainedFrames*frameRetained-c.charge.Retained {
		c.refused = true
		return errLimit
	}
	c.charge.Attempts++
	c.charge.Output += output
	c.charge.Retained += retained
	r, err := decodeGraphWireResponse(reply)
	if err != nil || r.kind != q.kind || r.sequence != q.sequence || r.nonce != q.nonce || r.peer.scope.partition != partition {
		c.refused = true
		return errors.Join(errCorrupt, err)
	}
	ownedRequest, ownedReply := make([]byte, len(request)), make([]byte, len(reply))
	copy(ownedRequest, request)
	copy(ownedReply, reply)
	c.frames = append(c.frames, p06Phase{origin, partition, ownedRequest, ownedReply})
	return nil
}

func (c *p05CampaignCapture) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	// Only the after hook belongs to this owner. The native before/failed gate
	// remains independently owned and is restored by its own LIFO cleanup.
	for _, slot := range c.slots {
		slot.hub.mu.Lock()
		slot.hub.afterRelay = slot.prior
		slot.hub.mu.Unlock()
	}
	c.closed = true
}

func (c *p05CampaignCapture) finish() ([]p06Phase, p05CampaignCaptureCharge) {
	c.f.drain()
	c.close()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refused {
		c.f.t.Fatal("P05 full-frame capture exceeded its original prepaid ledger")
	}
	rows := c.frames // Transfer the already-owned full rows; no second copy/arena.
	c.frames = nil
	return rows, c.charge
}

func p05CampaignGateRegister(f *pairedGraphFaultNetwork, actor uint64, capture *p05CampaignCapture) (<-chan struct{}, func()) {
	entered := make(chan struct{}, 1)
	released := make(chan struct{})
	var once, end sync.Once
	hub := f.n.all[actor].mux.hub
	hub.mu.Lock()
	prior := hub.beforeRelay
	hub.beforeRelay = func(ctx context.Context, origin, partition uint64, wire []byte) error {
		if err := capture.prepayGate(ctx, wire); err != nil {
			return err
		}
		q, err := decodeGraphSourceRequest(wire)
		if err != nil {
			return err
		}
		if origin == actor && partition == 3 && q.kind == sourceSubmit {
			p, err := decodeGraphSourceProposal(q.command)
			if err != nil {
				return err
			}
			if isCrossPartitionWire(p.wire) {
				command, _, err := decodeCrossCommand(p.wire, defaultMaterializerLimits())
				if err != nil {
					return err
				}
				if command.phase == crossRegister {
					once.Do(func() { entered <- struct{}{} })
					select {
					case <-released:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		}
		// Run the existing native selector AFTER the genuine authority hold has
		// released, so it still observes the actual queued Register command.
		if prior != nil {
			return prior(ctx, origin, partition, wire)
		}
		return nil
	}
	hub.mu.Unlock()
	return entered, func() {
		end.Do(func() {
			close(released)
			hub.mu.Lock()
			hub.beforeRelay = prior
			hub.mu.Unlock()
		})
	}
}

func (c *p05CampaignCapture) prepayGate(ctx context.Context, wire []byte) error {
	// The authority hold performs its own full canonical proposal/command
	// decode. Fund that temporary ownership BEFORE decode, on the same ledger
	// as capture; no fresh decoder allowance is created for a retry.
	const candidates = 2 * (processDeliveries + 96*4*1024)
	const frameWork = 4096 + 64*processFrameBytes
	const frameOutput = 8192 + 16*processFrameBytes
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		c.refused = true
		return err
	}
	work := uint64(4096 + 32*len(wire))
	output := uint64(8192 + 16*len(wire))
	if c.closed || c.charge.Candidates >= candidates || len(wire) > processFrameBytes || work > uint64(candidates)*frameWork-c.charge.Work || output > uint64(candidates)*frameOutput-c.charge.Output {
		c.refused = true
		return errLimit
	}
	c.charge.Candidates++
	c.charge.Work += work
	c.charge.Output += output
	return nil
}

func TestP05ObservationReuseKeepsActualCutAndBounds(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, parts := n.crossPlan(ops, claims, 24)
	n.crossResolve(n.crossCommit(tx, parts))
	c := remoteCertifiedFixture(t, n)
	held, err := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := held.Close(); err != nil {
			t.Error(err)
		}
	}()
	raw, err := held.ReadView()
	if err != nil {
		t.Fatal(err)
	}
	budget := held.sources[0].(*remoteGraphSource).endpoint.budget
	observe := func() graphstate.ReadView {
		t.Helper()
		v, err := newNativeObservationRead(raw, budget.output.(*graphstate.OutputBudget), c.host.machine.limits.graph.Planner.Component, []context.Context{held.sources[0].(*remoteGraphSource).ctx, held.sources[1].(*remoteGraphSource).ctx})
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	view := observe()
	before, err := view.Entity(t.Context(), 3)
	if err != nil || !before.Found {
		t.Fatal(before, err)
	}
	calls := budget.calls
	duplicate, err := view.Entity(t.Context(), 3)
	if err != nil || !reflect.DeepEqual(duplicate, before) {
		t.Fatal(duplicate, err)
	}
	if budget.calls != calls {
		t.Fatalf("same observation repeats actual immutable entity fetch: calls %d -> %d", calls, budget.calls)
	}
	for range 2 {
		absent, err := view.Entity(t.Context(), 9)
		if err != nil || absent.Found {
			t.Fatal("phantom entity", absent, err)
		}
	}
	if _, err := view.Entity(t.Context(), 0); !errors.Is(err, graphstore.ErrInvalid) {
		t.Fatal("invalid ID", err)
	}
	l := c.host.machine.limits.graph.Planner.Component
	lifetimes := []context.Context{held.sources[0].(*remoteGraphSource).ctx, held.sources[1].(*remoteGraphSource).ctx}
	zero, err := graphstate.NewOutputBudget(0)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := newNativeObservationRead(nil, zero, l, lifetimes); got != nil || !errors.Is(err, errInvalid) {
		t.Fatal("nil view", got, err)
	}
	if got, err := newNativeObservationRead(raw, nil, l, lifetimes); got != nil || !errors.Is(err, errInvalid) {
		t.Fatal("nil ledger", got, err)
	}
	if got, err := newNativeObservationRead(raw, zero, l, lifetimes); got != nil || !errors.Is(err, errLimit) {
		t.Fatal("exhausted original output", got, err)
	}
	if got, err := newNativeObservationRead(raw, budget.output.(*graphstate.OutputBudget), l, []context.Context{nil}); got != nil || !errors.Is(err, errInvalid) {
		t.Fatal("nil source context", got, err)
	}
	probe := &p05ComponentReadProbe{ReadView: raw}
	pageView, err := newNativeObservationRead(probe, budget.output.(*graphstate.OutputBudget), l, lifetimes)
	if err != nil {
		t.Fatal(err)
	}
	window := ops[0].Scope
	query := graphstate.ComponentQuery{Key: graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}, Window: window}
	bounds := graphstate.ReadBudget{Rows: 64, Bytes: 64 << 10}
	first, err := pageView.ComponentPage(t.Context(), query, 0, bounds)
	if err != nil || !first.Complete {
		t.Fatal(first, err)
	}
	pageCalls := probe.calls
	repeated, err := pageView.ComponentPage(t.Context(), query, 0, bounds)
	if err != nil || !reflect.DeepEqual(repeated, first) || probe.calls != pageCalls {
		t.Fatal("exact page not reused", probe.calls, pageCalls, err)
	}
	// Pieces exposes a defensive slice; modifying it must not alter captured State.
	pieces := repeated.Data.Pieces()
	if len(pieces) > 0 {
		pieces[0] = state.Piece{}
	}
	repeated, err = pageView.ComponentPage(t.Context(), query, 0, bounds)
	if err != nil || !reflect.DeepEqual(repeated, first) {
		t.Fatal("caller changed immutable page", err)
	}
	changed := bounds
	changed.Bytes--
	if _, err := pageView.ComponentPage(t.Context(), query, 0, changed); err != nil || probe.calls != pageCalls+1 {
		t.Fatal("changed budget hit", err, probe.calls)
	}
	pointWindow, err := temporal.Point(mustP05Point(t, window, 0))
	if err != nil {
		t.Fatal(err)
	}
	changedQuery := query
	changedQuery.Window = pointWindow
	if _, err := pageView.ComponentPage(t.Context(), changedQuery, 0, bounds); err != nil || probe.calls != pageCalls+2 {
		t.Fatal("changed window hit", err, probe.calls)
	}
	if _, err := pageView.ComponentPage(t.Context(), query, 99, bounds); !errors.Is(err, graphstore.ErrInvalid) || probe.calls != pageCalls+3 {
		t.Fatal("forged cursor hit", err, probe.calls)
	}
	absentQuery := query
	absentQuery.Key.Owner = 9
	probe.errOnce = errLimit
	if _, err := pageView.ComponentPage(t.Context(), absentQuery, 0, bounds); !errors.Is(err, errLimit) {
		t.Fatal("lost source sentinel", err)
	}
	probe.incompleteOnce = true
	incomplete, err := pageView.ComponentPage(t.Context(), absentQuery, 0, bounds)
	if err != nil || incomplete.Complete {
		t.Fatal("partial page", incomplete, err)
	}
	callsAfterPartial := probe.calls
	complete, err := pageView.ComponentPage(t.Context(), absentQuery, 0, bounds)
	if err != nil || !complete.Complete || probe.calls != callsAfterPartial+1 {
		t.Fatal("error/partial cached", complete, err, probe.calls)
	}
	lost, stopSource := context.WithCancel(t.Context())
	ownedContexts := append([]context.Context(nil), lifetimes...)
	ownedContexts[0] = lost
	lifetimeView, err := newNativeObservationRead(raw, budget.output.(*graphstate.OutputBudget), l, ownedContexts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lifetimeView.Entity(t.Context(), 3); err != nil {
		t.Fatal(err)
	}
	ownedContexts[0] = t.Context()
	stopSource()
	if _, err := lifetimeView.Entity(t.Context(), 3); !errors.Is(err, context.Canceled) || !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("cached result bypassed original source cancellation/owned contexts", err)
	}
	scope := ops[0].Scope
	oldProjection, err := graphstate.Project(t.Context(), view, 3, mustP05Point(t, scope, 0), graphstate.Effective, c.host.machine.limits.graph.Planner)
	if err != nil || !oldProjection.Active {
		t.Fatal(oldProjection, err)
	}
	tx, parts = n.crossPlan([]graphstate.Operation{{Kind: graphstate.Correct, Owner: 1, Life: 2, Scope: scope, Present: false}}, nil, 25)
	n.crossResolve(n.crossCommit(tx, parts))
	// A new invocation reuses no retained result from the preceding observation.
	after := observe()
	oldAgain, err := graphstate.Project(t.Context(), after, 3, mustP05Point(t, scope, 0), graphstate.Effective, c.host.machine.limits.graph.Planner)
	if err != nil || !reflect.DeepEqual(oldAgain, oldProjection) {
		t.Fatal("held cut became current", oldAgain, err)
	}
	current, err := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := current.Close(); err != nil {
			t.Error(err)
		}
	}()
	currentView, err := current.ReadView()
	if err != nil {
		t.Fatal(err)
	}
	currentBudget := current.sources[0].(*remoteGraphSource).endpoint.budget.output.(*graphstate.OutputBudget)
	currentView, err = newNativeObservationRead(currentView, currentBudget, c.host.machine.limits.graph.Planner.Component, []context.Context{current.sources[0].(*remoteGraphSource).ctx, current.sources[1].(*remoteGraphSource).ctx})
	if err != nil {
		t.Fatal(err)
	}
	now, err := graphstate.Project(t.Context(), currentView, 3, mustP05Point(t, scope, 0), graphstate.Effective, c.host.machine.limits.graph.Planner)
	if err != nil || now.Active {
		t.Fatal("current endpoint mask omitted", now, err)
	}
	canceled, stop := context.WithCancel(t.Context())
	stop()
	if _, err := view.Entity(canceled, 3); !errors.Is(err, context.Canceled) {
		t.Fatal("cached result bypassed cancellation", err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := view.Entity(t.Context(), 3); !errors.Is(err, graphstore.ErrClosed) && !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("closed original owner reused", err)
	}

}

func mustP05Point(t *testing.T, scope temporal.Scope, value int64) temporal.Position {
	t.Helper()
	p, err := temporal.RationalPosition(scope.Axis(), temporal.RationalInt64(value))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestP05FullObservationsKeepSameConditionalSourceAndOriginalAllowance(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := collectedGraphNetworkWithSchemas(t, nil, conditionalGraphSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 1}, raftlog.ApplicationRetentionConfig{}, p05Schemas())
	ops, claims := crossEdgeFixture(t, n)
	grant := claims[0].grant
	ops = append(ops, graphstate.Operation{Kind: graphstate.Set, Owner: 3, Life: 4, Name: "p", Scope: ops[0].Scope, Value: graphstate.I64(5), ValueID: 5})
	claims = append(claims, freshBinding{role: valueBinding, id: 5, grant: grant})
	tx, parts := n.crossPlan(ops, claims, 24)
	n.crossResolve(n.crossCommit(tx, parts))
	c := remoteCertifiedFixture(t, n)
	held, err := c.OpenForUpdate(t.Context(), graphReadRequest{mode: graphReadFresh})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := held.Close(); err != nil {
			t.Error(err)
		}
	}()
	sessions := &graphProcessCertifiedSessions{coordinator: c, owners: map[uint64]graphProcessCertifiedOwner{1: {read: held.read}}}
	p05 := newP05ProcessSessions(t.Context(), sessions, nil)
	p05.owners[1] = p05ProcessOwner{read: held}
	observe := func(handle uint64) p05Observation {
		t.Helper()
		wire, err := p05.observe(t.Context(), processRequest{index: handle})
		if err != nil {
			t.Fatal("full actual observation", handle, err)
		}
		var got p05Observation
		if err := json.Unmarshal(wire, &got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	before := observe(1)
	budget := held.read.sources[0].(*remoteGraphSource).endpoint.budget
	firstCalls := budget.calls
	t.Logf("first complete actual P05 observation calls=%d", firstCalls)
	scope, err := p05Span(ops[0].Scope.Axis())
	if err != nil {
		t.Fatal(err)
	}
	for i, operations := range [][]graphstate.Operation{
		{{Kind: graphstate.Set, Owner: 3, Life: 4, Scope: scope, Name: "p", Value: graphstate.I64(6), ValueID: 6}},
		{{Kind: graphstate.Set, Owner: 3, Life: 4, Scope: scope, Name: "p", Value: graphstate.I64(5), ValueID: 5}},
		{{Kind: graphstate.Close, Owner: 1, Life: 2, Scope: scope}},
		{{Kind: graphstate.Reopen, Owner: 1, Life: 7, Scope: scope}},
	} {
		var fresh []freshBinding
		if i == 0 {
			fresh = []freshBinding{{role: valueBinding, id: 6, grant: grant}}
		}
		if i == 3 {
			fresh = []freshBinding{{role: lifeBinding, owner: 1, id: 7, grant: grant}}
		}
		tx, parts = n.crossPlan(operations, fresh, uint64(25+i))
		n.crossResolve(n.crossCommit(tx, parts))
	}
	retained := observe(1)
	if !reflect.DeepEqual(before, retained) {
		t.Fatal("same held conditional observation became current")
	}
	t.Logf("full genuine observations shared calls first=%d final=%d cap=%d", firstCalls, budget.calls, budget.maxCalls)
	if budget.calls >= budget.maxCalls || budget.maxCalls != 1024 {
		t.Fatal("source grant exhausted or changed", budget.calls, budget.maxCalls)
	}
	current, err := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := current.Close(); err != nil {
			t.Error(err)
		}
	}()
	sessions.owners[2] = graphProcessCertifiedOwner{read: current}
	now := observe(2)
	if reflect.DeepEqual(now, before) || p05Presence(t, now, 1, 2).Life != 7 || p05Presence(t, before, 1, 2).Life != 2 {
		t.Fatal("full current/old ABA projections not distinguished")
	}
}

// Probe the real immutable reader's forwarding doors. Only injected error and
// partial flags are synthetic adversarial inputs to the owning reuse boundary.
type p05ComponentReadProbe struct {
	graphstate.ReadView
	calls          int
	errOnce        error
	incompleteOnce bool
}

func (p *p05ComponentReadProbe) ComponentPage(ctx context.Context, q graphstate.ComponentQuery, c graphstate.Cursor, b graphstate.ReadBudget) (graphstate.ComponentPage, error) {
	p.calls++
	if p.errOnce != nil {
		err := p.errOnce
		p.errOnce = nil
		return graphstate.ComponentPage{}, err
	}
	out, err := p.ReadView.ComponentPage(ctx, q, c, b)
	if err == nil && p.incompleteOnce {
		p.incompleteOnce = false
		out.Complete = false
		out.Next = 99
	}
	return out, err
}
