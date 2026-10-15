package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// No caller cursor is accepted. Query continuation belongs to the actual
// authenticated read, and only an opaque native continuation crosses renewal.
type nativeSelectorsInput struct {
	Version              uint8
	Action               string
	Request              [16]byte
	Handle, Sequence     uint64
	From, Through        uint64 `json:",omitzero"`
	Mode                 graphReadMode
	Cut, Original, Token []byte
	Partitions, IDs      []uint64
	Local, Transaction   [16]byte
	Coordinator          uint64
}
type nativeSelectorsReply struct {
	Version              uint8
	Action               string
	Handle, Sequence     uint64
	Cut, Original, Token []byte
	Points               []nativeLifecycleEntity
	Candidates           []uint64
	EOF, Replay          bool
	Work                 graphstore.PageWork
	Charged              nativeColumnWork
	Starts, Calls        uint64
	Nanos                int64
	Failure              byte                            `json:",omitzero"` // actual scope refusal, never an empty answer or source authority
	Index                *nativeLateCorrectionIndexReply `json:",omitzero"` // exact fresh11 only; historical bytes unchanged
}

type nativeSelectorsRemoteError struct {
	cause   error
	failure byte
}

func (e nativeSelectorsRemoteError) Error() string { return e.cause.Error() }
func (e nativeSelectorsRemoteError) Unwrap() error { return e.cause }
func (e nativeSelectorsRemoteError) Is(target error) bool {
	return e.failure == 1 && (target == errGraphCutScope || target == graphstore.ErrScopeUnavailable)
}

func nativeSelectorsInputWire(q nativeSelectorsInput) ([]byte, error) {
	if q.Version != 1 {
		return nil, errInvalid
	}
	body, err := json.Marshal(q)
	if err != nil || len(body) > 64<<10 {
		return nil, errors.Join(errLimit, err)
	}
	return append([]byte(nativeSelectorsMarker), body...), nil
}
func nativeSelectorsInputDecode(r processRequest) (q nativeSelectorsInput, err error) {
	if r.op != processCostObserve || r.index != 0 || r.entity != 0 || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 || len(r.data) > 64<<10+len(nativeSelectorsMarker) || !bytes.HasPrefix(r.data, []byte(nativeSelectorsMarker)) {
		return q, errInvalid
	}
	if err = nativeFeedDecodeJSON(r.data[len(nativeSelectorsMarker):], &q); err != nil {
		return q, err
	}
	canonical, err := nativeSelectorsInputWire(q)
	if err != nil || !bytes.Equal(canonical, r.data) || !graphE15ReadPartitionsValid(q.Partitions) || len(q.IDs) > 16 {
		return q, errors.Join(errInvalid, err)
	}
	for _, id := range q.IDs {
		if id == 0 {
			return q, errInvalid
		}
	}
	if q.Action != "records" && (q.From != 0 || q.Through != 0) {
		return q, errInvalid
	}
	switch q.Action {
	case "records":
		if q.From == 0 || q.From > q.Through || len(q.Cut) == 0 || q.Request != ([16]byte{}) || q.Handle != 0 || q.Sequence != 0 || q.Mode != 0 || len(q.Original)+len(q.Token)+len(q.IDs)+len(q.Partitions) != 0 || q.Local != ([16]byte{}) || q.Transaction != ([16]byte{}) || q.Coordinator != 0 {
			return q, errInvalid
		}
	case "prepare":
		if q.Request == ([16]byte{}) || q.Handle != 0 || q.Sequence != 0 || q.Mode < graphReadFresh || q.Mode > graphReadAfter || len(q.Original)+len(q.Token)+len(q.IDs) != 0 {
			return q, errInvalid
		}
		if (q.Mode == graphReadFresh || q.Mode == graphReadAfter) && len(q.Cut) != 0 || q.Mode == graphReadAt && len(q.Cut) == 0 {
			return q, errInvalid
		}
		if q.Mode == graphReadAfter {
			if (q.Local == ([16]byte{})) == (q.Transaction == ([16]byte{})) || q.Coordinator != 3 {
				return q, errInvalid
			}
		} else if q.Local != ([16]byte{}) || q.Transaction != ([16]byte{}) || q.Coordinator != 0 {
			return q, errInvalid
		}
	case "publish", "scan", "points", "index", "close", "release":
		if q.Handle == 0 || q.Request != ([16]byte{}) || q.Mode != 0 || len(q.Cut)+len(q.Original)+len(q.Token)+len(q.Partitions) != 0 || q.Local != ([16]byte{}) || q.Transaction != ([16]byte{}) || q.Coordinator != 0 || (q.Action == "scan" || q.Action == "points" || q.Action == "index") != (q.Sequence > 0) || q.Action != "points" && len(q.IDs) != 0 || q.Action == "points" && len(q.IDs) == 0 {
			return q, errInvalid
		}
	case "reopen":
		if q.Request == ([16]byte{}) || len(q.Token) == 0 || q.Handle != 0 || q.Sequence != 0 || q.Mode != 0 || len(q.Cut)+len(q.Original)+len(q.IDs)+len(q.Partitions) != 0 || q.Local != ([16]byte{}) || q.Transaction != ([16]byte{}) || q.Coordinator != 0 {
			return q, errInvalid
		}
	default:
		return q, errInvalid
	}
	return q, nil
}

// The declared selector client runs on member2 and its native terminal RPCs
// target keepers1/4. Both canonical local Close and durable Release need the
// existing control worker independently of saturated normal read workers.
func nativeSelectorsTerminalControl(r processRequest) bool {
	q, err := nativeSelectorsInputDecode(r)
	return err == nil && (q.Action == "close" || q.Action == "release")
}

// Scoped selector cuts are the exact one- or two-participant fixture scope.
// The wire carries no source, quorum, pin or query-handle authority.
func nativeSelectorsCutDecode(wire []byte) (graphCut, error) {
	cut, err := nativeScalarWorkloadCutDecode(wire, durableColumnGraphSemanticContractID(), 1, 2)
	if err != nil {
		return graphCut{}, err
	}
	var previous uint64
	for _, cert := range cut.certificates {
		partition := cert.scope.partition
		if partition != 3 && partition != 8 || partition <= previous {
			return graphCut{}, errInvalid
		}
		previous = partition
	}
	return cut, nil
}

type nativeSelectorsQuery struct {
	cancel       context.CancelFunc
	request      [16]byte
	inputHash    [32]byte
	cut          graphCut
	scope        []uint64
	initial      *certifiedGraphRead
	pin          *retainedPinRequest
	token        retainedPinToken
	publication  []byte
	predicate    graphstate.IncidentPredicate
	continuation graphstore.PartitionIncidentContinuation
	hasCursor    bool
	eof          bool
	sequence     uint64
	lastInput    [32]byte
	lastReply    []byte
	work         graphstore.PageWork
	correction   *nativeLateCorrectionIndexCursor // exact fresh11 continuation, never caller authority
}
type nativeSelectorsSessions struct {
	mu            sync.Mutex
	ctx           context.Context
	cleanup       context.Context
	cancelCleanup context.CancelFunc
	coordinator   *crossProcessCoordinator
	localRead     func(context.Context, graphCutQuery) (graphCutReply, error)
	spec          nativeSelectorsSpec
	admission     nativeSelectorsAdmission
	digest        [32]byte
	policy        nativeSelectorsReadPolicy
	next, queries uint64
	starts, calls uint64
	cleanupCalls  uint64
	charged       nativeColumnWork
	owners        map[uint64]*nativeSelectorsQuery
	closed        bool
	agreement     raftlog.ApplicationSemanticContractID // immutable exact constructor selection; zero retains old fixtures
}

// Sixteen admitted query originals, each with at most eight Close and eight
// terminal exchanges. This lane cannot create/read sources or renew the fixed
// deadline. Native pin terminal work still spends its original cleanup pools.
const nativeSelectorsCleanupCalls = 16 * 2 * 8

func newNativeSelectorsSessions(ctx context.Context, c *crossProcessCoordinator, localRead func(context.Context, graphCutQuery) (graphCutReply, error)) (*nativeSelectorsSessions, error) {
	if ctx == nil || c == nil || c.host == nil || c.host.machine == nil || c.host.machine.SemanticContractID() != durableColumnGraphSemanticContractID() || localRead == nil {
		return nil, errInvalid
	}
	spec, err := nativeSelectorsSpecFromEnv()
	if err != nil {
		return nil, err
	}
	a, digest, err := nativeSelectorsAdmissionFromEnv(spec)
	if err != nil || [16]byte(c.host.machine.ns.graph) != a.Graph || c.host.machine.declaration.Digest() != a.Declaration || c.host.machine.declaration.TopologyEpoch() != a.Topology {
		return nil, errors.Join(errInvalid, err)
	}
	deadline, ok := ctx.Deadline()
	if !ok || deadline.UnixNano() != a.DeadlineUnixNanos {
		return nil, errInvalid
	}
	policy, err := nativeSelectorsReadAllowance(spec, a)
	if err != nil {
		return nil, err
	}
	return newNativeScalarQuerySessions(ctx, c, localRead, spec, a, digest, policy)
}
func newNativeScalarQuerySessions(ctx context.Context, c *crossProcessCoordinator, localRead func(context.Context, graphCutQuery) (graphCutReply, error), spec nativeSelectorsSpec, a nativeSelectorsAdmission, digest [32]byte, policy nativeSelectorsReadPolicy) (*nativeSelectorsSessions, error) {
	return newNativeScalarQuerySessionsFor(ctx, c, localRead, spec, a, digest, policy, durableColumnGraphSemanticContractID())
}

func newNativeScalarQuerySessionsFor(ctx context.Context, c *crossProcessCoordinator, localRead func(context.Context, graphCutQuery) (graphCutReply, error), spec nativeSelectorsSpec, a nativeSelectorsAdmission, digest [32]byte, policy nativeSelectorsReadPolicy, agreement raftlog.ApplicationSemanticContractID) (*nativeSelectorsSessions, error) {
	if agreement != durableColumnGraphSemanticContractID() && agreement != rangeMovePooledSemanticContractID() || agreement == rangeMovePooledSemanticContractID() && (spec.Family != nativeTransferFamily || spec.Version != 2 || nativeTransferValidate(nativeGraphCostSpec{12, spec.Facts, spec.CrossPercent, spec.Writers}) != nil) {
		return nil, errInvalid
	}
	if ctx == nil || c == nil || c.host == nil || c.host.machine == nil || c.host.machine.SemanticContractID() != agreement || localRead == nil || [16]byte(c.host.machine.ns.graph) != a.Graph || c.host.machine.declaration.Digest() != a.Declaration || c.host.machine.declaration.TopologyEpoch() != a.Topology || a.Spec != spec || policy.DeadlineUnixNanos != a.DeadlineUnixNanos {
		return nil, errInvalid
	}
	deadline, ok := ctx.Deadline()
	if !ok || deadline.UnixNano() != a.DeadlineUnixNanos {
		return nil, errInvalid
	}
	cleanup, cancelCleanup := context.WithDeadline(context.WithoutCancel(ctx), deadline.Add(30*time.Second))
	return &nativeSelectorsSessions{ctx: ctx, cleanup: cleanup, cancelCleanup: cancelCleanup, coordinator: c, localRead: localRead, spec: spec, admission: a, digest: digest, policy: policy, owners: make(map[uint64]*nativeSelectorsQuery), agreement: agreement}, nil
}

func (s *nativeSelectorsSessions) semantic() raftlog.ApplicationSemanticContractID {
	if s.agreement == (raftlog.ApplicationSemanticContractID{}) {
		return durableColumnGraphSemanticContractID()
	}
	return s.agreement
}

func (s *nativeSelectorsSessions) cutDecode(wire []byte) (graphCut, error) {
	if s.semantic() == durableColumnGraphSemanticContractID() {
		return nativeSelectorsCutDecode(wire)
	}
	return nativeScalarWorkloadCutDecode(wire, s.semantic(), 1, 2)
}

// This workload-local prepayment is not reported as native installed work.
// Actual read work and process/frame IO remain separate receipt counters. A
// failed/unknown constructor spends its complete finite offered grant, and
// neither SAME sequence nor source renewal restores it.
func (s *nativeSelectorsSessions) fund(ctx context.Context) error {
	if ctx == nil || s == nil || s.closed {
		return errInvalid
	}
	if err := errors.Join(ctx.Err(), s.ctx.Err()); err != nil {
		return err
	}
	if s.starts >= s.policy.Starts {
		return errLimit
	}
	grant := nativeColumnRetentionWork(s.coordinator.host.machine.limits)
	next, err := s.charged.plus(grant)
	if err != nil || next.Records > s.policy.Rows || next.Bytes > s.policy.Bytes || next.OutputBytes > s.policy.Output || next.RPCs > s.policy.RPCs || next.WireBytes > s.policy.WireBytes {
		return errLimit
	}
	s.starts++
	s.charged = next
	return nil
}
func (s *nativeSelectorsSessions) receipt(ctx context.Context, q nativeSelectorsInput) (graphCommitReceipt, error) {
	if err := s.fund(ctx); err != nil {
		return graphCommitReceipt{}, err
	}
	l := s.coordinator.host.machine.limits
	if q.Local != ([16]byte{}) {
		nonce, err := graphReadNonce()
		if err != nil {
			return graphCommitReceipt{}, err
		}
		query := graphCutQuery{kind: 3, request: q.Local, nonce: nonce, sourceRows: l.sourceRows, sourceBytes: l.sourceBytes, outputBytes: l.outputBytes}
		reply, err := s.localRead(ctx, query)
		if err != nil || reply.err != nil || reply.question != query || reply.proof.owner != s.coordinator.host {
			return graphCommitReceipt{}, errors.Join(errInvalid, err, reply.err)
		}
		return graphReceiptFromLocal(reply.proof)
	}
	arena, err := graphstate.NewOutputBudget(min(l.sourceBytes, l.outputBytes))
	if err != nil {
		return graphCommitReceipt{}, err
	}
	endpoints, err := s.coordinator.endpoints(newGraphRemoteBudget(l, arena))
	if err != nil {
		return graphCommitReceipt{}, err
	}
	e := endpoints[q.Coordinator]
	if e == nil {
		return graphCommitReceipt{}, errGraphCutScope
	}
	authority, err := e.cross(ctx, q.Transaction, crossDecide, 1<<20)
	if err != nil {
		return graphCommitReceipt{}, err
	}
	return graphReceiptFromAuthority(authority)
}

func (s *nativeSelectorsSessions) prepare(ctx context.Context, q nativeSelectorsInput, input []byte) (out nativeSelectorsReply, err error) {
	for handle, owner := range s.owners {
		if owner.request == q.Request {
			if owner.inputHash != sha256.Sum256(input) {
				return out, errInvalid
			}
			wire, err := nativeFeedCutWire(owner.cut)
			return nativeSelectorsReply{Handle: handle, Cut: wire, Original: bytes.Clone(owner.publication), Replay: true}, err
		}
	}
	if len(s.owners) >= 2 || s.queries >= s.policy.Queries || s.next == ^uint64(0) {
		return out, errLimit
	}
	request := graphReadRequest{mode: q.Mode}
	if len(q.Cut) != 0 {
		request.cut, err = s.cutDecode(q.Cut)
		if err != nil {
			return out, err
		}
	}
	if q.Mode == graphReadAfter {
		request.receipt, err = s.receipt(ctx, q)
		if err != nil {
			return out, err
		}
	}
	if err = s.fund(ctx); err != nil {
		return out, err
	}
	s.queries++ // failed captures are not refunded
	queryCtx, cancelQuery := context.WithCancel(s.ctx)
	stop := context.AfterFunc(ctx, cancelQuery)
	defer stop()
	read, err := s.coordinator.OpenCertified(queryCtx, request, q.Partitions)
	if err != nil {
		cancelQuery()
		return out, err
	}
	defer func() {
		if err != nil {
			cancelQuery()
			err = errors.Join(err, read.Close())
		}
	}()
	cut, err := read.Cut()
	if err != nil {
		return out, err
	}
	owner := &nativeSelectorsQuery{cancel: cancelQuery, request: q.Request, inputHash: sha256.Sum256(input), cut: cut, scope: append([]uint64(nil), q.Partitions...), initial: read}
	if len(cut.certificates) == s.coordinator.host.machine.declaration.Len() {
		if err = s.fund(ctx); err != nil {
			return out, err
		}
		publisher, e := newRetainedPinPublisher(s.coordinator)
		if e != nil {
			return out, e
		}
		owner.pin, err = publisher.Begin(queryCtx, requestID(q.Request), graphReadRequest{mode: graphReadAt, cut: cut}, 100)
		if err != nil {
			return out, err
		}
		owner.publication, err = owner.pin.Original()
		if err != nil {
			return out, err
		}
	}
	// Predicate axis is read from actual authenticated configuration below;
	// ownership declaration alone carries no default temporal-axis authority.
	s.coordinator.host.mu.Lock()
	cfg, configErr := s.coordinator.host.currentConfiguration()
	s.coordinator.host.mu.Unlock()
	if configErr != nil {
		return out, configErr
	}
	window, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return out, err
	}
	owner.predicate = graphstate.IncidentPredicate{Endpoint: 1, Window: window}
	out.Cut, err = nativeFeedCutWire(cut)
	if err != nil {
		return out, err
	}
	if err = ctx.Err(); err != nil {
		return out, err
	}
	stop() // the returned owner now holds the SAME fixed whole-query lifetime
	s.next++
	s.owners[s.next] = owner
	out.Handle = s.next
	out.Original = bytes.Clone(owner.publication)
	return out, err
}

func (s *nativeSelectorsSessions) publish(ctx context.Context, owner *nativeSelectorsQuery) ([]byte, error) {
	if owner.pin == nil {
		return nil, errGraphCutScope
	}
	publisher, err := newRetainedPinPublisher(s.coordinator)
	if err != nil {
		return nil, err
	}
	if err = owner.pin.Issue(ctx, publisher); err != nil {
		return nil, err
	}
	owner.token, err = owner.pin.Publish(ctx)
	if err != nil {
		return nil, err
	}
	wire, err := encodeRetainedPinToken(owner.token)
	if err != nil {
		return nil, err
	}
	// Only the actual all-participant common-live publication permits releasing
	// the mode-selected temporary sources. Pin metadata alone is not authority.
	if owner.initial != nil {
		if err = owner.initial.Close(); err != nil {
			return nil, err
		}
		owner.work = nativeSelectorsAddWork(owner.work, owner.initial.Work())
		owner.initial = nil
	}
	return wire, nil
}

func nativeSelectorsAddWork(a, b graphstore.PageWork) graphstore.PageWork {
	return graphstore.PageWork{Records: a.Records + b.Records, Bytes: a.Bytes + b.Bytes, DirectoryPages: a.DirectoryPages + b.DirectoryPages, CheckpointPages: a.CheckpointPages + b.CheckpointPages, PatchPages: a.PatchPages + b.PatchPages, DecodedCells: a.DecodedCells + b.DecodedCells}
}

func nativeSelectorsEntity(ctx context.Context, view graphstate.ReadView, h *allocationHost, id uint64) (out nativeLifecycleEntity, err error) {
	out.ID, out.Labels = id, []string{}
	entity, err := view.Entity(ctx, graphstate.EntityID(id))
	if err != nil || !entity.Found {
		return out, err
	}
	h.mu.Lock()
	cfg, err := h.currentConfiguration()
	h.mu.Unlock()
	if err != nil {
		return out, err
	}
	point, err := temporal.RationalPosition(cfg.defaultAxis.Axis(), temporal.RationalInt64(0))
	if err != nil {
		return out, err
	}
	window, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return out, err
	}
	projected, err := graphstate.Project(ctx, view, graphstate.EntityID(id), point, graphstate.Effective, h.machine.limits.graph.Planner)
	if err != nil {
		return out, err
	}
	out.Found, out.Kind, out.Type, out.Mode = true, byte(entity.Record.Kind), entity.Record.Type, byte(entity.Record.Mode)
	out.Source, out.Target = uint64(entity.Record.Source), uint64(entity.Record.Target)
	out.Effective, out.EffectiveLife, out.Labels = projected.Active, uint64(projected.Life), projected.Labels
	out.Presence, err = nativeLifecycleCellRead(ctx, view, graphstate.ComponentKey{Owner: graphstate.EntityID(id), Kind: graphstate.Presence}, window, point, h.machine.limits.graph.Planner.Component)
	if err != nil {
		return out, err
	}
	if entity.Record.Kind == graphstate.Relationship && projected.Active {
		out.Property, err = nativeLifecycleCellRead(ctx, view, graphstate.ComponentKey{Owner: graphstate.EntityID(id), Life: projected.Life, Kind: graphstate.ScalarProperty, Name: "p"}, window, point, h.machine.limits.graph.Planner.Component)
	}
	return out, err
}

// A page attempt opens an independently bounded source only after retiring a
// whole grant from the SAME query owner. The actual durable pin reauthenticates
// C through original keepers. Export/Import carries only a genuine native
// cursor; neither an ID from the caller nor elapsed time advances it.
func (s *nativeSelectorsSessions) page(ctx context.Context, owner *nativeSelectorsQuery, ids []uint64, scan bool) (out nativeSelectorsReply, next graphstore.PartitionIncidentContinuation, err error) {
	return s.pagePredicate(ctx, owner, ids, scan, owner.predicate, owner.continuation, owner.hasCursor, owner.eof)
}

func (s *nativeSelectorsSessions) pagePredicate(ctx context.Context, owner *nativeSelectorsQuery, ids []uint64, scan bool, predicate graphstate.IncidentPredicate, continuation graphstore.PartitionIncidentContinuation, hasCursor, eof bool) (out nativeSelectorsReply, next graphstore.PartitionIncidentContinuation, err error) {
	var read *certifiedGraphRead
	if len(owner.token.manifest) == 0 {
		if scan || owner.initial == nil {
			return out, next, errInvalid
		}
		read = owner.initial // bounded scoped probes keep their genuine source
	} else {
		if err = s.fund(ctx); err != nil {
			return out, next, err
		}
		publisher, e := newRetainedPinPublisher(s.coordinator)
		if e != nil {
			return out, next, e
		}
		read, err = publisher.Open(ctx, owner.token)
		if err != nil {
			return out, next, err
		}
		defer func() {
			err = errors.Join(err, read.Close())
			owner.work = nativeSelectorsAddWork(owner.work, read.Work())
		}()
	}
	view, err := read.ReadView()
	if err != nil {
		return out, next, err
	}
	if scan {
		if eof {
			return out, next, errInvalid
		}
		// Authentic endpoint absence yields exact-empty; it is not a skipped
		// range or an assumed empty dataset inferred from family dimensions.
		endpoint, e := view.Entity(ctx, predicate.Endpoint)
		if e != nil {
			return out, next, e
		}
		if !endpoint.Found {
			out.EOF = true
			return out, next, nil
		}
		full, ok := view.(*graphstore.PartitionTransactionView)
		if !ok {
			return out, next, errInvalid
		}
		var cursor graphstate.Cursor
		if hasCursor {
			cursor, err = full.ImportIncidentContinuation(ctx, predicate, continuation)
			if err != nil {
				return out, next, err
			}
		}
		page, e := full.IncidentRelationshipsChunk(ctx, predicate, cursor, graphstate.ReadBudget{Rows: 512, Bytes: 1 << 20}, 16)
		if e != nil || page.View != full.Identity() || len(page.Entities) > 16 || page.Complete && page.Next != 0 || !page.Complete && page.Next == 0 {
			return out, next, errors.Join(errInvalid, e)
		}
		out.Candidates = make([]uint64, len(page.Entities))
		ids = make([]uint64, len(page.Entities))
		for i, id := range page.Entities {
			ids[i], out.Candidates[i] = uint64(id), uint64(id)
		}
		out.EOF = page.Complete
		if !page.Complete {
			next, err = full.ExportIncidentContinuation(ctx, predicate, page.Next)
			if err != nil {
				return out, next, err
			}
		}
	}
	out.Points = make([]nativeLifecycleEntity, 0, len(ids))
	for _, id := range ids {
		point, e := nativeSelectorsEntity(ctx, view, s.coordinator.host, id)
		if e != nil {
			return out, next, e
		}
		out.Points = append(out.Points, point)
	}
	return out, next, nil
}

func (s *nativeSelectorsSessions) reopen(ctx context.Context, q nativeSelectorsInput, input []byte) (out nativeSelectorsReply, err error) {
	for handle, owner := range s.owners {
		if owner.request == q.Request {
			if owner.inputHash != sha256.Sum256(input) {
				return out, errInvalid
			}
			cut, err := nativeFeedCutWire(owner.cut)
			return nativeSelectorsReply{Handle: handle, Cut: cut, Original: bytes.Clone(owner.publication), Token: bytes.Clone(q.Token), Replay: true}, err
		}
	}
	if len(s.owners) >= 2 || s.queries >= s.policy.Queries || s.next == ^uint64(0) {
		return out, errLimit
	}
	token, err := decodeRetainedPinToken(q.Token)
	if err != nil {
		return out, err
	}
	definition, err := decodeGraphPinManifest(token.manifest)
	if err != nil || definition.publisher != s.coordinator.host.machine.scope() || definition.publisher.semantic != s.semantic() {
		return out, errors.Join(errInvalid, err)
	}
	if err = s.fund(ctx); err != nil {
		return out, err
	}
	s.queries++
	publisher, err := newRetainedPinPublisher(s.coordinator)
	if err != nil {
		return out, err
	}
	pin, err := publisher.Resume(ctx, token.manifest)
	if err != nil {
		return out, err
	}
	if err = s.fund(ctx); err != nil {
		return out, err
	}
	read, err := publisher.Open(ctx, token)
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, read.Close()) }()
	cut, err := read.Cut()
	if err != nil {
		return out, err
	}
	s.coordinator.host.mu.Lock()
	cfg, err := s.coordinator.host.currentConfiguration()
	s.coordinator.host.mu.Unlock()
	if err != nil {
		return out, err
	}
	window, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return out, err
	}
	out.Cut, err = nativeFeedCutWire(cut)
	if err != nil {
		return out, err
	}
	s.next++
	s.owners[s.next] = &nativeSelectorsQuery{request: q.Request, inputHash: sha256.Sum256(input), cut: cut, pin: pin, token: token, publication: bytes.Clone(token.manifest), predicate: graphstate.IncidentPredicate{Endpoint: 1, Window: window}, work: read.Work()}
	out.Handle, out.Token, out.Original = s.next, bytes.Clone(q.Token), bytes.Clone(token.manifest)
	return out, nil
}

func (s *nativeSelectorsSessions) serve(ctx context.Context, r processRequest) (wire []byte, err error) {
	if s == nil || ctx == nil {
		return nil, errInvalid
	}
	q, err := nativeSelectorsInputDecode(r)
	if err != nil {
		return nil, err
	}
	return s.serveDecoded(ctx, r, q)
}

func (s *nativeSelectorsSessions) serveDecoded(ctx context.Context, r processRequest, q nativeSelectorsInput) (wire []byte, err error) {
	if s == nil || ctx == nil {
		return nil, errInvalid
	}
	if q.Action == "index" && (s.spec.Family != nativeLateCorrectionFamily || nativeLateCorrectionValidate(nativeLateCorrectionCost(s.spec)) != nil) {
		return nil, errInvalid
	}
	defer func() {
		if err != nil && errors.Is(err, graphstore.ErrScopeUnavailable) {
			// Only this canonical fresh request receives a scope classifier.
			// Actual errors supply it; caller coordinates and missing rows
			// cannot manufacture a successful empty result or live authority.
			body, encodeErr := json.Marshal(nativeSelectorsReply{Version: 1, Action: q.Action, Handle: q.Handle, Sequence: q.Sequence, Failure: 1})
			if encodeErr == nil {
				wire = body
			}
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, graphstore.ErrClosed
	}
	terminal := q.Action == "close" || q.Action == "release"
	parent := s.ctx
	if terminal {
		if s.cleanup == nil || s.cleanupCalls >= nativeSelectorsCleanupCalls {
			return nil, errLimit
		}
		s.cleanupCalls++ // unknown exchanges remain spent
		parent = s.cleanup
	} else {
		if s.calls >= s.policy.Calls {
			return nil, errLimit
		}
		s.calls++
	}
	operation, cancel := context.WithCancel(parent)
	stop := func() bool { return false }
	if !terminal {
		stop = context.AfterFunc(ctx, cancel)
	}
	defer func() { stop(); cancel() }()
	started := time.Now()
	err = operation.Err()
	if !terminal {
		err = errors.Join(ctx.Err(), err)
	}
	if err != nil {
		return nil, err
	}
	var out nativeSelectorsReply
	switch q.Action {
	case "records":
		valid := nativeSelectorsValidate(s.spec.cost())
		if s.spec.Version == 2 && s.spec.Family == nativeSelectorsFamily {
			valid = nativeGuardedScalarValidate(s.spec.cost())
		}
		if s.spec.Family == nativeImmutableColumnsFamily {
			valid = nativeImmutableColumnsValidate(nativeImmutableColumnsCost(s.spec))
		}
		if s.spec.Family == nativeLateCorrectionFamily {
			valid = nativeLateCorrectionValidate(nativeLateCorrectionCost(s.spec))
		}
		if s.spec.Family == nativeCreateUpdateFamily {
			valid = nativeCreateUpdateValidate(nativeCreateUpdateCost(s.spec))
		}
		if s.spec.Family == nativeTransferFamily && s.semantic() == rangeMovePooledSemanticContractID() {
			valid = nativeTransferValidate(nativeGraphCostSpec{12, s.spec.Facts, s.spec.CrossPercent, s.spec.Writers})
		}
		if valid != nil || s.coordinator.host.machine.SemanticContractID() != s.semantic() {
			return nil, errInvalid
		}
		if err = s.fund(operation); err != nil {
			return nil, err
		}
		return nativeScalarWorkloadRecords(operation, s.coordinator.host, q.From, q.Through, q.Cut, s.semantic())
	case "prepare":
		out, err = s.prepare(operation, q, r.data)
	case "reopen":
		out, err = s.reopen(operation, q, r.data)
	default:
		owner := s.owners[q.Handle]
		if owner == nil {
			return nil, errInvalid
		}
		out.Handle = q.Handle
		switch q.Action {
		case "publish":
			out.Token, err = s.publish(operation, owner)
		case "scan", "points", "index":
			hash := sha256.Sum256(r.data)
			if q.Sequence == owner.sequence {
				if owner.lastInput != hash || len(owner.lastReply) == 0 {
					return nil, errInvalid
				}
				return bytes.Clone(owner.lastReply), nil
			}
			if q.Sequence != owner.sequence+1 {
				return nil, errInvalid
			}
			var next graphstore.PartitionIncidentContinuation
			var correction *nativeLateCorrectionIndexCursor
			if q.Action == "index" {
				out, correction, err = s.correctionIndexPage(operation, owner)
			} else {
				out, next, err = s.page(operation, owner, q.IDs, q.Action == "scan")
			}
			if err != nil {
				return nil, err
			}
			out.Version, out.Action, out.Handle, out.Sequence = 1, q.Action, q.Handle, q.Sequence
			out.Work, out.Charged, out.Starts, out.Calls = owner.work, s.charged, s.starts, s.calls
			if owner.initial != nil {
				out.Work = nativeSelectorsAddWork(out.Work, owner.initial.Work())
			}
			out.Nanos = time.Since(started).Nanoseconds()
			wire, err = json.Marshal(out)
			if err != nil || len(wire) > 64<<10 {
				return nil, errors.Join(errLimit, err)
			}
			// Only this exact completed reply commits the private continuation.
			// A failed read/encode never advances from the previous genuine capsule.
			owner.sequence, owner.lastInput, owner.lastReply = q.Sequence, hash, bytes.Clone(wire)
			if q.Action == "scan" {
				owner.continuation, owner.hasCursor, owner.eof = next, !out.EOF, out.EOF
			}
			if q.Action == "index" {
				owner.correction = correction
			}
			return wire, nil
		case "close", "release":
			if owner.initial != nil {
				err = owner.initial.Close()
				owner.work = nativeSelectorsAddWork(owner.work, owner.initial.Work())
				owner.initial = nil
			}
			if q.Action == "release" && owner.pin != nil {
				if len(owner.token.manifest) == 0 {
					err = errors.Join(err, owner.pin.CancelIssue(operation))
				} else {
					err = errors.Join(err, owner.pin.Release(operation))
				}
			}
			if err == nil {
				if owner.cancel != nil {
					owner.cancel()
				}
				delete(s.owners, q.Handle)
			}
			out.Work = owner.work
		}
	}
	if err != nil {
		return nil, err
	}
	out.Version, out.Action, out.Charged, out.Starts, out.Calls = 1, q.Action, s.charged, s.starts, s.calls
	out.Nanos = time.Since(started).Nanoseconds()
	wire, err = json.Marshal(out)
	if err != nil || len(wire) > 64<<10 {
		return nil, errors.Join(errLimit, err)
	}
	return wire, nil
}

// Process/source Close retires only transient reads. Native publication tokens
// survive for genuine same-directory reopen, and explicit Release alone ends
// their durable lifecycle. Cleanup never claims a lost native terminal result.
func (s *nativeSelectorsSessions) close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.cancelCleanup != nil {
		defer s.cancelCleanup()
	}
	var combined error
	for _, owner := range s.owners {
		if owner.cancel != nil {
			owner.cancel()
		}
		if owner.initial != nil {
			combined = errors.Join(combined, owner.initial.Close())
			owner.initial = nil
		}
	}
	return combined
}
