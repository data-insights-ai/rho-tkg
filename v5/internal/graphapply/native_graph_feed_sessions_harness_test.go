package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func nativeFeedDecodeJSON(body []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return errors.Join(errInvalid, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.Join(errInvalid, err)
	}
	return nil
}

type nativeFeedInput struct {
	Version                   uint8
	Action                    string
	Request                   [16]byte
	Original, Token, Boundary []byte
	Digest                    [32]byte
	From                      uint64
	Through                   uint64 `json:",omitzero"`
}
type nativeFeedPoint struct {
	Fact   uint64
	Entity nativeLifecycleEntity
}
type nativeFeedReply struct {
	Version                                           uint8
	Action                                            string
	Original, Token, Initial, Boundary, Page, Catalog []byte
	Schemas                                           []graphstate.PropertyDefinition
	Digest                                            [32]byte
	Replay, EOF                                       bool
	Points                                            []nativeFeedPoint
	Next                                              uint64
	Completed                                         []byte
	Cursor                                            []byte
	Index                                             uint64
	Root                                              []byte
	Nanos                                             int64
	Failure                                           byte `json:",omitzero"` // fresh diagnostic classification only, never a decision/pin proof
}

func nativeFeedInputWire(q nativeFeedInput) ([]byte, error) {
	if q.Version != 1 {
		return nil, errInvalid
	}
	body, err := json.Marshal(q)
	if err != nil || len(body) > 64<<10 {
		return nil, errors.Join(errLimit, err)
	}
	return append([]byte(nativeFeedMarker), body...), nil
}
func nativeFeedInputDecode(r processRequest) (q nativeFeedInput, err error) {
	return nativeScalarFeedInputDecode(r, nativeFeedMarker, false)
}

func nativeScalarFeedInputDecode(r processRequest, marker string, retainedOnly bool) (q nativeFeedInput, err error) {
	if r.op != processCostObserve || r.index != 0 || r.entity != 0 || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 || !bytes.HasPrefix(r.data, []byte(marker)) || len(r.data) > 64<<10+len(marker) {
		return q, errInvalid
	}
	if err = nativeFeedDecodeJSON(r.data[len(marker):], &q); err != nil {
		return q, err
	}
	body, e := nativeFeedInputWire(q)
	if e == nil && marker != nativeFeedMarker {
		body = append([]byte(marker), body[len(nativeFeedMarker):]...)
	}
	if e != nil || !bytes.Equal(body, r.data) {
		return q, errors.Join(errInvalid, e)
	}
	if q.Action != "records" && q.Through != 0 {
		return q, errInvalid
	}
	switch q.Action {
	case "records":
		if q.Through == 0 || q.From == 0 || q.From > q.Through || len(q.Boundary) == 0 || q.Request != ([16]byte{}) || (!retainedOnly && len(q.Original) != 0) || len(q.Token) != 0 || q.Digest != ([32]byte{}) {
			return q, errInvalid
		}
	case "retain":
		if !retainedOnly || q.Request == ([16]byte{}) || len(q.Original)+len(q.Token)+len(q.Boundary) != 0 || q.Digest != ([32]byte{}) || q.From != 0 {
			return q, errInvalid
		}
	case "begin":
		if q.Request == ([16]byte{}) || len(q.Original)+len(q.Token)+len(q.Boundary) != 0 || q.Digest != ([32]byte{}) || q.From != 0 {
			return q, errInvalid
		}
	case "resume":
		if len(q.Original) == 0 || q.Request != ([16]byte{}) || len(q.Token)+len(q.Boundary) != 0 || q.Digest != ([32]byte{}) || q.From != 0 {
			return q, errInvalid
		}
	case "open":
		if len(q.Token) == 0 || q.Request != ([16]byte{}) || len(q.Original)+len(q.Boundary) != 0 || q.Digest != ([32]byte{}) || q.From != 0 {
			return q, errInvalid
		}
	case "page":
		if q.Request == ([16]byte{}) || len(q.Boundary) == 0 || len(q.Original)+len(q.Token) != 0 || q.Digest != ([32]byte{}) || q.From != 0 {
			return q, errInvalid
		}
	case "ack":
		if q.Request == ([16]byte{}) || q.Digest == ([32]byte{}) || len(q.Original)+len(q.Token)+len(q.Boundary) != 0 || q.From != 0 {
			return q, errInvalid
		}
	case "publish", "snapshot", "snapshot-close", "close", "release", "expire", "state":
		if q.Request != ([16]byte{}) || len(q.Original)+len(q.Token)+len(q.Boundary) != 0 || q.Digest != ([32]byte{}) || q.Action != "snapshot" && q.From != 0 {
			return q, errInvalid
		}
	default:
		return q, errInvalid
	}
	return q, nil
}
func nativeFeedTokenEncode(t retainedFeedToken) ([]byte, error) {
	return encodeRetainedFeedToken(t)
}

// Caller framing selects only the fresh response grammar. The actual error
// supplies every status; no token/journal field is evidence of terminal state.
func nativeFeedProfileResponseCode(r processRequest, feed, columns bool, cause error) byte {
	if r.op != processCostObserve || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 {
		return 0
	}
	valid := false
	if feed && bytes.HasPrefix(r.data, []byte(nativeFeedMarker)) {
		_, err := nativeFeedInputDecode(r)
		valid = err == nil
	}
	if columns && bytes.HasPrefix(r.data, []byte(nativeEdgeColumns)) && r.index == 0 && r.entity == 0 {
		q, err := nativeGraphEdgeUniqueColumnControlDecode(r.data)
		if err == nil {
			_, err = nativeGraphCostColumnDecode(processRequest{data: q.Body})
			valid = err == nil
		}
	}
	if !valid {
		return 0
	}
	return nativeColumnProcessErrorCode(cause)
}
func nativeFeedTokenDecode(wire []byte) (t retainedFeedToken, err error) {
	return decodeRetainedFeedToken(wire)
}
func nativeFeedCutWire(c graphCut) ([]byte, error) {
	return encodeGraphE15ReadRequest(graphE15ReadRequest{mode: graphReadAt, cut: c})
}
func nativeFeedCutDecode(wire []byte) (graphCut, error) {
	return nativeScalarWorkloadCutDecode(wire, retainedFeedSemanticContractID(), 2, 2)
}

// This decodes inert At coordinates only. Each admitted caller supplies the
// exact agreement/cardinality; native At still authenticates the stored cut.
func nativeScalarWorkloadCutDecode(wire []byte, semantic raftlog.ApplicationSemanticContractID, minParts, maxParts int) (graphCut, error) {
	q, err := decodeGraphE15ReadRequest(wire)
	if err != nil || q.mode != graphReadAt || q.transaction != ([16]byte{}) || q.coordinator != 0 || minParts < 1 || maxParts < minParts || len(q.cut.certificates) < minParts || len(q.cut.certificates) > maxParts {
		return graphCut{}, errors.Join(errInvalid, err)
	}
	for _, c := range q.cut.certificates {
		if c.scope.semantic != semantic {
			return graphCut{}, errInvalid
		}
	}
	return q.cut, nil
}

type nativeFeedSessions struct {
	mu             sync.Mutex
	ctx            context.Context
	coordinator    *crossProcessCoordinator
	spec           nativeFeedSpec
	admission      nativeFeedAdmission
	owner          *retainedFeedOwner
	snapshot       *certifiedGraphRead
	snapshotNext   uint64
	snapshotStarts uint64
	cleanup        context.Context // fresh13 only, absolute original deadline plus30s
	cancelCleanup  context.CancelFunc
	cleanupCalls   uint64
}

func newNativeFeedSessions(ctx context.Context, c *crossProcessCoordinator, cost *nativeGraphCostSpec) (*nativeFeedSessions, error) {
	if cost == nil || cost.Version != 5 {
		return nil, nil
	}
	s, err := nativeFeedSpecFromEnv()
	if err != nil || s.cost() != *cost || c == nil || c.host == nil || c.host.machine == nil || c.host.machine.SemanticContractID() != retainedFeedSemanticContractID() {
		return nil, errors.Join(errInvalid, err)
	}
	a, _, err := nativeFeedAdmissionFromEnv(s)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); !ok || deadline.UnixNano() != a.DeadlineUnixNanos {
		return nil, errInvalid
	}
	return &nativeFeedSessions{ctx: ctx, coordinator: c, spec: s, admission: a}, nil
}
func (s *nativeFeedSessions) close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelCleanup != nil {
		defer s.cancelCleanup()
	}
	var err error
	if s.snapshot != nil {
		err = s.snapshot.Close()
		s.snapshot = nil
	}
	if s.owner != nil {
		err = errors.Join(err, s.owner.Close())
		s.owner = nil
	}
	return err
}
func (s *nativeFeedSessions) snapshotPage(ctx context.Context, from uint64) (out nativeFeedReply, err error) {
	return s.snapshotScalarPage(ctx, from, s.spec.cost(), s.admission.BudgetSeconds)
}

func (s *nativeFeedSessions) snapshotScalarPage(ctx context.Context, from uint64, cost nativeGraphCostSpec, budgetSeconds uint64) (out nativeFeedReply, err error) {
	defer func() {
		if err != nil && s.snapshot != nil {
			err = errors.Join(err, s.snapshot.Close())
			s.snapshot = nil
		}
	}()
	if from != s.snapshotNext || from >= cost.Facts {
		return out, errInvalid
	}
	// A read lease never changes its cut. A bounded page may renew its transient
	// sources through OpenSnapshot under the SAME durable pin, spending another
	// native operation grant; continuation stays private to this actual owner.
	if s.snapshot == nil {
		if s.snapshotStarts >= cost.Facts/16+budgetSeconds/60+16 {
			return out, errLimit
		}
		s.snapshotStarts++
		s.snapshot, err = s.owner.OpenSnapshot(s.ctx)
		if err != nil {
			return out, err
		}
	}
	view, err := s.snapshot.ReadView()
	if err != nil {
		return out, err
	}
	end := min(from+16, cost.Facts)
	out.Points = make([]nativeFeedPoint, 0, end-from)
	s.coordinator.host.mu.Lock()
	cfg, err := s.coordinator.host.currentConfiguration()
	s.coordinator.host.mu.Unlock()
	if err != nil {
		return out, err
	}
	scope, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return out, err
	}
	point, err := temporal.RationalPosition(cfg.defaultAxis.Axis(), temporal.RationalInt64(0))
	if err != nil {
		return out, err
	}
	for fact := from; fact < end; fact++ {
		id, _, _, _ := cost.edge(fact)
		entity, err := view.Entity(ctx, graphstate.EntityID(id))
		if err != nil {
			return out, err
		}
		row := nativeLifecycleEntity{ID: id, Found: entity.Found, Labels: []string{}}
		if entity.Found {
			projection, err := graphstate.Project(ctx, view, graphstate.EntityID(id), point, graphstate.Effective, s.coordinator.host.machine.limits.graph.Planner)
			if err != nil {
				return out, err
			}
			row.Kind, row.Type, row.Mode, row.Source, row.Target = byte(entity.Record.Kind), entity.Record.Type, byte(entity.Record.Mode), uint64(entity.Record.Source), uint64(entity.Record.Target)
			row.Effective, row.EffectiveLife, row.Labels = projection.Active, uint64(projection.Life), projection.Labels
			row.Presence, err = nativeLifecycleCellRead(ctx, view, graphstate.ComponentKey{Owner: graphstate.EntityID(id), Kind: graphstate.Presence}, scope, point, s.coordinator.host.machine.limits.graph.Planner.Component)
			if err != nil {
				return out, err
			}
			row.Property, err = nativeLifecycleCellRead(ctx, view, graphstate.ComponentKey{Owner: graphstate.EntityID(id), Life: projection.Life, Kind: graphstate.ScalarProperty, Name: "p"}, scope, point, s.coordinator.host.machine.limits.graph.Planner.Component)
			if err != nil {
				return out, err
			}
		}
		out.Points = append(out.Points, nativeFeedPoint{fact, row})
	}
	s.snapshotNext = end
	out.Next, out.EOF = end, end == cost.Facts
	// Keep the first genuine lease held across the parent's update race. Other
	// pages release promptly and bound cumulative work by a16-row admission.
	if from != 0 || out.EOF {
		err = s.snapshot.Close()
		s.snapshot = nil
	}
	return out, err
}
func (s *nativeFeedSessions) serve(ctx context.Context, r processRequest) (wire []byte, err error) {
	if s == nil || ctx == nil {
		return nil, errInvalid
	}
	q, err := nativeFeedInputDecode(r)
	if err != nil {
		return nil, err
	}
	return s.serveDecoded(ctx, r, q, nativeFeedPolicy(s.spec.cost(), s.admission.DeadlineUnixNanos), nativeFeedCutDecode, s.ctx, s.spec.cost(), s.admission.BudgetSeconds)
}

// Exact-profile wrappers admit framing, policy and lifetime before entering the
// shared owner. They do not turn workload coordinates into native authority.
func (s *nativeFeedSessions) serveDecoded(ctx context.Context, r processRequest, q nativeFeedInput, policy retainedFeedPolicy, decodeCut func([]byte) (graphCut, error), lifetime context.Context, cost nativeGraphCostSpec, budgetSeconds uint64) (wire []byte, err error) {
	defer func() {
		if err == nil {
			return
		}
		failure := byte(0)
		switch {
		case errors.Is(err, errRetainedFeedPending):
			failure = 1
		case errors.Is(err, errLimit):
			failure = 2
		case errors.Is(err, errGraphPinReleased):
			failure = 3
		case errors.Is(err, errGraphPinExpired):
			failure = 4
		case errors.Is(err, errGraphHistoryExpired):
			failure = 5
		case errors.Is(err, context.DeadlineExceeded):
			failure = 7
		}
		if failure != 0 {
			var encoded error
			wire, encoded = nativeFeedReplyEncode(nativeFeedReply{Version: 1, Action: q.Action, Failure: failure}, 0)
			err = errors.Join(err, encoded)
		}
	}()
	if cost.Version == 13 && !nativeGuardedFeedTerminalAction(q.Action) {
		if err = errors.Join(ctx.Err(), lifetime.Err()); err != nil {
			return nil, err
		}
		operation, cancel := context.WithCancel(lifetime)
		stop := context.AfterFunc(ctx, cancel)
		defer func() { stop(); cancel() }()
		ctx = operation
	}
	if q.Action == "records" {
		return nativeFeedRecords(ctx, s.coordinator.host, q)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cost.Version == 13 && nativeGuardedFeedTerminalAction(q.Action) {
		if s.cleanup == nil || s.cleanupCalls >= nativeGuardedFeedCleanupCalls {
			return nil, errLimit
		}
		s.cleanupCalls++ // uncertain cleanup remains spent; no read/source grant
		ctx, lifetime = s.cleanup, s.cleanup
	}
	if err = errors.Join(ctx.Err(), lifetime.Err()); err != nil {
		return nil, err
	}
	started := time.Now()
	out := nativeFeedReply{Version: 1, Action: q.Action}
	publisher, err := newRetainedPinPublisher(s.coordinator)
	if err != nil {
		return nil, err
	}
	switch q.Action {
	case "begin":
		if s.owner != nil {
			return nil, errLimit
		}
		s.owner, err = beginRetainedFeed(s.ctx, publisher, requestID(q.Request), graphReadRequest{mode: graphReadFresh}, 100, policy)
		if err == nil {
			out.Original, err = s.owner.Original()
		}
		if err == nil {
			initial := graphCut{certificates: make([]graphCertificate, len(s.owner.pin.definition.parts))}
			for i, part := range s.owner.pin.definition.parts {
				initial.certificates[i] = part.certificate
			}
			out.Initial, err = nativeFeedCutWire(initial)
		}
	case "resume":
		if s.owner != nil {
			return nil, errLimit
		}
		s.owner, err = resumeRetainedFeed(s.ctx, publisher, q.Original)
	case "open":
		if s.owner != nil {
			return nil, errLimit
		}
		var token retainedFeedToken
		token, err = nativeFeedTokenDecode(q.Token)
		if err == nil {
			s.owner, err = openRetainedFeed(s.ctx, publisher, token)
		}
	case "close":
		if s.snapshot != nil {
			err = s.snapshot.Close()
			s.snapshot = nil
		}
		if s.owner != nil {
			err = errors.Join(err, s.owner.Close())
			s.owner = nil
		}
	case "snapshot-close":
		if s.snapshot != nil {
			err = s.snapshot.Close()
			s.snapshot = nil
		}
		s.snapshotNext = 0
	default:
		if s.owner == nil {
			return nil, errInvalid
		}
		switch q.Action {
		case "publish":
			var token retainedFeedToken
			token, err = s.owner.Publish(ctx)
			if err == nil {
				out.Token, err = nativeFeedTokenEncode(token)
			}
		case "state":
			var reader *reader
			h := s.coordinator.host
			h.mu.Lock()
			reader, err = graphRetentionReader(h, ctx)
			if err == nil {
				var cursor retainedFeedCursor
				var found bool
				cursor, found, err = readFeedCursor(reader, s.owner.pin.definition.id())
				if err == nil && !found {
					err = errCorrupt
				}
				if err == nil {
					out.Cursor, err = encodeRetainedFeedCursor(cursor)
					out.Index, out.Root = reader.base.Index, bytes.Clone(reader.base.Image)
				}
				err = errors.Join(err, reader.view.Close())
			}
			h.mu.Unlock()
		case "snapshot":
			out, err = s.snapshotScalarPage(ctx, q.From, cost, budgetSeconds)
			out.Version, out.Action = 1, q.Action
		case "page":
			var boundary graphCut
			boundary, err = decodeCut(q.Boundary)
			if err == nil {
				var d retainedFeedDelivery
				d, err = s.owner.Page(ctx, q.Request, boundary)
				if err == nil {
					out.Page, out.Digest, out.Replay, out.EOF = d.wire, d.digest, d.replay, d.page.eof
					out.Schemas = d.schemas
					if d.page.kind != 0 {
						out.Catalog, err = encodeTypedGraphChanges(d.resolved, s.coordinator.host.machine.limits, true)
					}
				}
			}
		case "ack":
			var cut graphCut
			cut, err = s.owner.Ack(ctx, q.Request, q.Digest)
			if err == nil {
				out.Completed, err = nativeFeedCutWire(cut)
			}
		case "retain":
			err = s.owner.ToRetainedOnly(ctx, q.Request)
		case "release":
			err = s.owner.Release(ctx)
		case "expire":
			err = s.owner.Expire(ctx)
		default:
			err = errInvalid
		}
	}
	if err != nil {
		return nil, err
	}
	// Publish is deliberately separate from Begin: the caller records the exact
	// preparing recipe before the first real native issuance/publication effects.
	if q.Action == "resume" || q.Action == "begin" {
		return nativeFeedReplyEncode(out, time.Since(started))
	}
	return nativeFeedReplyEncode(out, time.Since(started))
}
func nativeFeedReplyEncode(out nativeFeedReply, elapsed time.Duration) ([]byte, error) {
	out.Nanos = elapsed.Nanoseconds()
	wire, err := json.Marshal(out)
	if err != nil || len(wire) > 1<<20 {
		return nil, errors.Join(errLimit, err)
	}
	return wire, nil
}
func pollGraphProcessFeedSubmission(ctx context.Context, h *allocationHost, proposal allocationProposal, baseline uint64, scanned int) (uint64, int, []byte, error, error) {
	command, err := decodeRetainedFeedCommand(proposal.wire)
	if err != nil {
		return baseline, scanned, nil, err, nil
	}
	return pollGraphProcessSubmission(ctx, h, proposal, baseline, scanned, uint64(h.machine.limits.commandBytes), func(body []byte, index uint64) error {
		result, err := decodeRetainedFeedResult(body)
		if err != nil || result.index > index || result.phase != command.phase || result.action != command.action || result.original != command.original || result.command != sha256.Sum256(proposal.wire) {
			return errors.Join(errInvalid, err)
		}
		return nil
	})
}

type nativeFeedRecordPage struct {
	Partition, After, Next, Through uint64
	Root                            []byte
	Complete                        bool
	Frames                          []nativeColumnFrame
}

// This is actual native retained evidence, not a query-cut or source lease.
// A caller tip is only a bound to authenticate against this Store's applied
// prefix; each voter independently returns its exact native root/envelopes.
func nativeFeedRecords(ctx context.Context, h *allocationHost, q nativeFeedInput) (wire []byte, err error) {
	if ctx == nil || h == nil || h.machine == nil || h.machine.SemanticContractID() != retainedFeedSemanticContractID() || q.Through > h.driver.Applied() {
		return nil, errInvalid
	}
	return nativeScalarWorkloadRecords(ctx, h, q.From, q.Through, q.Boundary, retainedFeedSemanticContractID())
}

// Only exact admitted wrappers call this evidence exporter. No semantic
// capability predicate relaxes the current store/argument/certificate equality.
func nativeScalarWorkloadRecords(ctx context.Context, h *allocationHost, from, through uint64, boundaryWire []byte, semantic raftlog.ApplicationSemanticContractID) (wire []byte, err error) {
	if ctx == nil || h == nil || h.machine == nil || h.driver == nil || h.machine.SemanticContractID() != semantic || through > h.driver.Applied() {
		return nil, errInvalid
	}
	boundary, err := nativeScalarWorkloadCutDecode(boundaryWire, semantic, 2, 2)
	if err != nil {
		return nil, err
	}
	var cert graphCertificate
	for _, c := range boundary.certificates {
		if c.scope == h.machine.scope() {
			cert = c
		}
	}
	if cert.index == 0 || through < cert.index {
		return nil, errInvalid
	}
	// Genuine At first authenticates the immutable business certificate. Its
	// physical source view is separate from the later completed native tip.
	proof, err := h.AtGraphCertificate(ctx, cert)
	if err != nil {
		return nil, err
	}
	if err = proof.Close(); err != nil {
		return nil, err
	}
	view, err := h.machine.store.ApplicationView(through)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, view.Close())
		if err != nil {
			wire = nil
		}
	}()
	root, err := view.RootBounded(ctx, 172)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	cfg, err := h.currentConfiguration()
	h.mu.Unlock()
	if err != nil {
		return nil, err
	}
	source := nativeColumnSource{binding: nativeColumnBinding{Version: 2, Policy: nativeColumnPolicy{FrameBytes: 32 << 10, ChunkRows: 16}}}
	source.hosts[0], source.views[0], source.roots[0], source.final[0], source.configurations[0] = h, view, root, cert, cfg
	page := nativeFeedRecordPage{Partition: h.machine.ns.partition, After: from, Next: from, Through: through, Root: root.Image, Frames: []nativeColumnFrame{}}
	for len(page.Frames) < 4 && page.Next < through {
		frame, e := source.frame(ctx, 0, page.Next+1)
		if e != nil {
			return nil, e
		}
		page.Frames = append(page.Frames, frame)
		page.Next++
	}
	page.Complete = page.Next == through
	wire, err = json.Marshal(page)
	if len(wire) > 1<<20 {
		return nil, errLimit
	}
	return wire, err
}
