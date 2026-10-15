package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type nativeTransferInput struct {
	Version                                uint8
	Action                                 string
	Request                                [16]byte
	Sequence, Count, Cursor, Range, Origin uint64
	Original, Key                          []byte
	Control                                bool
	Feed                                   *nativeFeedInput
	Query                                  *nativeSelectorsInput
}

type nativeTransferReply struct {
	Version uint8
	Action  string
	Body    []byte
	Nanos   int64
	Failure byte `json:",omitzero"`
}

func nativeTransferInputWire(q nativeTransferInput) ([]byte, error) {
	if q.Version != 1 {
		return nil, errInvalid
	}
	body, err := json.Marshal(q)
	if err != nil || len(body) > 128<<10 {
		return nil, errors.Join(errLimit, err)
	}
	return append([]byte(nativeTransferMarker), body...), nil
}

func nativeTransferQueryValidate(q nativeSelectorsInput) error {
	// The legacy selector parser remains exact. The fresh transfer door also
	// permits an actual target8 local/cross receipt after ownership changes.
	if q.Action == "prepare" && q.Mode == graphReadAfter && q.Coordinator == 8 {
		q.Coordinator = 3
	}
	wire, err := nativeSelectorsInputWire(q)
	if err != nil {
		return err
	}
	_, err = nativeSelectorsInputDecode(processRequest{op: processCostObserve, data: wire})
	return err
}

func nativeTransferInputDecode(r processRequest) (q nativeTransferInput, err error) {
	if r.op != processCostObserve || r.index != 0 || r.entity != 0 || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 || len(r.data) > (128<<10)+len(nativeTransferMarker) || !bytes.HasPrefix(r.data, []byte(nativeTransferMarker)) {
		return q, errInvalid
	}
	if err := nativeFeedDecodeJSON(r.data[len(nativeTransferMarker):], &q); err != nil {
		return q, err
	}
	canonical, err := nativeTransferInputWire(q)
	if err != nil || !bytes.Equal(canonical, r.data) {
		return q, errors.Join(errInvalid, err)
	}
	want := nativeTransferInput{Version: 1, Action: q.Action}
	switch q.Action {
	case "acquire":
		if q.Sequence == 0 || q.Count == 0 {
			return q, errInvalid
		}
		want.Sequence, want.Count = q.Sequence, q.Count
	case "next":
		if q.Cursor == 0 || q.Sequence == 0 || q.Count == 0 || q.Count > 32 {
			return q, errInvalid
		}
		want.Cursor, want.Sequence, want.Count = q.Cursor, q.Sequence, q.Count
	case "move-prepare":
		if q.Request == ([16]byte{}) || q.Range == 0 {
			return q, errInvalid
		}
		want.Request, want.Range = q.Request, q.Range
	case "move-run", "move-resume":
		if len(q.Original) == 0 || len(q.Original) > 4096 {
			return q, errInvalid
		}
		want.Original = q.Original
	case "freeze", "register":
		maximum := 16 << 10
		if q.Action == "register" {
			maximum = 64 << 10
		}
		if len(q.Original) == 0 || len(q.Original) > maximum {
			return q, errInvalid
		}
		want.Original = q.Original
	case "lookup":
		if len(q.Original) == 0 || len(q.Original) > 4096 || q.Origin != 3 && q.Origin != 8 || len(q.Key) == 0 || len(q.Key) > 128 {
			return q, errInvalid
		}
		want.Original, want.Origin, want.Control, want.Key = q.Original, q.Origin, q.Control, q.Key
	case "feed":
		if q.Feed == nil {
			return q, errInvalid
		}
		wire, err := nativeFeedInputWire(*q.Feed)
		if err != nil {
			return q, err
		}
		wire = append([]byte(nativeTransferMarker), wire[len(nativeFeedMarker):]...)
		if _, err := nativeScalarFeedInputDecode(processRequest{op: processCostObserve, data: wire}, nativeTransferMarker, true); err != nil {
			return q, err
		}
		want.Feed = q.Feed
	case "query":
		if q.Query == nil || nativeTransferQueryValidate(*q.Query) != nil {
			return q, errInvalid
		}
		want.Query = q.Query
	default:
		return q, errInvalid
	}
	allowed, err := nativeTransferInputWire(want)
	if err != nil || !bytes.Equal(allowed, canonical) {
		return q, errInvalid
	}
	return q, nil
}

func nativeTransferTerminalControl(r processRequest) bool {
	q, err := nativeTransferInputDecode(r)
	return err == nil && (q.Feed != nil && (q.Feed.Action == "close" || q.Feed.Action == "snapshot-close" || q.Feed.Action == "release" || q.Feed.Action == "expire") || q.Query != nil && (q.Query.Action == "close" || q.Query.Action == "release"))
}

type nativeTransferSessions struct {
	mu                  sync.Mutex
	ctx, cleanup        context.Context
	cancel              context.CancelFunc
	coordinator         *crossProcessCoordinator
	move                *rangeMoveCoordinator
	feed                *nativeFeedSessions
	queries             *nativeSelectorsSessions
	allocation          *nativeTransferAllocator
	spec                nativeTransferSpec
	admission           nativeTransferAdmission
	digest              [32]byte
	prepared            [2][]byte
	frozen              uint8
	calls, cleanupCalls uint64
	closed              bool
}

func newNativeTransferSessions(ctx context.Context, c *crossProcessCoordinator, o *certifiedProcessOwners, pump *graphProcessChildPump) (*nativeTransferSessions, error) {
	if ctx == nil || c == nil || c.host == nil || c.host.machine == nil || o == nil || o.cost == nil || o.cost.Version != 12 || pump == nil || c.host.machine.SemanticContractID() != rangeMovePooledSemanticContractID() || c.host.machine.store.ApplicationRecovery().Version != 4 {
		return nil, errInvalid
	}
	spec, err := nativeTransferSpecFromEnv()
	if err != nil || spec.cost() != *o.cost {
		return nil, errors.Join(errInvalid, err)
	}
	a, digest, err := nativeTransferAdmissionFromEnv(spec)
	deadline, ok := ctx.Deadline()
	if err != nil || !ok || deadline.UnixNano() != a.DeadlineUnixNanos || [16]byte(c.host.machine.ns.graph) != a.Graph || c.host.machine.declaration.Digest() != a.Declaration || c.host.machine.declaration.TopologyEpoch() != a.Topology {
		return nil, errors.Join(errInvalid, err)
	}
	move, err := newRangeMoveCoordinatorOn(c)
	if err != nil {
		return nil, err
	}
	frames, err := nativeTransferFrames(spec.cost(), a.Grants)
	if err != nil {
		return nil, err
	}
	policy, err := nativeScalarReadAllowance(spec.Facts, a.BudgetSeconds, a.DeadlineUnixNanos, frames, 16)
	if err != nil {
		return nil, err
	}
	// The common query owner receives honest descriptive coordinates plus the
	// exact already-authenticated pooled agreement. This is not a Cost6 profile.
	qs := nativeSelectorsSpec{Version: spec.Version, Family: spec.Family, Facts: spec.Facts, CrossPercent: spec.CrossPercent, Writers: spec.Writers, Layout: spec.Layout}
	qa := nativeSelectorsAdmission{Version: a.Version, Family: a.Family, Run: a.Run, Spec: qs, Transport: a.Transport, BudgetSeconds: a.BudgetSeconds, CausalMinimumMillis: a.CausalMinimumMillis, DeadlineUnixNanos: a.DeadlineUnixNanos, Graph: a.Graph, Topology: a.Topology, Declaration: a.Declaration, BinarySHA256: a.BinarySHA256}
	queries, err := newNativeScalarQuerySessionsFor(ctx, c, pump.Cut, qs, qa, digest, policy, rangeMovePooledSemanticContractID())
	if err != nil {
		return nil, err
	}
	cleanup, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline.Add(30*time.Second))
	allocationCtx, cancelAllocation := context.WithCancel(ctx)
	allocation := &nativeTransferAllocator{ctx: allocationCtx, cancel: cancelAllocation, owners: o, read: pump.Allocation}
	o.transferAllocation = allocation
	return &nativeTransferSessions{ctx: ctx, cleanup: cleanup, cancel: cancel, coordinator: c, move: move, queries: queries, feed: &nativeFeedSessions{ctx: ctx, coordinator: c}, allocation: allocation, spec: spec, admission: a, digest: digest}, nil
}

func nativeTransferCutDecode(wire []byte) (graphCut, error) {
	return nativeScalarWorkloadCutDecode(wire, rangeMovePooledSemanticContractID(), 2, 2)
}

// Exactly the guarded update is frozen before Fence. The recipe owns bytes,
// not a source lease or a vote; later registration still uses native authority.
func nativeTransferFrozenShape(c partitionGraphCommand) bool {
	ops := c.request.operations
	if len(ops) != 2 {
		return false
	}
	value, typed := ops[0].Value.Int64Value()
	left, e := temporal.ScopeHash(ops[0].Scope, temporal.Limits{})
	right, e2 := temporal.ScopeHash(ops[1].Scope, temporal.Limits{})
	return typed && value == 6 && e == nil && e2 == nil && left == right && ops[0].Kind == graphstate.Set && ops[0].Name == "p" && ops[0].ValueID == 6 && ops[1].Kind == graphstate.Correct && ops[1].Present && ops[0].Owner == ops[1].Owner && ops[0].Life == ops[1].Life
}

func nativeTransferFrozenDecode(c *crossProcessCoordinator, body []byte, arena *graphstate.OutputBudget) (p nativeGraphEdgeUniquePlan, command partitionGraphCommand, err error) {
	if c == nil || c.host == nil || c.host.machine.SemanticContractID() != rangeMovePooledSemanticContractID() || len(body) == 0 || len(body) > 64<<10 || arena == nil {
		return p, command, errInvalid
	}
	if err := arena.Reserve(1024 + 8*len(body)); err != nil {
		return p, command, outputBudgetFailure(err)
	}
	if err := nativeFeedDecodeJSON(body, &p); err != nil {
		return p, command, err
	}
	wire, err := json.Marshal(p)
	if err != nil || !bytes.Equal(wire, body) || p.Version != 1 || p.BusinessSHA256 != sha256.Sum256(p.Business) || p.ManifestSHA256 != sha256.Sum256(p.Manifest) || p.Calls < 1 || p.Bytes < 1 || p.Rows < 0 || p.OutputUsed < 1 {
		return p, command, errInvalid
	}
	scope, err := decodeAllocationScope(p.Scope)
	if err != nil || scope != c.host.machine.scope() {
		return p, command, errors.Join(errInvalid, err)
	}
	command, err = decodePartitionGraphCommand(p.Business, c.host.machine.limits)
	if err != nil || command.request.ns != c.host.machine.ns || !nativeTransferFrozenShape(command) {
		return p, command, errors.Join(errInvalid, err)
	}
	wire, err = encodePartitionGraphCommand(command, c.host.machine.limits)
	if err != nil || !bytes.Equal(wire, p.Business) {
		return p, command, errors.Join(errInvalid, err)
	}
	wanted, err := c.business(command.request.id, command.request.operations, command.request.revision.Provenance(), command.request.claims)
	if err != nil {
		return p, command, err
	}
	return p, command, c.matchBusiness(p.Manifest, wanted, arena)
}

func nativeTransferFreeze(ctx context.Context, c *crossProcessCoordinator, business []byte) (wire []byte, err error) {
	if ctx == nil || c == nil || c.host == nil || c.host.machine.SemanticContractID() != rangeMovePooledSemanticContractID() || len(business) == 0 || len(business) > 16<<10 {
		return nil, errInvalid
	}
	l := c.host.machine.limits
	command, err := decodePartitionGraphCommand(business, l)
	if err != nil || command.request.ns != c.host.machine.ns || !nativeTransferFrozenShape(command) {
		return nil, errors.Join(errInvalid, err)
	}
	canonical, err := encodePartitionGraphCommand(command, l)
	if err != nil || !bytes.Equal(canonical, business) {
		return nil, errors.Join(errInvalid, err)
	}
	output, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		return nil, err
	}
	arena, err := output.Child(min(l.sourceBytes, l.outputBytes))
	if err != nil {
		return nil, err
	}
	initial := arena.Remaining()
	if err := arena.Reserve(1024 + 8*len(business)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	budget := newGraphRemoteBudget(l, arena)
	endpoints, err := c.endpoints(budget)
	if err != nil {
		return nil, err
	}
	sources := make([]*remoteGraphSource, 0, c.host.machine.declaration.Len())
	defer func() {
		for _, source := range sources {
			err = errors.Join(err, source.Close())
		}
		if err != nil {
			wire = nil
		}
	}()
	captured := make([]capturedGraphSource, c.host.machine.declaration.Len())
	for i := range captured {
		part, _ := c.host.machine.declaration.PartitionAt(i)
		source, e := endpoints[part.Partition].openSource(ctx)
		if e != nil {
			return nil, e
		}
		sources = append(sources, source)
		captured[i] = capturedGraphSource{observation: source.info.observation, root: source.info.root, floor: source.info.floor, data: source}
	}
	manifest, err := c.host.planCrossCaptured(ctx, command.request.id, command.request.operations, command.request.revision.Provenance(), command.request.claims, captured, arena, graphstore.PageWork{Records: 8 * len(captured), Bytes: 4096 * len(captured)})
	if err != nil {
		return nil, err
	}
	for _, source := range sources {
		if err := source.Close(); err != nil {
			return nil, err
		}
	}
	sources = nil
	if len(manifest) > 16<<10 {
		return nil, errLimit
	}
	scope, err := encodeAllocationScope(c.host.machine.scope())
	if err != nil {
		return nil, err
	}
	if err := arena.Reserve(2048 + 4*(len(scope)+len(business)+len(manifest))); err != nil {
		return nil, outputBudgetFailure(err)
	}
	budget.mu.Lock()
	p := nativeGraphEdgeUniquePlan{Version: 1, Scope: scope, Business: business, Manifest: manifest, BusinessSHA256: sha256.Sum256(business), ManifestSHA256: sha256.Sum256(manifest), Calls: budget.calls, Bytes: budget.bytes, Rows: budget.rows, OutputUsed: initial - arena.Remaining()}
	budget.mu.Unlock()
	wire, err = json.Marshal(p)
	if len(wire) > 64<<10 {
		return nil, errLimit
	}
	return wire, err
}

func nativeTransferRegister(ctx context.Context, c *crossProcessCoordinator, body []byte) ([]byte, error) {
	if ctx == nil || c == nil || c.host == nil || c.host.machine.SemanticContractID() != rangeMovePooledSemanticContractID() {
		return nil, errInvalid
	}
	l := c.host.machine.limits
	output, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		return nil, err
	}
	arena, err := output.Child(min(l.sourceBytes, l.outputBytes))
	if err != nil {
		return nil, err
	}
	p, _, err := nativeTransferFrozenDecode(c, body, arena)
	if err != nil {
		return nil, err
	}
	m, err := decodeCrossManifest(p.Manifest, l, arena)
	if err != nil {
		return nil, err
	}
	budget := newGraphRemoteBudget(l, arena)
	endpoints, err := c.endpoints(budget)
	if err != nil {
		return nil, err
	}
	e := endpoints[m.coordinator]
	if e == nil {
		return nil, errInvalid
	}
	prior, err := e.lookup(ctx, m.tx)
	if err != nil {
		return nil, errors.Join(errCrossProcessPending, err)
	}
	if prior.present {
		if prior.status.digest != p.ManifestSHA256 || !bytes.Equal(prior.manifest, p.Manifest) {
			return nil, errInvalid
		}
		return encodeCrossStatus(prior.status)
	}
	completion, err := budget.protectCompletion(len(m.parts), len(p.Manifest))
	if err != nil {
		return nil, err
	}
	proposal, err := c.host.proposalFromAuthority(crossRegister, m.coordinator, crossTransaction{owner: c.host, manifest: p.Manifest, digest: p.ManifestSHA256}, nil, completion)
	if err != nil {
		return nil, err
	}
	status, err := e.submit(ctx, proposal)
	if err != nil {
		return nil, errors.Join(errCrossProcessPending, err)
	}
	if status.tx != m.tx || status.digest != m.digest {
		return nil, errCorrupt
	}
	return encodeCrossStatus(status)
}

// Post-GC capture starts at an explicitly supplied retained checkpoint. Only an
// immutable original below this SAME live owner's authenticated ACK vector may
// omit its expired raw sidecars; the pre-GC capture remains required evidence.
func nativeTransferRecords(ctx context.Context, h *allocationHost, input nativeFeedInput) (wire []byte, err error) {
	if ctx == nil || h == nil || h.machine == nil || h.driver == nil || h.machine.SemanticContractID() != rangeMovePooledSemanticContractID() || len(input.Original) == 0 || len(input.Original) > graphPinManifestBytes+128 {
		return nil, errInvalid
	}
	boundary, err := nativeTransferCutDecode(input.Boundary)
	if err != nil {
		return nil, err
	}
	var cert graphCertificate
	for _, c := range boundary.certificates {
		if c.scope == h.machine.scope() {
			cert = c
		}
	}
	if cert.index == 0 || input.From > input.Through || input.Through < cert.index || input.Through > h.driver.Applied() {
		return nil, errInvalid
	}
	proof, err := h.AtGraphCertificate(ctx, cert)
	if err != nil {
		return nil, err
	}
	if err := proof.Close(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	q, err := graphRetentionReader(h, ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, q.view.Close())
		if err != nil {
			wire = nil
		}
	}()
	if err := reserveComposer(q, 8192+8*len(input.Original)); err != nil {
		return nil, err
	}
	d, err := decodeRetainedFeedDefinition(input.Original)
	if err != nil {
		return nil, err
	}
	m, err := decodeGraphPinManifest(d.manifest)
	if err != nil || m.publisher.semantic != rangeMovePooledSemanticContractID() {
		return nil, errors.Join(errInvalid, err)
	}
	stored, found, err := crossControl(q, retainedFeedDefinitionKey(h.machine.ns, m.id()), graphPinManifestBytes+128)
	if err != nil || !found || !bytes.Equal(stored.Value, input.Original) {
		return nil, errors.Join(errInvalid, err)
	}
	cursor, found, err := readFeedCursor(q, m.id())
	if err != nil || !found {
		return nil, errors.Join(errCorrupt, err)
	}
	ack, ok := feedLocalPosition(m, h.machine.ns.partition, cursor.positions)
	if !ok {
		return nil, errCorrupt
	}
	view, err := h.machine.store.ApplicationView(input.Through)
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
	cfg, err := h.currentConfiguration()
	if err != nil {
		return nil, err
	}
	source := nativeColumnSource{binding: nativeColumnBinding{Version: 2, Policy: nativeColumnPolicy{FrameBytes: 32 << 10, ChunkRows: 16}}}
	source.hosts[0], source.views[0], source.roots[0], source.final[0], source.configurations[0] = h, view, root, cert, cfg
	page := nativeFeedRetainedRecordPage{Page: nativeFeedRecordPage{Partition: h.machine.ns.partition, After: input.From, Next: input.From, Through: input.Through, Root: root.Image, Frames: []nativeColumnFrame{}}, BeforeACK: []bool{}}
	for len(page.Page.Frames) < 4 && page.Page.Next < input.Through {
		index := page.Page.Next + 1
		actual, e := h.machine.feedNativeFrame(q, index, 32<<10, ack)
		if e != nil {
			return nil, e
		}
		frame, e := source.frame(ctx, 0, index)
		if e != nil {
			if !actual.originalBeforeCut || !errors.Is(e, raftlog.ErrApplicationExpired) || len(actual.changes) != 0 || len(actual.originalOutcome) == 0 {
				return nil, e
			}
			frame = nativeColumnFrame{Partition: actual.partition, Index: actual.index, Root: actual.root, Outcome: bytes.Clone(actual.outcome), CDC: bytes.Clone(actual.changes), Registration: bytes.Clone(actual.registration), OriginalOutcome: bytes.Clone(actual.originalOutcome)}
		}
		if frame.Root != actual.root || !bytes.Equal(frame.Outcome, actual.outcome) || !bytes.Equal(frame.CDC, actual.changes) {
			return nil, errCorrupt
		}
		page.Page.Frames = append(page.Page.Frames, frame)
		page.BeforeACK = append(page.BeforeACK, actual.originalBeforeCut)
		page.Page.Next++
	}
	page.Page.Complete = page.Page.Next == input.Through
	wire, err = json.Marshal(page)
	if len(wire) > 1<<20 {
		return nil, errLimit
	}
	return wire, err
}

func (s *nativeTransferSessions) feedPolicy() retainedFeedPolicy {
	l := s.coordinator.host.machine.limits
	pages := nativeTransferPages(s.spec.cost())
	return retainedFeedPolicy{pageBytes: 64 << 10, pageRows: 16, maxPages: pages, maxAttempts: 4*pages + s.spec.Facts/8 + 1024, operationRows: l.sourceRows, operationBytes: l.sourceBytes, operationOutput: l.outputBytes, deadlineUnixNanos: s.admission.DeadlineUnixNanos}
}

func (s *nativeTransferSessions) serve(ctx context.Context, r processRequest) (wire []byte, err error) {
	if s == nil || ctx == nil {
		return nil, errInvalid
	}
	q, err := nativeTransferInputDecode(r)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errInvalid
	}
	terminal := nativeTransferTerminalControl(r)
	parent := s.ctx
	if terminal {
		if s.cleanupCalls == 128 {
			return nil, errLimit
		}
		s.cleanupCalls++
		parent = s.cleanup
	} else {
		frames, e := nativeTransferFrames(s.spec.cost(), s.admission.Grants)
		if e != nil || s.calls >= frames*8+s.spec.Facts/8+s.admission.BudgetSeconds/60+2048 {
			return nil, errors.Join(errLimit, e)
		}
		s.calls++
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	op, cancel := context.WithCancel(parent)
	stop := func() bool { return false }
	if !terminal {
		stop = context.AfterFunc(ctx, cancel)
	}
	defer func() { stop(); cancel() }()
	if err := op.Err(); err != nil {
		return nil, err
	}
	started := time.Now()
	out := nativeTransferReply{Version: 1, Action: q.Action}
	switch q.Action {
	case "acquire":
		out.Body, err = s.allocation.acquire(q.Sequence, q.Count)
	case "next":
		out.Body, err = s.allocation.next(q.Cursor, q.Sequence, int(q.Count)) // #nosec G115 -- strict decoder <=32.
	case "query":
		out.Body, err = s.queries.serveDecoded(op, r, *q.Query)
	case "freeze":
		if s.frozen >= 2 {
			return nil, errLimit
		}
		s.frozen++ // A failed capture does not refund the finite recipe grant.
		out.Body, err = nativeTransferFreeze(op, s.coordinator, q.Original)
	case "register":
		out.Body, err = nativeTransferRegister(op, s.coordinator, q.Original)
	case "feed":
		if q.Feed.Action == "records" {
			out.Body, err = nativeTransferRecords(op, s.coordinator.host, *q.Feed)
		} else {
			out.Body, err = s.feed.serveDecoded(op, r, *q.Feed, s.feedPolicy(), nativeTransferCutDecode, parent, s.spec.cost(), s.admission.BudgetSeconds)
		}
	case "move-prepare":
		var intent rangeMoveIntent
		intent, err = s.coordinator.host.PreparePooledRangeMove(op, requestID(q.Request), q.Range, 8, int(s.admission.Grants.MaxPages), 32, 64<<10, s.admission.Grants.pools()) // #nosec G115 -- immutable pages <=4M.
		if err == nil {
			out.Body, err = encodeRangeMoveIntent(intent)
			if err == nil {
				stored := false
				for i, prior := range s.prepared {
					if len(prior) == 0 {
						s.prepared[i] = bytes.Clone(out.Body)
						stored = true
						break
					}
					if bytes.Equal(prior, out.Body) {
						stored = true
						break
					}
				}
				if !stored {
					err = errLimit
				}
			}
		}
	case "move-run":
		found := false
		for _, original := range s.prepared {
			found = found || bytes.Equal(original, q.Original)
		}
		if !found {
			return nil, errInvalid
		}
		var intent rangeMoveIntent
		intent, err = decodeRangeMoveIntent(q.Original)
		if err == nil {
			var result rangeMoveResult
			result, err = s.move.Run(op, intent)
			out.Body, err = nativeTransferMoveReply(result, err)
		}
	case "move-resume", "lookup":
		var prior rangeMoveResult
		prior, err = decodeGraphE17MoveResult(processResponse{aux: q.Original})
		if err == nil || errors.Is(err, errRangeMovePending) {
			if q.Action == "move-resume" {
				var result rangeMoveResult
				result, err = s.move.Resume(op, prior.resume)
				out.Body, err = nativeTransferMoveReply(result, err)
			} else {
				var result rangeMoveOriginal
				result, err = s.move.LookupOriginal(op, prior.resume, namespace{s.coordinator.host.machine.ns.graph, q.Origin}, q.Control, q.Key)
				if err == nil {
					out.Body, err = json.Marshal(nativeTransferOriginalObservation{result.origin.partition, result.key, result.value, result.originalIndex, result.evaluationIndex, result.found})
				}
			}
		}
	}
	out.Nanos = time.Since(started).Nanoseconds()
	out.Failure = nativeColumnProcessErrorCode(err)
	wire, encoded := json.Marshal(out)
	if encoded != nil || len(wire) > 1<<20 {
		return nil, errors.Join(errLimit, err, encoded)
	}
	return wire, err
}

type nativeTransferOriginalObservation struct {
	Origin                         uint64
	Key, Value                     []byte
	OriginalIndex, EvaluationIndex uint64
	Found                          bool
}

func nativeTransferMoveReply(result rangeMoveResult, cause error) ([]byte, error) {
	if result.resume.request == (requestID{}) {
		return nil, cause
	}
	wire, err := encodeGraphE17MoveResult(result, cause)
	return wire, errors.Join(cause, err)
}

func (s *nativeTransferSessions) close() error {
	if s == nil {
		return nil
	}
	s.allocation.close()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	defer s.cancel()
	return errors.Join(s.feed.close(), s.queries.close())
}

// Reconnect keeps the SAME coordinator/run/intent and fixed admission. Every
// actual transient read must close first; copied tokens cannot waive that gate.
func nativeTransferReconnect(ctx context.Context, h *allocationHost, old *crossProcessCoordinator, sessions *graphProcessCertifiedSessions, server *graphSourceServer, r processRequest, workers, queued int, active bool, owner *nativeTransferSessions) (*crossProcessCoordinator, error) {
	if owner == nil || owner.coordinator != old || owner.move == nil || owner.move.base != old {
		return old, errInvalid
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	owner.feed.mu.Lock()
	defer owner.feed.mu.Unlock()
	if owner.feed.owner != nil || owner.feed.snapshot != nil {
		return old, errLimit
	}
	owner.move.mu.Lock()
	for _, run := range owner.move.runs {
		if run.busy {
			owner.move.mu.Unlock()
			return old, errLimit
		}
	}
	owner.move.mu.Unlock()
	// Existing retained-route reconnect authenticates exact peer identities and
	// preserves installed run originals; it does not create a new source or pin.
	return pairedGraphFaultReconnect(ctx, h, old, sessions, nil, server, r, workers, queued, active, true)
}

func newNativeTransferGC(ctx context.Context, h *allocationHost, member uint64, a nativeTransferAdmission) (*nativeRetentionOwner, error) {
	if ctx == nil || h == nil || h.machine == nil || h.driver == nil || h.machine.SemanticContractID() != rangeMovePooledSemanticContractID() || h.machine.store.ApplicationRecovery().Version != 4 || h.machine.store.ApplicationRetention().Version != 1 || member == 0 || member > 6 {
		return nil, errInvalid
	}
	expected, _, err := nativeTransferAdmissionFromEnv(a.Spec)
	deadline, bounded := ctx.Deadline()
	if err != nil || expected != a || !bounded || deadline.UnixNano() != a.DeadlineUnixNanos || [16]byte(h.machine.ns.graph) != a.Graph || h.machine.declaration.Digest() != a.Declaration || h.machine.ns.partition != 3+5*((member-1)/3) {
		return nil, errors.Join(errInvalid, err)
	}
	var run [16]byte
	decoded, err := hex.DecodeString(a.Run)
	if err != nil || len(decoded) != len(run) {
		return nil, errInvalid
	}
	copy(run[:], decoded)
	pages := h.machine.store.ApplicationLimits().RetainedApplicationRecords + 6
	base := nativeRetentionReceipt{Version: 1, Profile: nativeTransferFormat, Run: run, Member: member, PID: uint64(os.Getpid()), DeadlineUnixNanos: a.DeadlineUnixNanos}
	p := h.machine.store.ApplicationLimits()
	return newNativeRetentionOwnerFor(ctx, h, base, pages+128, pages, min(512, p.MaxPageRows), min(1<<20, p.MaxInstallBytes), true)
}
