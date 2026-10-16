package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Drives the genuine six-voter in-process Host fixture. This is a transport
// unit guard; process acceptance remains the separate six OS child test.
type nativeGraphTestPump struct {
	n     *allocationTestNetwork
	group int
	host  *allocationHost
}

func (p *nativeGraphTestPump) drive(ctx context.Context, event allocationEvent) (allocationEvent, error) {
	result := event
	queue := event.packets
	result.packets = nil
	for count := 0; len(queue) > 0; count++ {
		if err := ctx.Err(); err != nil {
			return allocationEvent{}, err
		}
		if count >= 4096 {
			return allocationEvent{}, errLimit
		}
		packet := queue[0]
		queue = queue[1:]
		if packet.Snapshot || packet.To < 1 || packet.To > 3 {
			return allocationEvent{}, errInvalid
		}
		h := p.n.hosts[p.group][packet.To-1]
		e, err := h.Step(packet)
		if err != nil {
			return allocationEvent{}, err
		}
		if len(queue)+len(e.packets) > 128 {
			return allocationEvent{}, errLimit
		}
		queue = append(queue, e.packets...)
		result.replies = append(result.replies, e.replies...)
		result.crossReplies = append(result.crossReplies, e.crossReplies...)
		result.requestReplies = append(result.requestReplies, e.requestReplies...)
		result.cutReplies = append(result.cutReplies, e.cutReplies...)
		result.moveReplies = append(result.moveReplies, e.moveReplies...)
	}
	return result, nil
}
func (p *nativeGraphTestPump) Allocation(ctx context.Context, q allocationQuery) (allocationReply, error) {
	e, err := p.host.Read(q)
	if err != nil {
		return allocationReply{}, err
	}
	id := e.readID
	e, err = p.drive(ctx, e)
	if err != nil {
		return allocationReply{}, err
	}
	for _, r := range e.replies {
		if r.id == id && r.question == q {
			return r, nil
		}
	}
	return allocationReply{}, graphstore.ErrSourceUnavailable
}
func (p *nativeGraphTestPump) Move(ctx context.Context, q rangeMoveQuery) (rangeMoveReply, error) {
	e, err := p.host.ReadRangeMove(ctx, q)
	if err != nil {
		return rangeMoveReply{}, err
	}
	id := e.readID
	e, err = p.drive(ctx, e)
	if err != nil {
		return rangeMoveReply{}, err
	}
	for _, r := range e.moveReplies {
		if r.id == id && r.question == q {
			return r, nil
		}
	}
	return rangeMoveReply{}, graphstore.ErrSourceUnavailable
}
func (p *nativeGraphTestPump) Cut(ctx context.Context, q graphCutQuery) (graphCutReply, error) {
	e, err := p.host.ReadGraphCut(q)
	if err != nil {
		return graphCutReply{}, err
	}
	id := e.readID
	e, err = p.drive(ctx, e)
	if err != nil {
		return graphCutReply{}, err
	}
	for _, r := range e.cutReplies {
		if r.id == id && r.question == q {
			return r, nil
		}
	}
	return graphCutReply{}, graphstore.ErrSourceUnavailable
}
func (p *nativeGraphTestPump) Cross(ctx context.Context, q crossQuery) (crossReply, error) {
	e, err := p.host.ReadCrossPhase(q.tx, q.phase, q.nonce)
	if err != nil {
		return crossReply{}, err
	}
	e, err = p.drive(ctx, e)
	if err != nil {
		return crossReply{}, err
	}
	for _, r := range e.crossReplies {
		if r.err != nil {
			return r, nil
		}
		if r.proof.question == q {
			return r, nil
		}
	}
	return crossReply{}, graphstore.ErrSourceUnavailable
}
func (p *nativeGraphTestPump) Request(ctx context.Context, q crossRequestQuestion) (crossRequestReply, error) {
	e, err := p.host.readCrossRequest(q)
	if err != nil {
		return crossRequestReply{}, err
	}
	id := e.readID
	e, err = p.drive(ctx, e)
	if err != nil {
		return crossRequestReply{}, err
	}
	for _, r := range e.requestReplies {
		if r.err != nil {
			return r, nil
		}
		if r.proof.id == id && r.proof.question == q {
			return r, nil
		}
	}
	return crossRequestReply{}, graphstore.ErrSourceUnavailable
}
func (p *nativeGraphTestPump) Submit(ctx context.Context, proposal allocationProposal) ([]byte, error) {
	e, err := p.host.Submit(proposal)
	if err != nil {
		p.n.t.Logf("native submit original error partition=%d: %T %v", p.host.machine.ns.partition, err, err)
		return nil, err
	}
	if _, err = p.drive(ctx, e); err != nil {
		p.n.t.Logf("native drive original error partition=%d: %T %v", p.host.machine.ns.partition, err, err)
		return nil, err
	}
	return p.host.machine.store.ApplicationRecord(ctx, p.host.driver.Applied(), true, 64<<10)
}

type nativeGraphTestTransport struct {
	service *graphPeerService
	pause   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (p *nativeGraphTestTransport) Peer() graphPeerIdentity { return p.service.peer }
func (p *nativeGraphTestTransport) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	r, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	response, err := p.service.serve(ctx, wire)
	if err != nil {
		return nil, err
	}
	if p.pause.Load() && r.kind == sourceGet {
		select {
		case p.entered <- struct{}{}:
		default:
		}
		// Test-controlled reply pause AFTER real native capture/access. This does
		// not replace a proof, manufacture data or simulate physical IO failure.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.release:
		}
	}
	return response, nil
}
func authenticatedNativeSourceFixture(t *testing.T, ctx context.Context) (*remoteGraphSource, *nativeGraphTestTransport) {
	t.Helper()
	n, server, _, _ := sourceSessionFixture(t)
	pump := &nativeGraphTestPump{n: n, group: 0, host: n.hosts[0][0]}
	service, err := newGraphPeerService(pump.host, server, pump)
	if err != nil {
		t.Fatal(err)
	}
	transport := &nativeGraphTestTransport{service: service, entered: make(chan struct{}, 1), release: make(chan struct{})}
	output, err := graphstate.NewOutputBudget(32 << 20)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := newGraphAuthenticatedEndpoint(n.d, transport, newGraphRemoteBudget(pump.host.machine.limits, output))
	if err != nil {
		t.Fatal(err)
	}
	source, err := endpoint.openSource(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return source, transport
}
func TestGraphSourceClientOwnerCancellationStopsPausedNativeReply(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	owner, cancelOwner := context.WithCancel(t.Context())
	defer cancelOwner()
	source, transport := authenticatedNativeSourceFixture(t, owner)
	transport.pause.Store(true)
	defer close(transport.release)
	done := make(chan error, 1)
	key := graphstore.RangePublicationKey(graphstore.Namespace{Graph: graphstate.GraphID{1}, Partition: 3}, 1)
	go func() {
		row, found, err := source.Get(t.Context(), key, 64<<10)
		if found || len(row.Key)+len(row.Value) != 0 {
			done <- errors.New("cancelled native peer returned data")
			return
		}
		done <- err
	}()
	deadline, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	select {
	case <-transport.entered:
	case <-deadline.Done():
		t.Fatal("native authenticated peer reply was not paused")
	}
	cancelOwner()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-deadline.Done():
		t.Fatal("client source owner cancellation did not reach in-flight RPC")
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestGraphSourceClientCloseCancelsPausedLiveQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	source, transport := authenticatedNativeSourceFixture(t, t.Context())
	transport.pause.Store(true)
	defer close(transport.release)
	queryDone := make(chan error, 1)
	key := graphstore.RangePublicationKey(graphstore.Namespace{Graph: graphstate.GraphID{1}, Partition: 3}, 1)
	go func() { _, _, err := source.Get(t.Context(), key, 64<<10); queryDone <- err }()
	deadline, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	select {
	case <-transport.entered:
	case <-deadline.Done():
		t.Fatal("native authenticated reply not paused")
	}
	closed := make(chan error, 1)
	go func() { closed <- source.Close() }()
	select {
	case err := <-queryDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-deadline.Done():
		t.Fatal("client Close waited for query mutex before initiating cancellation")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-deadline.Done():
		t.Fatal("client close did not release native source")
	}
}

func TestGraphSourceAuthenticatedNativeRootAndGetRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, kind := range []string{"root", "found", "absent"} {
		t.Run(kind, func(t *testing.T) {
			source, transport := authenticatedNativeSourceFixture(t, t.Context())
			defer func() {
				if err := source.Close(); err != nil {
					t.Error(err)
				}
			}()
			lease, err := transport.service.sources.lease(source.info.cookie)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "root" {
				want, err := lease.view.RootBounded(t.Context(), 172)
				if err != nil {
					t.Fatal(err)
				}
				got, err := source.RootBounded(t.Context(), 172)
				if err != nil || got.Generation != want.Generation || got.Index != want.Index || got.ImageHash != want.ImageHash || !bytes.Equal(got.Image, want.Image) || len(got.Image) != 172 {
					t.Fatal("genuine bounded native root framing", got, err)
				}
				return
			}
			key := graphstore.RangePublicationKey(graphstore.Namespace{Graph: graphstate.GraphID{1}, Partition: 3}, 1)
			if kind == "absent" {
				key = graphstore.RangePublicationKey(graphstore.Namespace{Graph: graphstate.GraphID{1}, Partition: 3}, 7)
			}
			want, found, err := lease.view.Get(t.Context(), key, 64<<10)
			if err != nil || found != (kind == "found") {
				t.Fatal("independent native point control", found, err)
			}
			got, present, err := source.Get(t.Context(), key, 64<<10)
			if err != nil || present != found || got.Deleted != want.Deleted || !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) {
				t.Fatal("genuine native point response framing", kind, present, got, err)
			}
		})
	}
}

func TestGraphSourceClientCloseUsesPrepaidCapacityAfterCallLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	source, transport := authenticatedNativeSourceFixture(t, t.Context())
	// Independent protocol ceiling: consume the remaining 1022 calls after the
	// prepaid close slot and real sourceOpen, with zero-payload admissions.
	budget := source.endpoint.budget
	if budget.calls != 2 {
		t.Fatal("open did not prepay one close call", budget.calls)
	}
	for range 1022 {
		if err := budget.admit(0, 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.endpoint.call(t.Context(), graphSourceRequest{kind: sourceRoot, cookie: source.info.cookie, maximum: 172}, 1024); !errors.Is(err, errLimit) {
		t.Fatal("exhausted ordinary call allowance", err)
	}
	if err := source.Close(); err != nil {
		t.Fatal("known native owner could not close after exhaustion", err)
	}
	lease, err := transport.service.sources.lease(source.info.cookie)
	if err != nil || !lease.done() {
		t.Fatal("prepaid close retained native owner", err)
	}
	if budget.calls != 1024 {
		t.Fatal("close reset/widened ordinary global calls", budget.calls)
	}
}

func TestGraphSourceQueuedAuthenticatedStreamWaitUsesCurrentCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	source, transport := authenticatedNativeSourceFixture(t, t.Context())
	transport.pause.Store(true)
	defer close(transport.release)
	first := make(chan error, 1)
	key := graphstore.RangePublicationKey(graphstore.Namespace{Graph: graphstate.GraphID{1}, Partition: 3}, 1)
	go func() { _, _, err := source.Get(t.Context(), key, 64<<10); first <- err }()
	select {
	case <-transport.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual native reply never paused")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	queued := make(chan error, 1)
	go func() {
		_, err := source.endpoint.call(ctx, graphSourceRequest{kind: sourceRoot, cookie: source.info.cookie, maximum: 172}, 1024)
		queued <- err
	}()
	select {
	case err := <-queued:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("queued stream deadline sentinel", err)
		}
	case <-time.After(2 * time.Second):
		source.cancel()
		<-first
		// Unblock and drain the actual queued request before Goexit.
		<-queued
		t.Fatal("cancelled request remained queued behind another authenticated RPC")
	}
	source.cancel()
	<-first
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestGraphSourceQueuedNativeServiceWaitUsesCurrentCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	source, transport := authenticatedNativeSourceFixture(t, t.Context())
	lease, err := transport.service.sources.lease(source.info.cookie)
	if err != nil {
		t.Fatal(err)
	}
	pause := &pausedGraphSourceData{ApplicationReadSource: lease.view, entered: make(chan struct{}, 1), release: make(chan struct{})}
	lease.mu.Lock()
	lease.read = pause
	lease.mu.Unlock()
	defer close(pause.release)
	first := make(chan error, 1)
	key := graphstore.RangePublicationKey(graphstore.Namespace{Graph: graphstate.GraphID{1}, Partition: 3}, 1)
	go func() { _, _, err := source.Get(t.Context(), key, 64<<10); first <- err }()
	select {
	case <-pause.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual native service data never paused")
	}
	// The controlled native-data entry happens after sequence publication. This
	// queued request has the same genuine caller association and owned cookie.
	wire, err := encodeGraphSourceRequest(graphSourceRequest{kind: sourceRoot, sequence: source.endpoint.stream.sequence + 1, nonce: [16]byte{213}, cookie: source.info.cookie, maximum: 172})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	queued := make(chan error, 1)
	go func() { _, err := transport.service.serve(ctx, wire); queued <- err }()
	select {
	case err := <-queued:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("queued service deadline sentinel", err)
		}
	case <-time.After(2 * time.Second):
		lease.cancel()
		<-first
		<-queued
		t.Fatal("cancelled native service remained queued behind owned data work")
	}
	lease.cancel()
	<-first
	// Direct native close cleans the owner; the queued inert envelope does not
	// mutate this genuine endpoint's request sequence or become proof authority.
	if err := transport.service.sources.closeSource(source.info.cookie); err != nil {
		t.Fatal(err)
	}
}

func TestGraphSourceCapturedMetadataBoundsCancellationAndExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	source, transport := authenticatedNativeSourceFixture(t, t.Context())
	root, err := source.RootBounded(t.Context(), 172)
	if err != nil || len(root.Image) != 172 || cap(root.Image) != 172 {
		t.Fatal("exact admitted metadata", root, err)
	}
	wider, err := source.RootBounded(t.Context(), 4096)
	if err != nil || !bytes.Equal(wider.Image, root.Image) {
		t.Fatal("maximum is image ceiling", err)
	}
	clear(wider.Image)
	if !bytes.Equal(source.info.root.Image, root.Image) {
		t.Fatal("metadata backing alias")
	}
	for _, tc := range []struct {
		maximum int
		want    error
	}{{171, raftlog.ErrLimit}, {-1, raftlog.ErrInvalid}} {
		before := source.endpoint.budget.output.Remaining()
		got, err := source.RootBounded(t.Context(), tc.maximum)
		if !errors.Is(err, tc.want) || got.Image != nil || source.endpoint.budget.output.Remaining() != before {
			t.Fatal("metadata ceiling refusal before copy/charge", got, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got, err := source.RootBounded(ctx, 172)
	if !errors.Is(err, context.Canceled) || got.Image != nil {
		t.Fatal("current metadata context", got, err)
	}
	// Keep the actual sourceRoot service codec directly exercised, independent of
	// the metadata fast path (same configured endpoint/native retained capture).
	reply, err := source.endpoint.call(t.Context(), graphSourceRequest{kind: sourceRoot, cookie: source.info.cookie, maximum: 172}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	body, err := graphBody(reply.body, "GSB\x01", 512)
	if err != nil {
		t.Fatal(err)
	}
	cursor := graphCursor{b: body}
	native := readSourceRoot(&cursor)
	if cursor.err != nil || len(cursor.b) != 0 || native.Index != root.Index || native.ImageHash != root.ImageHash || !bytes.Equal(native.Image, root.Image) {
		t.Fatal("authenticated native sourceRoot service tuple", native, cursor.err)
	}
	source.cancel()
	got, err = source.RootBounded(t.Context(), 172)
	if !errors.Is(err, graphstore.ErrSourceUnavailable) || got.Image != nil {
		t.Fatal("metadata owner expiry", got, err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	lease, err := transport.service.sources.lease(source.info.cookie)
	if err != nil || !lease.done() {
		t.Fatal("metadata source cleanup", err)
	}
}

type recordedNativeSourceWire struct {
	*nativeGraphTestTransport
	replyBytes int
}

func (w *recordedNativeSourceWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	reply, err := w.nativeGraphTestTransport.RoundTrip(ctx, wire)
	w.replyBytes = len(reply)
	return reply, err
}
func TestGraphSourceRemoteScanChargesNativeFutureVisitsInSharedLedger(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	source, transport := authenticatedNativeSourceFixture(t, t.Context())
	defer source.Close()
	pump := transport.service.pump.(*nativeGraphTestPump)
	scope, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := pump.host.PreparePartitionGraphOperations(pump.n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: scope, Name: "later"}}, 29, nil)
	if err != nil {
		t.Fatal(err)
	}
	pump.n.submitPartitionGraph(0, proposal, reasonNone)
	wire := &recordedNativeSourceWire{nativeGraphTestTransport: transport}
	source.endpoint.transport = wire
	request := graphSourceRequest{kind: sourceScan, sequence: 1, nonce: [16]byte{1}, cookie: source.info.cookie, maximum: 64 << 10, rows: 128}
	encoded, err := encodeGraphSourceRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	before := source.endpoint.budget.bytes
	page, err := source.Scan(t.Context(), nil, nil, nil, raftlog.ReadBudget{Rows: 128, Bytes: 64 << 10})
	if err != nil || page.Visited <= len(page.Rows) {
		t.Fatal("genuine older source did not traverse future keys", page.Visited, len(page.Rows), err)
	}
	// Each physical visit owns at least64-byte work metadata and a nonempty key;
	// retained key/value payload beyond that first byte and continuation are
	// additional. The received frame and request are independently accounted.
	minimum := 65*page.Visited + len(page.Next)
	for _, row := range page.Rows {
		minimum += len(row.Key) + len(row.Value) - 1
	}
	consumed := source.endpoint.budget.bytes - before
	if consumed < len(encoded)+wire.replyBytes+minimum {
		t.Fatal("shared source ledger omitted native future/retained work", consumed, len(encoded), wire.replyBytes, minimum, page.Visited, len(page.Rows))
	}
}

func TestRemoteGraphSourceRootBindingLimitsDirectContract(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	source, _ := authenticatedNativeSourceFixture(t, t.Context())
	root, err := source.Root()
	if err != nil || len(root.Image) != 172 || cap(root.Image) != 172 {
		t.Fatal("direct owned remote Root", root, err)
	}
	binding, err := source.ApplicationBinding(t.Context())
	want := raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 3, Group: [16]byte{4}}, SemanticContractID: certifiedGraphSemanticContractID()}
	if err != nil || binding != want {
		t.Fatal("direct captured binding", binding, err)
	}
	limits := source.ReadLimits()
	if limits != source.info.limits || limits.Rows <= 0 || limits.Bytes <= 0 {
		t.Fatal("direct immutable limits", limits)
	}
	clear(root.Image)
	if sha256.Sum256(source.info.root.Image) != source.info.root.ImageHash {
		t.Fatal("Root image borrowed source owner")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if binding, err := source.ApplicationBinding(ctx); !errors.Is(err, context.Canceled) || binding != (raftlog.ApplicationBinding{}) {
		t.Fatal("direct binding current cancellation", binding, err)
	}
	var nilSource *remoteGraphSource
	if root, err := nilSource.Root(); !errors.Is(err, errInvalid) || root.Image != nil {
		t.Fatal("nil Root", root, err)
	}
	if binding, err := nilSource.ApplicationBinding(t.Context()); !errors.Is(err, errInvalid) || binding != (raftlog.ApplicationBinding{}) {
		t.Fatal("nil binding", binding, err)
	}
	if nilSource.ReadLimits() != (raftlog.ReadBudget{}) {
		t.Fatal("nil limits selected defaults")
	}
	// Exhaust the original output ledger; metadata cannot reset a fresh budget,
	// while Close still uses its one pre-admitted cleanup exchange.
	if err := source.endpoint.budget.output.Reserve(source.endpoint.budget.output.Remaining()); err != nil {
		t.Fatal(err)
	}
	if root, err := source.Root(); !errors.Is(err, errLimit) || root.Image != nil {
		t.Fatal("direct metadata exhaustion", root, err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if root, err := source.Root(); !errors.Is(err, raftlog.ErrClosed) || root.Image != nil {
		t.Fatal("direct closed Root", root, err)
	}
	if binding, err := source.ApplicationBinding(t.Context()); !errors.Is(err, raftlog.ErrClosed) || binding != (raftlog.ApplicationBinding{}) {
		t.Fatal("direct closed binding", binding, err)
	}
	if source.ReadLimits() != limits {
		t.Fatal("closed metadata limits changed")
	}
	if message := (graphWireError{mask: 1}).Error(); message != "graphapply: remote source operation refused" {
		t.Fatal("direct remote error message", message)
	}
}

type corruptedNativeSourceReply struct{ graphPeerWireTransport }

func (w corruptedNativeSourceReply) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	response, err := w.graphPeerWireTransport.RoundTrip(ctx, wire)
	if err != nil {
		return nil, err
	}
	parsed, err := decodeGraphWireResponse(response)
	if err != nil {
		return nil, err
	}
	parsed.nonce[0] ^= 1
	return encodeGraphWireResponse(parsed)
}
func TestGraphSourceAuthenticatedCompletionMismatchAndDirectNilRefusals(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	source, transport := authenticatedNativeSourceFixture(t, t.Context())
	defer source.Close()
	if _, err := newGraphAuthenticatedEndpoint(transport.service.host.machine.declaration, nil, source.endpoint.budget); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	wrong := transport.Peer()
	wrong.scope.topology++
	if _, err := newGraphAuthenticatedEndpoint(transport.service.host.machine.declaration, &fixedWrongGraphPeer{graphPeerWireTransport: transport, peer: wrong}, source.endpoint.budget); !errors.Is(err, errInvalid) {
		t.Fatal("changed configured authority", err)
	}
	source.endpoint.transport = corruptedNativeSourceReply{graphPeerWireTransport: transport}
	key := graphstore.RangePublicationKey(graphstore.Namespace{Graph: graphstate.GraphID{1}, Partition: 3}, 1)
	row, found, err := source.Get(t.Context(), key, 64<<10)
	if !errors.Is(err, errInvalid) || found || row.Key != nil || row.Value != nil {
		t.Fatal("unmatched authenticated completion supplied data", row, found, err)
	}
	source.endpoint.transport = transport
	var endpoint *graphAuthenticatedEndpoint
	if _, err := endpoint.call(t.Context(), graphSourceRequest{kind: sourceRoot}, 1024); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	var absentGate graphRequestGate
	if !errors.Is(absentGate.lock(t.Context()), errInvalid) || absentGate.tryLock() {
		t.Fatal("unwired gate")
	}
	gate := newGraphRequestGate()
	//nolint:staticcheck // Nil context refusal is the required invalid-input contract.
	if !errors.Is(gate.lock(nil), errInvalid) {
		t.Fatal("nil gate context")
	}
	//nolint:staticcheck // Nil context refusal is the required invalid-input contract.
	if root, err := source.RootBounded(nil, 172); !errors.Is(err, raftlog.ErrInvalid) || root.Image != nil {
		t.Fatal(err)
	}
}

type fixedWrongGraphPeer struct {
	graphPeerWireTransport
	peer graphPeerIdentity
}

func (w *fixedWrongGraphPeer) Peer() graphPeerIdentity { return w.peer }

func TestRemoteGraphSourceDirectDataAdmissionAndOwnerExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	var missing *remoteGraphSource
	if row, found, err := missing.Get(t.Context(), []byte("missing"), 1); !errors.Is(err, errInvalid) || found || len(row.Key)+len(row.Value) != 0 {
		t.Fatal("nil Get published data", row, found, err)
	}
	if page, err := missing.Scan(t.Context(), nil, nil, nil, raftlog.ReadBudget{Rows: 1, Bytes: 1}); !errors.Is(err, errInvalid) || len(page.Rows) != 0 || page.Complete {
		t.Fatal("nil Scan published a page", page, err)
	}
	if err := missing.Close(); !errors.Is(err, errInvalid) {
		t.Fatal("nil Close", err)
	}
	owner, cancel := context.WithCancel(t.Context())
	defer cancel()
	source, transport := authenticatedNativeSourceFixture(t, owner)
	defer source.Close()
	if row, found, err := source.Get(t.Context(), []byte("missing"), -1); !errors.Is(err, errLimit) || found || len(row.Key)+len(row.Value) != 0 {
		t.Fatal("negative maximum published data", row, found, err)
	}
	// Exercise the local TTL check before the owner's asynchronous deadline
	// notification. The retained native capture/identity is unchanged.
	source.deadline = time.Now().Add(-time.Hour)
	if row, found, err := source.Get(t.Context(), []byte("missing"), 1); !errors.Is(err, graphstore.ErrSourceUnavailable) || found || len(row.Key)+len(row.Value) != 0 {
		t.Fatal("expired Get published data", row, found, err)
	}
	if page, err := source.Scan(t.Context(), nil, nil, nil, raftlog.ReadBudget{Rows: 1, Bytes: 1}); !errors.Is(err, graphstore.ErrSourceUnavailable) || len(page.Rows) != 0 || page.Complete {
		t.Fatal("expired Scan published a page", page, err)
	}
	source.deadline = time.Now().Add(time.Hour)
	cancel()
	if row, found, err := source.Get(t.Context(), []byte("missing"), 1); !errors.Is(err, context.Canceled) || !errors.Is(err, graphstore.ErrSourceUnavailable) || found || len(row.Key)+len(row.Value) != 0 {
		t.Fatal("owner cancellation Get published data", row, found, err)
	}
	if page, err := source.Scan(t.Context(), nil, nil, nil, raftlog.ReadBudget{Rows: 1, Bytes: 1}); !errors.Is(err, context.Canceled) || !errors.Is(err, graphstore.ErrSourceUnavailable) || len(page.Rows) != 0 || page.Complete {
		t.Fatal("owner cancellation Scan published a page", page, err)
	}
	if err := source.Close(); err != nil {
		t.Fatal("prepaid owner cleanup", err)
	}
	// These are inert codec guards, not fabricated expiry evidence. Production
	// status3 originates only after the actual stored GSC/floor authentication.
	base := graphWireResponse{peer: transport.Peer(), issuer: [16]byte{1}, kind: sourceCertifiedOpen, sequence: 1, nonce: [16]byte{2}}
	for _, agreement := range []raftlog.ApplicationSemanticContractID{retainedGraphSemanticContractID(), collectedGraphSemanticContractID()} {
		for _, cause := range []error{errGraphPinExpired, errGraphPinReleased} {
			r := base
			r.peer.scope.semantic, r.err = agreement, cause
			wire, err := encodeGraphWireResponse(r)
			if err != nil {
				t.Fatal("inherited pin classification", err)
			}
			decoded, err := decodeGraphWireResponse(wire)
			if err != nil || !bytes.Equal(wire[:4], []byte{'G', 'S', 'R', 2}) || !errors.Is(decoded.err, cause) || errors.Is(decoded.err, graphstore.ErrSourceUnavailable) || len(decoded.body) != 0 {
				t.Fatal("pin classification changed", decoded, err)
			}
		}
	}
	expired := base
	expired.peer.scope.semantic = collectedGraphSemanticContractID()
	expired.err = errors.Join(errGraphHistoryExpired, raftlog.ErrApplicationExpired)
	wire, err := encodeGraphWireResponse(expired)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGraphWireResponse(wire)
	if err != nil || !errors.Is(decoded.err, errGraphHistoryExpired) || !errors.Is(decoded.err, raftlog.ErrApplicationExpired) || errors.Is(decoded.err, errGraphPinExpired) || errors.Is(decoded.err, graphstore.ErrSourceUnavailable) || len(decoded.body) != 0 {
		t.Fatal("known graph/native expiry lost", decoded, err)
	}
	if graphRetentionStatus(raftlog.ErrApplicationExpired) != 0 || graphRetentionStatus(errGraphHistoryExpired) != 0 {
		t.Fatal("unbound cause manufactured authenticated graph expiry")
	}
	for _, agreement := range []raftlog.ApplicationSemanticContractID{certifiedGraphSemanticContractID(), retainedGraphSemanticContractID()} {
		expired.peer.scope.semantic = agreement
		if _, err := encodeGraphWireResponse(expired); !errors.Is(err, errInvalid) {
			t.Fatal("old agreement encoded collected expiry", err)
		}
		scope, err := encodeAllocationScope(expired.peer.scope)
		if err != nil {
			t.Fatal(err)
		}
		invalid, err := boundedEncoding(graphSourceFrameBytes, func(w *boundedWriter) {
			w.add([]byte{'G', 'S', 'R', 2})
			w.field(scope)
			w.u64(expired.peer.voter)
			w.add(expired.issuer[:])
			w.tag(expired.kind)
			w.u64(expired.sequence)
			w.add(expired.nonce[:])
			w.u16(0)
			w.tag(3)
			w.field(nil)
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeGraphWireResponse(invalid); !errors.Is(err, errCorrupt) {
			t.Fatal("old agreement decoded collected expiry", err)
		}
	}
	// Pin-free ordinary replies retain the original GSR1 frame byte for byte.
	base.err = graphstore.ErrSourceUnavailable
	actual, err := encodeGraphWireResponse(base)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := encodeAllocationScope(base.peer.scope)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := boundedEncoding(graphSourceFrameBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'S', 'R', 1})
		w.field(scope)
		w.u64(base.peer.voter)
		w.add(base.issuer[:])
		w.tag(base.kind)
		w.u64(base.sequence)
		w.add(base.nonce[:])
		w.u16(1 << 10)
		w.field(nil)
	})
	if err != nil || !bytes.Equal(actual, legacy) {
		t.Fatal("ordinary GSR1 wire changed", err)
	}
}
