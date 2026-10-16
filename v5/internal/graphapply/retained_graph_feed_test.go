package graphapply

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// One authored native guard parent. These genuine six-voter operations are not
// an executed qualification, OS-child workload, or cross-engine acceptance.
type retainedFeedFaultWire struct {
	graphPeerWireTransport
	dropPage, dropAdvance, dropAdmission bool
	reads                                int
	unavailable                          bool
	deadline                             time.Time
	wrongDeadline                        bool
	calls, captures, issues              int
}

func (w *retainedFeedFaultWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	r, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	w.calls++
	if r.kind == sourceCertifiedOpen {
		w.captures++
	}
	if !w.deadline.IsZero() && (r.kind == sourceCertifiedOpen || r.kind == sourceCutRead || r.kind == sourceSubmit) {
		deadline, ok := ctx.Deadline()
		w.wrongDeadline = w.wrongDeadline || !ok || !deadline.Equal(w.deadline)
	}
	if w.unavailable {
		return nil, replica.ErrUnavailable
	}
	page, advance, admission := false, false, false
	if r.kind == sourceCrossRead && bytes.HasPrefix(r.command, []byte("GFQ\x01")) {
		w.reads++
	}
	if r.kind == sourceSubmit {
		p, err := decodeGraphSourceProposal(r.command)
		if err != nil {
			return nil, err
		}
		if isRetainedFeedWire(p.wire) {
			c, err := decodeRetainedFeedCommand(p.wire)
			if err != nil {
				return nil, err
			}
			page = c.phase == feedSealPage
			admission = c.phase == feedAdmitOperation
		}
		if isGraphPinWire(p.wire) {
			c, err := decodeGraphPinCommand(p.wire)
			if err != nil {
				return nil, err
			}
			advance = c.phase == pinAdvanceFeed
			if c.phase == pinIssue {
				w.issues++
			}
		}
	}
	response, err := w.graphPeerWireTransport.RoundTrip(ctx, wire)
	if err == nil && (page && w.dropPage || advance && w.dropAdvance || admission && w.dropAdmission) {
		if page {
			w.dropPage = false
		}
		if advance {
			w.dropAdvance = false
		}
		if admission {
			w.dropAdmission = false
		}
		return nil, io.ErrUnexpectedEOF
	}
	return response, err
}

func TestRetainedFeedNativeCompletePagesOriginalACKAndSnapshot(t *testing.T) {
	t.Run("pages and ACK", retainedFeedPagesAndACK)
	t.Run("coarse order and incoming bank", retainedFeedCoarseCommitOrderAndIncomingBank)
	t.Run("move lineage and collected originals", retainedFeedMoveAndCollectedOriginals)
	t.Run("snapshot operation lifetime and exhaustion", retainedFeedSnapshotAdmission)
	t.Run("unissued bootstrap recovery and fixed deadline", retainedFeedUnissuedBootstrap)
}

func retainedFeedUnissuedBootstrap(t *testing.T) {
	n, p, _, _ := retainedPublisherFromNetwork(t, collectedGraphNetworkWithAgreement(t, nil, retainedFeedSemanticContractID()))
	policy := retainedFeedGuardPolicy(8)
	var wires [2]*retainedFeedFaultWire
	for i, part := range []uint64{3, 8} {
		wires[i] = &retainedFeedFaultWire{graphPeerWireTransport: p.coordinator.transports[part], deadline: time.Unix(0, policy.deadlineUnixNanos)}
		p.coordinator.transports[part] = wires[i]
	}
	owner, err := beginRetainedFeed(t.Context(), p, n.requestID(), graphReadRequest{mode: graphReadFresh}, 100, policy)
	if err != nil {
		t.Fatal(err)
	}
	original, err := owner.Original()
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range owner.pin.definition.parts {
		actual, err := owner.pin.invocation.endpoints[part.certificate.scope.partition].retainedPinRead(t.Context(), owner.pin.manifest)
		if err != nil || actual.issued.index != 0 || actual.result.index != 0 || actual.owner.publication != 0 {
			t.Fatal("recipe unexpectedly issued/published", actual, err)
		}
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	// Recovery below receives only the saved original, not this closed handle.
	definition, err := decodeRetainedFeedDefinition(original)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := decodeGraphPinManifest(definition.manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest.parts[0].certificate.image[52] ^= 1
	changedManifest, err := encodeGraphPinManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	changed := definition
	changed.manifest = changedManifest
	changedWire, err := encodeRetainedFeedDefinition(changed)
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := resumeRetainedFeed(t.Context(), p, changedWire)
	if err != nil {
		t.Fatal(err)
	}
	before := [2]uint64{n.hosts[0][0].driver.Applied(), n.hosts[1][0].driver.Applied()}
	if _, err = wrong.Publish(t.Context()); !errors.Is(err, errInvalid) {
		t.Fatal("copied changed certificate became issuance authority", err)
	}
	if n.hosts[0][0].driver.Applied() != before[0] || n.hosts[1][0].driver.Applied() != before[1] || wires[0].issues != 0 {
		t.Fatal("invalid recipe installed native intent/effects")
	}
	recovered, err := resumeRetainedFeed(t.Context(), p, original)
	if err != nil || recovered.pin.born || !bytes.Equal(recovered.wire, original) {
		t.Fatal("recipe alone authenticated or changed an original", err)
	}
	wires[1].unavailable = true
	if _, err = recovered.Publish(t.Context()); !errors.Is(err, replica.ErrUnavailable) {
		t.Fatal("missing keeper became absence/success", err)
	}
	wires[1].unavailable = false
	if n.hosts[0][0].driver.Applied() != before[0] || n.hosts[1][0].driver.Applied() != before[1] || wires[0].issues != 0 {
		t.Fatal("incomplete At installed native intent")
	}
	captures := [2]int{wires[0].captures, wires[1].captures}
	token, err := recovered.Publish(t.Context())
	if err != nil || !bytes.Equal(token.definition, original) || len(token.pin.publications) != 2 {
		t.Fatal("all-Issue-absent original did not genuinely recover", err)
	}
	if wires[0].issues != 1 || wires[0].captures <= captures[0] || wires[1].captures <= captures[1] || wires[0].wrongDeadline || wires[1].wrongDeadline {
		t.Fatal("bootstrap omitted genuine At or escaped saved deadline")
	}
	read, err := recovered.OpenSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	assertCertifiedProjection(t, certifiedView(t, read), 1, true, nil)
	if err = read.Close(); err != nil {
		t.Fatal(err)
	}
	// These are still inert recipes. Expired or unsupported policy refuses before
	// a native exchange; recovery never replaces the original deadline or quota.
	for _, limited := range []bool{false, true} {
		refused := definition
		want := context.DeadlineExceeded
		if limited {
			refused.policy.operationRows = p.coordinator.host.machine.limits.sourceRows + 1
			want = errLimit
		} else {
			refused.policy.deadlineUnixNanos = time.Now().Add(-time.Hour).UnixNano()
		}
		wire, err := encodeRetainedFeedDefinition(refused)
		if err != nil {
			t.Fatal(err)
		}
		calls := wires[0].calls + wires[1].calls
		if _, err = resumeRetainedFeed(t.Context(), p, wire); !errors.Is(err, want) || wires[0].calls+wires[1].calls != calls {
			t.Fatal("bootstrap admission performed native work", limited, err)
		}
		if _, err = beginRetainedFeed(t.Context(), p, n.requestID(), graphReadRequest{mode: graphReadFresh}, 100, refused.policy); !errors.Is(err, want) || wires[0].calls+wires[1].calls != calls {
			t.Fatal("initial capture escaped bootstrap admission", limited, err)
		}
	}
	// Terminal cleanup keeps its independently funded lane, even when the
	// caller's bootstrap lifetime is exhausted. No Page arena is activated.
	recovered.definition.policy.deadlineUnixNanos = time.Now().Add(-time.Hour).UnixNano()
	if _, err = recovered.Publish(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("live handle renewed bootstrap deadline", err)
	}
	if err = recovered.Release(t.Context()); err != nil {
		t.Fatal("deadline stranded funded terminal cleanup", err)
	}
}

func retainedFeedGuardPolicy(pages uint64) retainedFeedPolicy {
	l := defaultMaterializerLimits()
	return retainedFeedPolicy{pageBytes: 64 << 10, pageRows: 256, maxPages: pages, maxAttempts: 8 * pages, operationRows: l.sourceRows, operationBytes: l.sourceBytes, operationOutput: l.outputBytes, deadlineUnixNanos: time.Now().Add(10 * time.Minute).UnixNano()}
}
func retainedFeedPagesAndACK(t *testing.T) {
	n, p, edgeOps, _ := retainedPublisherFromNetwork(t, collectedGraphNetworkWithAgreement(t, nil, retainedFeedSemanticContractID()))
	h := n.hosts[0][0]
	policy := retainedFeedGuardPolicy(128)
	owner, err := beginRetainedFeed(t.Context(), p, n.requestID(), graphReadRequest{mode: graphReadFresh}, 100, policy)
	if err != nil {
		t.Fatal(err)
	}
	original, err := owner.Original()
	if err != nil {
		t.Fatal(err)
	}
	token, err := owner.Publish(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeRetainedFeedToken(token)
	if err != nil {
		t.Fatal(err)
	}
	token, err = decodeRetainedFeedToken(encoded)
	if err != nil {
		t.Fatal(err)
	}
	old, err := owner.OpenSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	assertCertifiedProjection(t, certifiedView(t, old), 1, true, nil)
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	// Actual cross COMMIT and a later mutation discriminate the old snapshot
	// from latest state. Complete envelopes must include both actual Resolves.
	tx, register := n.crossPlan(edgeOps, []freshBinding{{role: entityBinding, id: 3, grant: grantReference{session: h.recipients[[16]byte{1}].session, sequence: 1}}, {role: lifeBinding, owner: 3, id: 4, grant: grantReference{session: h.recipients[[16]byte{1}].session, sequence: 1}}}, 70)
	decision := n.crossCommit(tx, register)
	n.crossResolve(decision)
	local, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "after-feed-snapshot", Scope: edgeOps[0].Scope}}, 71, nil)
	if err != nil {
		t.Fatal(err)
	}
	localOriginal := n.submitPartitionGraph(0, local, reasonNone)
	current, err := p.coordinator.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	boundary, err := current.Cut()
	if err != nil {
		t.Fatal(err)
	}
	assertCertifiedProjection(t, certifiedView(t, current), 1, true, []string{"after-feed-snapshot"})
	if err = current.Close(); err != nil {
		t.Fatal(err)
	}
	old, err = owner.OpenSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	assertCertifiedProjection(t, certifiedView(t, old), 1, true, nil)
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	// Lost genuine page and feed-advance replies exercise immutable native
	// originals. No test DTO supplies a page result, cursor or ACK decision.
	fault := &retainedFeedFaultWire{graphPeerWireTransport: p.coordinator.transports[8], dropPage: true, dropAdmission: true}
	p.coordinator.transports[8] = fault
	homeFault := &retainedFeedFaultWire{graphPeerWireTransport: p.coordinator.transports[3]}
	p.coordinator.transports[3] = homeFault
	owner, err = resumeRetainedFeed(t.Context(), p, original)
	if err != nil {
		t.Fatal(err)
	}
	owner.token = token.pin
	id := [16]byte(n.requestID())
	setupInvocation := owner.pin.invocation
	var beforeAdmission [2]uint64
	for g := range 2 {
		q, e := graphRetentionReader(n.hosts[g][0], t.Context())
		if e != nil {
			t.Fatal(e)
		}
		cursor, found, e := readFeedCursor(q, owner.pin.definition.id())
		if e != nil || !found {
			t.Fatal(e)
		}
		beforeAdmission[g] = cursor.admitted
		if e = q.view.Close(); e != nil {
			t.Fatal(e)
		}
	}
	_, err = owner.Page(t.Context(), id, boundary)
	if !errors.Is(err, errRetainedFeedPending) || !errors.Is(err, io.ErrUnexpectedEOF) || homeFault.reads != 0 || fault.reads != 0 || owner.pin.invocation != setupInvocation {
		t.Fatal("unknown admission activated source work", err, homeFault.reads, fault.reads)
	}
	for g := range 2 {
		q, e := graphRetentionReader(n.hosts[g][0], t.Context())
		if e != nil {
			t.Fatal(e)
		}
		cursor, found, e := readFeedCursor(q, owner.pin.definition.id())
		if e != nil || !found || cursor.admitted != beforeAdmission[g]+1 || cursor.pending != ([32]byte{}) || cursor.spentOutput != cursor.admitted*uint64(policy.operationOutput) {
			t.Fatal("unknown installed admission refunded/created page", g, cursor, found, e)
		}
		if e = q.view.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	owner, err = resumeRetainedFeed(t.Context(), p, original)
	if err != nil {
		t.Fatal(err)
	}
	owner.token = token.pin
	_, err = owner.Page(t.Context(), id, boundary)
	if !errors.Is(err, errRetainedFeedPending) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("page reply was not genuinely lost", err)
	}
	recovered, err := openRetainedFeed(t.Context(), p, token)
	if err != nil {
		t.Fatal(err)
	}
	pending, found, err := recovered.Pending(t.Context())
	if err != nil || !found || pending.request != id || pending.acknowledging {
		t.Fatal("native pending-page recovery", pending, found, err)
	}
	delivery, err := recovered.Page(t.Context(), id, boundary)
	if err != nil || !delivery.replay || delivery.digest != pending.digest {
		t.Fatal("same native page replay", err)
	}
	// Canonical framing cannot turn a frame-free page into business evidence.
	// Build the adversarial bytes from a valid protocol page so Host and Stage
	// independently refuse the malformed identity, before owner/control effects.
	protocol := delivery.page
	protocol.request = [16]byte(n.requestID())
	protocol.kind, protocol.tx, protocol.round = 0, [16]byte{}, 0
	protocol.frames, protocol.payloads = nil, nil
	protocol.next = append([]uint64(nil), protocol.from...)
	protocolWire, err := encodeRetainedFeedPage(protocol, policy.pageBytes)
	if err != nil {
		t.Fatal(err)
	}
	protocolBody, err := graphBody(protocolWire, "GFP\x01", policy.pageBytes)
	if err != nil {
		t.Fatal(err)
	}
	position := graphCursor{b: protocolBody, maxOwnedBytes: 8*len(protocolWire) + 4096}
	position.take(16 + 8 + 32 + 32)
	readFeedVector(&position)
	readFeedVector(&position)
	readFeedCut(&position)
	readFeedCut(&position)
	position.take(len(protocol.from) * (8 + 172 + 1))
	if position.err != nil {
		t.Fatal(position.err)
	}
	identityOffset := len(protocolBody) - len(position.b)
	actual, err := graphRetentionReader(h, t.Context())
	if err != nil {
		t.Fatal(err)
	}
	businessFrame, err := h.machine.feedNativeFrame(actual, localOriginal.index, policy.pageBytes, recovered.pin.definition.parts[0].feedStart)
	if err != nil {
		t.Fatal(err)
	}
	businessHead, err := h.machine.feedEnvelope(businessFrame, actual.arena)
	if err != nil || businessHead.kind != feedLocalEnvelope || businessFrame.progressNext != 0 {
		t.Fatal("missing authentic first-install business frame", businessHead, err)
	}
	if err = actual.view.Close(); err != nil {
		t.Fatal(err)
	}
	for _, identity := range []struct {
		kind  byte
		tx    [16]byte
		round uint64
	}{{feedLocalEnvelope, [16]byte{1}, 1}, {feedCrossEnvelope, [16]byte{1}, 1}, {feedMoveEnvelope, [16]byte{1}, 1}, {feedLocalEnvelope, [16]byte{}, businessHead.round}, {feedLocalEnvelope, businessHead.tx, 0}, {0, [16]byte{1}, 0}, {0, [16]byte{}, 1}} {
		bad := protocol
		bad.kind, bad.tx, bad.round = identity.kind, identity.tx, identity.round
		if identity.kind != 0 && (identity.tx == ([16]byte{}) || identity.round == 0) {
			// A real business first-install frame has no protocol census; neither
			// empty-frame nor progressNext checks can mask the one-zero defect.
			bad.frames = []retainedFeedFrame{businessFrame}
		}
		if _, err := encodeRetainedFeedPage(bad, policy.pageBytes); !errors.Is(err, errInvalid) {
			t.Fatal("invalid business identity encoded", identity, err)
		}
		arena, err := graphstate.NewOutputBudget(h.machine.limits.outputBytes)
		if err != nil {
			t.Fatal(err)
		}
		if err = h.machine.checkFeedPageShape(bad, arena); !errors.Is(err, errInvalid) {
			t.Fatal("frame-free or partial identity passed shape", identity, err)
		}
		framed := bad
		framed.kind, framed.tx, framed.round = 0, [16]byte{}, 0
		framedWire, err := encodeRetainedFeedPage(framed, policy.pageBytes)
		if err != nil {
			t.Fatal(err)
		}
		framedBody, err := graphBody(framedWire, "GFP\x01", policy.pageBytes)
		if err != nil {
			t.Fatal(err)
		}
		body := bytes.Clone(framedBody)
		body[identityOffset] = identity.kind
		copy(body[identityOffset+1:identityOffset+17], identity.tx[:])
		binary.BigEndian.PutUint64(body[identityOffset+17:identityOffset+25], identity.round)
		// Keep the real page envelope valid so only its deliberately changed
		// business identity can explain the ErrInvalid below.
		pageWire := seal(append([]byte("GFP\x01"), body...))
		checkedBody, err := graphBody(pageWire, "GFP\x01", policy.pageBytes)
		if err != nil || !bytes.Equal(checkedBody, body) {
			t.Fatal("adversarial identity lost page framing/checksum", identity, err)
		}
		command := recovered.command(recovered.pin.invocation.endpoints[3], feedSealPage, protocol.request, pageWire)
		wire, err := encodeRetainedFeedCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		before, err := h.machine.store.ApplicationUsage()
		if err != nil {
			t.Fatal(err)
		}
		beforeApplied := h.driver.Applied()
		h.mu.Lock()
		err = h.preflightRetainedFeed(command, wire)
		h.mu.Unlock()
		if !errors.Is(err, errInvalid) {
			t.Fatal("preflight admitted empty business", identity, err)
		}
		if _, err = h.Submit(allocationProposal{target: h.machine.scope(), wire: wire}); !errors.Is(err, errInvalid) {
			t.Fatal("Host admitted empty business", identity, err)
		}
		limits := h.machine.store.ApplicationLimits()
		batch, err := h.machine.Stage(replica.Entry{Index: beforeApplied + 1, Generation: h.machine.store.ApplicationGeneration(), Data: wire}, raftlog.ApplicationBudget{Writes: limits.MaxInstallWrites, Bytes: limits.MaxInstallBytes, ImageBytes: limits.MaxImageBytes, ChangeBytes: limits.MaxChangeBytes, OutcomeBytes: limits.MaxOutcomeBytes})
		if !errors.Is(err, errInvalid) || len(batch.Writes)+len(batch.ControlPuts)+len(batch.Outcome)+len(batch.Changes) != 0 {
			t.Fatal("Stage admitted malformed business identity/effects", identity, err)
		}
		after, err := h.machine.store.ApplicationUsage()
		if err != nil || after != before || h.driver.Applied() != beforeApplied {
			t.Fatal("refused page changed native ownership", before, after, err)
		}
	}
	changedPage := delivery.page
	changedPage.eof = !changedPage.eof
	changedWire, err := encodeRetainedFeedPage(changedPage, policy.pageBytes)
	if err != nil {
		t.Fatal(err)
	}
	changedCommand := recovered.command(recovered.pin.invocation.endpoints[3], feedSealPage, id, changedWire)
	changedCommandWire, err := encodeRetainedFeedCommand(changedCommand)
	if err != nil {
		t.Fatal(err)
	}
	beforeChanged := h.driver.Applied()
	if _, err = h.Submit(allocationProposal{target: h.machine.scope(), wire: changedCommandWire}); !errors.Is(err, errInvalid) || h.driver.Applied() != beforeChanged {
		t.Fatal("changed page original reached effects", err)
	}
	fault.dropAdvance = true
	if _, err = recovered.Ack(t.Context(), id, delivery.digest); !errors.Is(err, errRetainedFeedPending) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("advance reply was not genuinely lost", err)
	}
	recovered, err = openRetainedFeed(t.Context(), p, token)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := recovered.Ack(t.Context(), id, delivery.digest)
	if err != nil {
		t.Fatal(err)
	}
	if !delivery.page.eof && feedCutEqual(completed, boundary) {
		t.Fatal("partial page advanced completed cut")
	}
	firstDelivery, firstRequest, firstCompleted, firstBoundary := delivery, id, completed, boundary
	seenCross, seenLocal := 0, 0
	done := delivery.page.eof
	if delivery.page.kind == feedCrossEnvelope {
		seenCross++
	}
	if delivery.page.kind == feedLocalEnvelope {
		seenLocal++
	}
	for pages := 0; !done && pages < 64; pages++ {
		// SAME owner receives another durably prepaid operation, without resetting
		// its original pin/cleanup invocation or native cumulative attempt totals.
		id = [16]byte(n.requestID())
		delivery, err = recovered.Page(t.Context(), id, boundary)
		if err != nil {
			t.Fatal(err)
		}
		if delivery.page.kind == feedCrossEnvelope {
			seenCross++
			if len(delivery.page.frames) != 2 || delivery.page.tx != decision.status.tx || delivery.page.round != decision.status.round {
				t.Fatal("half or substituted cross envelope")
			}
		}
		if delivery.page.kind == feedLocalEnvelope {
			seenLocal++
			if len(delivery.resolved.entities) == 0 {
				t.Fatal("unresolved original catalog footprint")
			}
		}
		completed, err = recovered.Ack(t.Context(), id, delivery.digest)
		if err != nil {
			t.Fatal(err)
		}
		done = delivery.page.eof
		if !done && feedCutEqual(completed, boundary) {
			t.Fatal("completed cut before complete vector")
		}
	}
	if !done || seenCross != 1 || seenLocal != 1 || !feedCutEqual(completed, boundary) {
		t.Fatal("missing, duplicate or half envelope", done, seenCross, seenLocal)
	}
	// A fixed boundary alone misses metadata self-amplification. Each NEW real
	// certified boundary includes the previous seal/admission/feed/ACK entries.
	// A quiescent stream must drain them in a bounded census, not multiply its
	// own metadata tail by making one new page for each old protocol record.
	for cycle := 0; cycle < 3; cycle++ {
		read, err := p.coordinator.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
		if err != nil {
			t.Fatal(err)
		}
		boundary, err = read.Cut()
		if err != nil {
			t.Fatal(err)
		}
		if err = read.Close(); err != nil {
			t.Fatal(err)
		}
		drained := false
		for count := 0; count < 2; count++ {
			beforeNative := [2]uint64{n.hosts[0][0].driver.Applied(), n.hosts[1][0].driver.Applied()}
			id = [16]byte(n.requestID())
			delivery, err = recovered.Page(t.Context(), id, boundary)
			if err != nil {
				t.Fatal(err)
			}
			if delivery.page.kind != 0 || delivery.resolved.nonempty() {
				t.Fatal("quiescent metadata invented business effects")
			}
			for _, f := range delivery.page.frames {
				if f.progressNext <= f.index || f.progressNext-f.index > retainedFeedProgressBatch || f.census == ([32]byte{}) {
					t.Fatal("unbounded or unauthenticated progress census")
				}
			}
			completed, err = recovered.Ack(t.Context(), id, delivery.digest)
			if err != nil {
				t.Fatal(err)
			}
			if delivery.page.eof {
				if !feedCutEqual(completed, boundary) {
					t.Fatal("whole vector not completed")
				}
				for g := range 2 {
					afterNative := n.hosts[g][0].driver.Applied()
					if delivery.page.tips[g].index <= beforeNative[g] || afterNative-beforeNative[g] != 5 || delivery.page.future[g] || delivery.page.next[g] != delivery.page.tips[g].index+1 || afterNative-delivery.page.tips[g].index != 4 {
						t.Fatal("quiescent physical backlog not bounded by genuine own controls", g, beforeNative[g], delivery.page.tips[g].index, afterNative, delivery.page.next[g])
					}
				}
				drained = true
				break
			}
		}
		if !drained {
			t.Fatal("new quiescent boundary amplifies its own metadata", cycle)
		}
	}
	// Native attempts/work are monotone across SAME-owner operations and a real
	// reconnect. Token/definition bytes alone never reset the installed totals.
	states, err := recovered.states(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	beforeCursor, err := decodeRetainedFeedCursor(states[0].cursor)
	if err != nil || beforeCursor.admitted < 6 || beforeCursor.spentOutput != beforeCursor.admitted*uint64(policy.operationOutput) {
		t.Fatal("missing native cumulative work", beforeCursor, err)
	}
	reconnected, err := openRetainedFeed(t.Context(), p, token)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = reconnected.Pending(t.Context()); err != nil {
		t.Fatal(err)
	}
	states, err = reconnected.states(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	afterCursor, err := decodeRetainedFeedCursor(states[0].cursor)
	if err != nil || afterCursor.admitted != beforeCursor.admitted+1 || afterCursor.spentOutput <= beforeCursor.spentOutput {
		t.Fatal("reopen refunded work", beforeCursor, afterCursor, err)
	}
	// A real later write may lie inside the physical tip of an older immutable
	// certificate. Stop BEFORE it; its native root cannot widen the business cut.
	futureProposal, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "beyond-boundary", Scope: edgeOps[0].Scope}}, 72, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, futureProposal, reasonNone)
	futureIndex := h.driver.Applied()
	stopped := false
	for step := 0; step < 4; step++ {
		id = [16]byte(n.requestID())
		d, e := recovered.Page(t.Context(), id, boundary)
		if e != nil {
			t.Fatal(e)
		}
		if d.page.kind != 0 {
			t.Fatal("future write escaped old business cut")
		}
		completed, e = recovered.Ack(t.Context(), id, d.digest)
		if e != nil {
			t.Fatal(e)
		}
		if d.page.eof {
			if !d.page.future[0] || d.page.next[0] != futureIndex || !feedCutEqual(completed, boundary) {
				t.Fatal("future head consumed or invented stop", d.page.next, d.page.future)
			}
			stopped = true
			break
		}
	}
	if !stopped {
		t.Fatal("old cut did not finish before future business")
	}
	fresh, e := p.coordinator.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if e != nil {
		t.Fatal(e)
	}
	later, e := fresh.Cut()
	if e != nil {
		t.Fatal(e)
	}
	if e = fresh.Close(); e != nil {
		t.Fatal(e)
	}
	futureSeen := 0
	finished := false
	for step := 0; step < 8; step++ {
		id = [16]byte(n.requestID())
		d, e := recovered.Page(t.Context(), id, later)
		if e != nil {
			t.Fatal(e)
		}
		if d.page.kind != 0 {
			futureSeen++
			if d.page.kind != feedLocalEnvelope || len(d.page.frames) != 1 || d.page.frames[0].index != futureIndex {
				t.Fatal("substituted future original")
			}
		}
		completed, e = recovered.Ack(t.Context(), id, d.digest)
		if e != nil {
			t.Fatal(e)
		}
		if d.page.eof {
			finished = true
			break
		}
	}
	if !finished || futureSeen != 1 || !feedCutEqual(completed, later) {
		t.Fatal("future business missing/duplicate after next cut", futureSeen, finished)
	}
	beforeReplay := [2]uint64{n.hosts[0][0].driver.Applied(), n.hosts[1][0].driver.Applied()}
	oldPage, err := recovered.Page(t.Context(), firstRequest, firstBoundary)
	if err != nil || !oldPage.replay || !bytes.Equal(oldPage.wire, firstDelivery.wire) || oldPage.digest != firstDelivery.digest {
		t.Fatal("acknowledged older page did not recover immutable result", err)
	}
	oldCompleted, err := recovered.Ack(t.Context(), firstRequest, firstDelivery.digest)
	if err != nil || !feedCutEqual(oldCompleted, firstCompleted) {
		t.Fatal("older ACK read current cursor result", err)
	}
	for g := range 2 {
		if n.hosts[g][0].driver.Applied()-beforeReplay[g] != 2 {
			t.Fatal("old originals resealed/advanced cursor instead of admission-only replay")
		}
	}
	// Authenticated publication IDs are owned after Open, not caller aliases.
	opened, err := openRetainedFeed(t.Context(), p, token)
	if err != nil {
		t.Fatal(err)
	}
	pub := opened.token.publications[0]
	token.pin.publications[0]++
	token.pin.manifest[0] ^= 1
	if opened.token.publications[0] != pub || !bytes.Equal(opened.token.manifest, opened.definition.manifest) {
		t.Fatal("caller token mutated retained owner")
	}
	// The actual RS2-backed installation census includes the new locator. This
	// checks eight terminal records against real native accounting, not a sum of
	// constants that mirrors the reservation recipe.
	before, err := h.machine.store.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	if err = opened.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	after, err := h.machine.store.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	if after.RetainedRecords-before.RetainedRecords != 8 || after.RecoveryRecords-before.RecoveryRecords != 1 || after.ControlRecords-before.ControlRecords != 2 {
		t.Fatal("terminal allowance does not fund real RS2/locator batch", before, after)
	}
	if _, err = p.Open(t.Context(), opened.token); !errors.Is(err, errGraphPinReleased) {
		t.Fatal("native released token remained live", err)
	}
	var nilOwner *retainedFeedOwner
	if _, err = nilOwner.Original(); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err = nilOwner.Page(t.Context(), id, boundary); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err = nilOwner.Ack(t.Context(), id, delivery.digest); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err = nilOwner.Close(); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err = nilOwner.Release(t.Context()); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err = nilOwner.Expire(t.Context()); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err = nilOwner.OpenSnapshot(t.Context()); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, _, err = nilOwner.Pending(t.Context()); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	var nilSource *retainedFeedRecordingSource
	if _, _, err = nilSource.Get(t.Context(), []byte("x"), 1); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err = nilSource.Scan(t.Context(), []byte("x"), nil, nil, raftlog.ReadBudget{Rows: 1, Bytes: 1}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
}

func retainedFeedCoarseCommitOrderAndIncomingBank(t *testing.T) {
	n, p, ops, _ := retainedPublisherFromNetwork(t, collectedGraphNetworkWithAgreement(t, nil, retainedFeedSemanticContractID()))
	h := n.hosts[0][0]
	// A owns genuine all-participant YES locks and a durable COMMIT. Resolve P
	// first while Q still holds A; the second independent original cannot install
	// an opposite first-commit order on Q. Real refusal/ABORT is never relabelled.
	a, register := n.crossPlan([]graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "A", Scope: ops[0].Scope}}, nil, 80)
	decision := n.crossCommit(a, register)
	crossResolveAt(n, 0, decision, 0)
	_, proposal := crossPlanAt(n, 1, n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 17, Life: 18, Name: "B-before-A-resolve", Scope: ops[0].Scope}}, nil, 81)
	beforeApplied := n.hosts[1][0].driver.Applied()
	_, err := n.hosts[1][0].Submit(proposal)
	if !errors.Is(err, raftlog.ErrLimit) && !errors.Is(err, errLimit) || n.hosts[1][0].driver.Applied() != beforeApplied {
		t.Fatal("held native recovery owner admitted a conflicting registration", err)
	}
	// The complete-head validator rejects a contradictory permutation rather
	// than skipping a head, advancing a vector, or accumulating a TxID map.
	first := retainedFeedEnvelope{kind: feedCrossEnvelope, tx: [16]byte{1}, round: 10, participants: []uint64{3, 8}}
	second := retainedFeedEnvelope{kind: feedCrossEnvelope, tx: [16]byte{2}, round: 11, participants: []uint64{3, 8}}
	if _, _, err = completeFeedEnvelope([]retainedFeedEnvelope{first, second}, []uint64{3, 8}); !errors.Is(err, errInvalid) {
		t.Fatal("opposite heads accepted", err)
	}
	crossResolveAt(n, 0, decision, 1)
	b, register := n.crossPlan([]graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "B-after-A-resolve", Scope: ops[0].Scope}}, nil, 82)
	secondDecision := n.crossCommit(b, register)
	n.crossResolve(secondDecision)
	if secondDecision.status.round <= decision.status.round {
		t.Fatal("actual second COMMIT did not advance installed round")
	}
	// The remaining guard below uses an actual AS7 incoming bank with a distinct
	// active bootstrap. GFL recovery classification must not call active feedAt.
	owner, err := beginRetainedFeed(t.Context(), p, n.requestID(), graphReadRequest{mode: graphReadFresh}, 100, retainedFeedGuardPolicy(16))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = owner.Publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, recipient, machine, manifest := retainedFeedIncoming(t, n, h)
	view, err := prepared.OpenReadView(t.Context(), manifest.Index, 172)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	base, err := view.RootBounded(t.Context(), 172)
	if err != nil || base.Generation == recipient.ApplicationGeneration() {
		t.Fatal("not a real incoming bank", base, err)
	}
	command := owner.command(owner.pin.invocation.endpoints[3], feedInitialize, retainedFeedPhaseAction(owner.pin.definition.request, feedInitialize), nil)
	wire, err := encodeRetainedFeedCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	before, err := recipient.ApplicationTransferUsage()
	if err != nil {
		t.Fatal(err)
	}
	purpose, err := machine.VerifyApplicationRecovery(t.Context(), wire, view)
	if err != nil || purpose.Reserve != nil || purpose.Use != nil || purpose.TerminalReserve != nil || purpose.TerminalUse != nil || purpose.ApplyBefore != nil {
		t.Fatal("incoming classification reopened active bank or borrowed credit", purpose, err)
	}
	q := feedReader(t.Context(), machine, view, base, nil)
	q.preparedHistory = prepared
	if err = machine.feedAt(&q, base.Index, func(*reader) error { t.Fatal("entered active bank"); return nil }); !errors.Is(err, errInvalid) {
		t.Fatal("prepared history admitted active reopen", err)
	}
	after, err := recipient.ApplicationTransferUsage()
	if err != nil || before != after {
		t.Fatal("classification changed incoming ownership", before, after, err)
	}
}

func retainedFeedIncoming(t *testing.T, n *allocationTestNetwork, donor *allocationHost) (*raftlog.PreparedApplicationSnapshot, *raftlog.Store, *declaredMaterializer, raftlog.ApplicationSnapshotManifest) {
	t.Helper()
	store := donor.machine.store
	if err := store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := store.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := store.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer export.Close()
	ready := false
	for range 256 {
		ready, err = export.BuildManifest(t.Context(), raftlog.ReadBudget{Rows: 64, Bytes: 256 << 10})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
	}
	if !ready {
		t.Fatal("bounded native manifest did not finish")
	}
	manifest, err := export.Manifest()
	if err != nil || manifest.Contract.Version != 5 || manifest.SemanticContractID != retainedFeedSemanticContractID() {
		t.Fatal(manifest, err)
	}
	cfg := n.configs[0][2]
	cfg.Dir = t.TempDir()
	cfg.Create = true
	recipient, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recipient.Close(); err != nil {
			t.Error(err)
		}
	})
	if err = graphstore.BootstrapBoundOwnership(recipient, recipient.ApplicationBinding(), n.d, [3]uint64{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	machine, err := newRetainedFeedMaterializer(recipient, donor.machine.ns, n.d, donor.machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	incoming, err := recipient.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := incoming.Abort(); err != nil {
			t.Error(err)
		}
	})
	complete := false
	for range 256 {
		chunk, err := export.Next(t.Context(), raftlog.ReadBudget{Rows: 64, Bytes: 256 << 10})
		if err != nil {
			t.Fatal(err)
		}
		if err = incoming.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		if chunk.Final {
			complete = true
			break
		}
	}
	if !complete {
		t.Fatal("bounded native import did not finish")
	}
	if err = incoming.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := incoming.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			t.Error(err)
		}
	})
	return prepared, recipient, machine, manifest
}

func retainedFeedMoveAndCollectedOriginals(t *testing.T) {
	n, p, ops, _ := retainedPublisherFromNetwork(t, collectedGraphNetworkWithAgreement(t, func(p *raftlog.ApplicationPolicy) {
		p.RetainedApplicationBytes = 64 << 20
		p.RetainedApplicationRecords = 32768
	}, retainedFeedSemanticContractID()))
	h := n.hosts[0][0]
	pre, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "before-C", Scope: ops[0].Scope}}, 50, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, pre, reasonNone)
	preIndex := h.driver.Applied()
	owner, err := beginRetainedFeed(t.Context(), p, n.requestID(), graphReadRequest{mode: graphReadFresh}, 100, retainedFeedGuardPolicy(128))
	if err != nil {
		t.Fatal(err)
	}
	token, err := owner.Publish(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	initial := owner.pin.definition.parts[0]
	post, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "after-C", Scope: ops[0].Scope}}, 51, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, post, reasonNone)
	postIndex := h.driver.Applied()
	event, err := h.Submit(post)
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, event)
	postReplay := h.driver.Applied()
	move, err := newRangeMoveCoordinatorOn(p.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := h.PrepareRangeMove(t.Context(), n.requestID(), 1, 8, 32, 64, 8<<10)
	if err != nil {
		t.Fatal(err)
	}
	done, err := move.Run(t.Context(), intent)
	if err != nil || !done.committed {
		t.Fatal("genuine move", done, err)
	}
	read, err := p.coordinator.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	boundary, err := read.Cut()
	if err != nil {
		t.Fatal(err)
	}
	if err = read.Close(); err != nil {
		t.Fatal(err)
	}
	seenLocal, seenMove := 0, 0
	complete := false
	for step := 0; step < 64; step++ {
		id := [16]byte(n.requestID())
		d, err := owner.Page(t.Context(), id, boundary)
		if err != nil {
			t.Fatal(err)
		}
		if d.page.kind == feedLocalEnvelope {
			seenLocal++
			if len(d.resolved.entities) == 0 {
				t.Fatal("unresolved actual mutation")
			}
		}
		if d.page.kind == feedMoveEnvelope {
			seenMove++
			if len(d.page.frames) != 2 || !bytes.HasPrefix(d.page.frames[0].changes, []byte("GML\x01")) || !bytes.Equal(d.page.frames[0].changes, d.page.frames[1].changes) || !bytes.Equal(d.page.frames[0].intent, d.page.frames[1].intent) || !bytes.Equal(d.page.frames[0].decision, d.page.frames[1].decision) {
				t.Fatal("half or substituted move lineage")
			}
		}
		completed, err := owner.Ack(t.Context(), id, d.digest)
		if err != nil {
			t.Fatal(err)
		}
		if d.page.eof {
			if !feedCutEqual(completed, boundary) {
				t.Fatal("half completed move cut")
			}
			complete = true
			break
		}
	}
	if !complete || seenLocal != 1 || seenMove != 1 {
		t.Fatal("missing/duplicate actual envelopes", complete, seenLocal, seenMove)
	}
	// A post-C replay still requires its authentic earlier raw sidecars. It is
	// never relabelled pre-C merely because its current CDC is empty.
	q, err := graphRetentionReader(h, t.Context())
	if err != nil {
		t.Fatal(err)
	}
	frame, err := h.machine.feedNativeFrame(q, postReplay, 64<<10, initial.feedStart)
	if err != nil || frame.originalBeforeCut || len(frame.originalChanges) == 0 {
		t.Fatal("post-C original lost its real sidecars", frame, err)
	}
	replayed, decodeErr := decodePartitionMappedOutcome(frame.outcome, h.machine.ns)
	prior, priorErr := decodePartitionMappedOutcome(frame.originalOutcome, h.machine.ns)
	if decodeErr != nil || priorErr != nil || replayed.index != postIndex || prior.index != postIndex || replayed.identity != prior.identity || replayed.hash != prior.hash || replayed.disposition != requestReplay || prior.disposition != applied {
		t.Fatal("post-C original identity", decodeErr, priorErr)
	}
	if err = q.view.Close(); err != nil {
		t.Fatal(err)
	}
	// Genuine ACKed feed floor can advance raw Outcome/CDC independently of the
	// still-pinned initial snapshot root. The immutable pre-C original survives.
	collect := func(outcome uint64) {
		proposal, err := h.PrepareGraphRetention(t.Context(), n.requestID(), graphRetentionFloors{initial.certificate.sourceIndex, initial.certificate.sourceIndex, outcome, outcome, initial.certificate.sourceIndex, initial.certificate.index})
		if err != nil {
			t.Fatal(err)
		}
		event, err := h.Submit(proposal)
		if err != nil {
			t.Fatal(err)
		}
		n.pump(0, event)
		r, found, err := h.GraphRetentionOriginal(t.Context(), proposal)
		if err != nil || !found || r.status != graphRetentionApplied {
			t.Fatal("actual feed-safe GC", r, found, err)
		}
		for _, voter := range n.hosts[0] {
			progress := raftlog.ApplicationRetentionProgress{}
			for step := 0; !progress.Complete && step < 256; step++ {
				progress, _, err = voter.machine.store.CollectApplicationRetention(t.Context(), r.id, raftlog.ReadBudget{Rows: 16, Bytes: 1 << 20})
				if err != nil {
					t.Fatal(err)
				}
			}
			if !progress.Complete {
				t.Fatal("bounded actual collector unfinished")
			}
		}
	}
	collect(initial.feedStart)
	if _, err = h.machine.store.ApplicationRecord(t.Context(), preIndex, true, 512); !errors.Is(err, raftlog.ErrApplicationExpired) {
		t.Fatal("pre-C raw outcome was not actually collected", err)
	}
	owner, err = openRetainedFeed(t.Context(), p, token)
	if err != nil {
		t.Fatal(err)
	}
	old, err := owner.OpenSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	assertCertifiedProjection(t, certifiedView(t, old), 1, true, []string{"before-C"})
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	event, err = h.Submit(pre)
	if err != nil {
		t.Fatal("retained pre-C original replay", err)
	}
	n.pump(0, event)
	q, err = graphRetentionReader(h, t.Context())
	if err != nil {
		t.Fatal(err)
	}
	frame, err = h.machine.feedNativeFrame(q, h.driver.Applied(), 64<<10, initial.feedStart)
	if err != nil || !frame.originalBeforeCut || len(frame.originalOutcome) == 0 || len(frame.originalChanges) != 0 || len(frame.changes) != 0 {
		t.Fatal("pre-C retry invented/missed original authority", frame, err)
	}
	if err = q.view.Close(); err != nil {
		t.Fatal(err)
	}
	// Once the actual ACK floor permits collection of the post-C original, it
	// MUST refuse a direct historical duplicate read instead of blessing it as a
	// pre-C retry. The original GPM cutoff is unchanged throughout.
	states, err := owner.states(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := decodeRetainedFeedCursor(states[0].cursor)
	if err != nil {
		t.Fatal(err)
	}
	if cursor.positions[0] <= postIndex {
		t.Fatal("test did not acknowledge post-C original")
	}
	collect(cursor.positions[0])
	for _, outcome := range []bool{false, true} {
		if _, err = h.machine.store.ApplicationRecord(t.Context(), postIndex, outcome, 64<<10); !errors.Is(err, raftlog.ErrApplicationExpired) {
			t.Fatal("acknowledged post-C raw sidecar was not collected", outcome, err)
		}
	}
	q, err = graphRetentionReader(h, t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.machine.feedNativeFrame(q, postReplay, 64<<10, initial.feedStart)
	if !errors.Is(err, raftlog.ErrApplicationExpired) {
		t.Fatal("missing post-C raw sidecar became old duplicate", err)
	}
	if err = q.view.Close(); err != nil {
		t.Fatal(err)
	}
	// A NEW same-original retry is different from that expired historical
	// frame. Authenticate the original against the actual acknowledged prefix;
	// its immutable retained result emits no second business delta after GC.
	event, err = h.Submit(post)
	if err != nil {
		t.Fatal("post-C original retry after acknowledged collection", err)
	}
	n.pump(0, event)
	q, err = graphRetentionReader(h, t.Context())
	if err != nil {
		t.Fatal(err)
	}
	actualCursor, found, err := readFeedCursor(q, owner.pin.definition.id())
	if err != nil || !found || actualCursor.positions[0] != cursor.positions[0] {
		t.Fatal("missing authentic acknowledged cursor", actualCursor, found, err)
	}
	frame, err = h.machine.feedNativeFrame(q, h.driver.Applied(), 64<<10, actualCursor.positions[0])
	if err != nil || !frame.originalBeforeCut || len(frame.originalOutcome) == 0 || len(frame.originalChanges) != 0 || len(frame.changes) != 0 {
		t.Fatal("new acknowledged retry lost immutable authority", frame, err)
	}
	replayed, decodeErr = decodePartitionMappedOutcome(frame.outcome, h.machine.ns)
	prior, priorErr = decodePartitionMappedOutcome(frame.originalOutcome, h.machine.ns)
	if decodeErr != nil || priorErr != nil || replayed.index != postIndex || prior.index != postIndex || replayed.identity != prior.identity || replayed.hash != prior.hash || replayed.disposition != requestReplay || prior.disposition != applied {
		t.Fatal("collected post-C original identity changed", decodeErr, priorErr)
	}
	if err = q.view.Close(); err != nil {
		t.Fatal(err)
	}
	read, err = p.coordinator.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	boundary, err = read.Cut()
	if err != nil {
		t.Fatal(err)
	}
	if err = read.Close(); err != nil {
		t.Fatal(err)
	}
	complete = false
	for range 64 {
		id := [16]byte(n.requestID())
		d, err := owner.Page(t.Context(), id, boundary)
		if err != nil || d.page.kind != 0 {
			t.Fatal("new retry stranded feed or duplicated business delta", d.page.kind, err)
		}
		completed, err := owner.Ack(t.Context(), id, d.digest)
		if err != nil {
			t.Fatal(err)
		}
		if d.page.eof {
			if !feedCutEqual(completed, boundary) {
				t.Fatal("post-GC retry did not complete actual boundary")
			}
			complete = true
			break
		}
	}
	if !complete {
		t.Fatal("post-GC original retry permanently stranded live feed")
	}
	if err = owner.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func retainedFeedSnapshotAdmission(t *testing.T) {
	n, p, _, _ := retainedPublisherFromNetwork(t, collectedGraphNetworkWithAgreement(t, nil, retainedFeedSemanticContractID()))
	policy := retainedFeedGuardPolicy(4)
	policy.deadlineUnixNanos = time.Now().Add(50 * time.Second).UnixNano()
	owner, err := beginRetainedFeed(t.Context(), p, n.requestID(), graphReadRequest{mode: graphReadFresh}, 100, policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = owner.Publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	caller, cancel := context.WithDeadline(t.Context(), time.Unix(0, policy.deadlineUnixNanos).Add(time.Hour))
	defer cancel()
	read, err := owner.OpenSnapshot(caller)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range read.sources {
		wrapped, ok := source.(*retainedFeedSnapshotSource)
		if !ok {
			t.Fatal("snapshot lost admitted owner")
		}
		deadline, ok := wrapped.lifetime.ctx.Deadline()
		if !ok || deadline.UnixNano() != policy.deadlineUnixNanos || wrapped.lifetime.ctx.Err() != nil {
			t.Fatal("snapshot escaped fixed deadline or was canceled on return", deadline, wrapped.lifetime.ctx.Err())
		}
	}
	assertCertifiedProjection(t, certifiedView(t, read), 1, true, nil)
	if err = read.Close(); err != nil {
		t.Fatal(err)
	}
	if err = owner.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Tight source/output ceilings fail inside the admitted operation, instead
	// of Publisher.Open silently allocating its historical default invocation.
	for _, output := range []bool{false, true} {
		small := retainedFeedGuardPolicy(1)
		small.maxAttempts = 1
		if output {
			small.operationOutput = 32768
		} else {
			small.operationRows = 16
		}
		o, err := beginRetainedFeed(t.Context(), p, n.requestID(), graphReadRequest{mode: graphReadFresh}, 100, small)
		if err != nil {
			t.Fatal(err)
		}
		token, err := o.Publish(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if got, err := o.OpenSnapshot(t.Context()); got != nil || !errors.Is(err, errLimit) {
			t.Fatal("snapshot bypassed explicit operation grant", output, err)
		}
		reopened, err := openRetainedFeed(t.Context(), p, token)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = reopened.Pending(t.Context()); !errors.Is(err, errLimit) || errors.Is(err, errRetainedFeedPending) {
			t.Fatal("native attempt exhaustion renewed or became pending", err)
		}
		if err = reopened.Release(t.Context()); err != nil {
			t.Fatal("grant exhaustion stranded prepaid terminal", err)
		}
	}
	// Two genuine preflights see an available final grant. The queued follower
	// proposal loses the actual Stage race and installs a deterministic limit,
	// never apply failure or a refundable/nonterminal pending outcome.
	last := retainedFeedGuardPolicy(1)
	last.maxAttempts = 1
	o, err := beginRetainedFeed(t.Context(), p, n.requestID(), graphReadRequest{mode: graphReadFresh}, 100, last)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = o.Publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	h := n.hosts[0][0]
	scope := h.machine.scope()
	command := o.command(o.pin.invocation.endpoints[3], feedAdmitOperation, [16]byte(n.requestID()), nil)
	wire, err := encodeRetainedFeedCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	held, err := n.hosts[0][1].Submit(allocationProposal{target: scope, wire: wire})
	if err != nil || len(held.packets) != 1 {
		t.Fatal("actual queued admission", held, err)
	}
	first := o.command(o.pin.invocation.endpoints[3], feedAdmitOperation, [16]byte(n.requestID()), nil)
	firstWire, err := encodeRetainedFeedCommand(first)
	if err != nil {
		t.Fatal(err)
	}
	event, err := h.Submit(allocationProposal{target: scope, wire: firstWire})
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, event)
	event, err = h.Step(held.packets[0])
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, event)
	resultWire, err := h.machine.store.ApplicationRecord(t.Context(), h.driver.Applied(), true, 256)
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeRetainedFeedResult(resultWire)
	if err != nil || result.status != feedLimited || !errors.Is(retainedFeedStatusError(result.status), errLimit) {
		t.Fatal("raced grant poisoned apply/became pending", result, err)
	}
	if err = o.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestNativeProtocolPooledFeedPublicationStagesOnRealQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	n := collectedGraphNetworkWithRecovery(t, func(p *raftlog.ApplicationPolicy) {
		p.RetainedApplicationBytes, p.RetainedApplicationRecords = 256<<20, 131072
	}, rangeMovePooledSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 4})
	_, publisher, _, _ := retainedPublisherFromNetwork(t, n)
	l := publisher.coordinator.host.machine.limits
	policy := retainedFeedPolicy{pageBytes: 64 << 10, pageRows: 256, maxPages: 64, maxAttempts: 512, operationRows: l.sourceRows, operationBytes: l.sourceBytes, operationOutput: l.outputBytes, deadlineUnixNanos: deadline.UnixNano()}
	owner, err := beginRetainedFeed(ctx, publisher, n.requestID(), graphReadRequest{mode: graphReadFresh}, 1, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(); err != nil {
			t.Error("close actual pooled feed owner", err)
		}
	}()
	token, err := owner.Publish(ctx)
	if err != nil || len(token.definition) == 0 || len(token.pin.publications) != 2 {
		t.Fatal("actual pooled publication and all-participant cursor initialization", err)
	}
}

func TestRetainedFeedCursorOwnsBoundedParticipantDecode(t *testing.T) {
	// Codec values are descriptive; genuine native installation is checked by
	// TestNativeProtocolPooledFeedPublicationStagesOnRealQuorum.
	for _, count := range []int{1, 2, maxGraphPins} {
		cursor := retainedFeedCursor{original: [32]byte{1}, positions: make([]uint64, count), completed: graphCut{certificates: make([]graphCertificate, count)}, sequence: 7, admitted: 9, spentRows: 144, spentBytes: 294912, spentOutput: 294912, spentCalls: 9216}
		for i := range count {
			cursor.positions[i] = uint64(i + 3)
			cursor.completed.certificates[i] = graphCertificate{scope: allocationScope{graph: [16]byte{1}, partition: uint64(i + 1), group: [16]byte{2}, ownership: 1, topology: 1, declaration: [32]byte{3}, semantic: rangeMovePooledSemanticContractID()}, configuration: [32]byte{4}, round: 5, index: 4, sourceIndex: 3}
		}
		wire, err := encodeRetainedFeedCursor(cursor)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := decodeRetainedFeedCursor(wire)
		if err != nil {
			t.Fatal("nonempty participant cursor", count, err)
		}
		canonical, err := encodeRetainedFeedCursor(actual)
		if err != nil || !bytes.Equal(canonical, wire) || !feedCutEqual(actual.completed, cursor.completed) || !feedVectorEqual(actual.positions, cursor.positions) {
			t.Fatal("cursor changed during decode", count, err)
		}
		body, err := graphBody(wire, "GFC\x01", graphPinManifestBytes+2048)
		if err != nil {
			t.Fatal(err)
		}
		for _, kind := range []string{"vector-count", "cut-count", "zero-position", "truncated", "trailing"} {
			bad := bytes.Clone(body)
			switch kind {
			case "vector-count":
				binary.BigEndian.PutUint32(bad[40:44], maxGraphPins+1)
			case "cut-count":
				binary.BigEndian.PutUint32(bad[44+8*count:48+8*count], maxGraphPins+1)
			case "zero-position":
				clear(bad[44:52])
			case "truncated":
				bad = bad[:len(bad)-1]
			case "trailing":
				bad = append(bad, 0)
			}
			if _, err = decodeRetainedFeedCursor(seal(append([]byte("GFC\x01"), bad...))); !errors.Is(err, errCorrupt) {
				t.Fatal("malformed cursor accepted", count, kind, err)
			}
		}
	}
}

// The actual Page3 residual fits these native records, despite the configured
// one-MiB change ceiling. No new reader, grant, or retained owner is created.
func TestRetainedFeedHistoryUsesFundedResidualForActualFrames(t *testing.T) {
	n := collectedGraphNetworkWithRecovery(t, nil, rangeMovePooledSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 4})
	h := n.hosts[0][0]
	m := h.machine
	oldIndex := h.driver.Applied()
	old, err := m.store.ApplicationView(oldIndex)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := old.Close(); err != nil {
			t.Error(err)
		}
	}()
	before, err := old.ApplicationRecord(t.Context(), oldIndex, false, 64<<10)
	if err != nil || len(before) == 0 {
		t.Fatal("actual first-install changes", err)
	}
	crossEdgeFixture(t, n)
	currentIndex := h.driver.Applied()
	if currentIndex <= oldIndex {
		t.Fatal("fixture did not mutate")
	}
	current, err := m.store.ApplicationView(currentIndex)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := current.Close(); err != nil {
			t.Error(err)
		}
	}()
	latest, err := current.ApplicationRecord(t.Context(), currentIndex, false, 64<<10)
	if err != nil || bytes.Equal(before, latest) {
		t.Fatal("old/current history did not diverge", err)
	}
	for _, item := range []struct {
		v     *raftlog.ApplicationView
		index uint64
	}{{old, oldIndex}, {current, currentIndex}} {
		base, err := item.v.RootBounded(t.Context(), 172)
		if err != nil {
			t.Fatal(err)
		}
		for _, outcome := range []bool{false, true} {
			expected, err := item.v.ApplicationRecord(t.Context(), item.index, outcome, 64<<10)
			if err != nil {
				t.Fatal(err)
			}
			arena, err := graphstate.NewOutputBudget(m.limits.outputBytes)
			if err != nil {
				t.Fatal(err)
			}
			q := feedReader(t.Context(), m, item.v, base, arena)
			q.rows, q.bytes = 181, 1820348
			start := arena.Remaining()
			got, err := m.feedRecord(&q, item.index, outcome, 64<<10)
			if err != nil || got == nil || !bytes.Equal(got, expected) {
				t.Fatalf("funded actual record at%d outcome%v: %v", item.index, outcome, err)
			}
			if q.rows != 182 || q.bytes != 1820348+1082+3*len(expected) || start-arena.Remaining() != 2048+2*len(expected) {
				t.Fatal("actual work/output accounting", q.rows, q.bytes, start-arena.Remaining())
			}
			if !outcome && item.index == oldIndex && !bytes.Equal(got, before) {
				t.Fatal("retained cut returned current changes")
			}
		}
	}
	base, err := current.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, item := range []struct {
		ctx     context.Context
		bytes   int
		maximum int
		want    error
	}{
		{t.Context(), m.limits.allocation.readBytes - 1082, 64 << 10, raftlog.ErrLimit},
		{t.Context(), 1820348, 0, raftlog.ErrLimit},
		{canceled, 1820348, 64 << 10, context.Canceled},
	} {
		arena, err := graphstate.NewOutputBudget(m.limits.outputBytes)
		if err != nil {
			t.Fatal(err)
		}
		q := feedReader(item.ctx, m, current, base, arena)
		q.rows, q.bytes = 181, item.bytes
		offer := min(1082+3*m.store.ApplicationLimits().MaxChangeBytes, q.limits.readBytes-q.bytes)
		data, err := m.feedRecord(&q, currentIndex, false, item.maximum)
		if !errors.Is(err, item.want) || data != nil || q.rows != 182 || q.bytes != item.bytes+offer || arena.Remaining() != m.limits.outputBytes-2048-2*item.maximum {
			t.Fatal("failed read lost cause/full offered spend", item, q.rows, q.bytes, arena.Remaining(), err)
		}
	}
	for _, kind := range []string{"negative-ownership", "rows", "lookup-bytes", "nil-output", "output"} {
		arena, err := graphstate.NewOutputBudget(2047)
		if err != nil {
			t.Fatal(err)
		}
		q := feedReader(t.Context(), m, current, base, arena)
		maximum := 0
		switch kind {
		case "negative-ownership":
			maximum = -1
		case "rows":
			q.rows = q.limits.readRows
		case "lookup-bytes":
			q.bytes = q.limits.readBytes - 1081
		case "nil-output":
			q.arena = nil
		}
		rows, bytes := q.rows, q.bytes
		if data, err := m.feedRecord(&q, currentIndex, false, maximum); !errors.Is(err, errLimit) || data != nil || q.rows != rows || q.bytes != bytes || arena.Remaining() != 2047 {
			t.Fatal("unfunded lookup had effects", kind, q.rows, q.bytes, err)
		}
	}
}

func TestRetainedFeedPooledImportCensusUsesActualRecoveryWork(t *testing.T) {
	n, p, _, _ := retainedPublisherFromNetwork(t, collectedGraphNetworkWithRecovery(t, func(policy *raftlog.ApplicationPolicy) {
		policy.RetainedApplicationBytes, policy.RetainedApplicationRecords = 256<<20, 131072
		if err := nativeProtocolStatePooledTailPolicy(policy); err != nil {
			t.Fatal(err)
		}
	}, rangeMovePooledSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 4}))
	owner, err := beginRetainedFeed(t.Context(), p, n.requestID(), graphReadRequest{mode: graphReadFresh}, 100, retainedFeedGuardPolicy(64))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err = owner.Publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	old, err := owner.OpenSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := old.Close(); err != nil {
			t.Error(err)
		}
	}()
	assertCertifiedProjection(t, certifiedView(t, old), 1, true, nil)
	h := n.hosts[1][0]
	first := h.driver.Applied() + 1
	intent, err := n.hosts[0][0].PreparePooledRangeMove(t.Context(), n.requestID(), 1, 8, 64, 32, 16<<10, rangeMovePools{2 << 20, 8 << 20, 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	move, err := newRangeMoveCoordinatorOn(p.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := runRangeMoveToCompletion(t, move, intent)
	if err != nil || !completed.committed {
		t.Fatal("genuine bounded move completion", err)
	}
	last := h.driver.Applied()
	if last < first || last-first >= retainedFeedProgressBatch {
		t.Fatal("native partial prefix outside census", first, last)
	}
	view, err := h.machine.store.ApplicationView(last)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := view.Close(); err != nil {
			t.Error(err)
		}
	}()
	imports, activation := 0, uint64(0)
	for index := first; index <= last; index++ {
		wire, err := view.ApplicationRecord(t.Context(), index, true, 64<<10)
		if err != nil {
			t.Fatal(err)
		}
		if !moveStatusWireFor(h.machine.SemanticContractID(), wire) {
			continue
		}
		status, err := decodeRangeMoveStatus(wire)
		if err != nil {
			t.Fatal(err)
		}
		if status.index != index {
			continue
		}
		if status.phase == rangeMoveImport && status.sequence > 0 {
			imports++
		}
		if status.phase == rangeMoveActivate {
			activation = index
		}
	}
	if imports < 4 || activation <= first {
		t.Fatal("missing actual consecutive imports/activation", imports, activation)
	}
	base, err := view.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	arena, err := graphstate.NewOutputBudget(h.machine.limits.outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	q := feedReader(t.Context(), h.machine, view, base, arena)
	frame, err := h.machine.feedProgress(&q, first, activation-1, 64<<10, first)
	if err != nil || frame.index != first || frame.progressNext != activation || frame.census == ([32]byte{}) || q.bytes >= 4<<20 || q.rows >= 1024 {
		t.Fatal("actual consecutive import frames exceed original reader", q.rows, q.bytes, first, last, err)
	}
	assertCertifiedProjection(t, certifiedView(t, old), 1, true, nil)
}
