package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func TestNativeTransferAdmissionKeepsHardCapsAndRequiresExplicitPools(t *testing.T) {
	for _, test := range []struct {
		kind   byte
		useful bool
	}{{0, false}, {feedLocalEnvelope, true}, {feedCrossEnvelope, true}, {feedMoveEnvelope, false}, {255, false}} {
		if nativeTransferUsefulPage(retainedFeedPage{kind: test.kind}) != test.useful {
			t.Fatal("only actual local/cross business envelopes can select the unread update", test.kind)
		}
	}
	for _, facts := range []uint64{40, 10000, 100000, 1000000} {
		for _, writers := range []int{1, 8, 32} {
			for _, cross := range []uint64{0, 10, 50, 100} {
				s := nativeGraphCostSpec{12, facts, cross, writers}
				g := nativeTransferGrants{4_000_000, 64 << 30, 384 << 30, 384 << 30}
				frames, err := nativeTransferFrames(s, g)
				if err != nil || frames <= nativeTransferOriginals(s) {
					t.Fatal("finite full dimensions", s, err)
				}
				p, l := raftlog.DefaultApplicationPolicy(1), raftlog.DefaultLimits()
				p.MaxInstallWrites, p.MaxImageBytes = 256, 172
				var init declaredInitCommand
				var generations raftlog.ApplicationGenerationLimits
				a := nativeTransferAdmission{Family: nativeTransferFamily, Spec: nativeTransferSpec{2, nativeTransferFamily, facts, cross, writers, "columns"}, Grants: g}
				if err = nativeTransferProfile(s, a, &init, &l, &p, &generations); err != nil || p.RetainedApplicationBytes != 1<<40 || l.MaxRetainedBytes != 1<<40 || l.MaxRetainedEntries != 10_000_000 || generations.MaxBytes != 1<<40 || p.RetainedApplicationRecords > 1<<40 {
					t.Fatal("native independent hard limits", s, err)
				}
			}
		}
	}
	for _, g := range []nativeTransferGrants{{}, {4_000_001, 1 << 20, 1 << 20, 1 << 20}, {64, 1 << 40, 1 << 20, 1 << 20}, {64, 1 << 20, 1 << 40, 1 << 40}} {
		if g.validate() == nil {
			t.Fatal("unfunded or overflowed grant admitted", g)
		}
	}
	t.Setenv(nativeTransferProcessFlag, "")
	if enabled, err := nativeTransferProcessProfile(nil); enabled || err != nil {
		t.Fatal("ordinary nil profile", enabled, err)
	}
	s := nativeGraphCostSpec{12, 40, 100, 32}
	if enabled, err := nativeTransferProcessProfile(&s); enabled || !errors.Is(err, errInvalid) {
		t.Fatal("cost12 silently selected without explicit admission", enabled, err)
	}
}

func TestNativeTransferProgressNeedsAppliedPhaseOrPage(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, _, _ := retainedPublisherFromNetwork(t, collectedGraphNetworkWithRecovery(t, func(policy *raftlog.ApplicationPolicy) {
		policy.RetainedApplicationBytes, policy.RetainedApplicationRecords = 512<<20, 262144
	}, rangeMovePooledSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 4}))
	intent, err := n.hosts[0][0].PreparePooledRangeMove(t.Context(), n.requestID(), 1, 8, 64, 32, 16<<10, rangeMovePools{2 << 20, 8 << 20, 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := encodeRangeMoveIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(wire)
	move := rangeMoveID(n.hosts[0][0].machine.ns, intent.id)
	observed := [2]rangeMoveStatus{{format: 3, ns: n.hosts[0][0].machine.ns, move: move, intent: digest, phase: rangeMoveFence, index: 100, result: rangeMoveBlocked}, {}}
	if _, advanced, err := nativeTransferMoveProgress(intent, digest, move, [2]rangeMoveStatus{}, observed); err != nil || advanced {
		t.Fatal("new blocked index became progress", err)
	}
	observed[0].result = rangeMoveApplied
	previous, advanced, err := nativeTransferMoveProgress(intent, digest, move, [2]rangeMoveStatus{}, observed)
	if err != nil || !advanced {
		t.Fatal("genuine applied Fence did not advance", err)
	}
	observed[0].index++
	if _, advanced, err := nativeTransferMoveProgress(intent, digest, move, previous, observed); err != nil || advanced {
		t.Fatal("physical-only movement reset retry grant", err)
	}
	observed[0].phase = rangeMoveDescribe
	observed[0].sequence = 1
	if _, advanced, err := nativeTransferMoveProgress(intent, digest, move, previous, observed); err != nil || !advanced {
		t.Fatal("applied page progression refused", err)
	}
	observed[0].intent[0] ^= 1
	if _, _, err := nativeTransferMoveProgress(intent, digest, move, previous, observed); !errors.Is(err, errCorrupt) {
		t.Fatal("foreign intent advanced", err)
	}
}

func TestNativeTransferPooledRefusalBindsActualFailedCommand(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, publisher, _, _ := retainedPublisherFromNetwork(t, collectedGraphNetworkWithRecovery(t, func(p *raftlog.ApplicationPolicy) {
		p.MaxTailEntries = 8
		p.RetainedApplicationBytes, p.RetainedApplicationRecords = 512<<20, 262144
	}, rangeMovePooledSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 4}))
	intent, err := n.hosts[0][0].PreparePooledRangeMove(t.Context(), n.requestID(), 1, 8, 64, 32, 16<<10, rangeMovePools{movePoolRefusalBytes + 1, 8 << 20, 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	capture := &movePoolLostDescribeReply{graphPeerWireTransport: publisher.coordinator.transports[3]}
	publisher.coordinator.transports[3] = capture
	c, err := newRangeMoveCoordinatorOn(publisher.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	result, cause := runRangeMoveToCompletion(t, c, intent)
	if errors.Is(cause, io.EOF) {
		result, cause = runMovePoolResume(t, c, result.resume)
	}
	if !errors.Is(cause, errLimit) || result.committed || result.source.phase != rangeMoveAbort || len(capture.command) == 0 {
		t.Fatal("real refusal and ABORT required", cause)
	}
	command, err := decodeRangeMoveCommand(capture.command)
	if err != nil {
		t.Fatal(err)
	}
	q, err := graphRetentionReader(n.hosts[0][0], t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := q.view.Close(); err != nil {
			t.Error(err)
		}
	}()
	row, found, err := crossControl(q, movePoolOriginalKey(command), 288)
	if err != nil || !found || len(row.Value) <= 32 {
		t.Fatal("actual consumed phase original", err)
	}
	status, err := decodeRangeMoveStatus(row.Value[32:])
	if err != nil || status.result != rangeMoveBudgetLimit || status.failedCommand != sha256.Sum256(capture.command) || !pooledMoveOriginalMatches(row, status, row.Value[32:]) {
		t.Fatal("actual command/refusal join", err)
	}
	if _, err := readPooledMoveOriginal(q, n.hosts[0][0].machine.scope(), status); err != nil {
		t.Fatal("immutable failed phase after ABORT", err)
	}
	bad := row
	bad.Value = bytes.Clone(row.Value)
	bad.Value[0] ^= 1 // Nonzero wrong SHA while exact outcome/index remain unchanged.
	if pooledMoveOriginalMatches(bad, status, row.Value[32:]) {
		t.Fatal("wrong GM/V SHA admitted by feed/column predicate")
	}
	bad = row
	bad.Index++
	if pooledMoveOriginalMatches(bad, status, row.Value[32:]) {
		t.Fatal("wrong installed index")
	}
}

func TestNativeTransferDecisionProofSurvivesActualRawACKPrefixGC(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	t.Setenv(nativeTransferProcessFlag, "1")
	n, p, ops, _ := retainedPublisherFromNetwork(t, collectedGraphNetworkWithRecovery(t, func(policy *raftlog.ApplicationPolicy) {
		policy.RetainedApplicationBytes, policy.RetainedApplicationRecords = 512<<20, 262144
	}, rangeMovePooledSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 4}))
	h := n.hosts[0][0]
	id := n.requestID()
	change := []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "K-after-C"}}
	result, err := p.coordinator.Run(t.Context(), id, change, 70, nil)
	if err != nil || !result.decision.commit {
		t.Fatal("real K commit", err)
	}
	before, err := nativeGraphCostCoordinatorProofWire(t.Context(), h, result)
	if err != nil {
		t.Fatal(err)
	}
	q, err := graphRetentionReader(h, t.Context())
	if err != nil {
		t.Fatal(err)
	}
	index := q.base.Index
	if err := q.view.Close(); err != nil {
		t.Fatal(err)
	}
	proposal, err := h.PrepareGraphRetention(t.Context(), n.requestID(), graphRetentionFloors{index, index, result.decision.index + 1, result.decision.index + 1, index, 1})
	if err != nil {
		t.Fatal(err)
	}
	event, err := h.Submit(proposal)
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, event)
	installed, found, err := h.GraphRetentionOriginal(t.Context(), proposal)
	if err != nil || !found || installed.status != graphRetentionApplied {
		t.Fatal("actual post-K retention", err)
	}
	for _, voter := range n.hosts[0] {
		var progress raftlog.ApplicationRetentionProgress
		for calls := 0; !progress.Complete && calls < 256; calls++ {
			progress, _, err = voter.machine.store.CollectApplicationRetention(t.Context(), installed.id, raftlog.ReadBudget{Rows: 16, Bytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
		}
		if !progress.Complete {
			t.Fatal("actual collection did not complete")
		}
	}
	if _, err := h.machine.store.ApplicationRecord(t.Context(), result.decision.index, true, 64<<10); !errors.Is(err, raftlog.ErrApplicationExpired) {
		t.Fatal("raw K decision did not actually expire", err)
	}
	replayed, err := p.coordinator.Run(t.Context(), id, change, 70, nil)
	if err != nil || replayed.decision != result.decision {
		t.Fatal("SAME original after raw GC", err)
	}
	after, err := nativeGraphCostCoordinatorProofWire(t.Context(), h, replayed)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("immutable D/R proof lost or refreshed original", err)
	}
	bad := result
	bad.decision.digest[0] ^= 1
	if _, err := nativeGraphCostCoordinatorProofWire(t.Context(), h, bad); !errors.Is(err, errCorrupt) {
		t.Fatal("caller digest became native decision proof", err)
	}
}

type nativeTransferMoveCapture struct {
	graphPeerWireTransport
	budget   *graphstate.OutputBudget
	commands [][]byte
}

func (w *nativeTransferMoveCapture) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	r, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	if r.kind == sourceSubmit {
		p, err := decodeGraphSourceProposal(r.command)
		if err != nil {
			return nil, err
		}
		if isRangeMoveWire(p.wire) {
			c, err := decodeRangeMoveCommand(p.wire)
			if err != nil {
				return nil, err
			}
			if c.phase == rangeMoveImport && c.sequence == 1 || c.phase == rangeMoveActivate {
				if len(w.commands) == 3 {
					return nil, errLimit
				}
				if err := w.budget.Reserve(128 + 2*len(p.wire)); err != nil {
					return nil, err
				}
				w.commands = append(w.commands, bytes.Clone(p.wire))
			}
		}
	}
	return w.graphPeerWireTransport.RoundTrip(ctx, wire)
}

// Actual pooled native imports/activation/replay enter the SAME production
// column classifier used by the workload. An imported graph is not fabricated
// business CDC; the current graph remains separately observable at its owner.
func TestNativeTransferColumnsJoinPooledOriginalsAndPreserveMoveCDC(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, publisher, _, _ := retainedPublisherFromNetwork(t, collectedGraphNetworkWithRecovery(t, func(p *raftlog.ApplicationPolicy) {
		p.MaxTailEntries = 8
		p.RetainedApplicationBytes, p.RetainedApplicationRecords = 512<<20, 262144
	}, rangeMovePooledSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 4}))
	budget, err := graphstate.NewOutputBudget(4 << 20)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := beginRetainedFeed(t.Context(), publisher, n.requestID(), graphReadRequest{mode: graphReadFresh}, 100, retainedFeedGuardPolicy(128))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	capture := &nativeTransferMoveCapture{publisher.coordinator.transports[8], budget, nil}
	publisher.coordinator.transports[8] = capture
	intent, err := n.hosts[0][0].PreparePooledRangeMove(t.Context(), n.requestID(), 1, 8, 64, 32, 16<<10, rangeMovePools{2 << 20, 8 << 20, 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := newRangeMoveCoordinatorOn(publisher.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	done, err := runRangeMoveToCompletion(t, coordinator, intent)
	if err != nil || !done.committed || len(capture.commands) != 2 {
		t.Fatal("actual import1 and activation", done, len(capture.commands), err)
	}
	// Re-submit the exact installed Import body after Finish. Its immutable
	// native original wins; it cannot import again or become a second GML.
	command, err := decodeRangeMoveCommand(capture.commands[0])
	if err != nil || command.phase != rangeMoveImport {
		t.Fatal(err)
	}
	before := n.hosts[1][0].driver.Applied()
	pump := nativeGraphTestPump{n: n, group: 1, host: n.hosts[1][0]}
	result, err := pump.Submit(t.Context(), allocationProposal{target: intent.target, wire: capture.commands[0]})
	if err != nil {
		t.Fatal(err)
	}
	original, err := decodeRangeMoveStatus(result)
	if err != nil || original.index > before || original.phase != rangeMoveImport {
		t.Fatal("retained import original", err)
	}
	var imports, activations, replays int
	for group := range 2 {
		h := n.hosts[group][0]
		view, err := h.machine.store.ApplicationView(h.driver.Applied())
		if err != nil {
			t.Fatal(err)
		}
		root, err := view.RootBounded(t.Context(), 172)
		if err != nil {
			t.Fatal(err)
		}
		source := &nativeColumnSource{binding: nativeColumnBinding{Version: 2, Policy: nativeColumnGuardPolicy()}}
		source.hosts[group], source.views[group], source.roots[group] = h, view, root
		for index := uint64(2); index <= root.Index; index++ {
			wire, err := view.ApplicationRecord(t.Context(), index, true, 32<<10)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasPrefix(wire, []byte("GMO\x03")) {
				continue
			}
			frame, err := source.frame(t.Context(), group, index)
			if err != nil || len(frame.Rows) != 0 {
				t.Fatal("genuine move metadata classification", group, index, err)
			}
			status, err := decodeRangeMoveStatus(wire)
			if err != nil {
				t.Fatal(err)
			}
			if status.phase == rangeMoveImport {
				imports++
			}
			if status.phase == rangeMoveActivate && status.index == index {
				activations++
				if !bytes.HasPrefix(frame.CDC, []byte("GML\x01")) {
					t.Fatal("activation lineage lost")
				}
			}
			if status.index < index {
				replays++
				if len(frame.CDC) != 0 || !bytes.Equal(frame.Outcome, frame.OriginalOutcome) {
					t.Fatal("replay invented business lineage")
				}
			}
			if len(frame.Registration) != 0 {
				c := graphCursor{b: frame.Registration}
				if string(c.take(4)) != "NCM1" {
					t.Fatal("missing actual I/V/D evidence")
				}
				for slot := range 3 {
					key, installed, value := c.field(1024), c.u64(), c.field(2048)
					if slot == 1 && status.phase == rangeMoveImport && status.sequence == 1 {
						hash := sha256.Sum256(capture.commands[0])
						if installed != status.index || !bytes.Equal(key, movePoolOriginalKey(command)) || len(value) < 32 || !bytes.Equal(value[:32], hash[:]) || !bytes.Equal(value[32:], wire) {
							t.Fatal("GM/V did not bind actual full GMQ3 body/original")
						}
					}
				}
				if c.err != nil || len(c.b) != 0 {
					t.Fatal("extra/truncated column evidence", c.err)
				}
			}
		}
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if imports < 2 || activations != 2 || replays == 0 {
		t.Fatal("missing actual import/two-lineage/replay coverage", imports, activations, replays)
	}
	if index, err := nativeColumnOriginalIndex(result, n.hosts[1][0].machine.ns); err != nil || index != original.index {
		t.Fatal("move original dependency dropped", index, err)
	}
	foreign := original.ns
	foreign.partition = 99
	if _, err := nativeColumnOriginalIndex(result, foreign); !errors.Is(err, errCorrupt) {
		t.Fatal("foreign move original became dependency authority", err)
	}
	// Complete the actual move envelope before advancing its Outcome floor.
	boundary := func() graphCut {
		read, err := publisher.coordinator.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
		if err != nil {
			t.Fatal(err)
		}
		cut, err := read.Cut()
		if err = errors.Join(err, read.Close()); err != nil {
			t.Fatal(err)
		}
		return cut
	}
	drain := func(cut graphCut, targetIndex uint64) {
		seen, completed := targetIndex == 0, false
		for range 64 {
			request := [16]byte(n.requestID())
			d, err := owner.Page(t.Context(), request, cut)
			if err != nil {
				t.Fatal("pooled complete Page", err)
			}
			for _, f := range d.page.frames {
				if f.partition == 8 && (f.index == targetIndex || f.index <= targetIndex && f.progressNext > targetIndex) {
					seen = true
				}
			}
			if d.page.kind == feedMoveEnvelope && nativeTransferUsefulPage(d.page) {
				t.Fatal("actual authenticated GML lineage became a useful update")
			}
			ack, err := owner.Ack(t.Context(), request, d.digest)
			if err != nil {
				t.Fatal("pooled ACK", err)
			}
			again, err := owner.Page(t.Context(), request, cut)
			if err != nil || !again.replay || !bytes.Equal(again.wire, d.wire) {
				t.Fatal("SAME Page after ACK", err)
			}
			if replay, err := owner.Ack(t.Context(), request, d.digest); err != nil || !feedCutEqual(replay, ack) {
				t.Fatal("SAME ACK", err)
			}
			if d.page.eof {
				if !feedCutEqual(ack, cut) {
					t.Fatal("half move completed cut")
				}
				completed = true
				break
			}
		}
		if !seen || !completed {
			t.Fatal("missing replay/protocol cursor or boundary completion", seen, completed)
		}
	}
	drain(boundary(), 0)
	h := n.hosts[1][0]
	q, err := graphRetentionReader(h, t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readPooledMoveOriginal(q, h.machine.scope(), original); err != nil {
		t.Fatal("genuine old phase after Finish", err)
	}
	for _, change := range []func(*rangeMoveStatus){func(s *rangeMoveStatus) { s.index++ }, func(s *rangeMoveStatus) { s.move[0] ^= 1 }, func(s *rangeMoveStatus) { s.intent[0] ^= 1 }, func(s *rangeMoveStatus) { s.phase = rangeMoveDescribe }} {
		bad := original
		change(&bad)
		if _, err := readPooledMoveOriginal(q, h.machine.scope(), bad); !errors.Is(err, errCorrupt) {
			t.Fatal("missing/wrong immutable control admitted", err)
		}
	}
	if err := q.view.Close(); err != nil {
		t.Fatal(err)
	}
	initial := owner.pin.definition.parts[1].certificate
	floor := original.index + 1
	proposal, err := h.PrepareGraphRetention(t.Context(), n.requestID(), graphRetentionFloors{initial.sourceIndex, initial.sourceIndex, floor, floor, initial.sourceIndex, initial.index})
	if err != nil {
		t.Fatal(err)
	}
	event, err := h.Submit(proposal)
	if err != nil {
		t.Fatal(err)
	}
	n.pump(1, event)
	installed, found, err := h.GraphRetentionOriginal(t.Context(), proposal)
	if err != nil || !found || installed.status != graphRetentionApplied {
		t.Fatal("genuine ACK-prefix floor", installed, found, err)
	}
	for _, voter := range n.hosts[1] {
		var progress raftlog.ApplicationRetentionProgress
		for calls := 0; !progress.Complete && calls < 256; calls++ {
			progress, _, err = voter.machine.store.CollectApplicationRetention(t.Context(), installed.id, raftlog.ReadBudget{Rows: 16, Bytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
		}
		if !progress.Complete {
			t.Fatal("bounded actual native collection unfinished")
		}
	}
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := view.ApplicationRecord(t.Context(), original.index, true, 32<<10); !errors.Is(err, raftlog.ErrApplicationExpired) {
		t.Fatal("old raw outcome was not actually expired", err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	// This NEW physical entry repeats the exact earlier GM/V result after its
	// raw sidecars are gone. Feed progress must use immutable ACK-prefix proof.
	if _, err := pump.Submit(t.Context(), allocationProposal{target: intent.target, wire: capture.commands[0]}); err != nil {
		t.Fatal(err)
	}
	replayedIndex := h.driver.Applied()
	drain(boundary(), replayedIndex)
	for member := range 3 {
		read, close := crossView(t, n, member, [2]uint64{})
		assertCertifiedProjection(t, read, 1, true, nil)
		assertCertifiedProjection(t, read, 17, true, nil)
		assertCertifiedProjection(t, read, 999, false, nil)
		close()
	}
}
