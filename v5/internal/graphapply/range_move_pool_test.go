package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// Authored direct guards; execution and 1M completion qualification are separate.
func TestRangeMovePooledWireDebtAndRefusalBoundaries(t *testing.T) {
	if !rangeMoveGraphBatches(rangeMovePooledSemanticContractID()) || !retainedOnlyFeedAgreement(rangeMovePooledSemanticContractID()) || rangeMoveStatusFormat(rangeMovePooledSemanticContractID()) != 3 {
		t.Fatal("fresh capability chain")
	}
	progress := rangeMoveProgress{imported: &graphstore.RangeMoveImportState{Sequence: 7, PreparationSteps: 2}, graphApplied: 3, replayPages: 2, replay: rangeMoveReplayCursor{sequence: 64}, reference: moveRetentionReference{bytes: 99}}
	for _, tc := range []struct {
		id   raftlog.ApplicationSemanticContractID
		want uint64
	}{{rangeMoveSemanticContractID(), 73}, {rangeMoveSpanSemanticContractID(), 11}, {rangeMoveGraphBatchSemanticContractID(), 7}, {rangeMovePooledSemanticContractID(), 7}} {
		if got := moveBaselinePages(tc.id, progress); got != tc.want {
			t.Fatal("logical frames substituted for installed baseline pages", got, tc.want)
		}
	}
	progress.descriptor.Source.Binding.SemanticContractID = rangeMovePooledSemanticContractID()
	if progress.retainedBytes() != 99 {
		t.Fatal("reference bytes lost from compact baseline")
	}
	if _, err := newRangeMovePooledMaterializer(nil, namespace{}, graphstore.OwnershipDeclaration{}, defaultMaterializerLimits()); !errors.Is(err, errInvalid) {
		t.Fatal("nil constructor", err)
	}
	if err := applyMovePools(nil, rangeMovePools{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil recipe", err)
	}
	if _, err := prepayMoveRefusal(nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil refusal", err)
	}
	for _, bad := range []rangeMovePools{{}, {1, 1, 1}, {math.MaxUint64, 1, 1}, {65537, 65537, 0}} {
		if bad.valid(true) {
			t.Fatal("bad hard grant", bad)
		}
	}
	n := namespace{graph: [16]byte{1}, partition: 3}
	s := rangeMoveStatus{format: 3, ns: n, move: [16]byte{2}, intent: [32]byte{3}, phase: rangeMoveCapture, index: 4, result: rangeMoveBudgetLimit, failedPhase: rangeMoveDescribe, failedSequence: 0, failedCommand: [32]byte{5}}
	wire, err := encodeRangeMoveStatus(s)
	if err != nil || len(wire) != 231 || rangeMoveTerminal(s) {
		t.Fatal("first Describe0 refusal is nonterminal", len(wire), err)
	}
	decoded, err := decodeRangeMoveStatus(wire)
	if err != nil || decoded != s {
		t.Fatal("native refusal roundtrip", err)
	}
	for _, mutate := range []func(*rangeMoveStatus){func(s *rangeMoveStatus) { s.failedPhase = rangeMoveImport }, func(s *rangeMoveStatus) { s.failedSequence = 1 }, func(s *rangeMoveStatus) { s.format = 2 }, func(s *rangeMoveStatus) { s.failedCommand = [32]byte{} }} {
		bad := s
		mutate(&bad)
		if _, err := encodeRangeMoveStatus(bad); !errors.Is(err, errInvalid) {
			t.Fatal("forged predecessor/legacy refusal", err)
		}
	}
	rows := []raftlog.KV{{Key: []byte{1}, Value: []byte("old5")}, {Key: []byte{2}, Deleted: true}}
	undo, err := encodeMoveUndoRows(rows, 65536)
	if err != nil {
		t.Fatal(err)
	}
	got, bound, err := decodePooledMoveUndo(undo)
	if err != nil || bound != 65536 || len(got) != 2 || !bytes.Equal(got[0].Value, []byte("old5")) || !got[1].Deleted {
		t.Fatal("exact old images/debt", err)
	}
	if _, err := decodeRangeMoveUndo(undo); !errors.Is(err, errCorrupt) {
		t.Fatal("GMU2 accepted as GMU1", err)
	}
	if _, _, err := decodePooledMoveUndo(undo[:len(undo)-1]); !errors.Is(err, errCorrupt) {
		t.Fatal("partial undo", err)
	}
	parent := moveSpanOutput(t)
	budget, err := prepayMoveRefusal(&reader{arena: parent})
	if err != nil || budget.Remaining() != movePoolRefusalOutput {
		t.Fatal("prepaid branch", err)
	}
	// A fully consumed candidate arena cannot consume or reset this envelope.
	if err = parent.Reserve(parent.Remaining()); err != nil {
		t.Fatal(err)
	}
	if err := budget.Reserve(movePoolRefusalOutput); err != nil {
		t.Fatal(err)
	}
	if err := budget.Reserve(1); !errors.Is(err, graphstate.ErrResourceLimit) {
		t.Fatal("refusal grant refilled")
	}
	p := raftlog.DefaultApplicationPolicy(1)
	b := raftlog.ApplicationBatch{Image: make([]byte, 172), Outcome: wire, Writes: []raftlog.KV{{Key: rangeMoveStatusKey(n, s.move), Value: wire}}, ControlPuts: []raftlog.ApplicationControlPut{{Key: make([]byte, 48), Value: make([]byte, 263)}}}
	retained, records, _, err := p.PooledRecoveryBatchBound(b, 0)
	if err != nil || retained >= movePoolRefusalBytes || records != 6 {
		t.Fatal("all refusal roots/control/outcome not funded", retained, records, err)
	}
}

func TestRangeMovePooledNativeOriginalRefusalUndoAndReopen(t *testing.T) {
	for _, mode := range []string{"commit", "partial-undo", "describe-zero-limit", "target-next-limit"} {
		t.Run(mode, func(t *testing.T) {
			network := collectedGraphNetworkWithRecovery(t, func(p *raftlog.ApplicationPolicy) {
				p.MaxTailEntries = 8
				p.RetainedApplicationBytes = 512 << 20
				p.RetainedApplicationRecords = 262144
			}, rangeMovePooledSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 4})
			n, p, _, _ := retainedPublisherFromNetwork(t, network)
			grants := rangeMovePools{2 << 20, 8 << 20, 8 << 20}
			if mode == "describe-zero-limit" {
				grants.describe = movePoolRefusalBytes + 1
			}
			if mode == "target-next-limit" {
				grants.imported = movePoolRefusalBytes + (16 << 10)
			}
			intent, err := n.hosts[0][0].PreparePooledRangeMove(t.Context(), n.requestID(), 1, 8, 64, 32, 16<<10, grants)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := encodeRangeMoveIntent(intent)
			if err != nil || !bytes.HasPrefix(wire, []byte("GMI\x03")) {
				t.Fatal("explicit original pools", err)
			}
			decoded, err := decodeRangeMoveIntent(wire)
			if err != nil || decoded.pools != grants {
				t.Fatal("grant binding", err)
			}
			coordinator, err := newRangeMoveCoordinatorOn(p.coordinator)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "target-next-limit" {
				lost := &movePoolLostDescribeReply{graphPeerWireTransport: p.coordinator.transports[8], target: true}
				p.coordinator.transports[8] = lost
				pending, err := runRangeMoveToCompletion(t, coordinator, intent)
				if !errors.Is(err, io.EOF) || len(lost.command) == 0 {
					t.Fatal("actual next import refusal reply was not lost", err)
				}
				target := moveSpanRead(t, n, 1, pending.resume.move, false)
				progress, e := decodeRangeMoveProgress(target.observation.progress, moveSpanOutput(t))
				if e != nil || target.observation.status.result != rangeMoveBudgetLimit || target.observation.status.sequence == 0 || progress.undoTotal != target.observation.status.sequence || progress.undoDebt == 0 {
					t.Fatal("refused page entered accepted prefix/debt", e)
				}
				assertMovePoolSnapshotAudit(t, n, 1)
				crossReopenAll(t, n)
				result, e := runMovePoolResume(t, coordinator, pending.resume)
				if !errors.Is(e, errLimit) || result.committed || result.source.phase != rangeMoveAbort || result.target.phase != rangeMoveAbort {
					t.Fatal("lost refusal did not resolve through real ABORT/undo", result, e)
				}
				assertMovePoolSnapshotAudit(t, n, 1)
				return
			}
			if mode == "describe-zero-limit" {
				capture := &movePoolLostDescribeReply{graphPeerWireTransport: p.coordinator.transports[3]}
				p.coordinator.transports[3] = capture
				result, err := runRangeMoveToCompletion(t, coordinator, intent)
				if errors.Is(err, io.EOF) {
					result, err = runMovePoolResume(t, coordinator, result.resume)
				}
				if !errors.Is(err, errLimit) || result.committed || result.source.phase != rangeMoveAbort {
					t.Fatal("real no-effect Describe0 → source ABORT", result, err)
				}
				crossReopenAll(t, n)
				source := moveSpanRead(t, n, 0, result.resume.move, false)
				if source.observation.status.phase != rangeMoveAbort || rangeMoveAdmissionOwner(t, n.hosts[0][0]) != (raftlog.ApplicationRecoveryOwner{}) {
					t.Fatal("refusal owner survived genuine terminal")
				}
				// The immutable exact failed body/outcome remains recoverable after
				// ABORT replaced the current status. A changed body is not SAME.
				view, err := n.hosts[0][0].machine.store.ApplicationView(source.observation.index)
				if err != nil {
					t.Fatal(err)
				}
				base, err := view.RootBounded(t.Context(), 172)
				if err != nil {
					t.Fatal(err)
				}
				q := &reader{ctx: t.Context(), view: view, ns: n.hosts[0][0].machine.ns, base: base, limits: n.hosts[0][0].machine.limits.allocation, arena: moveSpanOutput(t)}
				attempt, e := decodeRangeMoveCommand(capture.command)
				if e != nil {
					t.Fatal("missing genuine attempted body", e)
				}
				key := movePoolOriginalKey(attempt)
				control, found, err := crossControl(q, key, 288)
				if err != nil || !found {
					t.Fatal("missing exact refusal original", err)
				}
				original, err := decodeRangeMoveStatus(control.Value[32:])
				if err != nil || original.result != rangeMoveBudgetLimit || original.failedSequence != 0 || original.phase != rangeMoveCapture {
					t.Fatal("Describe0 original changed", err)
				}
				replayed, found, e := readMovePoolOriginal(q, attempt)
				if e != nil || !found || replayed != original {
					t.Fatal("SAME original changed after ABORT", e)
				}
				attempt.data = append(owned(attempt.data), 0)
				if _, _, e = readMovePoolOriginal(q, attempt); !errors.Is(e, errInvalid) {
					t.Fatal("changed body reused original", e)
				}
				if err = view.Close(); err != nil {
					t.Fatal(err)
				}
				pump := nativeGraphTestPump{n: n, group: 0, host: n.hosts[0][0]}
				out, e := pump.Submit(t.Context(), allocationProposal{target: intent.source, wire: capture.command})
				actual, e2 := decodeRangeMoveStatus(out)
				if e != nil || e2 != nil || actual != original || rangeMoveAdmissionOwner(t, n.hosts[0][0]) != (raftlog.ApplicationRecoveryOwner{}) {
					t.Fatal("SAME consumed another phase", actual, e, e2)
				}
				return
			}
			gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: p.coordinator.transports[8], sequence: 3}
			p.coordinator.transports[8] = gate
			pending, err := runRangeMoveToCompletion(t, coordinator, intent)
			if !gate.stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) {
				t.Fatal("real partial import", err)
			}
			target := moveSpanRead(t, n, 1, pending.resume.move, false)
			progress, err := decodeRangeMoveProgress(target.observation.progress, moveSpanOutput(t))
			if err != nil || progress.undoDebt == 0 || progress.undoTotal != target.observation.status.sequence || !movePoolProgressWire(target.observation.progress) {
				t.Fatal("accepted prefix/debt", err)
			}
			assertMovePoolSnapshotAudit(t, n, 1)
			forged := bytes.Clone(target.observation.progress[:len(target.observation.progress)-sha256.Size])
			forged[3] = 5
			if _, err = decodeRangeMoveProgress(seal(forged), moveSpanOutput(t)); !errors.Is(err, errCorrupt) {
				t.Fatal("GMP5 substituted", err)
			}
			crossReopenAll(t, n)
			if mode == "commit" {
				gate.sequence = 0
				done := rangeMoveKeeperResume(t, coordinator, pending.resume)
				if !done.committed {
					t.Fatal("actual completion")
				}
				crossReopenAll(t, n)
				assertMovePoolSnapshotAudit(t, n, 1)
			} else {
				if s := rangeMoveAdmissionSubmit(t, n, 0, wire, rangeMoveAbort, 0, nil, nil); s.result != rangeMoveApplied {
					t.Fatal(s)
				}
				authority := moveSpanRead(t, n, 0, pending.resume.move, false)
				for seq := uint64(1); seq <= progress.undoTotal; seq++ {
					s := rangeMoveAdmissionSubmit(t, n, 1, wire, rangeMoveAbort, seq, nil, []rangeMoveAuthority{authority})
					if s.result != rangeMoveApplied {
						t.Fatal(s)
					}
					if seq == 1 {
						assertMovePoolSnapshotAudit(t, n, 1)
						crossReopenAll(t, n)
						authority = moveSpanRead(t, n, 0, pending.resume.move, false)
					}
				}
				if s := rangeMoveAdmissionSubmit(t, n, 1, wire, rangeMoveAbort, 0, nil, []rangeMoveAuthority{authority}); s.result != rangeMoveApplied || rangeMoveAdmissionOwner(t, n.hosts[1][0]) != (raftlog.ApplicationRecoveryOwner{}) {
					t.Fatal("terminal GMU0 rollback", s)
				}
				crossReopenAll(t, n)
			}
			for member := range 3 {
				v, close := crossView(t, n, member, [2]uint64{})
				assertCertifiedProjection(t, v, 1, true, nil)
				assertCertifiedProjection(t, v, 17, true, nil)
				assertCertifiedProjection(t, v, 999, false, nil)
				close()
			}
		})
	}
}

// The only dropped message is a reply AFTER real native submission completed.
type movePoolLostDescribeReply struct {
	graphPeerWireTransport
	command []byte
	target  bool
}

func (g *movePoolLostDescribeReply) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	r, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	if r.kind == sourceSubmit && len(g.command) == 0 {
		p, e := decodeGraphSourceProposal(r.command)
		if e != nil {
			return nil, e
		}
		if isRangeMoveWire(p.wire) {
			c, e := decodeRangeMoveCommand(p.wire)
			if e != nil {
				return nil, e
			}
			if !g.target && c.phase == rangeMoveDescribe && c.sequence == 0 || g.target && c.phase == rangeMoveImport && c.sequence > 0 {
				out, e := g.graphPeerWireTransport.RoundTrip(ctx, wire)
				if e != nil {
					return out, e
				}
				if g.target {
					reply, e := decodeGraphWireResponse(out)
					if e != nil {
						return nil, e
					}
					s, e := decodeRangeMoveStatus(reply.body)
					if e != nil {
						return nil, e
					}
					if s.result != rangeMoveBudgetLimit {
						return out, nil
					}
				}
				g.command = owned(p.wire)
				return nil, io.EOF
			}
		}
	}
	return g.graphPeerWireTransport.RoundTrip(ctx, wire)
}
func runMovePoolResume(t *testing.T, c *rangeMoveCoordinator, resume rangeMoveResume) (rangeMoveResult, error) {
	t.Helper()
	r, err := c.Resume(t.Context(), resume)
	for range 128 {
		if !errors.Is(err, errRangeMovePending) {
			return r, err
		}
		r, err = c.Resume(t.Context(), resume)
	}
	return r, err
}
func assertMovePoolSnapshotAudit(t *testing.T, n *allocationTestNetwork, group int) {
	t.Helper()
	h := n.hosts[group][0]
	receiver, prepared, manifest, c, _ := preparedDeclaredAuditFixture(t, h.machine.store, n.configs[group][2])
	if manifest.Version != 9 || manifest.Contract.Version != 7 {
		t.Fatal("fresh pool snapshot profile", manifest.Version)
	}
	a, err := newDeclaredSnapshotAuditor(receiver, h.machine.ns, n.d, h.machine.limits, h.machine.SemanticContractID())
	if err != nil {
		t.Fatal(err)
	}
	owner := beginDeclaredTestAudit(t, a, prepared, c)
	done := false
	for range 256 {
		done, err = owner.Advance(t.Context(), replica.ApplicationAuditBudget{WorkBytes: 64 << 20, OwnedBytes: 64 << 20})
		if err != nil {
			t.Fatal("bounded pool snapshot advance", err)
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("bounded pool audit did not reach EOF")
	}
	if err = owner.Finish(t.Context(), replica.ApplicationAuditBudget{WorkBytes: 64 << 20, OwnedBytes: 64 << 20}); err != nil {
		t.Fatal(err)
	}
}
