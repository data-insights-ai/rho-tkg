package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Source-authored guards only, not executed transfer/capacity evidence.
func TestRangeMoveGraphBatchFormatAndNativeUndo(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	if rangeMoveGraphBatchSemanticContractID() == rangeMoveSpanSemanticContractID() || !rangeMoveSpans(rangeMoveGraphBatchSemanticContractID()) || !retainedOnlyFeedAgreement(rangeMoveGraphBatchSemanticContractID()) {
		t.Fatal("fresh semantic capability")
	}
	for _, tc := range []struct {
		batches, logical uint64
		want             bool
	}{{0, 0, true}, {1, 32, true}, {1, 33, false}, {2, 1, false}, {rangeMoveSpanMaxPages + 1, 1, false}} {
		if moveGraphCountValid(tc.batches, tc.logical) != tc.want {
			t.Fatal(tc)
		}
	}
	if moveBodyKindValid(rangeMoveSpanSemanticContractID(), 4) || moveBodyKindValid(rangeMoveGraphBatchSemanticContractID(), 1) || !moveBodyKindValid(rangeMoveGraphBatchSemanticContractID(), 4) {
		t.Fatal("body kind widened old format")
	}
	for _, abort := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "partial-undo-reopen"}[abort], func(t *testing.T) {
			network := collectedGraphNetworkWithRecovery(t, func(p *raftlog.ApplicationPolicy) {
				p.RetainedApplicationBytes = 512 << 20
				p.RetainedApplicationRecords = 262144
			}, rangeMoveGraphBatchSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 3})
			// Production batches never mix logical families: a multi-subpage native
			// batch needs several entity records, so group 0 holds three nodes.
			n, p, _, _ := retainedPublisherFromNetworkNodes(t, network, [][]graphstate.EntityID{{1, 5, 7}, {17}})
			h := n.hosts[0][0]
			if _, err := newRangeMoveGraphBatchMaterializer(h.machine.store, h.machine.ns, n.d, h.machine.limits); err != nil {
				t.Fatal("explicit fresh constructor", err)
			}

			intent, err := n.hosts[0][0].PrepareRangeMove(t.Context(), n.requestID(), 1, 8, 64, 32, 16<<10)
			if err != nil {
				t.Fatal(err)
			}
			coordinator, err := newRangeMoveCoordinatorOn(p.coordinator)
			if err != nil {
				t.Fatal(err)
			}
			gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: p.coordinator.transports[8], sequence: 3}
			p.coordinator.transports[8] = gate
			pending, err := runRangeMoveToCompletion(t, coordinator, intent)
			if !gate.stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) {
				t.Fatal("actual partial native batch", err)
			}
			source := moveSpanRead(t, n, 0, pending.resume.move, true)
			manifest, err := decodeRangeMoveProgress(source.observation.progress, moveSpanOutput(t))
			if err != nil || !bytes.HasPrefix(source.observation.progress, []byte("GMP\x05")) || !manifest.transferComplete() || manifest.graphPages >= manifest.cursor.Sequence {
				t.Fatal("logical sequence substituted for native batch count", err)
			}
			endpoints, err := p.coordinator.endpoints(newGraphRemoteBudget(n.hosts[0][0].machine.limits, moveSpanOutput(t)))
			if err != nil {
				t.Fatal(err)
			}
			frozen, err := endpoints[3].openMoveSource(t.Context(), pending.resume.move)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := frozen.moveFrozenPage(t.Context(), 1)
			if err = errors.Join(err, frozen.Close()); err != nil || len(raw) < 2 || raw[0] != 4 {
				t.Fatal("native frozen marker", err)
			}
			batch, err := graphstore.DecodeRangeMoveBatch(raw[1:], moveSpanOutput(t), intent.pageBytes)
			if err != nil || len(batch.Pages) < 2 {
				t.Fatal("not genuine multiple graph subpages", err)
			}
			// A recomputed outer seal with the old version still cannot borrow new
			// semantic authority or reinterpret the extra native batch counter.
			forged := bytes.Clone(source.observation.progress[:len(source.observation.progress)-sha256.Size])
			forged[3] = 4
			if _, err := decodeRangeMoveProgress(seal(forged), moveSpanOutput(t)); !errors.Is(err, errCorrupt) {
				t.Fatal("GMP4 relabel accepted", err)
			}
			target := moveSpanRead(t, n, 1, pending.resume.move, false)
			progress, err := decodeRangeMoveProgress(target.observation.progress, moveSpanOutput(t))
			if err != nil || progress.graphApplied != 1 || progress.imported.Sequence <= 1 {
				t.Fatal("actual installed graph batch", err)
			}
			if !abort {
				gate.sequence = 0
				done := rangeMoveKeeperResume(t, coordinator, pending.resume)
				if !done.committed || done.resume != pending.resume {
					t.Fatal("genuine completion changed identity")
				}
				crossReopenAll(t, n)
				for member := range 3 {
					v, close := crossView(t, n, member, [2]uint64{})
					assertCertifiedProjection(t, v, 1, true, nil)
					assertCertifiedProjection(t, v, 17, true, nil)
					assertCertifiedProjection(t, v, 999, false, nil)
					close()
				}
				moveSpanAudit(t, n, 1)
				return
			}
			wire, err := encodeRangeMoveIntent(intent)
			if err != nil {
				t.Fatal(err)
			}
			if s := rangeMoveAdmissionSubmit(t, n, 0, wire, rangeMoveAbort, 0, nil, nil); s.result != rangeMoveApplied {
				t.Fatal(s)
			}
			authority := moveSpanRead(t, n, 0, pending.resume.move, false)
			for seq := uint64(1); seq <= target.observation.status.sequence; seq++ {
				s := rangeMoveAdmissionSubmit(t, n, 1, wire, rangeMoveAbort, seq, nil, []rangeMoveAuthority{authority})
				if s.result != rangeMoveApplied || s.sequence != seq {
					t.Fatal("whole native batch undo", s)
				}
				if seq == 1 {
					crossReopenAll(t, n)
					authority = moveSpanRead(t, n, 0, pending.resume.move, false)
				}
			}
			if s := rangeMoveAdmissionSubmit(t, n, 1, wire, rangeMoveAbort, 0, nil, []rangeMoveAuthority{authority}); s.result != rangeMoveApplied || rangeMoveAdmissionOwner(t, n.hosts[1][0]) != (raftlog.ApplicationRecoveryOwner{}) {
				t.Fatal("actual final suffix retirement", s)
			}
			crossReopenAll(t, n)
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
