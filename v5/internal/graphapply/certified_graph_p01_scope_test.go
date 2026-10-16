package graphapply

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

// Holds an unchanged, genuine B decision reply after native ReadIndex/export.
// Decoding here records inert evidence; only the production endpoint seals it.
type p01DecisionReplyPause struct {
	graphPeerWireTransport
	entered          chan crossAuthorityBody
	release          chan struct{}
	certifiedSources int
}

func (p *p01DecisionReplyPause) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	request, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	if request.kind == sourceCertifiedOpen {
		p.certifiedSources++
	}
	reply, err := p.graphPeerWireTransport.RoundTrip(ctx, wire)
	if err != nil || request.kind != sourceCrossRead || request.phase != crossDecide {
		return reply, err
	}
	response, err := decodeGraphWireResponse(reply)
	if err != nil || response.err != nil {
		return reply, err
	}
	authority, err := decodeCrossAuthorityBody(response.body)
	if err != nil {
		return nil, err
	}
	select {
	case p.entered <- authority:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-p.release:
		return reply, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Six genuine native disk voters, not six OS processes. P01's missing
// conjunction is B's immutable COMMIT with A still prepared and DATA scope A.
func TestCertifiedGraphP01CoordinatorOutsideRequestedDataScope(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, mode := range []graphReadMode{graphReadFresh, graphReadAfter} {
		name := "Fresh"
		if mode == graphReadAfter {
			name = "After"
		}
		t.Run(name, func(t *testing.T) {
			n := newCertifiedGraphNetwork(t)
			edge, claims := crossEdgeFixture(t, n)
			c := remoteCertifiedFixture(t, n)
			open := func(request graphReadRequest) *certifiedGraphRead {
				t.Helper()
				read, err := c.OpenCertified(t.Context(), request, []uint64{3})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := read.Close(); err != nil {
						t.Error(err)
					}
				})
				return read
			}
			old := open(graphReadRequest{mode: graphReadFresh})
			oldCut, err := old.Cut()
			if err != nil || len(oldCut.certificates) != 1 || oldCut.certificates[0].scope.partition != 3 {
				t.Fatal("P01 old scope", oldCut, err)
			}
			// P01 needs both participants' effects, but A-only answers cannot
			// hydrate a cross-edge's B endpoint. E15/E18 cover that full scope.
			ops := []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: edge[0].Scope, Name: "p01-A"}, {Kind: graphstate.AddLabel, Owner: 17, Life: 18, Scope: edge[0].Scope, Name: "p01-B"}, {Kind: graphstate.Set, Owner: 1, Life: 2, Scope: edge[0].Scope, Name: "p", Value: graphstate.I64(9), ValueID: 5}}
			tx, register := crossPlanAt(n, 1, n.requestID(), ops, []freshBinding{{role: valueBinding, id: 5, grant: claims[0].grant}}, 24)
			registered := n.crossSubmit(1, register)
			registration := n.crossReadPhase(1, registered.tx, crossRegister)
			var votes []crossProof
			for group, partition := range []uint64{3, 8} {
				prepare, err := n.hosts[1][0].PrepareCrossParticipant(tx, partition, registration, votes)
				if err != nil {
					t.Fatal(err)
				}
				if vote := n.crossSubmit(group, prepare); !vote.yes {
					t.Fatal("P01 genuine YES", vote)
				}
				votes = append(votes, n.crossReadPhase(group, registered.tx, crossPrepare))
			}
			commit, err := n.hosts[1][0].DecideCrossTransaction(tx, votes)
			if err != nil {
				t.Fatal(err)
			}
			if status := n.crossSubmit(1, commit); !status.commit {
				t.Fatal("P01 durable B COMMIT", status)
			}
			decision := n.crossReadPhase(1, registered.tx, crossDecide)
			crossResolveAt(n, 1, decision, 1)
			// B's label is installed; all A voters still own only
			// their original private YES. A's high-water cannot prove closure.
			for _, h := range n.hosts[0] {
				status, manifest, err := h.machine.observeCrossTransaction(crossQuery{tx: registered.tx}, h.driver.Applied())
				if err != nil || status.phase != crossPrepare || !status.yes || status.digest != tx.digest || !bytes.Equal(manifest, tx.manifest) {
					t.Fatal("P01 A not original prepared participant", status, err)
				}
			}
			for group := range 2 {
				var originalChanges []byte
				for _, h := range n.hosts[group] {
					status, manifest, err := h.machine.observeCrossTransaction(crossQuery{tx: registered.tx}, h.driver.Applied())
					if err != nil || status.tx != registered.tx || status.digest != tx.digest || !bytes.Equal(manifest, tx.manifest) || group == 1 && (status.phase != crossResolve || !status.commit || status.round != decision.status.round) {
						t.Fatal("P01 partial installation identity", status, err)
					}
					changes, err := h.machine.store.ApplicationRecord(t.Context(), status.index, false, h.machine.store.ApplicationLimits().MaxChangeBytes)
					if err != nil || (len(changes) > 0) != (group == 1) {
						t.Fatal("P01 partial installation retained CDC", group, len(changes), err)
					}
					if originalChanges == nil {
						originalChanges = changes
					} else if !bytes.Equal(originalChanges, changes) {
						t.Fatal("P01 partial CDC diverged among voters", group)
					}
					if group == 1 {
						out := outcome{ns: h.machine.ns, kind: graphOperations, identity: status.tx, hash: status.digest, index: status.index, disposition: applied}
						cdc, err := decodeTypedChangeEnvelope(changes, out, h.machine.limits, true)
						if err != nil || len(cdc.entities)+len(cdc.lives)+len(cdc.values) != 0 || len(cdc.groups) != 1 || cdc.groups[0].Key.Owner != 17 || cdc.groups[0].Key.Name != "p01-B" || len(cdc.groups[0].Changes) != 1 || cdc.groups[0].Changes[0].After().Revision().ID() != decision.status.round {
							t.Fatal("P01 incomplete original B label CDC", err)
						}
					}
				}
			}
			native, _ := certifiedCoordinator(n)
			collected, err := native.read(t.Context(), n.hosts[0][0], 1, 0)
			if err != nil || collected.observation.pending != registered.tx || collected.observation.coordinator != 8 || collected.observation.floor >= decision.status.round {
				t.Fatal("P01 actual A collection missed outside authority", collected.observation, err)
			}
			partial, closePartial := crossView(t, n, 0, [2]uint64{})
			closePartialOnce := sync.OnceFunc(closePartial)
			defer closePartialOnce()
			assertCertifiedProjection(t, partial, 1, true, nil)
			assertCertifiedProjection(t, partial, 17, true, []string{"p01-B"})
			assertCertifiedIncident(t, partial, edge[0].Scope, 1, 2, nil)
			closePartialOnce()
			assertScalar := func(view graphstate.ReadView, current bool) {
				t.Helper()
				projection, err := graphstate.Project(t.Context(), view, 1, certifiedPoint(t), graphstate.Effective, defaultMaterializerLimits().graph.Planner)
				if err != nil || !projection.Exists || !projection.Active || (!current && len(projection.Properties) != 0) || (current && (len(projection.Properties) != 1 || projection.Properties[0].Name != "p" || projection.Properties[0].Cardinality != graphstate.ScalarCardinality)) {
					t.Fatal("P01 exact scoped scalar projection", projection, err)
				}
				if current {
					value, ok := projection.Properties[0].Scalar.Int64Value()
					if !ok || value != 9 {
						t.Fatal("P01 current scalar omitted original commit", value, ok)
					}
				}
				want := []graphstate.EntityID(nil)
				if current {
					want = []graphstate.EntityID{1}
				}
				assertCertifiedClaim(t, view, edge[0].Scope, 9, want)
				assertCertifiedClaim(t, view, edge[0].Scope, 999, nil)
			}
			receipt, err := graphReceiptFromDecision(decision)
			if err != nil {
				t.Fatal(err)
			}
			modeRequest := graphReadRequest{mode: mode}
			if mode == graphReadAfter {
				modeRequest.receipt = receipt
			}
			pause := &p01DecisionReplyPause{graphPeerWireTransport: c.transports[8], entered: make(chan crossAuthorityBody, 1), release: make(chan struct{})}
			c.transports[8] = pause
			beforeA := [3]uint64{n.hosts[0][0].driver.Applied(), n.hosts[0][1].driver.Applied(), n.hosts[0][2].driver.Applied()}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			release := sync.OnceFunc(func() { close(pause.release) })
			defer release()
			type answer struct {
				read *certifiedGraphRead
				err  error
			}
			done := make(chan answer, 1)
			go func() {
				read, err := c.OpenCertified(ctx, modeRequest, []uint64{3})
				done <- answer{read, err}
			}()
			drained := false
			defer func() {
				cancel()
				if !drained {
					result := <-done
					if result.read != nil {
						if err := result.read.Close(); err != nil {
							t.Error(err)
						}
					}
				}
			}()
			assertOriginal := func(authority crossAuthorityBody) {
				t.Helper()
				if authority.scope != n.hosts[1][0].machine.scope() || authority.status != decision.status || authority.question.tx != registered.tx || !bytes.Equal(authority.manifest, tx.manifest) {
					t.Fatal("P01 reply not original B authority", authority)
				}
			}
			select {
			case authority := <-pause.entered:
				assertOriginal(authority)
			case result := <-done:
				drained = true
				if result.read != nil {
					_ = result.read.Close()
				}
				t.Fatal("P01 read bypassed B authority", result.err)
			case <-ctx.Done():
				t.Fatal("P01 did not reach B decision lookup", ctx.Err())
			}
			select {
			case result := <-done:
				drained = true
				if result.read != nil {
					_ = result.read.Close()
				}
				t.Fatal("P01 published while B decision reply held", result.err)
			default:
			}
			if !slices.Equal(c.certified.stable.certificates, oldCut.certificates) {
				t.Fatal("P01 pending read replaced old Stable")
			}
			for member, h := range n.hosts[0] {
				if h.driver.Applied() != beforeA[member] {
					t.Fatal("P01 A published fence/certificate before required decision", member)
				}
			}
			// Real cancellation terminates the inaccessible authority path as
			// pending, never as success/absence. Retry keeps the original Tx.
			cancel()
			result := <-done
			drained = true
			if result.read != nil || !errors.Is(result.err, context.Canceled) || !errors.Is(result.err, errGraphCutPending) {
				t.Fatal("P01 unavailable reply did not preserve pending", result.err)
			}
			release()
			current := open(modeRequest)
			select {
			case authority := <-pause.entered:
				assertOriginal(authority)
			default:
				t.Fatal("P01 retry did not recover original B decision")
			}
			cut, err := current.Cut()
			if err != nil || len(cut.certificates) != 1 || cut.certificates[0].scope.partition != 3 || cut.certificates[0].round < decision.status.round || pause.certifiedSources != 0 {
				t.Fatal("P01 lost receipt or inflated data scope", cut, pause.certifiedSources, err)
			}
			assertCertifiedProjection(t, certifiedView(t, current), 1, true, []string{"p01-A"})
			assertScalar(certifiedView(t, current), true)
			assertCertifiedIncident(t, certifiedView(t, current), edge[0].Scope, 1, 2, nil)
			assertCertifiedProjection(t, certifiedView(t, current), 7, false, nil)
			actual := n.crossReadPhase(1, registered.tx, crossDecide)
			if actual.status != decision.status || !bytes.Equal(actual.manifest, tx.manifest) {
				t.Fatal("P01 recovery changed original decision")
			}
			for group := range 2 {
				for _, h := range n.hosts[group] {
					status, manifest, err := h.machine.observeCrossTransaction(crossQuery{tx: registered.tx}, h.driver.Applied())
					if err != nil || status.phase != crossResolve || !status.commit || status.tx != registered.tx || status.digest != tx.digest || status.round != decision.status.round || !bytes.Equal(manifest, tx.manifest) {
						t.Fatal("P01 original closure differs among voters", status, err)
					}
				}
			}
			assertOld := func(read *certifiedGraphRead) {
				t.Helper()
				cut, err := read.Cut()
				if err != nil || !slices.Equal(cut.certificates, oldCut.certificates) {
					t.Fatal("P01 old certificate identity changed", cut, err)
				}
				assertCertifiedProjection(t, certifiedView(t, read), 1, true, nil)
				assertScalar(certifiedView(t, read), false)
				assertCertifiedIncident(t, certifiedView(t, read), edge[0].Scope, 1, 2, nil)
				entity, err := certifiedView(t, read).Entity(t.Context(), 3)
				if err != nil || entity.Found {
					t.Fatal("P01 old At gained current relationship", entity, err)
				}
				assertCertifiedProjection(t, certifiedView(t, read), 7, false, nil)
			}
			assertOld(old)
			for _, read := range []*certifiedGraphRead{current, old} {
				if err := read.Close(); err != nil {
					t.Fatal(err)
				}
			}
			for group := range 2 {
				for _, h := range n.hosts[group] {
					usage, err := h.machine.store.ApplicationUsage()
					if err != nil || usage.Views != 0 || usage.ViewBytes != 0 {
						t.Fatal("P01 retained owners before reopen", usage, err)
					}
				}
			}
			for _, transport := range c.transports {
				if held, ok := transport.(*p01DecisionReplyPause); ok {
					transport = held.graphPeerWireTransport
				}
				if err := transport.(*nativeGraphTestTransport).service.sources.close(); err != nil {
					t.Fatal("P01 source service cleanup before reopen", err)
				}
			}
			crossReopenAll(t, n)
			c = remoteCertifiedFixture(t, n)
			assertOld(open(graphReadRequest{mode: graphReadAt, cut: oldCut}))
		})
	}
}
