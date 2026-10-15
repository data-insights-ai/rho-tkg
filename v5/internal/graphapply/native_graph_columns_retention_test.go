package graphapply

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// These compact source-authored guards have not been executed. They use the
// actual six native voters/Driver and real durable derived-store directories;
// no copied root/manifest is accepted as native source or pin authority.
type nativeColumnRetentionWire struct {
	graphPeerWireTransport
	phase byte
	after bool
}

func (w *nativeColumnRetentionWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	request, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	selected := false
	if request.kind == sourceSubmit {
		proposal, err := decodeGraphSourceProposal(request.command)
		if err != nil {
			return nil, err
		}
		if isGraphPinWire(proposal.wire) {
			command, err := decodeGraphPinCommand(proposal.wire)
			if err != nil {
				return nil, err
			}
			selected = command.phase == w.phase
		}
	}
	if selected && !w.after {
		return nil, io.EOF
	}
	reply, err := w.graphPeerWireTransport.RoundTrip(ctx, wire)
	if selected && err == nil {
		return nil, io.EOF
	}
	return reply, err
}

func TestNativeColumnsDurableGenerationOwnsExactPinAcrossReopenAndRetirement(t *testing.T) {
	n := collectedGraphNetworkWithAgreement(t, nil, durableColumnGraphSemanticContractID())
	ops, claims := crossEdgeFixture(t, n)
	c := remoteCertifiedFixture(t, n)
	// This owner covers A only; both original logical keepers still authenticate
	// the full cut/pin. B's separate column prefix must reach its own genuine EOF.
	c = &crossProcessCoordinator{host: n.hosts[0][0], transports: c.transports, streams: c.streams}
	publisher, err := newRetainedPinPublisher(c)
	if err != nil {
		t.Fatal(err)
	}
	cut := func() graphCut {
		t.Helper()
		r, err := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
		if err != nil {
			t.Fatal(err)
		}
		value, err := r.Cut()
		err = errors.Join(err, r.Close())
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	set := func(value int64, id uint64) allocationProposal {
		t.Helper()
		p, err := n.hosts[0][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.Set, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "p", Value: graphstate.I64(value), ValueID: graphstate.ValueID(id)}}, id+40, []freshBinding{{role: valueBinding, id: id, grant: claims[0].grant}})
		if err != nil {
			t.Fatal(err)
		}
		n.submitPartitionGraph(0, p, reasonNone)
		return p
	}
	original := set(5, 5)
	early := cut()
	issued, err := publisher.Begin(t.Context(), n.requestID(), graphReadRequest{mode: graphReadAt, cut: early}, 20)
	if err != nil {
		t.Fatal(err)
	}
	if err := issued.Issue(t.Context(), publisher); err != nil {
		t.Fatal(err)
	}
	issueWire, err := issued.Original()
	if err != nil {
		t.Fatal(err)
	}
	issueManifest, err := decodeGraphPinManifest(issueWire)
	if err != nil {
		t.Fatal(err)
	}
	issue := pinCommandFor(issueManifest, issueWire, n.hosts[0][0].machine.scope(), pinIssue, pinTerminalAction(issueManifest, issueWire, pinIssue), 0)
	set(6, 6)
	h := n.hosts[0][0]
	plan, err := h.PrepareGraphRetention(t.Context(), n.requestID(), graphRetentionFloors{1, 1, 1, 1, 1, 1})
	if err != nil {
		t.Fatal(err)
	}
	event, err := h.Submit(plan)
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, event)
	initial := cut()
	n.submitPartitionGraph(0, original, reasonNone)
	retainedPinSubmit(t, n, issue) // actual GPO retry inside the later raw prefix
	event, err = h.Submit(plan)
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, event) // actual GGO retry
	set(7, 7)
	final := cut()
	policy := nativeColumnGuardPolicy()
	id := [16]byte{201}
	dir := filepath.Join(t.TempDir(), "durable-generation")
	store, source, err := createDurablePartitionNativeColumns(t.Context(), dir, vfs.Default, c, id, initial, final, policy, 20)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.retentionRecord(id)
	if err != nil {
		t.Fatal(err)
	}
	savedToken := bytes.Clone(r.Token)
	old, err := decodePartitionGraphCommand(original.wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	// Compare to the actual native original outcome, not the request index or a
	// guessed round. The minimum must reach the pre-initial business original.
	actual, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	raw, found, err := actual.Get(t.Context(), declaredRequestKey(h.machine.ns, graphOperations, old.request.identity()), graphOutcomeBytes)
	err = errors.Join(err, actual.Close())
	if err != nil || !found {
		t.Fatal(err)
	}
	outcome, err := decodePartitionGraphOutcome(raw.Value, h.machine.ns)
	if err != nil || r.Feed[0] != outcome.index || r.Feed[0] >= source.initial[0].sourceIndex {
		t.Fatal("earlier original was not pinned", r.Feed, outcome, err)
	}
	if err := store.Retire(t.Context(), id, publisher); !errors.Is(err, errLimit) {
		t.Fatal("live hydration owner retired", err)
	}
	manifest, err := store.publish(t.Context(), id, source)
	if err != nil || manifest.Rows != 1 {
		t.Fatal(manifest, err)
	}
	// The same local generation ID in another directory is independently
	// owned, even with identical cuts, Host/session and embedding policy.
	other, otherSource, err := createDurablePartitionNativeColumns(t.Context(), filepath.Join(t.TempDir(), "independent-store"), vfs.Default, c, id, initial, final, policy, 20)
	if err != nil {
		t.Fatal(err)
	}
	otherRecord, err := other.retentionRecord(id)
	if err != nil || bytes.Equal(otherRecord.Original, r.Original) || otherRecord.Generation.Pin == r.Generation.Pin {
		t.Fatal("directory-local ID shared native pin", err)
	}
	if err := otherSource.Close(); err != nil {
		t.Fatal(err)
	}
	if err := other.Retire(t.Context(), id, publisher); err != nil {
		t.Fatal(err)
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	tokenStillOwned, err := decodeRetainedPinToken(savedToken)
	if err != nil {
		t.Fatal(err)
	}
	stillOwned, err := publisher.Open(t.Context(), tokenStillOwned)
	if err != nil {
		t.Fatal("other store retirement revoked this owner", err)
	}
	if err := stillOwned.Close(); err != nil {
		t.Fatal(err)
	}
	set(8, 8)
	_ = cut()
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	// The token is unchanged after transient Close; physical collection may now
	// reclaim older roots but must preserve the exact prefix and earlier originals.
	p, err := h.PrepareGraphRetention(t.Context(), n.requestID(), graphRetentionFloors{source.initial[0].sourceIndex, source.initial[0].sourceIndex, r.Feed[0], r.Feed[0], source.initial[0].sourceIndex, source.initial[0].index})
	if err != nil {
		t.Fatal(err)
	}
	event, err = h.Submit(p)
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, event)
	result, found, err := h.GraphRetentionOriginal(t.Context(), p)
	if err != nil || !found {
		t.Fatal(err)
	}
	for _, voter := range n.hosts[0] {
		var progress raftlog.ApplicationRetentionProgress
		for page := 0; !progress.Complete && page < 256; page++ {
			progress, _, err = voter.machine.store.CollectApplicationRetention(t.Context(), result.id, raftlog.ReadBudget{Rows: 16, Bytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
		}
		if !progress.Complete {
			t.Fatal("bounded collection incomplete")
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, source, err = reopenDurablePartitionNativeColumns(t.Context(), dir, vfs.Default, c, id, policy)
	if err != nil {
		t.Fatal(err)
	}
	r, err = store.retentionRecord(id)
	if err != nil || !bytes.Equal(r.Token, savedToken) {
		t.Fatal("reopen replaced original token", err)
	}
	rows := 0
	if _, err := store.walk(t.Context(), id, source, final, nil, func(row nativeColumnRow) error {
		rows++
		if row.Component != byte(graphstate.ScalarProperty) || row.Owner != 1 || row.After.Value != 7 || row.Before.Value != 6 {
			return errCorrupt
		}
		return nil
	}); err != nil || rows != 1 {
		t.Fatal("exact retained change set", rows, err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Retire(t.Context(), id, publisher); err != nil {
		t.Fatal(err)
	}
	if err := store.Retire(t.Context(), id, publisher); err != nil {
		t.Fatal("terminal retry", err)
	}
	token, err := decodeRetainedPinToken(savedToken)
	if err != nil {
		t.Fatal(err)
	}
	if read, err := publisher.Open(t.Context(), token); read != nil || !errors.Is(err, errGraphPinReleased) {
		t.Fatal("transient close or retired pin classification", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if s, v, err := reopenDurablePartitionNativeColumns(t.Context(), dir, vfs.Default, c, id, policy); s != nil || v != nil || !errors.Is(err, errGraphPinReleased) {
		t.Fatal("retiring generation reopened", err)
	}
	if _, err := newDurableColumnGraphMaterializer(nil, h.machine.ns, n.d, h.machine.limits); !errors.Is(err, errInvalid) {
		t.Fatal("nil fresh constructor", err)
	}
	var nilStore *nativeColumnStore
	if err := nilStore.Retire(t.Context(), id, publisher); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
}

func TestNativeColumnIssuanceAndCancellationPreserveInterruptedCreation(t *testing.T) {
	n := collectedGraphNetworkWithAgreement(t, nil, durableColumnGraphSemanticContractID())
	crossEdgeFixture(t, n)
	c := remoteCertifiedFixture(t, n)
	c = &crossProcessCoordinator{host: n.hosts[0][0], transports: c.transports, streams: c.streams}
	publisher, err := newRetainedPinPublisher(c)
	if err != nil {
		t.Fatal(err)
	}
	read, err := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cut, err := read.Cut()
	err = errors.Join(err, read.Close())
	if err != nil {
		t.Fatal(err)
	}
	// Issue is durably installed, but neither participant has a Publish original.
	request, err := publisher.Begin(t.Context(), n.requestID(), graphReadRequest{mode: graphReadAt, cut: cut}, 4)
	if err != nil {
		t.Fatal(err)
	}
	original, err := request.Original()
	if err != nil {
		t.Fatal(err)
	}
	if err := request.Issue(t.Context(), publisher); err != nil {
		t.Fatal(err)
	}
	resumed, err := publisher.Resume(t.Context(), original)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resumed.Publish(t.Context()); err != nil {
		t.Fatal("all-Publish-absent issue did not recover", err)
	}
	if err := resumed.CancelIssue(t.Context()); err != nil {
		t.Fatal("publication-before-cancellation", err)
	}
	// Close then native cancellation fences every participant. A delayed actual
	// forwarded Publish must install Released, never acquire a new bank owner.
	pending, err := publisher.Begin(t.Context(), n.requestID(), graphReadRequest{mode: graphReadAt, cut: cut}, 4)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := pending.Original()
	if err != nil {
		t.Fatal(err)
	}
	m, err := decodeGraphPinManifest(wire)
	if err != nil {
		t.Fatal(err)
	}
	if err := pending.Issue(t.Context(), publisher); err != nil {
		t.Fatal(err)
	}
	malformed := pinCommandFor(m, wire, n.hosts[0][0].machine.scope(), pinCancelIssue, pinTerminalAction(m, wire, pinCancelIssue), 0)
	malformedWire, err := encodeGraphPinCommand(malformed)
	if err != nil {
		t.Fatal(err)
	}
	malformedWire[4+(allocationScopeBytes-36)+1] ^= 1 // changed issuer, canonical seal recomputed
	malformedWire = seal(malformedWire[:len(malformedWire)-32])
	beforeMalformed := n.hosts[0][0].driver.Applied()
	if _, err := n.hosts[0][0].Submit(allocationProposal{target: n.hosts[0][0].machine.scope(), wire: malformedWire}); !errors.Is(err, errInvalid) || n.hosts[0][0].driver.Applied() != beforeMalformed {
		t.Fatal("malformed absent cancellation had effects", err)
	}
	q, err := graphRetentionReader(n.hosts[0][0], t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := crossControl(q, graphPinCancelKey(q.ns, m.id()), 128); err != nil || found {
		t.Fatal("invalid tuple installed tombstone", err)
	}
	if _, found, err := readPinOwner(q, m.id()); err != nil || found {
		t.Fatal("invalid tuple installed owner", err)
	}
	if err := n.hosts[0][0].machine.checkRetainedPinRoster(q, false); err != nil {
		t.Fatal("invalid tuple changed native bank", err)
	}
	if err := q.view.Close(); err != nil {
		t.Fatal(err)
	}
	command := pinCommandFor(m, wire, n.hosts[0][0].machine.scope(), pinPublish, m.request, 0)
	proposal, err := n.hosts[0][1].retainedPinProposal(command)
	if err != nil {
		t.Fatal(err)
	}
	held, err := n.hosts[0][1].Submit(proposal)
	if err != nil || len(held.packets) != 1 {
		t.Fatal("genuine forwarded Publish", err)
	}
	if err := pending.CancelIssue(t.Context()); err != nil {
		t.Fatal(err)
	}
	event, err := n.hosts[0][0].Step(held.packets[0])
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, event)
	body, err := n.hosts[0][0].machine.store.ApplicationRecord(t.Context(), n.hosts[0][0].driver.Applied(), true, 256)
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeGraphPinResult(body)
	if err != nil || result.status != pinReleased || result.publication != 0 {
		t.Fatal("delayed publication escaped cancellation", result, err)
	}
	if _, err := publisher.Resume(t.Context(), nil); !errors.Is(err, errCorrupt) {
		t.Fatal("nil original", err)
	}
	// A literal earliest prepass journal has no issuance; ordinary authenticated
	// At reacquisition must resume it rather than demand a nonexistent pin.
	policy := nativeColumnGuardPolicy()
	source, err := newPartitionNativeColumnSource(t.Context(), c, cut, cut, policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.retainedBinding(); err != nil {
		t.Fatal(err)
	}
	seedRequest, err := publisher.Begin(t.Context(), n.requestID(), graphReadRequest{mode: graphReadAt, cut: cut}, 4)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := seedRequest.Original()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "interrupted-prepass")
	store, err := openNativeColumnStore(dir, vfs.Default, true, source)
	if err != nil {
		t.Fatal(err)
	}
	id := [16]byte{203}
	if _, err := store.prepareRetention(id, source, seed); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, source, err = reopenDurablePartitionNativeColumns(t.Context(), dir, vfs.Default, c, id, policy)
	if err != nil {
		t.Fatal("unissued creation could not resume", err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Retire(t.Context(), id, publisher); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// A valid derived-store quota declines before Issue or Publish effects;
	// neither mutable terminal state nor later token room borrows extra bytes.
	tiny := policy
	tiny.MaxRetainedBytes = 4 * nativeColumnMetadataBytes
	before := [2]uint64{n.hosts[0][0].driver.Applied(), n.hosts[1][0].driver.Applied()}
	if store, source, err := createDurablePartitionNativeColumns(t.Context(), filepath.Join(t.TempDir(), "small"), vfs.Default, c, [16]byte{204}, cut, cut, tiny, 4); store != nil || source != nil || !errors.Is(err, errLimit) {
		t.Fatal("unfunded owner journal admitted", err)
	}
	if before != ([2]uint64{n.hosts[0][0].driver.Applied(), n.hosts[1][0].driver.Applied()}) {
		t.Fatal("quota refusal installed native pin effects")
	}
	legacy, err := nativeColumnMetadata(nativeColumnBinding{Version: 1})
	if err != nil || bytes.Contains(legacy, []byte("TerminalAttempts")) {
		t.Fatal("legacy binding bytes expanded", err)
	}
	var nilRequest *retainedPinRequest
	if err := nilRequest.Issue(t.Context(), publisher); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err := nilRequest.CancelIssue(t.Context()); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	// Both true journal crash windows: no Publish installed, then only A
	// installed. Drop before B's actual Submit; the issue remains authoritative.
	for _, blockedGroup := range []int{0, 1} {
		transports := make(map[uint64]graphPeerWireTransport, 2)
		var blocked *nativeColumnRetentionWire
		for g, partition := range []uint64{3, 8} {
			wrapped := &nativeColumnRetentionWire{graphPeerWireTransport: c.transports[partition]}
			if g == blockedGroup {
				wrapped.phase = pinPublish
				blocked = wrapped
			}
			transports[partition] = wrapped
		}
		attempt, err := newCrossProcessCoordinator(n.hosts[0][0], transports)
		if err != nil {
			t.Fatal(err)
		}
		id := [16]byte{byte(210 + blockedGroup)}
		dir := filepath.Join(t.TempDir(), "before-publish")
		if store, source, err := createDurablePartitionNativeColumns(t.Context(), dir, vfs.Default, attempt, id, cut, cut, policy, 4); store != nil || source != nil || !errors.Is(err, errRetainedPinPending) {
			t.Fatal("interrupted native publication", err)
		}
		journal, err := openNativeColumnStoreBinding(dir, vfs.Default, false, nativeColumnBinding{Policy: policy}, [32]byte{})
		if err != nil {
			t.Fatal(err)
		}
		before, err := journal.retentionRecord(id)
		if err != nil || before.State != nativeColumnPinPending || len(before.Token) != 0 {
			t.Fatal("pending exact original not preserved", err)
		}
		if err := journal.Close(); err != nil {
			t.Fatal(err)
		}
		blocked.phase = 0
		store, source, err := reopenDurablePartitionNativeColumns(t.Context(), dir, vfs.Default, attempt, id, policy)
		if err != nil {
			t.Fatal("exact pending creation recovery", err)
		}
		after, err := store.retentionRecord(id)
		if err != nil || !bytes.Equal(before.Original, after.Original) || after.State != nativeColumnPinLive {
			t.Fatal("original replaced on recovery", err)
		}
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}
		// Actual installed cancellation replies become unknown. The finite
		// persisted terminal policy retires work rather than silently resetting.
		blocked.phase, blocked.after = pinCancelIssue, true
		retiringPublisher, err := newRetainedPinPublisher(attempt)
		if err != nil {
			t.Fatal(err)
		}
		for range store.binding.TerminalAttempts {
			if err := store.Retire(t.Context(), id, retiringPublisher); !errors.Is(err, errRetainedPinPending) {
				t.Fatal("unknown retirement not preserved", err)
			}
		}
		if err := store.Retire(t.Context(), id, retiringPublisher); !errors.Is(err, errLimit) {
			t.Fatal("exhausted terminal policy silently reset", err)
		}
		retiring, err := store.retentionRecord(id)
		if err != nil || retiring.State != nativeColumnRetiring || retiring.TerminalAttempts != store.binding.TerminalAttempts {
			t.Fatal("retirement journal lost", err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		blocked.phase = 0
		if err := retireDurablePartitionNativeColumns(t.Context(), dir, vfs.Default, attempt, id, policy); !errors.Is(err, errLimit) {
			t.Fatal("reopen reset spent terminal policy", err)
		}
	}

}
