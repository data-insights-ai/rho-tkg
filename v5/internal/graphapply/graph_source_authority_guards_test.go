package graphapply

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// These are coverage guards against the frozen implementation, not original
// RED evidence. Native Hosts, read IDs and authority come from actual quorum
// completion; negative copies never acquire a new owner or a replacement seal.
type sourceAuthorityGuardState struct {
	checkpoint uint64
	image      []byte
	applied    uint64
	usage      raftlog.ApplicationUsage
}

func sourceAuthorityGuardCapture(t *testing.T, h *allocationHost) sourceAuthorityGuardState {
	t.Helper()
	index, image, err := h.machine.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	usage, err := h.machine.store.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	return sourceAuthorityGuardState{index, image, h.driver.Applied(), usage}
}

func sourceAuthorityGuardUnchanged(t *testing.T, h *allocationHost, before sourceAuthorityGuardState) {
	t.Helper()
	after := sourceAuthorityGuardCapture(t, h)
	if after.checkpoint != before.checkpoint || after.applied != before.applied || !bytes.Equal(after.image, before.image) || after.usage != before.usage {
		t.Fatal("authority refusal/read changed native state or leaked an owner", before, after)
	}
}

func sourceAuthorityGuardEmptyLookup(t *testing.T, p crossRequestProof) {
	t.Helper()
	if p.owner != nil || p.id != (allocationReadID{}) || p.question != (crossRequestQuestion{}) || p.scope != (allocationScope{}) || p.configuration != ([32]byte{}) || p.index != 0 || p.status != (crossStatus{}) || p.manifest != nil || p.present {
		t.Fatal("lookup error exported partial authority", p)
	}
}

func sourceAuthorityGuardEmptyExport(t *testing.T, p crossAuthority) {
	t.Helper()
	if p.scope != (allocationScope{}) || p.id != (allocationReadID{}) || p.question != (crossQuery{}) || p.index != 0 || p.status != (crossStatus{}) || p.manifest != nil || p.stamp != nil {
		t.Fatal("export refusal returned partial authority", p)
	}
}

func TestGraphSourceAuthorityCancelSuppressesAllThreeNativeReadFamilies(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	h := n.hosts[0][0]
	pump := &nativeGraphTestPump{n: n, group: 0, host: h}
	before := sourceAuthorityGuardCapture(t, h)
	question := crossRequestQuestion{tx: [16]byte{231}, nonce: [16]byte{232}}
	allocation, err := h.Read(allocationQuery{kind: observeConfiguration, nonce: [16]byte{233}})
	if err != nil {
		t.Fatal(err)
	}
	cross, err := h.ReadCrossPhase(question.tx, crossRegister, [16]byte{234})
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := h.readCrossRequest(question)
	if err != nil {
		t.Fatal(err)
	}
	// Do not drive packets until each genuine request has been cancelled.
	if len(h.pending) != 1 || len(h.crossPending) != 1 || len(h.requestPending) != 1 {
		t.Fatal("native reads did not occupy the three pending families")
	}
	for _, id := range []allocationReadID{allocation.readID, cross.readID, lookup.readID} {
		if id == (allocationReadID{}) {
			t.Fatal("native read omitted its issued ID")
		}
		if err := h.cancelSourceRead(id); err != nil {
			t.Fatal("cancel issued native read", err)
		}
		if err := h.cancelSourceRead(id); !errors.Is(err, errInvalid) {
			t.Fatal("cancel replay borrowed already released metadata", err)
		}
	}
	if len(h.pending)+len(h.crossPending)+len(h.requestPending) != 0 {
		t.Fatal("cancel retained Host request metadata")
	}
	wire := encodeCrossRequestContext(lookup.readID, question)
	reply, err := h.completeCrossRequestRead(wire, h.driver.Applied())
	if !errors.Is(err, errInvalid) || reply.err != nil {
		t.Fatal("cancelled lookup completion replay", err, reply.err)
	}
	sourceAuthorityGuardEmptyLookup(t, reply.proof)
	for _, event := range []allocationEvent{allocation, cross, lookup} {
		completed, err := pump.drive(t.Context(), event)
		if err != nil || len(completed.replies)+len(completed.crossReplies)+len(completed.requestReplies) != 0 {
			t.Fatal("late quorum delivery resurrected a cancelled proof", completed, err)
		}
	}
	if err := (*allocationHost)(nil).cancelSourceRead(allocationReadID{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil cancellation", err)
	}
	if err := h.cancelSourceRead(allocationReadID{session: h.session, sequence: h.sequence + 1}); !errors.Is(err, errInvalid) {
		t.Fatal("unissued cancellation", err)
	}
	// Driver quotas are released by the actual replies, not fabricated cleanup.
	healthy, err := pump.Request(t.Context(), crossRequestQuestion{tx: question.tx, nonce: [16]byte{235}})
	if err != nil || healthy.err != nil || healthy.proof.owner != h || healthy.proof.present || healthy.proof.status != (crossStatus{}) || len(healthy.proof.manifest) != 0 {
		t.Fatal("cancel stranded native driver or invented present lookup", healthy, err)
	}
	sourceAuthorityGuardUnchanged(t, h, before)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.cancelSourceRead(healthy.proof.id); !errors.Is(err, replica.ErrStopped) {
		t.Fatal("closed cancellation", err)
	}
}

func TestGraphSourceAuthorityLookupAdmissionQuotaAndFollowerRefuseWithoutProof(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	h := n.hosts[0][0]
	pump := &nativeGraphTestPump{n: n, group: 0, host: h}
	before := sourceAuthorityGuardCapture(t, h)
	question := crossRequestQuestion{tx: [16]byte{241}, nonce: [16]byte{242}}
	for _, tc := range []struct {
		host *allocationHost
		q    crossRequestQuestion
		want error
	}{
		{nil, question, errInvalid},
		{h, crossRequestQuestion{nonce: question.nonce}, errInvalid},
		{h, crossRequestQuestion{tx: question.tx}, errInvalid},
		{n.hosts[0][1], question, replica.ErrUnavailable},
	} {
		event, err := tc.host.readCrossRequest(tc.q)
		if !errors.Is(err, tc.want) || event.readID != (allocationReadID{}) || len(event.packets)+len(event.requestReplies) != 0 {
			t.Fatal("invalid/follower lookup admitted authority", event, err)
		}
		if tc.host != nil && len(tc.host.requestPending) != 0 {
			t.Fatal("driver admission failure left lookup metadata")
		}
	}
	// Fill the union cap with actual outstanding allocation reads. A lookup
	// must not reserve a seventeenth request in its separate family.
	events := make([]allocationEvent, 16)
	for i := range events {
		var err error
		events[i], err = h.Read(allocationQuery{kind: observeConfiguration, nonce: [16]byte{243, byte(i + 1)}})
		if err != nil || events[i].readID == (allocationReadID{}) {
			t.Fatal("real union-quota setup", i, err)
		}
	}
	issued := h.issuedReads.Load()
	event, err := h.readCrossRequest(question)
	if !errors.Is(err, errLimit) || event.readID != (allocationReadID{}) || len(event.packets)+len(event.requestReplies) != 0 || h.issuedReads.Load() != issued || len(h.pending) != 16 || len(h.requestPending) != 0 {
		t.Fatal("union quota refused after reserving work", event, err)
	}
	for _, event := range events {
		if err := h.cancelSourceRead(event.readID); err != nil {
			t.Fatal(err)
		}
	}
	for _, event := range events {
		completed, err := pump.drive(t.Context(), event)
		if err != nil || len(completed.replies)+len(completed.requestReplies) != 0 {
			t.Fatal("quota cancellation produced an authority", completed, err)
		}
	}
	reply, err := pump.Request(t.Context(), question)
	if err != nil || reply.err != nil || reply.proof.owner != h || reply.proof.question != question || reply.proof.present {
		t.Fatal("quota refusal prevented later actual absence read", reply, err)
	}
	sourceAuthorityGuardUnchanged(t, h, before)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if event, err := h.readCrossRequest(question); !errors.Is(err, replica.ErrStopped) || event.readID != (allocationReadID{}) || len(event.requestReplies) != 0 {
		t.Fatal("closed lookup", event, err)
	}
}

func TestGraphSourceAuthorityCompletionRejectsMalformedChangedAndReplayedContext(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	h := n.hosts[0][0]
	pump := &nativeGraphTestPump{n: n, group: 0, host: h}
	before := sourceAuthorityGuardCapture(t, h)
	question := crossRequestQuestion{tx: [16]byte{251}, nonce: [16]byte{252}}
	event, err := h.readCrossRequest(question)
	if err != nil {
		t.Fatal(err)
	}
	wire := encodeCrossRequestContext(event.readID, question)
	changed := bytes.Clone(wire)
	changed[len(changed)-1] ^= 1
	for _, tc := range []struct {
		wire []byte
		want error
	}{
		{nil, errCorrupt},
		{wire[:len(wire)-1], errCorrupt},
		{append([]byte("BAD1"), wire[4:]...), errCorrupt},
		{changed, errInvalid},
	} {
		reply, err := h.completeCrossRequestRead(tc.wire, h.driver.Applied())
		if !errors.Is(err, tc.want) || reply.err != nil {
			t.Fatal("bad native completion context", err, reply.err)
		}
		sourceAuthorityGuardEmptyLookup(t, reply.proof)
		if len(h.requestPending) != 1 {
			t.Fatal("refused completion consumed the genuine pending question")
		}
	}
	completed, err := pump.drive(t.Context(), event)
	if err != nil || len(completed.requestReplies) != 1 || completed.requestReplies[0].err != nil || completed.requestReplies[0].proof.owner != h || completed.requestReplies[0].proof.question != question || completed.requestReplies[0].proof.present {
		t.Fatal("genuine completed absence counterpart", completed, err)
	}
	replay, err := h.completeCrossRequestRead(wire, h.driver.Applied())
	if !errors.Is(err, errInvalid) || replay.err != nil {
		t.Fatal("successful native completion replay", err, replay.err)
	}
	sourceAuthorityGuardEmptyLookup(t, replay.proof)
	// A genuine issued completion with a future storage coordinate must fail
	// inside its reply, consume Host metadata, and never mint a partial proof.
	futureQuestion := crossRequestQuestion{tx: question.tx, nonce: [16]byte{253}}
	futureEvent, err := h.readCrossRequest(futureQuestion)
	if err != nil {
		t.Fatal(err)
	}
	futureWire := encodeCrossRequestContext(futureEvent.readID, futureQuestion)
	failed, err := h.completeCrossRequestRead(futureWire, h.driver.Applied()+1)
	if err != nil || !errors.Is(failed.err, raftlog.ErrInvalid) || len(h.requestPending) != 0 {
		t.Fatal("future completion coordinate admitted proof", err, failed.err)
	}
	sourceAuthorityGuardEmptyLookup(t, failed.proof)
	// Direct negative completion did not come from Driver delivery. Cancel the
	// still-genuine native request before driving its packets; do not fake ack.
	if err := h.driver.CancelReadIndex(futureWire); err != nil {
		t.Fatal(err)
	}
	late, err := pump.drive(t.Context(), futureEvent)
	if err != nil || len(late.requestReplies) != 0 {
		t.Fatal("failed future completion resurrected authority", late, err)
	}
	current, err := pump.Request(t.Context(), crossRequestQuestion{tx: question.tx, nonce: [16]byte{254}})
	if err != nil || current.err != nil || current.proof.owner != h || current.proof.present {
		t.Fatal("future refusal stranded later native completion", current, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	proof, err := h.machine.observeCrossRequest(ctx, question, h.driver.Applied())
	if !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled native observation", err)
	}
	sourceAuthorityGuardEmptyLookup(t, proof)
	proof, err = h.machine.observeCrossRequest(t.Context(), question, h.driver.Applied()+1)
	if !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("future native observation coordinate", err)
	}
	sourceAuthorityGuardEmptyLookup(t, proof)
	sourceAuthorityGuardUnchanged(t, h, before)
}

func TestGraphSourceAuthorityExportAndProposalRejectAlteredNativeEvidence(t *testing.T) {
	n, coordinator, ops, claims, _ := coordinatorNativeFixture(t)
	result, err := coordinator.Run(t.Context(), n.requestID(), ops, 37, claims)
	if err != nil || !result.decision.commit {
		t.Fatal("actual durable decision setup", result, err)
	}
	h := n.hosts[0][0]
	native := n.crossReadPhase(0, result.resume.tx, crossDecide)
	authority, err := h.exportCrossAuthority(native)
	if err != nil || !authority.validSeal() || authority.status != native.status {
		t.Fatal("actual native export counterpart", authority, err)
	}
	tx, err := h.RecoverCrossTransaction(native)
	if err != nil {
		t.Fatal(err)
	}
	before := sourceAuthorityGuardCapture(t, h)
	output, err := (*allocationHost)(nil).exportCrossAuthority(native)
	if !errors.Is(err, errInvalid) {
		t.Fatal("nil native export", err)
	}
	sourceAuthorityGuardEmptyExport(t, output)
	for _, mutate := range []func(*crossProof){
		func(p *crossProof) { p.owner = n.hosts[1][0] },
		func(p *crossProof) { p.index = 0 },
		func(p *crossProof) { p.index = h.driver.Applied() + 1 },
		func(p *crossProof) { p.id.sequence = h.issuedReads.Load() + 1 },
		func(p *crossProof) { p.manifest = bytes.Clone(p.manifest); p.manifest[0] ^= 1 },
	} {
		bad := native
		mutate(&bad)
		output, err := h.exportCrossAuthority(bad)
		if !errors.Is(err, errInvalid) {
			t.Fatal("altered native proof exported authority", err)
		}
		sourceAuthorityGuardEmptyExport(t, output)
	}
	arena := func(bytes int) *graphstate.OutputBudget {
		t.Helper()
		budget, err := graphstate.NewOutputBudget(bytes)
		if err != nil {
			t.Fatal(err)
		}
		return budget
	}
	assertRefusal := func(host *allocationHost, transaction crossTransaction, evidence []crossAuthority, budget *graphstate.OutputBudget, want error) {
		t.Helper()
		proposal, err := host.proposalFromAuthority(crossResolve, 8, transaction, evidence, budget)
		if !errors.Is(err, want) || proposal.target != (allocationScope{}) || proposal.wire != nil {
			t.Fatal("authority proposal refusal leaked a partial command", proposal, err)
		}
		sourceAuthorityGuardUnchanged(t, h, before)
	}
	assertRefusal(nil, tx, []crossAuthority{authority}, arena(32<<20), errInvalid)
	assertRefusal(h, tx, []crossAuthority{authority}, nil, errInvalid)
	foreign := tx
	foreign.owner = n.hosts[1][0]
	assertRefusal(h, foreign, []crossAuthority{authority}, arena(32<<20), errInvalid)
	changed := tx
	changed.digest[0] ^= 1
	assertRefusal(h, changed, []crossAuthority{authority}, arena(32<<20), errInvalid)
	assertRefusal(h, tx, []crossAuthority{authority, authority, authority, authority}, arena(32<<20), errLimit)
	assertRefusal(h, tx, []crossAuthority{authority}, arena(0), errLimit)
	for _, mutate := range []func(*crossAuthority){
		func(p *crossAuthority) { p.stamp = nil },
		func(p *crossAuthority) { p.scope.group[0] ^= 1 },
		func(p *crossAuthority) { p.scope.ownership++ },
		func(p *crossAuthority) { p.scope.topology++ },
		func(p *crossAuthority) { p.question.nonce[0] ^= 1 },
		func(p *crossAuthority) { p.manifest = bytes.Clone(p.manifest); p.manifest[0] ^= 1 },
	} {
		bad := authority
		mutate(&bad)
		assertRefusal(h, tx, []crossAuthority{bad}, arena(32<<20), errInvalid)
	}
	proposal, err := h.proposalFromAuthority(crossResolve, 8, tx, []crossAuthority{authority}, arena(32<<20))
	if err != nil || proposal.target != mustProcessScope(h, 8) {
		t.Fatal("genuine unchanged evidence was refused", err)
	}
	command, _, err := decodeCrossCommand(proposal.wire, h.machine.limits)
	if err != nil || command.phase != crossResolve || command.ns.partition != 8 || !bytes.Equal(command.manifest, tx.manifest) || len(command.proofs) != 1 || command.proofs[0] != native.status {
		t.Fatal("authority proposal changed actual immutable decision", command, err)
	}
	sourceAuthorityGuardUnchanged(t, h, before)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	output, err = h.exportCrossAuthority(native)
	if !errors.Is(err, replica.ErrStopped) {
		t.Fatal("closed native export", err)
	}
	sourceAuthorityGuardEmptyExport(t, output)
	if proposal, err := h.proposalFromAuthority(crossResolve, 8, tx, []crossAuthority{authority}, arena(32<<20)); !errors.Is(err, replica.ErrStopped) || proposal.target != (allocationScope{}) || proposal.wire != nil {
		t.Fatal("closed native proposal", proposal, err)
	}
}
