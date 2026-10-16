package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func allocationP08Call(t *testing.T, p *graphProcess, r processRequest) processResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	out, err := p.exchange(ctx, r)
	err = errors.Join(err, processResponseError(out))
	if err != nil {
		t.Fatal("native P08 child operation", r.op, err, out.detail)
	}
	return out
}
func allocationP08Acquire(t *testing.T, n *certifiedProcessNetwork, actor, sequence uint64) (uint64, idalloc.GrantRequest) {
	t.Helper()
	start := allocationP08Call(t, n.all[actor], processRequest{op: processP08BeginAcquire, index: sequence, entity: 16})
	request, err := decodeAllocationP08Request(start.aux)
	if err != nil {
		t.Fatal("actual child acquire request", err)
	}
	n.group(actor).event(t, actor, processRequest{op: processP08ReadAcquire})
	result := allocationP08Call(t, n.all[actor], processRequest{op: processP08FinishAcquire, entity: 1})
	alias, err := decodeAllocationP08Result(result.aux)
	if err != nil || alias == 0 {
		t.Fatal("actual fresh cursor delivery", alias, err)
	}
	return alias, request
}
func allocationP08Mint(t *testing.T, n *certifiedProcessNetwork, actor, alias uint64, want uint64) {
	t.Helper()
	out := allocationP08Call(t, n.all[actor], processRequest{op: processP08Mint, index: alias})
	id, err := decodeAllocationP08Result(out.aux)
	if err != nil || id != want {
		t.Fatal("exact actual opaque issued ID", actor, id, want, err)
	}
}
func TestAllocationP08NativeSixProcessesActualCachedCursorIssuance(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedProcessNetworkSetup(t, true, true)
	// Setup installs grants but no graph entity, life or value. Only these actual
	// child-owned Acquire/Next operations issue the graph-qualified IDs below.
	a, ar := allocationP08Acquire(t, n, 3, 1)
	b, br := allocationP08Acquire(t, n, 4, 1)
	if ar.Graph != br.Graph || ar.Session.ID == br.Session.ID || ar.Session.Incarnation == br.Session.Incarnation {
		t.Fatal("distinct genuine recipient incarnations", ar, br)
	}
	for offset := uint64(0); offset < 16; offset++ {
		allocationP08Mint(t, n, 3, a, 1+offset)
		allocationP08Mint(t, n, 4, b, 17+offset)
	}
	for _, q := range []struct{ actor, alias uint64 }{{3, a}, {4, b}} {
		out := allocationP08Call(t, n.all[q.actor], processRequest{op: processP08Mint, index: q.alias})
		id, err := decodeAllocationP08Result(out.aux)
		if id != 0 || !errors.Is(err, idalloc.ErrExhausted) {
			t.Fatal("explicit cursor exhaustion did not refuse", id, err)
		}
	}
}
func TestAllocationP08PrivateWireSentinelsAndOwnedRequests(t *testing.T) {
	session := idalloc.RecipientSession{ID: [16]byte{1}, Incarnation: [16]byte{2}, Epoch: 3}
	request := idalloc.GrantRequest{Graph: idalloc.GraphID{4}, Session: session, Sequence: 5, Count: 16}
	wire, err := encodeAllocationP08Request(request)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeAllocationP08Request(wire)
	if err != nil || decoded != request {
		t.Fatal(decoded, err)
	}
	wire[4] ^= 1
	if decoded != request {
		t.Fatal("inert request retained caller bytes")
	}
	for _, cause := range []error{idalloc.ErrInvalid, idalloc.ErrStaleAuthority, idalloc.ErrAlreadyReserved, idalloc.ErrUnknown, idalloc.ErrExhausted, idalloc.ErrPayloadMismatch, idalloc.ErrExpired, idalloc.ErrUnreserved} {
		out := processResponse{incarnation: [16]byte{1}, sequence: 1, code: 6, causeMask: processCauseMask(errors.Join(cause, errLimit))}
		wire, err := encodeProcessResponse(out)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeProcessResponse(wire)
		if err != nil || !errors.Is(processResponseError(got), cause) || !errors.Is(processResponseError(got), errLimit) {
			t.Fatal("native sentinel chain lost across child representation", cause, got, err)
		}
	}
	for _, cause := range []error{nil, idalloc.ErrStaleAuthority, idalloc.ErrAlreadyReserved, idalloc.ErrUnknown, idalloc.ErrExhausted, idalloc.ErrPayloadMismatch, errInvalid} {
		id := uint64(0)
		if cause == nil {
			id = 9
		}
		wire, err := encodeAllocationP08Result(id, cause)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeAllocationP08Result(wire)
		if got != id || cause != nil && !errors.Is(err, cause) || cause == nil && err != nil {
			t.Fatal("private result cause", got, err)
		}
	}
	if _, err := decodeAllocationP08Request(bytes.Repeat([]byte{255}, 256)); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	var owners *allocationP08Owners
	if _, err := owners.begin(1, 16); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
}

// A partition and logical ticks expire the old leader lease; only a successful
// candidate-owned native ReadIndex establishes the new role. No wall-time sleep,
// imported proof, RawNode state mutation or application leadership-transfer stub.
func allocationP08Elect(t *testing.T, n *certifiedProcessNetwork, prior, next uint64) {
	allocationP08ElectFor(t, n, rangeMoveSemanticContractID(), prior, next)
}
func allocationP08ElectFor(t *testing.T, n *certifiedProcessNetwork, semantic raftlog.ApplicationSemanticContractID, prior, next uint64) {
	t.Helper()
	group := n.group(next)
	if group.blocked == nil {
		group.blocked = make(map[uint64]bool)
	}
	group.blocked[prior] = true
	defer delete(group.blocked, prior)
	spec := partitionCodecRequest(t, 1, defaultMaterializerLimits())
	ns, _ := certifiedProcessNamespace(next)
	part, _ := spec.declaration.Partition(ns.partition)
	binding := raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(ns.graph), Partition: ns.partition, Group: part.Group}, SemanticContractID: semantic}
	oldDrop := group.dropPacket
	group.dropPacket = func(packet replica.Packet) bool {
		raw, err := replica.DecodeApplicationPacket(binding, packet, processFrameBytes)
		if err != nil {
			t.Fatal("actual native election packet binding", err)
		}
		message := &pb.Message{}
		if err := (proto.UnmarshalOptions{RecursionLimit: 8}).Unmarshal(raw.Payload, message); err != nil {
			t.Fatal("actual bounded native election message", err)
		}
		// Drop only competing candidates' genuine requests. Their replies to the
		// chosen candidate and all application/replication messages remain real.
		return packet.From != next && (message.GetType() == pb.MsgVote || message.GetType() == pb.MsgPreVote)
	}
	defer func() { group.dropPacket = oldDrop }()
	for round := range 64 {
		for id := range group.nodes {
			if id != prior {
				group.event(t, id, processRequest{op: processTick})
			}
		}
		group.event(t, next, processRequest{op: processCampaign})
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		out, err := n.all[next].exchange(ctx, processRequest{op: processCertifiedRead, entity: uint64(observeConfiguration)})
		cancel()
		err = errors.Join(err, processResponseError(out))
		if errors.Is(err, replica.ErrUnavailable) {
			continue
		}
		if err != nil {
			t.Fatal("candidate native leadership read", next, err)
		}
		group.enqueue(t, out)
		group.drain(t)
		if !n.all[next].call(t, processRequest{op: processStateOp}).ready {
			t.Fatal("new leader lost matching native own ReadIndex")
		}
		delete(group.blocked, prior)
		group.event(t, next, processRequest{op: processTick})
		if next <= 3 {
			n.leaders[0] = next
		} else {
			n.leaders[1] = next
		}
		t.Logf("actual native leader elected%d after%d bounded tick/campaign rounds; prior%d partition healed", next, round+1, prior)
		return
	}
	t.Fatal("bounded native election did not produce authoritative own read", next)
}

func allocationP08Prepare(t *testing.T, n *certifiedProcessNetwork, actor, alias, kind uint64, data []byte) allocationProposal {
	t.Helper()
	out := allocationP08Call(t, n.all[actor], processRequest{op: processP08PrepareGraph, index: alias, entity: kind, data: data})
	p, err := decodeCertifiedProcessProposal(out.aux)
	if err != nil {
		t.Fatal("actual cursor-produced graph proposal", err)
	}
	return p
}
func allocationP08Words(words ...uint64) []byte {
	var out []byte
	for _, v := range words {
		out = binary.BigEndian.AppendUint64(out, v)
	}
	return out
}
func allocationP08Submit(t *testing.T, n *certifiedProcessNetwork, p allocationProposal, want reason) outcome {
	t.Helper()
	wire, err := encodeCertifiedProcessProposal(p)
	if err != nil {
		t.Fatal(err)
	}
	g := 0
	if p.target.partition == 8 {
		g = 1
	}
	leader := n.leaders[g]
	before := n.all[leader].call(t, processRequest{op: processStateOp})
	n.groups[g].event(t, leader, processRequest{op: processPropose, data: wire})
	after := n.all[leader].call(t, processRequest{op: processStateOp})
	record := n.all[leader].call(t, processRequest{op: processEntryRecord, index: after.applied})
	ns, _ := certifiedProcessNamespace(leader)
	o, err := decodePartitionGraphOutcome(record.outcome, ns)
	if err != nil || o.reason != want || o.hash != sha256.Sum256(p.wire) || o.disposition != applied {
		t.Fatal("actual native graph claim outcome", o, want, err)
	}
	if want != reasonNone && (!bytes.Equal(before.image, after.image) || len(record.changes) != 0) {
		t.Fatal("refused claim changed graph root/CDC")
	}
	if want == reasonNone && len(record.changes) == 0 {
		t.Fatal("positive claim silently did not create graph data")
	}
	for id, voter := range n.groups[g].nodes {
		actual := voter.call(t, processRequest{op: processEntryRecord, index: after.applied})
		state := voter.call(t, processRequest{op: processStateOp})
		if !bytes.Equal(actual.outcome, record.outcome) || !bytes.Equal(actual.changes, record.changes) || !bytes.Equal(state.image, after.image) {
			t.Fatal("actual voter graph outcome/CDC/root differs", id)
		}
	}
	return o
}
func allocationP08GraphSetup(t *testing.T, n *certifiedProcessNetwork) (uint64, uint64) {
	t.Helper()
	for _, q := range []struct{ actor, id uint64 }{{3, 1}, {4, 17}} {
		out := n.all[q.actor].call(t, processRequest{op: processCertifiedObserve, entity: q.id})
		fact, err := decodeCertifiedProcessFact(out.aux)
		if err != nil || fact.Exists {
			t.Fatal("P08 graph identity existed before actual cursor issuance", q, fact, err)
		}
	}
	a, _ := allocationP08Acquire(t, n, 3, 1)
	b, _ := allocationP08Acquire(t, n, 4, 1)
	for _, q := range []struct{ actor, alias, first uint64 }{{3, a, 1}, {4, b, 17}} {
		first := allocationP08Prepare(t, n, q.actor, q.alias, 1, nil)
		if c, err := decodePartitionGraphCommand(first.wire, defaultMaterializerLimits()); err != nil || uint64(c.request.operations[0].Owner) != q.first || uint64(c.request.operations[0].Life) != q.first+1 {
			t.Fatal("real first minted node/life", err)
		}
		if q.actor == 4 {
			n.p08Original = first
		}
		allocationP08Submit(t, n, first, reasonNone)
		allocationP08Submit(t, n, allocationP08Prepare(t, n, q.actor, q.alias, 1, nil), reasonNone)
		allocationP08Submit(t, n, allocationP08Prepare(t, n, q.actor, q.alias, 2, allocationP08Words(q.first, q.first+2, q.first+1, q.first+3)), reasonNone)
	}
	allocationP08Submit(t, n, allocationP08Prepare(t, n, 3, a, 3, allocationP08Words(1, 2, 9)), reasonNone)
	// A remains onPID3, while actual consensus/service leadership moves toPID1.
	allocationP08Elect(t, n, 3, 1)
	n.read(t, 1, observeAllocator, 0)
	n.action(t, 1, certifiedP08Transfer, 0, nil)
	return a, b
}
func TestAllocationP08NativeSixProcessesGraphClaimsSurviveServiceOnlyRotation(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedProcessNetworkSetup(t, true, true)
	a, _ := allocationP08GraphSetup(t, n)
	allocationP08Submit(t, n, allocationP08Prepare(t, n, 3, a, 1, nil), reasonNone)
	handle, cut := graphE17Open(t, n, 5, graphReadFresh, graphCut{})
	for _, q := range []struct{ id, life uint64 }{{1, 2}, {3, 4}, {5, 6}, {8, 9}, {17, 18}, {19, 20}, {21, 22}} {
		input, err := encodeGraphProcessObserveIDs([]uint64{q.id})
		if err != nil {
			t.Fatal(err)
		}
		out := n.all[5].call(t, processRequest{op: processSourceCertifiedObserve, index: handle, data: input})
		facts, err := decodeGraphProcessObservationBatch(out.aux)
		if err != nil || len(facts) != 1 || !facts[0].fact.Exists || !facts[0].fact.Active || facts[0].fact.Life != q.life {
			t.Fatal("exact issued native graph identity/life", q, facts, err)
		}
	}
	out := n.all[5].call(t, processRequest{op: processCertifiedIndexObserve, index: handle, data: graphE18IndexInput(1, 9)})
	rows, err := decodeGraphE18Rows(out.aux)
	if err != nil || len(rows) != 1 || rows[0].owner != 1 || rows[0].ref != 7 || rows[0].value != 9 || rows[0].provenance != 41 || rows[0].revision == 0 {
		t.Fatal("actual issued typed value/ref/revision", rows, err)
	}
	n.all[5].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
	for _, certificate := range cut.certificates {
		leader := uint64(1)
		if certificate.scope.partition == 8 {
			leader = 4
		}
		for id, voter := range n.group(leader).nodes {
			evidence, err := decodeGraphE15CutEvidence(voter.call(t, processRequest{op: processSourceCutEvidence, entity: certificate.round}).aux)
			actual, de := decodeGraphCertificate(evidence.certificate)
			if err != nil || de != nil || actual != certificate {
				t.Fatal("issued graph cut differs on native voter", id, err, de)
			}
		}
	}
}

type allocationP08Observed struct {
	index, last uint64
	allocator   idalloc.State
	grant       protocolGrant
	found       bool
}

func allocationP08Observe(t *testing.T, n *certifiedProcessNetwork, actor uint64, request *idalloc.GrantRequest) allocationP08Observed {
	t.Helper()
	var wire []byte
	var err error
	if request != nil {
		wire, err = encodeAllocationP08Request(*request)
		if err != nil {
			t.Fatal(err)
		}
	}
	out := allocationP08Call(t, n.all[actor], processRequest{op: processP08Metadata, data: wire})
	c := graphCursor{b: out.aux}
	o := allocationP08Observed{index: c.u64(), last: c.u64()}
	state, grant := c.field(idalloc.CheckpointSize), c.field(publishedGrantBytes)
	if c.err != nil || len(c.b) != 0 {
		t.Fatal("native metadata framing", c.err)
	}
	if len(state) != 0 {
		o.allocator, err = idalloc.DecodeCheckpoint(state)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(grant) != 0 {
		ns, _ := certifiedProcessNamespace(actor)
		o.grant, err = decodeProtocolGrant(grant, ns)
		o.found = true
		if err != nil {
			t.Fatal(err)
		}
	}
	return o
}
func allocationP08Reserve(t *testing.T, n *certifiedProcessNetwork, actor uint64, request idalloc.GrantRequest) allocationProposal {
	t.Helper()
	n.read(t, actor, observeAllocator, 0)
	wire, err := encodeAllocationP08Request(request)
	if err != nil {
		t.Fatal(err)
	}
	response := allocationP08Call(t, n.all[actor], processRequest{op: processCertifiedAction, entity: certifiedP08Reserve, data: wire})
	proposal, err := decodeCertifiedProcessProposal(response.aux)
	if err != nil {
		t.Fatal(err)
	}
	return proposal
}
func allocationP08SubmitControl(t *testing.T, n *certifiedProcessNetwork, proposal allocationProposal) outcome {
	t.Helper()
	wire, err := encodeCertifiedProcessProposal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	group := 0
	if proposal.target.partition == 8 {
		group = 1
	}
	leader := n.leaders[group]
	n.groups[group].event(t, leader, processRequest{op: processPropose, data: wire})
	state := n.all[leader].call(t, processRequest{op: processStateOp})
	record := n.all[leader].call(t, processRequest{op: processEntryRecord, index: state.applied})
	ns, _ := certifiedProcessNamespace(leader)
	o, err := decodeDeclaredOutcome(record.outcome, ns)
	if err != nil || o.hash != sha256.Sum256(proposal.wire) {
		t.Fatal("actual native control original", o, err)
	}
	for id, p := range n.groups[group].nodes {
		got := p.call(t, processRequest{op: processEntryRecord, index: state.applied})
		if !bytes.Equal(got.outcome, record.outcome) || !bytes.Equal(got.changes, record.changes) {
			t.Fatal("actual control original/CDC differs on voter", id)
		}
	}
	return o
}
func allocationP08Install(t *testing.T, n *certifiedProcessNetwork, source uint64, request idalloc.GrantRequest) allocationProposal {
	return allocationP08InstallFor(t, n, source, 8, request)
}
func allocationP08InstallFor(t *testing.T, n *certifiedProcessNetwork, source, target uint64, request idalloc.GrantRequest) allocationProposal {
	t.Helper()
	wire, err := encodeAllocationP08Request(request)
	if err != nil {
		t.Fatal(err)
	}
	n.group(source).event(t, source, processRequest{op: processP08ReadGrant, data: wire})
	response := allocationP08Call(t, n.all[source], processRequest{op: processCertifiedAction, entity: certifiedInstallGrant, index: target})
	p, err := decodeCertifiedProcessProposal(response.aux)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func allocationP08Replace(t *testing.T, n *certifiedProcessNetwork, id uint64) {
	t.Helper()
	prior := n.all[id]
	hub := prior.mux.hub
	graphE17KillTargetFollower(t, prior)
	replacement := startGraphProcess(t, filepath.Join(n.dir, fmt.Sprint(id)), id, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_RANGE_PROCESS=1", "RHO_GRAPH_ALLOCATION_PROCESS=1", "RHO_GRAPH_PROCESS_REOPEN=1")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := replacement.shutdown(ctx, false); err != nil {
			t.Error("P08 replacement graceful cleanup", id, err)
		}
	})
	mux := &graphProcessMux{p: replacement, id: id, hub: hub, pending: make(map[uint64]chan graphProcessAnswer), relays: make(map[uint64]context.CancelFunc), relayQueue: make(chan graphProcessRelayJob, processSourceWorkers)}
	hub.mu.Lock()
	if hub.inFlight != 0 || hub.packetBytes != 0 || len(hub.packets) != 0 || hub.peers[id] != prior.mux {
		hub.mu.Unlock()
		t.Fatal("replacing native process outside drained boundary")
	}
	hub.peers[id] = mux
	hub.mu.Unlock()
	replacement.mux = mux
	n.all[id] = replacement
	n.group(id).nodes[id] = replacement
	n.retired = append(n.retired, prior)
	hub.wg.Go(mux.readLoop)
	for range processSourceWorkers {
		hub.wg.Go(mux.relayLoop)
	}
	t.Logf("actual P08 same-directory replacement: voter%d oldPID%d newPID%d; old incarnation reaped", id, prior.cmd.Process.Pid, replacement.cmd.Process.Pid)
}
func TestAllocationP08NativeSixProcessesBeforeQuorumReservationRemainsUnknownUntilOverwrite(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedProcessNetworkSetup(t, true, true)
	a, _ := allocationP08GraphSetup(t, n)
	started := allocationP08Call(t, n.all[4], processRequest{op: processP08BeginAcquire, index: 2, entity: 16})
	request, err := decodeAllocationP08Request(started.aux)
	if err != nil {
		t.Fatal(err)
	}
	proposal := allocationP08Reserve(t, n, 1, request)
	before := allocationP08Observe(t, n, 1, nil)
	high, err := idalloc.Inspect(&before.allocator)
	if err != nil || high.HighWater != 32 {
		t.Fatal(high, err)
	}
	wire, err := encodeCertifiedProcessProposal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	uncommitted := allocationP08Call(t, n.all[1], processRequest{op: processPropose, data: wire})
	// No returned packets are delivered. The real source has persisted its local
	// proposal but no majority can acknowledge/apply that reservation yet.
	after := allocationP08Observe(t, n, 1, nil)
	if after.index != before.index || after.last <= before.last || uncommitted.applied != before.index {
		t.Fatal("before-quorum boundary not reached", before, after, uncommitted.applied)
	}
	graphE17CrashDrain(t, n)
	allocationP08Replace(t, n, 1)
	n.groups[0].blocked = map[uint64]bool{1: true}
	allocationP08Elect(t, n, 1, 2)
	// Only this completed own ReadIndex after the new-term no-op establishes the
	// overwritten tail/high-water. A kill alone never means definitely unreserved.
	n.read(t, 2, observeAllocator, 0)
	current := allocationP08Observe(t, n, 2, &request)
	view, err := idalloc.Inspect(&current.allocator)
	if err != nil || view.HighWater != 32 || current.found || current.index <= before.index {
		t.Fatal("quorum overwrite did not preserve exact high-water/absence", current, view, err)
	}
	lost := allocationP08Call(t, n.all[4], processRequest{op: processP08FinishAcquire})
	id, err := decodeAllocationP08Result(lost.aux)
	if id != 0 || !errors.Is(err, idalloc.ErrUnknown) {
		t.Fatal("uncertain caller created cursor", id, err)
	}
	again := allocationP08Call(t, n.all[4], processRequest{op: processP08BeginAcquire, index: 2, entity: 16})
	id, err = decodeAllocationP08Result(again.aux)
	if id != 0 || !errors.Is(err, idalloc.ErrAlreadyReserved) {
		t.Fatal("burned local attempt was reused", id, err)
	}
	n.read(t, 2, observeAllocator, 0)
	n.action(t, 2, certifiedP08Transfer, 0, nil)
	if o := allocationP08SubmitControl(t, n, proposal); o.reason != reasonStale {
		t.Fatal("old queued allocation authority admitted fresh reservation", o)
	}
	// Cached A issuance remains independent of the service1 death/new service2.
	allocationP08Submit(t, n, allocationP08Prepare(t, n, 3, a, 1, nil), reasonNone)
}

func allocationP08ReplaceRecipient(t *testing.T, n *certifiedProcessNetwork) {
	t.Helper()
	source := n.leaders[0]
	n.read(t, 4, observeRecipient, 2)
	n.action(t, 4, certifiedBeginRecipient, 2, nil)
	n.read(t, source, observeRecipient, 2)
	n.action(t, source, certifiedRecipientTransition, 3, []byte{byte(fenceDeclaredRecipient)})
	n.action(t, source, certifiedRecipientTransition, 8, []byte{byte(fenceDeclaredRecipient)})
	n.read(t, 4, observeRecipient, 2)
	n.action(t, 4, certifiedRecipientTransition, 3, []byte{byte(acknowledgeDeclaredHome)})
	n.read(t, source, observeRecipient, 2)
	n.action(t, source, certifiedRecipientTransition, 3, []byte{byte(activateDeclaredRecipient)})
	n.read(t, source, observeRecipient, 2)
	n.action(t, source, certifiedRecipientTransition, 8, []byte{byte(publishDeclaredRecipient)})
	n.read(t, 4, observeRecipient, 2)
	n.action(t, 4, certifiedOpenRecipient, 0, nil)
}
func allocationP08AcquireReserved(t *testing.T, n *certifiedProcessNetwork, actor, sequence, source uint64) (uint64, idalloc.GrantRequest) {
	t.Helper()
	start := allocationP08Call(t, n.all[actor], processRequest{op: processP08BeginAcquire, index: sequence, entity: 16})
	request, err := decodeAllocationP08Request(start.aux)
	if err != nil {
		t.Fatal(err)
	}
	proposal := allocationP08Reserve(t, n, source, request)
	if o := allocationP08SubmitControl(t, n, proposal); o.reason != reasonNone || o.disposition != applied {
		t.Fatal("new actual reservation", o)
	}
	if o := allocationP08SubmitControl(t, n, allocationP08Install(t, n, source, request)); o.reason != reasonNone {
		t.Fatal(o)
	}
	n.group(actor).event(t, actor, processRequest{op: processP08ReadAcquire})
	out := allocationP08Call(t, n.all[actor], processRequest{op: processP08FinishAcquire, entity: 1})
	alias, err := decodeAllocationP08Result(out.aux)
	if err != nil || alias == 0 {
		t.Fatal(alias, err)
	}
	return alias, request
}
func TestAllocationP08NativeSixProcessesCommittedUnknownGrantRestartAndDelayedAcknowledgement(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedProcessNetworkSetup(t, true, true)
	a, b := allocationP08GraphSetup(t, n)
	oldHandle, oldCut := graphE17Open(t, n, 5, graphReadFresh, graphCut{})
	oldProperty := n.all[5].call(t, processRequest{op: processCertifiedIndexObserve, index: oldHandle, data: graphE18IndexInput(1, 9)}).aux
	n.all[5].call(t, processRequest{op: processSourceCertifiedClose, index: oldHandle})
	started := allocationP08Call(t, n.all[4], processRequest{op: processP08BeginAcquire, index: 2, entity: 16})
	request, err := decodeAllocationP08Request(started.aux)
	if err != nil {
		t.Fatal(err)
	}
	reserve := allocationP08Reserve(t, n, 1, request)
	original := allocationP08SubmitControl(t, n, reserve)
	if original.reason != reasonNone {
		t.Fatal(original)
	}
	install := allocationP08Install(t, n, 1, request)
	installed := allocationP08SubmitControl(t, n, install)
	if installed.reason != reasonNone {
		t.Fatal(installed)
	}
	sourceGrant := allocationP08Observe(t, n, 1, &request)
	if !sourceGrant.found || sourceGrant.grant.grant.Reservation.First != 33 || sourceGrant.grant.grant.Reservation.Last != 48 || sourceGrant.grant.source.index != original.index {
		t.Fatal("actual committed range/original coordinate", sourceGrant)
	}
	n.group(4).event(t, 4, processRequest{op: processP08ReadAcquire}) // Actual matching HOME reply is held inside child4.
	allocationP08ReplaceRecipient(t, n)
	late := allocationP08Call(t, n.all[4], processRequest{op: processP08FinishAcquire, entity: 1})
	id, err := decodeAllocationP08Result(late.aux)
	if id != 0 || !errors.Is(err, idalloc.ErrStaleAuthority) {
		t.Fatal("old matching acknowledgement created issuance after real recipient fence", id, err)
	}
	graphE17CrashDrain(t, n)
	before4 := n.all[4].call(t, processRequest{op: processStateOp})
	allocationP08Replace(t, n, 4)
	after4 := n.all[4].call(t, processRequest{op: processStateOp})
	if after4.incarnation == before4.incarnation || !bytes.Equal(before4.image, after4.image) || after4.applied != before4.applied {
		t.Fatal("recipient same-directory native Restore changed durable graph")
	}
	allocationP08Elect(t, n, 5, 4)
	n.read(t, 4, observeConfiguration, 0)
	staleCursor := allocationP08Call(t, n.all[4], processRequest{op: processP08Mint, index: b})
	id, err = decodeAllocationP08Result(staleCursor.aux)
	if id != 0 || !errors.Is(err, errInvalid) {
		t.Fatal("recipient restored volatile cursor", id, err)
	}
	n.read(t, 1, observeAllocator, 0)
	before1 := allocationP08Observe(t, n, 1, &request)
	state, err := idalloc.Inspect(&before1.allocator)
	if err != nil || state.HighWater != 48 {
		t.Fatal(state, err)
	}
	// Kill after observed quorum application, before any cursor for[33,48] escaped.
	graphE17CrashDrain(t, n)
	allocationP08Replace(t, n, 1)
	allocationP08Elect(t, n, 2, 1)
	n.read(t, 1, observeAllocator, 0)
	restored := allocationP08Observe(t, n, 1, &request)
	state, err = idalloc.Inspect(&restored.allocator)
	if err != nil || state.HighWater != 48 || restored.grant != before1.grant {
		t.Fatal("allocator restart lost high-water/original unknown block", state, restored, err)
	}
	n.action(t, 1, certifiedP08Transfer, 0, nil)
	allocationP08ReplaceRecipient(t, n)
	fresh, fr := allocationP08AcquireReserved(t, n, 4, 1, 1)
	grant := allocationP08Observe(t, n, 1, &fr)
	if !grant.found || grant.grant.grant.Reservation.First != 49 || grant.grant.grant.Reservation.Last != 64 {
		t.Fatal("reserved unknown/remainders were recycled", grant)
	}
	allocationP08Submit(t, n, allocationP08Prepare(t, n, 3, a, 1, nil), reasonNone) // Surviving cached A remains valid.
	allocationP08Submit(t, n, allocationP08Prepare(t, n, 4, fresh, 1, nil), reasonNone)
	at, actualCut := graphE17Open(t, n, 2, graphReadAt, oldCut)
	got := n.all[2].call(t, processRequest{op: processCertifiedIndexObserve, index: at, data: graphE18IndexInput(1, 9)}).aux
	if !bytes.Equal(got, oldProperty) || !reflect.DeepEqual(actualCut, oldCut) {
		t.Fatal("reservation/recipient/service restart lost old typed graph history")
	}
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: at})
	// Original committed reserve/install may recover, but no new control/graph
	// change or fresh cursor is authorized by their old delivery coordinates.
	if replay := allocationP08SubmitControl(t, n, reserve); replay.index != original.index || replay.disposition != requestReplay {
		t.Fatal("old exact reserve lost original outcome", replay, original)
	}
	if replay := allocationP08SubmitControl(t, n, install); replay.index != installed.index || replay.disposition != requestReplay {
		t.Fatal("old exact install lost original outcome", replay, installed)
	}
}

func TestAllocationP08NativeSixProcessesImmediateAfterQuorumReservationBurnsUnknownBlock(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedProcessNetworkSetup(t, true, true)
	a, _ := allocationP08GraphSetup(t, n)
	started := allocationP08Call(t, n.all[4], processRequest{op: processP08BeginAcquire, index: 2, entity: 16})
	request, err := decodeAllocationP08Request(started.aux)
	if err != nil {
		t.Fatal(err)
	}
	reserve := allocationP08Reserve(t, n, 1, request)
	original := allocationP08SubmitControl(t, n, reserve)
	if original.reason != reasonNone {
		t.Fatal(original)
	}
	// No InstallGrant, matching HOME read or cursor delivery occurs between the
	// actual quorum-applied source reservation and this immediate source SIGKILL.
	graphE17CrashDrain(t, n)
	allocationP08Replace(t, n, 1)
	lost := allocationP08Call(t, n.all[4], processRequest{op: processP08FinishAcquire})
	id, err := decodeAllocationP08Result(lost.aux)
	if id != 0 || !errors.Is(err, idalloc.ErrUnknown) {
		t.Fatal("uncertain grant created a cursor", id, err)
	}
	allocationP08Elect(t, n, 2, 1)
	n.read(t, 1, observeAllocator, 0)
	restored := allocationP08Observe(t, n, 1, &request)
	view, err := idalloc.Inspect(&restored.allocator)
	if err != nil || view.HighWater != 48 || !restored.found || restored.grant.source.index != original.index || restored.grant.grant.Reservation.First != 33 || restored.grant.grant.Reservation.Last != 48 {
		t.Fatal("quorum reservation lost across actual source restart", restored, view, err)
	}
	n.action(t, 1, certifiedP08Transfer, 0, nil)
	retry := allocationP08Call(t, n.all[4], processRequest{op: processP08BeginAcquire, index: 2, entity: 16})
	id, err = decodeAllocationP08Result(retry.aux)
	if id != 0 || !errors.Is(err, idalloc.ErrAlreadyReserved) {
		t.Fatal("recovered unknown reservation reopened issuance", id, err)
	}
	allocationP08ReplaceRecipient(t, n)
	fresh, request := allocationP08AcquireReserved(t, n, 4, 1, 1)
	freshGrant := allocationP08Observe(t, n, 1, &request)
	if freshGrant.grant.grant.Reservation.First != 49 {
		t.Fatal("unknown reserved IDs recycled", freshGrant)
	}
	allocationP08Submit(t, n, allocationP08Prepare(t, n, 4, fresh, 1, nil), reasonNone)
	allocationP08Submit(t, n, allocationP08Prepare(t, n, 3, a, 1, nil), reasonNone)
}

func TestAllocationP08NativeSixProcessesRetiredRecipientNewClaimsRefuseAndCurrentClaimsApply(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedProcessNetworkSetup(t, true, true)
	_, old := allocationP08GraphSetup(t, n)
	allocationP08ReplaceRecipient(t, n)
	allocationP08GraphReplay(t, n, n.p08Original)
	current, request := allocationP08AcquireReserved(t, n, 4, 1, 1)
	grant := allocationP08Observe(t, n, 1, &request)
	if grant.grant.grant.Reservation.First != 33 {
		t.Fatal("new recipient did not advance existing high-water", grant)
	}
	// Every old ID is genuinely emitted by the still-live retired cached cursor.
	// The non-fencing graph preconditions match a successful current-grant control.
	allocationP08Submit(t, n, allocationP08Prepare(t, n, 4, old, 1, nil), reasonStale)
	allocationP08Submit(t, n, allocationP08Prepare(t, n, 4, current, 1, nil), reasonNone)
	endpoints := allocationP08Words(17, 19, 18, 20)
	allocationP08Submit(t, n, allocationP08Prepare(t, n, 4, old, 2, endpoints), reasonStale)
	allocationP08Submit(t, n, allocationP08Prepare(t, n, 4, current, 2, endpoints), reasonNone)
	// Value13 is new typed identity, rather than a canonical-reuse no-op that
	// would introduce no Delta.Values and accidentally avoid value admission.
	oldValue := allocationP08Prepare(t, n, 4, old, 3, allocationP08Words(17, 18, 13))
	allocationP08Cross(t, n, 4, oldValue, false)
	freshValue := allocationP08Prepare(t, n, 4, current, 3, allocationP08Words(17, 18, 13))
	allocationP08Cross(t, n, 4, freshValue, true)
	closeReply := allocationP08Call(t, n.all[4], processRequest{op: processCertifiedAction, entity: certifiedP08Close, index: 17, data: allocationP08Words(18)})
	closeProposal, err := decodeCertifiedProcessProposal(closeReply.aux)
	if err != nil {
		t.Fatal(err)
	}
	allocationP08Submit(t, n, closeProposal, reasonNone)
	allocationP08Submit(t, n, allocationP08Prepare(t, n, 4, old, 4, allocationP08Words(17)), reasonStale)
	allocationP08Submit(t, n, allocationP08Prepare(t, n, 4, current, 4, allocationP08Words(17)), reasonNone)
	for id, voter := range n.groups[1].nodes {
		fact, err := decodeCertifiedProcessFact(voter.call(t, processRequest{op: processCertifiedObserve, entity: 17}).aux)
		if err != nil || !fact.Exists || !fact.Active || fact.Life != 38 {
			t.Fatal("current reopened owner must use exactly minted Life38", id, fact, err)
		}
		absent := allocationP08Call(t, voter, processRequest{op: processP08Metadata, data: allocationP08Words(17, 28, 27)})
		present := allocationP08Call(t, voter, processRequest{op: processP08Metadata, data: allocationP08Words(17, 38, 37)})
		if !bytes.Equal(absent.aux, []byte{0}) || !bytes.Equal(present.aux, []byte{3}) {
			t.Fatal("retired Life28/Value27 leaked or current Life38/Value37 missing", id, absent.aux, present.aux)
		}
	}
	for _, q := range []struct {
		id      uint64
		present bool
	}{{23, false}, {25, false}, {33, true}, {35, true}} {
		out := n.all[4].call(t, processRequest{op: processCertifiedObserve, entity: q.id})
		fact, err := decodeCertifiedProcessFact(out.aux)
		if err != nil || fact.Exists != q.present {
			t.Fatal("exact retired/current entity/relationship set", q, fact, err)
		}
	}
	handle, _ := graphE17Open(t, n, 5, graphReadFresh, graphCut{})
	defer n.all[5].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
	out := n.all[5].call(t, processRequest{op: processCertifiedIndexObserve, index: handle, data: graphE18IndexInput(1, 13)})
	rows, err := decodeGraphE18Rows(out.aux)
	if err != nil || len(rows) != 1 || rows[0].owner != 17 || rows[0].ref != 37 || rows[0].provenance != 41 {
		t.Fatal("new typed value did not reach real current admission", rows, err)
	}
}
func allocationP08Cross(t *testing.T, n *certifiedProcessNetwork, actor uint64, p allocationProposal, commit bool) crossProcessResult {
	t.Helper()
	var before [2][]byte
	for g, group := range n.groups {
		before[g] = group.nodes[n.leaders[g]].call(t, processRequest{op: processStateOp}).image
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	out, err := n.all[actor].exchange(ctx, processRequest{op: processSourceCoordinate, data: p.wire})
	result, de := decodeGraphProcessCoordinatorResult(out)
	if err != nil || de != nil || result.decision.phase != crossDecide || result.decision.commit != commit {
		t.Fatal("actual native value claim cross decision", result, err, de)
	}
	if err := drainGraphProcessMuxes(ctx, n.all); err != nil {
		t.Fatal(err)
	}
	if !commit {
		for g, group := range n.groups {
			for id, voter := range group.nodes {
				state := voter.call(t, processRequest{op: processStateOp})
				record := voter.call(t, processRequest{op: processEntryRecord, index: state.applied})
				status, err := decodeCrossStatus(record.outcome)
				if err != nil || status.phase != crossResolve || status.commit || len(record.changes) != 0 || !bytes.Equal(before[g], state.image) {
					t.Fatal("retired value claim emitted graph effect/CDC", id, status, err)
				}
			}
		}
	}
	return result
}

func allocationP08GraphReplay(t *testing.T, n *certifiedProcessNetwork, p allocationProposal) {
	t.Helper()
	_, err := decodePartitionGraphCommand(p.wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	leader := n.leaders[1]
	before := n.all[leader].call(t, processRequest{op: processStateOp})
	wire, err := encodeCertifiedProcessProposal(p)
	if err != nil {
		t.Fatal(err)
	}
	n.groups[1].event(t, leader, processRequest{op: processPropose, data: wire})
	after := n.all[leader].call(t, processRequest{op: processStateOp})
	record := n.all[leader].call(t, processRequest{op: processEntryRecord, index: after.applied})
	ns, _ := certifiedProcessNamespace(leader)
	result, err := decodePartitionGraphOutcome(record.outcome, ns)
	if err != nil || result.disposition != requestReplay || result.reason != reasonNone || result.index >= after.applied || result.hash != sha256.Sum256(p.wire) || !bytes.Equal(before.image, after.image) || len(record.changes) != 0 {
		t.Fatal("exact already-committed graph request did not recover before current recipient fences", result, err)
	}
	original := n.all[leader].call(t, processRequest{op: processEntryRecord, index: result.index})
	old, err := decodePartitionGraphOutcome(original.outcome, ns)
	if err != nil || old.index != result.index || old.identity != result.identity || old.hash != result.hash || len(original.changes) == 0 {
		t.Fatal("original graph result/index lost", old, err)
	}
	for id, voter := range n.groups[1].nodes {
		r := voter.call(t, processRequest{op: processEntryRecord, index: after.applied})
		o := voter.call(t, processRequest{op: processEntryRecord, index: result.index})
		if !bytes.Equal(r.outcome, record.outcome) || len(r.changes) != 0 || !bytes.Equal(o.outcome, original.outcome) || !bytes.Equal(o.changes, original.changes) {
			t.Fatal("exact original replay/CDC differs across voters", id)
		}
	}
	// This is an ordinary changed caller payload under the original request key,
	// produced by the Host factory; no completed proof/authority is rewritten.
	// The private IPC helper below invokes the Host factory with that SAME actual
	// original key and otherwise-valid graph arguments, changing provenance only.
	request := processRequest{op: processP08ChangedGraph, data: wire}
	out := allocationP08Call(t, n.all[4], request)
	changed, err := decodeCertifiedProcessProposal(out.aux)
	if err != nil {
		t.Fatal(err)
	}
	allocationP08Submit(t, n, changed, reasonMismatch)
}
