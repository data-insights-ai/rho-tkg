package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"google.golang.org/protobuf/proto"
)

type processSnapshotPacket struct {
	packet replica.Packet
	owner  uint64
}

// This first package4 acceptance deliberately requires an end-to-end cross-edge.
// The existing local door correctly refuses it; that observed business refusal
// is missing orchestration evidence, never a safety bug in the local API.
type certifiedProcessNetwork struct {
	all                                           map[uint64]*graphProcess
	groups                                        [2]*tinyGraphTransport
	dir                                           string
	rangeMoves                                    bool
	retired                                       []*graphProcess
	e17MovePages, e17PublicPhases, e17VoterPhases int
	e17PublicUsed, e17VoterUsed                   int
}

func newCertifiedProcessNetwork(t *testing.T) *certifiedProcessNetwork {
	t.Helper()
	return newCertifiedProcessNetworkProfile(t, false)
}

func newCertifiedProcessNetworkProfile(t *testing.T, rangeMoves bool) *certifiedProcessNetwork {
	t.Helper()
	dir, err := os.MkdirTemp(os.Getenv("RHO_GRAPH_PROCESS_ARTIFACT"), "rho-certified-process-")
	if err != nil {
		t.Fatal(err)
	}
	n := &certifiedProcessNetwork{all: make(map[uint64]*graphProcess), dir: dir, rangeMoves: rangeMoves}
	if rangeMoves {
		n.e17MovePages, n.e17PublicPhases, n.e17VoterPhases = graphE17AdmittedMovePages, 5, 2
	}
	t.Log("certified process durable directories", dir)
	for id := uint64(1); id <= 6; id++ {
		env := []string{"RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1"}
		if rangeMoves {
			env = append(env, "RHO_GRAPH_RANGE_PROCESS=1")
		}
		n.all[id] = startGraphProcess(t, filepath.Join(dir, fmt.Sprint(id)), id, env...)
	}
	attachGraphProcessMuxes(t, n.all)
	n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: n.all[1], 2: n.all[2], 3: n.all[3]}}
	n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: n.all[4], 5: n.all[5], 6: n.all[6]}}
	// This cleanup also runs after the intentional acceptance RED, and reports
	// failures independently instead of relying on failure-suppressed fallback.
	t.Cleanup(func() {
		var calls uint64
		reaped, successful := 0, 0
		for id := uint64(1); id <= 6; id++ {
			p := n.all[id]
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := p.shutdown(ctx, false)
			cancel()
			p.mux.mu.Lock()
			calls += p.sequence
			p.mux.mu.Unlock()
			if err != nil {
				t.Error("certified child cleanup", id, err)
			}
			if p.cmd.ProcessState != nil {
				reaped++
				if p.cmd.ProcessState.Success() {
					successful++
				}
			}
			if p.cmd.ProcessState == nil || !p.cmd.ProcessState.Success() {
				t.Error("certified child not reaped successfully", id)
			}
			if err := p.cmd.Process.Signal(syscall.Signal(0)); !errors.Is(err, os.ErrProcessDone) {
				t.Error("certified child still live", id, err)
			}
			if _, err := p.stdin.Write([]byte{1}); !errors.Is(err, os.ErrClosed) {
				t.Error("certified stdin leak", id, err)
			}
			if _, err := p.stdout.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
				t.Error("certified stdout leak", id, err)
			}
		}
		for _, retired := range n.retired {
			retired.mux.mu.Lock()
			calls += retired.sequence
			retired.mux.mu.Unlock()
			if !retired.closed || retired.cmd.ProcessState == nil {
				t.Error("retired certified child was not reaped")
			}
		}
		packets := n.groups[0].delivered + n.groups[1].delivered
		allowance := uint64(processDeliveries)
		if n.rangeMoves {
			allowance = graphE17FixtureDeliveries(n.e17MovePages, n.e17PublicPhases, n.e17VoterPhases)
			t.Logf("E17 counted phase ledger: maximum=%d pages=%d public=%d/%d nativeVoter=%d/%d", allowance, n.e17MovePages, n.e17PublicUsed, n.e17PublicPhases, n.e17VoterUsed, n.e17VoterPhases)
		}
		if calls+uint64(packets) > allowance {
			t.Error("certified aggregate RPC/packet bound", calls, packets)
		}
		t.Logf("certified cleanup: reaped=%d successfully-reaped=%d of6; calls=%d packets=%d maxFrame=%d/%d maxQueueBytes=%d/%d", reaped, successful, calls, packets, n.all[1].maxFrame, n.all[4].maxFrame, n.groups[0].maxQueueBytes, n.groups[1].maxQueueBytes)
	})
	for group, leader := range []uint64{1, 4} {
		n.groups[group].event(t, leader, processRequest{op: processCampaign})
	}
	n.action(t, 1, certifiedInitialize, 3, nil)
	n.read(t, 1, observeConfiguration, 0)
	n.action(t, 1, certifiedInitialize, 8, nil)
	for _, recipient := range []struct{ actor, id, partition uint64 }{{1, 1, 3}, {4, 2, 8}} {
		n.read(t, recipient.actor, observeConfiguration, 0)
		n.action(t, recipient.actor, certifiedBeginRecipient, recipient.id, nil)
		n.read(t, 1, observeRecipient, recipient.id)
		n.action(t, 1, certifiedRecipientTransition, 3, []byte{byte(fenceDeclaredRecipient)})
		if recipient.partition != 3 {
			n.action(t, 1, certifiedRecipientTransition, recipient.partition, []byte{byte(fenceDeclaredRecipient)})
		}
		n.read(t, recipient.actor, observeRecipient, recipient.id)
		n.action(t, recipient.actor, certifiedRecipientTransition, 3, []byte{byte(acknowledgeDeclaredHome)})
		n.read(t, 1, observeRecipient, recipient.id)
		n.action(t, 1, certifiedRecipientTransition, 3, []byte{byte(activateDeclaredRecipient)})
		n.read(t, 1, observeRecipient, recipient.id)
		n.action(t, 1, certifiedRecipientTransition, recipient.partition, []byte{byte(publishDeclaredRecipient)})
		n.read(t, recipient.actor, observeRecipient, recipient.id)
		n.action(t, recipient.actor, certifiedOpenRecipient, 0, nil)
		grants := [][2]uint64{{1, 16}}
		if rangeMoves && recipient.partition == 3 {
			grants = [][2]uint64{{1, 1}, {2, 15}}
		}
		for _, grant := range grants {
			n.read(t, 1, observeRecipient, recipient.id)
			n.read(t, 1, observeAllocator, 0)
			var input []byte
			if rangeMoves {
				input = graphE17GrantInput(grant[0], grant[1])
			}
			n.action(t, 1, certifiedReserve, 0, input)
			n.group(1).event(t, 1, processRequest{op: processCertifiedRead, entity: uint64(observeGrant), data: input})
			if !n.all[1].call(t, processRequest{op: processStateOp}).ready {
				t.Fatal("actual grant read did not complete", grant)
			}
			n.action(t, 1, certifiedInstallGrant, recipient.partition, nil)
		}
	}
	n.action(t, 1, certifiedCreateNode, 1, nil)
	n.action(t, 4, certifiedCreateNode, 17, nil)
	return n
}
func (n *certifiedProcessNetwork) group(actor uint64) *tinyGraphTransport {
	if actor <= 3 {
		return n.groups[0]
	}
	return n.groups[1]
}
func (n *certifiedProcessNetwork) read(t *testing.T, actor uint64, kind allocationObservationKind, recipient uint64) {
	t.Helper()
	n.group(actor).event(t, actor, processRequest{op: processCertifiedRead, entity: uint64(kind), index: recipient})
	r := n.all[actor].call(t, processRequest{op: processStateOp})
	if !r.ready {
		t.Fatal("actual Host request-bound quorum read did not complete", actor, kind)
	}
}
func (n *certifiedProcessNetwork) action(t *testing.T, actor, kind, index uint64, data []byte) (outcome, processResponse) {
	t.Helper()
	r := n.all[actor].call(t, processRequest{op: processCertifiedAction, entity: kind, index: index, data: data})
	if kind == certifiedOpenRecipient {
		if len(r.aux) != 0 {
			t.Fatal("recipient open returned proposal")
		}
		return outcome{}, r
	}
	proposal, err := decodeCertifiedProcessProposal(r.aux)
	if err != nil {
		t.Fatal("not a bounded source-produced proposal", err)
	}
	group, leader := 0, uint64(1)
	if proposal.target.partition == 8 {
		group, leader = 1, 4
	}
	ns, _ := certifiedProcessNamespace(leader)
	agreement := certifiedGraphSemanticContractID()
	if n.rangeMoves {
		agreement = rangeMoveSemanticContractID()
	}
	if proposal.target.graph != (idalloc.GraphID{1}) || proposal.target.partition != ns.partition || proposal.target.semantic != agreement {
		t.Fatal("source proposal escaped declared process topology", proposal.target)
	}
	n.groups[group].event(t, leader, processRequest{op: processPropose, data: r.aux})
	state := n.all[leader].call(t, processRequest{op: processStateOp})
	result := n.all[leader].call(t, processRequest{op: processEntryRecord, index: state.applied})
	var o outcome
	if kind == certifiedCreateNode || kind == certifiedCreateCrossEdge || kind == certifiedHealthyLabel {
		o, err = decodePartitionGraphOutcome(result.outcome, ns)
	} else {
		o, err = decodeDeclaredOutcome(result.outcome, ns)
	}
	validDisposition := o.disposition == applied || kind == certifiedInstallGrant && o.disposition == controlRecovery
	if err != nil || o.hash != sha256.Sum256(proposal.wire) || o.index != state.applied || !validDisposition {
		t.Fatal("actual committed operation outcome", o, err)
	}
	for id, p := range n.groups[group].nodes {
		actual := p.call(t, processRequest{op: processStateOp})
		if actual.applied != state.applied || !bytes.Equal(actual.image, state.image) {
			t.Fatal("certified process voter checkpoint mismatch", id)
		}
		got := p.call(t, processRequest{op: processEntryRecord, index: state.applied})
		if !bytes.Equal(got.outcome, result.outcome) || !bytes.Equal(got.changes, result.changes) {
			t.Fatal("certified process voter outcome/CDC mismatch", id)
		}
		if actual.stats[statViews] != 0 || actual.stats[statPinnedBytes] != 0 {
			t.Fatal("certified process operation leaked views/pins", id)
		}
	}
	if kind != certifiedCreateCrossEdge && o.reason != reasonNone {
		t.Fatal("certified process local positive control refused", kind, o)
	}
	t.Logf("actual certified child operation: actor=%d target=%d kind=%d index=%d reason=%d outcome=%x changes=%x", actor, proposal.target.partition, kind, o.index, o.reason, result.outcome, result.changes)
	return o, result
}
func (n *certifiedProcessNetwork) compare(t *testing.T, oracle *certifiedProcessOracle) {
	t.Helper()
	for group, ids := range [][]uint64{{1, 3, 7}, {17, 23}} {
		input, err := encodeGraphProcessObserveIDs(ids)
		if err != nil {
			t.Fatal(err)
		}
		for id, p := range n.groups[group].nodes {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			r, err := p.exchange(ctx, processRequest{op: processSourceObserve, data: input})
			cancel()
			if err != nil {
				stderr, _ := p.stderr.state()
				t.Fatalf("actual batched source oracle caller%d: %v limit=%v unavailable=%v stderr=%s", id, err, errors.Is(err, errLimit), errors.Is(err, graphstore.ErrSourceUnavailable), stderr)
			}
			tuples, err := decodeGraphProcessObservationBatch(r.aux)
			if err != nil || len(tuples) != len(ids) {
				t.Fatal("exact batch tuple count", id, err)
			}
			for i, tuple := range tuples {
				if tuple.id != ids[i] {
					t.Fatal("batch omitted/substituted entity", id, i, tuple.id)
				}
				if err := oracle.compare(tuple.id, tuple.fact); err != nil {
					t.Fatal(id, err)
				}
			}
			if r.stats[statViews] != 0 || r.stats[statPinnedBytes] != 0 {
				t.Fatal("batched oracle retained actual owners", id, r.stats)
			}
		}
	}
}

func TestReplicatedCertifiedGraphSixProcessesLocalControls(t *testing.T) {
	n := newCertifiedProcessNetwork(t)
	o := newCertifiedProcessOracle()
	n.compare(t, o)
	n.action(t, 1, certifiedHealthyLabel, 1, nil)
	n.action(t, 4, certifiedHealthyLabel, 17, nil)
	o.healthy()
	n.compare(t, o)
}
func TestReplicatedCertifiedGraphSixProcessesCrossEdge(t *testing.T) {
	n := newCertifiedProcessNetwork(t)
	oracle := newCertifiedProcessOracle()
	n.compare(t, oracle)
	// The native local factory produces the original qualified business command,
	// which enters the genuine remote coordinator before any local submission.
	command := n.all[1].call(t, processRequest{op: processCertifiedAction, entity: certifiedCreateCrossEdge, index: 3})
	proposal, err := decodeCertifiedProcessProposal(command.aux)
	if err != nil {
		t.Fatal(err)
	}
	response := n.all[1].call(t, processRequest{op: processSourceCoordinate, data: proposal.wire})
	result, err := decodeGraphProcessCoordinatorResult(response)
	if err != nil || !result.decision.commit || result.decision.phase != crossDecide || result.decision.tx != result.resume.tx {
		t.Fatal("same-GraphID six-process source-owned cross-edge decision", result, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := drainGraphProcessMuxes(ctx, n.all); err != nil {
		t.Fatal("real follower transport/apply drain", err)
	}
	for groupIndex, group := range n.groups {
		var original, originalRecord processResponse
		for id, p := range group.nodes {
			current := p.call(t, processRequest{op: processStateOp})
			record := p.call(t, processRequest{op: processEntryRecord, index: current.applied})
			status, err := decodeCrossStatus(record.outcome)
			if err != nil || status.phase != crossResolve || !status.commit || status.tx != result.resume.tx || status.digest != result.decision.digest || status.round != result.decision.round {
				t.Fatal("actual six-voter native resolve", id, status, err)
			}
			if original.image == nil {
				original = current
				originalRecord = record
			} else if original.applied != current.applied || !bytes.Equal(original.image, current.image) {
				t.Fatal("cross-edge voter graph/index mismatch", id)
			}
			if !bytes.Equal(record.outcome, originalRecord.outcome) || !bytes.Equal(record.changes, originalRecord.changes) || len(record.changes) == 0 {
				t.Fatal("exact six-voter outcome/CDC parity", id)
			}
			expected := outcome{ns: status.ns, kind: graphOperations, identity: status.tx, hash: status.digest, index: status.index, disposition: applied}
			logical, err := decodeTypedChangeEnvelope(record.changes, expected, defaultMaterializerLimits(), true)
			if err != nil || len(logical.values) != 0 {
				t.Fatal("committed typed cross-edge CDC", id, err)
			}
			if groupIndex == 0 {
				if len(logical.entities) != 1 || logical.entities[0].ID != 3 || len(logical.lives) != 1 || logical.lives[0].Owner != 3 || logical.lives[0].Life != 4 || len(logical.groups) != 1 {
					t.Fatal("primary relationship CDC shape", id, logical)
				}
				for _, component := range logical.groups {
					if component.Key != (graphstate.ComponentKey{Owner: 3, Kind: graphstate.Presence}) {
						t.Fatal("foreign component in edge CDC", id, component.Key)
					}
				}
			} else if len(logical.entities)+len(logical.lives)+len(logical.groups) != 0 {
				t.Fatal("secondary-only participant invented primary CDC", id, logical)
			}
			if current.stats[statViews] != 0 || current.stats[statPinnedBytes] != 0 {
				t.Fatal("cross-edge retained source owners", id, current.stats)
			}
		}
	}
	oracle.crossEdge()
	n.compare(t, oracle)
}

func TestReplicatedCertifiedGraphSixProcessesLocalCrossEdgeRefusalRemainsHealthy(t *testing.T) {
	n := newCertifiedProcessNetwork(t)
	oracle := newCertifiedProcessOracle()
	n.compare(t, oracle)
	before := make(map[uint64]processResponse)
	for id, p := range n.all {
		before[id] = p.call(t, processRequest{op: processStateOp})
	}
	o, response := n.action(t, 1, certifiedCreateCrossEdge, 3, nil)
	// Business refusal is valid local behavior. Check its complete negative
	// state and post-refusal health before asserting this broader acceptance.
	if o.reason == reasonRemoteParticipant {
		if len(response.changes) != 0 {
			t.Fatal("refused cross-edge emitted graph CDC")
		}
		for id, p := range n.all {
			after := p.call(t, processRequest{op: processStateOp})
			if !bytes.Equal(after.image, before[id].image) {
				t.Fatal("refused cross-edge changed graph", id)
			}
			if id >= 4 && (after.stats != before[id].stats || after.applied != before[id].applied) {
				t.Fatal("local refusal changed remote group", id)
			}
		}
		n.compare(t, oracle)
		n.action(t, 1, certifiedHealthyLabel, 1, nil)
		n.action(t, 4, certifiedHealthyLabel, 17, nil)
		oracle.healthy()
		n.compare(t, oracle)
	}
	if o.reason != reasonRemoteParticipant {
		t.Fatal("accepted local door changed its refusal contract", o.reason)
	}
}

type tinyGraphTransport struct {
	blocked                                              map[uint64]bool
	snapshots                                            []processSnapshotPacket
	nodes                                                map[uint64]*graphProcess
	queue                                                []replica.Packet
	queuedBytes, maxQueueBytes, maxQueueCount, delivered int
	reads                                                []replica.ReadResult
}

func (n *tinyGraphTransport) enqueue(t *testing.T, out processResponse) {
	t.Helper()
	snapshotAt := 0
	for _, p := range out.packets {
		if p.Snapshot {
			if snapshotAt >= len(out.handles) || len(n.snapshots) >= 2 {
				t.Fatal("snapshot lost bounded exact owner")
			}
			n.snapshots = append(n.snapshots, processSnapshotPacket{p, out.handles[snapshotAt]})
			snapshotAt++
			continue
		}
		cost := cap(p.Payload) + 64 // Conservative: borrowed frame tails are charged per packet.
		if len(n.queue) >= processPacketCount || cost > processQueueBytes-n.queuedBytes {
			t.Fatal("bounded packet queue exhausted")
		}
		if n.nodes[p.From] == nil || n.nodes[p.To] == nil {
			t.Fatal("traffic crosses distinct graph group", p.From, p.To)
		}
		n.queue = append(n.queue, p)
		n.queuedBytes += cost
		n.maxQueueCount = max(n.maxQueueCount, len(n.queue))
		n.maxQueueBytes = max(n.maxQueueBytes, n.queuedBytes)
	}
	if snapshotAt != len(out.handles) {
		t.Fatal("snapshot owner without packet")
	}
	if len(out.reads) > 16-len(n.reads) {
		t.Fatal("bounded read result collection")
	}
	n.reads = append(n.reads, out.reads...)
}
func (n *tinyGraphTransport) drain(t *testing.T) {
	t.Helper()
	for len(n.queue) > 0 {
		if n.delivered == processDeliveries {
			t.Fatal("bounded packet schedule exhausted")
		}
		p := n.queue[0]
		n.queue[0] = replica.Packet{}
		n.queue = n.queue[1:]
		n.queuedBytes -= cap(p.Payload) + 64
		n.delivered++
		if n.blocked[p.From] || n.blocked[p.To] {
			continue
		}
		n.enqueue(t, n.nodes[p.To].call(t, processRequest{op: processStep, packet: p}))
	}
}
func (n *tinyGraphTransport) event(t *testing.T, id uint64, r processRequest) {
	t.Helper()
	n.enqueue(t, n.nodes[id].call(t, r))
	n.drain(t)
}
func (n *tinyGraphTransport) commit(t *testing.T, follower, leader uint64, wire []byte, ns namespace) processResponse {
	t.Helper()
	n.event(t, follower, processRequest{op: processPropose, data: wire})
	r := n.nodes[leader].call(t, processRequest{op: processLookup, data: wire})
	o, err := decodeAnyOutcome(r.outcome, ns)
	if err != nil || o.reason != reasonNone || o.hash != sha256.Sum256(wire) || o.disposition != applied {
		t.Fatal("not original durable success", o, err)
	}
	for id, p := range n.nodes {
		if n.blocked[id] {
			continue
		}
		got := p.call(t, processRequest{op: processLookup, data: wire})
		if !bytes.Equal(got.outcome, r.outcome) || !bytes.Equal(got.changes, r.changes) {
			t.Fatal("replicated outcome/ordered CDC mismatch", id)
		}
	}
	return r
}

type snapshotGraphCommands struct{ init, recipient, grant, created, changed, tail []byte }

func decodeLeaderKillMarker(b []byte) (leaderKillMaterializer, error) {
	if len(b) > 256 {
		return leaderKillMaterializer{}, errLimit
	}
	c := graphCursor{b: b}
	if !bytes.Equal(c.take(4), []byte("GKM1")) {
		return leaderKillMaterializer{}, errCorrupt
	}
	m := leaderKillMaterializer{identity: c.array()}
	copy(m.hash[:], c.take(32))
	m.index = c.u64()
	m.term = c.u64()
	m.generation = c.u64()
	copy(m.rootHash[:], c.take(32))
	copy(m.outcomeHash[:], c.take(32))
	copy(m.changeHash[:], c.take(32))
	if len(c.b) != 0 || m.index == 0 || m.term == 0 || m.generation != 1 {
		c.err = errCorrupt
	}
	return m, c.err
}

// This returns only on the intended leader callback death, never hides a
// successful decoded RPC/Output or an application receipt already returned.
func deliverUntilLeaderDeath(t *testing.T, n *tinyGraphTransport) {
	t.Helper()
	for len(n.queue) > 0 {
		if n.delivered >= processDeliveries {
			t.Fatal("bounded kill schedule")
		}
		p := n.queue[0]
		n.queue[0] = replica.Packet{}
		n.queue = n.queue[1:]
		n.queuedBytes -= cap(p.Payload) + 64
		n.delivered++
		if n.blocked[p.From] || n.blocked[p.To] {
			continue
		}
		if p.To != 1 {
			n.enqueue(t, n.nodes[p.To].call(t, processRequest{op: processStep, packet: p}))
			continue
		}
		if p.From != 2 {
			t.Fatal("unexpected peer triggered target step", p.From)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		r, err := n.nodes[1].exchange(ctx, processRequest{op: processStep, packet: p})
		ctxErr := ctx.Err()
		cancel()
		if err == nil {
			n.enqueue(t, r)
			continue
		}
		if ctxErr != nil || t.Context().Err() != nil || !errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal("death is not complete-frame EOF before deadline", err, ctxErr)
		}
		ctx, cancel = context.WithTimeout(t.Context(), 10*time.Second)
		reaped := n.nodes[1].shutdown(ctx, true)
		cancel()
		if errors.Is(reaped, context.DeadlineExceeded) || errors.Is(reaped, context.Canceled) {
			t.Fatal("reaping deadline/cancellation cannot prove intended death", reaped)
		}
		if _, ok := errors.AsType[*exec.ExitError](reaped); !ok {
			t.Fatal("missing actual process death", reaped)
		}
		if n.nodes[1].cmd.ProcessState == nil {
			t.Fatal("intended death has not been reaped", reaped)
		}
		status, ok := n.nodes[1].cmd.ProcessState.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			t.Fatal("not intended SIGKILL", n.nodes[1].cmd.ProcessState)
		}
		if err := n.nodes[1].cmd.Process.Signal(syscall.Signal(0)); !errors.Is(err, os.ErrProcessDone) {
			t.Fatal("dead leader not reaped", err)
		}
		return
	}
	t.Fatal("durable/pre-output kill boundary not reached")
}

func TestReplicatedGraphLeaderDiesAfterCommitBeforeReceipt(t *testing.T) {
	if os.Getenv("RHO_GRAPH_PROCESS_CHILD") == "1" {
		t.Skip("parent only")
	}
	start := time.Now()
	dir, err := os.MkdirTemp(os.Getenv("RHO_GRAPH_PROCESS_ARTIFACT"), "rho-graph-kill-stores-")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("retained kill fixture directory", dir)
	all := map[uint64]*graphProcess{}
	var launched []*graphProcess
	for id := uint64(1); id <= 6; id++ {
		all[id] = startGraphProcess(t, filepath.Join(dir, fmt.Sprint(id)), id)
		launched = append(launched, all[id])
	}
	a := tinyGraphTransport{nodes: map[uint64]*graphProcess{1: all[1], 2: all[2], 3: all[3]}, blocked: map[uint64]bool{}}
	b := tinyGraphTransport{nodes: map[uint64]*graphProcess{4: all[4], 5: all[5], 6: all[6]}}
	an, _, _ := processNamespace(1)
	bn, _, _ := processNamespace(4)
	ac := tinySnapshotCommands(t, an)
	bc := tinySnapshotCommands(t, bn)
	var createdA, createdB uint64
	var bOriginals []struct {
		wire     []byte
		response processResponse
	}
	for _, g := range []struct {
		n                *tinyGraphTransport
		leader, follower uint64
		ns               namespace
		c                snapshotGraphCommands
	}{{&a, 1, 3, an, ac}, {&b, 4, 6, bn, bc}} {
		g.n.event(t, g.leader, processRequest{op: processCampaign})
		for _, wire := range [][]byte{g.c.init, g.c.recipient, g.c.grant, g.c.created} {
			r := g.n.commit(t, g.follower, g.leader, wire, g.ns)
			o, err := decodeAnyOutcome(r.outcome, g.ns)
			if err != nil {
				t.Fatal(err)
			}
			if g.leader == 1 {
				createdA = o.index
			} else {
				createdB = o.index
				bOriginals = append(bOriginals, struct {
					wire     []byte
					response processResponse
				}{wire, r})
			}
		}
	}
	bBefore := map[uint64]processResponse{}
	for _, id := range []uint64{4, 5, 6} {
		bBefore[id] = all[id].call(t, processRequest{op: processStateOp})
	}
	barrier := a.barrier(t, 1, []byte("before-guarded-submit"))
	if barrier != createdA {
		t.Fatal("unexpected pre-submit index", barrier, createdA)
	}
	oracle := newTinyGraphOracle()
	oracle.created(createdA)
	held := all[2].call(t, processRequest{op: processHold, index: createdA})
	if len(held.handles) != 1 {
		t.Fatal("missing actual read-session owner")
	}
	handle := held.handles[0]
	oldImage := assertHeldSnapshotFacts(t, all[2], handle, createdA, 1, oracle)
	minted := all[2].call(t, processRequest{op: processGuardRequest, index: handle, data: ac.changed})
	wire := minted.aux
	guarded, err := decodeGraphRequest(wire, defaultMaterializerLimits())
	if err != nil || guarded.kind != guardedGraphOperations || guarded.readBase.group != ([16]byte{41}) {
		t.Fatal("not actual guarded request", guarded, err)
	}
	all[1].call(t, processRequest{op: processArmLeaderKill, data: wire})
	a.blocked[3] = true
	proposal := all[1].call(t, processRequest{op: processPropose, data: wire})
	if proposal.applied != createdA || len(proposal.outcome) != 0 {
		t.Fatal("proposal unexpectedly supplied target application receipt")
	}
	a.enqueue(t, proposal)
	deliverUntilLeaderDeath(t, &a)
	markerPath := filepath.Join(dir, "leader-kill.marker")
	markerInfo, err := os.Stat(markerPath)
	if err != nil || markerInfo.Size() < 0 || markerInfo.Size() > 256 {
		t.Fatal("bounded marker framing", err)
	}
	markerBytes, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal("synced boundary diagnostic absent", err)
	}
	marker, err := decodeLeaderKillMarker(markerBytes)
	if err != nil || marker.hash != sha256.Sum256(wire) || marker.identity != guarded.identity() || marker.index != createdA+1 {
		t.Fatal("wrong committed kill target", marker, err)
	}
	// Controller-only marker; the simulated client has performed NO target
	// Lookup/EntryRecord before death or before the identical retry below.
	a.blocked[1] = true
	a.blocked[3] = false
	// Keep the dead peer in the membership/route table; blocked delivery never calls it.
	ticks := 0
	campaigns := 0
	newLeaderBarrier := uint64(0)
	for ticks < 40 && newLeaderBarrier == 0 {
		for _, id := range []uint64{2, 3} {
			a.event(t, id, processRequest{op: processTick})
		}
		ticks++
		if (ticks == 10 || ticks == 20) && campaigns < 2 {
			a.event(t, 2, processRequest{op: processCampaign})
			campaigns++
		}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		r, e := all[2].exchange(ctx, processRequest{op: processRead, data: []byte("failover-current-term")})
		cancel()
		if e != nil {
			t.Fatal(e)
		}
		if err := processResponseError(r); err != nil {
			if !errors.Is(err, replica.ErrUnavailable) {
				t.Fatal(err)
			}
			continue
		}
		a.enqueue(t, r)
		a.drain(t)
		if len(a.reads) != 1 || !bytes.Equal(a.reads[0].Context, []byte("failover-current-term")) {
			t.Fatal("failover barrier mismatch")
		}
		newLeaderBarrier = a.reads[0].Index
		a.reads = nil
	}
	if newLeaderBarrier <= marker.index {
		t.Fatal("new leader has not committed current-term no-op", newLeaderBarrier, marker.index)
	}
	beforeRetry := all[2].call(t, processRequest{op: processStateOp})
	if sha256.Sum256(beforeRetry.image) != marker.rootHash {
		t.Fatal("failover no-op changed original graph root")
	}
	root, err := graphstore.DecodeRoot(beforeRetry.image)
	if err != nil || root.SemanticEpoch() == guarded.readBase.guard.SemanticEpoch || root.EffectDigest() == guarded.readBase.guard.EffectDigest {
		t.Fatal("guard not stale after original mutation", root, err)
	}
	oracle.changed(marker.index)
	oracle.retrySame(beforeRetry.applied)
	// Identical bytes; no refreshed guard, modified request ID or reminted IDs.
	a.event(t, 2, processRequest{op: processPropose, data: wire})
	afterRetry := all[2].call(t, processRequest{op: processStateOp})
	retryIndex := beforeRetry.applied + 1
	if afterRetry.applied != retryIndex || !bytes.Equal(afterRetry.image, beforeRetry.image) {
		t.Fatal("retry changed graph or did not apply once", beforeRetry.applied, afterRetry.applied)
	}
	barrier = a.barrier(t, 2, []byte("first-client-retry-receipt"))
	if barrier < retryIndex {
		t.Fatal("retry barrier behind entry")
	}
	retry := all[2].call(t, processRequest{op: processEntryRecord, index: retryIndex})
	o, err := decodeAnyOutcome(retry.outcome, an)
	if err != nil || o.disposition != requestReplay || o.reason != reasonNone || o.index != marker.index || o.hash != marker.hash || o.identity != marker.identity || len(retry.changes) != 0 {
		t.Fatal("actual retry entry is not original-index replay", o, err)
	}
	original := all[2].call(t, processRequest{op: processLookup, data: wire})
	originalOutcome, err := decodeAnyOutcome(original.outcome, an)
	if err != nil || originalOutcome.disposition != applied || originalOutcome.index != marker.index || sha256.Sum256(original.outcome) != marker.outcomeHash || sha256.Sum256(original.changes) != marker.changeHash {
		t.Fatal("immutable original mapping/CDC changed", originalOutcome, err)
	}
	oracle.retrySame(retryIndex)
	for _, id := range []uint64{2, 3} {
		assertSnapshotFacts(t, all[id], createdA, barrier, 1, oracle)
		assertSnapshotFacts(t, all[id], marker.index, barrier, 1, oracle)
		assertSnapshotFacts(t, all[id], retryIndex, barrier, 1, oracle)
		r := all[id].call(t, processRequest{op: processLookup, data: wire})
		if !bytes.Equal(r.outcome, original.outcome) || !bytes.Equal(r.changes, original.changes) {
			t.Fatal("original replica parity", id)
		}
		r = all[id].call(t, processRequest{op: processEntryRecord, index: retryIndex})
		if !bytes.Equal(r.outcome, retry.outcome) || len(r.changes) != 0 {
			t.Fatal("retry entry parity", id)
		}
	}
	if !bytes.Equal(oldImage, assertHeldSnapshotFacts(t, all[2], handle, createdA, 1, oracle)) {
		t.Fatal("failover/retry moved original held read")
	}
	all[2].call(t, processRequest{op: processHoldClose, index: handle})
	oldNonce := all[1].incarnation
	all[1] = startGraphProcess(t, filepath.Join(dir, "1"), 1, "RHO_GRAPH_PROCESS_REOPEN=1")
	launched = append(launched, all[1])
	a.nodes[1] = all[1]
	a.blocked[1] = false
	reopened := all[1].call(t, processRequest{op: processStateOp})
	if reopened.incarnation == oldNonce || reopened.applied != marker.index || sha256.Sum256(reopened.image) != marker.rootHash || reopened.stats[statGeneration] != 1 {
		t.Fatal("old leader durable checkpoint not recovered", reopened.stats)
	}
	for range 3 {
		a.event(t, 2, processRequest{op: processTick})
	}
	barrier = a.barrier(t, 2, []byte("old-leader-rejoined"))
	for _, id := range []uint64{1, 2, 3} {
		for _, index := range []uint64{createdA, marker.index, retryIndex} {
			assertSnapshotFacts(t, all[id], index, barrier, 1, oracle)
		}
		r := all[id].call(t, processRequest{op: processLookup, data: wire})
		if !bytes.Equal(r.outcome, original.outcome) || !bytes.Equal(r.changes, original.changes) {
			t.Fatal("rejoin original outcome/CDC parity", id)
		}
		r = all[id].call(t, processRequest{op: processEntryRecord, index: retryIndex})
		if !bytes.Equal(r.outcome, retry.outcome) || len(r.changes) != 0 {
			t.Fatal("rejoin replay-entry parity", id)
		}
	}
	for _, id := range []uint64{4, 5, 6} {
		r := all[id].call(t, processRequest{op: processStateOp})
		if r.stats != bBefore[id].stats || !bytes.Equal(r.image, bBefore[id].image) || r.cutID != bBefore[id].cutID || r.manifestID != bBefore[id].manifestID {
			t.Fatal("distinct B checkpoint/ledger changed", id)
		}
	}
	boracle := newTinyGraphOracle()
	boracle.created(createdB)
	bbarrier := b.barrier(t, 4, []byte("distinct-B-after-leader-kill"))
	for _, id := range []uint64{4, 5, 6} {
		assertSnapshotFacts(t, all[id], createdB, bbarrier, 1, boracle)
		for _, saved := range bOriginals {
			r := all[id].call(t, processRequest{op: processLookup, data: saved.wire})
			if !bytes.Equal(r.outcome, saved.response.outcome) || !bytes.Equal(r.changes, saved.response.changes) {
				t.Fatal("B original records changed", id)
			}
		}
	}
	measurement := tinyProcessMeasurement{Scope: "ONE post-synced-install/realRestore pre-Output leader SIGKILL + identical stale-guard retry + rejoin; distinct sole-partition graphs; no snapshot/matrix/cut claim", Processes: 6, Packets: a.delivered + b.delivered, MaxQueueCount: max(a.maxQueueCount, b.maxQueueCount), MaxQueueBytes: max(a.maxQueueBytes, b.maxQueueBytes), StoreDirectory: dir}
	var rpcCount uint64
	for _, p := range launched {
		rpcCount += p.sequence
		measurement.MaxIPCFrameBytes = max(measurement.MaxIPCFrameBytes, p.maxFrame)
	}
	if rpcCount+uint64(measurement.Packets) > processDeliveries {
		t.Fatal("bounded total kill schedule")
	}
	for _, p := range all {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		err := p.shutdown(ctx, false)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	for id := 1; id <= 6; id++ {
		size, err := processStoreBytes(filepath.Join(dir, fmt.Sprint(id)))
		if err != nil {
			t.Fatal(err)
		}
		measurement.ClosedStoreFileBytes += size
	}
	if measurement.ClosedStoreFileBytes > 256<<20 {
		t.Fatal("closed stores exceed cap")
	}
	measurement.WallSeconds = time.Since(start).Seconds()
	report := struct {
		tinyProcessMeasurement
		ElectionTicks, Campaigns                   int
		RPCCount, OriginalIndex, RetryAppliedIndex uint64
		MarkerBytes                                int
	}{measurement, ticks, campaigns, rpcCount, marker.index, retryIndex, len(markerBytes)}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "measurement.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("leader kill measurement: %s", data)
}

func tinySnapshotCommands(t *testing.T, n namespace) snapshotGraphCommands {
	t.Helper()
	encode := func(r graphRequest) []byte {
		b, err := encodeGraphRequest(r, defaultMaterializerLimits())
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	init := graphInit(t)
	init.ns = n
	init.schemas = []graphstate.PropertyDefinition{{Name: "answer", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}}
	commands := snapshotGraphCommands{init: encode(init)}
	controls := codecRequests(t)
	for _, control := range []request{controls[2], controls[3]} {
		control.ns = n
		if control.kind == activateRecipient {
			control.home = n.partition
		} else {
			control.sequence = 1
			control.count = 16
		}
		wire, err := encodeRequest(control, defaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		if control.kind == activateRecipient {
			commands.recipient = wire
		} else {
			commands.grant = wire
		}
	}
	r := graphCodecRequest(t)
	r.ns = n
	r.id = requestID{8}
	scope := r.operations[0].Scope
	r.operations = nil
	r.claims = nil
	grant := grantReference{session: codecSession(), sequence: 1}
	for _, node := range []struct{ entity, life uint64 }{{1, 2}, {3, 4}} {
		r.operations = append(r.operations, graphstate.Operation{Kind: graphstate.CreateNode, Owner: graphstate.EntityID(node.entity), Life: graphstate.LifeID(node.life), Scope: scope, Record: graphstate.EntityRecord{ID: graphstate.EntityID(node.entity), Kind: graphstate.Node, Axis: scope.Axis()}}, graphstate.Operation{Kind: graphstate.AddLabel, Owner: graphstate.EntityID(node.entity), Life: graphstate.LifeID(node.life), Scope: scope, Name: "X"}, graphstate.Operation{Kind: graphstate.Set, Owner: graphstate.EntityID(node.entity), Life: graphstate.LifeID(node.life), Scope: scope, Name: "answer", Value: graphstate.String("old"), ValueID: 5})
		r.claims = append(r.claims, freshBinding{role: entityBinding, id: node.entity, grant: grant}, freshBinding{role: lifeBinding, owner: graphstate.EntityID(node.entity), id: node.life, grant: grant})
	}
	r.claims = append(r.claims, freshBinding{role: valueBinding, id: 5, grant: grant})
	commands.created = encode(r)
	r.id = requestID{9}
	var err error
	r.revision, err = state.NewRevision(4, 0)
	if err != nil {
		t.Fatal(err)
	}
	r.operations = []graphstate.Operation{{Kind: graphstate.Close, Owner: 1, Life: 2, Scope: scope}, {Kind: graphstate.RemoveLabel, Owner: 3, Life: 4, Scope: scope, Name: "X"}, {Kind: graphstate.AddLabel, Owner: 3, Life: 4, Scope: scope, Name: "Y"}, {Kind: graphstate.Set, Owner: 3, Life: 4, Scope: scope, Name: "answer", Value: graphstate.String("new"), ValueID: 6}}
	r.claims = []freshBinding{{role: valueBinding, id: 6, grant: grant}}
	commands.changed = encode(r)
	r.id = requestID{10}
	r.revision, err = state.NewRevision(5, 0)
	if err != nil {
		t.Fatal(err)
	}
	r.operations = []graphstate.Operation{{Kind: graphstate.Set, Owner: 3, Life: 4, Scope: scope, Name: "answer", Value: graphstate.String("tail"), ValueID: 7}}
	r.claims = []freshBinding{{role: valueBinding, id: 7, grant: grant}}
	commands.tail = encode(r)
	return commands
}

func processExpectRefusal(t *testing.T, p *graphProcess, r processRequest, want error) {
	t.Helper()
	before := p.call(t, processRequest{op: processStateOp})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	refused, err := p.exchange(ctx, r)
	if err != nil {
		t.Fatal("IPC refusal transport", err)
	}
	if err := processResponseError(refused); !errors.Is(err, want) {
		t.Fatal("wrong call-layer refusal", err, want)
	}
	after := p.call(t, processRequest{op: processStateOp})
	if before.stats != after.stats || before.cutID != after.cutID || before.manifestID != after.manifestID || !bytes.Equal(before.image, after.image) {
		t.Fatal("refusal changed active checkpoint/HardState/audit/transfer ledger", before.stats, after.stats)
	}
}
func assertSnapshotFacts(t *testing.T, p *graphProcess, index, barrier, generation uint64, oracle *tinyGraphOracle) {
	t.Helper()
	var active []uint64
	for _, id := range []uint64{1, 3, 99} {
		r := p.call(t, processRequest{op: processProject, index: index, entity: id})
		if r.applied < barrier || r.readGeneration != generation || r.stats[statGeneration] != generation {
			t.Fatal("fresh read base/barrier", r.applied, barrier, r.readGeneration, r.stats)
		}
		if err := oracle.compare(index, id, r.fact); err != nil {
			t.Fatal(err)
		}
		if r.fact.Active {
			active = append(active, id)
		}
	}
	var want []uint64
	for _, id := range []uint64{1, 3, 99} {
		if oracle.versions[index][id].Active {
			want = append(want, id)
		}
	}
	if !reflect.DeepEqual(active, want) {
		t.Fatal("exact snapshot active set", active, want)
	}
}
func assertHeldSnapshotFacts(t *testing.T, p *graphProcess, handle, index, activeGeneration uint64, oracle *tinyGraphOracle) []byte {
	t.Helper()
	var image []byte
	for _, id := range []uint64{1, 3, 99} {
		r := p.call(t, processRequest{op: processHeldProject, index: handle, entity: id})
		if r.readGeneration != 1 || r.stats[statGeneration] != activeGeneration || r.stats[statViews] != 1 {
			t.Fatal("held view rebound or leaked", r.readGeneration, r.stats)
		}
		if err := oracle.compare(index, id, r.fact); err != nil {
			t.Fatal(err)
		}
		t.Logf("held generation1 observation: activeGeneration=%d originalIndex=%d entity=%d cumulativeRecords=%d cumulativeBytes=%d", activeGeneration, index, id, r.stats[statHeldRows], r.stats[statHeldBytes])
		if image == nil {
			image = r.image
		} else if !bytes.Equal(image, r.image) {
			t.Fatal("held root changed")
		}
	}
	return image
}

// A real checked absent-entity read on the SAME held Full handle supplies the
// refusal positive control without repeating three complete projections and
// exhausting its intentionally cumulative source ledger.
func assertHeldPhantom(t *testing.T, p *graphProcess, handle uint64, image []byte) {
	t.Helper()
	r := p.call(t, processRequest{op: processHeldProject, index: handle, entity: 99})
	if err := compareProcessFact(r.fact, processFact{}); err != nil {
		t.Fatal(err)
	}
	if r.readGeneration != 1 || r.stats[statGeneration] != 1 || !bytes.Equal(r.image, image) {
		t.Fatal("held refusal control changed view")
	}
	t.Logf("held generation1 refusal control: entity99 cumulativeRecords=%d cumulativeBytes=%d", r.stats[statHeldRows], r.stats[statHeldBytes])
}

func TestReplicatedGraphSnapshotHeldViewTailAndReopen(t *testing.T) {
	if os.Getenv("RHO_GRAPH_PROCESS_CHILD") == "1" {
		t.Skip("parent only")
	}
	start := time.Now()
	dir, err := os.MkdirTemp(os.Getenv("RHO_GRAPH_PROCESS_ARTIFACT"), "rho-graph-snapshot-stores-")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("retained snapshot fixture directory", dir)
	all := map[uint64]*graphProcess{}
	var launched []*graphProcess
	for id := uint64(1); id <= 6; id++ {
		all[id] = startGraphProcess(t, filepath.Join(dir, fmt.Sprint(id)), id)
		launched = append(launched, all[id])
	}
	a := tinyGraphTransport{nodes: map[uint64]*graphProcess{1: all[1], 2: all[2], 3: all[3]}, blocked: map[uint64]bool{}}
	b := tinyGraphTransport{nodes: map[uint64]*graphProcess{4: all[4], 5: all[5], 6: all[6]}}
	an, _, _ := processNamespace(1)
	bn, _, _ := processNamespace(4)
	ac := tinySnapshotCommands(t, an)
	bc := tinySnapshotCommands(t, bn)
	var originals []struct {
		wire     []byte
		response processResponse
	}
	var bOriginals []struct {
		wire     []byte
		response processResponse
	}
	var createdA, createdB uint64
	for _, group := range []struct {
		network          *tinyGraphTransport
		leader, follower uint64
		ns               namespace
		commands         snapshotGraphCommands
	}{{&a, 1, 3, an, ac}, {&b, 4, 6, bn, bc}} {
		group.network.event(t, group.leader, processRequest{op: processCampaign})
		for _, wire := range [][]byte{group.commands.init, group.commands.recipient, group.commands.grant, group.commands.created} {
			r := group.network.commit(t, group.follower, group.leader, wire, group.ns)
			o, err := decodeAnyOutcome(r.outcome, group.ns)
			if err != nil {
				t.Fatal(err)
			}
			if group.leader == 1 {
				originals = append(originals, struct {
					wire     []byte
					response processResponse
				}{wire, r})
				createdA = o.index
			} else {
				createdB = o.index
				bOriginals = append(bOriginals, struct {
					wire     []byte
					response processResponse
				}{wire, r})
			}
		}
	}
	oracle := newTinyGraphOracle()
	oracle.created(createdA)
	held := all[3].call(t, processRequest{op: processHold, index: createdA})
	if len(held.handles) != 1 {
		t.Fatal("held owner missing")
	}
	heldID := held.handles[0]
	oldHeldImage := assertHeldSnapshotFacts(t, all[3], heldID, createdA, 1, oracle)
	bBefore := all[4].call(t, processRequest{op: processStateOp})
	a.blocked[3] = true
	changed := a.commit(t, 2, 1, ac.changed, an)
	o, err := decodeAnyOutcome(changed.outcome, an)
	if err != nil {
		t.Fatal(err)
	}
	changedIndex := o.index
	oracle.changed(changedIndex)
	originals = append(originals, struct {
		wire     []byte
		response processResponse
	}{ac.changed, changed})
	if all[3].call(t, processRequest{op: processStateOp}).applied >= changedIndex {
		t.Fatal("isolated follower invented change")
	}
	all[1].call(t, processRequest{op: processPublish})
	a.blocked[3] = false
	buildCalls := 0
	for ticks := 0; len(a.snapshots) == 0 && ticks < 20; ticks++ {
		a.event(t, 1, processRequest{op: processTick})
		if len(a.snapshots) > 0 {
			break
		}
		offers := all[1].call(t, processRequest{op: processOffers})
		for _, id := range offers.handles {
			ready := false
			for !ready && buildCalls < processDeliveries {
				r := all[1].call(t, processRequest{op: processBuild, index: id})
				buildCalls++
				ready = r.ready
			}
			if !ready {
				t.Fatal("bounded manifest build exhausted")
			}
			all[1].call(t, processRequest{op: processSeal, index: id})
		}
	}
	if len(a.snapshots) != 1 {
		t.Fatal("one actual snapshot packet required", len(a.snapshots))
	}
	send := a.snapshots[0]
	a.snapshots = nil
	if send.packet.From != 1 || send.packet.To != 3 || !send.packet.Snapshot {
		t.Fatal("wrong actual snapshot peers", send.packet)
	}
	manifestWire := all[1].call(t, processRequest{op: processManifest, index: send.owner}).aux
	binding := raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(an.graph), Partition: an.partition, Group: [16]byte{41}}, SemanticContractID: SemanticContractID()}
	manifest, err := raftlog.DecodeApplicationSnapshotManifestForBinding(manifestWire, 64<<10, binding)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 3 || manifest.Index != changedIndex || manifest.SemanticContractID != SemanticContractID() || !reflect.DeepEqual(manifest.ConfState.GetVoters(), []uint64{1, 2, 3}) {
		t.Fatal("unexpected portable source manifest", manifest)
	}
	// These are binding-aware CODEC refusals, before BeginImport; do not claim
	// that rechecksummed/foreign manifest admission reached the Store boundary.
	semanticOffset := len(manifestWire) - len(manifest.Image) - proto.Size(manifest.ConfState) - 32
	for _, offset := range []int{4, 27, 28, semanticOffset} {
		bad := bytes.Clone(manifestWire)
		bad[offset] ^= 0x40
		processExpectRefusal(t, all[3], processRequest{op: processBeginImport, data: bad}, raftlog.ErrInvalid)
		assertHeldPhantom(t, all[3], heldID, oldHeldImage)
	}
	imported := all[3].call(t, processRequest{op: processBeginImport, data: manifestWire})
	if len(imported.handles) != 1 || len(imported.aux) != 32 {
		t.Fatal("missing same-child import owner")
	}
	importID := imported.handles[0]
	var manifestID [32]byte
	copy(manifestID[:], imported.aux)
	pages := 0
	maxRows, maxVisitedBytes, maxDataBytes := uint64(0), uint64(0), 0
	for {
		if buildCalls+pages+a.delivered+b.delivered >= processDeliveries {
			t.Fatal("bounded total snapshot schedule")
		}
		r := all[1].call(t, processRequest{op: processNext, index: send.owner})
		chunk, err := decodeProcessChunk(r.aux)
		if err != nil {
			t.Fatal(err)
		}
		if chunk.Version != 3 || chunk.CutID != manifest.CutID || chunk.ManifestID != manifestID || chunk.Visited > 64 || chunk.VisitedBytes > 256<<10 {
			t.Fatal("snapshot page binding/budget", chunk)
		}
		maxRows = max(maxRows, chunk.Visited)
		maxVisitedBytes = max(maxVisitedBytes, chunk.VisitedBytes)
		maxDataBytes = max(maxDataBytes, len(chunk.Data))
		pages++
		all[3].call(t, processRequest{op: processAppend, index: importID, data: r.aux})
		if chunk.Final {
			break
		}
	}
	all[3].call(t, processRequest{op: processVerify, index: importID})
	prepared := all[3].call(t, processRequest{op: processPrepare, index: importID})
	if prepared.stats[statGeneration] != 1 || prepared.stats[statPrepared] != 1 || prepared.stats[statImport] != 1 || prepared.stats[statVerified] != 1 {
		t.Fatal("dormant prepared owner activated early", prepared.stats)
	}
	raw, err := replica.DecodeApplicationPacket(binding, send.packet, processFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"group", "semantic"} {
		foreign := binding
		if variant == "group" {
			foreign.Identity.Group[0] ^= 1
		} else {
			foreign.SemanticContractID[0] ^= 1
		}
		wrong, err := replica.EncodeApplicationPacket(foreign, raw, processFrameBytes)
		if err != nil {
			t.Fatal(err)
		}
		processExpectRefusal(t, all[3], processRequest{op: processActivate, index: importID, packet: wrong}, replica.ErrInvalid)
		assertHeldPhantom(t, all[3], heldID, oldHeldImage)
	}
	processExpectRefusal(t, all[3], processRequest{op: processActivate, packet: send.packet}, raftlog.ErrInvalid)
	activated := all[3].call(t, processRequest{op: processActivate, index: importID, packet: send.packet})
	if activated.stats[statGeneration] != 2 || activated.stats[statCheckpoint] != manifest.Index || activated.stats[statAuditIndex] != manifest.Index || activated.stats[statAuditTerm] != manifest.Term || activated.cutID != manifest.CutID || activated.manifestID != manifestID {
		t.Fatal("activation identity mismatch", activated.stats, activated.cutID, activated.manifestID)
	}
	installed := all[3].call(t, processRequest{op: processStateOp})
	if !bytes.Equal(installed.image, manifest.Image) {
		t.Fatal("installed checkpoint differs from exact donor image")
	}
	// Keep receiver response packets until exact-owner sender feedback; otherwise
	// its acknowledgement may make pending-snapshot feedback legitimately stale.
	a.enqueue(t, all[1].call(t, processRequest{op: processFeedback, index: send.owner, entity: 1}))
	a.enqueue(t, activated)
	a.drain(t)
	all[3].call(t, processRequest{op: processImportClose, index: importID})
	barrier := a.barrier(t, 1, []byte("after-activation"))
	if !bytes.Equal(oldHeldImage, assertHeldSnapshotFacts(t, all[3], heldID, createdA, 2, oracle)) {
		t.Fatal("held historical root moved")
	}
	assertSnapshotFacts(t, all[3], createdA, barrier, 2, oracle)
	assertSnapshotFacts(t, all[3], changedIndex, barrier, 2, oracle)
	for _, original := range originals {
		r := all[3].call(t, processRequest{op: processLookup, data: original.wire})
		if !bytes.Equal(r.outcome, original.response.outcome) || !bytes.Equal(r.changes, original.response.changes) {
			t.Fatal("imported original outcome/CDC mismatch")
		}
	}
	source := all[1].call(t, processRequest{op: processStateOp})
	if source.stats[statGeneration] != 1 || !bytes.Equal(source.image, manifest.Image) {
		t.Fatal("portable source changed generation/root")
	}
	tail := a.commit(t, 2, 1, ac.tail, an)
	o, err = decodeAnyOutcome(tail.outcome, an)
	if err != nil {
		t.Fatal(err)
	}
	tailIndex := o.index
	oracle.tail(tailIndex)
	originals = append(originals, struct {
		wire     []byte
		response processResponse
	}{ac.tail, tail})
	barrier = a.barrier(t, 1, []byte("after-tail"))
	assertSnapshotFacts(t, all[3], tailIndex, barrier, 2, oracle)
	if !bytes.Equal(oldHeldImage, assertHeldSnapshotFacts(t, all[3], heldID, createdA, 2, oracle)) {
		t.Fatal("tail mutated held generation")
	}
	all[3].call(t, processRequest{op: processHoldClose, index: heldID})
	clean := all[3].call(t, processRequest{op: processStateOp})
	if clean.stats[statViews] != 0 || clean.stats[statImport] != 0 || clean.stats[statPrepared] != 0 || clean.stats[statClaims] != 0 || clean.stats[statPinnedBytes] != 0 {
		t.Fatal("receiver owners leaked", clean.stats)
	}
	oldNonce := all[3].incarnation
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	err = all[3].shutdown(ctx, false)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	all[3] = startGraphProcess(t, filepath.Join(dir, "3"), 3, "RHO_GRAPH_PROCESS_REOPEN=1")
	launched = append(launched, all[3])
	a.nodes[3] = all[3]
	reopened := all[3].call(t, processRequest{op: processStateOp})
	if reopened.incarnation == oldNonce || reopened.stats[statGeneration] != 2 || reopened.cutID != manifest.CutID || reopened.manifestID != manifestID || reopened.stats[statViews] != 0 || reopened.stats[statImport] != 0 {
		t.Fatal("reopen resurrected volatile owners or changed durable activation", reopened.stats)
	}
	processExpectRefusal(t, all[3], processRequest{op: processHeldProject, index: heldID, entity: 3}, errInvalid)
	if offers := all[3].call(t, processRequest{op: processOffers}); len(offers.handles) != 0 {
		t.Fatal("restart restored sender IPC handles")
	}
	barrier = a.barrier(t, 1, []byte("after-reopen"))
	for _, index := range []uint64{createdA, changedIndex, tailIndex} {
		assertSnapshotFacts(t, all[3], index, barrier, 2, oracle)
	}
	for _, original := range originals {
		r := all[3].call(t, processRequest{op: processLookup, data: original.wire})
		if !bytes.Equal(r.outcome, original.response.outcome) || !bytes.Equal(r.changes, original.response.changes) {
			t.Fatal("reopen original outcome/CDC mismatch")
		}
	}
	bAfter := all[4].call(t, processRequest{op: processStateOp})
	if bBefore.stats != bAfter.stats || !bytes.Equal(bBefore.image, bAfter.image) || bAfter.cutID != ([32]byte{}) || bAfter.manifestID != ([32]byte{}) {
		t.Fatal("distinct graph group changed")
	}
	for _, original := range bOriginals {
		for _, id := range []uint64{4, 5, 6} {
			r := all[id].call(t, processRequest{op: processLookup, data: original.wire})
			if !bytes.Equal(r.outcome, original.response.outcome) || !bytes.Equal(r.changes, original.response.changes) {
				t.Fatal("distinct graph original outcome/CDC changed", id)
			}
		}
	}
	boracle := newTinyGraphOracle()
	boracle.created(createdB)
	bbarrier := b.barrier(t, 4, []byte("distinct-group-after-A"))
	for _, id := range []uint64{4, 5, 6} {
		assertSnapshotFacts(t, all[id], createdB, bbarrier, 1, boracle)
	}
	measurement := tinyProcessMeasurement{Scope: "ONE AS3 catch-up + retained Q-position0 view + tail/reopen; two DISTINCT sole-partition graphs; no crash/cut/2PC or independent CDC-oracle claim", Processes: 6, StoreDirectory: dir, Packets: a.delivered + b.delivered, MaxQueueCount: max(a.maxQueueCount, b.maxQueueCount), MaxQueueBytes: max(a.maxQueueBytes, b.maxQueueBytes)}
	var rpcCount, maxPinned, maxOffers, maxImports, maxViews uint64
	for _, p := range launched {
		rpcCount += p.sequence
		measurement.MaxIPCFrameBytes = max(measurement.MaxIPCFrameBytes, p.maxFrame)
		maxPinned = max(maxPinned, p.maxPinned)
		maxOffers = max(maxOffers, p.maxOffers)
		maxImports = max(maxImports, p.maxImports)
		maxViews = max(maxViews, p.maxHeld)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		err := p.shutdown(ctx, false)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	if rpcCount+uint64(buildCalls+pages) >= processDeliveries {
		t.Fatal("bounded RPC/page count exceeded")
	}
	measurement.ClosedStoreFileBytes, err = processStoreBytes(dir)
	if err != nil {
		t.Fatal(err)
	}
	measurement.WallSeconds = time.Since(start).Seconds()
	report := struct {
		tinyProcessMeasurement
		BuildCalls, ReplayPages, MaxChunkDataBytes                                                                                       int
		MaxChunkVisited, MaxChunkVisitedBytes, ObservedMaxPinnedBytes, ObservedMaxOffers, ObservedMaxImports, ObservedMaxViews, RPCCount uint64
	}{measurement, buildCalls, pages, maxDataBytes, maxRows, maxVisitedBytes, maxPinned, maxOffers, maxImports, maxViews, rpcCount}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "measurement.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("snapshot scenario measurement: %s", data)
}
func (n *tinyGraphTransport) barrier(t *testing.T, leader uint64, context []byte) uint64 {
	t.Helper()
	n.reads = nil
	n.event(t, leader, processRequest{op: processRead, data: context})
	if len(n.reads) != 1 || !bytes.Equal(n.reads[0].Context, context) {
		t.Fatal("request-bound quorum barrier missing", n.reads)
	}
	index := n.reads[0].Index
	n.reads = nil
	return index
}

type tinyProcessMeasurement struct {
	Scope                                                   string  `json:"scope"`
	WallSeconds                                             float64 `json:"wall_seconds"`
	Processes                                               int     `json:"processes"`
	Packets, MaxQueueCount, MaxQueueBytes, MaxIPCFrameBytes int
	ClosedStoreFileBytes                                    uint64 `json:"closed_store_file_bytes"`
	StoreDirectory                                          string `json:"store_directory"`
}

func TestReplicatedGraphSixProcessesOrdinaryAndHistory(t *testing.T) {
	if os.Getenv("RHO_GRAPH_PROCESS_CHILD") == "1" {
		t.Skip("parent only")
	}
	start := time.Now()
	parentDir := os.Getenv("RHO_GRAPH_PROCESS_ARTIFACT")
	dir, err := os.MkdirTemp(parentDir, "rho-graph-six-stores-")
	if err != nil {
		t.Fatal(err)
	}
	// Keep task-owned stores and diagnostics after any failure, rather than
	// hiding them in testing.T's automatic temporary-directory cleanup.
	t.Log("retained fixture directory", dir)
	all := make(map[uint64]*graphProcess)
	for id := uint64(1); id <= 6; id++ {
		all[id] = startGraphProcess(t, filepath.Join(dir, string(rune('0'+id))), id)
	}
	measurement := tinyProcessMeasurement{Scope: "Two DISTINCT graphs, each one sole partition with three replicas; no multipartition/cut/2PC acceptance", Processes: 6, StoreDirectory: dir}
	for _, leader := range []uint64{1, 4} {
		ns, voters, _ := processNamespace(leader)
		network := tinyGraphTransport{nodes: map[uint64]*graphProcess{voters[0]: all[voters[0]], voters[1]: all[voters[1]], voters[2]: all[voters[2]]}}
		network.event(t, leader, processRequest{op: processCampaign})
		init := graphInit(t)
		init.ns = ns
		init.schemas = []graphstate.PropertyDefinition{{Name: "answer", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}}
		wire, err := encodeGraphRequest(init, defaultMaterializerLimits())
		if err != nil {
			t.Fatal(err)
		}
		network.commit(t, voters[2], leader, wire, ns)
		controls := codecRequests(t)
		for _, r := range []request{controls[2], controls[3]} {
			r.ns = ns
			if r.kind == activateRecipient {
				r.home = ns.partition
			} else {
				r.sequence = 1
				r.count = 16
			}
			wire, err = encodeRequest(r, defaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			network.commit(t, voters[2], leader, wire, ns)
		}
		r := graphCodecRequest(t)
		r.ns = ns
		r.id = requestID{8}
		scope := r.operations[0].Scope
		r.operations = nil
		r.claims = nil
		grant := grantReference{session: codecSession(), sequence: 1}
		for _, node := range []struct{ entity, life uint64 }{{1, 2}, {3, 4}} {
			r.operations = append(r.operations, graphstate.Operation{Kind: graphstate.CreateNode, Owner: graphstate.EntityID(node.entity), Life: graphstate.LifeID(node.life), Scope: scope, Record: graphstate.EntityRecord{ID: graphstate.EntityID(node.entity), Kind: graphstate.Node, Axis: scope.Axis()}}, graphstate.Operation{Kind: graphstate.AddLabel, Owner: graphstate.EntityID(node.entity), Life: graphstate.LifeID(node.life), Scope: scope, Name: "X"}, graphstate.Operation{Kind: graphstate.Set, Owner: graphstate.EntityID(node.entity), Life: graphstate.LifeID(node.life), Scope: scope, Name: "answer", Value: graphstate.String("old"), ValueID: 5})
			r.claims = append(r.claims, freshBinding{role: entityBinding, id: node.entity, grant: grant}, freshBinding{role: lifeBinding, owner: graphstate.EntityID(node.entity), id: node.life, grant: grant})
		}
		r.claims = append(r.claims, freshBinding{role: valueBinding, id: 5, grant: grant})
		wire, err = encodeGraphRequest(r, defaultMaterializerLimits())
		if err != nil {
			t.Fatal(err)
		}
		created := network.commit(t, voters[2], leader, wire, ns)
		original, err := decodeAnyOutcome(created.outcome, ns)
		if err != nil {
			t.Fatal(err)
		}
		before := original.index
		r.id = requestID{9}
		r.revision, err = state.NewRevision(4, 0)
		if err != nil {
			t.Fatal(err)
		}
		r.operations = []graphstate.Operation{{Kind: graphstate.Close, Owner: 1, Life: 2, Scope: scope}, {Kind: graphstate.RemoveLabel, Owner: 3, Life: 4, Scope: scope, Name: "X"}, {Kind: graphstate.AddLabel, Owner: 3, Life: 4, Scope: scope, Name: "Y"}, {Kind: graphstate.Set, Owner: 3, Life: 4, Scope: scope, Name: "answer", Value: graphstate.String("new"), ValueID: 6}}
		r.claims = []freshBinding{{role: valueBinding, id: 6, grant: grant}}
		wire, err = encodeGraphRequest(r, defaultMaterializerLimits())
		if err != nil {
			t.Fatal(err)
		}
		changed := network.commit(t, voters[2], leader, wire, ns)
		last, err := decodeAnyOutcome(changed.outcome, ns)
		if err != nil {
			t.Fatal(err)
		}
		barrier := network.barrier(t, leader, []byte{byte(leader), 99}) // Not a certified database cut.
		if barrier < last.index {
			t.Fatal("barrier behind original mutation", barrier, last.index)
		}
		oracle := newTinyGraphOracle()
		oracle.created(before)
		oracle.changed(last.index)
		var currentRoot graphstore.Root
		for _, id := range voters {
			for _, version := range []uint64{before, last.index} {
				var active []uint64
				for _, entity := range []uint64{1, 3, 99} {
					got := all[id].call(t, processRequest{op: processProject, index: version, entity: entity})
					if got.applied < barrier {
						t.Fatal("replica has not applied barrier", id, got.applied, barrier)
					}
					if err := oracle.compare(version, entity, got.fact); err != nil {
						t.Fatal(id, version, entity, err)
					}
					if got.fact.Active {
						active = append(active, entity)
					}
					root, err := graphstore.DecodeRoot(got.image)
					if err != nil || root.Namespace().Graph != graphstate.GraphID(ns.graph) || root.Namespace().Partition != ns.partition {
						t.Fatal("wrong graph scope", id, root, err)
					}
					if version == last.index {
						if currentRoot.SemanticEpoch() == 0 {
							currentRoot = root
						} else if root.SemanticEpoch() != currentRoot.SemanticEpoch() || root.EffectDigest() != currentRoot.EffectDigest() {
							t.Fatal("logical root mismatch", id)
						}
					}
				}
				want := []uint64{1, 3}
				if version == last.index {
					want = []uint64{3}
				}
				if len(active) != len(want) {
					t.Fatal("exact active set", active, want)
				}
				for i := range want {
					if active[i] != want[i] {
						t.Fatal("exact active set", active, want)
					}
				}
			}
		}
		measurement.Packets += network.delivered
		measurement.MaxQueueCount = max(measurement.MaxQueueCount, network.maxQueueCount)
		measurement.MaxQueueBytes = max(measurement.MaxQueueBytes, network.maxQueueBytes)
	}
	for _, p := range all {
		measurement.MaxIPCFrameBytes = max(measurement.MaxIPCFrameBytes, p.maxFrame)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		err := p.shutdown(ctx, false)
		cancel()
		if err != nil {
			t.Fatal("child shutdown", err)
		}
	}
	measurement.ClosedStoreFileBytes, err = processStoreBytes(dir)
	if err != nil {
		t.Fatal("closed file-length budget", err)
	}
	measurement.WallSeconds = time.Since(start).Seconds()
	data, err := json.MarshalIndent(measurement, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "measurement.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("tiny six-process measurement: %s", data)
}
