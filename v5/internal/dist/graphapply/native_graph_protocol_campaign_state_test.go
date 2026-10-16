package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

type nativeProtocolStateCall struct {
	Member   uint64
	Input    nativeProtocolStateControl
	Response []byte
}
type nativeProtocolStateClient struct {
	t        *testing.T
	ctx      context.Context
	n        *certifiedProcessNetwork
	member   uint64
	cookie   [16]byte
	sequence uint64
	calls    []nativeProtocolStateCall
}

func (c *nativeProtocolStateClient) call(q nativeProtocolStateControl) (nativeProtocolStateReply, error) {
	c.t.Helper()
	q.Version = 1
	if q.Action != "inspect" {
		c.sequence++
		q.Sequence = c.sequence
		q.Cookie = c.cookie
	}
	wire, e := json.Marshal(q)
	if e != nil {
		return nativeProtocolStateReply{}, e
	}
	r, err := c.n.all[c.member].exchange(c.ctx, processRequest{op: processNativeTransitionControl, data: append([]byte(nativeProtocolStateMarker), wire...)})
	var reply nativeProtocolStateReply
	decode := nativeFeedDecodeJSON(r.aux, &reply)
	if decode != nil || reply.Profile != nativeProtocolStateProfile || reply.Version != 1 || reply.Action != q.Action || reply.Sequence != q.Sequence || reply.DeadlineUnixNanos != func() int64 { d, _ := c.ctx.Deadline(); return d.UnixNano() }() {
		return reply, errors.Join(errCorrupt, decode, err)
	}
	encoded, encodingErr := encodeProcessResponse(r)
	if encodingErr != nil {
		return reply, encodingErr
	}
	c.calls = append(c.calls, nativeProtocolStateCall{c.member, q, encoded})
	if q.Action == "inspect" {
		c.cookie = reply.Cookie
	}
	return reply, errors.Join(err, processResponseError(r))
}
func (c *nativeProtocolStateClient) must(q nativeProtocolStateControl) nativeProtocolStateReply {
	c.t.Helper()
	r, e := c.call(q)
	if e != nil {
		c.t.Fatal(q.Action, e, r.Result)
	}
	return r
}

type nativeProtocolStateRefusal struct {
	Kind                            string
	Proposal, Before, After, Record []byte
}

const nativeProtocolPooledStateProfile = "native-protocol-owned-pooled-state-transition-v1"

type nativeProtocolStatePoolFrame struct {
	Origin, Partition uint64
	Request, Reply    []byte
	Before, After     nativeProtocolStateCall
}
type nativeProtocolStatePooledEvidence struct {
	Version  uint8
	Semantic [32]byte
	Frames   []nativeProtocolStatePoolFrame
}

// One fresh observation owner surrounds the original production submissions.
// A lost service response stays absent. Its after-observation is deferred until
// the actual recovered original can be read before the next move submission.
type nativeProtocolPoolCapture struct {
	mu               sync.Mutex
	t                *testing.T
	ctx              context.Context
	n                *certifiedProcessNetwork
	hub              *graphProcessHub
	priorBefore      func(context.Context, uint64, uint64, []byte) error
	priorAfter       func(context.Context, uint64, uint64, []byte, []byte) error
	clients          map[uint64]*nativeProtocolStateClient
	processes        map[uint64]*graphProcess
	inspections      []nativeProtocolStateCall
	frames           []nativeProtocolStatePoolFrame
	pending          *nativeProtocolStatePoolFrame
	spent            uint64
	candidates, work uint64
	terminal         error
	output           *graphstate.OutputBudget
}

func newNativeProtocolPoolCapture(t *testing.T, ctx context.Context, n *certifiedProcessNetwork) *nativeProtocolPoolCapture {
	t.Helper()
	// Full diagnostic ownership is admitted before any graph/move effect;
	// actual retention grows only with admitted complete observations.
	if ctx.Err() != nil || nativeProtocolStateExpectedAgreement(t) != rangeMovePooledSemanticContractID() {
		t.Fatal("actual pooled capture admission", ctx.Err())
	}
	output, err := graphstate.NewOutputBudget(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if err = output.Reserve(16 << 20); err != nil {
		t.Fatal(err)
	}
	c := &nativeProtocolPoolCapture{t: t, ctx: ctx, n: n, clients: make(map[uint64]*nativeProtocolStateClient), processes: make(map[uint64]*graphProcess), output: output}
	c.attach(n.all[1].mux.hub)
	t.Cleanup(c.close)
	return c
}
func (c *nativeProtocolPoolCapture) attach(h *graphProcessHub) {
	c.hub = h
	h.mu.Lock()
	c.priorBefore, c.priorAfter = h.beforeRelay, h.afterRelay
	h.beforeRelay, h.afterRelay = c.before, c.after
	h.mu.Unlock()
}
func (c *nativeProtocolPoolCapture) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hub == nil {
		return
	}
	c.hub.mu.Lock()
	c.hub.beforeRelay, c.hub.afterRelay = c.priorBefore, c.priorAfter
	c.hub.mu.Unlock()
	c.hub = nil
}
func (c *nativeProtocolPoolCapture) proof(ctx context.Context, partition uint64, command, boundary []byte) (nativeProtocolStateCall, error) {
	operation, cancel := context.WithCancel(c.ctx)
	stop := context.AfterFunc(ctx, cancel)
	defer func() { stop(); cancel() }()
	member := c.n.leaders[0]
	if partition == 8 {
		member = c.n.leaders[1]
	} else if partition != 3 {
		return nativeProtocolStateCall{}, errInvalid
	}
	client := c.clients[member]
	fresh := client == nil || c.processes[member] != c.n.all[member]
	if fresh {
		client = &nativeProtocolStateClient{t: c.t, ctx: c.ctx, n: c.n, member: member}
	}
	originalContext := client.ctx
	client.ctx = operation
	defer func() { client.ctx = originalContext }()
	if fresh {
		if _, err := client.call(nativeProtocolStateControl{Action: "inspect"}); err != nil {
			return nativeProtocolStateCall{}, err
		}
		c.inspections = append(c.inspections, client.calls...)
		client.calls = nil
		c.clients[member], c.processes[member] = client, c.n.all[member]
	}
	priorCalls := len(client.calls)
	_, err := client.call(nativeProtocolStateControl{Action: "move-proof", Original: command, Boundary: boundary})
	if err != nil {
		return nativeProtocolStateCall{}, err
	}
	if len(client.calls) != priorCalls+1 {
		return nativeProtocolStateCall{}, errCorrupt
	}
	call := client.calls[priorCalls]
	client.calls = client.calls[:priorCalls]
	return call, nil // Only observation storage retires; native owner counters do not reset.
}
func nativeProtocolPoolBoundary(call nativeProtocolStateCall) ([]byte, error) {
	r, err := decodeProcessResponse(call.Response)
	if err != nil {
		return nil, err
	}
	var reply nativeProtocolStateReply
	if err = nativeFeedDecodeJSON(r.aux, &reply); err != nil {
		return nil, err
	}
	var proof nativeProtocolPoolProof
	if err = nativeFeedDecodeJSON(reply.Page, &proof); err != nil || !proof.Complete || len(proof.UndoIndices) == 0 || len(proof.UndoIndices) > 4 {
		return nil, errors.Join(errCorrupt, err)
	}
	var boundary []byte
	for _, index := range proof.UndoIndices {
		boundary = binary.BigEndian.AppendUint64(boundary, index)
	}
	return boundary, nil
}
func (c *nativeProtocolPoolCapture) finishPending(ctx context.Context, reply []byte) error {
	if c.pending == nil {
		return nil
	}
	request, err := decodeGraphSourceRequest(c.pending.Request)
	if err != nil {
		return err
	}
	proposal, err := decodeGraphSourceProposal(request.command)
	if err != nil {
		return err
	}
	boundary, err := nativeProtocolPoolBoundary(c.pending.Before)
	if err != nil {
		return err
	}
	after, err := c.proof(ctx, c.pending.Partition, proposal.wire, boundary)
	if err != nil {
		return err
	}
	c.pending.After, c.pending.Reply = after, bytes.Clone(reply)
	body, err := json.Marshal(c.pending)
	if err != nil || len(body)+1 > 512<<10 || uint64(len(body)+1) > (16<<20)-c.spent {
		return errors.Join(errLimit, err)
	}
	c.spent += uint64(len(body) + 1)
	c.frames = append(c.frames, *c.pending)
	c.pending = nil
	return nil
}
func (c *nativeProtocolPoolCapture) before(ctx context.Context, origin, partition uint64, wire []byte) error {
	if c.priorBefore != nil {
		if err := c.priorBefore(ctx, origin, partition, wire); err != nil {
			return err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminal != nil {
		return c.terminal
	}
	if err := errors.Join(ctx.Err(), c.ctx.Err()); err != nil {
		return err
	}
	if c.candidates >= 16384 || len(wire) > processFrameBytes {
		c.terminal = errLimit
		return errLimit
	}
	c.candidates++
	c.work += uint64(32*len(wire) + 4096) // Fixed finite whole-capture candidate/decode grant.
	request, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return err
	}
	if request.kind != sourceSubmit {
		return nil
	}
	proposal, err := decodeGraphSourceProposal(request.command)
	if err != nil {
		return err
	}
	if !isRangeMoveWire(proposal.wire) {
		return nil
	}
	if !bytes.HasPrefix(proposal.wire, []byte("GMQ\x03")) || len(wire) > 64<<10 || len(c.frames) >= 128 {
		return errLimit
	}
	// No later move effect can pass this hook before the preceding unknown
	// attempt has its exact immutable-original/current-control observation.
	if err = c.finishPending(ctx, nil); err != nil {
		c.terminal = err
		return err
	}
	before, err := c.proof(ctx, partition, proposal.wire, nil)
	if err != nil {
		c.terminal = err
		return err
	}
	owned := make([]byte, len(wire))
	copy(owned, wire)
	c.pending = &nativeProtocolStatePoolFrame{Origin: origin, Partition: partition, Request: owned, Before: before}
	return nil
}
func (c *nativeProtocolPoolCapture) after(ctx context.Context, origin, partition uint64, wire, reply []byte) error {
	c.mu.Lock()
	if c.pending != nil && bytes.Equal(c.pending.Request, wire) {
		if c.pending.Origin != origin || c.pending.Partition != partition {
			c.terminal = errCorrupt
		} else {
			c.terminal = c.finishPending(ctx, reply)
		}
	}
	err := c.terminal
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if c.priorAfter != nil {
		return c.priorAfter(ctx, origin, partition, wire, reply)
	}
	return nil
}
func (c *nativeProtocolPoolCapture) finish(a *nativeProtocolStateArtifact) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminal != nil {
		c.t.Fatal(c.terminal)
	}
	if err := c.finishPending(c.ctx, nil); err != nil {
		c.t.Fatal("unobserved exact pooled attempt", err)
	}
	a.Calls = append(a.Calls, c.inspections...)
	a.Pooled.Frames = c.frames
}

type nativeProtocolStateArtifact struct {
	Version                                   uint8
	Profile                                   string
	Recipe                                    nativeProtocolStateRecipe
	Run                                       [16]byte
	DeadlineUnixNanos                         int64
	Arm, Batch                                nativeBatchReceipt
	StateArm, State                           nativeStateReceipt
	Command, Intent                           []byte
	Calls                                     []nativeProtocolStateCall
	GC                                        []nativeRetentionReceipt
	BeforeGC, AfterGC                         [][]byte
	Before, Retained, Reopened                nativeMoveTransitionRead
	Processes                                 []pairedFaultProcess
	Original, Token                           []byte
	Business, BusinessReplay, BusinessAfterGC []byte
	LostRequest, LostReply                    []byte
	From, Through                             [2]uint64
	Records                                   []lifecycleRawRecord
	ReceiptReads, ReceiptGrantBytes           uint64
	Refusals                                  []nativeProtocolStateRefusal
	ChangedCaller, ChangedReply               []byte
	AllocatorBefore, AllocatorAfter           []byte
	Pooled                                    *nativeProtocolStatePooledEvidence  `json:",omitempty"`
	Program                                   *nativeProtocolStateProgramEvidence `json:",omitempty"`
	program                                   *nativeProtocolStateProgram
	capture                                   *nativeProtocolPoolCapture
	moveReserve                               allocationProposal
}

func nativeProtocolStateStart(t *testing.T) (context.Context, [16]byte, int64) {
	t.Helper()
	var run [16]byte
	if _, e := rand.Read(run[:]); e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(5 * time.Minute).UnixNano()
	t.Setenv("RHO_GRAPH_TRANSITION_RUN", hex.EncodeToString(run[:]))
	t.Setenv("RHO_GRAPH_TRANSITION_DEADLINE", strconv.FormatInt(deadline, 10))
	// Parent-side scope/command validation uses the same explicit agreement
	// selector as the actual child before Open. Bind the two base roles here too;
	// adding them only to startGraphProcess's extraEnv leaves this parent invalid.
	for _, k := range []string{"RHO_GRAPH_CERTIFIED_PROCESS", "RHO_GRAPH_SOURCE_PROCESS", "RHO_GRAPH_PAIRED_PROCESS", "RHO_GRAPH_PROTOCOL_STATE_PROCESS", "RHO_GRAPH_TRANSITION_PROCESS", "RHO_GRAPH_STABLE_STORAGE_PROCESS", "RHO_GRAPH_STABLE_STATE_PROCESS", "RHO_GRAPH_FAULT_FAMILY_PROCESS"} {
		t.Setenv(k, "1")
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	t.Cleanup(cancel)
	return ctx, run, deadline
}
func nativeProtocolStatePrepare(t *testing.T, n *certifiedProcessNetwork, actor, kind, index uint64, data []byte) allocationProposal {
	t.Helper()
	r := n.all[actor].call(t, processRequest{op: processCertifiedAction, entity: kind, index: index, data: data})
	p, e := decodeCertifiedProcessProposal(r.aux)
	if e != nil || p.target.semantic != nativeProtocolStateExpectedAgreement(t) {
		t.Fatal("actual span factory", e)
	}
	return p
}
func nativeProtocolStateSeed(t *testing.T, n *certifiedProcessNetwork) {
	t.Helper()
	graphE17SubmitLocal(t, n, 1, nativeProtocolStatePrepare(t, n, 1, certifiedRangeSet, 1, graphE17GrantInput(5, 7)), reasonNone)
	graphE17Cross(t, n, 4, nativeProtocolStatePrepare(t, n, 4, certifiedRangeSet, 17, graphE17GrantInput(19, 13)))
	graphE17Cross(t, n, 1, nativeProtocolStatePrepare(t, n, 1, certifiedCreateCrossEdge, 0, nil))
}
func nativeProtocolStateNewClient(t *testing.T, ctx context.Context, n *certifiedProcessNetwork) *nativeProtocolStateClient {
	c := &nativeProtocolStateClient{t: t, ctx: ctx, n: n, member: 2}
	c.must(nativeProtocolStateControl{Action: "inspect"})
	return c
}
func nativeProtocolStateRequest(run [16]byte, phase string, n uint64) [16]byte {
	return nativeFeedRequest(sha256.Sum256(run[:]), phase, n)
}

// A replacement is the actual same-volume native process. Its local restored
// image is recorded BEFORE campaign or fair catch-up; recipes remain inert.
func nativeProtocolStateRestart(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, killed pairedFaultProcess) []pairedFaultProcess {
	t.Helper()
	prior := f.n.all
	out := []pairedFaultProcess{killed}
	for id := uint64(1); id <= 6; id++ {
		if id != killed.Member {
			out = append(out, f.kill(id, "state-cohort"))
		}
	}
	all := make(map[uint64]*graphProcess)
	for id := uint64(1); id <= 6; id++ {
		all[id] = startGraphProcess(t, filepath.Join(f.n.dir, fmt.Sprint(id)), id, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_RANGE_PROCESS=1", "RHO_GRAPH_PROTOCOL_STATE_PROCESS=1", "RHO_GRAPH_PROCESS_REOPEN=1")
		f.n.retired = append(f.n.retired, prior[id])
	}
	f.n.all = all
	attachGraphProcessMuxes(t, all)
	f.n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: all[1], 2: all[2], 3: all[3]}}
	f.n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: all[4], 5: all[5], 6: all[6]}}
	f.mu.Lock()
	clear(f.down)
	f.mu.Unlock()
	f.installFilter()
	for id := uint64(1); id <= 6; id++ {
		state := all[id].call(t, processRequest{op: processStateOp})
		raw, e := encodeProcessResponse(state)
		if e != nil || all[id].incarnation == prior[id].incarnation || state.stats[statViews] != 0 || state.stats[statPinnedBytes] != 0 {
			t.Fatal("genuine restored process", e)
		}
		out = append(out, pairedFaultProcess{EventID: f.event(), Action: "reopen-before-campaign", Member: id, Instance: pairedGraphFaultInstance(id, all[id]), PreviousInstance: pairedGraphFaultInstance(id, prior[id]), Volume: filepath.Join(f.n.dir, fmt.Sprint(id)), Ready: true, Raw: raw, RecoveryRaw: raw})
	}
	for _, leader := range []uint64{1, 4} {
		allocationP08ElectFor(t, f.n, nativeProtocolStateExpectedAgreement(t), 0, leader)
	}
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
	return out
}

func nativeProtocolStateRecords(t *testing.T, n *certifiedProcessNetwork, from, through [2]uint64) []lifecycleRawRecord {
	t.Helper()
	var out []lifecycleRawRecord
	for g, leader := range n.leaders {
		if through[g] < from[g] || through[g]-from[g] > 256 {
			t.Fatal("finite native source prefix")
		}
		for i := from[g] + 1; i <= through[g]; i++ {
			r := n.all[leader].call(t, processRequest{op: processEntryRecord, index: i})
			part := uint64(3)
			if g == 1 {
				part = 8
			}
			out = append(out, lifecycleRawRecord{part, i, bytes.Clone(r.outcome), bytes.Clone(r.changes)})
		}
	}
	return out
}

func TestNativeProtocolStateInventoryAndOwnedDoorRefuseNominalAuthority(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	seen := make(map[nativeProtocolStateRecipe]bool)
	for _, r := range nativeProtocolStateInventory() {
		if !r.valid() || seen[r] {
			t.Fatal("duplicate/invalid state selector", r)
		}
		seen[r] = true
	}
	if len(seen) != 139 {
		t.Fatal("exact approved finite state inventory", len(seen))
	}
	var owner *nativeProtocolStateOwner
	if _, e := owner.serve(t.Context(), nil); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	if e := owner.close(); e != nil {
		t.Fatal(e)
	}
	good := nativeProtocolStateControl{Version: 1, Cookie: [16]byte{1}, Sequence: 1, Action: "close"}
	encode := func(c nativeProtocolStateControl) []byte {
		b, e := json.Marshal(c)
		if e != nil {
			t.Fatal(e)
		}
		return append([]byte(nativeProtocolStateMarker), b...)
	}
	if !nativeProtocolStateTerminalControl(processRequest{op: processNativeTransitionControl, data: encode(good)}) {
		t.Fatal("funded cleanup lost reserved lane")
	}
	for _, bad := range []nativeProtocolStateControl{{Version: 1, Action: "close"}, {Version: 1, Action: "close", Cookie: [16]byte{1}, Sequence: 1, Original: []byte{1}}, {Version: 1, Action: "page", Cookie: [16]byte{1}, Sequence: 1}} {
		if _, e := decodeNativeProtocolStateControl(encode(bad)); !errors.Is(e, errInvalid) {
			t.Fatal("inert bytes became authority", e)
		}
	}
	good.Action = "snapshot"
	if nativeProtocolStateTerminalControl(processRequest{op: processNativeTransitionControl, data: encode(good)}) {
		t.Fatal("normal source work spent terminal lane")
	}
	t.Setenv("RHO_GRAPH_PROTOCOL_STATE_PROCESS", "off")
	if ok, e := nativeProtocolStateProfileFromEnv(); e != nil || ok {
		t.Fatal("ordinary profile changed", e)
	}
	t.Setenv("RHO_GRAPH_PROTOCOL_STATE_PROCESS", "1")
	if _, e := nativeProtocolStateProfileFromEnv(); !errors.Is(e, errInvalid) {
		t.Fatal("missing actual immutable admission", e)
	}
}

func TestNativeProtocolStateP07P08P09ExactStableTransitions(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, recipe := range nativeProtocolStateInventory() {
		t.Run(fmt.Sprintf("%s-phase%d-p%d-page%d-kind%d-%s", recipe.Family, recipe.Phase, recipe.Partition, recipe.Page, recipe.Kind, recipe.Side), func(t *testing.T) {
			artifact := nativeProtocolStateCase(t, recipe)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "..", "tools", "v5-substrate", "native_protocol_campaign_state_check.py"), artifact)
			var diagnostics processStderr
			command.Stdout, command.Stderr = &diagnostics, &diagnostics
			if e := command.Run(); e != nil {
				body, _ := diagnostics.state()
				t.Fatal("independent state-transition original/cut/feed oracle", e, body)
			}
			if _, overflow := diagnostics.state(); overflow {
				t.Fatal("bounded checker output")
			}
		})
	}
}

func TestNativeProtocolPooledStateTransitionsKeepActualUndoOriginals(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for index, recipe := range nativeProtocolStateInventory() {
		if testing.Short() && index != 0 && index != 15 && index != 31 && index != 47 && index != 67 && index != 68 && index != 99 && index != 127 && index != 128 && index != 136 && index != 138 {
			continue
		}
		t.Run(fmt.Sprintf("%s-phase%d-p%d-page%d-kind%d-%s", recipe.Family, recipe.Phase, recipe.Partition, recipe.Page, recipe.Kind, recipe.Side), func(t *testing.T) {
			t.Setenv("RHO_GRAPH_PROTOCOL_STATE_RECOVERY", "4")
			artifact, deadline := nativeProtocolStateCaseAt(t, recipe)
			ctx, cancel := nativeProtocolStateCheckerContext(t, deadline)
			defer cancel()
			command := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "..", "tools", "v5-substrate", "native_protocol_campaign_state_check.py"), artifact)
			var diagnostics processStderr
			command.Stdout, command.Stderr = &diagnostics, &diagnostics
			if e := command.Run(); e != nil {
				body, _ := diagnostics.state()
				t.Fatal("independent pooled original/debt/state oracle", e, body)
			}
			if _, overflow := diagnostics.state(); overflow {
				t.Fatal("bounded independent diagnostics")
			}
		})
	}
}

func TestNativeProtocolPooledProofDoorKeepsCanonicalBoundedNativeKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	encode := func(c nativeProtocolStateControl) []byte {
		body, e := json.Marshal(c)
		if e != nil {
			t.Fatal(e)
		}
		return append([]byte(nativeProtocolStateMarker), body...)
	}
	q := nativeProtocolStateControl{Version: 1, Cookie: [16]byte{1}, Sequence: 1, Action: "move-proof", Original: []byte("GMQ\x03")}
	for _, indices := range [][]uint64{{0}, {1}, {0, 1, 2, 128}} {
		q.Boundary = nil
		for _, index := range indices {
			q.Boundary = binary.BigEndian.AppendUint64(q.Boundary, index)
		}
		if got, e := decodeNativeProtocolStateControl(encode(q)); e != nil || !bytes.Equal(got.Boundary, q.Boundary) {
			t.Fatal("canonical same native key window", e)
		}
		if _, e := nativeProtocolStateMoveProof(t.Context(), nil, q); !errors.Is(e, errInvalid) {
			t.Fatal("inert command/window recreated native source", e)
		}
	}
	for _, boundary := range [][]byte{{1}, make([]byte, 16), binary.BigEndian.AppendUint64(nil, 129), make([]byte, 40)} {
		q.Boundary = boundary
		if _, e := decodeNativeProtocolStateControl(encode(q)); !errors.Is(e, errInvalid) {
			t.Fatal("ambiguous/truncated/oversized native window admitted", e)
		}
	}
	q.Boundary = nil
	q.Action = "close"
	if _, e := decodeNativeProtocolStateControl(encode(q)); !errors.Is(e, errInvalid) {
		t.Fatal("diagnostic bytes borrowed terminal cleanup lane", e)
	}
}

func nativeProtocolStateCase(t *testing.T, recipe nativeProtocolStateRecipe, programs ...nativeProtocolProgramSpec) string {
	path, _ := nativeProtocolStateCaseAt(t, recipe, programs...)
	return path
}
func nativeProtocolStateCaseAt(t *testing.T, recipe nativeProtocolStateRecipe, programs ...nativeProtocolProgramSpec) (string, int64) {
	t.Helper()
	if !recipe.valid() {
		t.Fatal("exact finite recipe")
	}
	ctx, run, deadline := nativeProtocolStateStart(t)
	if recipe.Family == "allocation" {
		t.Setenv("RHO_GRAPH_ALLOCATION_PROCESS", "1")
	}
	n, bootstrap := nativeProtocolStateNetwork(t, recipe.Family == "allocation")
	f := &pairedGraphFaultNetwork{t: t, n: n, bootstrapAgreement: bootstrap, down: make(map[uint64]bool), intentionalKills: make(map[*graphProcess]bool)}
	f.installFilter()
	a := nativeProtocolStateArtifact{Version: 1, Profile: nativeProtocolStateProfile, Recipe: recipe, Run: run, DeadlineUnixNanos: deadline}
	var pooled *nativeProtocolPoolCapture
	if nativeProtocolStateExpectedAgreement(t) == rangeMovePooledSemanticContractID() {
		f.parent = ctx
		a.Version, a.Profile = 2, nativeProtocolPooledStateProfile
		a.Pooled = &nativeProtocolStatePooledEvidence{Version: 1, Semantic: rangeMovePooledSemanticContractID()}
		if len(programs) == 1 {
			a.Version, a.Profile = 3, nativeProtocolStateProgramProfile
			if recipe.Family == "move" {
				// The allocator still belongs to actual leader/session1 here.
				// Keep its genuine proposal, never its private proof, across
				// the program's later election to a different native leader.
				n.read(t, 1, observeRecipient, 1)
				n.read(t, 1, observeAllocator, 0)
				a.moveReserve = nativeProtocolStatePrepare(t, n, 1, certifiedReserve, 0, graphE17GrantInput(3, 4))
			}
			a.program = newNativeProtocolStateProgram(t, ctx, f, recipe, programs[0])
		} else if len(programs) != 0 {
			t.Fatal("one finite state program")
		}
		pooled = newNativeProtocolPoolCapture(t, ctx, n)
		a.capture = pooled
	} else if len(programs) != 0 {
		t.Fatal("state programs require actual pooled profile")
	}
	if recipe.Family == "allocation" {
		nativeProtocolStateAllocation(t, ctx, f, &a)
	} else {
		nativeProtocolStateGraph(t, ctx, f, &a)
	}
	a.Through = f.indexes()
	a.ReceiptGrantBytes = a.ReceiptReads * (64 << 10)
	if len(a.Records) == 0 {
		a.Records = nativeProtocolStateRecords(t, n, a.From, a.Through)
	}
	if pooled != nil {
		pooled.finish(&a)
		pooled.close()
	}
	if a.program != nil {
		a.Program = a.program.finish()
	}
	if ctx.Err() != nil {
		t.Fatal("same original campaign deadline", ctx.Err())
	}
	return nativeProtocolStateWrite(t, filepath.Join(n.dir, "native-protocol-state.jsonl"), a), deadline
}
func nativeProtocolStateAllocation(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, a *nativeProtocolStateArtifact) {
	t.Helper()
	n := f.n
	alias, request := allocationP08Acquire(t, n, 3, 1)
	allocationP08Mint(t, n, 3, alias, 1)
	started := allocationP08Call(t, n.all[3], processRequest{op: processP08BeginAcquire, index: 2, entity: 16})
	next, e := decodeAllocationP08Request(started.aux)
	if e != nil || next.Session != request.Session {
		t.Fatal("same native cursor owner", e)
	}
	proposal := allocationP08Reserve(t, n, 3, next)
	if commandKind(a.Recipe.Phase) == installDeclaredGrant {
		if got := allocationP08SubmitControl(t, n, proposal); got.reason != reasonNone {
			t.Fatal("real prerequisite reservation", got)
		}
		proposal = allocationP08InstallFor(t, n, 3, 3, next)
	}
	gate, e := newNativeProtocolStateGate(ctx, f, a.Recipe)
	if e != nil {
		t.Fatal(e)
	}
	if e = gate.selectProposal(ctx, proposal); e != nil {
		t.Fatal(e)
	}
	wire, e := encodeCertifiedProcessProposal(proposal)
	if e != nil {
		t.Fatal(e)
	}
	a.From = f.indexes()
	nativeProtocolStateGroupEvent(t, ctx, n, gate, 3, processRequest{op: processPropose, data: wire})
	receipt, e := gate.read()
	if e != nil {
		t.Fatal("actual allocation native storage transition", e)
	}
	a.Arm, a.Batch, a.Command, a.ReceiptReads = gate.arm, receipt, bytes.Clone(gate.command), gate.reads
	killed := nativeBatchReap(t, f, gate.owner.Member, receipt)
	gate.close()
	a.Processes = nativeProtocolStateRestart(t, ctx, f, killed)
	// The old local cursor handle is not an imported cursor. No ID is issued from
	// a copied journal after the recipient process died, regardless of tail fate.
	refused := allocationP08Call(t, n.all[3], processRequest{op: processP08Mint, index: alias})
	if id, e := decodeAllocationP08Result(refused.aux); id != 0 || !errors.Is(e, errInvalid) {
		t.Fatal("retired cursor reconstructed authority", id, e)
	}
	// Recover the identical native original through its ordinary Submit path;
	// renewed authority/source reads cannot change payload or recycle a range.
	if got := allocationP08SubmitControl(t, n, proposal); got.reason != reasonNone {
		t.Fatal("same allocation original", got)
	}
	n.read(t, 1, observeAllocator, 0)
	observed := allocationP08Observe(t, n, 1, &next)
	high, e := idalloc.Inspect(&observed.allocator)
	if e != nil || high.HighWater != 48 || !observed.found || observed.grant.grant.Reservation.First != 33 || observed.grant.grant.Reservation.Last != 48 {
		t.Fatal("exact durable nonoverlapping reservation", high, observed, e)
	}
	a.Business = p06ProcessWire(t, n.all[1].call(t, processRequest{op: processEntryRecord, index: observed.grant.source.index}))
	a.BusinessReplay = p06ProcessWire(t, n.all[1].call(t, processRequest{op: processEntryRecord, index: observed.index}))
}

func nativeProtocolStateGraph(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, a *nativeProtocolStateArtifact) {
	t.Helper()
	n := f.n
	a.From = [2]uint64{}
	nativeProtocolStateSeed(t, n)
	a.Before = nativeTransitionMoveReadFor(t, ctx, n, 2, graphReadFresh, graphCut{}, nativeProtocolStateExpectedAgreement(t))
	cut, e := pairedFaultCut(a.Before.Cut)
	if e != nil {
		t.Fatal(e)
	}
	client := nativeProtocolStateNewClient(t, ctx, n)
	if a.capture != nil {
		// A rotated A source is also the existing feed's actual child2. Share
		// its genuine client/sequence, never create a second observer ledger.
		a.capture.mu.Lock()
		a.capture.clients[2] = client
		a.capture.processes[2] = n.all[2]
		a.capture.mu.Unlock()
	}
	begin := client.must(nativeProtocolStateControl{Action: "begin", Request: nativeProtocolStateRequest(a.Run, "snapshot-feed", 0)})
	a.Original = bytes.Clone(begin.Original)
	if a.Recipe.Family == "publication-race" {
		definition, err := decodeRetainedFeedDefinition(a.Original)
		if err != nil {
			t.Fatal(err)
		}
		manifest, err := decodeGraphPinManifest(definition.manifest)
		if err != nil || len(manifest.parts) != 2 {
			t.Fatal("actual unpublished capture", err)
		}
		var captured graphCut
		for _, part := range manifest.parts {
			captured.certificates = append(captured.certificates, part.certificate)
		}
		nativeProtocolStatePublicationRace(t, ctx, f, a, client, captured)
		return
	}
	publish := a.Recipe.Family != "pin" || a.Recipe.Phase != pinPublish && a.Recipe.Phase != pinIssue && a.Recipe.Phase != pinCancelIssue
	if publish {
		r := client.must(nativeProtocolStateControl{Action: "publish"})
		a.Token = bytes.Clone(r.Token)
		client.must(nativeProtocolStateControl{Action: "snapshot"})
	}
	if a.Recipe == (nativeProtocolStateRecipe{Family: "move", Phase: byte(rangeMoveFence), Partition: 3, Kind: raftlog.StableStorageReady, Side: "before"}) {
		nativeProtocolStateTokenGuards(t, ctx, f, a, client)
	}
	if a.Recipe.Family == "pin" && a.Recipe.Phase == pinCancelIssue {
		client.must(nativeProtocolStateControl{Action: "issue"})
	}
	// Genuine K is installed after C. Hold its actual committed Decide reply at
	// the existing source relay; cancel the original caller before releasing it.
	k := nativeProtocolStatePrepare(t, n, 4, certifiedHealthyLabel, 1, nil)
	// The legacy gate owns a temporary callback window and clears both hooks
	// on release. A fresh pooled observer must resume the SAME saved callbacks,
	// not silently lose the later original/undo trace or acquire a new ledger.
	var restore func() error
	if a.program != nil {
		a.program.beginBusiness(k)
	}
	var entered <-chan pairedFaultBoundary
	var release func()
	if a.Pooled != nil {
		entered, release, restore = nativeProtocolStateCommittedGate(f)
	} else {
		entered, release = f.gate(5, true)
	}
	done, stop := f.beginRun(5, k)
	boundary := f.awaitBoundary(entered)
	if len(boundary.Request) == 0 || len(boundary.Reply) == 0 {
		t.Fatal("actual lost committed K reply")
	}
	a.LostRequest, a.LostReply = boundary.Request, boundary.Reply
	stop()
	release()
	if a.Pooled != nil {
		if err := restore(); err != nil {
			t.Fatal("LIFO committed-reply callback restore", err)
		}
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Complete ONLY the same original after the unknown reply, through the real
	// original lookup/coordinator, retaining exact result identity for later move.
	recovered := n.all[5].call(t, processRequest{op: processSourceCoordinate, data: k.wire})
	original, e := decodeGraphProcessCoordinatorResult(recovered)
	if e != nil || !original.decision.commit {
		t.Fatal("genuine K decision recovery", e)
	}
	a.Business = p06ProcessWire(t, recovered)
	if a.Recipe.Family == "move" {
		nativeProtocolStateMove(t, ctx, f, a, client)
	}
	if a.Recipe.Family == "pin" {
		nativeProtocolStatePin(t, ctx, f, a, client)
	}
	if a.Recipe.Family == "gc" {
		nativeProtocolStateGCTransition(t, ctx, f, a, client, cut)
	}
	replay := nativeProtocolStateReplayBarrier(t, f, k, original)
	same, e := decodeGraphProcessCoordinatorResult(replay)
	if e != nil || same.decision != original.decision || same.resume != original.resume {
		t.Fatal("move/restart changed immutable K", e)
	}
	a.BusinessReplay = p06ProcessWire(t, replay)
	// A genuine Host factory changes only provenance under K's exact request.
	// Run must authenticate the old original before refusing this caller body.
	encodedK, err := encodeCertifiedProcessProposal(k)
	if err != nil {
		t.Fatal(err)
	}
	changed := n.all[4].call(t, processRequest{op: processP08ChangedGraph, data: encodedK})
	changedProposal, err := decodeCertifiedProcessProposal(changed.aux)
	if err != nil {
		t.Fatal(err)
	}
	beforeChanged := f.indexes()
	changedResponse, actual := n.all[5].exchange(ctx, processRequest{op: processSourceCoordinate, data: changedProposal.wire})
	if refusal := errors.Join(actual, processResponseError(changedResponse)); !errors.Is(refusal, errInvalid) {
		t.Fatalf("changed K body lacks required invalid refusal: exchange=%T graphMask=%04x error=%v processCode=%d processMask=%04x detail=%.256q auxBytes=%d before=%v", actual, graphErrorMask(actual), actual, changedResponse.code, changedResponse.causeMask, changedResponse.detail, len(changedResponse.aux), beforeChanged)
	}
	if afterChanged := f.indexes(); beforeChanged != afterChanged {
		t.Fatalf("changed K body advanced native prefix despite invalid refusal: before=%v after=%v exchange=%T graphMask=%04x processCode=%d processMask=%04x", beforeChanged, afterChanged, actual, graphErrorMask(actual), changedResponse.code, changedResponse.causeMask)
	}
	a.ChangedCaller, a.ChangedReply = bytes.Clone(changedProposal.wire), p06ProcessWire(t, changedResponse)
	if a.Recipe.Family == "pin" && (a.Recipe.Phase == pinRelease || a.Recipe.Phase == pinExpire || a.Recipe.Phase == pinClose || a.Recipe.Phase == pinAdvanceEpoch || a.Recipe.Phase == pinCancelIssue) {
		a.Retained = nativeTransitionMoveReadFor(t, ctx, n, 2, graphReadAt, cut, nativeProtocolStateExpectedAgreement(t))
	} else {
		if len(a.Token) == 0 {
			r := client.must(nativeProtocolStateControl{Action: "publish"})
			a.Token = bytes.Clone(r.Token)
		}
		snap := client.must(nativeProtocolStateControl{Action: "snapshot"})
		if snap.Snapshot == nil {
			t.Fatal("actual same native owner lost snapshot")
		}
		client.must(nativeProtocolStateControl{Action: "snapshot-close"})
		a.Retained = nativeTransitionMoveReadFor(t, ctx, n, 2, graphReadAt, cut, nativeProtocolStateExpectedAgreement(t))
	}
	if !reflect.DeepEqual(a.Before, a.Retained) {
		t.Fatal("current substitution for original C")
	}
	// Full raw source prefix is captured before deletion. Its retained diagnostics
	// are evidence only; they cannot re-open a native cut or reconstruct a pin.
	beforeGC := f.indexes()
	captureFrom := a.From
	if len(a.Records) != 0 {
		captureFrom = a.Through
	}
	a.Records = append(a.Records, nativeProtocolStateRecords(t, n, captureFrom, beforeGC)...)
	if a.Recipe.Family == "move" && a.Recipe.Phase != byte(rangeMoveAbort) {
		positions := nativeProtocolStateDrain(t, ctx, f, a, client)
		// Admit/export the complete drain prefix before physical sidecar deletion.
		drained := f.indexes()
		a.Records = append(a.Records, nativeProtocolStateRecords(t, n, beforeGC, drained)...)
		beforeGC = drained
		nativeProtocolStateCollect(t, ctx, f, a, client, cut, positions, nil)
	}
	if a.Recipe.Family == "gc" || a.Recipe.Family == "move" && a.Recipe.Phase != byte(rangeMoveAbort) {
		postGC := n.all[5].call(t, processRequest{op: processSourceCoordinate, data: k.wire})
		result, err := decodeGraphProcessCoordinatorResult(postGC)
		if err != nil || result.resume != original.resume || result.decision != original.decision {
			t.Fatal("actual GC lost immutable K result/IDs", err)
		}
		a.BusinessAfterGC = p06ProcessWire(t, postGC)
		// Same production owner/pin, a new actual completed physical tip, and
		// different Page originals. No saved diagnostic bytes create a source.
		nativeProtocolStateDrainFor(t, ctx, f, a, client, "post-collection-feed")
	}
	a.Reopened = nativeTransitionMoveReadFor(t, ctx, n, 2, graphReadAt, cut, nativeProtocolStateExpectedAgreement(t))
	if !reflect.DeepEqual(a.Before, a.Reopened) {
		t.Fatal("old pinned C lost after actual eligible GC")
	}
	if a.Recipe.Family == "move" || a.Recipe.Family == "gc" || a.Recipe.Family == "pin" && (a.Recipe.Phase == pinPublish || a.Recipe.Phase == pinIssue || a.Recipe.Phase == pinAdvanceFeed) {
		client.must(nativeProtocolStateControl{Action: "release"})
	}
	client.must(nativeProtocolStateControl{Action: "close"})
	a.Calls = append(a.Calls, client.calls...)
	after := f.indexes()
	a.Records = append(a.Records, nativeProtocolStateRecords(t, n, beforeGC, after)...)
}

func nativeProtocolStateWaitMoveBoundary(ctx context.Context, lost <-chan struct{}, done <-chan graphProcessAnswer) (*graphProcessAnswer, error) {
	var completed *graphProcessAnswer
	for {
		select {
		case <-lost:
			return completed, nil
		case actual := <-done:
			completed = &actual
			// Native loss may already be authenticated when the caller retires.
			select {
			case <-lost:
				return completed, nil
			default:
			}
			err := errors.Join(actual.err, processResponseError(actual.response))
			if err == nil {
				_, err = decodeGraphE17MoveResult(actual.response)
			}
			if err != nil {
				return completed, err
			}
			// A successful caller may precede the selected follower's delivery.
			// Keep its actual reply while waiting under the ORIGINAL deadline.
			done = nil
		case <-ctx.Done():
			return completed, ctx.Err()
		}
	}
}

func nativeProtocolStateMove(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, a *nativeProtocolStateArtifact, c *nativeProtocolStateClient) {
	t.Helper()
	n := f.n
	// Preserve real old-owner factories across Fence; no authority is inferred
	// from these saved bytes. A later allocator epoch transfer fences the saved
	// genuine reservation independently of graph placement.
	delayed := nativeProtocolStatePrepare(t, n, 1, certifiedHealthyLabel, 1, nil)
	oldReserve := a.moveReserve
	if a.program == nil {
		// Literal fixtures have not rotated: preserve their original factory
		// order and actual owner1 proof acquisition.
		n.read(t, 1, observeRecipient, 1)
		n.read(t, 1, observeAllocator, 0)
		oldReserve = nativeProtocolStatePrepare(t, n, 1, certifiedReserve, 0, graphE17GrantInput(3, 4))
	} else if len(oldReserve.wire) == 0 {
		t.Fatal("missing genuine pre-election allocator proposal")
	}
	factory := nativeFaultFamilyCall(t, n.all[2], nativeFaultFamilyControl{Version: 1, Action: "inspect"})
	owner := nativeLogicalTransitionCall(t, n.all[2], nativeLogicalTransitionControl{Version: 1, Action: "inspect"})
	pages := graphE17AdmittedMovePages
	if a.Recipe.Phase == byte(rangeMoveAbort) {
		pages = 1
	}
	input := nativeTransitionMovePrepare{Version: 1, Cookie: owner.Cookie, FamilyCookie: factory.Cookie, RangeID: 1, Target: 8, Pages: pages, Rows: 8, Bytes: 16 << 10}
	body, e := json.Marshal(input)
	if e != nil {
		t.Fatal(e)
	}
	prepared := n.all[2].call(t, processRequest{op: processNativeTransitionControl, data: append([]byte(nativeTransitionMovePrepareMarker), body...)})
	initial, e := decodeGraphE17MoveResult(prepared)
	if !errors.Is(e, errRangeMovePending) {
		t.Fatal("actual source-owned move factory", e)
	}
	gate, e := newNativeProtocolStateGate(ctx, f, a.Recipe, a.program)
	if e != nil {
		t.Fatal(e)
	}
	operation, stop := context.WithCancel(ctx)
	done := make(chan graphProcessAnswer, 1)
	go func() {
		r, e := n.all[2].exchange(operation, processRequest{op: processRangeResume, data: prepared.aux})
		done <- graphProcessAnswer{response: r, err: e}
	}()
	early, waitErr := nativeProtocolStateWaitMoveBoundary(ctx, gate.lost, done)
	if waitErr != nil {
		stop()
		gate.close()
		t.Fatal("move invocation failed before required actual native boundary", waitErr)
	}
	receipt, e := gate.read()
	if e != nil {
		t.Fatal(e)
	}
	a.Arm, a.Batch, a.Command, a.ReceiptReads = gate.arm, receipt, bytes.Clone(gate.command), gate.reads
	command, e := decodeRangeMoveCommand(a.Command)
	if e != nil {
		t.Fatal(e)
	}
	a.Intent = bytes.Clone(command.intent)
	catchup, e := gate.moveCatchup(receipt)
	if e != nil {
		t.Fatal("actual owned move catch-up admission", e)
	}
	gate.mu.Lock()
	a.ReceiptReads = gate.reads
	gate.mu.Unlock()
	killed := nativeBatchReap(t, f, gate.owner.Member, receipt)
	stop()
	var pending processResponse
	if early != nil {
		pending = early.response
	} else {
		select {
		case r := <-done:
			pending = r.response
		case <-ctx.Done():
			t.Fatal("retire actual move caller", ctx.Err())
		}
	}
	gate.close()
	prior := n.all[gate.owner.Member]
	f.nativeMoveCatchup = catchup
	rejoined := func() pairedFaultProcess {
		defer func() { f.nativeMoveCatchup = nil }()
		return f.replaceBeforeHeal(gate.owner.Member, func(_ *graphProcess, _ processResponse) {
			observed := nativeTransitionMoveReadFor(t, ctx, n, 2, graphReadAt, func() graphCut {
				cut, e := pairedFaultCut(a.Before.Cut)
				if e != nil {
					t.Fatal(e)
				}
				return cut
			}(), nativeProtocolStateExpectedAgreement(t))
			if !reflect.DeepEqual(a.Before, observed) {
				t.Fatal("reopen replaced original pinned snapshot")
			}
		})
	}()
	n.retired = append(n.retired, prior)
	a.Processes = append(a.Processes, killed, rejoined)
	// The actual factory and original feed owner remain alive in child2. Neither
	// the pending result nor the copied intent recreates a lost factory stamp.
	saved := prepared.aux
	if result, e := decodeGraphE17MoveResult(pending); e == nil || errors.Is(e, errRangeMovePending) {
		if result.resume == initial.resume {
			saved = pending.aux
		}
	}
	var completed rangeMoveResult
	var actual error
	for range graphE17AdmittedMovePages {
		response, transport := n.all[2].exchange(ctx, processRequest{op: processRangeResume, data: saved})
		completed, actual = decodeGraphE17MoveResult(response)
		actual = errors.Join(actual, transport)
		if completed.resume != initial.resume {
			t.Fatal("foreign move original after native storage loss")
		}
		saved = response.aux
		if !errors.Is(actual, errRangeMovePending) {
			break
		}
		if nativeTransitionPinErrorClass(actual) != "pending-or-unavailable" && !graphE17PermittedResume(actual, nil) {
			t.Fatal("unexpected original move failure", actual)
		}
	}
	if a.Recipe.Phase == byte(rangeMoveAbort) {
		if completed.committed || !errors.Is(actual, errLimit) || completed.source.phase != rangeMoveAbort || completed.target.phase != rangeMoveAbort {
			t.Fatal("real bounded original ABORT", actual)
		}
	} else if actual != nil || !completed.committed || completed.source.phase != rangeMoveFinish || completed.target.phase != rangeMoveFinish {
		t.Fatal("real durable handoff", actual)
	}
	// Same original replay can return the same terminal only; changing bytes is
	// still rejected by the production manifest/intent binding.
	replay, transport := n.all[2].exchange(ctx, processRequest{op: processRangeResume, data: saved})
	same, err := decodeGraphE17MoveResult(replay)
	if same != completed || transport != nil && !errors.Is(transport, errLimit) || err != nil && !errors.Is(err, errLimit) {
		t.Fatal("same terminal move changed", err, transport)
	}
	if a.Recipe.Phase != byte(rangeMoveAbort) {
		nativeProtocolStateRefuse(t, f, a, "old-owner-write", delayed, reasonRemoteParticipant)
		allocator := uint64(1)
		if a.program != nil {
			allocator = n.leaders[0]
		}
		n.read(t, allocator, observeAllocator, 0)
		a.AllocatorBefore = p06ProcessWire(t, n.all[allocator].call(t, processRequest{op: processP08Metadata}))
		nativeProtocolStateAction(t, n, allocator, certifiedP08Transfer, 0, nil)
		nativeProtocolStateRefuse(t, f, a, "old-allocator-epoch", oldReserve, reasonStale)
		a.AllocatorAfter = p06ProcessWire(t, n.all[allocator].call(t, processRequest{op: processP08Metadata}))
		oldState := allocationP08Observe(t, n, allocator, nil)
		inspection, err := idalloc.Inspect(&oldState.allocator)
		if err != nil || inspection.HighWater != 32 {
			t.Fatal("stale reserve changed allocation high water", err)
		}
	}
	if len(a.Token) > 0 {
		snap := c.must(nativeProtocolStateControl{Action: "snapshot"})
		if snap.Snapshot == nil {
			t.Fatal("genuine held C owner lost")
		}
	}
}
func nativeProtocolStateRefuse(t *testing.T, f *pairedGraphFaultNetwork, a *nativeProtocolStateArtifact, kind string, proposal allocationProposal, want reason) {
	t.Helper()
	wire, err := encodeCertifiedProcessProposal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	leader := uint64(1)
	if a.program != nil {
		leader = f.n.leaders[0]
	}
	before := f.n.all[1].call(t, processRequest{op: processStateOp})
	f.n.groups[0].event(t, leader, processRequest{op: processPropose, data: wire})
	// The fair native packet drain above catches up this same original voter;
	// recording it does not grant that follower submission/read authority.
	after := f.n.all[1].call(t, processRequest{op: processStateOp})
	record := f.n.all[1].call(t, processRequest{op: processEntryRecord, index: after.applied})
	var outcome outcome
	if kind == "old-owner-write" {
		outcome, err = decodePartitionGraphOutcome(record.outcome, namespace{graph: proposal.target.graph, partition: proposal.target.partition})
	} else {
		outcome, err = decodeDeclaredOutcome(record.outcome, namespace{graph: proposal.target.graph, partition: proposal.target.partition})
	}
	if err != nil || outcome.reason != want || outcome.hash != sha256.Sum256(proposal.wire) || outcome.index != after.applied || after.applied <= before.applied || len(record.changes) != 0 || !bytes.Equal(before.image, after.image) {
		t.Fatal("old authority acknowledged success or changed native state", kind, err, outcome)
	}
	a.Refusals = append(a.Refusals, nativeProtocolStateRefusal{kind, bytes.Clone(proposal.wire), p06ProcessWire(t, before), p06ProcessWire(t, after), p06ProcessWire(t, record)})
}

func nativeProtocolStatePin(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, a *nativeProtocolStateArtifact, c *nativeProtocolStateClient) {
	t.Helper()
	if a.Recipe.Phase == pinExpire {
		c.must(nativeProtocolStateControl{Action: "snapshot-close"})
		c.must(nativeProtocolStateControl{Action: "epoch"})
	}
	if a.Recipe.Phase == pinClose || a.Recipe.Phase == pinRelease || a.Recipe.Phase == pinAdvanceEpoch {
		c.must(nativeProtocolStateControl{Action: "snapshot-close"})
	}
	var page nativeProtocolStateReply
	request := nativeProtocolStateRequest(a.Run, "cursor-across-move", 0)
	if a.Recipe.Phase == pinAdvanceFeed {
		boundary := nativeScalarWorkloadCut(t, ctx, f.n, graphReadFresh, graphCut{}, nativeProtocolStateExpectedAgreement(t))
		wire, e := nativeFeedCutWire(boundary)
		if e != nil {
			t.Fatal(e)
		}
		page = c.must(nativeProtocolStateControl{Action: "page", Request: request, Boundary: wire})
		// The real pending vector is allocated before Fence; a complete genuine move
		// precedes ACK. This is the P09 move/cursor race, not a direct cursor label.
		nativeProtocolStateWholeMove(t, ctx, f)
	}
	gate, e := newNativeProtocolStateGate(ctx, f, a.Recipe, a.program)
	if e != nil {
		t.Fatal(e)
	}
	actions := [8]string{"publish", "ack", "release", "expire", "pin-close", "epoch", "issue", "cancel"}
	input := nativeProtocolStateControl{Action: actions[a.Recipe.Phase-1]}
	if a.Recipe.Phase == pinAdvanceFeed {
		input.Request, input.Digest = request, page.Digest
	}
	done := make(chan error, 1)
	go func() { _, e := c.call(input); done <- e }()
	select {
	case <-gate.lost:
	case <-ctx.Done():
		gate.close()
		t.Fatal("required pin phase never matched actual native storage", ctx.Err())
	}
	receipt, e := gate.read()
	if e != nil {
		t.Fatal(e)
	}
	a.Arm, a.Batch, a.Command, a.ReceiptReads = gate.arm, receipt, bytes.Clone(gate.command), gate.reads
	killed := nativeBatchReap(t, f, gate.owner.Member, receipt)
	select {
	case e := <-done:
		if e != nil && nativeTransitionPinErrorClass(e) != "pending-or-unavailable" {
			t.Fatal("actual pin error classification", e)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	gate.close()
	prior := f.n.all[gate.owner.Member]
	rejoined := f.replaceBeforeHeal(gate.owner.Member, nil)
	f.n.retired = append(f.n.retired, prior)
	a.Processes = append(a.Processes, killed, rejoined)
	// A new finite Resume authenticates the same native original. The observer
	// cookie and the saved JSON body confer no current live-owner authority.
	c.must(nativeProtocolStateControl{Action: "close"})
	live := a.Recipe.Phase == pinPublish || a.Recipe.Phase == pinIssue || a.Recipe.Phase == pinAdvanceFeed
	if !live && len(a.Token) != 0 {
		refused, err := c.call(nativeProtocolStateControl{Action: "open", Token: a.Token})
		want := "released"
		if a.Recipe.Phase == pinExpire || a.Recipe.Phase == pinAdvanceEpoch {
			want = "expired"
		}
		if err == nil || refused.Result != want || refused.Snapshot != nil {
			t.Fatal("actual keeper terminal token classification", err, refused.Result)
		}
	}
	resume := nativeProtocolStateControl{Action: "resume", Original: a.Original}
	if live {
		resume.Token = a.Token
	}
	c.must(resume)
	if _, e := c.call(input); e != nil {
		class := nativeTransitionPinErrorClass(e)
		if class != "released" && class != "expired" {
			t.Fatal("same pin native original", e)
		}
	}
	if a.Recipe.Phase == pinAdvanceEpoch {
		// Epoch expiry is a live-policy refusal; install its separately funded
		// immutable terminal too so the durable bank is genuinely retired.
		c.must(nativeProtocolStateControl{Action: "expire"})
	}
	if a.Recipe.Phase == pinPublish || a.Recipe.Phase == pinIssue {
		r := c.must(nativeProtocolStateControl{Action: "publish"})
		a.Token = bytes.Clone(r.Token)
	}
	if a.Recipe.Phase == pinCancelIssue {
		refused, err := c.call(nativeProtocolStateControl{Action: "publish"})
		if err == nil || refused.Result != "released" || len(refused.Token) != 0 {
			t.Fatal("cancelled issuance published an owner", err)
		}
	}
}
func nativeProtocolStateWholeMove(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork) {
	t.Helper()
	response, transport := f.n.all[2].exchange(ctx, processRequest{op: processRangeRun, entity: 1, index: 8, data: graphE17MoveBounds(128, 8, 16<<10)})
	result, e := decodeGraphE17MoveResult(response)
	for range 128 {
		if !errors.Is(e, errRangeMovePending) {
			break
		}
		response, transport = f.n.all[2].exchange(ctx, processRequest{op: processRangeResume, data: response.aux})
		result, e = decodeGraphE17MoveResult(response)
	}
	if transport != nil || e != nil || !result.committed {
		t.Fatal("actual move before feed ACK", e, transport)
	}
}
func nativeProtocolStateDrain(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, a *nativeProtocolStateArtifact, c *nativeProtocolStateClient) []uint64 {
	return nativeProtocolStateDrainFor(t, ctx, f, a, c, "complete-feed")
}

func nativeProtocolStateDrainFor(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, a *nativeProtocolStateArtifact, c *nativeProtocolStateClient, domain string) []uint64 {

	t.Helper()
	cut := nativeScalarWorkloadCut(t, ctx, f.n, graphReadFresh, graphCut{}, nativeProtocolStateExpectedAgreement(t))
	wire, e := nativeFeedCutWire(cut)
	if e != nil {
		t.Fatal(e)
	}
	var positions []uint64
	for sequence := uint64(1); sequence <= nativeProtocolStatePages; sequence++ {
		request := nativeProtocolStateRequest(a.Run, domain, sequence)
		reply := c.must(nativeProtocolStateControl{Action: "page", Request: request, Boundary: wire})
		arena, e := graphstate.NewOutputBudget(2 << 20)
		if e != nil {
			t.Fatal(e)
		}
		page, e := decodeRetainedFeedPage(reply.Page, 64<<10, arena)
		if e != nil || sha256.Sum256(reply.Page) != reply.Digest || page.request != request {
			t.Fatal("actual complete page", e)
		}
		replay := c.must(nativeProtocolStateControl{Action: "page", Request: request, Boundary: wire})
		if !bytes.Equal(reply.Page, replay.Page) || replay.Digest != reply.Digest {
			t.Fatal("SAME pending page changed")
		}
		ack := c.must(nativeProtocolStateControl{Action: "ack", Request: request, Digest: reply.Digest})
		same := c.must(nativeProtocolStateControl{Action: "ack", Request: request, Digest: reply.Digest})
		if !bytes.Equal(ack.Completed, same.Completed) {
			t.Fatal("SAME ACK changed cut")
		}
		positions = page.next
		if page.kind == feedMoveEnvelope && (len(page.frames) != 2 || !bytes.Equal(page.frames[0].changes, page.frames[1].changes) || !bytes.Equal(page.frames[0].decision, page.frames[1].decision) || !bytes.Equal(page.frames[0].intent, page.frames[1].intent)) {
			t.Fatal("half move lineage")
		}
		if page.eof {
			completed, e := nativeProtocolStateCut(ack.Completed)
			if e != nil || !feedCutEqual(completed, cut) {
				t.Fatal("half transaction completed cut", e)
			}
			return positions
		}
	}
	t.Fatal("finite genuine feed did not reach certified boundary")
	return nil
}

type nativeProtocolStateGCClient struct {
	t        *testing.T
	ctx      context.Context
	n        *certifiedProcessNetwork
	member   uint64
	cookie   [16]byte
	sequence uint64
	calls    []nativeRetentionReceipt
}

func (c *nativeProtocolStateGCClient) call(q nativeRetentionControl) (nativeRetentionReceipt, processResponse, error) {
	q.Version = 1
	if q.Action != "inspect" {
		c.sequence++
		q.Cookie = c.cookie
		q.Sequence = c.sequence
	}
	wire, e := nativeRetentionWire(q)
	if e != nil {
		return nativeRetentionReceipt{}, processResponse{}, e
	}
	response, err := c.n.all[c.member].exchange(c.ctx, processRequest{op: processNativeFaultControl, data: wire})
	if nativeBatchFailedStepAllowed(response, err) {
		return nativeRetentionReceipt{}, response, err
	}
	if len(response.packets) > 0 {
		c.n.group(c.member).enqueue(c.t, response)
		c.n.group(c.member).drain(c.t)
	}
	var r nativeRetentionReceipt
	decode := nativeFeedDecodeJSON(response.aux, &r)
	if decode != nil || r.Profile != nativeProtocolStateProfile || r.Member != c.member || r.Action != q.Action || r.Sequence != q.Sequence {
		return r, response, errors.Join(errCorrupt, decode, err)
	}
	c.calls = append(c.calls, r)
	if q.Action == "inspect" {
		c.cookie = r.Cookie
	}
	return r, response, errors.Join(err, processResponseError(response))
}
func (c *nativeProtocolStateGCClient) must(q nativeRetentionControl) nativeRetentionReceipt {
	c.t.Helper()
	r, _, e := c.call(q)
	if e != nil {
		c.t.Fatal("actual native GC", q.Action, e)
	}
	return r
}
func nativeProtocolStateNewGC(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, member uint64) *nativeProtocolStateGCClient {
	c := &nativeProtocolStateGCClient{t: t, ctx: ctx, n: n, member: member}
	c.must(nativeRetentionControl{Action: "inspect"})
	return c
}
func nativeProtocolStateCollect(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, a *nativeProtocolStateArtifact, c *nativeProtocolStateClient, cut graphCut, positions []uint64, transition *nativeProtocolStateRecipe) {
	t.Helper()
	if len(positions) != 2 || len(cut.certificates) != 2 {
		t.Fatal("authenticated complete ACK/floor vector")
	}
	var clients [6]*nativeProtocolStateGCClient
	var originals [2][]byte
	var plans [2]raftlog.ApplicationRetentionState
	var beforeRecords [6]uint64
	for group, leader := range f.n.leaders {
		certificate := cut.certificates[group]
		client := nativeProtocolStateNewGC(t, ctx, f.n, leader)
		clients[leader-1] = client
		floors := [6]uint64{certificate.sourceIndex, certificate.sourceIndex, positions[group], positions[group], certificate.sourceIndex, certificate.index}
		prepared := client.must(nativeRetentionControl{Action: "prepare", Request: nativeProtocolStateRequest(a.Run, "native-collector", uint64(group)), Floors: floors})
		originals[group] = prepared.Original
		client.must(nativeRetentionControl{Action: "submit"})
		client.must(nativeRetentionControl{Action: "original"})
		current := client.must(nativeRetentionControl{Action: "current"})
		plans[group] = current.Plan
		if current.Plan.ID != sha256.Sum256(prepared.Original) || current.Plan.RootFloor != certificate.sourceIndex || current.Plan.OutcomeFloor != positions[group] {
			t.Fatal("native plan substituted floor")
		}
	}
	for member := uint64(1); member <= 6; member++ {
		group := 0
		if member > 3 {
			group = 1
		}
		if clients[member-1] == nil {
			clients[member-1] = nativeProtocolStateNewGC(t, ctx, f.n, member)
			clients[member-1].must(nativeRetentionControl{Action: "recover", Original: originals[group]})
		}
		before := f.n.all[member].call(t, processRequest{op: processStateOp})
		beforeRecords[member-1] = before.stats[statActiveRecords]
		a.BeforeGC = append(a.BeforeGC, p06ProcessWire(t, before))
	}
	// A genuine held physical C view blocks collection on its real source owner.
	// Durable pins instead permit deleting eligible earlier native sidecars.
	if c != nil {
		c.must(nativeProtocolStateControl{Action: "snapshot"})
		source := 0
		if a.program != nil {
			source = int(f.n.leaders[0] - 1)
		}
		_, blocked, e := clients[source].call(nativeRetentionControl{Action: "collect"})
		if !errors.Is(e, errLimit) && !errors.Is(e, raftlog.ErrLimit) {
			t.Fatal("collector bypassed held native owner", e)
		}
		if a.program != nil {
			a.program.evidence.BlockedCollectorResponse = p06ProcessWire(t, blocked)
		}
		c.must(nativeProtocolStateControl{Action: "snapshot-close"})
	}
	if transition != nil {
		member := uint64(3)
		if a.program != nil {
			member = a.program.evidence.Target
		}
		client := clients[member-1]
		inspect, e := nativeStateCall(ctx, f.n.all[member], nativeStateControl{Version: 1, Action: "inspect"})
		if e != nil {
			t.Fatal(e)
		}
		arm := nativeStateControl{Version: 1, Action: "arm", Cookie: inspect.Cookie, Sequence: 1, Kind: raftlog.StableStateRetentionPage, Side: transition.Side, Filter: nativeStateFilter{SourceIndex: plans[0].SourceIndex, PlanID: plans[0].ID}}
		a.StateArm, e = nativeStateCall(ctx, f.n.all[member], arm)
		if e != nil {
			t.Fatal(e)
		}
		// The native collector itself reaches Sync and SIGKILLs. A pipe failure alone
		// cannot qualify; read the exact opaque plan/bank/generation receipt first.
		_, response, actual := client.call(nativeRetentionControl{Action: "collect"})
		if actual == nil || !nativeBatchFailedStepAllowed(response, actual) {
			t.Fatal("selected actual collector was not interrupted", actual)
		}
		// Retire the fixed receipt/decode grant before the actual file read.
		a.ReceiptReads++
		receipt, e := nativeStateRead(filepath.Join(f.n.dir, fmt.Sprint(member)), a.StateArm)
		if e != nil {
			t.Fatal(e)
		}
		a.State = receipt
		killed := nativeTransitionReap(t, f, member, receipt.Member, receipt.PID, receipt, "native-retention-state-kill")
		prior := f.n.all[member]
		rejoined := f.replaceBeforeHeal(member, nil)
		f.n.retired = append(f.n.retired, prior)
		a.Processes = append(a.Processes, killed, rejoined)
		a.GC = append(a.GC, client.calls...)
		client = nativeProtocolStateNewGC(t, ctx, f.n, member)
		client.must(nativeRetentionControl{Action: "recover", Original: originals[0]})
		clients[member-1] = client
	}
	reclaimed := false
	for member := uint64(1); member <= 6; member++ {
		client := clients[member-1]
		complete := false
		for range 256 {
			r := client.must(nativeRetentionControl{Action: "collect"})
			if r.Progress.Complete {
				complete = true
				break
			}
		}
		if !complete {
			t.Fatal("bounded native collector failed EOF")
		}
		after := f.n.all[member].call(t, processRequest{op: processStateOp})
		a.AfterGC = append(a.AfterGC, p06ProcessWire(t, after))
		if after.stats[statActiveRecords] < beforeRecords[member-1] {
			reclaimed = true
		}
		a.GC = append(a.GC, client.calls...)
	}
	if !reclaimed {
		t.Fatal("actual GC reclaimed no eligible physical state")
	}
}
func nativeProtocolStateGCTransition(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, a *nativeProtocolStateArtifact, c *nativeProtocolStateClient, cut graphCut) {
	positions := nativeProtocolStateDrain(t, ctx, f, a, c)
	// Export BEFORE deleting, with actual complete prefix/EOF coordinates. Saved
	// diagnostics are never passed back as cut/feed/native collection authority.
	before := f.indexes()
	a.Records = nativeProtocolStateRecords(t, f.n, a.From, before)
	nativeProtocolStateCollect(t, ctx, f, a, c, cut, positions, &a.Recipe)
	after := f.indexes()
	a.Records = append(a.Records, nativeProtocolStateRecords(t, f.n, before, after)...)
	a.Through = after
}
func nativeProtocolStatePublicationRace(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, a *nativeProtocolStateArtifact, c *nativeProtocolStateClient, old graphCut) {
	// Captured-but-unpublished recipe owns no durable pin. A genuine subsequent
	// mutation produces a strictly newer root which the actual plan may collect
	// past; merely running GC would not establish this refusal condition.
	nativeProtocolStateActionProgram(t, f.n, 1, certifiedHealthyLabel, 1, nil, a.program)
	current := nativeScalarWorkloadCut(t, ctx, f.n, graphReadFresh, graphCut{}, nativeProtocolStateExpectedAgreement(t))
	for i := range current.certificates {
		if current.certificates[i].sourceIndex < old.certificates[i].sourceIndex {
			t.Fatal("nonmonotone native source")
		}
	}
	if current.certificates[0].sourceIndex <= old.certificates[0].sourceIndex {
		t.Fatal("needed old native source was not passed")
	}
	a.From = [2]uint64{}
	before := f.indexes()
	a.Records = nativeProtocolStateRecords(t, f.n, a.From, before)
	positions := []uint64{current.certificates[0].index + 1, current.certificates[1].index + 1}
	nativeProtocolStateCollect(t, ctx, f, a, nil, current, positions, nil)
	refused, e := c.call(nativeProtocolStateControl{Action: "publish"})
	if e == nil || len(refused.Token) != 0 || refused.Result != "expired" {
		t.Fatal("collected unpinned recipe published token", e, refused.Result)
	}
	// The same original remains a genuine failed original/refusal. No missing
	// source is turned into an empty success, and Resume supplies no fresh cut.
	again, e := c.call(nativeProtocolStateControl{Action: "publish"})
	if e == nil || again.Result != "expired" || len(again.Token) != 0 {
		t.Fatal("same expired creation resurrected", e)
	}
	c.must(nativeProtocolStateControl{Action: "close"})
	resumed, e := c.call(nativeProtocolStateControl{Action: "resume", Original: a.Original})
	if e == nil {
		c.must(nativeProtocolStateControl{Action: "cancel"})
	} else if resumed.Result != "expired" || len(resumed.Token) != 0 {
		t.Fatal("saved collected original changed classification", e)
	}
	c.must(nativeProtocolStateControl{Action: "close"})
	a.Calls = c.calls
	after := f.indexes()
	a.Records = append(a.Records, nativeProtocolStateRecords(t, f.n, before, after)...)
}

func nativeProtocolStateWrite(t *testing.T, path string, a nativeProtocolStateArtifact) string {
	t.Helper()
	file, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if file != nil {
			if e := file.Close(); e != nil {
				t.Error(e)
			}
		}
	}()
	hash, total := sha256.New(), 0
	write := func(row any, sealed bool) {
		body, e := json.Marshal(row)
		if e != nil || len(body)+1 > 512<<10 || len(body)+1 > (16<<20)-total {
			t.Fatal("finite state artifact before owned write", e)
		}
		body = append(body, '\n')
		total += len(body)
		if sealed {
			_, _ = hash.Write(body)
		}
		if n, e := file.Write(body); e != nil || n != len(body) {
			t.Fatal(e)
		}
	}
	records, calls, gc := a.Records, a.Calls, a.GC
	var pooled []nativeProtocolStatePoolFrame
	if a.Pooled != nil {
		pooled = a.Pooled.Frames
		if len(pooled) > 128 {
			t.Fatal("finite pooled submission observations")
		}
		copyEvidence := *a.Pooled
		copyEvidence.Frames = nil
		a.Pooled = &copyEvidence
	}
	a.Records, a.Calls, a.GC = nil, nil, nil
	write(struct {
		Type     string
		Artifact nativeProtocolStateArtifact
	}{"header", a}, true)
	if len(records) > 512 || len(calls) > nativeProtocolStateCalls+nativeProtocolStateTerminalCalls || len(gc) > 6*384 {
		t.Fatal("finite state proof collections")
	}
	for _, r := range records {
		write(struct {
			Type   string
			Record lifecycleRawRecord
		}{"record", r}, true)
	}
	for i, c := range calls {
		write(struct {
			Type     string
			Sequence int
			Call     nativeProtocolStateCall
		}{"call", i, c}, true)
	}
	for i, r := range gc {
		write(struct {
			Type     string
			Sequence int
			Receipt  nativeRetentionReceipt
		}{"gc", i, r}, true)
	}
	for i, frame := range pooled {
		write(struct {
			Type     string
			Sequence int
			Frame    nativeProtocolStatePoolFrame
		}{"pooled", i, frame}, true)
	}
	if a.Pooled != nil {
		write(struct {
			Type            string
			Rows, Calls, GC int
			Through         [2]uint64
			PayloadSHA256   string
			Pooled          int
		}{"footer", len(records), len(calls), len(gc), a.Through, fmt.Sprintf("%x", hash.Sum(nil)), len(pooled)}, false)
	} else {
		write(struct {
			Type            string
			Rows, Calls, GC int
			Through         [2]uint64
			PayloadSHA256   string
		}{"footer", len(records), len(calls), len(gc), a.Through, fmt.Sprintf("%x", hash.Sum(nil))}, false)
	}
	if e := file.Sync(); e != nil {
		t.Fatal(e)
	}
	if e := file.Close(); e != nil {
		t.Fatal(e)
	}
	file = nil
	return path
}

func nativeProtocolStateTokenGuards(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, a *nativeProtocolStateArtifact, c *nativeProtocolStateClient) {
	t.Helper()
	before := f.indexes()
	refused, e := c.call(nativeProtocolStateControl{Action: "resume", Original: a.Original, Token: a.Token})
	if !errors.Is(e, errLimit) || refused.Result != "limit" || f.indexes() != before {
		t.Fatal("live owner was overwritten by Resume", e)
	}
	refused, e = c.call(nativeProtocolStateControl{Action: "open", Token: a.Token})
	if !errors.Is(e, errLimit) || refused.Result != "limit" || f.indexes() != before {
		t.Fatal("live owner was overwritten by Open", e)
	}
	other := &nativeProtocolStateClient{t: t, ctx: ctx, n: f.n, member: 5}
	other.must(nativeProtocolStateControl{Action: "inspect"})
	other.must(nativeProtocolStateControl{Action: "begin", Request: nativeProtocolStateRequest(a.Run, "foreign-native-owner", 0)})
	foreign := other.must(nativeProtocolStateControl{Action: "publish"})
	if bytes.Equal(foreign.Original, a.Original) || bytes.Equal(foreign.Token, a.Token) {
		t.Fatal("independent genuine owner identity collision")
	}
	other.must(nativeProtocolStateControl{Action: "release"})
	other.must(nativeProtocolStateControl{Action: "close"})
	c.must(nativeProtocolStateControl{Action: "close"})
	before = f.indexes()
	refused, e = c.call(nativeProtocolStateControl{Action: "resume", Original: a.Original, Token: foreign.Token})
	if !errors.Is(e, errInvalid) || refused.Result != "invalid" || f.indexes() != before {
		t.Fatal("foreign native token adopted under original A", e)
	}
	c.must(nativeProtocolStateControl{Action: "resume", Original: a.Original, Token: a.Token})
	c.must(nativeProtocolStateControl{Action: "snapshot"})
	a.Calls = append(a.Calls, other.calls...)
}
