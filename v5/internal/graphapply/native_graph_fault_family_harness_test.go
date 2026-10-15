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
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

const nativeFaultFamilyProfile = "native-composed-d-family-v1"
const nativeFaultFamilyControls = 16
const nativeFaultFamilyModels = 2

// Initial Run, first Resume, the existing finite completion loop, last replay,
// and one inert-owner refusal each retire their own allowance.
const nativeFaultFamilyMoveCalls = graphE17AdmittedMovePages + 4
const nativeFaultFamilyClockLimitMillis int64 = 365 * 24 * 60 * 60 * 1000
const certifiedNativeFamilyBusiness = certifiedNativeDecisionBusiness + 1

func nativeFaultFamilyBusiness(o *certifiedProcessOwners, r processRequest) (allocationProposal, error) {
	if o == nil || o.family == nil || !o.host.machine.rangeMoves() || len(r.data) > 1 || len(r.data) == 1 && r.data[0] != 1 && r.data[0] != 2 || r.index != 3 && r.index != 8 {
		return allocationProposal{}, errInvalid
	}
	minimum := r.index == 3
	if len(r.data) == 1 {
		minimum = r.data[0] == 1
	}
	var selected requestID
	var priority [16]byte
	for value := 1; value < 256; value++ {
		id := requestID{byte(value), 0xe8} // #nosec G115 -- finite pure client retry-key selector1..255.
		tx := crossTransactionID(namespace{o.host.machine.ns.graph, r.index}, id)
		if selected == (requestID{}) || minimum && bytes.Compare(tx[:], priority[:]) < 0 || !minimum && bytes.Compare(tx[:], priority[:]) > 0 {
			selected, priority = id, tx
		}
	}
	cfg, err := o.host.currentConfiguration()
	if err != nil {
		return allocationProposal{}, err
	}
	window, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return allocationProposal{}, err
	}
	ops := []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "family-conflict"}, {Kind: graphstate.AddLabel, Owner: 17, Life: 18, Scope: window, Name: "family-conflict"}}
	return o.host.PreparePartitionGraphOperations(selected, ops, 71, nil)
}

// This private pipe control supplies scheduling/diagnostic input only. It has
// no graph proposal, certificate, stamp, proof, outcome or mutation import door.
type nativeFaultFamilyControl struct {
	Version      uint8
	Action       string
	Cookie       [16]byte
	Sequence     uint64
	OffsetMillis int64
}

type nativeFaultFamilyReceipt struct {
	Version       uint8
	Profile       string
	Cookie        [16]byte
	Member, PID   uint64
	Sequence      uint64
	OffsetMillis  int64
	Controls      int
	ModelCaptures int
	ModelCookie   [16]byte
	CatalogSHA    [32]byte
	ModelLedger   nativeDurableLedger
	Policy        nativeDurablePolicy
}

type nativeFaultFamilyOwner struct {
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	member     uint64
	cookie     [16]byte
	offset     atomic.Int64
	controls   int
	models     int
	sequence   uint64
	last       nativeFaultFamilyControl
	lastWire   []byte
	model      *nativeDurableFS
	arena      *graphstate.OutputBudget
	closed     bool
	move       *rangeMoveIntent // At most one genuine factory value, never decoded from GMR.
	moveResume rangeMoveResume
	moveBusy   bool
	moveCalls  int
}

func nativeDurableShadowPath(dir string, pid int) string {
	return filepath.Join(filepath.Dir(dir), fmt.Sprintf("%s.durable.%d", filepath.Base(dir), pid))
}

func newNativeFaultFamilyOwner(ctx context.Context, dir string, member uint64, create bool) (*nativeFaultFamilyOwner, vfs.FS, error) {
	switch os.Getenv("RHO_GRAPH_FAULT_FAMILY_PROCESS") {
	case "", "off":
		if mode := os.Getenv("RHO_GRAPH_DURABLE_MODEL"); mode != "" && mode != "off" {
			return nil, nil, errInvalid
		}
		return nil, nil, nil
	case "1":
	default:
		return nil, nil, errInvalid
	}
	if ctx == nil || member < 1 || member > 6 {
		return nil, nil, errInvalid
	}
	policy := nativeDurableDefaultPolicy()
	grant := (nativeFaultFamilyControls+2+nativeFaultFamilyMoveCalls)*8192 + nativeFaultFamilyModels*4*policy.CatalogBytes
	budget, err := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
	if err != nil {
		return nil, nil, err
	}
	arena, err := budget.Child(grant)
	if err != nil {
		return nil, nil, err
	}
	// This new diagnostic owner is bounded independently; ordinary native source
	// leases keep their original one-minute lifetime and invocation allowances.
	owner, cancel := context.WithTimeout(ctx, 2*time.Minute)
	o := &nativeFaultFamilyOwner{ctx: owner, cancel: cancel, member: member, arena: arena}
	if _, err := io.ReadFull(rand.Reader, o.cookie[:]); err != nil {
		cancel()
		return nil, nil, err
	}
	switch os.Getenv("RHO_GRAPH_DURABLE_MODEL") {
	case "", "off":
	case "1":
		if !create {
			cancel()
			return nil, nil, errInvalid // Recovery must explicitly install a sealed model image first.
		}
		o.model, err = newNativeDurableFS(owner, dir, nativeDurableShadowPath(dir, os.Getpid()), member, policy)
		if err != nil {
			cancel()
			return nil, nil, err
		}
	default:
		cancel()
		return nil, nil, errInvalid
	}
	if o.model != nil {
		return o, o.model, nil
	}
	return o, nil, nil
}

func (o *nativeFaultFamilyOwner) close() {
	if o == nil {
		return
	}
	o.cancel()
	o.mu.Lock()
	o.closed, o.lastWire = true, nil
	o.move = nil
	o.mu.Unlock()
}

// The fresh D caller keeps its original typed factory intent even when native
// admission refuses before a durable move exists. A reconstructed GMR identifies
// only that SAME live owner/request; absent owner state is an explicit refusal.
func (o *nativeFaultFamilyOwner) moveCall(caller context.Context, h *allocationHost, incarnation [16]byte, c *rangeMoveCoordinator, r processRequest) (wire []byte, err error) {
	if o == nil {
		return graphE17RunMove(caller, h, incarnation, c, r)
	}
	if caller == nil || c == nil || h == nil {
		return nil, errInvalid
	}
	ctx, cancel := context.WithCancel(caller)
	stop := context.AfterFunc(o.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := errors.Join(ctx.Err(), o.ctx.Err()); err != nil {
		return nil, err
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil, errInvalid
	}
	if o.moveCalls >= nativeFaultFamilyMoveCalls {
		o.mu.Unlock()
		return nil, errLimit
	}
	if err := o.arena.Reserve(8192); err != nil {
		o.mu.Unlock()
		return nil, outputBudgetFailure(err)
	}
	o.moveCalls++ // Failed and uncertain invocations retire the same finite grant.
	if o.moveBusy {
		o.mu.Unlock()
		return nil, errRangeMovePending
	}
	var intent rangeMoveIntent
	switch r.op {
	case processRangeRun:
		if o.move != nil || len(r.data) != 12 {
			o.mu.Unlock()
			return nil, errInvalid
		}
	case processRangeResume:
		prior, e := decodeGraphE17MoveResult(processResponse{aux: r.data})
		if e != nil && !errors.Is(e, errRangeMovePending) || o.move == nil || prior.resume != o.moveResume {
			o.mu.Unlock()
			return nil, errInvalid
		}
		intent = *o.move
	default:
		o.mu.Unlock()
		return nil, errInvalid
	}
	o.moveBusy = true
	o.mu.Unlock()
	defer func() { o.mu.Lock(); o.moveBusy = false; o.mu.Unlock() }()
	if r.op == processRangeRun {
		id := requestID(incarnation)
		binary.BigEndian.PutUint64(id[8:], r.sequence)
		intent, err = h.PrepareRangeMove(ctx, id, r.entity, r.index, int(binary.BigEndian.Uint32(r.data[:4])), int(binary.BigEndian.Uint32(r.data[4:8])), int(binary.BigEndian.Uint32(r.data[8:])))
		if err != nil {
			return nil, err
		}
		encoded, e := encodeRangeMoveIntent(intent)
		if e != nil {
			return nil, e
		}
		resume := rangeMoveResume{scope: intent.source, request: intent.id, move: rangeMoveID(namespace{intent.source.graph, intent.source.partition}, intent.id), intent: sha256.Sum256(encoded)}
		o.mu.Lock()
		if o.closed || o.ctx.Err() != nil {
			o.mu.Unlock()
			return nil, errors.Join(errInvalid, o.ctx.Err())
		}
		o.move, o.moveResume = &intent, resume
		o.mu.Unlock()
	}
	result, runErr := c.Run(ctx, intent)
	if result.resume != o.moveResume {
		return nil, errors.Join(errInvalid, runErr)
	}
	wire, err = encodeGraphE17MoveResult(result, runErr)
	return wire, errors.Join(runErr, err)
}

// The provider's function identity never changes after server publication. Its
// one atomic input is a diagnostic wall-clock offset, never a round or floor.
func (o *nativeFaultFamilyOwner) now() time.Time {
	return time.Now().Add(time.Duration(o.offset.Load()) * time.Millisecond)
}

func decodeNativeFaultFamilyControl(wire []byte) (nativeFaultFamilyControl, error) {
	var request nativeFaultFamilyControl
	if len(wire) == 0 || len(wire) > 4096 {
		return request, errInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return request, errors.Join(errInvalid, err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return request, errors.Join(errInvalid, err)
	}
	canonical, err := json.Marshal(request)
	if err != nil || !bytes.Equal(canonical, wire) || request.Version != 1 {
		return request, errors.Join(errInvalid, err)
	}
	return request, nil
}

func (o *nativeFaultFamilyOwner) control(wire []byte) ([]byte, error) {
	if o == nil {
		return nil, errInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.ctx.Err() != nil {
		return nil, errors.Join(errInvalid, o.ctx.Err())
	}
	if o.controls >= nativeFaultFamilyControls {
		return nil, errLimit
	}
	if err := o.arena.Reserve(8192); err != nil {
		return nil, outputBudgetFailure(err)
	}
	o.controls++
	request, err := decodeNativeFaultFamilyControl(wire)
	if err != nil {
		return nil, err
	}
	if request.Action == "inspect" {
		if request.Cookie != ([16]byte{}) && request.Cookie != o.cookie || request.Sequence != 0 || request.OffsetMillis != 0 {
			return nil, errInvalid
		}
	} else if request.Cookie == ([16]byte{}) || request.Cookie != o.cookie {
		return nil, errInvalid
	}
	receipt := nativeFaultFamilyReceipt{Version: 1, Profile: nativeFaultFamilyProfile, Cookie: o.cookie, Member: o.member, PID: uint64(os.Getpid()), Sequence: o.sequence, OffsetMillis: o.offset.Load(), Controls: o.controls, ModelCaptures: o.models}
	switch request.Action {
	case "inspect":
	case "clock":
		if request.Sequence != 0 && request == o.last {
			return bytes.Clone(o.lastWire), nil
		}
		if request.Sequence != o.sequence+1 || request.OffsetMillis < -nativeFaultFamilyClockLimitMillis || request.OffsetMillis > nativeFaultFamilyClockLimitMillis {
			return nil, errInvalid
		}
		o.sequence, o.last = request.Sequence, request
		o.offset.Store(request.OffsetMillis)
		receipt.Sequence, receipt.OffsetMillis = o.sequence, request.OffsetMillis
	case "model":
		if request.Sequence != 0 || request.OffsetMillis != 0 || o.model == nil || o.models >= nativeFaultFamilyModels {
			return nil, errInvalid
		}
		if err := o.arena.Reserve(4 * o.model.policy.CatalogBytes); err != nil {
			return nil, outputBudgetFailure(err)
		}
		o.models++
		// Source metadata capture never acquires Host or native event locks.
		image, digest, err := nativeDurableReadImage(o.ctx, o.model.shadow, o.model.policy)
		if err != nil || image.Cookie != o.model.cookie || image.Member != o.member {
			return nil, errors.Join(errInvalid, err)
		}
		receipt.ModelCaptures, receipt.ModelCookie, receipt.CatalogSHA = o.models, image.Cookie, digest
		receipt.ModelLedger, receipt.Policy = image.Ledger, image.Policy
	default:
		return nil, errInvalid
	}
	if err := o.ctx.Err(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(receipt)
	if len(encoded) > 4096 {
		return nil, errLimit
	}
	if request.Action == "clock" && err == nil {
		o.lastWire = bytes.Clone(encoded)
	}
	return encoded, err
}

func nativeFaultFamilyCall(t *testing.T, p *graphProcess, request nativeFaultFamilyControl) nativeFaultFamilyReceipt {
	t.Helper()
	wire, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	response := p.call(t, processRequest{op: processNativeFaultFamilyControl, data: wire})
	var receipt nativeFaultFamilyReceipt
	if err := json.Unmarshal(response.aux, &receipt); err != nil || receipt.Version != 1 || receipt.Profile != nativeFaultFamilyProfile || receipt.Cookie == ([16]byte{}) || receipt.PID != uint64(p.cmd.Process.Pid) || receipt.Member != p.mux.id || receipt.Controls < 1 || receipt.Controls > nativeFaultFamilyControls {
		t.Fatal("foreign/unbounded actual family owner", err, receipt)
	}
	return receipt
}

type nativeFaultFamilyTick struct {
	Member, Ordinal, Before, After uint64
	Response                       []byte
}

type nativeFaultFamilyModel struct {
	Capture                       nativeFaultFamilyReceipt
	Image                         []byte
	Recovery                      nativeDurableRecovery
	SourceVolume, VolatileArchive string
}

type nativeFaultFamilyArtifact struct {
	nativeFaultArtifact
	Ticks                         []nativeFaultFamilyTick
	Clocks                        []nativeFaultFamilyReceipt
	Model                         *nativeFaultFamilyModel
	Network                       []nativeFaultFamilyNetworkWindow
	HeldPacket                    *replica.Packet
	HeldTerm                      uint64
	LeaderBefore, LeaderAfter     []byte
	Corruption                    *nativeFaultFamilyCorruption
	OpenRefusal                   *nativeFaultFamilyOpenRefusal
	Repair                        *nativeDurableRecovery
	CorruptArchive                string
	Expiry                        []nativeFaultFamilyExpiry
	Snapshot                      []nativeFaultSnapshotBoundary
	Exploration                   *nativeFaultFamilyExploration
	DiskBytes, AllocatedDiskBytes uint64
	Scope                         string
	Replay                        *nativeTransitionComposedEvidence `json:",omitempty"`
}

type nativeFaultFamilyRangeRead struct {
	Cut     lifecycleRawCut
	Details [][]byte
}

func nativeFaultFamilyAssertConflictCDC(t *testing.T, records []lifecycleRawRecord, decision crossStatus, move rangeMoveResult, oldEpoch uint64) {
	t.Helper()
	seen := map[graphstate.EntityID]bool{}
	activations := map[uint64]bool{}
	for _, record := range records {
		if movement, err := decodeRangeMoveStatus(record.Outcome); err == nil && len(record.CDC) != 0 {
			if movement.phase != rangeMoveActivate || movement.result != rangeMoveApplied || movement.index != record.Index || movement.move != move.resume.move || movement.intent != move.resume.intent || movement.round != move.decision.round || movement.digest != move.decision.digest || activations[record.Partition] {
				t.Fatal("unexpected move original/replay CDC")
			}
			body, err := graphBody(record.CDC, "GML\x01", 2048)
			if err != nil {
				t.Fatal(err)
			}
			c := graphCursor{b: body}
			if c.array() != move.resume.move || !bytes.Equal(c.take(32), move.resume.intent[:]) {
				t.Fatal("move CDC original identity")
			}
			for _, want := range []uint64{1, 3, 8, oldEpoch, oldEpoch + 1, move.decision.round} {
				if c.u64() != want {
					t.Fatal("move CDC original range/placement/round")
				}
			}
			if !bytes.Equal(c.take(32), move.decision.digest[:]) || c.err != nil || len(c.b) != 0 {
				t.Fatal("move CDC complete-source digest", c.err)
			}
			activations[record.Partition] = true
			continue
		}
		status, err := decodeCrossStatus(record.Outcome)
		if err != nil || status.tx != decision.tx || status.phase != crossResolve || status.index != record.Index {
			if len(record.CDC) != 0 {
				t.Fatal("non-original conflict/control/replay emitted business CDC")
			}
			continue
		}
		if !status.commit || status.round != decision.round {
			t.Fatal("conflict CDC changed first decision")
		}
		original := outcome{ns: status.ns, kind: graphOperations, identity: status.tx, hash: status.digest, index: status.index, disposition: applied}
		changes, err := decodeTypedChangeEnvelope(record.CDC, original, defaultMaterializerLimits(), true)
		if err != nil || len(changes.entities)+len(changes.lives)+len(changes.values) != 0 {
			t.Fatal("conflict exact native CDC gained entity/life/value rows", err)
		}
		for _, group := range changes.groups {
			key := group.Key
			if key.Kind != graphstate.Label || key.Name != "family-conflict" || key.Life != graphstate.LifeID(key.Owner+1) || key.Owner != 1 && key.Owner != 17 || seen[key.Owner] || len(group.Changes) != 1 {
				t.Fatal("conflict complete CDC exact component set", key)
			}
			change := group.Changes[0]
			if change.Before().Present() || !change.After().Present() || change.After().Revision().ID() != decision.round || change.After().Revision().Provenance() != 71 {
				t.Fatal("conflict original CDC revision/provenance/cells")
			}
			seen[key.Owner] = true
		}
	}
	if len(seen) != 2 || !seen[1] || !seen[17] {
		t.Fatal("partial original conflict CDC")
	}
	if len(activations) != 2 || !activations[3] || !activations[8] {
		t.Fatal("partial actual activation CDC lineage")
	}
}

type nativeFaultFamilyExploration struct {
	Order                                        []string
	Originals                                    [2][]byte
	Manifests                                    [2][]byte
	Results                                      [2][]byte
	Gate                                         pairedFaultBoundary
	Pending                                      []byte
	MovePending, MoveComplete, MoveReplay        []byte
	MoveOwnerCookie                              [16]byte
	Before, Settled, Transferred, Held, Reopened nativeFaultFamilyRangeRead
	PlacementBefore                              []byte
	PlacementAfter                               [][]byte
}

func nativeFaultFamilyRangeReadAt(t *testing.T, n *certifiedProcessNetwork, actor, handle uint64, cut graphCut) (nativeFaultFamilyRangeRead, map[uint64]graphE17Fact) {
	t.Helper()
	n.e17PublicUsed++
	if n.e17PublicUsed > n.e17PublicPhases {
		t.Fatal("undeclared actual range oracle phase")
	}
	out := nativeFaultFamilyRangeRead{}
	var err error
	out.Cut, err = lifecycleRawCertificate(cut)
	if err != nil {
		t.Fatal(err)
	}
	facts := make(map[uint64]graphE17Fact, 4)
	for _, ids := range [][]uint64{{1, 3, 17}, {7}} {
		input, err := encodeGraphProcessObserveIDs(ids)
		if err != nil {
			t.Fatal(err)
		}
		response := n.all[actor].call(t, processRequest{op: processRangeCertifiedDetail, index: handle, data: input})
		decoded, err := decodeGraphE17Facts(response.aux)
		if err != nil || len(decoded) != len(ids) {
			t.Fatal("actual complete range facts", err)
		}
		out.Details = append(out.Details, bytes.Clone(response.aux))
		for i, fact := range decoded {
			if fact.id != ids[i] {
				t.Fatal("range oracle exact IDs")
			}
			facts[fact.id] = fact
		}
	}
	return out, facts
}

type nativeFaultFamilyExpiry struct{ Request, Reply, Response []byte }

type nativeFaultSnapshotBoundary struct {
	Phase                  string
	Donor, Receiver, Owner uint64
	Manifest, Chunk        []byte
	Packet                 replica.Packet
	Response               processResponse `json:"-"`
	ResponseWire           []byte
}

func (f *pairedGraphFaultNetwork) nativeSnapshotNotice(boundary nativeFaultSnapshotBoundary) bool {
	if f.nativeSnapshotHook == nil {
		return true
	}
	if f.nativeSnapshotArena == nil {
		var err error
		f.nativeSnapshotArena, err = graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
		if err != nil {
			f.t.Fatal(err)
		}
	}
	if f.nativeSnapshotNotices >= 16 {
		f.t.Fatal("finite D snapshot diagnostic bound")
	}
	r := boundary.Response
	variable := len(r.aux) + len(r.image) + len(r.outcome) + len(r.changes) + len(r.detail) + len(r.fact.Answer)
	for _, packet := range r.packets {
		variable += len(packet.Payload)
	}
	for _, read := range r.reads {
		variable += len(read.Context)
	}
	for _, label := range r.fact.Labels {
		variable += len(label)
	}
	if variable > processFrameBytes {
		f.t.Fatal("actual snapshot response frame bound")
	}
	if err := f.nativeSnapshotArena.Reserve(16384 + 4*variable + 4*len(boundary.Manifest) + 4*len(boundary.Chunk) + 4*len(boundary.Packet.Payload)); err != nil {
		f.t.Fatal("snapshot diagnostic prepaid ownership", err)
	}
	f.nativeSnapshotNotices++
	boundary.Manifest, boundary.Chunk, boundary.Packet.Payload = bytes.Clone(boundary.Manifest), bytes.Clone(boundary.Chunk), bytes.Clone(boundary.Packet.Payload)
	var err error
	boundary.ResponseWire, err = encodeProcessResponse(boundary.Response)
	if err != nil {
		f.t.Fatal(err)
	}
	boundary.Response = processResponse{} // No callback may mutate a live response's ACK slices.
	return f.nativeSnapshotHook(boundary)
}

type nativeFaultFamilyOpenRefusal struct {
	Version                    uint8
	Profile                    string
	Cookie                     [16]byte
	Member, PID                uint64
	Stage, Error               string
	RaftCorrupt, NativeCorrupt bool
}

func nativeFaultFamilyOpenRefusalPath(dir string, pid int) string {
	return filepath.Join(dir, fmt.Sprintf("native-family-open-refusal-%d.json", pid))
}

// Literal failed native Open, before Ready or any process/source identity is
// published. This diagnostic grants no graph or storage authority.
func (o *nativeFaultFamilyOwner) recordOpenRefusal(dir string, actual error) error {
	if o == nil {
		return nil
	}
	if actual == nil {
		return errInvalid
	}
	if err := o.arena.Reserve(8192); err != nil {
		return outputBudgetFailure(err)
	}
	receipt := nativeFaultFamilyOpenRefusal{Version: 1, Profile: nativeFaultFamilyProfile, Cookie: o.cookie, Member: o.member, PID: uint64(os.Getpid()), Stage: "raftlog.Open-before-Ready", Error: actual.Error(), RaftCorrupt: errors.Is(actual, raftlog.ErrCorrupt), NativeCorrupt: pebble.IsCorruptionError(actual)}
	wire, err := json.Marshal(receipt)
	if err != nil || len(wire) > 4096 {
		return errors.Join(errLimit, err)
	}
	file, err := os.OpenFile(nativeFaultFamilyOpenRefusalPath(dir, os.Getpid()), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	n, writeErr := file.Write(wire)
	if n != len(wire) {
		writeErr = errors.Join(io.ErrShortWrite, writeErr)
	}
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func nativeFaultFamilyReapOpenRefusal(t *testing.T, p *graphProcess, member uint64, dir string) (pairedFaultProcess, nativeFaultFamilyOpenRefusal) {
	t.Helper()
	if p == nil || p.closed || p.mux != nil {
		t.Fatal("pre-Ready refusal acquired a process source identity")
	}
	p.closed = true
	var output processStderr
	drained := make(chan error, 1)
	go func() { _, err := io.Copy(&output, p.stdout); drained <- err }()
	select {
	case err := <-drained:
		if err != nil {
			t.Fatal("native refusal stdout drain", err)
		}
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		_ = p.stdin.Close()
		_ = p.stdout.Close()
		reaped := make(chan error, 1)
		go func() { reaped <- p.cmd.Wait() }()
		select {
		case err := <-reaped:
			p.closeErr = err
		case <-time.After(10 * time.Second):
			p.closeErr = context.DeadlineExceeded
		}
		t.Fatal("native corrupted Open required controller kill; not qualified")
	}
	waited := make(chan error, 1)
	go func() { waited <- p.cmd.Wait() }()
	select {
	case err := <-waited:
		p.closeErr = errors.Join(err, p.stdin.Close(), p.stdout.Close())
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		select {
		case err := <-waited:
			p.closeErr = err
		case <-time.After(10 * time.Second):
		}
		t.Fatal("native corrupted Open did not reap itself")
	}
	if p.cmd.ProcessState == nil {
		t.Fatal("native corruption refusal missing process state")
	}
	status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || status.Signaled() || p.cmd.ProcessState.ExitCode() != 1 {
		t.Fatal("native corrupt Open was not an actual failure exit")
	}
	path := nativeFaultFamilyOpenRefusalPath(dir, p.cmd.Process.Pid)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > 4096 {
		t.Fatal("native refusal receipt bound", err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	wire, readErr := io.ReadAll(io.LimitReader(file, 4097))
	if err := errors.Join(readErr, file.Close()); err != nil || len(wire) > 4096 {
		t.Fatal("native refusal receipt read", err)
	}
	var receipt nativeFaultFamilyOpenRefusal
	if err := json.Unmarshal(wire, &receipt); err != nil || receipt.Version != 1 || receipt.Profile != nativeFaultFamilyProfile || receipt.Cookie == ([16]byte{}) || receipt.Member != member || receipt.PID != uint64(p.cmd.Process.Pid) || receipt.Stage != "raftlog.Open-before-Ready" || !receipt.RaftCorrupt && !receipt.NativeCorrupt || receipt.Error == "" {
		t.Fatal("native Open did not classify actual corruption", receipt, err)
	}
	stderr, overflow := p.stderr.state()
	stdout, stdoutOverflow := output.state()
	if overflow || stdoutOverflow || !strings.Contains(stderr+stdout, receipt.Error) {
		t.Fatal("native corruption error not bound to actual failed process diagnostic")
	}
	raw, err := json.Marshal(struct {
		Member, PID            uint64
		ExitCode               int
		Reaped, Signaled       bool
		Volume, Stderr, Stdout string
		Receipt                nativeFaultFamilyOpenRefusal
	}{member, uint64(p.cmd.Process.Pid), 1, true, false, dir, stderr, stdout, receipt})
	if err != nil {
		t.Fatal(err)
	}
	return pairedFaultProcess{Action: "native-corrupt-open-refusal", Role: "replica", Member: member, Instance: fmt.Sprintf("voter%d/pid%d/open-cookie%x", member, p.cmd.Process.Pid, receipt.Cookie), Volume: dir, Reaped: true, Raw: raw}, receipt
}

type nativeFaultFamilyNetworkWindow struct {
	Edges   [][2]uint64
	Packets []nativeFaultPacketEvent
}

// Both ordinary asynchronous source packets and explicit Tick/Campaign packets
// pass the same finite directed failure window. It owns no graph state.
func nativeFaultFamilyPartition(t *testing.T, f *pairedGraphFaultNetwork, edges [][2]uint64) func() nativeFaultFamilyNetworkWindow {
	t.Helper()
	if len(edges) < 1 || len(edges) > 12 {
		t.Fatal("finite directed partition")
	}
	blocked := make(map[[2]uint64]bool, len(edges))
	for _, edge := range edges {
		if edge[0] < 1 || edge[0] > 6 || edge[1] < 1 || edge[1] > 6 || edge[0] == edge[1] || (edge[0] <= 3) != (edge[1] <= 3) || blocked[edge] {
			t.Fatal("foreign/duplicate directed partition")
		}
		blocked[edge] = true
	}
	var mu sync.Mutex
	window := nativeFaultFamilyNetworkWindow{Edges: append([][2]uint64(nil), edges...)}
	closed := false
	var failure error
	drop := func(packet replica.Packet) (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		if closed || !blocked[[2]uint64{packet.From, packet.To}] {
			return false, nil
		}
		if len(window.Packets) >= 128 {
			failure = errLimit
			return true, failure
		}
		window.Packets = append(window.Packets, nativeFaultPacketEvent{Action: "directed-partition", From: packet.From, To: packet.To, Ordinal: uint64(len(window.Packets) + 1), Bytes: len(packet.Payload), SHA256: sha256.Sum256(packet.Payload)})
		return true, nil
	}
	hub := f.n.all[1].mux.hub
	hub.mu.Lock()
	prior := hub.packetFilter
	hub.packetFilter = func(packet replica.Packet) (bool, error) {
		if discarded, err := drop(packet); discarded || err != nil {
			return discarded, err
		}
		if prior != nil {
			return prior(packet)
		}
		return false, nil
	}
	hub.mu.Unlock()
	var tiny [2]func(replica.Packet) bool
	for i, group := range f.n.groups {
		tiny[i] = group.dropPacket
		old := tiny[i]
		group.dropPacket = func(packet replica.Packet) bool {
			discarded, _ := drop(packet)
			return discarded || old != nil && old(packet)
		}
	}
	var once sync.Once
	finish := func() nativeFaultFamilyNetworkWindow {
		once.Do(func() {
			hub.mu.Lock()
			hub.packetFilter = prior
			hub.mu.Unlock()
			for i, group := range f.n.groups {
				group.dropPacket = tiny[i]
			}
			mu.Lock()
			closed = true
			mu.Unlock()
		})
		mu.Lock()
		defer mu.Unlock()
		if failure != nil || len(window.Packets) == 0 {
			t.Fatal("partition never matched actual traffic or exceeded finite queue", failure)
		}
		return nativeFaultFamilyNetworkWindow{Edges: append([][2]uint64(nil), window.Edges...), Packets: append([]nativeFaultPacketEvent(nil), window.Packets...)}
	}
	t.Cleanup(func() {
		once.Do(func() {
			hub.mu.Lock()
			hub.packetFilter = prior
			hub.mu.Unlock()
			for i, group := range f.n.groups {
				group.dropPacket = tiny[i]
			}
		})
	})
	return finish
}

func nativeFaultFamilyTicks(t *testing.T, f *pairedGraphFaultNetwork, member uint64, count int) []nativeFaultFamilyTick {
	t.Helper()
	if member < 1 || member > 6 || count < 1 || count > 32 {
		t.Fatal("finite actual Tick vector")
	}
	rows := make([]nativeFaultFamilyTick, 0, count)
	for ordinal := 1; ordinal <= count; ordinal++ {
		before := f.n.all[member].call(t, processRequest{op: processStateOp})
		response := f.n.all[member].call(t, processRequest{op: processTick})
		f.n.group(member).enqueue(t, response)
		f.n.group(member).drain(t)
		after := f.n.all[member].call(t, processRequest{op: processStateOp})
		if after.applied < before.applied {
			t.Fatal("actual Tick lost native prefix")
		}
		rows = append(rows, nativeFaultFamilyTick{Member: member, Ordinal: uint64(ordinal), Before: before.applied, After: after.applied, Response: p06ProcessWire(t, response)})
	}
	return rows
}

// The old process must already be reaped. Recovery reads only its sealed native
// byte catalog and immutable images; current native files are archived without
// reading them. This explicit model never asserts the archive was durable.
func nativeFaultFamilyInstallModel(t *testing.T, f *pairedGraphFaultNetwork, member uint64, capture nativeFaultFamilyReceipt) nativeFaultFamilyModel {
	t.Helper()
	prior := f.n.all[member]
	if !prior.closed || prior.cmd.ProcessState == nil || capture.Member != member || capture.PID != uint64(prior.cmd.Process.Pid) || capture.ModelCookie == ([16]byte{}) || capture.Policy != nativeDurableDefaultPolicy() {
		t.Fatal("model recovery lacks reaped genuine source binding")
	}
	dir := filepath.Join(f.n.dir, fmt.Sprint(member))
	shadow := nativeDurableShadowPath(dir, prior.cmd.Process.Pid)
	image, digest, err := nativeDurableReadImage(t.Context(), shadow, capture.Policy)
	if err != nil || image.Cookie != capture.ModelCookie || image.Member != member || image.Ledger.Operations < capture.ModelLedger.Operations || image.Ledger.WorkBytes < capture.ModelLedger.WorkBytes {
		t.Fatal("last durable model catalog lost its acquired lineage", err)
	}
	// Include the exact canonical native catalog bytes in the bounded artifact.
	body, err := json.Marshal(image)
	if err != nil || len(body) > capture.Policy.CatalogBytes {
		t.Fatal("native catalog bound", err)
	}
	checksum := sha256.Sum256(body)
	wire := append(append(body, '\n'), []byte(hex.EncodeToString(checksum[:]))...)
	if sha256.Sum256(wire) != digest {
		t.Fatal("catalog canonical source changed")
	}
	recovered, err := nativeDurableRecover(t.Context(), shadow, f.n.dir, capture.ModelCookie, digest, capture.Policy)
	if err != nil {
		t.Fatal("native synced-image reconstruction", err)
	}
	archive := fmt.Sprintf("%s.volatile.%d", dir, prior.cmd.Process.Pid)
	if _, err := os.Lstat(archive); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("model archive is not exclusively owned", err)
	}
	if err := os.Rename(dir, archive); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(recovered.Directory, dir); err != nil {
		t.Fatal("installing owned model directory", errors.Join(err, os.Rename(archive, dir)))
	}
	directory, err := os.Open(f.n.dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
		t.Fatal(err)
	}
	return nativeFaultFamilyModel{Capture: capture, Image: wire, Recovery: recovered, SourceVolume: dir, VolatileArchive: archive}
}

// The same finite 512-record/16MiB envelope as the earlier actual fault bridge.
// Model shadows, archived volatile bytes, recovery and all six child files are
// included in the physical tree census; theoretical grants are separate.
func writeNativeFaultFamilyArtifact(t *testing.T, f *pairedGraphFaultNetwork, artifact nativeFaultFamilyArtifact) {
	t.Helper()
	artifact.Version, artifact.Profile = 1, nativeFaultFamilyProfile
	artifact.Scope = "Actual native graph and process schedules; synced-image cases are an explicit VFS model, not physical power-loss evidence. Full V2 workload/RTT matrix remains separate."
	var err error
	artifact.DiskBytes, artifact.AllocatedDiskBytes, err = nativeGraphCostFileBytes(f.n.dir)
	if err != nil {
		t.Fatal("all-child/model disk accounting", err)
	}
	path := filepath.Join(f.n.dir, "native-fault-family-"+artifact.Case+".jsonl")
	if artifact.Replay != nil {
		artifact.Version, artifact.Profile = 2, nativeTransitionComposedProfile
		path = filepath.Join(os.Getenv("RHO_GRAPH_COMPOSED_OUTPUT"), "native-composed-"+artifact.Case+".jsonl")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	digest := sha256.New()
	total := 0
	write := func(value any, hashed bool) {
		t.Helper()
		wire, err := json.Marshal(value)
		if err != nil || len(wire)+1 > 512<<10 || len(wire)+1 > (16<<20)-total {
			t.Fatal("bounded family artifact", err)
		}
		wire = append(wire, '\n')
		n, err := file.Write(wire)
		if err != nil || n != len(wire) {
			t.Fatal("family artifact write", errors.Join(io.ErrShortWrite, err))
		}
		if hashed {
			_, _ = digest.Write(wire)
		}
		total += len(wire)
	}
	header := artifact
	header.Records = nil
	header.Snapshot = nil
	if header.Replay != nil {
		replay := *header.Replay
		replay.Actions = nil
		header.Replay = &replay
	}
	if header.Model != nil {
		model := *header.Model
		model.Image = nil
		header.Model = &model
	}
	write(struct {
		Type     string
		Artifact nativeFaultFamilyArtifact
	}{"header", header}, true)
	if artifact.Replay != nil {
		for sequence, action := range artifact.Replay.Actions {
			write(struct {
				Type     string
				Sequence int
				Action   nativeTransitionComposedActionResult
			}{"replay-action", sequence, action}, true)
		}
	}
	if artifact.Model != nil {
		image := artifact.Model.Image
		if len(image) == 0 || len(image) > nativeDurableDefaultPolicy().CatalogBytes+65 {
			t.Fatal("model image envelope bound")
		}
		for sequence := 0; len(image) != 0; sequence++ {
			n := min(64<<10, len(image))
			write(struct {
				Type     string
				Sequence int
				Data     []byte
			}{"model-image", sequence, image[:n]}, true)
			image = image[n:]
		}
	}
	if len(artifact.Snapshot) > 16 {
		t.Fatal("snapshot notice envelope cap")
	}
	for sequence, boundary := range artifact.Snapshot {
		write(struct {
			Type     string
			Sequence int
			Boundary nativeFaultSnapshotBoundary
		}{"snapshot-boundary", sequence, boundary}, true)
	}
	if len(artifact.Records) > 512 {
		t.Fatal("family native prefix cap")
	}
	for _, record := range artifact.Records {
		write(struct {
			Type   string
			Record lifecycleRawRecord
		}{"record", record}, true)
	}
	write(struct {
		Type          string
		Rows          int
		Through       [2]uint64
		PayloadSHA256 string
	}{"footer", len(artifact.Records), artifact.Through, hex.EncodeToString(digest.Sum(nil))}, false)
	closed = true
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("native composed D genuine source artifact", path)
}
