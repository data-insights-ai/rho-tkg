package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

const nativeDecisionProfile = "native-queued-original-decisions-v1"
const nativeDecisionBodyBytes = 64 << 10
const nativeDecisionFrameBytes = 512 << 10
const nativeDecisionControls = 16
const certifiedNativeDecisionBusiness = certifiedGraphCost + 1

func nativeDecisionBusiness(o *certifiedProcessOwners, r processRequest) (allocationProposal, error) {
	if o == nil || o.decisions == nil || o.recipient == nil || len(r.data) != 0 || r.index != 3 && r.index != 8 {
		return allocationProposal{}, errInvalid
	}
	// Retry keys are selected BEFORE either original exists. This is a finite
	// pure key selector, as in crossRequestsWithLoser; it creates no authority.
	ns := namespace{o.host.machine.ns.graph, r.index}
	var selected requestID
	var priority [16]byte
	for value := 1; value < 256; value++ {
		id := requestID{byte(value), 0xd7} // #nosec G115 -- fixed client key selector1..255.
		tx := crossTransactionID(ns, id)
		if selected == (requestID{}) || r.index == 3 && bytes.Compare(tx[:], priority[:]) < 0 || r.index == 8 && bytes.Compare(tx[:], priority[:]) > 0 {
			selected, priority = id, tx
		}
	}
	p, err := nativeLifecycleProposal(o, processRequest{index: 3}, selected)
	if err != nil {
		return allocationProposal{}, err
	}
	command, err := decodePartitionGraphCommand(p.wire, o.host.machine.limits)
	if err != nil {
		return allocationProposal{}, err
	}
	if r.index == 3 {
		if len(command.request.claims) != 3 || command.request.claims[0].grant.sequence != 1 {
			return allocationProposal{}, errInvalid
		}
		// Deliberately reference an uninstalled grant. The native Prepare must
		// decide its real NO; no fixture supplies that outcome or a proof.
		command.request.claims[0].grant.sequence = 2
	}
	return o.host.PreparePartitionGraphOperations(selected, command.request.operations, 3, command.request.claims)
}

// Only a business retry key is caller data. Both phase proposals enter slots
// directly from their original coordinator's production before-submit seam.
// The control protocol has no proposal, vote, GCA or GCF import operation.
type nativeDecisionControl struct {
	Version  uint8
	Action   string
	Cookie   [16]byte
	Sequence uint64
	Mask     uint8
	Order    string
	Original []byte
}

type nativeDecisionReceipt struct {
	Version                                                      uint8
	Profile                                                      string
	Cookie                                                       [16]byte
	Sequence                                                     uint64
	Member, PID                                                  uint64
	Mask                                                         uint8
	Order                                                        string
	Queued, Closed                                               bool
	Original, Manifest                                           []byte
	Candidates                                                   [2][]byte // inert evidence only: capacity ABORT, then all-YES COMMIT
	EntryWires                                                   [2][]byte // actual retained native log inputs, in attempted queue order
	Accepted                                                     [2]bool   // indexed by attempted queue order, not by slot
	Errors                                                       [2]uint16
	BeforeApplied, AfterApplied, BeforeLastIndex, AfterLastIndex uint64
	Controls                                                     int
	PrepaidBytes                                                 int
}

type nativeDecisionOwner struct {
	mu                    sync.Mutex
	ctx                   context.Context
	cancel                context.CancelFunc
	coordinator           *crossProcessCoordinator
	member                uint64
	cookie                [16]byte
	arena                 *graphstate.OutputBudget
	prepaid, controls     int
	armed, queued, closed bool
	expected              crossProcessResume
	original, business    []byte
	tx                    crossTransaction
	slots                 [2]allocationProposal
	mask                  uint8
	changed               chan struct{}
	lastOrder             string
	last                  []byte
	lastErr               error
}

func newNativeDecisionOwner(ctx context.Context, c *crossProcessCoordinator, member uint64) (*nativeDecisionOwner, error) {
	if ctx == nil || c == nil || c.host == nil || c.host.machine == nil || member < 1 || member > 6 {
		return nil, errInvalid
	}
	l := c.host.machine.limits
	// Fund the whole finite owner before publishing the hook or retaining an
	// original. Quotas do not allocate memory and are not measured RSS/output.
	if l.commandBytes < 1 || l.commandBytes > (l.outputBytes-2*nativeDecisionControls*nativeDecisionFrameBytes)/4 {
		return nil, errLimit
	}
	prepaid := 4*l.commandBytes + 2*nativeDecisionControls*nativeDecisionFrameBytes
	parent, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		return nil, err
	}
	arena, err := parent.Child(prepaid)
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithTimeout(ctx, time.Minute)
	o := &nativeDecisionOwner{ctx: lifetime, cancel: cancel, coordinator: c, member: member, arena: arena, prepaid: prepaid, changed: make(chan struct{}, 1)}
	if _, err := io.ReadFull(rand.Reader, o.cookie[:]); err != nil {
		cancel()
		return nil, err
	}
	// Assignments are complete before deadline cleanup can touch the owner.
	context.AfterFunc(lifetime, o.close)
	return o, nil
}

func (o *nativeDecisionOwner) signal() {
	select {
	case o.changed <- struct{}{}:
	default:
	}
}

func (o *nativeDecisionOwner) close() {
	if o == nil {
		return
	}
	o.cancel()
	o.mu.Lock()
	o.closed = true
	o.slots = [2]allocationProposal{}
	o.tx = crossTransaction{}
	o.original, o.business = nil, nil
	o.last = nil
	o.lastErr = nil
	o.mu.Unlock()
	o.signal()
}

func (o *nativeDecisionOwner) hold(ctx context.Context, tx crossTransaction, p allocationProposal) error {
	if o == nil || ctx == nil {
		return errInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	o.mu.Lock()
	if o.closed || o.queued || o.ctx.Err() != nil {
		o.mu.Unlock()
		return errors.Join(errCrossProcessPending, o.ctx.Err())
	}
	if !o.armed {
		o.mu.Unlock()
		return nil
	}
	c := o.coordinator
	if tx.owner != c.host || tx.digest != sha256.Sum256(tx.manifest) || len(p.wire) > nativeDecisionBodyBytes || cap(p.wire) != len(p.wire) || len(tx.manifest) > nativeDecisionBodyBytes || cap(tx.manifest) != len(tx.manifest) || p.target != c.host.machine.scope() {
		o.mu.Unlock()
		return errInvalid
	}
	// These are predicate checks over the actual constructed object. Decoding
	// here never creates an authority or a private proof/stamp.
	decodeLimits := c.host.machine.limits
	decodeLimits.commandOwnedBytes = min(decodeLimits.commandOwnedBytes, 8192+16*len(p.wire))
	if err := o.arena.Reserve(decodeLimits.commandOwnedBytes + cap(p.wire) + cap(tx.manifest)); err != nil {
		o.mu.Unlock()
		return outputBudgetFailure(err)
	}
	command, _, err := decodeCrossCommand(p.wire, decodeLimits)
	if err != nil || command.phase != crossDecide || command.ns != c.host.machine.ns || !bytes.Equal(command.manifest, tx.manifest) {
		o.mu.Unlock()
		return errors.Join(errInvalid, err)
	}
	c.mu.Lock()
	run := c.runs[o.expected.tx]
	valid := run != nil && run.busy && run.resume == o.expected && run.tx.owner == c.host && run.tx.digest == tx.digest && bytes.Equal(run.tx.manifest, tx.manifest)
	c.mu.Unlock()
	if !valid {
		o.mu.Unlock()
		return errInvalid
	}
	slot := 0
	if command.conflict == nil {
		slot = 1
		if len(command.proofs) != 2 {
			o.mu.Unlock()
			return errInvalid
		}
		for i, vote := range command.proofs {
			partition := uint64(3)
			if i == 1 {
				partition = 8
			}
			if vote.ns.partition != partition || vote.tx != o.expected.tx || vote.digest != tx.digest || vote.phase != crossPrepare || !vote.yes {
				o.mu.Unlock()
				return errInvalid
			}
		}
	} else {
		loser, digest, coordinator := command.conflict.loser()
		if loser != o.expected.tx || digest != tx.digest || coordinator != c.host.machine.ns.partition {
			o.mu.Unlock()
			return errInvalid
		}
	}
	bit := uint8(1 << slot)
	if o.mask&bit != 0 || o.mask != 0 && (o.tx.owner != tx.owner || o.tx.digest != tx.digest || !bytes.Equal(o.tx.manifest, tx.manifest)) {
		o.mu.Unlock()
		return errInvalid
	}
	o.tx = tx
	o.slots[slot] = p // Preserve the actual builder-owned proposal; no wire import.
	o.mask |= bit
	o.mu.Unlock()
	o.signal()
	// The original Run retires as pending. The bounded child owner retains the
	// genuine candidate; cancellation is never a durable decision or NO vote.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.ctx.Done():
		return o.ctx.Err()
	}
}

func decodeNativeDecisionControl(wire []byte) (nativeDecisionControl, error) {
	var request nativeDecisionControl
	if len(wire) == 0 || len(wire) > nativeDecisionFrameBytes {
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

func (o *nativeDecisionOwner) receiptLocked() nativeDecisionReceipt {
	r := nativeDecisionReceipt{Version: 1, Profile: nativeDecisionProfile, Cookie: o.cookie, Sequence: 1, Member: o.member, PID: uint64(os.Getpid()), Mask: o.mask, Queued: o.queued, Closed: o.closed, Controls: o.controls, PrepaidBytes: o.prepaid, Original: o.original, Manifest: o.tx.manifest}
	for i := range o.slots {
		r.Candidates[i] = o.slots[i].wire
	}
	return r
}

// control's caller is the serialized native child event lane. A wait occupies
// only one process correlation, never one of the two source worker slots.
func (o *nativeDecisionOwner) control(wire []byte) ([]byte, uint8, allocationEvent, error) {
	if o == nil {
		return nil, 0, allocationEvent{}, errInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.controls >= nativeDecisionControls {
		return nil, 0, allocationEvent{}, errLimit
	}
	o.controls++
	if err := o.arena.Reserve(2 * nativeDecisionFrameBytes); err != nil {
		return nil, 0, allocationEvent{}, outputBudgetFailure(err)
	}
	r, err := decodeNativeDecisionControl(wire)
	if err != nil {
		return nil, 0, allocationEvent{}, err
	}
	if r.Action == "inspect" {
		if r.Cookie != ([16]byte{}) && r.Cookie != o.cookie || r.Sequence != 0 || r.Mask != 0 || r.Order != "" || len(r.Original) != 0 {
			return nil, 0, allocationEvent{}, errInvalid
		}
		body, err := json.Marshal(o.receiptLocked())
		return body, 0, allocationEvent{}, err
	}
	if r.Cookie != o.cookie || r.Cookie == ([16]byte{}) || r.Sequence != 1 || o.closed || o.ctx.Err() != nil {
		return nil, 0, allocationEvent{}, errors.Join(errInvalid, o.ctx.Err())
	}
	switch r.Action {
	case "arm":
		if o.armed || o.queued || r.Mask != 0 || r.Order != "" || len(r.Original) == 0 || len(r.Original) > nativeDecisionBodyBytes {
			return nil, 0, allocationEvent{}, errInvalid
		}
		decodeLimits := o.coordinator.host.machine.limits
		decodeLimits.commandOwnedBytes = min(decodeLimits.commandOwnedBytes, 8192+16*len(r.Original))
		if err := o.arena.Reserve(decodeLimits.commandOwnedBytes + 8*len(r.Original)); err != nil {
			return nil, 0, allocationEvent{}, outputBudgetFailure(err)
		}
		original, err := decodePartitionGraphCommand(r.Original, decodeLimits)
		if err != nil {
			return nil, 0, allocationEvent{}, err
		}
		business, err := o.coordinator.business(original.request.id, original.request.operations, original.request.revision.Provenance(), original.request.claims)
		if err != nil {
			return nil, 0, allocationEvent{}, err
		}
		if len(business) != len(r.Original) {
			return nil, 0, allocationEvent{}, errInvalid
		}
		h := o.coordinator.host
		o.original, o.business = bytes.Clone(r.Original), business
		o.expected = crossProcessResume{scope: h.machine.scope(), request: original.request.id, tx: crossTransactionID(h.machine.ns, original.request.id), business: sha256.Sum256(business)}
		o.armed = true
	case "wait":
		if !o.armed || o.queued || len(r.Original) != 0 || r.Order != "" || r.Mask != 1 && r.Mask != 3 {
			return nil, 0, allocationEvent{}, errInvalid
		}
		if o.mask&r.Mask != r.Mask {
			return nil, r.Mask, allocationEvent{}, nil
		}
	case "queue":
		if len(r.Original) != 0 || r.Mask != 0 || r.Order != "commit-first" && r.Order != "abort-first" {
			return nil, 0, allocationEvent{}, errInvalid
		}
		if o.queued {
			if r.Order != o.lastOrder {
				return nil, 0, allocationEvent{}, errInvalid
			}
			return bytes.Clone(o.last), 0, allocationEvent{}, o.lastErr // No native re-submit or uncertainty laundering.
		}
		if !o.armed || o.mask != 3 {
			return nil, 0, allocationEvent{}, errInvalid
		}
		o.coordinator.mu.Lock()
		run := o.coordinator.runs[o.expected.tx]
		retired := run != nil && !run.busy && run.resume == o.expected && run.tx.owner == o.tx.owner && run.tx.digest == o.tx.digest && bytes.Equal(run.tx.manifest, o.tx.manifest)
		o.coordinator.mu.Unlock()
		if !retired {
			return nil, 0, allocationEvent{}, errInvalid
		}
		// The two decoded native entries are an additional observation owner;
		// reserve their complete admitted prefix before either genuine Submit.
		if err := o.arena.Reserve(2*nativeDecisionBodyBytes + 1024); err != nil {
			return nil, 0, allocationEvent{}, outputBudgetFailure(err)
		}
		return o.queueLocked(r.Order)
	default:
		return nil, 0, allocationEvent{}, errInvalid
	}
	body, err := json.Marshal(o.receiptLocked())
	return body, 0, allocationEvent{}, err
}

func (o *nativeDecisionOwner) ready(mask uint8) ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.ctx.Err() != nil {
		return nil, errors.Join(context.Canceled, o.ctx.Err())
	}
	if o.mask&mask != mask {
		return nil, nil
	}
	return json.Marshal(o.receiptLocked())
}

// queueLocked consumes the slots once, then releases the state lock before any
// Host call. Both actual submissions precede publication of either event.
func (o *nativeDecisionOwner) queueLocked(order string) ([]byte, uint8, allocationEvent, error) {
	proposals := o.slots
	receipt := o.receiptLocked()
	o.queued, o.lastOrder = true, order
	o.slots = [2]allocationProposal{}
	o.mu.Unlock()
	h := o.coordinator.host
	receipt.Order, receipt.Queued = order, true
	receipt.BeforeApplied = h.driver.Applied()
	var event allocationEvent
	var err error
	receipt.BeforeLastIndex, err = h.machine.store.LastIndex()
	if err == nil {
		for i := range 2 {
			if err := o.ctx.Err(); err != nil {
				receipt.Errors[i] = graphErrorMask(err)
				break
			}
			slot := i
			if order == "commit-first" {
				slot = 1 - i
			}
			actual, submitErr := h.Submit(proposals[slot])
			receipt.Accepted[i] = submitErr == nil
			receipt.Errors[i] = graphErrorMask(submitErr)
			err = errors.Join(err, submitErr)
			if submitErr == nil {
				if len(event.packets)+len(actual.packets) > processPacketCount || len(actual.snapshotSends)+len(actual.replies)+len(actual.crossReplies)+len(actual.capacityReplies)+len(actual.cutReplies)+len(actual.requestReplies)+len(actual.moveReplies) != 0 {
					err = errLimit
					break
				}
				event.packets = append(event.packets, actual.packets...)
			}
		}
	}
	receipt.AfterApplied = h.driver.Applied()
	var lastErr error
	receipt.AfterLastIndex, lastErr = h.machine.store.LastIndex()
	err = errors.Join(err, lastErr, o.ctx.Err())
	if err == nil && receipt.Accepted[0] && receipt.Accepted[1] {
		if receipt.BeforeLastIndex > ^uint64(0)-3 || receipt.AfterLastIndex != receipt.BeforeLastIndex+2 {
			err = errInvalid
		} else {
			entries, readErr := h.machine.store.Entries(receipt.BeforeLastIndex+1, receipt.AfterLastIndex+1, 2*nativeDecisionBodyBytes+1024)
			err = readErr
			if readErr == nil && len(entries) != 2 {
				err = errInvalid
			}
			if err == nil {
				for i, entry := range entries {
					slot := i
					if order == "commit-first" {
						slot = 1 - i
					}
					if entry.GetIndex() != receipt.BeforeLastIndex+1+uint64(i) || !bytes.Equal(entry.GetData(), proposals[slot].wire) {
						err = errInvalid
						break
					}
					receipt.EntryWires[i] = entry.GetData()
				}
			}
		}
	}
	body, encodeErr := json.Marshal(receipt)
	if len(body) > nativeDecisionFrameBytes {
		encodeErr = errLimit
		body = nil
	}
	o.mu.Lock()
	if !o.closed {
		o.last = bytes.Clone(body)
		o.lastErr = errors.Join(err, encodeErr)
	}
	return body, 0, event, errors.Join(err, encodeErr)
}

func nativeDecisionCall(t *testing.T, p *graphProcess, request nativeDecisionControl) (nativeDecisionReceipt, processResponse) {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	response := p.call(t, processRequest{op: processNativeDecisionControl, data: body})
	var receipt nativeDecisionReceipt
	if err := json.Unmarshal(response.aux, &receipt); err != nil || receipt.Version != 1 || receipt.Profile != nativeDecisionProfile || receipt.Cookie == ([16]byte{}) || receipt.PID != uint64(p.cmd.Process.Pid) || receipt.Controls < 1 || receipt.Controls > nativeDecisionControls || receipt.PrepaidBytes < 1 {
		t.Fatal("actual private decision owner receipt", err, receipt)
	}
	return receipt, response
}

func nativeDecisionRetire(t *testing.T, call <-chan graphProcessAnswer, cancel context.CancelFunc, tx [16]byte) []byte {
	t.Helper()
	cancel()
	select {
	case answer := <-call:
		result, err := decodeGraphProcessCoordinatorResult(answer.response)
		if !errors.Is(answer.err, context.Canceled) || !errors.Is(err, errCrossProcessPending) || result.resume.tx != tx || result.decision != (crossStatus{}) {
			t.Fatal("retained candidate caller is not genuine pending", answer.err, err)
		}
		return p06ProcessWire(t, answer.response)
	case <-time.After(10 * time.Second):
		t.Fatal("retained candidate worker not retired")
	}
	return nil
}

type nativeDecisionArtifact struct {
	Version                                              uint8
	Profile, Case                                        string
	Records                                              []lifecycleRawRecord
	From, Through                                        [2]uint64
	OriginalA, OriginalB, ManifestA                      []byte
	Capacity                                             pairedFaultBoundary
	Queue                                                nativeDecisionReceipt
	Pending                                              [2][]byte
	DecisionA, DecisionB                                 []byte
	Before, Retained, Current, Reopened, ReopenedCurrent p06Observation
	Processes                                            []pairedFaultProcess
	Duplicates                                           []nativeDecisionRetry
}

type nativeDecisionRetry struct {
	Actor              uint64
	Original, Response []byte
	From, Through      [2]uint64
	Records            []lifecycleRawRecord
}

func nativeDecisionWrite(t *testing.T, dir string, artifact nativeDecisionArtifact) {
	t.Helper()
	file, err := os.OpenFile(filepath.Join(dir, "native-decision-race-"+artifact.Case+".jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
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
	digest, bytesUsed := sha256.New(), 0
	write := func(value any, hashed bool) {
		body, err := json.Marshal(value)
		if err != nil || len(body)+1 > nativeDecisionFrameBytes || len(body)+1 > (16<<20)-bytesUsed {
			t.Fatal("finite decision artifact admission", err)
		}
		body = append(body, '\n')
		if _, err := file.Write(body); err != nil {
			t.Fatal(err)
		}
		if hashed {
			_, _ = digest.Write(body)
		}
		bytesUsed += len(body)
	}
	header := artifact
	header.Records = nil
	write(struct {
		Type     string
		Artifact nativeDecisionArtifact
	}{"header", header}, true)
	if len(artifact.Records) > 512 {
		t.Fatal("finite decision record census")
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
	}{"footer", len(artifact.Records), artifact.Through, fmt.Sprintf("%x", digest.Sum(nil))}, false)
	closed = true
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeComposedFaultQueuedCapacityDecisionsKeepFirstOriginal(t *testing.T) {
	for _, order := range []string{"commit-first", "abort-first"} {
		t.Run(order, func(t *testing.T) {
			t.Setenv("RHO_GRAPH_DECISION_FAULT_PROCESS", "1")
			f := newPairedGraphFaultNetwork(t)
			nativeFaultSeed(t, f)
			old := p06Observe(f, 6, graphReadFresh, graphCut{})
			cut, err := pairedFaultCut(old.Cut)
			if err != nil {
				t.Fatal(err)
			}
			factory := func(partition uint64) allocationProposal {
				r := f.n.all[1].call(t, processRequest{op: processCertifiedAction, entity: certifiedNativeDecisionBusiness, index: partition})
				p, err := decodeCertifiedProcessProposal(r.aux)
				if err != nil {
					t.Fatal(err)
				}
				return p
			}
			a, b := factory(3), factory(8)
			ac, err := decodePartitionGraphCommand(a.wire, defaultMaterializerLimits())
			if err != nil {
				t.Fatal(err)
			}
			bc, err := decodePartitionGraphCommand(b.wire, defaultMaterializerLimits())
			if err != nil {
				t.Fatal(err)
			}
			txa := crossTransactionID(namespace{ac.request.ns.graph, 3}, ac.request.id)
			txb := crossTransactionID(namespace{bc.request.ns.graph, 8}, bc.request.id)
			if bytes.Compare(txa[:], txb[:]) >= 0 {
				t.Fatal("actual configured original priority")
			}
			from := f.indexes()
			hub := f.n.all[1].mux.hub
			entered, release, err := nativeFaultBeforePhase(t.Context(), hub, 3, 3, crossPrepare)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			var captureMu sync.Mutex
			var capacity pairedFaultBoundary
			hub.mu.Lock()
			hub.afterRelay = func(ctx context.Context, origin, partition uint64, request, reply []byte) error {
				if origin != 4 || partition != 3 {
					return nil
				}
				q, err := decodeGraphSourceRequest(request)
				if err != nil || q.kind != sourceCrossRead || !bytes.HasPrefix(q.command, []byte("GCQ\x01")) {
					return err
				}
				captureMu.Lock()
				defer captureMu.Unlock()
				if len(capacity.Request) != 0 {
					return errInvalid
				}
				capacity = pairedFaultBoundary{Request: bytes.Clone(request), Reply: bytes.Clone(reply)}
				return nil
			}
			hub.mu.Unlock()
			defer func() { hub.mu.Lock(); hub.afterRelay = nil; hub.mu.Unlock() }()
			aCall, aCancel := f.beginRun(3, a)
			defer aCancel()
			aPrepare := f.awaitBoundary(entered)
			aCommand, _, err := nativeFaultPhase(aPrepare.Request)
			if err != nil {
				t.Fatal(err)
			}
			owner, _ := nativeDecisionCall(t, f.n.all[4], nativeDecisionControl{Version: 1, Action: "inspect"})
			nativeDecisionCall(t, f.n.all[4], nativeDecisionControl{Version: 1, Action: "arm", Cookie: owner.Cookie, Sequence: 1, Original: b.wire})
			bCall, bCancel := f.beginRun(4, b)
			defer bCancel()
			first, _ := nativeDecisionCall(t, f.n.all[4], nativeDecisionControl{Version: 1, Action: "wait", Cookie: owner.Cookie, Sequence: 1, Mask: 1})
			if first.Mask != 1 || len(first.Candidates[0]) == 0 || len(first.Candidates[1]) != 0 {
				t.Fatal("missing actual capacity candidate")
			}
			pending0 := nativeDecisionRetire(t, bCall, bCancel, txb)
			release()
			var aResult crossProcessResult
			select {
			case answer := <-aCall:
				aResult, err = decodeGraphProcessCoordinatorResult(answer.response)
				if answer.err != nil || err != nil || aResult.decision.commit || aResult.decision.tx != txa {
					t.Fatal("uninstalled grant did not produce genuine original ABORT", answer.err, err, aResult)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("A original did not settle")
			}
			f.drain()
			bCall, bCancel = f.beginRun(4, b)
			defer bCancel()
			both, _ := nativeDecisionCall(t, f.n.all[4], nativeDecisionControl{Version: 1, Action: "wait", Cookie: owner.Cookie, Sequence: 1, Mask: 3})
			if both.Mask != 3 || !bytes.Equal(first.Candidates[0], both.Candidates[0]) || !bytes.Equal(first.Manifest, both.Manifest) {
				t.Fatal("retained private candidate changed")
			}
			pending1 := nativeDecisionRetire(t, bCall, bCancel, txb)
			f.packetDrain()
			queued, output := nativeDecisionCall(t, f.n.all[4], nativeDecisionControl{Version: 1, Action: "queue", Cookie: owner.Cookie, Sequence: 1, Order: order})
			if queued.Accepted != ([2]bool{true, true}) || queued.Errors != ([2]uint16{}) || queued.BeforeApplied != queued.AfterApplied || queued.AfterLastIndex != queued.BeforeLastIndex+2 {
				t.Fatal("two candidates were not actually queued before application", queued)
			}
			f.n.group(4).enqueue(t, output)
			f.n.group(4).drain(t)
			f.drain()
			replayed, replayOutput := nativeDecisionCall(t, f.n.all[4], nativeDecisionControl{Version: 1, Action: "queue", Cookie: owner.Cookie, Sequence: 1, Order: order})
			if !reflect.DeepEqual(replayed, queued) || len(replayOutput.packets) != 0 {
				t.Fatal("queue replay repeated native admission")
			}
			// The next ordinary Run reads the genuine first durable D and resolves
			// every actual participant. It never rebuilds either candidate.
			response := f.n.all[4].call(t, processRequest{op: processSourceCoordinate, data: b.wire})
			result, err := decodeGraphProcessCoordinatorResult(response)
			if err != nil || result.decision.tx != txb || result.decision.commit != (order == "commit-first") {
				t.Fatal("later candidate reversed first durable decision", err, result)
			}
			f.drain()
			if result.decision.commit {
				f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "edge-create", Round: result.decision.round})
			}
			current := p06Observe(f, 6, graphReadFresh, graphCut{})
			p06AssertView(t, current.View, result.decision.commit, false, false)
			retained := p06Observe(f, 6, graphReadAt, cut)
			if !reflect.DeepEqual(old, retained) {
				t.Fatal("decision race substituted current at old cut")
			}
			var duplicates []nativeDecisionRetry
			for i, original := range []allocationProposal{a, b} {
				actor := uint64(3)
				want := aResult.decision
				if i == 1 {
					actor = 4
					want = result.decision
				}
				start := f.indexes()
				response := f.n.all[actor].call(t, processRequest{op: processSourceCoordinate, data: original.wire})
				actual, err := decodeGraphProcessCoordinatorResult(response)
				if err != nil || actual.decision != want {
					t.Fatal("same business original changed durable decision", err, actual, want)
				}
				f.drain()
				end := f.indexes()
				tail := p06Records(f, start, end)
				for _, record := range tail {
					if len(record.CDC) != 0 {
						t.Fatal("business original replay emitted CDC")
					}
				}
				duplicates = append(duplicates, nativeDecisionRetry{Actor: actor, Original: original.wire, Response: p06ProcessWire(t, response), From: start, Through: end, Records: tail})
			}
			killed := f.kill(4, "replica")
			f.elect(4, 5)
			rejoined := f.replace(4)
			reopened := p06Observe(f, 4, graphReadAt, cut)
			if !reflect.DeepEqual(old, reopened) {
				t.Fatal("same-directory old cut changed")
			}
			reopenedCurrent := p06Observe(f, 4, graphReadFresh, graphCut{})
			if !reflect.DeepEqual(current.Native, reopenedCurrent.Native) {
				t.Fatal("replacement changed first-decision participant effects")
			}
			start := f.indexes()
			response = f.n.all[4].call(t, processRequest{op: processSourceCoordinate, data: b.wire})
			recoveredResult, replayErr := decodeGraphProcessCoordinatorResult(response)
			if replayErr != nil || recoveredResult.decision != result.decision {
				t.Fatal("replacement changed same original decision", replayErr, recoveredResult)
			}
			f.drain()
			end := f.indexes()
			tail := p06Records(f, start, end)
			for _, record := range tail {
				if len(record.CDC) != 0 {
					t.Fatal("replacement original replay emitted CDC")
				}
			}
			duplicates = append(duplicates, nativeDecisionRetry{Actor: 4, Original: b.wire, Response: p06ProcessWire(t, response), From: start, Through: end, Records: tail})
			through := f.indexes()
			captureMu.Lock()
			actualCapacity := capacity
			captureMu.Unlock()
			decisionA, err := encodeCrossStatus(aResult.decision)
			if err != nil {
				t.Fatal(err)
			}
			decisionB, err := encodeCrossStatus(result.decision)
			if err != nil {
				t.Fatal(err)
			}
			records := p06Records(f, from, through)
			resolves := 0
			for _, record := range records {
				status, statusErr := decodeCrossStatus(record.Outcome)
				if statusErr == nil && status.tx == txa {
					p06CheckRawRecord(t, record, crossManifest{tx: txa, digest: aResult.decision.digest}, p06LocalRecord{}, nil, p06LocalRecord{})
					if len(record.CDC) != 0 {
						t.Fatal("A ABORT emitted business CDC")
					}
					continue
				}
				p06CheckRawRecord(t, record, crossManifest{tx: txb, digest: result.decision.digest}, p06LocalRecord{}, nil, p06LocalRecord{})
				if statusErr != nil || status.phase != crossResolve || status.index != record.Index {
					continue
				}
				resolves++
				if status.commit != result.decision.commit || status.round != result.decision.round {
					t.Fatal("participant changed original first decision")
				}
				if !status.commit {
					if len(record.CDC) != 0 {
						t.Fatal("B ABORT emitted business CDC")
					}
					continue
				}
				original := outcome{ns: status.ns, kind: graphOperations, identity: status.tx, hash: status.digest, index: status.index, disposition: applied}
				logical, err := decodeTypedChangeEnvelope(record.CDC, original, defaultMaterializerLimits(), true)
				if err != nil {
					t.Fatal("queued first-decision complete CDC", err)
				}
				if record.Partition == 8 {
					if len(logical.groups)+len(logical.entities)+len(logical.lives)+len(logical.values) != 0 {
						t.Fatal("validation-only B acquired business effects")
					}
					continue
				}
				if len(logical.groups) != 2 || len(logical.entities) != 1 || len(logical.lives) != 1 || len(logical.values) != 1 || logical.entities[0].ID != 3 || logical.entities[0].Source != 1 || logical.entities[0].Target != 17 || logical.lives[0].Life != 4 || logical.lives[0].SourceLife != 2 || logical.lives[0].TargetLife != 18 || logical.values[0].ID != 5 {
					t.Fatal("queued COMMIT omitted or duplicated original declarations/groups")
				}
				value, ok := logical.values[0].Value.Int64Value()
				if !ok || value != 5 {
					t.Fatal("queued COMMIT changed exact typed dictionary value")
				}
				for i, group := range logical.groups {
					key, handle := graphstate.ComponentKey{Owner: 3, Kind: graphstate.Presence}, uint64(4)
					if i == 1 {
						key, handle = graphstate.ComponentKey{Owner: 3, Life: 4, Kind: graphstate.ScalarProperty, Name: "p"}, 5
					}
					if group.Key != key || len(group.Changes) != 1 || group.Changes[0].Before().Present() || group.Changes[0].Before().Revision().ID() != 0 || !group.Changes[0].After().Present() || group.Changes[0].After().Value().ID() != handle || group.Changes[0].After().Revision().ID() != status.round || group.Changes[0].After().Revision().Provenance() != 3 {
						t.Fatal("queued COMMIT changed original before/after")
					}
					p06CheckScope(t, group.Changes[0].Scope())
				}
			}
			if resolves != 2 {
				t.Fatal("queued original lacks both actual participant resolves", resolves)
			}
			nativeDecisionWrite(t, f.n.dir, nativeDecisionArtifact{Version: 2, Profile: nativeDecisionProfile, Case: order, From: from, Through: through, Records: records, OriginalA: a.wire, OriginalB: b.wire, ManifestA: aCommand.manifest, Capacity: actualCapacity, Queue: queued, Pending: [2][]byte{pending0, pending1}, DecisionA: decisionA, DecisionB: decisionB, Before: old, Retained: retained, Current: current, Reopened: reopened, ReopenedCurrent: reopenedCurrent, Processes: []pairedFaultProcess{killed, rejoined}, Duplicates: duplicates})
		})
	}
}

func TestNativeDecisionSubmissionSeamChecksContextAfterCallbackWithoutHostLock(t *testing.T) {
	n, c, ops, claims, _ := coordinatorNativeFixture(t)
	id := n.requestID()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	called := false
	c.beforeDecision = func(actual context.Context, tx crossTransaction, p allocationProposal) error {
		if actual != ctx || tx.owner != c.host || len(p.wire) == 0 || !c.host.mu.TryLock() {
			t.Fatal("before-submit callback context/ownership/Host lock")
		}
		c.host.mu.Unlock()
		// These are detached exact-capacity observations; corrupting them must
		// not poison the retained original used by the subsequent genuine Run.
		clear(tx.manifest)
		clear(p.wire)
		called = true
		cancel() // A nil callback return cannot hide same-operation cancellation.
		return nil
	}
	result, err := c.Run(ctx, id, ops, 37, claims)
	if !called || !errors.Is(err, context.Canceled) || !errors.Is(err, errCrossProcessPending) || result.decision != (crossStatus{}) {
		t.Fatal("canceled callback became completed", called, result, err)
	}
	if _, _, err := c.host.machine.observeCrossTransaction(crossQuery{tx: result.resume.tx, phase: crossDecide}, c.host.driver.Applied()); !errors.Is(err, replica.ErrUnavailable) {
		t.Fatal("callback cancellation invented a decision", err)
	}
	c.beforeDecision = func(_ context.Context, tx crossTransaction, p allocationProposal) error {
		clear(tx.manifest)
		clear(p.wire)
		return nil
	}
	recovered, err := c.Run(t.Context(), id, ops, 37, claims)
	if err != nil || !recovered.decision.commit || recovered.resume != result.resume {
		t.Fatal("mutating observer changed original recovery", err, recovered)
	}
	c.beforeDecision = nil
	again, err := c.Run(t.Context(), id, ops, 37, claims)
	if err != nil || again != recovered {
		t.Fatal("callback mutation changed actual submitted original", err, again)
	}
}

func TestNativeDecisionOwnerCloseRetiresActualCandidateWithoutImportingWire(t *testing.T) {
	n, c, ops, claims, _ := coordinatorNativeFixture(t)
	id := n.requestID()
	proposal, err := c.host.PreparePartitionGraphOperations(id, ops, 37, claims)
	if err != nil {
		t.Fatal(err)
	}
	o, err := newNativeDecisionOwner(t.Context(), c, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer o.close()
	control := func(request nativeDecisionControl) ([]byte, uint8, error) {
		wire, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		body, mask, event, err := o.control(wire)
		if len(event.packets) != 0 {
			t.Fatal("private guard dispatched native packets")
		}
		return body, mask, err
	}
	if _, _, err := control(nativeDecisionControl{Version: 1, Action: "arm", Cookie: o.cookie, Sequence: 1, Original: proposal.wire}); err != nil {
		t.Fatal(err)
	}
	c.beforeDecision = o.hold
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type completed struct {
		result crossProcessResult
		err    error
	}
	done := make(chan completed, 1)
	go func() { result, err := c.Run(ctx, id, ops, 37, claims); done <- completed{result, err} }()
	select {
	case <-o.changed:
	case <-time.After(10 * time.Second):
		t.Fatal("actual native candidate not retained")
	}
	if _, _, err := control(nativeDecisionControl{Version: 1, Action: "queue", Cookie: o.cookie, Sequence: 1, Order: "commit-first"}); !errors.Is(err, errInvalid) {
		t.Fatal("missing genuine capacity candidate accepted", err)
	}
	foreign := o.cookie
	foreign[0] ^= 1
	if _, _, err := control(nativeDecisionControl{Version: 1, Action: "wait", Cookie: foreign, Sequence: 1, Mask: 3}); !errors.Is(err, errInvalid) {
		t.Fatal("foreign owner entered", err)
	}
	o.close()
	var pending crossProcessResult
	select {
	case actual := <-done:
		pending = actual.result
		if !errors.Is(actual.err, context.Canceled) || !errors.Is(actual.err, errCrossProcessPending) || actual.result.decision != (crossStatus{}) {
			t.Fatal("owner close invented completion", actual)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("closed owner kept native callback")
	}
	o.mu.Lock()
	empty := len(o.slots[0].wire) == 0 && len(o.slots[1].wire) == 0 && o.tx.owner == nil
	o.mu.Unlock()
	if !empty {
		t.Fatal("closed owner retained authority")
	}
	if _, _, err := control(nativeDecisionControl{Version: 1, Action: "queue", Cookie: o.cookie, Sequence: 1, Order: "abort-first"}); !errors.Is(err, errInvalid) {
		t.Fatal("closed owner revived", err)
	}
	if _, _, err := c.host.machine.observeCrossTransaction(crossQuery{tx: pending.resume.tx, phase: crossDecide}, c.host.driver.Applied()); !errors.Is(err, replica.ErrUnavailable) {
		t.Fatal("owner close produced timeout ABORT", err)
	}
	c.beforeDecision = nil
	recovered, err := c.Run(t.Context(), id, ops, 37, claims)
	if err != nil || !recovered.decision.commit || recovered.resume != pending.resume {
		t.Fatal("original after lost owner did not recover genuinely", err, recovered)
	}
}
