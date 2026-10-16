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
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

const nativeDecisionProfile = "native-queued-original-decisions-v1"
const nativeDecisionBodyBytes = 64 << 10
const nativeDecisionFrameBytes = 512 << 10
const nativeDecisionControls = 16
const certifiedNativeDecisionBusiness = certifiedGraphCost + 1

func nativeDecisionFactoryMember(source uint64) (uint64, error) {
	if source < 1 || source > 3 {
		return 0, errInvalid
	}
	// The source follows the elected leader, but the factory's Recipient is
	// process-owned by the original A bootstrap member and is not replicated.
	return 1, nil
}

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
	case "campaign-frozen":
		// Fresh diagnostic only. The genuine B owner remains in this child;
		// foreign bytes can only be used as hostile admission inputs, never as
		// a replacement transaction, proof, or successful proposal authority.
		if os.Getenv("RHO_GRAPH_PROTOCOL_CAMPAIGN") != nativeProtocolCampaignProfile || !o.armed || o.queued || o.mask != 3 || r.Mask != 0 || r.Order != "" || len(r.Original) == 0 || len(r.Original) > nativeDecisionBodyBytes {
			return nil, 0, allocationEvent{}, errInvalid
		}
		o.coordinator.mu.Lock()
		run := o.coordinator.runs[o.expected.tx]
		retired := run != nil && !run.busy && run.resume == o.expected && run.tx.owner == o.tx.owner && run.tx.digest == o.tx.digest && bytes.Equal(run.tx.manifest, o.tx.manifest)
		o.coordinator.mu.Unlock()
		if !retired {
			return nil, 0, allocationEvent{}, errInvalid
		}
		if err := o.arena.Reserve(12*nativeDecisionBodyBytes + 8192 + 32*(len(r.Original)+len(o.tx.manifest))); err != nil {
			return nil, 0, allocationEvent{}, outputBudgetFailure(err)
		}
		tx := o.tx // The already-owned typed object, not a wire reconstruction.
		o.mu.Unlock()
		body, err := o.frozenRefusals(tx, r.Original)
		o.mu.Lock()
		return body, 0, allocationEvent{}, err
	default:
		return nil, 0, allocationEvent{}, errInvalid
	}
	body, err := json.Marshal(o.receiptLocked())
	return body, 0, allocationEvent{}, err
}

type nativeDecisionFrozenControl struct {
	Tag   byte
	Found bool
	Index uint64
	Value []byte
}

type nativeDecisionFrozenAttempt struct {
	Kind      string
	Target    []byte
	Command   []byte
	ErrorMask uint16
}

type nativeDecisionFrozenReceipt struct {
	Version                                                      uint8
	Cookie                                                       [16]byte
	Member, PID                                                  uint64
	GrantBytes                                                   uint64
	Manifest                                                     []byte
	BeforeApplied, AfterApplied, BeforeLastIndex, AfterLastIndex uint64
	Before, After                                                [4]nativeDecisionFrozenControl
	Attempts                                                     [3]nativeDecisionFrozenAttempt
}

// Called only by the serialized opt-in child control lane, outside coordinator
// and Host locks and outside SDK/submit callbacks. Every attempted body is a
// hostile input; only the original retained private slots can later succeed.
func (o *nativeDecisionOwner) frozenRefusals(tx crossTransaction, foreign []byte) ([]byte, error) {
	if o == nil || o.coordinator == nil || o.coordinator.host == nil {
		return nil, errInvalid
	}
	h := o.coordinator.host
	if err := o.ctx.Err(); err != nil {
		return nil, err
	}
	if tx.owner != h || tx.digest != sha256.Sum256(tx.manifest) {
		return nil, errInvalid
	}
	l := h.machine.limits
	l.commandOwnedBytes = min(l.commandOwnedBytes, 8192+16*(len(tx.manifest)+len(foreign)))
	m, err := decodeCrossManifest(tx.manifest, l, o.arena)
	if err != nil || len(m.parts) != 2 {
		return nil, errors.Join(errInvalid, err)
	}
	other, err := decodeCrossManifest(foreign, l, o.arena)
	if err != nil || len(other.parts) != 2 || other.tx == m.tx || other.digest == m.digest {
		return nil, errors.Join(errInvalid, err)
	}
	// Even validation-only kernels carry the source-created TxID. Require an
	// actual same-partition byte difference, rather than inferring it merely
	// from the foreign manifest or its home-partition business change.
	if m.parts[1].binding.Partition != other.parts[1].binding.Partition || m.parts[1].binding.TxID != m.tx || other.parts[1].binding.TxID != other.tx || bytes.Equal(m.parts[1].kernel, other.parts[1].kernel) {
		return nil, errInvalid
	}
	r := nativeDecisionFrozenReceipt{Version: 1, Cookie: o.cookie, Member: o.member, PID: uint64(os.Getpid()), GrantBytes: uint64(12*nativeDecisionBodyBytes + 8192 + 32*(len(foreign)+len(tx.manifest))), Manifest: tx.manifest, BeforeApplied: h.driver.Applied()}
	r.BeforeLastIndex, err = h.machine.store.LastIndex()
	if err != nil {
		return nil, err
	}
	read := func() (census [4]nativeDecisionFrozenControl, actual error) {
		view, e := h.machine.store.ApplicationView(h.driver.Applied())
		if e != nil {
			return census, e
		}
		defer func() { actual = errors.Join(actual, view.Close()) }()
		for i, tag := range []byte{'R', 'P', 'K', 'D'} {
			value, found, _, e := view.GetControl(o.ctx, crossControlKey(h.machine.ns, m.tx, tag), raftlog.ReadBudget{Rows: 2, Bytes: min(view.ReadLimits().Bytes, 4096+2*nativeDecisionBodyBytes)})
			if e != nil {
				return census, e
			}
			census[i] = nativeDecisionFrozenControl{Tag: tag, Found: found, Index: value.Index, Value: value.Value}
		}
		return census, nil
	}
	if r.Before, err = read(); err != nil {
		return nil, err
	}
	if !r.Before[0].Found || !r.Before[1].Found || !r.Before[2].Found || r.Before[3].Found || !bytes.Equal(r.Before[0].Value, tx.manifest) {
		return nil, errInvalid
	}
	short := m
	short.parts = short.parts[:1]
	shortWire, err := encodeCrossManifest(short, l)
	if err != nil {
		return nil, err
	}
	mixed := m
	mixed.parts = make([]crossPart, len(m.parts))
	copy(mixed.parts, m.parts)
	mixed.parts[1] = other.parts[1] // A real foreign kernel; checksums remain genuine.
	mixedWire, err := encodeCrossManifest(mixed, l)
	if err != nil {
		return nil, err
	}
	for i, kind := range []string{"missing-participant", "foreign-positive-ownership", "foreign-real-kernel"} {
		if err := o.ctx.Err(); err != nil {
			return nil, err
		}
		manifest := tx.manifest
		if i == 0 {
			manifest = shortWire
		}
		if i == 2 {
			manifest = mixedWire
		}
		wire, e := encodeCrossCommand(crossCommand{ns: h.machine.ns, phase: crossRegister, manifest: manifest}, l)
		if e != nil {
			return nil, e
		}
		proposal := allocationProposal{target: h.machine.scope(), wire: wire}
		if i == 1 {
			if proposal.target.ownership == ^uint64(0) {
				return nil, errInvalid
			}
			proposal.target.ownership++
		}
		target, e := encodeAllocationScope(proposal.target)
		if e != nil {
			return nil, e
		}
		event, e := h.Submit(proposal)
		want := errInvalid
		if i == 2 {
			want = errCorrupt
		}
		if !errors.Is(e, want) || graphErrorMask(e) != graphErrorMask(want) || len(event.packets)+len(event.snapshotSends)+len(event.replies)+len(event.crossReplies)+len(event.capacityReplies)+len(event.cutReplies)+len(event.requestReplies)+len(event.moveReplies) != 0 {
			return nil, errors.Join(errInvalid, e)
		}
		r.Attempts[i] = nativeDecisionFrozenAttempt{kind, target, wire, graphErrorMask(e)}
	}
	r.AfterApplied = h.driver.Applied()
	r.AfterLastIndex, err = h.machine.store.LastIndex()
	if err != nil {
		return nil, err
	}
	if r.After, err = read(); err != nil {
		return nil, err
	}
	if r.BeforeApplied != r.AfterApplied || r.BeforeLastIndex != r.AfterLastIndex || !reflect.DeepEqual(r.Before, r.After) {
		return nil, errCorrupt
	}
	body, err := json.Marshal(r)
	if err != nil || len(body) > nativeDecisionFrameBytes {
		return nil, errors.Join(errLimit, err)
	}
	return body, o.ctx.Err()
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

// Keep the original Run observable while its genuine candidate is awaited.
// Cancellation retires the unused native control correlation through the
// existing exchange cleanup; a pending/error Run is never a candidate.
func nativeDecisionAwaitCandidate(ctx context.Context, p *graphProcess, request nativeDecisionControl, caller <-chan graphProcessAnswer) (processResponse, *graphProcessAnswer, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return processResponse{}, nil, err
	}
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan graphProcessAnswer, 1)
	go func() {
		response, err := p.exchange(waitCtx, processRequest{op: processNativeDecisionControl, data: body})
		done <- graphProcessAnswer{response: response, err: err}
	}()
	select {
	case answer := <-done:
		return answer.response, nil, errors.Join(answer.err, processResponseError(answer.response), ctx.Err())
	case original := <-caller:
		cancel()
		// exchange keeps its pending slot until the child ACKs cancellation or
		// its original bounded cleanup closes the failed transport. Await that
		// real retirement before returning the original caller's separate result.
		retired := <-done
		return retired.response, &original, errors.Join(retired.err, processResponseError(retired.response), ctx.Err())
	}
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
			nativeDecisionCase(t, order)
		})
	}
}

// The caller serializes this inert diagnostic capture; it grants no native authority.
func nativeDecisionCaptureCapacity(campaign bool, capacity *pairedFaultBoundary, charge *nativeProtocolHoldCharge, request, reply []byte) error {
	if campaign && len(request) >= 5 && len(request) <= processFrameBytes && bytes.Equal(request[:4], []byte("GSQ\x01")) && request[4] >= sourceOpen && request[4] <= sourceReplacementRead {
		if request[4] != sourceCrossRead {
			return nil // Ordinary source traffic keeps its real response and global grants.
		}
		// Borrow only the fixed service envelope fields to classify the command.
		// No seal/nonce/proof authority comes from this diagnostic prefilter; the
		// actual service already checks the full request. Relevant or malformed
		// cross-read candidates still pay for the complete decoder below.
		c := graphCursor{b: request[5:]}
		c.take(8 + 16 + 40 + 8) // sequence, nonce, cookie, maximum and rows
		for range 3 {
			c.field(64 << 10) // key, upper, after (bounded borrowed slices)
		}
		command := c.field(1 << 20)
		c.take(16) // transaction
		phase := c.tag()
		c.take(32) // native envelope seal, validated only by the full decoder
		if c.err == nil && len(c.b) == 0 && phase <= byte(crossResolve) && !bytes.HasPrefix(command, []byte("GCQ\x01")) {
			return nil
		}
	}
	if campaign {
		if charge.Candidates >= 128 || len(request) > processFrameBytes || len(reply) > processFrameBytes {
			return errLimit
		}
		charge.Candidates++
		charge.Work += uint64(4096 + 32*(len(request)+len(reply)))
	}
	q, err := decodeGraphSourceRequest(request)
	if err != nil || q.kind != sourceCrossRead || !bytes.HasPrefix(q.command, []byte("GCQ\x01")) {
		return err
	}
	if len(capacity.Request) != 0 {
		return errInvalid
	}
	if campaign {
		if len(request)+len(reply) > nativeProtocolReplyBytes {
			return errLimit
		}
		charge.Output = uint64(8192 + 8*(len(request)+len(reply)))
		charge.Retained = uint64(len(request) + len(reply))
		capacity.Request, capacity.Reply = make([]byte, len(request)), make([]byte, len(reply))
		copy(capacity.Request, request)
		copy(capacity.Reply, reply)
	} else {
		*capacity = pairedFaultBoundary{Request: bytes.Clone(request), Reply: bytes.Clone(reply)}
	}
	return nil
}

func nativeDecisionCase(t *testing.T, order string, campaign ...nativeProtocolFrozenRecipe) {
	nativeDecisionCaseOwned(t, order, nil, campaign...)
}

func nativeDecisionCaseOwned(t *testing.T, order string, programSpec *nativeProtocolProgramSpec, campaign ...nativeProtocolFrozenRecipe) {
	if len(campaign) > 1 || len(campaign) == 1 && (!campaign[0].valid() || campaign[0].Order != order) {
		t.Fatal("invalid actual P04 queued/native occurrence")
	}
	t.Setenv("RHO_GRAPH_DECISION_FAULT_PROCESS", "1")
	var f *pairedGraphFaultNetwork
	ctx := t.Context()
	var run [16]byte
	var deadline int64
	var budget *graphstate.OutputBudget
	if len(campaign) == 0 {
		f = newPairedGraphFaultNetwork(t)
	} else {
		budget = nativeProtocolArtifactAllowance(t)
		run, deadline = nativeP06ConjunctionIdentity(t)
		t.Setenv("RHO_GRAPH_STABLE_STORAGE_PROCESS", "1")
		t.Setenv("RHO_GRAPH_PROTOCOL_CAMPAIGN", nativeProtocolCampaignProfile)
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.Unix(0, deadline))
		defer cancel()
		f = newPairedGraphFaultNetworkWithParent(t, ctx)
	}
	var program *nativeProtocolProgram
	aSource, bOwner := uint64(1), uint64(4)
	if programSpec != nil {
		if len(campaign) != 1 {
			t.Fatal("one actual frozen-original recipe owns the program")
		}
		program = nativeProtocolProgramStart(t, ctx, f, *programSpec)
		aSource, bOwner = f.n.leaders[0], f.n.leaders[1]
	}
	selector := func() nativeConjunctionSelector {
		selected := campaign[0].selector()
		if program != nil {
			selected = nativeProtocolProgramFrozenSelector(campaign[0], program.spec)
		}
		return selected
	}
	call := func(p *graphProcess, r processRequest) processResponse { return p.call(t, r) }
	control := func(p *graphProcess, r nativeDecisionControl) (nativeDecisionReceipt, processResponse) {
		return nativeDecisionCall(t, p, r)
	}
	retire := func(ch <-chan graphProcessAnswer, cancel context.CancelFunc, tx [16]byte) []byte {
		return nativeDecisionRetire(t, ch, cancel, tx)
	}
	if len(campaign) == 1 {
		call = f.call
		control = func(p *graphProcess, r nativeDecisionControl) (nativeDecisionReceipt, processResponse) {
			completed := false
			defer func() {
				if !completed {
					actor := uint64(0)
					for member := uint64(1); member <= 6; member++ {
						if f.n.all[member] == p {
							actor = member
							break
						}
					}
					// f.call already prints the actual typed transport/native
					// cause. Add only the bounded parent control coordinate.
					t.Logf("P04 native decision control interrupted: actor%d pid%d action%q mask%d sequence%d", actor, p.cmd.Process.Pid, r.Action[:min(len(r.Action), 64)], r.Mask, r.Sequence)
				}
			}()
			receipt, response := nativeProtocolDecisionCall(f, p, r)
			completed = true
			return receipt, response
		}
		retire = func(ch <-chan graphProcessAnswer, cancel context.CancelFunc, tx [16]byte) []byte {
			return nativeProtocolDecisionRetire(f, ch, cancel, tx)
		}
	}
	// Two fixed metadata summaries use the already prepaid campaign artifact
	// allowance. No request/reply payload is retained or replayed as authority.
	type sourceSummary struct {
		Seen, RequestDecoded, ReplyDecoded bool
		Kind, ReplyKind                    byte
		Sequence, ReplySequence            uint64
		RequestNonce, ReplyNonce           [16]byte
		Cookie                             graphSourceCookie
		Peer                               graphPeerIdentity
		Issuer, FirstIssuer                [16]byte
		IssuerChanged                      bool
		Mask                               uint16
		Command                            [4]byte
		RequestBytes, ReplyBytes           int
	}
	var sourceMu sync.Mutex
	var sourceSummaries [2]sourceSummary
	trackingSources := false
	observe := func(stage string, actor uint64, mode graphReadMode, cut graphCut) p06Observation {
		sourceMu.Lock()
		trackingSources = len(campaign) == 1
		sourceSummaries = [2]sourceSummary{}
		sourceMu.Unlock()
		completed := false
		defer func() {
			sourceMu.Lock()
			trackingSources = false
			summaries := sourceSummaries
			sourceMu.Unlock()
			if !completed && len(campaign) == 1 {
				t.Logf("P04 certified observation interrupted: stage%q actor%d pid%d mode%d certificates%d readStarts%d", stage, actor, f.n.all[actor].cmd.Process.Pid, mode, len(cut.certificates), f.reads)
				for part, summary := range summaries {
					t.Logf("P04 actual observation source summary: partition%d metadata%+v", []uint64{3, 8}[part], summary)
				}
			}
		}()
		result := p06Observe(f, actor, mode, cut)
		completed = true
		return result
	}
	nativeFaultSeed(t, f)
	old := observe("initial-old", 6, graphReadFresh, graphCut{})
	cut, err := pairedFaultCut(old.Cut)
	if err != nil {
		t.Fatal(err)
	}
	factoryMember, err := nativeDecisionFactoryMember(aSource)
	if err != nil {
		t.Fatal("original decision factory owner", err)
	}
	factory := func(partition uint64) allocationProposal {
		r := call(f.n.all[factoryMember], processRequest{op: processCertifiedAction, entity: certifiedNativeDecisionBusiness, index: partition})
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
	var entered <-chan pairedFaultBoundary
	var release func()
	var hold *nativeProtocolReplyHold
	if len(campaign) == 0 {
		entered, release, err = nativeFaultBeforePhase(t.Context(), hub, 3, 3, crossPrepare)
	} else {
		if len(cut.certificates) != 2 || cut.certificates[0].scope.partition != 3 || cut.certificates[1].scope.partition != 8 {
			t.Fatal("actual full cut scopes")
		}
		hold, err = newNativeProtocolBeforeHold(ctx, f, nativeProtocolReplySelector{Actor: 3, Partition: 3, Phase: crossPrepare, Transaction: txa, Peer: graphPeerIdentity{scope: cut.certificates[0].scope, voter: aSource}})
		if err == nil {
			entered, release = hold.entered, hold.release
			defer hold.close()
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var nativeGate *nativeConjunctionBatchGate
	var native nativeProtocolBatchEvidence
	var frozen nativeDecisionFrozenReceipt
	if len(campaign) == 1 && campaign[0].Candidate == "" {
		nativeGate, err = newNativeConjunctionBatchGate(ctx, f, selector(), run, deadline)
		if err != nil {
			t.Fatal(err)
		}
		defer nativeGate.close()
	}
	var captureMu sync.Mutex
	var capacity pairedFaultBoundary
	var capacityCharge nativeProtocolHoldCharge
	hub.mu.Lock()
	hub.afterRelay = func(ctx context.Context, origin, partition uint64, request, reply []byte) error {
		if origin == bOwner && (partition == 3 || partition == 8) && len(request) <= processFrameBytes && len(reply) <= processFrameBytes {
			sourceMu.Lock()
			part := 0
			if partition == 8 {
				part = 1
			}
			if trackingSources && sourceSummaries[part].Mask == 0 && !sourceSummaries[part].IssuerChanged {
				q, requestErr := decodeGraphSourceRequest(request)
				r, replyErr := decodeGraphWireResponse(reply)
				summary := sourceSummary{Seen: true, RequestDecoded: requestErr == nil, ReplyDecoded: replyErr == nil, Kind: q.kind, ReplyKind: r.kind, Sequence: q.sequence, ReplySequence: r.sequence, RequestNonce: q.nonce, ReplyNonce: r.nonce, Cookie: q.cookie, Peer: r.peer, Issuer: r.issuer, Mask: graphErrorMask(r.err), RequestBytes: len(request), ReplyBytes: len(reply)}
				summary.FirstIssuer = sourceSummaries[part].FirstIssuer
				if summary.FirstIssuer == ([16]byte{}) && replyErr == nil {
					summary.FirstIssuer = r.issuer
				}
				summary.IssuerChanged = replyErr == nil && summary.FirstIssuer != r.issuer
				copy(summary.Command[:], q.command)
				sourceSummaries[part] = summary // First native error survives cleanup traffic.
			}
			sourceMu.Unlock()
		}
		if origin != bOwner || partition != 3 {
			return nil
		}
		captureMu.Lock()
		defer captureMu.Unlock()
		return nativeDecisionCaptureCapacity(len(campaign) == 1, &capacity, &capacityCharge, request, reply)
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
	owner, _ := control(f.n.all[bOwner], nativeDecisionControl{Version: 1, Action: "inspect"})
	control(f.n.all[bOwner], nativeDecisionControl{Version: 1, Action: "arm", Cookie: owner.Cookie, Sequence: 1, Original: b.wire})
	bCall, bCancel := f.beginRun(bOwner, b)
	defer bCancel()
	waitCandidate := func(mask uint8, pending <-chan graphProcessAnswer) (nativeDecisionReceipt, processResponse) {
		request := nativeDecisionControl{Version: 1, Action: "wait", Cookie: owner.Cookie, Sequence: 1, Mask: mask}
		if len(campaign) == 0 {
			return control(f.n.all[bOwner], request)
		}
		waitCtx, cancel := f.operationContext(10 * time.Second)
		defer cancel()
		response, original, waitErr := nativeDecisionAwaitCandidate(waitCtx, f.n.all[bOwner], request, pending)
		if original != nil {
			captureMu.Lock()
			charge := capacityCharge
			// Decode only the pair already owned and funded by the capacity
			// observer. An authentic refusal is not a capacity observation.
			if len(capacity.Request) != 0 {
				q, requestErr := decodeGraphSourceRequest(capacity.Request)
				r, replyErr := decodeGraphWireResponse(capacity.Reply)
				_, grant, grantErr := decodeCapacityQuestion(q.command, defaultMaterializerLimits())
				t.Logf("P04 captured capacity exchange: requestBytes%d replyBytes%d requestKind%d requestSequence%d phase%d tx%x requestDecode=%v grant=%+v grantDecode=%v replyPartition%d replyVoter%d replyIssuer%x replyKind%d replySequence%d replyMask%x replyMagic%x replyDecode=%v", len(capacity.Request), len(capacity.Reply), q.kind, q.sequence, q.phase, q.tx, requestErr, grant, grantErr, r.peer.scope.partition, r.peer.voter, r.issuer, r.kind, r.sequence, graphErrorMask(r.err), r.body[:min(len(r.body), 4)], replyErr)
			}
			captureMu.Unlock()
			result, decodeErr := decodeGraphProcessCoordinatorResult(original.response)
			t.Fatalf("P04 original B returned before native candidate: actor%d pid%d mask%d sequence%d incarnation%x applied%d code%d causes%x detail%q transport=%v native=%v resultDecode=%v resumeTx%x decisionPhase%d commit%t capacityCharge=%+v controlRetirement=%v", bOwner, f.n.all[bOwner].cmd.Process.Pid, mask, original.response.sequence, original.response.incarnation, original.response.applied, original.response.code, original.response.causeMask, original.response.detail[:min(len(original.response.detail), 4096)], original.err, processResponseError(original.response), decodeErr, result.resume.tx, result.decision.phase, result.decision.commit, charge, waitErr)
		}
		if waitErr != nil {
			t.Fatal("P04 actual native candidate wait", mask, waitErr)
		}
		var receipt nativeDecisionReceipt
		if err := json.Unmarshal(response.aux, &receipt); err != nil || receipt.Version != 1 || receipt.Profile != nativeDecisionProfile || receipt.Cookie != owner.Cookie || receipt.Sequence != 1 || receipt.Member != bOwner || receipt.PID != uint64(f.n.all[bOwner].cmd.Process.Pid) || receipt.Controls < 1 || receipt.Controls > nativeDecisionControls || receipt.PrepaidBytes < 1 || receipt.Mask&mask != mask {
			t.Fatal("P04 actual native candidate owner receipt", err)
		}
		return receipt, response
	}
	first, _ := waitCandidate(1, bCall)
	if first.Mask != 1 || len(first.Candidates[0]) == 0 || len(first.Candidates[1]) != 0 {
		t.Fatal("missing actual capacity candidate")
	}
	pending0 := retire(bCall, bCancel, txb)
	release()
	var aResult crossProcessResult
	if len(campaign) == 1 {
		answer := nativeP06AwaitAnswer(f, aCall)
		aResult, err = decodeGraphProcessCoordinatorResult(answer.response)
		if answer.err != nil || err != nil || aResult.decision.commit || aResult.decision.tx != txa {
			t.Fatal("actual A NO/ABORT changed", answer.err, err)
		}
	} else {
		select {
		case answer := <-aCall:
			aResult, err = decodeGraphProcessCoordinatorResult(answer.response)
			if answer.err != nil || err != nil || aResult.decision.commit || aResult.decision.tx != txa {
				t.Fatal("uninstalled grant did not produce genuine original ABORT", answer.err, err, aResult)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("A original did not settle")
		}
	}
	f.drain()
	bCall, bCancel = f.beginRun(bOwner, b)
	defer bCancel()
	both, _ := waitCandidate(3, bCall)
	if both.Mask != 3 || !bytes.Equal(first.Candidates[0], both.Candidates[0]) || !bytes.Equal(first.Manifest, both.Manifest) {
		t.Fatal("retained private candidate changed")
	}
	pending1 := retire(bCall, bCancel, txb)
	f.packetDrain()
	if len(campaign) == 1 {
		if campaign[0].Candidate != "" {
			nativeGate, err = newNativeConjunctionBatchGate(ctx, f, selector(), run, deadline)
			if err != nil {
				t.Fatal(err)
			}
			defer nativeGate.close()
			slot := 0
			if campaign[0].Candidate == "all-YES" {
				slot = 1
			}
			proposal := p06SourceProposal(t, aPrepare.Request)
			proposal.target = cut.certificates[1].scope
			proposal.wire = both.Candidates[slot] // Diagnostic selection ONLY; queue consumes original private child slots.
			if err := nativeGate.armObserved(ctx, proposal); err != nil {
				t.Fatal(err)
			}
		} else if campaign[0].Phase != crossResolve || campaign[0].Original == "A" {
			native = nativeProtocolFinishBatch(t, f, nativeGate, cut)
		}
		body, marshalErr := json.Marshal(nativeDecisionControl{Version: 1, Action: "campaign-frozen", Cookie: owner.Cookie, Sequence: 1, Original: aCommand.manifest})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		response := call(f.n.all[bOwner], processRequest{op: processNativeDecisionControl, data: body})
		if err := json.Unmarshal(response.aux, &frozen); err != nil {
			t.Fatal(err)
		}
	}
	queued, output := control(f.n.all[bOwner], nativeDecisionControl{Version: 1, Action: "queue", Cookie: owner.Cookie, Sequence: 1, Order: order})
	if queued.Accepted != ([2]bool{true, true}) || queued.Errors != ([2]uint16{}) || queued.BeforeApplied != queued.AfterApplied || queued.AfterLastIndex != queued.BeforeLastIndex+2 {
		t.Fatal("two candidates were not actually queued before application", queued)
	}
	if program != nil {
		if err := hub.enqueue(bOwner, output.packets, output.handles...); err != nil {
			t.Fatal("genuine current-leader queued entries through actual program scheduler", err)
		}
		f.packetDrain()
	} else {
		f.n.group(bOwner).enqueue(t, output)
		if len(campaign) == 0 {
			f.n.group(bOwner).drain(t)
		} else {
			f.drainGroup(f.n.group(bOwner))
		}
	}
	f.drain()
	if len(campaign) == 1 && campaign[0].Candidate != "" {
		native = nativeProtocolFinishBatch(t, f, nativeGate, cut)
	}
	replayed, replayOutput := control(f.n.all[bOwner], nativeDecisionControl{Version: 1, Action: "queue", Cookie: owner.Cookie, Sequence: 1, Order: order})
	if !reflect.DeepEqual(replayed, queued) || len(replayOutput.packets) != 0 {
		t.Fatal("queue replay repeated native admission")
	}
	// The next ordinary Run reads the genuine first durable D and resolves
	// every actual participant. It never rebuilds either candidate.
	response := call(f.n.all[bOwner], processRequest{op: processSourceCoordinate, data: b.wire})
	result, err := decodeGraphProcessCoordinatorResult(response)
	if err != nil || result.decision.tx != txb || result.decision.commit != (order == "commit-first") {
		t.Fatal("later candidate reversed first durable decision", err, result)
	}
	f.drain()
	if len(campaign) == 1 && campaign[0].Original == "B" && campaign[0].Phase == crossResolve {
		native = nativeProtocolFinishBatch(t, f, nativeGate, cut)
	}
	if result.decision.commit {
		f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "edge-create", Round: result.decision.round})
	}
	current := observe("after-first-decision-current", 6, graphReadFresh, graphCut{})
	p06AssertView(t, current.View, result.decision.commit, false, false)
	retained := observe("after-first-decision-old", 6, graphReadAt, cut)
	if !reflect.DeepEqual(old, retained) {
		t.Fatal("decision race substituted current at old cut")
	}
	var duplicates []nativeDecisionRetry
	for i, original := range []allocationProposal{a, b} {
		actor := uint64(3)
		want := aResult.decision
		if i == 1 {
			actor = bOwner
			want = result.decision
		}
		start := f.indexes()
		response := call(f.n.all[actor], processRequest{op: processSourceCoordinate, data: original.wire})
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
	killed := f.kill(bOwner, "replica")
	f.elect(bOwner, 5)
	rejoined := f.replace(bOwner)
	reopened := observe("after-coordinator-replacement-old", bOwner, graphReadAt, cut)
	if !reflect.DeepEqual(old, reopened) {
		t.Fatal("same-directory old cut changed")
	}
	localDecision := call(f.n.all[bOwner], processRequest{op: processEntryRecord, index: result.decision.index})
	localStatus, localErr := decodeCrossStatus(localDecision.outcome)
	if localErr != nil || localStatus != result.decision || len(localDecision.changes) != 0 {
		t.Fatal("replacement local retained first-D differs", localErr, localStatus)
	}
	if len(campaign) == 0 {
		p03Reconnect(f, bOwner) // Subsequent Fresh/Run require actual leader ReadIndex.
	} else {
		nativeConjunctionReconnect(f, bOwner)
	}
	reopenedCurrent := observe("after-coordinator-replacement-current", bOwner, graphReadFresh, graphCut{})
	if !reflect.DeepEqual(current.Native, reopenedCurrent.Native) {
		t.Fatal("replacement changed first-decision participant effects")
	}
	start := f.indexes()
	response = call(f.n.all[bOwner], processRequest{op: processSourceCoordinate, data: b.wire})
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
	duplicates = append(duplicates, nativeDecisionRetry{Actor: bOwner, Original: b.wire, Response: p06ProcessWire(t, response), From: start, Through: end, Records: tail})
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
	artifact := nativeDecisionArtifact{Version: 2, Profile: nativeDecisionProfile, Case: order, From: from, Through: through, Records: records, OriginalA: a.wire, OriginalB: b.wire, ManifestA: aCommand.manifest, Capacity: actualCapacity, Queue: queued, Pending: [2][]byte{pending0, pending1}, DecisionA: decisionA, DecisionB: decisionB, Before: old, Retained: retained, Current: current, Reopened: reopened, ReopenedCurrent: reopenedCurrent, Processes: []pairedFaultProcess{killed, rejoined}, Duplicates: duplicates}
	if len(campaign) == 0 {
		nativeDecisionWrite(t, f.n.dir, artifact)
	} else {
		nativeProtocolFrozenWrite(t, ctx, f, campaign[0], run, deadline, native, frozen, artifact, capacityCharge, budget)
	}
}

func TestNativeDecisionFactoryKeepsActualRecipientOnOriginalOwnerAcrossSourceRotation(t *testing.T) {
	t.Setenv("RHO_GRAPH_LIFECYCLE_PROCESS", "1")
	n := newCertifiedGraphNetwork(t)
	recipient := n.activate(0, [16]byte{1})
	n.acquire(0, recipient, 1, 16)
	remote := remoteCertifiedFixture(t, n)
	coordinator, err := newCrossProcessCoordinator(n.hosts[0][0], remote.transports)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := newNativeDecisionOwner(t.Context(), coordinator, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer decision.close()
	owners := map[uint64]*certifiedProcessOwners{
		1: {host: n.hosts[0][0], recipient: recipient, decisions: decision},
		2: {host: n.hosts[0][1], decisions: decision},
	}
	for _, partition := range []uint64{3, 8} {
		request := processRequest{entity: certifiedNativeDecisionBusiness, index: partition}
		if proposal, err := nativeDecisionBusiness(owners[2], request); !errors.Is(err, errInvalid) || len(proposal.wire) != 0 {
			t.Fatal("serving replica acquired another process's Recipient", err)
		}
		member, err := nativeDecisionFactoryMember(2)
		if err != nil {
			t.Fatal(err)
		}
		proposal, err := nativeDecisionBusiness(owners[member], request)
		if err != nil {
			t.Fatal("rotated source selected a factory without its actual Recipient", member, err)
		}
		command, err := decodePartitionGraphCommand(proposal.wire, defaultMaterializerLimits())
		if err != nil || member != 1 || len(command.request.operations) != 2 || len(command.request.claims) != 3 || command.request.ns != n.hosts[0][0].machine.ns || command.request.revision.Provenance() != 3 {
			t.Fatal("factory changed complete original namespace/operations/claims", member, err)
		}
		for i, claim := range command.request.claims {
			sequence := uint64(1)
			if partition == 3 && i == 0 {
				sequence = 2 // The genuine deliberately uninstalled A claim stays hostile.
			}
			if claim.grant.session != recipient.session || claim.grant.sequence != sequence {
				t.Fatal("factory substituted actual original Recipient/grant", claim.grant)
			}
		}
	}
	if owners[2].recipient != nil {
		t.Fatal("factory installed Recipient authority on the source replica")
	}
	for _, source := range []uint64{0, 4} {
		if member, err := nativeDecisionFactoryMember(source); member != 0 || !errors.Is(err, errInvalid) {
			t.Fatal("foreign source selected a factory", member, err)
		}
	}
}

func TestNativeDecisionCertifiedCapacityReadKeepsDeclaredProbeGrant(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	ops = append(ops, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "p", Value: graphstate.I64(5), ValueID: 5})
	claims = append(claims, freshBinding{role: valueBinding, id: 5, grant: claims[0].grant})
	ids := crossRequestsWithLoser(n, 1)
	var originals [2]crossTransaction
	var registrations [2]crossProof
	for group := range 2 {
		actualClaims := append([]freshBinding(nil), claims...)
		if group == 0 {
			actualClaims[0].grant.sequence = 2 // SAME genuinely uninstalled A claim as P04.
		}
		tx, proposal := crossPlanAt(n, group, ids[group], ops, actualClaims, 3)
		originals[group] = tx
		status := n.crossSubmit(group, proposal)
		registrations[group] = n.crossReadPhase(group, status.tx, crossRegister)
	}
	// A is genuinely registered but its Prepare has not been submitted. Its
	// native recovery reservation, not a fabricated NO, blocks B's actual A entry.
	blocked, err := n.hosts[1][0].PrepareCrossParticipant(originals[1], 3, registrations[1], nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.hosts[0][0].Submit(blocked); !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal("actual registered A must block B Prepare", err)
	}
	pump := &capacityNativeTestPump{&nativeGraphTestPump{n: n, group: 0, host: n.hosts[0][0]}}
	grant := graphCapacityLimits{rows: 128, bytes: 128 << 10, output: 1 << 20}
	question := [16]byte{9}
	reply, err := pump.Capacity(t.Context(), blocked, question, grant)
	if err != nil || reply.err != nil {
		t.Fatal("genuine certified-capacity probe within unchanged declared grant", err, reply.err)
	}
	p := reply.proof
	if p.owner != pump.host || p.observation.desired != registrations[1].status.tx || p.observation.holder != registrations[0].status.tx || p.observation.nonce != question || p.observation.prepareHash != sha256.Sum256(blocked.wire) || p.work.Records < 1 || p.work.Records > grant.rows || p.work.Bytes < 1 || p.work.Bytes > grant.bytes || p.output < 1 || p.output > grant.output {
		t.Fatal("actual capacity proof/ownership/grant binding", p)
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

// Canonical service-codec regression only: these inert bytes confer no capacity
// or proposal authority. The separate six-voter capacity test supplies that proof.
func TestNativeDecisionCapacityObserverIgnoresOtherServiceTraffic(t *testing.T) {
	var capacity pairedFaultBoundary
	var charge nativeProtocolHoldCharge
	for i := range 129 {
		for _, kind := range []byte{sourceClose, sourceGet, sourceCertifiedOpen, sourceCutRead, sourceCrossRead} {
			request, err := encodeGraphSourceRequest(graphSourceRequest{kind: kind, sequence: uint64(i + 1), nonce: [16]byte{1}})
			if err != nil {
				t.Fatal(err)
			}
			if err := nativeDecisionCaptureCapacity(true, &capacity, &charge, request, nil); err != nil {
				t.Fatal("unrelated canonical service traffic exhausted capacity observer", i, kind, charge, err)
			}
		}
	}
	if charge != (nativeProtocolHoldCharge{}) || len(capacity.Request)+len(capacity.Reply) != 0 {
		t.Fatal("unrelated traffic spent capacity candidate grant", charge)
	}
	command, err := encodeCapacityQuestion(allocationProposal{target: wireGuardScope(), wire: []byte("inert-original")}, graphCapacityLimits{128, 128 << 10, 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	request, err := encodeGraphSourceRequest(graphSourceRequest{kind: sourceCrossRead, sequence: 130, nonce: [16]byte{1}, command: command})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := encodeGraphWireResponse(graphWireResponse{peer: graphPeerIdentity{scope: wireGuardScope(), voter: 1}, issuer: [16]byte{2}, kind: sourceCrossRead, sequence: 130, nonce: [16]byte{1}, err: replica.ErrUnavailable})
	if err != nil {
		t.Fatal(err)
	}
	if err := nativeDecisionCaptureCapacity(true, &capacity, &charge, request, reply); err != nil || !bytes.Equal(capacity.Request, request) || !bytes.Equal(capacity.Reply, reply) || charge.Candidates != 1 || charge.Work != uint64(4096+32*(len(request)+len(reply))) || charge.Output != uint64(8192+8*(len(request)+len(reply))) || charge.Retained != uint64(len(request)+len(reply)) {
		t.Fatal("actual GCQ codec candidate lost exact prepaid capture", charge, err)
	}
	if err := nativeDecisionCaptureCapacity(true, &capacity, &charge, request, reply); !errors.Is(err, errInvalid) || charge.Candidates != 2 || !bytes.Equal(capacity.Request, request) || !bytes.Equal(capacity.Reply, reply) {
		t.Fatal("second candidate overwrote immutable capture", charge, err)
	}
	for _, malformed := range [][]byte{nil, []byte("GSQ\x01\x06"), bytes.Clone(request)} {
		if len(malformed) == len(request) {
			malformed[len(malformed)-1] ^= 1
		}
		var rejected pairedFaultBoundary
		var consumed nativeProtocolHoldCharge
		if err := nativeDecisionCaptureCapacity(true, &rejected, &consumed, malformed, reply); err == nil || consumed.Candidates != 1 || consumed.Work != uint64(4096+32*(len(malformed)+len(reply))) || len(rejected.Request)+len(rejected.Reply) != 0 {
			t.Fatal("malformed relevant candidate escaped full decode/refusal grant", consumed, err)
		}
	}
	limited := nativeProtocolHoldCharge{Candidates: 128}
	var rejected pairedFaultBoundary
	if err := nativeDecisionCaptureCapacity(true, &rejected, &limited, request, reply); !errors.Is(err, errLimit) || limited != (nativeProtocolHoldCharge{Candidates: 128}) || len(rejected.Request)+len(rejected.Reply) != 0 {
		t.Fatal("actual capacity candidate cap changed", limited, err)
	}
}
