package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativeLogicalTransitionProfile = "native-live-submission-transition-v1"
const nativeLogicalTransitionBodyBytes = 64 << 10
const nativeLogicalTransitionFrameBytes = 512 << 10

type nativeLogicalTransitionControl struct {
	Version   uint8
	Action    string
	Cookie    [16]byte
	Sequence  uint64
	Family    string
	Phase     byte
	Side      string
	Identity  [16]byte
	Partition uint64
	Page      uint64
}

type nativeLogicalTransitionReceipt struct {
	Version                      uint8
	Profile                      string
	Cookie                       [16]byte
	Run                          [16]byte
	Member, PID, Sequence        uint64
	Control                      nativeLogicalTransitionControl
	State                        string
	DeadlineUnixNanos            int64
	Proposal                     []byte
	EntryData                    []byte
	Target                       []byte
	ProposalSHA256               [32]byte
	Index, Term                  uint64
	Root                         raftlog.ApplicationRoot
	Outcome, CDC                 []byte
	AttemptedWork, PrepaidOutput uint64
}

type nativeLogicalTransitionTicket struct {
	owner                  *nativeLogicalTransitionOwner
	ctx                    context.Context
	cancel                 context.CancelFunc
	stop                   func() bool
	ready, released        chan struct{}
	readyOnce, releaseOnce sync.Once
	proofBaseline          uint64
	proofScanned           int
	receipt                nativeLogicalTransitionReceipt // All access under owner.mu.
}

// There is at most one retained live call. Its actual proposal stays on the
// originating service goroutine's stack through hold; diagnostics contain an
// owned observation only. No control accepts or reconstructs proposal authority.
// Each immutable owner admits16 arms,64 controls,128 classification attempts per
// arm and1024 actual log entries per after-result capture. None changes ordinary
// source, Run, Raft, frame or queue limits.
type nativeLogicalTransitionOwner struct {
	mu                                       sync.Mutex
	ctx                                      context.Context
	cancel                                   context.CancelFunc
	host                                     *allocationHost
	member                                   uint64
	cookie                                   [16]byte
	run                                      [16]byte
	deadline                                 time.Time
	last                                     nativeLogicalTransitionControl
	armed                                    bool
	unavailable                              bool
	closed                                   bool
	sequence, controls, attempts             uint64
	work, maxWork, decodeGrant, captureGrant uint64
	prepareGrant                             uint64
	pinGrant                                 uint64
	ticket                                   *nativeLogicalTransitionTicket
	changed                                  chan struct{}
	awaiting                                 bool
	preparedMove                             *nativeTransitionPreparedMove
	pin                                      *nativeTransitionPinOwner
}

func newNativeLogicalTransitionOwner(ctx context.Context, h *allocationHost, member uint64) (*nativeLogicalTransitionOwner, error) {
	if ctx == nil || h == nil || member < 1 || member > 6 {
		return nil, errInvalid
	}
	p, limits := h.machine.store.ApplicationLimits(), h.machine.store.Limits()
	if p.MaxImageBytes < 172 || p.MaxImageBytes > 64<<20 || p.MaxChangeBytes < 1 || p.MaxChangeBytes > 64<<20 || p.MaxOutcomeBytes < 1 || p.MaxOutcomeBytes > 64<<20 || limits.Validate() != nil {
		return nil, errInvalid
	}
	// Entry reads authenticate their predecessor and may own the first complete
	// entry even when maxSize is smaller. Fund the actual immutable store ceiling,
	// not only the selected64KiB command. The conservative pool is accounting,
	// never an allocation or a claim of measured native work/RSS.
	decodeGrant := uint64(4096 + 16*nativeLogicalTransitionBodyBytes)
	captureGrant := 1026*uint64(65536+16*limits.MaxEntryBytes) + uint64(16384+8*p.MaxImageBytes+6*p.MaxChangeBytes+6*p.MaxOutcomeBytes)
	prepareGrant := uint64(0)
	if h.machine.rangeMoves() {
		l := h.machine.limits
		prepareGrant = 16384 + 8*uint64(l.allocation.readBytes) + 4*uint64(l.outputBytes) + 256*uint64(l.allocation.readRows)
	}
	maximum := 16*(128*decodeGrant+captureGrant) + prepareGrant
	if prepareGrant != 0 {
		maximum += 64 * 8192
	}
	pinGrant := uint64(0)
	if h.machine.SemanticContractID() == durableColumnGraphSemanticContractID() {
		l := h.machine.limits
		if l.sourceBytes < 1 || l.outputBytes < 1 || l.sourceRows < 1 || uint64(l.sourceBytes) > 1<<40 || uint64(l.outputBytes) > 1<<40 || uint64(l.sourceRows) > 1<<40 {
			return nil, errLimit
		}
		pinGrant = 65536 + 8*uint64(l.sourceBytes) + 8*uint64(l.outputBytes) + 512*uint64(l.sourceRows)
		maximum += 16*pinGrant + 64*nativeLogicalTransitionFrameBytes
	}
	if maximum == 0 || maximum > 1<<50 {
		return nil, errLimit
	}
	base, stopBase := ctx, func() {}
	if raw := os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE"); raw != "" {
		deadline, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || strconv.FormatInt(deadline, 10) != raw || deadline <= time.Now().UnixNano() || deadline-time.Now().UnixNano() > int64(1800*time.Second) {
			return nil, errors.Join(errInvalid, err)
		}
		base, stopBase = context.WithDeadline(ctx, time.Unix(0, deadline))
	}
	ownerCtx, stopTTL := context.WithTimeout(base, time.Minute)
	cancel := func() { stopTTL(); stopBase() }
	o := &nativeLogicalTransitionOwner{ctx: ownerCtx, cancel: cancel, host: h, member: member, decodeGrant: decodeGrant, captureGrant: captureGrant, prepareGrant: prepareGrant, pinGrant: pinGrant, maxWork: maximum, changed: make(chan struct{}, 1)}
	o.deadline, _ = ownerCtx.Deadline()
	if _, err := io.ReadFull(rand.Reader, o.cookie[:]); err != nil {
		cancel()
		return nil, err
	}
	if raw := os.Getenv("RHO_GRAPH_TRANSITION_RUN"); raw != "" {
		wire, err := hex.DecodeString(raw)
		if err != nil || len(wire) != 16 || hex.EncodeToString(wire) != raw {
			cancel()
			return nil, errors.Join(errInvalid, err)
		}
		copy(o.run[:], wire)
	} else if _, err := io.ReadFull(rand.Reader, o.run[:]); err != nil {
		cancel()
		return nil, err
	}
	if o.run == ([16]byte{}) {
		cancel()
		return nil, errInvalid
	}
	context.AfterFunc(ownerCtx, o.close)
	return o, nil
}

func decodeNativeLogicalTransitionControl(wire []byte) (nativeLogicalTransitionControl, error) {
	var c nativeLogicalTransitionControl
	if len(wire) == 0 || len(wire) > 4096 {
		return c, errInvalid
	}
	d := json.NewDecoder(bytes.NewReader(wire))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, errors.Join(errInvalid, err)
	}
	canonical, err := json.Marshal(c)
	if err != nil || !bytes.Equal(canonical, wire) || c.Version != 1 {
		return c, errors.Join(errInvalid, err)
	}
	return c, nil
}

func nativeLogicalTransitionSelector(c nativeLogicalTransitionControl) bool {
	if c.Partition != 3 && c.Partition != 8 || c.Side != "before-submit" && c.Side != "after-applied" {
		return false
	}
	switch c.Family {
	case "cross":
		return c.Phase >= 1 && c.Phase <= 4 && c.Identity != ([16]byte{}) && c.Page == 0
	case "cut":
		return c.Phase >= 1 && c.Phase <= 2 && c.Identity == ([16]byte{}) && c.Page == 0
	case "move":
		return c.Phase >= 1 && c.Phase <= 9 && c.Identity != ([16]byte{}) && c.Page <= 1024
	case "pin":
		return c.Phase >= 1 && c.Phase <= 8 && c.Identity != ([16]byte{}) && c.Page == 0
	default:
		return false
	}
}

func nativeLogicalTransitionIdentity(q allocationProposal, l materializerLimits) (family string, phase byte, identity [16]byte, page uint64, err error) {
	switch {
	case isCrossPartitionWire(q.wire):
		var c crossCommand
		c, _, err = decodeCrossCommand(q.wire, l)
		if err != nil {
			return
		}
		if c.ns.graph != q.target.graph || c.ns.partition != q.target.partition {
			err = errInvalid
			return
		}
		manifest := c.manifest
		if len(manifest) >= 4 && manifest[3] == 2 {
			var body []byte
			body, err = graphBody(manifest, "GCM\x02", nativeLogicalTransitionBodyBytes)
			if err != nil {
				return
			}
			d := graphCursor{b: body}
			manifest = d.field(nativeLogicalTransitionBodyBytes)
			_ = d.field(nativeLogicalTransitionBodyBytes)
			if d.err != nil || len(d.b) != 0 {
				err = errors.Join(errCorrupt, d.err)
				return
			}
		}
		var body []byte
		body, err = graphBody(manifest, "GCM\x01", nativeLogicalTransitionBodyBytes)
		if err != nil || len(body) < 16 {
			err = errors.Join(errCorrupt, err)
			return
		}
		copy(identity[:], body[:16])
		family, phase = "cross", byte(c.phase)
	case isGraphCutWire(q.wire):
		var c graphCutCommand
		c, err = decodeGraphCutCommand(q.wire)
		if err == nil && c.scope != q.target {
			err = errInvalid
		}
		family, phase = "cut", c.phase
	case isRangeMoveWire(q.wire):
		var c rangeMoveCommand
		c, err = decodeRangeMoveCommand(q.wire)
		if err != nil {
			return
		}
		if c.scope != q.target {
			err = errInvalid
			return
		}
		var intent rangeMoveIntent
		intent, err = decodeRangeMoveIntent(c.intent)
		family, phase, page = "move", byte(c.phase), c.sequence
		identity = rangeMoveID(namespace{intent.source.graph, intent.source.partition}, intent.id)
	case isGraphPinWire(q.wire):
		var c graphPinCommand
		c, err = decodeGraphPinCommand(q.wire)
		if err == nil && c.scope != q.target {
			err = errInvalid
		}
		family, phase, identity = "pin", c.phase, c.action
	default:
		err = errInvalid
	}
	return
}

func (o *nativeLogicalTransitionOwner) reserveLocked(work uint64) error {
	if o.closed || o.ctx.Err() != nil {
		return errors.Join(replica.ErrUnavailable, o.ctx.Err())
	}
	if o.work > o.maxWork || work > o.maxWork-o.work {
		return errLimit
	}
	o.work += work // An attempted/uncertain read or decode is never refunded.
	return nil
}

func (o *nativeLogicalTransitionOwner) begin(ctx context.Context, q allocationProposal) (*nativeLogicalTransitionTicket, error) {
	if o == nil {
		return nil, nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.armed {
		return nil, nil
	}
	// Diagnostic selection must not introduce failures in other native command
	// families, partitions or phases. Only potentially matching bytes are decoded.
	if !nativeLogicalTransitionPossible(o.last, q) {
		return nil, nil
	}
	if ctx == nil {
		return nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.attempts >= 128 || len(q.wire) == 0 || len(q.wire) > nativeLogicalTransitionBodyBytes {
		o.refuseLocked()
		return nil, nil
	}
	if err := o.reserveLocked(o.decodeGrant); err != nil {
		o.refuseLocked()
		return nil, nil
	}
	o.attempts++
	family, phase, identity, page, err := nativeLogicalTransitionIdentity(q, o.host.machine.limits)
	if err != nil {
		return nil, err
	}
	c := o.last
	if family != c.Family || phase != c.Phase || identity != c.Identity || q.target.partition != c.Partition || page != c.Page {
		return nil, nil
	}
	scope, scopeErr := o.host.target(q.target.partition)
	if o.ticket != nil || scopeErr != nil || q.target != scope {
		return nil, errInvalid
	}
	if err := o.reserveLocked(o.captureGrant); err != nil {
		o.refuseLocked()
		return nil, nil
	}
	callCtx, cancel := context.WithDeadline(ctx, o.deadline)
	t := &nativeLogicalTransitionTicket{owner: o, ctx: callCtx, cancel: cancel, ready: make(chan struct{}), released: make(chan struct{})}
	t.stop = context.AfterFunc(o.ctx, cancel)
	proposal := make([]byte, len(q.wire))
	copy(proposal, q.wire)
	deadline, _ := callCtx.Deadline()
	t.receipt = nativeLogicalTransitionReceipt{Version: 1, Profile: nativeLogicalTransitionProfile, Cookie: o.cookie, Run: o.run, Member: o.member, PID: uint64(os.Getpid()), Sequence: c.Sequence, Control: c, State: "waiting", DeadlineUnixNanos: deadline.UnixNano(), Proposal: proposal, Target: appendAllocationScope(nil, q.target), ProposalSHA256: sha256.Sum256(q.wire), AttemptedWork: o.work, PrepaidOutput: nativeLogicalTransitionFrameBytes}
	o.ticket, o.armed = t, false
	select {
	case o.changed <- struct{}{}:
	default:
	}
	return t, nil
}

// Header checks select candidates only; the actual codec and exact immutable
// scope/identity checks above remain necessary before retaining a live call.
func nativeLogicalTransitionPossible(c nativeLogicalTransitionControl, q allocationProposal) bool {
	if q.target.partition != c.Partition {
		return false
	}
	var family string
	var offset int
	switch {
	case isCrossPartitionWire(q.wire):
		family, offset = "cross", 4
	case isGraphCutWire(q.wire):
		family, offset = "cut", 4
	case isRangeMoveWire(q.wire):
		family, offset = "move", 124
	case isGraphPinWire(q.wire):
		family, offset = "pin", 124
	default:
		return false
	}
	return family == c.Family && (len(q.wire) <= offset || q.wire[offset] == c.Phase)
}

func (o *nativeLogicalTransitionOwner) refuseLocked() {
	o.armed, o.unavailable = false, true
	select {
	case o.changed <- struct{}{}:
	default:
	}
}

func (t *nativeLogicalTransitionTicket) hold() error {
	if t == nil {
		return nil
	}
	o := t.owner
	o.mu.Lock()
	if t.receipt.Control.Side == "before-submit" {
		t.receipt.State = "reached"
		t.readyOnce.Do(func() { close(t.ready) })
	}
	o.mu.Unlock()
	select {
	case <-t.ctx.Done():
		return errors.Join(replica.ErrUnavailable, t.ctx.Err())
	case <-t.released:
		return t.ctx.Err()
	}
}

func (t *nativeLogicalTransitionTicket) finish(actual error) {
	if t == nil {
		return
	}
	t.stop()
	t.cancel()
	o := t.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if actual != nil {
		t.receipt.State = "canceled"
	} else {
		t.receipt.State = "released"
	}
	t.readyOnce.Do(func() { close(t.ready) })
}

// Called by the existing serialized native event loop, never by a control
// consumer and never while holding owner.mu. A complete matching applied Entry
// is required; a high-water or a status from another request is insufficient.
func (o *nativeLogicalTransitionOwner) poll(h *allocationHost, c *graphProcessPumpCall) (bool, error) {
	t := c.transition
	if t == nil || t.receipt.Control.Side != "after-applied" {
		return true, nil
	}
	o.mu.Lock()
	observed := t.receipt.Index != 0
	o.mu.Unlock()
	if observed {
		return true, nil
	}
	next, scanned, body, operation, fatal := pollGraphProcessSubmission(t.ctx, h, c.proposal, t.proofBaseline, t.proofScanned, nativeLogicalTransitionBodyBytes, func([]byte, uint64) error { return nil })
	t.proofBaseline, t.proofScanned = next, scanned
	if err := errors.Join(operation, fatal); err != nil {
		return false, err
	}
	if body == nil {
		return false, nil
	}
	view, err := h.machine.store.ApplicationView(next)
	if err != nil {
		return false, err
	}
	root, err := view.RootAt(t.ctx, next, 172)
	var changes []byte
	var outcome []byte
	if err == nil {
		outcome, err = view.ApplicationRecord(t.ctx, next, true, 512)
	}
	if err == nil {
		changes, err = view.ApplicationRecord(t.ctx, next, false, nativeLogicalTransitionBodyBytes)
	}
	if err == nil && !bytes.Equal(outcome, body) {
		err = errCorrupt
	}
	err = errors.Join(err, view.Close(), t.ctx.Err())
	if err != nil {
		return false, err
	}
	entries, err := h.machine.store.Entries(next, next+1, nativeLogicalTransitionBodyBytes)
	if err != nil || len(entries) != 1 || entries[0].GetIndex() != next || !bytes.Equal(entries[0].GetData(), c.proposal.wire) {
		return false, errors.Join(errCorrupt, err)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || t.ctx.Err() != nil {
		return false, errors.Join(replica.ErrUnavailable, t.ctx.Err())
	}
	t.receipt.EntryData = make([]byte, len(entries[0].GetData()))
	copy(t.receipt.EntryData, entries[0].GetData())
	t.receipt.Index, t.receipt.Term, t.receipt.Root, t.receipt.Outcome, t.receipt.CDC, t.receipt.State = next, entries[0].GetTerm(), root, outcome, changes, "reached"
	t.readyOnce.Do(func() { close(t.ready) })
	return true, nil
}

func (o *nativeLogicalTransitionOwner) wireLocked() ([]byte, error) {
	r := nativeLogicalTransitionReceipt{Version: 1, Profile: nativeLogicalTransitionProfile, Cookie: o.cookie, Run: o.run, Member: o.member, PID: uint64(os.Getpid()), Sequence: o.sequence, State: "idle", DeadlineUnixNanos: o.deadline.UnixNano(), AttemptedWork: o.work}
	if o.ticket != nil {
		r = o.ticket.receipt
	}
	if o.armed {
		r.State, r.Control = "armed", o.last
	}
	if o.closed {
		r.State = "closed"
	} else if o.unavailable {
		r.State = "unavailable"
	}
	wire, err := json.Marshal(r)
	if err != nil || len(wire) > nativeLogicalTransitionFrameBytes {
		return nil, errors.Join(errLimit, err)
	}
	return wire, nil
}

func (o *nativeLogicalTransitionOwner) serve(ctx context.Context, wire []byte) ([]byte, error) {
	if o == nil || ctx == nil {
		return nil, errInvalid
	}
	c, err := decodeNativeLogicalTransitionControl(wire)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	if o.controls >= 64 {
		o.mu.Unlock()
		return nil, errLimit
	}
	o.controls++
	if c.Action == "inspect" && c.Cookie == ([16]byte{}) && c == (nativeLogicalTransitionControl{Version: 1, Action: "inspect"}) {
		body, err := o.wireLocked()
		o.mu.Unlock()
		return body, err
	}
	if c.Cookie != o.cookie || c.Cookie == ([16]byte{}) {
		o.mu.Unlock()
		return nil, replica.ErrUnavailable
	}
	if o.closed && c == (nativeLogicalTransitionControl{Version: 1, Action: "close", Cookie: o.cookie, Sequence: o.sequence}) {
		body, err := o.wireLocked()
		o.mu.Unlock()
		return body, err
	}
	if o.closed || o.ctx.Err() != nil {
		o.mu.Unlock()
		return nil, replica.ErrUnavailable
	}
	switch c.Action {
	case "arm":
		if c == o.last {
			body, err := o.wireLocked()
			o.mu.Unlock()
			return body, err
		}
		if !nativeLogicalTransitionSelector(c) || o.armed || o.sequence >= 16 || c.Sequence != o.sequence+1 || o.ticket != nil && o.ticket.receipt.State != "released" && o.ticket.receipt.State != "canceled" {
			o.mu.Unlock()
			return nil, errInvalid
		}
		o.last, o.sequence, o.armed, o.attempts, o.ticket = c, c.Sequence, true, 0, nil
		o.unavailable = false
	case "await", "release", "close":
		base := nativeLogicalTransitionControl{Version: 1, Action: c.Action, Cookie: o.cookie, Sequence: o.sequence}
		if c != base {
			o.mu.Unlock()
			return nil, errInvalid
		}
		if c.Action == "close" {
			o.mu.Unlock()
			o.close()
			o.mu.Lock()
			body, err := o.wireLocked()
			o.mu.Unlock()
			return body, err
		}
		if c.Action == "await" {
			if o.awaiting {
				o.mu.Unlock()
				return nil, errLimit
			}
			o.awaiting = true
			o.mu.Unlock()
			defer func() { o.mu.Lock(); o.awaiting = false; o.mu.Unlock() }()
			for {
				o.mu.Lock()
				if o.closed || o.unavailable || o.sequence != c.Sequence {
					o.mu.Unlock()
					return nil, replica.ErrUnavailable
				}
				t := o.ticket
				if t != nil {
					if t.receipt.State == "reached" {
						body, err := o.wireLocked()
						o.mu.Unlock()
						return body, err
					}
					if t.receipt.State != "waiting" {
						o.mu.Unlock()
						return nil, replica.ErrUnavailable
					}
					o.mu.Unlock()
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-o.ctx.Done():
						return nil, replica.ErrUnavailable
					case <-t.ready:
					}
				} else {
					if !o.armed {
						o.mu.Unlock()
						return nil, replica.ErrUnavailable
					}
					o.mu.Unlock()
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-o.ctx.Done():
						return nil, replica.ErrUnavailable
					case <-o.changed:
					}
				}
			}
		} else {
			t := o.ticket
			if t == nil || t.receipt.State != "reached" && t.receipt.State != "released" {
				o.mu.Unlock()
				return nil, replica.ErrUnavailable
			}
			t.releaseOnce.Do(func() { close(t.released) })
		}
	default:
		o.mu.Unlock()
		return nil, errInvalid
	}
	body, err := o.wireLocked()
	o.mu.Unlock()
	return body, err
}

func (o *nativeLogicalTransitionOwner) close() {
	if o == nil {
		return
	}
	o.cancel()
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	o.closed, o.armed = true, false
	if o.ticket != nil {
		o.ticket.cancel()
		o.ticket.receipt.State = "canceled"
		o.ticket.readyOnce.Do(func() { close(o.ticket.ready) })
	}
}

type nativeTransitionAction struct {
	Member uint64
	Ticks  int
}

// Recipes describe actions, never native proof bodies or authority. A fresh
// trial must obtain its own actual factory original, then keep that one exact
// original through uncertainty/restart. Actions may be deleted for supported
// 1-minimal counterexamples; the mandatory gate/recovery recipe stays fixed.
type nativeTransitionRecipe struct {
	Version   uint8
	Seed      uint64
	Phase     byte
	Partition uint64
	Side      string
	Actions   []nativeTransitionAction
}

func (r nativeTransitionRecipe) valid() bool {
	if r.Version != 1 || r.Seed == 0 || r.Phase < 1 || r.Phase > 4 || r.Side != "before-submit" && r.Side != "after-applied" || r.Partition != 3 && r.Partition != 8 || (r.Phase == 1 || r.Phase == 3) && r.Partition != 8 || len(r.Actions) > 8 {
		return false
	}
	ticks := 0
	for _, a := range r.Actions {
		if a.Member < 1 || a.Member > 6 || a.Ticks < 1 || a.Ticks > 3 {
			return false
		}
		ticks += a.Ticks
	}
	return ticks <= 16
}

type nativeLogicalTransitionArtifact struct {
	nativeFaultArtifact
	Recipe            nativeTransitionRecipe
	Gate              nativeLogicalTransitionReceipt
	Ticks             []nativeFaultFamilyTick
	Run               [16]byte
	DeadlineUnixNanos int64
	RecipeSHA256      [32]byte
}

func nativeLogicalTransitionCall(t *testing.T, p *graphProcess, c nativeLogicalTransitionControl) nativeLogicalTransitionReceipt {
	t.Helper()
	wire, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	response, transport := p.exchange(ctx, processRequest{op: processNativeTransitionControl, data: wire})
	cancel()
	if err := errors.Join(transport, processResponseError(response)); err != nil {
		t.Fatal("actual live transition control", err)
	}
	var receipt nativeLogicalTransitionReceipt
	d := json.NewDecoder(bytes.NewReader(response.aux))
	d.DisallowUnknownFields()
	if err := d.Decode(&receipt); err != nil || receipt.Version != 1 || receipt.Profile != nativeLogicalTransitionProfile || receipt.Cookie == ([16]byte{}) || receipt.PID != uint64(p.cmd.Process.Pid) {
		t.Fatal("actual live process transition receipt", err)
	}
	return receipt
}

func nativeLogicalTransitionWrite(t *testing.T, directory string, artifact nativeLogicalTransitionArtifact) string {
	t.Helper()
	path := filepath.Join(directory, "native-transition-"+artifact.Case+".jsonl")
	header := artifact
	header.Records = nil
	return nativeTransitionWriteRows(t, path, header, artifact.Records, artifact.Through)
}

func nativeTransitionWriteRows(t *testing.T, path string, header any, records []lifecycleRawRecord, through [2]uint64) string {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if file != nil {
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	digest, total := sha256.New(), 0
	write := func(value any, sealed bool) {
		body, err := json.Marshal(value)
		if err != nil || len(body)+1 > nativeLogicalTransitionFrameBytes || len(body)+1 > (16<<20)-total {
			t.Fatal("finite transition artifact admission", err)
		}
		body = append(body, '\n')
		total += len(body)
		if sealed {
			_, _ = digest.Write(body)
		}
		if n, err := file.Write(body); err != nil || n != len(body) {
			t.Fatal("transition artifact write", err)
		}
	}
	write(struct {
		Type     string
		Artifact any
	}{"header", header}, true)
	if len(records) > 512 {
		t.Fatal("transition complete prefix bound")
	}
	for _, record := range records {
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
	}{"footer", len(records), through, hex.EncodeToString(digest.Sum(nil))}, false)
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		t.Fatal(err)
	}
	file = nil
	t.Log("actual live transition complete source artifact", path)
	return path
}

// This driver executes a genuine cross write, withholding exactly one actual
// source-produced submission/result in its own child. The first selected phase
// is not recreated from the recorded observation after a lost owner. Recovery
// always re-enters the normal source APIs with the original business request.
func nativeLogicalCrossCase(t *testing.T, recipe nativeTransitionRecipe, output string) string {
	t.Helper()
	if !recipe.valid() {
		t.Fatal("finite recorded transition recipe")
	}
	var run [16]byte
	if raw := os.Getenv("RHO_GRAPH_TRANSITION_RUN"); raw == "" {
		if _, err := io.ReadFull(rand.Reader, run[:]); err != nil {
			t.Fatal(err)
		}
		t.Setenv("RHO_GRAPH_TRANSITION_RUN", hex.EncodeToString(run[:]))
	} else {
		wire, err := hex.DecodeString(raw)
		if err != nil || len(wire) != 16 || hex.EncodeToString(wire) != raw {
			t.Fatal("fresh run identity", err)
		}
		copy(run[:], wire)
	}
	if run == ([16]byte{}) {
		t.Fatal("zero fresh run identity")
	}
	if os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE") == "" {
		t.Setenv("RHO_GRAPH_TRANSITION_DEADLINE", strconv.FormatInt(time.Now().Add(3*time.Minute).UnixNano(), 10))
	}
	deadline, err := strconv.ParseInt(os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE"), 10, 64)
	if err != nil || deadline <= time.Now().UnixNano() || deadline-time.Now().UnixNano() > int64(1800*time.Second) {
		t.Fatal("fixed original replay deadline", err)
	}
	recipeWire, err := json.Marshal(recipe)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("RHO_GRAPH_TRANSITION_PROCESS", "1")
	f := newPairedGraphFaultNetwork(t)
	before := nativeFaultSeed(t, f)
	old := p06Observe(f, 6, graphReadFresh, graphCut{})
	cut, err := pairedFaultCut(old.Cut)
	if err != nil {
		t.Fatal(err)
	}
	proposal, from := f.proposal(3), f.indexes()
	original, err := decodePartitionGraphCommand(proposal.wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	tx := crossTransactionID(namespace{original.request.ns.graph, 8}, original.request.id)
	member, successor := uint64(1), uint64(2)
	if recipe.Partition == 8 {
		member, successor = 4, 5
	}
	owner := nativeLogicalTransitionCall(t, f.n.all[member], nativeLogicalTransitionControl{Version: 1, Action: "inspect"})
	control := nativeLogicalTransitionControl{Version: 1, Action: "arm", Cookie: owner.Cookie, Sequence: 1, Family: "cross", Phase: recipe.Phase, Side: recipe.Side, Identity: tx, Partition: recipe.Partition}
	armed := nativeLogicalTransitionCall(t, f.n.all[member], control)
	if armed.State != "armed" || armed.Control != control {
		t.Fatal("exact child transition arm")
	}
	call, cancel := f.beginRun(5, proposal)
	defer cancel()
	gate := nativeLogicalTransitionCall(t, f.n.all[member], nativeLogicalTransitionControl{Version: 1, Action: "await", Cookie: owner.Cookie, Sequence: 1})
	if gate.Control != control || gate.State != "reached" || gate.ProposalSHA256 != sha256.Sum256(gate.Proposal) || gate.PrepaidOutput != nativeLogicalTransitionFrameBytes || gate.AttemptedWork == 0 {
		t.Fatal("unmatched/foreign native phase gate")
	}
	command, _, err := decodeCrossCommand(gate.Proposal, defaultMaterializerLimits())
	if err != nil || command.phase != crossPhase(recipe.Phase) || command.ns.partition != recipe.Partition {
		t.Fatal("actual phase command", err)
	}
	if recipe.Side == "after-applied" {
		actual := f.n.all[member].call(t, processRequest{op: processEntryRecord, index: gate.Index})
		if gate.Index == 0 || gate.Term == 0 || gate.Root.Index != gate.Index || !bytes.Equal(gate.EntryData, gate.Proposal) || !bytes.Equal(gate.Outcome, actual.outcome) || !bytes.Equal(gate.CDC, actual.changes) {
			t.Fatal("after gate lacks exact actual native result")
		}
	} else if gate.Index != 0 || len(gate.Outcome) != 0 || len(gate.CDC) != 0 || len(gate.EntryData) != 0 {
		t.Fatal("before-submit gate claimed native application")
	}
	var ticks []nativeFaultFamilyTick
	for _, action := range recipe.Actions {
		ticks = append(ticks, nativeFaultFamilyTicks(t, f, action.Member, action.Ticks)...)
	}
	killed := f.kill(member, "replica")
	cancel()
	var pending []byte
	select {
	case answer := <-call:
		result, actual := decodeGraphProcessCoordinatorResult(answer.response)
		if !errors.Is(actual, errCrossProcessPending) || result.decision != (crossStatus{}) || result.resume.tx != tx {
			t.Fatal("interrupted original invented completion/ABORT", actual)
		}
		pending = p06ProcessWire(t, answer.response)
	case <-time.After(10 * time.Second):
		t.Fatal("original worker retained after selected owner loss")
	}
	f.elect(member, successor)
	p03Reconnect(f, 6)
	result := f.run(6, proposal)
	if result.decision.tx != tx {
		t.Fatal("restart replaced original request")
	}
	_ = p03Complete(f, 6, "edge-create", proposal, result, from, before)
	recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
	var reopened p06Observation
	rejoined := f.replaceBeforeHeal(member, func(_ *graphProcess, _ processResponse) { reopened = p06Observe(f, member, graphReadAt, cut) })
	retained := p06Observe(f, 6, graphReadAt, cut)
	through := f.indexes()
	name := fmt.Sprintf("cross-%d-p%d-%s", recipe.Phase, recipe.Partition, recipe.Side)
	artifact := nativeLogicalTransitionArtifact{nativeFaultArtifact: nativeFaultArtifact{Version: 2, Profile: nativeLogicalTransitionProfile, Case: name, Original: proposal.wire, Manifest: command.manifest, PendingCaller: pending, Before: old, Retained: retained, Recovered: recovered, Reopened: reopened, Processes: []pairedFaultProcess{killed, rejoined}, From: from, Through: through, Records: p06Records(f, from, through)}, Recipe: recipe, Gate: gate, Ticks: ticks, Run: run, DeadlineUnixNanos: deadline, RecipeSHA256: sha256.Sum256(recipeWire)}
	if output == "" {
		output = f.n.dir
	}
	return nativeLogicalTransitionWrite(t, output, artifact)
}
