package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativeRetentionMarker = nativeBatchMarker + "NGR1"
const nativeRetentionProfile = "native-actual-retained-reader-gc-process-v1"
const nativeRetentionReceiptBytes = 16 << 10

type nativeRetentionControl struct {
	Version  uint8
	Action   string
	Cookie   [16]byte
	Sequence uint64
	Request  [16]byte
	Floors   [6]uint64
	Index    uint64
	Original []byte
}

type nativeRetentionReceipt struct {
	Version                                                          uint8
	Profile                                                          string
	Cookie, Run                                                      [16]byte
	Member, PID                                                      uint64
	DeadlineUnixNanos                                                int64
	Sequence, Calls, Pages, GrantedRows, GrantedBytes, GrantedOutput uint64
	Action, State                                                    string
	ViewIndex                                                        uint64
	Original, Outcome, CDC                                           []byte
	Found                                                            bool
	Plan                                                             raftlog.ApplicationRetentionState
	Root                                                             raftlog.ApplicationRoot
	Progress                                                         raftlog.ApplicationRetentionProgress
	Work                                                             raftlog.ApplicationProofWork
}

// Exactly one genuine captured native view and one original produced by the
// local Host. Recover accepts inert command bytes only through the production
// GraphRetentionOriginal authentication door; it never recreates a proposal or
// submits those bytes. Physical cursor authority remains wholly in native Store.
type nativeRetentionOwner struct {
	mu                                         sync.Mutex
	once                                       sync.Once
	ctx                                        context.Context
	cancel                                     context.CancelFunc
	h                                          *allocationHost
	view                                       *raftlog.ApplicationView
	proposal                                   allocationProposal
	original                                   []byte
	result                                     graphRetentionResult
	lastWire, lastReply                        []byte
	lastError                                  error
	closeError                                 error
	sequence, calls, pages, rows, work, output uint64
	rowGrant, byteGrant                        uint64
	maxCalls, maxPages                         uint64
	ordinaryCalls, collectionCalls             uint64
	collectRows, collectBytes                  int
	freshCollection                            bool
	base                                       nativeRetentionReceipt
}

func newNativeRetentionOwner(h *allocationHost, batch *nativeBatchOwner) (*nativeRetentionOwner, error) {
	switch os.Getenv("RHO_GRAPH_STATE_RETENTION_PROCESS") {
	case "", "off":
		return nil, nil
	case "1":
	default:
		return nil, errInvalid
	}
	if h == nil || h.machine == nil || h.driver == nil || batch == nil || batch.nativeState == nil || batch.ctx.Err() != nil || h.machine.SemanticContractID() != durableColumnGraphSemanticContractID() || h.machine.store.ApplicationRetention().Version != 1 {
		return nil, errInvalid
	}
	base := nativeRetentionReceipt{Version: 1, Profile: nativeRetentionProfile, Run: batch.state.Run, Member: batch.state.Member, PID: batch.state.PID, DeadlineUnixNanos: batch.state.DeadlineUnixNanos}
	return newNativeRetentionOwnerFor(batch.ctx, h, base, 128, 64, 16, 1<<20, false)
}

// Only exact admitted constructors enter this shared native owner. The fresh
// workload supplies its own immutable finite ledger; no D/VFS receipt or copied
// caller state is turned into Store, plan, original or view authority.
func newNativeRetentionOwnerFor(parent context.Context, h *allocationHost, base nativeRetentionReceipt, calls, pages uint64, rows, bytes int, fresh bool) (*nativeRetentionOwner, error) {
	if parent == nil || h == nil || h.machine == nil || h.driver == nil || calls == 0 || pages == 0 || pages > calls || rows < 1 || bytes < 1 {
		return nil, errInvalid
	}
	ctx, cancel := context.WithCancel(parent)
	o := &nativeRetentionOwner{ctx: ctx, cancel: cancel, h: h, base: base, maxCalls: calls, maxPages: pages, collectRows: rows, collectBytes: bytes, freshCollection: fresh}
	if _, err := io.ReadFull(rand.Reader, o.base.Cookie[:]); err != nil {
		cancel()
		return nil, err
	}
	// Every native operation's existing source/page/output allowance is fully
	// funded before dispatch, including failed/unknown calls. These theoretical
	// grants allocate no quota-sized memory and raise no ordinary native limits.
	p := h.machine.store.ApplicationLimits()
	o.rowGrant = 128
	for _, n := range []int{h.machine.limits.sourceRows, p.MaxPageRows} {
		if n < 0 || uint64(n) > math.MaxUint64-o.rowGrant {
			cancel()
			return nil, errLimit
		}
		o.rowGrant += uint64(n)
	}
	o.byteGrant = 1 << 20
	for _, n := range []int{h.machine.limits.sourceBytes, p.MaxInstallBytes, p.MaxPageBytes, p.MaxOutcomeBytes, p.MaxChangeBytes} {
		if n < 0 || uint64(n) > (math.MaxUint64-o.byteGrant)/8 {
			cancel()
			return nil, errLimit
		}
		o.byteGrant += 8 * uint64(n)
	}
	fundedCalls := calls
	if fresh {
		fundedCalls = calls - pages
	}
	if fundedCalls == 0 || o.rowGrant > math.MaxUint64/fundedCalls || o.byteGrant > math.MaxUint64/fundedCalls || uint64(bytes) > math.MaxUint64/pages || uint64(rows) > math.MaxUint64/pages || calls > math.MaxUint64/(2*processFrameBytes+2*nativeRetentionReceiptBytes) {
		cancel()
		return nil, errLimit
	}
	if fresh && (!nativeColumnAdd(o.byteGrant*fundedCalls, uint64(bytes)*pages) || !nativeColumnAdd(o.rowGrant*fundedCalls, uint64(rows)*pages)) {
		cancel()
		return nil, errLimit
	}
	context.AfterFunc(ctx, func() { _ = o.close() })
	return o, nil
}

func (o *nativeRetentionOwner) close() error {
	if o == nil {
		return nil
	}
	o.once.Do(func() {
		o.cancel()
		o.mu.Lock()
		defer o.mu.Unlock()
		if o.view != nil {
			o.closeError = o.view.Close()
			o.view = nil
		}
		o.proposal = allocationProposal{}
		o.original = nil
		o.lastWire = nil
		o.lastReply = nil
	})
	return o.closeError
}

func nativeRetentionWire(c nativeRetentionControl) ([]byte, error) {
	body, err := json.Marshal(c)
	return append([]byte(nativeRetentionMarker), body...), err
}
func decodeNativeRetentionControl(wire []byte) (nativeRetentionControl, error) {
	var c nativeRetentionControl
	if len(wire) > 4096 || !bytes.HasPrefix(wire, []byte(nativeRetentionMarker)) {
		return c, errInvalid
	}
	body := wire[len(nativeRetentionMarker):]
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, errors.Join(errInvalid, err)
	}
	canonical, err := json.Marshal(c)
	if err != nil || !bytes.Equal(canonical, body) || c.Version != 1 || len(c.Original) > graphRetentionCommandBytes {
		return c, errors.Join(errInvalid, err)
	}
	base := nativeRetentionControl{Version: 1, Action: c.Action, Cookie: c.Cookie, Sequence: c.Sequence}
	switch c.Action {
	case "inspect":
		base.Cookie, base.Sequence = [16]byte{}, 0
	case "prepare":
		base.Request, base.Floors = c.Request, c.Floors
		if c.Request == ([16]byte{}) {
			return c, errInvalid
		}
	case "recover":
		base.Original = c.Original
		if len(c.Original) == 0 {
			return c, errInvalid
		}
	case "root", "open":
		base.Index = c.Index
		if c.Index == 0 {
			return c, errInvalid
		}
	case "submit", "original", "close", "collect", "current", "anchor":
	default:
		return c, errInvalid
	}
	want, err := json.Marshal(base)
	if err != nil || !bytes.Equal(want, body) {
		return c, errors.Join(errInvalid, err)
	}
	return c, nil
}

func (o *nativeRetentionOwner) receipt(c nativeRetentionControl) nativeRetentionReceipt {
	r := o.base
	r.Sequence, r.Calls, r.Pages = c.Sequence, o.calls, o.pages
	r.GrantedRows, r.GrantedBytes, r.GrantedOutput = o.rows, o.work, o.output
	r.Action = c.Action
	r.Original = o.original
	if o.view != nil {
		r.State = "view-held"
	} else {
		r.State = "no-view"
	}
	return r
}

// This callback is on the existing child native-event lane. Its real event and
// packets are handed back to ordinary plumbing; no successful Step/ACK is made
// up. The fixed view is explicitly closed before collection or on cancellation.
func (o *nativeRetentionOwner) event(wire []byte) (response processResponse, event allocationEvent, err error) {
	if o == nil {
		return response, event, errInvalid
	}
	c, err := decodeNativeRetentionControl(wire)
	if err != nil {
		return response, event, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.calls >= o.maxCalls {
		return response, event, errLimit
	}
	if o.freshCollection {
		if c.Action == "collect" {
			if o.collectionCalls >= o.maxPages {
				return response, event, errLimit
			}
			o.collectionCalls++
		} else {
			if o.ordinaryCalls >= o.maxCalls-o.maxPages {
				return response, event, errLimit
			}
			o.ordinaryCalls++
		}
	}
	o.calls++
	rows, grantBytes := o.rowGrant, o.byteGrant
	if o.freshCollection && c.Action == "collect" {
		rows, grantBytes = uint64(o.collectRows), uint64(o.collectBytes)
	}
	if !nativeColumnAdd(o.rows, rows) || !nativeColumnAdd(o.work, grantBytes) || !nativeColumnAdd(o.output, 2*processFrameBytes+2*nativeRetentionReceiptBytes) {
		return response, event, errLimit
	}
	o.rows += rows
	o.work += grantBytes
	o.output += 2*processFrameBytes + 2*nativeRetentionReceiptBytes
	if err := o.ctx.Err(); err != nil {
		return response, event, errors.Join(replica.ErrUnavailable, err)
	}
	if c.Action != "inspect" {
		if c.Cookie != o.base.Cookie || c.Cookie == ([16]byte{}) {
			return response, event, replica.ErrUnavailable
		}
		if c.Sequence == o.sequence && bytes.Equal(wire, o.lastWire) {
			if c.Action == "submit" {
				return response, event, replica.ErrUnavailable
			} // Lost ACK packets are not a fabricated successful replay.
			response.aux = bytes.Clone(o.lastReply)
			return response, event, o.lastError
		}
		if c.Sequence != o.sequence+1 {
			return response, event, errInvalid
		}
		o.sequence = c.Sequence
	}
	r := o.receipt(c)
	defer func() {
		// The response owns all variable fields; no live view/native buffers are
		// serialized or reused as authority. Failed replies retain their grant.
		r.Calls, r.Pages, r.GrantedRows, r.GrantedBytes, r.GrantedOutput = o.calls, o.pages, o.rows, o.work, o.output
		response.aux, err = marshalNativeRetentionReceipt(r, err)
		if c.Action != "inspect" {
			o.lastWire = bytes.Clone(wire)
			o.lastReply = bytes.Clone(response.aux)
			o.lastError = err
		}
	}()
	s := o.h.machine.store
	switch c.Action {
	case "inspect":
		return response, event, nil
	case "prepare":
		if len(o.original) != 0 || o.view != nil {
			return response, event, errLimit
		}
		f := c.Floors
		o.proposal, err = o.h.PrepareGraphRetention(o.ctx, requestID(c.Request), graphRetentionFloors{f[0], f[1], f[2], f[3], f[4], f[5]})
		if err == nil {
			o.original = bytes.Clone(o.proposal.wire)
			r.Original = o.original
		}
	case "submit":
		if len(o.proposal.wire) == 0 {
			return response, event, errInvalid
		}
		event, err = o.h.Submit(o.proposal)
	case "recover":
		if len(o.original) != 0 || o.view != nil {
			return response, event, errLimit
		}
		command, e := decodeGraphRetentionCommand(c.Original)
		if e != nil || command.scope != o.h.machine.scope() {
			return response, event, errors.Join(errInvalid, e)
		}
		// Inert bytes are read-only original lookup input, never resubmitted.
		o.result, r.Found, err = o.h.GraphRetentionOriginal(o.ctx, allocationProposal{target: command.scope, wire: c.Original})
		if err == nil && !r.Found {
			err = replica.ErrUnavailable
		}
		if err == nil {
			o.original = bytes.Clone(c.Original)
			r.Original = o.original
		}
	case "original":
		if len(o.original) == 0 {
			return response, event, errInvalid
		}
		command, e := decodeGraphRetentionCommand(o.original)
		if e != nil {
			return response, event, e
		}
		o.result, r.Found, err = o.h.GraphRetentionOriginal(o.ctx, allocationProposal{target: command.scope, wire: o.original})
		if err == nil && !r.Found {
			err = replica.ErrUnavailable
		}
	case "open":
		if o.view != nil {
			return response, event, errLimit
		}
		index := o.h.driver.Applied()
		o.view, err = s.ApplicationView(index)
		if err == nil {
			r.ViewIndex = index
			r.Root, err = o.view.RootAt(o.ctx, c.Index, 172)
			r.State = "view-held"
		}
	case "root":
		if o.view == nil {
			return response, event, errInvalid
		}
		r.Root, err = o.view.RootAt(o.ctx, c.Index, 172)
		r.State = "view-held"
	case "close":
		if o.view != nil {
			err = o.view.Close()
			o.view = nil
		}
		r.State = "no-view"
	case "collect":
		if o.result.status != graphRetentionApplied || o.result.id != sha256.Sum256(o.original) {
			return response, event, errInvalid
		}
		if o.pages >= o.maxPages {
			return response, event, errLimit
		}
		o.pages++
		r.Progress, r.Work, err = s.CollectApplicationRetention(o.ctx, o.result.id, raftlog.ReadBudget{Rows: o.collectRows, Bytes: o.collectBytes})
	case "current", "anchor":
		if o.view != nil {
			return response, event, errLimit
		}
		var v *raftlog.ApplicationView
		v, err = s.ApplicationView(o.h.driver.Applied())
		if err != nil {
			return response, event, err
		}
		defer func() { err = errors.Join(err, v.Close()) }()
		r.Plan, r.Found, r.Work, err = v.RetentionPlan(o.ctx, raftlog.ReadBudget{Rows: 1, Bytes: 16 << 10})
		if err == nil && (!r.Found || r.Plan.ID != o.result.id) {
			err = errCorrupt
		}
		if err == nil && c.Action == "current" {
			r.Root, err = v.RootBounded(o.ctx, 172)
		}
		if err == nil && c.Action == "anchor" {
			if len(r.Plan.Roots) == 0 {
				err = errCorrupt
			} else {
				r.Root, err = v.RetentionAnchor(o.ctx, r.Plan.ID, r.Plan.InstalledIndex, r.Plan.Roots[0].Index, 172)
			}
		}
	}
	if err == nil && (c.Action == "original" || c.Action == "recover") {
		if o.result.status != graphRetentionApplied || o.result.id != sha256.Sum256(o.original) {
			return response, event, errCorrupt
		}
		r.Outcome, err = s.ApplicationRecord(o.ctx, o.result.index, true, 256)
		if err == nil {
			r.CDC, err = s.ApplicationRecord(o.ctx, o.result.index, false, 256)
		}
		if err == nil {
			encoded, e := encodeGraphRetentionResult(o.result)
			if e != nil || !bytes.Equal(encoded, r.Outcome) || len(r.CDC) != 0 {
				err = errors.Join(errCorrupt, e)
			}
		}
	}
	return response, event, err
}

func marshalNativeRetentionReceipt(r nativeRetentionReceipt, op error) ([]byte, error) {
	b, err := json.Marshal(r)
	if err != nil || len(b) >= nativeRetentionReceiptBytes {
		return nil, errors.Join(op, errLimit, err)
	}
	return b, op
}
