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
	"slices"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

const nativeProtocolStateProfile = "native-protocol-owned-state-transition-v1"
const nativeProtocolStateMarker = "NPS1"
const nativeProtocolStateCalls = 384
const nativeProtocolStateTerminalCalls = 32
const nativeProtocolStatePages = 64

// A declared D-only native admission window, not an assertion that every
// message schedule produces this many entries. Overflow must backpressure.
const nativeProtocolStateTailEntries = processSourcePending + processSourceWorkers + 1

func nativeProtocolStatePooledTailPolicy(p *raftlog.ApplicationPolicy) error {
	if p == nil || p.MaxTailEntries != 64 || p.MaxInstallBytes != 4<<20 || p.RetainedApplicationBytes != 256<<20 {
		return errInvalid
	}
	// Existing 16 queued calls, two source workers and the native admission's
	// one internal no-op motivate this finite window. Its 19*4MiB protection
	// leaves room for the original pools in the SAME 256MiB retained backing.
	p.MaxTailEntries = nativeProtocolStateTailEntries
	return nil
}

// A D-only constructor, not a cost profile. Exact old constructors retain their
// agreement and observer refusals. This check runs BEFORE native Open, including
// reopen; a copied recipe cannot select a profile after storage is acquired.
func nativeProtocolStateProfileFromEnv() (bool, error) {
	switch os.Getenv("RHO_GRAPH_PROTOCOL_STATE_PROCESS") {
	case "", "off":
		return false, nil
	case "1":
	default:
		return false, errInvalid
	}
	for _, key := range []string{"RHO_GRAPH_CERTIFIED_PROCESS", "RHO_GRAPH_SOURCE_PROCESS", "RHO_GRAPH_STABLE_STORAGE_PROCESS", "RHO_GRAPH_STABLE_STATE_PROCESS", "RHO_GRAPH_TRANSITION_PROCESS"} {
		if os.Getenv(key) != "1" {
			return false, errInvalid
		}
	}
	// RANGE is only the existing small factory fixture's explicit switch. It does
	// not choose the materializer: the exact span selection is performed below.
	if value := os.Getenv("RHO_GRAPH_RANGE_PROCESS"); value != "" && value != "1" {
		return false, errInvalid
	}
	for _, key := range []string{"RHO_GRAPH_FEED_PROCESS", "RHO_GRAPH_FEED_RETAINED_PROCESS", "RHO_GRAPH_EDGE_UNIQUE_PROCESS", "RHO_GRAPH_SELECTORS_PROCESS", "RHO_GRAPH_CREATE_UPDATE_PROCESS", "RHO_GRAPH_IMMUTABLE_COLUMNS_PROCESS", "RHO_GRAPH_LATE_CORRECTION_PROCESS", "RHO_GRAPH_COST_SPEC", "RHO_GRAPH_NATIVE_WORKLOAD", "RHO_GRAPH_WORKLOAD_SPEC", "RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_STATE_RETENTION_PROCESS", "RHO_GRAPH_NATIVE_FAULT_PROCESS", "RHO_GRAPH_DECISION_FAULT_PROCESS"} {
		if value := os.Getenv(key); value != "" && value != "off" {
			return false, errInvalid
		}
	}
	run, err := hex.DecodeString(os.Getenv("RHO_GRAPH_TRANSITION_RUN"))
	deadline, e := strconv.ParseInt(os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE"), 10, 64)
	if err != nil || e != nil || len(run) != 16 || hex.EncodeToString(run) != os.Getenv("RHO_GRAPH_TRANSITION_RUN") || bytes.Equal(run, make([]byte, 16)) || strconv.FormatInt(deadline, 10) != os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE") || deadline <= time.Now().UnixNano() || deadline-time.Now().UnixNano() > int64(1800*time.Second) {
		return false, errors.Join(errInvalid, err, e)
	}
	return true, nil
}

type nativeProtocolStateControl struct {
	Version                   uint8
	Cookie                    [16]byte
	Sequence                  uint64
	Action                    string
	Request                   [16]byte
	Original, Token, Boundary []byte
	Digest                    [32]byte
}
type nativeProtocolStateReply struct {
	Version                                     uint8
	Profile                                     string
	Cookie, Run                                 [16]byte
	Sequence, Calls, TerminalCalls, GrantedWork uint64
	Member, PID                                 uint64
	DeadlineUnixNanos                           int64
	Action, Result                              string
	Original, Token, Page                       []byte
	Digest                                      [32]byte
	Snapshot                                    *nativeTransitionPinRead
	Completed                                   []byte
}

func decodeNativeProtocolStateControl(wire []byte) (nativeProtocolStateControl, error) {
	var c nativeProtocolStateControl
	if !bytes.HasPrefix(wire, []byte(nativeProtocolStateMarker)) || len(wire) > 64<<10 {
		return c, errInvalid
	}
	body := wire[len(nativeProtocolStateMarker):]
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&c); err != nil {
		return c, errors.Join(errInvalid, err)
	}
	canonical, err := json.Marshal(c)
	if err != nil || !bytes.Equal(body, canonical) || c.Version != 1 {
		return c, errors.Join(errInvalid, err)
	}
	base := nativeProtocolStateControl{Version: 1, Cookie: c.Cookie, Sequence: c.Sequence, Action: c.Action}
	switch c.Action {
	case "inspect":
		base.Cookie, base.Sequence = [16]byte{}, 0
	case "begin":
		base.Request = c.Request
		if c.Request == ([16]byte{}) {
			return c, errInvalid
		}
	case "resume":
		base.Original, base.Token = c.Original, c.Token
		if len(c.Original) == 0 {
			return c, errInvalid
		}
	case "open":
		base.Token = c.Token
		if len(c.Token) == 0 {
			return c, errInvalid
		}
	case "page":
		base.Request, base.Boundary = c.Request, c.Boundary
		if c.Request == ([16]byte{}) || len(c.Boundary) == 0 {
			return c, errInvalid
		}
	case "ack":
		base.Request, base.Digest = c.Request, c.Digest
		if c.Request == ([16]byte{}) || c.Digest == ([32]byte{}) {
			return c, errInvalid
		}
	case "move-proof":
		base.Original, base.Boundary = c.Original, c.Boundary
		if !bytes.HasPrefix(c.Original, []byte("GMQ\x03")) || len(c.Original) > 32<<10 || len(c.Boundary) > 32 || len(c.Boundary)%8 != 0 {
			return c, errInvalid
		}
		for i := 0; i < len(c.Boundary); i += 8 {
			n := binary.BigEndian.Uint64(c.Boundary[i:])
			if n > 128 || i > 0 && n <= binary.BigEndian.Uint64(c.Boundary[i-8:]) {
				return c, errInvalid
			}
		}
	case "issue", "publish", "snapshot", "snapshot-close", "close", "release", "expire", "epoch", "cancel", "pin-close":
	default:
		return c, errInvalid
	}
	want, err := json.Marshal(base)
	if err != nil || !bytes.Equal(want, body) || c.Action != "inspect" && (c.Cookie == ([16]byte{}) || c.Sequence == 0 || c.Sequence > nativeProtocolStateCalls+nativeProtocolStateTerminalCalls) {
		return c, errors.Join(errInvalid, err)
	}
	return c, nil
}
func nativeProtocolStateTerminal(action string) bool {
	return action == "snapshot-close" || action == "close" || action == "release" || action == "expire" || action == "cancel" || action == "pin-close"
}
func nativeProtocolStateTerminalControl(r processRequest) bool {
	if r.op != processNativeTransitionControl || r.index != 0 || r.entity != 0 || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 {
		return false
	}
	c, err := decodeNativeProtocolStateControl(r.data)
	return err == nil && nativeProtocolStateTerminal(c.Action)
}

// The second result retains terminal cleanup lifetime independently of worker
// classification. It never admits another owner, grant or worker slot.
func nativeProtocolStateWorkerClass(r processRequest, primitiveControl bool) (control, terminal bool) {
	if nativeProtocolStateTerminalControl(r) {
		// These compound owners may relay native Close/Cancel back to this
		// child. Keep the one reserved control slot for that primitive RPC;
		// the orchestrator uses the existing ordinary slots and cleanup ctx.
		return false, true
	}
	return primitiveControl, primitiveControl
}

type nativeProtocolStateOwner struct {
	mu                               sync.Mutex
	ctx, cleanup                     context.Context
	cancel, stopCleanup              context.CancelFunc
	h                                *allocationHost
	publisher                        *retainedPinPublisher
	feed                             *retainedFeedOwner
	snapshot                         *certifiedGraphRead
	token                            retainedFeedToken
	original                         []byte
	gc                               *nativeRetentionOwner
	base                             nativeProtocolStateReply
	sequence, calls, terminals, work uint64
	busy                             bool
	last                             nativeProtocolStateControl
	reply                            []byte
	lastError                        error
	once                             sync.Once
}

func newNativeProtocolStateOwner(ctx context.Context, c *crossProcessCoordinator, batch *nativeBatchOwner) (*nativeProtocolStateOwner, error) {
	enabled, err := nativeProtocolStateProfileFromEnv()
	if err != nil || !enabled {
		return nil, err
	}
	agreement, err := nativeProtocolStateAgreement()
	if err != nil {
		return nil, err
	}
	if ctx == nil || c == nil || c.host == nil || c.host.machine.SemanticContractID() != agreement || batch == nil || batch.nativeState == nil || batch.ctx.Err() != nil {
		return nil, errInvalid
	}
	p, err := newRetainedPinPublisher(c)
	if err != nil {
		return nil, err
	}
	deadline := time.Unix(0, batch.state.DeadlineUnixNanos)
	normal, cancel := context.WithDeadline(ctx, deadline)
	cleanup, stop := context.WithDeadline(context.WithoutCancel(ctx), deadline.Add(30*time.Second))
	o := &nativeProtocolStateOwner{ctx: normal, cleanup: cleanup, cancel: cancel, stopCleanup: stop, h: c.host, publisher: p, base: nativeProtocolStateReply{Version: 1, Profile: nativeProtocolStateProfile, Run: batch.state.Run, Member: batch.state.Member, PID: batch.state.PID, DeadlineUnixNanos: deadline.UnixNano()}}
	if _, err := io.ReadFull(rand.Reader, o.base.Cookie[:]); err != nil {
		cancel()
		stop()
		return nil, err
	}
	base := nativeRetentionReceipt{Version: 1, Profile: nativeProtocolStateProfile, Run: batch.state.Run, Member: batch.state.Member, PID: batch.state.PID, DeadlineUnixNanos: deadline.UnixNano()}
	o.gc, err = newNativeRetentionOwnerFor(normal, c.host, base, 384, 256, 16, 1<<20, true)
	if err != nil {
		cancel()
		stop()
		return nil, err
	}
	return o, nil
}
func (o *nativeProtocolStateOwner) close() error {
	if o == nil {
		return nil
	}
	var err error
	o.once.Do(func() {
		o.cancel()
		o.stopCleanup()
		o.mu.Lock()
		defer o.mu.Unlock()
		if o.snapshot != nil {
			err = o.snapshot.Close()
			o.snapshot = nil
		}
		if o.feed != nil {
			err = errors.Join(err, o.feed.Close())
			o.feed = nil
		}
		err = errors.Join(err, o.gc.close())
	})
	return err
}
func (o *nativeProtocolStateOwner) serve(caller context.Context, wire []byte) ([]byte, error) {
	if o == nil || caller == nil {
		return nil, errInvalid
	}
	c, err := decodeNativeProtocolStateControl(wire)
	if err != nil {
		return nil, err
	}
	terminal := nativeProtocolStateTerminal(c.Action)
	o.mu.Lock()
	if o.busy {
		o.mu.Unlock()
		return nil, errRetainedFeedPending
	}
	if c.Action != "inspect" && c.Cookie != o.base.Cookie {
		o.mu.Unlock()
		return nil, replica.ErrUnavailable
	}
	if c.Sequence != 0 && c.Sequence == o.sequence && nativeColumnSame(c, o.last) {
		body, e := bytes.Clone(o.reply), o.lastError
		o.mu.Unlock()
		return body, e
	}
	if c.Sequence != 0 && c.Sequence != o.sequence+1 || terminal && o.terminals >= nativeProtocolStateTerminalCalls || !terminal && o.calls >= nativeProtocolStateCalls {
		o.mu.Unlock()
		return nil, errLimit
	}
	ownerCtx := o.ctx
	if terminal {
		ownerCtx = o.cleanup
		o.terminals++
	} else {
		o.calls++
	}
	// A finite invocation's complete ordinary production allowance is retired
	// before effects. Native durable feed attempts are a separate, persistent
	// ledger; no reconstructed recipe resets them after process loss.
	grant := uint64(4*(64<<20) + 2*processFrameBytes)
	if !nativeColumnAdd(o.work, grant) {
		o.mu.Unlock()
		return nil, errLimit
	}
	o.work += grant
	if e := errors.Join(ownerCtx.Err(), func() error {
		if terminal {
			return nil
		}
		return caller.Err()
	}()); e != nil {
		o.mu.Unlock()
		return nil, e
	}
	o.busy = true
	if c.Sequence != 0 {
		o.sequence, o.last = c.Sequence, c
	}
	o.mu.Unlock()
	operation, cancel := context.WithCancel(ownerCtx)
	var stop func() bool
	if !terminal {
		stop = context.AfterFunc(caller, cancel)
	}
	defer func() {
		if stop != nil {
			stop()
		}
		cancel()
	}()
	r := o.base
	r.Action, r.Sequence = c.Action, c.Sequence
	if c.Action != "inspect" && c.Action != "begin" && c.Action != "resume" && c.Action != "open" && c.Action != "close" && c.Action != "snapshot-close" && c.Action != "move-proof" && o.feed == nil {
		err = errInvalid
	} else {
		switch c.Action {
		case "inspect":
		case "move-proof":
			r.Page, err = nativeProtocolStateMoveProof(operation, o.h, c)
			if err == nil {
				r.Digest = sha256.Sum256(r.Page)
			}
		case "begin":
			if o.feed != nil || len(o.original) != 0 {
				err = errInvalid
				break
			}
			l := o.h.machine.limits
			policy := retainedFeedPolicy{pageBytes: 64 << 10, pageRows: 256, maxPages: nativeProtocolStatePages, maxAttempts: 8 * nativeProtocolStatePages, operationRows: l.sourceRows, operationBytes: l.sourceBytes, operationOutput: l.outputBytes, deadlineUnixNanos: o.base.DeadlineUnixNanos}
			o.feed, err = beginRetainedFeed(operation, o.publisher, requestID(c.Request), graphReadRequest{mode: graphReadFresh}, 1, policy)
			if err == nil {
				o.original, err = o.feed.Original()
			}
		case "resume":
			if o.snapshot != nil || o.feed != nil {
				err = errLimit
				break
			}
			if len(o.original) != 0 && !bytes.Equal(c.Original, o.original) {
				err = errInvalid
				break
			}
			if len(c.Token) != 0 {
				var token retainedFeedToken
				token, err = decodeRetainedFeedToken(c.Token)
				if err == nil && !bytes.Equal(token.definition, c.Original) {
					err = errInvalid
				}
				if err == nil {
					o.feed, err = openRetainedFeed(operation, o.publisher, token)
				}
				if err == nil {
					o.token = token
				}
			} else {
				o.feed, err = resumeRetainedFeed(operation, o.publisher, c.Original)
				if err == nil {
					o.token = retainedFeedToken{}
				}
			}
			if err == nil {
				o.original = bytes.Clone(c.Original)
			}
		case "open":
			if o.snapshot != nil || o.feed != nil {
				err = errLimit
				break
			}
			var token retainedFeedToken
			token, err = decodeRetainedFeedToken(c.Token)
			if err == nil && len(o.original) != 0 && !bytes.Equal(token.definition, o.original) {
				err = errInvalid
			}
			if err == nil {
				o.feed, err = openRetainedFeed(operation, o.publisher, token)
			}
			if err == nil {
				o.token = token
				o.original, err = o.feed.Original()
			}
		case "issue":
			err = o.feed.pin.Issue(operation, o.publisher)
		case "publish":
			o.token, err = o.feed.Publish(operation)
		case "snapshot":
			if o.snapshot == nil {
				o.snapshot, err = o.feed.OpenSnapshot(o.ctx)
			}
			if err == nil {
				r.Snapshot, err = nativeTransitionPinProjectionFor(operation, o.h, o.snapshot, true)
			}
		case "snapshot-close":
			if o.snapshot != nil {
				err = o.snapshot.Close()
				o.snapshot = nil
			}
		case "page":
			var boundary graphCut
			boundary, err = nativeProtocolStateCut(c.Boundary)
			if err == nil {
				var d retainedFeedDelivery
				d, err = o.feed.Page(operation, c.Request, boundary)
				if err == nil {
					r.Page, r.Digest = d.wire, d.digest
				}
			}
		case "ack":
			var cut graphCut
			cut, err = o.feed.Ack(operation, c.Request, c.Digest)
			if err == nil && len(cut.certificates) != 0 {
				r.Completed, err = nativeFeedCutWire(cut)
			}
		case "release":
			err = o.feed.Release(operation)
		case "expire":
			err = o.feed.Expire(operation)
		case "pin-close":
			err = o.feed.pin.Close(operation)
		case "cancel":
			err = o.feed.pin.CancelIssue(operation)
		case "epoch":
			m := o.feed.pin.definition
			err = o.publisher.AdvanceEpoch(operation, m.issuer, pinTerminalAction(m, o.feed.pin.manifest, pinAdvanceEpoch), m.epoch)
		case "close":
			if o.snapshot != nil {
				err = o.snapshot.Close()
				o.snapshot = nil
			}
			if o.feed != nil {
				err = errors.Join(err, o.feed.Close())
				o.feed = nil
			}
		}
	}
	r.Original = bytes.Clone(o.original)
	if len(o.token.pin.publications) != 0 {
		r.Token, err = func(previous error) ([]byte, error) {
			b, e := encodeRetainedFeedToken(o.token)
			return b, errors.Join(previous, e)
		}(err)
	}
	r.Result = nativeTransitionPinErrorClass(err)
	o.mu.Lock()
	r.Calls, r.TerminalCalls, r.GrantedWork = o.calls, o.terminals, o.work
	body, encodeErr := json.Marshal(r)
	if len(body) > processFrameBytes/2 {
		encodeErr = errors.Join(encodeErr, errLimit)
		body = nil
	}
	err = errors.Join(err, encodeErr)
	o.reply, o.lastError, o.busy = bytes.Clone(body), err, false
	o.mu.Unlock()
	return body, err
}

// Complete refers only to the SAME <=4 requested native keys, never a claimed
// EOF census of all historical undo. None of these observations is authority.
type nativeProtocolPoolControl struct {
	Found bool
	Key   []byte
	Index uint64
	Value []byte
}
type nativeProtocolPoolProof struct {
	Version                                         uint8
	CommandSHA256                                   [32]byte
	Scope                                           []byte
	RootIndex                                       uint64
	Root                                            [172]byte
	Frame                                           *nativeColumnFrame
	PreviousRoot                                    [172]byte
	IntentControl, OriginalControl, DecisionControl nativeProtocolPoolControl
	StatusFound                                     bool
	Status                                          []byte
	ProgressFound                                   bool
	Progress                                        []byte
	UndoIndices                                     []uint64
	Undo                                            []nativeProtocolPoolControl
	Rows, Bytes, Output                             uint64
	Complete                                        bool
}

func nativeProtocolStateMoveProof(ctx context.Context, h *allocationHost, c nativeProtocolStateControl) (body []byte, err error) {
	if ctx == nil || h == nil || h.machine.SemanticContractID() != rangeMovePooledSemanticContractID() || c.Action != "move-proof" {
		return nil, errInvalid
	}
	command, err := decodeRangeMoveCommand(c.Original)
	if err != nil || command.scope != h.machine.scope() {
		return nil, errors.Join(errInvalid, err)
	}
	intent, err := decodeRangeMoveIntent(command.intent)
	if err != nil || intent.maxPages > 128 {
		return nil, errors.Join(errInvalid, err)
	}
	// The owner has retired its full invocation grant before this read. One
	// output arena bounds decode/copies/encoding, and only one view is acquired.
	arena, err := graphstate.NewOutputBudget(32 << 20)
	if err != nil {
		return nil, err
	}
	v, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, v.Close()) }()
	root, err := v.RootBounded(ctx, 172)
	if err != nil || len(root.Image) != 172 {
		return nil, errors.Join(errCorrupt, err)
	}
	q := reader{ctx: ctx, view: v, ns: h.machine.ns, base: root, limits: limits{readRows: 256, readBytes: 16 << 20}, arena: arena}
	p := nativeProtocolPoolProof{Version: 1, CommandSHA256: sha256.Sum256(c.Original), Scope: appendAllocationScope(nil, command.scope), RootIndex: root.Index}
	copy(p.Root[:], root.Image)
	move := rangeMoveID(namespace{intent.source.graph, intent.source.partition}, intent.id)
	control := func(key []byte, maximum int) (nativeProtocolPoolControl, error) {
		r, found, e := crossControl(&q, key, maximum)
		if e != nil || found && (r.Index == 0 || r.Index > root.Index || !bytes.Equal(r.Key, key)) {
			return nativeProtocolPoolControl{}, errors.Join(errCorrupt, e)
		}
		out := nativeProtocolPoolControl{Found: found, Key: key}
		if found {
			out.Index, out.Value = r.Index, r.Value
		}
		return out, nil
	}
	p.IntentControl, err = control(rangeMoveControlKey(q.ns, move, 'I'), 1024)
	if err != nil {
		return nil, err
	}
	p.OriginalControl, err = control(movePoolOriginalKey(command), 288)
	if err != nil {
		return nil, err
	}
	if p.OriginalControl.Found && (len(p.OriginalControl.Value) < 32 || !bytes.Equal(p.OriginalControl.Value[:32], p.CommandSHA256[:])) {
		return nil, errInvalid
	}
	if (command.phase == rangeMoveFence || command.phase == rangeMoveAbort) && !p.OriginalControl.Found && p.IntentControl.Found {
		if !bytes.Equal(p.IntentControl.Value, command.intent) {
			return nil, errCorrupt
		}
		// Only a genuinely uncredited native original may use I. An existing
		// Fence I must not masquerade as a later, not-yet-installed Abort.
		wire, e := v.ApplicationRecord(ctx, p.IntentControl.Index, true, min(64<<10, h.machine.store.ApplicationLimits().MaxPageBytes))
		if e != nil {
			return nil, e
		}
		original, e := decodeRangeMoveStatus(wire)
		if e != nil {
			return nil, e
		}
		if original.ns == q.ns && original.move == move && original.intent == sha256.Sum256(command.intent) && original.index == p.IntentControl.Index && original.phase == command.phase && original.sequence == command.sequence {
			p.OriginalControl = p.IntentControl
		} else if command.phase == rangeMoveFence {
			return nil, errCorrupt
		}
	}
	p.DecisionControl, err = control(rangeMoveControlKey(q.ns, move, 'D'), 256)
	if err != nil {
		return nil, err
	}
	p.Status, p.StatusFound, err = rangeMoveUndoOriginal(&q, rangeMoveStatusKey(q.ns, move))
	if err != nil {
		return nil, err
	}
	p.Progress, p.ProgressFound, err = rangeMoveUndoOriginal(&q, rangeMoveProgressKey(q.ns, move))
	if err != nil {
		return nil, err
	}
	if len(p.Status) > 256 || len(p.Progress) > rangeMoveSpanProgressBytes {
		return nil, errLimit
	}
	if len(c.Boundary) != 0 {
		for i := 0; i < len(c.Boundary); i += 8 {
			p.UndoIndices = append(p.UndoIndices, binary.BigEndian.Uint64(c.Boundary[i:]))
		}
	} else {
		// Native fence undo0 is GMU1; only accepted import GMU2 contributes
		// to GMP6 debt. Always observe the distinct fence key explicitly.
		p.UndoIndices = []uint64{0}
		add := func(n uint64) {
			if n > 0 && n <= 128 && !slices.Contains(p.UndoIndices, n) {
				p.UndoIndices = append(p.UndoIndices, n)
			}
		}
		if command.phase == rangeMoveImport {
			add(command.sequence)
		}
		if p.StatusFound {
			status, e := decodeRangeMoveStatus(p.Status)
			if e != nil {
				return nil, e
			}
			if status.phase == rangeMoveImport {
				add(status.sequence)
			}
		}
		if p.ProgressFound && movePoolProgressWire(p.Progress) {
			progress, e := decodeRangeMoveProgress(p.Progress, arena)
			if e != nil {
				return nil, e
			}
			add(progress.undoTotal)
			if progress.undoNext < progress.undoTotal {
				add(progress.undoTotal - progress.undoNext)
			}
			if len(p.UndoIndices) > 4 {
				return nil, errLimit
			}
		}
		slices.Sort(p.UndoIndices)
	}
	for _, n := range p.UndoIndices {
		u, e := control(rangeMoveUndoControlKey(q.ns, move, n), 16<<10)
		if e != nil {
			return nil, e
		}
		p.Undo = append(p.Undo, u)
	}
	if p.OriginalControl.Found {
		index := p.OriginalControl.Index
		first, e := v.RootAt(ctx, index, 172)
		if e != nil || len(first.Image) != 172 || index < 1 {
			return nil, errors.Join(errCorrupt, e)
		}
		previous, e := v.RootAt(ctx, index-1, 172)
		if e != nil || len(previous.Image) != 172 {
			return nil, errors.Join(errCorrupt, e)
		}
		f := nativeColumnFrame{Partition: q.ns.partition, Index: index}
		copy(f.Root[:], first.Image)
		copy(p.PreviousRoot[:], previous.Image)
		maximum := min(64<<10, h.machine.store.ApplicationLimits().MaxPageBytes)
		f.Outcome, err = v.ApplicationRecord(ctx, index, true, maximum)
		if err != nil {
			return nil, err
		}
		f.CDC, err = v.ApplicationRecord(ctx, index, false, maximum)
		if err != nil || f.Outcome == nil || f.CDC == nil {
			return nil, errors.Join(errCorrupt, err)
		}
		f.Registration, err = boundedEncoding(8192, func(w *boundedWriter) {
			w.add([]byte("NCM1"))
			for slot, r := range []nativeProtocolPoolControl{p.IntentControl, p.OriginalControl, p.DecisionControl} {
				if r.Found && r.Index > index && slot != 2 {
					w.err = errCorrupt
					return
				}
				// A later D is still exported as actual DecisionControl above;
				// it was not part of this earlier immutable original's frame.
				if r.Found && r.Index <= index {
					w.field(r.Key)
					w.u64(r.Index)
					w.field(r.Value)
				} else {
					w.field(nil)
					w.u64(0)
					w.field(nil)
				}
			}
		})
		if err != nil {
			return nil, err
		}
		p.Frame = &f
	}
	p.Rows, p.Bytes, p.Output, p.Complete = uint64(q.rows), uint64(q.bytes), uint64(arena.Used()), true
	body, err = json.Marshal(p)
	if len(body) > 64<<10 {
		return nil, errLimit
	}
	return body, err
}

func nativeProtocolStateCut(wire []byte) (graphCut, error) {
	agreement, err := nativeProtocolStateAgreement()
	if err != nil {
		return graphCut{}, err
	}
	return nativeScalarWorkloadCutDecode(wire, agreement, 2, 2)
}
func nativeProtocolStateNetwork(t *testing.T, p08 bool) (*certifiedProcessNetwork, [32]byte) {
	t.Helper()
	rangeMoves := true
	dir, err := os.MkdirTemp(os.Getenv("RHO_GRAPH_PROCESS_ARTIFACT"), "rho-certified-process-")
	if err != nil {
		t.Fatal(err)
	}
	n := &certifiedProcessNetwork{all: make(map[uint64]*graphProcess), dir: dir, rangeMoves: rangeMoves, leaders: [2]uint64{1, 4}}
	if rangeMoves {
		n.e17MovePages, n.e17PublicPhases, n.e17VoterPhases = graphE17AdmittedMovePages, 16, 4
	}
	t.Log("certified process durable directories", dir)
	for id := uint64(1); id <= 6; id++ {
		env := []string{"RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_PROTOCOL_STATE_PROCESS=1"}
		if p08 {
			env = append(env, "RHO_GRAPH_ALLOCATION_PROCESS=1")
		}
		if rangeMoves {
			env = append(env, "RHO_GRAPH_RANGE_PROCESS=1")
		}
		n.all[id] = startGraphProcess(t, filepath.Join(dir, fmt.Sprint(id)), id, env...)
	}
	attachGraphProcessMuxes(t, n.all)
	n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: n.all[1], 2: n.all[2], 3: n.all[3]}}
	n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: n.all[4], 5: n.all[5], 6: n.all[6]}}
	// Cleanup also runs if a required actual transition is not reached. It reports
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
		if n.indexReadPhases > 0 {
			// Fixed E18 fixture allowance: seven admitted certified operations, each
			// allowing four IPC events per existing 1024-call remote operation ceiling.
			// This is a test allowance, not an algorithmic worst-case or production SLO.
			allowance = uint64(processDeliveries + n.indexReadPhases*4*1024)
			t.Logf("E18 counted query ledger: maximum=%d operations=%d/%d", allowance, n.indexReads, n.indexReadPhases)
		}
		if calls+uint64(packets) > allowance {
			t.Error("certified aggregate RPC/packet bound", calls, packets)
		}
		t.Logf("certified cleanup: reaped=%d successfully-reaped=%d of6; calls=%d packets=%d maxFrame=%d/%d maxQueueBytes=%d/%d", reaped, successful, calls, packets, n.all[1].maxFrame, n.all[4].maxFrame, n.groups[0].maxQueueBytes, n.groups[1].maxQueueBytes)
	})
	for group, leader := range []uint64{1, 4} {
		n.groups[group].event(t, leader, processRequest{op: processCampaign})
	}
	var bootstrap [32]byte
	nativeProtocolStateAction(t, n, 1, certifiedInitialize, 3, nil, &bootstrap)
	n.read(t, 1, observeConfiguration, 0)
	nativeProtocolStateAction(t, n, 1, certifiedInitialize, 8, nil, &bootstrap)
	source := uint64(1)
	if p08 {
		allocationP08ElectFor(t, n, nativeProtocolStateExpectedAgreement(t), 1, 3)
		n.leaders[0], source = 3, 3
		n.read(t, source, observeAllocator, 0)
		nativeProtocolStateAction(t, n, source, certifiedP08Transfer, 0, nil)
	}

	recipients := []struct{ actor, id, partition uint64 }{{1, 1, 3}, {4, 2, 8}}
	if p08 {
		recipients[0].actor = 3
	}
	for _, recipient := range recipients {
		n.read(t, recipient.actor, observeConfiguration, 0)
		nativeProtocolStateAction(t, n, recipient.actor, certifiedBeginRecipient, recipient.id, nil)
		n.read(t, source, observeRecipient, recipient.id)
		nativeProtocolStateAction(t, n, source, certifiedRecipientTransition, 3, []byte{byte(fenceDeclaredRecipient)})
		if recipient.partition != 3 {
			nativeProtocolStateAction(t, n, source, certifiedRecipientTransition, recipient.partition, []byte{byte(fenceDeclaredRecipient)})
		}
		n.read(t, recipient.actor, observeRecipient, recipient.id)
		nativeProtocolStateAction(t, n, recipient.actor, certifiedRecipientTransition, 3, []byte{byte(acknowledgeDeclaredHome)})
		n.read(t, source, observeRecipient, recipient.id)
		nativeProtocolStateAction(t, n, source, certifiedRecipientTransition, 3, []byte{byte(activateDeclaredRecipient)})
		n.read(t, source, observeRecipient, recipient.id)
		nativeProtocolStateAction(t, n, source, certifiedRecipientTransition, recipient.partition, []byte{byte(publishDeclaredRecipient)})
		n.read(t, recipient.actor, observeRecipient, recipient.id)
		nativeProtocolStateAction(t, n, recipient.actor, certifiedOpenRecipient, 0, nil)
		grants := [][2]uint64{{1, 16}}
		if rangeMoves && !p08 && recipient.partition == 3 {
			grants = [][2]uint64{{1, 1}, {2, 15}}
		}
		for _, grant := range grants {
			n.read(t, source, observeRecipient, recipient.id)
			n.read(t, source, observeAllocator, 0)
			var input []byte
			if rangeMoves {
				input = graphE17GrantInput(grant[0], grant[1])
			}
			nativeProtocolStateAction(t, n, source, certifiedReserve, 0, input)
			n.group(source).event(t, source, processRequest{op: processCertifiedRead, entity: uint64(observeGrant), data: input})
			if !n.all[source].call(t, processRequest{op: processStateOp}).ready {
				t.Fatal("actual grant read did not complete", grant)
			}
			nativeProtocolStateAction(t, n, source, certifiedInstallGrant, recipient.partition, nil)
		}
	}
	if !p08 {
		nativeProtocolStateAction(t, n, 1, certifiedCreateNode, 1, nil)
		nativeProtocolStateAction(t, n, 4, certifiedCreateNode, 17, nil)
	}
	return n, bootstrap
}
func nativeProtocolStateAction(t *testing.T, n *certifiedProcessNetwork, actor, kind, index uint64, data []byte, bootstrap ...*[32]byte) (outcome, processResponse) {
	return nativeProtocolStateActionProgram(t, n, actor, kind, index, data, nil, bootstrap...)
}

func nativeProtocolStateOwnershipUnchanged(before, after [statCount]uint64) bool {
	return before[statViews] == after[statViews] && before[statViewBytes] == after[statViewBytes] && before[statPinnedBytes] == after[statPinnedBytes]
}

func nativeProtocolStateActionProgram(t *testing.T, n *certifiedProcessNetwork, actor, kind, index uint64, data []byte, program *nativeProtocolStateProgram, bootstrap ...*[32]byte) (outcome, processResponse) {
	t.Helper()
	// Source factories and old-cut/feed owners may remain live deliberately.
	// Observe their actual native accounting before the factory runs, then
	// require the committed action to preserve it on the SAME child instance.
	type ownership struct {
		process *graphProcess
		state   processResponse
	}
	before := make(map[uint64]ownership, len(n.all))
	if kind != certifiedOpenRecipient {
		for id, p := range n.all {
			before[id] = ownership{p, p.call(t, processRequest{op: processStateOp})}
		}
	}
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
	group, leader := 0, n.leaders[0]
	if proposal.target.partition == 8 {
		group, leader = 1, n.leaders[1]
	}
	ns, _ := certifiedProcessNamespace(leader)
	agreement := nativeProtocolStateExpectedAgreement(t)
	if proposal.target.graph != (idalloc.GraphID{1}) || proposal.target.partition != ns.partition || proposal.target.semantic != agreement {
		t.Fatal("source proposal escaped declared process topology", proposal.target)
	}
	if program != nil {
		if err := program.localSubmit(proposal, r.aux); err != nil {
			t.Fatal("actual publication predecessor message program", err)
		}
	} else {
		n.groups[group].event(t, leader, processRequest{op: processPropose, data: r.aux})
	}
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
		owned := before[id]
		if owned.process != p || owned.state.incarnation != actual.incarnation || !nativeProtocolStateOwnershipUnchanged(owned.state.stats, actual.stats) {
			t.Fatalf("certified process operation changed retained ownership: actor=%d kind=%d member=%d applied=%d before(instance=%x views=%d viewBytes=%d pins=%d) after(instance=%x views=%d viewBytes=%d pins=%d)", actor, kind, id, state.applied, owned.state.incarnation, owned.state.stats[statViews], owned.state.stats[statViewBytes], owned.state.stats[statPinnedBytes], actual.incarnation, actual.stats[statViews], actual.stats[statViewBytes], actual.stats[statPinnedBytes])
		}
	}
	if kind != certifiedCreateCrossEdge && o.reason != reasonNone {
		t.Fatal("certified process local positive control refused", kind, o)
	}
	if program != nil {
		program.mu.Lock()
		program.evidence.LocalRecord = p06ProcessWire(t, result)
		program.mu.Unlock()
	}
	if len(bootstrap) != 0 {
		if kind != certifiedInitialize || len(bootstrap) != 1 || bootstrap[0] == nil || *bootstrap[0] != ([32]byte{}) && *bootstrap[0] != proposal.target.semantic {
			t.Fatal("ambiguous actual state bootstrap")
		}
		*bootstrap[0] = proposal.target.semantic // After the actual all-voter original/CDC/root census above.
	}
	t.Logf("actual certified child operation: actor=%d target=%d kind=%d identity=%x index=%d reason=%d", actor, proposal.target.partition, kind, o.identity, o.index, o.reason)
	return o, result
}

// The family/phase decoder authenticates diagnostic selection only. The actual
// production proposal remains on its original child. Complete Ready membership
// and native Sync receipts, rather than this label, qualify a selected loss.
type nativeProtocolStateRecipe struct {
	Family          string
	Phase           byte
	Partition, Page uint64
	Kind            raftlog.StableStorageKind
	Side            string
}

func (r nativeProtocolStateRecipe) valid() bool {
	if r.Family == "gc" {
		return r.Phase == 0 && r.Partition == 3 && r.Page == 0 && r.Kind == 0 && (r.Side == "before" || r.Side == "after")
	}
	if r.Family == "publication-race" {
		return r == (nativeProtocolStateRecipe{Family: "publication-race", Partition: 3, Side: "after-gc"})
	}
	if r.Kind != raftlog.StableStorageReady && r.Kind != raftlog.StableStorageApplication || r.Side != "before" && r.Side != "after" {
		return false
	}
	switch r.Family {
	case "move":
		return (nativeTransitionMoveRecipe{Version: 1, Phase: r.Phase, Partition: r.Partition, Page: r.Page, Side: "before-submit"}).valid()
	case "pin":
		return r.Page == 0 && (nativeTransitionPinRecipe{Version: 1, Phase: r.Phase, Partition: r.Partition, Side: "before-submit"}).valid()
	case "allocation":
		return r.Page == 0 && r.Partition == 3 && (commandKind(r.Phase) == reserveDeclaredGrant || commandKind(r.Phase) == installDeclaredGrant)
	}
	return false
}
func nativeProtocolStateInventory() []nativeProtocolStateRecipe {
	var out []nativeProtocolStateRecipe
	for _, kind := range []raftlog.StableStorageKind{raftlog.StableStorageReady, raftlog.StableStorageApplication} {
		for _, side := range []string{"before", "after"} {
			for _, old := range nativeTransitionMoveInventory() {
				if old.Side == "before-submit" {
					out = append(out, nativeProtocolStateRecipe{"move", old.Phase, old.Partition, old.Page, kind, side})
				}
			}
			for _, old := range nativeTransitionPinInventory() {
				if old.Side == "before-submit" {
					out = append(out, nativeProtocolStateRecipe{"pin", old.Phase, old.Partition, 0, kind, side})
				}
			}
			for _, phase := range []commandKind{reserveDeclaredGrant, installDeclaredGrant} {
				out = append(out, nativeProtocolStateRecipe{"allocation", byte(phase), 3, 0, kind, side})
			}
		}
	}
	out = append(out, nativeProtocolStateRecipe{Family: "gc", Partition: 3, Side: "before"}, nativeProtocolStateRecipe{Family: "gc", Partition: 3, Side: "after"}, nativeProtocolStateRecipe{Family: "publication-race", Partition: 3, Side: "after-gc"})
	return out
}
func (r nativeProtocolStateRecipe) matches(p allocationProposal) (bool, error) {
	agreement, err := nativeProtocolStateAgreement()
	if err != nil {
		return false, err
	}
	if !r.valid() || !p.target.valid() || p.target.semantic != agreement || p.target.partition != r.Partition {
		return false, nil
	}
	switch r.Family {
	case "move":
		if !isRangeMoveWire(p.wire) {
			return false, nil
		}
		c, err := decodeRangeMoveCommand(p.wire)
		return err == nil && c.scope == p.target && byte(c.phase) == r.Phase && c.sequence == r.Page, err
	case "pin":
		if !isGraphPinWire(p.wire) {
			return false, nil
		}
		c, err := decodeGraphPinCommand(p.wire)
		return err == nil && c.scope == p.target && c.phase == r.Phase, err
	case "allocation":
		if !bytes.HasPrefix(p.wire, []byte("AQP\x01")) {
			return false, nil
		}
		c, err := decodeAllocationProtocolCommand(p.wire, defaultMaterializerLimits())
		return err == nil && c.ns.graph == p.target.graph && c.ns.partition == p.target.partition && byte(c.kind) == r.Phase, err
	}
	return false, nil
}

type nativeProtocolStateGate struct {
	mu                sync.Mutex
	ctx               context.Context
	f                 *pairedGraphFaultNetwork
	target            *graphProcess
	hub               *graphProcessHub
	recipe            nativeProtocolStateRecipe
	owner, arm        nativeBatchReceipt
	command           []byte
	reads, candidates uint64
	priorBefore       func(context.Context, uint64, uint64, []byte) error
	priorFailed       func(graphProcessPacketJob, error) bool
	lost              chan struct{}
	once              sync.Once
	closed            bool
	program           *nativeProtocolStateProgram
}

type nativeProtocolMoveCatchupVoter struct {
	Member, PID uint64
	Instance    string
	State       []byte
}
type nativeProtocolMoveCatchupProof struct {
	Version    uint8
	Actor      uint64
	Arm, Batch nativeBatchReceipt
	Command    []byte
	ActorState []byte
	Voters     []nativeProtocolMoveCatchupVoter
}

// This live fixture binding permits only native committed/apply catch-up while
// the original move fences allocation observations. It grants no read proof,
// move success, current cut, or replacement proposal authority.
type nativeProtocolMoveCatchup struct {
	gate    *nativeProtocolStateGate
	receipt nativeBatchReceipt
	command []byte
	actor   uint64
	process *graphProcess
	proof   *nativeProtocolMoveCatchupProof
}

func nativeProtocolMoveCheckpointReady(state processResponse) bool {
	if state.code != 0 || state.causeMask != 0 || len(state.image) != 172 || state.stats[statCommit] == 0 || state.stats[statTerm] == 0 || state.applied != state.stats[statCheckpoint] || state.applied != state.stats[statCommit] {
		return false
	}
	_, err := graphstore.DecodeRoot(state.image)
	return err == nil
}

func (g *nativeProtocolStateGate) moveCatchup(receipt nativeBatchReceipt) (*nativeProtocolMoveCatchup, error) {
	if g == nil || g.f == nil || g.f.n == nil || g.target == nil || g.target.cmd == nil || g.target.cmd.Process == nil {
		return nil, errInvalid
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.ctx.Err() != nil || g.recipe.Family != "move" || g.f.parent == nil || g.f.parent.Err() != nil || g.f.nativeMoveCatchup != nil || g.f.n.all[g.owner.Member] != g.target || g.owner.PID != uint64(g.target.cmd.Process.Pid) || !nativeBatchReceiptValid(receipt, g.arm) || sha256.Sum256(g.command) != receipt.Control.CommandSHA256 {
		return nil, errInvalid
	}
	command, err := decodeRangeMoveCommand(g.command)
	match, matchErr := g.recipe.matches(allocationProposal{target: command.scope, wire: g.command})
	if err != nil || matchErr != nil || !match || uint64(len(g.command)) != receipt.Events[0].CommandBytes || command.scope.semantic != g.f.nativeAgreement() {
		return nil, errors.Join(errInvalid, err, matchErr)
	}
	// Re-read the SAME owned native receipt; caller fields alone cannot select
	// this door. This consumes the existing finite receipt-read allowance.
	if g.reads >= 129 {
		return nil, errLimit
	}
	g.reads++
	actual, err := nativeBatchRead(filepath.Join(g.f.n.dir, fmt.Sprint(g.owner.Member)), g.arm)
	if err != nil || actual != receipt {
		return nil, errors.Join(errInvalid, err)
	}
	actor := g.f.n.leaders[0]
	if command.scope.partition == 8 {
		actor = g.f.n.leaders[1]
	}
	return &nativeProtocolMoveCatchup{gate: g, receipt: actual, command: bytes.Clone(g.command), actor: actor, process: g.f.n.all[actor]}, nil
}

func (c *nativeProtocolMoveCatchup) check(f *pairedGraphFaultNetwork, actor uint64) error {
	if c == nil || c.gate == nil || c.gate.f != f || f.parent == nil || f.parent.Err() != nil || c.gate.ctx.Err() != nil || actor != c.actor || f.n.all[actor] != c.process || !nativeBatchReceiptValid(c.receipt, c.gate.arm) || !bytes.Equal(c.command, c.gate.command) {
		return errInvalid
	}
	command, err := decodeRangeMoveCommand(c.command)
	ns, _ := certifiedProcessNamespace(actor)
	if err != nil || command.scope.partition != ns.partition || command.scope.semantic != f.nativeAgreement() || c.receipt.Events[0].CommandSHA256 != sha256.Sum256(c.command) {
		return errors.Join(errInvalid, err)
	}
	for id := range f.n.group(actor).nodes {
		f.mu.Lock()
		dead := f.down[id]
		f.mu.Unlock()
		if dead {
			return errInvalid // This fixture restores its sole selected follower.
		}
	}
	return nil
}

func newNativeProtocolStateGate(ctx context.Context, f *pairedGraphFaultNetwork, r nativeProtocolStateRecipe, programs ...*nativeProtocolStateProgram) (*nativeProtocolStateGate, error) {
	if ctx == nil || f == nil || f.n == nil || !r.valid() || r.Kind == 0 {
		return nil, errInvalid
	}
	member := uint64(3)
	if r.Partition == 8 {
		member = 6
	}
	if len(programs) > 1 {
		return nil, errInvalid
	}
	var program *nativeProtocolStateProgram
	if len(programs) == 1 && programs[0] != nil {
		program = programs[0]
		if program.f != f || program.recipe != r || program.evidence.Window != "selected-command" {
			return nil, errInvalid
		}
		member = program.evidence.Target
	}
	p := f.n.all[member]
	if p == nil || p.mux == nil {
		return nil, errInvalid
	}
	owner, err := nativeBatchCall(ctx, p, nativeBatchControl{Version: 1, Action: "inspect"})
	if err != nil || owner.State != "idle" || owner.Member != member || owner.PID != uint64(p.cmd.Process.Pid) {
		return nil, errors.Join(errCorrupt, err)
	}
	deadline, ok := ctx.Deadline()
	if !ok || owner.DeadlineUnixNanos != deadline.UnixNano() {
		return nil, errInvalid
	}
	g := &nativeProtocolStateGate{ctx: ctx, f: f, target: p, hub: p.mux.hub, recipe: r, owner: owner, lost: make(chan struct{}), program: program}
	g.hub.mu.Lock()
	g.priorBefore, g.priorFailed = g.hub.beforeRelay, g.hub.failedStep
	g.hub.beforeRelay, g.hub.failedStep = g.before, g.failed
	g.hub.mu.Unlock()
	return g, nil
}
func (g *nativeProtocolStateGate) selectProposal(ctx context.Context, p allocationProposal) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.ctx.Err() != nil {
		return errors.Join(replica.ErrUnavailable, g.ctx.Err())
	}
	if g.arm.Sequence != 0 {
		return nil
	}
	if g.candidates >= 128 || len(p.wire) > nativeLogicalTransitionBodyBytes {
		return errLimit
	}
	g.candidates++
	match, err := g.recipe.matches(p)
	if err != nil || !match {
		return err
	}
	control := nativeBatchControl{Version: 1, Action: "arm", Cookie: g.owner.Cookie, Sequence: 1, Kind: g.recipe.Kind, Side: g.recipe.Side, CommandSHA256: sha256.Sum256(p.wire)}
	actual, err := nativeBatchCall(ctx, g.target, control)
	if err != nil || actual.Control != control || actual.Run != g.owner.Run || actual.PID != g.owner.PID || actual.DeadlineUnixNanos != g.owner.DeadlineUnixNanos || actual.State != "armed" {
		return errors.Join(errCorrupt, err)
	}
	g.arm, g.command = actual, bytes.Clone(p.wire)
	if g.program != nil {
		if err := g.program.arm(g.owner.Member, p.target.partition, p.wire); err != nil {
			return err
		}
	}
	return nil
}
func (g *nativeProtocolStateGate) before(ctx context.Context, origin, part uint64, wire []byte) error {
	if g.priorBefore != nil {
		if err := g.priorBefore(ctx, origin, part, wire); err != nil {
			return err
		}
	}
	if part != g.recipe.Partition {
		return nil
	}
	// Decode the actual framed request, never a caller's expected phase label.
	q, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return err
	}
	if q.kind != sourceSubmit {
		return nil
	}
	p, err := decodeGraphSourceProposal(q.command)
	if err != nil {
		return err
	}
	return g.selectProposal(ctx, p)
}
func (g *nativeProtocolStateGate) read() (nativeBatchReceipt, error) {
	g.mu.Lock()
	if g.reads >= 129 || g.arm.Sequence == 0 {
		g.mu.Unlock()
		return nativeBatchReceipt{}, errLimit
	}
	g.reads++
	arm := g.arm
	g.mu.Unlock()
	if err := g.ctx.Err(); err != nil {
		return nativeBatchReceipt{}, err
	}
	return nativeBatchRead(filepath.Join(g.f.n.dir, fmt.Sprint(g.owner.Member)), arm)
}
func (g *nativeProtocolStateGate) failed(job graphProcessPacketJob, actual error) bool {
	if g.ctx.Err() == nil && job.packet.To == g.owner.Member && nativeBatchFailedStepAllowed(processResponse{}, actual) && g.hub.peer(g.owner.Member).p == g.target {
		if _, err := g.read(); err == nil {
			g.f.mu.Lock()
			g.f.down[g.owner.Member] = true
			g.f.mu.Unlock()
			g.once.Do(func() { close(g.lost) })
			return true
		}
	}
	return g.priorFailed != nil && g.priorFailed(job, actual)
}
func (g *nativeProtocolStateGate) close() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
	g.hub.mu.Lock()
	g.hub.beforeRelay, g.hub.failedStep = g.priorBefore, g.priorFailed
	g.hub.mu.Unlock()
}

// The explicit parent wrapper supplies its own original deadline to a genuine
// P08 event and drains only the pre-existing bounded tiny transport queue.
func nativeProtocolStateGroupEvent(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, g *nativeProtocolStateGate, actor uint64, r processRequest) {
	t.Helper()
	group := n.group(actor)
	out, err := n.all[actor].exchange(ctx, r)
	if err = errors.Join(err, processResponseError(out)); err != nil {
		if g == nil || actor != g.owner.Member || !nativeBatchFailedStepAllowed(out, err) {
			t.Fatal(err)
		}
		if _, e := g.read(); e != nil {
			t.Fatal(e)
		}
		g.f.mu.Lock()
		g.f.down[actor] = true
		g.f.mu.Unlock()
		g.once.Do(func() { close(g.lost) })
	} else {
		group.enqueue(t, out)
	}
	for len(group.queue) > 0 {
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		if group.delivered >= processDeliveries {
			t.Fatal("existing packet ceiling")
		}
		p := group.queue[0]
		group.queue[0] = replica.Packet{}
		group.queue = group.queue[1:]
		group.queuedBytes -= cap(p.Payload) + 64
		group.delivered++
		if group.blocked[p.From] || group.blocked[p.To] || group.dropPacket != nil && group.dropPacket(p) {
			continue
		}
		if g != nil {
			g.f.mu.Lock()
			dead := g.f.down[p.From] || g.f.down[p.To]
			g.f.mu.Unlock()
			if dead {
				continue
			}
		}
		response, err := group.nodes[p.To].exchange(ctx, processRequest{op: processStep, packet: p})
		if err = errors.Join(err, processResponseError(response)); err != nil {
			if g != nil && nativeBatchFailedStepAllowed(response, err) && g.failed(graphProcessPacketJob{origin: p.From, packet: p}, err) {
				continue
			}
			t.Fatal("actual selected P08 Step", err)
		}
		group.enqueue(t, response)
	}
}
