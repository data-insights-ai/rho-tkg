package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativeBatchMarker = "NSB1"
const nativeBatchProfile = "native-exact-sync-batch-transition-v1"
const nativeBatchReceiptBytes = 8192

type nativeBatchControl struct {
	Version       uint8
	Action        string
	Cookie        [16]byte
	Sequence      uint64
	Kind          raftlog.StableStorageKind
	Side          string
	CommandSHA256 [32]byte
}

type nativeBatchReceipt struct {
	Version           uint8
	Profile           string
	Cookie, Run       [16]byte
	Member, PID       uint64
	Sequence          uint64
	Control           nativeBatchControl
	State             string
	DeadlineUnixNanos int64
	Attempts, Work    uint64
	Entries, Output   uint64
	Count             uint8
	Events            [2]raftlog.StableStorageObservation
}

// This owner never receives proposal authority or calls the Store. One fixed
// event pair and one hash are retained, with16 arms/128 controls and128 prepaid
// candidate batches per arm. All cumulative grants are spent before hashing;
// exhaustion simply withholds diagnostic qualification, leaving native IO alone.
type nativeBatchOwner struct {
	mu          sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	dir         string
	controls    uint64
	attempts    uint64
	last        nativeBatchControl
	state       nativeBatchReceipt
	nativeState *nativeStateOwner // Immutable before raftlog.Open/worker publication.
}

type nativeBatchFS struct {
	vfs.FS
	owner *nativeBatchOwner
}

func (f *nativeBatchFS) StableStorageSelection(k raftlog.StableStorageKind) (raftlog.StableStorageSelection, bool) {
	return f.owner.StableStorageSelection(k)
}
func (f *nativeBatchFS) ReserveStableStorageObservation(s raftlog.StableStorageSelection, w raftlog.StableStorageWork) bool {
	return f.owner.ReserveStableStorageObservation(s, w)
}
func (f *nativeBatchFS) ObserveStableStorage(e raftlog.StableStorageObservation) {
	f.owner.ObserveStableStorage(e)
}

func newNativeBatchOwner(ctx context.Context, dir string, member uint64, run [16]byte, deadline time.Time) (*nativeBatchOwner, error) {
	if ctx == nil || dir == "" || member < 1 || member > 6 || run == ([16]byte{}) || !deadline.After(time.Now()) || deadline.Sub(time.Now()) > 1800*time.Second {
		return nil, errInvalid
	}
	root, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	ownerCtx, cancel := context.WithDeadline(ctx, deadline)
	o := &nativeBatchOwner{ctx: ownerCtx, cancel: cancel, dir: root, state: nativeBatchReceipt{Version: 1, Profile: nativeBatchProfile, Run: run, Member: member, PID: uint64(os.Getpid()), State: "idle"}}
	if _, err := io.ReadFull(rand.Reader, o.state.Cookie[:]); err != nil {
		cancel()
		return nil, err
	}
	actual, _ := ownerCtx.Deadline()
	o.state.DeadlineUnixNanos = actual.UnixNano()
	context.AfterFunc(ownerCtx, o.close)
	return o, nil
}

// Installed immutably before raftlog.Open; the ordinary nil branch returns the
// exact existing filesystem and creates no owner, timer, hashes or receipts.
func nativeBatchOpen(ctx context.Context, dir string, member uint64, base vfs.FS) (*nativeBatchOwner, vfs.FS, error) {
	stateProfile := os.Getenv("RHO_GRAPH_STABLE_STATE_PROCESS")
	if stateProfile != "" && stateProfile != "off" && stateProfile != "1" {
		return nil, nil, errInvalid
	}
	switch os.Getenv("RHO_GRAPH_STABLE_STORAGE_PROCESS") {
	case "", "off":
		if stateProfile == "1" {
			return nil, nil, errInvalid
		}
		return nil, base, nil
	case "1":
	default:
		return nil, nil, errInvalid
	}
	raw, err := hex.DecodeString(os.Getenv("RHO_GRAPH_TRANSITION_RUN"))
	if err != nil || len(raw) != 16 || hex.EncodeToString(raw) != os.Getenv("RHO_GRAPH_TRANSITION_RUN") {
		return nil, nil, errors.Join(errInvalid, err)
	}
	var run [16]byte
	copy(run[:], raw)
	nanos, err := strconv.ParseInt(os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE"), 10, 64)
	if err != nil || strconv.FormatInt(nanos, 10) != os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE") {
		return nil, nil, errors.Join(errInvalid, err)
	}
	o, err := newNativeBatchOwner(ctx, dir, member, run, time.Unix(0, nanos))
	if err != nil {
		return nil, nil, err
	}
	if base == nil {
		base = vfs.Default
	}
	fs := &nativeBatchFS{FS: base, owner: o}
	if stateProfile == "1" {
		state, err := newNativeStateOwner(o)
		if err != nil {
			o.close()
			return nil, nil, err
		}
		o.nativeState = state
		return o, &nativeStateFS{nativeBatchFS: fs, state: state}, nil
	}
	return o, fs, nil
}

func nativeBatchControlDecode(wire []byte) (nativeBatchControl, error) {
	var c nativeBatchControl
	if !bytes.HasPrefix(wire, []byte(nativeBatchMarker)) || len(wire) > 4096 {
		return c, errInvalid
	}
	wire = wire[len(nativeBatchMarker):]
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

func (o *nativeBatchOwner) control(wire []byte) ([]byte, error) {
	if o == nil {
		return nil, replica.ErrUnavailable
	}
	if o.nativeState != nil && bytes.HasPrefix(wire, []byte(nativeStateMarker)) {
		return o.nativeState.control(wire)
	}
	c, err := nativeBatchControlDecode(wire)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.controls >= 128 || o.state.Output > (2<<20)-nativeBatchReceiptBytes {
		return nil, errLimit
	}
	o.controls++
	o.state.Output += nativeBatchReceiptBytes // Prepaid reply, including failures.
	if c != (nativeBatchControl{Version: 1, Action: "inspect"}) {
		if c.Cookie == ([16]byte{}) || c.Cookie != o.state.Cookie {
			return nil, replica.ErrUnavailable
		}
		if o.ctx.Err() != nil || o.state.State == "closed" {
			return nil, errors.Join(replica.ErrUnavailable, o.ctx.Err())
		}
		switch c.Action {
		case "arm":
			if c == o.last {
				break // Exact last replay never re-arms or refunds it.
			}
			if o.state.State == "armed" || o.state.State == "observing" || o.state.Sequence >= 16 || c.Sequence != o.state.Sequence+1 || c.Kind != raftlog.StableStorageReady && c.Kind != raftlog.StableStorageApplication || c.Side != "before" && c.Side != "after" || c.CommandSHA256 == ([32]byte{}) || o.state.Output > (2<<20)-nativeBatchReceiptBytes {
				return nil, errInvalid
			}
			o.state.Output += nativeBatchReceiptBytes // Own one durable JSON receipt.
			o.last, o.state.Control, o.state.Sequence, o.state.State = c, c, c.Sequence, "armed"
			o.state.Count, o.state.Events = 0, [2]raftlog.StableStorageObservation{}
			o.attempts = 0
		case "disarm":
			if c != (nativeBatchControl{Version: 1, Action: "disarm", Cookie: o.state.Cookie, Sequence: o.state.Sequence}) || o.state.State == "observing" {
				return nil, errInvalid
			}
			o.state.State = "unavailable" // An unmatched arm cannot pass a cell.
		default:
			return nil, errInvalid
		}
	}
	body, err := json.Marshal(o.state)
	if err != nil || len(body) >= nativeBatchReceiptBytes {
		return nil, errors.Join(errLimit, err)
	}
	return body, nil
}

func (o *nativeBatchOwner) selectionLocked(s raftlog.StableStorageSelection) bool {
	return o.ctx.Err() == nil && o.state.State == "armed" && s == (raftlog.StableStorageSelection{Cookie: o.state.Cookie, Sequence: o.state.Sequence, CommandSHA256: o.last.CommandSHA256})
}

func (o *nativeBatchOwner) StableStorageSelection(k raftlog.StableStorageKind) (raftlog.StableStorageSelection, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	s := raftlog.StableStorageSelection{Cookie: o.state.Cookie, Sequence: o.state.Sequence, CommandSHA256: o.last.CommandSHA256}
	return s, k == o.last.Kind && o.selectionLocked(s)
}

func (o *nativeBatchOwner) ReserveStableStorageObservation(s raftlog.StableStorageSelection, w raftlog.StableStorageWork) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.selectionLocked(s) {
		return false
	}
	// Quotas fund diagnostic traversal only. They allocate no quota-sized arena,
	// raise no native ceiling and make no throughput or whole-family claim.
	if o.attempts >= 128 || w.Entries == 0 || w.Bytes == 0 || w.OutputBytes == 0 || w.Entries > (1<<20)-o.state.Entries || w.Bytes > (1<<40)-o.state.Work || w.OutputBytes > (2<<20)-o.state.Output {
		o.state.State = "unavailable"
		return false
	}
	o.state.Attempts++
	o.attempts++
	o.state.Entries += w.Entries
	o.state.Work += w.Bytes
	o.state.Output += w.OutputBytes
	return true
}

func nativeBatchEventValid(e raftlog.StableStorageObservation, c nativeBatchControl) bool {
	return e.Selection == (raftlog.StableStorageSelection{Cookie: c.Cookie, Sequence: c.Sequence, CommandSHA256: c.CommandSHA256}) && e.Kind == c.Kind && e.Matches == 1 && e.EntryCount >= 1 && e.EntryCount <= 4096 && (e.Kind != raftlog.StableStorageApplication || e.EntryCount == 1) && e.Position >= 1 && e.Position <= e.EntryCount && e.FirstIndex > 0 && e.LastIndex >= e.FirstIndex && e.LastIndex-e.FirstIndex == e.EntryCount-1 && e.Index >= e.FirstIndex && e.Index <= e.LastIndex && e.Index-e.FirstIndex == e.Position-1 && e.Term > 0 && e.EntryType == 0 && e.CommandBytes > 0 && e.CommandBytes <= nativeLogicalTransitionBodyBytes && e.CommandSHA256 == c.CommandSHA256 && e.EntryManifestSHA256 != ([32]byte{}) && e.BatchBytes >= 12 && e.BatchPayloadSHA256 != ([32]byte{}) && e.Work.Entries == e.EntryCount+1 && e.Work.Bytes >= e.BatchBytes && e.Work.OutputBytes == 1024
}

func (o *nativeBatchOwner) ObserveStableStorage(e raftlog.StableStorageObservation) {
	o.mu.Lock()
	if !o.selectionLocked(e.Selection) || !nativeBatchEventValid(e, o.last) || o.state.Attempts == 0 || e.Work.Bytes > o.state.Work || e.Work.Entries > o.state.Entries {
		o.state.State = "unavailable"
		o.mu.Unlock()
		return
	}
	if e.Stage == 1 && !e.Synced && o.state.Count == 0 {
		o.state.Events[0], o.state.Count = e, 1
	} else if e.Stage == 2 && e.Synced && o.state.Count == 1 {
		before := e
		before.Stage, before.Synced = 1, false
		if before != o.state.Events[0] {
			o.state.State = "unavailable"
			o.mu.Unlock()
			return
		}
		o.state.Events[1], o.state.Count = e, 2
	} else {
		o.state.State = "unavailable"
		o.mu.Unlock()
		return
	}
	stop := o.last.Side == "before" && e.Stage == 1 || o.last.Side == "after" && e.Stage == 2
	if !stop {
		o.mu.Unlock()
		return
	}
	o.state.State = "observing"
	receipt := o.state // Value-only owned snapshot; no callback/store re-entry.
	o.mu.Unlock()
	if o.ctx.Err() != nil {
		return
	}
	if err := nativeBatchPersist(o.dir, receipt); err != nil {
		o.mu.Lock()
		o.state.State = "unavailable"
		o.mu.Unlock()
		return // Diagnostic failure must not alter native persistence/outcomes.
	}
	// The real synced diagnostic precedes actual self-SIGKILL. Neither a parent
	// fallback kill nor a plain armed/unmatched selector can qualify this case.
	if o.ctx.Err() != nil {
		return
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		o.mu.Lock()
		o.state.State = "unavailable"
		o.mu.Unlock()
	}
}

func nativeBatchReceiptPath(dir string, cookie [16]byte, sequence uint64) string {
	return filepath.Join(dir, "native-batch-"+hex.EncodeToString(cookie[:])+"-"+nativeFaultSequence(sequence)+".json")
}

func nativeBatchPersist(dir string, receipt nativeBatchReceipt) error {
	body, err := json.Marshal(receipt)
	if err != nil || len(body)+1 > nativeBatchReceiptBytes || receipt.State != "observing" {
		return errors.Join(errInvalid, err)
	}
	f, err := os.OpenFile(nativeBatchReceiptPath(dir, receipt.Cookie, receipt.Sequence), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	body = append(body, '\n')
	n, writeErr := f.Write(body)
	if n != len(body) {
		writeErr = errors.Join(writeErr, io.ErrShortWrite)
	}
	return errors.Join(writeErr, f.Sync(), f.Close())
}

func (o *nativeBatchOwner) close() {
	if o == nil {
		return
	}
	o.cancel()
	o.mu.Lock()
	o.state.State = "closed"
	o.mu.Unlock()
}

func nativeBatchControlWire(c nativeBatchControl) ([]byte, error) {
	body, err := json.Marshal(c)
	return append([]byte(nativeBatchMarker), body...), err
}

func nativeBatchRead(dir string, expected nativeBatchReceipt) (nativeBatchReceipt, error) {
	var r nativeBatchReceipt
	f, err := os.Open(nativeBatchReceiptPath(dir, expected.Cookie, expected.Sequence))
	if err != nil {
		return r, err
	}
	body, readErr := io.ReadAll(io.LimitReader(f, nativeBatchReceiptBytes+1))
	if err := errors.Join(readErr, f.Close()); err != nil || len(body) > nativeBatchReceiptBytes {
		return r, errors.Join(errLimit, err)
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return r, errors.Join(errCorrupt, err)
	}
	canonical, err := json.Marshal(r)
	if err != nil || !bytes.Equal(append(canonical, '\n'), body) || !nativeBatchReceiptValid(r, expected) {
		return r, errors.Join(errCorrupt, err)
	}
	return r, nil
}

func nativeBatchReceiptValid(r, expected nativeBatchReceipt) bool {
	if r.Version != 1 || r.Profile != nativeBatchProfile || r.Cookie == ([16]byte{}) || r.Cookie != expected.Cookie || r.Run == ([16]byte{}) || r.Run != expected.Run || r.Member != expected.Member || r.PID != expected.PID || r.Sequence != expected.Sequence || r.Control != expected.Control || r.State != "observing" || r.DeadlineUnixNanos != expected.DeadlineUnixNanos || r.Attempts < 1 || r.Attempts > 16*128 || r.Work == 0 || r.Work > 1<<40 || r.Entries == 0 || r.Entries > 1<<20 || r.Output < 2*nativeBatchReceiptBytes+1024 || r.Output > 2<<20 || r.Count != 1 && r.Count != 2 {
		return false
	}
	if r.Control.Side == "before" && r.Count != 1 || r.Control.Side == "after" && r.Count != 2 {
		return false
	}
	first := r.Events[0]
	if !nativeBatchEventValid(first, r.Control) || first.Stage != 1 || first.Synced || first.Work.Bytes > r.Work || first.Work.Entries > r.Entries {
		return false
	}
	if r.Count == 1 {
		return r.Events[1] == (raftlog.StableStorageObservation{})
	}
	last := r.Events[1]
	if !last.Synced || last.Stage != 2 {
		return false
	}
	last.Stage, last.Synced = 1, false
	return last == first
}

// Only actual pipe/process loss without ANY correlated native response may use
// the optional failed-Step diagnostic. A returned native code, caller expiry,
// malformed frame or a joined non-transport failure always propagates unchanged.
// Bounded traversal rejects unknown leaves; EOF joined to errLimit is not loss.
func nativeBatchFailedStepAllowed(r processResponse, err error) bool {
	// This cold diagnostic assertion covers every response field, including
	// uncorrelated native code/mask/output and nonnil empty ownership slices.
	// The ordinary nil-hook path never invokes it.
	if err == nil || !reflect.ValueOf(r).IsZero() {
		return false
	}
	var visit func(error, int) bool
	leaves := 0
	visit = func(e error, depth int) bool {
		if e == nil || depth > 8 || leaves >= 8 {
			return false
		}
		if many, ok := e.(interface{ Unwrap() []error }); ok {
			children := many.Unwrap()
			if len(children) == 0 || len(children) > 8 {
				return false
			}
			for _, child := range children {
				if !visit(child, depth+1) {
					return false
				}
			}
			return true
		}
		if one, ok := e.(interface{ Unwrap() error }); ok {
			return visit(one.Unwrap(), depth+1)
		}
		leaves++
		return e == io.EOF || e == io.ErrUnexpectedEOF || e == io.ErrClosedPipe || e == os.ErrClosed || e == syscall.EPIPE
	}
	return visit(err, 0)
}
