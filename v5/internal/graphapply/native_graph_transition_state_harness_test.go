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
	"sync"
	"syscall"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// The already-owned NSB1 pipe route admits this exact fresh nested prefix.
// The ordinary NSB1 JSON schema, bytes, controls and grants remain unchanged.
const nativeStateMarker = nativeBatchMarker + "NSS1"
const nativeStateProfile = "native-actual-state-sync-process-v1"
const nativeStateReceiptBytes = 16 << 10

// A filter selects diagnostic work only. Every nonzero value is matched; the
// actual complete identity always comes back from the native Store, by value.
// Target is explicit because bank0 is meaningful. No filter can create an
// import handle, activate a manifest or issue publication/collection authority.
type nativeStateFilter struct {
	Applied, SourceIndex, Sequence, TargetGeneration uint64
	SourceCutID, ManifestID, PlanID                  [32]byte
	TargetBank                                       uint8
	MatchSequence, MatchTarget                       bool
}

func (f nativeStateFilter) matches(id raftlog.StableStateIdentity) bool {
	return (f.Applied == 0 || f.Applied == id.Applied) &&
		(f.SourceIndex == 0 || f.SourceIndex == id.SourceIndex) &&
		(f.SourceCutID == ([32]byte{}) || f.SourceCutID == id.SourceCutID) &&
		(f.ManifestID == ([32]byte{}) || f.ManifestID == id.ManifestID) &&
		(f.PlanID == ([32]byte{}) || f.PlanID == id.PlanID) &&
		(!f.MatchSequence || f.Sequence == id.Sequence) &&
		(!f.MatchTarget || f.TargetBank == id.TargetBank && f.TargetGeneration == id.TargetGeneration)
}

func (f nativeStateFilter) valid(kind raftlog.StableStateKind) bool {
	if !f.MatchSequence && f.Sequence != 0 || !f.MatchTarget && (f.TargetBank != 0 || f.TargetGeneration != 0) || f.TargetBank > 1 {
		return false
	}
	switch kind {
	case raftlog.StableStateCheckpoint, raftlog.StableStatePublication:
		return f.Applied > 0 && f.ManifestID == ([32]byte{}) && f.PlanID == ([32]byte{}) && !f.MatchTarget && !f.MatchSequence
	case raftlog.StableStateImportBegin:
		return f.SourceIndex > 0 && f.SourceCutID != ([32]byte{}) && f.PlanID == ([32]byte{}) && !f.MatchTarget && !f.MatchSequence
	case raftlog.StableStateImportAppend, raftlog.StableStateImportSeal, raftlog.StableStateActivation, raftlog.StableStateImportRetire:
		return f.SourceIndex > 0 && f.SourceCutID != ([32]byte{}) && f.ManifestID != ([32]byte{}) && f.PlanID == ([32]byte{})
	case raftlog.StableStateRetentionPage:
		return f.SourceIndex > 0 && f.PlanID != ([32]byte{}) && f.ManifestID == ([32]byte{}) && f.SourceCutID == ([32]byte{})
	case raftlog.StableStateGenerationClear:
		return f.MatchTarget && f.TargetGeneration > 0 && f.ManifestID == ([32]byte{}) && f.PlanID == ([32]byte{}) && f.SourceCutID == ([32]byte{}) && f.SourceIndex == 0
	case raftlog.StableStateDormantClear:
		return f.MatchTarget && f.ManifestID == ([32]byte{}) && f.PlanID == ([32]byte{}) && f.SourceCutID == ([32]byte{}) && f.SourceIndex == 0
	default:
		return false // Genesis Initialize is not admitted by this import composition.
	}
}

type nativeStateControl struct {
	Version  uint8
	Action   string
	Cookie   [16]byte
	Sequence uint64
	Kind     raftlog.StableStateKind
	Side     string
	Filter   nativeStateFilter
}

type nativeStateReceipt struct {
	Version                                                            uint8
	Profile                                                            string
	Cookie, Run                                                        [16]byte
	Member, PID                                                        uint64
	Sequence                                                           uint64
	Control                                                            nativeStateControl
	State                                                              string
	DeadlineUnixNanos                                                  int64
	Attempts, WorkBytes, WorkEntries, ObservedOutput, SerializedOutput uint64
	Count                                                              uint8
	Events                                                             [2]raftlog.StableStateObservation
}

type nativeStateOwner struct {
	mu                 sync.Mutex
	closeOnce          sync.Once
	ctx                context.Context
	cancel             context.CancelFunc
	dir                string
	controls, attempts uint64
	last               nativeStateControl
	selection          raftlog.StableStateSelection
	grant              raftlog.StableStorageWork
	state              nativeStateReceipt
}

// This wrapper exists ONLY in the fresh state profile. Ordinary nativeBatchFS
// does not accidentally acquire a state-observer capability at raftlog.Open.
type nativeStateFS struct {
	*nativeBatchFS
	state *nativeStateOwner
}

func (f *nativeStateFS) StableStateSelection(k raftlog.StableStateKind, id raftlog.StableStateIdentity) (raftlog.StableStateSelection, bool) {
	return f.state.StableStateSelection(k, id)
}
func (f *nativeStateFS) ReserveStableStateObservation(s raftlog.StableStateSelection, w raftlog.StableStorageWork) bool {
	return f.state.ReserveStableStateObservation(s, w)
}
func (f *nativeStateFS) ObserveStableState(e raftlog.StableStateObservation) {
	f.state.ObserveStableState(e)
}

func newNativeStateOwner(parent *nativeBatchOwner) (*nativeStateOwner, error) {
	if parent == nil || parent.ctx.Err() != nil {
		return nil, errInvalid
	}
	ctx, cancel := context.WithCancel(parent.ctx) // SAME original absolute deadline.
	o := &nativeStateOwner{ctx: ctx, cancel: cancel, dir: parent.dir, state: nativeStateReceipt{Version: 1, Profile: nativeStateProfile, Run: parent.state.Run, Member: parent.state.Member, PID: parent.state.PID, DeadlineUnixNanos: parent.state.DeadlineUnixNanos, State: "idle"}}
	if _, err := io.ReadFull(rand.Reader, o.state.Cookie[:]); err != nil {
		cancel()
		return nil, err
	}
	context.AfterFunc(ctx, o.close)
	return o, nil
}

func decodeNativeStateControl(wire []byte) (nativeStateControl, error) {
	var c nativeStateControl
	if !bytes.HasPrefix(wire, []byte(nativeStateMarker)) || len(wire) > 4096 {
		return c, errInvalid
	}
	wire = wire[len(nativeStateMarker):]
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

func (o *nativeStateOwner) control(wire []byte) ([]byte, error) {
	c, err := decodeNativeStateControl(wire)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.controls >= 128 || o.state.SerializedOutput > (4<<20)-nativeStateReceiptBytes {
		return nil, errLimit
	}
	o.controls++
	o.state.SerializedOutput += nativeStateReceiptBytes
	if c != (nativeStateControl{Version: 1, Action: "inspect"}) {
		if c.Cookie != o.state.Cookie || c.Cookie == ([16]byte{}) || o.ctx.Err() != nil || o.state.State == "closed" {
			return nil, errors.Join(replica.ErrUnavailable, o.ctx.Err())
		}
		switch c.Action {
		case "arm":
			if c == o.last {
				break // Exact replay never resets work, identity or sequence.
			}
			if o.state.State == "armed" || o.state.State == "observing" || o.state.Sequence >= 16 || c.Sequence != o.state.Sequence+1 || !c.Filter.valid(c.Kind) || c.Side != "before" && c.Side != "after" && c.Side != "observe" || o.state.SerializedOutput > (4<<20)-nativeStateReceiptBytes {
				return nil, errInvalid
			}
			o.state.SerializedOutput += nativeStateReceiptBytes // Full receipt grant, never refunded.
			o.last, o.state.Control, o.state.Sequence, o.state.State = c, c, c.Sequence, "armed"
			o.state.Count, o.state.Events, o.attempts = 0, [2]raftlog.StableStateObservation{}, 0
			o.selection, o.grant = raftlog.StableStateSelection{}, raftlog.StableStorageWork{}
		case "disarm":
			if c != (nativeStateControl{Version: 1, Action: "disarm", Cookie: o.state.Cookie, Sequence: o.state.Sequence}) || o.state.State == "observing" {
				return nil, errInvalid
			}
			o.state.State = "unavailable"
		default:
			return nil, errInvalid
		}
	}
	body, err := json.Marshal(o.state)
	if err != nil || len(body) >= nativeStateReceiptBytes {
		return nil, errors.Join(errLimit, err)
	}
	return body, nil
}

func (o *nativeStateOwner) StableStateSelection(kind raftlog.StableStateKind, id raftlog.StableStateIdentity) (raftlog.StableStateSelection, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.ctx.Err() != nil || o.state.State != "armed" || kind != o.last.Kind || !o.last.Filter.matches(id) {
		return raftlog.StableStateSelection{}, false
	}
	o.selection = raftlog.StableStateSelection{Cookie: o.state.Cookie, Sequence: o.state.Sequence, Identity: id}
	return o.selection, true
}

func (o *nativeStateOwner) ReserveStableStateObservation(s raftlog.StableStateSelection, w raftlog.StableStorageWork) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.ctx.Err() != nil || o.state.State != "armed" || s != o.selection || s.Cookie == ([16]byte{}) {
		return false
	}
	if o.attempts >= 128 || w.Entries != 3 || w.Bytes == 0 || w.Bytes > (1<<42)-o.state.WorkBytes || w.Entries > (1<<20)-o.state.WorkEntries || w.OutputBytes != 2048 || w.OutputBytes > (8<<20)-o.state.ObservedOutput {
		o.state.State = "unavailable"
		return false
	}
	o.attempts++
	o.state.Attempts++
	o.state.WorkBytes += w.Bytes
	o.state.WorkEntries += w.Entries
	o.state.ObservedOutput += w.OutputBytes
	o.grant = w
	return true
}

func (o *nativeStateOwner) ObserveStableState(e raftlog.StableStateObservation) {
	o.mu.Lock()
	if o.ctx.Err() != nil || o.state.State != "armed" {
		o.mu.Unlock()
		return
	}
	if e.Selection != o.selection || e.Work != o.grant || !nativeStateEventValid(e, o.last) {
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
	if o.last.Side == "observe" && e.Stage == 2 {
		o.state.State = "observed" // Same-live diagnostic only; no handle/authority.
		o.mu.Unlock()
		return
	}
	if o.last.Side != "before" && e.Stage == 1 {
		o.mu.Unlock()
		return
	}
	o.state.State = "observing"
	r := o.state // No native slices, buffers, proof/claim or mutable identity.
	o.mu.Unlock()
	if o.ctx.Err() != nil {
		return
	}
	if err := nativeStatePersist(o.dir, r); err != nil {
		o.mu.Lock()
		if o.ctx.Err() == nil {
			o.state.State = "unavailable"
		}
		o.mu.Unlock()
		return // Native persistence must continue on diagnostic failure.
	}
	if o.ctx.Err() == nil {
		if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
			o.mu.Lock()
			if o.ctx.Err() == nil {
				o.state.State = "unavailable"
			}
			o.mu.Unlock()
		}
	}
}

func nativeStateReceiptPath(dir string, cookie [16]byte, sequence uint64) string {
	return filepath.Join(dir, "native-state-"+hex.EncodeToString(cookie[:])+"-"+nativeFaultSequence(sequence)+".json")
}

func nativeStatePersist(dir string, r nativeStateReceipt) error {
	body, err := json.Marshal(r)
	if err != nil || len(body)+1 > nativeStateReceiptBytes || r.State != "observing" {
		return errors.Join(errInvalid, err)
	}
	f, err := os.OpenFile(nativeStateReceiptPath(dir, r.Cookie, r.Sequence), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
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

func (o *nativeStateOwner) close() {
	o.closeOnce.Do(func() {
		o.cancel()
		o.mu.Lock()
		o.state.State = "closed"
		o.mu.Unlock()
	})
}

func nativeStateControlWire(c nativeStateControl) ([]byte, error) {
	body, err := json.Marshal(c)
	return append([]byte(nativeStateMarker), body...), err
}

func nativeStateEventValid(e raftlog.StableStateObservation, c nativeStateControl) bool {
	return e.Selection.Cookie == c.Cookie && e.Selection.Sequence == c.Sequence && c.Filter.valid(c.Kind) && c.Filter.matches(e.Selection.Identity) && e.Kind == c.Kind && e.BeforeMetadataBytes > 0 && e.BeforeMetadataBytes <= 65536 && e.MetadataBytes > 0 && e.MetadataBytes <= 65536 && e.BeforeMetadataSHA256 != ([32]byte{}) && e.MetadataSHA256 != ([32]byte{}) && e.BatchBytes >= 12 && e.BatchBytes <= 1<<42 && e.BatchPayloadSHA256 != ([32]byte{}) && e.Work.Entries == 3 && e.Work.Bytes >= e.BatchBytes && e.Work.Bytes <= 1<<42 && e.Work.OutputBytes == 2048
}

func nativeStateReceiptValid(r, expected nativeStateReceipt) bool {
	if r.Version != 1 || r.Profile != nativeStateProfile || r.Cookie == ([16]byte{}) || r.Cookie != expected.Cookie || r.Run == ([16]byte{}) || r.Run != expected.Run || r.Member != expected.Member || r.PID != expected.PID || r.Sequence != expected.Sequence || r.Control != expected.Control || r.DeadlineUnixNanos != expected.DeadlineUnixNanos || r.Attempts < 1 || r.Attempts > 16*128 || r.WorkBytes == 0 || r.WorkBytes > 1<<42 || r.WorkEntries == 0 || r.WorkEntries > 1<<20 || r.ObservedOutput < 2048 || r.ObservedOutput > 8<<20 || r.SerializedOutput < 2*nativeStateReceiptBytes || r.SerializedOutput > 4<<20 {
		return false
	}
	count := uint8(2)
	state := "observing"
	if r.Control.Side == "before" {
		count = 1
	} else if r.Control.Side == "observe" {
		state = "observed"
	} else if r.Control.Side != "after" {
		return false
	}
	if r.State != state || r.Count != count {
		return false
	}
	first := r.Events[0]
	if !nativeStateEventValid(first, r.Control) || first.Stage != 1 || first.Synced || first.Work.Bytes > r.WorkBytes || first.Work.Entries > r.WorkEntries {
		return false
	}
	if count == 1 {
		return r.Events[1] == (raftlog.StableStateObservation{})
	}
	last := r.Events[1]
	if !last.Synced || last.Stage != 2 {
		return false
	}
	last.Stage, last.Synced = 1, false
	return last == first
}

func nativeStateRead(dir string, expected nativeStateReceipt) (nativeStateReceipt, error) {
	var r nativeStateReceipt
	f, err := os.Open(nativeStateReceiptPath(dir, expected.Cookie, expected.Sequence))
	if err != nil {
		return r, err
	}
	body, readErr := io.ReadAll(io.LimitReader(f, nativeStateReceiptBytes+1))
	if err := errors.Join(readErr, f.Close()); err != nil || len(body) > nativeStateReceiptBytes {
		return r, errors.Join(errLimit, err)
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return r, errors.Join(errCorrupt, err)
	}
	canonical, err := json.Marshal(r)
	if err != nil || !bytes.Equal(append(canonical, '\n'), body) || !nativeStateReceiptValid(r, expected) || r.Control.Side == "observe" {
		return r, errors.Join(errCorrupt, err)
	}
	return r, nil
}

func nativeStateCall(ctx context.Context, p *graphProcess, c nativeStateControl) (nativeStateReceipt, error) {
	var r nativeStateReceipt
	wire, err := nativeStateControlWire(c)
	if err != nil {
		return r, err
	}
	out, transport := p.exchange(ctx, processRequest{op: processNativeFaultControl, data: wire})
	if err := errors.Join(transport, processResponseError(out)); err != nil {
		return r, err
	}
	if len(out.aux) >= nativeStateReceiptBytes {
		return r, errLimit
	}
	d := json.NewDecoder(bytes.NewReader(out.aux))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil || r.Version != 1 || r.Profile != nativeStateProfile || r.Cookie == ([16]byte{}) || r.PID != uint64(p.cmd.Process.Pid) {
		return r, errors.Join(errCorrupt, err)
	}
	canonical, err := json.Marshal(r)
	if err != nil || !bytes.Equal(canonical, out.aux) {
		return r, errors.Join(errCorrupt, err)
	}
	return r, nil
}
