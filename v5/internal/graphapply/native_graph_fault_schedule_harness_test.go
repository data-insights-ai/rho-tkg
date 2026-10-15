package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

const nativeFaultProfile = "native-composed-graph-faults-v1"

// These process controls are embedding diagnostics, never peer/source or graph
// authority. The live pipe incarnation authenticates the controller; its private
// cookie and sequence also prevent an old arm from targeting a reopened owner.
type nativeFaultControl struct {
	Version  uint8
	Action   string
	Cookie   [16]byte
	Sequence uint64
	Kind     string
	Nth      uint64
}

type nativeFaultIO struct {
	Kind, Path    string
	Directory     bool
	Requested     uint64
	Written       uint64
	InputSHA256   [32]byte
	WrittenSHA256 [32]byte
	Error         string
	Destination   string `json:",omitempty"`
	Offset        int64  `json:",omitempty"`
}

type nativeFaultReceipt struct {
	Version        uint8
	Profile        string
	Cookie         [16]byte
	Member, PID    uint64
	Sequence, Arms uint64
	Kind           string
	Nth, Seen      uint64
	Armed, Matched bool
	IO             nativeFaultIO
}

// Only one arm/receipt is live. The receipt is durably written through the real
// OS outside the injected FS before a fatal Pebble error can reap this child.
// It states the actual operation and errno, not whether a proposal committed.
type nativeFaultFS struct {
	vfs.FS
	mu      sync.Mutex
	dir     string
	state   nativeFaultReceipt
	last    nativeFaultControl
	pending bool
}

func newNativeFaultFS(dir string, member uint64) (*nativeFaultFS, error) {
	if dir == "" || member < 1 || member > 6 {
		return nil, errInvalid
	}
	root, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	f := &nativeFaultFS{FS: vfs.Default, dir: root, state: nativeFaultReceipt{Version: 1, Profile: nativeFaultProfile, Member: member, PID: uint64(os.Getpid())}}
	if _, err := io.ReadFull(rand.Reader, f.state.Cookie[:]); err != nil {
		return nil, err
	}
	return f, nil
}

func decodeNativeFaultControl(raw []byte) (nativeFaultControl, error) {
	var c nativeFaultControl
	if len(raw) == 0 || len(raw) > 4096 {
		return c, errInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, errors.Join(errInvalid, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return c, errors.Join(errInvalid, err)
	}
	canonical, err := json.Marshal(c)
	if err != nil || !bytes.Equal(canonical, raw) || c.Version != 1 {
		return c, errors.Join(errInvalid, err)
	}
	return c, nil
}

func (f *nativeFaultFS) control(raw []byte) ([]byte, error) {
	if f == nil {
		return nil, errInvalid
	}
	c, err := decodeNativeFaultControl(raw)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if c.Action == "inspect" {
		if c.Cookie != ([16]byte{}) && c.Cookie != f.state.Cookie || c.Sequence != 0 || c.Kind != "" || c.Nth != 0 {
			return nil, errInvalid
		}
		return json.Marshal(f.state)
	}
	if c.Cookie != f.state.Cookie || c.Cookie == ([16]byte{}) || f.pending {
		return nil, errInvalid
	}
	if c.Action == "disarm" {
		if c.Sequence != f.state.Sequence || c.Kind != "" || c.Nth != 0 {
			return nil, errInvalid
		}
		f.state.Armed = false
		return json.Marshal(f.state)
	}
	if c.Action != "arm" || c.Sequence == 0 || c.Nth < 1 || c.Nth > 128 || !nativeFaultKind(c.Kind) {
		return nil, errInvalid
	}
	if c == f.last {
		return json.Marshal(f.state) // Exact last arm replay never re-arms it.
	}
	if f.state.Armed || c.Sequence != f.state.Sequence+1 || f.state.Arms >= 16 {
		return nil, errLimit
	}
	f.last = c
	f.state.Sequence, f.state.Kind, f.state.Nth = c.Sequence, c.Kind, c.Nth
	f.state.Arms++
	f.state.Seen, f.state.Armed, f.state.Matched, f.state.IO = 0, true, false, nativeFaultIO{}
	return json.Marshal(f.state)
}

func nativeFaultKind(kind string) bool {
	switch kind {
	case "short-write", "write-eio", "write-enospc", "file-sync", "directory-sync", "torn-wal-kill", "options-rename":
		return true
	default:
		return false
	}
}

func (f *nativeFaultFS) match(path string, directory bool, operation string, input []byte) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.state.Armed {
		return "", false, nil
	}
	wal := strings.HasSuffix(path, ".log")
	matched := operation == "write" && wal && (f.state.Kind == "short-write" || f.state.Kind == "write-eio" || f.state.Kind == "write-enospc" || f.state.Kind == "torn-wal-kill") || operation == "sync" && (directory && f.state.Kind == "directory-sync" || !directory && wal && f.state.Kind == "file-sync") || operation == "rename" && f.state.Kind == "options-rename"
	if !matched {
		return "", false, nil
	}
	rel, err := filepath.Rel(f.dir, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) || len(rel) > 1024 || len(input) > processFrameBytes {
		return "", false, errors.Join(errLimit, err)
	}
	f.state.Seen++
	if f.state.Seen != f.state.Nth {
		return "", false, nil
	}
	f.pending, f.state.Armed, f.state.Matched = true, false, true
	f.state.IO = nativeFaultIO{Kind: operation, Path: rel, Directory: directory, Requested: uint64(len(input)), InputSHA256: sha256.Sum256(input)}
	return f.state.Kind, true, nil
}

func (f *nativeFaultFS) complete(written []byte, operation error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.pending {
		return errInvalid
	}
	f.state.IO.Written, f.state.IO.WrittenSHA256 = uint64(len(written)), sha256.Sum256(written)
	f.state.IO.Error = operation.Error()
	f.pending = false
	body, err := json.Marshal(f.state)
	if err != nil {
		return err
	}
	// A diagnostic file cannot become a graph publication/commit marker. Its
	// exclusive sequence name keeps every matched actual injection reviewable.
	path := nativeFaultIOPath(f.dir, f.state.Cookie, f.state.Sequence)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(append(body, '\n'))
	return errors.Join(writeErr, file.Sync(), file.Close())
}

func nativeFaultIOPath(dir string, cookie [16]byte, sequence uint64) string {
	return filepath.Join(dir, "native-fault-io-"+hex.EncodeToString(cookie[:])+"-"+nativeFaultSequence(sequence)+".json")
}

func nativeFaultSequence(sequence uint64) string {
	const digits = "0123456789abcdef"
	var out [16]byte
	for i := range len(out) {
		out[len(out)-1-i] = digits[sequence&15]
		sequence >>= 4
	}
	return string(out[:])
}

type nativeFaultFile struct {
	vfs.File
	writeMu   sync.Mutex
	offset    int64
	owner     *nativeFaultFS
	path      string
	directory bool
}

func (f *nativeFaultFS) wrap(path string, directory bool, file vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	return &nativeFaultFile{File: file, owner: f, path: path, directory: directory}, nil
}
func (f *nativeFaultFS) Create(path string, category vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.Create(path, category)
	return f.wrap(path, false, file, err)
}
func (f *nativeFaultFS) Unwrap() vfs.FS { return f.FS }
func (f *nativeFaultFS) Open(path string, options ...vfs.OpenOption) (vfs.File, error) {
	file, err := f.FS.Open(path)
	wrapped, err := f.wrap(path, false, file, err)
	if err != nil {
		return nil, err
	}
	for _, option := range options {
		option.Apply(wrapped)
	}
	return wrapped, nil
}
func (f *nativeFaultFS) OpenReadWrite(path string, category vfs.DiskWriteCategory, options ...vfs.OpenOption) (vfs.File, error) {
	file, err := f.FS.OpenReadWrite(path, category)
	wrapped, err := f.wrap(path, false, file, err)
	if err != nil {
		return nil, err
	}
	for _, option := range options {
		option.Apply(wrapped)
	}
	return wrapped, nil
}
func (f *nativeFaultFS) OpenDir(path string) (vfs.File, error) {
	file, err := f.FS.OpenDir(path)
	return f.wrap(path, true, file, err)
}
func (f *nativeFaultFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.ReuseForWrite(oldname, newname, category)
	return f.wrap(newname, false, file, err)
}

func (f *nativeFaultFS) Rename(oldname, newname string) error {
	// Pebble2.1.7 Open publishes OPTIONS by this real rename. Its manifest
	// marker instead uses Create/Close/directory Sync; no fake rename is added.
	if !strings.HasPrefix(filepath.Base(newname), "OPTIONS-") {
		return f.FS.Rename(oldname, newname)
	}
	rel, err := filepath.Rel(f.dir, newname)
	if err != nil || rel == ".." || filepath.IsAbs(rel) || strings.Contains(rel, string(os.PathSeparator)) || len(rel) > 1024 {
		return errors.Join(errInvalid, err)
	}
	_, matched, err := f.match(oldname, false, "rename", nil)
	if err != nil || !matched {
		if err != nil {
			return err
		}
		return f.FS.Rename(oldname, newname)
	}
	f.mu.Lock()
	f.state.IO.Destination = rel
	f.mu.Unlock()
	return errors.Join(syscall.EIO, f.complete(nil, syscall.EIO))
}

func (f *nativeFaultFile) write(input []byte, offset int64, write func([]byte) (int, error)) (int, error) {
	kind, matched, err := f.owner.match(f.path, f.directory, "write", input)
	if err != nil {
		return 0, err
	}
	if !matched {
		return write(input)
	}
	var n int
	switch kind {
	case "torn-wal-kill":
		if len(input) < 2 {
			return 0, errors.Join(errInvalid, f.owner.complete(nil, errInvalid))
		}
		// Real native prefix write followed by a real process SIGKILL before
		// the caller observes a result. This is a declared torn-write schedule,
		// not evidence that the unsynced prefix survived physical power loss.
		n, err = write(input[:len(input)/2])
		f.owner.mu.Lock()
		f.owner.state.IO.Offset = offset
		f.owner.mu.Unlock()
		torn := errors.New("declared native torn WAL prefix before SIGKILL")
		if receiptErr := f.owner.complete(input[:n], errors.Join(torn, err)); receiptErr != nil {
			return n, errors.Join(torn, err, receiptErr)
		}
		return n, errors.Join(torn, err, syscall.Kill(os.Getpid(), syscall.SIGKILL))
	case "short-write":
		if len(input) > 0 {
			n, err = write(input[:len(input)-1])
		}
		err = errors.Join(io.ErrShortWrite, err)
	case "write-eio":
		err = syscall.EIO
	case "write-enospc":
		err = syscall.ENOSPC
	default:
		err = errInvalid
	}
	return n, errors.Join(err, f.owner.complete(input[:n], err))
}
func (f *nativeFaultFile) Write(input []byte) (int, error) {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	n, err := f.write(input, f.offset, f.File.Write)
	f.offset += int64(n)
	return n, err
}
func (f *nativeFaultFile) WriteAt(input []byte, offset int64) (int, error) {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	return f.write(input, offset, func(part []byte) (int, error) { return f.File.WriteAt(part, offset) })
}
func (f *nativeFaultFile) sync(call func() error) error {
	_, matched, err := f.owner.match(f.path, f.directory, "sync", nil)
	if err != nil || !matched {
		if err != nil {
			return err
		}
		return call()
	}
	return errors.Join(syscall.EIO, f.owner.complete(nil, syscall.EIO))
}
func (f *nativeFaultFile) Sync() error     { return f.sync(f.File.Sync) }
func (f *nativeFaultFile) SyncData() error { return f.sync(f.File.SyncData) }
func (f *nativeFaultFile) SyncTo(length int64) (bool, error) {
	full := false
	err := f.sync(func() error {
		var err error
		full, err = f.File.SyncTo(length)
		return err
	})
	return full, err
}

// All delays/holds derive from this fixed owner context. A consumer's cancel
// retires paused traffic; it can never complete a graph operation as success.
func nativeFaultWait(ctx context.Context, released <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-released:
		return nil
	}
}

type nativeFaultPacketEvent struct {
	Action                string
	From, To, Ordinal     uint64
	Bytes                 int
	SHA256                [32]byte
	ConfiguredDelayMillis int
	ObservedDelayNanos    int64
}

type nativeFaultPacketScheduler struct {
	mu        sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	action    string
	from, to  uint64
	nth, seen uint64
	delay     int
	used      bool
	active    bool
	closed    bool
	terminal  error
	held      *graphProcessPacketJob
	matched   chan struct{} // Fresh hold schedule only; nil for existing modes.
	charge    int
	events    []nativeFaultPacketEvent
}

func newNativeFaultPacketScheduler(ctx context.Context, action string, from, to, nth uint64, delay int) (*nativeFaultPacketScheduler, error) {
	if ctx == nil || from < 1 || from > 6 || to < 1 || to > 6 || from == to || (from <= 3) != (to <= 3) || nth < 1 || nth > 128 {
		return nil, errInvalid
	}
	switch action {
	case "drop", "duplicate", "reorder", "partition", "hold":
		if delay != 0 {
			return nil, errInvalid
		}
	case "delay":
		if delay != 1 && delay != 20 && delay != 100 {
			return nil, errInvalid
		}
	default:
		return nil, errInvalid
	}
	owner, cancel := context.WithTimeout(ctx, 30*time.Second)
	schedule := &nativeFaultPacketScheduler{ctx: owner, cancel: cancel, action: action, from: from, to: to, nth: nth, delay: delay}
	if action == "hold" {
		schedule.matched = make(chan struct{})
	}
	return schedule, nil
}

func (s *nativeFaultPacketScheduler) reserve(h *graphProcessHub, cost int) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cost < 64 || cost > processQueueBytes-h.packetBytes-h.snapshotBytes || len(h.packets)+h.scheduledPackets >= processPacketCount {
		return errLimit
	}
	h.packetBytes += cost
	h.scheduledPackets++
	s.charge += cost
	return nil
}

func (s *nativeFaultPacketScheduler) release(h *graphProcessHub) {
	if s.charge == 0 {
		return
	}
	h.mu.Lock()
	h.packetBytes -= s.charge
	h.scheduledPackets--
	h.mu.Unlock()
	s.charge = 0
	h.changed()
}

func (s *nativeFaultPacketScheduler) route(h *graphProcessHub, job graphProcessPacketJob) (jobs [2]graphProcessPacketJob, count int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return jobs, 0, err
	}
	jobs[0], count = job, 1
	if s.held != nil && s.action != "hold" {
		jobs[1], count = *s.held, 2
		s.held = nil
		s.active = true
		// The held frame stays charged through the actual re-delivery. done()
		// releases it after the two ordinary Step calls, including failures.
		return jobs, count, nil
	}
	if job.packet.From != s.from || job.packet.To != s.to || s.used && s.action != "partition" {
		return jobs, count, nil
	}
	s.seen++
	if s.seen < s.nth {
		return jobs, count, nil
	}
	if len(s.events) >= 128 {
		return jobs, 0, errLimit
	}
	event := nativeFaultPacketEvent{Action: s.action, From: job.packet.From, To: job.packet.To, Ordinal: s.seen, Bytes: len(job.packet.Payload), SHA256: sha256.Sum256(job.packet.Payload), ConfiguredDelayMillis: s.delay}
	s.used = true
	switch s.action {
	case "drop", "partition":
		count = 0
	case "duplicate":
		if err := s.reserve(h, len(job.packet.Payload)+64); err != nil {
			return jobs, 0, err
		}
		jobs[1] = job
		jobs[1].packet.Payload = make([]byte, len(job.packet.Payload))
		copy(jobs[1].packet.Payload, job.packet.Payload)
		count = 2
		s.active = true
	case "reorder", "hold":
		if err := s.reserve(h, cap(job.packet.Payload)+64); err != nil {
			return jobs, 0, err
		}
		s.held = new(job)
		count = 0
		if s.action == "hold" {
			close(s.matched)
		}
	case "delay":
		start := time.Now()
		timer := time.NewTimer(time.Duration(s.delay) * time.Millisecond)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return jobs, 0, s.ctx.Err()
		case <-timer.C:
		}
		event.ObservedDelayNanos = time.Since(start).Nanoseconds()
	}
	s.events = append(s.events, event)
	return jobs, count, nil
}

// Explicit release is used only by the fresh D family after a genuine election.
// The retained actual packet remains charged through its ordinary native Step;
// the copied bytes carry no proof/decision authority outside that Driver path.
func (s *nativeFaultPacketScheduler) deliverHeld(h *graphProcessHub) error {
	if s == nil || h == nil {
		return errInvalid
	}
	s.mu.Lock()
	if s.closed || s.action != "hold" || s.held == nil || s.active || s.ctx.Err() != nil {
		s.mu.Unlock()
		return replica.ErrUnavailable
	}
	job := *s.held
	s.held, s.active = nil, true
	h.mu.Lock()
	h.delivered++
	h.inFlight++
	full := h.delivered > h.deliveryLimit
	h.mu.Unlock()
	s.mu.Unlock()
	var err error
	if full {
		err = errLimit
	} else {
		ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
		response, transport := h.peer(job.packet.To).exchange(ctx, processRequest{op: processStep, packet: job.packet})
		err = errors.Join(transport, processResponseError(response))
		if err == nil {
			err = h.enqueue(job.packet.To, response.packets, response.handles...)
		}
		cancel()
	}
	s.mu.Lock()
	s.active = false
	if err == nil && s.ctx.Err() == nil && len(s.events) < 128 {
		s.events = append(s.events, nativeFaultPacketEvent{Action: "release-oldleader", From: job.packet.From, To: job.packet.To, Ordinal: s.seen, Bytes: len(job.packet.Payload), SHA256: sha256.Sum256(job.packet.Payload)})
	} else {
		err = errors.Join(replica.ErrUnavailable, err, s.ctx.Err())
		s.terminal = errors.Join(s.terminal, err)
	}
	s.release(h)
	s.mu.Unlock()
	h.mu.Lock()
	h.inFlight--
	h.mu.Unlock()
	h.changed()
	return err
}

func (s *nativeFaultPacketScheduler) done(h *graphProcessHub) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.action == "hold" && s.active {
		return
	} // Explicit release owns its charge until Step retires.
	s.active = false
	if s.held == nil {
		s.release(h)
	}
}

func (s *nativeFaultPacketScheduler) failed(err error) {
	s.cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.terminal = errors.Join(s.terminal, replica.ErrUnavailable, err)
}

func (s *nativeFaultPacketScheduler) close(h *graphProcessHub) ([]nativeFaultPacketEvent, error) {
	if s == nil || h == nil {
		return nil, errInvalid
	}
	// Cancel before taking the lock held by a configured delay.
	s.cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		if s.terminal == nil && (s.held != nil || !s.used || s.active || len(s.events) == 0) {
			s.terminal = replica.ErrUnavailable
		}
	}
	s.held = nil
	if !s.active {
		s.release(h)
	}
	if s.terminal != nil {
		return nil, s.terminal
	}
	return append([]nativeFaultPacketEvent(nil), s.events...), nil
}

func attachNativeFaultPacketScheduler(h *graphProcessHub, s *nativeFaultPacketScheduler) (func() ([]nativeFaultPacketEvent, error), error) {
	if h == nil || s == nil {
		return nil, errInvalid
	}
	h.mu.Lock()
	if h.nativeFaultSchedule != nil || h.packetBytes != 0 || h.inFlight != 0 || len(h.packets) != 0 {
		h.mu.Unlock()
		return nil, errInvalid
	}
	h.nativeFaultSchedule = s
	h.mu.Unlock()
	// The owner is fully published before expiry can tear down a held frame.
	// Active RPCs use this same canceled context and retain their charge until
	// done() owns their terminal response/cancellation cleanup.
	stop := context.AfterFunc(s.ctx, func() { _, _ = s.close(h) })
	return func() ([]nativeFaultPacketEvent, error) {
		stop()
		events, err := s.close(h)
		h.mu.Lock()
		if h.nativeFaultSchedule == s {
			h.nativeFaultSchedule = nil
		}
		busy := h.scheduledPackets != 0
		h.mu.Unlock()
		if busy {
			err = errors.Join(err, replica.ErrUnavailable)
		}
		return events, err
	}, nil
}

type nativeFaultPartialGate struct {
	mu       sync.Mutex
	actor    uint64
	first    pairedFaultBoundary
	second   pairedFaultBoundary
	entered  chan pairedFaultBoundary
	released chan struct{}
	once     sync.Once
	release  func()
}

func nativeFaultPhase(wire []byte) (crossCommand, bool, error) {
	request, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return crossCommand{}, false, err
	}
	if request.kind != sourceSubmit {
		return crossCommand{}, false, nil
	}
	proposal, err := decodeGraphSourceProposal(request.command)
	if err != nil {
		return crossCommand{}, false, err
	}
	if !isCrossPartitionWire(proposal.wire) {
		return crossCommand{}, false, nil
	}
	command, _, err := decodeCrossCommand(proposal.wire, defaultMaterializerLimits())
	return command, err == nil, err
}

// The gate is entered only after the genuine A service has installed Resolve,
// and before B sees its Resolve. Other certified clients receive an actual
// unavailable transport result while this directed B installation is withheld.
func newNativeFaultPartialGate(ctx context.Context, h *graphProcessHub, actor uint64) (*nativeFaultPartialGate, error) {
	if ctx == nil || h == nil || actor < 1 || actor > 6 {
		return nil, errInvalid
	}
	g := &nativeFaultPartialGate{actor: actor, entered: make(chan pairedFaultBoundary, 1), released: make(chan struct{})}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.beforeRelay != nil || h.afterRelay != nil {
		return nil, errInvalid
	}
	h.afterRelay = func(ctx context.Context, origin, partition uint64, request, reply []byte) error {
		select {
		case <-g.released:
			return nil
		default:
		}
		if origin != actor || partition != 3 {
			return nil
		}
		command, cross, err := nativeFaultPhase(request)
		if err != nil || !cross || command.phase != crossResolve {
			return err
		}
		response, err := decodeGraphWireResponse(reply)
		if err != nil || response.err != nil {
			return errors.Join(err, response.err)
		}
		status, err := decodeCrossStatus(response.body)
		if err != nil || !status.commit || status.phase != crossResolve || status.ns.partition != 3 {
			return errors.Join(errInvalid, err)
		}
		g.mu.Lock()
		defer g.mu.Unlock()
		if len(g.first.Request) != 0 {
			return errInvalid
		}
		g.first = pairedFaultBoundary{Request: bytes.Clone(request), Reply: bytes.Clone(reply)}
		return nil
	}
	h.beforeRelay = func(ctx context.Context, origin, partition uint64, request []byte) error {
		if partition != 8 {
			return nil
		}
		command, cross, err := nativeFaultPhase(request)
		if err != nil || !cross || command.phase != crossResolve {
			return err
		}
		select {
		case <-g.released:
			return nil
		default:
		}
		g.mu.Lock()
		first := g.first
		g.mu.Unlock()
		if len(first.Request) == 0 {
			return errInvalid
		}
		firstCommand, _, err := nativeFaultPhase(first.Request)
		if err != nil || !bytes.Equal(firstCommand.manifest, command.manifest) {
			return errors.Join(errInvalid, err)
		}
		if origin != actor {
			return replica.ErrUnavailable
		}
		g.once.Do(func() {
			g.mu.Lock()
			g.second = pairedFaultBoundary{Request: bytes.Clone(request)}
			g.mu.Unlock()
			g.entered <- pairedFaultBoundary{Request: bytes.Clone(request)}
		})
		return nativeFaultWait(ctx, g.released)
	}
	g.release = sync.OnceFunc(func() {
		close(g.released)
		h.mu.Lock()
		h.beforeRelay, h.afterRelay = nil, nil
		h.mu.Unlock()
	})
	context.AfterFunc(ctx, g.release)
	return g, nil
}

func nativeFaultControlCall(t *testing.T, p *graphProcess, control nativeFaultControl) nativeFaultReceipt {
	t.Helper()
	body, err := json.Marshal(control)
	if err != nil {
		t.Fatal(err)
	}
	r := p.call(t, processRequest{op: processNativeFaultControl, data: body})
	var receipt nativeFaultReceipt
	if err := json.Unmarshal(r.aux, &receipt); err != nil || receipt.Version != 1 || receipt.Profile != nativeFaultProfile || receipt.Cookie == ([16]byte{}) || receipt.PID != uint64(p.cmd.Process.Pid) {
		t.Fatal("actual child fault owner receipt", receipt, err)
	}
	return receipt
}

type nativeFaultPartialRead struct {
	Mode, Classification string
	Request, Response    []byte
}

type nativeFaultArtifact struct {
	Version                               uint8
	Profile, Case                         string
	Original, Manifest, PendingCaller     []byte
	InstalledA, WithheldB                 pairedFaultBoundary
	PartialReads                          []nativeFaultPartialRead
	Before, Retained, Recovered, Reopened p06Observation
	Processes                             []pairedFaultProcess
	From, Through                         [2]uint64
	Records                               []lifecycleRawRecord
	PacketEvents                          []nativeFaultPacketEvent
	IO                                    *nativeFaultReceipt
	Refused                               *p06LocalRecord
	RefusedReplay                         *p06LocalRecord
}

func nativeFaultInstalledResolve(t *testing.T, original allocationProposal, boundary pairedFaultBoundary) (crossStatus, crossManifest) {
	t.Helper()
	proposal := p06SourceProposal(t, boundary.Request)
	command, _, err := decodeCrossCommand(proposal.wire, defaultMaterializerLimits())
	if err != nil || command.phase != crossResolve || command.ns.partition != 3 {
		t.Fatal("actual A Resolve command", err)
	}
	arena, err := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := decodeCrossManifest(command.manifest, defaultMaterializerLimits(), arena)
	if err != nil || len(manifest.parts) != 2 {
		t.Fatal("actual immutable manifest", err)
	}
	p06BindBusiness(t, original, manifest)
	status, err := decodeCrossStatus(p06ReplyBody(t, boundary.Reply))
	if err != nil || status.phase != crossResolve || !status.commit || status.tx != manifest.tx || status.digest != manifest.digest || status.round == 0 {
		t.Fatal("actual installed Resolve receipt", err, status)
	}
	return status, manifest
}

func nativeFaultPartialReadCall(t *testing.T, f *pairedGraphFaultNetwork, actor uint64, mode graphReadMode, tx [16]byte, coordinator uint64) nativeFaultPartialRead {
	t.Helper()
	input, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: mode, transaction: tx, coordinator: coordinator})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	response, transport := f.n.all[actor].exchange(ctx, processRequest{op: processSourceCertifiedOpen, data: input})
	cancel()
	actual := errors.Join(transport, processResponseError(response))
	if !errors.Is(actual, replica.ErrUnavailable) || errors.Is(actual, context.DeadlineExceeded) || len(response.aux) != 0 || response.sequence == 0 || response.incarnation != f.n.all[actor].incarnation {
		t.Fatal("partial original certified read was not an authenticated unavailable reply", actual, response)
	}
	encoded, err := encodeProcessResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	name := "Fresh"
	if mode == graphReadAfter {
		name = "After"
	}
	return nativeFaultPartialRead{Mode: name, Classification: "unavailable", Request: input, Response: encoded}
}

func writeNativeFaultArtifact(t *testing.T, dir string, artifact nativeFaultArtifact) {
	t.Helper()
	path := filepath.Join(dir, "native-composed-fault-"+artifact.Case+".jsonl")
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
	payload := sha256.New()
	rows, output := uint64(0), 0
	write := func(value any, hashed bool) error {
		body, err := json.Marshal(value)
		if err != nil || len(body)+1 > 512<<10 || len(body)+1 > (16<<20)-output {
			return errors.Join(errLimit, err)
		}
		body = append(body, '\n')
		if _, err := file.Write(body); err != nil {
			return err
		}
		if hashed {
			_, _ = payload.Write(body)
		}
		output += len(body)
		return nil
	}
	header := artifact
	header.Records = nil
	if err := write(struct {
		Type     string
		Artifact nativeFaultArtifact
	}{"header", header}, true); err != nil {
		t.Fatal(err)
	}
	for _, record := range artifact.Records {
		if rows >= 512 {
			t.Fatal("native fault prefix cap")
		}
		if err := write(struct {
			Type   string
			Record lifecycleRawRecord
		}{"record", record}, true); err != nil {
			t.Fatal(err)
		}
		rows++
	}
	if err := write(struct {
		Type          string
		Rows          uint64
		Through       [2]uint64
		PayloadSHA256 string
	}{"footer", rows, artifact.Through, hex.EncodeToString(payload.Sum(nil))}, false); err != nil {
		t.Fatal(err)
	}
	closed = true
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("native composed fault genuine source artifact", path)
}

func nativeFaultBeforePhase(ctx context.Context, h *graphProcessHub, actor, partition uint64, phase crossPhase) (<-chan pairedFaultBoundary, func(), error) {
	if ctx == nil || h == nil || actor < 1 || actor > 6 || partition != 3 && partition != 8 {
		return nil, nil, errInvalid
	}
	entered, released := make(chan pairedFaultBoundary, 1), make(chan struct{})
	var once sync.Once
	h.mu.Lock()
	if h.beforeRelay != nil || h.afterRelay != nil {
		h.mu.Unlock()
		return nil, nil, errInvalid
	}
	h.beforeRelay = func(ctx context.Context, origin, part uint64, request []byte) error {
		if origin != actor || part != partition {
			return nil
		}
		command, cross, err := nativeFaultPhase(request)
		if err != nil || !cross || command.phase != phase {
			return err
		}
		once.Do(func() { entered <- pairedFaultBoundary{Request: bytes.Clone(request)} })
		return nativeFaultWait(ctx, released)
	}
	h.mu.Unlock()
	release := sync.OnceFunc(func() { close(released); h.mu.Lock(); h.beforeRelay = nil; h.mu.Unlock() })
	context.AfterFunc(ctx, release)
	return entered, release, nil
}

func nativeFaultCaptureManifest(h *graphProcessHub, actor uint64) (func() ([]byte, error), error) {
	if h == nil || actor < 1 || actor > 6 {
		return nil, errInvalid
	}
	var mu sync.Mutex
	var manifest []byte
	var failure error
	h.mu.Lock()
	if h.beforeRelay != nil || h.afterRelay != nil {
		h.mu.Unlock()
		return nil, errInvalid
	}
	h.afterRelay = func(ctx context.Context, origin, partition uint64, request, reply []byte) error {
		if origin != actor {
			return nil
		}
		command, cross, err := nativeFaultPhase(request)
		if err != nil || !cross {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		if len(manifest) == 0 {
			manifest = bytes.Clone(command.manifest)
		} else if !bytes.Equal(manifest, command.manifest) {
			failure = errInvalid
		}
		return failure
	}
	h.mu.Unlock()
	return func() ([]byte, error) {
		h.mu.Lock()
		h.afterRelay = nil
		h.mu.Unlock()
		mu.Lock()
		defer mu.Unlock()
		if len(manifest) == 0 {
			return nil, errInvalid
		}
		return manifest, failure
	}, nil
}

// Wait for the actual native fatal exit before fallback kill. A scheduled IO
// error is not qualified as a native failure if only our timeout/SIGKILL exits.
// This also owns a candidate failing before its first Ready/IPC incarnation.
func nativeFaultReap(t *testing.T, p *graphProcess, member uint64, dir string, receipt nativeFaultReceipt) pairedFaultProcess {
	t.Helper()
	startup := receipt.Cookie == ([16]byte{})
	if p == nil || p.closed || !startup && (receipt.Member != member || receipt.PID != uint64(p.cmd.Process.Pid) || !receipt.Matched || receipt.Armed) {
		t.Fatal("unmatched/foreign native IO failure")
	}
	p.closed = true
	var output processStderr
	drained := make(chan error, 1)
	var drainErr error
	var forced error
	if p.mux == nil {
		go func() { _, err := io.Copy(&output, p.stdout); drained <- err }()
		// StdoutPipe must be drained before Wait closes it. A bounded writer
		// keeps the final native Open failure diagnostic without an OS-pipe
		// reader/Wait race. A fallback kill is cleanup, never qualification.
		select {
		case drainErr = <-drained:
		case <-time.After(10 * time.Second):
			forced = errors.Join(context.DeadlineExceeded, p.cmd.Process.Kill(), p.stdout.Close())
			select {
			case drainErr = <-drained:
			case <-time.After(10 * time.Second):
				drainErr = context.DeadlineExceeded
			}
		}
	}
	// With a mux, its existing readLoop exclusively owns stdout and has
	// already retired the failed service exchange; do not add another reader.
	waited := make(chan error, 1)
	go func() { waited <- p.cmd.Wait() }()
	var waitErr error
	select {
	case waitErr = <-waited:
	case <-time.After(10 * time.Second):
		killErr := p.cmd.Process.Kill()
		select {
		case waitErr = <-waited:
		case <-time.After(10 * time.Second):
			waitErr = context.DeadlineExceeded
		}
		p.closeErr = errors.Join(waitErr, killErr, context.DeadlineExceeded)
		_ = p.stdin.Close()
		_ = p.stdout.Close()
		t.Fatal("native fault child did not exit/reap itself", p.closeErr)
	}
	p.closeErr = errors.Join(waitErr, drainErr, forced, p.stdin.Close(), p.stdout.Close())
	if forced != nil || drainErr != nil {
		t.Fatal("startup diagnostic drain required fallback cleanup", p.closeErr)
	}
	if p.cmd.ProcessState == nil {
		t.Fatal("native failure missing OS process state", p.closeErr)
	}
	status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus)
	torn := receipt.Kind == "torn-wal-kill"
	if !ok || torn && (!status.Signaled() || status.Signal() != syscall.SIGKILL) || !torn && (status.Signaled() || p.cmd.ProcessState.ExitCode() != 1) {
		t.Fatal("native matched IO did not cause actual failure exit", p.closeErr, status)
	}
	if startup {
		if p.mux != nil {
			t.Fatal("startup failure unexpectedly acquired IPC authority")
		}
		receipt = readNativeFaultStartupIO(t, dir, member, uint64(p.cmd.Process.Pid))
	}
	stderr, overflow := p.stderr.state()
	stdout, outputOverflow := output.state()
	if overflow || outputOverflow {
		t.Fatal("native failure diagnostic exceeded bound")
	}
	if !torn && !strings.Contains(stderr+stdout, receipt.IO.Error) {
		t.Fatal("native fatal diagnostic does not bind matched IO error", stderr, stdout, receipt.IO.Error)
	}
	signalNumber := 0
	if torn {
		signalNumber = int(status.Signal())
	}
	raw, err := json.Marshal(struct {
		Member, PID            uint64
		ExitCode               int
		Signaled, Reaped       bool
		Volume, Stderr, Stdout string
		Receipt                nativeFaultReceipt
		SignalNumber           int `json:",omitempty"`
	}{member, uint64(p.cmd.Process.Pid), p.cmd.ProcessState.ExitCode(), status.Signaled(), true, dir, stderr, stdout, receipt, signalNumber})
	if err != nil {
		t.Fatal(err)
	}
	instance := "voter" + nativeFaultSequence(member) + "/pid" + nativeFaultSequence(uint64(p.cmd.Process.Pid)) + "/io-cookie" + hex.EncodeToString(receipt.Cookie[:])
	if p.mux != nil {
		instance = pairedGraphFaultInstance(member, p)
	}
	action := "native-io-failure"
	if torn {
		action = "native-torn-prefix-kill"
	}
	return pairedFaultProcess{Action: action, Role: "replica", Member: member, Instance: instance, Volume: dir, Reaped: true, Raw: raw}
}

func readNativeFaultIO(t *testing.T, dir string, expected nativeFaultReceipt) nativeFaultReceipt {
	t.Helper()
	file, err := os.Open(nativeFaultIOPath(dir, expected.Cookie, expected.Sequence))
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(io.LimitReader(file, 4097))
	if err := errors.Join(readErr, file.Close()); err != nil || len(body) > 4096 {
		t.Fatal("native IO receipt admission", err)
	}
	var actual nativeFaultReceipt
	if err := json.Unmarshal(body, &actual); err != nil || actual.Cookie != expected.Cookie || actual.Member != expected.Member || actual.PID != expected.PID || actual.Sequence != expected.Sequence || actual.Kind != expected.Kind || actual.Nth != expected.Nth || !actual.Matched || actual.Armed || actual.Seen != actual.Nth {
		t.Fatal("actual matched IO receipt mismatch", actual, err)
	}
	return actual
}

func readNativeFaultStartupIO(t *testing.T, dir string, member, pid uint64) nativeFaultReceipt {
	t.Helper()
	directory, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := directory.Close(); err != nil {
			t.Error(err)
		}
	}()
	var found nativeFaultReceipt
	for {
		entries, err := directory.Readdir(32)
		for _, entry := range entries {
			if !entry.Mode().IsRegular() || !strings.HasPrefix(entry.Name(), "native-fault-io-") || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			file, err := os.Open(filepath.Join(dir, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(io.LimitReader(file, 4097))
			if err := errors.Join(readErr, file.Close()); err != nil || len(body) > 4096 {
				t.Fatal("startup IO receipt admission", err)
			}
			var receipt nativeFaultReceipt
			if err := json.Unmarshal(body, &receipt); err != nil {
				t.Fatal(err)
			}
			if receipt.Member != member || receipt.PID != pid {
				continue
			}
			actualDirectory := receipt.Kind == "directory-sync" && receipt.IO.Directory && receipt.IO.Kind == "sync" && receipt.IO.Path == "." && receipt.IO.Destination == ""
			actualRename := receipt.Kind == "options-rename" && !receipt.IO.Directory && receipt.IO.Kind == "rename" && strings.HasPrefix(receipt.IO.Destination, "OPTIONS-") && !strings.ContainsAny(receipt.IO.Destination, "/\\") && receipt.IO.Path != "."
			if found.Cookie != ([16]byte{}) || nativeFaultIOPath(dir, receipt.Cookie, receipt.Sequence) != filepath.Join(dir, entry.Name()) || receipt.Profile != nativeFaultProfile || receipt.Version != 1 || receipt.Sequence != 1 || !receipt.Matched || receipt.Armed || !actualDirectory && !actualRename || receipt.IO.Error != syscall.EIO.Error() {
				t.Fatal("startup receipt not exact native publication operation", receipt)
			}
			found = receipt
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if found.Cookie == ([16]byte{}) {
		t.Fatal("startup arm never matched genuine native publication IO")
	}
	return found
}
