package main

// Genuine SDK processing owners inside the already monitored client container.
// Logical source buckets, their owner epochs and four physical FDB roles stay
// distinct. This private bounded pipe carries originals, never commit authority.
import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphTransferRPCBytes = 512 << 10

type graphTransferChildInput struct {
	Binding           graphOwnerBinding
	Authority         graphOwnerAuthority
	Role              string
	DeadlineUnixNanos int64
}
type graphTransferPrepare struct {
	Name     string
	Command  graphCommand
	Metadata lifecycleMetadata
	Claims   []graphOwnerAllocatedID
}
type graphTransferCall struct {
	ID                  uint64
	Client              int
	Method              string
	Request             string                       `json:",omitempty"`
	Grant               string                       `json:",omitempty"`
	Secret, GrantSecret [32]byte                     `json:",omitzero"`
	Count               uint64                       `json:",omitzero"`
	Bucket              uint8                        `json:",omitzero"`
	Session             *graphOwnerAllocationSession `json:",omitempty"`
	Intent              *graphOwnerIntent            `json:",omitempty"`
	Prepare             *graphTransferPrepare        `json:",omitempty"`
	Source, Target      *endpoint                    `json:",omitempty"`
}
type graphTransferReply struct {
	ID                        uint64
	Client                    int
	PID                       int
	Routes                    [2]graphOwnerRoute
	Nanos                     int64
	Callbacks                 int
	Error, Refusal            string
	NativeCode                *int `json:",omitempty"`
	Retryable, MaybeCommitted bool
	Receipt                   *graphOwnerAllocationReceipt `json:",omitempty"`
	Session                   *graphOwnerAllocationSession `json:",omitempty"`
	Intent                    *graphOwnerIntent            `json:",omitempty"`
	Candidate                 *graphOwnerIntent            `json:",omitempty"`
	Proof                     *graphCostParallelProof      `json:",omitempty"`
	Outcome                   *outcome                     `json:",omitempty"`
	Node                      *endpoint                    `json:",omitempty"`
	Edge                      *graphEdge                   `json:",omitempty"`
	Life                      uint64                       `json:",omitzero"`
	Allocated                 []graphOwnerAllocatedID      `json:",omitempty"`
}

func (v graphTransferCall) validate() error {
	if v.ID == 0 || v.Client < 0 || v.Client >= 32 || len(v.Request) > 256 || len(v.Grant) > 256 {
		return errInvalid
	}
	switch v.Method {
	case "describe", "refresh":
	case "reserve":
		if !validName(v.Request) || v.Secret == [32]byte{} || v.Count == 0 || v.Bucket > 1 {
			return errInvalid
		}
	case "session":
		if !validName(v.Request) || !validName(v.Grant) || v.Secret == [32]byte{} || v.GrantSecret == [32]byte{} {
			return errInvalid
		}
	case "install", "close-session":
		if v.Session == nil {
			return errInvalid
		}
	case "next-node", "next-value", "next-edge", "close-cursor":
		if !validName(v.Grant) || v.Method == "next-edge" && (v.Source == nil || v.Target == nil) {
			return errInvalid
		}
	case "prepare":
		if v.Prepare == nil || !validName(v.Prepare.Name) || v.Intent != nil {
			return errInvalid
		}
	case "submit", "hold-submit", "resolve", "original", "stale-submit":
		if v.Intent == nil || v.Prepare != nil || len(v.Intent.Body) == 0 || len(v.Intent.Body) > 32<<10 {
			return errInvalid
		}
	default:
		return errInvalid
	}
	return nil
}
func (r *graphTransferReply) failure(err error) {
	if err == nil {
		return
	}
	r.Error = err.Error()
	if e, ok := errors.AsType[fdb.Error](err); ok {
		r.NativeCode = new(e.Code)
		r.Retryable, r.MaybeCommitted = graphCostParallelError(err)
		r.Refusal = "native"
		return
	}
	switch {
	case errors.Is(err, errStale):
		r.Refusal = "stale"
	case errors.Is(err, errOwnerBusy):
		r.Refusal = "owner-busy"
	case errors.Is(err, errMismatch):
		r.Refusal = "mismatch"
	case errors.Is(err, errAllocationConsumed):
		r.Refusal = "allocation-consumed"
	case errors.Is(err, errHistoryExpired):
		r.Refusal = "expired"
	case errors.Is(err, errLimit):
		r.Refusal = "limit"
	case errors.Is(err, context.Canceled):
		r.Refusal = "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		r.Refusal = "deadline"
	default:
		r.Refusal = "failure"
	}
}
func (r graphTransferReply) cause() error {
	if r.Error == "" {
		return nil
	}
	if r.NativeCode != nil {
		return fdb.Error{Code: *r.NativeCode}
	}
	var target error
	switch r.Refusal {
	case "stale":
		target = errStale
	case "owner-busy":
		target = errOwnerBusy
	case "mismatch":
		target = errMismatch
	case "allocation-consumed":
		target = errAllocationConsumed
	case "expired":
		target = errHistoryExpired
	case "limit":
		target = errLimit
	case "canceled":
		target = context.Canceled
	case "deadline":
		target = context.DeadlineExceeded
	}
	return errors.Join(target, errors.New(r.Error))
}

func transferRPCRead[T any](reader *bufio.Reader) (out T, err error) {
	line, err := reader.ReadSlice('\n')
	if err != nil {
		return out, err
	}
	if len(line) < 2 || len(line) > graphTransferRPCBytes {
		return out, errLimit
	}
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&out); err != nil {
		return out, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return out, errors.Join(errInvalid, err)
	}
	body, err := json.Marshal(out)
	if err != nil || !bytes.Equal(body, line[:len(line)-1]) {
		return out, errors.Join(errMismatch, err)
	}
	return out, nil
}
func transferRPCWrite(writer io.Writer, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(body)+1 > graphTransferRPCBytes {
		return errLimit
	}
	body = append(body, '\n')
	n, err := writer.Write(body)
	if n != len(body) {
		err = errors.Join(err, io.ErrShortWrite)
	}
	return err
}

type graphTransferServer struct {
	ctx              context.Context
	root             *os.Root
	h                *harness
	catalog          *graphOwnerCatalog
	authority        graphOwnerAuthority
	mu               sync.RWMutex // Refresh drains native callers; captured old client stays exact.
	client, captured *graphOwnerClient
	cursorMu         sync.Mutex
	cursors          map[string]*graphOwnerAllocationCursor // <= explicitly admitted64 blocks.
}

func (s *graphTransferServer) perform(v graphTransferCall) (out graphTransferReply, err error) {
	begin := time.Now()
	out.ID, out.Client, out.PID = v.ID, v.Client, os.Getpid()
	defer func() { out.Nanos = time.Since(begin).Nanoseconds(); out.failure(err) }()
	if err = v.validate(); err != nil {
		return out, err
	}
	if v.Method == "refresh" {
		s.mu.Lock()
		defer s.mu.Unlock()
		var current *graphOwnerClient
		current, err = s.catalog.Client(s.ctx, s.authority)
		if err == nil {
			s.client = current
			out.Routes = s.client.routes
		}
		return out, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := s.client
	out.Routes = c.routes
	switch v.Method {
	case "describe":
		return out, nil
	case "reserve":
		r, e := c.ReserveIDs(s.ctx, v.Request, v.Secret, v.Count, v.Bucket)
		out.Receipt = &r
		return out, e
	case "session":
		r, e := c.AllocationSession(s.ctx, v.Grant, v.Request, v.GrantSecret, v.Secret)
		out.Session = &r
		return out, e
	case "install":
		s.cursorMu.Lock()
		defer s.cursorMu.Unlock()
		if len(s.cursors) >= int(s.catalog.binding.Transfer.MaxGrants) {
			return out, errLimit
		}
		cursor, r, e := c.InstallIDs(s.ctx, *v.Session)
		out.Receipt = &r
		if cursor != nil {
			if _, ok := s.cursors[v.Session.Grant]; ok {
				_, closeErr := cursor.Close(s.ctx)
				return out, errors.Join(errMismatch, closeErr)
			}
			s.cursors[v.Session.Grant] = cursor
		}
		return out, e
	case "close-session":
		r, e := c.CloseAllocationSession(s.ctx, *v.Session)
		out.Receipt = &r
		return out, e
	case "next-node", "next-value", "next-edge", "close-cursor":
		s.cursorMu.Lock()
		cursor := s.cursors[v.Grant]
		s.cursorMu.Unlock()
		if cursor == nil {
			return out, errUnavailable
		}
		switch v.Method {
		case "next-node":
			n, ids, e := cursor.Node()
			out.Node = &n
			out.Allocated = ids[:]
			return out, e
		case "next-value":
			id, e := cursor.Next()
			out.Allocated = []graphOwnerAllocatedID{id}
			return out, e
		case "next-edge":
			edge, life, ids, e := cursor.Edge(*v.Source, *v.Target, 5)
			out.Edge = &edge
			out.Life = life
			out.Allocated = ids[:]
			return out, e
		default:
			r, e := cursor.Close(s.ctx)
			out.Receipt = &r
			return out, e
		}
	case "prepare":
		q := v.Prepare
		before, n, e := graphCostParallelRead(s.ctx, s.h.db, func(tr fdb.ReadTransaction) (snapshot, error) { return s.h.graphCostPointSnapshotIn(tr, q.Command) })
		out.Callbacks = n
		if e != nil {
			return out, e
		}
		coordinator := uint8(0)
		if q.Command.Kind == "node-create" {
			coordinator = q.Command.Node.Owner
		}
		x := s.h.graphTransaction(q.Name, coordinator, q.Command, before)
		x.Lifecycle = new(q.Metadata)
		body, e := wire(x)
		if e != nil {
			return out, e
		}
		allocated, e := graphOwnerAllocatedInput(q.Claims)
		if e != nil {
			return out, e
		}
		// Preserve the actual captured immutable SDK input even if Prepare's
		// reply is unknown. Candidate is submitted input, never retained authority.
		out.Candidate = &graphOwnerIntent{Body: bytes.Clone(body), Digest: sha256.Sum256(body), Routes: c.routes, Allocated: allocated}
		r, e := c.PrepareAllocated(s.ctx, body, q.Claims)
		out.Intent = &r
		return out, e
	case "submit", "hold-submit", "resolve", "stale-submit":
		if v.Method == "stale-submit" {
			c = s.captured
		}
		var o outcome
		if v.Method == "resolve" {
			o, out.Callbacks, err = c.ResolveFenced(s.ctx, *v.Intent)
		} else {
			o, out.Callbacks, err = c.Submit(s.ctx, *v.Intent)
		}
		out.Outcome = &o
		if err == nil {
			proof, n, e := c.Original(s.ctx, *v.Intent)
			out.Callbacks += n
			out.Proof = &proof
			err = e
		}
		if v.Method == "hold-submit" && err == nil {
			// Observation is AFTER genuine native Submit+original lookup, outside
			// all retries. It is NOT the correlated business RPC response.
			out.Nanos = time.Since(begin).Nanoseconds()
			if err = feedChildWrite(s.root, fmt.Sprintf("fdb-transfer-lost-%s-%016x.json", s.authority.ID, v.ID), out); err != nil {
				return out, err
			}
			<-s.ctx.Done()
			return out, s.ctx.Err() // Successful fixed schedule SIGKILLs before this.
		}
		return out, err
	case "original":
		r, n, e := c.Original(s.ctx, *v.Intent)
		out.Proof = &r
		out.Callbacks = n
		return out, e
	}
	return out, errInvalid
}

func runGraphTransferChild(db fdb.Database, role string) (err error) {
	if role != "A" && role != "B" {
		return errInvalid
	}
	root, err := os.OpenRoot(os.Getenv("FDB_COST_ARTIFACT"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	input, err := feedChildRead[graphTransferChildInput](root, "fdb-transfer-"+role+"-input.json")
	if err != nil {
		return err
	}
	if input.Role != role || input.Authority.ID != role || input.Binding.validate() != nil || input.Binding.Transfer == nil || input.Binding.Transfer.Version != 2 || input.DeadlineUnixNanos != input.Binding.Transfer.DeadlineUnixNanos {
		return errInvalid
	}
	admission, err := graphTransportFromEnv()
	if err != nil || admission == nil || !graphColumnSame(*admission, *input.Binding.Transfer.Admission) {
		return errors.Join(errMismatch, err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, input.DeadlineUnixNanos))
	defer cancel()
	h, err := newHarnessContext(ctx, db, input.Binding.Config)
	if err != nil {
		return err
	}
	catalog, _, err := graphCostParallelRead(ctx, db, func(tr fdb.ReadTransaction) (*graphOwnerCatalog, error) { return h.ownerCatalogIn(tr) })
	if err != nil || catalog == nil || !graphColumnSame(catalog.binding, input.Binding) {
		return errors.Join(errMismatch, err)
	}
	client, err := catalog.Client(ctx, input.Authority)
	if err != nil {
		return err
	}
	s := &graphTransferServer{ctx: ctx, root: root, h: h, catalog: catalog, authority: input.Authority, client: client, captured: client, cursors: make(map[string]*graphOwnerAllocationCursor)}
	reader := bufio.NewReaderSize(os.Stdin, graphTransferRPCBytes)
	var jobs [32]chan graphTransferCall
	slots := make(chan struct{}, 32)
	var workers sync.WaitGroup
	var output sync.Mutex
	var failed error
	for client := range input.Binding.Config.Cost.Writers {
		jobs[client] = make(chan graphTransferCall, 1)
		workers.Go(func() {
			for v := range jobs[client] {
				reply, _ := s.perform(v)
				output.Lock()
				if failed == nil {
					failed = transferRPCWrite(os.Stdout, reply)
				}
				output.Unlock()
				<-slots
			}
		})
	}
	last := uint64(0)
	for {
		v, e := transferRPCRead[graphTransferCall](reader)
		if errors.Is(e, io.EOF) {
			break
		}
		if e != nil {
			err = e
			break
		}
		if v.ID <= last || v.validate() != nil || v.Client >= input.Binding.Config.Cost.Writers {
			err = errInvalid
			break
		}
		last = v.ID
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			err = ctx.Err()
		}
		if err == nil {
			select {
			case jobs[v.Client] <- v:
			case <-ctx.Done():
				<-slots
				err = ctx.Err()
			}
		}
		if err != nil {
			break
		}
	}
	for _, queue := range jobs {
		if queue != nil {
			close(queue)
		}
	}
	workers.Wait()
	// Ordinary EOF closes the genuine local cursors; SIGKILL cannot reach this
	// branch. Replacement cleanup uses only previously persisted session IDs.
	for _, cursor := range s.cursors {
		_, e := cursor.Close(ctx)
		err = errors.Join(err, e)
	}
	return errors.Join(err, failed)
}

// At most32 correlated calls, with one genuine worker per declared caller in
// the child. No dataset-sized retry registry or per-entity pipe is retained.
type graphTransferPending struct {
	Call   graphTransferCall
	answer chan graphTransferReply
}
type graphTransferProcess struct {
	ctx            context.Context
	command        *exec.Cmd
	stdin          io.WriteCloser
	stdout         io.ReadCloser
	stderr         *os.File
	log            *graphFeedBoundedOutput
	role           string
	mu             sync.Mutex
	writeMu        sync.Mutex
	pending        map[uint64]*graphTransferPending
	window         chan struct{}
	last           uint64
	readDone       chan struct{}
	failure        error
	closed, waited bool
}

func startGraphTransferProcess(ctx context.Context, root *os.Root, role, incarnation string) (*graphTransferProcess, error) {
	if ctx == nil || root == nil || role != "A" && role != "B" || !validName(incarnation) {
		return nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	output, _, err := openGraphCostArtifact(root, "fdb-transfer-"+role+"-"+incarnation+".stderr")
	if err != nil {
		return nil, err
	}
	command := exec.CommandContext(ctx, executable, "transfer-owner-"+role)
	command.WaitDelay = 3 * time.Second
	log := &graphFeedBoundedOutput{file: output}
	command.Stderr = log
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, errors.Join(err, output.Close())
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return nil, errors.Join(err, stdin.Close(), output.Close())
	}
	if err = command.Start(); err != nil {
		return nil, errors.Join(err, stdin.Close(), stdout.Close(), output.Close())
	}
	p := &graphTransferProcess{ctx: ctx, command: command, stdin: stdin, stdout: stdout, stderr: output, log: log, role: role, pending: make(map[uint64]*graphTransferPending), window: make(chan struct{}, 32), readDone: make(chan struct{})}
	go p.read(stdout)
	return p, nil
}
func (p *graphTransferProcess) read(stdout io.ReadCloser) {
	defer close(p.readDone)
	defer stdout.Close()
	reader := bufio.NewReaderSize(stdout, graphTransferRPCBytes)
	for {
		r, err := transferRPCRead[graphTransferReply](reader)
		p.mu.Lock()
		if err != nil {
			if !errors.Is(err, io.EOF) || len(p.pending) != 0 {
				p.failure = errors.Join(errUnknown, err)
			}
			for id, pending := range p.pending {
				failure := graphTransferReply{ID: id, Client: pending.Call.Client, PID: p.command.Process.Pid, Error: "owner SDK process reply unavailable", Refusal: "reply-unavailable"}
				pending.answer <- failure
				delete(p.pending, id)
			}
			p.closed = true
			p.mu.Unlock()
			return
		}
		pending := p.pending[r.ID]
		if pending == nil || r.PID != p.command.Process.Pid || r.Client != pending.Call.Client || r.Nanos < 1 {
			p.failure = errMismatch
			p.mu.Unlock()
			_ = p.command.Process.Kill()
			continue
		}
		delete(p.pending, r.ID)
		pending.answer <- r
		p.mu.Unlock()
	}
}
func (p *graphTransferProcess) begin(v graphTransferCall) (*graphTransferPending, error) {
	if p == nil {
		return nil, errInvalid
	}
	if err := p.ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case p.window <- struct{}{}:
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	}
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	p.mu.Lock()
	if p.closed || p.last == math.MaxUint64 {
		err := errors.Join(errUnavailable, p.failure)
		p.mu.Unlock()
		<-p.window
		return nil, err
	}
	p.last++
	v.ID = p.last
	if err := v.validate(); err != nil {
		p.mu.Unlock()
		<-p.window
		return nil, err
	}
	pending := &graphTransferPending{Call: v, answer: make(chan graphTransferReply, 1)}
	p.pending[v.ID] = pending
	p.mu.Unlock()
	if err := transferRPCWrite(p.stdin, v); err != nil {
		// The pipe may have delivered a prefix or the whole request. Preserve
		// the SAME intent; a broken pipe never grants native ABORT authority.
		_ = p.command.Process.Kill()
		return pending, errors.Join(errUnknown, err)
	}
	return pending, nil
}
func (p *graphTransferProcess) await(pending *graphTransferPending) (graphTransferReply, error) {
	if p == nil || pending == nil {
		return graphTransferReply{}, errInvalid
	}
	defer func() { <-p.window }()
	select {
	case r := <-pending.answer:
		if r.Refusal == "reply-unavailable" {
			return r, errors.Join(errUnknown, errors.New(r.Error))
		}
		return r, r.cause()
	case <-p.ctx.Done():
		return graphTransferReply{ID: pending.Call.ID, Client: pending.Call.Client}, errors.Join(errUnknown, p.ctx.Err())
	}
}

type graphTransferProcessExit struct {
	Role   string
	PID    int
	Signal string
	Exit   int
	Reaped bool
}

func (p *graphTransferProcess) finish(kill bool) (out graphTransferProcessExit, err error) {
	if p == nil {
		return out, errInvalid
	}
	out.Role, out.PID = p.role, p.command.Process.Pid
	if p.waited {
		return out, errStale
	}
	// Exactly one Wait owner, including a failed bounded teardown. A later
	// caller must not race a still-finishing Wait or claim an unobserved reap.
	p.waited = true
	if kill {
		if err = p.command.Process.Signal(syscall.SIGKILL); err != nil {
			// Still own Wait/pipe cleanup when a signal races process exit.
			err = errors.Join(errUnavailable, err)
		}
	}
	_ = p.stdin.Close()
	// Local teardown only; this neither renews the original run deadline nor
	// creates any native owner/cleanup authority. Failure always retains evidence.
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case <-p.readDone:
	case <-timer.C:
		_ = p.command.Process.Kill()
		_ = p.stdout.Close() // Wake the owned pipe reader, not a foreign process.
		timer.Reset(3 * time.Second)
		select {
		case <-p.readDone:
		case <-timer.C:
			err = errors.Join(err, errUnavailable, errors.New("owner stdout did not terminate within cleanup allowance"))
		}
	}
	// Cmd.Wait owns process reaping. Its result is observed through a bounded
	// channel even if the OS fails to finish teardown after SIGKILL/pipe close.
	waitDone := make(chan error, 1)
	go func() { waitDone <- p.command.Wait() }()
	timer.Stop()
	timer.Reset(3 * time.Second)
	var waitErr error
	select {
	case waitErr = <-waitDone:
		if p.command.ProcessState == nil {
			return out, errors.Join(err, errUnavailable, waitErr, p.log.Err(), p.stderr.Close())
		}
		out.Reaped = true
		out.Exit = p.command.ProcessState.ExitCode()
	case <-timer.C:
		_ = p.command.Process.Kill()
		_ = p.stdout.Close()
		return out, errors.Join(err, errUnavailable, errors.New("owner process reap not observed within cleanup allowance"), p.log.Err(), p.stderr.Close())
	}
	if kill {
		exit, ok := errors.AsType[*exec.ExitError](waitErr)
		if !ok {
			return out, errors.Join(errMismatch, waitErr)
		}
		status, ok := exit.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			return out, errMismatch
		}
		out.Signal = "SIGKILL"
	} else {
		err = errors.Join(err, waitErr)
	}
	p.mu.Lock()
	if !kill {
		err = errors.Join(err, p.failure)
	}
	p.mu.Unlock()
	return out, errors.Join(err, p.log.Err(), p.stderr.Close())
}
