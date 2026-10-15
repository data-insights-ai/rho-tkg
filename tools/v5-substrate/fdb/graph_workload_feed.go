package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphFeedWorkloadFamily = "snapshot-to-feed"
const graphFeedWorkloadFormat = "fdb-snapshot-feed-workload-v1"

type graphFeedWorkloadSpec struct {
	Version      uint8  `json:"version"`
	Family       string `json:"family"`
	Facts        uint64 `json:"facts"`
	CrossPercent uint64 `json:"crossPercent"`
	Writers      int    `json:"writers"`
	Layout       string `json:"layout"`
}

type graphFeedWorkloadSummary struct {
	Original                               graphFeedOriginal
	Token                                  graphFeedToken
	Pages, AcknowledgedRows, ReplayedPages uint64
	Completed                              certificate
	Processes                              [2]int
	KilledReaped, ReplacementReaped        bool
	Storage                                graphFeedWorkloadStorage
}
type graphFeedChildInput struct {
	Config                     config
	Source                     graphHistoryBinding
	Policy                     graphFeedPolicy
	Token                      graphFeedToken
	Boundary                   certificate
	ReaderRequest, PageRequest string
	DeadlineNanos              int64
}
type graphFeedChildResult struct {
	PID                            int
	Owner                          graphHistoryOwner
	Delivery                       graphFeedDelivery
	Ack                            *graphFeedAcknowledgement `json:",omitempty"`
	Cleanup                        graphSnapshotWork
	OpenNanos, PageNanos, AckNanos int64
}

// These are bounded local SDK client processes INSIDE the existing monitored
// client container, not extra machines or independent storage fault domains.
// Files are rooted/O_EXCL; a ready file is emitted only after Page returned its
// genuinely committed native seal. Killing before ACK promises replay only.
func feedChildWrite(root *os.Root, name string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(raw) > 512<<10 {
		return errLimit
	}
	// One parent owns these fixed paths; final existence is never a resume or
	// replacement authority. Native intent/cursor owns process recovery instead.
	if _, err := root.Stat(name); err == nil {
		return os.ErrExist
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := root.OpenFile(name+".tmp", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(raw)
	if err = errors.Join(writeErr, file.Close()); err != nil {
		return err
	}
	// Atomic no-replace publication: a pre-existing final identity is never
	// overwritten, including by a concurrent process in this owned directory.
	if err := root.Link(name+".tmp", name); err != nil {
		return err
	}
	return root.Remove(name + ".tmp")
}
func feedChildRead[T any](root *os.Root, name string) (out T, err error) {
	file, err := root.Open(name)
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	raw, err := io.ReadAll(io.LimitReader(file, (512<<10)+1))
	if err != nil {
		return out, err
	}
	if len(raw) == 0 || len(raw) > 512<<10 {
		return out, errLimit
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&out); err != nil {
		return out, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return out, errors.Join(errInvalid, err)
	}
	canonical, err := json.Marshal(out)
	if err != nil || !bytes.Equal(canonical, raw) {
		return out, errors.Join(errMismatch, err)
	}
	return out, nil
}
func runGraphFeedChild(db fdb.Database, resume bool) (err error) {
	root, err := os.OpenRoot(os.Getenv("FDB_COST_ARTIFACT"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	input, err := feedChildRead[graphFeedChildInput](root, "fdb-feed-child-input.json")
	if err != nil {
		return err
	}
	if !validGraphFeedWorkloadConfig(input.Config) || input.Source.Version != 1 || input.Source.Config != input.Config || input.Policy != graphFeedPolicyFor(input.Config.Cost) || input.DeadlineNanos <= 0 || input.Token.DeadlineNanos != input.DeadlineNanos || input.ReaderRequest != "process-reader" || input.PageRequest != "page-0" {
		return errInvalid
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, input.DeadlineNanos))
	defer cancel()
	h, err := newHarnessContext(ctx, db, input.Config)
	if err != nil {
		return err
	}
	source, err := newGraphHistoryRetentionContext(ctx, h, cut{input.Source.Initial}, input.Source.Policy)
	if err != nil {
		return err
	}
	store, err := newGraphFeedStore(ctx, source, input.Policy)
	if err != nil {
		return err
	}
	begin := time.Now()
	feed, err := store.Open(ctx, input.Token, input.ReaderRequest)
	if err != nil {
		return err
	}
	result := graphFeedChildResult{PID: os.Getpid(), Owner: feed.reader.id, OpenNanos: time.Since(begin).Nanoseconds()}
	// The second actual process resolves the SAME persisted reader factory
	// original. Open is idempotent while live; it cannot mint after expiry.
	begin = time.Now()
	result.Delivery, err = feed.Page(ctx, input.PageRequest, input.Boundary)
	if err != nil {
		return errors.Join(err, feed.Close())
	}
	result.PageNanos = time.Since(begin).Nanoseconds()
	if !resume {
		if err := feedChildWrite(root, "fdb-feed-child-ready.json", result); err != nil {
			return errors.Join(err, feed.Close())
		}
		// Parent owns the pipe and kills this process. A timeout/EOF is failure,
		// never ACK, ABORT, successful cleanup or proof of process replacement.
		one := make([]byte, 1)
		_, err := os.Stdin.Read(one)
		return errors.Join(errUnavailable, err, feed.Close())
	}
	if !result.Delivery.Replay {
		return errors.Join(errMismatch, feed.Close())
	}
	begin = time.Now()
	ack, err := feed.Ack(ctx, input.PageRequest, sha256.Sum256(result.Delivery.Record.Value))
	if err != nil {
		return errors.Join(err, feed.Close())
	}
	result.Ack = &ack
	result.AckNanos = time.Since(begin).Nanoseconds()
	if err := feed.Close(); err != nil {
		return err
	}
	result.Cleanup = feed.Work()
	return feedChildWrite(root, "fdb-feed-child-resumed.json", result)
}

type graphFeedBoundedOutput struct {
	mu      sync.Mutex
	file    *os.File
	bytes   int
	failure error
}

func (w *graphFeedBoundedOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(p) > (512<<10)-w.bytes {
		w.failure = errors.Join(w.failure, errLimit)
		return 0, errLimit
	}
	n, err := w.file.Write(p)
	w.bytes += n
	w.failure = errors.Join(w.failure, err)
	return n, err
}
func (w *graphFeedBoundedOutput) Err() error { w.mu.Lock(); defer w.mu.Unlock(); return w.failure }

func (a *graphFeedWorkloadArtifacts) replaceProcess(ctx context.Context, s *graphFeedStore, token graphFeedToken) (graphFeedChildResult, error) {
	var zero graphFeedChildResult
	deadline, ok := ctx.Deadline()
	if !ok {
		return zero, errInvalid
	}
	input := graphFeedChildInput{a.h.config, s.source.binding, s.binding.Policy, token, a.final.record, "process-reader", "page-0", deadline.UnixNano()}
	if err := feedChildWrite(a.root, "fdb-feed-child-input.json", input); err != nil {
		return zero, err
	}
	executable, err := os.Executable()
	if err != nil {
		return zero, err
	}
	launch := func(mode string) (*exec.Cmd, *os.File, error) {
		command := exec.CommandContext(ctx, executable, mode)
		command.WaitDelay = 3 * time.Second
		// One bounded synchronized writer owns both child diagnostic streams.
		output, _, err := openGraphCostArtifact(a.root, "fdb-feed-"+mode+".output")
		if err != nil {
			return nil, nil, err
		}
		writer := &graphFeedBoundedOutput{file: output}
		command.Stdout = writer
		command.Stderr = writer
		stdin, err := command.StdinPipe()
		if err != nil {
			return nil, nil, errors.Join(err, output.Close())
		}
		if err := command.Start(); err != nil {
			return nil, nil, errors.Join(err, stdin.Close(), output.Close())
		}
		// The parent intentionally keeps the first pipe open until genuine SIGKILL.
		if mode == "feed-client-resume" {
			if err := stdin.Close(); err != nil {
				_ = command.Process.Kill()
				return nil, nil, errors.Join(err, command.Wait(), output.Close())
			}
		} else {
			a.childInput = stdin
		}
		return command, output, nil
	}
	first, output, err := launch("feed-client-hold")
	if err != nil {
		return zero, err
	}
	waited := false
	defer func() {
		if !waited {
			_ = first.Process.Kill()
			_ = first.Wait()
		}
		_ = output.Close()
		if a.childInput != nil {
			_ = a.childInput.Close()
			a.childInput = nil
		}
	}()
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	var ready graphFeedChildResult
	for {
		ready, err = feedChildRead[graphFeedChildResult](a.root, "fdb-feed-child-ready.json")
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return zero, err
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-timer.C:
			return zero, context.DeadlineExceeded
		case <-tick.C:
		}
	}
	if ready.PID != first.Process.Pid || ready.Delivery.Replay || ready.Ack != nil {
		return zero, errMismatch
	}
	if err := first.Process.Signal(syscall.SIGKILL); err != nil {
		return zero, err
	}
	if a.childInput != nil {
		if err := a.childInput.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			return zero, err
		}
		a.childInput = nil
	}
	waitErr := first.Wait()
	waited = true
	if err := first.Stdout.(*graphFeedBoundedOutput).Err(); err != nil {
		return zero, err
	}
	exit, ok := errors.AsType[*exec.ExitError](waitErr)
	if !ok {
		return zero, errors.Join(errMismatch, waitErr)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		return zero, errMismatch
	}
	if err := output.Close(); err != nil {
		return zero, err
	}
	a.end.Feed.Processes[0] = ready.PID
	a.end.Feed.KilledReaped = true
	if err := a.event(struct {
		Type   string
		Input  graphFeedChildInput
		Result graphFeedChildResult
		Signal string
		Reaped bool
	}{"client-killed", input, ready, "SIGKILL", true}); err != nil {
		return zero, err
	}
	second, secondOutput, err := launch("feed-client-resume")
	if err != nil {
		return zero, err
	}
	waitErr = second.Wait()
	closeErr := secondOutput.Close()
	if err := errors.Join(waitErr, closeErr, second.Stdout.(*graphFeedBoundedOutput).Err()); err != nil {
		return zero, err
	}
	resumed, err := feedChildRead[graphFeedChildResult](a.root, "fdb-feed-child-resumed.json")
	if err != nil {
		return zero, err
	}
	if resumed.PID != second.Process.Pid || resumed.PID == ready.PID || !resumed.Delivery.Replay || resumed.Ack == nil || !graphColumnSame(resumed.Owner, ready.Owner) || !bytes.Equal(resumed.Delivery.Record.Value, ready.Delivery.Record.Value) {
		return zero, errMismatch
	}
	a.end.Feed.Processes[1] = resumed.PID
	a.end.Feed.ReplacementReaped = true
	if err := a.event(struct {
		Type   string
		Input  graphFeedChildInput
		Result graphFeedChildResult
		Exit   int
		Reaped bool
	}{"client-replaced", input, resumed, 0, true}); err != nil {
		return zero, err
	}
	return resumed, nil
}

func runGraphFeedWorkload(db fdb.Database, spec graphFeedWorkloadSpec, run string) (err error) {
	c := graphFeedWorkloadConfig(spec, run)
	if spec.Version != 1 || spec.Family != graphFeedWorkloadFamily || !validGraphFeedWorkloadConfig(c) {
		return errInvalid
	}
	seconds := 1800
	if raw := os.Getenv("FDB_COST_SECONDS"); raw != "" {
		n, parseErr := strconv.Atoi(raw)
		if parseErr != nil || n < 1 || n > 1800 {
			return errInvalid
		}
		seconds = n
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(seconds)*time.Second)
	defer cancel()
	h, err := newHarnessContext(ctx, db, c)
	if err != nil {
		return err
	}
	binding, err := h.graphFeedWorkloadInstall(ctx)
	if err != nil {
		return err
	}
	initial, _, err := historyTransactContext(ctx, h.db, func() error { return nil }, func(tr fdb.Transaction) (cut, error) {
		result, err := h.freshIn(tr, []uint8{0, 1}, nil)
		if err != nil {
			return cut{}, err
		}
		return result.(cut), nil
	})
	if err != nil {
		return err
	}
	source, err := newGraphHistoryRetentionContext(ctx, h, initial, graphFeedWorkloadPolicy(c).History)
	if err != nil {
		return err
	}
	store, err := newGraphFeedStore(ctx, source, graphFeedPolicyFor(c.Cost))
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(os.Getenv("FDB_COST_ARTIFACT"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	a, err := newGraphFeedWorkloadArtifacts(ctx, h, root, spec, binding, initial)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, a.Close()) }()
	apply := func(phase string, fact uint64) error {
		begin := time.Now()
		var output error
		err := h.graphFeedWorkloadApply(ctx, phase, fact, 0, func(e graphCostParallelEvent) { output = errors.Join(output, a.accept(e)) })
		return errors.Join(err, output, a.latency(phase, fact, 0, time.Since(begin).Nanoseconds()))
	}
	for i := uint64(0); i < 3; i++ {
		if err := apply("setup", i); err != nil {
			return err
		}
	}
	if err := apply("create", 0); err != nil {
		return err
	}
	if err := h.graphFeedWorkloadPhase(ctx, a, "create"); err != nil {
		return err
	}
	secret, err := newGraphFeedSecret()
	if err != nil {
		return err
	}
	begin := time.Now()
	original, err := store.Begin(ctx, "workload", secret)
	if err != nil {
		return err
	}
	if err := a.feedControl(ctx, store, "begin", original, time.Since(begin).Nanoseconds()); err != nil {
		return err
	}
	begin = time.Now()
	snapshot, token, err := store.Publish(ctx, original)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, snapshot.Close()) }()
	a.end.Feed.Original, a.end.Feed.Token = original, token
	a.old = cut{original.Cut}
	if err := a.feedControl(ctx, store, "publish", token, time.Since(begin).Nanoseconds()); err != nil {
		return err
	}
	if err := a.inventory("old", a.old); err != nil {
		return err
	}
	if spec.Layout == "columns" {
		if err := a.columnsStart(); err != nil {
			return err
		}
	}
	// Genuine snapshot/write race: first actual delivered row precedes all
	// updates; backpressure holds that scan open while independent writers commit.
	// The SAME retained snapshot must finish at5 despite current becoming6.
	a.firstRow = func() error {
		if err := a.event(struct {
			Type string
			Cut  certificate
		}{"race-first-row", original.Cut}); err != nil {
			return err
		}
		if err := apply("update", 0); err != nil {
			return err
		}
		if err := h.graphFeedWorkloadPhase(ctx, a, "update"); err != nil {
			return err
		}
		return a.event(struct {
			Type    string
			Updates uint64
		}{"race-writes-complete", a.end.Updates})
	}
	if err := a.observe("racing-old", snapshot, 5); err != nil {
		return err
	}
	if a.firstRow != nil {
		return errUnavailable
	}
	if err := a.observe("held-old-after-update", snapshot, 5); err != nil {
		return err
	}
	current, err := a.open(source, "fresh-current", graphSnapshotRequest{Mode: "Fresh", Scope: []uint8{0, 1}})
	if err != nil {
		return err
	}
	a.final = cut{current.owner.id.Cut}
	if err := a.observe("fresh-current", current, 6); err != nil {
		return errors.Join(err, current.Close())
	}
	if err := current.Close(); err != nil {
		return err
	}
	resumed, err := a.replaceProcess(ctx, store, token)
	if err != nil {
		return err
	}
	first := resumed.Delivery.Page
	a.end.Feed.Pages = 1
	a.end.Feed.ReplayedPages = 1
	a.end.Feed.AcknowledgedRows = uint64(len(first.Rows))
	a.end.Feed.Completed = resumed.Ack.Ack.Completed
	begin = time.Now()
	feed, err := store.Open(ctx, token, "local-one")
	if err != nil {
		return err
	}
	if err := a.feedControl(ctx, store, "local-open", feed.reader.id, time.Since(begin).Nanoseconds()); err != nil {
		return errors.Join(err, feed.Close())
	}
	// Ordinary handle Close/reopen is separate from the real process proof.
	if err := feed.Close(); err != nil {
		return err
	}
	begin = time.Now()
	feed, err = store.Open(ctx, token, "local-two")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, feed.Close()) }()
	if err := a.feedControl(ctx, store, "local-reopen", feed.reader.id, time.Since(begin).Nanoseconds()); err != nil {
		return err
	}
	for !first.EOF {
		name := fmt.Sprintf("page-%d", a.end.Feed.Pages)
		begin = time.Now()
		delivery, err := feed.Page(ctx, name, a.final.record)
		if err != nil {
			return err
		}
		pageNanos := time.Since(begin).Nanoseconds()
		begin = time.Now()
		ack, err := feed.Ack(ctx, name, sha256.Sum256(delivery.Record.Value))
		if err != nil {
			return err
		}
		ackNanos := time.Since(begin).Nanoseconds()
		if err := a.event(struct {
			Type                string
			Delivery            graphFeedDelivery
			Ack                 graphFeedAcknowledgement
			PageNanos, AckNanos int64
		}{"feed-page", delivery, ack, pageNanos, ackNanos}); err != nil {
			return err
		}
		a.end.Feed.Pages++
		a.end.Feed.AcknowledgedRows += uint64(len(delivery.Page.Rows))
		a.end.Feed.Completed = ack.Ack.Completed
		first = delivery.Page
	}
	// Duplicate original delivery/ACK is explicit replay, never a second useful
	// completion; it cannot advance to a caller-invented vector or round.
	begin = time.Now()
	replay, err := feed.Page(ctx, "page-0", a.final.record)
	replayNanos := time.Since(begin).Nanoseconds()
	if err != nil || !replay.Replay || !bytes.Equal(replay.Record.Value, resumed.Delivery.Record.Value) {
		return errors.Join(errMismatch, err)
	}
	begin = time.Now()
	ackReplay, err := feed.Ack(ctx, "page-0", sha256.Sum256(replay.Record.Value))
	ackReplayNanos := time.Since(begin).Nanoseconds()
	if err != nil {
		return err
	}
	a.end.Feed.ReplayedPages++
	if err := a.event(struct {
		Type                string
		Delivery            graphFeedDelivery
		Ack                 graphFeedAcknowledgement
		PageNanos, AckNanos int64
	}{"feed-replay", replay, ackReplay, replayNanos, ackReplayNanos}); err != nil {
		return err
	}
	if err := store.Finish(ctx, token, "expire-workload", true); err != nil {
		return err
	}
	absent, refused := store.Open(ctx, token, "expired-new")
	if absent != nil || !errors.Is(refused, errHistoryExpired) {
		return errors.Join(errMismatch, refused)
	}
	if err := a.feedControl(ctx, store, "expired-new-refused", struct{ Request, Error string }{"expired-new", refused.Error()}, 0); err != nil {
		return err
	}
	// Genuine held owner survives pin expiry; a fixed three-probe old5/phantom
	// result is a complete semantic scan, not an indexed latency claim.
	begin = time.Now()
	points, err := snapshot.Points(ctx, []graphSnapshotKey{{0, edgeKey("edge", c.Cost.edge(0, 5))}, {0, edgeKey("edge", c.Cost.edge(c.Cost.Facts-1, 5))}, {0, edgeKey("edge", c.Cost.edge(c.Cost.Facts+16, 5))}})
	if err != nil || !points[0].Found || !points[1].Found || points[2].Found || points[0].Value.Value != 5 || points[1].Value.Value != 5 {
		return errors.Join(errMismatch, err)
	}
	if err := a.event(struct {
		Type   string
		Points []graphSnapshotPoint
		Work   graphSnapshotWork
		Nanos  int64
	}{"expired-held-points", points, snapshot.Work(), time.Since(begin).Nanoseconds()}); err != nil {
		return err
	}
	if err := feed.Close(); err != nil {
		return err
	}
	if err := snapshot.Close(); err != nil {
		return err
	}
	if err := a.feedControl(ctx, store, "terminal", token, 0); err != nil {
		return err
	}
	if err := a.feedStorage(ctx, store); err != nil {
		return err
	}
	if spec.Layout == "columns" {
		if err := a.columnsFinish(); err != nil {
			return err
		}
	}
	if err := a.finish(source); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(a.end)
}

func (s graphFeedWorkloadSpec) profile() graphCostProfile {
	return graphCostProfile{5, s.Facts, s.CrossPercent, s.Writers}
}
func graphFeedWorkloadLimit(p graphCostProfile) limits {
	originals := 3 + 2*p.Facts*graphCostParallelAttempts
	return limits{Bytes: (64 << 20) + p.Facts*(64<<10) + originals*2048, Transactions: originals, Versions: 3 + 6*p.Facts, Certificates: 1024}
}
func graphFeedWorkloadConfig(spec graphFeedWorkloadSpec, run string) config {
	return graphCostConfig("graph-workload-feed-v1-"+spec.Layout+"-"+run, spec.profile())
}
func validGraphFeedWorkloadConfig(c config) bool {
	p := c.Cost
	if p.Writers != 1 && p.Writers != 8 && p.Writers != 32 {
		return false
	}
	firstJobs := uint64(p.Writers + 1) // #nosec G115 -- admitted writers1/8/32.
	if p.Version != 5 || p.Facts < 20 || p.Facts < firstJobs || p.Facts > 1_000_000 || p.Facts%20 != 0 || p.CrossPercent != 0 && p.CrossPercent != 10 && p.CrossPercent != 50 && p.CrossPercent != 100 || c.Lifecycle != (lifecycleProfile{}) || c.CostScope != lifecycleConfig("validation").Lifecycle.Scope {
		return false
	}
	name := strings.TrimPrefix(c.Graph, "graph-workload-feed-v1-")
	if name == c.Graph {
		return false
	}
	run, found := strings.CutPrefix(name, "kv-")
	if !found {
		run, found = strings.CutPrefix(name, "columns-")
	}
	if !found || len(run) != 32 {
		return false
	}
	nonce, err := hex.DecodeString(run)
	if err != nil || hex.EncodeToString(nonce) != run || bytes.Equal(nonce, make([]byte, 16)) {
		return false
	}
	n := p.nodes()
	return c.Nodes == [2]uint64{n[0].Node, n[2].Node}
}

type graphFeedWorkloadBinding struct {
	Version                                  uint8
	Config                                   config
	Dataset                                  [32]byte
	Window, Attempts, Submissions, Callbacks int
	Read                                     graphSnapshotPolicy
	History                                  graphHistoryPolicy
}

func graphFeedWorkloadPolicy(c config) graphFeedWorkloadBinding {
	p := c.Cost
	datasetProfile := p
	datasetProfile.Version = 1
	return graphFeedWorkloadBinding{1, c, graphCostOperationID(datasetProfile, "setup", 0).Dataset, 32, 64, 8, 16,
		graphSnapshotPolicy{PageRows: 8, PageBytes: 4 << 20, InventoryBytes: 4 << 20, WorkRows: 1024*p.Facts + 4096, WorkBytes: (64 << 20) + p.Facts*(2<<20), ChainSteps: 128, Cleanup: graphSnapshotCleanupDefault()},
		graphHistoryPolicy{MaxOwners: 64, MaxControlBytes: 4 << 20, PageRows: 32, PageBytes: 4 << 20, MaxWorkRows: 32*p.Facts + 4096, MaxWorkBytes: (64 << 20) + p.Facts*(1<<20), MaxChainSteps: 128, MaxOutputBytes: 4096}}
}
func (h *harness) graphFeedWorkloadBindingIn(tr fdb.ReadTransaction) error {
	raw, err := tr.Get(h.key(0, "workload-feed-v1", "binding")).Get()
	if err != nil {
		return err
	}
	actual, err := decode[graphFeedWorkloadBinding](raw)
	if err != nil || actual != graphFeedWorkloadPolicy(h.config) {
		return errors.Join(errMismatch, err)
	}
	return nil
}
func (h *harness) graphFeedWorkloadInstall(ctx context.Context) (graphCostStoredRecord, error) {
	if h == nil || ctx == nil {
		return graphCostStoredRecord{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return graphCostStoredRecord{}, err
	}
	key := h.key(0, "workload-feed-v1", "binding")
	body, err := wire(graphFeedWorkloadPolicy(h.config))
	if err != nil {
		return graphCostStoredRecord{}, err
	}
	_, _, err = historyTransactContext(ctx, h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		for g := uint8(0); g < 2; g++ {
			if err := h.checkConfig(tr, g); err != nil {
				return false, err
			}
			m, err := h.meta(tr, g)
			if err != nil {
				return false, err
			}
			if m.Groups != 0 || m.Transactions != 0 || m.Versions != 0 || m.Certificates != 0 {
				return false, errUnavailable
			}
		}
		prior, err := tr.Get(key).Get()
		if err != nil {
			return false, err
		}
		if len(prior) != 0 {
			if !bytes.Equal(prior, body) {
				return false, errMismatch
			}
			return true, nil
		}
		m, err := h.meta(tr, 0)
		if err != nil {
			return false, err
		}
		n := uint64(len(key) + len(body))
		if n > h.config.Limits.Bytes-m.Bytes {
			return false, errLimit
		}
		m.Bytes += n
		tr.Set(key, body)
		return true, put(tr, h.key(0, "meta", "state"), m)
	})
	if err != nil {
		return graphCostStoredRecord{}, graphHistoryCommitError(err)
	}
	stored, _, err := graphCostParallelRead(ctx, h.db, func(tr fdb.ReadTransaction) (graphCostStoredRecord, error) {
		if err := h.graphFeedWorkloadBindingIn(tr); err != nil {
			return graphCostStoredRecord{}, err
		}
		return snapshotStored(tr, key, 64<<10)
	})
	return stored, err
}

func graphFeedWorkloadSpecFromEnv() (graphFeedWorkloadSpec, string, error) {
	var spec graphFeedWorkloadSpec
	raw, run := os.Getenv("RHO_GRAPH_WORKLOAD_SPEC"), os.Getenv("RHO_GRAPH_WORKLOAD_RUN")
	if len(raw) == 0 || len(raw) > 1024 {
		return spec, "", errInvalid
	}
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&spec); err != nil {
		return spec, "", errors.Join(errInvalid, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return spec, "", errors.Join(errInvalid, err)
	}
	if spec.Version != 1 || spec.Family != graphFeedWorkloadFamily || !validGraphFeedWorkloadConfig(graphFeedWorkloadConfig(spec, run)) {
		return spec, "", errInvalid
	}
	return spec, run, nil
}

func (h *harness) graphFeedWorkloadApply(ctx context.Context, phase string, fact uint64, client int, emit func(graphCostParallelEvent)) error {
	p := h.config.Cost
	p.Version = 1
	_, err := h.graphCostApplyOperation(ctx, graphCostParallelJob{phase, fact}, client, graphCostOperationID(p, phase, fact), "feed", emit)
	return err
}

// Real independent callers share only bounded immutable jobs/results. The one
// owner drains admitted events even on error; no terminal/unknown attempt drops.
func (h *harness) graphFeedWorkloadPhase(ctx context.Context, a *graphFeedWorkloadArtifacts, phase string) error {
	type job struct {
		fact  uint64
		begin time.Time
	}
	jobs := make(chan job, 32)
	type event struct {
		Value        graphCostParallelEvent
		ElapsedNanos int64
	}
	events := make(chan event, 32)
	phaseBegin := time.Now()
	var wg sync.WaitGroup
	for client := range h.config.Cost.Writers {
		wg.Go(func() {
			perform := func(work job) {
				fact := work.fact
				err := h.graphFeedWorkloadApply(ctx, phase, fact, client, func(e graphCostParallelEvent) { events <- event{Value: e} })
				events <- event{Value: graphCostParallelEvent{Client: client, Done: true, Err: err, Sample: graphCostSample{Phase: phase, Fact: fact}}, ElapsedNanos: time.Since(work.begin).Nanoseconds()}
			}
			// One deterministic first job per genuine independent caller; shared
			// queue scheduling cannot accidentally leave a declared writer unused.
			perform(job{uint64(client + 1), phaseBegin}) // #nosec G115 -- admitted clients0..31.
			for work := range jobs {
				perform(work)
			}
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(events); close(done) }()
	// Work proceeds in finite windows. Each admitted client receives a real job;
	// errors stop further admissions but the current window is fully observed.
	var result error
	next := uint64(h.config.Cost.Writers + 1) // #nosec G115 -- admitted1/8/32.
	inflight := h.config.Cost.Writers
	for next < h.config.Cost.Facts || inflight > 0 {
		for result == nil && next < h.config.Cost.Facts && inflight < 32 {
			jobs <- job{next, time.Now()}
			next++
			inflight++
		}
		if inflight == 0 {
			break
		}
		e := <-events
		if e.Value.Done {
			inflight--
			result = errors.Join(result, e.Value.Err, a.latency(e.Value.Sample.Phase, e.Value.Sample.Fact, e.Value.Client, e.ElapsedNanos))
		}
		result = errors.Join(result, a.accept(e.Value))
	}
	close(jobs)
	for e := range events {
		result = errors.Join(result, e.Value.Err, a.accept(e.Value))
	}
	<-done
	return result
}
