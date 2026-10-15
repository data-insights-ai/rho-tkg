package main

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
	"strings"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphContentionFamily = "contention-empty-range"
const graphContentionFormat = "fdb-contention-empty-range-v1"

type graphContentionSpec struct {
	Version      uint8  `json:"version"`
	Family       string `json:"family"`
	Facts        uint64 `json:"facts"`
	CrossPercent uint64 `json:"crossPercent"`
	Writers      int    `json:"writers"`
	Layout       string `json:"layout"`
}

func (s graphContentionSpec) profile() graphCostProfile {
	return graphCostProfile{Version: 2, Facts: s.Facts, CrossPercent: s.CrossPercent, Writers: s.Writers}
}
func graphContentionValid(p graphCostProfile) bool {
	return p.Version == 2 && p.Facts >= 20 && p.Facts <= 1_000_000 && p.Facts%20 == 0 && (p.CrossPercent == 0 || p.CrossPercent == 10 || p.CrossPercent == 50 || p.CrossPercent == 100) && (p.Writers == 1 || p.Writers == 8 || p.Writers == 32)
}
func graphContentionCompetitors(p graphCostProfile) uint64 { return uint64(max(2, p.Writers)) } // #nosec G115 -- admitted1/8/32.
func graphContentionLimit(p graphCostProfile) limits {
	if !graphContentionValid(p) {
		return limits{}
	}
	originals := 3 + p.Facts/2 + p.Facts*graphCostParallelAttempts + p.Facts/2*graphContentionCompetitors(p)
	return limits{Bytes: (64 << 20) + p.Facts*(128<<10) + originals*2048, Transactions: originals, Versions: 3 + 6*p.Facts, Certificates: 1024}
}
func graphContentionConfig(p graphCostProfile, layout, run string) config {
	c := graphCostConfig("graph-workload-contention-v1-"+layout+"-"+run, p)
	c.Limits = graphContentionLimit(p)
	return c
}
func graphContentionGraph(graph string) bool {
	for _, layout := range []string{"kv", "columns"} {
		run, found := strings.CutPrefix(graph, "graph-workload-contention-v1-"+layout+"-")
		raw, err := hex.DecodeString(run)
		if found && err == nil && len(raw) == 16 && hex.EncodeToString(raw) == run && !bytes.Equal(raw, make([]byte, 16)) {
			return true
		}
	}
	return false
}
func validGraphContentionConfig(c config) bool {
	p := c.Cost
	if !graphContentionValid(p) || !graphContentionGraph(c.Graph) || c.Lifecycle != (lifecycleProfile{}) || c.CostScope != lifecycleConfig("validation").Lifecycle.Scope {
		return false
	}
	nodes := p.nodes()
	return c.Nodes == [2]uint64{nodes[0].Node, nodes[2].Node} && c.Limits == graphContentionLimit(p)
}
func graphContentionTarget(p graphCostProfile, phase string, fact uint64) (uint64, error) {
	if !graphContentionValid(p) {
		return 0, errInvalid
	}
	switch phase {
	case "seed":
		if fact >= p.Facts/2 {
			return 0, errInvalid
		}
		return fact, nil
	case "claim":
		if fact >= p.Facts/2 {
			return 0, errInvalid
		}
		return p.Facts/2 + fact, nil
	case "hot":
		if fact >= p.Facts {
			return 0, errInvalid
		}
		// The admitted20-row period guarantees both relevant endpoints in the seed
		// half. Hot targets follow the requested TRANSACTION mix, not just dataset mix.
		for candidate := uint64(0); candidate < 10; candidate++ {
			if p.cross(candidate) == p.cross(fact) {
				return candidate, nil
			}
		}
	}
	return 0, errInvalid
}
func graphContentionOrdinal(p graphCostProfile, phase string, fact, claimant uint64) (uint64, error) {
	switch phase {
	case "setup":
		if fact < 3 && claimant == 0 {
			return fact + 1, nil
		}
	case "seed":
		if fact < p.Facts/2 && claimant == 0 {
			return 4 + fact, nil
		}
	case "hot":
		if fact < p.Facts && claimant == 0 {
			return 4 + p.Facts/2 + fact, nil
		}
	case "claim":
		if fact < p.Facts/2 && claimant < graphContentionCompetitors(p) {
			return 4 + p.Facts/2 + p.Facts + fact*graphContentionCompetitors(p) + claimant, nil
		}
	}
	return 0, errInvalid
}
func graphContentionDecodeOrdinal(p graphCostProfile, ordinal uint64) (string, uint64, error) {
	if ordinal >= 1 && ordinal <= 3 {
		return "setup", ordinal - 1, nil
	}
	if ordinal >= 4 && ordinal < 4+p.Facts/2 {
		return "seed", ordinal - 4, nil
	}
	if ordinal >= 4+p.Facts/2 && ordinal < 4+p.Facts/2+p.Facts {
		return "hot", ordinal - 4 - p.Facts/2, nil
	}
	start := 4 + p.Facts/2 + p.Facts
	if ordinal >= start && ordinal < start+p.Facts/2*graphContentionCompetitors(p) {
		return "claim", (ordinal - start) / graphContentionCompetitors(p), nil
	}
	return "", 0, errInvalid
}
func graphContentionIncrement(prior value, next int64) error {
	if prior.Version == 0 || prior.Deleted || prior.Value < 5 || prior.Value >= 1_000_005 || next != prior.Value+1 {
		return errInvalid
	}
	return nil
}
func (h *harness) validateGraphContention(x transaction) error {
	p := h.config.Cost
	if !validGraphContentionConfig(h.config) || x.Lifecycle == nil || x.GraphChange == nil {
		return errInvalid
	}
	phase, fact, err := graphContentionDecodeOrdinal(p, x.Lifecycle.Provenance)
	if err != nil {
		return err
	}
	command, m := *x.GraphChange, *x.Lifecycle
	if phase == "setup" {
		if command != (graphCommand{Kind: "node-create", Node: p.nodes()[fact]}) || m.EdgeLife != 0 || m.Definition != nil {
			return errInvalid
		}
		return nil
	}
	target, err := graphContentionTarget(p, phase, fact)
	if err != nil {
		return err
	}
	kind := "edge-create"
	if phase == "hot" {
		kind = "edge-update"
	}
	if command.Kind != kind || command.Node != (endpoint{}) || command.Edge != p.edge(target, command.Edge.Value) || m.EdgeLife != command.Edge.ID+1 || m.Definition == nil || m.Definition.Kind != "i64" || m.Definition.I64 != command.Edge.Value {
		return errInvalid
	}
	if phase != "hot" && command.Edge.Value != 5 || phase == "hot" && (command.Edge.Value < 6 || command.Edge.Value > int64(5+p.Facts)) {
		return errInvalid
	} // #nosec G115 -- Facts<=1M.
	if m.Definition.ID != 4*p.Facts+32+uint64(command.Edge.Value) {
		return errInvalid
	} // #nosec G115 -- admitted positive value<=1M+5, distinct fixed fixture ID range.
	return nil
}

type graphContentionBinding struct {
	Version                                                        uint8
	Family                                                         string
	Config                                                         config
	Competitors, Window, MaxAttempts, MaxSubmissions, MaxCallbacks uint64
	Policy                                                         string
}

func graphContentionPolicy(c config) graphContentionBinding {
	return graphContentionBinding{1, graphContentionFamily, c, graphContentionCompetitors(c.Cost), 32, 64, 8, 16, "retained-all; hot native current-value+revision increment; each empty-ID wave isolated after hot drain; exact paired ABORT only authorizes replan"}
}
func (h *harness) graphContentionBindingIn(tr fdb.ReadTransaction) error {
	raw, err := tr.Get(h.key(0, "workload-contention-v1", "binding")).Get()
	if err != nil {
		return err
	}
	body, err := wire(graphContentionPolicy(h.config))
	if err != nil || !bytes.Equal(raw, body) {
		return errors.Join(errMismatch, err)
	}
	return nil
}
func (h *harness) graphContentionInstall() (graphCostStoredRecord, error) {
	body, err := wire(graphContentionPolicy(h.config))
	if err != nil {
		return graphCostStoredRecord{}, err
	}
	key := h.key(0, "workload-contention-v1", "binding")
	_, err = h.db.Transact(func(tr fdb.Transaction) (any, error) {
		for g := uint8(0); g < 2; g++ {
			if err := h.checkConfig(tr, g); err != nil {
				return nil, err
			}
			m, err := h.meta(tr, g)
			if err != nil {
				return nil, err
			}
			if m.Transactions != 0 || m.Groups != 0 || m.Versions != 0 || m.Certificates != 0 {
				return nil, errUnavailable
			}
		}
		prior, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		if prior != nil && !bytes.Equal(prior, body) {
			return nil, errMismatch
		}
		if prior != nil {
			return nil, nil
		}
		m, err := h.meta(tr, 0)
		if err != nil {
			return nil, err
		}
		added := uint64(len(key) + len(body))
		if added > h.config.Limits.Bytes || m.Bytes > h.config.Limits.Bytes-added {
			return nil, errLimit
		}
		m.Bytes += added
		tr.Set(key, body)
		return nil, put(tr, h.key(0, "meta", "state"), m)
	})
	if err != nil {
		return graphCostStoredRecord{}, graphHistoryCommitError(err)
	}
	got, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		if err := h.graphContentionBindingIn(tr); err != nil {
			return nil, err
		}
		raw, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		return graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)}, nil
	})
	if err != nil {
		return graphCostStoredRecord{}, err
	}
	return got.(graphCostStoredRecord), nil
}

type graphContentionCapture struct {
	Metadata             [2]metadata
	Records              [2]graphCostStoredRecord
	ReadVersion          int64
	RangeBegin, RangeEnd []byte                  `json:",omitempty"`
	RangeRows            []graphCostStoredRecord `json:",omitempty"`
}
type graphContentionSubmission struct {
	graphCostParallelSubmission
	NativeCode *int `json:",omitempty"`
}
type graphContentionAttempt struct {
	Operation                             graphCostOperation
	Family, Engine                        string
	ClientID, AttemptOrdinal              int
	PreviousRequest, Request, Transaction string
	Target, Claimant                      uint64
	Proposal                              []byte
	BodySHA256                            [32]byte
	Capture                               graphContentionCapture
	ReadCallbacks                         int
	Submissions                           []graphContentionSubmission
	Classification, Error                 string
	Proof                                 graphCostParallelProof
	ElapsedNanos                          int64
}
type graphContentionJob struct {
	Phase          string
	Fact, Claimant uint64
	Admitted       time.Time // Controller admission, including bounded inbox wait.
}
type graphContentionFrozen struct {
	Attempt graphContentionAttempt
	Tx      transaction
}
type graphContentionEvent struct {
	Attempt      *graphContentionAttempt
	Group        changeGroup
	Done         bool
	Operation    graphCostOperation
	Client       int
	ElapsedNanos int64
	Err          error
}

func graphContentionOperation(p graphCostProfile, job graphContentionJob) (graphCostOperation, error) {
	ordinal, err := graphContentionOrdinal(p, job.Phase, job.Fact, job.Claimant)
	if err != nil {
		return graphCostOperation{}, err
	}
	dataset := p
	dataset.Version = 1
	op := graphCostOperationID(dataset, job.Phase, job.Fact)
	op.Ordinal = ordinal
	return op, nil
}
func (h *harness) graphContentionFreeze(ctx context.Context, job graphContentionJob, client, ordinal int, previous string) (graphContentionFrozen, error) {
	var frozen graphContentionFrozen
	if h == nil || ctx == nil || client < 0 || client >= h.config.Cost.Writers || ordinal < 1 || ordinal > graphCostParallelAttempts || (ordinal == 1) != (previous == "") {
		return frozen, errInvalid
	}
	op, err := graphContentionOperation(h.config.Cost, job)
	if err != nil {
		return frozen, err
	}
	target := job.Fact
	if job.Phase != "setup" {
		target, err = graphContentionTarget(h.config.Cost, job.Phase, job.Fact)
		if err != nil {
			return frozen, err
		}
	}
	a := graphContentionAttempt{Operation: op, Family: graphContentionFamily, Engine: "foundationdb", ClientID: client, AttemptOrdinal: ordinal, PreviousRequest: previous, Target: target, Claimant: job.Claimant}
	command := graphCommand{Kind: "edge-create", Edge: h.config.Cost.edge(target, 5)}
	if job.Phase == "setup" {
		command = graphCommand{Kind: "node-create", Node: h.config.Cost.nodes()[job.Fact]}
	}
	if job.Phase == "hot" {
		command.Kind = "edge-update"
	}
	type capture struct {
		Snapshot snapshot
		Actual   graphContentionCapture
	}
	got, callbacks, err := graphCostParallelRead(ctx, h.db, func(tr fdb.ReadTransaction) (capture, error) {
		var c capture
		if err := h.graphContentionBindingIn(tr); err != nil {
			return c, err
		}
		c.Snapshot, err = h.graphCostPointSnapshotIn(tr, command)
		if err != nil {
			return c, err
		}
		for g := uint8(0); g < 2; g++ {
			c.Actual.Metadata[g], err = h.meta(tr, g)
			if err != nil {
				return c, err
			}
			key := h.key(g, "meta", "state")
			raw, err := tr.Get(key).Get()
			if err != nil {
				return c, err
			}
			c.Actual.Records[g] = graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)}
		}
		c.Actual.ReadVersion, err = tr.GetReadVersion().Get()
		if err != nil {
			return c, err
		}
		if job.Phase == "claim" {
			interval := h.logicalRange(0, fmt.Sprintf("edge/%016x/", command.Edge.ID))
			rows, err := tr.GetRange(interval, fdb.RangeOptions{Limit: 2}).GetSliceWithError()
			if err != nil {
				return c, err
			}
			c.Actual.RangeBegin = bytes.Clone(interval.Begin.(fdb.Key))
			c.Actual.RangeEnd = bytes.Clone(interval.End.(fdb.Key))
			if len(rows) != 0 {
				return c, errUnavailable
			} // Genuine absence BEFORE any contender is submitted.
		}
		return c, nil
	})
	a.ReadCallbacks = callbacks
	a.Capture = got.Actual
	if err != nil {
		a.Classification, a.Error = "preparation-error", err.Error()
		frozen.Attempt = a
		return frozen, err
	}
	if job.Phase == "hot" {
		prior := got.Snapshot.Values[0][edgeKey("edge", command.Edge)]
		if prior.Value < 5 || prior.Value >= int64(5+h.config.Cost.Facts) {
			a.Classification, a.Error = "preparation-error", errInvalid.Error()
			frozen.Attempt = a
			return frozen, errInvalid
		} // #nosec G115 -- Facts<=1M.
		command.Edge.Value = prior.Value + 1
	}
	coordinator := uint8(0)
	if job.Phase == "setup" {
		coordinator = command.Node.Owner
	}
	x := h.graphTransaction(fmt.Sprintf("contention-%s-%d-c%d-q%d-a%d", job.Phase, job.Fact, client, job.Claimant, ordinal), coordinator, command, got.Snapshot)
	x.Lifecycle = &lifecycleMetadata{Provenance: op.Ordinal}
	if job.Phase != "setup" {
		x.Lifecycle.EdgeLife = command.Edge.ID + 1
		x.Lifecycle.Definition = &lifecycleDefinition{ID: 4*h.config.Cost.Facts + 32 + uint64(command.Edge.Value), Kind: "i64", I64: command.Edge.Value}
	} // #nosec G115 -- positive admitted value<=1M+5.
	body, err := wire(x)
	if err == nil {
		err = h.validate(x)
	}
	if err != nil {
		a.Classification, a.Error = "invalid-proposal", err.Error()
		frozen.Attempt = a
		return frozen, err
	}
	a.Proposal, a.BodySHA256, a.Request, a.Transaction = bytes.Clone(body), sha256.Sum256(body), x.Request, x.ID
	return graphContentionFrozen{a, x}, nil
}
func (h *harness) graphContentionSubmit(ctx context.Context, frozen graphContentionFrozen) (a graphContentionAttempt, group changeGroup, err error) {
	started := time.Now()
	a = frozen.Attempt
	defer func() {
		a.ElapsedNanos = time.Since(started).Nanoseconds()
		if err != nil {
			a.Classification, a.Error = "incomplete", err.Error()
		}
	}()
	for invocation := 1; invocation <= graphCostParallelSubmissions; invocation++ {
		submitted := h.graphCostParallelSubmit(ctx, frozen.Tx, a.Proposal)
		submitted.Ordinal = invocation
		row := graphContentionSubmission{graphCostParallelSubmission: submitted}
		if native, ok := errors.AsType[fdb.Error](submitted.cause); ok {
			row.NativeCode = new(native.Code)
		}
		a.Submissions = append(a.Submissions, row)
		proof, callbacks, readErr := h.graphCostReadParallelOriginal(ctx, frozen.Tx, a.Proposal)
		a.ReadCallbacks += callbacks
		if readErr == nil {
			a.Proof = proof
			if submitted.Error == "" && submitted.Outcome != proof.Outcome {
				return a, group, errMismatch
			}
			if proof.Outcome.Commit {
				a.Classification = "committed"
				return a, proof.Group, nil
			}
			a.Classification = "aborted"
			return a, group, nil
		}
		if !errors.Is(readErr, errUnknown) {
			return a, group, readErr
		}
		if submitted.Error == "" {
			return a, group, errUnavailable
		}
		if !submitted.Retryable && !submitted.MaybeCommitted {
			return a, group, submitted.cause
		}
		if ctx.Err() != nil {
			return a, group, errors.Join(errUnknown, submitted.cause, ctx.Err())
		}
	}
	return a, group, errUnknown // Bounded uncertainty never creates a new request or ABORT.
}
func (h *harness) graphContentionApply(parent context.Context, job graphContentionJob, client int, emit func(graphContentionEvent)) error {
	ctx, cancel := context.WithTimeout(parent, graphCostParallelOperationMillis*time.Millisecond)
	defer cancel()
	previous := ""
	maxAttempts := 1
	if job.Phase == "hot" {
		maxAttempts = graphCostParallelAttempts
	}
	for ordinal := 1; ordinal <= maxAttempts; ordinal++ {
		frozen, err := h.graphContentionFreeze(ctx, job, client, ordinal, previous)
		if err != nil {
			emit(graphContentionEvent{Attempt: &frozen.Attempt})
			return err
		}
		a, group, err := h.graphContentionSubmit(ctx, frozen)
		emit(graphContentionEvent{Attempt: &a, Group: group})
		if err != nil {
			return err
		}
		if a.Proof.Outcome.Commit {
			return nil
		}
		if job.Phase != "hot" || !graphCostParallelReplan(a.Proof.Outcome) {
			return errUnavailable
		}
		previous = a.Request // Only exact retained paired ABORT authorizes this fresh read/request.
	}
	return errLimit
}

// A batch admits at most32 jobs/results; one owner serializes every artifact.
// Clients are independent native SDK callers, not goroutines writing the output.
func (h *harness) graphContentionHot(a *graphContentionArtifacts) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writers := h.config.Cost.Writers
	inbox := make([]chan graphContentionJob, writers)
	events := make(chan graphContentionEvent, 32)
	var workers sync.WaitGroup
	for client := range writers {
		inbox[client] = make(chan graphContentionJob, (32+writers-1)/writers)
		workers.Go(func() {
			for job := range inbox[client] {
				start := job.Admitted
				err := h.graphContentionApply(ctx, job, client, func(e graphContentionEvent) { events <- e })
				op, _ := graphContentionOperation(h.config.Cost, job)
				events <- graphContentionEvent{Done: true, Operation: op, Client: client, ElapsedNanos: time.Since(start).Nanoseconds(), Err: err}
			}
		})
	}
	var result error
	for first := uint64(0); first < h.config.Cost.Facts && result == nil; first += 32 {
		count := min(uint64(32), h.config.Cost.Facts-first)
		for offset := uint64(0); offset < count; offset++ {
			fact := first + offset
			inbox[int(fact%uint64(writers))] <- graphContentionJob{Phase: "hot", Fact: fact, Admitted: time.Now()}
		}
		for done := uint64(0); done < count; {
			event := <-events
			if err := a.accept(event); err != nil {
				result = errors.Join(result, err)
				cancel()
			}
			if event.Done {
				done++
			}
		}
	}
	for _, queue := range inbox {
		close(queue)
	}
	workers.Wait()
	return result
}
func (h *harness) graphContentionClaims(a *graphContentionArtifacts) error {
	p := h.config.Cost
	for fact := uint64(0); fact < p.Facts/2; fact++ {
		ctx, cancel := context.WithTimeout(context.Background(), graphCostParallelOperationMillis*time.Millisecond)
		count := int(graphContentionCompetitors(p)) // #nosec G115 -- bounded2/8/32.
		frozen := make([]graphContentionFrozen, count)
		started := make([]time.Time, count)
		var result error
		for claimant := range count {
			client := claimant
			if p.Writers == 1 {
				client = 0
			}
			var err error
			started[claimant] = time.Now()
			frozen[claimant], err = h.graphContentionFreeze(ctx, graphContentionJob{Phase: "claim", Fact: fact, Claimant: uint64(claimant)}, client, 1, "")
			if err != nil {
				result = errors.Join(err, a.accept(graphContentionEvent{Attempt: &frozen[claimant].Attempt}))
				break
			}
			if claimant > 0 && frozen[claimant].Attempt.Capture.Metadata != frozen[0].Attempt.Capture.Metadata {
				result = errMismatch
				break
			}
		}
		if result != nil {
			cancel()
			return result
		}
		events := make(chan graphContentionEvent, count)
		submit := func(claimant int) {
			row, group, err := h.graphContentionSubmit(ctx, frozen[claimant])
			events <- graphContentionEvent{Attempt: &row, Group: group, Done: true, Operation: row.Operation, Client: row.ClientID, ElapsedNanos: time.Since(started[claimant]).Nanoseconds(), Err: err}
		}
		var workers sync.WaitGroup
		if p.Writers == 1 {
			for claimant := range count {
				submit(claimant)
			}
		} else {
			for claimant := range count {
				workers.Go(func() { submit(claimant) })
			}
		}
		commits, aborts := 0, 0
		for range count {
			event := <-events
			if err := a.accept(event); err != nil {
				result = errors.Join(result, err)
				cancel()
			}
			if event.Attempt.Classification == "committed" {
				commits++
			}
			if event.Attempt.Classification == "aborted" {
				aborts++
			}
		}
		workers.Wait()
		cancel()
		if result != nil {
			return result
		}
		if commits != 1 || aborts != count-1 {
			return errMismatch
		}
	}
	return nil
}
func graphContentionSpecFromEnv() (graphContentionSpec, string, error) {
	var spec graphContentionSpec
	raw := os.Getenv("RHO_GRAPH_WORKLOAD_SPEC")
	run := os.Getenv("RHO_GRAPH_WORKLOAD_RUN")
	if len(raw) == 0 || len(raw) > 1024 {
		return spec, "", errInvalid
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&spec); err != nil {
		return spec, "", errors.Join(errInvalid, err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return spec, "", errors.Join(errInvalid, err)
	}
	if spec.Version != 1 || spec.Family != graphContentionFamily || (spec.Layout != "kv" && spec.Layout != "columns") || !validGraphContentionConfig(graphContentionConfig(spec.profile(), spec.Layout, run)) {
		return spec, "", errInvalid
	}
	return spec, run, nil
}
func runGraphContention(db fdb.Database, spec graphContentionSpec, run string) (err error) {
	c := graphContentionConfig(spec.profile(), spec.Layout, run)
	if spec.Version != 1 || spec.Family != graphContentionFamily || !validGraphContentionConfig(c) {
		return errInvalid
	}
	h, err := newHarness(db, c)
	if err != nil {
		return err
	}
	binding, err := h.graphContentionInstall()
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(os.Getenv("FDB_COST_ARTIFACT"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	initial, err := h.fresh([]uint8{0, 1}, nil)
	if err != nil {
		return err
	}
	a, err := newGraphContentionArtifacts(h, root, spec, binding, initial)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, a.Close()) }()
	p := c.Cost
	// Separate explicit source-reader policy; no deletion/retirement in this family.
	source, err := newGraphHistoryRetention(h, initial, graphHistoryPolicy{MaxOwners: 64, MaxControlBytes: 512 << 10, PageRows: 32, PageBytes: 4 << 20, MaxWorkRows: 16*p.Facts + 64, MaxWorkBytes: (64 << 20) + (16*p.Facts+64)*(512<<10), MaxChainSteps: 128, MaxOutputBytes: 4096})
	if err != nil {
		return err
	}
	apply := func(job graphContentionJob) error {
		start := time.Now()
		var outputErr error
		err := h.graphContentionApply(context.Background(), job, 0, func(event graphContentionEvent) { outputErr = errors.Join(outputErr, a.accept(event)) })
		if err != nil || outputErr != nil {
			return errors.Join(err, outputErr)
		}
		op, _ := graphContentionOperation(p, job)
		return a.accept(graphContentionEvent{Done: true, Operation: op, Client: 0, ElapsedNanos: time.Since(start).Nanoseconds()})
	}
	for fact := uint64(0); fact < 3; fact++ {
		if err := apply(graphContentionJob{Phase: "setup", Fact: fact}); err != nil {
			return err
		}
	}
	for fact := uint64(0); fact < p.Facts/2; fact++ {
		if err := apply(graphContentionJob{Phase: "seed", Fact: fact}); err != nil {
			return err
		}
	}
	old, err := h.fresh([]uint8{0, 1}, nil)
	if err != nil {
		return err
	}
	if err = a.barrier("old", old); err != nil {
		return err
	}
	var secret [32]byte
	if _, err = rand.Read(secret[:]); err != nil {
		return err
	}
	token, err := source.pin("workload-old", old, secret)
	if err != nil {
		return err
	}
	held, err := source.open(context.Background(), token)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, historyReadCleanup(held), source.release("workload-old-close", token)) }()
	var columns *graphCostColumnLane
	if spec.Layout == "columns" {
		columns, err = a.columnStart(initial, old)
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, columnReadCleanup(columns.held), columns.Close()) }()
	}
	if err = h.graphContentionHot(a); err != nil {
		return err
	}
	if err = h.graphContentionClaims(a); err != nil {
		return err
	}
	final, err := h.fresh([]uint8{0, 1}, nil)
	if err != nil {
		return err
	}
	if err = a.barrier("final", final); err != nil {
		return err
	}
	if _, err = a.probeWalk("old-held", held, func(graphCostRawRow) error { return nil }); err != nil {
		return err
	}
	if columns != nil {
		if err = a.columnFinish(columns, final); err != nil {
			return err
		}
	}
	if err = historyReadCleanup(held); err != nil {
		return err
	}
	if err = source.release("workload-old-close", token); err != nil {
		return err
	}
	if err = a.finish(source); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(a.end)
}
