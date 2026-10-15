package main

// #define FDB_API_VERSION 730
// #include <foundationdb/fdb_c.h>
import "C"

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const (
	graphCostParallelAttempts        = 64
	graphCostParallelSubmissions     = 8
	graphCostParallelCallbacks       = 16
	graphCostParallelWindow          = 32
	graphCostParallelTimeoutMillis   = 3000
	graphCostParallelOperationMillis = 30000
	graphCostParallelFormat          = "fdb-graph-cost-parallel-v1"
	graphCostParallelColumnFormat    = "fdb-graph-cost-parallel-columns-v1"
)

// These are fixture policies, not a qualified capacity. The persisted fresh
// binding makes distinct attempts, SAME-wire submissions and SDK retries finite.
type graphCostOperation struct {
	Dataset       [32]byte
	Phase         string
	Fact, Ordinal uint64
}
type graphCostParallelBinding struct {
	Version                                           uint8
	Config                                            config
	Dataset                                           [32]byte
	Window, MaxAttempts, MaxSubmissions, MaxCallbacks int
	TransactionMillis, OperationMillis                int
}
type graphCostParallelSubmission struct {
	Ordinal, Callbacks        int
	Outcome                   outcome
	Error                     string
	Retryable, MaybeCommitted bool
	cause                     error
}
type graphCostParallelAttempt struct {
	Operation                             graphCostOperation
	Engine                                string
	ClientID, AttemptOrdinal              int
	PreviousRequest, Request, Transaction string
	BodySHA256                            [32]byte
	Proposal                              []byte
	Submissions                           []graphCostParallelSubmission
	ReadCallbacks                         int
	Classification, Error                 string
	Originals                             [2]graphCostStoredRecord
	Original                              graphCostStoredRecord
}
type graphCostParallelBarrier struct {
	Cut                       certificate
	Metadata                  [2]metadata
	Records                   [2]graphCostStoredRecord
	Attempts, AbortedAttempts uint64
}
type graphCostParallelEnd struct {
	Format                                                                    string
	Binding                                                                   graphCostParallelBinding
	BindingRecord                                                             graphCostStoredRecord
	Old, Final                                                                graphCostParallelBarrier
	Attempts, AbortedAttempts, SubmitInvocations, SDKCallbacks, ReadCallbacks uint64
	ClientCompletions                                                         [32]uint64
	PayloadSHA256                                                             string
}

func (e graphCostParallelEnd) complete(p graphCostProfile) bool {
	format := graphCostParallelFormat
	if graphCostRetainedConfig(e.Binding.Config) {
		format = graphCostRetentionParallelFormat
	}
	if p.Writers != 8 && p.Writers != 32 || !validGraphCostProfile(e.Binding.Config) || e.Binding.Config.Cost != p || e.Format != format || e.Binding != graphCostParallelPolicy(graphCostConfig(e.Binding.Config.Graph, p)) ||
		e.Attempts != 3+2*p.Facts+e.AbortedAttempts || e.Attempts > 3+2*p.Facts*graphCostParallelAttempts ||
		e.Old.Attempts != 3+p.Facts+e.Old.AbortedAttempts || e.Old.Attempts > 3+p.Facts*graphCostParallelAttempts || e.Final.Attempts != e.Attempts || e.Final.AbortedAttempts != e.AbortedAttempts ||
		e.SubmitInvocations < e.Attempts || e.SubmitInvocations > e.Attempts*graphCostParallelSubmissions ||
		e.SDKCallbacks < e.SubmitInvocations || e.SDKCallbacks > e.SubmitInvocations*graphCostParallelCallbacks || e.ReadCallbacks < 2*e.Attempts || e.ReadCallbacks > e.Attempts*graphCostParallelCallbacks*(1+graphCostParallelSubmissions) || len(e.PayloadSHA256) != 64 {
		return false
	}
	var completed uint64
	for i, n := range e.ClientCompletions {
		if i >= p.Writers && n != 0 || i < p.Writers && p.Facts > uint64(p.Writers) && n == 0 {
			return false
		}
		completed += n
	}
	body, err := wire(e.Binding)
	key := (&harness{config: e.Binding.Config}).key(0, "cost-parallel", "binding")
	_, sealErr := hex.DecodeString(e.PayloadSHA256)
	return completed == 2*p.Facts && err == nil && sealErr == nil && bytes.Equal(e.BindingRecord.Key, key) && bytes.Equal(e.BindingRecord.Value, body)
}
func graphCostOperationID(p graphCostProfile, phase string, fact uint64) graphCostOperation {
	var raw [25]byte
	raw[0] = p.Version
	binary.BigEndian.PutUint64(raw[1:9], p.Facts)
	binary.BigEndian.PutUint64(raw[9:17], p.CrossPercent)
	copy(raw[17:], "graph-v2")
	ordinal := fact + 1
	if phase == "create" {
		ordinal = 4 + fact
	}
	if phase == "update" {
		ordinal = 4 + p.Facts + fact
	}
	return graphCostOperation{sha256.Sum256(raw[:]), phase, fact, ordinal}
}
func graphCostParallelPolicy(c config) graphCostParallelBinding {
	return graphCostParallelBinding{1, c, graphCostOperationID(c.Cost, "setup", 0).Dataset,
		graphCostParallelWindow, graphCostParallelAttempts, graphCostParallelSubmissions, graphCostParallelCallbacks,
		graphCostParallelTimeoutMillis, graphCostParallelOperationMillis}
}
func graphCostBodyDigest(body []byte) [32]byte { return sha256.Sum256(body) }

func (h *harness) graphCostInstallParallel() (graphCostStoredRecord, error) {
	binding := graphCostParallelPolicy(h.config)
	body, err := wire(binding)
	if err != nil {
		return graphCostStoredRecord{}, err
	}
	key := h.key(0, "cost-parallel", "binding")
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
		old, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		if len(old) != 0 {
			if !bytes.Equal(old, body) {
				return nil, errMismatch
			}
			return nil, nil // Exact retry only; already funded, not a resumed run.
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
		return graphCostStoredRecord{}, err
	}
	// Submitted tr.Set bytes are not an observation. Capture the genuine stored
	// immutable binding after commit, under both actual full config checks.
	observed, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		for g := uint8(0); g < 2; g++ {
			if err := h.checkConfig(tr, g); err != nil {
				return nil, err
			}
		}
		raw, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 || len(raw) > 64<<10 {
			return nil, errUnavailable
		}
		stored, err := decode[graphCostParallelBinding](raw)
		if err != nil || stored != binding || !bytes.Equal(raw, body) {
			return nil, errors.Join(errMismatch, err)
		}
		return graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)}, nil
	})
	if err != nil {
		return graphCostStoredRecord{}, err
	}
	return observed.(graphCostStoredRecord), nil
}

// A decoded outcome is authority only when BOTH genuine immutable originals
// bind this actual request, ID and exact body digest. Missing is never ABORT.
func (h *harness) graphCostParallelOriginal(x transaction, body []byte, pair [2]graphCostStoredRecord) (outcome, error) {
	if h == nil || len(body) == 0 || len(body) > 64<<10 {
		return outcome{}, errInvalid
	}
	if err := h.validate(x); err != nil {
		return outcome{}, err
	}
	canonical, err := wire(x)
	if err != nil || !bytes.Equal(canonical, body) {
		return outcome{}, errors.Join(errMismatch, err)
	}
	if len(pair[0].Value) == 0 && len(pair[1].Value) == 0 {
		return outcome{}, errUnknown
	}
	if len(pair[0].Value) == 0 || len(pair[1].Value) == 0 {
		return outcome{}, errUnavailable
	}
	if !bytes.Equal(pair[0].Key, h.key(x.Coordinator, "id", x.ID)) || !bytes.Equal(pair[1].Key, h.key(x.Coordinator, "request", x.Request)) || !bytes.Equal(pair[0].Value, pair[1].Value) {
		return outcome{}, errMismatch
	}
	o, err := decode[outcome](pair[0].Value)
	if err != nil {
		return outcome{}, err
	}
	if o.ID != x.ID || o.Request != x.Request || o.Coordinator != x.Coordinator || o.Digest != graphCostBodyDigest(body) {
		return outcome{}, errMismatch
	}
	if o.Commit {
		if o.Reason != "" || o.Round == 0 || o.Sequence == 0 || o.Sequence > h.config.Limits.Transactions || o.Round <= x.Dependency {
			return outcome{}, errInvalid
		}
	} else if o.Round != 0 || o.Sequence != 0 || (o.Reason != "generation" && o.Reason != "revision" && o.Reason != "endpoint-life" && o.Reason != "edge-identity" && o.Reason != "graph-shape" && !(h.config.Cost.Version == 4 && o.Reason == "unique")) {
		return outcome{}, errInvalid
	}
	return o, nil
}
func graphCostParallelReplan(o outcome) bool {
	return !o.Commit && (o.Reason == "generation" || o.Reason == "revision")
}

// Classification comes from the actual native error predicates. Definite
// invalid/config/storage errors are not wrapped into an ambiguous outcome.
func graphCostParallelError(err error) (retryable, maybe bool) {
	if e, ok := errors.AsType[fdb.Error](err); ok {
		if e.Code < 0 || e.Code > 1<<31-1 {
			return false, false
		}
		code := C.fdb_error_t(e.Code) // #nosec G115 -- Native error code admitted in signed32 range.
		return C.fdb_error_predicate(C.int(fdb.ErrorPredicateRetryable), code) != 0,
			C.fdb_error_predicate(C.int(fdb.ErrorPredicateMaybeCommitted), code) != 0
	}
	return errors.Is(err, errLimit), errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// Database retry callbacks own no external IO. Cancellation remains registered
// through Commit/OnError, and a callback admission guard complements RetryLimit.
func graphCostParallelRead[T any](ctx context.Context, db fdb.Database, fn func(fdb.ReadTransaction) (T, error)) (T, int, error) {
	var captured T
	callbacks := 0
	var stop func() bool
	result, err := db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		if stop == nil {
			stop = context.AfterFunc(ctx, tr.Cancel)
		}
		callbacks++
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if callbacks > graphCostParallelCallbacks {
			return nil, errLimit
		}
		if err := tr.Options().SetTimeout(graphCostParallelTimeoutMillis); err != nil {
			return nil, err
		}
		if err := tr.Options().SetRetryLimit(graphCostParallelCallbacks - 1); err != nil {
			return nil, err
		}
		var readErr error
		captured, readErr = fn(tr)
		return captured, readErr
	})
	if stop != nil {
		stop()
	}
	if err != nil {
		return captured, callbacks, err
	}
	return result.(T), callbacks, nil
}
func (h *harness) graphCostParallelSubmit(ctx context.Context, x transaction, body []byte) graphCostParallelSubmission {
	row := graphCostParallelSubmission{}
	var stop func() bool
	result, err := h.db.Transact(func(tr fdb.Transaction) (any, error) {
		if stop == nil {
			stop = context.AfterFunc(ctx, tr.Cancel)
		}
		row.Callbacks++
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if row.Callbacks > graphCostParallelCallbacks {
			return nil, errLimit
		}
		if err := tr.Options().SetTimeout(graphCostParallelTimeoutMillis); err != nil {
			return nil, err
		}
		if err := tr.Options().SetRetryLimit(graphCostParallelCallbacks - 1); err != nil {
			return nil, err
		}
		if err := h.graphTransportCheckIn(tr); err != nil {
			return nil, err
		}
		return h.execute(tr, x, graphCostBodyDigest(body))
	})
	if stop != nil {
		stop()
	}
	if err != nil {
		row.cause = err
		row.Error = err.Error()
		row.Retryable, row.MaybeCommitted = graphCostParallelError(err)
	} else {
		row.Outcome = result.(outcome)
	}
	return row
}

type graphCostParallelProof struct {
	Pair     [2]graphCostStoredRecord
	Original graphCostStoredRecord
	Group    changeGroup
	Outcome  outcome
}

func (h *harness) graphCostReadParallelOriginal(ctx context.Context, x transaction, body []byte) (graphCostParallelProof, int, error) {
	if graphOwnerSelected(h.config) {
		return graphCostParallelProof{}, 0, errStale
	}
	return graphCostParallelRead(ctx, h.db, func(tr fdb.ReadTransaction) (graphCostParallelProof, error) {
		return h.graphCostParallelOriginalIn(tr, x, body)
	})
}

// One canonical retained-original reader. Owned callers authenticate their
// current native catalog in this SAME read transaction before invoking it.
func (h *harness) graphCostParallelOriginalIn(tr fdb.ReadTransaction, x transaction, body []byte) (graphCostParallelProof, error) {
	var proof graphCostParallelProof
	if err := h.graphTransportCheckIn(tr); err != nil {
		return proof, err
	}
	if err := h.checkConfig(tr, x.Coordinator); err != nil {
		return proof, err
	}
	for i, key := range []fdb.Key{h.key(x.Coordinator, "id", x.ID), h.key(x.Coordinator, "request", x.Request)} {
		raw, err := tr.Get(key).Get()
		if err != nil {
			return proof, err
		}
		if len(raw) > 64<<10 {
			return proof, errLimit
		}
		proof.Pair[i] = graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)}
	}
	o, err := h.graphCostParallelOriginal(x, body, proof.Pair)
	if err != nil {
		return proof, err
	}
	proof.Outcome = o
	if !o.Commit {
		return proof, nil
	}
	key := h.changeKey(o.Coordinator, o.Sequence, o.Request)
	raw, err := tr.Get(key).Get()
	if err != nil {
		return proof, err
	}
	group, err := decode[changeGroup](raw)
	if err != nil {
		return proof, errors.Join(errUnavailable, err)
	}
	if err := h.validateChange(group); err != nil {
		return proof, err
	}
	if !reflect.DeepEqual(group.Tx, x) || group.Outcome != o {
		return proof, errMismatch
	}
	proof.Group, proof.Original = group, graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)}
	return proof, nil
}

type graphCostParallelEvent struct {
	Attempt *graphCostParallelAttempt
	Group   changeGroup
	Sample  graphCostSample
	Client  int
	Done    bool
	Err     error
}
type graphCostParallelJob struct {
	Phase string
	Fact  uint64
}

func (h *harness) graphCostParallelApply(parent context.Context, job graphCostParallelJob, client int, emit func(graphCostParallelEvent)) (graphCostSample, error) {
	return h.graphCostApplyOperation(parent, job, client, graphCostOperationID(h.config.Cost, job.Phase, job.Fact), "cost", emit)
}

// Fresh workload callers supply their separately bound operation/factory
// namespace; the historical cost wrapper retains its exact operation and IDs.
func (h *harness) graphCostApplyOperation(parent context.Context, job graphCostParallelJob, client int, op graphCostOperation, identity string, emit func(graphCostParallelEvent)) (graphCostSample, error) {
	p := h.config.Cost
	if !validName(identity) || emit == nil || parent == nil || client < 0 || client >= p.Writers {
		return graphCostSample{}, errInvalid
	}
	command := graphCommand{Kind: "edge-" + job.Phase, Edge: p.edge(job.Fact, 5)}
	if job.Phase == "update" {
		command.Edge.Value = 6
	}
	maxAttempts := graphCostParallelAttempts
	if job.Phase == "setup" {
		command = graphCommand{Kind: "node-create", Node: p.nodes()[job.Fact]}
		maxAttempts = 1
	}
	ctx, cancel := context.WithTimeout(parent, graphCostParallelOperationMillis*time.Millisecond)
	defer cancel()
	sample := graphCostSample{Phase: job.Phase, Fact: job.Fact, Cross: job.Phase != "setup" && p.cross(job.Fact)}
	previous := ""
	for ordinal := 1; ordinal <= maxAttempts; ordinal++ {
		attempt := graphCostParallelAttempt{Operation: op, Engine: "foundationdb", ClientID: client, AttemptOrdinal: ordinal, PreviousRequest: previous}
		begin := time.Now()
		before, reads, err := graphCostParallelRead(ctx, h.db, func(tr fdb.ReadTransaction) (snapshot, error) {
			if err := h.graphTransportCheckIn(tr); err != nil {
				return snapshot{}, err
			}
			return h.graphCostPointSnapshotIn(tr, command)
		})
		attempt.ReadCallbacks += reads
		sample.ProposalAttempts += reads
		sample.ProposalNanos += time.Since(begin).Nanoseconds()
		if err != nil {
			attempt.Classification, attempt.Error = "preparation-error", err.Error()
			emit(graphCostParallelEvent{Attempt: &attempt})
			return sample, err
		}
		coordinator := uint8(0)
		if job.Phase == "setup" {
			coordinator = command.Node.Owner
		}
		x := h.graphTransaction(fmt.Sprintf("%s-%s-%d-c%d-a%d", identity, job.Phase, job.Fact, client, ordinal), coordinator, command, before)
		m := lifecycleMetadata{Provenance: op.Ordinal}
		if job.Phase != "setup" {
			id := uint64(5)
			if job.Phase == "update" {
				id = 6
			}
			m.EdgeLife, m.Definition = command.Edge.ID+1, &lifecycleDefinition{ID: id, Kind: "i64", I64: command.Edge.Value}
		}
		x.Lifecycle = &m
		body, err := wire(x)
		if err == nil {
			err = h.validate(x)
		}
		if err != nil {
			attempt.Classification, attempt.Error = "invalid-proposal", err.Error()
			emit(graphCostParallelEvent{Attempt: &attempt})
			return sample, err
		}
		attempt.Request, attempt.Transaction, attempt.Proposal, attempt.BodySHA256 = x.Request, x.ID, body, graphCostBodyDigest(body)
		var proof graphCostParallelProof
		var finalErr error
		for invocation := 1; invocation <= graphCostParallelSubmissions; invocation++ {
			begin = time.Now()
			submission := h.graphCostParallelSubmit(ctx, x, body)
			submission.Ordinal = invocation
			attempt.Submissions = append(attempt.Submissions, submission)
			sample.CommitAttempts += submission.Callbacks
			sample.AckNanos += time.Since(begin).Nanoseconds()
			begin = time.Now()
			var readErr error
			proof, reads, readErr = h.graphCostReadParallelOriginal(ctx, x, body)
			attempt.ReadCallbacks += reads
			sample.VerificationNanos += time.Since(begin).Nanoseconds()
			if readErr == nil {
				if submission.Error == "" && submission.Outcome != proof.Outcome {
					finalErr = errMismatch
					break
				}
				finalErr = nil
				break
			}
			if !errors.Is(readErr, errUnknown) {
				finalErr = readErr
				break
			}
			if submission.Error == "" {
				finalErr = errUnavailable
				break
			}
			// Durable retirement-busy is graph authority, not an SDK predicate or
			// ABORT. Fresh retention runner/artifact wiring must record it separately.
			if !submission.Retryable && !submission.MaybeCommitted && !errors.Is(submission.cause, errHistoryBusy) {
				finalErr = submission.cause
				break
			}
			finalErr = errors.Join(errUnknown, submission.cause)
			if ctx.Err() != nil {
				break
			}
		}
		attempt.Originals, attempt.Original = proof.Pair, proof.Original
		if finalErr != nil {
			attempt.Classification, attempt.Error = "incomplete", finalErr.Error()
			emit(graphCostParallelEvent{Attempt: &attempt})
			return sample, finalErr
		}
		sample.Outcome = proof.Outcome
		if proof.Outcome.Commit {
			attempt.Classification = "committed"
			// Existing native validator checks every physical effect. These strict
			// fixture before/after checks also refuse a stale/current substitution.
			for _, row := range proof.Group.Writes {
				if job.Phase == "update" && (row.Before == nil || row.Before.Deleted || row.Before.Value != 5 || row.After.Deleted || row.After.Value != 6) ||
					job.Phase == "create" && (row.Before != nil || row.After.Deleted || row.After.Value != 5) ||
					job.Phase == "setup" && (row.Before != nil || row.After.Deleted || row.After.Value != 1) {
					finalErr = errMismatch
				}
			}
			if finalErr != nil {
				attempt.Classification, attempt.Error = "incomplete", finalErr.Error()
			}
			emit(graphCostParallelEvent{Attempt: &attempt, Group: proof.Group})
			return sample, finalErr
		}
		attempt.Classification = "aborted"
		emit(graphCostParallelEvent{Attempt: &attempt})
		if !graphCostParallelReplan(proof.Outcome) {
			return sample, errors.Join(errUnavailable, fmt.Errorf("retained refusal: %s", proof.Outcome.Reason))
		}
		previous = x.Request // Only this authenticated original authorizes replan.
	}
	return sample, errLimit // No useful completion, never an invented ABORT.
}

func (h *harness) graphCostParallelBarrier(cert cut, e graphCostParallelEnd) (graphCostParallelBarrier, error) {
	result, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		if err := h.verifyCut(tr, cert); err != nil {
			return nil, err
		}
		barrier := graphCostParallelBarrier{Cut: cert.record, Attempts: e.Attempts, AbortedAttempts: e.AbortedAttempts}
		for g := uint8(0); g < 2; g++ {
			if err := h.checkConfig(tr, g); err != nil {
				return nil, err
			}
			m, err := h.meta(tr, g)
			if err != nil {
				return nil, err
			}
			if m.Groups != cert.record.Groups[g] {
				return nil, errMismatch
			}
			key := h.key(g, "meta", "state")
			raw, err := tr.Get(key).Get()
			if err != nil {
				return nil, err
			}
			barrier.Metadata[g], barrier.Records[g] = m, graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)}
		}
		if barrier.Metadata[0].Transactions+barrier.Metadata[1].Transactions != e.Attempts {
			return nil, errMismatch
		}
		return barrier, nil
	})
	if err != nil {
		return graphCostParallelBarrier{}, err
	}
	return result.(graphCostParallelBarrier), nil
}

func runGraphCostParallel(db fdb.Database, p graphCostProfile) (err error) {
	started := time.Now()
	layout := os.Getenv("RHO_GRAPH_COST_LAYOUT")
	if p.Writers != 8 && p.Writers != 32 || layout != "" && layout != "kv" && layout != "columns" {
		return errInvalid
	}
	graph := "graph-cost-parallel-v1"
	if layout == "columns" {
		graph = "graph-cost-parallel-columns-v1"
	}
	retained, err := graphCostRetentionSelected()
	if err != nil {
		return err
	}
	if retained {
		graph = graphCostRetentionGraph(layout, p.Writers)
	}
	if os.Getenv("FDB_COST_ARTIFACT") == "" {
		return errInvalid
	}
	root, err := os.OpenRoot(os.Getenv("FDB_COST_ARTIFACT"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	c := graphCostConfig(graph, p)
	h, err := newHarness(db, c)
	if err != nil {
		return err
	}
	binding, err := h.graphCostInstallParallel()
	if err != nil {
		return err
	}
	attemptFile, _, err := openGraphCostArtifact(root, "fdb-cost-attempts.jsonl")
	if err != nil {
		return err
	}
	latency, samples, err := openGraphCostArtifact(root, "fdb-cost-latency.jsonl")
	if err != nil {
		return errors.Join(err, attemptFile.Close())
	}
	cdc, groups, err := openGraphCostArtifact(root, "fdb-cost-operation-groups.jsonl")
	if err != nil {
		return errors.Join(err, attemptFile.Close(), latency.Close())
	}
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, attemptFile.Close(), latency.Close(), cdc.Close())
		}
	}()
	var payload hash.Hash = sha256.New()
	attempts := json.NewEncoder(io.MultiWriter(attemptFile, payload))
	end := graphCostParallelEnd{Format: graphCostParallelFormat, Binding: graphCostParallelPolicy(c), BindingRecord: binding}
	if retained {
		end.Format = graphCostRetentionParallelFormat
	}
	if err := attempts.Encode(struct {
		Type, Format string
		Binding      graphCostParallelBinding
		Record       graphCostStoredRecord
	}{"header", end.Format, end.Binding, binding}); err != nil {
		return err
	}
	result := graphCostResult{Version: 4, Engine: "foundationdb", Spec: p, Config: c, Deployment: "single-machine distributed: four logical fault-domain containers, triple ssd", SetupNodes: 3, Creates: p.Facts, Updates: p.Facts, IDAdmission: "fixed declared fixture IDs/ranges; independent SDK clients, no rho allocator protocol"}
	initial, err := h.fresh([]uint8{0, 1}, nil)
	if err != nil {
		return err
	}
	result.InitialCut = initial.record
	var retention *graphCostRetentionLane
	if retained {
		retention, err = newGraphCostRetentionLane(h, root, initial)
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, retention.Close()) }()
	}
	var columns *graphCostColumnLane
	if layout == "columns" {
		columns, err = newGraphCostColumnLane(h, root, initial)
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, columns.Close()) }()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The owner drains all started workers after any error. No worker writes an
	// artifact; terminal/unknown rows remain even when a cell cannot reach EOF.
	accept := func(event graphCostParallelEvent) error {
		if event.Attempt != nil {
			a := event.Attempt
			if err := attempts.Encode(struct {
				Type    string
				Attempt *graphCostParallelAttempt
			}{"attempt", a}); err != nil {
				return err
			}
			end.Attempts++
			end.ReadCallbacks += uint64(a.ReadCallbacks)
			for _, s := range a.Submissions {
				end.SubmitInvocations++
				end.SDKCallbacks += uint64(s.Callbacks)
			}
			if a.Classification == "aborted" {
				end.AbortedAttempts++
			}
			if a.Classification == "committed" {
				if err := groups.Encode(event.Group); err != nil {
					return err
				}
				result.CDCGroups++
				result.PhysicalGraphChanges += uint64(len(event.Group.Writes))
				logical := uint64(1)
				if a.Operation.Phase == "create" {
					logical = 2
				}
				result.LogicalComponentChanges += logical
			}
		}
		if event.Done && event.Err == nil {
			s := event.Sample
			if err := samples.Encode(struct {
				graphCostSample
				Operation graphCostOperation
				ClientID  int
			}{s, graphCostOperationID(p, s.Phase, s.Fact), event.Client}); err != nil {
				return err
			}
			if s.Phase != "setup" {
				end.ClientCompletions[event.Client]++
				result.WritesNanos += s.ProposalNanos + s.AckNanos
				if s.Phase == "create" {
					result.CompletedCreates++
				} else {
					result.CompletedUpdates++
				}
			}
		}
		return event.Err
	}
	single := func(job graphCostParallelJob) error {
		var emitErr error
		sample, err := h.graphCostParallelApply(ctx, job, 0, func(e graphCostParallelEvent) { emitErr = errors.Join(emitErr, accept(e)) })
		return errors.Join(emitErr, accept(graphCostParallelEvent{Done: true, Sample: sample, Client: 0, Err: err}))
	}
	for i := range 3 {
		if err := single(graphCostParallelJob{"setup", uint64(i)}); err != nil {
			return err
		}
	}
	result.SetupNanos = time.Since(started).Nanoseconds()
	phase := func(name string) error {
		// fact0 creates the actual value5/6 definition before any dependent work.
		if err := single(graphCostParallelJob{name, 0}); err != nil {
			return err
		}
		jobs := make([]chan graphCostParallelJob, p.Writers)
		events := make(chan graphCostParallelEvent, graphCostParallelWindow)
		var workers sync.WaitGroup
		for client := range p.Writers {
			jobs[client] = make(chan graphCostParallelJob, 1)
			workers.Go(func() {
				for job := range jobs[client] {
					sample, err := h.graphCostParallelApply(ctx, job, client, func(e graphCostParallelEvent) { events <- e })
					events <- graphCostParallelEvent{Done: true, Sample: sample, Client: client, Err: err}
				}
			})
		}
		var phaseErr error
		for first := uint64(1); first < p.Facts && phaseErr == nil; first += graphCostParallelWindow {
			n := min(uint64(graphCostParallelWindow), p.Facts-first)
			// At most32 jobs are started; each client owns at mostceil(32/Writers)
			// jobs. Dispatch/drain together avoids a full inbox deadlock at8.
			started, done := uint64(0), uint64(0)
			for done < n {
				var inbox chan graphCostParallelJob
				job := graphCostParallelJob{name, first + started}
				if started < n && phaseErr == nil {
					inbox = jobs[int(job.Fact%uint64(p.Writers))]
				}
				select {
				case inbox <- job:
					started++
				case event := <-events:
					if err := accept(event); err != nil {
						phaseErr = errors.Join(phaseErr, err)
						cancel()
					}
					if event.Done {
						done++
					}
				}
				if phaseErr != nil {
					n = started
				} // Drain every admitted job only.
			}
		}
		for _, inbox := range jobs {
			close(inbox)
		}
		workers.Wait()
		return phaseErr
	}
	var old cut
	for _, name := range []string{"create", "update"} {
		if err := phase(name); err != nil {
			return err
		}
		cert, err := h.fresh([]uint8{0, 1}, nil)
		if err != nil {
			return err
		}
		barrier, err := h.graphCostParallelBarrier(cert, end)
		if err != nil {
			return err
		}
		if name == "create" {
			old, result.OldCut, end.Old = cert, cert.record, barrier
			if retention != nil {
				if err := retention.holdOld(cert); err != nil {
					return err
				}
			}
			if columns != nil {
				if err := columns.holdOld(cert); err != nil {
					return err
				}
			}
		} else {
			result.FinalCut, end.Final = cert.record, barrier
		}
		if err := attempts.Encode(struct {
			Type, Phase string
			Barrier     graphCostParallelBarrier
		}{"barrier", name, barrier}); err != nil {
			return err
		}
	}
	begin := time.Now()
	end.PayloadSHA256 = hex.EncodeToString(payload.Sum(nil))
	if !end.complete(p) {
		return errMismatch
	}
	if err := json.NewEncoder(attemptFile).Encode(struct {
		Type string
		End  graphCostParallelEnd
	}{"footer", end}); err != nil {
		return err
	}
	closed = true
	if err := errors.Join(attemptFile.Close(), latency.Close(), cdc.Close()); err != nil {
		return err
	}
	if columns != nil {
		columnEnd, err := columns.finish(cut{result.FinalCut})
		if err != nil {
			return err
		}
		result.Version, result.OracleFormat, result.Columns = 5, graphCostParallelColumnFormat, &columnEnd
	} else {
		result.OracleFormat = graphCostParallelFormat
	}
	if retention != nil {
		retentionEnd, err := retention.finish(cut{result.FinalCut}, columns)
		if err != nil {
			return err
		}
		result.Version, result.OracleFormat, result.Retention, result.Parallel = 6, graphCostRetentionFormat, &retentionEnd, &end
		result.StreamEnds = retentionEnd.Sources
		result.VerificationNanos, result.TotalNanos = time.Since(begin).Nanoseconds(), time.Since(started).Nanoseconds()
		return publishGraphCostResult(root, result)
	}
	ends, err := h.graphCostArtifacts(root, initial, old, cut{result.FinalCut}, graphCostStreamPolicy{128, 16 << 20})
	if err != nil {
		return err
	}
	if ends[2].Rows[0]+ends[2].Rows[1] != 3+2*p.Facts || ends[0].Rows[0]+ends[0].Rows[1] != 3+6*p.Facts || ends[1].Rows[0]+ends[1].Rows[1] != 3+3*p.Facts || result.CDCGroups != 3+2*p.Facts || result.LogicalComponentChanges != 3+3*p.Facts || result.PhysicalGraphChanges != 3+6*p.Facts {
		return errMismatch
	}
	result.StreamEnds, result.Parallel = ends, &end
	result.VerificationNanos, result.TotalNanos = time.Since(begin).Nanoseconds(), time.Since(started).Nanoseconds()
	return publishGraphCostResult(root, result)
}
