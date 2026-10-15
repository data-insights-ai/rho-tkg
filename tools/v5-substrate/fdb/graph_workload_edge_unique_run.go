package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type graphEdgeUniqueSpec graphContentionSpec

func (s graphEdgeUniqueSpec) profile() graphCostProfile {
	return graphCostProfile{Version: 4, Facts: s.Facts, CrossPercent: s.CrossPercent, Writers: s.Writers}
}
func graphEdgeUniqueSpecFromEnv() (graphEdgeUniqueSpec, string, error) {
	var s graphEdgeUniqueSpec
	raw := os.Getenv("RHO_GRAPH_WORKLOAD_SPEC")
	if len(raw) == 0 || len(raw) > 4096 {
		return s, "", errInvalid
	}
	d := json.NewDecoder(bytes.NewBufferString(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&s); err != nil {
		return s, "", errors.Join(errInvalid, err)
	}
	if err := d.Decode(new(any)); err != io.EOF || s.Version != 1 || s.Family != graphEdgeUniqueFamily || !graphEdgeUniqueValid(s.profile()) || s.Layout != "kv" && s.Layout != "columns" {
		return s, "", errInvalid
	}
	run := os.Getenv("RHO_GRAPH_WORKLOAD_RUN")
	c := graphEdgeUniqueConfig(s.profile(), s.Layout, run)
	if !validGraphEdgeUniqueConfig(c) {
		return s, "", errInvalid
	}
	return s, run, nil
}

type graphEdgeUniqueJob struct {
	Phase           string
	Fact, Candidate uint64
	Command         graphCommand
	Ordinal         uint64
	Frozen          *graphEdgeUniqueFrozen
}
type graphEdgeUniqueCapture struct {
	Metadata    [2]metadata
	ReadVersion int64
	Reads       [2][]graphCostStoredRecord
	UniqueRange []graphCostStoredRecord
	Begin, End  []byte
}
type graphEdgeUniqueFrozen struct {
	Job      graphEdgeUniqueJob
	Tx       transaction
	Capture  graphEdgeUniqueCapture
	Body     []byte
	Callback int
}
type graphEdgeUniqueAttempt struct {
	Family, Engine, Phase, PreviousRequest string
	Fact, Candidate, Ordinal               uint64
	Client, Attempt, ReadCallbacks         int
	Capture                                graphEdgeUniqueCapture
	Proposal                               []byte
	Digest                                 [32]byte
	Submissions                            []graphCostParallelSubmission
	Proof                                  graphCostParallelProof
	Classification, Error                  string
}
type graphEdgeUniqueEvent struct {
	Attempt *graphEdgeUniqueAttempt
	Job     graphEdgeUniqueJob
	Proof   graphCostParallelProof
	Client  int
	Done    bool
	Err     error
	Timing  *graphEdgeUniqueTiming
}

type graphEdgeUniqueClockKey struct{}
type graphEdgeUniqueTiming struct {
	Phase                                               string
	Fact, Candidate, Ordinal                            uint64
	Client                                              int
	StartNanos, StopNanos, ElapsedNanos, AdmissionNanos int64
	Attempts, Submissions, ReadCallbacks, SDKCallbacks  uint64
	Commit                                              bool
}

func (h *harness) graphEdgeUniqueInstall(ctx context.Context) (graphCostStoredRecord, error) {
	body, err := wire(graphEdgeUniquePolicy(h.config))
	if err != nil || !validGraphEdgeUniqueConfig(h.config) {
		return graphCostStoredRecord{}, errors.Join(errInvalid, err)
	}
	key := h.key(0, "workload-edge-unique-v1", "binding")
	_, err = graphCostWriteContext(ctx, h.db, func(tr fdb.Transaction) (any, error) {
		for g := uint8(0); g < 2; g++ {
			if err := h.checkConfig(tr, g); err != nil {
				return nil, err
			}
			m, err := h.meta(tr, g)
			if err != nil || m.Transactions != 0 || m.Groups != 0 || m.Versions != 0 || m.Certificates != 0 {
				return nil, errors.Join(errUnavailable, err)
			}
		}
		prior, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		if prior != nil {
			if !bytes.Equal(prior, body) {
				return nil, errMismatch
			}
			return nil, nil
		}
		m, err := h.meta(tr, 0)
		if err != nil {
			return nil, err
		}
		added := uint64(len(key) + len(body))
		if added > h.config.Limits.Bytes-m.Bytes {
			return nil, errLimit
		}
		m.Bytes += added
		tr.Set(key, body)
		return nil, put(tr, h.key(0, "meta", "state"), m)
	})
	if err != nil {
		return graphCostStoredRecord{}, graphColumnCommitError(err)
	}
	got, err := graphCostReadContext(ctx, h.db, func(tr fdb.ReadTransaction) (any, error) {
		if err := h.graphEdgeUniqueBindingIn(tr); err != nil {
			return nil, err
		}
		actual, err := tr.Get(key).Get()
		return graphCostStoredRecord{bytes.Clone(key), bytes.Clone(actual)}, err
	})
	if err != nil {
		return graphCostStoredRecord{}, err
	}
	return got.(graphCostStoredRecord), nil
}

func (h *harness) graphEdgeUniqueFreeze(ctx context.Context, job graphEdgeUniqueJob, client, ordinal int, previous string) (graphEdgeUniqueFrozen, error) {
	var frozen graphEdgeUniqueFrozen
	if h == nil || ctx == nil || job.Frozen != nil || job.Ordinal == 0 || client < 0 || client >= h.config.Cost.Writers || ordinal < 1 || ordinal > 64 || (ordinal == 1) != (previous == "") {
		return frozen, errInvalid
	}
	type captured struct {
		Snapshot snapshot
		Actual   graphEdgeUniqueCapture
	}
	got, callbacks, err := graphCostParallelRead(ctx, h.db, func(tr fdb.ReadTransaction) (captured, error) {
		c := captured{Snapshot: snapshot{Values: [2]map[string]value{{}, {}}}}
		if err := h.graphEdgeUniqueBindingIn(tr); err != nil {
			return c, err
		}
		keys := graphEdgeUniqueReadKeys(job.Command)
		for g := uint8(0); g < 2; g++ {
			if err := h.checkConfig(tr, g); err != nil {
				return c, err
			}
			m, err := h.meta(tr, g)
			if err != nil {
				return c, err
			}
			c.Actual.Metadata[g], c.Snapshot.Generations[g] = m, m.Generation
			for _, logical := range keys[g] {
				key := h.key(g, "current", logical)
				raw, err := tr.Get(key).Get()
				if err != nil {
					return c, err
				}
				c.Actual.Reads[g] = append(c.Actual.Reads[g], graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)})
				if raw != nil {
					v, err := decode[value](raw)
					if err != nil || !validGraphValue(v) {
						return c, errors.Join(errInvalid, err)
					}
					c.Snapshot.Values[g][logical] = v
				}
			}
		}
		if job.Command.Kind == "edge-create" {
			// A real native range read also binds the observed empty/singleton
			// unique bucket; the key is not an invented empty-ID marker.
			key := h.key(0, "current", graphEdgeUniqueKey(job.Command.Edge.Value))
			end := append(bytes.Clone(key), 0)
			rows, err := tr.GetRange(fdb.KeyRange{Begin: key, End: fdb.Key(end)}, fdb.RangeOptions{Limit: 2}).GetSliceWithError()
			if err != nil || len(rows) > 1 {
				return c, errors.Join(errInvalid, err)
			}
			c.Actual.Begin, c.Actual.End = bytes.Clone(key), end
			for _, row := range rows {
				c.Actual.UniqueRange = append(c.Actual.UniqueRange, graphCostStoredRecord{bytes.Clone(row.Key), bytes.Clone(row.Value)})
			}
		}
		c.Actual.ReadVersion, err = tr.GetReadVersion().Get()
		return c, err
	})
	if err != nil {
		return frozen, err
	}
	x := h.graphEdgeUniqueTransaction(fmt.Sprintf("edge-unique-%s-%d-%d-c%d-a%d", job.Phase, job.Fact, job.Candidate, client, ordinal), job.Command, got.Snapshot)
	m := lifecycleMetadata{Provenance: job.Ordinal}
	if job.Command.Kind == "edge-create" || job.Command.Kind == "edge-close" {
		m.EdgeLife = job.Command.Edge.ID + 1
		if job.Command.Kind == "edge-create" {
			m.Definition = &lifecycleDefinition{graphEdgeUniqueValueID(h.config.Cost, job.Fact), "i64", job.Command.Edge.Value}
		}
	}
	x.Lifecycle = &m
	body, err := wire(x)
	if err == nil {
		err = h.validate(x)
	}
	if err != nil || len(body) > 8<<10 {
		return frozen, errors.Join(errLimit, err)
	}
	return graphEdgeUniqueFrozen{job, x, got.Actual, body, callbacks}, nil
}

// Unknown outcomes repeat only the SAME bytes. Only paired, authenticated
// generation/revision ABORT authorizes a new linked request/body. A real unique
// or old-life ABORT is a terminal loser, never silently promoted to success.
func (h *harness) graphEdgeUniqueApply(parent context.Context, job graphEdgeUniqueJob, client int, emit func(graphEdgeUniqueEvent)) (graphCostParallelProof, error) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	previous := ""
	maxAttempts := 64
	if job.Phase == "setup" || job.Phase == "life" || job.Phase == "stale" || job.Phase == "frozen-close" {
		maxAttempts = 1
	}
	for ordinal := 1; ordinal <= maxAttempts; ordinal++ {
		var frozen graphEdgeUniqueFrozen
		var err error
		if ordinal == 1 && job.Frozen != nil {
			frozen = *job.Frozen
		} else {
			copyJob := job
			copyJob.Frozen = nil
			frozen, err = h.graphEdgeUniqueFreeze(ctx, copyJob, client, ordinal, previous)
		}
		a := graphEdgeUniqueAttempt{Family: graphEdgeUniqueFamily, Engine: "foundationdb", Phase: job.Phase, PreviousRequest: previous, Fact: job.Fact, Candidate: job.Candidate, Ordinal: job.Ordinal, Client: client, Attempt: ordinal, Capture: frozen.Capture, Proposal: frozen.Body, Digest: sha256.Sum256(frozen.Body), ReadCallbacks: frozen.Callback}
		if err != nil {
			a.Classification, a.Error = "preparation-error", err.Error()
			emit(graphEdgeUniqueEvent{Attempt: &a})
			return graphCostParallelProof{}, err
		}
		var proof graphCostParallelProof
		for invocation := 1; invocation <= 8; invocation++ {
			sub := h.graphCostParallelSubmit(ctx, frozen.Tx, frozen.Body)
			sub.Ordinal = invocation
			a.Submissions = append(a.Submissions, sub)
			var callbacks int
			proof, callbacks, err = h.graphCostReadParallelOriginal(ctx, frozen.Tx, frozen.Body)
			a.ReadCallbacks += callbacks
			if err == nil {
				if sub.Error == "" && sub.Outcome != proof.Outcome {
					err = errMismatch
				}
				break
			}
			if !errors.Is(err, errUnknown) || sub.Error == "" || !sub.Retryable && !sub.MaybeCommitted || ctx.Err() != nil {
				break
			}
		}
		a.Proof = proof
		if err != nil {
			a.Classification, a.Error = "incomplete", err.Error()
			emit(graphEdgeUniqueEvent{Attempt: &a})
			return proof, err
		}
		if proof.Outcome.Commit {
			// Full canonical group, not just its proposal, must fit the fresh
			// column envelope before the lane can advertise complete publication.
			if len(proof.Original.Value) > 8<<10 {
				err = errLimit
			}
			a.Classification = "committed"
		} else {
			a.Classification = "aborted"
		}
		emit(graphEdgeUniqueEvent{Attempt: &a})
		if err != nil || proof.Outcome.Commit || !graphCostParallelReplan(proof.Outcome) || ordinal == maxAttempts {
			return proof, err
		}
		previous = frozen.Tx.Request
	}
	return graphCostParallelProof{}, errLimit
}

// A fixed window owns at most32 frozen requests/results; no fact-sized map.
// All admitted jobs are drained before workers and files may close.
func (h *harness) graphEdgeUniqueWindow(ctx context.Context, jobs []graphEdgeUniqueJob, emit func(graphEdgeUniqueEvent) error) error {
	if len(jobs) < 1 || len(jobs) > 32 || emit == nil {
		return errInvalid
	}
	origin, ok := ctx.Value(graphEdgeUniqueClockKey{}).(time.Time)
	if !ok || origin.IsZero() {
		return errInvalid
	}
	queued := time.Now()
	queues := make([]chan graphEdgeUniqueJob, h.config.Cost.Writers)
	events := make(chan graphEdgeUniqueEvent, 32)
	var workers sync.WaitGroup
	for client := range queues {
		queues[client] = make(chan graphEdgeUniqueJob, 1)
		workers.Go(func() {
			for job := range queues[client] {
				began := time.Now()
				timing := graphEdgeUniqueTiming{Phase: job.Phase, Fact: job.Fact, Candidate: job.Candidate, Ordinal: job.Ordinal, Client: client, StartNanos: queued.Sub(origin).Nanoseconds(), AdmissionNanos: began.Sub(queued).Nanoseconds()}
				proof, err := h.graphEdgeUniqueApply(ctx, job, client, func(e graphEdgeUniqueEvent) {
					if e.Attempt != nil {
						timing.Attempts++
						timing.ReadCallbacks += uint64(e.Attempt.ReadCallbacks)
						timing.Submissions += uint64(len(e.Attempt.Submissions))
						for _, sub := range e.Attempt.Submissions {
							timing.SDKCallbacks += uint64(sub.Callbacks)
						}
					} // #nosec G115 -- admitted nonnegative SDK counters.
					events <- e
				})
				ended := time.Now()
				timing.StopNanos = ended.Sub(origin).Nanoseconds()
				timing.ElapsedNanos = ended.Sub(queued).Nanoseconds()
				timing.Commit = proof.Outcome.Commit
				events <- graphEdgeUniqueEvent{Job: job, Proof: proof, Client: client, Done: true, Err: err, Timing: &timing}
			}
		})
	}
	var failed error
	started, done := 0, 0
	for done < len(jobs) {
		var queue chan graphEdgeUniqueJob
		var job graphEdgeUniqueJob
		if started < len(jobs) && failed == nil {
			job = jobs[started]
			queue = queues[(job.Ordinal-1)%uint64(len(queues))]
		}
		select {
		case queue <- job:
			started++
		case event := <-events:
			failed = errors.Join(failed, event.Err, emit(event))
			if event.Done {
				done++
			}
		}
		if failed != nil && done == started {
			break
		}
	}
	for _, queue := range queues {
		close(queue)
	}
	workers.Wait()
	return failed
}

// The disk recipe is bounded per line and contains request bytes, not owner
// authority. Every replay still resolves the genuine paired native originals.
func graphEdgeUniqueRecipes(file *os.File, consume func(graphEdgeUniqueFrozen) error) error {
	if file == nil || consume == nil {
		return errInvalid
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), 128<<10)
	for scanner.Scan() {
		var frozen graphEdgeUniqueFrozen
		if err := json.Unmarshal(scanner.Bytes(), &frozen); err != nil {
			return err
		}
		if frozen.Job.Frozen != nil || len(frozen.Body) == 0 || len(frozen.Body) > 8<<10 {
			return errInvalid
		}
		if err := consume(frozen); err != nil {
			return err
		}
	}
	return scanner.Err()
}
