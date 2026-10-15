package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type graphEdgeUniqueEnd struct {
	Version                                                uint8               `json:"version"`
	Format                                                 string              `json:"format"`
	Spec                                                   graphEdgeUniqueSpec `json:"spec"`
	Config                                                 config
	Binding                                                graphCostStoredRecord
	Initial, Old, Final                                    certificate
	Attempts, Aborted, Committed                           uint64
	Completed                                              [32]uint64
	OldMetadata, FinalMetadata                             [2]metadata
	Sources                                                [3]graphCostStreamEnd
	Columns                                                [2]graphColumnManifest
	OldHeldRows                                            [2]uint64
	Views                                                  [2]graphSnapshotEnd
	AttemptPayloadSHA256                                   string
	LatencyPayloadSHA256                                   string
	LogicalOperations                                      uint64
	SetupNanos, BusinessNanos, DiagnosticNanos, TotalNanos int64
}
type graphEdgeUniqueArtifacts struct {
	ctx         context.Context
	h           *harness
	root        *os.Root
	file        *os.File
	replays     *os.File
	payload     hash.Hash
	end         graphEdgeUniqueEnd
	latency     *json.Encoder
	latencyHash hash.Hash
}

func (a *graphEdgeUniqueArtifacts) write(v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(body)+1 > 512<<10 {
		return errLimit
	}
	_, err = io.MultiWriter(a.file, a.payload).Write(append(body, '\n'))
	return err
}
func (a *graphEdgeUniqueArtifacts) accept(e graphEdgeUniqueEvent) error {
	if e.Attempt != nil {
		x := e.Attempt
		if err := a.write(struct {
			Type    string
			Attempt *graphEdgeUniqueAttempt
		}{"attempt", x}); err != nil {
			return err
		}
		a.end.Attempts++
		if a.end.Attempts > a.h.config.Limits.Transactions {
			return errLimit
		}
		switch x.Classification {
		case "committed":
			a.end.Committed++
		case "aborted":
			a.end.Aborted++
		default:
			return errors.Join(errUnavailable, e.Err)
		}
	}
	if e.Done && e.Err == nil {
		if e.Timing == nil || e.Timing.Ordinal != e.Job.Ordinal || e.Timing.Client != e.Client || e.Timing.ElapsedNanos <= 0 || e.Timing.AdmissionNanos < 0 || e.Timing.AdmissionNanos > e.Timing.ElapsedNanos || e.Timing.StopNanos-e.Timing.StartNanos != e.Timing.ElapsedNanos {
			return errMismatch
		}
		if err := a.latency.Encode(e.Timing); err != nil {
			return err
		}
		a.end.LogicalOperations++
		o := e.Proof.Outcome
		mustAbort := e.Job.Phase == "stale" || e.Job.Phase == "frozen-close"
		if mustAbort && o.Commit || !mustAbort && e.Job.Phase != "claim" && !o.Commit {
			return errMismatch
		}
		if mustAbort && (e.Job.Phase == "stale" && o.Reason != "endpoint-life" || e.Job.Phase == "frozen-close" && o.Reason != "revision") {
			return errMismatch
		}
		if e.Client < 0 || e.Client >= a.h.config.Cost.Writers {
			return errInvalid
		}
		a.end.Completed[e.Client]++
		if e.Job.Phase == "close" {
			if err := json.NewEncoder(a.replays).Encode(e.Proof); err != nil {
				return err
			}
		}
	}
	return e.Err
}
func (a *graphEdgeUniqueArtifacts) barrier(c cut) ([2]metadata, error) {
	result, err := graphCostReadContext(a.ctx, a.h.db, func(tr fdb.ReadTransaction) (any, error) {
		var m [2]metadata
		if err := a.h.verifyCut(tr, c); err != nil {
			return nil, err
		}
		if err := a.h.graphEdgeUniqueBindingIn(tr); err != nil {
			return nil, err
		}
		for g := uint8(0); g < 2; g++ {
			var err error
			m[g], err = a.h.meta(tr, g)
			if err != nil {
				return nil, err
			}
			if m[g].Groups != c.record.Groups[g] || m[g].Floor != c.record.Round {
				return nil, errMismatch
			}
		}
		if m[0].Transactions+m[1].Transactions != a.end.Attempts {
			return nil, errMismatch
		}
		return m, nil
	})
	if err != nil {
		return [2]metadata{}, err
	}
	return result.([2]metadata), nil
}
func (h *harness) graphEdgeUniqueWinner(ctx context.Context, fact uint64) (uint64, error) {
	winner, _, err := graphCostParallelRead(ctx, h.db, func(tr fdb.ReadTransaction) (uint64, error) {
		if err := h.graphEdgeUniqueBindingIn(tr); err != nil {
			return 0, err
		}
		v, found, err := get[value](tr, h.key(0, "current", graphEdgeUniqueKey(int64(fact+5)))) // #nosec G115 -- Facts<=1M.
		if err != nil || !found || !validGraphValue(v) || v.Deleted || v.Value <= 0 {
			return 0, errors.Join(errUnavailable, err)
		}
		f, c, err := graphEdgeUniqueIdentity(h.config.Cost, uint64(v.Value)) // #nosec G115 -- positive above.
		if err != nil || f != fact {
			return 0, errMismatch
		}
		e, err := graphEdgeUniqueEdge(h.config.Cost, fact, c, false)
		if err != nil {
			return 0, err
		}
		rows, err := h.graphRows(tr, 0, fmt.Sprintf("edge/%016x/", e.ID))
		if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].Key, h.key(0, "current", edgeKey("edge", e))) {
			return 0, errors.Join(errMismatch, err)
		}
		actual, err := decode[value](rows[0].Value)
		if err != nil || !validGraphValue(actual) || actual.Deleted || actual.Value != e.Value {
			return 0, errors.Join(errMismatch, err)
		}
		for _, endpoint := range []endpoint{e.Source, e.Target} {
			live, err := h.nativeLive(tr, endpoint)
			if err != nil || !live {
				return 0, errors.Join(errMismatch, err)
			}
		}
		return c, nil
	})
	return winner, err
}

func runGraphEdgeUnique(db fdb.Database, spec graphEdgeUniqueSpec, run string) (err error) {
	started := time.Now()
	c := graphEdgeUniqueConfig(spec.profile(), spec.Layout, run)
	if spec.Version != 1 || spec.Family != graphEdgeUniqueFamily || !validGraphEdgeUniqueConfig(c) {
		return errInvalid
	}
	seconds := 1800
	if raw := os.Getenv("FDB_COST_SECONDS"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 || n > 1800 {
			return errInvalid
		}
		seconds = n
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(seconds)*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, graphEdgeUniqueClockKey{}, started)
	h, err := newHarnessContext(ctx, db, c)
	if err != nil {
		return err
	}
	binding, err := h.graphEdgeUniqueInstall(ctx)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(os.Getenv("FDB_COST_ARTIFACT"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	file, _, err := openGraphCostArtifact(root, "fdb-workload-attempts.jsonl")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	replays, err := root.OpenFile("fdb-edge-unique-old-close-originals.jsonl", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, replays.Close()) }()
	fresh := func() (cut, error) {
		value, e := graphCostWriteContext(ctx, h.db, func(tr fdb.Transaction) (any, error) { return h.freshIn(tr, []uint8{0, 1}, nil) })
		if e != nil {
			return cut{}, e
		}
		return value.(cut), nil
	}
	initial, err := fresh()
	if err != nil {
		return err
	}
	a := &graphEdgeUniqueArtifacts{ctx: ctx, h: h, root: root, file: file, replays: replays, payload: sha256.New(), end: graphEdgeUniqueEnd{Version: 1, Format: graphEdgeUniqueFormat, Spec: spec, Config: c, Binding: binding, Initial: initial.record}}
	latency, _, err := openGraphCostArtifact(root, "fdb-workload-latency.jsonl")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, latency.Close()) }()
	a.latencyHash = sha256.New()
	a.latency = json.NewEncoder(io.MultiWriter(latency, a.latencyHash))
	if err = a.write(struct {
		Type string
		End  graphEdgeUniqueEnd
	}{"header", a.end}); err != nil {
		return err
	}
	p := c.Cost
	a.end.SetupNanos = time.Since(started).Nanoseconds()
	businessStarted := time.Now()
	apply := func(job graphEdgeUniqueJob) error {
		return h.graphEdgeUniqueWindow(ctx, []graphEdgeUniqueJob{job}, a.accept)
	}
	for i, node := range graphEdgeUniqueNodes(p) {
		if err = apply(graphEdgeUniqueJob{Phase: "setup", Fact: uint64(i), Command: graphCommand{Kind: "node-create", Node: node}, Ordinal: uint64(i + 1)}); err != nil {
			return err
		}
	} // #nosec G115 -- fixed3 nodes.
	// Freeze each bounded pair-window before ANY candidate in it can submit.
	// All candidates therefore truly observed empty unique ranges, even atW1.
	for first := uint64(0); first < p.Facts; first += 16 {
		jobs := make([]graphEdgeUniqueJob, 0, 32)
		for fact := first; fact < min(first+16, p.Facts); fact++ {
			for candidate := uint64(0); candidate < 2; candidate++ {
				e, _ := graphEdgeUniqueEdge(p, fact, candidate, false)
				job := graphEdgeUniqueJob{Phase: "claim", Fact: fact, Candidate: candidate, Command: graphCommand{Kind: "edge-create", Edge: e}, Ordinal: 4 + 2*fact + candidate}
				frozen, e1 := h.graphEdgeUniqueFreeze(ctx, job, int((job.Ordinal-1)%uint64(p.Writers)), 1, "")
				if e1 != nil {
					return e1
				}
				if len(frozen.Capture.UniqueRange) != 0 {
					return errMismatch
				}
				job.Frozen = &frozen
				jobs = append(jobs, job)
			}
		}
		if err = h.graphEdgeUniqueWindow(ctx, jobs, a.accept); err != nil {
			return err
		}
		for fact := first; fact < min(first+16, p.Facts); fact++ {
			if _, err = h.graphEdgeUniqueWinner(ctx, fact); err != nil {
				return err
			}
		}
	}
	old, err := fresh()
	if err != nil {
		return err
	}
	a.end.Old = old.record
	if a.end.OldMetadata, err = a.barrier(old); err != nil {
		return err
	}
	if old.record.Groups[0]+old.record.Groups[1] != 3+p.Facts {
		return errMismatch
	}
	// A genuine durable source owner keeps old-cut history independently of
	// column/cache lifetimes. No retirement is claimed by this workload.
	source, err := newGraphHistoryRetentionContext(ctx, h, initial, graphHistoryPolicy{MaxOwners: 64, MaxControlBytes: 512 << 10, PageRows: 32, PageBytes: 4 << 20, MaxWorkRows: 16*p.Facts + 64, MaxWorkBytes: (64 << 20) + (16*p.Facts+64)*(512<<10), MaxChainSteps: 128, MaxOutputBytes: 4096})
	if err != nil {
		return err
	}
	var secret [32]byte
	if _, err = rand.Read(secret[:]); err != nil {
		return err
	}
	token, err := source.pin("edge-unique-old", old, secret)
	if err != nil {
		return err
	}
	held, err := source.open(ctx, token)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, historyReadCleanup(held), source.release("edge-unique-old-close", token))
	}()
	recipes, err := root.OpenFile("fdb-edge-unique-frozen-close.jsonl", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, recipes.Close()) }()
	for fact := uint64(0); fact < p.Facts; fact++ {
		winner, e1 := h.graphEdgeUniqueWinner(ctx, fact)
		if e1 != nil {
			return e1
		}
		e, _ := graphEdgeUniqueEdge(p, fact, winner, false)
		e.Value = 0
		job := graphEdgeUniqueJob{Phase: "frozen-close", Fact: fact, Candidate: winner, Command: graphCommand{Kind: "edge-close", Edge: e}, Ordinal: 4 + 4*p.Facts + fact}
		frozen, e1 := h.graphEdgeUniqueFreeze(ctx, job, int((job.Ordinal-1)%uint64(p.Writers)), 1, "")
		if e1 != nil {
			return e1
		}
		if err = json.NewEncoder(recipes).Encode(frozen); err != nil {
			return err
		}
	}
	if err = recipes.Sync(); err != nil {
		return err
	}
	var columns *graphColumnStore
	a.end.BusinessNanos = time.Since(businessStarted).Nanoseconds()
	if spec.Layout == "columns" {
		policy, e1 := graphEdgeUniqueColumnPolicy(p)
		if e1 != nil {
			return e1
		}
		columns, e1 = newGraphColumnStoreContext(ctx, h, policy)
		if e1 != nil {
			return e1
		}
		a.end.Columns[0], err = columns.publish(ctx, "old", initial, old)
		if err != nil {
			return err
		}
	}
	businessStarted = time.Now()
	for i, remote := range []bool{false, true} {
		for k, reopened := range []bool{false, true} {
			kind := "node-close"
			if reopened {
				kind = "node-create"
			}
			node := graphEdgeUniqueNode(p, remote, reopened)
			if err = apply(graphEdgeUniqueJob{Phase: "life", Fact: uint64(i*2 + k), Command: graphCommand{Kind: kind, Node: node}, Ordinal: 4 + 6*p.Facts + uint64(i*2+k)}); err != nil {
				return err
			} // #nosec G115 -- fixed4 operations.
		}
	}
	var pending []graphEdgeUniqueJob
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		e := h.graphEdgeUniqueWindow(ctx, pending, a.accept)
		pending = nil
		return e
	}
	appendJob := func(job graphEdgeUniqueJob) error {
		pending = append(pending, job)
		if len(pending) == 32 {
			return flush()
		}
		return nil
	}
	// Replay the saved old winner recipe only to choose the disjoint candidate;
	// all actual stale/replacement authority is freshly read by each invocation.
	for _, phase := range []string{"stale", "replace", "frozen-close", "close"} {
		err = graphEdgeUniqueRecipes(recipes, func(frozen graphEdgeUniqueFrozen) error {
			job := frozen.Job
			job.Phase = phase
			switch phase {
			case "stale", "replace":
				job.Candidate = 1 - job.Candidate
				e, _ := graphEdgeUniqueEdge(p, job.Fact, job.Candidate, phase == "replace")
				job.Command = graphCommand{Kind: "edge-create", Edge: e}
				job.Ordinal = 4 + 2*p.Facts + job.Fact
				if phase == "replace" {
					job.Ordinal += p.Facts
				}
			case "frozen-close":
				job.Frozen = &frozen
			case "close":
				job.Ordinal = 4 + 5*p.Facts + job.Fact
			}
			return appendJob(job)
		})
		if err != nil {
			return err
		}
		if err = flush(); err != nil {
			return err
		}
	}
	if err = a.replayOldCloses(); err != nil {
		return err
	}
	final, err := fresh()
	if err != nil {
		return err
	}
	a.end.Final = final.record
	if a.end.FinalMetadata, err = a.barrier(final); err != nil {
		return err
	}
	n, _ := graphEdgeUniqueCounts(p)
	if final.record.Groups[0]+final.record.Groups[1] != n.Groups || a.end.Committed != n.Groups || a.end.FinalMetadata[0].Versions+a.end.FinalMetadata[1].Versions != n.Versions {
		return errMismatch
	}
	a.end.BusinessNanos += time.Since(businessStarted).Nanoseconds()
	if a.end.OldHeldRows, err = held.walk(ctx, func(graphCostRawRow) error { return nil }); err != nil {
		return err
	}
	if a.end.OldHeldRows[0]+a.end.OldHeldRows[1] != 3+4*p.Facts {
		return errMismatch
	}
	if err = a.views(source, old, final); err != nil {
		return err
	}
	if columns != nil {
		a.end.Columns[1], err = columns.publish(ctx, "final", initial, final)
		if err != nil {
			return err
		}
		if err = a.columns(columns, old, final); err != nil {
			return err
		}
	}
	a.end.Sources, err = a.rawArtifacts(initial, old, final, graphCostStreamPolicy{128, 16 << 20})
	if err != nil {
		return err
	}
	if a.end.Sources[0].Rows[0]+a.end.Sources[0].Rows[1] != n.Versions || a.end.Sources[1].Rows[0]+a.end.Sources[1].Rows[1] != 5+7*p.Facts || a.end.Sources[2].Rows[0]+a.end.Sources[2].Rows[1] != n.Groups {
		return errMismatch
	}
	if err = historyReadCleanup(held); err != nil {
		return err
	}
	if err = source.release("edge-unique-old-close", token); err != nil {
		return err
	}
	a.end.AttemptPayloadSHA256 = hex.EncodeToString(a.payload.Sum(nil))
	a.end.LatencyPayloadSHA256 = hex.EncodeToString(a.latencyHash.Sum(nil))
	a.end.TotalNanos = time.Since(started).Nanoseconds()
	a.end.DiagnosticNanos = a.end.TotalNanos - a.end.SetupNanos - a.end.BusinessNanos
	if a.end.BusinessNanos <= 0 || a.end.DiagnosticNanos < 0 || a.end.LogicalOperations != 6*p.Facts+7 {
		return errMismatch
	}
	if err = json.NewEncoder(file).Encode(struct {
		Type string
		End  graphEdgeUniqueEnd
	}{"footer", a.end}); err != nil {
		return err
	}
	result, _, err := openGraphCostArtifact(root, "fdb-workload-result.json")
	if err != nil {
		return err
	}
	if err = errors.Join(json.NewEncoder(result).Encode(a.end), result.Close()); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(a.end)
}

func (a *graphEdgeUniqueArtifacts) rawArtifacts(initial, old, final cut, p graphCostStreamPolicy) (complete [3]graphCostStreamEnd, err error) {
	header, err := a.h.graphCostArtifactHeaderContext(a.ctx, initial, old, final, p)
	if err != nil {
		return complete, err
	}
	for i, name := range []string{"history", "current", "cdc"} {
		header.Stream = name
		complete[i], err = writeGraphCostRawArtifact(a.root, header, func(emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
			if name == "cdc" {
				return a.h.graphCostWalkChangesContext(a.ctx, initial, final, p, emit)
			}
			from := final
			if name == "history" {
				from = old
			}
			return a.h.graphCostWalkRowsContext(a.ctx, from, final, p, emit, name == "history")
		})
		if err != nil {
			return complete, err
		}
	}
	return complete, nil
}

func (a *graphEdgeUniqueArtifacts) replayOldCloses() error {
	if _, err := a.replays.Seek(0, io.SeekStart); err != nil {
		return err
	}
	scanner := bufio.NewScanner(a.replays)
	scanner.Buffer(make([]byte, 64<<10), 512<<10)
	var count uint64
	for scanner.Scan() {
		var before graphCostParallelProof
		if err := json.Unmarshal(scanner.Bytes(), &before); err != nil {
			return err
		}
		x := before.Group.Tx
		if !before.Outcome.Commit || x.GraphChange == nil || x.GraphChange.Kind != "edge-close" {
			return errInvalid
		}
		body, err := wire(x)
		if err != nil {
			return err
		}
		metadataIn := func(tr fdb.ReadTransaction) ([2]metadata, error) {
			var m [2]metadata
			for g := uint8(0); g < 2; g++ {
				var e error
				m[g], e = a.h.meta(tr, g)
				if e != nil {
					return m, e
				}
			}
			return m, nil
		}
		m0, _, err := graphCostParallelRead(a.ctx, a.h.db, metadataIn)
		if err != nil {
			return err
		}
		sub := a.h.graphCostParallelSubmit(a.ctx, x, body)
		after, _, err := a.h.graphCostReadParallelOriginal(a.ctx, x, body)
		if err != nil {
			return err
		}
		m1, _, err := graphCostParallelRead(a.ctx, a.h.db, metadataIn)
		if err != nil {
			return err
		}
		if m0 != m1 || !graphColumnSame(before, after) || sub.Error != "" && !sub.MaybeCommitted && !sub.Retryable || sub.Error == "" && sub.Outcome != after.Outcome {
			return errMismatch
		}
		if err = a.write(struct {
			Type          string
			Before, After graphCostParallelProof
			Metadata      [2]metadata
			Submission    graphCostParallelSubmission
		}{"old-close-replay", before, after, m1, sub}); err != nil {
			return err
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if count != a.h.config.Cost.Facts {
		return errMismatch
	}
	return nil
}

func (a *graphEdgeUniqueArtifacts) views(source *graphHistoryRetention, old, final cut) (err error) {
	file, _, err := openGraphCostArtifact(a.root, "fdb-workload-views.jsonl")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	encode := json.NewEncoder(file)
	n := a.h.config.Cost.Facts
	policy := graphSnapshotPolicy{PageRows: 16, PageBytes: 4 << 20, InventoryBytes: 32 << 20, WorkRows: 256*n + 4096, WorkBytes: (64 << 20) + (256*n+4096)*(64<<10), ChainSteps: 16, Cleanup: graphSnapshotCleanupDefault()}
	for i, at := range []cut{old, final} {
		read, e := source.OpenGraphSnapshot(a.ctx, graphSnapshotRequest{Mode: "At", Scope: []uint8{0, 1}, At: at.record}, policy)
		if e != nil {
			return e
		}
		e = encode.Encode(struct {
			Type      string
			Index     int
			Policy    graphSnapshotPolicy
			Selection graphSnapshotSelection
		}{"header", i, policy, read.selection})
		if e == nil {
			a.end.Views[i], e = read.Scan(a.ctx, func(row graphSnapshotRow) error {
				return encode.Encode(struct {
					Type  string
					Index int
					Row   graphSnapshotRow
				}{"row", i, row})
			})
		}
		if e == nil && a.end.Views[i].Rows[0]+a.end.Views[i].Rows[1] != 3+4*n {
			e = errMismatch
		}
		if e == nil {
			e = encode.Encode(struct {
				Type  string
				Index int
				End   graphSnapshotEnd
			}{"footer", i, a.end.Views[i]})
		}
		e = errors.Join(e, read.Close())
		if e != nil {
			return e
		}
	}
	return nil
}

// Every emitted row re-authenticates its original and Before/After histories;
// cache is verified against native chunks, never a substitute source.
func (a *graphEdgeUniqueArtifacts) columns(s *graphColumnStore, old, final cut) (err error) {
	file, _, err := openGraphCostArtifact(a.root, "fdb-workload-columns.jsonl")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	encode := json.NewEncoder(file)
	for i, at := range []cut{old, final} {
		id := "old"
		if i == 1 {
			id = "final"
		}
		m := a.end.Columns[i]
		if err = encode.Encode(struct {
			Type     string
			Binding  graphColumnBinding
			Manifest graphColumnManifest
		}{"header", s.binding, m}); err != nil {
			return err
		}
		for ordinal := uint64(0); ordinal < m.Chunks; ordinal++ {
			result, e := graphCostReadContext(a.ctx, s.h.db, func(tr fdb.ReadTransaction) (any, error) { return tr.Get(s.chunkKey(id, ordinal)).Get() })
			if e != nil {
				return e
			}
			body := result.([]byte)
			if len(body) < graphColumnHeader || len(body) > s.binding.Policy.ChunkBytes {
				return errInvalid
			}
			cache, e := a.root.OpenFile(graphColumnCacheName(m.Generation, ordinal), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if e != nil {
				return e
			}
			_, e = cache.Write(body)
			if e = errors.Join(e, cache.Close()); e != nil {
				return e
			}
		}
		var counts [2]uint64
		walk := func(cache *os.Root) error {
			_, e := s.walk(a.ctx, id, at, cache, func(row graphColumnRow) error {
				counts[row.Coordinator]++
				return encode.Encode(struct {
					Type, Generation string
					Cached           bool
					Row              graphColumnRow
				}{"row", id, cache != nil, row})
			})
			return e
		}
		if err = walk(nil); err != nil {
			return err
		}
		if err = walk(a.root); err != nil {
			return err
		}
		if counts != [2]uint64{2 * at.record.Groups[0], 2 * at.record.Groups[1]} {
			return errMismatch
		}
		if err = encode.Encode(struct {
			Type, Generation string
			Counts           [2]uint64
		}{"footer", id, counts}); err != nil {
			return err
		}
	}
	return nil
}
