package main

import (
	"bytes"
	"context"
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

const graphSelectorsFamily = "snapshot-selectors"
const graphSelectorsFormat = "fdb-snapshot-selectors-v1"

type graphSelectorsSpec struct {
	Version      uint8  `json:"version"`
	Family       string `json:"family"`
	Facts        uint64 `json:"facts"`
	CrossPercent uint64 `json:"crossPercent"`
	Writers      int    `json:"writers"`
	Layout       string `json:"layout"`
}

func (s graphSelectorsSpec) profile() graphCostProfile {
	return graphCostProfile{3, s.Facts, s.CrossPercent, s.Writers}
}
func graphSelectorsLimit(p graphCostProfile) limits {
	originals := 3 + 2*p.Facts*graphCostParallelAttempts
	return limits{Bytes: (64 << 20) + p.Facts*(64<<10) + originals*2048, Transactions: originals, Versions: 3 + 6*p.Facts, Certificates: 1024}
}
func graphSelectorsConfig(spec graphSelectorsSpec, run string) config {
	return graphCostConfig("graph-workload-selectors-v1-"+spec.Layout+"-"+run, spec.profile())
}
func validGraphSelectorsConfig(c config) bool {
	p := c.Cost
	if p.Writers != 1 && p.Writers != 8 && p.Writers != 32 {
		return false
	}
	firstJobs := uint64(p.Writers + 1) // #nosec G115 -- admitted writers1/8/32.
	if p.Version != 3 || p.Facts < 20 || p.Facts < firstJobs || p.Facts > 1_000_000 || p.Facts%20 != 0 || p.CrossPercent != 0 && p.CrossPercent != 10 && p.CrossPercent != 50 && p.CrossPercent != 100 || c.Lifecycle != (lifecycleProfile{}) || c.CostScope != lifecycleConfig("validation").Lifecycle.Scope {
		return false
	}
	name := strings.TrimPrefix(c.Graph, "graph-workload-selectors-v1-")
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

type graphSelectorsBinding struct {
	Version                                  uint8
	Config                                   config
	Dataset                                  [32]byte
	Window, Attempts, Submissions, Callbacks int
	Read                                     graphSnapshotPolicy
	History                                  graphHistoryPolicy
}

func graphSelectorsPolicy(c config) graphSelectorsBinding {
	p := c.Cost
	datasetProfile := p
	datasetProfile.Version = 1
	return graphSelectorsBinding{1, c, graphCostOperationID(datasetProfile, "setup", 0).Dataset, 32, 64, 8, 16,
		graphSnapshotPolicy{PageRows: 8, PageBytes: 4 << 20, InventoryBytes: 4 << 20, WorkRows: 1024*p.Facts + 4096, WorkBytes: (64 << 20) + p.Facts*(2<<20), ChainSteps: 128, Cleanup: graphSnapshotCleanupDefault()},
		graphHistoryPolicy{MaxOwners: 64, MaxControlBytes: 4 << 20, PageRows: 32, PageBytes: 4 << 20, MaxWorkRows: 32*p.Facts + 4096, MaxWorkBytes: (64 << 20) + p.Facts*(1<<20), MaxChainSteps: 128, MaxOutputBytes: 4096}}
}
func (h *harness) graphSelectorsBindingIn(tr fdb.ReadTransaction) error {
	raw, err := tr.Get(h.key(0, "workload-selectors-v1", "binding")).Get()
	if err != nil {
		return err
	}
	actual, err := decode[graphSelectorsBinding](raw)
	if err != nil || actual != graphSelectorsPolicy(h.config) {
		return errors.Join(errMismatch, err)
	}
	return nil
}
func (h *harness) graphSelectorsInstall() (graphCostStoredRecord, error) {
	key := h.key(0, "workload-selectors-v1", "binding")
	body, err := wire(graphSelectorsPolicy(h.config))
	if err != nil {
		return graphCostStoredRecord{}, err
	}
	_, err = h.db.Transact(func(tr fdb.Transaction) (any, error) {
		for g := uint8(0); g < 2; g++ {
			if err := h.checkConfig(tr, g); err != nil {
				return nil, err
			}
			m, err := h.meta(tr, g)
			if err != nil {
				return nil, err
			}
			if m.Groups != 0 || m.Transactions != 0 || m.Versions != 0 || m.Certificates != 0 {
				return nil, errUnavailable
			}
		}
		prior, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		if len(prior) != 0 {
			if !bytes.Equal(prior, body) {
				return nil, errMismatch
			}
			return nil, nil
		}
		m, err := h.meta(tr, 0)
		if err != nil {
			return nil, err
		}
		n := uint64(len(key) + len(body))
		if n > h.config.Limits.Bytes-m.Bytes {
			return nil, errLimit
		}
		m.Bytes += n
		tr.Set(key, body)
		return nil, put(tr, h.key(0, "meta", "state"), m)
	})
	if err != nil {
		return graphCostStoredRecord{}, graphHistoryCommitError(err)
	}
	stored, _, err := graphCostParallelRead(context.Background(), h.db, func(tr fdb.ReadTransaction) (graphCostStoredRecord, error) {
		if err := h.graphSelectorsBindingIn(tr); err != nil {
			return graphCostStoredRecord{}, err
		}
		return snapshotStored(tr, key, 64<<10)
	})
	return stored, err
}

func graphSelectorsSpecFromEnv() (graphSelectorsSpec, string, error) {
	var spec graphSelectorsSpec
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
	if spec.Version != 1 || spec.Family != graphSelectorsFamily || !validGraphSelectorsConfig(graphSelectorsConfig(spec, run)) {
		return spec, "", errInvalid
	}
	return spec, run, nil
}

func (h *harness) graphSelectorsApply(ctx context.Context, phase string, fact uint64, client int, emit func(graphCostParallelEvent)) error {
	p := h.config.Cost
	p.Version = 1
	_, err := h.graphCostApplyOperation(ctx, graphCostParallelJob{phase, fact}, client, graphCostOperationID(p, phase, fact), "selectors", emit)
	return err
}

// Real independent callers share only bounded immutable jobs/results. The one
// owner drains admitted events even on error; no terminal/unknown attempt drops.
func (h *harness) graphSelectorsPhase(ctx context.Context, a *graphSelectorsArtifacts, phase string) error {
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
				err := h.graphSelectorsApply(ctx, phase, fact, client, func(e graphCostParallelEvent) { events <- event{Value: e} })
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

func runGraphSelectors(db fdb.Database, spec graphSelectorsSpec, run string) (err error) {
	c := graphSelectorsConfig(spec, run)
	if spec.Version != 1 || spec.Family != graphSelectorsFamily || !validGraphSelectorsConfig(c) {
		return errInvalid
	}
	h, err := newHarness(db, c)
	if err != nil {
		return err
	}
	binding, err := h.graphSelectorsInstall()
	if err != nil {
		return err
	}
	initial, err := h.fresh([]uint8{0, 1}, nil)
	if err != nil {
		return err
	}
	policy := graphSelectorsPolicy(c)
	source, err := newGraphHistoryRetention(h, initial, policy.History)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(os.Getenv("FDB_COST_ARTIFACT"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	a, err := newGraphSelectorsArtifacts(h, root, spec, binding, initial)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, a.Close()) }()
	apply := func(phase string, fact uint64) error {
		begin := time.Now()
		var outputErr error
		err := h.graphSelectorsApply(context.Background(), phase, fact, 0, func(e graphCostParallelEvent) { outputErr = errors.Join(outputErr, a.accept(e)) })
		if err != nil || outputErr != nil {
			return errors.Join(err, outputErr)
		}
		return a.latency(phase, fact, 0, time.Since(begin).Nanoseconds())
	}
	// This initial genuine certificate is exact-empty, not unavailable.
	empty, err := a.open(source, "initial-empty", graphSnapshotRequest{Mode: "Stable", Scope: []uint8{0, 1}})
	if err != nil {
		return err
	}
	if err := a.observe("initial-empty", empty, 0); err != nil {
		return errors.Join(err, empty.Close())
	}
	if err := empty.Close(); err != nil {
		return err
	}
	for i := uint64(0); i < 3; i++ {
		if err := apply("setup", i); err != nil {
			return err
		}
	}
	if err := apply("create", 0); err != nil {
		return err
	}
	if err := h.graphSelectorsPhase(context.Background(), a, "create"); err != nil {
		return err
	}
	old, err := a.open(source, "old-fresh", graphSnapshotRequest{Mode: "Fresh", Scope: []uint8{0, 1}})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, old.Close()) }()
	a.old = cut{old.owner.id.Cut}
	if err := a.observe("old-fresh", old, 5); err != nil {
		return err
	}
	// Exact single-group certificates are genuine native closure, never slices
	// of the full certificate. Stable later chooses only the requested scope.
	for _, scope := range [][]uint8{{0}, {1}} {
		name := fmt.Sprintf("old-scope-%d", scope[0])
		r, err := a.open(source, name, graphSnapshotRequest{Mode: "Fresh", Scope: scope})
		if err != nil {
			return err
		}
		if err := a.observe(name, r, 5); err != nil {
			return errors.Join(err, r.Close())
		}
		if err := r.Close(); err != nil {
			return err
		}
	}
	if spec.Layout == "columns" {
		if err := a.columnsStart(); err != nil {
			return err
		}
	}
	if err := a.inventory("old", a.old); err != nil {
		return err
	}
	if err := apply("update", 0); err != nil {
		return err
	}
	if err := h.graphSelectorsPhase(context.Background(), a, "update"); err != nil {
		return err
	}
	// No new certificate has been created since old5. Current is now6.
	for _, scope := range [][]uint8{{0, 1}, {0}, {1}} {
		name := fmt.Sprintf("stable-after-update-%d", len(scope)*2+int(scope[0]))
		r, err := a.open(source, name, graphSnapshotRequest{Mode: "Stable", Scope: scope})
		if err != nil {
			return err
		}
		if err := a.observe(name, r, 5); err != nil {
			return errors.Join(err, r.Close())
		}
		if err := r.Close(); err != nil {
			return err
		}
	}
	if err := a.observe("held-old-after-update", old, 5); err != nil {
		return err
	}
	at, err := a.open(source, "at-old-after-update", graphSnapshotRequest{Mode: "At", Scope: []uint8{0, 1}, At: a.old.record})
	if err != nil {
		return err
	}
	if err := a.observe("at-old-after-update", at, 5); err != nil {
		return errors.Join(err, at.Close())
	}
	if err := at.Close(); err != nil {
		return err
	}
	if a.receipt == nil {
		return errUnavailable
	}
	after, err := a.open(source, "after-current", graphSnapshotRequest{Mode: "After", Scope: []uint8{0, 1}, After: a.receipt})
	if err != nil {
		return err
	}
	a.final = cut{after.owner.id.Cut}
	if err := a.observe("after-current", after, 6); err != nil {
		return errors.Join(err, after.Close())
	}
	if err := after.Close(); err != nil {
		return err
	}
	fresh, err := a.open(source, "fresh-current", graphSnapshotRequest{Mode: "Fresh", Scope: []uint8{0, 1}})
	if err != nil {
		return err
	}
	if err := a.observe("fresh-current", fresh, 6); err != nil {
		return errors.Join(err, fresh.Close())
	}
	if !sameCertificate(fresh.owner.id.Cut, a.final.record) {
		return errors.Join(errMismatch, fresh.Close())
	}
	if err := fresh.Close(); err != nil {
		return err
	}
	if err := old.Close(); err != nil {
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
