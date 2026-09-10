// Package temporalreplay is the R0/R1 consumer-shaped replay benchmark for
// tasks/plan-temporal-index-ingestion.md. It drives ONLY the public pkg/graph
// API, exactly as the AI-SOC convergent projection does, on a durable on-disk
// Badger store with SyncWrites, and compares three write doors on one identical
// workload:
//
//	tx          g.Tx().Run per batch — the consumer's current path
//	strong      g.Ingest().NewSession(Sync: true) — one shared applier
//	concurrent  g.Ingest().NewSession(Concurrent: true) — self-applying sessions
//
// The run is a test gated by TKG_REPLAY_MODE so `make test` never executes it;
// run.sh drives every configuration under strace for exact fsync counts.
package temporalreplay

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

const (
	labelAsset      = "Asset"
	labelFact       = "Fact"
	labelOccurrence = "Occurrence"
	labelInbox      = "Inbox"
	labelCounter    = "Counter"
	keyProp         = "key"
	relOccurrenceOf = "OCCURRENCE_OF"
	relObservedOn   = "OBSERVED_ON"
)

// row is one source record after extraction: a fact identity, its event time
// and the host it was observed on.
type row struct {
	idx     int
	factKey string
	host    string
	ts      int64 // event time, Unix ms — NEVER the write time
	srcID   string
}

// workloadSpec is the deterministic shape shared by every mode so their durable
// output can be compared byte for byte.
type workloadSpec struct {
	rows      int
	producers int
	seed      int64
	// distinctFacts controls the repeat ratio: rows/distinctFacts occurrences per fact.
	distinctFacts int
	hotHosts      int // shared by every producer (the hot component)
	coldHosts     int // per producer (disjoint components)
}

func (s workloadSpec) generate() [][]row {
	rng := rand.New(rand.NewSource(s.seed))
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	per := make([][]row, s.producers)
	for i := 0; i < s.rows; i++ {
		p := i % s.producers
		// Half the rows land on a hot shared host, half on a producer-private one.
		var host string
		if rng.Intn(2) == 0 {
			host = fmt.Sprintf("hot-%02d", rng.Intn(s.hotHosts))
		} else {
			host = fmt.Sprintf("p%d-host-%03d", p, rng.Intn(s.coldHosts))
		}
		// Skewed fact choice: the first 10% of facts take 50% of rows.
		var f int
		if rng.Intn(2) == 0 {
			f = rng.Intn(max(1, s.distinctFacts/10))
		} else {
			f = rng.Intn(s.distinctFacts)
		}
		per[p] = append(per[p], row{
			idx:     i,
			factKey: fmt.Sprintf("fact-%06d", f),
			host:    host,
			ts:      base + int64(i)*997 + int64(rng.Intn(500)), // strictly increasing per idx, ms jitter
			srcID:   fmt.Sprintf("src-%d-%d", p, i),
		})
	}
	return per
}

func openGraph(dir string) (*graphpkg.Graph, error) {
	// The consumer's own configuration (engine/convergent.go OpenConvergent).
	g, err := graphpkg.New(graphpkg.Config{
		SnowflakeNodeID:      1,
		BadgerDir:            dir,
		SyncWrites:           true,
		LabelIndexOnDisk:     true,
		PropertyIndexOnDisk:  true,
		AdjacencyIndexOnDisk: true,
		DisablePlannerStats:  true,
		Validation:           graphpkg.ValidationLimits{AllowSelfLoops: true},
	})
	if err != nil {
		return nil, err
	}
	for _, l := range []string{labelAsset, labelFact, labelOccurrence, labelInbox, labelCounter} {
		if err := g.Constraints().CreateUnique(context.Background(), l, keyProp); err != nil && !errors.Is(err, graphpkg.ErrUniqueConstraintExists) {
			_ = g.Close()
			return nil, err
		}
	}
	return g, nil
}

// ---------------------------------------------------------------------------
// tx mode: the consumer's current path, one transaction per batch.
// ---------------------------------------------------------------------------

func writeBatchTx(g *graphpkg.Graph, tenant string, rows []row) error {
	return g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		for _, r := range rows {
			asset, _, err := tx.GetOrCreateByKey(labelAsset, keyProp, r.host, map[string]any{"host": r.host})
			if err != nil {
				return err
			}
			fact, created, err := tx.GetOrCreateByKey(labelFact, keyProp, r.factKey, map[string]any{"first_seen": r.ts, "last_seen": r.ts})
			if err != nil {
				return err
			}
			if !created {
				// last_seen is an EVENT-TIME maximum, not the write order: a
				// replay with several producers delivers rows out of order.
				cur, _ := tx.NodeProperty(fact, "last_seen")
				if curTS, _ := cur.(int64); r.ts > curTS {
					if _, err := tx.UpdateNode(fact.ID(), map[string]any{"last_seen": r.ts}); err != nil {
						return err
					}
				}
			}
			occ, err := tx.AddNode([]string{labelOccurrence}, map[string]any{
				keyProp: occurrenceKey(tenant, r), "fact_key": r.factKey, "ts": r.ts, "src": r.srcID,
			})
			if err != nil {
				return err
			}
			if _, err := tx.AddRelationship(relOccurrenceOf, occ, fact, nil); err != nil {
				return err
			}
			if _, err := tx.AddRelationship(relObservedOn, occ, asset, nil); err != nil {
				return err
			}
		}
		// Admission: a global per-tenant sequence allocated in the same commit.
		counter, _, err := tx.GetOrCreateByKey(labelCounter, keyProp, "admission-"+tenant, map[string]any{"next": int64(1)})
		if err != nil {
			return err
		}
		nextAny, _ := tx.NodeProperty(counter, "next")
		seq, _ := nextAny.(int64)
		if seq < 1 {
			return errors.New("admission counter unreadable")
		}
		for _, r := range rows {
			if _, err := tx.AddNode([]string{labelInbox}, map[string]any{
				keyProp: "inbox-" + occurrenceKey(tenant, r), "occurrence_key": occurrenceKey(tenant, r), "seq": seq,
			}); err != nil {
				return err
			}
			seq++
		}
		_, err = tx.UpdateNode(counter.ID(), map[string]any{"next": seq})
		return err
	})
}

func occurrenceKey(tenant string, r row) string {
	h := sha256.Sum256([]byte(tenant + "|" + r.factKey + "|" + strconv.FormatInt(r.ts, 10) + "|" + r.srcID))
	return hex.EncodeToString(h[:16])
}

// ---------------------------------------------------------------------------
// session modes (strong / concurrent): read-then-prepare, Submit, reconcile.
// The Session API has no MERGE, so identities are resolved by a read outside
// any transaction and a losing racer is repaired after Submit reports a
// unique violation.
// ---------------------------------------------------------------------------

type sessionWriter struct {
	g        *graphpkg.Graph
	s        *ingest.Session
	tenant   string
	lane     int
	seq      int64 // lane-local admission sequence (no global counter is possible here)
	assets   map[string]*types.Node
	facts    map[string]*types.Node // per batch
	batchMax map[string]int64       // per batch: last_seen already prepared/stored
	retries  int
}

func newSessionWriter(g *graphpkg.Graph, tenant string, lane int, concurrent bool) (*sessionWriter, error) {
	opts := ingest.IngestOptions{Sync: true, Concurrent: concurrent}
	if os.Getenv("TKG_REPLAY_NO_DECLARE") == "" {
		opts.DeclareLabels = []string{labelAsset, labelFact, labelOccurrence, labelInbox}
		opts.DeclareRelTypes = []string{relOccurrenceOf, relObservedOn}
	}
	s, err := g.Ingest().NewSession(opts)
	if err != nil {
		return nil, err
	}
	return &sessionWriter{g: g, s: s, tenant: tenant, lane: lane, assets: map[string]*types.Node{}, facts: map[string]*types.Node{}}, nil
}

func (w *sessionWriter) lookup(label, key string) (*types.Node, error) {
	nodes, err := w.g.Nodes().ByLabelAndProperty(label, keyProp, key, storepkg.QueryOpts{})
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, nil
	}
	return nodes[0], nil
}

// pendingCreate remembers a node this batch tried to create so a unique
// violation can be reconciled against the winner.
type pendingCreate struct {
	label, key string
	node       *types.Node
	occ        *types.Node // occurrence that must be linked to the winner
	relType    string
}

func (w *sessionWriter) writeBatch(rows []row) error {
	w.facts = map[string]*types.Node{}
	w.batchMax = map[string]int64{}
	var creates []pendingCreate
	for _, r := range rows {
		asset, ok := w.assets[r.host]
		if !ok {
			n, err := w.lookup(labelAsset, r.host)
			if err != nil {
				return err
			}
			if n == nil {
				n, err = w.s.AddNode([]string{labelAsset}, map[string]any{keyProp: r.host, "host": r.host})
				if err != nil {
					return err
				}
				creates = append(creates, pendingCreate{label: labelAsset, key: r.host, node: n})
			}
			w.assets[r.host] = n
			asset = n
		}
		// The Session API has no read-modify-write: the repeat's last_seen
		// maximum is decided by a read OUTSIDE the group, so the value is racy
		// across producers by construction. Within one batch the skeleton of a
		// fact created earlier in the same group is reused (w.facts is reset per
		// batch); across batches the fact is re-read so another producer's
		// newer maximum is seen.
		fact, ok := w.facts[r.factKey]
		if !ok {
			n, err := w.lookup(labelFact, r.factKey)
			if err != nil {
				return err
			}
			if n == nil {
				n, err = w.s.AddNode([]string{labelFact}, map[string]any{keyProp: r.factKey, "first_seen": r.ts, "last_seen": r.ts})
				if err != nil {
					return err
				}
				creates = append(creates, pendingCreate{label: labelFact, key: r.factKey, node: n})
				w.batchMax[r.factKey] = r.ts
			} else {
				cur, _ := n.GetProperty("last_seen")
				curTS, _ := cur.(int64)
				w.batchMax[r.factKey] = curTS
			}
			w.facts[r.factKey] = n
			fact = n
		}
		if r.ts > w.batchMax[r.factKey] {
			w.batchMax[r.factKey] = r.ts
			if err := w.s.UpdateNode(fact.ID(), map[string]any{"last_seen": r.ts}); err != nil {
				return err
			}
		}
		occ, err := w.s.AddNode([]string{labelOccurrence}, map[string]any{
			keyProp: occurrenceKey(w.tenant, r), "fact_key": r.factKey, "ts": r.ts, "src": r.srcID,
		})
		if err != nil {
			return err
		}
		if _, err := w.s.AddRelationship(relOccurrenceOf, occ, fact, nil); err != nil {
			return err
		}
		if _, err := w.s.AddRelationship(relObservedOn, occ, asset, nil); err != nil {
			return err
		}
		for i := range creates {
			if creates[i].node == fact && creates[i].occ == nil {
				creates[i].occ, creates[i].relType = occ, relOccurrenceOf
			}
			if creates[i].node == asset && creates[i].occ == nil {
				creates[i].occ, creates[i].relType = occ, relObservedOn
			}
		}
		w.seq++
		if _, err := w.s.AddNode([]string{labelInbox}, map[string]any{
			keyProp: "inbox-" + occurrenceKey(w.tenant, r), "occurrence_key": occurrenceKey(w.tenant, r),
			"seq": w.seq, "lane": int64(w.lane),
		}); err != nil {
			return err
		}
	}
	_, err := w.s.Submit()
	if err == nil {
		return nil
	}
	if !errors.Is(err, graphpkg.ErrUniqueViolation) {
		return err
	}
	// Reconcile: the API reports ONE error for the group and commits every
	// survivor, so every create in the group must be re-read to find the losers.
	w.retries++
	return w.reconcile(creates, rows)
}

// reconcile re-reads each attempted create; a loser's dependent relationships
// were short-circuited by the batch, so they are re-issued against the winner.
// Only the FIRST occurrence that referenced the loser is repaired here; later
// occurrences in the same batch referenced the same loser skeleton and are
// repaired by the full-scan repair pass below (repairDanglingOccurrences).
func (w *sessionWriter) reconcile(creates []pendingCreate, rows []row) error {
	for _, c := range creates {
		winner, err := w.lookup(c.label, c.key)
		if err != nil {
			return err
		}
		if winner == nil {
			return fmt.Errorf("reconcile: %s %q neither created nor found", c.label, c.key)
		}
		if winner.ID() == c.node.ID() {
			continue
		}
		switch c.label {
		case labelFact:
			w.facts[c.key] = winner
		case labelAsset:
			w.assets[c.key] = winner
		}
	}
	// Re-link every occurrence in this batch whose relationships were dropped.
	for _, r := range rows {
		occ, err := w.lookup(labelOccurrence, occurrenceKey(w.tenant, r))
		if err != nil {
			return err
		}
		if occ == nil {
			return fmt.Errorf("reconcile: occurrence for row %d missing after partial commit", r.idx)
		}
		if deg, err := w.g.Rels().OutgoingDegree(occ.ID(), relOccurrenceOf); err != nil {
			return err
		} else if deg == 0 {
			if _, err := w.s.AddRelationship(relOccurrenceOf, occ, w.facts[r.factKey], nil); err != nil {
				return err
			}
			// The fact's last_seen update was also on the loser skeleton.
			cur, _ := w.facts[r.factKey].GetProperty("last_seen")
			if curTS, _ := cur.(int64); r.ts > curTS {
				if err := w.s.UpdateNode(w.facts[r.factKey].ID(), map[string]any{"last_seen": r.ts}); err != nil {
					return err
				}
			}
		}
		if deg, err := w.g.Rels().OutgoingDegree(occ.ID(), relObservedOn); err != nil {
			return err
		} else if deg == 0 {
			if _, err := w.s.AddRelationship(relObservedOn, occ, w.assets[r.host], nil); err != nil {
				return err
			}
		}
	}
	_, err := w.s.Submit()
	return err
}

// ---------------------------------------------------------------------------
// Durable-output digest: computed after Close + reopen so it reads only what
// survived on disk.
// ---------------------------------------------------------------------------

type digest struct {
	Assets, Facts, Occurrences, Inbox int
	OccurrenceOf, ObservedOn          int
	Hash                              string
	Problems                          []string
}

func computeDigest(g *graphpkg.Graph) (digest, error) {
	var d digest
	var err error
	if d.Assets, err = g.Nodes().CountByLabel(labelAsset); err != nil {
		return d, err
	}
	if d.Facts, err = g.Nodes().CountByLabel(labelFact); err != nil {
		return d, err
	}
	if d.Occurrences, err = g.Nodes().CountByLabel(labelOccurrence); err != nil {
		return d, err
	}
	if d.Inbox, err = g.Nodes().CountByLabel(labelInbox); err != nil {
		return d, err
	}
	if d.OccurrenceOf, err = g.Rels().CountByType(relOccurrenceOf); err != nil {
		return d, err
	}
	if d.ObservedOn, err = g.Rels().CountByType(relObservedOn); err != nil {
		return d, err
	}
	factByID := map[types.NodeID]string{}
	lastSeen := map[string]int64{}
	if err := g.Nodes().ForEachByLabel(labelFact, storepkg.QueryOpts{}, func(n *types.Node) bool {
		k, _ := n.GetProperty(keyProp)
		ls, _ := n.GetProperty("last_seen")
		factByID[n.ID()] = k.(string)
		lastSeen[k.(string)] = ls.(int64)
		return true
	}); err != nil {
		return d, err
	}
	assetByID := map[types.NodeID]string{}
	if err := g.Nodes().ForEachByLabel(labelAsset, storepkg.QueryOpts{}, func(n *types.Node) bool {
		k, _ := n.GetProperty(keyProp)
		assetByID[n.ID()] = k.(string)
		return true
	}); err != nil {
		return d, err
	}
	// Per fact: the exact multiset of occurrence times, via the relationship the
	// consumer would follow — not via the occurrence's own fact_key label.
	times := map[string][]int64{}
	hosts := map[string]map[string]int{}
	if err := g.Nodes().ForEachByLabel(labelOccurrence, storepkg.QueryOpts{}, func(n *types.Node) bool {
		ts, _ := n.GetProperty("ts")
		var fk, hk string
		nOf, nOn := 0, 0
		_ = g.Rels().ForEachAdjacentEndpoint(n.ID(), relOccurrenceOf, false, func(_ types.RelID, other types.NodeID) bool {
			fk = factByID[other]
			nOf++
			return true
		})
		_ = g.Rels().ForEachAdjacentEndpoint(n.ID(), relObservedOn, false, func(_ types.RelID, other types.NodeID) bool {
			hk = assetByID[other]
			nOn++
			return true
		})
		if nOf != 1 || nOn != 1 {
			k, _ := n.GetProperty(keyProp)
			d.Problems = append(d.Problems, fmt.Sprintf("occurrence %v has %d OCCURRENCE_OF and %d OBSERVED_ON", k, nOf, nOn))
		}
		times[fk] = append(times[fk], ts.(int64))
		if hosts[fk] == nil {
			hosts[fk] = map[string]int{}
		}
		hosts[fk][hk]++
		return true
	}); err != nil {
		return d, err
	}
	keys := make([]string, 0, len(times))
	for k := range times {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		ts := times[k]
		sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
		if lastSeen[k] != ts[len(ts)-1] {
			d.Problems = append(d.Problems, fmt.Sprintf("fact %s last_seen=%d but max occurrence ts=%d", k, lastSeen[k], ts[len(ts)-1]))
		}
		hk := make([]string, 0, len(hosts[k]))
		for hn, c := range hosts[k] {
			hk = append(hk, fmt.Sprintf("%s:%d", hn, c))
		}
		sort.Strings(hk)
		fmt.Fprintf(h, "%s|%v|%d|%s\n", k, ts, lastSeen[k], strings.Join(hk, ","))
	}
	fmt.Fprintf(h, "counts|%d|%d|%d|%d|%d|%d\n", d.Assets, d.Facts, d.Occurrences, d.Inbox, d.OccurrenceOf, d.ObservedOn)
	d.Hash = hex.EncodeToString(h.Sum(nil))
	return d, nil
}

// ---------------------------------------------------------------------------
// Process-level physical write accounting (/proc/self/io) and peak RSS.
// ---------------------------------------------------------------------------

type ioStat struct{ SyscW, WriteBytes int64 }

func readIO() ioStat {
	var s ioStat
	f, err := os.Open("/proc/self/io")
	if err != nil {
		return s
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != 2 {
			continue
		}
		v, _ := strconv.ParseInt(fields[1], 10, 64)
		switch fields[0] {
		case "syscw:":
			s.SyscW = v
		case "write_bytes:":
			s.WriteBytes = v
		}
	}
	return s
}

func peakRSSBytes() int64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "VmHWM:") {
			fields := strings.Fields(sc.Text())
			kb, _ := strconv.ParseInt(fields[1], 10, 64)
			return kb * 1024
		}
	}
	return 0
}

func dirBytes(dir string) int64 {
	var n int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			n += info.Size()
		}
		return nil
	})
	return n
}

// ---------------------------------------------------------------------------
// The run.
// ---------------------------------------------------------------------------

type phaseStat struct {
	Rows       int     `json:"rows"`
	Seconds    float64 `json:"seconds"`
	RowsPerSec float64 `json:"rows_per_sec"`
}

type runResult struct {
	Mode          string      `json:"mode"`
	Producers     int         `json:"producers"`
	BatchRows     int         `json:"batch_rows"`
	Rows          int         `json:"rows"`
	DistinctFacts int         `json:"distinct_facts"`
	WallSeconds   float64     `json:"wall_seconds"`
	RowsPerSec    float64     `json:"rows_per_sec"`
	Batches       int         `json:"batches"`
	CommitP50ms   float64     `json:"commit_p50_ms"`
	CommitP99ms   float64     `json:"commit_p99_ms"`
	CommitMaxms   float64     `json:"commit_max_ms"`
	Retries       int         `json:"unique_retries"`
	SyscW         int64       `json:"write_syscalls"`
	WriteBytes    int64       `json:"write_bytes"`
	PeakRSSBytes  int64       `json:"peak_rss_bytes"`
	DirBytes      int64       `json:"dir_bytes"`
	Phases        []phaseStat `json:"phases"`
	Digest        digest      `json:"digest"`
	GoVersion     string      `json:"go_version"`
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

// TestReplayRun executes one configuration and writes a JSON result. Gated by
// TKG_REPLAY_MODE (tx | strong | concurrent).
func TestReplayRun(t *testing.T) {
	mode := os.Getenv("TKG_REPLAY_MODE")
	if mode == "" {
		t.Skip("set TKG_REPLAY_MODE=tx|strong|concurrent to run the replay benchmark")
	}
	spec := workloadSpec{
		rows:          envInt("TKG_REPLAY_ROWS", 4000),
		producers:     envInt("TKG_REPLAY_PRODUCERS", 1),
		seed:          20260909,
		distinctFacts: envInt("TKG_REPLAY_FACTS", 1000),
		hotHosts:      8,
		coldHosts:     64,
	}
	batch := envInt("TKG_REPLAY_BATCH", 64)
	dir := os.Getenv("TKG_REPLAY_DIR")
	if dir == "" {
		dir = t.TempDir()
	} else {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	out := os.Getenv("TKG_REPLAY_OUT")

	g, err := openGraph(dir)
	if err != nil {
		t.Fatal(err)
	}
	per := spec.generate()

	res := runResult{Mode: mode, Producers: spec.producers, BatchRows: batch, Rows: spec.rows, DistinctFacts: spec.distinctFacts, GoVersion: runtime.Version()}
	var mu sync.Mutex
	var latencies []time.Duration
	var retries int
	phaseBoundary := spec.rows / 4
	phaseRows := make([]int, 4)
	phaseStart := time.Now()
	var phaseDone [4]time.Time

	io0 := readIO()
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, spec.producers)
	var rowsDone int
	for p := 0; p < spec.producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			rows := per[p]
			var w *sessionWriter
			if mode != "tx" {
				var err error
				w, err = newSessionWriter(g, "t1", p+1, mode == "concurrent")
				if err != nil {
					errCh <- err
					return
				}
				defer w.s.Close()
			}
			for i := 0; i < len(rows); i += batch {
				j := min(i+batch, len(rows))
				t0 := time.Now()
				var err error
				if mode == "tx" {
					err = writeBatchTx(g, "t1", rows[i:j])
				} else {
					err = w.writeBatch(rows[i:j])
				}
				lat := time.Since(t0)
				if err != nil {
					errCh <- fmt.Errorf("producer %d batch %d: %w", p, i/batch, err)
					return
				}
				mu.Lock()
				latencies = append(latencies, lat)
				rowsDone += j - i
				ph := min((rowsDone-1)/max(1, phaseBoundary), 3)
				phaseRows[ph] += j - i
				if phaseDone[ph].IsZero() || time.Now().After(phaseDone[ph]) {
					phaseDone[ph] = time.Now()
				}
				mu.Unlock()
			}
			if w != nil {
				mu.Lock()
				retries += w.retries
				mu.Unlock()
			}
		}(p)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	res.WallSeconds = time.Since(start).Seconds()
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	io1 := readIO()
	res.SyscW = io1.SyscW - io0.SyscW
	res.WriteBytes = io1.WriteBytes - io0.WriteBytes
	res.PeakRSSBytes = peakRSSBytes()
	res.DirBytes = dirBytes(dir)
	res.RowsPerSec = float64(spec.rows) / res.WallSeconds
	res.Batches = len(latencies)
	res.Retries = retries
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if n := len(latencies); n > 0 {
		res.CommitP50ms = float64(latencies[n/2].Microseconds()) / 1000
		res.CommitP99ms = float64(latencies[min(n-1, n*99/100)].Microseconds()) / 1000
		res.CommitMaxms = float64(latencies[n-1].Microseconds()) / 1000
	}
	prev := phaseStart
	for i := 0; i < 4; i++ {
		if phaseDone[i].IsZero() {
			continue
		}
		sec := phaseDone[i].Sub(prev).Seconds()
		res.Phases = append(res.Phases, phaseStat{Rows: phaseRows[i], Seconds: sec, RowsPerSec: float64(phaseRows[i]) / sec})
		prev = phaseDone[i]
	}

	// Reopen: only what survived on disk counts.
	g2, err := openGraph(dir)
	if err != nil {
		t.Fatal(err)
	}
	res.Digest, err = computeDigest(g2)
	if err != nil {
		t.Fatal(err)
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
	if res.Digest.Occurrences != spec.rows || res.Digest.Inbox != spec.rows {
		t.Errorf("durable output incomplete: %d occurrences, %d inbox entries for %d rows", res.Digest.Occurrences, res.Digest.Inbox, spec.rows)
	}
	// A lost read-modify-write (last_seen below the true maximum) is a MEASURED
	// property of a mode, recorded in the result; only TKG_REPLAY_STRICT=1
	// turns it into a failure.
	for _, p := range res.Digest.Problems {
		if os.Getenv("TKG_REPLAY_STRICT") == "1" {
			t.Errorf("digest problem: %s", p)
		}
	}
	js, _ := json.MarshalIndent(res, "", "  ")
	if out != "" {
		if err := os.WriteFile(out, js, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("%s", js)
}
