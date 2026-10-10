package slice

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
)

// RunConfig selects the pinned fixture, native backend and new output destination.
type RunConfig struct {
	Dataset, Reference, ReferencePin, Backend, Output string
	Limits                                            Limits
}

// QueryStatus records exact current validation or an explicit pending feature lane.
type QueryStatus struct {
	ID          string `json:"query_id"`
	Lane        string `json:"lane"`
	Status      string `json:"status"`
	Export      string `json:"export,omitempty"`
	Rows        int64  `json:"rows,omitzero"`
	MultisetSHA string `json:"normalized_multiset_sha256,omitempty"`
}

// EventCalls records the instrumented native API delta for one mutation.
type EventCalls struct {
	Revision    int               `json:"revision"`
	Op          string            `json:"op"`
	NativeCalls map[string]uint64 `json:"public_native_api_calls"`
}

// HistoryCost counts identities with native history, not retained version totals.
type HistoryCost struct {
	NodesWithHistory uint64 `json:"nodes_with_history"`
	RelsWithHistory  uint64 `json:"rels_with_history"`
	Scope            string `json:"scope"`
}

// DiskFile records apparent and allocated bytes for one native store file.
type DiskFile struct {
	Name           string `json:"name"`
	ApparentBytes  int64  `json:"apparent_bytes"`
	AllocatedBytes int64  `json:"allocated_bytes"`
}

// Costs separates observed resource snapshots from unmeasured categories.
type Costs struct {
	DiskScope                     string     `json:"disk_scope"`
	DiskFiles                     []DiskFile `json:"disk_files"`
	ApparentBytes, AllocatedBytes int64
	HeapAllocBytes                uint64   `json:"process_heap_alloc_bytes"`
	MemoryScope                   string   `json:"memory_scope"`
	Unmeasured                    []string `json:"unmeasured"`
}

// Report records the functional slice and its explicit evidence limits.
type Report struct {
	SchemaVersion           int    `json:"schema_version"`
	DatasetSHA              string `json:"dataset_sha256"`
	SourceCommit            string `json:"rho_source_commit"`
	Backend                 string `json:"backend"`
	GoVersion, GOOS, GOARCH string
	ValidatedCurrent        int               `json:"validated_current_answers"`
	PendingHistory          int               `json:"pending_history_answers"`
	ReopenedCurrent         int               `json:"validated_r3_reopen_answers"`
	Queries                 []QueryStatus     `json:"queries"`
	Events                  []EventCalls      `json:"events"`
	NativeCalls             map[string]uint64 `json:"public_native_api_calls"`
	RangeCandidates         uint64            `json:"range_candidate_rows"`
	History                 HistoryCost       `json:"native_history_cost"`
	Costs                   Costs             `json:"costs"`
	PerformanceAcceptance   bool              `json:"performance_acceptance"`
	NativeV5                bool              `json:"native_v5"`
	Limits                  []string          `json:"evidence_limits"`
}

func fileSHA(path string) (string, error) {
	f, err := os.Open(path) // #nosec G304 -- Caller-selected local inputs; pinned inventories constrain fixture/release paths.
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, copyErr := io.Copy(h, f)
	err = errors.Join(copyErr, f.Close())
	return hex.EncodeToString(h.Sum(nil)), err
}
func verifyPin(root, pin, expectedSHA string) error {
	h, err := fileSHA(pin)
	if err != nil || h != expectedSHA {
		return errors.Join(ErrContract, err)
	}
	b, err := os.ReadFile(pin) // #nosec G304 -- Explicit local lock path; its complete bytes must match the trusted digest.
	if err != nil {
		return err
	}
	var p struct {
		Files map[string]string `json:"files_sha256"`
	}
	if err := json.Unmarshal(b, &p); err != nil {
		return errors.Join(ErrContract, err)
	}
	if len(p.Files) == 0 {
		return ErrContract
	}
	for name, want := range p.Files {
		if filepath.IsAbs(name) || filepath.Clean(name) != name || name == ".." || len(name) > 256 {
			return ErrContract
		}
		path := filepath.Join(root, name)
		relative, err := filepath.Rel(root, path)
		if err != nil || relative == ".." || len(relative) > 3 && relative[:3] == "../" {
			return ErrContract
		}
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			return errors.Join(ErrContract, err)
		}
		actual, err := fileSHA(path)
		if err != nil || actual != want {
			return errors.Join(ErrContract, err)
		}
	}
	return filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			return nil
		}
		name, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if _, ok := p.Files[name]; !ok {
			return ErrContract
		}
		return nil
	})
}

// verifySource binds extracted bytes to the trusted compact content pin. The
// reproducer independently checks actual Git commit/tree before extraction;
// historical manifest/tar receipt hashes are not runtime content authorities.
func verifySource(root, pin string) error {
	h, err := fileSHA(pin)
	if err != nil || h != sourcePinSHA {
		return errors.Join(ErrContract, err)
	}
	b, err := os.ReadFile(pin) // #nosec G304 -- Explicit local lock path; its complete bytes must match the trusted digest.
	if err != nil {
		return err
	}
	var p struct {
		Commit  string `json:"commit"`
		Tree    string `json:"tree"`
		Count   int    `json:"file_count"`
		Content string `json:"content_inventory_sha256"`
	}
	if err := json.Unmarshal(b, &p); err != nil {
		return errors.Join(ErrContract, err)
	}
	if p.Commit != sourceCommit || p.Tree != "4c2ca34b38cd7ec394dfa4c83ea18350197400b2" || p.Count != 1438 || p.Content != "25814bd41daab2174b8ad085bf205b7726af3f65fc17119b48001fd08f43e907" {
		return ErrContract
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() {
		return errors.Join(ErrContract, err)
	}
	files := make(map[string]string)
	err = filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			return nil
		}
		if !e.Type().IsRegular() {
			return ErrContract
		}
		name, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		h, err := fileSHA(path)
		if err != nil {
			return err
		}
		files[name] = h
		return nil
	})
	if err != nil {
		return errors.Join(ErrContract, err)
	}
	if len(files) != p.Count {
		return ErrContract
	}
	b, err = json.Marshal(files)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(append(b, '\n'))
	if hex.EncodeToString(sum[:]) != p.Content {
		return ErrContract
	}
	return nil
}

func readQueries(c RunConfig) (string, []Query, error) {
	if err := verifyPin(c.Reference, c.ReferencePin, referencePinSHA); err != nil {
		return "", nil, err
	}
	base := filepath.Dir(c.Reference)
	if err := verifySource(filepath.Join(base, "rho-v4-source"), filepath.Join(base, "source-pin.json")); err != nil {
		return "", nil, err
	}
	got, err := filepath.Abs(c.Dataset)
	if err != nil {
		return "", nil, err
	}
	want, err := filepath.Abs(filepath.Join(c.Reference, "fixtures/basic-small"))
	if err != nil || got != want {
		return "", nil, ErrContract
	}
	b, err := os.ReadFile(filepath.Join(c.Dataset, "manifest.json"))
	if err != nil {
		return "", nil, err
	}
	var m struct {
		Mapping string `json:"mapping_version"`
		ID      string `json:"canonical_input_sha256"`
	}
	if err := json.Unmarshal(b, &m); err != nil || m.Mapping != "basic-graph-v1" || len(m.ID) != 64 {
		return "", nil, ErrContract
	}
	b, err = os.ReadFile(filepath.Join(c.Dataset, "queries.json"))
	if err != nil {
		return "", nil, err
	}
	var queries []Query
	if err := strictJSON(b, &queries); err != nil {
		return "", nil, err
	}
	if len(queries) != 93 {
		return "", nil, ErrContract
	}
	current, history := 0, 0
	seen := make(map[string]bool, len(queries))
	for _, q := range queries {
		if seen[q.ID] || q.ID == "" || q.Revision < 0 || q.Revision > 3 {
			return "", nil, ErrContract
		}
		seen[q.ID] = true
		switch q.Lane {
		case "basic-graph-current":
			current++
		case "retained-history-feature":
			history++
		default:
			return "", nil, ErrUnsupported
		}
	}
	if current != 92 || history != 1 {
		return "", nil, ErrContract
	}
	return m.ID, queries, nil
}
func eachJSONL[T any](path string, fn func(T) error) (retErr error) {
	f, err := os.Open(path) // #nosec G304 -- Caller-selected local inputs; pinned inventories constrain fixture/release paths.
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 4096), 1<<20)
	for s.Scan() {
		var row T
		if err := strictJSON(s.Bytes(), &row); err != nil {
			return err
		}
		if err := fn(row); err != nil {
			return err
		}
	}
	return s.Err()
}
func exportQuery(ctx context.Context, a *Adapter, r Request, path string, l Limits) (retErr error) {
	if ctx == nil || a == nil || !l.valid() {
		return ErrContract
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return ErrContract
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".answer-")
	if err != nil {
		return err
	}
	name := f.Name()
	published := false
	defer func() {
		if !published {
			_ = f.Close()
			_ = os.Remove(name)
		}
	}()
	rows, size := 0, 0
	err = a.query(ctx, r, func(v any) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if rows >= l.MaxRows || len(b)+1 > l.MaxBytes-size {
			return ErrLimit
		}
		rows++
		size += len(b) + 1
		_, err = f.Write(append(b, '\n'))
		return err
	}, l.MaxVisited)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Link(name, path); err != nil {
		return err
	}
	if err := os.Remove(name); err != nil {
		_ = os.Remove(path)
		return err
	}
	published = true
	return nil
}
func normalize(ctx context.Context, c RunConfig, q Query, path, metadataPath string, id string) (QueryStatus, error) {
	meta := struct{ Mapping, Dataset, Query, Lane string }{"basic-graph-v1", id, q.ID, q.Lane}
	b, err := json.Marshal(map[string]string{"mapping_version": meta.Mapping, "dataset_sha256": meta.Dataset, "query_id": meta.Query, "lane": meta.Lane})
	if err != nil {
		return QueryStatus{}, err
	}
	if err := os.WriteFile(metadataPath, b, 0600); err != nil {
		return QueryStatus{}, err
	}
	// #nosec G204 -- Fixed interpreter and argument vector, no shell; complete reference script/query inventory is pinned before execution.
	cmd := exec.CommandContext(ctx, "python3", "-B", filepath.Join(c.Reference, "normalize.py"), "--dataset", c.Dataset, "--query", q.ID, "--actual", path, "--metadata", metadataPath)
	b, err = cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return QueryStatus{}, ctx.Err()
		}
		return QueryStatus{}, fmt.Errorf("%w: normalizer query %s: %s", ErrContract, q.ID, string(b))
	}
	var n struct {
		Rows   int64  `json:"rows"`
		SHA    string `json:"normalized_multiset_sha256"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal(b, &n); err != nil || n.Result != "equal-canonical-output" || n.Rows != q.Expected.Rows {
		return QueryStatus{}, ErrContract
	}
	return QueryStatus{ID: q.ID, Lane: q.Lane, Status: "equal-complete-native-current-output", Export: filepath.Base(path), Rows: n.Rows, MultisetSHA: n.SHA}, nil
}
func diskCosts(root string, c *Costs) error {
	if root == "" {
		c.DiskScope = "Memory backend is explicitly nondurable; no native durable store directory"
		return nil
	}
	c.DiskScope = "Observed all native Badger files after close, including retained history/index/dictionary/integrity/WAL costs; no semantic-category or disk-quota claim"
	return filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			return nil
		}
		i, err := e.Info()
		if err != nil {
			return err
		}
		s, ok := i.Sys().(*syscall.Stat_t)
		if !ok {
			return ErrUnsupported
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		c.DiskFiles = append(c.DiskFiles, DiskFile{relative, i.Size(), s.Blocks * 512})
		c.ApparentBytes += i.Size()
		c.AllocatedBytes += s.Blocks * 512
		return nil
	})
}

// Run stages and validates the complete supported fixture before publishing outputs.
func Run(ctx context.Context, c RunConfig) (result Report, retErr error) {
	if ctx == nil || !c.Limits.valid() || c.Output == "" {
		return Report{}, ErrContract
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	id, queries, err := readQueries(c)
	if err != nil {
		return Report{}, err
	}
	if _, err := os.Lstat(c.Output); !errors.Is(err, os.ErrNotExist) {
		return Report{}, ErrContract
	}
	if err := os.MkdirAll(filepath.Dir(c.Output), 0700); err != nil {
		return Report{}, err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(c.Output), ".native-rhov4-")
	if err != nil {
		return Report{}, err
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(tmp)
		}
	}()
	dir := ""
	if c.Backend == "badger" {
		dir = filepath.Join(tmp, "store")
	}
	a, err := openAdapter(c.Backend, dir)
	if err != nil {
		return Report{}, err
	}
	defer func() {
		if err := a.Close(); err != nil {
			retErr = errors.Join(retErr, err)
			result = Report{}
		}
	}()
	r := Report{SchemaVersion: 1, DatasetSHA: id, SourceCommit: sourceCommit, Backend: c.Backend, GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, Limits: []string{"Correctness exercise only; no vendor/native-v5 adapter or performance/phase acceptance", "Serial private adapter; no global cut, power-loss or concurrent transaction isolation proof", "Tracked graph/store API calls exclude row value/accessor methods and internal storage-write/fsync counts", "MaxVisited bounds delivered adapter callbacks only; native range/full/adjacency paths may allocate/precompute candidates before callbacks; no engine-memory or internal-work bound is established", "Retained history IDs are not total retained-version count; all durable bytes remain included", "Heap snapshot includes engine+adapter in this process; RSS/PSS/mmap/cgroup and peaks unmeasured", "Generator/reference/normalizer output costs are separate, not native engine bytes"}}
	for _, name := range []string{"nodes.jsonl", "edges.jsonl"} {
		if err := eachJSONL(filepath.Join(c.Dataset, name), func(e Entity) error { return a.Load(ctx, e) }); err != nil {
			return Report{}, err
		}
	}
	if err := a.BuildIndexes(); err != nil {
		return Report{}, err
	}
	events, err := os.Open(filepath.Join(c.Dataset, "changes.jsonl"))
	if err != nil {
		return Report{}, err
	}
	defer func() { _ = events.Close() }() // Read-only descriptor cleanup on earlier refusal; successful path checks Close below.
	scan := bufio.NewScanner(events)
	scan.Buffer(make([]byte, 4096), 1<<20)
	var next Event
	hasNext := false
	advance := func() error {
		hasNext = scan.Scan()
		if !hasNext {
			return scan.Err()
		}
		next = Event{}
		return strictJSON(scan.Bytes(), &next)
	}
	if err := advance(); err != nil {
		return Report{}, err
	}
	for stage := range 4 {
		for hasNext && next.Revision == stage {
			before := maps.Clone(a.Calls)
			if err := a.Apply(ctx, next); err != nil {
				return Report{}, fmt.Errorf("apply stage%d op%s: %w", stage, next.Op, err)
			}
			delta := make(map[string]uint64)
			for k, v := range a.Calls {
				if v > before[k] {
					delta[k] = v - before[k]
				}
			}
			r.Events = append(r.Events, EventCalls{next.Revision, next.Op, delta})
			if err := advance(); err != nil {
				return Report{}, fmt.Errorf("decode next event stage%d: %w", stage, err)
			}
		}
		if hasNext && next.Revision < stage {
			return Report{}, ErrContract
		}
		for _, q := range queries {
			if q.Lane != "basic-graph-current" || q.Revision != stage {
				continue
			}
			path := filepath.Join(tmp, q.ID+".jsonl")
			if err := exportQuery(ctx, a, q.Request, path, c.Limits); err != nil {
				return Report{}, fmt.Errorf("export %s: %w", q.ID, err)
			}
			status, err := normalize(ctx, c, q, path, path+".meta.json", id)
			if err != nil {
				return Report{}, fmt.Errorf("normalize %s: %w", q.ID, err)
			}
			r.Queries = append(r.Queries, status)
			r.ValidatedCurrent++
		}
	}
	if hasNext {
		return Report{}, ErrContract
	}
	if err := events.Close(); err != nil {
		return Report{}, err
	}
	for _, q := range queries {
		if q.Lane == "retained-history-feature" {
			r.Queries = append(r.Queries, QueryStatus{ID: q.ID, Lane: q.Lane, Status: "pending-separate-history-feature-adapter"})
			r.PendingHistory++
		}
	}
	if c.Backend == "badger" {
		if err := a.Reopen(); err != nil {
			return Report{}, err
		}
		for _, q := range queries {
			if q.Lane != "basic-graph-current" || q.Revision != 3 {
				continue
			}
			path := filepath.Join(tmp, "reopen."+q.ID+".jsonl")
			if err := exportQuery(ctx, a, q.Request, path, c.Limits); err != nil {
				return Report{}, err
			}
			if _, err := normalize(ctx, c, q, path, path+".meta.json", id); err != nil {
				return Report{}, err
			}
			r.ReopenedCurrent++
		}
	}
	a.call("history_counts")
	h, supported, err := a.g.Stats().HistoryCounts()
	if err != nil {
		return Report{}, err
	}
	if !supported {
		return Report{}, ErrUnsupported
	}
	if h.Nodes < 0 || h.Rels < 0 {
		return Report{}, ErrContract
	}
	// #nosec G115 -- Native counts are checked nonnegative above; int values fit uint64.
	r.History = HistoryCost{uint64(h.Nodes), uint64(h.Rels), "Exact native count of IDs with history, including deleted identities; not total versions"}
	r.RangeCandidates = a.RangeCandidates
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	r.Costs.HeapAllocBytes = mem.HeapAlloc
	r.Costs.MemoryScope = "Observed whole harness process heap snapshot with native engine/adapter; includes temporary/native caches; no isolated engine memory or peak claim"
	r.Costs.Unmeasured = []string{"RSS/PSS", "mmap/page cache", "cgroup/host total", "peak heap/disk", "disk bytes by semantic category", "internal storage/fsync calls", "CPU/latency/throughput"}
	if err := a.Close(); err != nil {
		return Report{}, err
	}
	r.NativeCalls = maps.Clone(a.Calls)
	if err := diskCosts(dir, &r.Costs); err != nil {
		return Report{}, err
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	if r.ValidatedCurrent != 92 || r.PendingHistory != 1 {
		return Report{}, ErrContract
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return Report{}, err
	}
	if err := os.WriteFile(filepath.Join(tmp, "report.json"), append(b, '\n'), 0600); err != nil {
		return Report{}, err
	}
	if err := os.Rename(tmp, c.Output); err != nil {
		return Report{}, err
	}
	published = true
	return r, nil
}
