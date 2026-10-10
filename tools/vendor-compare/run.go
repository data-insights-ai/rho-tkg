package vendorcompare

import (
	"bufio"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
)

//go:embed pins/reference.json
var referencePin []byte

// Backend answers from native storage; no oracle values enter these methods.
type Backend interface {
	Load(context.Context, Entity) error
	Apply(context.Context, Event) error
	Query(context.Context, Request, func(any) error) error
	Traffic() (Traffic, error)
	PhysicalInventory(context.Context) (Inventory, error)
}

// Lifecycle operates only the separately provisioned, verified owned deployment.
type Lifecycle interface {
	Prepare(context.Context) (Identity, error)
	Reopen(context.Context) error
	Observe(context.Context) (Costs, error)
}

// Validator checks a completed native answer against the frozen independent reference.
type Validator interface {
	Validate(context.Context, string, Query, string, string) (Answer, error)
}

// Config bounds complete exported answers and selects a new owned artifact directory.
type Config struct {
	Reference, Output string
	Limits            Limits
}

// Answer records equality for a complete multiset, including every duplicate.
type Answer struct {
	QueryID string `json:"query_id"`
	Phase   string `json:"phase"`
	Rows    int64  `json:"rows"`
	SHA256  string `json:"normalized_multiset_sha256"`
}

// Identity states deployment evidence separately from any durability/performance verdict.
type Identity struct {
	Vendor              string `json:"vendor"`
	Version             string `json:"version"`
	ImageReference      string `json:"image_reference"`
	ImageID             string `json:"image_id"`
	Platform            string `json:"platform"`
	HostArchitecture    string `json:"docker_host_architecture"`
	ExecutionMode       string `json:"execution_mode"`
	Project             string `json:"project"`
	ContainerID         string `json:"container_id"`
	DeclaredCopies      int    `json:"declared_data_copies"`
	DeclaredPartitions  int    `json:"declared_partitions"`
	VersionOutputSHA256 string `json:"version_output_sha256"`
}

// Report grants only all current/reopened answer equality, never benchmark acceptance.
type Report struct {
	SchemaVersion         int       `json:"schema_version"`
	Status                string    `json:"status"`
	OutputDirectory       string    `json:"output_directory"`
	DatasetSHA256         string    `json:"dataset_sha256"`
	Identity              Identity  `json:"identity"`
	Answers               []Answer  `json:"answers"`
	CurrentAnswers        int       `json:"current_answers"`
	ReopenedAnswers       int       `json:"reopened_answers"`
	Pending               []string  `json:"pending"`
	Inventory             Inventory `json:"physical_inventory"`
	Traffic               Traffic   `json:"application_http_traffic"`
	Costs                 Costs     `json:"costs"`
	PerformanceAcceptance bool      `json:"performance_acceptance"`
	V7Complete            bool      `json:"V7_complete"`
	Durability            string    `json:"durable_acknowledgement"`
	Limitations           []string  `json:"limitations"`
}
type fixture struct {
	id           string
	nodes, edges []Entity
	events       []Event
	queries      []Query
}

func checksum(data []byte) string { hash := sha256.Sum256(data); return hex.EncodeToString(hash[:]) }
func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Func, reflect.Chan, reflect.Slice:
		return v.IsNil()
	}
	return false
}
func copyReference(source, target string) error {
	if checksum(referencePin) != referencePinSHA {
		return ErrContract
	}
	var pin struct {
		Files map[string]string `json:"files_sha256"`
		Head  string            `json:"copied_from_source_head"`
	}
	if err := strictJSON(referencePin, &pin); err != nil || len(pin.Files) != 114 {
		return ErrContract
	}
	info, err := os.Lstat(source)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(ErrContract, err)
	}
	seen := map[string]bool{}
	err = filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return ErrContract
		}
		name, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		expected, ok := pin.Files[name]
		if !ok {
			return ErrContract
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if checksum(data) != expected {
			return ErrContract
		}
		destination := filepath.Join(target, name)
		if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
			return err
		}
		if err := os.WriteFile(destination, data, 0600); err != nil {
			return err
		}
		seen[name] = true
		return nil
	})
	if err != nil || len(seen) != 114 {
		return errors.Join(ErrContract, err)
	}
	return nil
}
func readRows[T any](path string, limit int) ([]T, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var rows []T
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		if len(rows) >= limit {
			_ = file.Close()
			return nil, ErrLimit
		}
		var row T
		if err := strictJSON(scanner.Bytes(), &row); err != nil {
			_ = file.Close()
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, errors.Join(scanner.Err(), file.Close())
}
func readFixture(reference string) (fixture, error) {
	var f fixture
	dataset := filepath.Join(reference, "fixtures/basic-small")
	data, err := os.ReadFile(filepath.Join(dataset, "manifest.json"))
	if err != nil {
		return f, err
	}
	var manifest struct {
		ID string `json:"canonical_input_sha256"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return f, err
	}
	if manifest.ID != "d72e780e2c7638be85d658c183a64ecc3cb70754e5ae4716b328e69a9d2080ec" {
		return f, ErrContract
	}
	f.id = manifest.ID
	f.nodes, err = readRows[Entity](filepath.Join(dataset, "nodes.jsonl"), 128)
	if err != nil {
		return fixture{}, err
	}
	f.edges, err = readRows[Entity](filepath.Join(dataset, "edges.jsonl"), 256)
	if err != nil {
		return fixture{}, err
	}
	f.events, err = readRows[Event](filepath.Join(dataset, "changes.jsonl"), 128)
	if err != nil {
		return fixture{}, err
	}
	data, err = os.ReadFile(filepath.Join(dataset, "queries.json"))
	if err != nil {
		return fixture{}, err
	}
	if err := strictJSON(data, &f.queries); err != nil {
		return fixture{}, err
	}
	if len(f.nodes) != 32 || len(f.edges) != 64 || len(f.events) != 9 || len(f.queries) != 93 {
		return fixture{}, ErrContract
	}
	last := 0
	for _, row := range append(slices.Clone(f.nodes), f.edges...) {
		if err := validateRow(row); err != nil {
			return fixture{}, err
		}
	}
	for _, event := range f.events {
		if event.Revision < last || event.Revision < 1 || event.Revision > 3 {
			return fixture{}, ErrContract
		}
		last = event.Revision
	}
	seen := map[string]bool{}
	current := map[int]int{}
	history := 0
	for _, query := range f.queries {
		if seen[query.ID] || query.ID == "" || strings.ContainsAny(query.ID, "/\\") {
			return fixture{}, ErrContract
		}
		seen[query.ID] = true
		if query.Lane == "basic-graph-current" {
			if query.Revision < 0 || query.Revision > 3 {
				return fixture{}, ErrContract
			}
			current[query.Revision]++
			request := query.Request
			if request.Op == "label" {
				request.Kind = "node"
			}
			if request.Op == "type" {
				request.Kind = "edge"
			}
			if err := validateRequest(request); err != nil {
				return fixture{}, err
			}
		} else if query.ID == "history.retained-versions" && query.Lane == "retained-history-feature" {
			history++
		} else {
			return fixture{}, ErrUnsupported
		}
	}
	for stage := range 4 {
		if current[stage] != 23 {
			return fixture{}, ErrContract
		}
	}
	if history != 1 {
		return fixture{}, ErrContract
	}
	return f, nil
}

// PythonValidator invokes only the pinned normalizer over complete staged exports.
type PythonValidator struct{}

func (PythonValidator) Validate(ctx context.Context, reference string, query Query, path, id string) (Answer, error) {
	if ctx == nil {
		return Answer{}, ErrContract
	}
	meta := map[string]string{"mapping_version": "basic-graph-v1", "dataset_sha256": id, "query_id": query.ID, "lane": query.Lane}
	data, err := json.Marshal(meta)
	if err != nil {
		return Answer{}, err
	}
	if err := os.WriteFile(path+".meta.json", data, 0600); err != nil {
		return Answer{}, err
	}
	// #nosec G204 -- Fixed interpreter/arguments; reference source bytes were pinned.
	command := exec.CommandContext(ctx, "python3", "-B", filepath.Join(reference, "normalize.py"), "--dataset", filepath.Join(reference, "fixtures/basic-small"), "--query", query.ID, "--actual", path, "--metadata", path+".meta.json")
	output, err := command.CombinedOutput()
	if err != nil {
		return Answer{}, fmt.Errorf("%w: normalizer refused %s output_sha256=%s", errors.Join(ErrContract, ctx.Err(), err), query.ID, checksum(output))
	}
	var result struct {
		Rows   int64  `json:"rows"`
		SHA    string `json:"normalized_multiset_sha256"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal(output, &result); err != nil || result.Result != "equal-canonical-output" || result.Rows != query.Expected.Rows || len(result.SHA) != 64 {
		return Answer{}, ErrContract
	}
	return Answer{QueryID: query.ID, Rows: result.Rows, SHA256: result.SHA}, nil
}
func exportAnswer(ctx context.Context, backend Backend, request Request, path string, limits Limits) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	rows, size := 0, 0
	err = backend.Query(ctx, request, func(row any) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := json.Marshal(row)
		if err != nil {
			return err
		}
		if rows >= limits.MaxRows || rows >= limits.MaxVisited || len(data)+1 > limits.MaxBytes-size {
			return ErrLimit
		}
		rows++
		size += len(data) + 1
		_, err = file.Write(append(data, '\n'))
		return err
	})
	if err != nil {
		return errors.Join(err, file.Close())
	}
	return errors.Join(file.Sync(), file.Close())
}

// Run writes a local completion receipt only after all 92 current and 23 restarted native answers
// match. Failure may leave the owned vendor instance mutated, never a completion receipt.
func Run(ctx context.Context, config Config, backend Backend, lifecycle Lifecycle, validator Validator) (Report, error) {
	if ctx == nil || nilInterface(backend) || nilInterface(lifecycle) || nilInterface(validator) || config.Reference == "" || !config.Limits.valid() {
		return Report{}, ErrContract
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	guard, err := guardOutput(ctx, config.Output, outsideGit)
	if err != nil {
		return Report{}, err
	}
	defer func() { _ = guard.close() }()
	config.Output = guard.path
	work, err := os.MkdirTemp(guard.parent, ".vendor-compare-")
	if err != nil {
		return Report{}, err
	}
	workLeaf := filepath.Base(work)
	workInfo, err := guard.root.Lstat(workLeaf)
	if err != nil {
		return Report{}, err
	}
	defer func() {
		current, err := guard.root.Lstat(workLeaf)
		if err == nil && current.IsDir() && os.SameFile(current, workInfo) {
			_ = guard.root.RemoveAll(workLeaf)
		}
	}()
	if err := guard.recheck(ctx); err != nil {
		return Report{}, err
	}
	reference := filepath.Join(work, "reference")
	if err := copyReference(config.Reference, reference); err != nil {
		return Report{}, err
	}
	f, err := readFixture(reference)
	if err != nil {
		return Report{}, err
	}
	if err := guard.recheck(ctx); err != nil {
		return Report{}, err
	}
	identity, err := lifecycle.Prepare(ctx)
	if err != nil {
		return Report{}, err
	}
	if identity.Vendor != "TigerGraph" || identity.Version != "4.2.5" || identity.ImageReference != ImageReference || identity.Platform != "linux/amd64" || identity.Project != ProjectName || identity.ContainerID == "" || identity.HostArchitecture == "" || identity.DeclaredCopies != 1 || identity.DeclaredPartitions != 1 {
		return Report{}, ErrContract
	}
	report := Report{SchemaVersion: 1, OutputDirectory: guard.path, DatasetSHA256: f.id, Identity: identity, Pending: []string{"history.retained-versions", "native-v5", "Neo4j", "Memgraph", "durable/power-loss acknowledgement", "native-x86 comparable performance", "retained/temporal/distributed lanes"},
		Durability:  "Upserts request ack=all and gsql-atomic-level:atomic; GPE acknowledgement/readback and graceful container restart do not establish fsync, power-loss or quorum durability.",
		Limitations: []string{"Frozen 32-node/64-edge fixture only; no large-data capacity or benchmark acceptance.", "Native REST scans, client predicates/projections and recursive adjacency walks; no native index/planner equivalence claim.", "One declared partition/copy; every observed forward/reverse/helper row is separately accounted.", "ARM Docker Desktop amd64 emulation must be labeled; never substitutes for native x86 comparisons.", "Cost receipt explicitly remains incomplete wherever metrics or categories are unavailable.", "Output artifacts are local and must remain outside Git worktrees."}}
	for _, row := range append(slices.Clone(f.nodes), f.edges...) {
		if err := backend.Load(ctx, row); err != nil {
			return Report{}, err
		}
	}
	next := 0
	validate := func(query Query, phase string) error {
		path := filepath.Join(work, phase+"."+query.ID+".jsonl")
		if err := guard.recheck(ctx); err != nil {
			return err
		}
		if err := exportAnswer(ctx, backend, query.Request, path, config.Limits); err != nil {
			return err
		}
		if err := guard.recheck(ctx); err != nil {
			return err
		}
		answer, err := validator.Validate(ctx, reference, query, path, f.id)
		if err != nil {
			return err
		}
		if answer.QueryID != query.ID || answer.Rows != query.Expected.Rows || len(answer.SHA256) != 64 {
			return ErrContract
		}
		answer.Phase = phase
		report.Answers = append(report.Answers, answer)
		return nil
	}
	for stage := range 4 {
		for next < len(f.events) && f.events[next].Revision == stage {
			if err := backend.Apply(ctx, f.events[next]); err != nil {
				return Report{}, err
			}
			next++
		}
		for _, query := range f.queries {
			if query.Lane == "basic-graph-current" && query.Revision == stage {
				if err := validate(query, "current"); err != nil {
					return Report{}, err
				}
				report.CurrentAnswers++
			}
		}
	}
	if next != len(f.events) || report.CurrentAnswers != 92 {
		return Report{}, ErrContract
	}
	if err := guard.recheck(ctx); err != nil {
		return Report{}, err
	}
	if err := lifecycle.Reopen(ctx); err != nil {
		return Report{}, err
	}
	for _, query := range f.queries {
		if query.Lane == "basic-graph-current" && query.Revision == 3 {
			if err := validate(query, "reopened"); err != nil {
				return Report{}, err
			}
			report.ReopenedAnswers++
		}
	}
	if report.ReopenedAnswers != 23 {
		return Report{}, ErrContract
	}
	report.Inventory, err = backend.PhysicalInventory(ctx)
	if err != nil {
		return Report{}, err
	}
	if report.Inventory.LogicalNodes != 32 || report.Inventory.LogicalEdges != 64 || report.Inventory.ForwardRows != 64 || report.Inventory.ReverseRows != 64 {
		return Report{}, ErrContract
	}
	report.Costs, err = lifecycle.Observe(ctx)
	if err != nil {
		return Report{}, err
	} // Ownership/observer failure cannot certify this deployment; per-metric unavailability is represented in Costs.
	report.Traffic, err = backend.Traffic()
	if err != nil {
		return Report{}, err
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	report.Status = "all-92-current-and-23-reopened-answers-equal"
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return Report{}, err
	}
	if err := guard.recheck(ctx); err != nil {
		return Report{}, err
	}
	if err := os.WriteFile(filepath.Join(work, "completion.json"), append(data, '\n'), 0600); err != nil {
		return Report{}, err
	}
	// Reserve destination atomically. Write complete answers before the completion marker.
	// An interrupted write may leave incomplete owned artifacts without a completion marker.
	if err := guard.create(ctx); err != nil {
		return Report{}, err
	}
	entries, err := os.ReadDir(work)
	if err != nil {
		return Report{}, err
	}
	slices.SortFunc(entries, func(a, b os.DirEntry) int {
		if a.Name() == "completion.json" {
			return 1
		}
		if b.Name() == "completion.json" {
			return -1
		}
		return strings.Compare(a.Name(), b.Name())
	})
	sourceRoot, err := guard.root.OpenRoot(workLeaf)
	if err != nil {
		return Report{}, err
	}
	defer func() { _ = sourceRoot.Close() }()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		} // Frozen input copy stays disposable reference work.
		if err := ctx.Err(); err != nil {
			return Report{}, err
		}
		if err := guard.copyFile(ctx, sourceRoot, entry.Name()); err != nil {
			return Report{}, err
		}
	}
	return report, nil
}
