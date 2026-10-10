package vendorcompare

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

type fakeLifecycle struct {
	reopens int
	fail    bool
}

func (f *fakeLifecycle) Prepare(context.Context) (Identity, error) {
	if f.fail {
		return Identity{}, ErrContract
	}
	return Identity{Vendor: "TigerGraph", Version: "4.2.5", ImageReference: ImageReference, Platform: "linux/amd64", HostArchitecture: "aarch64", ExecutionMode: "amd64 emulation; fixture-only fake", Project: ProjectName, ContainerID: "test-owned", DeclaredCopies: 1, DeclaredPartitions: 1}, nil
}
func (f *fakeLifecycle) Reopen(context.Context) error { f.reopens++; return nil }
func (f *fakeLifecycle) Observe(context.Context) (Costs, error) {
	return unavailableCosts("fake resource collector; no vendor metrics"), nil
}

type exactWireValidator struct{ fail bool }

func canonicalRows(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var rows []string
	scan := bufio.NewScanner(file)
	scan.Buffer(make([]byte, 4096), 1<<20)
	for scan.Scan() {
		decoder := json.NewDecoder(bytes.NewReader(scan.Bytes()))
		decoder.UseNumber()
		var row any
		if err := decoder.Decode(&row); err != nil {
			_ = file.Close()
			return nil, err
		}
		data, err := json.Marshal(row)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		rows = append(rows, string(data))
	}
	err = errors.Join(scan.Err(), file.Close())
	slices.Sort(rows)
	return rows, err
}
func (v exactWireValidator) Validate(_ context.Context, reference string, query Query, path, id string) (Answer, error) {
	if v.fail {
		return Answer{}, ErrContract
	}
	expected, err := canonicalRows(filepath.Join(reference, "fixtures/basic-small", query.ExpectedFile))
	if err != nil {
		return Answer{}, err
	}
	actual, err := canonicalRows(path)
	if err != nil {
		return Answer{}, err
	}
	if !slices.Equal(expected, actual) {
		return Answer{}, ErrContract
	}
	return Answer{QueryID: query.ID, Rows: int64(len(actual)), SHA256: checksum([]byte(strings.Join(actual, "\n") + "\n"))}, nil
}
func protectedReference(t *testing.T) string {
	t.Helper()
	reference := os.Getenv("VENDOR_COMPARE_REFERENCE")
	if reference == "" {
		t.Fatal("VENDOR_COMPARE_REFERENCE must name the unchanged 114-file reference for source-isolated tests")
	}
	return reference
}
func TestRunAll92Plus23FromNativeHTTPFakeAndCompleteMultisets(t *testing.T) {
	_, backend := newFake(t)
	controller := &fakeLifecycle{}
	base := t.TempDir()
	t.Setenv("TMPDIR", base)
	report, err := Run(t.Context(), Config{Reference: protectedReference(t), Limits: defaultLimits()}, backend, controller, exactWireValidator{})
	if err != nil {
		t.Fatal(err)
	}
	if report.CurrentAnswers != 92 || report.ReopenedAnswers != 23 || len(report.Answers) != 115 || controller.reopens != 1 || report.PerformanceAcceptance || report.V7Complete || report.Costs.Complete {
		t.Fatal(report)
	}
	if report.Inventory.ForwardRows != 64 || report.Inventory.ReverseRows != 64 {
		t.Fatal(report.Inventory)
	}
	if !filepath.IsAbs(report.OutputDirectory) {
		t.Fatal("missing resolved output", report.OutputDirectory)
	}
	if _, err := os.Stat(filepath.Join(report.OutputDirectory, "completion.json")); err != nil {
		t.Fatal(err)
	}
	// Returned costs must describe unavailable measurements, never a fabricated 0.
	if report.Costs.ClientMemory.Value != nil || report.Costs.AllReplicaTotal.Value != nil {
		t.Fatal("fabricated resource value")
	}
}
func TestRunRefusesPartialFailureNoCompletionAndNoOverwrite(t *testing.T) {
	reference := protectedReference(t)
	for _, failure := range []string{"lifecycle", "validator", "limit", "nil-backend", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			_, native := newFake(t)
			var backend Backend = native
			controller := &fakeLifecycle{fail: failure == "lifecycle"}
			validator := exactWireValidator{fail: failure == "validator"}
			limits := defaultLimits()
			if failure == "limit" {
				limits.MaxRows = 1
			}
			if failure == "nil-backend" {
				backend = (*TigerGraph)(nil)
			}
			ctx := t.Context()
			if failure == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			output := filepath.Join(t.TempDir(), "output")
			result, err := Run(ctx, Config{Reference: reference, Output: output, Limits: limits}, backend, controller, validator)
			if err == nil || result.Status != "" {
				t.Fatal(result, err)
			}
			if _, err := os.Stat(filepath.Join(output, "completion.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("partial success", err)
			}
		})
	}
	dir := t.TempDir()
	if _, err := Run(t.Context(), Config{Reference: reference, Output: dir, Limits: defaultLimits()}, (*TigerGraph)(nil), &fakeLifecycle{}, exactWireValidator{}); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
}
func TestPythonValidatorNilAndFrozenFixtureIntegrity(t *testing.T) {
	if _, err := (PythonValidator{}).Validate(nil, "", Query{}, "", ""); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "reference")
	if err := copyReference(protectedReference(t), target); err != nil {
		t.Fatal(err)
	}
	if _, err := readFixture(target); err != nil {
		t.Fatal(err)
	}
	f, err := readFixture(target)
	if err != nil {
		t.Fatal(err)
	}
	query := f.queries[0]
	actual := filepath.Join(t.TempDir(), "actual.jsonl")
	expected, err := os.ReadFile(filepath.Join(target, "fixtures/basic-small", query.ExpectedFile))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(actual, expected, 0600); err != nil {
		t.Fatal(err)
	}
	result, err := (PythonValidator{}).Validate(t.Context(), target, query, actual, f.id)
	if err != nil || result.Rows != query.Expected.Rows {
		t.Fatal(result, err)
	}
	if err := os.WriteFile(actual, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := (PythonValidator{}).Validate(t.Context(), target, query, actual, f.id); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "extra.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := copyReference(target, filepath.Join(t.TempDir(), "rejected")); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	if _, err := Run(nil, Config{}, nil, nil, nil); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
}
func TestInputJSONScalarsAndRequestContractsRefuseMalformedValues(t *testing.T) {
	var object struct {
		Known string `json:"known"`
	}
	if err := strictJSON([]byte(`{"known":"value"}`), &object); err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{[]byte(`{"known":"a","known":"b"}`), []byte(`{"other":0}`), []byte(`{} {}`), []byte(`{"known":`), {0xff}, []byte(strings.Repeat(" ", 1<<20) + "{}"), []byte(strings.Repeat("[", 18) + "0" + strings.Repeat("]", 18))} {
		if err := strictJSON(data, &object); !errors.Is(err, ErrContract) {
			t.Fatal(string(data), err)
		}
	}
	for _, n := range []int{4096, 4097} {
		data := []byte("[" + strings.Repeat("0,", n-1) + "0]")
		var values []int
		err := strictJSON(data, &values)
		if n == 4096 {
			if err != nil || len(values) != n {
				t.Fatal(n, err)
			}
		} else if !errors.Is(err, ErrContract) {
			t.Fatal(n, err)
		}
	}
	for _, n := range []int{512, 513} {
		values := map[string]int{}
		for i := range n {
			values[fmt.Sprint(i)] = i
		}
		data, _ := json.Marshal(values)
		var decoded map[string]int
		err := strictJSON(data, &decoded)
		if n == 512 {
			if err != nil || len(decoded) != n {
				t.Fatal(n, err)
			}
		} else if !errors.Is(err, ErrContract) {
			t.Fatal(n, err)
		}
	}
	for _, cell := range []Cell{{Type: "bool", Value: json.RawMessage("null")}, {Type: "bool", Value: json.RawMessage("true"), Bits: "0"}, {Type: "i64"}, {Type: "i64", Value: json.RawMessage("1.0")}, {Type: "i64", Value: json.RawMessage(`"broken"`)}, {Type: "i64", Value: json.RawMessage("1"), Bits: "0"}, {Type: "text", Value: json.RawMessage(`"text"`), Bits: "0"}, {Type: "f64", Bits: "0"}, {Type: "f64", Bits: "000000000000000A"}, {Type: "f64", Bits: "7ff8000000000000"}, {Type: "f64", Bits: "fff0000000000000"}, {Type: "f64", Bits: "0000000000000000", Value: json.RawMessage("0")}} {
		if _, err := cell.Native(); !errors.Is(err, ErrContract) {
			t.Fatal(cell, err)
		}
	}
	for _, value := range []any{nil, []int{1}, math.NaN(), math.Inf(1), string([]byte{0xff})} {
		if _, err := cellFromNative(value); !errors.Is(err, ErrContract) {
			t.Fatal(value, err)
		}
	}
	for _, test := range []struct {
		request Request
		want    error
	}{
		{Request{Op: "unknown", Kind: "node"}, ErrUnsupported}, {Request{Op: "scan", Kind: "unknown"}, ErrContract},
		{Request{Op: "label", Kind: "node"}, ErrContract}, {Request{Op: "type", Kind: "edge", Type: strings.Repeat("a", 257)}, ErrContract},
		{Request{Op: "projection", Kind: "node"}, ErrContract}, {Request{Op: "projection", Kind: "node", Keys: []string{"a", "a"}}, ErrContract}, {Request{Op: "projection", Kind: "node", Keys: []string{""}}, ErrContract},
		{Request{Op: "equality", Kind: "edge"}, ErrContract}, {Request{Op: "equality", Kind: "node", Key: "k", Value: intCell("bad")}, ErrContract},
		{Request{Op: "range", Kind: "node", Key: "k", Low: intCell("1"), High: boolCell(true)}, ErrContract}, {Request{Op: "range", Kind: "node", Key: "k", Low: intCell("2"), High: intCell("1")}, ErrContract},
		{Request{Op: "range", Kind: "node", Key: "k", Low: Cell{Type: "f64", Bits: "4000000000000000"}, High: Cell{Type: "f64", Bits: "3ff0000000000000"}}, ErrContract}, {Request{Op: "range", Kind: "edge", Key: "k", Low: boolCell(false), High: boolCell(true)}, ErrUnsupported},
		{Request{Op: "adjacency", ID: nodeID(0), Direction: "sideways"}, ErrContract}, {Request{Op: "expand", ID: nodeID(0), MaxDepth: 5}, ErrContract},
	} {
		if err := validateRequest(test.request); !errors.Is(err, test.want) {
			t.Fatal(test.request, err, test.want)
		}
	}
}
func TestReferenceMissingChangedExtraAndSymlinkFilesRefuseBeforeVendor(t *testing.T) {
	for _, fault := range []string{"missing", "changed", "extra", "symlink-file", "symlink-root"} {
		t.Run(fault, func(t *testing.T) {
			base := t.TempDir()
			reference := filepath.Join(base, "reference")
			if err := copyReference(protectedReference(t), reference); err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(reference, "fixtures/basic-small/nodes.jsonl")
			switch fault {
			case "missing":
				if err := os.Remove(file); err != nil {
					t.Fatal(err)
				}
			case "changed":
				if err := os.WriteFile(file, []byte("{}\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "extra":
				if err := os.WriteFile(filepath.Join(reference, "extra"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink-file":
				if err := os.Remove(file); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(base, "foreign"), file); err != nil {
					t.Fatal(err)
				}
			case "symlink-root":
				alias := filepath.Join(base, "alias")
				if err := os.Symlink(reference, alias); err != nil {
					t.Fatal(err)
				}
				reference = alias
			}
			spy := &untouchedVendor{}
			output := filepath.Join(t.TempDir(), "output")
			if _, err := Run(t.Context(), Config{Reference: reference, Output: output, Limits: defaultLimits()}, spy, spy, exactWireValidator{}); !errors.Is(err, ErrContract) || spy.calls != 0 {
				t.Fatal(fault, err, spy.calls)
			}
			if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid reference created output", err)
			}
		})
	}
}
func TestFixtureRowsQueryStagesAndRevisionOrderContracts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rows.jsonl")
	if err := os.WriteFile(path, []byte("{}\n{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readRows[Entity](path, 1); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readRows[Entity](path, 1); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	for _, fault := range []string{"dataset-id", "duplicate-query", "query-path", "query-lane", "query-revision", "stage-count", "event-order", "invalid-row"} {
		t.Run(fault, func(t *testing.T) {
			reference := filepath.Join(t.TempDir(), "reference")
			if err := copyReference(protectedReference(t), reference); err != nil {
				t.Fatal(err)
			}
			dataset := filepath.Join(reference, "fixtures/basic-small")
			f, err := readFixture(reference)
			if err != nil {
				t.Fatal(err)
			}
			write := func(name string, value any) {
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dataset, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			want := ErrContract
			switch fault {
			case "dataset-id":
				write("manifest.json", map[string]string{"canonical_input_sha256": "changed"})
			case "duplicate-query":
				f.queries[1].ID = f.queries[0].ID
				write("queries.json", f.queries)
			case "query-path":
				f.queries[0].ID = "../answer"
				write("queries.json", f.queries)
			case "query-lane":
				f.queries[0].Lane = "unknown"
				write("queries.json", f.queries)
				want = ErrUnsupported
			case "query-revision":
				f.queries[0].Revision = 4
				write("queries.json", f.queries)
			case "stage-count":
				f.queries[0].Revision = 1
				write("queries.json", f.queries)
			case "event-order":
				f.events[0].Revision = 2
				file, err := os.Create(filepath.Join(dataset, "changes.jsonl"))
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range f.events {
					if err := json.NewEncoder(file).Encode(event); err != nil {
						t.Fatal(err)
					}
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			case "invalid-row":
				f.nodes[0].Labels = append(f.nodes[0].Labels, f.nodes[0].Labels[0])
				file, err := os.Create(filepath.Join(dataset, "nodes.jsonl"))
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range f.nodes {
					if err := json.NewEncoder(file).Encode(row); err != nil {
						t.Fatal(err)
					}
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := readFixture(reference); !errors.Is(err, want) {
				t.Fatal(fault, err, want)
			}
		})
	}
}
func TestRawUTF8TextRefusesBeforeNormalizationAndMutation(t *testing.T) {
	controls := []struct{ raw, want string }{
		{`"\ud83d\ude42"`, "🙂"},
		{`"\ufffd"`, "�"},
		{`"�"`, "�"},
		{`"\\ud800"`, `\ud800`},
		{`"quoted \"\\café\u0020\ud83d\ude42"`, `quoted "\café 🙂`},
		{`"café \n🙂"`, "café \n🙂"},
	}
	for _, control := range controls {
		cell := Cell{Type: "text", Value: json.RawMessage(control.raw)}
		if got, err := cell.Native(); err != nil || got != control.want {
			t.Fatal("valid Unicode control", control, got, err)
		}
		var decoded string
		if err := strictJSON(cell.Value, &decoded); err != nil || decoded != control.want {
			t.Fatal("outer Unicode control", control, decoded, err)
		}
		row := literalNodes()[0]
		row.Properties["p_text"] = cell
		attrs, err := attributes(row)
		if err != nil || attrs["p_text"].Value != control.want {
			t.Fatal("Unicode attributes", control, attrs, err)
		}
		_, client := newFake(t)
		if err := client.Load(t.Context(), row); err != nil {
			t.Fatal("Unicode native round trip", control, err)
		}
		rows := collect(t, client, Request{Op: "lookup", Kind: "node", ID: row.ID})
		if len(rows) != 1 {
			t.Fatal(rows)
		}
		got, err := rows[0].(Entity).Properties["p_text"].Native()
		if err != nil || got != control.want {
			t.Fatal("Unicode export", control, got, err)
		}
	}
	invalid := []json.RawMessage{
		{'"', 0xff, '"'},
		json.RawMessage(`"\ud800"`), json.RawMessage(`"\udc00"`), json.RawMessage(`"\udc00\ud800"`),
		json.RawMessage(`"\ud800\u0041"`), json.RawMessage(`"\u12xz"`), json.RawMessage(`"\u123"`), json.RawMessage(`"trailing\`),
	}
	for i, raw := range invalid {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			cell := Cell{Type: "text", Value: raw}
			if got, err := cell.Native(); !errors.Is(err, ErrContract) {
				t.Error("invalid Unicode normalized", got, err)
			}
			var decoded string
			if err := strictJSON(raw, &decoded); !errors.Is(err, ErrContract) {
				t.Error("outer decoder normalized invalid Unicode", decoded, err)
			}
			row := literalNodes()[0]
			row.Properties["p_text"] = cell
			if _, err := attributes(row); !errors.Is(err, ErrContract) {
				t.Error("lossy attributes admitted", err)
			}
			_, client := newFake(t)
			if err := client.Load(t.Context(), row); !errors.Is(err, ErrContract) {
				t.Error("lossy Load admitted", err)
			}
			traffic, _ := client.Traffic()
			if traffic.ByMethod["POST"] != 0 {
				t.Error("invalid Load mutated state", traffic)
			}
			_, client = newFake(t)
			if err := client.Load(t.Context(), literalNodes()[0]); err != nil {
				t.Fatal(err)
			}
			before, _ := client.Traffic()
			if err := client.Apply(t.Context(), Event{Revision: 1, Op: "update", Kind: "node", ID: nodeID(0), Set: map[string]Cell{"p_text": cell}}); !errors.Is(err, ErrContract) {
				t.Error("lossy Apply admitted", err)
			}
			after, _ := client.Traffic()
			if before.ByMethod["POST"] != after.ByMethod["POST"] {
				t.Error("invalid Apply mutated state", before, after)
			}
		})
	}
}
func TestUnicodeLexicalCheckLeavesGrammarToDecoder(t *testing.T) {
	for _, raw := range []string{`{"\\ud800":"literal backslash"}`, `"\\\"\\\\"`, `"\ud83D\uDe42"`} {
		if !validJSONUnicode([]byte(raw)) {
			t.Fatal("valid lexical control refused", raw)
		}
	}
	for _, raw := range []string{`"\ud800"`, `"\udc00"`, `"\ud800\u0041"`, `"\ud800\uDCx0"`, `"\u12xz"`, `"\u123`, `"tail\`, `"unterminated`} {
		if validJSONUnicode([]byte(raw)) {
			t.Fatal("invalid lexical Unicode admitted", raw)
		}
	}
	if _, ok := jsonHex16([]byte("123")); ok {
		t.Fatal("truncated hex admitted")
	}
	if got, ok := jsonHex16([]byte("aBcD")); !ok || got != 0xabcd {
		t.Fatal(got, ok)
	}
	if !validJSONUnicode([]byte("not JSON")) {
		t.Fatal("Unicode checker claimed grammar validation")
	}
	var value any
	if err := strictJSON([]byte("not JSON"), &value); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
}
