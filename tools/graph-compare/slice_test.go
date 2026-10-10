package slice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func testCell(t *testing.T, wire string) Cell {
	t.Helper()
	c, err := parseCell([]byte(wire))
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func testID(kind string, n int) string { return kind + ":" + fmtHex(uint64(n)) }
func testEntities(t *testing.T) []Entity {
	t.Helper()
	return []Entity{
		{Kind: "node", ID: testID("n", 0), Labels: []string{"Entity", "A"}, Properties: map[string]Cell{"p_i64": testCell(t, `{"type":"i64","value":"-9223372036854775808"}`), "p_bool": testCell(t, `{"type":"bool","value":false}`), "p_text": testCell(t, `{"type":"text","value":""}`)}},
		{Kind: "node", ID: testID("n", 1), Labels: []string{"Entity", "B"}, Properties: map[string]Cell{"p_i64": testCell(t, `{"type":"i64","value":"9007199254740992"}`), "p_text": testCell(t, `{"type":"text","value":"café\n🙂"}`)}},
		{Kind: "node", ID: testID("n", 2), Labels: []string{"Entity", "C"}, Properties: map[string]Cell{"p_i64": testCell(t, `{"type":"i64","value":"9007199254740993"}`)}},
		{Kind: "edge", ID: testID("e", 0), Source: testID("n", 0), Target: testID("n", 1), Type: "HOP", Properties: map[string]Cell{"p_f64": testCell(t, `{"type":"f64","bits":"3ff8000000000000"}`)}},
		{Kind: "edge", ID: testID("e", 1), Source: testID("n", 0), Target: testID("n", 1), Type: "HOP", Properties: map[string]Cell{"p_f64": testCell(t, `{"type":"f64","bits":"3ff8000000000000"}`)}},
		{Kind: "edge", ID: testID("e", 2), Source: testID("n", 0), Target: testID("n", 0), Type: "HOP", Properties: map[string]Cell{}},
	}
}
func testAdapter(t *testing.T, backend string) *Adapter {
	t.Helper()
	dir := ""
	if backend == "badger" {
		dir = filepath.Join(t.TempDir(), "store")
	}
	a, err := openAdapter(backend, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, r := range testEntities(t) {
		if err := a.Load(t.Context(), r); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.BuildIndexes(); err != nil {
		t.Fatal(err)
	}
	return a
}
func queryRows(t *testing.T, a *Adapter, r Request) []json.RawMessage {
	t.Helper()
	var rows []json.RawMessage
	err := a.Query(t.Context(), r, func(v any) error {
		b, e := json.Marshal(v)
		if e == nil {
			rows = append(rows, b)
		}
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestContractRejectsFloatMediatedIntegersAndIDOverflow(t *testing.T) {
	for _, wire := range []string{`{"type":"i64","value":9007199254740993.0}`, `{"type":"i64","value":true}`, `{"type":"i64","value":"9223372036854775808"}`, `{"type":"i64","value":"01"}`, `{"type":"bool","value":0}`, `{"type":"bool","value":null}`, `{"type":"f64","bits":"7ff0000000000000"}`, `{"type":"text","value":null}`, `{"type":"i64","value":"1","bits":"0000000000000000"}`, `{"type":"bool","value":false,"value":true}`} {
		if _, err := parseCell([]byte(wire)); !errors.Is(err, ErrContract) {
			t.Fatal("malformed native scalar accepted", wire, err)
		}
	}
	for _, n := range []int64{math.MinInt64, -9007199254740993, 9007199254740993, math.MaxInt64} {
		c := testCell(t, `{"type":"i64","value":"`+strconv.FormatInt(n, 10)+`"}`)
		v, err := c.Native()
		if err != nil || v != n {
			t.Fatal("signed64 changed", n, v, err)
		}
		actual, err := cellFromNative(v)
		if err != nil || !reflect.DeepEqual(actual, c) {
			t.Fatal("native type/value lost", err)
		}
	}
	for _, v := range []any{uint64(1), int(1), math.Inf(1), math.NaN(), nil} {
		if _, err := cellFromNative(v); !errors.Is(err, ErrContract) {
			t.Fatal("unsupported native type coerced", v, err)
		}
	}
	for _, id := range []string{"n:7fffffffffffffff", "n:8000000000000000", "n:ffffffffffffffff", "e:0000000000000000", "n:0", "n:000000000000000G"} {
		if _, err := nativeID(id, "node"); !errors.Is(err, ErrContract) {
			t.Fatal("ID namespace/overflow accepted", id, err)
		}
	}
	for _, id := range []string{"n:0000000000000000", "n:7ffffffffffffffe"} {
		n, err := nativeID(id, "node")
		if err != nil || n <= 0 || externalID(n, "node") != id {
			t.Fatal("positive native ID roundtrip", id, n, err)
		}
	}
}

func TestNativeRangeResidualMultiplicityWalksAndMutation(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			a := testAdapter(t, backend)
			bound := testCell(t, `{"type":"i64","value":"9007199254740993"}`)
			r := queryRows(t, a, Request{Op: "range", Kind: "node", Key: "p_i64", Low: bound, High: bound})
			if len(r) != 1 || !bytes.Contains(r[0], []byte(testID("n", 2))) {
				t.Fatal("widened float candidate not checked exactly", r)
			}
			if a.RangeCandidates < 2 {
				t.Fatal("test failed to exercise widened same-float int64 candidates")
			}
			rows := queryRows(t, a, Request{Op: "adjacency", Direction: "out", ID: testID("n", 0)})
			if len(rows) != 3 {
				t.Fatal("parallel/self-edge collapsed", len(rows))
			}
			walks := queryRows(t, a, Request{Op: "expand", ID: testID("n", 0), MaxDepth: 2})
			if len(walks) != 6 {
				t.Fatal("repeatable walk multiset changed", len(walks))
			}
			found := false
			for _, wire := range walks {
				var row Walk
				if err := json.Unmarshal(wire, &row); err != nil {
					t.Fatal(err)
				}
				if reflect.DeepEqual(row.EdgeIDs, []string{testID("e", 2), testID("e", 2)}) {
					found = true
				}
			}
			if !found {
				t.Fatal("same self-edge could not repeat")
			}
			bad := Event{Revision: 1, Op: "delete", Kind: "node", ID: testID("n", 0)}
			if err := a.Apply(t.Context(), bad); !errors.Is(err, ErrContract) {
				t.Fatal("implicit cascade accepted noncanonical event", err)
			}
			before := queryRows(t, a, Request{Op: "lookup", Kind: "node", ID: testID("n", 0)})
			if err := a.Apply(t.Context(), Event{Revision: 1, Op: "update", Kind: "node", ID: testID("n", 0), Set: map[string]Cell{"p_i64": bound}, Remove: []string{"p_bool"}}); err != nil {
				t.Fatal(err)
			}
			if len(before) != 1 || !bytes.Contains(before[0], []byte(`"p_bool"`)) {
				t.Fatal("literal old export lost false")
			}
			current := queryRows(t, a, Request{Op: "lookup", Kind: "node", ID: testID("n", 0)})
			if len(current) != 1 || bytes.Contains(current[0], []byte(`"p_bool"`)) || !bytes.Contains(current[0], []byte("9007199254740993")) {
				t.Fatal("native removal/int64 update lost", string(current[0]))
			}
			replacement := testEntities(t)[0]
			replacement.Properties = map[string]Cell{"p_text": testCell(t, `{"type":"text","value":"new café"}`)}
			if err := a.Apply(t.Context(), Event{Revision: 2, Op: "upsert", Row: replacement}); err != nil {
				t.Fatal(err)
			}
			row := queryRows(t, a, Request{Op: "lookup", Kind: "node", ID: testID("n", 0)})
			if bytes.Contains(row[0], []byte(`"p_i64"`)) {
				t.Fatal("upsert merged rather than replaced properties")
			}
			if err := a.Apply(t.Context(), Event{Revision: 3, Op: "delete", Kind: "edge", ID: testID("e", 1)}); err != nil {
				t.Fatal(err)
			}
			if rows := queryRows(t, a, Request{Op: "lookup", Kind: "edge", ID: testID("e", 1)}); len(rows) != 0 {
				t.Fatal("deleted phantom")
			}
			if backend == "badger" {
				if err := a.Reopen(); err != nil {
					t.Fatal(err)
				}
				if rows := queryRows(t, a, Request{Op: "lookup", Kind: "node", ID: testID("n", 0)}); len(rows) != 1 || !bytes.Contains(rows[0], []byte("new café")) || bytes.Contains(rows[0], []byte(`"p_i64"`)) {
					t.Fatal("reopen changed current body")
				}
			}
		})
	}
}

func TestAtomicExportErrorCancelAndQuotaNeverPublishPartial(t *testing.T) {
	a := testAdapter(t, "memory")
	dir := t.TempDir()
	path := filepath.Join(dir, "answer.jsonl")
	if err := exportQuery(t.Context(), a, Request{Op: "scan", Kind: "node"}, path, Limits{MaxRows: 1, MaxBytes: 1 << 20, MaxVisited: 100}); !errors.Is(err, ErrLimit) {
		t.Fatal("row quota lost", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("quota published partial answer", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := exportQuery(ctx, a, Request{Op: "scan", Kind: "node"}, path, defaultLimits()); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel lost", err)
	}
	if err := exportQuery(t.Context(), a, Request{Op: "history"}, path, defaultLimits()); !errors.Is(err, ErrUnsupported) {
		t.Fatal("history labeled current success", err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := exportQuery(t.Context(), a, Request{Op: "scan", Kind: "node"}, path, defaultLimits()); err == nil {
		t.Fatal("closed backend published success")
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatal("failed export retained published/temporary bytes", files, err)
	}
}

func TestMalformedEmptyQueriesAndAtomicExistingOutput(t *testing.T) {
	a, err := openAdapter("memory", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, r := range []Request{{Op: "range", Kind: "node", Key: "missing"}, {Op: "equality", Kind: "edge", Key: "missing"}, {Op: "projection", Kind: "node", Keys: []string{"x", "x"}}, {Op: "adjacency", ID: testID("n", 9), Direction: "sideways"}, {Op: "expand", ID: testID("n", 9), MaxDepth: 5}} {
		calls := 0
		err := a.Query(t.Context(), r, func(any) error { calls++; return nil })
		if !errors.Is(err, ErrContract) || calls != 0 {
			t.Fatal("empty data hid malformed query", r, err)
		}
	}
	path := filepath.Join(t.TempDir(), "existing.jsonl")
	want := []byte("existing bytes")
	if err := os.WriteFile(path, want, 0600); err != nil {
		t.Fatal(err)
	}
	if err := exportQuery(t.Context(), a, Request{Op: "scan", Kind: "node"}, path, defaultLimits()); !errors.Is(err, ErrContract) {
		t.Fatal("existing output accepted", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("existing output modified", err)
	}
	loaded := testAdapter(t, "memory")
	if err := exportQuery(t.Context(), loaded, Request{Op: "scan", Kind: "node"}, filepath.Join(t.TempDir(), "work.jsonl"), Limits{MaxRows: 100, MaxBytes: 1 << 20, MaxVisited: 1}); !errors.Is(err, ErrLimit) {
		t.Fatal("work budget ignored", err)
	}
	if err := exportQuery(t.Context(), loaded, Request{Op: "scan", Kind: "node"}, filepath.Join(t.TempDir(), "bytes.jsonl"), Limits{MaxRows: 100, MaxBytes: 1, MaxVisited: 100}); !errors.Is(err, ErrLimit) {
		t.Fatal("byte budget ignored", err)
	}
}

func TestAll92ExportsAndBadgerReopenWithSeparateHistoryInventory(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "published")
			r, err := Run(t.Context(), RunConfig{Dataset: "../reference/fixtures/basic-small", Reference: "../reference", ReferencePin: "../reference-pin.json", Backend: backend, Output: out, Limits: defaultLimits()})
			if err != nil {
				t.Fatal(err)
			}
			if r.ValidatedCurrent != 92 || r.PendingHistory != 1 || len(r.Queries) != 93 || r.PerformanceAcceptance || r.NativeV5 {
				t.Fatal("incorrect capability/answer inventory", r.ValidatedCurrent, r.PendingHistory)
			}
			if len(r.Events) != 9 || r.NativeCalls["nodes_import"] != 33 || r.NativeCalls["rels_import"] != 66 {
				t.Fatal("native event/call ledger missing", r.Events, r.NativeCalls)
			}
			if backend == "badger" && r.ReopenedCurrent != 23 {
				t.Fatal("durable r3 reopen answer inventory", r.ReopenedCurrent)
			}
			if r.History.NodesWithHistory < 1 || r.History.RelsWithHistory < 1 || r.Costs.DiskScope == "" {
				t.Fatal("inherent history/storage costs omitted", r.History, r.Costs)
			}
		})
	}
}

func TestWrongPinsAndRunQuotaLeaveNoSuccessArtifact(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad-pin.json")
	if err := os.WriteFile(bad, []byte(`{"files_sha256":{"format.py":"0000000000000000000000000000000000000000000000000000000000000000"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		pin    string
		limits Limits
		cause  error
	}{{bad, defaultLimits(), ErrContract}, {"../reference-pin.json", Limits{MaxRows: 1, MaxBytes: 1 << 20, MaxVisited: 1000}, ErrLimit}} {
		out := filepath.Join(dir, "run")
		_, err := Run(t.Context(), RunConfig{Dataset: "../reference/fixtures/basic-small", Reference: "../reference", ReferencePin: tc.pin, Backend: "memory", Output: out, Limits: tc.limits})
		if !errors.Is(err, tc.cause) {
			t.Fatal("wrong pin/quota lost refusal", err)
		}
		if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("refused run published success artifact", err)
		}
	}
}

func TestAdmissionAndWholeRowLabelReplacementPreserveNativeTruth(t *testing.T) {
	var nilAdapter *Adapter
	for _, err := range []error{nilAdapter.Load(t.Context(), Entity{}), nilAdapter.Apply(t.Context(), Event{}), nilAdapter.BuildIndexes(), nilAdapter.Reopen(), nilAdapter.Close(), nilAdapter.Query(t.Context(), Request{}, func(any) error { return nil })} {
		if !errors.Is(err, ErrContract) {
			t.Fatal("nil pointer cause", err)
		}
	}
	if _, err := openAdapter("native-v5", ""); !errors.Is(err, ErrContract) {
		t.Fatal("fake native v5 accepted", err)
	}
	a := testAdapter(t, "memory")
	if err := a.Reopen(); !errors.Is(err, ErrUnsupported) {
		t.Fatal("memory labeled durable", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := a.Load(ctx, testEntities(t)[0]); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled mutation", err)
	}
	var nilContext context.Context
	if err := a.Load(nilContext, testEntities(t)[0]); !errors.Is(err, ErrContract) {
		t.Fatal("nil context", err)
	}
	for _, edit := range []func(*Entity){func(e *Entity) { e.Labels = append(e.Labels, e.Labels[0]) }, func(e *Entity) { e.Source = testID("n", 1) }, func(e *Entity) {
		e.Properties = map[string]Cell{"tkg_reserved": testCell(t, `{"type":"bool","value":false}`)}
	}, func(e *Entity) { e.Properties = nil }} {
		e := testEntities(t)[0]
		e.ID = testID("n", 20)
		edit(&e)
		if err := a.Load(t.Context(), e); !errors.Is(err, ErrContract) {
			t.Fatal("malformed entity admitted", err)
		}
		if rows := queryRows(t, a, Request{Op: "lookup", Kind: "node", ID: e.ID}); len(rows) != 0 {
			t.Fatal("refusal published entity")
		}
	}
	for _, event := range []Event{{Revision: 1, Op: "unknown", Kind: "node", ID: testID("n", 0)}, {Revision: 1, Op: "update", Kind: "node", ID: testID("n", 0), Set: map[string]Cell{}, Remove: []string{"absent"}}, {Revision: 1, Op: "update", Kind: "node", ID: testID("n", 0), Set: map[string]Cell{"p_bool": testCell(t, `{"type":"bool","value":true}`)}, Remove: []string{"p_bool"}}} {
		if err := a.Apply(t.Context(), event); err == nil {
			t.Fatal("invalid event admitted")
		}
	}
	e := testEntities(t)[0]
	e.Labels = []string{"Replacement"}
	e.Properties = map[string]Cell{"p_bool": testCell(t, `{"type":"bool","value":false}`)}
	if err := a.Apply(t.Context(), Event{Revision: 1, Op: "upsert", Row: e}); err != nil {
		t.Fatal(err)
	}
	rows := queryRows(t, a, Request{Op: "lookup", Kind: "node", ID: e.ID})
	var got Entity
	if len(rows) != 1 || json.Unmarshal(rows[0], &got) != nil || !reflect.DeepEqual(got.Labels, []string{"Replacement"}) || len(got.Properties) != 1 {
		t.Fatal("whole-row replacement retained old labels/properties")
	}
	if a.Calls["tx_add_label"] != 1 || a.Calls["tx_remove_label"] != 2 || a.Calls["tx_commit"] != 1 {
		t.Fatal("multi-call label transaction ledger", a.Calls)
	}
	if err := a.BuildIndexes(); err != nil {
		t.Fatal("explicit fallback cannot query nonuniform labels", err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Load(t.Context(), e); err == nil {
		t.Fatal("closed mutation")
	}
	if err := a.BuildIndexes(); err == nil {
		t.Fatal("closed index build")
	}
}

func TestReferenceNormalizerAndParserRefusalsAreExplicit(t *testing.T) {
	for _, b := range [][]byte{[]byte("{} trailing"), []byte("["), []byte(strings.Repeat("[", 18) + strings.Repeat("]", 18)), []byte("{\"x\":1,\"x\":2}"), {255}} {
		var v any
		if err := strictJSON(b, &v); !errors.Is(err, ErrContract) {
			t.Fatal("malformed JSON", err)
		}
	}
	for _, req := range []Request{{Op: "range", Kind: "node", Key: "x", Low: testCell(t, `{"type":"i64","value":"2"}`), High: testCell(t, `{"type":"i64","value":"1"}`)}, {Op: "range", Kind: "node", Key: "x", Low: testCell(t, `{"type":"text","value":"a"}`), High: testCell(t, `{"type":"text","value":"b"}`)}, {Op: "projection", Kind: "edge"}, {Op: "invalid", Kind: "node"}} {
		if err := validateRequest(req); err == nil {
			t.Fatal("invalid selector hidden by empty data", req)
		}
	}
	dir := t.TempDir()
	actual := filepath.Join(dir, "actual.jsonl")
	if err := os.WriteFile(actual, nil, 0600); err != nil {
		t.Fatal(err)
	}
	c := RunConfig{Dataset: "../reference/fixtures/basic-small", Reference: "../reference", ReferencePin: "../reference-pin.json"}
	id, qs, err := readQueries(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := normalize(t.Context(), c, qs[0], actual, actual+".meta", id); !errors.Is(err, ErrContract) {
		t.Fatal("incorrect complete export bypassed oracle", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := normalize(ctx, c, qs[0], actual, actual+".meta", id); !errors.Is(err, context.Canceled) {
		t.Fatal("normalization cancel", err)
	}
	if _, _, err := readQueries(RunConfig{Dataset: dir, Reference: c.Reference, ReferencePin: c.ReferencePin}); !errors.Is(err, ErrContract) {
		t.Fatal("wrong dataset root accepted", err)
	}
	bad := filepath.Join(dir, "row.jsonl")
	if err := os.WriteFile(bad, []byte("{\"wrong\":true}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := eachJSONL(bad, func(Entity) error { return nil }); !errors.Is(err, ErrContract) {
		t.Fatal("unknown entity shape", err)
	}
	if err := verifyPin(c.Reference, filepath.Join(dir, "missing"), referencePinSHA); !errors.Is(err, ErrContract) {
		t.Fatal("missing trusted pin", err)
	}
}

func TestRunRefusalsPreserveOwnedOutputsAndReturnZeroReport(t *testing.T) {
	dir := t.TempDir()
	c := RunConfig{Dataset: "../reference/fixtures/basic-small", Reference: "../reference", ReferencePin: "../reference-pin.json", Backend: "memory", Output: filepath.Join(dir, "new-run"), Limits: defaultLimits()}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		ctx    context.Context
		config RunConfig
		cause  error
	}{{nil, c, ErrContract}, {ctx, c, context.Canceled}, {t.Context(), RunConfig{}, ErrContract}} {
		got, err := Run(tc.ctx, tc.config)
		if !errors.Is(err, tc.cause) || !reflect.DeepEqual(got, Report{}) {
			t.Fatal("refused run returned nonzero success evidence", got, err)
		}
	}
	c.Backend = "native-v5"
	got, err := Run(t.Context(), c)
	if !errors.Is(err, ErrContract) || !reflect.DeepEqual(got, Report{}) {
		t.Fatal("unimplemented adapter success", got, err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
		t.Fatal("failed adapter leaked temporary output", entries, err)
	}
	c.Backend = "memory"
	sentinel := []byte("prior evidence must survive\n")
	if err := os.WriteFile(c.Output, sentinel, 0600); err != nil {
		t.Fatal(err)
	}
	got, err = Run(t.Context(), c)
	if !errors.Is(err, ErrContract) || !reflect.DeepEqual(got, Report{}) {
		t.Fatal("existing output relabeled", got, err)
	}
	if b, err := os.ReadFile(c.Output); err != nil || !bytes.Equal(b, sentinel) {
		t.Fatal("existing bytes changed", err)
	}
	c.Output = filepath.Join(c.Output, "child")
	got, err = Run(t.Context(), c)
	if err == nil || !reflect.DeepEqual(got, Report{}) {
		t.Fatal("unavailable output parent success", got, err)
	}
	a := testAdapter(t, "memory")
	var nilContext context.Context
	for _, err := range []error{exportQuery(nilContext, a, Request{}, filepath.Join(dir, "answer"), defaultLimits()), exportQuery(t.Context(), nil, Request{}, filepath.Join(dir, "answer"), defaultLimits()), exportQuery(t.Context(), a, Request{}, filepath.Join(dir, "answer"), Limits{})} {
		if !errors.Is(err, ErrContract) {
			t.Fatal("bad export admission lost contract cause", err)
		}
	}
	if err := exportQuery(t.Context(), a, Request{Op: "scan", Kind: "node"}, filepath.Join(dir, "missing", "answer"), defaultLimits()); err == nil {
		t.Fatal("unavailable answer destination success")
	}
}

func TestRunCannotLabelAnUnwritableWorkspaceSuccessful(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root does not exercise caller permission refusal")
	}
	parent := t.TempDir()
	if err := os.Chmod(parent, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(parent, 0700); err != nil {
			t.Error(err)
		}
	})
	out := filepath.Join(parent, "never-published")
	got, err := Run(t.Context(), RunConfig{Dataset: "../reference/fixtures/basic-small", Reference: "../reference", ReferencePin: "../reference-pin.json", Backend: "memory", Output: out, Limits: defaultLimits()})
	if !errors.Is(err, os.ErrPermission) || !reflect.DeepEqual(got, Report{}) {
		t.Fatal("unwritable workspace produced success evidence", got, err)
	}
	if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refusal published output", err)
	}
}

func TestCompactSourcePinRefusesWrongContentAndInventedProvenance(t *testing.T) {
	const source = "../rho-v4-source"
	const pin = "../source-pin.json"
	if err := verifySource(source, pin); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	bad := filepath.Join(dir, "rewritten-source-pin.json")
	b, err := os.ReadFile(pin)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bad, append(b, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if err := verifySource(source, bad); !errors.Is(err, ErrContract) {
		t.Fatal("self-rewritten compact lock admitted", err)
	}
	if err := verifySource(filepath.Join(dir, "missing"), pin); !errors.Is(err, ErrContract) {
		t.Fatal("absent engine source admitted", err)
	}
	if err := verifySource(dir, pin); !errors.Is(err, ErrContract) {
		t.Fatal("wrong source count admitted", err)
	}
	copyDir := filepath.Join(dir, "source")
	if err := filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(copyDir, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0700)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(destination, b, 0600)
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copyDir, "go.mod"), []byte("module foreign\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := verifySource(copyDir, pin); !errors.Is(err, ErrContract) {
		t.Fatal("same count and forged source provenance accepted changed engine bytes", err)
	}
	link := filepath.Join(copyDir, "linked-source")
	if err := os.Symlink("go.mod", link); err != nil {
		t.Fatal(err)
	}
	if err := verifySource(copyDir, pin); !errors.Is(err, ErrContract) {
		t.Fatal("source link admitted", err)
	}
}
