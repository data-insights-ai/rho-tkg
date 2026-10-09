package graphstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"testing"
)

const revisedCorpusSHA256 = "263de8c0c2bfd078747bd26cc46c0b3e2f5897b264698bc2f891e5c0eeb06f85"

type revisedLane string

const (
	revisedNative       revisedLane = "executable-native"
	revisedPreservation revisedLane = "byte-preservation-only"
	revisedRefusal      revisedLane = "unsupported-native-predicate"
	revisedPending      revisedLane = "pending-graph-attachment-or-public-api"
)

type revisedRule struct {
	kind string
	lane revisedLane
	gap  string
}

// These categories count complete native contracts separately from envelope
// preservation, type-level refusal probes and required implementation gaps.
// Pending records are never skipped or counted as accepted native cases.
var revisedRules = map[string]revisedRule{
	"E03-E08-support-Q":                               {"region", revisedNative, ""},
	"E03-E08-support-Z":                               {"region", revisedNative, ""},
	"E03-E08-support-QN":                              {"region", revisedNative, ""},
	"E02-components-correction-retraction":            {"component", revisedNative, ""},
	"SEM12-null-revision-replay":                      {"component", revisedNative, ""},
	"E01-event-multiplicity":                          {"graph", revisedPending, "EntityRecord does not attach interpretation and temporal role."},
	"E01-E03-interpretation-independent-of-support-Z": {"graph", revisedPending, "Equal support cannot substitute for missing interpretation and temporal role."},
	"E19-close-reopen-identity-reference":             {"graph", revisedNative, ""},
	"E19-strict-life-bound-presence-correction":       {"graph", revisedNative, ""},
	"SEM03-06-node-rel-property-history":              {"graph", revisedNative, ""},
	"SEM14-16-endpoint-boundary":                      {"graph", revisedPending, "Wrong-graph refusal needs the embedding/public qualified-handle boundary."},
	"E02-disjoint-native-region":                      {"graph", revisedNative, ""},
	"E06-preservation":                                {"graph", revisedPreservation, "Opaque envelope bytes only; graph attachment/shared-constraint interpretation pending."},
	"E10-preservation":                                {"graph", revisedPreservation, "Opaque envelope bytes only; graph attachment pending; no STN evaluation."},
	"E13-preservation":                                {"graph", revisedPreservation, "Opaque envelope bytes only; graph attachment pending; no calendar evaluation."},
	"E14-preservation":                                {"graph", revisedPreservation, "Opaque envelope bytes only; declared typed property/axis attachment is not supplied by this JSON record."},
	"E20-preservation":                                {"graph", revisedPreservation, "Opaque envelope bytes only; atomic graph summary/coverage attachment pending."},
	"E07-causal-records":                              {"graph", revisedPending, "Asserted relation interpretation and temporal role cannot be dropped."},
	"E04-exact-nearby-values":                         {"value", revisedPreservation, "Exact scalar preservation only; no event-ID attachment or caller rounding implementation is claimed."},
	"E09-source-history-derived-fraction":             {"region", revisedPreservation, "Supplied source/result scopes round-trip only; no join/shift or graph-history attachment is claimed."},
	"E05-possible-only":                               {"knowledge", revisedNative, ""},
	"E05-definite":                                    {"knowledge", revisedNative, ""},
	"E05-empty-hard-support":                          {"knowledge", revisedNative, ""},
	"E05-decline-confidence_95_percent":               {"knowledge", revisedRefusal, "Native confidence predicate refusal; no hard-support promotion."},
	"E05-decline-confidence_100_percent":              {"knowledge", revisedRefusal, "Native confidence predicate refusal even at level one."},
	"E05-decline-nominal":                             {"knowledge", revisedRefusal, "Type-level refusal probe only: corpus supplies no source nominal coordinate."},
	"E05-decline-opaque_correlated_constraint":        {"knowledge", revisedRefusal, "Type-level refusal probe using the complete uninterpreted record as opaque envelope payload."},
	"E05-decline-unspecified":                         {"knowledge", revisedRefusal, "Native unspecified-evidence predicate refusal."},
	"V0-units-us-to-ms-Z-1000":                        {"unit_mapping", revisedPending, "No declared native unit-conversion/import API."},
	"V0-units-us-to-ms-Q-1":                           {"unit_mapping", revisedPending, "No declared native unit-conversion/import API."},
	"V0-units-us-to-ms-Q-1001":                        {"unit_mapping", revisedPending, "No declared native unit-conversion/import API."},
	"V0-units-us-to-ms-Z-1":                           {"unit_mapping", revisedPending, "No declared native unit-conversion/import refusal API."},
	"V0-default-ms-identity":                          {"unit_mapping", revisedPending, "Default public Instant/import contract is not a general Integer coordinate."},
	"V0-default-instant-codec-range":                  {"codec_contract", revisedPending, "General integer codecs do not implement the public int64 millisecond Instant contract."},
	"V0-order-only-no-seconds":                        {"unit_mapping", revisedPending, "No public mapping negotiation; must not invent seconds for ordinal axes."},
	"V6-one-tick-import-ambiguity":                    {"import_contract", revisedPending, "Importer/writer-provenance ambiguity reporting is not implemented here."},
	"strict-symbolic-placement-decline":               {"failure", revisedPending, "No symbolic graph-placement attachment; translating it to Unplaced would change the input."},
	"axis-conflicting-definition-empty":               {"failure", revisedNative, ""},
	"budget-variable-name-axis-change-ledger":         {"budget", revisedPending, "Reference JSON ledgers are not Go metadata/wire/heap byte policies."},
	"dependency-absence-phantom-budget-contract":      {"transaction_contract", revisedPending, "Complete dependency validation/installation requires the embedding transaction/store."},
	"replay-request-vs-revision-contract":             {"transaction_contract", revisedPending, "Durable request-key replay/retention lease is separate from component replay."},
	"budget-tight-Z-replay-no-transient-successor":    {"component", revisedNative, ""},
}

// Per-record fields prevent accepting a known field from a different contract
// while its native executor silently ignores it. Missing fields also fail closed.
var revisedRecordKeys = map[string][]string{
	"E03-E08-support-Q":                               {"axis", "expected", "id", "input", "kind", "semantics"},
	"E03-E08-support-Z":                               {"axis", "expected", "id", "input", "kind", "semantics"},
	"E03-E08-support-QN":                              {"axis", "expected", "id", "input", "kind", "semantics"},
	"E02-components-correction-retraction":            {"axis", "changes", "expected", "id", "kind", "operations", "probes", "semantics"},
	"SEM12-null-revision-replay":                      {"axis", "expected", "id", "kind", "operations", "semantics"},
	"E01-event-multiplicity":                          {"axis", "failures", "id", "kind", "reads", "semantics", "steps"},
	"E01-E03-interpretation-independent-of-support-Z": {"axis", "failures", "id", "kind", "reads", "semantics", "steps"},
	"E19-close-reopen-identity-reference":             {"axis", "failures", "id", "kind", "reads", "semantics", "steps"},
	"E19-strict-life-bound-presence-correction":       {"axis", "failures", "id", "kind", "reads", "semantics", "steps"},
	"SEM03-06-node-rel-property-history":              {"axis", "failures", "id", "kind", "reads", "semantics", "steps"},
	"SEM14-16-endpoint-boundary":                      {"axis", "failures", "id", "kind", "reads", "semantics", "steps"},
	"E02-disjoint-native-region":                      {"axis", "failures", "id", "kind", "reads", "semantics", "steps"},
	"E06-preservation":                                {"axis", "failures", "id", "kind", "must_not_infer", "native_predicates", "reads", "semantics", "steps"},
	"E10-preservation":                                {"axis", "failures", "id", "kind", "must_not_infer", "native_predicates", "reads", "semantics", "steps"},
	"E13-preservation":                                {"axis", "failures", "id", "kind", "must_not_infer", "native_predicates", "reads", "semantics", "steps"},
	"E14-preservation":                                {"axis", "failures", "id", "kind", "must_not_infer", "native_predicates", "reads", "semantics", "steps"},
	"E20-preservation":                                {"axis", "failures", "id", "kind", "must_not_infer", "native_predicates", "reads", "semantics", "steps"},
	"E07-causal-records":                              {"axis", "failures", "id", "kind", "reads", "semantics", "steps"},
	"E04-exact-nearby-values":                         {"expected", "id", "input", "kind", "semantics"},
	"E09-source-history-derived-fraction":             {"expected", "id", "kind", "semantics", "source_regions", "supplied_result", "supplied_shifted_result"},
	"E05-possible-only":                               {"expected", "id", "kind", "semantics", "support", "window"},
	"E05-definite":                                    {"expected", "id", "kind", "semantics", "support", "window"},
	"E05-empty-hard-support":                          {"expected", "id", "kind", "support", "window"},
	"E05-decline-confidence_95_percent":               {"expected_error", "id", "kind", "knowledge_kind", "support", "window"},
	"E05-decline-confidence_100_percent":              {"expected_error", "id", "kind", "knowledge_kind", "support", "window"},
	"E05-decline-nominal":                             {"expected_error", "id", "kind", "knowledge_kind", "support", "window"},
	"E05-decline-opaque_correlated_constraint":        {"expected_error", "id", "kind", "knowledge_kind", "support", "window"},
	"E05-decline-unspecified":                         {"expected_error", "id", "kind", "knowledge_kind", "support", "window"},
	"V0-units-us-to-ms-Z-1000":                        {"expected", "id", "kind", "mapping", "source_coordinate", "source_unit", "target_axis"},
	"V0-units-us-to-ms-Q-1":                           {"expected", "id", "kind", "mapping", "source_coordinate", "source_unit", "target_axis"},
	"V0-units-us-to-ms-Q-1001":                        {"expected", "id", "kind", "mapping", "source_coordinate", "source_unit", "target_axis"},
	"V0-units-us-to-ms-Z-1":                           {"expected_error", "id", "kind", "mapping", "source_coordinate", "source_unit", "target_axis"},
	"V0-default-ms-identity":                          {"expected", "id", "kind", "mapping", "note", "source_coordinate", "source_unit", "target_axis"},
	"V0-default-instant-codec-range":                  {"expected", "id", "kind", "semantics"},
	"V0-order-only-no-seconds":                        {"expected_error", "id", "kind", "source_coordinate", "source_unit", "target_axis"},
	"V6-one-tick-import-ambiguity":                    {"expected", "id", "kind", "must_not_infer", "valid_from_ms", "valid_to_ms", "writer_provenance"},
	"strict-symbolic-placement-decline":               {"caller_solver_evidence", "expected_error", "id", "kind", "operation"},
	"axis-conflicting-definition-empty":               {"expected_error", "id", "kind", "left_axis", "left_scope", "right_axis", "right_scope", "semantics"},
	"budget-variable-name-axis-change-ledger":         {"accounting", "axis", "change_bytes", "exact_fit", "expected_atomic", "id", "kind", "one_byte_less", "operations", "semantics", "snapshot_bytes"},
	"dependency-absence-phantom-budget-contract":      {"budget_failure", "expected", "id", "kind", "model_status", "reads", "semantics"},
	"replay-request-vs-revision-contract":             {"component_replay", "create_identity_reuse", "id", "kind", "new_provenance_same_revision", "new_revision_same_value", "request_key_deduplication"},
	"budget-tight-Z-replay-no-transient-successor":    {"axis", "expected", "id", "initial_cell", "initial_scope", "kind", "limits", "mutation_cell", "mutation_scope", "semantics"},
}

var errRevisedPending = errors.New("revised adapter: pending implementation contract")

type revisedAxis struct {
	Identity  string `json:"identity"`
	Profile   string `json:"profile"`
	Reference string `json:"reference"`
	Unit      string `json:"unit"`
	Version   uint16 `json:"version"`
}

type revisedPiece struct {
	Lower       json.RawMessage `json:"lower"`
	Upper       json.RawMessage `json:"upper"`
	LowerClosed bool            `json:"lower_closed"`
	UpperClosed bool            `json:"upper_closed"`
}

type revisedRegion struct {
	Axis   revisedAxis    `json:"axis"`
	Pieces []revisedPiece `json:"pieces"`
}

type revisedCell struct {
	Present    bool            `json:"present"`
	Value      json.RawMessage `json:"value"`
	Revision   *string         `json:"revision"`
	Provenance *string         `json:"provenance"`
}

type revisedComponentOp struct {
	Scope revisedRegion `json:"scope"`
	Cell  revisedCell   `json:"cell"`
}

type revisedOperation struct {
	Op             string                     `json:"op"`
	ID             string                     `json:"id"`
	Owner          string                     `json:"owner"`
	Life           uint64                     `json:"life"`
	Source         string                     `json:"source"`
	Target         string                     `json:"target"`
	Type           string                     `json:"type"`
	ReferenceMode  string                     `json:"reference_mode"`
	Valid          json.RawMessage            `json:"valid"`
	LowerClosed    *bool                      `json:"lower_closed"`
	UpperClosed    *bool                      `json:"upper_closed"`
	Properties     map[string]json.RawMessage `json:"properties"`
	Labels         []string                   `json:"labels"`
	Key            string                     `json:"key"`
	Label          string                     `json:"label"`
	Value          json.RawMessage            `json:"value"`
	Present        *bool                      `json:"present"`
	Graph          string                     `json:"graph"`
	Axis           string                     `json:"axis"`
	Interpretation string                     `json:"interpretation"`
	TemporalRole   string                     `json:"temporal_role"`
}

// Explicit null cannot turn an unknown flag into a missing/default flag.
func (operation *revisedOperation) UnmarshalJSON(raw []byte) error {
	type plain revisedOperation
	var value plain
	if err := revisedStrictJSON(raw, &value); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := revisedStrictJSON(raw, &fields); err != nil {
		return err
	}
	for _, name := range []string{"present", "lower_closed", "upper_closed"} {
		if flag, exists := fields[name]; exists && !bytes.Equal(bytes.TrimSpace(flag), []byte("true")) && !bytes.Equal(bytes.TrimSpace(flag), []byte("false")) {
			return fmt.Errorf("revised adapter: %s must be an explicit bool", name)
		}
	}
	*operation = revisedOperation(value)
	return nil
}

type revisedStep struct {
	Snapshot    string             `json:"snapshot"`
	Revision    string             `json:"revision"`
	Provenance  string             `json:"provenance"`
	Operations  []revisedOperation `json:"operations"`
	ChangeCount int                `json:"change_count"`
}

type revisedRead struct {
	Snapshot        string          `json:"snapshot"`
	Coordinate      json.RawMessage `json:"coordinate"`
	Mode            string          `json:"mode"`
	LifecycleStatus bool            `json:"lifecycle_status"`
	Expected        json.RawMessage `json:"expected"`
}

type revisedFailure struct {
	BaseSnapshot   string             `json:"base_snapshot"`
	Operations     []revisedOperation `json:"operations"`
	ExpectedError  string             `json:"expected_error"`
	ExpectedAtomic bool               `json:"expected_atomic"`
}

type revisedGraph struct {
	Axis     revisedAxis      `json:"axis"`
	Steps    []revisedStep    `json:"steps"`
	Reads    []revisedRead    `json:"reads"`
	Failures []revisedFailure `json:"failures"`
}

type revisedRecord struct {
	ID     string
	Kind   string
	Raw    json.RawMessage
	Fields map[string]json.RawMessage
}

func revisedStrictJSON(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("revised adapter: trailing JSON")
	}
	return nil
}

func revisedField[T any](record revisedRecord, name string) (T, error) {
	var value T
	raw, exists := record.Fields[name]
	if !exists {
		return value, fmt.Errorf("revised adapter: missing %s", name)
	}
	err := revisedStrictJSON(raw, &value)
	return value, err
}

func revisedRequireKeys(fields map[string]json.RawMessage, allowed []string) error {
	for name := range fields {
		if !slices.Contains(allowed, name) {
			return fmt.Errorf("revised adapter: unknown field %s", name)
		}
	}
	return nil
}

func revisedDecodeRecord(raw json.RawMessage) (revisedRecord, error) {
	var fields map[string]json.RawMessage
	if err := revisedStrictJSON(raw, &fields); err != nil || fields == nil {
		return revisedRecord{}, fmt.Errorf("revised adapter: invalid record: %w", err)
	}
	record := revisedRecord{Raw: bytes.Clone(raw), Fields: fields}
	var err error
	record.ID, err = revisedField[string](record, "id")
	if err != nil {
		return revisedRecord{}, err
	}
	record.Kind, err = revisedField[string](record, "kind")
	if err != nil {
		return revisedRecord{}, err
	}
	rule, known := revisedRules[record.ID]
	if !known || rule.kind != record.Kind {
		return revisedRecord{}, fmt.Errorf("revised adapter: unknown ID/kind %s/%s", record.ID, record.Kind)
	}
	keys := revisedRecordKeys[record.ID]
	if err := revisedRequireKeys(fields, keys); err != nil {
		return revisedRecord{}, err
	}
	if len(fields) != len(keys) {
		return revisedRecord{}, fmt.Errorf("revised adapter: incomplete fields for %s", record.ID)
	}
	if record.Kind == "graph" {
		graph, err := revisedGraphRecord(record)
		if err != nil {
			return revisedRecord{}, err
		}
		for _, step := range graph.Steps {
			for _, operation := range step.Operations {
				if err := revisedValidateOperation(operation); err != nil {
					return revisedRecord{}, err
				}
			}
		}
		for _, failure := range graph.Failures {
			for _, operation := range failure.Operations {
				if err := revisedValidateOperation(operation); err != nil {
					return revisedRecord{}, err
				}
			}
		}
	}
	return record, nil
}

func revisedGraphRecord(record revisedRecord) (revisedGraph, error) {
	var graph revisedGraph
	var err error
	graph.Axis, err = revisedField[revisedAxis](record, "axis")
	if err != nil {
		return graph, err
	}
	graph.Steps, err = revisedField[[]revisedStep](record, "steps")
	if err != nil {
		return graph, err
	}
	graph.Reads, err = revisedField[[]revisedRead](record, "reads")
	if err != nil {
		return graph, err
	}
	graph.Failures, err = revisedField[[]revisedFailure](record, "failures")
	return graph, err
}

func revisedValidateOperation(operation revisedOperation) error {
	switch operation.Op {
	case "create_vertex", "create_edge", "reopen", "close", "correct", "set", "unset", "add", "remove", "add_label", "remove_label":
		return nil
	default:
		return fmt.Errorf("revised adapter: unknown operation %s", operation.Op)
	}
}

func revisedLoad(t testing.TB) []revisedRecord {
	t.Helper()
	raw, err := os.ReadFile("testdata/revised-acceptance-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	if hex.EncodeToString(hash[:]) != revisedCorpusSHA256 {
		t.Fatal("revised corpus drift")
	}
	var envelope struct {
		Schema           string            `json:"schema"`
		Version          int               `json:"version"`
		Revision         int               `json:"corpus_revision"`
		Profile          string            `json:"profile"`
		PhysicalUnits    string            `json:"physical_units"`
		Limits           json.RawMessage   `json:"limits"`
		InputCorrections []json.RawMessage `json:"input_corrections"`
		Cases            []json.RawMessage `json:"cases"`
	}
	if err := revisedStrictJSON(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Schema != "rho-v5-independent-acceptance" || envelope.Version != 1 || envelope.Revision != 3 || len(envelope.Cases) != 42 {
		t.Fatal("unsupported corpus contract")
	}
	records := make([]revisedRecord, len(envelope.Cases))
	seen := make(map[string]bool)
	for i, raw := range envelope.Cases {
		records[i], err = revisedDecodeRecord(raw)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if seen[records[i].ID] {
			t.Fatal("duplicate record", records[i].ID)
		}
		seen[records[i].ID] = true
	}
	if len(seen) != len(revisedRules) {
		t.Fatal("unaccounted record")
	}
	return records
}

func revisedJSONEqual(t testing.TB, expected json.RawMessage, actual any) {
	t.Helper()
	actualBytes, err := json.Marshal(actual)
	if err != nil {
		t.Fatal(err)
	}
	var want, got any
	if err := revisedStrictJSON(expected, &want); err != nil {
		t.Fatal(err)
	}
	if err := revisedStrictJSON(actualBytes, &got); err != nil {
		t.Fatal(err)
	}
	wantBytes, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	gotBytes, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wantBytes, gotBytes) {
		t.Fatalf("complete answer differs\nwant %s\ngot  %s", wantBytes, gotBytes)
	}
}

func TestRevisedCorpusAccountingDoesNotClaimPendingAcceptance(t *testing.T) {
	counts := map[revisedLane]int{}
	for _, record := range revisedLoad(t) {
		rule := revisedRules[record.ID]
		counts[rule.lane]++
		if rule.lane != revisedNative && rule.gap == "" {
			t.Fatal("missing scope/gap", record.ID)
		}
		t.Logf("%s: %s; %s", record.ID, rule.lane, rule.gap)
	}
	for lane, want := range map[revisedLane]int{revisedNative: 14, revisedPreservation: 7, revisedRefusal: 5, revisedPending: 16} {
		if counts[lane] != want {
			t.Fatal("case accounting", counts)
		}
	}
}

func TestRevisedAdapterRejectsUnknownFieldsKindsAndMetadataInsteadOfDroppingThem(t *testing.T) {
	records := revisedLoad(t)
	for _, mutate := range []func(map[string]json.RawMessage){
		func(fields map[string]json.RawMessage) { fields["unreviewed"] = json.RawMessage("true") },
		func(fields map[string]json.RawMessage) { fields["source_regions"] = json.RawMessage("[]") },
		func(fields map[string]json.RawMessage) { fields["kind"] = json.RawMessage(`"new-kind"`) },
		func(fields map[string]json.RawMessage) { fields["id"] = json.RawMessage(`"new-ID"`) },
	} {
		var fields map[string]json.RawMessage
		if err := revisedStrictJSON(records[0].Raw, &fields); err != nil {
			t.Fatal(err)
		}
		mutate(fields)
		raw, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := revisedDecodeRecord(raw); err == nil {
			t.Fatal("unknown contract accepted")
		}
	}
	var crossContract map[string]json.RawMessage
	if err := revisedStrictJSON(records[0].Raw, &crossContract); err != nil {
		t.Fatal(err)
	}
	crossContract["source_regions"] = json.RawMessage("[]")
	crossRaw, err := json.Marshal(crossContract)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := revisedDecodeRecord(crossRaw); err == nil {
		t.Fatal("field from a different region contract was ignored")
	}
	for _, raw := range []json.RawMessage{
		json.RawMessage(`{"op":"correct","valid":[0,1],"present":null}`),
		json.RawMessage(`{"op":"set","valid":[0,1],"lower_closed":null}`),
		json.RawMessage(`{"op":"invented","valid":[0,1]}`),
		json.RawMessage(`{"op":"set","valid":[0,1],"ignored_field":true}`),
	} {
		var operation revisedOperation
		err := revisedStrictJSON(raw, &operation)
		if err == nil {
			err = revisedValidateOperation(operation)
		}
		if err == nil {
			t.Fatal("unknown graph operation/field accepted")
		}
	}
	for _, record := range records {
		if record.ID == "E01-event-multiplicity" || record.ID == "E07-causal-records" {
			graph, err := revisedGraphRecord(record)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := revisedTranslateGraph(record, graph); !errors.Is(err, errRevisedPending) {
				t.Fatalf("unattached interpretation/role must be refused: %s: %v", record.ID, err)
			}
		}
	}
}
