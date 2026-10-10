package vendorcompare

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	output := filepath.Join(t.TempDir(), "output")
	report, err := Run(t.Context(), Config{Reference: protectedReference(t), Output: output, Limits: defaultLimits()}, backend, controller, exactWireValidator{})
	if err != nil {
		t.Fatal(err)
	}
	if report.CurrentAnswers != 92 || report.ReopenedAnswers != 23 || len(report.Answers) != 115 || controller.reopens != 1 || report.PerformanceAcceptance || report.V7Complete || report.Costs.Complete {
		t.Fatal(report)
	}
	if report.Inventory.ForwardRows != 64 || report.Inventory.ReverseRows != 64 {
		t.Fatal(report.Inventory)
	}
	if _, err := os.Stat(filepath.Join(output, "completion.json")); err != nil {
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
