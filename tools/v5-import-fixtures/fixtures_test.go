package fixtures

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

const testdata = "testdata"

func gen(t *testing.T, s Scenario) Output {
	t.Helper()
	o, err := Generate(s)
	if err != nil {
		t.Fatalf("%s: Generate: %v", s.Name, err)
	}
	return o
}

// Regenerating must be byte-identical (no wall clock, no random IDs).
func TestDeterministic(t *testing.T) {
	for _, s := range Scenarios() {
		a, b := gen(t, s), gen(t, s)
		if !bytes.Equal(a.Export, b.Export) {
			t.Errorf("%s: export differs between two runs", s.Name)
		}
		if !bytes.Equal(a.Expected, b.Expected) {
			t.Errorf("%s: expected-answer file differs between two runs", s.Name)
		}
	}
}

// The checked-in files are exactly what the generator writes now.
func TestCheckedInMatchesGenerator(t *testing.T) {
	for _, s := range Scenarios() {
		o := gen(t, s)
		for name, want := range map[string][]byte{s.Name + ".export": o.Export, s.Name + ".expected.json": o.Expected} {
			got, err := os.ReadFile(filepath.Join(testdata, name))
			if err != nil {
				t.Errorf("%s: %v (run: go run ./cmd/gen -out testdata)", name, err)
				continue
			}
			if !bytes.Equal(got, want) {
				t.Errorf("%s: checked-in file differs from generator output", name)
			}
		}
	}
}

// Re-reading every pin through v4 after a v4 Import of the export gives the
// recorded answers: the oracle is consistent with the export.
func TestExpectedSelfCheck(t *testing.T) {
	for _, s := range Scenarios() {
		o := gen(t, s)
		if err := Verify(s, o.Export, o.Expected); err != nil {
			t.Errorf("%s: %v", s.Name, err)
		}
	}
}

// A wrong fixture must fail the self-check: tamper one answer at a time.
func TestVerifyDetectsWrongExpected(t *testing.T) {
	tampers := map[string]func(*Expected) bool{
		"drop a pin node": func(e *Expected) bool {
			for i := range e.Pins {
				if len(e.Pins[i].Nodes) > 0 {
					e.Pins[i].Nodes = e.Pins[i].Nodes[1:]
					return true
				}
			}
			return false
		},
		"change a lookup result": func(e *Expected) bool {
			for i := range e.Lookups {
				if len(e.Lookups[i].IDs) > 0 {
					e.Lookups[i].IDs = e.Lookups[i].IDs[1:]
					return true
				}
			}
			return false
		},
		"drop a current row": func(e *Expected) bool {
			for i := range e.Nodes {
				if e.Nodes[i].Current != nil {
					e.Nodes[i].Current = nil
					return true
				}
			}
			return false
		},
		"flip a retracted flag": func(e *Expected) bool {
			for i := range e.Nodes {
				for j := range e.Nodes[i].History {
					if e.Nodes[i].History[j].Retracted {
						e.Nodes[i].History[j].Retracted = false
						return true
					}
				}
			}
			return false
		},
		"drop a history row": func(e *Expected) bool {
			for i := range e.Nodes {
				if len(e.Nodes[i].History) > 1 {
					e.Nodes[i].History = e.Nodes[i].History[1:]
					return true
				}
			}
			return false
		},
		"change a timeline": func(e *Expected) bool {
			for i := range e.Timelines {
				for j := range e.Timelines[i].Nodes {
					if len(e.Timelines[i].Nodes[j].Segments) > 0 {
						e.Timelines[i].Nodes[j].Segments = nil
						return true
					}
				}
			}
			return false
		},
		"drop an index": func(e *Expected) bool {
			if len(e.Indexes.Property) > 0 {
				e.Indexes.Property = nil
				return true
			}
			return false
		},
	}
	applied := map[string]bool{}
	for _, s := range Scenarios() {
		o := gen(t, s)
		for name, f := range tampers {
			var c Expected
			if err := json.Unmarshal(o.Expected, &c); err != nil {
				t.Fatal(err)
			}
			if !f(&c) {
				continue
			}
			applied[name] = true
			bad, _ := json.Marshal(&c)
			if err := Verify(s, o.Export, bad); err == nil {
				t.Errorf("%s/%s: Verify accepted a tampered expected file", s.Name, name)
			}
		}
	}
	for name := range tampers {
		if !applied[name] {
			t.Errorf("tamper %q never applicable to any scenario", name)
		}
	}
}

// v4 Import must reject a corrupted or truncated export.
func TestCorruptExportRejected(t *testing.T) {
	for _, s := range Scenarios() {
		o := gen(t, s)
		for _, cut := range []int{len(o.Export) / 2, len(o.Export) - 1, 3} {
			if err := importInto(s, o.Export[:cut]); err == nil {
				t.Errorf("%s: truncated export (%d of %d bytes) imported without error", s.Name, cut, len(o.Export))
			}
		}
		if err := importInto(s, nil); err == nil {
			t.Errorf("%s: empty export imported without error", s.Name)
		}
		if s.Hashes {
			offs := payloadOffsets(o.Export)
			if len(offs) == 0 {
				t.Errorf("%s: no payload offsets to corrupt", s.Name)
			}
			for _, off := range offs {
				bad := append([]byte(nil), o.Export...)
				bad[off] ^= 0xff
				if err := importInto(s, bad); err == nil {
					t.Errorf("%s: hashed export with flipped byte at %d imported without error", s.Name, off)
				}
			}
		}
	}
}

func TestTotalSize(t *testing.T) {
	var total int
	for _, s := range Scenarios() {
		o := gen(t, s)
		total += len(o.Export) + len(o.Expected)
	}
	if total >= 2<<20 {
		t.Errorf("fixtures total %d bytes, want < 2 MiB", total)
	}
	t.Logf("fixtures total %d bytes", total)
}

// What the fixtures cover is read back from the expected files (not from the
// scenario declarations), so a scenario that silently stops producing a row
// kind fails here.
func TestCoverage(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range Scenarios() {
		o := gen(t, s)
		var e Expected
		if err := json.Unmarshal(o.Expected, &e); err != nil {
			t.Fatal(err)
		}
		for f := range Observed(&e) {
			seen[f] = true
		}
	}
	for _, f := range RequiredFeatures {
		if !seen[f] {
			t.Errorf("no fixture covers %q", f)
		}
	}
}

func TestScenarioNamesUnique(t *testing.T) {
	names := map[string]bool{}
	for _, s := range Scenarios() {
		if s.Name == "" || names[s.Name] {
			t.Errorf("bad or duplicate scenario name %q", s.Name)
		}
		names[s.Name] = true
	}
	if len(names) < 4 {
		t.Errorf("only %d scenarios", len(names))
	}
}
