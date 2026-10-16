package fixtures

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// ReadExport reproduces, from the export alone, the entities of the oracle.
func TestReadExportMatchesOracle(t *testing.T) {
	for _, s := range Scenarios() {
		b, err := os.ReadFile(filepath.Join(testdata, s.Name+".export"))
		if err != nil {
			t.Fatal(err)
		}
		src, err := ReadExport(s.info(), b)
		if err != nil {
			t.Fatalf("%s: %v", s.Name, err)
		}
		o := gen(t, s)
		var want Expected
		if err := json.Unmarshal(o.Expected, &want); err != nil {
			t.Fatal(err)
		}
		got, _ := json.Marshal([2]any{src.Nodes, src.Rels})
		exp, _ := json.Marshal([2]any{want.Nodes, want.Rels})
		if !bytes.Equal(got, exp) {
			t.Errorf("%s: entities read from the export differ from the oracle: %s (nodes %d/%d rels %d/%d)", s.Name, firstDiff(exp, got), len(src.Nodes), len(want.Nodes), len(src.Rels), len(want.Rels))
		}
	}
}

func TestReadExportRefusesBadInput(t *testing.T) {
	good, _ := os.ReadFile(filepath.Join(testdata, "txfrom_nohash.export"))
	cases := map[string]struct {
		cfg Config
		in  []byte
	}{
		"empty":        {Config{UntimedValidStart: "tx_from"}, nil},
		"truncated":    {Config{UntimedValidStart: "tx_from"}, good[:len(good)/2]},
		"unknown mode": {Config{UntimedValidStart: "later"}, good},
		"garbage":      {Config{UntimedValidStart: "tx_from"}, []byte("not an export at all")},
	}
	for name, c := range cases {
		if _, err := ReadExport(c.cfg, c.in); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}
