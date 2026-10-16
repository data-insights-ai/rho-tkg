package importmap

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	fixtures "github.com/data-insights-ai/rho-tkg/tools/v5-import-fixtures"
)

var update = flag.Bool("update", false, "rewrite the golden files")

const fixtureDir = "../v5-import-fixtures/testdata"

func load(t *testing.T, name string) *fixtures.Source {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(fixtureDir, name+".export"))
	if err != nil {
		t.Fatal(err)
	}
	var cfg fixtures.Config
	for _, s := range fixtures.Scenarios() {
		if s.Name == name {
			e := struct{ Config fixtures.Config }{}
			ex, err := os.ReadFile(filepath.Join(fixtureDir, name+".expected.json"))
			if err != nil || json.Unmarshal(ex, &e) != nil {
				t.Fatal("expected file", err)
			}
			cfg = e.Config
		}
	}
	src, err := fixtures.ReadExport(cfg, b)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

var names = []string{"txfrom_hashed", "txfrom_nohash", "unbounded_hashed", "unbounded_nohash"}

func golden(t *testing.T, file string, got []byte) {
	t.Helper()
	p := filepath.Join("testdata", file)
	if *update {
		if err := os.WriteFile(p, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("%v (run: go test -update)", err)
	}
	if !bytes.Equal(want, got) {
		t.Errorf("%s differs from the checked-in golden file", file)
	}
}

func TestOpStreamMatchesExpectedJSON(t *testing.T) {
	for _, n := range names {
		r, err := Map(load(t, n))
		if err != nil {
			t.Fatalf("%s: %v", n, err)
		}
		golden(t, n+".ops.json", r.OpsJSON())
	}
}

// The ambiguity report is exactly the expected rows, and two runs give the same bytes.
func TestAmbiguityReportIsExactAndStable(t *testing.T) {
	for _, n := range names {
		src := load(t, n)
		a, err := Map(src)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := Map(src)
		if !bytes.Equal(a.AmbiguityJSON(), b.AmbiguityJSON()) || !bytes.Equal(a.OpsJSON(), b.OpsJSON()) {
			t.Errorf("%s: two runs differ", n)
		}
		golden(t, n+".ambiguities.json", a.AmbiguityJSON())
		// The four fixtures hold only these kinds; malformed or unsupported would be a mapper gap.
		expected := map[string]bool{KindMap: true, KindTemporal: true, KindUintOverMaxInt64: true,
			KindVoidRow: true, KindReimportAfterDel: true}
		for _, x := range a.Ambiguities {
			if !expected[x.Kind] {
				t.Errorf("%s: unexpected ambiguity %+v", n, x)
			}
		}
	}
}
