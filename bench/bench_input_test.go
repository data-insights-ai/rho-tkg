package bench

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestBenchmarkInputCompleteness(t *testing.T) {
	const a = "BenchmarkAlpha/memory-4 10 200 ns/op 8 B/op 1 allocs/op\n"
	const b = "BenchmarkBeta/badger-4 10 300 ns/op 16 B/op 2 allocs/op\n"
	for _, x := range []struct {
		name, old, new string
		bad            bool
	}{
		{"complete", a + b, a + b, false}, {"empty head", a + b, "", true}, {"empty baseline", "", a, true},
		{"missing row", a + b, a, true}, {"extra row", a, a + b, true}, {"missing repetition", a + a, a, true},
		{"failure despite rows", a, a + "FAIL\n", true}, {"incomplete metrics", a, "BenchmarkAlpha/memory-4 10 200 ns/op\n", true},
		{"invalid metrics", a, "BenchmarkAlpha/memory-4 10 invalid ns/op 8 B/op 1 allocs/op\n", true},
	} {
		t.Run(x.name, func(t *testing.T) {
			dir := t.TempDir()
			old, new := filepath.Join(dir, "old.txt"), filepath.Join(dir, "new.txt")
			if e := os.WriteFile(old, []byte(x.old), 0600); e != nil {
				t.Fatal(e)
			}
			if e := os.WriteFile(new, []byte(x.new), 0600); e != nil {
				t.Fatal(e)
			}
			cmd := exec.CommandContext(t.Context(), "awk", "-f", "bench-input.awk", old, new)
			out, e := cmd.CombinedOutput()
			if (e != nil) != x.bad {
				t.Fatalf("bad=%v err=%v output=%s", x.bad, e, out)
			}
		})
	}
}
