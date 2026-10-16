package fixtures

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEntityIDs(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(testdata, "txfrom_nohash.export"))
	if err != nil {
		t.Fatal(err)
	}
	n, r, err := entityIDs(b)
	if err != nil || len(n) != 6 || len(r) != 4 || n[0] != 1001 || r[0] != 2001 {
		t.Fatalf("nodes %v rels %v err %v", n, r, err)
	}
}
