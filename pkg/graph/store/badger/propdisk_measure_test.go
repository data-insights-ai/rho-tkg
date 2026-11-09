//go:build measure

package badger

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPropDiskAbsentLookupCost: µs per absent-value lookup on an on-disk
// property index in a multi-table LSM (PROPDISK_N nodes, PROPDISK_DIR store).
func TestPropDiskAbsentLookupCost(t *testing.T) {
	n := 1_000_000
	if v, err := strconv.Atoi(os.Getenv("PROPDISK_N")); err == nil {
		n = v
	}
	dir := os.Getenv("PROPDISK_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	cfg := Config{Dir: dir, PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), CacheCapacity: 1}
	_, statErr := os.Stat(dir + "/MANIFEST")
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if statErr != nil {
		if err := bs.CreatePropertyIndex(1, "k"); err != nil {
			t.Fatal(err)
		}
		pad := string(make([]byte, 200))
		for b := 0; b < n; b += 10000 {
			batch := make([]*types.Node, 0, 10000)
			for i := b; i < b+10000 && i < n; i++ {
				nd := types.NewNode(types.NodeID(existMapBase+int64(i)), 1, nil)
				_ = nd.SetProperty("k", fmt.Sprintf("v%09d", i*7))
				_ = nd.SetProperty("pad", pad)
				batch = append(batch, nd)
			}
			if err := bs.PutNodesBatch(batch); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if bs, err = New(cfg); err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	const probes = 20000
	for range 2 {
		start := time.Now()
		for i := range probes {
			v := fmt.Sprintf("v%09d", (i*7919%n)*7+3) // between present values
			if ids, _, err := bs.NodeIDsByLabelAndPropertyKeys(1, "k", []string{indexpkg.PropertyValueKey(v)}, QueryOpts{}); err != nil || len(ids) != 0 {
				t.Fatalf("absent %s: %v %v", v, ids, err)
			}
		}
		t.Logf("%.1f µs per absent lookup", float64(time.Since(start).Microseconds())/probes)
	}
}
