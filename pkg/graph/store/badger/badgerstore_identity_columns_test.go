package badger

import (
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestDiskIdentityColumnsAndMutationEpochsMatchResident(t *testing.T) {
	resident := newTestBadgerStore(t)
	disk, err := New(diskIdentityConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	for _, bs := range []*Store{resident, disk} {
		for _, id := range []types.NodeID{1, 2} {
			n := types.NewNode(id, colScanLabel, nil)
			n.SetTemporal(&types.TemporalMetadata{ValidFrom: 1, ValidTo: 10})
			if err := n.SetProperty("qty", int64(id)); err != nil {
				t.Fatal(err)
			}
			if err := bs.PutNode(n); err != nil {
				t.Fatal(err)
			}
		}
		r := types.NewRelationship(10, relScanType, 1, 2)
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: 1, ValidTo: 10})
		if err := r.SetProperty("weight", int64(7)); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	assert := func() {
		t.Helper()
		if a, b := drain(t, resident, []string{"qty"}, QueryOpts{}), drain(t, disk, []string{"qty"}, QueryOpts{}); !reflect.DeepEqual(a, b) {
			t.Fatal("column node rows changed", a, b)
		}
		if a, b := drainRelScan(t, resident, []string{"weight"}, QueryOpts{}), drainRelScan(t, disk, []string{"weight"}, QueryOpts{}); !reflect.DeepEqual(a, b) {
			t.Fatal("column relationship rows changed", a, b)
		}
	}
	assert()
	beforeNode, beforeRel := disk.nodeEpoch.Load(), disk.relEpoch.Load()
	for _, bs := range []*Store{resident, disk} {
		n, err := bs.GetNode(1)
		if err != nil {
			t.Fatal(err)
		}
		if err := n.SetProperty("qty", int64(99)); err != nil {
			t.Fatal(err)
		}
		if err := bs.ReplaceNode(n); err != nil {
			t.Fatal(err)
		}
		r, err := bs.GetRelationship(10)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.SetProperty("weight", int64(11)); err != nil {
			t.Fatal(err)
		}
		if err := bs.ReplaceRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	assert()
	if disk.nodeEpoch.Load() <= beforeNode || disk.relEpoch.Load() <= beforeRel {
		t.Fatal("removing revision maps stopped column invalidation")
	}
	if len(disk.docColumns)+len(disk.relColumns) != 0 {
		t.Fatal("disk-only profile created resident full-label columns")
	}
}
