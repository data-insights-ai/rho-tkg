package index

import (
	"errors"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"testing"
)

func TestDocValuesTypedSummarySurvivesAppendAndCache(t *testing.T) {
	gp := func(id types.NodeID, _ string) (any, bool) { return int64(id), true }
	gt := func(types.NodeID) (int64, int64, bool) { return -1, 0, true }
	legacy := BuildLabelDocValues(1, []types.NodeID{1}, []string{"n"}, gp, gt)
	if legacy.HasTypedTemporal() {
		t.Fatal("legacy column marked typed")
	}
	typed := legacy.Extend(2, []types.NodeID{2}, gp, gt, true)
	if typed == nil || !typed.HasTypedTemporal() || legacy.HasTypedTemporal() {
		t.Fatal("append lost summary or changed previous snapshot")
	}
	appended := typed.Extend(3, []types.NodeID{3}, gp, gt)
	if appended == nil || !appended.HasTypedTemporal() {
		t.Fatal("ordinary append reset unsupported temporal summary")
	}
	raw := EncodeColumns(appended)
	restored, err := DecodeColumns(raw, func(v int64) types.NodeID { return types.NodeID(v) })
	if err != nil || !restored.HasTypedTemporal() {
		t.Fatal("cache erased typed summary", err)
	}
	old := append([]byte{1}, raw[1:]...)
	if _, err := DecodeColumns(old, func(v int64) types.NodeID { return types.NodeID(v) }); !errors.Is(err, ErrColumnBlobUnreadable) {
		t.Fatal("old cache reused without presence summary", err)
	}
	raw[9] = 2
	if _, err := DecodeColumns(raw, func(v int64) types.NodeID { return types.NodeID(v) }); !errors.Is(err, ErrColumnBlobUnreadable) {
		t.Fatal("invalid summary accepted", err)
	}
}
