package graphstore

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// revisionFixture installs revision rows (no known instant) for owner 5 at the
// given revisions plus extra raw rows, and returns a read view over them.
func revisionFixture(t *testing.T, revs []uint64, extra ...raftlog.KV) *ReadView {
	t.Helper()
	sources, d, binding, cfg, stores := transactionViewFixture(t)
	actual, _ := sources[0].Root()
	root, err := DecodeRoot(actual.Image)
	if err != nil {
		t.Fatal(err)
	}
	n := root.namespace
	var rows []raftlog.KV
	for _, r := range revs {
		v, err := encodeRevision(n, 5, r, temporal.Position{}, DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, raftlog.KV{Key: revisionKey(n, 5, r), Value: v})
	}
	rows = append(rows, extra...)
	slices.SortFunc(rows, func(a, b raftlog.KV) int { return bytes.Compare(a.Key, b.Key) })
	held := ownershipView(t, stores[0], ownershipInstallRaw(t, stores[0], actual.Image, rows))
	rv, _, err := OpenPartitionGraphReadView(t.Context(), held, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rv.Close() })
	return rv
}

func TestReadViewRevisionsPages(t *testing.T) {
	var revs []uint64
	for i := uint64(1); i <= 130; i++ {
		revs = append(revs, i*3)
	}
	// A neighbouring owner's rows never leak in.
	sources, _, _, _, _ := transactionViewFixture(t)
	actual, _ := sources[0].Root()
	root, _ := DecodeRoot(actual.Image)
	other, _ := encodeRevision(root.namespace, 6, 1, temporal.Position{}, DefaultLimits())
	rv := revisionFixture(t, revs, raftlog.KV{Key: revisionKey(root.namespace, 6, 1), Value: other})
	ctx := t.Context()
	for _, c := range []struct {
		after    uint64
		limit    int
		want     []uint64
		complete bool
	}{
		{0, 64, revs[:64], false},
		{revs[63], 64, revs[64:128], false},
		{revs[127], 64, revs[128:], true},
		{0, 130, revs, true},
		{0, 129, revs[:129], false},
		{0, 1, revs[:1], false},
		{4, 2, []uint64{6, 9}, false}, // after is exclusive and need not exist
		{999, 5, nil, true},
	} {
		got, complete, err := rv.Revisions(ctx, 5, c.after, c.limit)
		if err != nil {
			t.Fatal(c, err)
		}
		var ids []uint64
		for _, m := range got {
			ids = append(ids, m.Revision)
		}
		if !slices.Equal(ids, c.want) || complete != c.complete {
			t.Errorf("after=%d limit=%d: got %v complete=%v", c.after, c.limit, ids, complete)
		}
	}
	if _, _, err := rv.Revisions(ctx, 5, 0, 0); !errors.Is(err, ErrInvalid) {
		t.Errorf("limit 0: %v", err)
	}
	if _, _, err := rv.Revisions(ctx, 0, 0, 5); !errors.Is(err, ErrInvalid) {
		t.Errorf("owner 0: %v", err)
	}
	if got, complete, err := rv.Revisions(ctx, 7, 0, 5); err != nil || len(got) != 0 || !complete {
		t.Errorf("unknown owner: %v %v %v", got, complete, err)
	}
}

func TestReadViewRevisionsCorruptRow(t *testing.T) {
	sources, _, _, _, _ := transactionViewFixture(t)
	actual, _ := sources[0].Root()
	root, _ := DecodeRoot(actual.Image)
	n := root.namespace
	good, _ := encodeRevision(n, 5, 1, temporal.Position{}, DefaultLimits())
	wrongRev, _ := encodeRevision(n, 5, 99, temporal.Position{}, DefaultLimits())
	for name, row := range map[string]raftlog.KV{
		"garbage value":       {Key: revisionKey(n, 5, 2), Value: []byte("x")},
		"value of a revision": {Key: revisionKey(n, 5, 2), Value: wrongRev},
		"short key":           {Key: append(revisionPrefix(n, 5), 1, 2, 3), Value: good},
	} {
		rv := revisionFixture(t, []uint64{1}, row)
		if _, _, err := rv.Revisions(t.Context(), 5, 0, 10); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestRangeMoveExportRefusesRangeHoldingRevisionRows(t *testing.T) {
	sources, d, binding, cfg, stores := transactionViewFixture(t)
	actual, _ := sources[0].Root()
	descriptor := rangeMoveTestPlan(t, sources, d, binding, cfg)
	budget := raftlog.ReadBudget{Rows: 1, Bytes: 64 << 10}
	if _, _, err := ExportRangeMovePage(t.Context(), sources[0], descriptor, RangeMoveCursor{}, budget, Limits{}, GraphLimits{}, transactionTestOutput(t)); err != nil {
		t.Fatal("range without revision rows must export", err)
	}
	root, err := DecodeRoot(actual.Image)
	if err != nil {
		t.Fatal(err)
	}
	owner := graphstate.EntityID(descriptor.Spec.Publication.First)
	value, err := encodeRevision(root.namespace, owner, 5, temporal.Position{}, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	held := ownershipView(t, stores[0], ownershipInstallRaw(t, stores[0], actual.Image, []raftlog.KV{{Key: revisionKey(root.namespace, owner, 5), Value: value}}))
	sources[0] = held
	descriptor = rangeMoveTestPlan(t, sources, d, binding, cfg)
	if _, _, err := ExportRangeMovePage(t.Context(), held, descriptor, RangeMoveCursor{}, budget, Limits{}, GraphLimits{}, transactionTestOutput(t)); !errors.Is(err, graphstate.ErrUnsupported) {
		t.Fatal("range holding revision rows was exported", err)
	}
}
