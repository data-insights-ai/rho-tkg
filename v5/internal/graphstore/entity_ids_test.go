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

// entityIDsFixture stores entity records (nodes, plus relationships for the
// ids in rels) under the given ids, and returns a read view over them.
func entityIDsFixture(t *testing.T, ids []uint64, rels map[uint64]bool, extra ...raftlog.KV) *ReadView {
	t.Helper()
	sources, d, binding, cfg, stores := transactionViewFixture(t)
	actual, _ := sources[0].Root()
	root, err := DecodeRoot(actual.Image)
	if err != nil {
		t.Fatal(err)
	}
	n := root.namespace
	l, _ := (Limits{}).resolve()
	a := testAxis(t, 2, temporal.ProfileIntegerZ)
	var rows []raftlog.KV
	for _, id := range ids {
		r := graphstate.EntityRecord{ID: graphstate.EntityID(id), Kind: graphstate.Node, Axis: a}
		if rels[id] {
			r = graphstate.EntityRecord{ID: graphstate.EntityID(id), Kind: graphstate.Relationship, Axis: a, Type: "R", Source: 1, Target: 2, Mode: graphstate.IdentityReference}
		}
		v, err := encodeEntity(n, r, l)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, raftlog.KV{Key: entityKey(n, graphstate.EntityID(id)), Value: v})
	}
	rows = append(rows, extra...)
	slices.SortFunc(rows, func(x, y raftlog.KV) int { return bytes.Compare(x.Key, y.Key) })
	held := ownershipView(t, stores[0], ownershipInstallRaw(t, stores[0], actual.Image, rows))
	rv, _, err := OpenPartitionGraphReadView(t.Context(), held, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rv.Close() })
	return rv
}

func TestEntityIDsPagesAndResumes(t *testing.T) {
	// Snowflake-sized IDs, a gap of 8e17, nodes and relationships interleaved.
	const base = uint64(800_000_000_000_000_000)
	ids := []uint64{1, 5, base + 1, base + 2, base + 3, base + 4, base + 9}
	rels := map[uint64]bool{5: true, base + 2: true, base + 9: true}
	// A neighbouring row family never leaks in.
	sources, _, _, _, _ := transactionViewFixture(t)
	actual, _ := sources[0].Root()
	root, _ := DecodeRoot(actual.Image)
	other, _ := encodeRevision(root.namespace, 6, 1, temporal.Position{}, DefaultLimits())
	rv := entityIDsFixture(t, ids, rels, raftlog.KV{Key: revisionKey(root.namespace, 6, 1), Value: other})
	ctx := t.Context()
	type want struct {
		id  uint64
		rel bool
	}
	mk := func(from, to int) []want {
		var w []want
		for _, id := range ids[from:to] {
			w = append(w, want{id, rels[id]})
		}
		return w
	}
	for _, c := range []struct {
		after    uint64
		limit    int
		want     []want
		complete bool
	}{
		{0, 3, mk(0, 3), false},
		{5, 3, mk(2, 5), false},       // after is exclusive
		{base + 3, 3, mk(5, 7), true}, // exactly the tail
		{0, 7, mk(0, 7), true},        // limit == count: complete
		{0, 6, mk(0, 6), false},       // one short
		{0, 1, mk(0, 1), false},
		{2, 2, mk(1, 3), false},  // after need not exist
		{base + 9, 5, nil, true}, // after the last
		{base + 100, 5, nil, true},
		{^uint64(0) >> 1, 5, nil, true}, // MaxInt64
	} {
		got, complete, err := rv.EntityIDs(ctx, graphstate.EntityID(c.after), c.limit)
		if err != nil {
			t.Fatal(c, err)
		}
		var w []want
		for _, e := range got {
			w = append(w, want{uint64(e.ID), e.Kind == graphstate.Relationship})
		}
		if !slices.Equal(w, c.want) || complete != c.complete {
			t.Errorf("after=%d limit=%d: got %v complete=%v", c.after, c.limit, w, complete)
		}
	}
	// Resuming page by page visits every ID once, in order.
	var all []uint64
	var after graphstate.EntityID
	for guard := 0; guard < 20; guard++ {
		got, complete, err := rv.EntityIDs(ctx, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range got {
			all = append(all, uint64(e.ID))
			after = e.ID
		}
		if complete {
			break
		}
	}
	if !slices.Equal(all, ids) {
		t.Errorf("resume: %v", all)
	}
	if _, _, err := rv.EntityIDs(ctx, 0, 0); !errors.Is(err, ErrInvalid) {
		t.Errorf("limit 0: %v", err)
	}
	if _, _, err := rv.EntityIDs(ctx, 0, -1); !errors.Is(err, ErrInvalid) {
		t.Errorf("limit -1: %v", err)
	}
	if got, complete, err := rv.EntityIDs(ctx, graphstate.EntityID(^uint64(0)), 5); err != nil || len(got) != 0 || !complete {
		t.Errorf("after MaxUint64: %v %v %v", got, complete, err)
	}
	empty := entityIDsFixture(t, nil, nil)
	if got, complete, err := empty.EntityIDs(ctx, 0, 5); err != nil || len(got) != 0 || !complete {
		t.Errorf("empty: %v %v %v", got, complete, err)
	}
}

func TestEntityIDsCorruptRow(t *testing.T) {
	sources, _, _, _, _ := transactionViewFixture(t)
	actual, _ := sources[0].Root()
	root, _ := DecodeRoot(actual.Image)
	n := root.namespace
	for name, row := range map[string]raftlog.KV{
		"garbage value": {Key: entityKey(n, 9), Value: []byte("x")},
		"short key":     {Key: append(keyPrefix(n, entityRecord), 1, 2, 3), Value: []byte("x")},
	} {
		rv := entityIDsFixture(t, []uint64{1}, nil, row)
		if _, _, err := rv.EntityIDs(t.Context(), 0, 10); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
