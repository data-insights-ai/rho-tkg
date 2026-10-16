package graph_test

// Backlog 73: Config.UntimedValidStart. UntimedFromTxFrom (the default) starts
// a row without a valid-from at its TxFrom (backlog 62); UntimedUnbounded
// starts an entity's FIRST version without a valid-from at -inf
// (types.UnboundedValidFrom) and every later untimed version at its TxFrom.
// TestUntimedValidStartModes is the red test.

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// uvsFar and uvsNeg are valid instants long before any TxFrom of a test row:
// 1970 and before the epoch.
const (
	uvsFar types.Instant = 1_000
	uvsNeg types.Instant = -5_000
)

func uvsModeName(m graphpkg.UntimedValidStart) string {
	switch m {
	case graphpkg.UntimedUnbounded:
		return "unbounded"
	case graphpkg.UntimedDefault:
		return "default"
	}
	return "txfrom"
}

func TestUntimedValidStartModes(t *testing.T) {
	t.Parallel()
	for _, mode := range []graphpkg.UntimedValidStart{graphpkg.UntimedFromTxFrom, graphpkg.UntimedUnbounded} {
		backends := allStoreBackendsWith(func(c *graphpkg.Config) {
			c.AllowTxBackfill = true
			c.UntimedValidStart = mode
		})
		for _, b := range backends {
			t.Run(uvsModeName(mode)+"/"+b.name, func(t *testing.T) {
				t.Parallel()
				uvsReads(t, b.open(t), mode)
			})
		}
	}
	for _, b := range uvsDurable {
		t.Run("reopen/"+b.name, func(t *testing.T) {
			t.Parallel()
			uvsReopen(t, b)
		})
		t.Run("convert/"+b.name, func(t *testing.T) {
			t.Parallel()
			uvsConvert(t, b)
		})
	}
	t.Run("convert/memory", func(t *testing.T) {
		t.Parallel()
		g, err := graphpkg.New(graphpkg.Config{UntimedValidStart: graphpkg.UntimedFromTxFrom})
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		uvsConvertLive(t, g)
	})
	t.Run("invalid", func(t *testing.T) {
		t.Parallel()
		if _, err := graphpkg.New(graphpkg.Config{UntimedValidStart: 7}); !errors.Is(err, graphpkg.ErrInvalidUntimedValidStart) {
			t.Fatalf("New with mode 7: err = %v, want ErrInvalidUntimedValidStart", err)
		}
	})
}

// uvsWant is the answer of a single-version untimed first row at valid t.
func uvsWant(mode graphpkg.UntimedValidStart, tx, t types.Instant) bool {
	return mode == graphpkg.UntimedUnbounded || t >= tx
}

func uvsReads(t *testing.T, g *graphpkg.Graph, mode graphpkg.UntimedValidStart) {
	ctx := context.Background()
	if err := g.Index().CreateTemporal("U"); err != nil {
		t.Fatal(err)
	}
	if err := g.Index().CreateRelTemporal("T"); err != nil {
		t.Fatal(err)
	}
	now, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}

	// Every create door, each a first version without a valid-from.
	var nodes []*types.Node
	add := func(n *types.Node, err error) *types.Node {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, n)
		return n
	}
	a := add(g.Nodes().Add(ctx, []string{"U"}, map[string]any{"k": "add"}))
	goc, created, err := g.Nodes().GetOrCreateByKey(ctx, "U", "key", "entity-1", map[string]any{"k": "getorcreate"})
	if err != nil || !created {
		t.Fatalf("GetOrCreateByKey: created=%v err=%v", created, err)
	}
	add(goc, nil)
	if again, created, err := g.Nodes().GetOrCreateByKey(ctx, "U", "key", "entity-1", nil); err != nil || created || again.ID() != goc.ID() {
		t.Fatalf("GetOrCreateByKey again: %v created=%v err=%v", again, created, err)
	}
	add(g.Nodes().AddWithTx(ctx, []string{"U"}, map[string]any{"k": "backfill"}, now-30_000))
	other, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatal(err)
	}
	minted, err := other.Nodes().Add(ctx, []string{"X"}, nil)
	_ = other.Close()
	if err != nil {
		t.Fatal(err)
	}
	add(g.Nodes().Import(ctx, minted.ID(), []string{"U"}, map[string]any{"k": "import"}))
	bb, err := g.Batch().New()
	if err != nil {
		t.Fatal(err)
	}
	bn := add(bb.AddNode([]string{"U"}, map[string]any{"k": "batch"}))
	if _, err := bb.Execute(); err != nil {
		t.Fatal(err)
	}
	s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	add(s.AddNode([]string{"U"}, map[string]any{"k": "ingest"}))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var txNode *types.Node
	var txGoc *types.Node
	if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		var err error
		if txNode, err = tx.AddNode([]string{"U"}, map[string]any{"k": "tx"}); err != nil {
			return err
		}
		txGoc, _, err = tx.GetOrCreateByKey("U", "key", "entity-2", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	add(txNode, nil)
	add(txGoc, nil)

	var rels []*types.Relationship
	addRel := func(r *types.Relationship, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		rels = append(rels, r)
	}
	addRel(g.Rels().Add(ctx, "T", a, bn, map[string]any{"p": "x"}))
	addRel(g.Rels().AddWithTx(ctx, "T", bn, a, map[string]any{"p": "x"}, now-20_000))
	rb, err := g.Batch().New()
	if err != nil {
		t.Fatal(err)
	}
	br := func() *types.Relationship {
		r, err := rb.AddRelationship("T", a, goc, map[string]any{"p": "x"})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}()
	if _, err := rb.Execute(); err != nil {
		t.Fatal(err)
	}
	addRel(g.Rels().Get(ctx, br.ID()))

	// A later untimed version starts at its TxFrom in both modes.
	if _, err := g.Temporal().AdvanceClock(now + 5_000); err != nil {
		t.Fatal(err)
	}
	upd, err := g.Nodes().Add(ctx, []string{"U"}, map[string]any{"k": "updated", "v": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	v1, err := g.Nodes().Update(ctx, upd.ID(), map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	// A close of the first version keeps its start.
	closed, err := g.Nodes().Add(ctx, []string{"U"}, map[string]any{"k": "closed"})
	if err != nil {
		t.Fatal(err)
	}
	closeEnd := closed.Temporal().TxFrom + 60_000
	if _, err := g.Nodes().Update(ctx, closed.ID(), map[string]any{"tkg_valid_to": closeEnd}); err != nil {
		t.Fatal(err)
	}
	// An excise before the TxFrom: under unbounded it cuts a hole into
	// (-inf, ...); under TxFrom there is nothing there to cut.
	exc, err := g.Nodes().Add(ctx, []string{"U"}, map[string]any{"k": "excised"})
	if err != nil {
		t.Fatal(err)
	}
	excErr := g.Temporal().ExciseNodeValidity(ctx, exc.ID(), 2_000, 3_000)
	if mode == graphpkg.UntimedUnbounded && excErr != nil {
		t.Fatalf("excise under unbounded: %v", excErr)
	}
	if mode == graphpkg.UntimedFromTxFrom && !errors.Is(excErr, graphpkg.ErrNoVersionValidAt) {
		t.Fatalf("excise before TxFrom under txfrom: err = %v, want ErrNoVersionValidAt", excErr)
	}
	// A re-import of a deleted ID starts a later life: not a first version,
	// so it starts at its TxFrom in both modes and the gap stays a gap.
	life, err := g.Nodes().Add(ctx, []string{"L"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes().Delete(ctx, life.ID()); err != nil {
		t.Fatal(err)
	}
	gapAt, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Temporal().AdvanceClock(gapAt + 1_000); err != nil {
		t.Fatal(err)
	}
	relife, err := g.Nodes().Import(ctx, life.ID(), []string{"L"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if vf := relife.Temporal().ValidFrom; vf != 0 {
		t.Errorf("re-imported later life: stored ValidFrom = %d, want 0 (starts at its TxFrom)", vf)
	}
	if _, err := g.Temporal().NodeAt(life.ID(), relife.Temporal().TxFrom-1); err == nil {
		t.Error("re-imported later life: valid before its TxFrom (in the gap after the delete)")
	}
	if _, err := g.Temporal().NodeAt(life.ID(), uvsFar); (err == nil) != (mode == graphpkg.UntimedUnbounded) {
		t.Errorf("first life at far: valid = %v (%v), want %v", err == nil, err, mode == graphpkg.UntimedUnbounded)
	}

	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}

	// The stored row: the sentinel on a first version under unbounded, 0
	// (byte-identical to main) under TxFrom.
	wantVF := types.Instant(0)
	if mode == graphpkg.UntimedUnbounded {
		wantVF = types.UnboundedValidFrom
	}
	for _, n := range nodes {
		got, err := g.Nodes().Get(ctx, n.ID())
		if err != nil {
			t.Fatal(err)
		}
		k, _ := got.GetProperty("k")
		if vf := got.Temporal().ValidFrom; vf != wantVF {
			t.Errorf("%v: stored ValidFrom = %d, want %d", k, vf, wantVF)
		}
		tx := got.Temporal().TxFrom
		for _, at := range []types.Instant{uvsNeg, uvsFar, tx - 1, tx} {
			want := uvsWant(mode, tx, at)
			_, err := g.Temporal().NodeAt(n.ID(), at)
			if got := err == nil; got != want {
				t.Errorf("%v NodeAt(%d, TxFrom%+d) = %v (%v), want %v", k, at, at-tx, got, err, want)
			}
			_, err = g.Temporal().NodeAtTx(n.ID(), at, pin)
			if got := err == nil; got != want {
				t.Errorf("%v NodeAtTx(TxFrom%+d) = %v (%v), want %v", k, at-tx, got, err, want)
			}
			if got := g.Temporal().NodeMatchesValidTime(got, storepkg.QueryOpts{ValidAt: at}); got != want {
				t.Errorf("%v NodeMatchesValidTime(TxFrom%+d) = %v, want %v", k, at-tx, got, want)
			}
			during, err := g.Temporal().NodesDuring(at, at+1)
			if err != nil {
				t.Fatal(err)
			}
			if got := uvsHasNode(during, n.ID()); got != want {
				t.Errorf("%v NodesDuring(TxFrom%+d) lists it = %v, want %v", k, at-tx, got, want)
			}
		}
		segs, err := g.Temporal().NodeEffectiveTimeline(n.ID(), pin)
		wantStart := got.Temporal().TxFrom
		if mode == graphpkg.UntimedUnbounded {
			wantStart = types.UnboundedValidFrom
		}
		if err != nil || len(segs) != 1 || segs[0].ValidFrom != wantStart || segs[0].ValidTo != 0 {
			t.Errorf("%v NodeEffectiveTimeline = %v (%v), want one open segment from %d", k, segs, err, wantStart)
		}
	}
	for _, r := range rels {
		got, err := g.Rels().Get(ctx, r.ID())
		if err != nil {
			t.Fatal(err)
		}
		if vf := got.Temporal().ValidFrom; vf != wantVF {
			t.Errorf("rel %d: stored ValidFrom = %d, want %d", r.ID(), vf, wantVF)
		}
		tx := got.Temporal().TxFrom
		for _, at := range []types.Instant{uvsNeg, uvsFar, tx - 1, tx} {
			want := uvsWant(mode, tx, at)
			_, err := g.Temporal().RelAt(r.ID(), at)
			if got := err == nil; got != want {
				t.Errorf("rel RelAt(TxFrom%+d) = %v (%v), want %v", at-tx, got, err, want)
			}
		}
	}

	// The later version: v0 holds until v1's TxFrom (unbounded: from -inf).
	tx0, tx1 := upd.Temporal().TxFrom, v1.Temporal().TxFrom
	for _, c := range []struct {
		at   types.Instant
		want any // the answering v, nil = absent
	}{
		{uvsFar, map[bool]any{true: int64(0), false: nil}[mode == graphpkg.UntimedUnbounded]},
		{tx0 - 1, map[bool]any{true: int64(0), false: nil}[mode == graphpkg.UntimedUnbounded]},
		{tx0, int64(0)},
		{tx1 - 1, int64(0)},
		{tx1, int64(1)},
	} {
		var got any
		if row, err := g.Temporal().NodeAt(upd.ID(), c.at); err == nil {
			got, _ = row.GetProperty("v")
		}
		if got != c.want {
			t.Errorf("updated node at %d (tx0%+d): v = %v, want %v", c.at, c.at-tx0, got, c.want)
		}
	}
	if segs, err := g.Temporal().NodeEffectiveTimeline(upd.ID(), pin); err != nil || len(segs) != 2 || segs[1].ValidFrom != tx1 {
		t.Errorf("updated node timeline = %v (%v), want two segments, the second from v1's TxFrom %d", segs, err, tx1)
	}

	// The closed first version keeps its start and ends at closeEnd.
	ctx0 := closed.Temporal().TxFrom
	for _, c := range []struct {
		at   types.Instant
		want bool
	}{{uvsFar, mode == graphpkg.UntimedUnbounded}, {ctx0 - 1, mode == graphpkg.UntimedUnbounded}, {ctx0, true}, {closeEnd - 1, true}, {closeEnd, false}} {
		if _, err := g.Temporal().NodeAt(closed.ID(), c.at); (err == nil) != c.want {
			t.Errorf("closed node at TxFrom%+d: valid = %v (%v), want %v", c.at-ctx0, err == nil, err, c.want)
		}
	}

	// The excised hole [2000, 3000).
	unb := mode == graphpkg.UntimedUnbounded
	for _, c := range []struct {
		at   types.Instant
		want bool
	}{{1_999, unb}, {2_000, false}, {2_999, false}, {3_000, unb}, {exc.Temporal().TxFrom, true}} {
		if _, err := g.Temporal().NodeAt(exc.ID(), c.at); (err == nil) != c.want {
			t.Errorf("excised node at %d: valid = %v (%v), want %v", c.at, err == nil, err, c.want)
		}
	}

	// Prune soundness: the indexed and paged doors agree with the full fold
	// (NodeAt / RelAt per entity) at every instant of the grid.
	uvsDifferential(t, g, pin, []types.Instant{uvsNeg, uvsFar, 2_500, now - 1, now, tx0 - 1, tx0, tx1 - 1, tx1, closeEnd, pin})

	// Member time statistics: untimed rows hold no explicit valid time in
	// either mode, so a label of only get-or-create identities is eternal.
	if _, _, err := g.Nodes().GetOrCreateByKey(ctx, "E", "key", "id-1", nil); err != nil {
		t.Fatal(err)
	}
	if st, _, err := g.Stats().LabelTimeStats("E"); err != nil || !st.Eternal() {
		t.Errorf("LabelTimeStats(E) = %+v (%v), want Eternal", st, err)
	}
}

// uvsDifferential compares ByLabel / CountByLabelAt / NodesAt (envelope
// pruned) and ByType / RelsEffectivePage with the per-entity fold.
func uvsDifferential(t *testing.T, g *graphpkg.Graph, pin types.Instant, grid []types.Instant) {
	t.Helper()
	ctx := context.Background()
	all, err := g.Nodes().ByLabel("U", storepkg.QueryOpts{TxPin: pin})
	if err != nil {
		t.Fatal(err)
	}
	allRels, err := g.Rels().ByType("T", storepkg.QueryOpts{TxPin: pin})
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range grid {
		var want []types.NodeID
		for _, n := range all {
			if _, err := g.Temporal().NodeAt(n.ID(), at); err == nil {
				want = append(want, n.ID())
			}
		}
		slices.Sort(want)
		byLabel, err := g.Nodes().ByLabel("U", storepkg.QueryOpts{ValidAt: at})
		if err != nil {
			t.Fatal(err)
		}
		if got := uvsNodeIDs(byLabel); !slices.Equal(got, want) {
			t.Errorf("ByLabel(U, ValidAt %d) = %v, fold %v", at, got, want)
		}
		nodesAt, err := g.Temporal().NodesAt(at)
		if err != nil {
			t.Fatal(err)
		}
		var gotAt []types.NodeID
		for _, id := range uvsNodeIDs(nodesAt) {
			if slices.ContainsFunc(all, func(n *types.Node) bool { return n.ID() == id }) {
				gotAt = append(gotAt, id)
			}
		}
		if !slices.Equal(gotAt, want) {
			t.Errorf("NodesAt(%d) ∩ U = %v, fold %v", at, gotAt, want)
		}
		if n, err := g.Nodes().CountByLabelAt("U", storepkg.QueryOpts{ValidAt: at}); err != nil || n != len(want) {
			t.Errorf("CountByLabelAt(U, ValidAt %d) = %d (%v), fold %d", at, n, err, len(want))
		}

		var wantRels []types.RelID
		for _, r := range allRels {
			if _, err := g.Temporal().RelAt(r.ID(), at); err == nil {
				wantRels = append(wantRels, r.ID())
			}
		}
		slices.Sort(wantRels)
		byType, err := g.Rels().ByType("T", storepkg.QueryOpts{ValidAt: at})
		if err != nil {
			t.Fatal(err)
		}
		if got := uvsRelIDs(byType); !slices.Equal(got, wantRels) {
			t.Errorf("ByType(T, ValidAt %d) = %v, fold %v", at, got, wantRels)
		}
		if at <= 0 {
			continue // the paged door refuses a valid filter at or before the epoch
		}
		segs, _, err := g.Temporal().RelsEffectivePage(ctx, temporal.Pin{Tx: pin, ValidAt: at}, temporal.Filter{Prop: "p", Eq: "x"}, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		var paged []types.RelID
		for _, s := range segs {
			if !slices.Contains(paged, s.Rel.ID()) {
				paged = append(paged, s.Rel.ID())
			}
		}
		slices.Sort(paged)
		if !slices.Equal(paged, wantRels) {
			t.Errorf("RelsEffectivePage(ValidAt %d) = %v, fold %v", at, paged, wantRels)
		}
	}
}

func uvsHasNode(ns []*types.Node, id types.NodeID) bool {
	return slices.ContainsFunc(ns, func(n *types.Node) bool { return n.ID() == id })
}

func uvsNodeIDs(ns []*types.Node) []types.NodeID {
	out := make([]types.NodeID, 0, len(ns))
	for _, n := range ns {
		out = append(out, n.ID())
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func uvsRelIDs(rs []*types.Relationship) []types.RelID {
	out := make([]types.RelID, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.ID())
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// uvsBackend opens a durable store in dir.
type uvsBackend struct {
	name string
	open func(t *testing.T, dir string) storepkg.MandatoryStore
}

var uvsDurable = []uvsBackend{
	{"badger", func(t *testing.T, dir string) storepkg.MandatoryStore {
		bs, err := badger.New(badger.Config{Dir: dir, TemporalIndexOnDisk: true})
		if err != nil {
			t.Fatal(err)
		}
		return bs
	}},
	{"tiered", func(t *testing.T, dir string) storepkg.MandatoryStore {
		ts, err := tiered.New(tiered.Config{DataDir: dir, ShardWindow: 7 * 24 * time.Hour, RefLabels: []string{"Ref"}})
		if err != nil {
			t.Fatal(err)
		}
		return ts
	}},
	{"sharded", func(t *testing.T, dir string) storepkg.MandatoryStore {
		st, err := sharded.New(sharded.Config{Dir: dir, BaseSlot: 0, SlotCount: 2})
		if err != nil {
			t.Fatal(err)
		}
		return st
	}},
}

func uvsOpen(t *testing.T, b uvsBackend, dir string, mode graphpkg.UntimedValidStart) (*graphpkg.Graph, storepkg.MandatoryStore, error) {
	t.Helper()
	st := b.open(t, dir)
	g, err := graphpkg.New(graphpkg.Config{Store: st, UntimedValidStart: mode})
	if err != nil {
		_ = st.Close()
		return nil, nil, err
	}
	return g, st, nil
}

func uvsMustOpen(t *testing.T, b uvsBackend, dir string, mode graphpkg.UntimedValidStart) (*graphpkg.Graph, storepkg.MandatoryStore) {
	t.Helper()
	g, st, err := uvsOpen(t, b, dir, mode)
	if err != nil {
		t.Fatalf("open %s under %s: %v", dir, uvsModeName(mode), err)
	}
	return g, st
}

// uvsCheckFirst asserts node id's answer at uvsFar on every probed door:
// NodeAt and the envelope-pruned ByLabel.
func uvsCheckFirst(t *testing.T, g *graphpkg.Graph, id types.NodeID, want bool, what string) {
	t.Helper()
	if _, err := g.Temporal().NodeAt(id, uvsFar); (err == nil) != want {
		t.Errorf("%s: NodeAt(far) valid = %v (%v), want %v", what, err == nil, err, want)
	}
	ns, err := g.Nodes().ByLabel("U", storepkg.QueryOpts{ValidAt: uvsFar})
	if err != nil {
		t.Fatal(err)
	}
	if got := uvsHasNode(ns, id); got != want {
		t.Errorf("%s: ByLabel(U, ValidAt far) lists it = %v, want %v", what, got, want)
	}
}

func uvsReopen(t *testing.T, b uvsBackend) {
	ctx := context.Background()
	unb, def := graphpkg.UntimedUnbounded, graphpkg.UntimedFromTxFrom
	for _, written := range []graphpkg.UntimedValidStart{def, unb} {
		other := unb
		if written == unb {
			other = def
		}
		dir := t.TempDir()
		g, _ := uvsMustOpen(t, b, dir, written)
		if err := g.Index().CreateTemporal("U"); err != nil {
			t.Fatal(err)
		}
		n, err := g.Nodes().Add(ctx, []string{"U"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := uvsOpen(t, b, dir, other); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
			t.Fatalf("store written %s reopened %s: err = %v, want ErrUntimedModeMismatch", uvsModeName(written), uvsModeName(other), err)
		}
		g, _ = uvsMustOpen(t, b, dir, written)
		// Envelopes rebuilt or reloaded at open (badger: the 0x0B keyspace).
		uvsCheckFirst(t, g, n.ID(), written == unb, "reopened "+uvsModeName(written))
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// A store with rows but no persisted mode (written before the option
	// existed) is a TxFrom store.
	dir := t.TempDir()
	g, st := uvsMustOpen(t, b, dir, def)
	if _, err := g.Nodes().Add(ctx, []string{"U"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.(storepkg.MetaKVCapability).MetaSet("untimed_valid_start", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := uvsOpen(t, b, dir, unb); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
		t.Fatalf("legacy store opened unbounded: err = %v, want ErrUntimedModeMismatch", err)
	}
	g, _ = uvsMustOpen(t, b, dir, def)
	_ = g.Close()

	// An empty store takes the configured mode, also over a key.
	empty := t.TempDir()
	g, _ = uvsMustOpen(t, b, empty, unb)
	_ = g.Close()
	g, _ = uvsMustOpen(t, b, empty, def)
	_ = g.Close()
}

// uvsConvert converts a TxFrom store with history to unbounded, then checks
// the reopen rules and an interrupted conversion.
func uvsConvert(t *testing.T, b uvsBackend) {
	unb, def := graphpkg.UntimedUnbounded, graphpkg.UntimedFromTxFrom
	dir := t.TempDir()
	g, _ := uvsMustOpen(t, b, dir, def)
	uvsConvertLive(t, g)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := uvsOpen(t, b, dir, def); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
		t.Fatalf("converted store reopened txfrom: err = %v, want ErrUntimedModeMismatch", err)
	}
	g, _ = uvsMustOpen(t, b, dir, unb)
	ns, err := g.Nodes().ByLabel("U", storepkg.QueryOpts{ValidAt: uvsFar})
	if err != nil || len(ns) == 0 {
		t.Fatalf("converted store reopened: ByLabel(U, far) = %v (%v), want the converted rows", ns, err)
	}
	// Decoded from disk: no first version is left untimed (a queued create
	// whose pre-encoded row kept ValidFrom 0 would show here).
	cur, err := g.Nodes().ByLabel("U", storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range cur {
		if tm := n.Temporal(); n.Version() == 0 && tm.ValidFrom == 0 {
			k, _ := n.GetProperty("k")
			t.Errorf("converted store reopened: node %d (%v) is an untimed first version on disk", n.ID(), k)
		}
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	// An interrupted conversion (key 2, rows not yet stamped): a TxFrom open
	// fails closed, an unbounded open finishes it.
	dir = t.TempDir()
	g, st := uvsMustOpen(t, b, dir, def)
	if err := g.Index().CreateTemporal("U"); err != nil {
		t.Fatal(err)
	}
	n, err := g.Nodes().Add(context.Background(), []string{"U"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.(storepkg.MetaKVCapability).MetaSet("untimed_valid_start", []byte{2}); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := uvsOpen(t, b, dir, def); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
		t.Fatalf("interrupted conversion reopened txfrom: err = %v, want ErrUntimedModeMismatch", err)
	}
	g, _ = uvsMustOpen(t, b, dir, unb)
	uvsCheckFirst(t, g, n.ID(), true, "resumed conversion")
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g, _ = uvsMustOpen(t, b, dir, unb)
	uvsCheckFirst(t, g, n.ID(), true, "resumed conversion reopened")
	_ = g.Close()
}

// uvsConvertLive writes under TxFrom (a first version, an updated entity, a
// timed row, a relationship), converts and checks the answers.
func uvsConvertLive(t *testing.T, g *graphpkg.Graph) {
	t.Helper()
	ctx := context.Background()
	if err := g.Index().CreateTemporal("U"); err != nil {
		t.Fatal(err)
	}
	if err := g.Index().CreateRelTemporal("T"); err != nil {
		t.Fatal(err)
	}
	a, err := g.Nodes().Add(ctx, []string{"U"}, map[string]any{"v": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes().Add(ctx, []string{"U"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	timed, err := g.Nodes().Add(ctx, []string{"U"}, map[string]any{"tkg_valid_from": types.Instant(50_000)})
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels().Add(ctx, "T", a, b, map[string]any{"p": "x"})
	if err != nil {
		t.Fatal(err)
	}
	now, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Temporal().AdvanceClock(now + 5_000); err != nil {
		t.Fatal(err)
	}
	v1, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	uvsCheckFirst(t, g, a.ID(), false, "before conversion")
	// Warm the cached state cut at a pin before the conversion: the
	// conversion must invalidate it.
	pin0, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	cutOpts := storepkg.QueryOpts{ValidAt: uvsFar, TxAt: pin0}
	for i := 0; i < 2; i++ {
		if n, err := g.Nodes().CountByLabelAt("U", cutOpts); err != nil || n != 0 {
			t.Fatalf("CountByLabelAt(U, far, pin0) before conversion = %d (%v), want 0", n, err)
		}
	}

	// Creates queued before the conversion and committed after it are
	// written under the new mode: a batch, an ingest session (its rows
	// pre-encoded at queue) and a concurrent ingest session.
	bb, err := g.Batch().New()
	if err != nil {
		t.Fatal(err)
	}
	queuedBatch, err := bb.AddNode([]string{"U"}, map[string]any{"k": "queued-batch"})
	if err != nil {
		t.Fatal(err)
	}
	queuedBatchRel, err := bb.AddRelationship("T", a, b, map[string]any{"p": "x"})
	if err != nil {
		t.Fatal(err)
	}
	var queued []types.NodeID
	for _, conc := range []bool{false, true} {
		s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true, Concurrent: conc})
		if err != nil {
			t.Fatal(err)
		}
		n, err := s.AddNode([]string{"U"}, map[string]any{"k": "queued-ingest"})
		if err != nil {
			t.Fatal(err)
		}
		queued = append(queued, n.ID())
		defer func(s *ingest.Session, conc bool) {
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			uvsCheckFirst(t, g, queued[map[bool]int{false: 0, true: 1}[conc]], true, "ingest queued before conversion")
		}(s, conc)
	}

	if err := g.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedFromTxFrom); err != nil {
		t.Fatalf("SetUntimedValidStart(TxFrom) on a TxFrom store: %v", err)
	}
	if err := g.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedUnbounded); err != nil {
		t.Fatalf("SetUntimedValidStart(Unbounded): %v", err)
	}
	// The first version now holds from -inf, the later one still from its TxFrom.
	for _, c := range []struct {
		at   types.Instant
		want any
	}{{uvsFar, int64(0)}, {v1.Temporal().TxFrom - 1, int64(0)}, {v1.Temporal().TxFrom, int64(1)}} {
		var got any
		if row, err := g.Temporal().NodeAt(a.ID(), c.at); err == nil {
			got, _ = row.GetProperty("v")
		}
		if got != c.want {
			t.Errorf("converted a at %d: v = %v, want %v", c.at, got, c.want)
		}
	}
	uvsCheckFirst(t, g, b.ID(), true, "converted b")
	if n, err := g.Nodes().CountByLabelAt("U", cutOpts); err != nil || n != 2 {
		t.Errorf("CountByLabelAt(U, far, pin0) after conversion = %d (%v), want 2 (a, b)", n, err)
	}
	if _, err := g.Temporal().NodeAt(timed.ID(), 49_999); err == nil {
		t.Error("converted: a row with an explicit valid-from must keep it")
	}
	if _, err := g.Temporal().RelAt(r.ID(), uvsFar); err != nil {
		t.Errorf("converted rel at far: %v", err)
	}
	if _, err := bb.Execute(); err != nil {
		t.Fatal(err)
	}
	uvsCheckFirst(t, g, queuedBatch.ID(), true, "batch queued before conversion")
	if _, err := g.Temporal().RelAt(queuedBatchRel.ID(), uvsFar); err != nil {
		t.Errorf("batch rel queued before conversion at far: %v", err)
	}
	// New creates take the mode at once.
	c, err := g.Nodes().Add(ctx, []string{"U"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	uvsCheckFirst(t, g, c.ID(), true, "created after conversion")
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	uvsDifferential(t, g, pin, []types.Instant{uvsNeg, uvsFar, 50_000, now, v1.Temporal().TxFrom, pin})
	// One way only.
	if err := g.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedFromTxFrom); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
		t.Fatalf("SetUntimedValidStart(TxFrom) after conversion: err = %v, want ErrUntimedModeMismatch", err)
	}
	// Idempotent: a second run stamps nothing new and keeps the answers.
	if err := g.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedUnbounded); err != nil {
		t.Fatalf("second SetUntimedValidStart(Unbounded): %v", err)
	}
	uvsCheckFirst(t, g, b.ID(), true, "converted twice")
}
