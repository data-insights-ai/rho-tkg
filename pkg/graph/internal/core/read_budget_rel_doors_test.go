package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RH list R2, the relationship by-ID doors (Rels().GetByIDsCtx, HasHistoryCtx,
// LatestStampsCtx): the node doors' contract on relationships. ctx is checked
// first; one candidate per ID is counted before any read; past a limit
// *store.ReadBudgetError and no answer; without a budget exactly the plain
// door's answer. Decoded bytes: the DecodedBytes tests below; shard opens: the
// tiered shard-opens test (read_budget_shard_opens_test.go); the probe matrix
// (read_budget_matrix_test.go) holds GetByIDsCtx to a budget of 1 on every
// counter.

// TestReadBudget_RelByIDDoors: candidates are counted per ID before the read
// and refused past the limit with no answer; the meter is cumulative; a
// cancelled context is refused before the budget is touched; and the answers
// under an unlimited budget are the plain doors' (a deleted relationship and
// an updated one included).
func TestReadBudget_RelByIDDoors(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			var ns []*types.Node
			for range 3 {
				n, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				ns = append(ns, n)
			}
			var ids []types.RelID
			for i := range 3 {
				r, err := g.Rels.Add(ctx, "R", ns[i], ns[(i+1)%3], map[string]any{"v": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, r.ID())
			}
			if _, err := g.Rels.Update(ctx, ids[0], map[string]any{"v": int64(9)}); err != nil { // history
				t.Fatal(err)
			}
			if err := g.Rels.Delete(ctx, ids[2]); err != nil { // live nowhere
				t.Fatal(err)
			}
			live := ids[:2]
			refused := func(what, door string, err error) {
				t.Helper()
				var rb *storepkg.ReadBudgetError
				if !errors.Is(err, storepkg.ErrReadBudget) || !errors.As(err, &rb) || rb.Door != door || rb.Limit != "candidates" {
					t.Fatalf("%s: %v, want ErrReadBudget at candidates from %s", what, err, door)
				}
			}

			// GetByIDs: 2 IDs against a limit of 1 are refused with no rows; within
			// the limit the answer is the plain door's, counted.
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
			got, err := g.Rels.GetByIDsCtx(bctx, live)
			refused("GetByIDs of 2 under 1", "GetByIDs", err)
			if got != nil || meter.Work().Candidates != 2 {
				t.Fatalf("refused GetByIDs = %d rows, work %+v; want none and 2 counted", len(got), meter.Work())
			}
			want, err := g.Rels.GetByIDs(live)
			if err != nil {
				t.Fatal(err)
			}
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 2})
			got, err = g.Rels.GetByIDsCtx(bctx, live)
			if err != nil || !slices.EqualFunc(got, want, func(a, b *types.Relationship) bool { return a.ID() == b.ID() && a.Version() == b.Version() }) || meter.Work().Candidates != 2 {
				t.Fatalf("GetByIDs within = %d rows, %v, %+v; want the plain door's %d", len(got), err, meter.Work(), len(want))
			}
			// A missing relationship is the plain door's error, not a refusal.
			bctx, _ = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 5})
			if _, err := g.Rels.GetByIDsCtx(bctx, []types.RelID{ids[2]}); !errors.Is(err, storepkg.ErrRelNotFound) {
				t.Fatalf("GetByIDs of a deleted rel = %v, want ErrRelNotFound", err)
			}

			// HasHistory and LatestStamps: one candidate each, cumulative.
			bctx, _ = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
			if _, err := g.Rels.HasHistoryCtx(bctx, ids[0]); err != nil {
				t.Fatal(err)
			}
			_, err = g.Rels.HasHistoryCtx(bctx, ids[1])
			refused("second HasHistory", "HasHistory", err)
			bctx, _ = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
			if _, _, _, err := g.Rels.LatestStampsCtx(bctx, ids[0]); err != nil {
				t.Fatal(err)
			}
			_, _, _, err = g.Rels.LatestStampsCtx(bctx, ids[1])
			refused("second LatestStamps", "LatestStamps", err)

			// Unlimited: the plain answers, for an updated, a plain and a deleted
			// relationship.
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			for i, id := range ids {
				wantHas, err := g.Rels.HasHistory(id)
				if err != nil {
					t.Fatal(err)
				}
				if has, err := g.Rels.HasHistoryCtx(bctx, id); err != nil || has != wantHas {
					t.Fatalf("rel %d HasHistoryCtx = %v, %v; want %v", i, has, err, wantHas)
				}
				wf, wt, wd, err := g.Rels.LatestStamps(id)
				if err != nil {
					t.Fatal(err)
				}
				if f, to, d, err := g.Rels.LatestStampsCtx(bctx, id); err != nil || f != wf || to != wt || d != wd {
					t.Fatalf("rel %d LatestStampsCtx = %d %d %v, %v; want %d %d %v", i, f, to, d, err, wf, wt, wd)
				}
			}
			if w := meter.Work(); w.Candidates != 6 {
				t.Fatalf("unlimited work %+v, want 6 candidates", w)
			}
			// An unknown ID is the plain door's error.
			if _, _, _, err := g.Rels.LatestStampsCtx(bctx, types.RelID(1)); err == nil {
				t.Fatal("LatestStampsCtx of an unknown ID answered")
			}

			// The context is checked first: a cancelled one is refused before the
			// budget counts anything.
			cctx, cancel := context.WithCancel(ctx)
			cancel()
			bctx, meter = storepkg.WithReadBudget(cctx, storepkg.ReadBudget{Candidates: 100})
			if _, err := g.Rels.GetByIDsCtx(bctx, live); !errors.Is(err, context.Canceled) {
				t.Fatalf("GetByIDsCtx on a cancelled context = %v", err)
			}
			if _, err := g.Rels.HasHistoryCtx(bctx, ids[0]); !errors.Is(err, context.Canceled) {
				t.Fatalf("HasHistoryCtx on a cancelled context = %v", err)
			}
			if _, _, _, err := g.Rels.LatestStampsCtx(bctx, ids[0]); !errors.Is(err, context.Canceled) {
				t.Fatalf("LatestStampsCtx on a cancelled context = %v", err)
			}
			if w := meter.Work(); w != (storepkg.ReadWork{}) {
				t.Fatalf("a cancelled context counted %+v", w)
			}
		})
	}
}

// TestReadBudget_DecodedBytes_RelGetByIDs: Rels().GetByIDsCtx counts the
// stored bytes of the rows the store decodes. Cold, the count covers every
// row's wire bytes and is the same on every cold run; within a budget of
// exactly that count the door answers as without one; one byte under it
// returns ErrReadBudget "decoded bytes" and no rows, and nothing is decoded
// after the refusal. Warm, the entity cache serves the rows and the read counts
// 0. A single ID and a repeated ID read through the same count.
func TestReadBudget_DecodedBytes_RelGetByIDs(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			s, e := txbEndpoints(t, g)
			const rows = 6
			var ids []types.RelID
			for i := range rows {
				r, err := g.Rels.AddByID(ctx, "R", s, e, map[string]any{"pad": fmt.Sprintf("%0*d", 100+i, i)})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, r.ID())
			}
			shut()
			g, shut = be.open(t, dir)
			defer shut()

			for _, c := range []struct {
				name string
				ids  []types.RelID
				want int // rows returned
				min  uint64
			}{
				{"many", ids, rows, rows * 100},
				{"one", ids[:1], 1, 100},
				{"repeated", []types.RelID{ids[0], ids[1], ids[0]}, 3, 2 * 100}, // the repeated row decodes once
			} {
				rbdExact(t, "rel GetByIDs "+c.name, "GetByIDs", c.min,
					func() { rbdEvict(t, g, nil, ids) },
					func(ctx context.Context) (bool, error) {
						got, err := g.Rels.GetByIDsCtx(ctx, c.ids)
						return len(got) == c.want, err
					})
			}
			// Warm: the entity cache serves the rows; nothing is decoded.
			if _, err := g.Rels.GetByIDsCtx(context.Background(), ids); err != nil {
				t.Fatal(err)
			}
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
			got, err := g.Rels.GetByIDsCtx(bctx, ids)
			if err != nil || len(got) != rows || meter.Work().DecodedBytes != 0 {
				t.Fatalf("warm rel GetByIDs = %d rows, %v, %+v; want the rows and 0 decoded bytes", len(got), err, meter.Work())
			}
		})
	}
}

// TestReadBudget_DecodedBytes_RelPresenceAndStampsDecodeNoRow: HasHistoryCtx
// and LatestStampsCtx on a relationship read key presence and transaction
// stamps, not a row: they count no decoded bytes, and a DecodedBytes limit
// never refuses them. Candidates still count.
func TestReadBudget_DecodedBytes_RelPresenceAndStampsDecodeNoRow(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			s, e := txbEndpoints(t, g)
			r, err := g.Rels.AddByID(ctx, "R", s, e, map[string]any{"pad": fmt.Sprintf("%0*d", 500, 1)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(1)}); err != nil {
				t.Fatal(err)
			}
			shut()
			g, shut = be.open(t, dir)
			defer shut()
			rbdEvict(t, g, nil, []types.RelID{r.ID()})
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
			if has, err := g.Rels.HasHistoryCtx(bctx, r.ID()); err != nil || !has {
				t.Fatalf("HasHistoryCtx = %v, %v", has, err)
			}
			if _, _, _, err := g.Rels.LatestStampsCtx(bctx, r.ID()); err != nil {
				t.Fatalf("LatestStampsCtx: %v", err)
			}
			if w := meter.Work(); w.DecodedBytes != 0 || w.Candidates != 2 {
				t.Fatalf("work %+v, want 2 candidates and 0 decoded bytes", w)
			}
		})
	}
}

// TestReadBudget_UnsupportedStoreRefusesRelDoors: on a wrapper around a native
// store, which does not count decodes or shard opens, the relationship doors
// (the two property lookups and the adjacency doors Sigma reads included)
// refuse a budget bounding them before any work (ErrReadBudget, Limit
// "unsupported"), and answer under a budget bounding only the candidates the
// core counts, through the wrapper's own reads.
func TestReadBudget_UnsupportedStoreRefusesRelDoors(t *testing.T) {
	t.Parallel()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Config{Store: rbWrapStore{bs}, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = bs.Close() })
	ctx := context.Background()
	s, e := txbEndpoints(t, g)
	r, err := g.Rels.AddByID(ctx, "R", s, e, map[string]any{"k": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes.Add(ctx, []string{"U"}, map[string]any{"k": int64(1)}); err != nil {
		t.Fatal(err)
	}
	doors := map[string]func(ctx context.Context) error{
		"Outgoing": func(ctx context.Context) error {
			_, err := g.Rels.OutgoingCtx(ctx, s, "R")
			return err
		},
		"Incoming": func(ctx context.Context) error {
			_, err := g.Rels.IncomingCtx(ctx, e, "R")
			return err
		},
		"ByTypeAndProperty": func(ctx context.Context) error {
			_, err := g.Rels.ByTypeAndPropertyCtx(ctx, "R", "k", int64(1), storepkg.QueryOpts{})
			return err
		},
		"IDsByLabelAndPropertyValues": func(ctx context.Context) error {
			_, err := g.Nodes.IDsByLabelAndPropertyValuesCtx(ctx, "U", "k", []any{int64(1)}, storepkg.QueryOpts{})
			return err
		},
		"GetByIDs": func(ctx context.Context) error {
			_, err := g.Rels.GetByIDsCtx(ctx, []types.RelID{r.ID()})
			return err
		},
		"HasHistory": func(ctx context.Context) error {
			_, err := g.Rels.HasHistoryCtx(ctx, r.ID())
			return err
		},
		"LatestStamps": func(ctx context.Context) error {
			_, _, _, err := g.Rels.LatestStampsCtx(ctx, r.ID())
			return err
		},
	}
	for door, call := range doors {
		for _, b := range []storepkg.ReadBudget{{DecodedBytes: 1 << 30}, {ShardOpens: 100}} {
			bctx, meter := storepkg.WithReadBudget(ctx, b)
			be := rbdBudgetErr(t, call(bctx), "unsupported")
			if be.Door != door || meter.Work() != (storepkg.ReadWork{}) {
				t.Fatalf("%s under %+v: %+v, work %+v; want a refusal from %s before any work", door, b, be, meter.Work(), door)
			}
		}
		bctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 100})
		if err := call(bctx); err != nil {
			t.Fatalf("%s under a Candidates budget: %v; want an answer", door, err)
		}
	}
}
