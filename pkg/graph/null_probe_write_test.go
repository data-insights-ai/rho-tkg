package graph_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// nullRowIDs is the scan oracle of a Null probe: the rows of rs whose dev is
// absent or nil, sorted.
func nullRowIDs(rs []*types.Relationship) []int64 {
	var out []int64
	for _, r := range rs {
		if v, ok := r.GetProperty("dev"); !ok || v == nil {
			out = append(out, int64(r.ID()))
		}
	}
	slices.Sort(out)
	return out
}

func nullSortedIDs(rs []*types.Relationship) []int64 {
	ids := relIDs(rs)
	slices.Sort(ids)
	return ids
}

// TestNullProbeEveryEqualityDoor: every relationship equality door accepts
// types.Null and returns the window's null rows (ByTypeAndProperty, its Ctx
// door, Temporal RelsByTypePropertyAt and RelsByTypePropertyDuring, and their
// GraphTx mirrors); plain nil matches nothing at each.
func TestNullProbeEveryEqualityDoor(t *testing.T) {
	t.Parallel()
	for _, b := range nullProbeBackends(nil) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, err := g.Nodes().Add(ctx, []string{"D"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateRelProperty("LOGON", "dev"); err != nil {
				t.Fatal(err)
			}
			addLogon(t, g, a, a, "d1", 100, 200)
			early := int64(addLogon(t, g, a, a, "", 100, 200))
			late := int64(addLogon(t, g, a, a, "", 300, 400))
			doors := []struct {
				name string
				run  func(v any) ([]*types.Relationship, error)
				want []int64
			}{
				{"ByTypeAndProperty", func(v any) ([]*types.Relationship, error) {
					return g.Rels().ByTypeAndProperty("LOGON", "dev", v, store.QueryOpts{})
				}, []int64{early, late}},
				{"ByTypeAndPropertyCtx", func(v any) ([]*types.Relationship, error) {
					return g.Rels().ByTypeAndPropertyCtx(ctx, "LOGON", "dev", v, store.QueryOpts{ValidAt: 150, ValidAtSet: true})
				}, []int64{early}},
				{"RelsByTypePropertyAt", func(v any) ([]*types.Relationship, error) {
					return g.Temporal().RelsByTypePropertyAt("LOGON", "dev", v, 350)
				}, []int64{late}},
				{"RelsByTypePropertyDuring", func(v any) ([]*types.Relationship, error) {
					return g.Temporal().RelsByTypePropertyDuring("LOGON", "dev", v, 100, 400)
				}, []int64{early, late}},
				{"GraphTx.RelsByTypePropertyAt", func(v any) (out []*types.Relationship, err error) {
					err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
						out, err = tx.RelsByTypePropertyAt("LOGON", "dev", v, 150)
						return err
					})
					return out, err
				}, []int64{early}},
				{"GraphTx.RelsByTypePropertyDuring", func(v any) (out []*types.Relationship, err error) {
					err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
						out, err = tx.RelsByTypePropertyDuring("LOGON", "dev", v, 300, 400)
						return err
					})
					return out, err
				}, []int64{late}},
			}
			for _, d := range doors {
				got, err := d.run(types.Null)
				if err != nil {
					t.Fatalf("%s(Null): %v", d.name, err)
				}
				if ids := nullSortedIDs(got); !slices.Equal(ids, d.want) {
					t.Errorf("%s(Null): %v, want %v", d.name, ids, d.want)
				}
				if got, err := d.run(nil); err != nil || len(got) != 0 {
					t.Errorf("%s(nil): %d rows, %v; want none", d.name, len(got), err)
				}
			}
		})
	}
}

// TestNullValueStatsHeaviest: with absent and nil the heaviest value of the
// index, RelPropertyValueStats at pin 0 and at a pin states it exactly
// (MaxValue types.Null, its rows MaxRows) and both agree, where the backend
// answers (exact); memory and badger must.
func TestNullValueStatsHeaviest(t *testing.T) {
	t.Parallel()
	for _, b := range nullProbeBackends(nil) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, err := g.Nodes().Add(ctx, []string{"D"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateRelProperty("LOGON", "dev"); err != nil {
				t.Fatal(err)
			}
			for _, dev := range []string{"d1", "d1", "d2", "", ""} {
				addLogon(t, g, a, a, dev, 100, 200)
			}
			if _, err := g.Rels().Add(ctx, "LOGON", a, a, map[string]any{"dev": nil}); err != nil {
				t.Fatal(err)
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			want := store.PropertyValueStats{Distinct: 3, Rows: 6, MaxRows: 3, MaxValue: types.Null}
			must := b.name == "memory" || b.name == "badger"
			for _, p := range []types.Instant{0, pin} {
				st, exact, err := g.Stats().RelPropertyValueStats("LOGON", "dev", p)
				if err != nil {
					t.Fatalf("pin %d: %v", p, err)
				}
				if !exact {
					if must {
						t.Errorf("pin %d: declined", p)
					}
					continue
				}
				if st != want {
					t.Errorf("pin %d: %+v, want %+v", p, st, want)
				}
			}
		})
	}
}

// TestNullProbeWritePathMatchesScan: after each write a Null probe (current,
// at two valid instants, and at every earlier transaction pin) equals the scan
// oracle (ByType over the same options, the rows whose dev is absent or nil):
// value to absent to value, delete, a history version, a valid-window end, a
// nil-valued property, erasure and purge.
func TestNullProbeWritePathMatchesScan(t *testing.T) {
	t.Parallel()
	mut := func(c *graphpkg.Config) { c.AllowRetentionPurge, c.AllowExactErasure = true, true }
	for _, b := range nullProbeBackends(mut) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, err := g.Nodes().Add(ctx, []string{"D"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateRelProperty("LOGON", "dev"); err != nil {
				t.Fatal(err)
			}
			now := func() types.Instant {
				n, err := g.Temporal().NowTx()
				if err != nil {
					t.Fatal(err)
				}
				return n
			}
			purged := addLogon(t, g, a, a, "", 100, 200) // purged by age last
			for hi := now(); types.Instant(time.Now().UnixMilli()) <= hi+1; {
				time.Sleep(time.Millisecond) // the purge boundary: a later wall millisecond
			}
			var pins []types.Instant
			check := func(step string, wantNull int) {
				t.Helper()
				pins = append(pins, now())
				opts := []store.QueryOpts{{}, {ValidAt: 150, ValidAtSet: true}, {ValidAt: 350, ValidAtSet: true}}
				for _, p := range pins {
					opts = append(opts, store.QueryOpts{TxAt: p, ValidAt: 150, ValidAtSet: true}, store.QueryOpts{TxAt: p, ValidAt: 350, ValidAtSet: true})
				}
				for i, o := range opts {
					got, gerr := g.Rels().ByTypeAndProperty("LOGON", "dev", types.Null, o)
					all, oerr := g.Rels().ByType("LOGON", o)
					if gerr != nil || oerr != nil {
						if !errors.Is(gerr, graphpkg.ErrRetentionExpired) || !errors.Is(oerr, graphpkg.ErrRetentionExpired) {
							t.Fatalf("%s opts %+v: probe %v, scan %v", step, o, gerr, oerr)
						}
						continue
					}
					ids, want := nullSortedIDs(got), nullRowIDs(all)
					if !slices.Equal(ids, want) {
						t.Errorf("%s opts %+v: Null %v, scan %v", step, o, ids, want)
					}
					if i == 0 && len(want) != wantNull {
						t.Errorf("%s: %d current null rows, want %d", step, len(want), wantNull)
					}
					at, err := g.Temporal().RelsByTypePropertyAt("LOGON", "dev", types.Null, o.ValidAt)
					if err == nil && o.ValidAtSet && o.TxAt == 0 {
						scan, err := g.Temporal().RelsByTypeAt("LOGON", o.ValidAt)
						if err != nil {
							t.Fatal(err)
						}
						if ids, want := nullSortedIDs(at), nullRowIDs(scan); !slices.Equal(ids, want) {
							t.Errorf("%s At %d: Null %v, scan %v", step, o.ValidAt, ids, want)
						}
					}
				}
			}
			openLogon := func(props map[string]any) types.RelID {
				t.Helper()
				props["tkg_valid_from"] = types.Instant(100)
				r, err := g.Rels().Add(ctx, "LOGON", a, a, props)
				if err != nil {
					t.Fatal(err)
				}
				return r.ID()
			}
			check("start", 1)
			r1 := openLogon(map[string]any{"dev": "d1"})
			check("value", 1)
			if _, err := g.Rels().Update(ctx, r1, map[string]any{"dev": nil}); err != nil {
				t.Fatal(err)
			}
			check("value to absent", 2)
			if _, err := g.Rels().Update(ctx, r1, map[string]any{"dev": "d2"}); err != nil {
				t.Fatal(err)
			}
			check("absent to value", 1)
			r2 := openLogon(map[string]any{})
			check("null added", 2)
			if _, err := g.Rels().Update(ctx, r2, map[string]any{"other": "x"}); err != nil {
				t.Fatal(err)
			}
			check("null history version", 2)
			if err := g.Rels().Delete(ctx, r2); err != nil {
				t.Fatal(err)
			}
			check("delete", 1)
			late, err := g.Rels().Add(ctx, "LOGON", a, a, map[string]any{"tkg_valid_from": types.Instant(300)})
			if err != nil {
				t.Fatal(err)
			}
			r3 := late.ID()
			check("late null", 2)
			if err := g.Rels().EndValidity(ctx, r3); err != nil {
				t.Fatal(err)
			}
			check("valid-window end", 1)
			nilRel, err := g.Rels().Add(ctx, "LOGON", a, a, map[string]any{"tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(400), "dev": nil})
			if err != nil {
				t.Fatal(err)
			}
			check("nil-valued", 2)
			erased := addLogon(t, g, a, a, "", 100, 400)
			check("to erase", 3)
			_, err = g.Admin().ExactErase(ctx, adminpkg.ExactErasureRequest{RelationshipIDs: []types.RelID{erased}, Bounds: publicExactErasureBounds})
			if errors.Is(err, graphpkg.ErrCapabilityNotSupported) { // tiered and sharded erase nothing exactly: a delete keeps the counts
				err = g.Rels().Delete(ctx, erased)
			}
			if err != nil {
				t.Fatalf("ExactErase: %v", err)
			}
			pins = nil // erasure rewrites history: every earlier pin answers without it
			check("erasure", 2)
			if _, err := g.Admin().PurgeExpiredRels(ctx, adminpkg.RelPurgePolicy{Types: []string{"LOGON"}, Mode: adminpkg.PurgeByAge, Before: r1.MintInstant()}); err != nil {
				t.Fatalf("PurgeExpiredRels: %v", err)
			}
			check("purge", 1)
			if got, err := g.Rels().ByTypeAndProperty("LOGON", "dev", types.Null, store.QueryOpts{}); err != nil || len(got) != 1 || got[0].ID() != nilRel.ID() || purged == nilRel.ID() {
				t.Errorf("after purge: %v, %v; want only the nil-valued row", relIDs(got), err)
			}
		})
	}
}
