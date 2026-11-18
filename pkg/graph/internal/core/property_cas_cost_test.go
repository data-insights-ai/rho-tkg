package core

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestCASComparisonPersistedHistoryGrowth(t *testing.T) {
	testtier.Slow(t)
	// Native decoder/proof observers are process-wide; keep this test serial.
	for _, be := range rbdBackendsWith(func(c *Config) { c.UntimedValidStart = UntimedWholeAxis }) {
		t.Run(be.name, func(t *testing.T) {
			g, shut := be.open(t, t.TempDir())
			defer shut()
			a, err := g.Nodes.Add(t.Context(), []string{"Ref"}, map[string]any{"epoch": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			b, err := g.Nodes.Add(t.Context(), []string{"Event"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			rel, err := g.Rels.Add(t.Context(), "LINK", a, b, map[string]any{"epoch": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			var controls [2]storepkg.ReadWork
			var native [2]int64
			for cut := int64(1); cut <= 8; cut++ {
				if _, err := g.Nodes.Update(t.Context(), a.ID(), map[string]any{"epoch": cut}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels.Update(t.Context(), rel.ID(), map[string]any{"epoch": cut}); err != nil {
					t.Fatal(err)
				}
				if cut != 2 && cut != 8 {
					continue
				}
				if err := g.store.(interface{ Flush() error }).Flush(); err != nil {
					t.Fatal(err)
				}
				for i, compare := range []func(context.Context) (bool, error){
					func(ctx context.Context) (bool, error) {
						return g.Nodes.CompareAndSetProperty(ctx, a.ID(), "epoch", int64(-1), int64(9))
					},
					func(ctx context.Context) (bool, error) {
						return g.Rels.CompareAndSetProperty(ctx, rel.ID(), "epoch", int64(-1), int64(9))
					},
				} {
					run := func(budget storepkg.ReadBudget) (storepkg.ReadWork, int64, error) {
						rbdEvict(t, g, []types.NodeID{a.ID(), b.ID()}, []types.RelID{rel.ID()})
						ctx, meter := storepkg.WithReadBudget(t.Context(), budget)
						var matched bool
						var err error
						before := g.lastInstant.Load()
						decoded := badger.CountRowDecodesForTest(func() { matched, err = compare(ctx) })
						if matched || g.lastInstant.Load() != before {
							t.Fatal("mismatch changed state or clock")
						}
						return meter.Work(), decoded, err
					}
					work, decoded, err := run(storepkg.ReadBudget{})
					if err != nil || decoded == 0 || work.DecodedBytes == 0 {
						t.Fatalf("cut %d kind %d nonempty control: %+v, %d decodes, %v", cut, i, work, decoded, err)
					}
					if cut == 2 {
						controls[i], native[i] = work, decoded
					} else if base := controls[i]; work.Candidates > base.Candidates || work.HistoryExamined > base.HistoryExamined || work.DecodedBytes > base.DecodedBytes || work.BufferBytes > base.BufferBytes || decoded != native[i] {
						t.Errorf("kind %d comparison grows with history: two %+v/%d decodes, eight %+v/%d decodes", i, base, native[i], work, decoded)
					}
					base := controls[i]
					limited, decodes, err := run(storepkg.ReadBudget{Candidates: base.Candidates, HistoryExamined: 2, DecodedBytes: base.DecodedBytes, BufferBytes: base.BufferBytes})
					if err != nil || limited != work || decodes != decoded {
						t.Errorf("cut %d kind %d under the same two-cut allowance: %+v/%d decodes, %v; control %+v/%d", cut, i, limited, decodes, err, work, decoded)
					}
				}
			}
		})
	}
}

type casBoundsSnapshot struct {
	storepkg.ReadSnapshot
	version uint32
	newest  types.Instant
	known   bool
	err     error
	proofs  int
	metas   int
}

func (s *casBoundsSnapshot) NodeHistoryBounds(types.NodeID) (uint32, types.Instant, bool, error) {
	s.proofs++
	return s.version, s.newest, s.known, s.err
}

func (s *casBoundsSnapshot) RelHistoryBounds(types.RelID) (uint32, types.Instant, bool, error) {
	return s.NodeHistoryBounds(1)
}

func (s *casBoundsSnapshot) NodeHistoryTemporalMeta(types.NodeID) ([]storepkg.VersionTemporalMeta, error) {
	s.metas++
	return []storepkg.VersionTemporalMeta{{Version: 1, Temporal: &types.TemporalMetadata{TxFrom: 10, TxTo: 20}}}, nil
}

func (s *casBoundsSnapshot) RelHistoryTemporalMeta(types.RelID) ([]storepkg.VersionTemporalMeta, error) {
	return s.NodeHistoryTemporalMeta(1)
}

// A complete proof must preserve the same boundary semantics as the admitted
// metadata fold. Unknown is a fallback; corruption and refusal must stop it.
func TestCASComparisonKnownBoundsParity(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New(), UntimedValidStart: UntimedWholeAxis})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	n, err := g.Nodes.Add(t.Context(), []string{"Head"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	end, err := g.Nodes.Add(t.Context(), []string{"End"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add(t.Context(), "LINK", n, end, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name             string
		version, top     uint32
		txFrom, newest   types.Instant
		known, segmented bool
		proofErr, want   error
	}{
		{"ordinary", 2, 1, 30, 30, true, false, nil, nil},
		{"newer version", 2, 3, 30, 30, true, false, nil, ErrUntimedModeMismatch},
		{"newer belief", 2, 1, 30, 31, true, false, nil, ErrUntimedModeMismatch},
		{"version ceiling", math.MaxUint32 - 1, math.MaxUint32, 30, 30, true, false, nil, nil},
		{"negative belief", 2, 1, -1, -20, true, false, nil, ErrUntimedModeMismatch},
		{"unknown", 2, 0, 30, 0, false, false, nil, nil},
		{"segmented", 2, 1, 30, 30, true, true, nil, ErrUntimedModeMismatch},
		{"refusal", 2, 1, 30, 30, false, false, storepkg.ErrReadBudget, storepkg.ErrReadBudget},
		{"corruption", 2, 1, 30, 30, true, false, storepkg.ErrCorruptWire, storepkg.ErrCorruptWire},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, rel := range []bool{false, true} {
				snap := &casBoundsSnapshot{version: tc.top, newest: tc.newest, known: tc.known, err: tc.proofErr}
				_, m := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{})
				reads := entityReads{c: g, snap: snap, tally: m.Tally("CAS")}
				tm := *n.Temporal()
				tm.TxFrom = tc.txFrom
				if tc.segmented {
					tm.ValidFrom = 10
				}
				if rel {
					current := r.DeepCopy()
					current.SetVersion(tc.version)
					current.SetTemporal(&tm)
					err = g.checkWholeAxisVersionedRel(r.ID(), current, "compare-and-set", reads)
				} else {
					current := n.DeepCopy()
					current.SetVersion(tc.version)
					current.SetTemporal(&tm)
					err = g.checkWholeAxisVersionedNode(n.ID(), current, "compare-and-set", reads)
				}
				proofs, metas := 1, 0
				if tc.segmented {
					proofs = 0
				} else if !tc.known && tc.proofErr == nil {
					metas = 1
				}
				if !errors.Is(err, tc.want) || snap.proofs != proofs || snap.metas != metas {
					t.Fatalf("rel=%v: err %v want %v, proofs %d want %d, metas %d want %d", rel, err, tc.want, snap.proofs, proofs, snap.metas, metas)
				}
			}
		})
	}
}
