package migrate

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	v4graph "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	v4types "github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// This external v4 source wrapper models old/current-vs-history collisions,
// inverted legacy stamps and failures in the actual v4 public reader path.
// It does not implement a second temporal resolver or alter stored data.
type legacySource struct {
	*memory.Store
	collision, inverted          bool
	historyCalls, currentCalls   int
	historyFailAt, currentFailAt int
	failure                      error
}

func (s *legacySource) GetNode(id v4types.NodeID) (*v4types.Node, error) {
	s.currentCalls++
	if s.currentFailAt == s.currentCalls {
		return nil, s.failure
	}
	n, err := s.Store.GetNode(id)
	if err == nil && s.collision {
		n = n.DeepCopy()
		n.SetVersion(0)
	}
	return n, err
}
func (s *legacySource) GetRelationship(id v4types.RelID) (*v4types.Relationship, error) {
	s.currentCalls++
	if s.currentFailAt == s.currentCalls {
		return nil, s.failure
	}
	r, err := s.Store.GetRelationship(id)
	if err == nil && s.collision {
		r = r.DeepCopy()
		r.SetVersion(0)
	}
	return r, err
}
func (s *legacySource) GetNodeHistory(id v4types.NodeID) ([]*v4types.Node, error) {
	s.historyCalls++
	if s.historyFailAt == s.historyCalls {
		return nil, s.failure
	}
	rows, err := s.Store.GetNodeHistory(id)
	if err == nil && s.inverted && len(rows) > 0 {
		rows[0] = rows[0].DeepCopy()
		rows[0].Temporal().TxTo = rows[0].Temporal().TxFrom - 1
	}
	return rows, err
}
func (s *legacySource) GetRelHistory(id v4types.RelID) ([]*v4types.Relationship, error) {
	s.historyCalls++
	if s.historyFailAt == s.historyCalls {
		return nil, s.failure
	}
	rows, err := s.Store.GetRelHistory(id)
	if err == nil && s.inverted && len(rows) > 0 {
		rows[0] = rows[0].DeepCopy()
		rows[0].Temporal().TxTo = rows[0].Temporal().TxFrom - 1
	}
	return rows, err
}

func legacyGraph(t *testing.T) (*v4graph.Graph, *legacySource) {
	t.Helper()
	s := &legacySource{Store: memory.New(), failure: errors.New("injected source read failure")}
	g, err := v4graph.New(v4graph.Config{Store: s, AllowTxBackfill: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := g.Close(); err != nil {
			t.Error(err)
		}
	})
	return g, s
}

func TestPreflightLossCollisionAndInvertedLegacyStampsRefuse(t *testing.T) {
	for _, rel := range []bool{false, true} {
		for _, mode := range []string{"loss", "collision", "inverted", "corrupt"} {
			t.Run(fmt.Sprintf("%s/rel=%v", mode, rel), func(t *testing.T) {
				g, st := legacyGraph(t)
				s := newSubject(t, g, rel, map[string]any{"value": "X", "tkg_valid_from": int64(1000)})
				s.update(map[string]any{"value": "Y", "tkg_valid_from": int64(2000)}, 3000)
				before, err := s.preflight(options(t, 100))
				if err != nil {
					t.Fatal(err)
				}
				assertShape(t, before, "[1000,0)=X")
				want := HistoryGap
				switch mode {
				case "loss":
					if rel {
						err = st.TruncateRelHistory(v4types.RelID(s.id), 0)
					} else {
						err = st.TruncateNodeHistory(v4types.NodeID(s.id), 0)
					}
					if err != nil {
						t.Fatal(err)
					}
				case "collision":
					st.collision = true
					want = VersionCollision
				case "inverted":
					st.inverted = true
					want = InvertedTransactionRange
				case "corrupt":
					// Actual current stored content no longer matches its old
					// hash, as an in-place/raw-source overwrite might leave it.
					if rel {
						r, err := st.Store.GetRelationship(v4types.RelID(s.id))
						if err != nil {
							t.Fatal(err)
						}
						r = r.DeepCopy()
						_ = r.SetProperty("value", "corrupt")
						err = st.ReplaceRelationship(r)
						if err != nil {
							t.Fatal(err)
						}
					} else {
						n, err := st.Store.GetNode(v4types.NodeID(s.id))
						if err != nil {
							t.Fatal(err)
						}
						n = n.DeepCopy()
						_ = n.SetProperty("value", "corrupt")
						err = st.ReplaceNode(n)
						if err != nil {
							t.Fatal(err)
						}
					}
					want = IntegrityUnverified
				}
				r, err := s.preflight(options(t, 3000))
				if !errors.Is(err, ErrAmbiguousHistory) {
					t.Fatalf("legacy source admitted: %v, findings %v", err, r.Findings)
				}
				found := false
				for _, f := range r.Findings {
					if f.Kind == want {
						found = true
					}
				}
				if !found {
					t.Fatalf("finding %s missing: %v", want, r.Findings)
				}
				for _, seg := range r.Segments {
					if seg.Scope.Kind() != 0 {
						t.Fatal("refused source produced importable scope")
					}
				}
				if mode == "loss" && (!r.SourceChainVerified || len(r.Rows) != 1 || r.Rows[0].Identity.Version != 1) {
					t.Fatal("loss fixture did not expose hash verifier's retained-prefix leniency")
				}
				if mode == "inverted" && r.Rows[0].Identity.Temporal.TxTo != 99 {
					t.Fatal("inverted stamp repaired or dropped")
				}
			})
		}
	}
}

func TestPreflightSourceErrorsAtEachReadLayer(t *testing.T) {
	for _, rel := range []bool{false, true} {
		for _, door := range []string{"timeline", "raw-history", "raw-current", "verifier"} {
			t.Run(fmt.Sprintf("%s/rel=%v", door, rel), func(t *testing.T) {
				g, st := legacyGraph(t)
				s := newSubject(t, g, rel, map[string]any{"value": "X", "tkg_valid_from": int64(1000)})
				s.update(map[string]any{"value": "Y", "tkg_valid_from": int64(2000)}, 3000)
				st.historyCalls = 0
				st.currentCalls = 0
				switch door {
				case "timeline":
					st.historyFailAt = 1
				case "raw-history":
					st.historyFailAt = 2
				case "raw-current":
					st.currentFailAt = 2
				case "verifier":
					st.historyFailAt = 3
				}
				r, err := s.preflight(options(t, 100))
				if !errors.Is(err, st.failure) || !reflect.DeepEqual(r, Report{}) {
					t.Fatalf("layer %s report=%v error=%v calls h%d c%d", door, r, err, st.historyCalls, st.currentCalls)
				}
			})
		}
	}
}

func TestPreflightRelKeepsDeclaredViewAfterEndpointClose(t *testing.T) {
	g := graphFor(t, "tiered")
	s := newSubject(t, g, true, map[string]any{"value": "edge", "tkg_valid_from": int64(10)})
	raw, err := g.Rels().Get(t.Context(), v4types.RelID(s.id))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes().CloseVersion(t.Context(), raw.EndNodeID(), raw.EndNodeID().MintInstant()+100); err != nil {
		t.Fatal(err)
	}
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.preflight(options(t, pin))
	if err != nil {
		t.Fatal(err)
	}
	assertShape(t, r, "[10,0)=edge")
	assertPoint(t, r, int64(raw.EndNodeID().MintInstant()+200), "edge")
	if len(r.Rows) != 1 || r.Rows[0].Rel.EndNodeID() != raw.EndNodeID() || r.Rows[0].Rel.StartNodeID() != raw.StartNodeID() {
		t.Fatal("cross-class endpoint evidence lost")
	}
}

func TestPreflightNilTemporalKeepsSourceMintAndAudit(t *testing.T) {
	for _, rel := range []bool{false, true} {
		t.Run(fmt.Sprintf("rel=%v", rel), func(t *testing.T) {
			g, st := legacyGraph(t)
			s := newSubject(t, g, rel, map[string]any{"value": "nil-metadata"})
			if rel {
				r, err := st.Store.GetRelationship(v4types.RelID(s.id))
				if err != nil {
					t.Fatal(err)
				}
				r = r.DeepCopy()
				r.SetTemporal(nil)
				if err := st.ReplaceRelationship(r); err != nil {
					t.Fatal(err)
				}
			} else {
				n, err := st.Store.GetNode(v4types.NodeID(s.id))
				if err != nil {
					t.Fatal(err)
				}
				n = n.DeepCopy()
				n.SetTemporal(nil)
				if err := st.ReplaceNode(n); err != nil {
					t.Fatal(err)
				}
			}
			r, err := s.preflight(options(t, 100))
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Rows) != 1 || r.Rows[0].Identity.HasTemporal || r.Rows[0].Identity.Temporal != (v4types.TemporalMetadata{}) || r.Rows[0].MintInstant == 0 || len(r.Segments) != 1 || r.Segments[0].RawFrom != r.Rows[0].MintInstant {
				t.Fatalf("nil source metadata changed: %v", r)
			}
		})
	}
}

func TestPreflightRawAuditAndLaterZeroTilingStaySeparate(t *testing.T) {
	for _, rel := range []bool{false, true} {
		t.Run(fmt.Sprintf("rel=%v", rel), func(t *testing.T) {
			g, st := legacyGraph(t)
			s := newSubject(t, g, rel, map[string]any{"value": "X"})
			mint := v4types.NodeID(s.id).MintInstant()
			if rel {
				mint = v4types.RelID(s.id).MintInstant()
			}
			tx := v4types.Instant(time.Now().UnixMilli())
			for tx <= mint {
				time.Sleep(time.Millisecond)
				tx = v4types.Instant(time.Now().UnixMilli())
			}
			s.update(map[string]any{"value": "Y"}, tx)
			// Preserve raw author/audit data that is outside the v4 hash.
			if rel {
				r, err := st.Store.GetRelationship(v4types.RelID(s.id))
				if err != nil {
					t.Fatal(err)
				}
				r = r.DeepCopy()
				tm := r.Temporal()
				tm.CreatedBy = "original-author"
				tm.UpdatedBy = "correcting-author"
				tm.SetBaseEntityID(v4types.EntityID(17))
				if err := st.ReplaceRelationship(r); err != nil {
					t.Fatal(err)
				}
			} else {
				n, err := st.Store.GetNode(v4types.NodeID(s.id))
				if err != nil {
					t.Fatal(err)
				}
				n = n.DeepCopy()
				tm := n.Temporal()
				tm.CreatedBy = "original-author"
				tm.UpdatedBy = "correcting-author"
				tm.SetBaseEntityID(v4types.EntityID(17))
				if err := st.ReplaceNode(n); err != nil {
					t.Fatal(err)
				}
			}
			r, err := s.preflight(options(t, tx))
			if err != nil {
				t.Fatal(err)
			}
			assertShape(t, r, fmt.Sprintf("[100,%d)=X", tx), fmt.Sprintf("[%d,0)=Y", tx))
			row := r.Rows[len(r.Rows)-1]
			if row.Identity.Temporal.ValidFrom != 0 || row.MintInstant == tx || row.Identity.Temporal.UpdatedAt != tx || row.Identity.Temporal.CreatedBy != "original-author" || row.Identity.Temporal.UpdatedBy != "correcting-author" || row.Identity.Temporal.BaseEntityID() != 17 {
				t.Fatal("raw audit/fallback evidence changed")
			}
			old, err := s.preflight(options(t, 100))
			if err != nil {
				t.Fatal(err)
			}
			assertShape(t, old, "[100,0)=X")
		})
	}
}

func TestPreflightCurrentValidityRejectsUnboundPolicyBeforeSourceReads(t *testing.T) {
	for _, rel := range []bool{false, true} {
		t.Run(fmt.Sprintf("rel=%v", rel), func(t *testing.T) {
			g, source := legacyGraph(t)
			s := newSubject(t, g, rel, map[string]any{"value": "X"})
			for _, change := range []func(*Source){
				func(s *Source) { s.ID = " " },
				func(s *Source) { s.Reader = " " },
				func(s *Source) { s.Writer = " " },
				func(s *Source) { s.Format = " " },
				func(s *Source) { s.Evidence = " " },
				func(s *Source) { s.ValidStartPolicy = 255 },
			} {
				o := currentValidityOptions(t, 100)
				change(&o.Source)
				source.historyCalls, source.currentCalls = 0, 0
				r, err := s.preflight(o)
				if !errors.Is(err, ErrInvalidSource) || !reflect.DeepEqual(r, Report{}) || source.historyCalls != 0 || source.currentCalls != 0 {
					t.Fatal("unbound policy read source or produced evidence", r, err)
				}
			}
		})
	}
}

func TestPreflightValidityAuditUsesFixedFallbackWithoutInventingOracle(t *testing.T) {
	// Inert raw-row audit inputs exercise fallback precedence; no source read,
	// effective timeline, history completeness or graph authority is fabricated.
	for _, kind := range []EntityKind{NodeKind, RelationshipKind} {
		for _, tc := range []struct {
			name             string
			policy           ValidStartPolicy
			from, tx, update v4types.Instant
			mint             v4types.Instant
			to               v4types.Instant
			ambiguous        bool
		}{
			{"current-tx-before-updated", ValidStartTxFromUnboundedV1, 0, 100, 200, 300, 101, true},
			{"current-genesis-updated-fallback", ValidStartTxFromUnboundedV1, 0, 0, 100, 300, 101, true},
			{"current-mint-last", ValidStartTxFromUnboundedV1, 0, 0, 0, 100, 101, true},
			{"legacy-genesis-mint-literal", ValidStartLegacy, 0, 100, 200, 300, 101, false},
			{"legacy-real-tick-still-refuses", ValidStartLegacy, 0, 100, 200, 300, 301, true},
			{"explicit-start-wins", ValidStartTxFromUnboundedV1, 10, 100, 200, 300, 11, true},
			{"current-infinity-is-not-one-tick", ValidStartTxFromUnboundedV1, v4types.UnboundedValidFrom, 100, 200, 300, v4types.UnboundedValidFrom + 1, false},
			{"legacy-minimum-is-finite-tick", ValidStartLegacy, v4types.UnboundedValidFrom, 100, 200, 300, v4types.UnboundedValidFrom + 1, true},
		} {
			t.Run(fmt.Sprintf("kind%d/%s", kind, tc.name), func(t *testing.T) {
				o := currentValidityOptions(t, 100)
				o.Source.ValidStartPolicy = tc.policy
				row := EvidenceRow{Identity: RowIdentity{Kind: kind, ID: 17, HasTemporal: true, Temporal: v4types.TemporalMetadata{ValidFrom: tc.from, TxFrom: tc.tx, UpdatedAt: tc.update, ValidTo: tc.to}}, MintInstant: tc.mint}
				input := Report{Source: o.Source, SourceChainVerified: true, Rows: []EvidenceRow{row}}
				r, err := finish(input, o)
				if errors.Is(err, ErrAmbiguousHistory) != tc.ambiguous || err != nil && !errors.Is(err, ErrAmbiguousHistory) || !reflect.DeepEqual(r.Rows, input.Rows) || len(r.Segments) != 0 {
					t.Fatal("audit changed raw rows or invented effective support", r, err)
				}
				if tc.ambiguous && !reflect.DeepEqual(r.Findings, []Finding{{OneTickProvenanceUnknown, 0}}) || !tc.ambiguous && len(r.Findings) != 0 {
					t.Fatal("wrong exact fallback/provenance finding", r.Findings)
				}
			})
		}
	}
}
