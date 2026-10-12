package migrate

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	v4graph "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	v4types "github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	v5types "github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func options(t *testing.T, pin v4types.Instant) Options {
	t.Helper()
	binding, err := v5types.NewDefaultAxisBinding(v5types.GraphID{1}, temporal.AxisID{2}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return Options{Source: Source{ID: "captured-store-A", Reader: "rho-v4.50.1 effective reader", Evidence: "source capture digest"}, Pin: pin, DefaultAxis: binding}
}

func graphFor(t *testing.T, backend string) *v4graph.Graph {
	t.Helper()
	var st store.Store
	var err error
	switch backend {
	case "memory":
		st = memory.New()
	case "badger":
		st, err = badger.New(badger.Config{InMemory: true, Logger: nil, MemTableSize: 8 << 20})
	case "tiered":
		st, err = tiered.New(tiered.Config{InMemory: true, ShardWindow: 7 * 24 * time.Hour, MemTableSize: 8 << 20, RefLabels: []string{"Reference"}})
	case "sharded":
		st, err = sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, MemTableSize: 8 << 20})
	default:
		t.Fatalf("unknown backend %q", backend)
	}
	if err != nil {
		t.Fatal(err)
	}
	g, err := v4graph.New(v4graph.Config{Store: st, AllowTxBackfill: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := g.Close(); err != nil {
			t.Error(err)
		}
	})
	return g
}

type subject struct {
	t   *testing.T
	g   *v4graph.Graph
	rel bool
	id  v4types.EntityID
}

func newSubject(t *testing.T, g *v4graph.Graph, rel bool, props map[string]any) *subject {
	t.Helper()
	s := &subject{t: t, g: g, rel: rel}
	if rel {
		a, err := g.Nodes().Add(t.Context(), []string{"Reference"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes().Add(t.Context(), []string{"Event"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels().AddWithTx(t.Context(), "LINK", a, b, props, 100)
		if err != nil {
			t.Fatal(err)
		}
		s.id = v4types.EntityID(r.ID())
	} else {
		n, err := g.Nodes().AddWithTx(t.Context(), []string{"Event", "Evidence"}, props, 100)
		if err != nil {
			t.Fatal(err)
		}
		s.id = v4types.EntityID(n.ID())
	}
	return s
}

func (s *subject) preflight(o Options) (Report, error) {
	if s.rel {
		return PreflightRel(s.g, v4types.RelID(s.id), o)
	}
	return PreflightNode(s.g, v4types.NodeID(s.id), o)
}
func (s *subject) update(props map[string]any, tx v4types.Instant) {
	s.t.Helper()
	var err error
	if s.rel {
		_, err = s.g.Rels().UpdateWithTx(s.t.Context(), v4types.RelID(s.id), props, tx)
	} else {
		_, err = s.g.Nodes().UpdateWithTx(s.t.Context(), v4types.NodeID(s.id), props, tx)
	}
	if err != nil {
		s.t.Fatal(err)
	}
}
func (s *subject) correct(from, to, tx v4types.Instant, props map[string]any) {
	s.t.Helper()
	var err error
	if s.rel {
		_, err = s.g.Temporal().SetRelVersionIntervalWithTx(s.t.Context(), v4types.RelID(s.id), from, to, props, tx)
	} else {
		_, err = s.g.Temporal().SetNodeVersionIntervalWithTx(s.t.Context(), v4types.NodeID(s.id), from, to, props, tx)
	}
	if err != nil {
		s.t.Fatal(err)
	}
}
func (s *subject) end(mode string, tx v4types.Instant) {
	s.t.Helper()
	var err error
	switch mode {
	case "retract":
		if s.rel {
			err = s.g.Rels().RetractWithTx(s.t.Context(), v4types.RelID(s.id), tx)
		} else {
			err = s.g.Nodes().RetractWithTx(s.t.Context(), v4types.NodeID(s.id), tx)
		}
	case "delete":
		if s.rel {
			err = s.g.Rels().DeleteWithTx(s.t.Context(), v4types.RelID(s.id), tx)
		} else {
			err = s.g.Nodes().DeleteWithTx(s.t.Context(), v4types.NodeID(s.id), tx)
		}
	case "close":
		if s.rel {
			err = s.g.Rels().CloseVersion(s.t.Context(), v4types.RelID(s.id), tx)
		} else {
			err = s.g.Nodes().CloseVersion(s.t.Context(), v4types.NodeID(s.id), tx)
		}
	}
	if err != nil {
		s.t.Fatal(err)
	}
}

func rowProps(row EvidenceRow) map[string]any {
	if row.Node != nil {
		return row.Node.PropertiesMap()
	}
	return row.Rel.PropertiesMap()
}
func timelineShape(r Report) []string {
	out := make([]string, len(r.Segments))
	for i, s := range r.Segments {
		out[i] = fmt.Sprintf("[%d,%d)=%v", s.RawFrom, s.RawTo, rowProps(s.Row)["value"])
	}
	return out
}
func assertShape(t *testing.T, r Report, want ...string) {
	t.Helper()
	if got := timelineShape(r); !slices.Equal(got, want) {
		t.Fatalf("timeline %v, want exact %v", got, want)
	}
}
func assertPoint(t *testing.T, r Report, ms int64, want any) {
	t.Helper()
	p, err := v5types.InstantPosition(r.DefaultAxis.Axis(), v5types.Instant(ms), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	var got []any
	for _, s := range r.Segments {
		contains, err := s.Scope.Contains(p, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if contains {
			got = append(got, rowProps(s.Row)["value"])
		}
	}
	var expected []any
	if want != nil {
		expected = []any{want}
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("point %d returned %v, want exact %v", ms, got, expected)
	}
}

func TestPreflightHistoricalCorrectionUsesEffectiveState(t *testing.T) {
	for _, backend := range []string{"memory", "badger", "tiered", "sharded"} {
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", backend, rel), func(t *testing.T) {
				g := graphFor(t, backend)
				s := newSubject(t, g, rel, map[string]any{"value": "X", "tkg_valid_from": int64(1000)})
				// A distinct entity has a divergent lifecycle; it must never be
				// over-reported by this per-ID migration door.
				other := newSubject(t, g, rel, map[string]any{"value": "phantom", "tkg_valid_from": int64(1000)})
				other.end("retract", 1500)
				before, err := s.preflight(options(t, 100))
				if err != nil {
					t.Fatal(err)
				}
				assertShape(t, before, "[1000,0)=X")
				s.update(map[string]any{"value": "Y", "tkg_valid_from": int64(2000)}, 3000)
				s.correct(1200, 1600, 4000, map[string]any{"value": "corrected"})
				old, err := s.preflight(options(t, 100))
				if err != nil {
					t.Fatal(err)
				}
				assertShape(t, old, "[1000,0)=X")
				current, err := s.preflight(options(t, 4000))
				if err != nil {
					t.Fatal(err)
				}
				assertShape(t, current, "[1000,1200)=X", "[1200,1600)=corrected", "[1600,2000)=X", "[2000,0)=Y")
				for _, r := range current.Rows {
					if r.Identity.ID != s.id {
						t.Fatal("phantom entity leaked")
					}
				}
				for _, test := range []struct {
					at    int64
					value any
				}{{999, nil}, {1000, "X"}, {1199, "X"}, {1200, "corrected"}, {1599, "corrected"}, {1600, "X"}, {1999, "X"}, {2000, "Y"}} {
					assertPoint(t, current, test.at, test.value)
				}
				// The source record/current door is not this timeline: newest
				// state is Y, but the corrected piece and old X still answer.
				if rowProps(current.Segments[1].Row)["value"] == rowProps(current.Rows[len(current.Rows)-1])["value"] {
					t.Fatal("fixture fails to distinguish current from corrected state")
				}
				if !current.SourceChainVerified {
					t.Fatal("corrected DAG chain did not verify")
				}
			})
		}
	}
}

func TestPreflightRetractionDeleteAndCloseRemainDistinct(t *testing.T) {
	for _, rel := range []bool{false, true} {
		for _, mode := range []string{"retract", "delete", "close"} {
			t.Run(fmt.Sprintf("%s/rel=%v", mode, rel), func(t *testing.T) {
				g := graphFor(t, "memory")
				s := newSubject(t, g, rel, map[string]any{"value": "original", "tkg_valid_from": int64(10)})
				s.end(mode, 200)
				old, err := s.preflight(options(t, 100))
				if err != nil {
					t.Fatal(err)
				}
				assertShape(t, old, "[10,0)=original")
				pin := v4types.Instant(200)
				if mode == "close" {
					pin, err = g.Temporal().NowTx()
					if err != nil {
						t.Fatal(err)
					}
				}
				r, err := s.preflight(options(t, pin))
				if err != nil {
					t.Fatal(err)
				}
				if mode == "retract" {
					assertShape(t, r)
					assertPoint(t, r, 50, nil)
				} else {
					assertShape(t, r, "[10,200)=original")
					assertPoint(t, r, 50, "original")
					assertPoint(t, r, 200, nil)
				}
				marked := false
				for _, row := range r.Rows {
					if row.Identity.Temporal.Retracted {
						marked = true
						if row.Identity.Temporal.DeletedAt != 200 {
							t.Fatal("lost raw retraction instant")
						}
					}
				}
				if marked != (mode == "retract") {
					t.Fatalf("marker %v for %s", marked, mode)
				}
				// Raw rows still carry the later tombstone while the old
				// oracle row is normalized to the belief at pin 100.
				if mode != "close" && old.Rows[0].Identity.Temporal.DeletedAt != 200 {
					t.Fatal("raw audit tombstone lost")
				}
			})
		}
	}
}

func TestPreflightZeroFallbackExactKindsAndOwnedEvidence(t *testing.T) {
	for _, rel := range []bool{false, true} {
		t.Run(fmt.Sprintf("rel=%v", rel), func(t *testing.T) {
			g := graphFor(t, "memory")
			props := map[string]any{"value": "X", "tkg_valid_from": int64(0), "string": "2024-01-01", "date": v4types.TemporalValue{Kind: v4types.TemporalDate, Value: "2024-01-01"}, "fine-source-time": int64(133444444444444444), "nested": []any{map[string]any{"v": []float32{1, 2}}, nil}, "bytes": []byte{1, 2}, "small": int8(-5), "float": float32(1.25), "nil-slice": []string(nil), "tkg_author_id": "author", "tkg_authorized_by": "approver", "tkg_auth_level": uint8(2), "tkg_signature": []byte{3, 4}}
			s := newSubject(t, g, rel, props)
			r, err := s.preflight(options(t, 100))
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Rows) != 1 || len(r.Segments) != 1 {
				t.Fatalf("unexpected report %v", timelineShape(r))
			}
			row := r.Rows[0]
			if row.Identity.Temporal.ValidFrom != 0 || row.MintInstant == 0 || r.Segments[0].RawFrom != row.MintInstant {
				t.Fatal("zero interpreted as coordinate or fallback changed")
			}
			lo, hi, ok := r.Segments[0].Scope.Bounds()
			if !ok || !lo.Inclusive() || hi.Kind() != temporal.BoundPositiveInfinity || hi.Inclusive() {
				t.Fatal("lost half-open infinity")
			}
			p, _ := lo.Position()
			ms, err := temporal.InstantMillis(p, temporal.Limits{})
			if err != nil || ms != int64(row.MintInstant) || p.Profile() != temporal.ProfileRationalQ {
				t.Fatal("fallback not exact Q/ms")
			}
			got := rowProps(row)
			for _, key := range []string{"string", "date", "fine-source-time", "nested", "bytes", "small", "float", "nil-slice"} {
				if !reflect.DeepEqual(got[key], props[key]) {
					t.Fatalf("kind/value %q: %T %v", key, got[key], got[key])
				}
			}
			var hash string
			if rel {
				ig := row.Rel.Integrity()
				hash = ig.Hash
				if ig.AuthorID != "author" || ig.AuthorizedBy != "approver" || ig.AuthorizationLevel != 2 || !slices.Equal(ig.Signature, []byte{3, 4}) || ig.FromNodeHash == "" || ig.ToNodeHash == "" {
					t.Fatal("relationship audit lost")
				}
			} else {
				ig := row.Node.Integrity()
				hash = ig.Hash
				if ig.AuthorID != "author" || ig.AuthorizedBy != "approver" || ig.AuthorizationLevel != 2 || !slices.Equal(ig.Signature, []byte{3, 4}) {
					t.Fatal("node audit lost")
				}
			}
			if hash == "" || hash != row.Identity.Hash {
				t.Fatal("original hash lost")
			}
			got["bytes"].([]byte)[0] = 99
			got["nested"].([]any)[0].(map[string]any)["v"].([]float32)[0] = 99
			if rowProps(row)["bytes"].([]byte)[0] != 1 {
				t.Fatal("property output aliases row")
			}
			if row.Node != nil {
				cp := row.Node.DeepCopy()
				_ = cp.SetProperty("value", "changed")
				cp.Temporal().CreatedBy = "changed"
				cp.Integrity().Signature[0] = 99
			} else {
				cp := row.Rel.DeepCopy()
				_ = cp.SetProperty("value", "changed")
				cp.Temporal().CreatedBy = "changed"
				cp.Integrity().Signature[0] = 99
			}
			again, err := s.preflight(options(t, 100))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(rowProps(again.Rows[0]), rowProps(row)) || again.Rows[0].Identity != row.Identity {
				t.Fatal("evidence escaped source ownership")
			}
		})
	}
}

func TestPreflightOneTickRequiresExactVerifiedProvenance(t *testing.T) {
	for _, rel := range []bool{false, true} {
		t.Run(fmt.Sprintf("rel=%v", rel), func(t *testing.T) {
			g := graphFor(t, "memory")
			s := newSubject(t, g, rel, map[string]any{"value": "tick", "tkg_valid_from": int64(1800000000000), "tkg_valid_to": int64(1800000000001)})
			o := options(t, 100)
			o.Source.Writer = "v4.50.1 claimed"
			o.Source.Format = "fv2" // Date/release identity alone is not evidence.
			r, err := s.preflight(o)
			if !errors.Is(err, ErrAmbiguousHistory) || !reflect.DeepEqual(r.Findings, []Finding{{OneTickProvenanceUnknown, 0}}) {
				t.Fatalf("ambiguity %v %v", r.Findings, err)
			}
			if r.Segments[0].Scope.Kind() != temporal.ScopeInvalid || r.Rows[0].Identity.Temporal.ValidTo != 1800000000001 {
				t.Fatal("ambiguous raw span was inferred or discarded")
			}
			att := SpanAttestation{SourceID: o.Source.ID, Row: r.Rows[0].Identity, Writer: "audited writer digest", Format: "verified writer/row schema", Evidence: "row-qualified no-sentinel audit", ExcludesOneTickSentinels: true}
			for _, change := range []func(*SpanAttestation){func(a *SpanAttestation) { a.SourceID = "another source" }, func(a *SpanAttestation) { a.Row.Version++ }, func(a *SpanAttestation) { a.Row.Temporal.TxFrom++ }, func(a *SpanAttestation) { a.Row.Temporal.ValidTo++ }, func(a *SpanAttestation) { a.Row.Kind = 0 }, func(a *SpanAttestation) { a.Row.Hash = "wrong hash" }, func(a *SpanAttestation) { a.Row.Audit.Signature = "different bytes" }, func(a *SpanAttestation) { a.Row.Audit.AuthorizationLevel++ }, func(a *SpanAttestation) { a.Row.Temporal.Retracted = true }, func(a *SpanAttestation) { a.Writer = "" }, func(a *SpanAttestation) { a.Format = "" }, func(a *SpanAttestation) { a.Evidence = "" }, func(a *SpanAttestation) { a.ExcludesOneTickSentinels = false }} {
				bad := att
				change(&bad)
				o.VerifiedSpans = []SpanAttestation{bad}
				if _, err := s.preflight(o); !errors.Is(err, ErrAmbiguousHistory) {
					t.Fatal("unbound attestation admitted", err)
				}
			}
			o.VerifiedSpans = []SpanAttestation{att}
			mapped, err := s.preflight(o)
			if err != nil {
				t.Fatal(err)
			}
			if mapped.Segments[0].Scope.Kind() != temporal.ScopeSpan || !reflect.DeepEqual(mapped.AcceptedSpans, []SpanAttestation{att}) {
				t.Fatal("genuine one-tick span lost provenance or became point")
			}
			assertPoint(t, mapped, 1800000000000, "tick")
			assertPoint(t, mapped, 1800000000001, nil)
		})
	}
}

func TestMapSpanExactExtremesAndFraction(t *testing.T) {
	o := options(t, 100)
	for _, pair := range [][2]int64{{math.MinInt64, math.MaxInt64}, {-2, -1}, {0, 1}, {math.MaxInt64, 0}} {
		s, err := mapSpan(o.DefaultAxis.Axis(), v4types.Instant(pair[0]), v4types.Instant(pair[1]), o.Limits)
		if err != nil || s.Kind() != temporal.ScopeSpan {
			t.Fatalf("exact span %v: %v", pair, err)
		}
		lo, hi, _ := s.Bounds()
		p, _ := lo.Position()
		ms, err := temporal.InstantMillis(p, o.Limits)
		if err != nil || ms != pair[0] {
			t.Fatal("start changed")
		}
		if pair[1] == 0 {
			if hi.Kind() != temporal.BoundPositiveInfinity {
				t.Fatal("open sentinel lost")
			}
		} else {
			p, _ := hi.Position()
			ms, err = temporal.InstantMillis(p, o.Limits)
			if err != nil || ms != pair[1] {
				t.Fatal("end changed")
			}
		}
	}
	s, err := mapSpan(o.DefaultAxis.Axis(), 0, 1, o.Limits)
	if err != nil {
		t.Fatal(err)
	}
	half, err := temporal.Fraction(temporal.Int64(1), temporal.Int64(2), o.Limits)
	if err != nil {
		t.Fatal(err)
	}
	p, err := temporal.RationalPosition(o.DefaultAxis.Axis(), half)
	if err != nil {
		t.Fatal(err)
	}
	contains, err := s.Contains(p, o.Limits)
	if err != nil || !contains {
		t.Fatal("Q/ms one tick was discretized")
	}
	_, err = mapSpan(o.DefaultAxis.Axis(), 100, 200, temporal.Limits{MaxMagnitudeBits: 1})
	if !errors.Is(err, temporal.ErrResourceLimit) {
		t.Fatal("magnitude limit lost", err)
	}
}

func TestPreflightRefusalsPreserveSourceSentinels(t *testing.T) {
	for _, rel := range []bool{false, true} {
		t.Run(fmt.Sprintf("rel=%v", rel), func(t *testing.T) {
			g := graphFor(t, "memory")
			s := newSubject(t, g, rel, map[string]any{"value": "X"})
			check := func(g *v4graph.Graph, id v4types.EntityID, o Options, want error) {
				t.Helper()
				var r Report
				var err error
				if rel {
					r, err = PreflightRel(g, v4types.RelID(id), o)
				} else {
					r, err = PreflightNode(g, v4types.NodeID(id), o)
				}
				if !errors.Is(err, want) || !reflect.DeepEqual(r, Report{}) {
					t.Fatalf("refusal report=%v err=%v want=%v", r, err, want)
				}
			}
			check(nil, s.id, options(t, 100), v4graph.ErrNilGraph)
			check(new(v4graph.Graph), s.id, options(t, 100), v4graph.ErrNilGraph)
			o := options(t, 100)
			o.Source.ID = " "
			check(g, s.id, o, ErrInvalidSource)
			o = options(t, 100)
			o.DefaultAxis = v5types.DefaultAxisBinding{}
			check(g, s.id, o, v5types.ErrInvalidGraphIdentity)
			o = options(t, 100)
			o.Limits.MaxValueBytes = -1
			check(g, s.id, o, temporal.ErrInvalidLimits)
			o = options(t, 100)
			o.Limits.MaxMagnitudeBits = 1
			check(g, s.id, o, temporal.ErrResourceLimit)
			check(g, s.id, options(t, 0), v4graph.ErrInvalidTimeRange)
			check(g, s.id, options(t, math.MaxInt64), v4graph.ErrTxPinTooNew)
			check(g, 0, options(t, 100), store.ErrInvalidStoreMutation)
			missing := v4graph.ErrNodeNotFound
			if rel {
				missing = v4graph.ErrRelNotFound
			}
			check(g, 1, options(t, 100), missing)
			r, err := s.preflight(options(t, 99))
			if err != nil || len(r.Segments) != 0 || len(r.Rows) != 1 {
				t.Fatalf("created-after-pin %v %v", r, err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			check(g, s.id, options(t, 100), v4graph.ErrGraphClosed)
		})
	}
}
