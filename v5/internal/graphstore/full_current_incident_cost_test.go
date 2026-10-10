package graphstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

type incidentPilotMetric struct {
	Calls                       int
	ElapsedNS                   int64
	AllocatedBytes, Allocations uint64
	Work                        PageWork
	Native                      currentPresenceTreeWork
	Baseline                    currentIncidentCounters
	Candidates                  []IncidentAtCandidate
	Complete                    bool
	Refusal                     string
	CatalogLimits               Limits
	GraphLimits                 GraphLimits
	CallerBudget                graphstate.ReadBudget
}

func pilotFixture(t *testing.T, n int, masked bool) *currentIncidentFixture {
	t.Helper()
	f := newCurrentIncidentFixture(t, vfs.NewMem(), "pilot")
	w := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: w}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: w})
	for start := 0; start < n+10; {
		count := min(8, n+10-start)
		ops := make([]graphstate.Operation, 0, 2*count)
		for j := range count {
			id := graphstate.EntityID(start + j + 3)
			target := graphstate.EntityID(2)
			if start+j < 10 {
				target = 1
			}
			ops = append(ops, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: id, Life: 31, Scope: w, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: target, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}}, graphstate.Operation{Kind: graphstate.Set, Owner: id, Life: 31, Scope: w, Name: "payload", Value: graphstate.String(fmt.Sprintf("payload-%d", id)), ValueID: graphstate.ValueID(id + 1000)})
		}
		f.applyBounded(t, ops, 2)
		start += count
	}
	if masked {
		op := graphstate.Operation{Kind: graphstate.Close, Owner: 2, Life: 11, Scope: f.span(t, 50, 100)}
		c, close := f.catalog(t, f.index)
		revision, _ := state.NewRevision(f.index+1, f.index+1000)
		_, work, diagnosticErr := stageOperations(t.Context(), c, []graphstate.Operation{op}, revision, f.StageLimits, nil)
		v, openErr := OpenReadView(t.Context(), c, f.StageLimits)
		if openErr != nil {
			t.Fatal(openErr)
		}
		limits, _ := f.StageLimits.resolve()
		_, planErr := graphstate.Plan(t.Context(), &pilotDiagnosticReader{ReadView: v, t: t}, []graphstate.Operation{op}, revision, limits.Planner)
		t.Logf("PLAN_DEFAULT_DIAGNOSTIC n=%d work=%+v err=%v", n, v.Work(), planErr)
		_ = v.Close()
		close()
		t.Logf("BUILD_DEFAULT_DIAGNOSTIC n=%d limits=%+v work=%+v err=%v", n, f.StageLimits, work, diagnosticErr)
		if diagnosticErr != nil || planErr != nil {
			t.Fatal("accepted retraction fixture/default admission failed", work, diagnosticErr, planErr)
		}
		f.apply(t, op)
	} else {
		for start := 0; start < n; {
			count := min(8, n-start)
			ops := make([]graphstate.Operation, 0, count)
			for j := range count {
				ops = append(ops, graphstate.Operation{Kind: graphstate.Close, Owner: graphstate.EntityID(start + j + 13), Life: 31, Scope: f.span(t, 50, 100)})
			}
			f.applyBounded(t, ops, 1)
			start += count
		}
	}
	return f
}

func pilotQueryCatalog(t *testing.T, f *currentIncidentFixture) (*Catalog, func()) {
	t.Helper()
	raw, err := f.db.ApplicationView(f.index)
	if err != nil {
		t.Fatal(err)
	}
	c, err := OpenCatalog(raw, testNamespace(), 3, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return c, func() {
		if err := raw.Close(); err != nil {
			t.Error(err)
		}
	}
}
func pilotCurrent(t *testing.T, f *currentIncidentFixture, l GraphLimits) (out incidentPilotMetric, err error) {
	t.Helper()
	c, close := pilotQueryCatalog(t, f)
	defer close()
	out.CatalogLimits = c.limits
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	v, err := OpenReadView(t.Context(), c, l)
	if err != nil {
		return out, err
	}
	defer v.Close()
	out.GraphLimits = pilotBudgetDescriptor(t, v.limits)
	out.CallerBudget = graphstate.ReadBudget{Rows: 32, Bytes: 64 << 10}
	defer func() {
		out.ElapsedNS = time.Since(started).Nanoseconds()
		runtime.ReadMemStats(&after)
		out.AllocatedBytes = after.TotalAlloc - before.TotalAlloc
		out.Allocations = after.Mallocs - before.Mallocs
		out.Work = v.Work()
		out.Native = v.currentWork
	}()
	query := IncidentAtQuery{Endpoint: 1, Life: 11, At: f.position(t, 75), Direction: IncidentBoth, Type: "R", Visible: graphstate.Effective}
	var cursor graphstate.Cursor
	for range 4096 {
		page, e := v.IncidentAt(t.Context(), query, cursor, out.CallerBudget)
		out.Calls++
		if e != nil {
			return out, e
		}
		out.Candidates = append(out.Candidates, page.Candidates...)
		if page.Complete {
			if page.Next != 0 {
				return out, ErrCorrupt
			}
			out.Complete = true
			return out, nil
		}
		if page.Next == 0 || page.Next == cursor {
			return out, ErrCorrupt
		}
		cursor = page.Next
	}
	return out, ErrCorrupt
}
func pilotBaseline(t *testing.T, f *currentIncidentFixture, ended func(graphstate.EntityID) bool, l GraphLimits) (out incidentPilotMetric, err error) {
	t.Helper()
	c, close := pilotQueryCatalog(t, f)
	defer close()
	out.CatalogLimits = c.limits
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	v, err := OpenReadView(t.Context(), c, l)
	if err != nil {
		return out, err
	}
	defer v.Close()
	out.GraphLimits = pilotBudgetDescriptor(t, v.limits)
	out.CallerBudget = graphstate.ReadBudget{Rows: 256, Bytes: 4 << 20}
	p := &currentIncidentProbe{ReadView: v, Ended: ended}
	query := currentIncidentQuery{Endpoint: 1, Life: 11, At: f.position(t, 75), Type: "R", Visible: graphstate.Effective}
	ids, complete, err := currentIncidentObserve(t.Context(), p, query)
	out.Calls = p.Counters.RawPages
	out.ElapsedNS = time.Since(started).Nanoseconds()
	out.Work = v.Work()
	out.Baseline = p.Counters
	out.Complete = complete
	runtime.ReadMemStats(&after)
	out.AllocatedBytes = after.TotalAlloc - before.TotalAlloc
	out.Allocations = after.Mallocs - before.Mallocs
	if err != nil {
		return out, err
	}
	if !complete {
		return out, ErrCorrupt
	}
	// The unchanged baseline atomically discards selection on refusal. Exact
	// role/life qualification is independent validation, excluded from costs.
	oracle, err := OpenReadView(t.Context(), c, l)
	if err != nil {
		return out, err
	}
	defer oracle.Close()
	for _, id := range ids {
		projection, e := graphstate.Project(t.Context(), oracle, id, query.At, query.Visible, oracle.limits.Planner)
		if e != nil {
			return out, e
		}
		if !projection.Active {
			return out, ErrCorrupt
		}
		var roles IncidentDirection
		for i, endpoint := range projection.Endpoints {
			if endpoint.ID == query.Endpoint && (projection.Record.Mode == graphstate.IdentityReference || query.Life == 0 || endpoint.BoundLife == query.Life) {
				if i == 0 {
					roles |= IncidentSource
				} else {
					roles |= IncidentTarget
				}
			}
		}
		out.Candidates = append(out.Candidates, IncidentAtCandidate{id, projection.Life, roles})
	}
	return out, nil
}
func pilotRefusal(t *testing.T, metric *incidentPilotMetric, err error) {
	t.Helper()
	if err == nil {
		if !metric.Complete {
			t.Fatal("incomplete result without refusal")
		}
		return
	}
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatal("non-resource pilot failure", metric, err)
	}
	metric.Refusal = err.Error()
	metric.Complete = false
}
func runIncidentAtSmallCostPilot(t *testing.T, label string, l GraphLimits) {
	t.Helper()
	for _, masked := range []bool{false, true} {
		lane := "own-ended"
		if masked {
			lane = "opposite-masked"
		}
		for _, n := range []int{0, 64, 256} {
			t.Run(fmt.Sprintf("%s/%d", lane, n), func(t *testing.T) {
				f := pilotFixture(t, n, masked)
				current, err := pilotCurrent(t, f, l)
				pilotRefusal(t, &current, err)
				baseline, err := pilotBaseline(t, f, func(id graphstate.EntityID) bool { return !masked && id >= 13 }, l)
				pilotRefusal(t, &baseline, err)
				want := make([]IncidentAtCandidate, 10)
				for i := range want {
					want[i] = IncidentAtCandidate{graphstate.EntityID(i + 3), 31, IncidentBoth}
				}
				if current.Complete {
					exactIncidentAt(t, current.Candidates, want)
				}
				if baseline.Complete {
					exactIncidentAt(t, baseline.Candidates, want)
				}
				if current.Complete && baseline.Complete {
					exactIncidentAt(t, current.Candidates, baseline.Candidates)
				}
				if !masked && current.Complete && current.Native.WitnessEntityCalls != 10 {
					t.Fatal("own-ended metadata fetched", current.Native)
				}
				fixtureLimits, err := f.StageLimits.resolve()
				if err != nil {
					t.Fatal(err)
				}
				fixtureLimits = pilotBudgetDescriptor(t, fixtureLimits)
				buildCatalog, close := f.catalog(t, f.index)
				fixtureCatalog := buildCatalog.limits
				close()
				b, err := json.Marshal(struct {
					Label, Lane        string
					OtherRelationships int
					FixtureLimits      GraphLimits
					FixtureCatalog     Limits
					FixtureWork        PageWork
					Baseline, Current  incidentPilotMetric
					Interpretation     string
				}{label, lane, n, fixtureLimits, fixtureCatalog, f.BuildWork, baseline, current, "Diagnostic only: any REFUSAL remains incomplete, not performance/default acceptance. Current partial pages retained; baseline selection discarded on refusal. Qualification excluded; single process allocation/time includes background. No10k/100k acceptance."})
				if err != nil {
					t.Fatal(err)
				}
				t.Log("PILOT " + string(b))
			})
		}
	}
}
func TestIncidentAtSmallCostPilotExpandedLimits(t *testing.T) {
	runIncidentAtSmallCostPilot(t, "expanded64MiB", pilotQueryLimits())
}
func TestIncidentAtSmallCostPilotLiteralDefaults(t *testing.T) {
	runIncidentAtSmallCostPilot(t, "literal-default", GraphLimits{})
}

type pilotDiagnosticReader struct {
	*ReadView
	t *testing.T
}

func (p *pilotDiagnosticReader) IncidentRelationships(ctx context.Context, query graphstate.IncidentPredicate, cursor graphstate.Cursor, budget graphstate.ReadBudget) (graphstate.EntityPage, error) {
	before := p.Work()
	page, err := p.ReadView.IncidentRelationships(ctx, query, cursor, budget)
	if err != nil {
		p.t.Logf("PLAN_FAILED_DOOR IncidentRelationships endpoint=%d budget=%+v before=%+v after=%+v err=%v", query.Endpoint, budget, before, p.Work(), err)
	}
	return page, err
}
func (p *pilotDiagnosticReader) ComponentKeys(ctx context.Context, query graphstate.KeyPredicate, cursor graphstate.Cursor, budget graphstate.ReadBudget) (graphstate.KeyPage, error) {
	before := p.Work()
	page, err := p.ReadView.ComponentKeys(ctx, query, cursor, budget)
	if err != nil {
		p.t.Logf("PLAN_FAILED_DOOR ComponentKeys query=%+v budget=%+v before=%+v after=%+v err=%v", query, budget, before, p.Work(), err)
	}
	return page, err
}
func (p *pilotDiagnosticReader) ComponentPage(ctx context.Context, query graphstate.ComponentQuery, cursor graphstate.Cursor, budget graphstate.ReadBudget) (graphstate.ComponentPage, error) {
	before := p.Work()
	page, err := p.ReadView.ComponentPage(ctx, query, cursor, budget)
	if err != nil {
		p.t.Logf("PLAN_FAILED_DOOR ComponentPage key=%+v budget=%+v before=%+v after=%+v err=%v", query.Key, budget, before, p.Work(), err)
	}
	return page, err
}

func pilotQueryLimits() GraphLimits {
	return GraphLimits{MaxSourceRows: 65536, MaxSourceBytes: 64 << 20, Planner: graphstate.Limits{MaxReadBytes: 64 << 20, MaxRows: 65536}}
}

// Only these two planner overrides are used in this small pilot. Report their
// effective policy explicitly while still passing literal GraphLimits{} to the
// default query, without modifying any production/default configuration.
func pilotBudgetDescriptor(t *testing.T, l GraphLimits) GraphLimits {
	t.Helper()
	if l.Planner != (graphstate.Limits{MaxReadBytes: l.Planner.MaxReadBytes, MaxRows: l.Planner.MaxRows}) {
		t.Fatal("unreported pilot planner override", l.Planner)
	}
	defaults := graphstate.DefaultLimits()
	if l.Planner.MaxReadBytes != 0 {
		defaults.MaxReadBytes = l.Planner.MaxReadBytes
	}
	if l.Planner.MaxRows != 0 {
		defaults.MaxRows = l.Planner.MaxRows
	}
	l.Planner = defaults
	return l
}
