package graphstore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Observes grants to a genuine immutable native provider; it changes no rows,
// captures, authority, errors, timing or successful completion.
type rangeMoveGrantProbe struct {
	ApplicationReadSource
	output    *graphstate.OutputBudget
	oversized bool
}

func (p *rangeMoveGrantProbe) Get(ctx context.Context, key []byte, maximum int) (raftlog.KV, bool, error) {
	if maximum > p.output.Remaining()-64 {
		p.oversized = true
	}
	return p.ApplicationReadSource.Get(ctx, key, maximum)
}
func (p *rangeMoveGrantProbe) Scan(ctx context.Context, lower, upper, after []byte, b raftlog.ReadBudget) (raftlog.ApplicationPage, error) {
	if b.Bytes > p.output.Remaining()-128 {
		p.oversized = true
	}
	return p.ApplicationReadSource.Scan(ctx, lower, upper, after, b)
}

func TestRangeMoveLargeNativeCatalogContinuationAdmitsOutputBeforeSource(t *testing.T) {
	sources, d, binding, cfg, stores := transactionViewFixture(t)
	actual, _ := sources[0].Root()
	root, e := DecodeRoot(actual.Image)
	if e != nil {
		t.Fatal(e)
	}
	catalog := &Catalog{view: sources[0], root: root, rootImageBytes: 172, limits: DefaultLimits(), hash: equalityDigest}
	q, e := catalog.reader(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	index, found, e := q.fullDescriptor(root)
	if e != nil || !found {
		t.Fatal(e)
	}
	stage, e := allocateFullStage(catalog, fullStageState{root, index, DefaultPageLimits()})
	if e != nil {
		t.Fatal(e)
	}
	defer stage.Close()
	if e = stage.Value(t.Context(), ValueRef{d.Graph(), 5}, graphstate.String(strings.Repeat("v", 24<<10))); e != nil {
		t.Fatal(e)
	}
	writes, e := stage.Writes()
	if e != nil {
		t.Fatal(e)
	}
	sources[0] = ownershipView(t, stores[0], ownershipInstallRaw(t, stores[0], actual.Image, writes))
	descriptor := rangeMoveTestPlan(t, sources, d, binding, cfg)
	first, _, e := ExportRangeMovePage(t.Context(), sources[0], descriptor, RangeMoveCursor{}, raftlog.ReadBudget{Rows: 1, Bytes: 4160}, Limits{}, GraphLimits{}, transactionTestOutput(t))
	if e != nil || first.RecordDone || first.Kind != rangeMoveValues || first.Next.Offset != 64 {
		t.Fatal("fixture has no large native fragmented value", e)
	}
	for i, cursor := range []RangeMoveCursor{{}, first.Next} {
		name := "initial"
		if i == 1 {
			name = "continuation"
		}
		t.Run(name, func(t *testing.T) {
			output, _ := graphstate.NewOutputBudget(16 << 10)
			probe := &rangeMoveGrantProbe{ApplicationReadSource: sources[0], output: output}
			page, _, err := ExportRangeMovePage(t.Context(), probe, descriptor, cursor, raftlog.ReadBudget{Rows: 1, Bytes: 4160}, Limits{}, GraphLimits{}, output)
			if !errors.Is(err, ErrResourceLimit) && !errors.Is(err, raftlog.ErrLimit) || page.Payload != nil {
				t.Fatal("tight output exposed partial read", err)
			}
			if probe.oversized {
				t.Fatal("native large-row source grant exceeded original output allowance before backing allocation", cursor.Offset)
			}
		})
	}
}
