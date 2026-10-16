package graphstore

import (
	"context"
	"errors"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestGenuineIncidentContinuationKeepsWholeRunsAndRejectsRebinding(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	whole := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: whole})
	for _, id := range []graphstate.EntityID{3, 4, 5} {
		mode := graphstate.LifeBound
		binding := graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}
		if id == 4 {
			mode = graphstate.IdentityReference
			binding = graphstate.LifeRecord{}
		}
		f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: id, Life: 31, Scope: whole, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 1, Mode: mode}, Binding: binding})
	}
	// A self-loop has both roles and this relationship has two retained lives.
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 3, Life: 31, Scope: f.span(t, 50, 100)}, graphstate.Operation{Kind: graphstate.Reopen, Owner: 3, Life: 32, Scope: f.span(t, 50, 100), Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}})
	old := f.index
	p := graphstate.IncidentPredicate{Endpoint: 1, Life: 11, Window: whole}
	v := f.view(t, old)
	b := raftlog.ReadBudget{Rows: 512, Bytes: 4 << 20}
	page, err := v.IncidentRelationshipsChunk(t.Context(), p, 0, b, 1)
	if err != nil || page.Complete || !slices.Equal(page.Entities, []graphstate.EntityID{3}) {
		t.Fatal(page, err)
	}
	capsule, err := v.ExportIncidentContinuation(t.Context(), p, page.Next)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.ExportIncidentContinuation(t.Context(), p, 0); !errors.Is(err, ErrInvalid) {
		t.Fatal("start minted capsule", err)
	}
	if _, err := v.ExportIncidentContinuation(t.Context(), p, page.Next+1000); !errors.Is(err, ErrInvalid) {
		t.Fatal("unknown cursor accepted", err)
	}
	if _, err := v.ImportIncidentContinuation(t.Context(), p, IncidentContinuation{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("empty capsule accepted", err)
	}
	_ = v.Close()
	_ = v.c.view.Close()
	// Current mutation cannot replace the original retained cut on import.
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 5, Life: 31, Scope: whole})
	current := f.view(t, f.index)
	if _, err := current.ImportIncidentContinuation(t.Context(), p, capsule); !errors.Is(err, ErrInvalid) {
		t.Fatal("current root accepted old capsule", err)
	}
	retained := f.view(t, old)
	for _, wrong := range []graphstate.IncidentPredicate{{Endpoint: 9, Life: 11, Window: whole}, {Endpoint: 1, Life: 12, Window: whole}, {Endpoint: 1, Life: 11, Window: f.span(t, 0, 50)}} {
		if _, err := retained.ImportIncidentContinuation(t.Context(), wrong, capsule); !errors.Is(err, ErrInvalid) {
			t.Fatal("predicate rebound", wrong, err)
		}
	}

	foreign, err := temporal.All(testAxis(t, 2, temporal.ProfileIntegerZ))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retained.ImportIncidentContinuation(t.Context(), graphstate.IncidentPredicate{Endpoint: 1, Life: 11, Window: foreign}, capsule); !errors.Is(err, ErrInvalid) {
		t.Fatal("axis rebound", err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := retained.ImportIncidentContinuation(cancelled, p, capsule); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled import accepted", err)
	}
	original := retained.limits.Pages.MaxCursorBytes
	retained.limits.Pages.MaxCursorBytes = 255
	if _, err := retained.ImportIncidentContinuation(t.Context(), p, capsule); !errors.Is(err, ErrResourceLimit) || len(retained.cursors) != 0 {
		t.Fatal("cursor cap bypassed", err)
	}
	retained.limits.Pages.MaxCursorBytes = original
	token, err := retained.ImportIncidentContinuation(t.Context(), p, capsule)
	if err != nil {
		t.Fatal(err)
	}
	got := []graphstate.EntityID{3}
	for range 3 {
		page, err = retained.IncidentRelationshipsChunk(t.Context(), p, token, b, 1)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, page.Entities...)
		token = page.Next
		if page.Complete {
			break
		}
	}
	if token != 0 || !slices.Equal(got, []graphstate.EntityID{3, 4, 5}) {
		t.Fatal("whole-run or retained-cut mismatch", got, page)
	}
	phantom, err := retained.IncidentRelationshipsChunk(t.Context(), graphstate.IncidentPredicate{Endpoint: 99, Window: whole}, 0, b, 1)
	if err != nil || !phantom.Complete || len(phantom.Entities) != 0 {
		t.Fatal("phantom endpoint", phantom, err)
	}
}
