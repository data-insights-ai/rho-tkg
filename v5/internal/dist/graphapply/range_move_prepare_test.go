package graphapply

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
)

func TestRangeMovePrepareNativeOriginalSurvivesPlacementAndRejectsChangedInputs(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, c, original, _, _ := rangeMoveCoordinatorFixture(t, 64)
	h := n.hosts[0][0]
	intent, err := h.PrepareRangeMove(t.Context(), original.id, original.publication.RangeID, 8, original.maxPages, original.pageRows, original.pageBytes)
	if err != nil || intent != original {
		t.Fatal("fresh native preparation changed actual publication/binding", intent, err)
	}
	done, err := runRangeMoveToCompletion(t, c, intent)
	if err != nil || !done.committed {
		t.Fatal(done, err)
	}
	crossReopenAll(t, n)
	h = n.hosts[0][0]
	recovered, err := h.PrepareRangeMove(t.Context(), intent.id, intent.publication.RangeID, 8, intent.maxPages, intent.pageRows, intent.pageBytes)
	if err != nil || recovered != intent {
		t.Fatal("original recovery depended on current owner/source issuer", recovered, err)
	}
	if _, err := h.PrepareRangeMove(t.Context(), intent.id, intent.publication.RangeID, 8, intent.maxPages, intent.pageRows, intent.pageBytes+1); !errors.Is(err, errInvalid) {
		t.Fatal("changed retry reinterpreted original", err)
	}
	if _, err := h.PrepareRangeMove(t.Context(), n.requestID(), intent.publication.RangeID, 8, 64, 64, 8192); !errors.Is(err, graphstore.ErrStaleOwner) {
		t.Fatal("fresh original-source preparation admitted moved range", err)
	}
}

func TestRangeMovePrepareNativeBoundaryRefusalsDoNotAcquireOwners(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, intent, _, _ := rangeMoveCoordinatorFixture(t, 64)
	h := n.hosts[0][0]
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	cases := []struct {
		name               string
		host               *allocationHost
		ctx                context.Context
		id                 requestID
		rangeID, target    uint64
		pages, rows, bytes int
		cause              error
	}{
		{"nil receiver", nil, t.Context(), intent.id, 1, 8, 64, 64, 8192, errInvalid},
		{"nil context", h, nil, intent.id, 1, 8, 64, 64, 8192, errInvalid},
		{"cancelled", h, cancelled, intent.id, 1, 8, 64, 64, 8192, context.Canceled},
		{"zero request", h, t.Context(), requestID{}, 1, 8, 64, 64, 8192, errInvalid},
		{"zero range", h, t.Context(), intent.id, 0, 8, 64, 64, 8192, errInvalid},
		{"zero target", h, t.Context(), intent.id, 1, 0, 64, 64, 8192, errInvalid},
		{"self target", h, t.Context(), intent.id, 1, 3, 64, 64, 8192, errInvalid},
		{"unknown target", h, t.Context(), intent.id, 1, 999, 64, 64, 8192, errInvalid},
		{"interior ID is not RangeID", h, t.Context(), intent.id, 7, 8, 64, 64, 8192, graphstore.ErrStaleOwner},
		{"unknown range", h, t.Context(), intent.id, 33, 8, 64, 64, 8192, graphstore.ErrRoutingUnknown},
		{"zero pages", h, t.Context(), intent.id, 1, 8, 0, 64, 8192, errInvalid},
		{"oversize pages", h, t.Context(), intent.id, 1, 8, 1025, 64, 8192, errInvalid},
		{"zero rows", h, t.Context(), intent.id, 1, 8, 64, 0, 8192, errInvalid},
		{"zero bytes", h, t.Context(), intent.id, 1, 8, 64, 64, 0, errInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := tc.host.PrepareRangeMove(tc.ctx, tc.id, tc.rangeID, tc.target, tc.pages, tc.rows, tc.bytes)
			if actual != (rangeMoveIntent{}) || !errors.Is(err, tc.cause) {
				t.Fatal("native refusal returned partial intent/lost sentinel", actual, err)
			}
		})
	}
	if owner := rangeMoveAdmissionOwner(t, h); owner.ID != ([32]byte{}) {
		t.Fatal("refused prepare acquired recovery authority", owner)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.PrepareRangeMove(t.Context(), intent.id, 1, 8, 64, 64, 8192); !errors.Is(err, replica.ErrStopped) {
		t.Fatal("closed host", err)
	}
}
