package core

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestTypedBoundsNilMutationDoorsRefuse(t *testing.T) {
	r := types.TemporalRange{From: types.TemporalEndpoint{Value: 0, Finite: true}}
	for _, ops := range []*TempOps{nil, {}} {
		if _, err := ops.SetNodeVersionBounds(t.Context(), 1, r, nil); !errors.Is(err, ErrNilGraph) {
			t.Fatal(err)
		}
		if _, err := ops.SetRelVersionBounds(t.Context(), 1, r, nil); !errors.Is(err, ErrNilGraph) {
			t.Fatal(err)
		}
	}
	var b *BatchBuilder
	if err := b.SetNodeVersionBounds(1, r, nil); !errors.Is(err, ErrNilGraph) {
		t.Fatal(err)
	}
	if err := b.SetRelVersionBounds(1, r, nil); !errors.Is(err, ErrNilGraph) {
		t.Fatal(err)
	}
	var s *Session
	if err := s.SetNodeVersionBounds(1, r, nil); !errors.Is(err, ErrNilSession) {
		t.Fatal(err)
	}
	if err := s.SetRelVersionBounds(1, r, nil); !errors.Is(err, ErrNilSession) {
		t.Fatal(err)
	}
}
