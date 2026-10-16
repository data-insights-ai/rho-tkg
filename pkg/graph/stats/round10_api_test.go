package stats

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func (s *statsOpsSpy) PropertyTypeClassCountsAt(label, key string, pin types.Instant) (storepkg.PropertyTypeClassCounts, bool, error) {
	return storepkg.PropertyTypeClassCounts{Numeric: int64(pin), NaN: 1}, label == "L" && key == "k", s.propertyStatsErr
}

func (s *statsOpsSpy) RelPropertyTypeClassCountsAt(typeName, key string, pin types.Instant) (storepkg.PropertyTypeClassCounts, bool, error) {
	return storepkg.PropertyTypeClassCounts{Other: int64(pin)}, typeName == "R" && key == "k", s.propertyStatsErr
}

// TestPropertyTypeClassCountsAtForwards: the round 10 value classes forward
// their arguments, answer and error; a nil API reports the nil graph.
func TestPropertyTypeClassCountsAtForwards(t *testing.T) {
	var nilAPI *API
	if _, _, err := nilAPI.PropertyTypeClassCountsAt("L", "k", 7); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil PropertyTypeClassCountsAt error = %v", err)
	}
	if _, _, err := nilAPI.RelPropertyTypeClassCountsAt("R", "k", 7); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil RelPropertyTypeClassCountsAt error = %v", err)
	}
	wantErr := errors.New("ops failed")
	ops := &statsOpsSpy{propertyStatsErr: wantErr}
	if c, ok, err := New(ops).PropertyTypeClassCountsAt("L", "k", 7); !errors.Is(err, wantErr) || !ok || c.Numeric != 7 || c.NaN != 1 {
		t.Fatalf("PropertyTypeClassCountsAt = %+v, %v, %v", c, ok, err)
	}
	if c, ok, err := New(ops).RelPropertyTypeClassCountsAt("R", "k", 8); !errors.Is(err, wantErr) || !ok || c.Other != 8 {
		t.Fatalf("RelPropertyTypeClassCountsAt = %+v, %v, %v", c, ok, err)
	}
}
