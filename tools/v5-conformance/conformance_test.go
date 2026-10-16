package conformance_test

import (
	"os"
	"strings"
	"testing"

	conformance "github.com/data-insights-ai/rho-tkg/tools/v5-conformance"
	"github.com/data-insights-ai/rho-tkg/tools/v5-conformance/internal/mutant"
	"github.com/data-insights-ai/rho-tkg/tools/v5-conformance/internal/v4adapter"
	"github.com/data-insights-ai/rho-tkg/tools/v5-conformance/internal/v5adapter"
)

// TestV4 runs the suite against rho-tkg v4.56.1. Setting CONFORMANCE_MUTANT to a
// comma-separated list of mutant names (or "all") runs it against a deliberately
// broken adapter instead, to show that the suite fails when a door is wrong.
func TestV4(t *testing.T) {
	spec := os.Getenv("CONFORMANCE_MUTANT")
	conformance.Run(t, func(t *testing.T) conformance.Adapter {
		var base conformance.Adapter = v4adapter.Adapter{}
		if spec == "" {
			return base
		}
		return mutant.Wrap(base, strings.Split(spec, ","))
	})
}

// TestV5 runs the suite against v5/pkg/graph. While the package has no engine
// every case skips with the not-implemented reason; once Open succeeds the
// cases run for real.
func TestV5(t *testing.T) {
	conformance.Run(t, func(*testing.T) conformance.Adapter { return v5adapter.Adapter{} })
}
