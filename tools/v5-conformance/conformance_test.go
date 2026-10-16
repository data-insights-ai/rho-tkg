package conformance_test

import (
	"os"
	"strings"
	"testing"

	conformance "github.com/data-insights-ai/rho-tkg/tools/v5-conformance"
	"github.com/data-insights-ai/rho-tkg/tools/v5-conformance/internal/mutant"
	"github.com/data-insights-ai/rho-tkg/tools/v5-conformance/internal/v4adapter"
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
