package compatadapter

import (
	"testing"

	conformance "github.com/data-insights-ai/rho-tkg/tools/v5-conformance"
)

// TestCompat runs the whole suite, native and compat profile, through v5compat.
func TestCompat(t *testing.T) {
	conformance.Run(t, func(*testing.T) conformance.Adapter { return Adapter{} })
}
