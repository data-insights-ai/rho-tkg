package conformance_test

import (
	"os"
	"testing"

	conformance "github.com/data-insights-ai/rho-tkg/tools/v5-conformance"
	"github.com/data-insights-ai/rho-tkg/tools/v5-conformance/internal/v4adapter"
)

// noStampAdapter is a v4 engine that refuses caller-chosen transaction instants
// at open (Options.AllowTxBackfill forced off), the way a native v5 adapter
// would. The native profile must pass on it; the compat profile cannot, which
// is why an adapter declares the profiles it serves.
type noStampAdapter struct{ profiles []conformance.Profile }

func (a noStampAdapter) Open(t testing.TB, o conformance.Options) (conformance.Graph, error) {
	o.AllowTxBackfill = false
	return v4adapter.Adapter{}.Open(t, o)
}

func (a noStampAdapter) Profiles() []conformance.Profile { return a.profiles }

func TestNativeProfileNeedsNoCallerStamps(t *testing.T) {
	served := []conformance.Profile{conformance.Native}
	if os.Getenv("CONFORMANCE_CLAIM_COMPAT") != "" { // demonstrates the red: claiming compat fails
		served = append(served, conformance.Compat)
	}
	conformance.Run(t, func(*testing.T) conformance.Adapter { return noStampAdapter{served} })
}
