package v5adapter

import (
	"errors"
	"testing"

	conformance "github.com/data-insights-ai/rho-tkg/tools/v5-conformance"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
)

func TestConfigValidForBothModes(t *testing.T) {
	for _, o := range []conformance.Options{{}, {Dir: t.TempDir()}} {
		c, err := Config(t, o)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Validate(); err != nil {
			t.Fatalf("%+v: %v", o, err)
		}
	}
}

func TestOpenReportsBothSentinels(t *testing.T) {
	_, err := Adapter{}.Open(t, conformance.Options{})
	if !errors.Is(err, graph.ErrNotImplemented) || !errors.Is(err, conformance.ErrNotImplemented) {
		t.Fatal(err)
	}
}

func TestProfilesNativeOnly(t *testing.T) {
	p := Adapter{}.Profiles()
	if len(p) != 1 || p[0] != conformance.Native {
		t.Fatalf("profiles = %v, want [native]", p)
	}
}
