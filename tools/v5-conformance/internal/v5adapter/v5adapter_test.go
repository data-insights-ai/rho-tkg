package v5adapter

import (
	"testing"

	conformance "github.com/data-insights-ai/rho-tkg/tools/v5-conformance"
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

func TestOpenAndCloseBothModes(t *testing.T) {
	for _, o := range []conformance.Options{{}, {Dir: t.TempDir()}} {
		g, err := Adapter{}.Open(t, o)
		if err != nil {
			t.Fatalf("%+v: %v", o, err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProfilesNativeOnly(t *testing.T) {
	p := Adapter{}.Profiles()
	if len(p) != 1 || p[0] != conformance.Native {
		t.Fatalf("profiles = %v, want [native]", p)
	}
}
