package tiered

import (
	"errors"
	"testing"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/registry"
)

func TestTieredStoreRegistryMethodsRejectNilRegistries(t *testing.T) {
	ts := newDiskTestTieredStore(t)
	labels := registrypkg.NewLabelRegistry()
	relTypes := registrypkg.NewRelTypeRegistry()

	cases := []struct {
		name string
		run  func() error
	}{
		{name: "SaveRegistriesNilLabels", run: func() error { return ts.SaveRegistries(nil, relTypes) }},
		{name: "SaveRegistriesNilRelTypes", run: func() error { return ts.SaveRegistries(labels, nil) }},
		{name: "SaveLabelRegistry", run: func() error { return ts.SaveLabelRegistry(nil) }},
		{name: "LoadLabelRegistry", run: func() error {
			_, err := ts.LoadLabelRegistry(nil)
			return err
		}},
		{name: "SaveRelTypeRegistry", run: func() error { return ts.SaveRelTypeRegistry(nil) }},
		{name: "LoadRelTypeRegistry", run: func() error {
			_, err := ts.LoadRelTypeRegistry(nil)
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("%s error = %v, want ErrInvalidStoreMutation", tc.name, err)
			}
		})
	}
}

func TestTieredStoreRegistryNilChecksPreserveClosedPrecedence(t *testing.T) {
	ts := newDiskTestTieredStore(t)
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := ts.SaveLabelRegistry(nil); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("SaveLabelRegistry(nil) after close = %v, want ErrStoreClosed", err)
	}
	if _, err := ts.LoadRelTypeRegistry(nil); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("LoadRelTypeRegistry(nil) after close = %v, want ErrStoreClosed", err)
	}
}

func TestTieredStoreSetLabelRegistryIgnoresNil(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	if _, err := reg.GetOrCreate("Case"); err != nil {
		t.Fatalf("GetOrCreate Case: %v", err)
	}
	userTok, err := reg.GetOrCreate("User")
	if err != nil {
		t.Fatalf("GetOrCreate User: %v", err)
	}

	ts.SetLabelRegistry(reg)
	ts.SetLabelRegistry(nil)

	if got := ts.ontology.ClassifyByToken(userTok); got != ClassReference {
		t.Fatalf("ClassifyByToken after SetLabelRegistry(nil) = %v, want ClassReference", got)
	}
}
