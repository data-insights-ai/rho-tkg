package badger

import (
	"errors"
	"testing"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
)

func TestBadgerStoreRegistryMethodsRejectNilRegistries(t *testing.T) {
	bs := newTestBadgerStore(t)
	labels := registrypkg.NewLabelRegistry()
	relTypes := registrypkg.NewRelTypeRegistry()

	cases := []struct {
		name string
		run  func() error
	}{
		{name: "SaveRegistriesNilLabels", run: func() error { return bs.SaveRegistries(nil, relTypes) }},
		{name: "SaveRegistriesNilRelTypes", run: func() error { return bs.SaveRegistries(labels, nil) }},
		{name: "SaveLabelRegistry", run: func() error { return bs.SaveLabelRegistry(nil) }},
		{name: "LoadLabelRegistry", run: func() error {
			_, err := bs.LoadLabelRegistry(nil)
			return err
		}},
		{name: "SaveRelTypeRegistry", run: func() error { return bs.SaveRelTypeRegistry(nil) }},
		{name: "LoadRelTypeRegistry", run: func() error {
			_, err := bs.LoadRelTypeRegistry(nil)
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

func TestBadgerStoreRegistryNilChecksPreserveClosedPrecedence(t *testing.T) {
	bs := newTestBadgerStore(t)
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := bs.SaveLabelRegistry(nil); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("SaveLabelRegistry(nil) after close = %v, want ErrStoreClosed", err)
	}
	if _, err := bs.LoadRelTypeRegistry(nil); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("LoadRelTypeRegistry(nil) after close = %v, want ErrStoreClosed", err)
	}
}
