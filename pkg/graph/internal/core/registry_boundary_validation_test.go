package core

import (
	"bytes"
	"errors"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

func TestImportRejectsRegistryNamesOverGraphLimit(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		reg  tiered.RegistryFileData
	}{
		{
			name: "label",
			reg: tiered.RegistryFileData{
				Labels:   []string{"", "TooLong"},
				RelTypes: []string{"", "Rel"},
			},
		},
		{
			name: "reltype",
			reg: tiered.RegistryFileData{
				Labels:   []string{"", "Label"},
				RelTypes: []string{"", "TooLong"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			g, err := New(Config{Validation: ValidationLimits{MaxNameLength: 5}})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })

			var stream bytes.Buffer
			writeImportMsgpackRecord(t, &stream, exportTagHeader, exportHeader{Version: exportFormatVersion})
			writeImportMsgpackRecord(t, &stream, exportTagRegistry, tc.reg)

			err = g.IO.Import(bytes.NewReader(stream.Bytes()), tkgio.ImportOptions{})
			if !errors.Is(err, ErrNameTooLong) {
				t.Fatalf("Import = %v, want ErrNameTooLong", err)
			}
			if tok, ok := g.Resolve.LookupLabel("TooLong"); ok || tok != 0 {
				t.Fatalf("LookupLabel(TooLong) = %d, %v; want zero, false", tok, ok)
			}
			if tok, ok := g.Resolve.LookupRelType("TooLong"); ok || tok != 0 {
				t.Fatalf("LookupRelType(TooLong) = %d, %v; want zero, false", tok, ok)
			}
		})
	}
}

func TestNewRejectsPersistedRegistryNamesOverGraphLimit(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		label   string
		relType string
	}{
		{name: "label", label: "TooLong", relType: "Rel"},
		{name: "reltype", label: "Label", relType: "TooLong"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			bs, err := badger.New(badger.Config{Dir: dir})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			labels := registrypkg.NewLabelRegistry()
			if _, err := labels.GetOrCreate(tc.label); err != nil {
				t.Fatalf("GetOrCreate label: %v", err)
			}
			relTypes := registrypkg.NewRelTypeRegistry()
			if _, err := relTypes.GetOrCreate(tc.relType); err != nil {
				t.Fatalf("GetOrCreate reltype: %v", err)
			}
			if err := bs.SaveRegistries(labels, relTypes); err != nil {
				t.Fatalf("SaveRegistries: %v", err)
			}
			if err := bs.Close(); err != nil {
				t.Fatalf("Close badger: %v", err)
			}

			_, err = New(Config{
				BadgerDir:  dir,
				Validation: ValidationLimits{MaxNameLength: 5},
			})
			if !errors.Is(err, ErrNameTooLong) {
				t.Fatalf("New = %v, want ErrNameTooLong", err)
			}
		})
	}
}
