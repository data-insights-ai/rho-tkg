package core

import (
	"context"
	"bytes"
	"errors"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/tiered"
	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/io"
)

func TestR5_Import_MalformedRegistryRecordsAreCorruptExport(t *testing.T) {
	t.Parallel()

	src := newTestGraph(t)
	defer src.Close()
	a, err := src.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := src.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.Rels.Add(context.Background(), "KNOWS", a, b, nil); err != nil {
		t.Fatal(err)
	}

	var stream bytes.Buffer
	if err := src.IO.Export(&stream); err != nil {
		t.Fatalf("Export: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*tiered.RegistryFileData)
	}{
		{
			name: "label reserved slot nonempty",
			mutate: func(reg *tiered.RegistryFileData) {
				reg.Labels[0] = "reserved"
			},
		},
		{
			name: "label duplicate name",
			mutate: func(reg *tiered.RegistryFileData) {
				reg.Labels = append(reg.Labels, reg.Labels[1])
			},
		},
		{
			name: "reltype duplicate name",
			mutate: func(reg *tiered.RegistryFileData) {
				reg.RelTypes = append(reg.RelTypes, reg.RelTypes[1])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			corrupt := patchRegistryRecord(t, stream.Bytes(), tc.mutate)
			dst := newTestGraph(t)
			defer dst.Close()

			err := dst.IO.Import(bytes.NewReader(corrupt), tkgio.ImportOptions{})
			if !errors.Is(err, ErrCorruptExport) {
				t.Fatalf("Import: got %v, want errors.Is ErrCorruptExport", err)
			}
		})
	}
}

func patchRegistryRecord(t *testing.T, full []byte, mutate func(*tiered.RegistryFileData)) []byte {
	t.Helper()
	return patchFirstRecord(t, full, exportTagRegistry, func(body []byte) []byte {
		var reg tiered.RegistryFileData
		if err := msgpack.Unmarshal(body, &reg); err != nil {
			t.Fatalf("unmarshal registry wire: %v", err)
		}
		mutate(&reg)
		out, err := msgpack.Marshal(&reg)
		if err != nil {
			t.Fatalf("marshal registry wire: %v", err)
		}
		return out
	})
}
