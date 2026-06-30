package core

import (
	"bytes"
	"errors"
	"testing"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// The import header guard accepts v1 (pre-SnapshotLSN), v2 (full export), and
// v3 (delta) formats and rejects anything outside [min, max] — the backward-compat
// gate for the format bump. A too-new header must fail with ErrIncompatibleExport
// so an old build refuses a newer snapshot instead of silently mis-reading it.
func TestImport_HeaderVersionRange(t *testing.T) {
	build := func(ver uint8) []byte {
		var buf bytes.Buffer
		hdr := exportHeader{Version: ver, ExportedAt: 1}
		if err := marshalAndWrite(&buf, exportTagHeader, &hdr); err != nil {
			t.Fatalf("marshal header: %v", err)
		}
		reg := tiered.RegistryFileData{Labels: []string{""}, RelTypes: []string{""}}
		if err := marshalAndWrite(&buf, exportTagRegistry, &reg); err != nil {
			t.Fatalf("marshal registry: %v", err)
		}
		return buf.Bytes()
	}

	cases := []struct {
		ver     uint8
		wantErr bool
	}{
		{exportFormatVersionMin, false},    // v1 — backward-compat
		{exportFormatVersion, false},       // v2 — current full export
		{exportFormatVersionMax + 1, true}, // too new
		{0, true},                          // below min
	}
	for _, tc := range cases {
		dst := newTestGraph(t)
		err := dst.IO.Import(bytes.NewReader(build(tc.ver)), tkgio.ImportOptions{})
		switch {
		case tc.wantErr && !errors.Is(err, ErrIncompatibleExport):
			t.Errorf("import v%d: err = %v, want ErrIncompatibleExport", tc.ver, err)
		case !tc.wantErr && err != nil:
			t.Errorf("import v%d: unexpected err %v", tc.ver, err)
		}
	}
}
