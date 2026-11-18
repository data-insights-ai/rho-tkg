package core

import (
	"bytes"
	"errors"
	"testing"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// The import header guard accepts every format version from
// exportFormatVersionMin to exportFormatVersionMax (v1 pre-SnapshotLSN, v2 full
// export, v3 delta, v4 literal valid axis) and rejects anything outside
// [min, max] — the backward-compat gate for the format bump. A too-new header must fail with ErrIncompatibleExport
// so an old build refuses a newer snapshot instead of silently mis-reading it.
// The v4 case carries UntimedWholeAxis, which that version requires.
func TestImport_HeaderVersionRange(t *testing.T) {
	t.Parallel()
	build := func(hdr exportHeader) []byte {
		var buf bytes.Buffer
		hdr.ExportedAt = 1
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
		hdr     exportHeader
		wantErr bool
	}{
		{exportHeader{Version: exportFormatVersionMin}, false},                            // v1 — backward-compat
		{exportHeader{Version: exportFormatVersion}, false},                               // v2 — current full export
		{exportHeader{Version: exportFormatVersionDelta}, false},                          // v3 — full stream at the delta version
		{exportHeader{Version: exportFormatVersionMax, UntimedWholeAxis: true}, false},    // v4 — max, literal axis
		{exportHeader{Version: exportFormatVersionMax + 1, UntimedWholeAxis: true}, true}, // too new
		{exportHeader{Version: exportFormatVersionMax + 1}, true},                         // too new
		{exportHeader{Version: 0}, true},                                                  // below min
	}
	for _, tc := range cases {
		dst := newTestGraph(t)
		err := dst.IO.Import(bytes.NewReader(build(tc.hdr)), tkgio.ImportOptions{})
		switch {
		case tc.wantErr && !errors.Is(err, ErrIncompatibleExport):
			t.Errorf("import v%d: err = %v, want ErrIncompatibleExport", tc.hdr.Version, err)
		case !tc.wantErr && err != nil:
			t.Errorf("import v%d: unexpected err %v", tc.hdr.Version, err)
		}
	}
}
