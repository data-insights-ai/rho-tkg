package io_test

import (
	"bytes"
	stdio "io"
	"testing"

	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/io"
)

type fakeOps struct {
	importOpts tkgio.ImportOptions
	called     bool
}

func (f *fakeOps) Export(w stdio.Writer) error { return nil }

func (f *fakeOps) Import(r stdio.Reader) error { return nil }

func (f *fakeOps) ImportWithOptions(r stdio.Reader, opts tkgio.ImportOptions) error {
	f.called = true
	f.importOpts = opts
	return nil
}

func TestAPIImportWithOptionsForwardsOptions(t *testing.T) {
	ops := &fakeOps{}
	api := tkgio.New(ops)

	opts := tkgio.ImportOptions{
		StagingDir:     t.TempDir(),
		MaxStagedBytes: 4096,
	}
	if err := api.ImportWithOptions(bytes.NewReader(nil), opts); err != nil {
		t.Fatalf("ImportWithOptions: %v", err)
	}
	if !ops.called {
		t.Fatal("ImportWithOptions did not call underlying ops")
	}
	if ops.importOpts != opts {
		t.Fatalf("forwarded opts = %+v, want %+v", ops.importOpts, opts)
	}
}
