package io_test

import (
	"bytes"
	"errors"
	stdio "io"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/grapherr"
	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/io"
)

type fakeOps struct {
	exportWriter            stdio.Writer
	importReader            stdio.Reader
	importWithOptionsReader stdio.Reader
	importOpts              tkgio.ImportOptions

	exportErr            error
	importErr            error
	importWithOptionsErr error

	exportCalled            bool
	importCalled            bool
	importWithOptionsCalled bool
	called                  bool
}

func (f *fakeOps) Export(w stdio.Writer) error {
	f.exportCalled = true
	f.exportWriter = w
	return f.exportErr
}

func (f *fakeOps) Import(r stdio.Reader) error {
	f.importCalled = true
	f.importReader = r
	return f.importErr
}

func (f *fakeOps) ImportWithOptions(r stdio.Reader, opts tkgio.ImportOptions) error {
	f.importWithOptionsCalled = true
	f.called = true
	f.importWithOptionsReader = r
	f.importOpts = opts
	return f.importWithOptionsErr
}

func TestAPINilReceiversReturnErrNilGraph(t *testing.T) {
	t.Parallel()

	var nilAPI *tkgio.API
	if err := nilAPI.Export(nil); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil Export = %v, want ErrNilGraph", err)
	}
	if err := nilAPI.Import(nil); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil Import = %v, want ErrNilGraph", err)
	}
	if err := nilAPI.ImportWithOptions(nil, tkgio.ImportOptions{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil ImportWithOptions = %v, want ErrNilGraph", err)
	}

	api := tkgio.New((*fakeOps)(nil))
	if err := api.Export(nil); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil Export = %v, want ErrNilGraph", err)
	}
	if err := api.Import(nil); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil Import = %v, want ErrNilGraph", err)
	}
	if err := api.ImportWithOptions(nil, tkgio.ImportOptions{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil ImportWithOptions = %v, want ErrNilGraph", err)
	}
}

func TestAPIForwardsMethodsAndErrors(t *testing.T) {
	t.Parallel()

	exportErr := errors.New("export failed")
	importErr := errors.New("import failed")
	importWithOptionsErr := errors.New("import with options failed")
	ops := &fakeOps{
		exportErr:            exportErr,
		importErr:            importErr,
		importWithOptionsErr: importWithOptionsErr,
	}
	api := tkgio.New(ops)

	writer := &bytes.Buffer{}
	reader := bytes.NewReader(nil)
	optionsReader := bytes.NewReader([]byte("records"))
	opts := tkgio.ImportOptions{
		StagingDir:     t.TempDir(),
		MaxStagedBytes: 1024,
	}

	if err := api.Export(writer); !errors.Is(err, exportErr) {
		t.Fatalf("Export error = %v, want %v", err, exportErr)
	}
	if err := api.Import(reader); !errors.Is(err, importErr) {
		t.Fatalf("Import error = %v, want %v", err, importErr)
	}
	if err := api.ImportWithOptions(optionsReader, opts); !errors.Is(err, importWithOptionsErr) {
		t.Fatalf("ImportWithOptions error = %v, want %v", err, importWithOptionsErr)
	}

	if !ops.exportCalled || !ops.importCalled || !ops.importWithOptionsCalled {
		t.Fatalf("call flags = export %v import %v importWithOptions %v", ops.exportCalled, ops.importCalled, ops.importWithOptionsCalled)
	}
	if ops.exportWriter != writer {
		t.Fatalf("Export writer = %p, want %p", ops.exportWriter, writer)
	}
	if ops.importReader != reader {
		t.Fatalf("Import reader = %p, want %p", ops.importReader, reader)
	}
	if ops.importWithOptionsReader != optionsReader {
		t.Fatalf("ImportWithOptions reader = %p, want %p", ops.importWithOptionsReader, optionsReader)
	}
	if ops.importOpts != opts {
		t.Fatalf("ImportWithOptions opts = %+v, want %+v", ops.importOpts, opts)
	}
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
