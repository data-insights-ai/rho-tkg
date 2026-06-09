package io_test

import (
	"bytes"
	"errors"
	stdio "io"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
)

type fakeOps struct {
	exportWriter            stdio.Writer
	importWithOptionsReader stdio.Reader
	importOpts              tkgio.ImportOptions

	exportErr            error
	importWithOptionsErr error

	exportCalled            bool
	importWithOptionsCalled bool
	called                  bool
}

func (f *fakeOps) Export(w stdio.Writer) error {
	f.exportCalled = true
	f.exportWriter = w
	return f.exportErr
}

func (f *fakeOps) Import(r stdio.Reader, opts tkgio.ImportOptions) error {
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
	if err := nilAPI.Import(nil, tkgio.ImportOptions{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil Import = %v, want ErrNilGraph", err)
	}

	api := tkgio.New((*fakeOps)(nil))
	if err := api.Export(nil); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil Export = %v, want ErrNilGraph", err)
	}
	if err := api.Import(nil, tkgio.ImportOptions{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil Import = %v, want ErrNilGraph", err)
	}
}

func TestAPIForwardsMethodsAndErrors(t *testing.T) {
	t.Parallel()

	exportErr := errors.New("export failed")
	importErr := errors.New("import failed")
	ops := &fakeOps{
		exportErr:            exportErr,
		importWithOptionsErr: importErr,
	}
	api := tkgio.New(ops)

	writer := &bytes.Buffer{}
	reader := bytes.NewReader([]byte("records"))
	opts := tkgio.ImportOptions{
		StagingDir:     t.TempDir(),
		MaxStagedBytes: 1024,
	}

	if err := api.Export(writer); !errors.Is(err, exportErr) {
		t.Fatalf("Export error = %v, want %v", err, exportErr)
	}
	if err := api.Import(reader, opts); !errors.Is(err, importErr) {
		t.Fatalf("Import error = %v, want %v", err, importErr)
	}

	if !ops.exportCalled || !ops.importWithOptionsCalled {
		t.Fatalf("call flags = export %v import %v", ops.exportCalled, ops.importWithOptionsCalled)
	}
	if ops.exportWriter != writer {
		t.Fatalf("Export writer = %p, want %p", ops.exportWriter, writer)
	}
	if ops.importWithOptionsReader != reader {
		t.Fatalf("Import reader = %p, want %p", ops.importWithOptionsReader, reader)
	}
	if ops.importOpts != opts {
		t.Fatalf("Import opts = %+v, want %+v", ops.importOpts, opts)
	}
}

func TestAPIImportForwardsOptions(t *testing.T) {
	ops := &fakeOps{}
	api := tkgio.New(ops)

	opts := tkgio.ImportOptions{
		StagingDir:     t.TempDir(),
		MaxStagedBytes: 4096,
	}
	if err := api.Import(bytes.NewReader(nil), opts); err != nil {
		t.Fatalf("Import: %v", err)
	}
	if !ops.called {
		t.Fatal("Import did not call underlying ops")
	}
	if ops.importOpts != opts {
		t.Fatalf("forwarded opts = %+v, want %+v", ops.importOpts, opts)
	}
}
