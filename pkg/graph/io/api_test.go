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

	exportSinceWriter stdio.Writer
	exportSinceCursor tkgio.Cursor
	importMergeReader stdio.Reader
	importMergeOpts   tkgio.MergeOptions
	watermarkCursor   tkgio.Cursor

	exportErr            error
	importWithOptionsErr error
	exportSinceErr       error
	importMergeErr       error
	watermarkErr         error

	exportCalled            bool
	importWithOptionsCalled bool
	called                  bool
	exportSinceCalled       bool
	importMergeCalled       bool
	watermarkCalled         bool
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

func (f *fakeOps) Watermark() (tkgio.Cursor, error) {
	f.watermarkCalled = true
	return f.watermarkCursor, f.watermarkErr
}

func (f *fakeOps) ExportSince(w stdio.Writer, since tkgio.Cursor) error {
	f.exportSinceCalled = true
	f.exportSinceWriter = w
	f.exportSinceCursor = since
	return f.exportSinceErr
}

func (f *fakeOps) ImportMerge(r stdio.Reader, opts tkgio.MergeOptions) error {
	f.importMergeCalled = true
	f.importMergeReader = r
	f.importMergeOpts = opts
	return f.importMergeErr
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

	if _, err := nilAPI.Watermark(); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil Watermark = %v, want ErrNilGraph", err)
	}
	if err := nilAPI.ExportSince(nil, tkgio.Cursor{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil ExportSince = %v, want ErrNilGraph", err)
	}
	if err := nilAPI.ImportMerge(nil, tkgio.MergeOptions{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil ImportMerge = %v, want ErrNilGraph", err)
	}

	api := tkgio.New((*fakeOps)(nil))
	if err := api.Export(nil); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil Export = %v, want ErrNilGraph", err)
	}
	if err := api.Import(nil, tkgio.ImportOptions{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil Import = %v, want ErrNilGraph", err)
	}
	if _, err := api.Watermark(); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil Watermark = %v, want ErrNilGraph", err)
	}
	if err := api.ExportSince(nil, tkgio.Cursor{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil ExportSince = %v, want ErrNilGraph", err)
	}
	if err := api.ImportMerge(nil, tkgio.MergeOptions{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil ImportMerge = %v, want ErrNilGraph", err)
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

func TestAPIForwardsDeltaMethods(t *testing.T) {
	t.Parallel()

	wantCursor := tkgio.Cursor{LSN: 42, Epoch: 7}
	exportSinceErr := errors.New("export since failed")
	importMergeErr := errors.New("import merge failed")
	ops := &fakeOps{
		watermarkCursor: wantCursor,
		exportSinceErr:  exportSinceErr,
		importMergeErr:  importMergeErr,
	}
	api := tkgio.New(ops)

	gotCursor, err := api.Watermark()
	if err != nil {
		t.Fatalf("Watermark err = %v", err)
	}
	if gotCursor != wantCursor {
		t.Fatalf("Watermark cursor = %+v, want %+v", gotCursor, wantCursor)
	}
	if !ops.watermarkCalled {
		t.Fatal("Watermark did not call underlying ops")
	}

	writer := &bytes.Buffer{}
	since := tkgio.Cursor{LSN: 10, Epoch: 7}
	if err := api.ExportSince(writer, since); !errors.Is(err, exportSinceErr) {
		t.Fatalf("ExportSince error = %v, want %v", err, exportSinceErr)
	}
	if !ops.exportSinceCalled || ops.exportSinceWriter != writer || ops.exportSinceCursor != since {
		t.Fatalf("ExportSince forwarding wrong: called=%v cursor=%+v", ops.exportSinceCalled, ops.exportSinceCursor)
	}

	reader := bytes.NewReader([]byte("delta"))
	mopts := tkgio.MergeOptions{StagingDir: t.TempDir(), MaxStagedBytes: 2048, ExpectBase: since, Strict: true}
	if err := api.ImportMerge(reader, mopts); !errors.Is(err, importMergeErr) {
		t.Fatalf("ImportMerge error = %v, want %v", err, importMergeErr)
	}
	if !ops.importMergeCalled || ops.importMergeReader != reader || ops.importMergeOpts != mopts {
		t.Fatalf("ImportMerge forwarding wrong: called=%v opts=%+v", ops.importMergeCalled, ops.importMergeOpts)
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
