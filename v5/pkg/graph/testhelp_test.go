package graph

import "context"

// testCut and testReceipt build valid opaque tokens; only the future engine
// mints real ones, so tests use the package-private constructors.
func testCut() Cut         { return Cut{tok: []byte{1}} }
func testReceipt() Receipt { return Receipt{tok: []byte{1}} }

// stubSnap and stubEngine are doors that validate nothing and answer
// ErrNotImplemented, so tests can prove the public wrappers validate first.
type stubSnap struct{}

func (stubSnap) scan(context.Context, ScanRequest) (Page, error) { return Page{}, ErrNotImplemented }
func (stubSnap) resume(context.Context, Cursor, PageLimits) (Page, error) {
	return Page{}, ErrNotImplemented
}
func (stubSnap) lookup(context.Context, LookupRequest) (Page, error) {
	return Page{}, ErrNotImplemented
}
func (stubSnap) timeline(context.Context, TimelineRequest) (Page, error) {
	return Page{}, ErrNotImplemented
}
func (stubSnap) cut() Cut     { return Cut{} }
func (stubSnap) close() error { return nil }

type stubEngine struct{}

func (stubEngine) update(context.Context, func(Tx) error) (Receipt, error) {
	return Receipt{}, ErrNotImplemented
}
func (stubEngine) openSnapshot(context.Context, SnapshotSpec) (snapshotEngine, error) {
	return nil, ErrNotImplemented
}
func (stubEngine) close() error { return nil }

func (stubSnap) meta(context.Context, string) ([]byte, bool, error) {
	return nil, false, ErrNotImplemented
}

func (stubEngine) declareUnique(context.Context, string, ValueKind) (Receipt, error) {
	return Receipt{}, ErrNotImplemented
}
