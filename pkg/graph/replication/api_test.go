package replication_test

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/replication"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// fakeOps is a minimal Ops stand-in so the wrapper's forwarding and nil-guard
// behaviour can be tested directly (the end-to-end wiring is covered in the
// graph package).
type fakeOps struct {
	feed       []store.ChangeRecord
	lastLSN    uint64
	err        error
	forEach    int
	applied    []store.ChangeRecord
	appliedLSN uint64
}

func (f *fakeOps) ChangeFeed(afterLSN uint64, limit int) ([]store.ChangeRecord, error) {
	return f.feed, f.err
}

func (f *fakeOps) ForEachChange(afterLSN uint64, fn func(store.ChangeRecord) bool) error {
	if f.err != nil {
		return f.err
	}
	for _, r := range f.feed {
		f.forEach++
		if !fn(r) {
			break
		}
	}
	return nil
}

func (f *fakeOps) LastCommittedLSN() (uint64, error) { return f.lastLSN, f.err }

func (f *fakeOps) ApplyChange(rec store.ChangeRecord) error {
	if f.err != nil {
		return f.err
	}
	f.applied = append(f.applied, rec)
	f.appliedLSN = rec.LSN
	return nil
}

func (f *fakeOps) ApplyChanges(recs []store.ChangeRecord) (uint64, error) {
	var last uint64
	for _, r := range recs {
		if err := f.ApplyChange(r); err != nil {
			return last, err
		}
		last = r.LSN
	}
	return last, nil
}

func (f *fakeOps) AppliedLSN() (uint64, error)    { return f.appliedLSN, f.err }
func (f *fakeOps) SetAppliedLSN(lsn uint64) error { f.appliedLSN = lsn; return f.err }

func TestAPI_ForwardsToOps(t *testing.T) {
	ops := &fakeOps{
		feed:    []store.ChangeRecord{{LSN: 1, Tag: store.ChangeNodePut}, {LSN: 2, Tag: store.ChangeRelPut}},
		lastLSN: 2,
	}
	api := replication.New(ops)

	feed, err := api.ChangeFeed(0, 0)
	if err != nil || len(feed) != 2 {
		t.Fatalf("ChangeFeed = (%v, %v), want 2 records", feed, err)
	}
	lsn, err := api.LastCommittedLSN()
	if err != nil || lsn != 2 {
		t.Fatalf("LastCommittedLSN = (%d, %v), want (2, nil)", lsn, err)
	}
	var n int
	if err := api.ForEachChange(0, func(store.ChangeRecord) bool { n++; return true }); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if n != 2 {
		t.Fatalf("ForEachChange visited %d, want 2", n)
	}

	if err := api.ApplyChange(store.ChangeRecord{LSN: 7, Tag: store.ChangeNodePut}); err != nil {
		t.Fatalf("ApplyChange: %v", err)
	}
	last, err := api.ApplyChanges([]store.ChangeRecord{{LSN: 8}, {LSN: 9}})
	if err != nil || last != 9 {
		t.Fatalf("ApplyChanges = (%d, %v), want (9, nil)", last, err)
	}
	if got, _ := api.AppliedLSN(); got != 9 {
		t.Fatalf("AppliedLSN = %d, want 9", got)
	}
	if err := api.SetAppliedLSN(42); err != nil {
		t.Fatalf("SetAppliedLSN: %v", err)
	}
	if got, _ := api.AppliedLSN(); got != 42 {
		t.Fatalf("AppliedLSN after Set = %d, want 42", got)
	}
}

func TestAPI_PropagatesOpsError(t *testing.T) {
	sentinel := errors.New("boom")
	api := replication.New(&fakeOps{err: sentinel})
	if _, err := api.LastCommittedLSN(); !errors.Is(err, sentinel) {
		t.Fatalf("LastCommittedLSN err = %v, want boom", err)
	}
	if _, err := api.ChangeFeed(0, 0); !errors.Is(err, sentinel) {
		t.Fatalf("ChangeFeed err = %v, want boom", err)
	}
}

func TestAPI_NilAndZeroValueFailClosed(t *testing.T) {
	// New(nil) yields a not-ready API; methods return ErrNilGraph.
	api := replication.New(nil)
	if _, err := api.LastCommittedLSN(); err == nil {
		t.Fatalf("New(nil).LastCommittedLSN = nil err, want ErrNilGraph")
	}
	// A nil *API must not panic.
	var nilAPI *replication.API
	if _, err := nilAPI.ChangeFeed(0, 0); err == nil {
		t.Fatalf("nil API ChangeFeed = nil err, want ErrNilGraph")
	}
	if err := nilAPI.ForEachChange(0, func(store.ChangeRecord) bool { return true }); err == nil {
		t.Fatalf("nil API ForEachChange = nil err, want ErrNilGraph")
	}
}
