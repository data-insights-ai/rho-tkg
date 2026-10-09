package raftlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func preflightFixture() (ApplicationPolicy, ApplicationBatch) {
	p := DefaultApplicationPolicy(1)
	p.MaxKeyBytes, p.MaxValueBytes = 2, 3
	p.MaxImageBytes, p.MaxChangeBytes, p.MaxOutcomeBytes = 4, 3, 2
	p.MaxPageBytes, p.MaxInstallWrites, p.MaxInstallBytes = 100, 5, 2128
	return p, ApplicationBatch{BaseIndex: 1, BaseImageHash: sha256.Sum256(nil), Image: []byte("root"), Changes: []byte("cdc"), Outcome: []byte("ok"), Writes: []KV{
		{Key: []byte{0, 0}}, {Key: []byte("a"), Value: nil}, {Key: []byte("b"), Value: []byte{}}, {Key: []byte("c"), Deleted: true}, {Key: []byte("d"), Value: []byte("xyz")},
	}}
}
func openPreflightStore(t *testing.T, p ApplicationPolicy) *Store {
	t.Helper()
	s, err := Open(Config{Dir: "preflight", FS: vfs.NewMem(), Create: true, Limits: DefaultLimits(), Application: p})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Initialize([]uint64{1}, nil); err != nil {
		t.Fatal(err)
	}
	return s
}
func TestApplicationPolicyPreflightExactGrowthAndInstall(t *testing.T) {
	p, b := preflightFixture()
	// Independent fixed wire oracle: root/CDC/outcome =
	// (9+36+4)+(9+36+3)+(9+36+2) = 144 bytes.
	// NUL key: tag 1 + escaped bytes 4 + terminator 2 + index 8 + frame 36 = 51.
	// a/b/c each 1+1+2+8+36 = 48 (nil/empty-present/tombstone); d = 48+3 = 51.
	// Total 144+51+48+48+48+51 = 390, eight records.
	want := ApplicationBatchUsage{390, 8}
	got, err := p.Preflight(b, DefaultLimits())
	if err != nil || got != want {
		t.Fatal(got, err)
	}
	s := openPreflightStore(t, p)
	before, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 2, ent(2, 2, "command"))
	if err := s.InstallApplication(2, b); err != nil {
		t.Fatal(err)
	}
	after, err := s.ApplicationUsage()
	if err != nil || after.RetainedBytes-before.RetainedBytes != want.RetainedBytes || after.RetainedRecords-before.RetainedRecords != want.RetainedRecords {
		t.Fatal(before, after, err)
	}
	if err := s.ScrubApplication(t.Context()); err != nil {
		t.Fatal(err)
	}
	view := viewApplication(t, s, 2)
	for _, w := range b.Writes {
		got, found, err := view.Get(t.Context(), w.Key, 100)
		if err != nil || !found || got.Deleted != w.Deleted || !bytes.Equal(got.Value, w.Value) {
			t.Fatal(w, got, found, err)
		}
	}
}
func TestApplicationPolicyPreflightEmptyEnvelopes(t *testing.T) {
	for _, b := range []ApplicationBatch{{}, {Image: []byte{}, Changes: []byte{}, Outcome: []byte{}, Writes: []KV{}}} {
		got, err := DefaultApplicationPolicy(1).Preflight(b, DefaultLimits())
		// Three nine-byte index keys and 36-byte frames remain with no payload.
		if err != nil || got != (ApplicationBatchUsage{135, 3}) {
			t.Fatal(got, err)
		}
	}
}
func TestApplicationPolicyPreflightOneShortAndShapeInstallParity(t *testing.T) {
	cases := []struct {
		name string
		edit func(*ApplicationPolicy, *ApplicationBatch)
		want error
	}{
		{"key", func(p *ApplicationPolicy, _ *ApplicationBatch) { p.MaxKeyBytes-- }, ErrLimit},
		{"value", func(p *ApplicationPolicy, _ *ApplicationBatch) { p.MaxValueBytes-- }, ErrLimit},
		{"image", func(p *ApplicationPolicy, _ *ApplicationBatch) { p.MaxImageBytes-- }, ErrLimit},
		{"change", func(p *ApplicationPolicy, _ *ApplicationBatch) { p.MaxChangeBytes-- }, ErrLimit},
		{"outcome", func(p *ApplicationPolicy, _ *ApplicationBatch) { p.MaxOutcomeBytes-- }, ErrLimit},
		{"writes", func(p *ApplicationPolicy, _ *ApplicationBatch) { p.MaxInstallWrites-- }, ErrLimit},
		// Work = 2*390 + image 4 + allowance 1024 + five 64-byte write allowances = 2128.
		{"work", func(p *ApplicationPolicy, _ *ApplicationBatch) { p.MaxInstallBytes-- }, ErrLimit},
		{"empty key", func(_ *ApplicationPolicy, b *ApplicationBatch) { b.Writes[0].Key = nil }, ErrInvalid},
		{"duplicate", func(_ *ApplicationPolicy, b *ApplicationBatch) { b.Writes[2].Key = []byte("a") }, ErrInvalid},
		{"unsorted", func(_ *ApplicationPolicy, b *ApplicationBatch) { b.Writes[1], b.Writes[2] = b.Writes[2], b.Writes[1] }, ErrInvalid},
		{"tombstone payload", func(_ *ApplicationPolicy, b *ApplicationBatch) { b.Writes[3].Value = []byte("x") }, ErrInvalid},
		// Exercise final aggregate check when there are no writes.
		{"envelope work", func(p *ApplicationPolicy, b *ApplicationBatch) { p.MaxInstallBytes = 1294; b.Writes = nil }, ErrLimit},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, b := preflightFixture()
			tc.edit(&p, &b)
			if err := p.Validate(DefaultLimits()); err != nil {
				t.Fatal("invalid test policy", err)
			}
			got, err := p.Preflight(b, DefaultLimits())
			if !errors.Is(err, tc.want) || got != (ApplicationBatchUsage{}) {
				t.Fatal(got, err, tc.want)
			}
			s := openPreflightStore(t, p)
			persist(t, s, 2, 2, ent(2, 2, "command"))
			before, err := s.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			if err := s.InstallApplication(2, b); !errors.Is(err, tc.want) {
				t.Fatal(err, tc.want)
			}
			after, err := s.ApplicationUsage()
			if err != nil || before != after {
				t.Fatal("refused install mutated usage", before, after, err)
			}
		})
	}
}
func TestApplicationPolicyPreflightInvalidConfiguration(t *testing.T) {
	cases := []struct {
		name string
		edit func(*ApplicationPolicy, *Limits)
	}{
		{"zero limits", func(_ *ApplicationPolicy, l *Limits) { *l = Limits{} }},
		{"invalid raft cache", func(_ *ApplicationPolicy, l *Limits) { l.CacheBytes = 0 }},
		{"invalid raft read", func(_ *ApplicationPolicy, l *Limits) { l.MaxReadEntries = 0 }},
		{"disabled", func(p *ApplicationPolicy, _ *Limits) { *p = ApplicationPolicy{} }},
		{"voter", func(p *ApplicationPolicy, _ *Limits) { p.LocalVoter = 0 }},
		{"negative key", func(p *ApplicationPolicy, _ *Limits) { p.MaxKeyBytes = -1 }},
		{"missing work", func(p *ApplicationPolicy, _ *Limits) { p.MaxInstallBytes = 0 }},
		{"snapshot mismatch", func(_ *ApplicationPolicy, l *Limits) { l.MaxSnapshotBytes = 3 }},
		{"tail mismatch", func(_ *ApplicationPolicy, l *Limits) { l.MaxRetainedEntries = 63 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, b := preflightFixture()
			l := DefaultLimits()
			tc.edit(&p, &l)
			got, err := p.Preflight(b, l)
			if !errors.Is(err, ErrInvalid) || got != (ApplicationBatchUsage{}) {
				t.Fatal(got, err)
			}
		})
	}
}
func TestApplicationPolicyPreflightDoesNotMutate(t *testing.T) {
	p, b := preflightFixture()
	_, before := preflightFixture()
	originalPolicy, l := p, DefaultLimits()
	originalLimits := l
	if _, err := p.Preflight(b, l); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(b, before) || p != originalPolicy || l != originalLimits {
		t.Fatal("success mutated input")
	}
	b.Writes[2].Key = []byte("a")
	before.Writes[2].Key = []byte("a")
	if _, err := p.Preflight(b, l); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(b, before) || p != originalPolicy || l != originalLimits {
		t.Fatal("failure mutated input")
	}
}
func TestApplicationPolicyPreflightDoesNotCertifyDynamicInstallation(t *testing.T) {
	p, b := preflightFixture()
	s := openPreflightStore(t, p)
	persist(t, s, 2, 2, ent(2, 2, "first"))
	if err := s.InstallApplication(2, b); err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 3, ent(3, 2, "next"))
	before, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, stale := range []ApplicationBatch{b, {BaseIndex: 2, BaseImageHash: sha256.Sum256([]byte("wrong")), Image: b.Image, Writes: b.Writes}} {
		if _, err := p.Preflight(stale, s.Limits()); err != nil {
			t.Fatal("pure check rejected dynamic base", err)
		}
		if err := s.InstallApplication(3, stale); !errors.Is(err, ErrInvalid) {
			t.Fatal("accepted stale base", err)
		}
	}
	current := b
	current.BaseIndex, current.BaseImageHash = 2, sha256.Sum256(b.Image)
	if _, err := p.Preflight(current, s.Limits()); err != nil {
		t.Fatal(err)
	}
	if err := s.InstallApplication(4, current); !errors.Is(err, ErrInvalid) {
		t.Fatal("accepted uncommitted index", err)
	}
	after, err := s.ApplicationUsage()
	if err != nil || before != after {
		t.Fatal(before, after, err)
	}
	if err := s.InstallApplication(3, current); err != nil {
		t.Fatal(err)
	}
}
func TestApplicationPolicyPreflightDoesNotReserveHeadroom(t *testing.T) {
	p := DefaultApplicationPolicy(1)
	p.MaxInstallBytes, p.MaxInstallWrites = 1294, 1
	p.RetainedApplicationBytes, p.RetainedApplicationRecords = 2588, 8
	s := openPreflightStore(t, p)
	for range 2 {
		if got, err := p.Preflight(ApplicationBatch{}, s.Limits()); err != nil || got != (ApplicationBatchUsage{135, 3}) {
			t.Fatal(got, err)
		}
	}
	// Initial root already occupies 135 bytes/3 records. Two worst-case installs
	// need 2588 bytes/8 records. Pure preflight neither claims nor replenishes it.
	before, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AdmitApplication(0); !errors.Is(err, ErrLimit) {
		t.Fatal("admission ignored headroom", err)
	}
	after, err := s.ApplicationUsage()
	if err != nil || before != after {
		t.Fatal(before, after, err)
	}
}
