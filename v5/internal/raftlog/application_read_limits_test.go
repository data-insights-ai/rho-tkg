package raftlog

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestApplicationViewReadLimitsNilCustomAndClosed(t *testing.T) {
	if (*ApplicationView)(nil).ReadLimits() != (ReadBudget{}) || new(ApplicationView).ReadLimits() != (ReadBudget{}) {
		t.Fatal("nil/unwired view reported configured limits")
	}
	p := DefaultApplicationPolicy(1)
	p.MaxPageRows, p.MaxPageBytes = 11, 3<<20
	s := openApplication(t, vfs.NewMem(), p)
	v := viewApplication(t, s, 1)
	want := ReadBudget{Rows: 11, Bytes: 3 << 20}
	if v.ReadLimits() != want {
		t.Fatal("reported defaults instead of configured per-call limits")
	}
	if _, err := v.Scan(t.Context(), nil, nil, nil, ReadBudget{Rows: 12, Bytes: want.Bytes}); !errors.Is(err, ErrLimit) {
		t.Fatal("configured read bound was not authoritative", err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if v.ReadLimits() != want {
		t.Fatal("configuration became a view-validity or remaining-quota claim")
	}
	if _, err := v.Root(); !errors.Is(err, ErrClosed) {
		t.Fatal("closed view remained valid", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if v.ReadLimits() != want {
		t.Fatal("immutable configuration changed after store close")
	}
}
