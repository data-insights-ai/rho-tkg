package graphstore

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestRootConstantSizeAndCounterBoundaries(t *testing.T) {
	n := testNamespace()
	r, err := NewRoot(n, 3)
	if err != nil {
		t.Fatal(err)
	}
	if r.Namespace() != n || r.OwnershipEpoch() != 3 || r.SemanticEpoch() != 0 || r.NextPhysicalID() != 1 || r.EffectDigest() == ([32]byte{}) {
		t.Fatal(r)
	}
	initial := r
	reserved, first, err := r.ReservePhysical(4)
	if err != nil || first != 1 || reserved.NextPhysicalID() != 5 || reserved.SemanticEpoch() != 0 || reserved.EffectDigest() != r.EffectDigest() || r != initial {
		t.Fatal(reserved, first, err)
	}
	advanced, err := reserved.AdvanceEffects([32]byte{9})
	if err != nil || advanced.SemanticEpoch() != 1 || advanced.NextPhysicalID() != 5 || advanced.EffectDigest() != ([32]byte{9}) {
		t.Fatal(advanced, err)
	}
	encoded, err := EncodeRoot(advanced)
	if err != nil || len(encoded) != rootBytes {
		t.Fatal(len(encoded), err)
	}
	decoded, err := DecodeRoot(encoded)
	if err != nil || decoded != advanced {
		t.Fatal(decoded, err)
	}
	encoded[0] ^= 1
	if decoded.Namespace() != n {
		t.Fatal("decoder retained input")
	}
	for _, n := range []Namespace{{Graph: graphstate.GraphID{}, Partition: 7}, {Graph: testNamespace().Graph, Partition: 0}} {
		if _, err := NewRoot(n, 3); !errors.Is(err, ErrNamespace) {
			t.Fatal(err)
		}
	}
	if _, err := NewRoot(testNamespace(), 0); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := r.AdvanceEffects([32]byte{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, err := r.ReservePhysical(0); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	r.next = math.MaxUint64
	if _, _, err := r.ReservePhysical(1); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	r.epoch = math.MaxUint64
	if _, err := r.AdvanceEffects([32]byte{1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := EncodeRoot(Root{}); !errors.Is(err, ErrNamespace) {
		t.Fatal(err)
	}
	valid, _ := EncodeRoot(advanced)
	for i := range len(valid) {
		if _, err := DecodeRoot(valid[:i]); !errors.Is(err, ErrCorrupt) {
			t.Fatal(i, err)
		}
	}
	for _, mutate := range []func([]byte){func(b []byte) { b[2] = 2 }, func(b []byte) { b[3] = 2 }, func(b []byte) { b[40] ^= 1 }} {
		b := bytes.Clone(valid)
		mutate(b)
		if _, err := DecodeRoot(b); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
	if _, err := DecodeRoot(append(valid, 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}
func TestCatalogLimitsAreFinite(t *testing.T) {
	if err := (Limits{}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := DefaultLimits().Validate(); err != nil {
		t.Fatal(err)
	}
	db, _ := newStore(t, vfs.NewMem())
	view, err := db.ApplicationView(1)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	cases := []struct {
		edit func(*Limits)
		want error
	}{
		{func(l *Limits) { l.MaxReadRows = -1 }, ErrInvalid},
		{func(l *Limits) { l.MaxStages = 65 }, ErrInvalid},
		{func(l *Limits) { l.MaxValueBytes = 1 << 30 }, ErrInvalid},
		{func(l *Limits) { l.MaxRecordBytes = 1 }, ErrInvalid},
		{func(l *Limits) { l.MaxStageBytes = 1 }, ErrInvalid},
		{func(l *Limits) { l.MaxNameBytes = 65536 }, ErrInvalid},
		{func(l *Limits) { l.MaxBucketValues = 65537 }, ErrInvalid},
		{func(l *Limits) { l.Temporal.MaxMagnitudeBits = -1 }, temporal.ErrInvalidLimits},
	}
	for _, tc := range cases {
		l := DefaultLimits()
		tc.edit(&l)
		if err := l.Validate(); !errors.Is(err, tc.want) {
			t.Fatal(l, err)
		}
		if c, err := OpenCatalog(view, testNamespace(), 3, l); !errors.Is(err, tc.want) || c != nil {
			t.Fatal("constructor did not preserve sentinel", c, err)
		}
	}
}
