package raftlog

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func c0Install(t *testing.T, s *Store, rows ...KV) uint64 {
	t.Helper()
	index, batch := controlBatch(t, s, "published", []ApplicationControlPut{{Key: []byte("journal"), Value: []byte("registered")}}, rows...)
	if err := s.InstallApplication(index, batch); err != nil {
		t.Fatal(err)
	}
	return index
}

func TestC0ControlsDoNotBecomeGraphRecords(t *testing.T) {
	expected := KV{Key: []byte{'k', 0, 'v'}, Value: []byte("declaration")}
	for _, tc := range []struct {
		name      string
		rows      []KV
		duplicate bool
		wantOnly  bool
		wantEmpty bool
	}{
		{name: "zero", wantEmpty: true},
		{name: "one", rows: []KV{expected}, wantOnly: true},
		{name: "two", rows: []KV{expected, {Key: []byte("z"), Value: []byte("other")}}},
		{name: "duplicate version", rows: []KV{expected}, duplicate: true},
		{name: "tombstone", rows: []KV{{Key: expected.Key, Deleted: true}}},
		{name: "extra tombstone", rows: []KV{expected, {Key: []byte("z"), Deleted: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newControlStore(t, vfs.NewMem(), 1)
			old := viewApplication(t, s, 1)
			c0Install(t, s, tc.rows...)
			var index uint64
			if tc.duplicate {
				var batch ApplicationBatch
				index, batch = controlBatch(t, s, "published", nil, expected)
				if err := s.InstallApplication(index, batch); err != nil {
					t.Fatal(err)
				}
			} else {
				index = installControl(t, s, "published", ApplicationControlPut{Key: []byte("second"), Value: []byte("registered")})
			}
			v := viewApplication(t, s, index)
			root, work, err := v.ProveOnlyApplicationKV(t.Context(), expected, 9, 4096)
			if tc.wantOnly {
				if err != nil || root.Index != index || string(root.Image) != "published" || cap(root.Image) != 9 || work != (ApplicationProofWork{1, 160}) {
					t.Fatal(root, work, err)
				}
			} else if !errors.Is(err, ErrInvalid) || root.Index != 0 || root.Image != nil || work.Records != map[bool]int{true: 1, false: 0}[tc.name == "tombstone"] {
				t.Fatal("exclusive graph proof", root, work, err)
			}
			empty, err := v.ProveNoApplicationData(t.Context())
			if tc.wantEmpty {
				if err != nil || empty.Index != index {
					t.Fatal(empty, err)
				}
			} else if !errors.Is(err, ErrInvalid) || empty.Index != 0 {
				t.Fatal("graph versions hidden by control rows", empty, err)
			}
			graphVersions := uint64(len(tc.rows))
			if tc.duplicate {
				graphVersions++
			}
			if s.meta.App.Records != 3*index+graphVersions || s.meta.Controls.Records != 2-c0DuplicateCount(tc.duplicate) {
				t.Fatal("graph/control ledger conflation", s.meta.App, s.meta.Controls)
			}
			bank := s.meta.Gen.Banks[s.meta.Gen.Active]
			if bank.Records != s.meta.App.Records+s.meta.Controls.Records || bank.Bytes != s.meta.App.Bytes+s.meta.Controls.Bytes || bank.ControlRecords != s.meta.Controls.Records || bank.ControlBytes != s.meta.Controls.Bytes {
				t.Fatal("joint generation ledger", bank)
			}
			if got, used, err := old.ProveOnlyApplicationKV(t.Context(), expected, 9, 4096); !errors.Is(err, ErrInvalid) || got.Index != 0 || used != (ApplicationProofWork{Bytes: 4}) || s.poison != nil {
				t.Fatal("old view borrowed current proof", got, used, err)
			}
			if _, err := old.Root(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func c0DuplicateCount(duplicate bool) uint64 {
	if duplicate {
		return 1
	}
	return 0
}

func TestC0ControlAndGenerationPreflightAfterCurrentFence(t *testing.T) {
	expected := KV{Key: []byte{'k', 0, 'v'}, Value: []byte("declaration")}
	for _, tc := range []struct {
		name   string
		mutate func(*Store, *ApplicationView)
		want   error
	}{
		{"control byte overflow", func(s *Store, _ *ApplicationView) { s.meta.Controls.Bytes = math.MaxUint64 }, ErrCorrupt},
		{"impossible control records", func(s *Store, _ *ApplicationView) {
			s.meta.Controls.Records = s.meta.Controls.Bytes/(12+appFrameBytes) + 1
		}, ErrCorrupt},
		{"graph byte overflow", func(s *Store, _ *ApplicationView) { s.meta.App.Bytes = math.MaxUint64 }, ErrLimit},
		{"generation control bytes", func(s *Store, _ *ApplicationView) { s.meta.Gen.Banks[s.meta.Gen.Active].ControlBytes++ }, ErrInvalid},
		{"generation control records", func(s *Store, _ *ApplicationView) { s.meta.Gen.Banks[s.meta.Gen.Active].ControlRecords++ }, ErrInvalid},
		{"generation joint bytes", func(s *Store, _ *ApplicationView) { s.meta.Gen.Banks[s.meta.Gen.Active].Bytes++ }, ErrInvalid},
		{"generation joint records", func(s *Store, _ *ApplicationView) { s.meta.Gen.Banks[s.meta.Gen.Active].Records++ }, ErrInvalid},
		{"generation control mode", func(s *Store, _ *ApplicationView) { s.meta.Gen.ControlEnabled = false }, ErrInvalid},
		{"control before image hash", func(s *Store, v *ApplicationView) { s.meta.Controls.Bytes = math.MaxUint64; v.image[0] ^= 1 }, ErrCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newControlStore(t, vfs.NewMem(), 1)
			old, err := s.ApplicationView(1)
			if err != nil {
				t.Fatal(err)
			}
			owned := []*ApplicationView{old}
			t.Cleanup(func() {
				for i := len(owned) - 1; i >= 0; i-- {
					if err := owned[i].Close(); err != nil {
						if s.poison == nil || !errors.Is(err, ErrPoisoned) || !errors.Is(err, s.poison) {
							t.Error("owned borrow cleanup lost recorded cause", err, s.poison)
						}
					}
				}
			})
			index := c0Install(t, s, expected)
			v, err := s.ApplicationView(index)
			if err != nil {
				t.Fatal(err)
			}
			owned = append(owned, v)
			ref := v.ref
			if ref == nil || old.ref != ref || ref.refs != 2 || s.views != 2 || s.viewBytes != 13 {
				t.Fatal("borrow fixture accounting", ref, s.views, s.viewBytes)
			}
			tc.mutate(s, v)
			got, work, err := old.ProveOnlyApplicationKV(t.Context(), expected, 9, 4096)
			if !errors.Is(err, ErrInvalid) || got.Index != 0 || got.Image != nil || work != (ApplicationProofWork{Bytes: 4}) || s.poison != nil {
				t.Fatal("retained fence must precede current corruption", got, work, err, s.poison)
			}
			if _, err := old.Root(); err != nil {
				t.Fatal("retained borrow", err)
			}
			got, work, err = v.ProveOnlyApplicationKV(t.Context(), expected, 9, 4096)
			if !errors.Is(err, tc.want) || got.Index != 0 || got.Image != nil || got.Generation != 0 || got.ImageHash != ([32]byte{}) || work != (ApplicationProofWork{Bytes: 9}) || !errors.Is(s.poison, tc.want) {
				t.Fatal("current metadata must refuse before hash/output", got, work, err, s.poison)
			}
			if err := v.Close(); err != nil {
				t.Fatal("current close with retained reference", err)
			}
			if err := old.Close(); !errors.Is(err, ErrPoisoned) || !errors.Is(err, tc.want) {
				t.Fatal("last close lost poisoned-store cause", err)
			}
			if !v.closed || !old.closed || v.image != nil || old.image != nil || s.views != 0 || s.viewBytes != 0 || ref.refs != 0 || !errors.Is(s.poison, tc.want) {
				t.Fatal("poisoned close failed accounting/cause release", v.closed, old.closed, s.views, s.viewBytes, ref.refs, s.poison)
			}
			if err := v.Close(); err != nil {
				t.Fatal("repeat current close", err)
			}
			if err := old.Close(); err != nil {
				t.Fatal("repeat retained close", err)
			}
			if s.views != 0 || s.viewBytes != 0 || ref.refs != 0 || !errors.Is(s.poison, tc.want) {
				t.Fatal("repeat close changed accounting/cause", s.views, s.viewBytes, ref.refs, s.poison)
			}
		})
	}
}

func TestC0ControlModeLiteralReadBoundaries(t *testing.T) {
	s, _ := newControlStore(t, vfs.NewMem(), 1)
	expected := KV{Key: []byte{'k', 0, 'v'}, Value: []byte("declaration")}
	index := c0Install(t, s, expected)
	v := viewApplication(t, s, index)
	// Independent representation oracle: image 9 + prefix reserve 9 + upper
	// copy 7 + iterator 64 + stored key 15 + frame/value 47 + output image 9.
	for _, limit := range []int{159, 160, 161} {
		root, work, err := v.ProveOnlyApplicationKV(t.Context(), expected, 9, limit)
		if limit == 159 {
			if !errors.Is(err, ErrLimit) || root.Index != 0 || root.Image != nil || work != (ApplicationProofWork{1, 151}) {
				t.Fatal(root, work, err)
			}
		} else if err != nil || root.Index != index || !bytes.Equal(root.Image, []byte("published")) || cap(root.Image) != 9 || work != (ApplicationProofWork{1, 160}) {
			t.Fatal(root, work, err)
		}
		if s.poison != nil {
			t.Fatal(s.poison)
		}
		if _, err := v.Root(); err != nil {
			t.Fatal(err)
		}
	}
	if root, work, err := v.ProveOnlyApplicationKV(t.Context(), expected, 8, 160); !errors.Is(err, ErrLimit) || root.Index != 0 || work != (ApplicationProofWork{}) {
		t.Fatal(root, work, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if root, work, err := v.ProveOnlyApplicationKV(ctx, expected, 9, 160); !errors.Is(err, context.Canceled) || root.Index != 0 || work != (ApplicationProofWork{}) {
		t.Fatal(root, work, err)
	}
	if _, err := v.Root(); err != nil {
		t.Fatal(err)
	}
}
