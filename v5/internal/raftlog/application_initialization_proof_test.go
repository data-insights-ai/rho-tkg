package raftlog

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestProveOnlyApplicationKVExactCurrentHistoryAndBoundaries(t *testing.T) {
	for _, generations := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "generation"}[generations], func(t *testing.T) {
			var s *Store
			if generations {
				s = generationStore(t, vfs.NewMem())
			} else {
				s = openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
			}
			expected := KV{Key: []byte{'k', 0, 'v'}, Value: []byte("declaration")}
			old := viewApplication(t, s, 1)
			index := generationApply(t, s, "published", expected)
			current := viewApplication(t, s, index)
			root, work, err := current.ProveOnlyApplicationKV(t.Context(), expected, 9, s.ApplicationLimits().MaxPageBytes)
			if err != nil || root.Index != index || string(root.Image) != "published" || cap(root.Image) != len(root.Image) || work.Records != 1 || work.Bytes == 0 {
				t.Fatal(root, work, err)
			}
			root.Image[0] ^= 1
			for _, delta := range []int{-1, 0, 1} {
				got, used, err := current.ProveOnlyApplicationKV(t.Context(), expected, 9, work.Bytes+delta)
				if delta < 0 {
					if !errors.Is(err, ErrLimit) || got.Index != 0 || used.Records != 1 || used.Bytes > work.Bytes+delta || s.poison != nil {
						t.Fatal(got, used, err)
					}
				} else if err != nil || string(got.Image) != "published" || used != work {
					t.Fatal(got, used, err)
				}
			}
			if got, _, err := old.ProveOnlyApplicationKV(t.Context(), expected, 9, 4096); !errors.Is(err, ErrInvalid) || got.Index != 0 {
				t.Fatal("retained empty view borrowed current declaration", got, err)
			}
			generationApply(t, s, "updated", expected)
			latest := viewApplication(t, s, index+1)
			if got, used, err := latest.ProveOnlyApplicationKV(t.Context(), expected, 9, 4096); !errors.Is(err, ErrInvalid) || got.Index != 0 || used.Records != 0 || s.poison != nil {
				t.Fatal("second identical version hidden by current point read", got, used, err)
			}
		})
	}
}

func TestProveOnlyApplicationKVHiddenTombstoneWrongValueAndNoops(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows []KV
		want error
	}{
		{"one", []KV{{Key: []byte("key"), Value: []byte("value")}}, nil},
		{"wrong key", []KV{{Key: []byte("other"), Value: []byte("value")}}, ErrInvalid},
		{"wrong value", []KV{{Key: []byte("key"), Value: []byte("other")}}, ErrInvalid},
		{"tombstone", []KV{{Key: []byte("key"), Deleted: true}}, ErrInvalid},
		{"extra tombstone", []KV{{Key: []byte("key"), Value: []byte("value")}, {Key: []byte("z"), Deleted: true}}, ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
			generationApply(t, s, "published", tc.rows...)
			index := generationApply(t, s, "noop")
			v := viewApplication(t, s, index)
			root, _, err := v.ProveOnlyApplicationKV(t.Context(), KV{Key: []byte("key"), Value: []byte("value")}, 64, 4096)
			if tc.want == nil {
				if err != nil || root.Index != index {
					t.Fatal(root, err)
				}
			} else if !errors.Is(err, tc.want) || root.Index != 0 || s.poison != nil {
				t.Fatal(root, err)
			}
		})
	}
}

func TestProveOnlyApplicationKVInvalidLifetimeAndCorruptFrame(t *testing.T) {
	expected := KV{Key: []byte("key"), Value: []byte("value")}
	var nilView *ApplicationView
	if _, work, err := nilView.ProveOnlyApplicationKV(t.Context(), expected, 64, 4096); !errors.Is(err, ErrInvalid) || work != (ApplicationProofWork{}) {
		t.Fatal(work, err)
	}
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	index := generationApply(t, s, "published", expected)
	v := viewApplication(t, s, index)
	for _, row := range []KV{{}, {Key: []byte("key"), Deleted: true}, {Key: bytes.Repeat([]byte{'x'}, s.ApplicationLimits().MaxKeyBytes+1)}} {
		want := ErrInvalid
		if len(row.Key) > s.ApplicationLimits().MaxKeyBytes {
			want = ErrLimit
		}
		if _, work, err := v.ProveOnlyApplicationKV(t.Context(), row, 64, 4096); !errors.Is(err, want) || work != (ApplicationProofWork{}) {
			t.Fatal(work, err)
		}
	}
	if _, _, err := v.ProveOnlyApplicationKV(t.Context(), expected, -1, 4096); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	//nolint:staticcheck // SA1012: deliberate nil-context contract test.
	if _, _, err := v.ProveOnlyApplicationKV(nil, expected, 64, 4096); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, work, err := v.ProveOnlyApplicationKV(ctx, expected, 64, 4096); !errors.Is(err, context.Canceled) || work != (ApplicationProofWork{}) {
		t.Fatal(work, err)
	}
	if _, work, err := v.ProveOnlyApplicationKV(t.Context(), expected, 8, 4096); !errors.Is(err, ErrLimit) || work != (ApplicationProofWork{}) {
		t.Fatal(work, err)
	}
	physical := bankVersionKey(s.activeBank(), expected.Key, index)
	if err := s.db.Set(physical, []byte("corrupt"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if root, work, err := v.ProveOnlyApplicationKV(t.Context(), expected, 64, 4096); !errors.Is(err, ErrCorrupt) || root.Index != 0 || work.Records != 1 || s.poison == nil {
		t.Fatal(root, work, err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := v.ProveOnlyApplicationKV(t.Context(), expected, 64, 4096); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestProveOnlyApplicationKVCurrentMetadataPreflightAndRetainedRefusal(t *testing.T) {
	expected := KV{Key: []byte("key"), Value: []byte("declaration")}
	for _, mutation := range []string{"through", "record underflow", "record overflow", "image bytes", "image policy", "image hash"} {
		t.Run(mutation, func(t *testing.T) {
			s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
			old := viewApplication(t, s, 1)
			index := generationApply(t, s, "different current image", expected)
			current := viewApplication(t, s, index)
			switch mutation {
			case "through":
				s.meta.App.Through++
			case "record underflow":
				s.meta.App.Records = 3*index - 1
			case "record overflow":
				s.meta.Applied, s.meta.App.Through, current.index = math.MaxUint64, math.MaxUint64, math.MaxUint64
			case "image bytes":
				s.meta.ImageBytes++
			case "image policy":
				s.meta.App.Policy.MaxImageBytes = 1
			case "image hash":
				current.image[0] ^= 1
			}
			// A different retained root/image is refused before inspecting current
			// metadata; it never turns a stale request into store corruption.
			if got, work, err := old.ProveOnlyApplicationKV(t.Context(), expected, 64, 4096); !errors.Is(err, ErrInvalid) || got.Index != 0 || work != (ApplicationProofWork{Bytes: len("initial")}) || s.poison != nil {
				t.Fatal("retained-view refusal poisoned current store", got, work, err)
			}
			want := ErrCorrupt
			if mutation == "image hash" {
				want = ErrInvalid
			}
			got, work, err := current.ProveOnlyApplicationKV(t.Context(), expected, 64, 4096)
			if !errors.Is(err, want) || got.Index != 0 || work != (ApplicationProofWork{Bytes: len("different current image")}) || (s.poison != nil) != (want == ErrCorrupt) {
				t.Fatal("current authoritative preflight misclassified/lost work", got, work, err)
			}
		})
	}
}
