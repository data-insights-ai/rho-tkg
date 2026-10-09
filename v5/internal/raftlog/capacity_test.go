package raftlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestApplicationCapacityScanOwnedOutputMustFitByteBudget(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	var writes []KV
	for i := range 10 {
		writes = append(writes, KV{Key: []byte{byte('a' + i)}})
	}
	applyApplication(t, s, "many", writes...)
	v := viewApplication(t, s, 2)
	b := ReadBudget{10, 651}
	page, err := v.Scan(t.Context(), nil, nil, nil, b)
	if err != nil || !page.Complete || len(page.Rows) != 10 {
		t.Fatal(page, err)
	}
	owned := cap(page.Rows)*int(unsafe.Sizeof(KV{})) + cap(page.Next)
	for _, r := range page.Rows {
		owned += len(r.Key) + len(r.Value)
	}
	if owned > b.Bytes {
		t.Fatalf("owned result exceeds budget: owned minimum=%d budget=%d reported=%d rows=%d capacity=%d", owned, b.Bytes, page.Bytes, len(page.Rows), cap(page.Rows))
	}
}

func TestApplicationCapacityGetOwnedOutputMustFitPolicy(t *testing.T) {
	p := DefaultApplicationPolicy(1)
	p.MaxKeyBytes = 1
	p.MaxValueBytes = 37
	p.MaxPageBytes = 103
	p.MaxImageBytes = 16
	p.MaxChangeBytes = 103
	p.MaxOutcomeBytes = 103
	if err := p.Validate(DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	s := openApplication(t, vfs.NewMem(), p)
	applyApplication(t, s, "odd", KV{Key: []byte("a"), Value: make([]byte, 37)})
	v := viewApplication(t, s, 2)
	row, found, err := v.Get(t.Context(), []byte("a"), 103)
	if err != nil || !found || len(row.Key) != 1 || len(row.Value) != 37 {
		t.Fatal(row, found, err)
	}
	header := int(unsafe.Sizeof(row))
	owned := header + cap(row.Key) + cap(row.Value)
	if owned > p.MaxPageBytes {
		t.Fatalf("Get owned output exceeds policy: header=%d key cap=%d value cap=%d owned=%d MaxPageBytes=%d", header, cap(row.Key), cap(row.Value), owned, p.MaxPageBytes)
	}
	if cap(row.Key) != len(row.Key) || cap(row.Value) != len(row.Value) {
		t.Fatal("Get exposes unaccounted spare capacity")
	}
	row.Key[0] = 'b'
	row.Value[0] = 7
	again, found, err := v.Get(t.Context(), []byte("a"), 103)
	if err != nil || !found || string(again.Key) != "a" || again.Value[0] != 0 {
		t.Fatal("Get aliases immutable store bytes", again, found, err)
	}
}

func TestApplicationCapacityRecordOwnedOutputMustFitPolicy(t *testing.T) {
	for _, outcome := range []bool{false, true} {
		t.Run(fmt.Sprint(outcome), func(t *testing.T) {
			p := DefaultApplicationPolicy(1)
			p.MaxKeyBytes = 1
			p.MaxValueBytes = 37
			p.MaxPageBytes = 103
			p.MaxImageBytes = 16
			p.MaxChangeBytes = 103
			p.MaxOutcomeBytes = 103
			if err := p.Validate(DefaultLimits()); err != nil {
				t.Fatal(err)
			}
			s := openApplication(t, vfs.NewMem(), p)
			persist(t, s, 2, 2, ent(2, 2, "odd"))
			batch := ApplicationBatch{BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("initial")), Image: []byte("odd"), Changes: make([]byte, 103), Outcome: make([]byte, 103)}
			if err := s.InstallApplication(2, batch); err != nil {
				t.Fatal(err)
			}
			record, err := s.ApplicationRecord(t.Context(), 2, outcome, 103)
			if err != nil || len(record) != 103 {
				t.Fatal(len(record), err)
			}
			if cap(record) > p.MaxPageBytes {
				t.Fatalf("record owned byte capacity exceeds policy: len=%d cap=%d MaxPageBytes=%d", len(record), cap(record), p.MaxPageBytes)
			}
			before, err := encodeMeta(s.meta)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.ApplicationRecord(t.Context(), 2, outcome, 102); !errors.Is(err, ErrLimit) {
				t.Fatal("one-short envelope budget accepted", err)
			}
			after, err := encodeMeta(s.meta)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("envelope rejection changed durable metadata", err)
			}
			persist(t, s, 2, 3, ent(3, 2, "later"))
			if err := s.InstallApplication(3, ApplicationBatch{BaseIndex: 2, BaseImageHash: sha256.Sum256([]byte("odd")), Image: []byte("new"), Changes: []byte("new"), Outcome: []byte("new")}); err != nil {
				t.Fatal(err)
			}
			current, err := s.ApplicationRecord(t.Context(), 3, outcome, 3)
			if err != nil || string(current) != "new" || cap(current) != 3 {
				t.Fatal("current envelope capacity/content", current, err)
			}
			record[0] = 7
			again, err := s.ApplicationRecord(t.Context(), 2, outcome, 103)
			if err != nil || len(again) != 103 || again[0] != 0 {
				t.Fatal("record aliases immutable store bytes", err)
			}
		})
	}
}

func TestApplicationCapacityViewAndRootImageCapacities(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(fmt.Sprint(retained), func(t *testing.T) {
			p := DefaultApplicationPolicy(1)
			p.MaxImageBytes = 3
			p.MaxViewBytes = 3
			if err := p.Validate(DefaultLimits()); err != nil {
				t.Fatal(err)
			}
			s, err := Open(Config{Dir: "tiny-root", FS: vfs.NewMem(), Create: true, Application: p})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := s.Initialize([]uint64{1}, []byte("abc")); err != nil {
				t.Fatal(err)
			}
			v := viewApplication(t, s, 1)
			usage, err := s.ApplicationUsage()
			if err != nil || usage.ViewBytes != 3 {
				t.Fatal(usage, err)
			}
			if retained {
				if cap(v.image) > p.MaxViewBytes {
					t.Fatalf("retained view image exceeds live-root budget: len=%d cap=%d charged=%d MaxViewBytes=%d", len(v.image), cap(v.image), usage.ViewBytes, p.MaxViewBytes)
				}
			} else {
				root, err := v.Root()
				if err != nil {
					t.Fatal(err)
				}
				if cap(root.Image) > p.MaxImageBytes {
					t.Fatalf("returned root image exceeds image limit: len=%d cap=%d MaxImageBytes=%d", len(root.Image), cap(root.Image), p.MaxImageBytes)
				}
				root.Image[0] = 'x'
				again, err := v.Root()
				if err != nil || string(again.Image) != "abc" {
					t.Fatal("root aliases retained image", again, err)
				}
			}
		})
	}
}

func TestApplicationCapacityHistoricalPagesChargeWorkAndBacking(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	applyApplication(t, s, "old", KV{Key: []byte("a"), Value: bytes.Repeat([]byte{'x'}, 37)}, KV{Key: []byte("b"), Value: []byte{}}, KV{Key: []byte("c"), Deleted: true}, KV{Key: []byte("d"), Value: []byte("old")}, KV{Key: []byte("e")})
	old := viewApplication(t, s, 2)
	applyApplication(t, s, "new", KV{Key: []byte("a"), Value: []byte("new")}, KV{Key: []byte("c"), Value: []byte("revived")}, KV{Key: []byte("d"), Deleted: true}, KV{Key: []byte("f")}, KV{Key: []byte("g"), Deleted: true}, KV{Key: []byte("h"), Value: []byte("future")}, KV{Key: []byte("i")}, KV{Key: []byte("j"), Deleted: true})
	for _, scenario := range []struct {
		name string
		v    *ApplicationView
		want map[string]string
	}{
		{"initial", viewApplication(t, s, 1), map[string]string{}},
		{"old", old, map[string]string{"a": string(bytes.Repeat([]byte{'x'}, 37)), "b": "", "d": "old", "e": ""}},
		{"current", viewApplication(t, s, 3), map[string]string{"a": "new", "b": "", "c": "revived", "e": "", "f": "", "h": "future", "i": ""}},
	} {
		for _, limit := range []int{103, 131, 651} {
			t.Run(fmt.Sprintf("%s/%d", scenario.name, limit), func(t *testing.T) {
				var after []byte
				seen := map[string]string{}
				visited := 0
				for pages := 0; ; pages++ {
					if pages > 10 {
						t.Fatal("continuation stalled")
					}
					page, err := scenario.v.Scan(t.Context(), nil, nil, after, ReadBudget{10, limit})
					if err != nil || page.Visited < 1 || page.Visited > 10 || page.Bytes > limit {
						t.Fatal(page, err)
					}
					assertApplicationPageCapacity(t, page, limit)
					visited += page.Visited
					for _, r := range page.Rows {
						key := string(r.Key)
						if _, exists := seen[key]; exists || r.Deleted {
							t.Fatal("duplicate or tombstone emitted", key)
						}
						seen[key] = string(r.Value)
						if key == "b" && r.Value == nil {
							t.Fatal("present empty value became nil/tombstone")
						}
						r.Key[0] = 'z'
						if len(r.Value) > 0 {
							r.Value[0] = '!'
						}
					}
					if page.Complete {
						if page.Next != nil {
							t.Fatal("complete page has continuation")
						}
						break
					}
					if bytes.Compare(page.Next, after) <= 0 {
						t.Fatal("continuation did not advance", page.Next, after)
					}
					after = page.Next
				}
				if visited != 10 || !maps.Equal(seen, scenario.want) {
					t.Fatal("wrong historical set or skipped future/tombstone work", visited, seen, scenario.want)
				}
				for key, value := range scenario.want {
					r, found, err := scenario.v.Get(t.Context(), []byte(key), 128)
					if err != nil || !found || r.Deleted || string(r.Value) != value {
						t.Fatal("Scan copied rows alias storage", key, r, err)
					}
				}
			})
		}
	}
	before, err := encodeMeta(s.meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Scan(t.Context(), nil, nil, nil, ReadBudget{10, 102}); !errors.Is(err, ErrLimit) {
		t.Fatal("one-short max row accepted", err)
	}
	after, err := encodeMeta(s.meta)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("read rejection mutated durable state", err)
	}
	if _, err := s.ApplicationUsage(); err != nil {
		t.Fatal("budget refusal poisoned store", err)
	}
	if r, found, err := old.Get(t.Context(), []byte("f"), 1); err != nil || found || r.Key != nil {
		t.Fatal("old view exposed future content", r, found, err)
	}
	if r, found, err := viewApplication(t, s, 3).Get(t.Context(), []byte("d"), 1); err != nil || !found || !r.Deleted || len(r.Value) != 0 || cap(r.Key) != 1 {
		t.Fatal("point read lost tombstone", r, found, err)
	}
}

func assertApplicationPageCapacity(t *testing.T, page ApplicationPage, limit int) {
	t.Helper()
	owned := 64*cap(page.Rows) + cap(page.Next)
	for _, r := range page.Rows {
		if cap(r.Key) != len(r.Key) || cap(r.Value) != len(r.Value) {
			t.Fatal("spare row byte capacity", r)
		}
		owned += cap(r.Key) + cap(r.Value)
	}
	if cap(page.Next) != len(page.Next) || owned > page.Bytes || page.Bytes > limit {
		t.Fatal("uncharged capacity", owned, page.Bytes, limit)
	}
}

func TestApplicationCapacityExactRootAggregateAndEmptyCopies(t *testing.T) {
	for _, src := range [][]byte{nil, {}, []byte("abc")} {
		got := copyApplicationBytes(src)
		if (src == nil) != (got == nil) || cap(got) != len(src) || !bytes.Equal(src, got) {
			t.Fatal("copy changed nil/empty/capacity", src, got)
		}
		if len(got) > 0 {
			got[0] = '!'
			if src[0] != 'a' {
				t.Fatal("copy aliases source")
			}
		}
	}
	p := DefaultApplicationPolicy(1)
	p.MaxImageBytes = 3
	p.MaxViewBytes = 6
	s, err := Open(Config{Dir: "tiny", FS: vfs.NewMem(), Create: true, Application: p})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Initialize([]uint64{1}, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	old := viewApplication(t, s, 1)
	applyApplication(t, s, "def")
	current := viewApplication(t, s, 2)
	usage, err := s.ApplicationUsage()
	if err != nil || usage.ViewBytes != 6 || cap(old.image)+cap(current.image) != 6 {
		t.Fatal("aggregate visible image budget mismatch", usage, err)
	}
	if _, err := s.ApplicationView(2); !errors.Is(err, ErrLimit) {
		t.Fatal("extra retained image exceeded aggregate budget", err)
	}
	for _, pair := range []struct {
		v    *ApplicationView
		want string
	}{{old, "abc"}, {current, "def"}} {
		root, err := pair.v.Root()
		if err != nil || cap(root.Image) != 3 || string(root.Image) != pair.want {
			t.Fatal(root, err)
		}
		root.Image[0] = '!'
		again, err := pair.v.Root()
		if err != nil || string(again.Image) != pair.want || again.ImageHash != sha256.Sum256([]byte(pair.want)) {
			t.Fatal("root image aliases retained view", again, err)
		}
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	third := viewApplication(t, s, 2)
	usage, err = s.ApplicationUsage()
	if err != nil || usage.ViewBytes != 6 || cap(current.image)+cap(third.image) != 6 {
		t.Fatal("closed root accounting not released", usage, err)
	}
}

func TestApplicationCapacityVariableContinuationAndEmptyPages(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	applyApplication(t, s, "old", KV{Key: []byte("bb")}, KV{Key: []byte("ccc"), Deleted: true})
	applyApplication(t, s, "new", KV{Key: []byte("a"), Value: []byte{}})
	for _, index := range []uint64{1, 2, 3} {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			v := viewApplication(t, s, index)
			var after []byte
			seen := map[string]bool{}
			for pageNumber := range 3 {
				page, err := v.Scan(t.Context(), nil, nil, after, ReadBudget{3, 70})
				if err != nil || page.Visited != 1 {
					t.Fatal("variable-key work not bounded", page, err)
				}
				assertApplicationPageCapacity(t, page, 70)
				for _, r := range page.Rows {
					seen[string(r.Key)] = true
				}
				if pageNumber < 2 {
					want := []string{"a", "bb"}[pageNumber]
					if page.Complete || string(page.Next) != want || cap(page.Next) != len(want) {
						t.Fatal("wrong owned continuation", page)
					}
					after = page.Next
				} else if !page.Complete || page.Next != nil || len(page.Rows) != 0 || page.Bytes != 67 {
					t.Fatal("final tombstone/future-only page changed", page)
				}
			}
			want := map[string]bool{}
			if index >= 2 {
				want["bb"] = true
			}
			if index == 3 {
				want["a"] = true
			}
			if !maps.Equal(seen, want) {
				t.Fatal("wrong old/current set", seen, want)
			}
		})
	}
}

func TestApplicationCapacityFullBufferThenOversizedRow(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	large := bytes.Repeat([]byte{'x'}, 1<<20)
	applyApplication(t, s, "wide", KV{Key: []byte("a")}, KV{Key: []byte("b")}, KV{Key: []byte("c"), Value: large})
	v := viewApplication(t, s, 2)
	// The candidate growth arithmetic sees negative available capacity when
	// the next row is too large; it must return the intact prior page safely.
	page, err := v.Scan(t.Context(), nil, nil, nil, ReadBudget{3, 131})
	if err != nil || page.Complete || page.Visited != 2 || len(page.Rows) != 2 || string(page.Next) != "b" {
		t.Fatal(page, err)
	}
	assertApplicationPageCapacity(t, page, 131)
	if string(page.Rows[0].Key) != "a" || string(page.Rows[1].Key) != "b" {
		t.Fatal("oversized lookahead replaced prior rows", page.Rows)
	}
	before, err := encodeMeta(s.meta)
	if err != nil {
		t.Fatal(err)
	}
	if refused, err := v.Scan(t.Context(), nil, nil, page.Next, ReadBudget{3, 131}); !errors.Is(err, ErrLimit) || len(refused.Rows) != 0 || refused.Next != nil {
		t.Fatal("oversized resumed row admitted partial output", refused, err)
	}
	after, err := encodeMeta(s.meta)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("oversized resume mutated durable state", err)
	}
	if string(page.Next) != "b" || string(page.Rows[0].Key) != "a" || string(page.Rows[1].Key) != "b" {
		t.Fatal("resume changed caller-owned page", page)
	}
	assertApplicationPageCapacity(t, page, 131)
	limit := len(large) + 66
	last, err := v.Scan(t.Context(), nil, nil, page.Next, ReadBudget{3, limit})
	if err != nil || !last.Complete || last.Next != nil || last.Visited != 1 || len(last.Rows) != 1 || string(last.Rows[0].Key) != "c" || !bytes.Equal(last.Rows[0].Value, large) {
		t.Fatal("larger-budget resume skipped/repeated oversized row", last, err)
	}
	assertApplicationPageCapacity(t, last, limit)
	last.Rows[0].Value[0] = '!'
	row, found, err := v.Get(t.Context(), []byte("c"), len(large)+1)
	if err != nil || !found || row.Value[0] != 'x' {
		t.Fatal("final complete page aliases storage", found, err)
	}
}
