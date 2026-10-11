package raftlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func reverseRows(t *testing.T, page ApplicationPage, want ...string) {
	t.Helper()
	got := make([]string, len(page.Rows))
	owned := 64*cap(page.Rows) + cap(page.Next)
	for i, row := range page.Rows {
		got[i] = string(row.Key) + "=" + string(row.Value)
		owned += cap(row.Key) + cap(row.Value)
		if row.Deleted || cap(row.Key) != len(row.Key) || cap(row.Value) != len(row.Value) {
			t.Fatal("row ownership/tombstone", row)
		}
	}
	if !slices.Equal(got, want) || page.Bytes < owned || cap(page.Next) != len(page.Next) {
		t.Fatal("exact descending rows/capacities", got, want, page)
	}
}

func TestApplicationScanReverseHistoryRangeAndPagination(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	first := applyApplication(t, s, "first",
		KV{Key: []byte{0}, Value: []byte("zero")}, KV{Key: []byte("a"), Value: []byte("old")},
		KV{Key: []byte("a\x00"), Value: []byte{}}, KV{Key: []byte("aa"), Value: []byte("prefix")},
		KV{Key: []byte("b"), Value: []byte("middle")}, KV{Key: []byte("z"), Value: []byte("last")})
	old := viewApplication(t, s, first)
	at := applyApplication(t, s, "later", KV{Key: []byte("a"), Value: []byte("new")}, KV{Key: []byte("z"), Deleted: true}, KV{Key: []byte("zz"), Value: []byte("future")})
	current := viewApplication(t, s, at)
	for _, tc := range []struct {
		v    *ApplicationView
		want []string
	}{
		{old, []string{"z=last", "b=middle", "aa=prefix", "a\x00=", "a=old", "\x00=zero"}},
		{current, []string{"zz=future", "b=middle", "aa=prefix", "a\x00=", "a=new", "\x00=zero"}},
	} {
		page, err := tc.v.ScanReverse(t.Context(), nil, nil, nil, ReadBudget{Rows: 10, Bytes: 4096})
		if err != nil || !page.Complete || page.Next != nil || page.Visited != 7 {
			t.Fatal(page, err)
		}
		reverseRows(t, page, tc.want...)
		var got []string
		var before []byte
		for n := 0; ; n++ {
			if n > 7 {
				t.Fatal("reverse cursor stalled")
			}
			part, err := tc.v.ScanReverse(t.Context(), nil, nil, before, ReadBudget{Rows: 1, Bytes: 128})
			if err != nil || part.Visited != 1 || part.Bytes > 128 {
				t.Fatal(part, err)
			}
			for _, row := range part.Rows {
				got = append(got, string(row.Key)+"="+string(row.Value))
			}
			if part.Complete {
				break
			}
			if len(before) > 0 && bytes.Compare(part.Next, before) >= 0 {
				t.Fatal("cursor not exclusive", before, part.Next)
			}
			before = part.Next
		}
		if !slices.Equal(got, tc.want) {
			t.Fatal(got, tc.want)
		}
	}
	page, err := old.ScanReverse(t.Context(), []byte("a"), []byte("b"), nil, ReadBudget{Rows: 4, Bytes: 1024})
	if err != nil || !page.Complete || page.Visited != 3 {
		t.Fatal(page, err)
	}
	reverseRows(t, page, "aa=prefix", "a\x00=", "a=old")
	page, err = old.ScanReverse(t.Context(), []byte("a"), []byte("b"), []byte("aa"), ReadBudget{Rows: 4, Bytes: 1024})
	if err != nil || !page.Complete {
		t.Fatal(page, err)
	}
	reverseRows(t, page, "a\x00=", "a=old")
	page.Rows[1].Value[0] = 'X'
	getApplication(t, old, "a", "old", true, false)
	page, err = old.ScanReverse(t.Context(), []byte("absent"), []byte("b"), nil, ReadBudget{Rows: 1, Bytes: 128})
	if err != nil || !page.Complete || page.Visited != 0 || page.Bytes != 0 {
		t.Fatal("phantom range", page, err)
	}
}

func TestApplicationScanReverseFutureOnlyAndVersionRuns(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	first := applyApplication(t, s, "first", KV{Key: []byte("b"), Value: []byte("first")})
	old := viewApplication(t, s, first)
	for i := range 64 {
		applyApplication(t, s, fmt.Sprint(i), KV{Key: []byte("b"), Value: []byte("new")})
	}
	applyApplication(t, s, "future", KV{Key: []byte("a"), Value: []byte("future")}, KV{Key: []byte("c"), Value: []byte("future")})
	page, err := old.ScanReverse(t.Context(), nil, nil, nil, ReadBudget{Rows: 1, Bytes: 66})
	if err != nil || page.Complete || page.Visited != 1 || string(page.Next) != "c" || page.Bytes != 66 {
		t.Fatal(page, err)
	}
	reverseRows(t, page)
	page, err = old.ScanReverse(t.Context(), nil, nil, page.Next, ReadBudget{Rows: 1, Bytes: 128})
	if err != nil || page.Complete || page.Visited != 1 || string(page.Next) != "b" {
		t.Fatal("future run lost predecessor", page, err)
	}
	reverseRows(t, page, "b=first")
	page, err = old.ScanReverse(t.Context(), nil, nil, page.Next, ReadBudget{Rows: 1, Bytes: 66})
	if err != nil || !page.Complete || page.Visited != 1 || page.Bytes != 65 || page.Next != nil {
		t.Fatal(page, err)
	}
	reverseRows(t, page)
	initial := viewApplication(t, s, 1)
	page, err = initial.ScanReverse(t.Context(), nil, nil, nil, ReadBudget{Rows: 3, Bytes: 256})
	if err != nil || !page.Complete || page.Visited != 3 || page.Bytes != 195 {
		t.Fatal("all future keys charged", page, err)
	}
	reverseRows(t, page)
}

func TestApplicationScanReverseExactBudgetsAndCapacities(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	applyApplication(t, s, "rows", KV{Key: []byte("a"), Value: []byte("x")}, KV{Key: []byte("b"), Value: []byte("y")}, KV{Key: []byte("c"), Value: []byte("z")})
	v := viewApplication(t, s, 2)
	for _, n := range []int{66, 67, 68} {
		page, err := v.ScanReverse(t.Context(), nil, nil, nil, ReadBudget{Rows: 1, Bytes: n})
		if n == 66 {
			if !errors.Is(err, ErrLimit) || page.Rows != nil || page.Next != nil || page.Visited != 0 {
				t.Fatal(page, err)
			}
			continue
		}
		if err != nil || page.Complete || page.Visited != 1 || page.Bytes != 67 || string(page.Next) != "c" {
			t.Fatal(page, err)
		}
		reverseRows(t, page, "c=z")
	}
	page, err := v.ScanReverse(t.Context(), nil, nil, nil, ReadBudget{Rows: 3, Bytes: 133})
	if err != nil || page.Complete || page.Visited != 2 || page.Bytes != 133 || string(page.Next) != "b" || cap(page.Rows) != 2 {
		t.Fatal("independent byte stop", page, err)
	}
	reverseRows(t, page, "c=z", "b=y")
	page, err = v.ScanReverse(t.Context(), nil, nil, page.Next, ReadBudget{Rows: 3, Bytes: 67})
	if err != nil || !page.Complete || page.Visited != 1 || page.Bytes != 66 {
		t.Fatal(page, err)
	}
	reverseRows(t, page, "a=x")
	applyApplication(t, s, "deleted", KV{Key: []byte("c"), Deleted: true})
	deleted := viewApplication(t, s, 3)
	for _, n := range []int{65, 66} {
		page, err = deleted.ScanReverse(t.Context(), nil, nil, nil, ReadBudget{Rows: 1, Bytes: n})
		if n == 65 {
			if !errors.Is(err, ErrLimit) || page.Rows != nil {
				t.Fatal(page, err)
			}
		} else {
			if err != nil || page.Complete || page.Visited != 1 || page.Bytes != 66 || string(page.Next) != "c" {
				t.Fatal(page, err)
			}
			reverseRows(t, page)
		}
	}
	if s.poison != nil {
		t.Fatal("budget refusal poisoned store", s.poison)
	}
}

func TestApplicationScanReverseInvalidLifetimeAndCancellation(t *testing.T) {
	s := receiverStore(t, vfs.NewMem(), 1)
	generationApply(t, s, "rows", KV{Key: []byte("a"), Value: []byte("old")})
	v := viewApplication(t, s, 2)
	copyV := reflect.New(reflect.TypeFor[ApplicationView]()).Interface().(*ApplicationView)
	reflect.ValueOf(copyV).Elem().Set(reflect.ValueOf(v).Elem())
	unregistered := &ApplicationView{s: s, index: 2}
	unregistered.self = unregistered
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	p := s.ApplicationLimits()
	for _, tc := range []struct {
		v                    *ApplicationView
		ctx                  context.Context
		lower, upper, before []byte
		b                    ReadBudget
		want                 error
	}{
		{nil, t.Context(), nil, nil, nil, ReadBudget{1, 128}, ErrInvalid},
		{&ApplicationView{}, t.Context(), nil, nil, nil, ReadBudget{1, 128}, ErrInvalid},
		{copyV, t.Context(), nil, nil, nil, ReadBudget{1, 128}, ErrInvalid},
		{unregistered, t.Context(), nil, nil, nil, ReadBudget{1, 128}, ErrInvalid},
		{v, nil, nil, nil, nil, ReadBudget{1, 128}, ErrInvalid},
		{v, ctx, nil, nil, nil, ReadBudget{1, 128}, context.Canceled},
		{v, t.Context(), []byte("z"), []byte("a"), nil, ReadBudget{1, 128}, ErrInvalid},
		{v, t.Context(), []byte("a"), []byte("z"), []byte("0"), ReadBudget{1, 128}, ErrInvalid},
		{v, t.Context(), []byte("a"), []byte("z"), []byte("z"), ReadBudget{1, 128}, ErrInvalid},
		{v, t.Context(), nil, nil, nil, ReadBudget{}, ErrInvalid},
		{v, t.Context(), nil, nil, nil, ReadBudget{p.MaxPageRows + 1, 128}, ErrLimit},
		{v, t.Context(), nil, nil, nil, ReadBudget{1, p.MaxPageBytes + 1}, ErrLimit},
		{v, t.Context(), bytes.Repeat([]byte("a"), p.MaxKeyBytes+1), nil, nil, ReadBudget{1, 128}, ErrLimit},
		{v, t.Context(), nil, bytes.Repeat([]byte("z"), p.MaxKeyBytes+1), nil, ReadBudget{1, 128}, ErrLimit},
		{v, t.Context(), nil, nil, bytes.Repeat([]byte("z"), p.MaxKeyBytes+1), ReadBudget{1, 128}, ErrLimit},
	} {
		page, err := tc.v.ScanReverse(tc.ctx, tc.lower, tc.upper, tc.before, tc.b)
		if !errors.Is(err, tc.want) || page.Rows != nil || page.Next != nil || page.Complete || page.Bytes != 0 || page.Visited != 0 {
			t.Fatal(page, err, tc.want)
		}
	}
	if s.poison != nil {
		t.Fatal(s.poison)
	}
	v.mu.Lock()
	gen := v.generation
	v.generation++
	v.mu.Unlock()
	_, err := v.ScanReverse(t.Context(), nil, nil, nil, ReadBudget{1, 128})
	v.mu.Lock()
	v.generation = gen
	v.mu.Unlock()
	if !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := v.ScanReverse(ctx, nil, nil, nil, ReadBudget{1, 128}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	w := viewApplication(t, s, 2)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.ScanReverse(t.Context(), nil, nil, nil, ReadBudget{1, 128}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestApplicationScanReversePreparedAndRetiredBank(t *testing.T) {
	id := ApplicationSemanticContractID{41}
	s := semanticStore(t, vfs.NewMem(), id, 1)
	donor := semanticStore(t, vfs.NewMem(), id, 2)
	generationApply(t, s, "local", KV{Key: []byte("z"), Value: []byte("local")})
	old := viewApplication(t, s, 2)
	generationApply(t, donor, "incoming", KV{Key: []byte("a"), Value: []byte("incoming")})
	generationApply(t, donor, "tail")
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := donor.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := donor.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	manifest := buildPublished(t, export, ReadBudget{1, 4096})
	chunks := collectPublished(t, export, ReadBudget{1, 4096})
	if err := export.Close(); err != nil {
		t.Fatal(err)
	}
	p, snapshot := semanticPrepared(t, s, manifest, chunks)
	incoming, err := p.OpenReadView(t.Context(), 3, 8)
	if err != nil {
		t.Fatal(err)
	}
	page, err := incoming.ScanReverse(t.Context(), nil, nil, nil, ReadBudget{4, 1024})
	if err != nil || !page.Complete {
		t.Fatal(page, err)
	}
	reverseRows(t, page, "a=incoming")
	if err := incoming.Close(); err != nil {
		t.Fatal(err)
	}
	claim, err := p.Claim(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	raw, ready := readyMatchRaw(t, s, snapshot)
	root, err := s.PersistApplicationReady(ready, claim)
	if err != nil || root.Generation != 2 {
		t.Fatal(root, err)
	}
	raw.Advance(ready)
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	page, err = old.ScanReverse(t.Context(), nil, nil, nil, ReadBudget{4, 1024})
	if err != nil || !page.Complete {
		t.Fatal(page, err)
	}
	reverseRows(t, page, "z=local")
	current := viewApplication(t, s, root.Index)
	page, err = current.ScanReverse(t.Context(), nil, nil, nil, ReadBudget{4, 1024})
	if err != nil || !page.Complete {
		t.Fatal(page, err)
	}
	reverseRows(t, page, "a=incoming")
}

func TestApplicationScanReverseDeferredReadAndCorruption(t *testing.T) {
	for _, damaged := range []bool{false, true} {
		t.Run(fmt.Sprint(damaged), func(t *testing.T) {
			f := newLazyReadFixture(t, "scan")
			f.caller = "(*ApplicationView).ScanReverse"
			if damaged {
				if err := f.s.db.Set(f.key, []byte("damaged"), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			before := readyMetadataFingerprint(t, f.s)
			if !damaged {
				f.toggle.On()
			}
			page, err := f.v.ScanReverse(t.Context(), nil, nil, nil, ReadBudget{Rows: 4, Bytes: 4096})
			f.toggle.Off()
			want := f.cause
			if damaged {
				want = ErrCorrupt
			}
			if !errors.Is(err, want) || page.Rows != nil || page.Next != nil || page.Visited != 0 {
				t.Fatal(page, err)
			}
			if !damaged && (f.hits.Load() == 0 || errors.Is(err, ErrCorrupt)) {
				t.Fatal("deferred error classification", f.hits.Load(), err)
			}
			_, follow := f.s.LastIndex()
			if !errors.Is(follow, ErrPoisoned) || !errors.Is(follow, want) || readyMetadataFingerprint(t, f.s) != before {
				t.Fatal(follow)
			}
		})
	}
}

func TestApplicationScanReversePredecessorCost(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	writes := make([]KV, 2048)
	for i := range writes {
		writes[i] = KV{Key: []byte(fmt.Sprintf("%04d", i)), Value: []byte("old")}
	}
	at := applyApplication(t, s, "range", writes...)
	v := viewApplication(t, s, at)
	applyApplication(t, s, "later", KV{Key: []byte("1023"), Value: []byte("new")})
	page, err := v.ScanReverse(t.Context(), nil, []byte("1024"), nil, ReadBudget{Rows: 1, Bytes: 75})
	if err != nil || page.Complete || page.Visited != 1 || page.Bytes != 75 || string(page.Next) != "1023" {
		t.Fatal("predecessor scanned earlier range", page, err)
	}
	reverseRows(t, page, "1023=old")
}

func TestApplicationScanReverseConcurrentClose(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	applyApplication(t, s, "rows", KV{Key: []byte("a"), Value: []byte("old")})
	v := viewApplication(t, s, 2)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 16 {
				page, err := v.ScanReverse(t.Context(), nil, nil, nil, ReadBudget{1, 128})
				if err != nil {
					if !errors.Is(err, ErrClosed) {
						t.Error(err)
					}
				} else if len(page.Rows) != 1 || string(page.Rows[0].Value) != "old" || !page.Complete {
					t.Error(page)
				}
			}
		})
	}
	wg.Go(func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	})
	wg.Wait()
}
