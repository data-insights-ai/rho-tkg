package apiutil

import (
	"context"
	"errors"
	"testing"
)

func TestCloneSlice(t *testing.T) {
	t.Parallel()
	if got := CloneSlice[string](nil); got != nil {
		t.Fatalf("CloneSlice(nil) = %v, want nil", got)
	}
	in := []string{"a", "b"}
	out := CloneSlice(in)
	if len(out) != len(in) {
		t.Fatalf("CloneSlice length = %d, want %d", len(out), len(in))
	}
	out[0] = "mutated"
	if in[0] == "mutated" {
		t.Fatal("CloneSlice did not return an independent copy")
	}
}

func TestCloneMap(t *testing.T) {
	t.Parallel()
	if got := CloneMap[string, int](nil); got != nil {
		t.Fatalf("CloneMap(nil) = %v, want nil", got)
	}
	in := map[string]int{"a": 1, "b": 2}
	out := CloneMap(in)
	if len(out) != len(in) {
		t.Fatalf("CloneMap length = %d, want %d", len(out), len(in))
	}
	out["a"] = 99
	if in["a"] == 99 {
		t.Fatal("CloneMap did not return an independent copy")
	}
}

func TestIterateForEach_YieldsAllValues(t *testing.T) {
	t.Parallel()
	scan := func(fn func(int) bool) error {
		for _, v := range []int{1, 2, 3} {
			if !fn(v) {
				return nil
			}
		}
		return nil
	}
	var got []int
	IterateForEach(context.Background(), func(v int, err error) bool {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got = append(got, v)
		return true
	}, scan)
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("got %v, want [1 2 3]", got)
	}
}

func TestIterateForEach_EarlyStopViaYieldFalse(t *testing.T) {
	t.Parallel()
	scanCalls := 0
	scan := func(fn func(int) bool) error {
		for _, v := range []int{1, 2, 3, 4, 5} {
			scanCalls++
			if !fn(v) {
				return nil
			}
		}
		return nil
	}
	var got []int
	IterateForEach(context.Background(), func(v int, err error) bool {
		got = append(got, v)
		return v < 2 // stop after yielding 2
	}, scan)
	if len(got) != 2 {
		t.Fatalf("yielded %d values, want 2 (early stop)", len(got))
	}
	if scanCalls != 2 {
		t.Fatalf("scan invoked fn %d times, want exactly 2 (must not overrun after stop)", scanCalls)
	}
}

func TestIterateForEach_PreCanceledContextYieldsOnceAndNeverScans(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	scanned := false
	yieldCalls := 0
	IterateForEach(ctx, func(v int, err error) bool {
		yieldCalls++
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		return true
	}, func(fn func(int) bool) error {
		scanned = true
		return nil
	})
	if scanned {
		t.Fatal("scan was invoked despite a pre-canceled context")
	}
	if yieldCalls != 1 {
		t.Fatalf("yield called %d times, want exactly 1", yieldCalls)
	}
}

func TestIterateForEach_CancellationMidScanStopsAndYieldsOnce(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	yieldCalls := 0
	scanFnCalls := 0
	IterateForEach(ctx, func(v int, err error) bool {
		yieldCalls++
		if yieldCalls == 1 {
			if err != nil {
				t.Fatalf("first yield: unexpected error %v", err)
			}
			return true
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("second yield: err = %v, want context.Canceled", err)
		}
		return true
	}, func(fn func(int) bool) error {
		for _, v := range []int{1, 2, 3, 4} {
			scanFnCalls++
			if scanFnCalls == 2 {
				cancel() // cancel after the first value is delivered
			}
			if !fn(v) {
				return nil
			}
		}
		return nil
	})
	if yieldCalls != 2 {
		t.Fatalf("yield called %d times, want exactly 2 (1 value + 1 cancellation)", yieldCalls)
	}
}

func TestIterateForEach_ScanErrorSurfacedOnceAtEnd(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("scan failed")
	scan := func(fn func(int) bool) error {
		fn(1)
		return wantErr
	}
	var errs []error
	var vals []int
	IterateForEach(context.Background(), func(v int, err error) bool {
		if err != nil {
			errs = append(errs, err)
		} else {
			vals = append(vals, v)
		}
		return true
	}, scan)
	if len(vals) != 1 || vals[0] != 1 {
		t.Fatalf("vals = %v, want [1]", vals)
	}
	if len(errs) != 1 || !errors.Is(errs[0], wantErr) {
		t.Fatalf("errs = %v, want exactly [%v]", errs, wantErr)
	}
}
