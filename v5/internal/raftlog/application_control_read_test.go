package raftlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

func TestControlHistoricalOwnedWorkAndRefusals(t *testing.T) {
	s, _ := newControlStore(t, nil, 1)
	old := viewApplication(t, s, 1)
	installControl(t, s, "seed", ApplicationControlPut{Key: []byte("r"), Value: []byte("reg")}, ApplicationControlPut{Key: []byte("z"), Value: []byte{}})
	now := viewApplication(t, s, 2)
	row, found, work, err := now.GetControl(t.Context(), []byte("r"), ReadBudget{2, 4096})
	if err != nil || !found || string(row.Value) != "reg" || work.Rows < 1 || work.Rows > 2 || work.Bytes < 3 || len(row.Value) != cap(row.Value) || len(row.Key) != cap(row.Key) {
		t.Fatal(row, found, work, err)
	}
	row.Value[0] = 'X'
	row.Key[0] = 'X'
	row, found, _, err = now.GetControl(t.Context(), []byte("r"), ReadBudget{2, 4096})
	if err != nil || !found || string(row.Value) != "reg" {
		t.Fatal("mutable alias", row, err)
	}
	for _, key := range []string{"r", "z", "phantom"} {
		row, found, w, err := old.GetControl(t.Context(), []byte(key), ReadBudget{2, 4096})
		if err != nil || found || row.Value != nil || key != "phantom" && w.Rows < 1 {
			t.Fatal("old/future miss", key, row, found, w, err)
		}
	}
	row, found, _, err = now.GetControl(t.Context(), []byte("z"), ReadBudget{2, 4096})
	if err != nil || !found || len(row.Value) != 0 {
		t.Fatal("empty value became absence", row, found, err)
	}
	for _, b := range []ReadBudget{{}, {Rows: 1, Bytes: 1}, {Rows: s.meta.App.Policy.MaxPageRows + 1, Bytes: 4096}, {Rows: 2, Bytes: s.meta.App.Policy.MaxPageBytes + 1}} {
		row, found, _, err := now.GetControl(t.Context(), []byte("r"), b)
		if !errors.Is(err, ErrLimit) && !errors.Is(err, ErrInvalid) || found || row.Value != nil || s.poison != nil {
			t.Fatal(b, row, found, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, _, err := now.GetControl(ctx, []byte("r"), ReadBudget{2, 4096}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := now.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := now.GetControl(t.Context(), []byte("r"), ReadBudget{2, 4096}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	var absent *ApplicationView
	var store *Store
	if _, _, _, err := absent.GetControl(t.Context(), []byte("r"), ReadBudget{2, 4096}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := store.ApplicationControlUsage(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if store.ApplicationControls() != (ApplicationControlConfig{}) {
		t.Fatal("nil config")
	}
	if _, err := ApplicationContractForPolicyWithControls(DefaultApplicationPolicy(1), ApplicationControlConfig{Version: 2}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
func TestControlStoredCorruptionNeverPartialOrCallerError(t *testing.T) {
	for _, kind := range []string{"hash", "tombstone", "duplicate", "future"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := newControlStore(t, nil, 1)
			installControl(t, s, "seed", ApplicationControlPut{Key: []byte("r"), Value: []byte("reg")})
			if kind == "duplicate" {
				installControl(t, s, "seed", ApplicationControlPut{Key: []byte("x"), Value: nil})
			}
			at, _, err := s.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			v := viewApplication(t, s, at)
			k := controlVersionKey(0, []byte("r"), 2)
			value := appFrame(k, []byte("reg"), false)
			switch kind {
			case "hash":
				value[4] ^= 1
			case "tombstone":
				value = appFrame(k, nil, true)
			case "duplicate":
				k = controlVersionKey(0, []byte("r"), 3)
				value = appFrame(k, []byte("other"), false)
			case "future":
				k = controlVersionKey(0, []byte("r"), 4)
				value = appFrame(k, nil, false)
			}
			if err := s.db.Set(k, value, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			row, found, _, err := v.GetControl(t.Context(), []byte("r"), ReadBudget{4, 4096})
			if !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrInvalid) || found || row.Value != nil {
				t.Fatal(row, found, err)
			}
			if _, err := s.ApplicationUsage(); !errors.Is(err, ErrPoisoned) {
				t.Fatal(err)
			}
			if err := v.Close(); !errors.Is(err, ErrPoisoned) || !errors.Is(err, ErrCorrupt) {
				t.Fatal("poisoned view cleanup lost original corruption", err)
			}
		})
	}
}
func TestControlConcurrentCloseLeavesNoUnboundedOwner(t *testing.T) {
	s, _ := newControlStore(t, nil, 1)
	installControl(t, s, "seed", ApplicationControlPut{Key: []byte("r"), Value: []byte("reg")})
	v := viewApplication(t, s, 2)
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			for range 16 {
				row, found, _, err := v.GetControl(t.Context(), []byte("r"), ReadBudget{2, 4096})
				if err == nil && (!found || !bytes.Equal(row.Value, []byte("reg"))) || err != nil && !errors.Is(err, ErrClosed) {
					t.Error(row, found, err)
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

func TestControlDuplicateKeyChargesSecondObservedKeyBeforeClassification(t *testing.T) {
	for _, short := range []bool{true, false} {
		t.Run(map[bool]string{true: "one-short", false: "exact-fit"}[short], func(t *testing.T) {
			s, _ := newControlStore(t, nil, 1)
			installControl(t, s, "seed", ApplicationControlPut{Key: []byte("r"), Value: []byte("reg")})
			installControl(t, s, "seed", ApplicationControlPut{Key: []byte("x"), Value: nil})
			// This ordinary static corruption has a genuine preceding committed
			// index3. The selected prefix encounters r@3 first, then r@2.
			key := controlVersionKey(0, []byte("r"), 3)
			if err := s.db.Set(key, appFrame(key, []byte("other"), false), pebble.Sync); err != nil {
				t.Fatal(err)
			}
			v, err := s.ApplicationView(3)
			if err != nil {
				t.Fatal(err)
			}
			// Independently: fixed128+4*12=176; first12+41+64=117;
			// second examined physical key12+64=76. Total369, two rows.
			budget := 369
			if short {
				budget--
			}
			row, found, work, err := v.GetControl(t.Context(), []byte("r"), ReadBudget{2, budget})
			closeErr := v.Close()
			if found || row.Key != nil || row.Value != nil || row.Index != 0 {
				t.Fatal("refusal returned partial original", row, found, err)
			}
			if short {
				if !errors.Is(err, ErrLimit) || errors.Is(err, ErrCorrupt) || s.poison != nil || closeErr != nil || work.Bytes > budget || work.Rows > 2 {
					t.Fatal("unbudgeted corruption examination", work, err, closeErr)
				}
			} else if !errors.Is(err, ErrCorrupt) || work != (ApplicationControlReadWork{2, 369}) || !errors.Is(closeErr, ErrPoisoned) || !errors.Is(closeErr, ErrCorrupt) {
				t.Fatal("second examined key was not fully charged", work, err, closeErr)
			}
		})
	}
}

func TestControlLargeKeyQuotaRefusalPrecedesPrefixAllocation(t *testing.T) {
	s, _ := newControlStore(t, nil, 1)
	v := viewApplication(t, s, 1)
	key := make([]byte, 1024)
	// Every zero escapes to two bytes: physical key 1+2048+2+8=2059.
	// Fixed admission is128+4*2059=8364, without any stored rows.
	row, found, work, err := v.GetControl(t.Context(), key, ReadBudget{2, 8363})
	if !errors.Is(err, ErrLimit) || found || row.Key != nil || row.Value != nil || row.Index != 0 || work != (ApplicationControlReadWork{}) || s.poison != nil {
		t.Fatal("one-short caller refusal", row, found, work, err)
	}
	ctx := t.Context()
	allocations := testing.AllocsPerRun(3, func() {
		row, found, work, err = v.GetControl(ctx, key, ReadBudget{2, 8363})
	})
	if allocations != 0 || !errors.Is(err, ErrLimit) || found || row.Key != nil || row.Value != nil || work != (ApplicationControlReadWork{}) || s.poison != nil {
		t.Fatal("quota refusal allocated physical-prefix state", allocations, work, err)
	}
	row, found, work, err = v.GetControl(ctx, key, ReadBudget{2, 8364})
	if err != nil || found || row.Key != nil || row.Value != nil || work != (ApplicationControlReadWork{0, 8364}) || s.poison != nil {
		t.Fatal("exact-fit bounded absence", row, found, work, err)
	}
}

// This context observes existing per-frame cancellation checks, not elapsed
// time, allocations or database I/O. It is concurrency-safe and cancellation
// closes Done permanently before Err reports context.Canceled.
type controlCountedContext struct {
	context.Context
	mu           sync.Mutex
	done         chan struct{}
	after, calls int
	canceled     bool
}

func (c *controlCountedContext) Done() <-chan struct{} { return c.done }
func (c *controlCountedContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.canceled {
		return context.Canceled
	}
	c.calls++
	if c.calls >= c.after {
		c.canceled = true
		close(c.done)
		return context.Canceled
	}
	return nil
}
func (c *controlCountedContext) observations() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestControlScrubRejectsAggregateOverflowBeforeUnboundedIllegalHistory(t *testing.T) {
	for _, records := range []bool{true, false} {
		t.Run(fmt.Sprintf("records=%v", records), func(t *testing.T) {
			s := boundedControlStore(t, func(c *Config) {
				if records {
					c.Application.RetainedApplicationRecords = 8
				} else {
					c.Application.RetainedApplicationBytes = 3328
				}
			})
			installControl(t, s, "seed", ApplicationControlPut{Key: []byte{'A', 0}, Value: []byte("reg")})
			u, err := s.ApplicationControlUsage()
			if err != nil || u != (ApplicationControlUsage{53, 1, 333, 7}) {
				t.Fatal("actual bounded baseline", u, err)
			}
			value := []byte(nil)
			bound := 10
			if !records {
				value = bytes.Repeat([]byte{'v'}, 100)
				bound = 28
			}
			for n := range 40 {
				key := controlVersionKey(0, []byte(fmt.Sprintf("z%03d", n)), 2)
				if err := s.db.Set(key, appFrame(key, value, false), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			before := readyMetadataFingerprint(t, s)
			// Records: baseline7 + second extra row exceeds legal8: nine
			// visited rows + initial check =10. Bytes: each extra frame costs
			// physical15+frame136=151;333+20*151=3353>3328:27 rows+1=28.
			ctx := &controlCountedContext{Context: context.Background(), done: make(chan struct{}), after: bound + 1}
			err = s.ScrubApplication(ctx)
			if !errors.Is(err, ErrCorrupt) || errors.Is(err, context.Canceled) || ctx.observations() != bound {
				t.Fatal("aggregate cap failed to stop visited work", ctx.observations(), bound, err)
			}
			if readyMetadataFingerprint(t, s) != before {
				t.Fatal("scrub changed persisted metadata")
			}
			if _, err := s.ApplicationControlUsage(); !errors.Is(err, ErrPoisoned) || !errors.Is(err, ErrCorrupt) {
				t.Fatal("corruption did not poison", err)
			}
		})
	}
}

func TestControlPublicReadAndUsageNilDisabledClosedErrors(t *testing.T) {
	s, cfg := newControlStore(t, nil, 1)
	v := viewApplication(t, s, 1)
	for _, tc := range []struct {
		ctx  context.Context
		key  []byte
		want error
	}{
		{nil, []byte("r"), ErrInvalid},
		{t.Context(), nil, ErrInvalid},
		{t.Context(), make([]byte, 1025), ErrLimit},
	} {
		row, found, work, err := v.GetControl(tc.ctx, tc.key, ReadBudget{2, 4096})
		if !errors.Is(err, tc.want) || found || row.Key != nil || row.Value != nil || work != (ApplicationControlReadWork{}) || s.poison != nil {
			t.Fatal(row, found, work, err)
		}
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if out, err := s.ApplicationControlUsage(); out != (ApplicationControlUsage{}) || !errors.Is(err, ErrClosed) {
		t.Fatal(out, err)
	}
	if s.ApplicationControls() != cfg.Controls {
		t.Fatal("immutable configuration changed on closure")
	}
	legacy := semanticStore(t, cfg.FS, ApplicationSemanticContractID{8}, 2)
	if out, err := legacy.ApplicationControlUsage(); out != (ApplicationControlUsage{}) || !errors.Is(err, ErrInvalid) {
		t.Fatal(out, err)
	}
	row, found, work, err := viewApplication(t, legacy, 1).GetControl(t.Context(), []byte("r"), ReadBudget{2, 4096})
	if !errors.Is(err, ErrInvalid) || found || row.Key != nil || row.Value != nil || work != (ApplicationControlReadWork{}) {
		t.Fatal(row, found, work, err)
	}
}

func TestControlScrubOversizedStoredKeyRejectsBeforeCanonicalCopy(t *testing.T) {
	s, _ := newControlStore(t, nil, 1)
	if err := s.ScrubApplication(t.Context()); err != nil {
		t.Fatal("warm valid store", err)
	}
	// Ordinary static malformed data. The legal aggregate cap exceeds the
	// complete frame, so quota cannot mask the selected key's shape refusal.
	key := bytes.Repeat([]byte{'x'}, 256<<10)
	key[0] = 18
	if err := s.db.Set(key, appFrame(key, nil, false), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if s.meta.App.Policy.RetainedApplicationBytes <= 139+(256<<10)+36 {
		t.Fatal("aggregate quota masks selected malformed key")
	}
	if err := s.ScrubApplication(t.Context()); !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrLimit) {
		t.Fatal("stored shape classification", err)
	}
	if out, err := s.ApplicationControlUsage(); out != (ApplicationControlUsage{}) || !errors.Is(err, ErrPoisoned) || !errors.Is(err, ErrCorrupt) {
		t.Fatal("corruption output or poison", out, err)
	}
}

func TestControlScrubControlKeyAdmissionRejectsOversizedWithoutAllocation(t *testing.T) {
	// Allocate the static input before measuring the actual production boundary.
	key := bytes.Repeat([]byte{'x'}, 256<<10)
	key[0] = 18
	var canonical []byte
	var err error
	allocations := testing.AllocsPerRun(1, func() {
		canonical, err = admitScrubControlKey(0, key, nil, 1024, 1)
	})
	if allocations != 0 || canonical != nil || !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrLimit) {
		t.Fatal("oversized stored key admission allocated or returned partial state", allocations, canonical, err)
	}
}

func TestControlScrubControlKeyAdmissionPreservesValidationAndOwnership(t *testing.T) {
	// Literal r@2 physical/canonical bytes; the final suffix is ^uint64(2).
	canonical := []byte{18, 'r', 0, 0, 255, 255, 255, 255, 255, 255, 255, 253}
	for _, tc := range []struct {
		name     string
		bank     byte
		key      []byte
		previous []byte
		applied  uint64
		want     []byte
	}{
		{"bank-a", 0, canonical, nil, 2, canonical},
		{"bank-b", 1, []byte{19, 'r', 0, 0, 255, 255, 255, 255, 255, 255, 255, 253}, nil, 2, canonical},
		{"future", 0, canonical, nil, 1, nil},
		{"duplicate", 0, []byte{18, 'r', 0, 0, 255, 255, 255, 255, 255, 255, 255, 252}, canonical, 3, nil},
		{"invalid-index", 0, []byte{18, 'r', 0, 0, 255, 255, 255, 255, 255, 255, 255, 254}, nil, 2, nil},
		{"bad-escape", 0, []byte{18, 'r', 0, 1, 255, 255, 255, 255, 255, 255, 255, 253}, nil, 2, nil},
		{"empty", 0, nil, nil, 2, nil},
		{"wrong-bank", 1, canonical, nil, 2, nil},
		{"invalid-bank", 2, canonical, nil, 2, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := admitScrubControlKey(tc.bank, tc.key, tc.previous, 1024, tc.applied)
			if tc.want == nil {
				if out != nil || !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrLimit) {
					t.Fatal("invalid admission returned partial state", out, err)
				}
				return
			}
			if err != nil || !bytes.Equal(out, tc.want) || len(out) != cap(out) {
				t.Fatal("canonical owned admission", out, err)
			}
			out[1] = 'X'
			if tc.key[1] != 'r' || canonical[1] != 'r' {
				t.Fatal("owned result aliases borrowed input or previous canonical key")
			}
		})
	}
}
