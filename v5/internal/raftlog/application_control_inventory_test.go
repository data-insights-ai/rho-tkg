package raftlog

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// This storage-primitive fixture uses the existing unit Ready helper. It is
// not evidence of a public Driver election, quorum, or registered admission.
func TestControlInventoryFirstContract(t *testing.T) {
	s, _ := newControlStore(t, nil, 1)
	installControl(t, s, "r2", ApplicationControlPut{Key: []byte{'A', 0}, Value: []byte("reg")})
	installControl(t, s, "r3", ApplicationControlPut{Key: []byte("z"), Value: []byte{}})
	v := viewApplication(t, s, 3)
	root, err := v.RootBounded(t.Context(), 2)
	if err != nil || root.Index != 3 || !bytes.Equal(root.Image, []byte("r3")) {
		t.Fatalf("ordinary fixture root: %+v %v", root, err)
	}
	// Independently: control53+48=101; nine envelopes417; total518/11.
	wantUsage := ApplicationControlUsage{Bytes: 101, Records: 2, TotalBytes: 518, TotalRecords: 11}
	baseline, err := s.ApplicationControlUsage()
	if err != nil || baseline != wantUsage {
		t.Fatalf("ordinary fixture census: %+v %v", baseline, err)
	}
	// Full actual work:132+134+39+5+124+64+36+1+128+64=727.
	page, err := v.ControlPage(t.Context(), nil, ReadBudget{Rows: 2, Bytes: 727})
	if err != nil {
		t.Fatalf("valid current-view ControlPage contract missing: %v", err)
	}
	check := func(page ApplicationControlPage) {
		t.Helper()
		if !page.Complete || page.After != nil || len(page.Records) != 2 || cap(page.Records) != 2 || page.Work != (ApplicationControlReadWork{Rows: 2, Bytes: 727}) {
			t.Fatalf("independent complete-page contract: %+v", page)
		}
		want := []ApplicationControlRecord{
			{Key: []byte{'A', 0}, Value: []byte("reg"), Index: 2},
			{Key: []byte("z"), Value: []byte{}, Index: 3},
		}
		for i, row := range page.Records {
			if !bytes.Equal(row.Key, want[i].Key) || !bytes.Equal(row.Value, want[i].Value) || row.Index != want[i].Index || len(row.Key) != cap(row.Key) || len(row.Value) != cap(row.Value) {
				t.Fatalf("independent original tuple %d: %+v want %+v", i, row, want[i])
			}
		}
	}
	check(page)
	usage, err := v.ControlInventoryUsage(t.Context())
	if err != nil || usage != wantUsage {
		t.Fatalf("same-current-view census: %+v %v", usage, err)
	}
	page.Records[0].Key[0] = 'X'
	page.Records[0].Value[0] = 'X'
	again, err := v.ControlPage(t.Context(), nil, ReadBudget{Rows: 2, Bytes: 727})
	if err != nil {
		t.Fatalf("owned-copy reread: %v", err)
	}
	check(again)
	if err := v.Close(); err != nil {
		t.Fatalf("ordinary owned-view cleanup: %v", err)
	}
}

func inventoryFixture(t *testing.T) (*Store, *ApplicationView) {
	t.Helper()
	s, _ := newControlStore(t, nil, 1)
	installControl(t, s, "r2", ApplicationControlPut{Key: []byte{'A', 0}, Value: []byte("reg")})
	installControl(t, s, "r3", ApplicationControlPut{Key: []byte("z"), Value: []byte{}})
	return s, viewApplication(t, s, 3)
}

func inventoryZero(t *testing.T, page ApplicationControlPage, err, want error) {
	t.Helper()
	if !errors.Is(err, want) || page.Records != nil || page.After != nil || page.Complete {
		t.Fatalf("zero semantic page with %v: %+v %v", want, page, err)
	}
}

func TestControlInventorySelectedBudgetsAndContinuation(t *testing.T) {
	_, v := inventoryFixture(t)
	for _, tc := range []struct {
		bytes, records, work int
		complete             bool
		limit                bool
	}{
		{499, 0, 0, false, true}, {500, 1, 500, false, false}, {501, 1, 500, false, false},
		{726, 1, 537, false, false}, {727, 2, 727, true, false}, {728, 2, 727, true, false},
	} {
		page, err := v.ControlPage(t.Context(), nil, ReadBudget{Rows: 2, Bytes: tc.bytes})
		if tc.limit {
			inventoryZero(t, page, err, ErrLimit)
			continue
		}
		if err != nil || len(page.Records) != tc.records || page.Complete != tc.complete || page.Work != (ApplicationControlReadWork{Rows: 2, Bytes: tc.work}) {
			t.Fatalf("budget%d: %+v %v", tc.bytes, page, err)
		}
		if tc.complete && page.After != nil || !tc.complete && (!bytes.Equal(page.After, []byte{'A', 0}) || cap(page.After) != 2) {
			t.Fatalf("cursor%d: %+v", tc.bytes, page)
		}
	}
	for _, budget := range []int{376, 377, 378} {
		after := []byte{'A', 0}
		page, err := v.ControlPage(t.Context(), after, ReadBudget{Rows: 1, Bytes: budget})
		if budget == 376 {
			inventoryZero(t, page, err, ErrLimit)
			continue
		}
		if err != nil || !page.Complete || page.After != nil || len(page.Records) != 1 || !bytes.Equal(page.Records[0].Key, []byte("z")) || page.Records[0].Index != 3 || len(page.Records[0].Value) != 0 || page.Work != (ApplicationControlReadWork{Rows: 1, Bytes: 377}) {
			t.Fatal(page, err)
		}
		after[0] = 'X'
		if string(page.Records[0].Key) != "z" {
			t.Fatal("input alias")
		}
	}
	page, err := v.ControlPage(t.Context(), []byte("z"), ReadBudget{Rows: 1, Bytes: 144})
	if err != nil || !page.Complete || page.Records != nil || page.After != nil || page.Work != (ApplicationControlReadWork{Bytes: 144}) {
		t.Fatal(page, err)
	}
	page, err = v.ControlPage(t.Context(), nil, ReadBudget{Rows: 1, Bytes: 4096})
	inventoryZero(t, page, err, ErrLimit)
}

func TestControlInventoryFutureAndHistoricalCensus(t *testing.T) {
	s, _ := inventoryFixture(t)
	old := viewApplication(t, s, 1)
	for _, tc := range []struct {
		bytes, work int
		complete    bool
	}{{430, 0, false}, {431, 431, false}, {432, 431, false}, {466, 431, false}, {467, 465, true}, {468, 465, true}} {
		page, err := old.ControlPage(t.Context(), nil, ReadBudget{Rows: 2, Bytes: tc.bytes})
		if tc.bytes == 430 {
			inventoryZero(t, page, err, ErrLimit)
			continue
		}
		if err != nil || page.Records != nil || page.Complete != tc.complete || page.Work != (ApplicationControlReadWork{Rows: 2, Bytes: tc.work}) {
			t.Fatal(tc, page, err)
		}
		if tc.complete && page.After != nil || !tc.complete && !bytes.Equal(page.After, []byte{'A', 0}) {
			t.Fatal(page)
		}
	}
	usage, err := old.ControlInventoryUsage(t.Context())
	if !errors.Is(err, ErrInvalid) || usage != (ApplicationControlUsage{}) {
		t.Fatal(usage, err)
	}
	middle := viewApplication(t, s, 2)
	page, err := middle.ControlPage(t.Context(), nil, ReadBudget{Rows: 2, Bytes: 4096})
	if err != nil || !page.Complete || len(page.Records) != 1 || page.Records[0].Index != 2 || string(page.Records[0].Value) != "reg" {
		t.Fatal(page, err)
	}
	if s.poison != nil {
		t.Fatal("historical refusal poisoned store")
	}
}

func TestControlInventoryArgumentAndOwnerRefusals(t *testing.T) {
	s, v := inventoryFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		view   *ApplicationView
		ctx    context.Context
		after  []byte
		budget ReadBudget
		want   error
	}{
		{nil, t.Context(), nil, ReadBudget{Rows: 2, Bytes: 4096}, ErrInvalid},
		{&ApplicationView{}, t.Context(), nil, ReadBudget{Rows: 2, Bytes: 4096}, ErrInvalid},
		{v, nil, nil, ReadBudget{Rows: 2, Bytes: 4096}, ErrInvalid},
		{v, ctx, nil, ReadBudget{Rows: 2, Bytes: 4096}, context.Canceled},
		{v, t.Context(), []byte{}, ReadBudget{Rows: 2, Bytes: 4096}, ErrInvalid},
		{v, t.Context(), nil, ReadBudget{}, ErrInvalid},
		{v, t.Context(), nil, ReadBudget{Rows: v.ReadLimits().Rows + 1, Bytes: 4096}, ErrLimit},
		{v, t.Context(), nil, ReadBudget{Rows: 2, Bytes: v.ReadLimits().Bytes + 1}, ErrLimit},
		{v, t.Context(), bytes.Repeat([]byte{'x'}, s.ApplicationLimits().MaxKeyBytes+1), ReadBudget{Rows: 2, Bytes: 4096}, ErrLimit},
	} {
		page, err := tc.view.ControlPage(tc.ctx, tc.after, tc.budget)
		inventoryZero(t, page, err, tc.want)
	}
	before, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	// Runtime copy refusal is tested with an idle mutex; this is not production copying.
	copied := reflect.New(reflect.TypeFor[ApplicationView]()).Interface().(*ApplicationView)
	reflect.ValueOf(copied).Elem().Set(reflect.ValueOf(v).Elem())
	page, err := copied.ControlPage(t.Context(), nil, ReadBudget{Rows: 2, Bytes: 4096})
	inventoryZero(t, page, err, ErrInvalid)
	if err := copied.Close(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := copied.RootBounded(t.Context(), 2); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	// Identity alone is insufficient: this real-Store handle was never registered.
	unregistered := &ApplicationView{s: s, index: 3}
	unregistered.self = unregistered
	page, err = unregistered.ControlPage(t.Context(), nil, ReadBudget{Rows: 2, Bytes: 4096})
	inventoryZero(t, page, err, ErrInvalid)
	if err := unregistered.Close(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	usage, err := unregistered.ControlInventoryUsage(t.Context())
	if !errors.Is(err, ErrInvalid) || usage != (ApplicationControlUsage{}) {
		t.Fatal(usage, err)
	}
	after, err := s.ApplicationUsage()
	if err != nil || before != after {
		t.Fatal(before, after, err)
	}
	for _, bad := range []*ApplicationView{nil, {}, copied} {
		u, err := bad.ControlInventoryUsage(t.Context())
		if !errors.Is(err, ErrInvalid) || u != (ApplicationControlUsage{}) {
			t.Fatal(u, err)
		}
	}
	//nolint:staticcheck // SA1012: exercise the intentional nil-context rejection contract.
	u, err := v.ControlInventoryUsage(nil)
	if !errors.Is(err, ErrInvalid) || u != (ApplicationControlUsage{}) {
		t.Fatal(u, err)
	}
	u, err = v.ControlInventoryUsage(ctx)
	if !errors.Is(err, context.Canceled) || u != (ApplicationControlUsage{}) {
		t.Fatal(u, err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	page, err = v.ControlPage(t.Context(), nil, ReadBudget{Rows: 2, Bytes: 4096})
	inventoryZero(t, page, err, ErrClosed)
	u, err = v.ControlInventoryUsage(t.Context())
	if !errors.Is(err, ErrClosed) || u != (ApplicationControlUsage{}) {
		t.Fatal(u, err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	legacy := receiverStore(t, vfs.NewMem(), 1)
	lv := viewApplication(t, legacy, 1)
	page, err = lv.ControlPage(t.Context(), nil, ReadBudget{Rows: 1, Bytes: 4096})
	inventoryZero(t, page, err, ErrInvalid)
	u, err = lv.ControlInventoryUsage(t.Context())
	if !errors.Is(err, ErrInvalid) || u != (ApplicationControlUsage{}) {
		t.Fatal(u, err)
	}
}

func TestControlInventoryStockCorruptionNeverReturnsPartial(t *testing.T) {
	for _, kind := range []string{"checksum", "tombstone", "duplicate", "future-through"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := newControlStore(t, nil, 1)
			installControl(t, s, "r2", ApplicationControlPut{Key: []byte{'A', 0}, Value: []byte("reg")})
			installControl(t, s, "r3", ApplicationControlPut{Key: []byte("z"), Value: []byte{}})
			key := controlVersionKey(0, []byte{'A', 0}, 2)
			raw := appFrame(key, []byte("reg"), false)
			switch kind {
			case "checksum":
				raw[4] ^= 1
			case "tombstone":
				raw = appFrame(key, nil, true)
			case "duplicate":
				key = controlVersionKey(0, []byte{'A', 0}, 3)
				raw = appFrame(key, []byte("other"), false)
			case "future-through":
				key = controlVersionKey(0, []byte{'A', 0}, 4)
				raw = appFrame(key, nil, false)
			}
			// Exactly the already reviewed stock static corruption family; no IO hook.
			if err := s.db.Set(key, raw, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			v, err := s.ApplicationView(3)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "duplicate" {
				page, err := v.ControlPage(t.Context(), nil, ReadBudget{Rows: 2, Bytes: 449})
				inventoryZero(t, page, err, ErrLimit)
				if s.poison != nil {
					t.Fatal("unadmitted duplicate poisoned store")
				}
			}
			budget := 4096
			if kind == "duplicate" {
				budget = 450
			}
			page, err := v.ControlPage(t.Context(), nil, ReadBudget{Rows: 2, Bytes: budget})
			inventoryZero(t, page, err, ErrCorrupt)
			if kind == "duplicate" && page.Work != (ApplicationControlReadWork{Rows: 2, Bytes: 448}) {
				t.Fatal(page.Work)
			}
			if _, err := s.ApplicationUsage(); !errors.Is(err, ErrPoisoned) || !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
			if err := v.Close(); !errors.Is(err, ErrPoisoned) || !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
		})
	}
}

func TestControlInventoryStructureAllowances(t *testing.T) {
	if unsafe.Sizeof(ApplicationControlRecord{}) > 64 {
		t.Fatal("record slot exceeds admitted64")
	}
	if unsafe.Sizeof(ApplicationView{})+unsafe.Sizeof(generationRef{})+2*unsafe.Sizeof(uintptr(0)) > 1024 {
		t.Fatal("read owner metadata exceeds fixed1024")
	}
}

// These are additional pre-fix guards against the source-reviewed cursor bug.
// Public stock storage installs remain unit evidence, never quorum authority.
func TestControlInventoryRetainsCandidateCursorThroughGrowth(t *testing.T) {
	t.Run("first-candidate", func(t *testing.T) {
		_, v := inventoryFixture(t)
		page, err := v.ControlPage(t.Context(), nil, ReadBudget{Rows: 2, Bytes: 499})
		inventoryZero(t, page, err, ErrLimit)
		if page.Work != (ApplicationControlReadWork{Rows: 2, Bytes: 434}) {
			t.Fatalf("premature first backing growth: Work%+v, want Rows2/Bytes434", page.Work)
		}
	})
	t.Run("prior-safe-long-candidate", func(t *testing.T) {
		s, _ := newControlStore(t, nil, 1)
		installControl(t, s, "r2", ApplicationControlPut{Key: []byte{'A', 0}, Value: []byte("reg")})
		installControl(t, s, "r3", ApplicationControlPut{Key: bytes.Repeat([]byte{'B'}, 100), Value: []byte{}})
		installControl(t, s, "r4", ApplicationControlPut{Key: []byte("z"), Value: []byte{}})
		v := viewApplication(t, s, 4)
		usage, err := s.ApplicationControlUsage()
		if err != nil || usage != (ApplicationControlUsage{Bytes: 248, Records: 3, TotalBytes: 804, TotalRecords: 15}) {
			t.Fatalf("three-key actual unit fixture: %+v %v", usage, err)
		}
		// Work1253 plus candidate cursor100 must refuse growth192. Keep
		// earlier A only and copy final AfterA2 once: reported Work1255.
		page, err := v.ControlPage(t.Context(), nil, ReadBudget{Rows: 3, Bytes: 1447})
		if err != nil || page.Complete || page.Work != (ApplicationControlReadWork{Rows: 3, Bytes: 1255}) || len(page.Records) != 1 || cap(page.Records) != 1 || !bytes.Equal(page.After, []byte{'A', 0}) || cap(page.After) != 2 {
			t.Fatalf("candidate cursor lost prior safe page: %+v %v", page, err)
		}
		row := page.Records[0]
		if !bytes.Equal(row.Key, []byte{'A', 0}) || !bytes.Equal(row.Value, []byte("reg")) || row.Index != 2 {
			t.Fatalf("returned an uncommitted candidate: %+v", row)
		}
		if s.poison != nil {
			t.Fatal("quota refusal poisoned unit store")
		}
	})
}
