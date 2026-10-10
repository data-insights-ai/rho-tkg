package raftlog

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Adapt the existing public stock storage fixtures with configuration established
// BEFORE Open. Unit Ready/export/import inputs grant no real quorum/R0 evidence.
func preparedReadFixture(t *testing.T, version int, edit func(*Config)) (*Store, *PreparedApplicationSnapshot, *pb.Snapshot) {
	t.Helper()
	open := func(voter uint64, receiver bool) *Store {
		p, tc := transferConfig(voter)
		p.MaxImageBytes = 8
		tc.Contract = ApplicationContractForPolicy(p)
		cfg := Config{Dir: "db", FS: vfs.NewMem(), Create: true, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: ApplicationPublishedCutLimits{MaxTransferChunks: 10000}, Replication: ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}}
		if version >= 3 {
			cfg.SemanticContractID = ApplicationSemanticContractID{41}
		}
		if version == 4 {
			cfg.Controls = ApplicationControlConfig{Version: 1}
			contract, err := ApplicationContractForPolicyWithControls(p, cfg.Controls)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Transfer.Contract = contract
		}
		if receiver && edit != nil {
			edit(&cfg)
		}
		s, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		image := "initial"
		if version == 4 {
			image = "seed"
		}
		if err := s.Initialize([]uint64{1, 2, 3}, []byte(image)); err != nil {
			t.Fatal(err)
		}
		return s
	}
	donor := open(2, false)
	receiver := open(1, true)
	if version == 4 {
		installControl(t, donor, "r2", ApplicationControlPut{Key: []byte{'A', 0}, Value: []byte("reg")})
		installControl(t, donor, "r3", ApplicationControlPut{Key: []byte("z"), Value: []byte{}})
	} else {
		generationApply(t, donor, "incoming", KV{Key: []byte("a"), Value: []byte("incoming")})
	}
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
	defer export.Close()
	manifest := buildPublished(t, export, ReadBudget{Rows: 1, Bytes: 4096})
	if manifest.Version != uint32(version) {
		t.Fatal(manifest.Version)
	}
	chunks := collectPublished(t, export, ReadBudget{Rows: 1, Bytes: 4096})
	prepared, snapshot := semanticPrepared(t, receiver, manifest, chunks)
	return receiver, prepared, snapshot
}

func TestPreparedReadViewActualAS2AS3AS4Reads(t *testing.T) {
	for _, version := range []int{2, 3, 4} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			s, p, snapshot := preparedReadFixture(t, version, nil)
			at := snapshot.GetMetadata().GetIndex()
			v, err := p.OpenReadView(t.Context(), at, 8)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := v.Close(); err != nil {
					t.Error(err)
				}
			})
			root, err := v.RootBounded(t.Context(), 8)
			wantImage := "incoming"
			if version == 4 {
				wantImage = "r3"
			}
			if err != nil || root.Index != at || root.Generation != 2 || string(root.Image) != wantImage || cap(root.Image) != len(root.Image) {
				t.Fatal(root, err)
			}
			binding, err := v.ApplicationBinding(t.Context())
			if err != nil || binding != s.ApplicationBinding() {
				t.Fatal(binding, err)
			}
			row, found, err := v.Get(t.Context(), []byte("a"), 16)
			if err != nil || version < 4 && (!found || string(row.Value) != "incoming") || version == 4 && (found || row.Key != nil || row.Value != nil) {
				t.Fatal(row, found, err)
			}
			page, err := v.Scan(t.Context(), nil, nil, nil, ReadBudget{Rows: 4, Bytes: 4096})
			if err != nil || !page.Complete || version < 4 && len(page.Rows) != 1 || version == 4 && len(page.Rows) != 0 {
				t.Fatal(page, err)
			}
			control, present, _, err := v.GetControl(t.Context(), []byte{'A', 0}, ReadBudget{Rows: 2, Bytes: 4096})
			if version < 4 {
				if !errors.Is(err, ErrInvalid) || present || control.Key != nil {
					t.Fatal(control, present, err)
				}
				inventory, err := v.ControlPage(t.Context(), nil, ReadBudget{Rows: 2, Bytes: 4096})
				inventoryZero(t, inventory, err, ErrInvalid)
				usage, err := v.ControlInventoryUsage(t.Context())
				if !errors.Is(err, ErrInvalid) || usage != (ApplicationControlUsage{}) {
					t.Fatal(usage, err)
				}
			} else {
				if err != nil || !present || control.Index != 2 || string(control.Value) != "reg" {
					t.Fatal(control, present, err)
				}
				inventory, err := v.ControlPage(t.Context(), nil, ReadBudget{Rows: 2, Bytes: 727})
				if err != nil || !inventory.Complete || len(inventory.Records) != 2 || inventory.Records[1].Index != 3 || inventory.Work.Bytes != 727 {
					t.Fatal(inventory, err)
				}
				usage, err := v.ControlInventoryUsage(t.Context())
				if err != nil || usage != (ApplicationControlUsage{Bytes: 101, Records: 2, TotalBytes: 518, TotalRecords: 11}) {
					t.Fatal(usage, err)
				}
			}
			if root, err := v.ProveNoApplicationData(t.Context()); !errors.Is(err, ErrInvalid) || root.Image != nil || root.Index != 0 {
				t.Fatal(root, err)
			}
			old, err := p.OpenReadView(t.Context(), 1, 8)
			if err != nil {
				t.Fatal(err)
			}
			oldRoot, err := old.RootBounded(t.Context(), 8)
			wantOld := "initial"
			if version == 4 {
				wantOld = "seed"
			}
			if err != nil || oldRoot.Index != 1 || string(oldRoot.Image) != wantOld {
				t.Fatal(oldRoot, err)
			}
			if row, found, err := old.Get(t.Context(), []byte("a"), 16); err != nil || found || row.Key != nil {
				t.Fatal(row, found, err)
			}
			if version == 4 {
				row, found, _, err := old.GetControl(t.Context(), []byte{'A', 0}, ReadBudget{Rows: 2, Bytes: 4096})
				if err != nil || found || row.Key != nil || row.Index != 0 {
					t.Fatal(row, found, err)
				}
				usage, err := old.ControlInventoryUsage(t.Context())
				if !errors.Is(err, ErrInvalid) || usage != (ApplicationControlUsage{}) {
					t.Fatal(usage, err)
				}
			}
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			if err := v.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPreparedReadViewInvalidImageIndexContextAndCopies(t *testing.T) {
	s, p, snapshot := preparedReadFixture(t, 4, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before, err := s.ApplicationTransferUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		p    *PreparedApplicationSnapshot
		ctx  context.Context
		at   uint64
		max  int
		want error
	}{
		{nil, t.Context(), 3, 8, ErrInvalid}, {&PreparedApplicationSnapshot{}, t.Context(), 3, 8, ErrInvalid},
		{p, nil, 3, 8, ErrInvalid}, {p, ctx, 3, 8, context.Canceled},
		{p, t.Context(), 0, 8, ErrInvalid}, {p, t.Context(), 4, 8, ErrInvalid},
		{p, t.Context(), 3, 0, ErrInvalid}, {p, t.Context(), 3, -1, ErrInvalid},
		{p, t.Context(), 3, 9, ErrLimit}, {p, t.Context(), 3, 1, ErrLimit},
	} {
		view, err := tc.p.OpenReadView(tc.ctx, tc.at, tc.max)
		if view != nil || !errors.Is(err, tc.want) {
			t.Fatal(view, err, tc.want)
		}
	}
	copyP := reflect.New(reflect.TypeFor[PreparedApplicationSnapshot]()).Interface().(*PreparedApplicationSnapshot)
	reflect.ValueOf(copyP).Elem().Set(reflect.ValueOf(p).Elem())
	if v, err := copyP.OpenReadView(t.Context(), 3, 8); v != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal(v, err)
	}
	after, err := s.ApplicationTransferUsage()
	if err != nil || before != after {
		t.Fatal(before, after, err)
	}
	claim, err := p.Claim(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := p.OpenReadView(t.Context(), 3, 8); v != nil || !errors.Is(err, ErrLimit) {
		t.Fatal(v, err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if v, err := p.OpenReadView(t.Context(), 3, 8); v != nil || !errors.Is(err, ErrClosed) {
		t.Fatal(v, err)
	}
}

func TestPreparedReadViewBlockRevocationAndClaim(t *testing.T) {
	s, p, snapshot := preparedReadFixture(t, 4, nil)
	v, err := p.OpenReadView(t.Context(), 3, 8)
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.ApplicationTransferUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, revoke := range []func() error{p.Close, p.i.Abort} {
		if err := revoke(); !errors.Is(err, ErrLimit) {
			t.Fatal(err)
		}
	}
	if claim, err := p.Claim(snapshot); claim != nil || !errors.Is(err, ErrLimit) {
		t.Fatal(claim, err)
	}
	if claim, err := s.ClaimPreparedApplicationSnapshot(p, snapshot); claim != nil || !errors.Is(err, ErrLimit) {
		t.Fatal(claim, err)
	}
	after, err := s.ApplicationTransferUsage()
	if err != nil || after != before {
		t.Fatal(before, after, err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	usage, err := s.ApplicationTransferUsage()
	if err != nil || usage.PinnedLogicalBytes != 1024 {
		t.Fatal(usage, err)
	}
	claim, err := s.ClaimPreparedApplicationSnapshot(p, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.i.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedReadViewQuotasBeforeOwnership(t *testing.T) {
	// Configured4101..4103 exceed the actual replication floor3080.
	// Prepared1024 + two real owners2*1026 =3076; third needs4102.
	for _, allowance := range []uint64{4101, 4102, 4103} {
		s, p, _ := preparedReadFixture(t, 4, func(c *Config) { c.Transfer.Limits.MaxPinnedLogicalBytes = allowance })
		var held []*ApplicationView
		for range 2 {
			view, err := p.OpenReadView(t.Context(), 3, 8)
			if err != nil {
				t.Fatal(err)
			}
			held = append(held, view)
			t.Cleanup(func() {
				if err := view.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		before, err := s.ApplicationTransferUsage()
		if err != nil || before.PinnedLogicalBytes != 3076 || s.views != 2 || s.viewBytes != 4 {
			t.Fatal(before, err)
		}
		ref := held[0].ref
		refsBefore := ref.refs
		v, err := p.OpenReadView(t.Context(), 3, 8)
		if allowance == 4101 {
			if v != nil || !errors.Is(err, ErrLimit) {
				t.Fatal(v, err)
			}
			after, err := s.ApplicationTransferUsage()
			if err != nil || after != before || s.views != 2 || s.viewBytes != 4 || ref.refs != refsBefore {
				t.Fatal(before, after, err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			usage, err := s.ApplicationTransferUsage()
			if err != nil || usage.PinnedLogicalBytes != 4102 {
				t.Fatal(usage, err)
			}
			if err := v.Close(); err != nil {
				t.Fatal(err)
			}
		}
		for _, view := range held {
			if err := view.Close(); err != nil {
				t.Fatal(err)
			}
		}
		usage, err := s.ApplicationTransferUsage()
		if err != nil || usage.PinnedLogicalBytes != 1024 || s.views != 0 || s.viewBytes != 0 {
			t.Fatal(usage, err)
		}
	}
	for _, kind := range []string{"views", "image-bytes"} {
		s, p, _ := preparedReadFixture(t, 4, func(c *Config) {
			if kind == "views" {
				c.Application.MaxViews = 1
			} else {
				c.Application.MaxViewBytes = 8
			}
		})
		first, err := s.ApplicationView(1)
		if err != nil {
			t.Fatal(err)
		}
		var second *ApplicationView
		if kind == "image-bytes" {
			second, err = s.ApplicationView(1)
			if err != nil {
				t.Fatal(err)
			}
		}
		before, err := s.ApplicationTransferUsage()
		if err != nil {
			t.Fatal(err)
		}
		v, err := p.OpenReadView(t.Context(), 3, 8)
		if v != nil || !errors.Is(err, ErrLimit) {
			t.Fatal(v, err)
		}
		after, err := s.ApplicationTransferUsage()
		if err != nil || before != after {
			t.Fatal(before, after, err)
		}
		if second != nil {
			if err := second.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if err := first.Close(); err != nil {
			t.Fatal(err)
		}
		v, err = p.OpenReadView(t.Context(), 3, 8)
		if err != nil {
			t.Fatal(err)
		}
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPreparedReadViewStoreCloseNoLockInversionOrDoubleRelease(t *testing.T) {
	s, p, _ := preparedReadFixture(t, 4, nil)
	v, err := p.OpenReadView(t.Context(), 3, 8)
	if err != nil {
		t.Fatal(err)
	}
	copied := reflect.New(reflect.TypeFor[ApplicationView]()).Interface().(*ApplicationView)
	reflect.ValueOf(copied).Elem().Set(reflect.ValueOf(v).Elem())
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 16 {
			root, err := v.RootBounded(t.Context(), 8)
			if err != nil && !errors.Is(err, ErrClosed) || err == nil && !bytes.Equal(root.Image, []byte("r3")) {
				t.Error(root, err)
			}
		}
	})
	wg.Go(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	wg.Wait()
	if root, err := v.RootBounded(t.Context(), 8); !errors.Is(err, ErrClosed) || root.Image != nil {
		t.Fatal(root, err)
	}
	if err := copied.Close(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if s.views != 0 || s.viewBytes != 0 || s.pinnedApplicationBytes != 0 {
		t.Fatal(s.views, s.viewBytes, s.pinnedApplicationBytes)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if s.views != 0 || s.viewBytes != 0 || s.pinnedApplicationBytes != 0 {
		t.Fatal("second cleanup decremented accounting")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}
