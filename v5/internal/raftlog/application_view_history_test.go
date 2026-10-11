package raftlog

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// Public stock storage/generationApply unit inputs are not Driver quorum proof.
func TestApplicationViewHistoryFirstContract(t *testing.T) {
	s := receiverStore(t, vfs.NewMem(), 1)
	if at := generationApply(t, s, "incoming"); at != 2 {
		t.Fatal(at)
	}
	if at := generationApply(t, s, "tail"); at != 3 {
		t.Fatal(at)
	}
	v := viewApplication(t, s, 3)
	current, err := v.RootBounded(t.Context(), 4)
	if err != nil || current.Generation != 1 || current.Index != 3 || string(current.Image) != "tail" {
		t.Fatal(current, err)
	}
	usage, err := s.ApplicationUsage()
	if err != nil || usage.GraphBytes != 478 || usage.GraphRecords != 9 || usage.ControlBytes != 0 || usage.ControlRecords != 0 {
		t.Fatal(usage, err)
	}
	decode := func(text string) []byte {
		t.Helper()
		data, err := hex.DecodeString(text)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	// Full independent bank0 keys and AV1 frames from the external AS2 fixture;
	// no candidate Go encoder supplies any expected bytes or digest.
	for _, row := range []struct{ key, frame string }{
		{"090000000000000001", "415601005c278a352ace7b2c3963e4e3c3d56d0fefa7720d23fb04c3af6eac2e9aba0d49696e697469616c"},
		{"0a0000000000000002", "41560100038895def8786b024ce0c9b382fe3e35ef595e6aac730e66a7764a60d5d2cbf36368616e67653a696e636f6d696e67"},
		{"0b0000000000000002", "41560100b7a506ffbc724fbd67f44f654d2d672843877581731f284df670050e1e1a2c486f7574636f6d653a696e636f6d696e67"},
	} {
		raw, closer, err := s.db.Get(decode(row.key))
		if err != nil {
			t.Fatal(err)
		}
		matches := bytes.Equal(raw, decode(row.frame))
		closeErr := closer.Close()
		if !matches || closeErr != nil {
			t.Fatalf("independent stored frame %s: match%v %v", row.key, matches, closeErr)
		}
	}
	root, err := v.RootAt(t.Context(), 1, 7)
	if err != nil {
		t.Fatalf("valid captured-bank RootAt contract missing: %v", err)
	}
	if root.Generation != 1 || root.Index != 1 || !bytes.Equal(root.Image, decode("696e697469616c")) || len(root.Image) != cap(root.Image) || hex.EncodeToString(root.ImageHash[:]) != "ac1b5c0961a7269b6a053ee64276ed0e20a7f48aefb9f67519539d23aaf10149" {
		t.Fatalf("independent historical root1: %+v", root)
	}
	for _, row := range []struct {
		outcome bool
		payload string
	}{
		{false, "6368616e67653a696e636f6d696e67"},
		{true, "6f7574636f6d653a696e636f6d696e67"},
	} {
		want := decode(row.payload)
		got, err := v.ApplicationRecord(t.Context(), 2, row.outcome, len(want))
		if err != nil || !bytes.Equal(got, want) || len(got) != cap(got) {
			t.Fatalf("independent record2 outcome%v: %x %v", row.outcome, got, err)
		}
		got[0] = 'X'
		again, err := v.ApplicationRecord(t.Context(), 2, row.outcome, len(want))
		if err != nil || !bytes.Equal(again, want) {
			t.Fatal("record alias", again, err)
		}
	}
	root.Image[0] = 'X'
	again, err := v.RootAt(t.Context(), 1, 7)
	if err != nil || !bytes.Equal(again.Image, decode("696e697469616c")) {
		t.Fatal("root alias", again, err)
	}
	after, err := s.ApplicationUsage()
	if err != nil || after != usage {
		t.Fatal("history read changed owner/retention ledgers", usage, after, err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
}

func historyCheck(t *testing.T, v *ApplicationView, at, generation uint64, image, change, outcome string) {
	t.Helper()
	hashes := map[string]string{
		"initial":  "ac1b5c0961a7269b6a053ee64276ed0e20a7f48aefb9f67519539d23aaf10149",
		"seed":     "19b25856e1c150ca834cffc8b59b23adbd0ec0389e58eb22b3b64768098d002b",
		"incoming": "3a7470e00c076b678c41c8ae4a4945198a9e243cdb42e044612535facb5f225d",
		"local":    "25bf8e1a2393f1108d37029b3df5593236c755742ec93465bbafa9b290bddcf6",
		"tail":     "0c62f876ef1dea830de9f32c2f4b46dd6d74d50d15896e09ef5a2fcd4ac7e1d7",
	}
	before, err := v.s.ApplicationTransferUsage()
	if err != nil {
		t.Fatal(err)
	}
	views, viewBytes, refs := v.s.views, v.s.viewBytes, v.ref.refs
	root, err := v.RootAt(t.Context(), at, len(image))
	if err != nil || root.Index != at || root.Generation != generation || string(root.Image) != image || len(root.Image) != cap(root.Image) || hex.EncodeToString(root.ImageHash[:]) != hashes[image] {
		t.Fatal(root, err)
	}
	for _, row := range []struct {
		outcome bool
		want    string
	}{{false, change}, {true, outcome}} {
		data, err := v.ApplicationRecord(t.Context(), at, row.outcome, len(row.want))
		if err != nil || data == nil || string(data) != row.want || len(data) != cap(data) {
			t.Fatal(row, data, err)
		}
	}
	after, err := v.s.ApplicationTransferUsage()
	if err != nil || before != after || views != v.s.views || viewBytes != v.s.viewBytes || refs != v.ref.refs {
		t.Fatal("history added ownership", before, after, err)
	}
}

func TestApplicationViewHistoryPreparedRetiredAndReopen(t *testing.T) {
	for _, version := range []int{2, 3, 4} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			fs := vfs.NewMem()
			var donor, receiver *Store
			var cfg Config
			seed := "initial"
			switch version {
			case 2:
				donor = receiverStore(t, vfs.NewMem(), 2)
				receiver = receiverStore(t, fs, 1)
				cfg = semanticConfig(receiver, fs)
			case 3:
				donor = semanticStore(t, vfs.NewMem(), ApplicationSemanticContractID{41}, 2)
				receiver = semanticStore(t, fs, ApplicationSemanticContractID{41}, 1)
				cfg = semanticConfig(receiver, fs)
			case 4:
				donor, _ = newControlStore(t, vfs.NewMem(), 2)
				receiver, cfg = newControlStore(t, fs, 1)
				seed = "seed"
			}
			generationApply(t, receiver, "local")
			old := viewApplication(t, receiver, 2)
			generationApply(t, donor, "incoming")
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
			m := buildPublished(t, export, ReadBudget{Rows: 1, Bytes: 4096})
			if m.Version != uint32(version) || m.Index != 3 || m.Term != 2 {
				t.Fatal(m)
			}
			chunks := collectPublished(t, export, ReadBudget{Rows: 1, Bytes: 4096})
			if err := export.Close(); err != nil {
				t.Fatal(err)
			}
			prepared, snapshot := semanticPrepared(t, receiver, m, chunks)
			incoming, err := prepared.OpenReadView(t.Context(), 3, 8)
			if err != nil {
				t.Fatal(err)
			}
			historyCheck(t, incoming, 1, 2, seed, "", "")
			historyCheck(t, incoming, 2, 2, "incoming", "change:incoming", "outcome:incoming")
			local, err := receiver.ApplicationRecord(t.Context(), 2, false, 12)
			if err != nil || string(local) != "change:local" {
				t.Fatal("prepared active-bank fallback", local, err)
			}
			if err := incoming.Close(); err != nil {
				t.Fatal(err)
			}
			claim, err := prepared.Claim(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			raw, ready := readyMatchRaw(t, receiver, snapshot)
			if root, err := receiver.PersistApplicationReady(ready, claim); err != nil || root.Generation != 2 || root.Index != 3 {
				t.Fatal(root, err)
			}
			raw.Advance(ready)
			if err := claim.Close(); err != nil {
				t.Fatal(err)
			}
			historyCheck(t, old, 2, 1, "local", "change:local", "outcome:local")
			if root, err := old.RootAt(t.Context(), 3, 8); !errors.Is(err, ErrInvalid) || root.Image != nil || root.Index != 0 {
				t.Fatal("old selected newer bank", root, err)
			}
			if data, err := old.ApplicationRecord(t.Context(), 3, false, 15); !errors.Is(err, ErrInvalid) || data != nil {
				t.Fatal(data, err)
			}
			current := viewApplication(t, receiver, 3)
			historyCheck(t, current, 1, 2, seed, "", "")
			historyCheck(t, current, 2, 2, "incoming", "change:incoming", "outcome:incoming")
			historyCheck(t, current, 3, 2, "tail", "change:tail", "outcome:tail")
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			if err := current.Close(); err != nil {
				t.Fatal(err)
			}
			if err := receiver.Close(); err != nil {
				t.Fatal(err)
			}
			cfg.Create = false
			reopened, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			})
			v := viewApplication(t, reopened, 3)
			historyCheck(t, v, 1, 2, seed, "", "")
			historyCheck(t, v, 2, 2, "incoming", "change:incoming", "outcome:incoming")
			if err := v.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestApplicationViewHistoryCapsContextsAndOwnership(t *testing.T) {
	s := receiverStore(t, vfs.NewMem(), 1)
	generationApply(t, s, "incoming")
	v := viewApplication(t, s, 2)
	before, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, budget := range []int{6, 7, 8} {
		root, err := v.RootAt(t.Context(), 1, budget)
		if budget == 6 {
			if !errors.Is(err, ErrLimit) || root.Image != nil || root.Generation != 0 || root.Index != 0 || root.ImageHash != ([32]byte{}) {
				t.Fatal(root, err)
			}
		} else if err != nil || string(root.Image) != "initial" {
			t.Fatal(root, err)
		}
	}
	for _, row := range []struct {
		outcome bool
		n       int
	}{{false, 15}, {true, 16}} {
		for _, budget := range []int{row.n - 1, row.n, row.n + 1} {
			data, err := v.ApplicationRecord(t.Context(), 2, row.outcome, budget)
			if budget < row.n {
				if !errors.Is(err, ErrLimit) || data != nil {
					t.Fatal(data, err)
				}
			} else if err != nil || len(data) != row.n || len(data) != cap(data) {
				t.Fatal(data, err)
			}
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	copyV := reflect.New(reflect.TypeFor[ApplicationView]()).Interface().(*ApplicationView)
	reflect.ValueOf(copyV).Elem().Set(reflect.ValueOf(v).Elem())
	unregistered := &ApplicationView{s: s, index: 2}
	unregistered.self = unregistered
	for _, row := range []struct {
		v                  *ApplicationView
		ctx                context.Context
		index              uint64
		rootCap, recordCap int
		want               error
	}{
		{nil, t.Context(), 1, 7, 15, ErrInvalid}, {&ApplicationView{}, t.Context(), 1, 7, 15, ErrInvalid},
		{copyV, t.Context(), 1, 7, 15, ErrInvalid}, {unregistered, t.Context(), 1, 7, 15, ErrInvalid},
		{v, nil, 1, 7, 15, ErrInvalid}, {v, ctx, 1, 7, 15, context.Canceled},
		{v, t.Context(), 0, 7, 15, ErrInvalid}, {v, t.Context(), 3, 7, 15, ErrInvalid},
		{v, t.Context(), 1, -1, -1, ErrInvalid},
		{v, t.Context(), 1, s.ApplicationLimits().MaxImageBytes + 1, s.ApplicationLimits().MaxPageBytes + 1, ErrLimit},
	} {
		root, err := row.v.RootAt(row.ctx, row.index, row.rootCap)
		if !errors.Is(err, row.want) || root.Image != nil || root.Generation != 0 || root.Index != 0 || root.ImageHash != ([32]byte{}) {
			t.Fatal(row, root, err)
		}
		data, err := row.v.ApplicationRecord(row.ctx, row.index, false, row.recordCap)
		if !errors.Is(err, row.want) || data != nil {
			t.Fatal(row, data, err)
		}
	}
	after, err := s.ApplicationUsage()
	if err != nil || after != before || s.poison != nil {
		t.Fatal(before, after, err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if root, err := v.RootAt(t.Context(), 1, 7); !errors.Is(err, ErrClosed) || root.Image != nil {
		t.Fatal(root, err)
	}
	if data, err := v.ApplicationRecord(t.Context(), 1, false, 0); !errors.Is(err, ErrClosed) || data != nil {
		t.Fatal(data, err)
	}
}

func TestApplicationViewHistoryPresentEmptyAndSizeAllowance(t *testing.T) {
	p, tc := transferConfig(1)
	s, err := Open(Config{Dir: "empty", FS: vfs.NewMem(), Create: true, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Initialize([]uint64{1}, nil); err != nil {
		t.Fatal(err)
	}
	v := viewApplication(t, s, 1)
	root, err := v.RootAt(t.Context(), 1, 0)
	if err != nil || root.Image == nil || len(root.Image) != 0 || cap(root.Image) != 0 || hex.EncodeToString(root.ImageHash[:]) != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Fatal(root, err)
	}
	for _, outcome := range []bool{false, true} {
		if data, err := v.ApplicationRecord(t.Context(), 1, outcome, 0); err != nil || data == nil || len(data) != 0 || cap(data) != 0 {
			t.Fatal(data, err)
		}
	}
	if unsafe.Sizeof(ApplicationRoot{})+unsafe.Sizeof([9]byte{})+unsafe.Sizeof([32]byte{})+unsafe.Sizeof([]byte{}) > 1024 {
		t.Fatal("history metadata exceeds fixed1024")
	}
}

func TestApplicationViewHistoryApprovedDamagedFrames(t *testing.T) {
	for _, tag := range []byte{9, 10, 11} {
		t.Run(string(rune(tag+'0')), func(t *testing.T) {
			s := receiverStore(t, vfs.NewMem(), 1)
			generationApply(t, s, "incoming")
			v, err := s.ApplicationView(2)
			if err != nil {
				t.Fatal(err)
			}
			// Only the existing 'damaged' static-frame pattern at exact root/
			// change/outcome keys; no missing-key or IO/closer fault producer.
			key := bankIndexKey(0, tag, 1)
			if err := s.db.Set(key, []byte("damaged"), pebble.Sync); err != nil {
				t.Fatal(err)
			}
			if tag == 9 {
				root, err := v.RootAt(t.Context(), 1, 0)
				if !errors.Is(err, ErrCorrupt) || root.Image != nil {
					t.Fatal(root, err)
				}
			} else {
				data, err := v.ApplicationRecord(t.Context(), 1, tag == 11, 0)
				if !errors.Is(err, ErrCorrupt) || data != nil {
					t.Fatal(data, err)
				}
			}
			if _, err := v.RootAt(t.Context(), 2, 8); !errors.Is(err, ErrPoisoned) || !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
			if err := v.Close(); !errors.Is(err, ErrPoisoned) || !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
		})
	}
}

func TestApplicationViewHistoryConcurrentStoreClose(t *testing.T) {
	s := receiverStore(t, vfs.NewMem(), 1)
	generationApply(t, s, "incoming")
	v := viewApplication(t, s, 2)
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 16 {
			root, err := v.RootAt(t.Context(), 1, 7)
			if err != nil && !errors.Is(err, ErrClosed) || err == nil && string(root.Image) != "initial" {
				t.Error(root, err)
			}
			data, err := v.ApplicationRecord(t.Context(), 2, false, 15)
			if err != nil && !errors.Is(err, ErrClosed) || err == nil && string(data) != "change:incoming" {
				t.Error(data, err)
			}
		}
	})
	wg.Go(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	wg.Wait()
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if s.views != 0 || s.viewBytes != 0 {
		t.Fatal(s.views, s.viewBytes)
	}
}
