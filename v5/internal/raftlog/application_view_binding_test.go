package raftlog

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestApplicationViewApplicationBindingValueAndUnbound(t *testing.T) {
	if unsafe.Sizeof(ApplicationBinding{}) != 72 {
		t.Fatal("binding is not the fixed 72-byte value")
	}
	for _, tc := range []struct {
		name  string
		store func() *Store
	}{
		{"legacy singleton", func() *Store { return openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1)) }},
		{"unbound fixed three", func() *Store { return receiverStore(t, vfs.NewMem(), 1) }},
		{"bound", func() *Store { return semanticStore(t, vfs.NewMem(), ApplicationSemanticContractID{13}, 1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.store()
			v := viewApplication(t, s, 1)
			want := s.ApplicationBinding() // Outside the view/store lock.
			before := readyMetadataFingerprint(t, s)
			usage, err := s.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			got, err := v.ApplicationBinding(t.Context())
			if err != nil || got != want {
				t.Fatal(got, want, err)
			}
			got.Identity.Graph[0]++
			got.Identity.Partition++
			got.Identity.Group[0]++
			got.SemanticContractID[0]++
			again, err := v.ApplicationBinding(t.Context())
			if err != nil || again != want {
				t.Fatal("returned value aliases metadata", again, err)
			}
			if allocations := testing.AllocsPerRun(100, func() {
				value, err := v.ApplicationBinding(t.Context())
				if err != nil || value != want {
					t.Fatal(value, err)
				}
			}); allocations != 0 {
				t.Fatal("fixed binding getter allocated", allocations)
			}
			after, err := s.ApplicationUsage()
			if err != nil || after != usage || readyMetadataFingerprint(t, s) != before {
				t.Fatal("getter changed pins/quota/durable metadata", after, usage, err)
			}
		})
	}
}

func TestApplicationViewApplicationBindingInvalidAndLifetimeErrors(t *testing.T) {
	s := semanticStore(t, vfs.NewMem(), ApplicationSemanticContractID{13}, 1)
	for _, v := range []*ApplicationView{nil, {}, {s: s}} {
		got, err := v.ApplicationBinding(t.Context())
		if !errors.Is(err, ErrInvalid) || got != (ApplicationBinding{}) {
			t.Fatal("invalid view", got, err)
		}
	}
	v := viewApplication(t, s, 1)
	//nolint:staticcheck // SA1012: exercise the nil-context error contract.
	if got, err := v.ApplicationBinding(nil); !errors.Is(err, ErrInvalid) || got != (ApplicationBinding{}) {
		t.Fatal("nil context", got, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := v.ApplicationBinding(ctx); !errors.Is(err, context.Canceled) || got != (ApplicationBinding{}) {
		t.Fatal("live canceled view", got, err)
	}
	backend := errors.New("original backend error")
	s.mu.Lock()
	s.poison = backend
	s.mu.Unlock()
	got, err := v.ApplicationBinding(t.Context())
	s.mu.Lock()
	s.poison = nil
	s.mu.Unlock()
	if !errors.Is(err, backend) || !errors.Is(err, ErrPoisoned) || got != (ApplicationBinding{}) {
		t.Fatal("poison cause lost", got, err)
	}
	// A live pinned view cannot normally suffer bank reuse. Simulate only the
	// internal mismatch to exercise the existing stale-generation refusal guard.
	v.mu.Lock()
	generation := v.generation
	v.generation++
	v.mu.Unlock()
	got, err = v.ApplicationBinding(t.Context())
	v.mu.Lock()
	v.generation = generation
	v.mu.Unlock()
	if !errors.Is(err, ErrClosed) || got != (ApplicationBinding{}) {
		t.Fatal("stale bank", got, err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := v.ApplicationBinding(ctx); !errors.Is(err, ErrClosed) || got != (ApplicationBinding{}) {
		t.Fatal("closed view must preserve lock precedence", got, err)
	}
	w := viewApplication(t, s, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := w.ApplicationBinding(t.Context()); !errors.Is(err, ErrClosed) || got != (ApplicationBinding{}) {
		t.Fatal("closed store", got, err)
	}
}

func TestApplicationViewApplicationBindingRetainsMeaningAcrossActualAS3Activation(t *testing.T) {
	id := ApplicationSemanticContractID{13}
	s := semanticStore(t, vfs.NewMem(), id, 1)
	old := viewApplication(t, s, 1)
	want, err := old.ApplicationBinding(t.Context())
	if err != nil || want != s.ApplicationBinding() {
		t.Fatal(want, err)
	}
	_, claim, snapshot, manifest := readyMatchPrepared(t, s, id)
	raw, ready := readyMatchRaw(t, s, snapshot) // Actual unmodified pinned Raft Ready.
	root, err := s.PersistApplicationReady(ready, claim)
	if err != nil || root.Generation != 2 || root.Index != manifest.Index {
		t.Fatal("true activation failed", root, err)
	}
	raw.Advance(ready)
	current := viewApplication(t, s, root.Index)
	for _, v := range []*ApplicationView{old, current} {
		binding, err := v.ApplicationBinding(t.Context())
		if err != nil || binding != want {
			t.Fatal("binding changed across bank activation", binding, want, err)
		}
	}
	oldRoot, err := old.Root()
	if err != nil || oldRoot.Generation != 1 || oldRoot.Index != 1 || string(oldRoot.Image) != "initial" {
		t.Fatal("old view rebound", oldRoot, err)
	}
	newRoot, err := current.Root()
	if err != nil || newRoot.Generation != 2 || string(newRoot.Image) != "incoming" {
		t.Fatal(newRoot, err)
	}
	getApplication(t, old, "a", "", false, false)
	getApplication(t, current, "a", "incoming", true, false)
	if manifest.Version != 3 || want.Identity != manifest.Identity || want.SemanticContractID != manifest.SemanticContractID {
		t.Fatal("getter does not expose actual transferred store scope", want, manifest)
	}
}

func TestApplicationViewApplicationBindingBoundedInstallCloseRace(t *testing.T) {
	for _, closeStore := range []bool{false, true} {
		t.Run(map[bool]string{false: "view close", true: "store close"}[closeStore], func(t *testing.T) {
			s := semanticStore(t, vfs.NewMem(), ApplicationSemanticContractID{13}, 1)
			v := viewApplication(t, s, 1)
			want := s.ApplicationBinding()
			first := make(chan struct{}, 16)
			continueReaders := make(chan struct{})
			releaseReaders := sync.OnceFunc(func() { close(continueReaders) })
			defer releaseReaders()
			var readers sync.WaitGroup
			initialDeadline := time.NewTimer(10 * time.Second)
			defer initialDeadline.Stop()
			for range 16 {
				readers.Go(func() {
					binding, err := v.ApplicationBinding(t.Context())
					if err != nil || binding != want {
						t.Error("initial live binding", binding, err)
					}
					first <- struct{}{}
					<-continueReaders
					for range 15 { // Exactly256 total calls across16 bounded readers.
						binding, err := v.ApplicationBinding(t.Context())
						if err == nil {
							if binding != want {
								t.Error("torn immutable binding", binding)
							}
						} else if !errors.Is(err, ErrClosed) || binding != (ApplicationBinding{}) {
							t.Error("unexpected raced error", binding, err)
						}
					}
				})
			}
			for range 16 {
				select {
				case <-first:
				case <-initialDeadline.C:
					t.Fatal("initial readers did not join")
				}
			}
			releaseReaders()
			// Fatal test helpers stay in this main test goroutine.
			generationApply(t, s, "later", KV{Key: []byte("a"), Value: []byte("later")})
			var closeErr error
			if closeStore {
				closeErr = s.Close()
			} else {
				closeErr = v.Close()
			}
			if closeErr != nil {
				t.Fatal("close after real install", closeErr)
			}
			joined := make(chan struct{})
			go func() { readers.Wait(); close(joined) }()
			select {
			case <-joined:
			case <-time.After(10 * time.Second):
				t.Fatal("bounded readers/writer did not join")
			}
			if binding, err := v.ApplicationBinding(t.Context()); !errors.Is(err, ErrClosed) || binding != (ApplicationBinding{}) {
				t.Fatal("final closed binding", binding, err)
			}
		})
	}
}
