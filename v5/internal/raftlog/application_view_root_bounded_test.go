package raftlog

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func boundedRootIsZero(root ApplicationRoot) bool {
	return root.Index == 0 && root.Generation == 0 && root.Image == nil && root.ImageHash == ([32]byte{})
}

func TestApplicationViewRootBoundedExactCapsAndOwnership(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	v := viewApplication(t, s, 1)
	before := readyMetadataFingerprint(t, s)
	usage, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, capBytes := range []int{6, 7, 8} {
		root, err := v.RootBounded(t.Context(), capBytes)
		if capBytes == 6 {
			if !errors.Is(err, ErrLimit) || !boundedRootIsZero(root) {
				t.Fatal("one byte short", root, err)
			}
			continue
		}
		if err != nil || root.Index != 1 || root.Generation != 0 || string(root.Image) != "initial" || cap(root.Image) != 7 || root.ImageHash != sha256.Sum256([]byte("initial")) {
			t.Fatal("exact owned root", root, err)
		}
		root.Image[0] = 'X'
	}
	root, err := v.RootBounded(t.Context(), 7)
	if err != nil || string(root.Image) != "initial" {
		t.Fatal("root aliases prior returned bytes", root, err)
	}
	after, err := s.ApplicationUsage()
	if err != nil || after != usage || readyMetadataFingerprint(t, s) != before || s.poison != nil {
		t.Fatal("bounded read changed accounting or durable metadata", after, usage, err)
	}
}

func TestApplicationViewRootBoundedLargeOpaqueRefusalAllocatesNothing(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	image := strings.Repeat("x", 32<<10)
	index := applyApplication(t, s, image)
	v := viewApplication(t, s, index)
	before := readyMetadataFingerprint(t, s)
	usage, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	for _, capBytes := range []int{0, 172, len(image) - 1} {
		if allocations := testing.AllocsPerRun(100, func() {
			root, err := v.RootBounded(ctx, capBytes)
			if !errors.Is(err, ErrLimit) || !boundedRootIsZero(root) {
				t.Fatal("oversized opaque root escaped", root, err)
			}
		}); allocations != 0 {
			t.Fatal("oversized refusal allocated before admission", capBytes, allocations)
		}
	}
	root, err := v.RootBounded(ctx, len(image))
	if err != nil || root.Index != index || cap(root.Image) != len(image) || string(root.Image) != image || root.ImageHash != sha256.Sum256([]byte(image)) {
		t.Fatal("positive control after refusal", root.Index, err)
	}
	after, err := s.ApplicationUsage()
	if err != nil || after != usage || s.poison != nil || readyMetadataFingerprint(t, s) != before {
		t.Fatal("limit refusal poisoned or charged the store", after, usage, err)
	}
}

func TestApplicationViewRootBoundedZeroCapAcceptsEmptyImage(t *testing.T) {
	s, err := Open(Config{Dir: "empty", FS: vfs.NewMem(), Create: true, Application: DefaultApplicationPolicy(1)})
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
	root, err := v.RootBounded(t.Context(), 0)
	if err != nil || root.Index != 1 || root.Generation != 0 || len(root.Image) != 0 || cap(root.Image) != 0 || root.ImageHash != sha256.Sum256(nil) {
		t.Fatal("empty opaque image", root, err)
	}
}

func TestApplicationViewRootBoundedRetainedBeforeAndAfterMutation(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	first := applyApplication(t, s, "first", KV{Key: []byte("a"), Value: []byte("old")})
	old := viewApplication(t, s, first)
	second := applyApplication(t, s, "second", KV{Key: []byte("a"), Value: []byte("new")})
	current := viewApplication(t, s, second)
	for _, tc := range []struct {
		view  *ApplicationView
		index uint64
		image string
	}{{old, first, "first"}, {current, second, "second"}} {
		root, err := tc.view.RootBounded(t.Context(), len(tc.image))
		if err != nil || root.Index != tc.index || root.Generation != 0 || string(root.Image) != tc.image || cap(root.Image) != len(tc.image) || root.ImageHash != sha256.Sum256([]byte(tc.image)) {
			t.Fatal("historical root substituted current bytes", root, tc.index, tc.image, err)
		}
	}
	getApplication(t, old, "a", "old", true, false)
	getApplication(t, current, "a", "new", true, false)
}

func TestApplicationViewRootBoundedActualAS3GenerationTransition(t *testing.T) {
	id := ApplicationSemanticContractID{13}
	s := semanticStore(t, vfs.NewMem(), id, 1)
	old := viewApplication(t, s, 1)
	_, claim, snapshot, manifest := readyMatchPrepared(t, s, id)
	raw, ready := readyMatchRaw(t, s, snapshot)
	installed, err := s.PersistApplicationReady(ready, claim)
	if err != nil || installed.Generation != 2 || installed.Index != manifest.Index {
		t.Fatal("true generation activation", installed, err)
	}
	raw.Advance(ready)
	current := viewApplication(t, s, installed.Index)
	for _, tc := range []struct {
		view              *ApplicationView
		index, generation uint64
		image             string
	}{{old, 1, 1, "initial"}, {current, manifest.Index, 2, "incoming"}} {
		root, err := tc.view.RootBounded(t.Context(), len(tc.image))
		if err != nil || root.Index != tc.index || root.Generation != tc.generation || string(root.Image) != tc.image || cap(root.Image) != len(tc.image) || root.ImageHash != sha256.Sum256([]byte(tc.image)) {
			t.Fatal("root lost retained generation", root, tc.generation, err)
		}
	}
	getApplication(t, old, "a", "", false, false)
	getApplication(t, current, "a", "incoming", true, false)
}

func TestApplicationViewRootBoundedInvalidAndOrderedLifetimeErrors(t *testing.T) {
	s := semanticStore(t, vfs.NewMem(), ApplicationSemanticContractID{13}, 1)
	for _, v := range []*ApplicationView{nil, {}, {s: s}, {index: 1}} {
		root, err := v.RootBounded(t.Context(), 7)
		if !errors.Is(err, ErrInvalid) || !boundedRootIsZero(root) {
			t.Fatal("invalid view", root, err)
		}
	}
	v := viewApplication(t, s, 1)
	//nolint:staticcheck // SA1012: exercise the nil-context error contract.
	if root, err := v.RootBounded(nil, 7); !errors.Is(err, ErrInvalid) || !boundedRootIsZero(root) {
		t.Fatal("nil context", root, err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if root, err := v.RootBounded(canceled, -1); !errors.Is(err, ErrInvalid) || !boundedRootIsZero(root) {
		t.Fatal("negative cap must refuse before lock", root, err)
	}
	if root, err := v.RootBounded(canceled, 0); !errors.Is(err, context.Canceled) || !boundedRootIsZero(root) {
		t.Fatal("cancellation precedes image limit", root, err)
	}
	deadline, stop := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer stop()
	if root, err := v.RootBounded(deadline, 7); !errors.Is(err, context.DeadlineExceeded) || !boundedRootIsZero(root) {
		t.Fatal("deadline cause", root, err)
	}
	backend := errors.New("bounded-root original backend error")
	s.mu.Lock()
	s.poison = backend
	s.mu.Unlock()
	root, err := v.RootBounded(t.Context(), 0)
	s.mu.Lock()
	s.poison = nil
	s.mu.Unlock()
	if !errors.Is(err, backend) || !errors.Is(err, ErrPoisoned) || !boundedRootIsZero(root) {
		t.Fatal("poison precedes limit and retains cause", root, err)
	}
	// Real pinned views cannot normally suffer bank reuse. Exercise only the
	// existing lock's internal generation-mismatch branch without changing Store.
	v.mu.Lock()
	generation := v.generation
	v.generation++
	v.mu.Unlock()
	root, err = v.RootBounded(t.Context(), 0)
	v.mu.Lock()
	v.generation = generation
	v.mu.Unlock()
	if !errors.Is(err, ErrClosed) || !boundedRootIsZero(root) {
		t.Fatal("stale bank precedes limit", root, err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if root, err := v.RootBounded(canceled, 7); !errors.Is(err, ErrClosed) || !boundedRootIsZero(root) {
		t.Fatal("closed view precedes cancellation", root, err)
	}
	if root, err := v.RootBounded(canceled, -1); !errors.Is(err, ErrInvalid) || !boundedRootIsZero(root) {
		t.Fatal("negative cap remains pre-lock invalid", root, err)
	}
	w := viewApplication(t, s, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if root, err := w.RootBounded(t.Context(), 7); !errors.Is(err, ErrClosed) || !boundedRootIsZero(root) {
		t.Fatal("closed store", root, err)
	}
}

func TestApplicationViewRootBoundedConcurrentInstallAndClose(t *testing.T) {
	for _, closeStore := range []bool{false, true} {
		t.Run(map[bool]string{false: "view close", true: "store close"}[closeStore], func(t *testing.T) {
			s := semanticStore(t, vfs.NewMem(), ApplicationSemanticContractID{13}, 1)
			v := viewApplication(t, s, 1)
			ctx := t.Context()
			first := make(chan struct{}, 16)
			start := make(chan struct{})
			release := sync.OnceFunc(func() { close(start) })
			defer release()
			var readers sync.WaitGroup
			wantHash := sha256.Sum256([]byte("initial"))
			for range 16 {
				readers.Go(func() {
					root, err := v.RootBounded(ctx, 7)
					if err != nil || root.Index != 1 || root.Generation != 1 || string(root.Image) != "initial" || root.ImageHash != wantHash {
						t.Error("initial live root", root, err)
					}
					first <- struct{}{}
					<-start
					for range 15 {
						root, err := v.RootBounded(ctx, 7)
						if err == nil {
							if root.Index != 1 || root.Generation != 1 || string(root.Image) != "initial" || cap(root.Image) != 7 || root.ImageHash != wantHash {
								t.Error("torn/current substituted root", root)
							}
							root.Image[0] = 'X'
						} else if !errors.Is(err, ErrClosed) || !boundedRootIsZero(root) {
							t.Error("unexpected raced result", root, err)
						}
					}
				})
			}
			joined := make(chan struct{})
			go func() { readers.Wait(); close(joined) }()
			t.Cleanup(func() {
				release()
				select {
				case <-joined:
				case <-time.After(10 * time.Second):
					t.Error("bounded reader cleanup did not join")
				}
			})
			initialDeadline := time.NewTimer(10 * time.Second)
			defer initialDeadline.Stop()
			for range 16 {
				select {
				case <-first:
				case <-initialDeadline.C:
					t.Fatal("initial live readers did not join")
				}
			}
			release()
			// Fatal helpers and writer/close stay in the main test goroutine.
			generationApply(t, s, "later", KV{Key: []byte("a"), Value: []byte("later")})
			var closeErr error
			if closeStore {
				closeErr = s.Close()
			} else {
				closeErr = v.Close()
			}
			if closeErr != nil {
				t.Fatal(closeErr)
			}
			select {
			case <-joined:
			case <-time.After(10 * time.Second):
				t.Fatal("256 root reads did not join")
			}
			if root, err := v.RootBounded(ctx, 7); !errors.Is(err, ErrClosed) || !boundedRootIsZero(root) {
				t.Fatal("final closed root", root, err)
			}
		})
	}
}
