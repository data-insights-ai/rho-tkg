package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// These compact model-contract guards do not stand in for the real child/native
// recovery schedules. In particular, queued SyncTo is not a persistence claim.
type nativeDurableQueuedFile struct{ vfs.File }

func (nativeDurableQueuedFile) SyncTo(int64) (bool, error) { return false, nil }

func TestNativeDurableBootstrapAllowsOnlyImmediateParentMetadata(t *testing.T) {
	outer := t.TempDir()
	parent := filepath.Join(outer, "owner")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	dir, shadow := filepath.Join(parent, "native"), filepath.Join(parent, "shadow")
	model, err := newNativeDurableFS(t.Context(), dir, shadow, 1, nativeDurableDefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	before := model.ledger
	if info, err := model.Stat(parent); err != nil || !info.IsDir() {
		t.Fatal("Pebble bootstrap needs the actual immediate parent Stat", err)
	}
	ancestor, err := model.OpenDir(parent)
	if err != nil {
		t.Fatal("Pebble bootstrap needs the actual immediate parent OpenDir", err)
	}
	if err := ancestor.SyncData(); !errors.Is(err, errInvalid) {
		t.Fatal("parent data Sync admitted", err)
	}
	if full, err := ancestor.SyncTo(0); full || !errors.Is(err, errInvalid) {
		t.Fatal("parent queued Sync admitted", full, err)
	}
	if err := errors.Join(ancestor.Sync(), ancestor.Close()); err != nil {
		t.Fatal("actual parent directory Sync", err)
	}
	if model.ledger.Operations <= before.Operations || model.ledger.WorkBytes <= before.WorkBytes || len(model.names) != 0 || len(model.durable) != 0 || len(model.files) != 0 {
		t.Fatal("ancestor metadata imported names or bypassed its work ledger")
	}
	if _, err := os.Lstat(filepath.Join(shadow, "CATALOG")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("parent Sync invented a native catalog", err)
	}
	if err := model.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	unsynced, err := model.Create(filepath.Join(dir, "unsynced"), vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	if err := unsynced.Close(); err != nil {
		t.Fatal(err)
	}
	ancestor, err = model.OpenDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(ancestor.Sync(), ancestor.Close()); err != nil || len(model.durable) != 0 {
		t.Fatal("parent Sync published native child names", err)
	}
	if _, err := os.Lstat(filepath.Join(shadow, "CATALOG")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("parent Sync published native child catalog", err)
	}
	for _, path := range []string{filepath.Dir(parent), parent + string(os.PathSeparator) + ".", dir + string(os.PathSeparator) + ".."} {
		if _, err := model.Stat(path); !errors.Is(err, errInvalid) {
			t.Fatal("outside/traversal metadata admitted", path, err)
		}
		if file, err := model.OpenDir(path); !errors.Is(err, errInvalid) || file != nil {
			t.Fatal("outside/traversal directory admitted", path, err)
		}
	}
	foreign := filepath.Join(parent, "foreign")
	if err := os.WriteFile(foreign, []byte("outside native catalog"), 0600); err != nil {
		t.Fatal(err)
	}
	if file, err := model.Open(foreign); !errors.Is(err, errInvalid) || file != nil {
		t.Fatal("ancestor data imported", err)
	}
	alias := filepath.Join(parent, "alias")
	if err := os.Symlink(parent, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := model.Stat(alias); !errors.Is(err, errInvalid) {
		t.Fatal("ancestor symlink admitted", err)
	}
	if owner, err := newNativeDurableFS(t.Context(), filepath.Join(alias, "native2"), filepath.Join(outer, "shadow2"), 1, nativeDurableDefaultPolicy()); !errors.Is(err, errInvalid) || owner != nil {
		t.Fatal("symlink parent constructor admitted", err)
	}
	db, err := pebble.Open(dir, &pebble.Options{FS: model, CacheSize: 2 << 20, MemTableSize: 1 << 20, MaxOpenFiles: 74, FormatMajorVersion: pebble.FormatMinSupported})
	if err != nil {
		t.Fatal("genuine Pebble bootstrap under synced-image model", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	saved := filepath.Join(outer, "saved")
	if err := os.Rename(parent, saved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(saved, parent); err != nil {
		t.Fatal(err)
	}
	if _, err := model.Stat(parent); !errors.Is(err, errInvalid) {
		t.Fatal("changed parent symlink admitted", err)
	}
	if file, err := model.OpenDir(parent); !errors.Is(err, errInvalid) || file != nil {
		t.Fatal("changed parent directory admitted", err)
	}
	model.mu.Lock()
	model.ledger.WorkBytes = model.policy.WorkBytes
	model.mu.Unlock()
	if _, err := model.Stat(parent); !errors.Is(err, errLimit) {
		t.Fatal("parent metadata bypassed exhausted work", err)
	}
	if file, err := model.OpenDir(parent); !errors.Is(err, errLimit) || file != nil {
		t.Fatal("parent directory bypassed exhausted work", err)
	}
}

func TestNativeDurableImageUsesSyncedIdentityAndNamesInsteadOfCurrentDisk(t *testing.T) {
	parent := t.TempDir()
	dir, shadow := filepath.Join(parent, "native"), filepath.Join(parent, "shadow")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	policy := nativeDurableDefaultPolicy()
	model, err := newNativeDurableFS(t.Context(), dir, shadow, 1, policy)
	if err != nil {
		t.Fatal(err)
	}
	file, err := model.Create(filepath.Join(dir, "a"), vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := file.Write([]byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := file.SyncData(); err != nil {
		t.Fatal(err)
	}
	directory, err := model.OpenDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := directory.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := directory.Sync(); err != nil {
		t.Fatal(err)
	}
	// The actual native file now differs, but neither these bytes nor the new
	// names have completed the corresponding native persistence operation.
	if _, err := file.WriteAt([]byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	if err := model.Rename(filepath.Join(dir, "a"), filepath.Join(dir, "b")); err != nil {
		t.Fatal(err)
	}
	if err := model.Link(filepath.Join(dir, "b"), filepath.Join(dir, "alias")); err != nil {
		t.Fatal(err)
	}
	recover := func() nativeDurableRecovery {
		t.Helper()
		image, digest, err := nativeDurableReadImage(t.Context(), shadow, policy)
		if err != nil || image.Cookie != model.cookie {
			t.Fatal("sealed native model binding", err)
		}
		receipt, err := nativeDurableRecover(t.Context(), shadow, parent, model.cookie, digest, policy)
		if err != nil || receipt.CatalogSHA != digest || receipt.WorkBytes > policy.WorkBytes {
			t.Fatal("bounded native image recovery", err)
		}
		return receipt
	}
	first := recover()
	got, err := os.ReadFile(filepath.Join(first.Directory, "a"))
	if err != nil || !bytes.Equal(got, []byte("old")) {
		t.Fatal("recovery borrowed unsynced current bytes", string(got), err)
	}
	if _, err := os.Lstat(filepath.Join(first.Directory, "b")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unsynced rename entered image", err)
	}
	if err := directory.Sync(); err != nil {
		t.Fatal(err)
	}
	second := recover()
	b, err := os.Stat(filepath.Join(second.Directory, "b"))
	if err != nil {
		t.Fatal(err)
	}
	alias, err := os.Stat(filepath.Join(second.Directory, "alias"))
	if err != nil || !os.SameFile(b, alias) {
		t.Fatal("native hard-link identity was flattened", err)
	}
	got, err = os.ReadFile(filepath.Join(second.Directory, "b"))
	if err != nil || string(got) != "old" {
		t.Fatal("directory Sync certified unsynced data", string(got), err)
	}
	// SyncTo(false,nil) retires work but adds no stable bytes or version.
	wrapped := file.(*nativeDurableFile)
	actual := wrapped.File
	wrapped.File = nativeDurableQueuedFile{actual}
	before := model.ledger
	if full, err := wrapped.SyncTo(0); err != nil || full {
		t.Fatal("queued SyncTo contract", full, err)
	}
	wrapped.File = actual
	if model.ledger.ShadowBytes != before.ShadowBytes || model.ledger.Versions != before.Versions || model.ledger.WorkBytes <= before.WorkBytes {
		t.Fatal("queued sync invented physical bytes or refunded its grant")
	}
	if err := file.SyncData(); err != nil {
		t.Fatal(err)
	}
	third := recover()
	got, err = os.ReadFile(filepath.Join(third.Directory, "alias"))
	if err != nil || string(got) != "new" {
		t.Fatal("synced alias data lost", string(got), err)
	}
	if _, err := os.Lstat(filepath.Join(third.Directory, "a")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("removed old name revived", err)
	}
	// Directly exercise the remaining native FS/file doors in this same small
	// fixture. These calls do not qualify a native graph crash or recovery.
	if model.Unwrap() != vfs.Default {
		t.Fatal("model lost actual native backing FS")
	}
	reader, err := model.Open(filepath.Join(dir, "b"))
	if err != nil {
		t.Fatal(err)
	}
	var buffer [3]byte
	if n, err := reader.Read(buffer[:]); err != nil || n != 3 || string(buffer[:]) != "new" {
		t.Fatal("native sequential read", n, err)
	}
	if n, err := reader.ReadAt(buffer[:], 0); err != nil && !errors.Is(err, io.EOF) || n != 3 {
		t.Fatal("native positional read", n, err)
	}
	if _, err := reader.ReadAt(buffer[:], -1); !errors.Is(err, errInvalid) {
		t.Fatal("negative native read", err)
	}
	if info, err := reader.Stat(); err != nil || info.Size() != 3 {
		t.Fatal("native file Stat", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := model.Stat(filepath.Join(dir, "b")); err != nil {
		t.Fatal(err)
	}
	if _, err := model.GetDiskUsage(dir); err != nil {
		t.Fatal(err)
	}
	if names, err := model.List(dir); err != nil || !slices.Contains(names, "b") {
		t.Fatal("bounded native names", err)
	}
	lock, err := model.Lock(filepath.Join(dir, "LOCK"))
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	writable, err := model.OpenReadWrite(filepath.Join(dir, "alias"), vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	if err := writable.Preallocate(-1, 1); !errors.Is(err, errLimit) {
		t.Fatal("negative native allocation", err)
	}
	if err := writable.Preallocate(0, 4096); err != nil {
		t.Fatal(err)
	}
	if err := writable.Close(); err != nil {
		t.Fatal(err)
	}
	reused, err := model.ReuseForWrite(filepath.Join(dir, "alias"), filepath.Join(dir, "reused"), vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reused.WriteAt([]byte("end"), 0); err != nil {
		t.Fatal(err)
	}
	if err := reused.Close(); err != nil {
		t.Fatal(err)
	}
	if err := model.Remove(filepath.Join(dir, "reused")); err != nil {
		t.Fatal(err)
	}
	if err := model.MkdirAll(filepath.Join(dir, "sub", "child"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := model.RemoveAll(filepath.Join(dir, "sub")); err != nil {
		t.Fatal(err)
	}
	if err := model.RemoveAll(dir); !errors.Is(err, errInvalid) {
		t.Fatal("model removed native owner root", err)
	}
}

func TestNativeDurableImageRefusesForeignCorruptAndUnfundedRecovery(t *testing.T) {
	if owner, err := newNativeDurableFS(nil, "native", "shadow", 1, nativeDurableDefaultPolicy()); owner != nil || !errors.Is(err, errInvalid) {
		t.Fatal("nil model context", err)
	}
	parent := t.TempDir()
	dir, shadow := filepath.Join(parent, "native"), filepath.Join(parent, "shadow")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	policy := nativeDurableDefaultPolicy()
	model, err := newNativeDurableFS(t.Context(), dir, shadow, 2, policy)
	if err != nil {
		t.Fatal(err)
	}
	file, err := model.Create(filepath.Join(dir, "actual.log"), vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.Write([]byte("native bytes"))
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		t.Fatal(err)
	}
	directory, err := model.OpenDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
		t.Fatal(err)
	}
	image, digest, err := nativeDurableReadImage(t.Context(), shadow, policy)
	if err != nil {
		t.Fatal(err)
	}
	foreign := model.cookie
	foreign[0] ^= 1
	if receipt, err := nativeDurableRecover(t.Context(), shadow, parent, foreign, digest, policy); !errors.Is(err, errInvalid) || receipt.Directory != "" {
		t.Fatal("foreign image admitted", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := nativeDurableRecover(canceled, shadow, parent, model.cookie, digest, policy); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled recovery admitted", err)
	}
	if len(image.Files) != 1 || image.Files[0].SHA256 != sha256.Sum256([]byte("native bytes")) {
		t.Fatal("real native file digest missing")
	}
	if err := os.WriteFile(filepath.Join(shadow, image.Files[0].Image), []byte("corrupt byte"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeDurableRecover(t.Context(), shadow, parent, model.cookie, digest, policy); !errors.Is(err, errCorrupt) {
		t.Fatal("corrupt native image accepted", err)
	}
	// Admission happens before native Create, even with a genuine source root.
	model.mu.Lock()
	model.ledger.WorkBytes = policy.WorkBytes
	model.mu.Unlock()
	if _, err := model.Create(filepath.Join(dir, "unfunded"), vfs.WriteCategoryUnspecified); !errors.Is(err, errLimit) {
		t.Fatal("unfunded operation entered native FS", err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "unfunded")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("denied operation left native file", err)
	}
}
