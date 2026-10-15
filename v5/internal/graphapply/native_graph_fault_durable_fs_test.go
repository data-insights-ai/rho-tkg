package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/atomicfs"
)

const nativeDurableProfile = "native-vfs-synced-image-v1"

// These are explicit diagnostic/model allowances for the fixed D slice. They
// neither change raftlog/Pebble admission nor describe measured memory/cost.
// File contents stay on disk; one bounded copy buffer and a finite catalog are
// resident. Every old shadow version remains charged until fixture cleanup.
type nativeDurablePolicy struct {
	Files, Names, CatalogBytes, CopyBytes int
	Operations                            uint64
	ShadowBytes, WorkBytes                uint64
}

func nativeDurableDefaultPolicy() nativeDurablePolicy {
	return nativeDurablePolicy{Files: 1024, Names: 2048, CatalogBytes: 1 << 20, CopyBytes: 64 << 10, Operations: 65536, ShadowBytes: 1 << 30, WorkBytes: 1 << 40}
}

func (p nativeDurablePolicy) valid() bool {
	return p.Files > 0 && p.Files <= 1024 && p.Names > 0 && p.Names <= 2048 && p.CatalogBytes >= 4096 && p.CatalogBytes <= 1<<20 && p.CopyBytes >= 4096 && p.CopyBytes <= 64<<10 && p.Operations > 0 && p.Operations <= 65536 && p.ShadowBytes > 0 && p.ShadowBytes <= 1<<30 && p.WorkBytes > 0 && p.WorkBytes <= 1<<40
}

type nativeDurableName struct {
	Path      string
	File      uint64
	Directory bool
}

type nativeDurableVersion struct {
	File, Operation, Bytes uint64
	Image                  string
	SHA256                 [32]byte
}

type nativeDurableLedger struct {
	Operations, Files, Versions, ShadowBytes, WorkBytes uint64
	CopyReadBytes, CopyWriteBytes                       uint64
}

// Names contains ONLY directory-synced associations. Versions contains ONLY
// data guaranteed by completed native Sync/SyncData or fullSync=true SyncTo.
// A new name without a synced version correctly recovers an empty file.
type nativeDurableImage struct {
	Version uint8
	Profile string
	Cookie  [16]byte
	Member  uint64
	Policy  nativeDurablePolicy
	Ledger  nativeDurableLedger
	Names   []nativeDurableName
	Files   []nativeDurableVersion
}

type nativeDurableFileState struct {
	version nativeDurableVersion
}

// This wrapper executes native IO against the real OS FS. Its separate shadow
// namespace is an explicit crash model, not evidence of physical power loss.
// Callers may restore a sealed image only into a fresh owned directory; they
// never copy the current, possibly unsynced native directory after a crash.
type nativeDurableFS struct {
	vfs.FS
	mu             sync.Mutex
	ctx            context.Context
	dir, shadow    string
	policy         nativeDurablePolicy
	cookie         [16]byte
	member, next   uint64
	ledger         nativeDurableLedger
	names, durable map[string]nativeDurableName
	files          map[uint64]*nativeDurableFileState
	poison         error
}

func newNativeDurableFS(ctx context.Context, dir, shadow string, member uint64, policy nativeDurablePolicy) (*nativeDurableFS, error) {
	if ctx == nil || !policy.valid() || member < 1 || member > 6 {
		return nil, errInvalid
	}
	root, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	model, err := filepath.Abs(shadow)
	if err != nil || model == root || strings.HasPrefix(model, root+string(os.PathSeparator)) || strings.HasPrefix(root, model+string(os.PathSeparator)) {
		return nil, errors.Join(errInvalid, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Exclusive creation gives this actual process one private model namespace.
	if err := os.Mkdir(model, 0700); err != nil {
		return nil, err
	}
	f := &nativeDurableFS{FS: vfs.Default, ctx: ctx, dir: root, shadow: model, policy: policy, member: member, names: make(map[string]nativeDurableName), durable: make(map[string]nativeDurableName), files: make(map[uint64]*nativeDurableFileState)}
	if _, err := io.ReadFull(rand.Reader, f.cookie[:]); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *nativeDurableFS) Unwrap() vfs.FS { return f.FS }

func (f *nativeDurableFS) path(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(f.dir, absolute)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) || filepath.IsAbs(relative) || len(relative) > 1024 {
		return "", errors.Join(errInvalid, err)
	}
	return filepath.ToSlash(relative), nil
}

func (f *nativeDurableFS) admitLocked(work uint64) error {
	if f.poison != nil {
		return errors.Join(errInvalid, f.poison)
	}
	if err := f.ctx.Err(); err != nil {
		return err
	}
	if f.ledger.Operations >= f.policy.Operations || work > f.policy.WorkBytes-f.ledger.WorkBytes {
		return errLimit
	}
	f.ledger.Operations++
	f.ledger.WorkBytes += work
	return nil
}

func (f *nativeDurableFS) failLocked(err error) error {
	if err != nil {
		f.poison = errors.Join(f.poison, err)
	}
	return err
}

func (f *nativeDurableFS) allocateLocked(name string, directory bool) (nativeDurableName, error) {
	if len(f.files) >= f.policy.Files || len(f.names) >= f.policy.Names || f.next == ^uint64(0) {
		return nativeDurableName{}, errLimit
	}
	f.next++
	n := nativeDurableName{Path: name, File: f.next, Directory: directory}
	f.files[n.File] = &nativeDurableFileState{version: nativeDurableVersion{File: n.File}}
	f.names[name] = n
	f.ledger.Files++
	return n, nil
}

func (f *nativeDurableFS) Create(path string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name, err := f.path(path)
	if err != nil {
		return nil, err
	}
	if err := f.admitLocked(4096); err != nil {
		return nil, err
	}
	if len(f.files) >= f.policy.Files || len(f.names) >= f.policy.Names {
		return nil, errLimit
	}
	actual, err := f.FS.Create(path, category)
	if err != nil {
		return nil, err
	}
	n, err := f.allocateLocked(name, false)
	if err != nil {
		return nil, errors.Join(f.failLocked(err), actual.Close())
	}
	return &nativeDurableFile{File: actual, owner: f, id: n.File, path: name}, nil
}

func (f *nativeDurableFS) Open(path string, options ...vfs.OpenOption) (vfs.File, error) {
	return f.open(path, false, vfs.WriteCategoryUnspecified, options...)
}

func (f *nativeDurableFS) OpenReadWrite(path string, category vfs.DiskWriteCategory, options ...vfs.OpenOption) (vfs.File, error) {
	return f.open(path, true, category, options...)
}

func (f *nativeDurableFS) open(path string, writable bool, category vfs.DiskWriteCategory, options ...vfs.OpenOption) (vfs.File, error) {
	file, err := f.openOwned(path, writable, category)
	if err != nil {
		return nil, err
	}
	// Native OpenOptions may call the wrapped file. Apply them after releasing
	// the model lock; they cannot change its recorded file/name identity.
	for _, option := range options {
		option.Apply(file)
	}
	return file, nil
}

func (f *nativeDurableFS) openOwned(path string, writable bool, category vfs.DiskWriteCategory) (vfs.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name, err := f.path(path)
	if err != nil {
		return nil, err
	}
	if err := f.admitLocked(4096); err != nil {
		return nil, err
	}
	n, found := f.names[name]
	if found && n.Directory || !found && writable && (len(f.files) >= f.policy.Files || len(f.names) >= f.policy.Names) {
		return nil, errInvalid
	}
	var actual vfs.File
	if writable {
		actual, err = f.FS.OpenReadWrite(path, category)
	} else {
		actual, err = f.FS.Open(path)
	}
	if err != nil {
		return nil, err
	}
	if !found && !writable {
		// Preserve genuine missing-file errors during Pebble initialization,
		// but never silently import an unknown pre-existing native data file.
		return nil, errors.Join(f.failLocked(errInvalid), actual.Close())
	}
	if !found {
		n, err = f.allocateLocked(name, false)
		if err != nil {
			return nil, errors.Join(f.failLocked(err), actual.Close())
		}
	}
	return &nativeDurableFile{File: actual, owner: f, id: n.File, path: name}, nil
}

func (f *nativeDurableFS) OpenDir(path string) (vfs.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name, err := f.path(path)
	if err != nil {
		return nil, err
	}
	if err := f.admitLocked(4096); err != nil {
		return nil, err
	}
	if n, exists := f.names[name]; name != "." && (!exists || !n.Directory) {
		return nil, errInvalid
	}
	actual, err := f.FS.OpenDir(path)
	if err != nil {
		return nil, err
	}
	return &nativeDurableFile{File: actual, owner: f, path: name, directory: true}, nil
}

func (f *nativeDurableFS) Link(oldpath, newpath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	oldname, err := f.path(oldpath)
	if err != nil {
		return err
	}
	newname, err := f.path(newpath)
	if err != nil {
		return err
	}
	if err := f.admitLocked(4096); err != nil {
		return err
	}
	n, found := f.names[oldname]
	if !found || n.Directory || len(f.names) >= f.policy.Names {
		return errInvalid
	}
	if err := f.FS.Link(oldpath, newpath); err != nil {
		return err
	}
	n.Path = newname
	f.names[newname] = n
	return nil
}

func (f *nativeDurableFS) Rename(oldpath, newpath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.renameLocked(oldpath, newpath)
}

func (f *nativeDurableFS) renameLocked(oldpath, newpath string) error {
	oldname, err := f.path(oldpath)
	if err != nil {
		return err
	}
	newname, err := f.path(newpath)
	if err != nil {
		return err
	}
	if err := f.admitLocked(4096); err != nil {
		return err
	}
	n, found := f.names[oldname]
	if !found || n.Directory {
		return errInvalid
	}
	if err := f.FS.Rename(oldpath, newpath); err != nil {
		return err
	}
	delete(f.names, oldname)
	n.Path = newname
	f.names[newname] = n
	return nil
}

func (f *nativeDurableFS) ReuseForWrite(oldpath, newpath string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Default's concrete ReuseForWrite is rename + writable open without
	// truncation; preserving identity is essential for an unsynced reused WAL.
	if err := f.renameLocked(oldpath, newpath); err != nil {
		return nil, err
	}
	actual, err := f.FS.OpenReadWrite(newpath, category)
	if err != nil {
		return nil, f.failLocked(err)
	}
	name, _ := f.path(newpath)
	return &nativeDurableFile{File: actual, owner: f, id: f.names[name].File, path: name}, nil
}

func (f *nativeDurableFS) Remove(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	name, err := f.path(path)
	if err != nil {
		return err
	}
	if err := f.admitLocked(4096); err != nil {
		return err
	}
	if err := f.FS.Remove(path); err != nil {
		return err
	}
	delete(f.names, name)
	return nil
}

func (f *nativeDurableFS) RemoveAll(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	name, err := f.path(path)
	if err != nil || name == "." {
		return errors.Join(errInvalid, err)
	}
	if err := f.admitLocked(4096); err != nil {
		return err
	}
	if err := f.FS.RemoveAll(path); err != nil {
		return err
	}
	for key := range f.names {
		if key == name || strings.HasPrefix(key, name+"/") {
			delete(f.names, key)
		}
	}
	return nil
}

func (f *nativeDurableFS) MkdirAll(path string, mode os.FileMode) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	name, err := f.path(path)
	if err != nil {
		return err
	}
	if err := f.admitLocked(4096); err != nil {
		return err
	}
	if name == "." {
		return f.FS.MkdirAll(path, mode)
	}
	parts := strings.Split(name, "/")
	if len(parts) > 8 || len(parts) > f.policy.Files-len(f.files) || len(parts) > f.policy.Names-len(f.names) {
		return errLimit
	}
	if err := f.FS.MkdirAll(path, mode); err != nil {
		return err
	}
	for end := range parts {
		prefix := strings.Join(parts[:end+1], "/")
		if _, exists := f.names[prefix]; !exists {
			if _, err := f.allocateLocked(prefix, true); err != nil {
				return f.failLocked(err)
			}
		}
	}
	return nil
}

type nativeDurableFile struct {
	vfs.File
	owner     *nativeDurableFS
	id        uint64
	path      string
	directory bool
	closed    bool
	closeErr  error
}

func (file *nativeDurableFile) Read(data []byte) (int, error) {
	return file.read(data, func() (int, error) { return file.File.Read(data) })
}

func (file *nativeDurableFile) ReadAt(data []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, errInvalid
	}
	return file.read(data, func() (int, error) { return file.File.ReadAt(data, offset) })
}

func (file *nativeDurableFile) read(data []byte, call func() (int, error)) (int, error) {
	f := file.owner
	f.mu.Lock()
	defer f.mu.Unlock()
	if file.closed || file.directory || len(data) > processFrameBytes {
		return 0, errInvalid
	}
	if err := f.admitLocked(uint64(len(data)) + 4096); err != nil {
		return 0, err
	}
	return call()
}

func (file *nativeDurableFile) Stat() (vfs.FileInfo, error) {
	f := file.owner
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.admitLocked(4096); err != nil {
		return nil, err
	}
	return file.File.Stat()
}

func (file *nativeDurableFile) Close() error {
	f := file.owner
	f.mu.Lock()
	defer f.mu.Unlock()
	if file.closed {
		return file.closeErr
	}
	file.closed = true
	// Cleanup must release the actual descriptor even after budget/deadline
	// exhaustion. It cannot publish any new stable image or native success.
	file.closeErr = file.File.Close()
	return file.closeErr
}

func (f *nativeDurableFS) Lock(path string) (io.Closer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name, err := f.path(path)
	if err != nil || name != "LOCK" {
		return nil, errors.Join(errInvalid, err)
	}
	if err := f.admitLocked(4096); err != nil {
		return nil, err
	}
	// The native OS advisory lock remains the sole process exclusion. A LOCK
	// file carries no graph data and is never restored from the shadow model.
	return f.FS.Lock(path)
}

func (f *nativeDurableFS) Stat(path string) (vfs.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.path(path); err != nil {
		return nil, err
	}
	if err := f.admitLocked(4096); err != nil {
		return nil, err
	}
	return f.FS.Stat(path)
}

func (f *nativeDurableFS) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.path(path); err != nil {
		return vfs.DiskUsage{}, err
	}
	if err := f.admitLocked(4096); err != nil {
		return vfs.DiskUsage{}, err
	}
	return f.FS.GetDiskUsage(path)
}

func (f *nativeDurableFS) List(path string) (names []string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.path(path); err != nil {
		return nil, err
	}
	if err := f.admitLocked(uint64(f.policy.CatalogBytes)); err != nil {
		return nil, err
	}
	directory, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, directory.Close()) }()
	for {
		page, readErr := directory.Readdirnames(32)
		// A few explicitly external diagnostic/control files can share the
		// native directory; none enters the stable graph catalog.
		if len(page) > f.policy.Names+64-len(names) {
			return nil, errLimit
		}
		names = append(names, page...)
		if errors.Is(readErr, io.EOF) {
			return names, nil
		}
		if readErr != nil {
			return nil, readErr
		}
	}
}

func (file *nativeDurableFile) Write(data []byte) (int, error) {
	return file.write(data, func() (int, error) { return file.File.Write(data) })
}

func (file *nativeDurableFile) WriteAt(data []byte, offset int64) (int, error) {
	if offset < 0 || uint64(offset) > file.owner.policy.ShadowBytes || uint64(len(data)) > file.owner.policy.ShadowBytes-uint64(offset) {
		return 0, errLimit
	}
	return file.write(data, func() (int, error) { return file.File.WriteAt(data, offset) })
}

func (file *nativeDurableFile) write(data []byte, call func() (int, error)) (int, error) {
	f := file.owner
	f.mu.Lock()
	defer f.mu.Unlock()
	if file.closed || file.directory || len(data) > processFrameBytes {
		return 0, errLimit
	}
	if err := f.admitLocked(uint64(len(data)) + 4096); err != nil {
		return 0, err
	}
	return call()
}

func (file *nativeDurableFile) Preallocate(offset, length int64) error {
	f := file.owner
	f.mu.Lock()
	defer f.mu.Unlock()
	if offset < 0 || length < 0 || uint64(offset) > f.policy.ShadowBytes || uint64(length) > f.policy.ShadowBytes-uint64(offset) {
		return errLimit
	}
	if err := f.admitLocked(4096); err != nil {
		return err
	}
	return file.File.Preallocate(offset, length)
}

func (file *nativeDurableFile) Sync() error {
	_, err := file.sync(1, 0)
	return err
}

func (file *nativeDurableFile) SyncData() error {
	_, err := file.sync(2, 0)
	return err
}

func (file *nativeDurableFile) SyncTo(length int64) (bool, error) {
	if length < 0 {
		return false, errInvalid
	}
	return file.sync(3, length)
}

func (file *nativeDurableFile) sync(kind uint8, prefix int64) (bool, error) {
	f := file.owner
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.admitLocked(4096); err != nil {
		return false, err
	}
	if file.directory {
		if kind == 3 {
			return file.File.SyncTo(prefix) // No directory persistence guarantee.
		}
		if kind == 2 {
			return false, file.File.SyncData() // Data-only sync does not publish names.
		}
		if err := f.admitCatalogLocked(); err != nil {
			return false, err
		}
		if err := file.File.Sync(); err != nil {
			return false, err
		}
		for name := range f.durable {
			if filepath.ToSlash(filepath.Dir(name)) == file.path {
				delete(f.durable, name)
			}
		}
		for name, entry := range f.names {
			if filepath.ToSlash(filepath.Dir(name)) == file.path {
				f.durable[name] = entry
			}
		}
		return true, f.persistLocked()
	}
	info, err := file.File.Stat()
	if err != nil || info.Size() < 0 {
		return false, errors.Join(errInvalid, err)
	}
	size := uint64(info.Size())
	if err := f.admitCopyLocked(size); err != nil {
		return false, err
	}
	full := true
	if kind == 3 {
		full, err = file.File.SyncTo(prefix)
	} else if kind == 2 {
		err = file.File.SyncData()
	} else {
		err = file.File.Sync()
	}
	if err != nil || !full {
		// SyncTo(false,nil) is only a queued prefix, with NO guarantee. No
		// byte or name from that call enters this conservative stable image.
		return full, err
	}
	version := nativeDurableVersion{File: file.id, Operation: f.ledger.Operations, Bytes: size, Image: fmt.Sprintf("%016x-%016x.image", file.id, f.ledger.Operations)}
	version.SHA256, err = f.copyNativeLocked(file.File, version.Image, size)
	if err != nil {
		return false, f.failLocked(err)
	}
	state := f.files[file.id]
	if state == nil {
		return false, f.failLocked(errInvalid)
	}
	state.version = version
	f.ledger.Versions++
	return true, f.persistLocked()
}

func (f *nativeDurableFS) admitCatalogLocked() error {
	// Marshal, retained image bytes and temporary atomic metadata publication
	// are charged before either native directory Sync or source-file copying.
	return f.admitLocked(uint64(4 * f.policy.CatalogBytes))
}

func (f *nativeDurableFS) admitCopyLocked(size uint64) error {
	if size > f.policy.ShadowBytes-f.ledger.ShadowBytes || size > (^uint64(0)-uint64(4*f.policy.CatalogBytes))/2 {
		return errLimit
	}
	if err := f.admitLocked(2*size + uint64(4*f.policy.CatalogBytes)); err != nil {
		return err
	}
	// Work is retired even when SyncTo returns false. ShadowBytes records
	// actual copied disk bytes, rather than the theoretical prepaid grant.
	return nil
}

func (f *nativeDurableFS) copyNativeLocked(source vfs.File, name string, size uint64) (sum [32]byte, err error) {
	path := filepath.Join(f.shadow, name)
	destination, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return sum, err
	}
	buffer := make([]byte, f.policy.CopyBytes)
	digest := sha256.New()
	for offset := uint64(0); offset < size; {
		if err := f.ctx.Err(); err != nil {
			return sum, errors.Join(err, destination.Close())
		}
		n := min(uint64(len(buffer)), size-offset)
		read, readErr := source.ReadAt(buffer[:int(n)], int64(offset)) // #nosec G115 -- bounded by the finite1GiB shadow policy.
		f.ledger.CopyReadBytes += uint64(read)
		if read != int(n) || readErr != nil && !errors.Is(readErr, io.EOF) {
			return sum, errors.Join(io.ErrUnexpectedEOF, readErr, destination.Close())
		}
		written, writeErr := destination.Write(buffer[:read])
		f.ledger.CopyWriteBytes += uint64(written)
		f.ledger.ShadowBytes += uint64(written)
		if written != read || writeErr != nil {
			return sum, errors.Join(io.ErrShortWrite, writeErr, destination.Close())
		}
		_, _ = digest.Write(buffer[:read])
		offset += n
	}
	copy(sum[:], digest.Sum(nil))
	return sum, errors.Join(destination.Sync(), destination.Close())
}

func (f *nativeDurableFS) imageLocked() nativeDurableImage {
	image := nativeDurableImage{Version: 1, Profile: nativeDurableProfile, Cookie: f.cookie, Member: f.member, Policy: f.policy, Ledger: f.ledger}
	for _, name := range f.durable {
		image.Names = append(image.Names, name)
	}
	for _, state := range f.files {
		image.Files = append(image.Files, state.version)
	}
	slices.SortFunc(image.Names, func(a, b nativeDurableName) int { return strings.Compare(a.Path, b.Path) })
	slices.SortFunc(image.Files, func(a, b nativeDurableVersion) int {
		if a.File < b.File {
			return -1
		}
		if a.File > b.File {
			return 1
		}
		return 0
	})
	return image
}

func (f *nativeDurableFS) persistLocked() error {
	wire, err := json.Marshal(f.imageLocked())
	if err != nil || len(wire) > f.policy.CatalogBytes {
		return f.failLocked(errors.Join(errLimit, err))
	}
	digest := sha256.Sum256(wire)
	wire = append(append(wire, '\n'), []byte(hex.EncodeToString(digest[:]))...)
	temporary := filepath.Join(f.shadow, fmt.Sprintf("catalog-%016x.tmp", f.ledger.Operations))
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return f.failLocked(err)
	}
	n, writeErr := file.Write(wire)
	if n != len(wire) {
		writeErr = errors.Join(io.ErrShortWrite, writeErr)
	}
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return f.failLocked(err)
	}
	if err := os.Rename(temporary, filepath.Join(f.shadow, "CATALOG")); err != nil {
		return f.failLocked(err)
	}
	directory, err := os.Open(f.shadow)
	if err != nil {
		return f.failLocked(err)
	}
	return f.failLocked(errors.Join(directory.Sync(), directory.Close()))
}

func decodeNativeDurableImage(wire []byte, policy nativeDurablePolicy) (nativeDurableImage, error) {
	var image nativeDurableImage
	if !policy.valid() || len(wire) < 66 || len(wire) > policy.CatalogBytes+65 || wire[len(wire)-65] != '\n' {
		return image, errInvalid
	}
	body := wire[:len(wire)-65]
	sum := sha256.Sum256(body)
	if string(wire[len(wire)-64:]) != hex.EncodeToString(sum[:]) {
		return image, errCorrupt
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&image); err != nil {
		return image, errors.Join(errCorrupt, err)
	}
	canonical, err := json.Marshal(image)
	if err != nil || !bytes.Equal(body, canonical) || image.Version != 1 || image.Profile != nativeDurableProfile || image.Cookie == ([16]byte{}) || image.Member < 1 || image.Member > 6 || image.Policy != policy || len(image.Names) > policy.Names || len(image.Files) > policy.Files || image.Ledger.Operations > policy.Operations || image.Ledger.WorkBytes > policy.WorkBytes || image.Ledger.ShadowBytes > policy.ShadowBytes {
		return image, errors.Join(errCorrupt, err)
	}
	if image.Ledger.Files != uint64(len(image.Files)) || image.Ledger.Versions > image.Ledger.Operations || image.Ledger.CopyReadBytes > image.Ledger.ShadowBytes || image.Ledger.CopyWriteBytes > image.Ledger.ShadowBytes {
		return image, errCorrupt
	}
	files := make(map[uint64]nativeDurableVersion, len(image.Files))
	lastID, latestBytes := uint64(0), uint64(0)
	for _, file := range image.Files {
		if file.File <= lastID || file.Operation > image.Ledger.Operations || file.Bytes > policy.ShadowBytes-latestBytes {
			return image, errCorrupt
		}
		if file.Operation == 0 {
			if file.Image != "" || file.Bytes != 0 || file.SHA256 != ([32]byte{}) {
				return image, errCorrupt
			}
		} else if file.Image != fmt.Sprintf("%016x-%016x.image", file.File, file.Operation) || file.SHA256 == ([32]byte{}) {
			return image, errCorrupt
		}
		files[file.File], lastID = file, file.File
		latestBytes += file.Bytes
	}
	if latestBytes > image.Ledger.ShadowBytes {
		return image, errCorrupt
	}
	lastName := ""
	kinds := make(map[uint64]bool, len(image.Names))
	for _, name := range image.Names {
		file, found := files[name.File]
		kind, seen := kinds[name.File]
		if !found || name.Path <= lastName || len(name.Path) > 1024 || name.Path == "." || filepath.IsAbs(name.Path) || filepath.ToSlash(filepath.Clean(name.Path)) != name.Path || strings.HasPrefix(name.Path, "../") || name.Path == ".." || strings.Contains(name.Path, "\\") || seen && kind != name.Directory || name.Directory && file.Operation != 0 {
			return image, errCorrupt
		}
		kinds[name.File], lastName = name.Directory, name.Path
	}
	return image, nil
}

type nativeDurableRecovery struct {
	Version                        uint8
	Profile                        string
	SourceCookie                   [16]byte
	Member                         uint64
	CatalogSHA                     [32]byte
	Names, Files, Bytes, WorkBytes uint64
	Directory                      string
}

func nativeDurableReadImage(ctx context.Context, shadow string, policy nativeDurablePolicy) (nativeDurableImage, [32]byte, error) {
	if ctx == nil || !policy.valid() {
		return nativeDurableImage{}, [32]byte{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nativeDurableImage{}, [32]byte{}, err
	}
	path := filepath.Join(shadow, "CATALOG")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 66 || info.Size() > int64(policy.CatalogBytes+65) {
		return nativeDurableImage{}, [32]byte{}, errors.Join(errCorrupt, err)
	}
	file, err := os.Open(path)
	if err != nil {
		return nativeDurableImage{}, [32]byte{}, err
	}
	wire, readErr := io.ReadAll(io.LimitReader(file, int64(policy.CatalogBytes+66)))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return nativeDurableImage{}, [32]byte{}, err
	}
	image, err := decodeNativeDurableImage(wire, policy)
	return image, sha256.Sum256(wire), err
}

// nativeDurableRecover uses ONLY the sealed native byte images. Its caller
// binds the actual source cookie/catalog hash before this call and reaps the
// old process first. No current native file, record map or synthetic Host is
// consulted when selecting recovered bytes.
func nativeDurableRecover(ctx context.Context, shadow, parent string, expectedCookie [16]byte, expectedSHA [32]byte, policy nativeDurablePolicy) (receipt nativeDurableRecovery, err error) {
	return nativeDurableRecoverCharged(ctx, shadow, parent, expectedCookie, expectedSHA, policy, 0)
}

func nativeDurableRecoverCharged(ctx context.Context, shadow, parent string, expectedCookie [16]byte, expectedSHA [32]byte, policy nativeDurablePolicy, previousWork uint64) (receipt nativeDurableRecovery, err error) {
	image, digest, err := nativeDurableReadImage(ctx, shadow, policy)
	if err != nil || expectedCookie == ([16]byte{}) || expectedSHA == ([32]byte{}) || image.Cookie != expectedCookie || digest != expectedSHA {
		return receipt, errors.Join(errInvalid, err)
	}
	grant := 2*image.Ledger.ShadowBytes + uint64(4*policy.CatalogBytes+4096*policy.Names)
	if previousWork == 0 {
		previousWork = image.Ledger.WorkBytes
	}
	if previousWork < image.Ledger.WorkBytes || previousWork > policy.WorkBytes || grant > policy.WorkBytes-previousWork {
		return receipt, errLimit
	}
	parentInfo, err := os.Lstat(parent)
	if err != nil || !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 {
		return receipt, errors.Join(errInvalid, err)
	}
	destination, err := os.MkdirTemp(parent, "native-stable-recovery-")
	if err != nil {
		return receipt, err
	}
	receipt = nativeDurableRecovery{Version: 1, Profile: nativeDurableProfile, SourceCookie: image.Cookie, Member: image.Member, CatalogSHA: digest, Directory: destination, WorkBytes: previousWork + grant}
	files := make(map[uint64]nativeDurableVersion, len(image.Files))
	for _, version := range image.Files {
		files[version.File] = version
	}
	visible := map[string]bool{".": true}
	created := make(map[uint64]string, len(image.Files))
	buffer := make([]byte, policy.CopyBytes)
	for _, name := range image.Names {
		if !visible[filepath.ToSlash(filepath.Dir(name.Path))] {
			continue // A child directory's own Sync cannot publish its parent name.
		}
		if err := ctx.Err(); err != nil {
			return receipt, err
		}
		path := filepath.Join(destination, filepath.FromSlash(name.Path))
		receipt.Names++
		if name.Directory {
			if err := os.Mkdir(path, 0700); err != nil {
				return receipt, err
			}
			visible[name.Path] = true
			continue
		}
		version := files[name.File]
		if prior, exists := created[name.File]; exists {
			if err := os.Link(prior, path); err != nil {
				return receipt, err
			}
			continue
		}
		if version.Bytes > policy.ShadowBytes-receipt.Bytes || receipt.Files >= uint64(policy.Names) {
			return receipt, errLimit
		}
		// Admit each complete native source/copy before opening either file.
		receipt.Files++
		receipt.Bytes += version.Bytes
		if err := nativeDurableCopyVersion(ctx, shadow, path, version, buffer); err != nil {
			return receipt, err
		}
		created[name.File] = path
	}
	// Child directories are synced deepest-first, then their parent root. The
	// resulting OS directory is genuinely durable before native raftlog.Open.
	directories := make([]string, 0, len(visible))
	for directory := range visible {
		directories = append(directories, directory)
	}
	slices.SortFunc(directories, func(a, b string) int {
		if len(a) != len(b) {
			return len(b) - len(a)
		}
		return strings.Compare(a, b)
	})
	for _, name := range directories {
		directory, err := os.Open(filepath.Join(destination, filepath.FromSlash(name)))
		if err != nil {
			return receipt, err
		}
		if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
			return receipt, err
		}
	}
	return receipt, ctx.Err()
}

type nativeFaultFamilyCorruption struct {
	SourceCookie                     [16]byte
	Path                             string
	File, Operation, Size, WorkBytes uint64
	Offset                           int64
	Before, After                    byte
	BeforeSHA256, AfterSHA256        [32]byte
	ActiveManifest                   string
	EligibleWALs                     int
	RequiredImage, RequiredOutcome   []byte
}

// Mutate an actual restored native record byte, not the shadow/catalog. The
// caller reaps its old process first and later qualifies literal native Open's
// refusal. No logical graph row or reconstructed Host enters this operation.
func nativeDurableCorrupt(ctx context.Context, dir string, image nativeDurableImage, suffix string, spent uint64, requiredImage, requiredOutcome []byte) (receipt nativeFaultFamilyCorruption, err error) {
	if ctx == nil || !image.Policy.valid() || suffix != ".log" && suffix != "manifest" || spent > image.Policy.WorkBytes || len(requiredImage) != 172 || len(requiredOutcome) == 0 || len(requiredOutcome) > 512 {
		return receipt, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return receipt, err
	}
	metadataGrant := uint64(4*image.Policy.CatalogBytes + 8192)
	if spent < image.Ledger.WorkBytes || metadataGrant > image.Policy.WorkBytes-spent {
		return receipt, errLimit
	}
	spent += metadataGrant // Before native metadata effects or catalog ownership.
	root, err := os.Lstat(dir)
	if err != nil || !root.IsDir() || root.Mode()&os.ModeSymlink != 0 {
		return receipt, errors.Join(errInvalid, err)
	}
	versions := make(map[uint64]nativeDurableVersion, len(image.Files))
	for _, version := range image.Files {
		versions[version.File] = version
	}
	listing := make([]string, 0, len(image.Names))
	for _, name := range image.Names {
		if filepath.Base(name.Path) == name.Path {
			listing = append(listing, name.Path)
		}
	}
	// Invoke Pebble's own marker selection grammar against genuine sealed
	// directory entries, never a filename-first guess at a required manifest.
	marker, active, err := atomicfs.LocateMarkerInListing(vfs.Default, dir, "manifest", listing)
	if err != nil {
		return receipt, err
	}
	if err := marker.Close(); err != nil {
		return receipt, err
	}
	if active == "" || filepath.Base(active) != active || !strings.HasPrefix(active, "MANIFEST-") {
		return receipt, errCorrupt
	}
	receipt.ActiveManifest = active
	var version nativeDurableVersion
	for _, name := range image.Names {
		if suffix == ".log" && !name.Directory && strings.HasSuffix(name.Path, ".log") {
			receipt.EligibleWALs++
		}
		if !name.Directory && (suffix == ".log" && strings.HasSuffix(name.Path, ".log") || suffix == "manifest" && name.Path == active) && versions[name.File].Bytes >= 32 {
			receipt.Path = name.Path
			version = versions[name.File]
		}
	}
	if suffix == ".log" && (receipt.EligibleWALs != 1 || len(requiredImage) != 172 || len(requiredOutcome) == 0 || len(requiredOutcome) > 512) {
		return receipt, errInvalid
	}
	if receipt.Path == "" || version.Operation == 0 || version.Bytes > image.Policy.ShadowBytes || filepath.Base(receipt.Path) != receipt.Path {
		return receipt, errInvalid
	}
	grant := 8*version.Bytes + uint64(2*image.Policy.CopyBytes+8192)
	if grant > image.Policy.WorkBytes-spent {
		return receipt, errLimit
	}
	receipt.SourceCookie, receipt.File, receipt.Operation, receipt.Size, receipt.WorkBytes = image.Cookie, version.File, version.Operation, version.Bytes, spent+grant
	path := filepath.Join(dir, receipt.Path)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != version.Bytes {
		return receipt, errors.Join(errCorrupt, err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return receipt, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	buffer := make([]byte, image.Policy.CopyBytes+511)
	foundImage, foundOutcome := false, false
	hash := func() ([32]byte, error) {
		digest := sha256.New()
		overlap := 0
		for offset := uint64(0); offset < version.Bytes; {
			if err := ctx.Err(); err != nil {
				return [32]byte{}, err
			}
			n := min(uint64(image.Policy.CopyBytes), version.Bytes-offset)
			read, err := file.ReadAt(buffer[overlap:overlap+int(n)], int64(offset)) // #nosec G115 -- finite1GiB native diagnostic policy.
			if err != nil || read != int(n) {
				return [32]byte{}, errors.Join(io.ErrUnexpectedEOF, err)
			}
			_, _ = digest.Write(buffer[overlap : overlap+read])
			window := buffer[:overlap+read]
			foundImage = foundImage || bytes.Contains(window, requiredImage)
			foundOutcome = foundOutcome || bytes.Contains(window, requiredOutcome)
			overlap = min(511, len(window))
			copy(buffer[:overlap], window[len(window)-overlap:])
			offset += n
		}
		var sum [32]byte
		copy(sum[:], digest.Sum(nil))
		return sum, nil
	}
	receipt.BeforeSHA256, err = hash()
	if err != nil || receipt.BeforeSHA256 != version.SHA256 {
		return receipt, errors.Join(errCorrupt, err)
	}
	if suffix == ".log" {
		if !foundImage || !foundOutcome {
			return receipt, errCorrupt
		}
		receipt.RequiredImage, receipt.RequiredOutcome = bytes.Clone(requiredImage), bytes.Clone(requiredOutcome)
	}
	var actual [1]byte
	if n, err := file.ReadAt(actual[:], 0); err != nil || n != 1 {
		return receipt, errors.Join(io.ErrUnexpectedEOF, err)
	}
	receipt.Before, receipt.After = actual[0], actual[0]^0x80
	actual[0] = receipt.After
	if n, err := file.WriteAt(actual[:], 0); err != nil || n != 1 {
		return receipt, errors.Join(io.ErrShortWrite, err)
	}
	if err := file.Sync(); err != nil {
		return receipt, err
	}
	receipt.AfterSHA256, err = hash()
	if err == nil && receipt.AfterSHA256 == receipt.BeforeSHA256 {
		err = errCorrupt
	}
	return receipt, err
}

func nativeDurableCopyVersion(ctx context.Context, shadow, destination string, version nativeDurableVersion, buffer []byte) (err error) {
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, output.Close()) }()
	if version.Operation == 0 {
		return output.Sync() // Genuine directory-synced, never-data-synced empty file.
	}
	path := filepath.Join(shadow, version.Image)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != version.Bytes {
		return errors.Join(errCorrupt, err)
	}
	input, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, input.Close()) }()
	info, err = input.Stat()
	if err != nil || info.Size() < 0 || uint64(info.Size()) != version.Bytes || !info.Mode().IsRegular() {
		return errors.Join(errCorrupt, err)
	}
	digest := sha256.New()
	for remaining := version.Bytes; remaining != 0; {
		if err := ctx.Err(); err != nil {
			return err
		}
		length := min(remaining, uint64(len(buffer)))
		read, readErr := io.ReadFull(input, buffer[:int(length)]) // #nosec G115 -- finite64KiB copy page.
		if readErr != nil || read != int(length) {
			return errors.Join(io.ErrUnexpectedEOF, readErr)
		}
		written, writeErr := output.Write(buffer[:read])
		if writeErr != nil || written != read {
			return errors.Join(io.ErrShortWrite, writeErr)
		}
		_, _ = digest.Write(buffer[:read])
		remaining -= length
	}
	if !bytes.Equal(digest.Sum(nil), version.SHA256[:]) {
		return errCorrupt
	}
	return output.Sync()
}
