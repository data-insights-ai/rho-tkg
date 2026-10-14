package graphapply

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// Caller-owned bounded file cache; no automatic miss fallback or authority.
// Failed writes retire their durable reservation. Extra/temporary files cannot
// be adopted as a generation. The directory has one actual native LOCK owner.
type nativeColumnCache struct {
	mu     sync.Mutex
	fs     vfs.FS
	dir    string
	lock   io.Closer
	closed bool
	poison error
	ledger nativeColumnCacheLedger
}
type nativeColumnCacheLedger struct {
	Version          uint8
	Maximum, Charged uint64
}

func openNativeColumnCache(dir string, fs vfs.FS, create bool, maximum uint64) (cache *nativeColumnCache, err error) {
	if dir == "" || maximum < 2*nativeColumnMetadataBytes || maximum > 1<<40 {
		return nil, errInvalid
	}
	if fs == nil {
		fs = vfs.Default
	}
	if create {
		if _, e := fs.Stat(dir); e == nil {
			return nil, errInvalid
		} else if !errors.Is(e, os.ErrNotExist) {
			return nil, e
		}
		if err = fs.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}
	} else if _, err = fs.Stat(dir); err != nil {
		return nil, err
	}
	lock, err := fs.Lock(fs.PathJoin(dir, "LOCK"))
	if err != nil {
		return nil, err
	}
	c := &nativeColumnCache{fs: fs, dir: dir, lock: lock, ledger: nativeColumnCacheLedger{Version: 1, Maximum: maximum, Charged: 2 * nativeColumnMetadataBytes}}
	defer func() {
		if err != nil {
			err = errors.Join(err, lock.Close())
		}
	}()
	if create {
		wire, e := nativeColumnMetadata(c.ledger)
		if e != nil {
			return nil, e
		}
		if err = c.atomic("LEDGER", wire); err != nil {
			return nil, err
		}
	} else {
		body, e := c.read("LEDGER", nativeColumnMetadataBytes)
		if e != nil {
			return nil, e
		}
		if e = nativeColumnDecodeMetadata(body, &c.ledger); e != nil {
			return nil, e
		}
		if c.ledger.Version != 1 || c.ledger.Maximum != maximum || c.ledger.Charged < 2*nativeColumnMetadataBytes || c.ledger.Charged > maximum {
			return nil, errCorrupt
		}
	}
	return c, nil
}
func (c *nativeColumnCache) Close() error {
	if c == nil {
		return errInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.lock.Close()
}
func nativeColumnCacheChunkName(g nativeColumnGeneration, n uint64) string {
	hash, _ := nativeColumnGenerationHash(g)
	return fmt.Sprintf("chunk-%x-%016x.bin", hash, n)
}
func nativeColumnCacheManifestName(g nativeColumnGeneration) string {
	hash, _ := nativeColumnGenerationHash(g)
	return "manifest-" + hex.EncodeToString(hash[:]) + ".bin"
}
func (c *nativeColumnCache) read(name string, maximum int) (body []byte, err error) {
	file, err := c.fs.Open(c.fs.PathJoin(c.dir, name))
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, file.Close())
		if err != nil {
			body = nil
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > int64(maximum) {
		return nil, errLimit
	}
	body, err = io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	if len(body) > maximum {
		return nil, errLimit
	}
	return body, err
}
func (c *nativeColumnCache) get(name string, maximum int) ([]byte, error) {
	if c == nil || maximum < 1 || maximum > 64<<10 || name == "" || c.fs.PathBase(name) != name {
		return nil, errInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errInvalid
	}
	if c.poison != nil {
		return nil, c.poison
	}
	return c.read(name, maximum)
}
func (c *nativeColumnCache) put(name string, body []byte) error {
	if c == nil || len(body) == 0 || len(body) > 64<<10 || name == "" || c.fs.PathBase(name) != name || name == "LEDGER" || name == "LOCK" {
		return errInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errInvalid
	}
	if c.poison != nil {
		return c.poison
	}
	prior, err := c.read(name, 64<<10)
	if err == nil {
		if !bytes.Equal(prior, body) {
			return errCorrupt
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	charge := uint64(len(name) + len(body))
	if charge > c.ledger.Maximum-c.ledger.Charged {
		return errLimit
	}
	prospective := c.ledger
	prospective.Charged += charge
	wire, err := nativeColumnMetadata(prospective)
	if err != nil {
		return err
	}
	if err = c.atomic("LEDGER", wire); err != nil {
		c.poison = err
		return err
	}
	c.ledger = prospective
	if err = c.atomic(name, body); err != nil {
		c.poison = err
		return err
	}
	return nil
}
func (c *nativeColumnCache) atomic(name string, body []byte) error {
	// FS.Write may mutate its input; hash/identity callers keep their original.
	file, err := c.fs.Create(c.fs.PathJoin(c.dir, name+".tmp"), vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	owned := bytes.Clone(body)
	n, writeErr := file.Write(owned)
	if n != len(owned) && writeErr == nil {
		writeErr = io.ErrShortWrite
	}
	err = errors.Join(writeErr, file.Sync(), file.Close())
	if err != nil {
		return err
	}
	if err = c.fs.Rename(c.fs.PathJoin(c.dir, name+".tmp"), c.fs.PathJoin(c.dir, name)); err != nil {
		return err
	}
	dir, err := c.fs.OpenDir(c.dir)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
