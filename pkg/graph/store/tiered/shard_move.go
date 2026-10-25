package tiered

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// The mover (ADR-0012, SHARD step 4): a closed event shard is copied file by
// file into <volume>/events/<name>.moving-<nonce>, renamed to
// <volume>/events/<name>, the catalog saved with the new volume (the commit
// point), then the source removed; files and directories are fsynced before
// each next step. A crash before the catalog save leaves the shard where it
// was, one from it on leaves it on the new volume; the copy the catalog does
// not name is removed at the next open (removeMoveOrphans).

// ErrShardOpen is returned by MoveShard for a shard whose store is open.
var ErrShardOpen = errors.New("graph: shard is open")

// moveChunk is the copy buffer; an all-zero chunk is skipped, so a sparse
// file stays sparse on the new volume.
const moveChunk = 1 << 20

// MoveShard moves the closed event shard name to volume (hot, warm or cold).
// While it moves, the shard's readers and writers wait. A shard already on
// volume is left alone. ctx, or Close, cancels a move before its catalog save
// and leaves the shard where it was.
func (ts *Store) MoveShard(ctx context.Context, name, volume string) error {
	vol, ok := parseVolume(volume)
	if !ok {
		return fmt.Errorf("graph: MoveShard: %q is not a volume (hot, warm or cold)", volume)
	}
	if ts.inMemory {
		return fmt.Errorf("graph: MoveShard: an in-memory store has no volumes")
	}
	dstRoot, ok := ts.volumeDir(vol)
	if !ok {
		return fmt.Errorf("graph: MoveShard: volume %s, which the config does not map (Config.WarmDir, Config.ColdDir)", volumeName(vol))
	}
	release, err := ts.beginSequentialStoreWideOperation()
	if err != nil {
		return err
	}
	defer release()

	ts.mu.RLock()
	es := ts.eventShards[name]
	hot := es != nil && es == ts.hotShard
	ts.mu.RUnlock()
	if es == nil {
		return fmt.Errorf("graph: MoveShard: no event shard %q", name)
	}
	if hot {
		return fmt.Errorf("graph: MoveShard: %q is the hot shard, which stays on the hot volume", name)
	}

	es.shardMu.Lock()
	es.waitMoveLocked()
	if es.store != nil {
		es.shardMu.Unlock()
		return fmt.Errorf("%w: %s", ErrShardOpen, name)
	}
	done := make(chan struct{})
	es.moveDone = done
	es.shardMu.Unlock()
	defer func() {
		es.shardMu.Lock()
		es.moveDone = nil
		close(done)
		es.shardMu.Unlock()
	}()

	entry, ok := ts.catalog.GetShard(name)
	if !ok {
		return fmt.Errorf("graph: MoveShard: no event shard %q", name)
	}
	if entry.Volume == vol {
		return nil
	}
	if err := ts.moveFaultAt("gated"); err != nil {
		return err
	}
	return ts.moveClosedShard(ctx, es, vol, dstRoot)
}

// waitMoveLocked returns once no move of es is running. Caller holds
// es.shardMu, which it holds again on return; every open of a closed shard
// calls it first.
func (es *EventShard) waitMoveLocked() {
	for es.moveDone != nil {
		done := es.moveDone
		es.shardMu.Unlock()
		<-done
		es.shardMu.Lock()
	}
}

// moveClosedShard runs the move of the closed shard es; the caller holds
// its move gate.
func (ts *Store) moveClosedShard(ctx context.Context, es *EventShard, vol, dstRoot string) error {
	if _, err := os.Stat(dstRoot); err != nil {
		return fmt.Errorf("graph: MoveShard: volume directory %s: %w", dstRoot, err)
	}
	src := ts.shardDir(es.path)
	dst := filepath.Join(dstRoot, es.path)
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return fmt.Errorf("graph: MoveShard: %w", err)
	}
	if err := syncDir(dstRoot); err != nil {
		return err
	}
	// A copy already there is not the shard's (the catalog names another
	// volume): one left by a move that crashed before its catalog save.
	if err := os.RemoveAll(dst); err != nil {
		return fmt.Errorf("graph: MoveShard: remove stale copy: %w", err)
	}
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("graph: MoveShard: %w", err)
	}
	tmp := dst + ".moving-" + hex.EncodeToString(nonce)
	if err := ts.copyShardDir(ctx, src, tmp); err != nil {
		return errors.Join(err, os.RemoveAll(tmp))
	}
	if err := ts.moveFaultAt("copied"); err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		return errors.Join(fmt.Errorf("graph: MoveShard: %w", err), os.RemoveAll(tmp))
	}
	if err := syncDir(filepath.Dir(dst)); err != nil {
		return errors.Join(err, os.RemoveAll(dst))
	}
	if err := ts.moveFaultAt("renamed"); err != nil {
		return err
	}
	snapshot := ts.catalog.snapshotShards()
	if !ts.catalog.UpdateShardVolume(es.name, vol) {
		return errors.Join(fmt.Errorf("graph: MoveShard: shard %q left the catalog during the move", es.name), os.RemoveAll(dst))
	}
	if err := ts.catalog.Save(); err != nil {
		ts.catalog.restoreShards(snapshot)
		return errors.Join(fmt.Errorf("graph: MoveShard: save catalog: %w", err), os.RemoveAll(dst))
	}
	if err := ts.moveFaultAt("saved"); err != nil {
		return err
	}
	// Committed: the source is now an orphan; one left here goes at the next open.
	if err := os.RemoveAll(src); err != nil {
		slog.Warn("tiered: moved shard's old copy not removed", "shard", es.name, "dir", src, "err", err)
	} else if err := syncDir(filepath.Dir(src)); err != nil {
		slog.Warn("tiered: moved shard's old directory removal not synced", "shard", es.name, "err", err)
	}
	return ts.moveFaultAt("removed")
}

// moveCancelled is ctx's error, or ErrStoreClosed once Close has begun.
func (ts *Store) moveCancelled(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ts.closing.Load() {
		return ErrStoreClosed
	}
	return nil
}

func (ts *Store) moveFaultAt(step string) error {
	if ts.moveFault != nil {
		return ts.moveFault(step)
	}
	return nil
}

// copyShardDir copies src's tree to dst (which must not exist), byte for
// byte, and fsyncs every file and directory it makes.
func (ts *Store) copyShardDir(ctx context.Context, src, dst string) error {
	var dirs []string
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		switch {
		case d.IsDir():
			if err := os.Mkdir(target, 0o750); err != nil {
				return err
			}
			dirs = append(dirs, target)
			return nil
		case d.Type().IsRegular():
			return ts.copyShardFile(ctx, p, target)
		}
		return fmt.Errorf("%s is not a regular file or directory", p)
	})
	if err != nil {
		return fmt.Errorf("graph: MoveShard: copy: %w", err)
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := syncDir(dirs[i]); err != nil {
			return err
		}
	}
	return nil
}

func (ts *Store) copyShardFile(ctx context.Context, src, dst string) (err error) {
	in, err := os.Open(src) // #nosec G304 -- a shard file under a configured volume
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, info.Mode().Perm()) // #nosec G304 -- the move's own copy
	if err != nil {
		return err
	}
	defer func() {
		if cerr := out.Close(); err == nil {
			err = cerr
		}
	}()
	buf := make([]byte, moveChunk)
	for {
		if err := ts.moveCancelled(ctx); err != nil {
			return err
		}
		n, rerr := io.ReadFull(in, buf)
		if n > 0 {
			if allZero(buf[:n]) {
				_, err = out.Seek(int64(n), io.SeekCurrent)
			} else {
				_, err = out.Write(buf[:n])
			}
			if err != nil {
				return err
			}
		}
		if errors.Is(rerr, io.EOF) || errors.Is(rerr, io.ErrUnexpectedEOF) {
			break
		}
		if rerr != nil {
			return rerr
		}
	}
	if err := out.Truncate(info.Size()); err != nil { // a trailing hole
		return err
	}
	return out.Sync()
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

func syncDir(dir string) error {
	d, err := os.Open(dir) // #nosec G304 -- a directory under a configured volume
	if err != nil {
		return fmt.Errorf("graph: sync dir %s: %w", dir, err)
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("graph: sync dir %s: %w", dir, err)
	}
	return nil
}

// removeMoveOrphans removes, at open, what a crashed move left: copies being
// made (*.moving-*) and a copy of an event shard on a volume its catalog
// entry does not name, once the named copy is there. It never fails the
// store: what it cannot remove it logs.
func (ts *Store) removeMoveOrphans() {
	for _, v := range []string{"", VolumeWarm, VolumeCold} {
		root, ok := ts.volumeDir(v)
		if !ok {
			continue
		}
		dir := filepath.Join(root, "events")
		ents, err := os.ReadDir(dir)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				slog.Warn("tiered: move orphans not checked", "dir", dir, "err", err)
			}
			continue
		}
		for _, e := range ents {
			name := e.Name()
			if !strings.Contains(name, ".moving-") {
				entry, ok := ts.catalog.GetShard(name)
				if !ok || entry.Kind != ShardEvent || entry.Volume == v || entry.Path != filepath.Join("events", name) {
					continue
				}
				named, _ := ts.volumeDir(entry.Volume)
				if _, err := os.Stat(filepath.Join(named, entry.Path)); err != nil {
					slog.Warn("tiered: shard copy kept: the copy its catalog entry names is not there", "shard", name, "kept", filepath.Join(dir, name), "err", err)
					continue
				}
			}
			if err := os.RemoveAll(filepath.Join(dir, name)); err != nil {
				slog.Warn("tiered: move orphan not removed", "dir", filepath.Join(dir, name), "err", err)
			}
		}
	}
}
