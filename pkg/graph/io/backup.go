package io

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// One-call backup/restore ergonomics over Export / ExportSince / ImportMerge.
// No new wire format: BackupTo writes exactly what Export writes, and
// BackupDeltaTo writes exactly what ExportSince writes, into deterministically
// named files so a directory of backups can be discovered and chained back
// together (see (graph package) RestoreInto) without an external manifest.
//
// Naming is derived from the STREAM'S OWN header cursor, never wall time: two
// backups taken at the same change-log point always produce the same
// filename, and a directory listing sorts in chain order because the LSN is
// zero-padded to the full width of a uint64 (20 digits).

// ErrBackupExists is returned by BackupTo / BackupDeltaTo when the
// deterministic target filename already exists in dir. Backup filenames are
// content-addressed by change-log cursor, so an existing file means either an
// identical backup was already taken (a harmless re-run) or the caller is
// about to silently overwrite a diverged file — either way this library never
// picks for the caller. Retention/pruning/re-running-after-inspection is a
// caller decision.
var ErrBackupExists = errors.New("graph: backup file already exists")

const backupFileExt = ".tkg"

// exportTagChangeWire mirrors internal/core.exportTagChange (0x07) — the tag
// byte for a verbatim change-log record inside a delta stream. Duplicated
// here (like headerWire above) because pkg/graph/internal/core imports this
// package; TestHeaderWireMatchesCore-style parity is unnecessary since this
// value is never decoded, only compared — a wire format bump to the tag space
// would need to update both anyway.
const exportTagChangeWire byte = 0x07

// backupFullName is the deterministic filename for a full backup whose
// export-header cursor has the given LSN.
func backupFullName(lsn uint64) string {
	return fmt.Sprintf("backup-%020d-full%s", lsn, backupFileExt)
}

// backupDeltaName is the deterministic filename for a delta backup spanning
// the change-log range (since, to].
func backupDeltaName(since, to uint64) string {
	return fmt.Sprintf("backup-%020d-to-%020d-delta%s", since, to, backupFileExt)
}

// BackupTo writes a full export of the graph to a deterministically named
// file in dir: backup-<LSN>-full.tkg, where LSN (zero-padded to 20 digits) is
// the change-log point the export's own header reports itself consistent at
// (DeltaHeader.To — the same value HeaderOf would decode back out of the
// file). The file is fsync'd before BackupTo returns, and the containing
// directory is fsync'd after the file is published under its final name —
// so both the backup's data and its directory-entry visibility survive a
// crash immediately after a successful return.
//
// On a backend with no active change-log, the export still succeeds and the
// returned Cursor is the zero Cursor (there is no change-log point to name it
// by) — every such backup in the same dir therefore shares the name
// backup-00000000000000000000-full.tkg, so only the first call to a given dir
// succeeds; a second call refuses exactly like a byte-identical backup would
// on a change-log-enabled graph (see below).
//
// Refuses to silently overwrite: if the target filename already exists in
// dir, BackupTo returns ErrBackupExists and dir is left untouched (the
// in-progress export is staged to a temp file first and only published to
// the final name — atomically, via renameNoClobber's os.Link — once no
// collision is found). Concurrent callers targeting the same dir race
// safely: at most one ever succeeds, every other one observes
// ErrBackupExists (see renameNoClobber).
func (a *API) BackupTo(dir string) (Cursor, error) {
	ops, err := a.ready()
	if err != nil {
		return Cursor{}, err
	}
	if dir == "" {
		return Cursor{}, fmt.Errorf("graph: backup to: dir must not be empty")
	}

	tmp, err := os.CreateTemp(dir, ".backup-full-*.tmp")
	if err != nil {
		return Cursor{}, fmt.Errorf("graph: backup to: stage file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }() // no-op once renameNoClobber links+removes it; best-effort cleanup on any earlier failure

	if err := ops.Export(tmp); err != nil {
		_ = tmp.Close()
		return Cursor{}, fmt.Errorf("graph: backup to: export: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return Cursor{}, fmt.Errorf("graph: backup to: sync: %w", err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		_ = tmp.Close()
		return Cursor{}, fmt.Errorf("graph: backup to: seek: %w", err)
	}
	hdr, err := HeaderOf(tmp)
	if err != nil {
		_ = tmp.Close()
		return Cursor{}, fmt.Errorf("graph: backup to: read header: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return Cursor{}, fmt.Errorf("graph: backup to: close: %w", err)
	}

	finalPath := filepath.Join(dir, backupFullName(hdr.To.LSN))
	if err := renameNoClobber(tmpPath, finalPath); err != nil {
		return Cursor{}, err
	}
	return hdr.To, nil
}

// BackupDeltaTo writes a delta export (see ExportSince) covering every change
// committed after `since` to a deterministically named file in dir:
// backup-<sinceLSN>-to-<toLSN>-delta.tkg (both zero-padded to 20 digits).
// Like BackupTo, the name is derived from the stream's own header cursors,
// the file is fsync'd before returning, and the containing directory is
// fsync'd after the file is published under its final name.
//
// Empty delta: if there is nothing to reproduce after `since` (zero change
// records beyond the header/registry — e.g. called again right after a
// successful backup, before any further mutation), NO file is written and
// BackupDeltaTo returns `since` UNCHANGED. A caller polling "is there
// anything new since my last backup" gets a side-effect-free no-op instead of
// an empty file to track and prune.
//
// Declines with the same error ExportSince returns when the backend has no
// active change-log — a wrapped store.ErrCapabilityNotSupported — so the
// caller's fallback is the same: call BackupTo for a full backup instead.
//
// Refuses to silently overwrite an existing target filename (ErrBackupExists),
// exactly like BackupTo.
func (a *API) BackupDeltaTo(dir string, since Cursor) (Cursor, error) {
	ops, err := a.ready()
	if err != nil {
		return Cursor{}, err
	}
	if dir == "" {
		return Cursor{}, fmt.Errorf("graph: backup delta to: dir must not be empty")
	}

	tmp, err := os.CreateTemp(dir, ".backup-delta-*.tmp")
	if err != nil {
		return Cursor{}, fmt.Errorf("graph: backup delta to: stage file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if err := ops.ExportSince(tmp, since); err != nil {
		_ = tmp.Close()
		return Cursor{}, fmt.Errorf("graph: backup delta to: export since: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return Cursor{}, fmt.Errorf("graph: backup delta to: sync: %w", err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		_ = tmp.Close()
		return Cursor{}, fmt.Errorf("graph: backup delta to: seek: %w", err)
	}
	hdr, err := HeaderOf(tmp)
	if err != nil {
		_ = tmp.Close()
		return Cursor{}, fmt.Errorf("graph: backup delta to: read header: %w", err)
	}
	changeCount, err := countStreamChangeRecords(tmp)
	if err != nil {
		_ = tmp.Close()
		return Cursor{}, fmt.Errorf("graph: backup delta to: scan records: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return Cursor{}, fmt.Errorf("graph: backup delta to: close: %w", err)
	}

	if changeCount == 0 {
		return since, nil
	}

	finalPath := filepath.Join(dir, backupDeltaName(hdr.From.LSN, hdr.To.LSN))
	if err := renameNoClobber(tmpPath, finalPath); err != nil {
		return Cursor{}, err
	}
	return hdr.To, nil
}

// countStreamChangeRecords scans every framed record from r's CURRENT
// position onward and counts how many carry the delta change-record tag,
// without decoding any record body. It has no precondition on where r is
// positioned — it simply consumes frames until EOF — so a caller may start
// it anywhere in a well-formed frame sequence. BACKLOG 8f: BackupDeltaTo's
// call site passes r already past the header record (consumed by the
// preceding HeaderOf call), NOT at the stream's start, which is exactly
// what's wanted here: counting the header would corrupt the "empty delta"
// heuristic below. BackupDeltaTo uses the result to detect an "empty delta"
// — a stream holding only a header and registry record, no actual mutations
// — so it can skip writing a file entirely.
func countStreamChangeRecords(r io.Reader) (int, error) {
	var frame [exportRecordHeaderSz]byte
	count := 0
	for {
		if _, err := io.ReadFull(r, frame[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return count, nil
			}
			return count, fmt.Errorf("truncated record frame: %w", err)
		}
		length := binary.BigEndian.Uint32(frame[1:exportRecordHeaderSz])
		if frame[0] == exportTagChangeWire {
			count++
		}
		if _, err := io.CopyN(io.Discard, r, int64(length)); err != nil {
			return count, fmt.Errorf("truncated record body: %w", err)
		}
	}
}

// renameNoClobber refuses to replace an existing file at finalPath: it
// returns ErrBackupExists rather than overwriting, and otherwise publishes
// tmpPath's content at finalPath.
//
// A stat-then-rename implementation has a TOCTOU window: os.Rename on POSIX
// unconditionally replaces an existing destination, so two callers that both
// pass the "does finalPath exist" stat check (e.g. two BackupTo calls racing
// on the same dir when nothing mutated the graph between them, so both
// resolve the identical deterministic filename) both proceed to rename —
// each succeeds, and the second silently clobbers the first with no error
// from either caller. os.Link closes that window: it asks the filesystem to
// atomically create the finalPath directory entry pointing at tmpPath's
// inode, and the kernel itself serializes concurrent creations of the same
// path — at most one Link call can ever succeed, every other caller
// observes syscall.EEXIST (wrapped as fs.ErrExist), with no state where a
// partial or "second" file is visible at finalPath. tmpPath and finalPath
// are always siblings under the same dir (see BackupTo / BackupDeltaTo), so
// this is always a same-filesystem link — no cross-device (EXDEV) case.
func renameNoClobber(tmpPath, finalPath string) error {
	if err := os.Link(tmpPath, finalPath); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("%w: %s", ErrBackupExists, finalPath)
		}
		return fmt.Errorf("graph: backup: link to %s: %w", finalPath, err)
	}
	// finalPath is now a second directory entry for the same inode as
	// tmpPath; tmpPath's own entry is redundant. Removing it is best-effort
	// cleanup (like the outer defer in BackupTo/BackupDeltaTo) — the backup
	// is already durably published at finalPath regardless of whether this
	// Remove succeeds.
	_ = os.Remove(tmpPath)
	// fsync the CONTAINING DIRECTORY, not just the file: tmp.Sync() (already
	// called by both BackupTo/BackupDeltaTo before this rename) only makes
	// the file's DATA durable. The directory ENTRY that publishes it under
	// finalPath (this Link, plus the Remove of tmpPath's entry — both
	// metadata changes in the SAME directory) is a separate durability
	// domain on POSIX filesystems that don't implicitly journal directory
	// metadata alongside file data: a crash right after a successful
	// BackupTo/BackupDeltaTo return could otherwise lose the publish itself,
	// leaving the data on disk but the backup absent from a directory
	// listing after recovery.
	if err := fsyncDir(filepath.Dir(finalPath)); err != nil {
		return fmt.Errorf("graph: backup: sync dir for %s: %w", finalPath, err)
	}
	return nil
}

// fsyncDir opens dir and fsyncs it, flushing pending directory-entry
// metadata changes (create/link/remove) to durable storage.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
