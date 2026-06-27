package io

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/vmihailenco/msgpack/v5"
)

// Delta export / merge surface.
//
// ExportSince(w, cursor) writes a DELTA stream — only the mutations committed to
// the graph's change-log AFTER `cursor` — framed exactly like Export (a delta
// header, the token registry, then change records). ImportMerge(r, opts) replays
// such a stream onto a base graph, reproducing the source's rows verbatim. The
// pair turns a "tenant changed → re-export the whole graph" backup into a
// "tenant changed → ship only what changed since the parent" delta backup.
//
// Cursor basis. A delta is keyed to the change-log LSN, NOT to wall-clock /
// transaction time. The LSN is monotonic, store-owned, and durable, and the
// underlying change records are the primitive mutations the replica-apply path
// already replays correctly (label-token changes, cascade deletes, history
// versions, idempotency). A valid-time or transaction-time DIFF would silently
// drop backdated/backfilled writes from a backup (a fact recorded now with an
// old valid-from is "unchanged" by a valid-time diff) — so the delta is taken
// from the ordered op-log, the one source that records every committed mutation.
//
// Precondition. ExportSince requires the graph to have a change-log
// (graph.Config.ChangeLog) on a backend that exposes a change-feed (memory or
// badger; the tiered backend declines). Without one, ExportSince fails closed
// and the caller falls back to a full Export — exactly the degradation a backup
// pipeline already handles for an unchanged tenant.

var (
	// ErrCursorUnknown is returned by ExportSince when the supplied cursor
	// cannot be serviced: it is from a DIFFERENT graph lineage (its Epoch does
	// not match this graph's), its LSN is ahead of the graph's current position,
	// or the change-log records it needs have been discarded (compaction/GC).
	// The caller MUST fall back to a full Export. No graph-entity bytes are
	// written to the writer before this error is returned.
	ErrCursorUnknown = errors.New("graph: export cursor is from another graph, ahead of the log, or predates retained history")

	// ErrDeltaBaseMismatch is returned by ImportMerge when the delta is being
	// applied onto the wrong base: MergeOptions.ExpectBase is set and does not
	// equal the delta header's From cursor, or MergeOptions.Strict saw an
	// update/tombstone for an entity absent from the base.
	ErrDeltaBaseMismatch = errors.New("graph: delta does not match the base it is applied to")
)

// Cursor is an opaque, comparable marker of a point in a graph's change-log
// timeline. Obtain one from Watermark() (the live graph's current point) or read
// it back from a delta stream's header (HeaderOf). A delta exported with
// ExportSince(w, C) contains exactly the changes with LSN in (C.LSN, To.LSN],
// where To is the cursor stamped in the delta header.
//
// Cursor is value-comparable and round-trips losslessly through msgpack and JSON
// so it can be embedded in a delta header and stored in a backup manifest.
type Cursor struct {
	// LSN is the change-log sequence number this cursor marks. A delta since C
	// covers records with LSN > C.LSN. Zero on the zero cursor.
	LSN uint64 `msgpack:"lsn" json:"lsn"`

	// Epoch identifies the graph lineage the cursor came from. It MUST match
	// between an ExportSince base and the graph the delta is produced against; a
	// mismatch means the cursor belongs to a different graph (e.g. a
	// restored-from-scratch tenant) and the delta would be meaningless
	// (ErrCursorUnknown). Derived from a durable per-graph identifier minted on
	// first use, so a cursor cannot be silently reused across an unrelated graph.
	Epoch uint64 `msgpack:"epoch" json:"epoch"`
}

// IsZero reports whether c is the zero cursor. ExportSince with the zero cursor
// emits every change the log retains (the base of a delta chain).
func (c Cursor) IsZero() bool { return c.LSN == 0 && c.Epoch == 0 }

// MergeOptions configures ImportMerge. The zero value is the lenient,
// idempotent-merge default suitable for restore (base + ordered deltas).
type MergeOptions struct {
	// StagingDir is the directory for the per-import temp file (empty = platform
	// default temp dir). Mirrors ImportOptions.StagingDir.
	StagingDir string

	// MaxStagedBytes caps the staging file size (zero = unlimited; negative is
	// invalid). Mirrors ImportOptions.MaxStagedBytes.
	MaxStagedBytes int64

	// ExpectBase, when non-zero, asserts the delta was exported with
	// `since == ExpectBase`. ImportMerge checks it against the delta header's
	// From cursor and returns ErrDeltaBaseMismatch on disagreement — proving a
	// restore chain applies delta K+1 only after delta K (and the base).
	ExpectBase Cursor

	// Strict turns "an update or tombstone for an entity not present in the
	// base" from a tolerated no-op into ErrDeltaBaseMismatch. Off by default so
	// an idempotent re-apply (or a delta that overlaps an already-applied one)
	// does not fail.
	Strict bool
}

// DeltaHeader is the decoded leading record of an export or delta stream.
// HeaderOf reads ONLY this record (it does not consume entity/change records),
// so a caller can route a stream (full vs delta) and validate the base cursor
// before committing to a merge.
type DeltaHeader struct {
	// Version is the export-stream format version.
	Version uint8
	// IsDelta is false for an Export stream, true for an ExportSince stream.
	IsDelta bool
	// From is the ExportSince `since` cursor (zero for a full Export).
	From Cursor
	// To is the point the stream is consistent at — the `since` for the NEXT
	// delta in the chain. For a full Export, To.LSN is the snapshot's change-log
	// LSN (To.Epoch is not carried in a full-export header; use Watermark()).
	To Cursor
	// ExportedAt is the Unix-millisecond wall-clock time the stream was written
	// (informational; not part of any content digest).
	ExportedAt int64
	// NodeCount / RelCount are the whole-graph current-entity totals for a full
	// Export. They are NOT populated for a delta stream (a delta names only
	// changed entities); both read 0 when IsDelta is true.
	NodeCount int64
	RelCount  int64
}

// headerWire mirrors the internal/core exportHeader msgpack layout. It is
// duplicated here (rather than imported) because pkg/graph/internal/core imports
// this package; the field tags MUST stay in lock-step with core.exportHeader.
// TestHeaderWireMatchesCore (internal/core) decodes a core-produced header
// through this struct to pin the agreement.
type headerWire struct {
	Version     uint8  `msgpack:"v"`
	ExportedAt  int64  `msgpack:"at"`
	NodeCount   int64  `msgpack:"nc"`
	RelCount    int64  `msgpack:"rc"`
	SnapshotLSN uint64 `msgpack:"lsn,omitempty"`
	IsDelta     bool   `msgpack:"d,omitempty"`
	FromLSN     uint64 `msgpack:"fl,omitempty"`
	FromEpoch   uint64 `msgpack:"fe,omitempty"`
	ToLSN       uint64 `msgpack:"tl,omitempty"`
	ToEpoch     uint64 `msgpack:"te,omitempty"`
	Epoch       uint64 `msgpack:"ep,omitempty"`
}

// exportTagHeader and the framing constants mirror internal/core/export.go. The
// leading record of every stream is a header record so a digest computed over a
// header-stripped stream stays stable across re-exports.
const (
	exportTagHeader      byte  = 0x01
	headerRecordSizeCap  int64 = 1 << 20 // a header body is tiny; cap defensively.
	exportRecordHeaderSz       = 5       // 1-byte tag + 4-byte big-endian length.
)

// HeaderOf decodes the leading header record of an export or delta stream
// WITHOUT applying it or consuming entity/change records. It returns
// ErrCorruptExport if the first record is not a header or is malformed, and
// ErrIncompatibleExport if the format version is outside the supported range.
//
// HeaderOf reads exactly one framed record from r; the caller may continue
// reading r for the remaining records (or discard it and re-open the source).
func HeaderOf(r io.Reader) (DeltaHeader, error) {
	if grapherr.IsNil(r) {
		return DeltaHeader{}, ErrNilReader
	}
	var frame [exportRecordHeaderSz]byte
	if _, err := io.ReadFull(r, frame[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return DeltaHeader{}, fmt.Errorf("%w: stream has no header record: %v", ErrCorruptExport, err)
		}
		return DeltaHeader{}, err
	}
	if frame[0] != exportTagHeader {
		return DeltaHeader{}, fmt.Errorf("%w: first record tag 0x%02x is not a header", ErrCorruptExport, frame[0])
	}
	length := binary.BigEndian.Uint32(frame[1:exportRecordHeaderSz])
	if int64(length) > headerRecordSizeCap {
		return DeltaHeader{}, fmt.Errorf("%w: header record too large (%d bytes)", ErrCorruptExport, length)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return DeltaHeader{}, fmt.Errorf("%w: truncated header body: %v", ErrCorruptExport, err)
	}
	// The header is a flat struct with no interface-typed or deeply-nestable
	// field, so a plain decode cannot hit the msgpack panic class (lesson 47).
	var hw headerWire
	if err := msgpack.Unmarshal(body, &hw); err != nil {
		return DeltaHeader{}, fmt.Errorf("%w: malformed header: %v", ErrCorruptExport, err)
	}
	if hw.Version < exportFormatVersionMin || hw.Version > exportFormatVersionMax {
		return DeltaHeader{}, fmt.Errorf("%w: got %d, want %d..%d", ErrIncompatibleExport, hw.Version, exportFormatVersionMin, exportFormatVersionMax)
	}
	dh := DeltaHeader{
		Version:    hw.Version,
		IsDelta:    hw.IsDelta,
		ExportedAt: hw.ExportedAt,
		NodeCount:  hw.NodeCount,
		RelCount:   hw.RelCount,
	}
	if hw.IsDelta {
		dh.From = Cursor{LSN: hw.FromLSN, Epoch: hw.FromEpoch}
		dh.To = Cursor{LSN: hw.ToLSN, Epoch: hw.ToEpoch}
		// A delta names only changed entities; whole-graph counts are not
		// populated. Report 0 rather than leaking the header's reused fields.
		dh.NodeCount = 0
		dh.RelCount = 0
	} else {
		// A full export's To is the complete base cursor for the first delta in a
		// chain: SnapshotLSN + the lineage epoch (both stamped when the source has
		// an active change-log; zero otherwise).
		dh.To = Cursor{LSN: hw.SnapshotLSN, Epoch: hw.Epoch}
	}
	return dh, nil
}

// exportFormatVersionMin / Max bound the versions HeaderOf accepts. They mirror
// internal/core/export.go: v1 (pre-SnapshotLSN), v2 (full export with
// SnapshotLSN), v3 (delta). Kept here so HeaderOf can route a stream without
// importing internal/core.
const (
	exportFormatVersionMin byte = 1
	exportFormatVersionMax byte = 3
)
