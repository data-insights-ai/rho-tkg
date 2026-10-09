package graphstore

import (
	"cmp"
	"errors"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// ErrPatchConflict rejects stale/incorrect before images or a replacement state
// inconsistent with supplied changes. It is not a durable transaction decision.
var ErrPatchConflict = errors.New("graphstore: component patch conflict")

// PageLimits bounds physical pages, replay, one stage sequence and reader-owned
// continuations independently. Byte ledgers are conservative representation/work
// bounds, not Go heap/RSS limits. Catalog/application limits apply additionally.
type PageLimits struct {
	MaxChildren, MaxLevels, MaxCells, MaxCheckpointBytes     int
	MaxTailRecords, MaxTailBytes, MaxTailAtoms               int
	MaxPatches, MaxWorkRecords, MaxWorkBytes, MaxChangeBytes int
	MaxCursors, MaxCursorBytes                               int
}

// DefaultPageLimits returns provisional measured-later page/replay settings.
func DefaultPageLimits() PageLimits {
	return PageLimits{64, 8, 64, 256 << 10, 32, 64 << 10, 128, 128, 512, 4 << 20, 1 << 20, 32, 1 << 20}
}

// Validate checks finite page/work/cursor bounds.
func (l PageLimits) Validate() error { _, err := l.resolve(); return err }
func (l PageLimits) resolve() (PageLimits, error) {
	d := DefaultPageLimits()
	l.MaxChildren = cmp.Or(l.MaxChildren, d.MaxChildren)
	l.MaxLevels = cmp.Or(l.MaxLevels, d.MaxLevels)
	l.MaxCells = cmp.Or(l.MaxCells, d.MaxCells)
	l.MaxCheckpointBytes = cmp.Or(l.MaxCheckpointBytes, d.MaxCheckpointBytes)
	l.MaxTailRecords = cmp.Or(l.MaxTailRecords, d.MaxTailRecords)
	l.MaxTailBytes = cmp.Or(l.MaxTailBytes, d.MaxTailBytes)
	l.MaxTailAtoms = cmp.Or(l.MaxTailAtoms, d.MaxTailAtoms)
	l.MaxPatches = cmp.Or(l.MaxPatches, d.MaxPatches)
	l.MaxWorkRecords = cmp.Or(l.MaxWorkRecords, d.MaxWorkRecords)
	l.MaxWorkBytes = cmp.Or(l.MaxWorkBytes, d.MaxWorkBytes)
	l.MaxChangeBytes = cmp.Or(l.MaxChangeBytes, d.MaxChangeBytes)
	l.MaxCursors = cmp.Or(l.MaxCursors, d.MaxCursors)
	l.MaxCursorBytes = cmp.Or(l.MaxCursorBytes, d.MaxCursorBytes)
	for _, n := range []int{l.MaxChildren, l.MaxLevels, l.MaxCells, l.MaxCheckpointBytes, l.MaxTailRecords, l.MaxTailBytes, l.MaxTailAtoms, l.MaxPatches, l.MaxWorkRecords, l.MaxWorkBytes, l.MaxChangeBytes, l.MaxCursors, l.MaxCursorBytes} {
		if n < 1 || n > 64<<20 {
			return PageLimits{}, ErrInvalid
		}
	}
	if l.MaxChildren < 3 || l.MaxChildren > 64 || l.MaxLevels > 8 || l.MaxCells > 4096 || l.MaxCheckpointBytes > 1<<20 || l.MaxTailRecords > 32 || l.MaxTailBytes > 1<<20 || l.MaxTailAtoms > 4096 || l.MaxPatches > 4096 || l.MaxWorkRecords > 65536 || l.MaxWorkBytes < l.MaxCheckpointBytes+128 || l.MaxCursors > 4096 || l.MaxCursorBytes < 256 {
		return PageLimits{}, ErrInvalid
	}
	return l, nil
}
func (l PageLimits) stateLimits(c *Catalog) state.Limits {
	return state.Limits{Temporal: c.limits.Temporal, MaxPieces: max(4096, l.MaxCells+2*l.MaxTailAtoms), MaxMetadataBytes: 1 << 20, MaxChangePieces: max(4096, l.MaxTailAtoms)}
}
func (l PageLimits) codecLimits(c *Catalog) state.CodecLimits {
	return state.CodecLimits{State: l.stateLimits(c), MaxEncodedBytes: 1 << 20}
}

// PageWork reports actual bounded direct-lookups and decoded/replayed metadata.
// Records includes catalog and private staging-overlay point lookups. Bytes
// also charges repeated decoded materialization; it is not physical disk I/O.
type PageWork struct{ Records, Bytes, DirectoryPages, CheckpointPages, PatchPages, DecodedCells int }

// ComponentChangeGroup preserves one original sequential patch's complete CDC.
// Overlapping groups stay separate and ordered; no flattening is performed.
type ComponentChangeGroup struct {
	Key     graphstate.ComponentKey
	Owned   temporal.Scope
	Changes []state.Change
}

// StagedComponents owns a private root and complete ordered changes. Only a
// later materializer/application installation publishes them atomically.
type StagedComponents struct {
	Root   Root
	Groups []ComponentChangeGroup
	Work   PageWork
}

type componentMeta struct {
	Key  graphstate.ComponentKey
	Axis temporal.Axis
	Root uint64
}
type childPage struct {
	ID    uint64
	Owned temporal.Scope
}
type directoryPage struct {
	ID                                       uint64
	Key                                      graphstate.ComponentKey
	Owned                                    temporal.Scope
	Level                                    int
	Children                                 []childPage
	Base, Head                               uint64
	TailRecords, TailBytes, TailAtoms, Cells int
}
type patchPage struct {
	ID, Previous uint64
	Key          graphstate.ComponentKey
	Owned        temporal.Scope
	Changes      []state.Change
}
type continuation struct {
	query     [32]byte
	remaining temporal.Scope
	bytes     int
}

// PageReader borrows Catalog/view and owns only bounded active cursors. It
// resolves stable page handles at that immutable MVCC view, not by future-key
// scans. Close serializes with reads and never closes Catalog or its view.
type PageReader struct {
	mu          sync.Mutex
	c           *Catalog
	complete    *fullIndexDescriptor
	limits      PageLimits
	id          graphstate.ViewID
	cursors     map[graphstate.Cursor]continuation
	index       uint64
	cursorBytes int
	closed      bool
	last        PageWork
}
