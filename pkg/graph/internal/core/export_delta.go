package core

import (
	"fmt"
	"io"
	"time"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// Delta export (ExportSince) — see pkg/graph/io/delta.go for the public contract.
//
// A delta is the slice of the change-log in (since.LSN, to], framed into the
// export stream as: a delta header, the full current token registry (so the
// change records' label/rel-type tokens resolve on a merge target with no live
// primary), then one exportTagChange record per committed mutation. The records
// ARE the change-log records verbatim; ImportMerge replays them through the same
// foreign-ID apply doors the replica-apply path uses, so the merged rows are
// byte-exact reproductions of the source.

// changeRecordWire is the body of an exportTagChange record: a change-log record
// (LSN + tag + the tag-specific msgpack body) carried verbatim into the export
// stream. The body is opaque here — ImportMerge dispatches on Tag through the
// same storeutil decoders the replica-apply path uses, all of which fail closed
// via SafeUnmarshal.
type changeRecordWire struct {
	LSN     uint64 `msgpack:"l"`
	Tag     uint8  `msgpack:"t"`
	Payload []byte `msgpack:"p"`
}

// changeLogActive reports whether the backend is actually recording mutations to
// its change-log. memory and badger expose the feed methods unconditionally
// (returning empty when disabled), so a nil-check on c.changeFeed is not enough —
// a delta from a disabled log would silently record nothing. A feed backend that
// does not report status is assumed active (it opted into exposing the feed).
func (c *Core) changeLogActive() bool {
	if c.changeFeed == nil {
		return false
	}
	if s, ok := c.store.(storepkg.ChangeLogStatusCapability); ok {
		return s.ChangeLogEnabled()
	}
	return true
}

// Watermark returns a Cursor for the graph's current change-log point. See
// io.API.Watermark.
func (o *IOOps) Watermark() (tkgio.Cursor, error) {
	c := o.c
	if err := c.checkOpen(); err != nil {
		return tkgio.Cursor{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return tkgio.Cursor{}, ErrGraphClosed
	}
	if !c.changeLogActive() {
		return tkgio.Cursor{}, fmt.Errorf("graph: watermark: %w (no change-log)", storepkg.ErrCapabilityNotSupported)
	}
	lsn, err := c.changeFeed.LastCommittedLSN()
	if err != nil {
		return tkgio.Cursor{}, fmt.Errorf("graph: watermark: %w", err)
	}
	epoch, err := c.graphEpochLocked()
	if err != nil {
		return tkgio.Cursor{}, fmt.Errorf("graph: watermark: %w", err)
	}
	return tkgio.Cursor{LSN: lsn, Epoch: epoch}, nil
}

// ExportSince writes the delta stream for changes after `since`. See
// io.API.ExportSince.
func (o *IOOps) ExportSince(w io.Writer, since tkgio.Cursor) error {
	c := o.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if isNilInterfaceValue(w) {
		return ErrNilWriter
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	if !c.changeLogActive() {
		return fmt.Errorf("graph: export since: %w (no change-log)", storepkg.ErrCapabilityNotSupported)
	}
	epoch, err := c.graphEpochLocked()
	if err != nil {
		return fmt.Errorf("graph: export since: %w", err)
	}
	// Lineage guard: a cursor from a different graph (Epoch mismatch) cannot be
	// serviced. The zero cursor (whole-log delta, base of a chain) is exempt.
	if !since.IsZero() && since.Epoch != epoch {
		return fmt.Errorf("graph: export since: %w (cursor epoch %d != graph epoch %d)", ErrCursorUnknown, since.Epoch, epoch)
	}
	// Drain the backend's async write buffer so every committed mutation is
	// durable BEFORE we read the change feed: ChangeFeed / LastCommittedLSN
	// surface only durably-committed records, so an unflushed buffer (badger)
	// would otherwise yield a delta missing the most recent mutations. No-op on
	// the memory backend (no buffer).
	if err := c.flushStoreLocked(); err != nil {
		return fmt.Errorf("graph: export since: flush: %w", err)
	}
	// Capture the upper bound under the held write lock, so the change set is
	// computed against one consistent point and no commit can interleave.
	to, err := c.changeFeed.LastCommittedLSN()
	if err != nil {
		return fmt.Errorf("graph: export since: %w", err)
	}
	if since.LSN > to {
		return fmt.Errorf("graph: export since: %w (cursor LSN %d ahead of current %d)", ErrCursorUnknown, since.LSN, to)
	}
	return c.exportSinceLocked(w, since, to, epoch)
}

// exportSinceLocked writes the delta stream. Caller holds c.mu.Lock.
func (c *Core) exportSinceLocked(w io.Writer, since tkgio.Cursor, to, epoch uint64) error {
	hdr := exportHeader{
		Version:    exportFormatVersionDelta,
		ExportedAt: time.Now().UnixMilli(),
		IsDelta:    true,
		FromLSN:    since.LSN,
		FromEpoch:  since.Epoch,
		ToLSN:      to,
		ToEpoch:    epoch,
	}
	if err := marshalAndWrite(w, exportTagHeader, &hdr); err != nil {
		return fmt.Errorf("export since: header: %w", err)
	}

	// The full current registry — a SUPERSET-safe set the merge append-extends
	// onto the base, so every change record's label/rel-type token resolves
	// locally without a live primary. Property keys are untokenized in the
	// records and need no registry.
	reg := tiered.RegistryFileData{
		Labels:   c.labels.ExportNames(),
		RelTypes: c.relTypes.ExportNames(),
	}
	if err := marshalAndWrite(w, exportTagRegistry, &reg); err != nil {
		return fmt.Errorf("export since: registry: %w", err)
	}

	var emitErr error
	if err := c.changeFeed.ForEachChange(since.LSN, func(rec storepkg.ChangeRecord) bool {
		if rec.LSN > to {
			return false // beyond the captured snapshot point
		}
		switch rec.Tag {
		case storepkg.ChangeClear:
			// A full-graph clear cannot be expressed as an additive delta merge.
			// Fail closed so the caller falls back to a full Export.
			emitErr = fmt.Errorf("export since: %w (change-log Clear at LSN %d cannot be delta-merged)", ErrCursorUnknown, rec.LSN)
			return false
		case storepkg.ChangeMeta:
			return true // not entity state; nothing to reproduce on the target
		}
		crw := changeRecordWire{LSN: rec.LSN, Tag: uint8(rec.Tag), Payload: rec.Payload}
		if werr := marshalAndWrite(w, exportTagChange, &crw); werr != nil {
			emitErr = fmt.Errorf("export since: write change LSN %d: %w", rec.LSN, werr)
			return false
		}
		return true
	}); err != nil {
		return fmt.Errorf("export since: change feed: %w", err)
	}
	return emitErr
}
