package graphstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// RangeMoveBatchMaxPages bounds one owned graph envelope.
const RangeMoveBatchMaxPages = 32

// RangeMoveBatchMaxBytes includes all cursor/framing/seal bytes.
const RangeMoveBatchMaxBytes = 64 << 10
const rangeMoveBatchFraming = 4 + 4 + 32

// RangeMoveBatch owns at most32 exact original RMP1 pages. Logical cursor
// sequence/digest remain distinct from the outer native import/undo ordinal.
// This inert envelope supplies no source or processing-owner authority.
type RangeMoveBatch struct{ Pages []RangeMovePage }

func rangeMoveBatchValid(b RangeMoveBatch) bool {
	if len(b.Pages) == 0 || len(b.Pages) > RangeMoveBatchMaxPages {
		return false
	}
	for i, p := range b.Pages {
		if !rangeMoveValidPage(p) || i > 0 && (!rangeMoveCursorEqual(b.Pages[i-1].Next, p.Before) || b.Pages[i-1].Complete) {
			return false
		}
	}
	return true
}

// EncodeRangeMoveBatch checks complete contiguous pages before owning bytes.
func EncodeRangeMoveBatch(b RangeMoveBatch, output *graphstate.OutputBudget, maximum int) ([]byte, error) {
	if output == nil || maximum < 8192 || maximum > RangeMoveBatchMaxBytes || !rangeMoveBatchValid(b) {
		return nil, ErrInvalid
	}
	if err := output.Reserve(32 * len(b.Pages)); err != nil {
		return nil, callerError(err)
	}
	fields := make([][]byte, 0, len(b.Pages))
	size := rangeMoveBatchFraming
	for _, p := range b.Pages {
		wire, err := EncodeRangeMovePage(p, output)
		if err != nil {
			return nil, err
		}
		if len(wire) > maximum-size-4 {
			return nil, ErrResourceLimit
		}
		size += 4 + len(wire)
		fields = append(fields, wire)
	}
	return rangeMoveEncode(output, maximum, func(w *rangeMoveWriter) {
		w.add([]byte("RMB\x01"))
		var count [4]byte
		binary.BigEndian.PutUint32(count[:], uint32(len(fields))) // #nosec G115 -- checked1..32.
		w.add(count[:])
		for _, wire := range fields {
			w.field(wire)
		}
	})
}

// DecodeRangeMoveBatch owns bounded page backing only after count/byte checks.
func DecodeRangeMoveBatch(wire []byte, output *graphstate.OutputBudget, maximum int) (RangeMoveBatch, error) {
	if output == nil || maximum < 8192 || maximum > RangeMoveBatchMaxBytes {
		return RangeMoveBatch{}, ErrInvalid
	}
	body, err := rangeMoveBody(wire, "RMB\x01", maximum)
	if err != nil {
		return RangeMoveBatch{}, err
	}
	c := cursor{src: body}
	count, err := c.take(4)
	if err != nil {
		return RangeMoveBatch{}, err
	}
	n := binary.BigEndian.Uint32(count)
	if n == 0 || n > RangeMoveBatchMaxPages || uint64(n) > uint64(len(c.src)/4) {
		return RangeMoveBatch{}, ErrCorrupt
	}
	if err := output.Reserve(4096 + 4*len(wire) + 2048*int(n)); err != nil { // #nosec G115 -- n<=32 before conversion/allocation.
		return RangeMoveBatch{}, callerError(err)
	}
	b := RangeMoveBatch{Pages: make([]RangeMovePage, 0, int(n))}
	for range n {
		field, err := c.field(maximum)
		if err != nil {
			return RangeMoveBatch{}, err
		}
		p, err := DecodeRangeMovePage(field, output)
		if err != nil {
			return RangeMoveBatch{}, err
		}
		b.Pages = append(b.Pages, p)
	}
	if c.done() != nil || !rangeMoveBatchValid(b) {
		return RangeMoveBatch{}, ErrCorrupt
	}
	return b, nil
}

// ExportRangeMoveBatch composes genuine frozen native reads under ONE shared
// output arena and cumulative source ledger. The wire grant includes every
// cursor, length and outer seal before issuing another native subcall.
// Production composition separates nonempty logical families and limits component
// envelopes to8 subpages: per-component checkpoint/directory/meta writes share
// the same bounded key-tree path. Generic Stage still enforces128 final writes;
// arbitrary/wide temporal records can explicitly refuse, never auto-grow caps.
func ExportRangeMoveBatch(ctx context.Context, source ApplicationReadSource, d RangeMoveDescriptor, before RangeMoveCursor, budget raftlog.ReadBudget, l Limits, g GraphLimits, output *graphstate.OutputBudget) (RangeMoveBatch, PageWork, error) {
	var work PageWork
	if ctx == nil || source == nil || output == nil || budget.Rows < 1 || budget.Rows > RangeMoveBatchMaxPages || budget.Bytes < 8192 || budget.Bytes > RangeMoveBatchMaxBytes {
		return RangeMoveBatch{}, work, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return RangeMoveBatch{}, work, err
	}
	g, err := g.resolve()
	if err != nil {
		return RangeMoveBatch{}, work, err
	}
	if err = output.Reserve(2048 * budget.Rows); err != nil {
		return RangeMoveBatch{}, work, callerError(err)
	}
	b := RangeMoveBatch{Pages: make([]RangeMovePage, 0, budget.Rows)}
	size, current := rangeMoveBatchFraming, before
	family := byte(0)
	for len(b.Pages) < budget.Rows {
		remaining := budget.Bytes - size - 4
		// RMP1 owns two fixed-path cursors and at most their retained key copies.
		framing := 2048 + 4*(len(current.After)+len(current.Key))
		if remaining < framing+rangeMoveFixedBytes+1 {
			break
		}
		limits := g
		limits.MaxSourceRows = min(g.MaxSourceRows, g.Pages.MaxWorkRecords) - work.Records
		limits.MaxSourceBytes = min(g.MaxSourceBytes, g.Pages.MaxWorkBytes) - work.Bytes
		limits.Pages.MaxWorkRecords = min(limits.Pages.MaxWorkRecords, limits.MaxSourceRows)
		limits.Pages.MaxWorkBytes = min(limits.Pages.MaxWorkBytes, limits.MaxSourceBytes)
		if limits.MaxSourceRows < 1 || limits.MaxSourceBytes < limits.Pages.MaxCheckpointBytes+128 {
			return RangeMoveBatch{}, work, ErrResourceLimit
		}
		p, w, err := ExportRangeMovePage(ctx, source, d, current, raftlog.ReadBudget{Rows: 1, Bytes: remaining - framing}, l, limits, output)
		work = addWork(work, w)
		if err != nil {
			return RangeMoveBatch{}, work, err
		}
		wire, err := EncodeRangeMovePage(p, output)
		if err != nil {
			return RangeMoveBatch{}, work, err
		}
		if len(wire) > remaining {
			return RangeMoveBatch{}, work, ErrResourceLimit
		}
		// A discarded boundary candidate still consumes real source/arena work;
		// the next envelope starts at its exact Before, never a skipped row.
		if family != 0 && p.Kind != 0 && p.Kind != family {
			break
		}
		if p.Kind != 0 {
			family = p.Kind
		}
		size += 4 + len(wire)
		b.Pages = append(b.Pages, p)
		current = p.Next
		if p.Complete || family == rangeMoveComponents && len(b.Pages) >= 8 {
			break
		}
	}
	if len(b.Pages) == 0 {
		return RangeMoveBatch{}, work, ErrResourceLimit
	}
	return b, work, nil
}

// StageRangeMoveBatch shares the genuine pageStage pending index/tree/fragment
// reads across every subpage and emits ONE prospective native batch. Any error
// returns no effects. The outer materializer co-commits ONE whole-batch undo;
// its historical128-write ceiling remains a separate admission bound.
func StageRangeMoveBatch(ctx context.Context, target *raftlog.ApplicationView, d RangeMoveDescriptor, prior RangeMoveImportState, b RangeMoveBatch, l Limits, g GraphLimits, budget OwnershipBudget, output *graphstate.OutputBudget) (RangeMovePageEffects, PageWork, error) {
	if !rangeMoveBatchValid(b) || !rangeMoveCursorEqual(b.Pages[0].Before, prior.Cursor) {
		return RangeMovePageEffects{}, PageWork{}, ErrInvalid
	}
	p, base, err := rangeMoveTargetReader(ctx, target, d, &prior, l, g, budget, output)
	if err != nil {
		return RangeMovePageEffects{}, PageWork{}, err
	}
	next := prior
	for _, page := range b.Pages {
		next, err = stageRangeMovePageIn(p, d, next, page)
		if err != nil {
			err = errors.Join(err, p.budget())
			return RangeMovePageEffects{}, p.work, err
		}
		if len(p.q.pending) > 128 {
			err = errors.Join(ErrResourceLimit, p.budget())
			return RangeMovePageEffects{}, p.work, err
		}
	}
	effects, work, err := rangeMovePageResult(p, base, next)
	if err != nil {
		return RangeMovePageEffects{}, work, err
	}
	for i := 1; i < len(effects.Writes); i++ {
		if bytes.Compare(effects.Writes[i-1].Key, effects.Writes[i].Key) >= 0 {
			return RangeMovePageEffects{}, work, errors.Join(ErrCorrupt, ErrInvalid)
		}
	}
	return effects, work, nil
}
