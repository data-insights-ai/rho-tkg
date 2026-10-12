package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
)

type rangeMoveProgress struct {
	descriptor          graphstore.RangeMoveDescriptor
	cursor              graphstore.RangeMoveCursor
	imported            *graphstore.RangeMoveImportState
	replay, replayEnd   rangeMoveReplayCursor
	graphPages          uint64
	undoNext, undoTotal uint64
}

func encodeRangeMoveProgress(p rangeMoveProgress, output *graphstate.OutputBudget) ([]byte, error) {
	descriptor, err := graphstore.EncodeRangeMoveDescriptor(p.descriptor, output)
	if err != nil {
		return nil, err
	}
	var cursor, imported []byte
	if p.cursor.Binding != ([32]byte{}) {
		cursor, err = graphstore.EncodeRangeMoveCursor(p.cursor, output)
		if err != nil {
			return nil, err
		}
	}
	if p.imported != nil {
		imported, err = graphstore.EncodeRangeMoveImportState(*p.imported, output)
		if err != nil {
			return nil, err
		}
	}
	replay, err := encodeRangeMoveReplayCursor(p.replay, output)
	if err != nil {
		return nil, err
	}
	replayEnd, err := encodeRangeMoveReplayCursor(p.replayEnd, output)
	if err != nil {
		return nil, err
	}
	known := 4096 + 8*(len(descriptor)+len(cursor)+len(imported)+len(replay)+len(replayEnd))
	if err := output.Reserve(known); err != nil {
		return nil, outputBudgetFailure(err)
	}
	return boundedEncoding((1<<20)-128, func(w *boundedWriter) {
		w.add([]byte{'G', 'M', 'P', 2})
		w.field(descriptor)
		w.field(cursor)
		w.field(imported)
		w.field(replay)
		w.field(replayEnd)
		w.u64(p.graphPages)
		w.u64(p.undoNext)
		w.u64(p.undoTotal)
	})
}

func decodeRangeMoveProgress(wire []byte, output *graphstate.OutputBudget) (rangeMoveProgress, error) {
	if output == nil {
		return rangeMoveProgress{}, errInvalid
	}
	body, err := graphBody(wire, "GMP\x02", (1<<20)-128)
	if err != nil {
		return rangeMoveProgress{}, err
	}
	c := graphCursor{b: body}
	descriptor, cursor, imported := c.field(16<<10), c.field(129<<10), c.field((1<<20)-1024)
	replay, replayEnd := c.field(rangeMoveReplayCursorBytes), c.field(rangeMoveReplayCursorBytes)
	graphPages := c.u64()
	undoNext, undoTotal := c.u64(), c.u64()
	if c.err != nil || len(c.b) != 0 {
		return rangeMoveProgress{}, errors.Join(errCorrupt, c.err)
	}
	p := rangeMoveProgress{undoNext: undoNext, undoTotal: undoTotal, graphPages: graphPages}
	p.replay, err = decodeRangeMoveReplayCursor(replay, output)
	if err != nil {
		return rangeMoveProgress{}, err
	}
	p.replayEnd, err = decodeRangeMoveReplayCursor(replayEnd, output)
	if err != nil {
		return rangeMoveProgress{}, err
	}
	p.descriptor, err = graphstore.DecodeRangeMoveDescriptor(descriptor, output)
	if err != nil {
		return rangeMoveProgress{}, err
	}
	if len(cursor) != 0 {
		p.cursor, err = graphstore.DecodeRangeMoveCursor(cursor, output)
		if err != nil {
			return rangeMoveProgress{}, err
		}
	}
	if len(imported) != 0 {
		state, err := graphstore.DecodeRangeMoveImportState(imported, output)
		if err != nil {
			return rangeMoveProgress{}, err
		}
		p.imported = new(state)
	}
	if p.imported != nil && p.cursor.Binding != ([32]byte{}) || p.undoNext > p.undoTotal || p.undoTotal > 1024 || p.graphPages > 1024 || p.replay.sequence > 1024 || p.replayEnd.sequence > 1024 || p.descriptor.Rows > math.MaxUint64-p.manifestReplay().rows || p.descriptor.Bytes > math.MaxUint64-p.manifestReplay().bytes {

		return rangeMoveProgress{}, errCorrupt
	}
	if p.imported == nil {
		if p.graphPages != p.cursor.Sequence || p.replayEnd.family != 0 || p.replay.family != 0 && !p.descriptor.Sealed {
			return rangeMoveProgress{}, errCorrupt
		}
	} else if !p.descriptor.Sealed || !p.replayEnd.complete || p.replay.sequence > p.replayEnd.sequence || p.replay.rows > p.replayEnd.rows || p.replay.bytes > p.replayEnd.bytes {
		return rangeMoveProgress{}, errCorrupt
	}
	return p, nil
}

// The sealed handoff includes graph content and exact original protocol history.
// This digest binds the two frozen streams without retaining their records.
func (p rangeMoveProgress) transferComplete() bool { return p.descriptor.Sealed && p.replay.complete }
func (p rangeMoveProgress) manifestReplay() rangeMoveReplayCursor {
	if p.imported != nil {
		return p.replayEnd
	}
	return p.replay
}
func (p rangeMoveProgress) transferSummary() (rows, size uint64, digest [32]byte) {
	replay := p.manifestReplay()
	rows, size = p.descriptor.Rows+replay.rows, p.descriptor.Bytes+replay.bytes
	h := sha256.New()
	_, _ = h.Write([]byte("rho-tkg:range-move-complete-stream:v1\x00"))
	_, _ = h.Write(p.descriptor.Digest[:])
	_, _ = h.Write(replay.digest[:])
	var b [8]byte
	for _, v := range []uint64{p.graphPages, replay.sequence, rows, size, p.descriptor.Source.Index, p.descriptor.Target.Index} {
		binary.BigEndian.PutUint64(b[:], v)
		_, _ = h.Write(b[:])
	}
	_, _ = h.Write(p.descriptor.Source.ImageHash[:])
	_, _ = h.Write(p.descriptor.Target.ImageHash[:])
	h.Sum(digest[:0])
	return
}
func rangeMoveReplaySame(a, b rangeMoveReplayCursor) bool {
	return a.family == b.family && a.sequence == b.sequence && a.rows == b.rows && a.bytes == b.bytes && a.digest == b.digest && a.complete == b.complete && a.offset == b.offset && a.recordBytes == b.recordBytes && a.startSequence == b.startSequence && bytes.Equal(a.after, b.after) && bytes.Equal(a.key, b.key)
}
