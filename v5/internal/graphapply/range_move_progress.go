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
	reference           moveRetentionReference
	inlineRetention     bool
	replayPages         uint64
	descriptor          graphstore.RangeMoveDescriptor
	retention           []byte
	retentionRows       uint64
	cursor              graphstore.RangeMoveCursor
	imported            *graphstore.RangeMoveImportState
	replay, replayEnd   rangeMoveReplayCursor
	graphPages          uint64
	graphApplied        uint64 // GMP5 only: installed graph envelopes, not logical RMC sequence
	undoNext, undoTotal uint64
}

func encodeRangeMoveProgress(p rangeMoveProgress, output *graphstate.OutputBudget) ([]byte, error) {
	version := byte(2)
	fresh := rangeMoveSpans(p.descriptor.Source.Binding.SemanticContractID)
	if fresh {
		if len(p.retention) != 0 {
			packet, err := decodeMoveRetainedLineage(p.retention, output)
			if err != nil {
				return nil, err
			}
			if err = p.bindRetention(packet, p.retention); err != nil {
				return nil, err
			}
		}
		if !p.reference.valid(p.descriptor) {
			return nil, errInvalid
		}
		version = 4
		if rangeMoveGraphBatches(p.descriptor.Source.Binding.SemanticContractID) {
			version = 5
		}
	} else if collectedGraphAgreement(p.descriptor.Source.Binding.SemanticContractID) {
		packet, err := decodeMoveRetainedLineage(p.retention, output)
		if err != nil {
			return nil, err
		}
		if checkMoveRetentionDescriptor(packet, p.descriptor) != nil || p.retentionRows != uint64(len(packet.owners)+1) {
			return nil, errInvalid
		}
		version = 3
	} else if len(p.retention) != 0 || p.retentionRows != 0 {
		return nil, errInvalid
	}
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
	known := 4096 + 8*(len(descriptor)+len(cursor)+len(imported)+len(replay)+len(replayEnd)+len(p.retention))
	if err := output.Reserve(known); err != nil {
		return nil, outputBudgetFailure(err)
	}
	maximum := (1 << 20) - 128
	if fresh && !p.inlineRetention {
		maximum = rangeMoveSpanProgressBytes
	}
	return boundedEncoding(maximum, func(w *boundedWriter) {
		w.add([]byte{'G', 'M', 'P', version})
		w.field(descriptor)
		w.field(cursor)
		w.field(imported)
		w.field(replay)
		w.field(replayEnd)
		w.u64(p.graphPages)
		w.u64(p.undoNext)
		w.u64(p.undoTotal)
		if version == 3 {
			w.field(p.retention)
		}
		if version >= 4 {
			p.emitReference(w)
			w.u64(p.replayPages)
			if version == 5 {
				w.u64(p.graphApplied)
			}
			if p.inlineRetention {
				w.tag(1)
				w.field(p.retention)
			} else {
				w.tag(0)
				w.field(nil)
			}
		}
	})
}

func decodeRangeMoveProgress(wire []byte, output *graphstate.OutputBudget) (rangeMoveProgress, error) {
	if output == nil {
		return rangeMoveProgress{}, errInvalid
	}
	version, magic := byte(2), "GMP\x02"
	if len(wire) >= 4 && bytes.Equal(wire[:4], []byte{'G', 'M', 'P', 3}) {
		version, magic = 3, "GMP\x03"
	}
	if bytes.HasPrefix(wire, []byte("GMP\x04")) {
		version, magic = 4, "GMP\x04"
	}
	if bytes.HasPrefix(wire, []byte("GMP\x05")) {
		version, magic = 5, "GMP\x05"
	}
	body, err := graphBody(wire, magic, (1<<20)-128)
	if err != nil {
		return rangeMoveProgress{}, err
	}
	c := graphCursor{b: body}
	descriptor, cursor, imported := c.field(16<<10), c.field(129<<10), c.field((1<<20)-1024)
	replay, replayEnd := c.field(rangeMoveReplayCursorBytes), c.field(rangeMoveReplayCursorBytes)
	graphPages := c.u64()
	undoNext, undoTotal := c.u64(), c.u64()
	var retained []byte
	var reference moveRetentionReference
	var replayPages, graphApplied uint64
	inline := byte(0)
	if version == 3 {
		retained = c.field(rangeMoveRetentionBytes)
	}
	if version >= 4 {
		reference = readMoveReference(&c)
		replayPages = c.u64()
		if version == 5 {
			graphApplied = c.u64()
		}
		inline = c.tag()
		retained = c.field(rangeMoveRetentionBytes)
	}
	if c.err != nil || len(c.b) != 0 || inline > 1 || version >= 4 && (inline == 1) != (len(retained) != 0) {
		return rangeMoveProgress{}, errors.Join(errCorrupt, c.err)
	}
	p := rangeMoveProgress{graphApplied: graphApplied, reference: reference, replayPages: replayPages, inlineRetention: inline == 1, undoNext: undoNext, undoTotal: undoTotal, graphPages: graphPages}
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
	if version >= 4 {
		if !rangeMoveSpans(p.descriptor.Source.Binding.SemanticContractID) || !p.reference.valid(p.descriptor) || !p.inlineRetention && len(wire) > rangeMoveSpanProgressBytes {
			return rangeMoveProgress{}, errCorrupt
		}
		p.retentionRows = p.reference.rows
		if e := output.Reserve(128 + len(p.reference.key)); e != nil {
			return rangeMoveProgress{}, outputBudgetFailure(e)
		}
		p.reference.key = owned(p.reference.key)
		if len(retained) != 0 {
			packet, e := decodeMoveRetainedLineage(retained, output)
			if e != nil {
				return rangeMoveProgress{}, e
			}
			if e = p.bindRetention(packet, retained); e != nil {
				return rangeMoveProgress{}, e
			}
			if e = output.Reserve(len(retained) + 64); e != nil {
				return rangeMoveProgress{}, outputBudgetFailure(e)
			}
			p.retention = owned(retained)
		}
	} else if version == 3 {
		packet, e := decodeMoveRetainedLineage(retained, output)
		if e != nil {
			return rangeMoveProgress{}, e
		}
		if checkMoveRetentionDescriptor(packet, p.descriptor) != nil {
			return rangeMoveProgress{}, errCorrupt
		}
		if e := output.Reserve(len(retained) + 64); e != nil {
			return rangeMoveProgress{}, outputBudgetFailure(e)
		}
		p.retention, p.retentionRows = owned(retained), uint64(len(packet.owners)+1)
	} else if collectedGraphAgreement(p.descriptor.Source.Binding.SemanticContractID) {
		return rangeMoveProgress{}, errCorrupt
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
	if (version >= 4) != rangeMoveSpans(p.descriptor.Source.Binding.SemanticContractID) || (version == 5) != rangeMoveGraphBatches(p.descriptor.Source.Binding.SemanticContractID) {
		return rangeMoveProgress{}, errCorrupt
	}
	limit := uint64(rangeMovePageLimit(p.descriptor.Source.Binding.SemanticContractID))
	replayLimit := limit
	if version >= 4 {
		replayLimit *= rangeMoveSpanBatchRows
	}
	if p.imported != nil && p.cursor.Binding != ([32]byte{}) || p.undoNext > p.undoTotal || p.undoTotal > limit || p.graphPages > limit || p.replay.sequence > replayLimit || p.replayEnd.sequence > replayLimit || p.descriptor.Rows > math.MaxUint64-p.manifestReplay().rows || p.descriptor.Rows+p.manifestReplay().rows > math.MaxUint64-p.retentionRows || p.descriptor.Bytes > math.MaxUint64-p.manifestReplay().bytes || p.descriptor.Bytes+p.manifestReplay().bytes > math.MaxUint64-p.retainedBytes() {

		return rangeMoveProgress{}, errCorrupt
	}
	if version >= 4 && (p.graphPages+p.replayPages > limit || p.replayPages > limit || p.replayPages > p.replay.sequence || p.replay.sequence > rangeMoveSpanBatchRows*p.replayPages) {
		return rangeMoveProgress{}, errCorrupt
	}
	if p.imported == nil {
		if !rangeMoveGraphBatches(p.descriptor.Source.Binding.SemanticContractID) && p.graphPages != p.cursor.Sequence || version == 5 && (p.graphApplied != 0 || !moveGraphCountValid(p.graphPages, p.cursor.Sequence)) || p.replayEnd.family != 0 || p.replay.family != 0 && !p.descriptor.Sealed {
			return rangeMoveProgress{}, errCorrupt
		}
	} else if !p.descriptor.Sealed || !p.replayEnd.complete || p.replay.sequence > p.replayEnd.sequence || p.replay.rows > p.replayEnd.rows || p.replay.bytes > p.replayEnd.bytes {
		return rangeMoveProgress{}, errCorrupt
	}
	if version == 5 && p.imported != nil && (!moveGraphCountValid(p.graphApplied, p.imported.Sequence) || p.graphApplied > p.graphPages || p.imported.Complete != (p.graphApplied == p.graphPages)) {
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
	rows, size = p.descriptor.Rows+replay.rows+p.retentionRows, p.descriptor.Bytes+replay.bytes+p.retainedBytes()
	h := sha256.New()
	_, _ = h.Write([]byte("rho-tkg:range-move-complete-stream:v1\x00"))
	_, _ = h.Write(p.descriptor.Digest[:])
	_, _ = h.Write(replay.digest[:])
	if p.retainedBytes() != 0 {
		_, _ = h.Write([]byte("retained-lineage-v1\x00"))
		sum := sha256.Sum256(p.retention)
		if rangeMoveSpans(p.descriptor.Source.Binding.SemanticContractID) {
			sum = p.reference.packet
		}
		_, _ = h.Write(sum[:])
	}
	if rangeMoveSpans(p.descriptor.Source.Binding.SemanticContractID) {
		_, _ = h.Write([]byte("batch-pages-v1\x00"))
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], p.replayPages)
		_, _ = h.Write(n[:])
	}
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
