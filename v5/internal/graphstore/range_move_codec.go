package graphstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

type rangeMoveWriter struct {
	b      []byte
	max    int
	err    error
	output *graphstate.OutputBudget
	sizing bool
	n      int
}

func (w *rangeMoveWriter) add(b []byte) {
	if w.err != nil {
		return
	}
	if len(b) > w.max-w.n {
		w.err = ErrResourceLimit
		return
	}
	w.n += len(b)
	if w.sizing {
		return
	}
	if w.output == nil {
		w.err = ErrInvalid
		return
	}
	w.b = append(w.b, b...)
}
func (w *rangeMoveWriter) tag(v byte) { w.add([]byte{v}) }
func (w *rangeMoveWriter) number(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	w.add(b[:])
}
func (w *rangeMoveWriter) field(b []byte) {
	if len(b) > rangeMoveMaxLogicalBytes {
		w.err = ErrResourceLimit
		return
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(b)))
	w.add(length[:])
	w.add(b)
} // #nosec G115 -- field length checked <=16MiB.
func (w *rangeMoveWriter) flag(v bool) {
	if v {
		w.tag(1)
	} else {
		w.tag(0)
	}
}
func rangeMoveBuild(output *graphstate.OutputBudget, maximum int, emit func(*rangeMoveWriter)) ([]byte, error) {
	if output == nil {
		return nil, ErrInvalid
	}
	sizing := rangeMoveWriter{max: maximum - 32, sizing: true}
	emit(&sizing)
	if sizing.err != nil {
		return nil, sizing.err
	}
	// Own exact final backing before allocation; canonical header/hash scratch
	// is separately charged. Capacity includes the seal, so no retired growth
	// backing can disappear from the representation ledger.
	if e := output.Reserve(128 + 3*(sizing.n+32)); e != nil {
		return nil, callerError(e)
	}
	w := rangeMoveWriter{max: maximum - 32, output: output, b: make([]byte, 0, sizing.n+32)}
	emit(&w)
	if w.err != nil {
		return nil, w.err
	}
	return w.b, nil
}
func rangeMoveEncode(output *graphstate.OutputBudget, maximum int, emit func(*rangeMoveWriter)) ([]byte, error) {
	b, e := rangeMoveBuild(output, maximum, emit)
	if e != nil {
		return nil, e
	}
	return sealRecord(b), nil
}
func rangeMoveBody(wire []byte, magic string, maximum int) ([]byte, error) {
	if len(wire) > maximum {
		return nil, ErrResourceLimit
	}
	if len(wire) < 36 || !bytes.Equal(wire[:4], []byte(magic)) {
		return nil, ErrCorrupt
	}
	hash := sha256.Sum256(wire[:len(wire)-32])
	if !bytes.Equal(hash[:], wire[len(wire)-32:]) {
		return nil, ErrCorrupt
	}
	return wire[4 : len(wire)-32], nil
}
func rangeMoveFlag(c *cursor) (bool, error) {
	v, e := c.tag()
	if e != nil {
		return false, e
	}
	if v > 1 {
		return false, ErrCorrupt
	}
	return v == 1, nil
}
func rangeMoveArray(c *cursor, n int, dst []byte) error {
	b, e := c.take(n)
	if e == nil {
		copy(dst, b)
	}
	return e
}

func rangeMoveEmitCapture(w *rangeMoveWriter, c RangeMoveCapture) {
	w.add(c.Binding.Identity.Graph[:])
	w.number(c.Binding.Identity.Partition)
	w.add(c.Binding.Identity.Group[:])
	w.add(c.Binding.SemanticContractID[:])
	w.number(c.Generation)
	w.number(c.Index)
	w.add(c.Image[:])
	w.add(c.ImageHash[:])
}
func rangeMoveReadCapture(c *cursor) (r RangeMoveCapture, err error) {
	if err = rangeMoveArray(c, 16, r.Binding.Identity.Graph[:]); err != nil {
		return r, err
	}
	if r.Binding.Identity.Partition, err = c.number(); err != nil {
		return r, err
	}
	if err = rangeMoveArray(c, 16, r.Binding.Identity.Group[:]); err != nil {
		return r, err
	}
	if err = rangeMoveArray(c, 32, r.Binding.SemanticContractID[:]); err != nil {
		return r, err
	}
	if r.Generation, err = c.number(); err != nil {
		return r, err
	}
	if r.Index, err = c.number(); err != nil {
		return r, err
	}
	if err = rangeMoveArray(c, 172, r.Image[:]); err != nil {
		return r, err
	}
	err = rangeMoveArray(c, 32, r.ImageHash[:])
	return r, err
}

// EncodeRangeMoveDescriptor owns bounded inert manifest metadata.
func EncodeRangeMoveDescriptor(d RangeMoveDescriptor, output *graphstate.OutputBudget) ([]byte, error) {
	if !d.valid() || output == nil {
		return nil, ErrInvalid
	}
	if e := output.Reserve(rangeMoveFixedBytes); e != nil {
		return nil, callerError(e)
	}
	p, e := EncodeRangePublication(d.Spec.Publication)
	if e != nil {
		return nil, e
	}
	from, e := EncodeRangeOwnership(d.Spec.From)
	if e != nil {
		return nil, e
	}
	to, e := EncodeRangeOwnership(d.Spec.To)
	if e != nil {
		return nil, e
	}
	axis, e := appendAxis(nil, d.DefaultAxis.Axis(), DefaultLimits())
	if e != nil {
		return nil, e
	}
	return rangeMoveEncode(output, rangeMoveFixedBytes, func(w *rangeMoveWriter) {
		w.add([]byte{'R', 'M', 'D', 1})
		w.field(p)
		w.field(from)
		w.field(to)
		rangeMoveEmitCapture(w, d.Source)
		rangeMoveEmitCapture(w, d.Target)
		w.add(d.Declaration[:])
		w.add(d.Configuration[:])
		w.field(axis)
		w.number(d.Rows)
		w.number(d.Bytes)
		w.add(d.Digest[:])
		w.flag(d.Sealed)
	})
}

// DecodeRangeMoveDescriptor returns metadata only, never an admission capability.
func DecodeRangeMoveDescriptor(wire []byte, output *graphstate.OutputBudget) (RangeMoveDescriptor, error) {
	if output == nil {
		return RangeMoveDescriptor{}, ErrInvalid
	}
	body, e := rangeMoveBody(wire, "RMD\x01", rangeMoveFixedBytes)
	if e != nil {
		return RangeMoveDescriptor{}, e
	}
	if e = output.Reserve(rangeMoveFixedBytes + 4*len(wire)); e != nil {
		return RangeMoveDescriptor{}, callerError(e)
	}
	c := cursor{src: body}
	var d RangeMoveDescriptor
	p, e := c.field(256)
	if e != nil {
		return d, e
	}
	from, e := c.field(256)
	if e != nil {
		return d, e
	}
	to, e := c.field(256)
	if e != nil {
		return d, e
	}
	if d.Spec.Publication, e = DecodeRangePublication(p); e != nil {
		return RangeMoveDescriptor{}, e
	}
	if d.Spec.From, e = DecodeRangeOwnership(from); e != nil {
		return RangeMoveDescriptor{}, e
	}
	if d.Spec.To, e = DecodeRangeOwnership(to); e != nil {
		return RangeMoveDescriptor{}, e
	}
	if d.Source, e = rangeMoveReadCapture(&c); e != nil {
		return RangeMoveDescriptor{}, e
	}
	if d.Target, e = rangeMoveReadCapture(&c); e != nil {
		return RangeMoveDescriptor{}, e
	}
	if e = rangeMoveArray(&c, 32, d.Declaration[:]); e != nil {
		return RangeMoveDescriptor{}, e
	}
	if e = rangeMoveArray(&c, 32, d.Configuration[:]); e != nil {
		return RangeMoveDescriptor{}, e
	}
	axisWire, e := c.field(2048)
	if e != nil {
		return RangeMoveDescriptor{}, e
	}
	ac := cursor{src: axisWire}
	axis, e := decodeAxis(&ac, DefaultLimits())
	if e != nil || ac.done() != nil {
		return RangeMoveDescriptor{}, errors.Join(ErrCorrupt, e)
	}
	d.DefaultAxis, e = types.BindDefaultAxis(types.GraphID(d.Spec.Publication.Graph), axis, temporal.DefaultLimits())
	if e != nil {
		return RangeMoveDescriptor{}, errors.Join(ErrCorrupt, e)
	}
	if d.Rows, e = c.number(); e != nil {
		return RangeMoveDescriptor{}, e
	}
	if d.Bytes, e = c.number(); e != nil {
		return RangeMoveDescriptor{}, e
	}
	if e = rangeMoveArray(&c, 32, d.Digest[:]); e != nil {
		return RangeMoveDescriptor{}, e
	}
	if d.Sealed, e = rangeMoveFlag(&c); e != nil {
		return RangeMoveDescriptor{}, e
	}
	if c.done() != nil || !d.valid() {
		return RangeMoveDescriptor{}, ErrCorrupt
	}
	return d, nil
}

func rangeMoveEmitCursor(w *rangeMoveWriter, c RangeMoveCursor) {
	w.add(c.Binding[:])
	w.tag(c.Family)
	w.field(c.After)
	w.field(c.Key)
	w.tag(c.Depth)
	for _, v := range c.Path {
		w.number(v)
	}
	for _, v := range c.Slots {
		var b [2]byte
		binary.BigEndian.PutUint16(b[:], v)
		w.add(b[:])
	}
	w.number(c.Record)
	w.number(uint64(c.Cell))
	w.number(uint64(c.Offset))
	w.number(c.Sequence)
	w.number(c.Rows)
	w.number(c.Bytes)
	w.add(c.Digest[:])
}
func rangeMoveReadCursor(c *cursor) (v RangeMoveCursor, e error) {
	if e = rangeMoveArray(c, 32, v.Binding[:]); e != nil {
		return v, e
	}
	if v.Family, e = c.tag(); e != nil {
		return v, e
	}
	if v.After, e = c.field(rangeMoveMaxCursorBytes); e != nil {
		return v, e
	}
	if v.Key, e = c.field(rangeMoveMaxCursorBytes); e != nil {
		return v, e
	}
	if v.Depth, e = c.tag(); e != nil {
		return v, e
	}
	for i := range v.Path {
		if v.Path[i], e = c.number(); e != nil {
			return v, e
		}
	}
	for i := range v.Slots {
		b, err := c.take(2)
		if err != nil {
			return v, err
		}
		v.Slots[i] = binary.BigEndian.Uint16(b)
	}
	if v.Record, e = c.number(); e != nil {
		return v, e
	}
	cell, e := c.number()
	if e != nil || cell > 4096 {
		return v, errors.Join(ErrCorrupt, e)
	}
	v.Cell = uint32(cell) // #nosec G115 -- cell index admitted <=4096.
	offset, e := c.number()
	if e != nil || offset > rangeMoveMaxLogicalBytes {
		return v, errors.Join(ErrCorrupt, e)
	}
	v.Offset = uint32(offset) // #nosec G115 -- offset checked <=16MiB.
	if v.Sequence, e = c.number(); e != nil {
		return v, e
	}
	if v.Rows, e = c.number(); e != nil {
		return v, e
	}
	if v.Bytes, e = c.number(); e != nil {
		return v, e
	}
	e = rangeMoveArray(c, 32, v.Digest[:])
	return v, e
}
func rangeMoveValidCursor(c RangeMoveCursor) bool {
	return c.Binding != ([32]byte{}) && c.Family >= rangeMoveValues && c.Family <= rangeMoveDone && int(c.Depth) <= 8 && len(c.After)+len(c.Key) <= rangeMoveMaxCursorBytes && c.Cell <= 4096 && c.Offset <= rangeMoveMaxLogicalBytes && c.Digest != ([32]byte{})
}

// EncodeRangeMoveCursor owns a portable fixed-source continuation.
func EncodeRangeMoveCursor(c RangeMoveCursor, output *graphstate.OutputBudget) ([]byte, error) {
	if !rangeMoveValidCursor(c) || output == nil {
		return nil, ErrInvalid
	}
	return rangeMoveEncode(output, rangeMoveMaxCursorBytes+1056, func(w *rangeMoveWriter) { w.add([]byte{'R', 'M', 'C', 1}); rangeMoveEmitCursor(w, c) })
}

// DecodeRangeMoveCursor returns inert continuation data.
func DecodeRangeMoveCursor(wire []byte, output *graphstate.OutputBudget) (RangeMoveCursor, error) {
	if output == nil {
		return RangeMoveCursor{}, ErrInvalid
	}
	b, e := rangeMoveBody(wire, "RMC\x01", rangeMoveMaxCursorBytes+1060)
	if e != nil {
		return RangeMoveCursor{}, e
	}
	if e = output.Reserve(1024 + 2*len(wire)); e != nil {
		return RangeMoveCursor{}, callerError(e)
	}
	c := cursor{src: b}
	v, e := rangeMoveReadCursor(&c)
	if e != nil || c.done() != nil || !rangeMoveValidCursor(v) {
		return RangeMoveCursor{}, errors.Join(ErrCorrupt, e)
	}
	v.After = exactCopy(v.After)
	v.Key = exactCopy(v.Key)
	return v, nil
}

// Progress-only pages retain actual catalog continuation without claiming a
// logical record. A partial record cannot be skipped, and every accumulator and
// unrelated frozen path field remains exact. The outer native proof still owns
// source provenance; this is canonical inert transport validation only.
func rangeMoveScanProgress(p RangeMovePage) bool {
	if p.Complete || p.Kind != 0 || p.RecordDone || p.RecordBytes != 0 || p.Offset != 0 || len(p.Key) != 0 || len(p.Payload) != 0 || p.Before.Offset != 0 || p.Next.Offset != 0 || p.Next.Family < rangeMoveValues || p.Next.Family > rangeMoveComponents || len(p.Next.Key) != 0 || p.Next.Record != 0 || p.Next.Cell != 0 || p.Next.Depth != 0 || p.Next.Path != ([32]uint64{}) || p.Next.Slots != ([32]uint16{}) || p.Next.Rows != p.Before.Rows || p.Next.Bytes != p.Before.Bytes || p.Next.Digest != p.Before.Digest || len(p.Next.After) == 0 || p.Next.Family == p.Before.Family && bytes.Compare(p.Next.After, p.Before.After) <= 0 {
		return false
	}
	return p.Next.Family >= p.Before.Family
}
func rangeMoveValidPage(p RangeMovePage) bool {
	if !rangeMoveValidCursor(p.Before) || !rangeMoveValidCursor(p.Next) || p.Before.Binding != p.Next.Binding || p.Before.Sequence == ^uint64(0) || p.Next.Sequence != p.Before.Sequence+1 || p.Next.Family < p.Before.Family || p.Offset != p.Before.Offset || len(p.Key) > rangeMoveMaxCursorBytes || len(p.Payload) > rangeMoveMaxLogicalBytes || p.RecordBytes > rangeMoveMaxLogicalBytes || uint64(p.Offset)+uint64(len(p.Payload)) > uint64(p.RecordBytes) {
		return false
	}
	if p.Kind == 0 && !p.Complete {
		return rangeMoveScanProgress(p)
	}
	return p.Complete && !p.RecordDone && p.RecordBytes == 0 && p.Offset == 0 && p.Next.Offset == 0 && p.Kind == 0 && len(p.Key) == 0 && len(p.Payload) == 0 && p.Next.Family == rangeMoveDone || !p.Complete && p.Kind == p.Next.Family && p.Kind >= rangeMoveValues && p.Kind < rangeMoveDone && len(p.Key) > 0 && p.RecordBytes > 0 && len(p.Payload) > 0 && p.RecordDone == (uint64(p.Offset)+uint64(len(p.Payload)) == uint64(p.RecordBytes))
}

// EncodeRangeMovePage owns one bounded fragment, never a whole transfer.
func EncodeRangeMovePage(p RangeMovePage, output *graphstate.OutputBudget) ([]byte, error) {
	if !rangeMoveValidPage(p) || output == nil {
		return nil, ErrInvalid
	}
	return rangeMoveEncode(output, rangeMoveMaxLogicalBytes+2*rangeMoveMaxCursorBytes+4128, func(w *rangeMoveWriter) {
		w.add([]byte{'R', 'M', 'P', 1})
		rangeMoveEmitCursor(w, p.Before)
		rangeMoveEmitCursor(w, p.Next)
		w.tag(p.Kind)
		w.field(p.Key)
		w.field(p.Payload)
		w.number(uint64(p.RecordBytes))
		w.number(uint64(p.Offset))
		w.flag(p.RecordDone)
		w.flag(p.Complete)
	})
}

// DecodeRangeMovePage owns decoded fragment buffers and no source proof.
func DecodeRangeMovePage(wire []byte, output *graphstate.OutputBudget) (RangeMovePage, error) {
	if output == nil {
		return RangeMovePage{}, ErrInvalid
	}
	b, e := rangeMoveBody(wire, "RMP\x01", rangeMoveMaxLogicalBytes+2*rangeMoveMaxCursorBytes+4132)
	if e != nil {
		return RangeMovePage{}, e
	}
	if e = output.Reserve(4096 + 3*len(wire)); e != nil {
		return RangeMovePage{}, callerError(e)
	}
	c := cursor{src: b}
	var p RangeMovePage
	if p.Before, e = rangeMoveReadCursor(&c); e != nil {
		return RangeMovePage{}, e
	}
	if p.Next, e = rangeMoveReadCursor(&c); e != nil {
		return RangeMovePage{}, e
	}
	if p.Kind, e = c.tag(); e != nil {
		return RangeMovePage{}, e
	}
	if p.Key, e = c.field(rangeMoveMaxCursorBytes); e != nil {
		return RangeMovePage{}, e
	}
	if p.Payload, e = c.field(rangeMoveMaxLogicalBytes); e != nil {
		return RangeMovePage{}, e
	}
	n, e := c.number()
	if e != nil || n > rangeMoveMaxLogicalBytes {
		return RangeMovePage{}, errors.Join(ErrCorrupt, e)
	}
	p.RecordBytes = uint32(n) // #nosec G115 -- count checked <=16MiB.
	n, e = c.number()
	if e != nil || n > rangeMoveMaxLogicalBytes {
		return RangeMovePage{}, errors.Join(ErrCorrupt, e)
	}
	p.Offset = uint32(n) // #nosec G115 -- offset checked <=16MiB.
	if p.RecordDone, e = rangeMoveFlag(&c); e != nil {
		return RangeMovePage{}, e
	}
	if p.Complete, e = rangeMoveFlag(&c); e != nil {
		return RangeMovePage{}, e
	}
	if c.done() != nil || !rangeMoveValidPage(p) {
		return RangeMovePage{}, ErrCorrupt
	}
	p.Key = exactCopy(p.Key)
	p.Payload = exactCopy(p.Payload)
	p.Before.After = exactCopy(p.Before.After)
	p.Before.Key = exactCopy(p.Before.Key)
	p.Next.After = exactCopy(p.Next.After)
	p.Next.Key = exactCopy(p.Next.Key)
	return p, nil
}

// EncodeRangeMoveImportState owns fixed roots/progress and one record fragment.
func EncodeRangeMoveImportState(s RangeMoveImportState, output *graphstate.OutputBudget) ([]byte, error) {
	if output == nil || !rangeMoveValidImportState(s) {
		return nil, ErrInvalid
	}
	if e := output.Reserve(1024); e != nil {
		return nil, callerError(e)
	}
	root, e := EncodeRoot(s.FutureRoot)
	if e != nil {
		return nil, e
	}
	index, e := encodeFullDescriptor(s.index, s.FutureRoot, DefaultLimits())
	if e != nil {
		return nil, e
	}
	return rangeMoveEncode(output, 3*rangeMoveMaxCursorBytes+8224, func(w *rangeMoveWriter) {
		w.add([]byte{'R', 'M', 'S', 1})
		rangeMoveEmitCursor(w, s.Cursor)
		w.number(s.Sequence)
		w.number(s.Rows)
		w.number(s.Bytes)
		w.add(s.Digest[:])
		w.flag(s.Complete)
		w.flag(s.Sealed)
		w.flag(s.PreparationComplete)
		w.number(s.PreparationSteps)
		w.field(s.component)
		w.field(root)
		w.field(index)
		w.tag(s.fragmentKind)
		w.field(s.fragmentKey)
		w.number(s.fragmentStart)
		w.number(uint64(s.fragmentOffset))
		w.number(uint64(s.fragmentBytes))
	})
}

// DecodeRangeMoveImportState returns progress data; the outer native control is
// its only admission source. No current root or placement is published here.
func DecodeRangeMoveImportState(wire []byte, output *graphstate.OutputBudget) (RangeMoveImportState, error) {
	if output == nil {
		return RangeMoveImportState{}, ErrInvalid
	}
	b, e := rangeMoveBody(wire, "RMS\x01", 3*rangeMoveMaxCursorBytes+8224)
	if e != nil {
		return RangeMoveImportState{}, e
	}
	if e = output.Reserve(8192 + 3*len(wire)); e != nil {
		return RangeMoveImportState{}, callerError(e)
	}
	c := cursor{src: b}
	var s RangeMoveImportState
	if s.Cursor, e = rangeMoveReadCursor(&c); e != nil {
		return RangeMoveImportState{}, e
	}
	if s.Sequence, e = c.number(); e != nil {
		return RangeMoveImportState{}, e
	}
	if s.Rows, e = c.number(); e != nil {
		return RangeMoveImportState{}, e
	}
	if s.Bytes, e = c.number(); e != nil {
		return RangeMoveImportState{}, e
	}
	if e = rangeMoveArray(&c, 32, s.Digest[:]); e != nil {
		return RangeMoveImportState{}, e
	}
	if s.Complete, e = rangeMoveFlag(&c); e != nil {
		return RangeMoveImportState{}, e
	}
	if s.Sealed, e = rangeMoveFlag(&c); e != nil {
		return RangeMoveImportState{}, e
	}
	if s.PreparationComplete, e = rangeMoveFlag(&c); e != nil {
		return RangeMoveImportState{}, e
	}
	if s.PreparationSteps, e = c.number(); e != nil {
		return RangeMoveImportState{}, e
	}
	if s.component, e = c.field(rangeMoveMaxCursorBytes); e != nil {
		return RangeMoveImportState{}, e
	}
	root, e := c.field(172)
	if e != nil {
		return RangeMoveImportState{}, e
	}
	s.FutureRoot, e = DecodeRoot(root)
	if e != nil {
		return RangeMoveImportState{}, e
	}
	index, e := c.field(512)
	if e != nil {
		return RangeMoveImportState{}, e
	}
	cat := &Catalog{root: s.FutureRoot, rootImageBytes: 172, limits: DefaultLimits()}
	key := componentIndexDescriptorKey(s.FutureRoot.namespace)
	q := reader{c: cat, ctx: context.Background(), pending: map[string]raftlog.KV{string(key): {Key: key, Value: index}}}
	s.index, _, e = q.fullDescriptor(s.FutureRoot)
	if e != nil {
		return RangeMoveImportState{}, e
	}
	if s.fragmentKind, e = c.tag(); e != nil {
		return RangeMoveImportState{}, e
	}
	if s.fragmentKey, e = c.field(rangeMoveMaxCursorBytes); e != nil {
		return RangeMoveImportState{}, e
	}
	if s.fragmentStart, e = c.number(); e != nil {
		return RangeMoveImportState{}, e
	}
	offset, e := c.number()
	if e != nil || offset > rangeMoveMaxLogicalBytes {
		return RangeMoveImportState{}, errors.Join(ErrCorrupt, e)
	}
	s.fragmentOffset = uint32(offset) // #nosec G115 -- admitted <=16MiB.
	n, e := c.number()
	if e != nil || n > rangeMoveMaxLogicalBytes {
		return RangeMoveImportState{}, errors.Join(ErrCorrupt, e)
	}
	s.fragmentBytes = uint32(n) // #nosec G115 -- admitted <=16MiB.
	if c.done() != nil {
		return RangeMoveImportState{}, ErrCorrupt
	}
	encoded, e := EncodeRangeMoveImportState(s, output)
	if e != nil || !bytes.Equal(encoded, wire) {
		return RangeMoveImportState{}, errors.Join(ErrCorrupt, e)
	}
	s.Cursor.After = exactCopy(s.Cursor.After)
	s.Cursor.Key = exactCopy(s.Cursor.Key)
	s.fragmentKey = exactCopy(s.fragmentKey)
	s.component = exactCopy(s.component)
	return s, nil
}

func rangeMoveValidImportState(s RangeMoveImportState) bool {
	if !rangeMoveValidCursor(s.Cursor) || s.Sequence != s.Cursor.Sequence || s.Rows != s.Cursor.Rows || s.Bytes != s.Cursor.Bytes || s.Digest != s.Cursor.Digest || s.Complete != (s.Cursor.Family == rangeMoveDone) || s.Sealed && (!s.Complete || !s.PreparationComplete) || len(s.component) > rangeMoveMaxCursorBytes || len(s.fragmentKey) > rangeMoveMaxCursorBytes || s.fragmentOffset != s.Cursor.Offset {
		return false
	}
	if s.fragmentOffset == 0 {
		return s.fragmentStart == 0 && s.fragmentKind == 0 && s.fragmentBytes == 0 && len(s.fragmentKey) == 0
	}
	return s.PreparationComplete && !s.Complete && !s.Sealed && s.fragmentStart > 0 && s.fragmentStart <= s.Sequence && s.fragmentKind == s.Cursor.Family && s.fragmentBytes > s.fragmentOffset && s.fragmentBytes <= rangeMoveMaxLogicalBytes && bytes.Equal(s.fragmentKey, s.Cursor.Key)
}
