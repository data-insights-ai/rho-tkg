package graphapply

import (
	"bytes"
	"errors"
)

const nativeColumnNumeric = 14

func nativeColumnNumbers(r nativeColumnRow) [nativeColumnNumeric]uint64 {
	return [nativeColumnNumeric]uint64{r.Owner, r.Life, r.Source, r.Target, r.SourceLife, r.TargetLife, r.Before.Handle, uint64(r.Before.Value), r.Before.Revision, r.Before.Provenance, r.After.Handle, uint64(r.After.Value), r.After.Revision, r.After.Provenance} // #nosec G115 -- signed values preserve their exact two's-complement wire bits.
}
func nativeColumnFlags(c nativeColumnCell) byte {
	var flag byte
	if c.Present {
		flag |= 1
	}
	if c.Null {
		flag |= 2
	}
	return flag
}

// Originals occur once per frame in the arena. Numeric columns are contiguous,
// not a sequence of serialized row objects. Every admission precedes copying.
func encodeNativeColumnChunk(g nativeColumnGeneration, ordinal uint64, previous [32]byte, frames []nativeColumnFrame, p nativeColumnPolicy) ([]byte, error) {
	if p.validate() != nil || len(frames) < 1 || len(frames) > p.ChunkRecords {
		return nil, errInvalid
	}
	generation, err := nativeColumnGenerationHash(g)
	if err != nil {
		return nil, err
	}
	rows, size := 0, 4+32+8+32+4+32
	for _, f := range frames {
		if f.Outcome == nil || f.CDC == nil || f.Partition == 0 || f.Index == 0 {
			return nil, errInvalid
		}
		if len(f.Outcome) > p.FrameBytes || len(f.CDC) > p.FrameBytes || len(f.Registration) > p.FrameBytes || len(f.OriginalOutcome) > p.FrameBytes || len(f.OriginalCDC) > p.FrameBytes || len(f.Rows) > p.ChunkRows-rows {
			return nil, errLimit
		}
		rows += len(f.Rows)
		size += 8 + 8 + 172 + 4 + 4 + 4 + 4 + 4 + 4 + len(f.Outcome) + len(f.CDC) + len(f.Registration) + len(f.OriginalOutcome) + len(f.OriginalCDC)
		for _, r := range f.Rows {
			if r.Owner == 0 || len(r.Scope) == 0 || len(r.Axis) == 0 || len(r.Name) > 256 || len(r.Type) > 256 {
				return nil, errInvalid
			}
			if len(r.Axis) > p.FrameBytes || len(r.Scope) > p.FrameBytes {
				return nil, errLimit
			}
			size += 7 + 8*nativeColumnNumeric + 16 + len(r.Axis) + len(r.Scope) + len(r.Name) + len(r.Type)
		}
	}
	if size > p.ChunkBytes {
		return nil, errLimit
	}
	return boundedEncoding(p.ChunkBytes, func(w *boundedWriter) {
		w.add([]byte("NCC1"))
		w.add(generation[:])
		w.u64(ordinal)
		w.add(previous[:])
		w.u32(len(frames))
		for _, f := range frames {
			w.u64(f.Partition)
			w.u64(f.Index)
			w.add(f.Root[:])
			w.field(f.Outcome)
			w.field(f.CDC)
			w.field(f.Registration)
			w.field(f.OriginalOutcome)
			w.field(f.OriginalCDC)
			w.u32(len(f.Rows))
		}
		for column := range 7 {
			for _, f := range frames {
				for _, r := range f.Rows {
					flags := [7]byte{r.Component, r.Kind, r.Mode, r.Interpretation, r.Role, nativeColumnFlags(r.Before), nativeColumnFlags(r.After)}
					w.tag(flags[column])
				}
			}
		}
		for column := range nativeColumnNumeric {
			for _, f := range frames {
				for _, r := range f.Rows {
					w.u64(nativeColumnNumbers(r)[column])
				}
			}
		}
		for _, f := range frames {
			for _, r := range f.Rows {
				w.text(r.Name)
				w.text(r.Type)
				w.field(r.Axis)
				w.field(r.Scope)
			}
		}
	})
}
func decodeNativeColumnChunk(g nativeColumnGeneration, ordinal uint64, previous [32]byte, wire []byte, p nativeColumnPolicy) ([]nativeColumnFrame, error) {
	if p.validate() != nil {
		return nil, errInvalid
	}
	body, err := graphBody(wire, "NCC1", p.ChunkBytes)
	if err != nil {
		return nil, err
	}
	generation, err := nativeColumnGenerationHash(g)
	if err != nil {
		return nil, err
	}
	c := graphCursor{b: body}
	if !bytes.Equal(c.take(32), generation[:]) || c.u64() != ordinal || !bytes.Equal(c.take(32), previous[:]) {
		return nil, errCorrupt
	}
	count := c.count(p.ChunkRecords, 212)
	if c.err != nil || count < 1 {
		return nil, errors.Join(errCorrupt, c.err)
	}
	frames := make([]nativeColumnFrame, count)
	rows := 0
	var counts [16]int
	for i := range frames {
		f := &frames[i]
		f.Partition, f.Index = c.u64(), c.u64()
		copy(f.Root[:], c.take(172))
		f.Outcome = owned(c.field(p.FrameBytes))
		f.CDC = owned(c.field(p.FrameBytes))
		f.Registration = owned(c.field(p.FrameBytes))
		f.OriginalOutcome = owned(c.field(p.FrameBytes))
		f.OriginalCDC = owned(c.field(p.FrameBytes))
		n := c.count(p.ChunkRows-rows, 0)
		if c.err != nil || f.Partition == 0 || f.Index == 0 {
			return nil, errors.Join(errCorrupt, c.err)
		}
		rows += n
		counts[i] = n
	}
	if rows > len(c.b)/(7+8*nativeColumnNumeric+16) {
		return nil, errCorrupt
	}
	for i := range frames {
		frames[i].Rows = make([]nativeColumnRow, counts[i])
	}
	for column := range 7 {
		for i := range frames {
			for j := range frames[i].Rows {
				r := &frames[i].Rows[j]
				flag := c.tag()
				switch column {
				case 0:
					r.Component = flag
				case 1:
					r.Kind = flag
				case 2:
					r.Mode = flag
				case 3:
					r.Interpretation = flag
				case 4:
					r.Role = flag
				case 5:
					r.Before.Present = flag&1 != 0
					r.Before.Null = flag&2 != 0
				case 6:
					r.After.Present = flag&1 != 0
					r.After.Null = flag&2 != 0
				}
				if column >= 5 && (flag > 3 || flag == 2) {
					return nil, errCorrupt
				}
			}
		}
	}
	for column := range nativeColumnNumeric {
		for i := range frames {
			for j := range frames[i].Rows {
				r := &frames[i].Rows[j]
				v := c.u64()
				switch column {
				case 0:
					r.Owner = v
				case 1:
					r.Life = v
				case 2:
					r.Source = v
				case 3:
					r.Target = v
				case 4:
					r.SourceLife = v
				case 5:
					r.TargetLife = v
				case 6:
					r.Before.Handle = v
				case 7:
					r.Before.Value = int64(v)
				case 8:
					r.Before.Revision = v
				case 9:
					r.Before.Provenance = v
				case 10:
					r.After.Handle = v
				case 11:
					r.After.Value = int64(v)
				case 12:
					r.After.Revision = v
				case 13:
					r.After.Provenance = v
				} // #nosec G115 -- exact signed wire bit restoration.
			}
		}
	}
	for i := range frames {
		for j := range frames[i].Rows {
			r := &frames[i].Rows[j]
			r.Name = string(c.field(256))
			r.Type = string(c.field(256))
			r.Axis = owned(c.field(p.FrameBytes))
			r.Scope = owned(c.field(p.FrameBytes))
			if r.Owner == 0 || len(r.Scope) == 0 || len(r.Axis) == 0 {
				return nil, errCorrupt
			}
		}
	}
	if c.err != nil || len(c.b) != 0 {
		return nil, errors.Join(errCorrupt, c.err)
	}
	canonical, err := encodeNativeColumnChunk(g, ordinal, previous, frames, p)
	if err != nil || !bytes.Equal(canonical, wire) {
		return nil, errors.Join(errCorrupt, err)
	}
	return frames, nil
}
