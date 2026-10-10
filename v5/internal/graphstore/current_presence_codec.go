package graphstore

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func cpValidGroup(g currentPresenceGroup, p currentPresencePage) bool {
	return g.endpoint != 0 && int(g.axis) < len(p.axes) && (g.mode == graphstate.LifeBound && g.bound != 0 || g.mode == graphstate.IdentityReference && g.bound == 0)
}
func cpValidFence(f currentPresenceFence, p currentPresencePage) bool {
	return f.relationship != 0 && f.life != 0 && int(f.group) < len(p.groups) && f.flags & ^(cpLowerInfinite|cpLowerClosed) == 0 && f.flags&(cpLowerInfinite|cpLowerClosed) != (cpLowerInfinite|cpLowerClosed)
}
func cpValidExtent(e currentPresenceExtent, lower bool) bool {
	infinite := cpUpperInfinite
	if lower {
		infinite = cpLowerInfinite
	}
	return e.flags == 0 || e.flags == cpLowerClosed || e.flags == infinite
}
func cpValidatePage(ctx context.Context, p currentPresencePage, l currentPresenceLimits) (currentPresenceUsage, error) {
	if err := cpCheckContext(ctx); err != nil {
		return currentPresenceUsage{}, err
	}
	if err := l.validate(); err != nil {
		return currentPresenceUsage{}, err
	}
	if p.id == 0 || p.level >= 8 || len(p.axes) > 128 || len(p.groups) > 128 || len(p.rows) > 128 || len(p.children) > 64 || p.level == 0 && len(p.children) != 0 || p.level > 0 && (len(p.rows) != 0 || len(p.children) < 2) {
		return currentPresenceUsage{}, ErrCorrupt
	}
	if int(p.level) >= l.maxLevels || len(p.axes) > l.maxAxes || len(p.groups) > l.maxGroups || len(p.rows) > l.maxRows || len(p.children) > l.maxChildren {
		return currentPresenceUsage{}, ErrResourceLimit
	}
	usage := currentPresenceUsage{OwnedBytes: cpOwnedBytes(cap(p.wire), cap(p.axes), cap(p.groups), cap(p.rows), cap(p.children))}
	if usage.OwnedBytes > l.maxOwnedBytes {
		return currentPresenceUsage{}, ErrResourceLimit
	}
	for i, a := range p.axes {
		if a.id == (temporal.AxisID{}) || a.hash == ([32]byte{}) || a.profile < temporal.ProfileIntegerZ || a.profile > temporal.ProfileLexicographicQN || i > 0 && (cpAxisCompare(p.axes[i-1], a) >= 0 || p.axes[i-1].id == a.id) {
			return currentPresenceUsage{}, ErrCorrupt
		}
	}
	for i, g := range p.groups {
		if !cpValidGroup(g, p) || i > 0 && cpGroupCompare(p.groups[i-1], g, p.axes) >= 0 {
			return currentPresenceUsage{}, ErrCorrupt
		}
	}
	scratch, err := cpScratchBytes(p, l)
	if err != nil {
		return currentPresenceUsage{}, err
	}
	if scratch > l.maxScratchBytes {
		return currentPresenceUsage{}, ErrResourceLimit
	}
	usage.ScratchBytes = scratch
	usedAxes := [128]bool{}
	usedGroups := [128]bool{}
	mark := func(g uint16) { usedGroups[g] = true; usedAxes[p.groups[g].axis] = true }
	for i, r := range p.rows {
		if err := ctx.Err(); err != nil {
			return currentPresenceUsage{}, err
		}
		if r.relationship == 0 || r.life == 0 || int(r.group) >= len(p.groups) || r.flags&128 != 0 || r.flags&(cpSource|cpTarget) == 0 {
			return currentPresenceUsage{}, ErrCorrupt
		}
		mark(r.group)
		if r.flags&cpPoint != 0 {
			if r.flags & ^(cpPoint|cpSource|cpTarget) != 0 || r.upper != 0 {
				return currentPresenceUsage{}, ErrCorrupt
			}
		} else {
			if r.flags&cpLowerInfinite != 0 && r.flags&cpLowerClosed != 0 || r.flags&cpUpperInfinite != 0 && r.flags&cpUpperClosed != 0 {
				return currentPresenceUsage{}, ErrCorrupt
			}
		}
		axis := p.axes[p.groups[r.group].axis].profile
		if r.flags&cpLowerInfinite != 0 {
			if r.lower != 0 {
				return currentPresenceUsage{}, ErrCorrupt
			}
		} else {
			if _, err := cpReadCoordinate(p.wire, r.lower, axis, l.temporal); err != nil {
				return currentPresenceUsage{}, err
			}
		}
		if r.flags&cpPoint == 0 {
			if r.flags&cpUpperInfinite != 0 {
				if r.upper != 0 {
					return currentPresenceUsage{}, ErrCorrupt
				}
			} else {
				if _, err := cpReadCoordinate(p.wire, r.upper, axis, l.temporal); err != nil {
					return currentPresenceUsage{}, err
				}
			}
			c, err := cpCompareBound(p, r.lower, r.flags&cpLowerInfinite, p, r.upper, r.flags&cpUpperInfinite, axis, l)
			if err != nil {
				return currentPresenceUsage{}, err
			}
			if c >= 0 {
				return currentPresenceUsage{}, ErrCorrupt
			}

			if axis != temporal.ProfileRationalQ {
				if r.flags&cpLowerInfinite == 0 && r.flags&cpLowerClosed == 0 || r.flags&cpUpperInfinite == 0 && r.flags&cpUpperClosed != 0 {
					return currentPresenceUsage{}, ErrCorrupt
				}
				if r.flags&(cpLowerInfinite|cpUpperInfinite) == 0 {
					lo, e := cpReadCoordinate(p.wire, r.lower, axis, l.temporal)
					if e != nil {
						return currentPresenceUsage{}, e
					}
					hi, e := cpReadCoordinate(p.wire, r.upper, axis, l.temporal)
					if e != nil {
						return currentPresenceUsage{}, e
					}
					if cpAdjacentCoordinates(lo, hi, axis) {
						return currentPresenceUsage{}, ErrCorrupt
					}
				}
			}
		}
		if i > 0 {
			c, err := cpCompareFence(cpRowFence(p.rows[i-1]), p, cpRowFence(r), p, l)
			if err != nil {
				return currentPresenceUsage{}, err
			}
			if c >= 0 {
				return currentPresenceUsage{}, ErrCorrupt
			}
		}
	}
	if p.level > 0 {
		if _, err := cpCount(p.children); err != nil {
			return currentPresenceUsage{}, err
		}
		for i, ch := range p.children {
			if err := ctx.Err(); err != nil {
				return currentPresenceUsage{}, err
			}
			if ch.id == 0 || ch.id == p.id || ch.digest == ([32]byte{}) || !cpValidFence(ch.first, p) || !cpValidFence(ch.last, p) {
				return currentPresenceUsage{}, ErrCorrupt
			}
			for j := range i {
				if p.children[j].id == ch.id {
					return currentPresenceUsage{}, ErrCorrupt
				}
			}
			mark(ch.first.group)
			mark(ch.last.group)
			c, err := cpCompareFence(ch.first, p, ch.last, p, l)
			if err != nil {
				return currentPresenceUsage{}, err
			}
			if c > 0 {
				return currentPresenceUsage{}, ErrCorrupt
			}
			if i > 0 {
				c, err := cpCompareFence(p.children[i-1].last, p, ch.first, p, l)
				if err != nil {
					return currentPresenceUsage{}, err
				}
				if c >= 0 {
					return currentPresenceUsage{}, ErrCorrupt
				}
			}
			for _, f := range []currentPresenceFence{ch.first, ch.last} {
				if f.flags&cpLowerInfinite != 0 {
					if f.lower != 0 {
						return currentPresenceUsage{}, ErrCorrupt
					}
				} else {
					if _, err := cpCoordinateAt(p, f.group, f.lower, l); err != nil {
						return currentPresenceUsage{}, err
					}
				}
			}
			if ch.mixed {
				if ch.axis != 0 || ch.min != (currentPresenceExtent{}) || ch.max != (currentPresenceExtent{}) {
					return currentPresenceUsage{}, ErrCorrupt
				}
				continue
			}
			if int(ch.axis) >= len(p.axes) || !cpValidExtent(ch.min, true) || !cpValidExtent(ch.max, false) || p.groups[ch.first.group].axis != ch.axis || p.groups[ch.last.group].axis != ch.axis {
				return currentPresenceUsage{}, ErrCorrupt
			}
			usedAxes[ch.axis] = true
			profile := p.axes[ch.axis].profile
			for _, e := range []currentPresenceExtent{ch.min, ch.max} {
				if e.flags&(cpLowerInfinite|cpUpperInfinite) != 0 {
					if e.offset != 0 {
						return currentPresenceUsage{}, ErrCorrupt
					}
				} else {
					if _, err := cpReadCoordinate(p.wire, e.offset, profile, l.temporal); err != nil {
						return currentPresenceUsage{}, err
					}
				}
			}
			c, err = cpCompareBound(p, ch.min.offset, ch.min.flags, p, ch.max.offset, ch.max.flags, profile, l)
			if err != nil {
				return currentPresenceUsage{}, err
			}
			if c > 0 || c == 0 && (ch.min.flags&cpLowerClosed == 0 || ch.max.flags&cpLowerClosed == 0) {
				return currentPresenceUsage{}, ErrCorrupt
			}
			for _, f := range []currentPresenceFence{ch.first, ch.last} {
				c, err = cpCompareBound(p, ch.min.offset, ch.min.flags, p, f.lower, f.flags, profile, l)
				if err != nil {
					return currentPresenceUsage{}, err
				}
				if c > 0 {
					return currentPresenceUsage{}, ErrCorrupt
				}
				c, err = cpCompareBound(p, ch.max.offset, ch.max.flags, p, f.lower, f.flags, profile, l)
				if err != nil {
					return currentPresenceUsage{}, err
				}
				if c < 0 {
					return currentPresenceUsage{}, ErrCorrupt
				}
			}
		}
	}
	for i := range p.axes {
		if !usedAxes[i] {
			return currentPresenceUsage{}, ErrCorrupt
		}
	}
	for i := range p.groups {
		if !usedGroups[i] {
			return currentPresenceUsage{}, ErrCorrupt
		}
	}
	if err := ctx.Err(); err != nil {
		return currentPresenceUsage{}, err
	}
	return usage, nil
}

func cpAppendCoordinate(dst []byte, p currentPresencePage, offset uint32, axis uint8, l currentPresenceLimits) ([]byte, error) {
	coord, err := cpReadCoordinate(p.wire, offset, p.axes[axis].profile, l.temporal)
	if err != nil {
		return nil, err
	}
	return append(dst, p.wire[int(offset):coord.end]...), nil
}
func cpAppendFence(dst []byte, f currentPresenceFence, p currentPresencePage, l currentPresenceLimits) ([]byte, error) {
	dst = binary.BigEndian.AppendUint16(dst, f.group)
	dst = append(dst, f.flags)
	var err error
	if f.flags&cpLowerInfinite == 0 {
		dst, err = cpAppendCoordinate(dst, p, f.lower, p.groups[f.group].axis, l)
		if err != nil {
			return nil, err
		}
	}
	dst = binary.BigEndian.AppendUint64(dst, uint64(f.relationship))
	return binary.BigEndian.AppendUint64(dst, uint64(f.life)), nil
}
func cpAppendExtent(dst []byte, e currentPresenceExtent, p currentPresencePage, axis uint8, l currentPresenceLimits) ([]byte, error) {
	dst = append(dst, e.flags)
	if e.flags&(cpLowerInfinite|cpUpperInfinite) == 0 {
		return cpAppendCoordinate(dst, p, e.offset, axis, l)
	}
	return dst, nil
}
func encodeCurrentPresencePage(ctx context.Context, n Namespace, p currentPresencePage, l currentPresenceLimits) ([]byte, currentPresenceUsage, error) {
	if err := n.validate(); err != nil {
		return nil, currentPresenceUsage{}, err
	}
	usage, err := cpValidatePage(ctx, p, l)
	if err != nil {
		return nil, currentPresenceUsage{}, err
	}
	// Encode into private bounded storage. Counts and coordinate lengths are
	// preflighted before allocation, including the simultaneously retained input.
	size := currentPresenceHeaderBytes + 49*len(p.axes)
	for i := 0; i < len(p.groups); {
		end := i + 1
		for end < len(p.groups) && p.groups[end].endpoint == p.groups[i].endpoint && p.groups[end].axis == p.groups[i].axis && p.groups[end].mode == p.groups[i].mode {
			end++
		}
		size += 12
		for j := i; j < end; j++ {
			size += 2
			if p.groups[j].mode == graphstate.LifeBound {
				size += 8
			}
		}
		i = end
	}
	coordSize := func(offset uint32, axis uint8) int {
		v, _ := cpReadCoordinate(p.wire, offset, p.axes[axis].profile, l.temporal)
		return v.end - int(offset)
	}
	fenceSize := func(f currentPresenceFence) int {
		s := 19
		if f.flags&cpLowerInfinite == 0 {
			s += coordSize(f.lower, p.groups[f.group].axis)
		}
		return s
	}
	for _, r := range p.rows {
		size += 17
		if r.flags&cpLowerInfinite == 0 {
			size += coordSize(r.lower, p.groups[r.group].axis)
		}
		if r.flags&(cpPoint|cpUpperInfinite) == 0 {
			size += coordSize(r.upper, p.groups[r.group].axis)
		}
	}
	for _, c := range p.children {
		size += 49 + fenceSize(c.first) + fenceSize(c.last)
		if !c.mixed {
			size += 3
			for _, e := range []currentPresenceExtent{c.min, c.max} {
				if e.flags&(cpLowerInfinite|cpUpperInfinite) == 0 {
					size += coordSize(e.offset, c.axis)
				}
			}
		}
	}
	if size > l.maxPageBytes || size > l.maxOwnedBytes-usage.OwnedBytes {
		return nil, currentPresenceUsage{}, ErrResourceLimit
	}
	if err := ctx.Err(); err != nil {
		return nil, currentPresenceUsage{}, err
	}
	b := make([]byte, 0, size)
	b = append(b, recordHeader(n, currentPresenceRecord)...)
	b = binary.BigEndian.AppendUint64(b, p.id)
	b = append(b, 1, p.level)
	count := len(p.rows)
	if p.level > 0 {
		count = len(p.children)
	}
	b = binary.BigEndian.AppendUint16(b, uint16(count))
	b = append(b, byte(len(p.axes)))
	b = binary.BigEndian.AppendUint16(b, uint16(len(p.groups)))
	for _, a := range p.axes {
		b = append(b, a.id[:]...)
		b = append(b, a.hash[:]...)
		b = append(b, byte(a.profile))
	}
	for i := 0; i < len(p.groups); {
		g := p.groups[i]
		end := i + 1
		for end < len(p.groups) && p.groups[end].endpoint == g.endpoint && p.groups[end].axis == g.axis && p.groups[end].mode == g.mode {
			end++
		}
		b = binary.BigEndian.AppendUint64(b, uint64(g.endpoint))
		b = append(b, g.axis, byte(g.mode))
		b = binary.BigEndian.AppendUint16(b, uint16(end-i))
		for j := i; j < end; j++ {
			g = p.groups[j]
			if g.mode == graphstate.LifeBound {
				b = binary.BigEndian.AppendUint64(b, uint64(g.bound))
			}
			rows := 0
			for _, r := range p.rows {
				if int(r.group) == j {
					rows++
				}
			}
			b = binary.BigEndian.AppendUint16(b, uint16(rows))
		}
		i = end
	}
	for _, r := range p.rows {
		b = binary.BigEndian.AppendUint64(b, uint64(r.relationship))
		b = binary.BigEndian.AppendUint64(b, uint64(r.life))
		b = append(b, r.flags)
		if r.flags&cpLowerInfinite == 0 {
			b, _ = cpAppendCoordinate(b, p, r.lower, p.groups[r.group].axis, l)
		}
		if r.flags&(cpPoint|cpUpperInfinite) == 0 {
			b, _ = cpAppendCoordinate(b, p, r.upper, p.groups[r.group].axis, l)
		}
	}
	for _, ch := range p.children {
		b = binary.BigEndian.AppendUint64(b, ch.id)
		b = binary.BigEndian.AppendUint64(b, ch.count)
		b = append(b, ch.digest[:]...)
		b, _ = cpAppendFence(b, ch.first, p, l)
		b, _ = cpAppendFence(b, ch.last, p, l)
		if ch.mixed {
			b = append(b, 0)
		} else {
			b = append(b, 1, ch.axis)
			b, _ = cpAppendExtent(b, ch.min, p, ch.axis, l)
			b, _ = cpAppendExtent(b, ch.max, p, ch.axis, l)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, currentPresenceUsage{}, err
	}
	usage.OwnedBytes += cap(b)
	return b, usage, nil
} // #nosec G115 -- all counts are bounded by128; byte levels and dictionary slots by8/128.

func currentPresenceDigest(wire []byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("rho-tkg:own-presence-page:v1\x00"))
	_, _ = h.Write(wire)
	var out [32]byte
	h.Sum(out[:0])
	return out
}

// An expected digest must originate in a trusted parent/root descriptor. A
// self-consistent page is insufficient authority for a pruning summary.
func decodeCurrentPresencePage(ctx context.Context, n Namespace, id uint64, expected [32]byte, wire []byte, l currentPresenceLimits) (currentPresencePage, currentPresenceUsage, error) {
	fail := func(err error) (currentPresencePage, currentPresenceUsage, error) {
		return currentPresencePage{}, currentPresenceUsage{}, err
	}
	if err := cpCheckContext(ctx); err != nil {
		return fail(err)
	}
	if err := l.validate(); err != nil {
		return fail(err)
	}
	if err := n.validate(); err != nil {
		return fail(err)
	}
	if id == 0 || expected == ([32]byte{}) {
		return fail(ErrInvalid)
	}
	if len(wire) > l.maxPageBytes {
		return fail(ErrResourceLimit)
	}
	if len(wire) < currentPresenceHeaderBytes || !bytes.Equal(wire[:28], recordHeader(n, currentPresenceRecord)) || binary.BigEndian.Uint64(wire[28:36]) != id || wire[36] != 1 || wire[37] >= 8 || currentPresenceDigest(wire) != expected {
		return fail(ErrCorrupt)
	}
	count := int(binary.BigEndian.Uint16(wire[38:40]))
	axes := int(wire[40])
	groups := int(binary.BigEndian.Uint16(wire[41:43]))
	level := wire[37]
	if count > 128 || axes > 128 || groups > 128 || level > 0 && (count < 2 || count > 64) || axes*49+groups*2 > len(wire)-currentPresenceHeaderBytes {
		return fail(ErrCorrupt)
	}
	rows, children := count, 0
	if level > 0 {
		rows, children = 0, count
	}
	owned := cpOwnedBytes(len(wire), axes, groups, rows, children)
	if axes > l.maxAxes || groups > l.maxGroups || rows > l.maxRows || children > l.maxChildren || int(level) >= l.maxLevels || owned > l.maxOwnedBytes || l.maxScratchBytes < 4096 {
		return fail(ErrResourceLimit)
	}
	p := currentPresencePage{id: id, level: level, axes: make([]currentPresenceAxis, axes), groups: make([]currentPresenceGroup, groups), rows: make([]currentPresenceRow, rows), children: make([]currentPresenceChild, children), wire: wire, digest: expected}
	c := cursor{src: wire[currentPresenceHeaderBytes:]}
	for i := range axes {
		b, err := c.take(49)
		if err != nil {
			return fail(err)
		}
		copy(p.axes[i].id[:], b[:16])
		copy(p.axes[i].hash[:], b[16:48])
		p.axes[i].profile = temporal.Profile(b[48])
	}
	rowCounts := [128]int{}
	commonPrevious := currentPresenceGroup{}
	haveCommon := false
	for at := 0; at < groups; {
		h, err := c.take(12)
		if err != nil {
			return fail(err)
		}
		g := currentPresenceGroup{endpoint: graphstate.EntityID(binary.BigEndian.Uint64(h)), axis: h[8], mode: graphstate.ReferenceMode(h[9])}
		runs := int(binary.BigEndian.Uint16(h[10:]))
		if runs == 0 || runs > groups-at || (g.endpoint == 0 || int(g.axis) >= axes || g.mode != graphstate.LifeBound && g.mode != graphstate.IdentityReference || g.mode == graphstate.IdentityReference && runs != 1) {
			return fail(ErrCorrupt)
		}
		if haveCommon && commonPrevious.endpoint == g.endpoint && commonPrevious.axis == g.axis && commonPrevious.mode == g.mode {
			return fail(ErrCorrupt)
		}
		commonPrevious = g
		haveCommon = true
		for range runs {
			run := g
			if g.mode == graphstate.LifeBound {
				v, e := c.number()
				if e != nil {
					return fail(e)
				}
				run.bound = graphstate.LifeID(v)
			}
			b, e := c.take(2)
			if e != nil {
				return fail(e)
			}
			rowCounts[at] = int(binary.BigEndian.Uint16(b))
			p.groups[at] = run
			at++
		}
	}
	takeCoord := func(axis uint8) (uint32, error) {
		if int(axis) >= axes {
			return 0, ErrCorrupt
		}
		offset := len(wire) - len(c.src)
		start := len(c.src)
		parts := 1
		profile := p.axes[axis].profile
		if profile == temporal.ProfileRationalQ {
			parts = 2
		} else if profile == temporal.ProfileLexicographicQN {
			parts = 3
		} else if profile != temporal.ProfileIntegerZ {
			return 0, ErrCorrupt
		}
		for range parts {
			if _, err := cpReadInteger(&c, l.temporal); err != nil {
				return 0, err
			}
		}
		if start == len(c.src) {
			return 0, ErrCorrupt
		}
		return uint32(offset), nil
	}
	total := 0
	for g, nrows := range rowCounts[:groups] {
		if level > 0 && nrows != 0 || level == 0 && nrows == 0 || nrows > rows-total {
			return fail(ErrCorrupt)
		}
		for range nrows {
			b, err := c.take(17)
			if err != nil {
				return fail(err)
			}
			r := currentPresenceRow{relationship: graphstate.EntityID(binary.BigEndian.Uint64(b)), life: graphstate.LifeID(binary.BigEndian.Uint64(b[8:])), group: uint16(g), flags: b[16]}
			if r.flags&cpLowerInfinite == 0 {
				r.lower, err = takeCoord(p.groups[g].axis)
				if err != nil {
					return fail(err)
				}
			}
			if r.flags&(cpPoint|cpUpperInfinite) == 0 {
				r.upper, err = takeCoord(p.groups[g].axis)
				if err != nil {
					return fail(err)
				}
			}
			p.rows[total] = r
			total++
		}
	}
	if level == 0 && total != rows {
		return fail(ErrCorrupt)
	}
	takeFence := func() (currentPresenceFence, error) {
		b, e := c.take(3)
		if e != nil {
			return currentPresenceFence{}, e
		}
		f := currentPresenceFence{group: binary.BigEndian.Uint16(b), flags: b[2]}
		if int(f.group) >= groups {
			return currentPresenceFence{}, ErrCorrupt
		}
		if f.flags&cpLowerInfinite == 0 {
			f.lower, e = takeCoord(p.groups[f.group].axis)
			if e != nil {
				return currentPresenceFence{}, e
			}
		}
		b, e = c.take(16)
		if e != nil {
			return currentPresenceFence{}, e
		}
		f.relationship = graphstate.EntityID(binary.BigEndian.Uint64(b))
		f.life = graphstate.LifeID(binary.BigEndian.Uint64(b[8:]))
		return f, nil
	}
	for i := range children {
		b, e := c.take(48)
		if e != nil {
			return fail(e)
		}
		ch := currentPresenceChild{id: binary.BigEndian.Uint64(b), count: binary.BigEndian.Uint64(b[8:])}
		copy(ch.digest[:], b[16:])
		ch.first, e = takeFence()
		if e != nil {
			return fail(e)
		}
		ch.last, e = takeFence()
		if e != nil {
			return fail(e)
		}
		tag, e := c.tag()
		if e != nil || tag > 1 {
			return fail(ErrCorrupt)
		}
		ch.mixed = tag == 0
		if !ch.mixed {
			ch.axis, e = c.tag()
			if e != nil {
				return fail(e)
			}
			for _, ex := range []*currentPresenceExtent{&ch.min, &ch.max} {
				ex.flags, e = c.tag()
				if e != nil {
					return fail(e)
				}
				if ex.flags&(cpLowerInfinite|cpUpperInfinite) == 0 {
					ex.offset, e = takeCoord(ch.axis)
					if e != nil {
						return fail(e)
					}
				}
			}
		}
		p.children[i] = ch
	}
	if c.done() != nil {
		return fail(ErrCorrupt)
	}
	// The borrowed source's spare capacity is not retained: account exact owned
	// bytes, validate all numeric scratch before making the ownership copy.
	p.wire = p.wire[:len(p.wire):len(p.wire)]
	usage, err := cpValidatePage(ctx, p, l)
	if err != nil {
		return fail(err)
	}
	ownedWire := make([]byte, len(p.wire))
	copy(ownedWire, p.wire)
	p.wire = ownedWire
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	return p, usage, nil
} // #nosec G115 -- delivered node<=1MiB, groups/counts<=128 before narrowing.

// Summaries are authenticated by the containing page digest. Uniform extrema
// cover every endpoint/life group on that axis; key fences alone cannot do so.
func (p currentPresencePage) summary(l currentPresenceLimits) (currentPresenceChild, error) {
	s := currentPresenceChild{id: p.id, digest: p.digest}
	if p.level == 0 {
		s.count = uint64(len(p.rows))
		if len(p.rows) == 0 {
			s.mixed = true
			return s, nil
		}
		s.first = cpRowFence(p.rows[0])
		s.last = cpRowFence(p.rows[len(p.rows)-1])
		s.axis = p.groups[p.rows[0].group].axis
		for i, r := range p.rows {
			if p.groups[r.group].axis != s.axis {
				s.mixed = true
				break
			}
			lo := currentPresenceExtent{r.lower, r.flags & (cpLowerInfinite | cpLowerClosed)}
			hi := currentPresenceExtent{r.upper, r.flags & (cpUpperInfinite)}
			if r.flags&cpUpperClosed != 0 {
				hi.flags |= cpLowerClosed
			}
			if r.flags&cpPoint != 0 {
				lo.flags = cpLowerClosed
				hi = currentPresenceExtent{r.lower, cpLowerClosed}
			}
			if i == 0 {
				s.min, s.max = lo, hi
			} else {
				var err error
				s.min, err = cpMergeExtent(p, s.min, lo, true, s.axis, l)
				if err != nil {
					return currentPresenceChild{}, err
				}
				s.max, err = cpMergeExtent(p, s.max, hi, false, s.axis, l)
				if err != nil {
					return currentPresenceChild{}, err
				}
			}
		}
	} else {
		var err error
		s.count, err = cpCount(p.children)
		if err != nil {
			return currentPresenceChild{}, err
		}
		s.first = p.children[0].first
		s.last = p.children[len(p.children)-1].last
		s.axis = p.children[0].axis
		for i, ch := range p.children {
			if ch.mixed || ch.axis != s.axis {
				s.mixed = true
				break
			}
			if i == 0 {
				s.min, s.max = ch.min, ch.max
			} else {
				s.min, err = cpMergeExtent(p, s.min, ch.min, true, s.axis, l)
				if err != nil {
					return currentPresenceChild{}, err
				}
				s.max, err = cpMergeExtent(p, s.max, ch.max, false, s.axis, l)
				if err != nil {
					return currentPresenceChild{}, err
				}
			}
		}
	}
	if s.mixed {
		s.axis = 0
		s.min = currentPresenceExtent{}
		s.max = currentPresenceExtent{}
	}
	return s, nil
}
func cpMergeExtent(p currentPresencePage, a, b currentPresenceExtent, lower bool, axis uint8, l currentPresenceLimits) (currentPresenceExtent, error) {
	c, err := cpCompareBound(p, a.offset, a.flags, p, b.offset, b.flags, p.axes[axis].profile, l)
	if err != nil {
		return currentPresenceExtent{}, err
	}
	if lower && c > 0 || !lower && c < 0 {
		return b, nil
	}
	if c == 0 {
		a.flags |= b.flags & cpLowerClosed
	}
	return a, nil
}
func verifyCurrentPresenceChild(parent currentPresencePage, expected currentPresenceChild, child currentPresencePage, l currentPresenceLimits) error {
	actual, err := child.summary(l)
	if err != nil {
		return err
	}
	if parent.level != child.level+1 || expected.id != actual.id || expected.count != actual.count || expected.digest != actual.digest || expected.mixed != actual.mixed {
		return ErrCorrupt
	}
	for _, pair := range [][2]currentPresenceFence{{expected.first, actual.first}, {expected.last, actual.last}} {
		c, err := cpCompareFence(pair[0], parent, pair[1], child, l)
		if err != nil {
			return err
		}
		if c != 0 || pair[0].flags != pair[1].flags {
			return ErrCorrupt
		}
	}
	if !expected.mixed {
		if cpAxisCompare(parent.axes[expected.axis], child.axes[actual.axis]) != 0 {
			return ErrCorrupt
		}
		for _, pair := range [][2]currentPresenceExtent{{expected.min, actual.min}, {expected.max, actual.max}} {
			c, err := cpCompareBound(parent, pair[0].offset, pair[0].flags, child, pair[1].offset, pair[1].flags, parent.axes[expected.axis].profile, l)
			if err != nil {
				return err
			}
			if c != 0 || pair[0].flags != pair[1].flags {
				return ErrCorrupt
			}
		}
	}
	return nil
}

// Only a verified parent summary can be supplied here. Mixed-axis children
// always decline numeric pruning: never compare across reference systems.
func cpSummaryMayContain(p currentPresencePage, s currentPresenceChild, axis currentPresenceAxis, coordinate []byte, l currentPresenceLimits) (bool, error) {
	if err := l.validate(); err != nil {
		return false, err
	}
	if axis.id == (temporal.AxisID{}) || axis.hash == ([32]byte{}) || axis.profile < temporal.ProfileIntegerZ || axis.profile > temporal.ProfileLexicographicQN {
		return false, ErrInvalid
	}
	// Preflight borrowed query bytes and numeric scratch before reduction or
	// comparison. The coordinate stays borrowed; only scratch is materialized.
	maxInput := min(cmp.Or(l.temporal.MaxInputBytes, temporal.DefaultLimits().MaxInputBytes), cmp.Or(l.temporal.MaxValueBytes, temporal.DefaultLimits().MaxValueBytes))
	if len(coordinate) > maxInput-52 {
		return false, ErrResourceLimit
	}
	scratch := 2048 + 64*(len(coordinate)+32)
	pageScratch, err := cpScratchBytes(p, l)
	if err != nil {
		return false, err
	}
	if max(scratch, pageScratch) > l.maxScratchBytes {
		return false, ErrResourceLimit
	}
	q, err := cpReadCoordinate(coordinate, 0, axis.profile, l.temporal)
	if err != nil {
		return false, err
	}
	if q.end != len(coordinate) {
		return false, ErrCorrupt
	}
	if s.count == 0 {
		return false, nil
	}
	if s.mixed {
		return true, nil
	}
	if int(s.axis) >= len(p.axes) {
		return false, ErrCorrupt
	}
	if cpAxisCompare(p.axes[s.axis], axis) != 0 {
		return false, nil
	}
	query := currentPresencePage{wire: coordinate}
	for i, e := range []currentPresenceExtent{s.min, s.max} {
		c, err := cpCompareBound(p, e.offset, e.flags, query, 0, 0, axis.profile, l)
		if err != nil {
			return false, err
		}
		if i == 0 && (c > 0 || c == 0 && e.flags&cpLowerClosed == 0) || i == 1 && (c < 0 || c == 0 && e.flags&cpLowerClosed == 0) {
			return false, nil
		}
	}
	return true, nil
}
