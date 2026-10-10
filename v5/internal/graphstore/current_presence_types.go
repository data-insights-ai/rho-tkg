package graphstore

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"math/big"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Private own-presence access format. This is neither a complete effective
// neighborhood nor a replacement for the retained raw incident postings.
// Qualified LifeBound lookup selects one bound-life run. All-life lookup visits
// the same runs; there are no duplicated unqualified postings. Type is resolved
// through the immutable relationship Entity only after own-presence pruning.
const currentPresenceRecord recordKind = 0x15
const currentPresenceHeaderBytes = 43
const (
	cpPoint byte = 1 << iota
	cpLowerInfinite
	cpUpperInfinite
	cpLowerClosed
	cpUpperClosed
	cpSource
	cpTarget
)

type currentPresenceAxis struct {
	id      temporal.AxisID
	hash    [32]byte
	profile temporal.Profile
}
type currentPresenceGroup struct {
	endpoint graphstate.EntityID
	bound    graphstate.LifeID
	axis     uint8
	mode     graphstate.ReferenceMode
}

// Numeric payloads share page-owned encoded backing. There is no per-row Axis,
// Position, big.Int or full posting union. Offset zero is a valid coordinate.
type currentPresenceRow struct {
	relationship graphstate.EntityID
	life         graphstate.LifeID
	lower, upper uint32
	group        uint16
	flags        byte
}
type currentPresenceFence struct {
	relationship graphstate.EntityID
	life         graphstate.LifeID
	lower        uint32
	group        uint16
	flags        byte // only lower infinity/inclusivity; point is inclusive
}
type currentPresenceExtent struct {
	offset uint32
	flags  byte
}
type currentPresenceChild struct {
	id, count   uint64
	digest      [32]byte
	first, last currentPresenceFence
	min, max    currentPresenceExtent
	axis        uint8 // dictionary slot when uniform; ignored when mixed
	mixed       bool
}
type currentPresencePage struct {
	id       uint64
	level    uint8
	axes     []currentPresenceAxis
	groups   []currentPresenceGroup
	rows     []currentPresenceRow
	children []currentPresenceChild
	wire     []byte
	digest   [32]byte
}
type currentPresenceLimits struct {
	temporal                                            temporal.Limits
	maxPageBytes, maxOwnedBytes, maxScratchBytes        int
	maxRows, maxGroups, maxAxes, maxChildren, maxLevels int
}
type currentPresenceUsage struct{ OwnedBytes, ScratchBytes int }

func defaultCurrentPresenceLimits() currentPresenceLimits {
	return currentPresenceLimits{temporal.DefaultLimits(), 256 << 10, 512 << 10, 1 << 20, 128, 128, 128, 64, 8}
}
func (l currentPresenceLimits) validate() error {
	if err := l.temporal.Validate(); err != nil {
		return errors.Join(ErrInvalid, err)
	}
	if l.maxPageBytes < currentPresenceHeaderBytes || l.maxPageBytes > 1<<20 || l.maxOwnedBytes < 1 || l.maxOwnedBytes > 4<<20 || l.maxScratchBytes < 1 || l.maxScratchBytes > 4<<20 || l.maxRows < 1 || l.maxRows > 128 || l.maxGroups < 1 || l.maxGroups > 128 || l.maxAxes < 1 || l.maxAxes > 128 || l.maxChildren < 2 || l.maxChildren > 64 || l.maxLevels < 1 || l.maxLevels > 8 {
		return ErrInvalid
	}
	return nil
}

// These are conservative concrete representation allowances, verified with
// unsafe.Sizeof in tests. wire capacity is charged in addition. Scratch is a
// reusable, separate allowance for temporary numeric validation/comparison;
// this ledger is not a Go allocator/RSS limit.
const (
	cpPageOwned  = 256
	cpAxisOwned  = 56
	cpGroupOwned = 24
	cpRowOwned   = 32
	cpChildOwned = 136
)

func cpOwnedBytes(wire, axes, groups, rows, children int) int {
	return cpPageOwned + wire + cpAxisOwned*axes + cpGroupOwned*groups + cpRowOwned*rows + cpChildOwned*children
}
func cpAxisCompare(a, b currentPresenceAxis) int {
	return cmp.Or(bytes.Compare(a.id[:], b.id[:]), bytes.Compare(a.hash[:], b.hash[:]), cmp.Compare(a.profile, b.profile))
}
func cpGroupCompare(a, b currentPresenceGroup, axes []currentPresenceAxis) int {
	return cmp.Or(cmp.Compare(a.endpoint, b.endpoint), cpAxisCompare(axes[a.axis], axes[b.axis]), cmp.Compare(a.mode, b.mode), cmp.Compare(a.bound, b.bound))
}

// A validated coordinate view owns nothing. Integers stay inline through 64
// magnitude bits; wider numbers are materialized only within bounded scratch.
type cpInteger struct {
	sign      byte
	magnitude []byte
}

func cpReadInteger(c *cursor, l temporal.Limits) (cpInteger, error) {
	h, err := c.take(3)
	if err != nil {
		return cpInteger{}, err
	}
	size := int(binary.BigEndian.Uint16(h[1:]))
	sign := h[0]
	b, err := c.take(size)
	if err != nil {
		return cpInteger{}, err
	}
	if sign > 2 || (size == 0) != (sign == 0) || size > 0 && b[0] == 0 {
		return cpInteger{}, ErrCorrupt
	}
	bits := 0
	if size > 0 {
		top := b[0]
		for top != 0 {
			bits++
			top >>= 1
		}
		bits += (size - 1) * 8
	}
	ceiling := cmp.Or(l.MaxMagnitudeBits, temporal.DefaultLimits().MaxMagnitudeBits)
	if bits > ceiling {
		return cpInteger{}, errors.Join(ErrResourceLimit, temporal.ErrResourceLimit)
	}
	return cpInteger{sign, b}, nil
}
func (n cpInteger) big() *big.Int {
	b := new(big.Int).SetBytes(n.magnitude)
	if n.sign == 2 {
		b.Neg(b)
	}
	return b
}
func cpCompareInteger(a, b cpInteger) int {
	sign := func(n cpInteger) int {
		if n.sign == 2 {
			return -1
		}
		if n.sign == 1 {
			return 1
		}
		return 0
	}
	if c := cmp.Compare(sign(a), sign(b)); c != 0 {
		return c
	}
	c := cmp.Or(cmp.Compare(len(a.magnitude), len(b.magnitude)), bytes.Compare(a.magnitude, b.magnitude))
	if a.sign == 2 {
		return -c
	}
	return c
}

type cpCoordinate struct {
	n, d, m cpInteger
	end     int
}

func cpReadCoordinate(wire []byte, offset uint32, profile temporal.Profile, l temporal.Limits) (cpCoordinate, error) {
	if uint64(offset) > uint64(len(wire)) {
		return cpCoordinate{}, ErrCorrupt
	}
	c := cursor{src: wire[offset:]}
	start := len(c.src)
	n, err := cpReadInteger(&c, l)
	if err != nil {
		return cpCoordinate{}, err
	}
	out := cpCoordinate{n: n}
	if profile != temporal.ProfileIntegerZ {
		if profile != temporal.ProfileRationalQ && profile != temporal.ProfileLexicographicQN {
			return cpCoordinate{}, ErrCorrupt
		}
		out.d, err = cpReadInteger(&c, l)
		if err != nil {
			return cpCoordinate{}, err
		}
		if out.d.sign != 1 {
			return cpCoordinate{}, ErrCorrupt
		}

	}
	if profile == temporal.ProfileLexicographicQN {
		out.m, err = cpReadInteger(&c, l)
		if err != nil {
			return cpCoordinate{}, err
		}
		if out.m.sign == 2 {
			return cpCoordinate{}, ErrCorrupt
		}
	}
	out.end = int(offset) + start - len(c.src)
	maxValue := cmp.Or(l.MaxValueBytes, temporal.DefaultLimits().MaxValueBytes)
	maxInput := cmp.Or(l.MaxInputBytes, temporal.DefaultLimits().MaxInputBytes)
	if out.end-int(offset)+52 > min(maxValue, maxInput) {
		return cpCoordinate{}, errors.Join(ErrResourceLimit, temporal.ErrResourceLimit)
	}
	if profile != temporal.ProfileIntegerZ {
		if out.n.sign == 0 {
			if len(out.d.magnitude) != 1 || out.d.magnitude[0] != 1 {
				return cpCoordinate{}, ErrCorrupt
			}
		} else {
			// Canonical reduced fractions are required, including wide operands.
			g := new(big.Int).GCD(nil, nil, out.n.big(), out.d.big())
			if g.Cmp(big.NewInt(1)) != 0 {
				return cpCoordinate{}, ErrCorrupt
			}
		}
	}
	return out, nil
}
func cpCompareCoordinate(a, b cpCoordinate, profile temporal.Profile) int {
	if profile == temporal.ProfileIntegerZ {
		return cpCompareInteger(a.n, b.n)
	}
	// Exact rational products; their double width is scratch, not a wider key.
	left := a.n.big()
	left.Mul(left, b.d.big())
	right := b.n.big()
	right.Mul(right, a.d.big())
	if c := left.Cmp(right); c != 0 || profile == temporal.ProfileRationalQ {
		return c
	}
	return cpCompareInteger(a.m, b.m)
}
func cpCoordinateAt(p currentPresencePage, group uint16, offset uint32, l currentPresenceLimits) (cpCoordinate, error) {
	if int(group) >= len(p.groups) || int(p.groups[group].axis) >= len(p.axes) {
		return cpCoordinate{}, ErrCorrupt
	}
	return cpReadCoordinate(p.wire, offset, p.axes[p.groups[group].axis].profile, l.temporal)
}
func cpCompareFence(a currentPresenceFence, pa currentPresencePage, b currentPresenceFence, pb currentPresencePage, l currentPresenceLimits) (int, error) {
	ga, gb := pa.groups[a.group], pb.groups[b.group]
	aa, ab := pa.axes[ga.axis], pb.axes[gb.axis]
	if c := cmp.Or(cmp.Compare(ga.endpoint, gb.endpoint), cpAxisCompare(aa, ab), cmp.Compare(ga.mode, gb.mode), cmp.Compare(ga.bound, gb.bound)); c != 0 {
		return c, nil
	}
	c, err := cpCompareBound(pa, a.lower, a.flags, pb, b.lower, b.flags, aa.profile, l)
	if err != nil {
		return 0, err
	}
	if c == 0 {
		c = cmp.Compare(b.flags&cpLowerClosed, a.flags&cpLowerClosed)
	}
	return cmp.Or(c, cmp.Compare(a.relationship, b.relationship), cmp.Compare(a.life, b.life)), nil
}

// Lower-bound ordering puts a closed lower endpoint before an open one at the
// same coordinate. Extrema comparisons distinguish inclusion at equality.
func cpCompareBound(a currentPresencePage, oa uint32, fa byte, b currentPresencePage, ob uint32, fb byte, profile temporal.Profile, l currentPresenceLimits) (int, error) {
	rank := func(flags byte) int {
		if flags&cpLowerInfinite != 0 {
			return -1
		}
		if flags&cpUpperInfinite != 0 {
			return 1
		}
		return 0
	}
	ra, rb := rank(fa), rank(fb)
	if ra != 0 || rb != 0 {
		return cmp.Compare(ra, rb), nil
	}
	ca, err := cpReadCoordinate(a.wire, oa, profile, l.temporal)
	if err != nil {
		return 0, err
	}
	cb, err := cpReadCoordinate(b.wire, ob, profile, l.temporal)
	if err != nil {
		return 0, err
	}
	if c := cpCompareCoordinate(ca, cb, profile); c != 0 {
		return c, nil
	}
	return 0, nil
}
func cpRowFence(r currentPresenceRow) currentPresenceFence {
	flags := r.flags & (cpLowerInfinite | cpLowerClosed)
	if r.flags&cpPoint != 0 {
		flags = cpLowerClosed
	}
	return currentPresenceFence{r.relationship, r.life, r.lower, r.group, flags}
}
func cpScratchBytes(p currentPresencePage, l currentPresenceLimits) (int, error) {
	// Preflight the largest encoded magnitude without materializing big.Int.
	// Repeated comparisons reuse this allowance; encoded backing remains owned.
	maximum := 0
	scan := func(offset uint32, axis uint8) error {
		if int(axis) >= len(p.axes) || uint64(offset) > uint64(len(p.wire)) {
			return ErrCorrupt
		}
		c := cursor{src: p.wire[offset:]}
		n := 1
		if p.axes[axis].profile == temporal.ProfileRationalQ {
			n = 2
		}
		if p.axes[axis].profile == temporal.ProfileLexicographicQN {
			n = 3
		}
		for range n {
			v, err := cpReadInteger(&c, l.temporal)
			if err != nil {
				return err
			}
			maximum = max(maximum, len(v.magnitude))
		}
		return nil
	}
	for _, r := range p.rows {
		if int(r.group) >= len(p.groups) {
			return 0, ErrCorrupt
		}
		axis := p.groups[r.group].axis
		if r.flags&cpLowerInfinite == 0 {
			if err := scan(r.lower, axis); err != nil {
				return 0, err
			}
		}
		if r.flags&(cpPoint|cpUpperInfinite) == 0 {
			if err := scan(r.upper, axis); err != nil {
				return 0, err
			}
		}
	}
	for _, ch := range p.children {
		for _, f := range []currentPresenceFence{ch.first, ch.last} {
			if int(f.group) >= len(p.groups) {
				return 0, ErrCorrupt
			}
			if f.flags&cpLowerInfinite == 0 {
				if err := scan(f.lower, p.groups[f.group].axis); err != nil {
					return 0, err
				}
			}
		}
		if !ch.mixed {
			for _, ex := range []currentPresenceExtent{ch.min, ch.max} {
				if ex.flags&(cpLowerInfinite|cpUpperInfinite) == 0 {
					if err := scan(ex.offset, ch.axis); err != nil {
						return 0, err
					}
				}
			}
		}
	}
	return 2048 + 64*((maximum+7)/8*8+32), nil
}
func cpCheckContext(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalid
	}
	return ctx.Err()
}
func cpCount(children []currentPresenceChild) (uint64, error) {
	var count uint64
	for _, c := range children {
		if c.count == 0 || c.count > math.MaxUint64-count {
			return 0, ErrCorrupt
		}
		count += c.count
	}
	return count, nil
}

// Canonical discrete atoms encode singletons as one coordinate. QxN has a
// successor only within one rational model coordinate; different rational
// coordinates must not be collapsed even when their microsteps are small.
func cpAdjacentCoordinates(a, b cpCoordinate, profile temporal.Profile) bool {
	if profile == temporal.ProfileLexicographicQN {
		if cpCompareInteger(a.n, b.n) != 0 || cpCompareInteger(a.d, b.d) != 0 {
			return false
		}
		a.n, b.n = a.m, b.m
	}
	next := a.n.big()
	next.Add(next, big.NewInt(1))
	return next.Cmp(b.n.big()) == 0
}
