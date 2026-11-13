package index

import (
	"math"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// DiskMember is one member's posting as the on-disk membership folds it from
// its span entries (RELDISKWIN, storeutil.EntityIndexRelSpan): one entry per
// row of the member that carries the value, each with the row's from, allTo,
// lateTo and last (the entry's value), so the posting the RAM sidecar keeps per
// (value, member) is rebuilt as
//
//	all  = union of the rows' [from, allTo)   (RowInfo.AnyPinSpan)
//	late = union of the rows' [from, lateTo)  (the stored end, or a replacing
//	       row's start: RowInfo.EndedBy), empty pieces left out as addLate does
//	last = the newest stamp of any row
//
// which are the sidecar's all, late (history rows and the current row's span
// together) and last, so Meets answers a window as MembersDuring does: a member
// written after the pin (last above it) by its any-pin envelope, the others by
// its late envelope. An entry without a span (a value that is not a span's)
// makes the member meet every window, a sound superset.
type DiskMember struct {
	p txPosting
}

// NewDiskMember returns a member with no entry folded.
func NewDiskMember() DiskMember { return DiskMember{p: newPosting()} }

// AddEntry folds one row's entry: tx the row's TxFrom (the key's), the span
// its value.
func (m *DiskMember) AddEntry(tx, from, allTo, lateTo, last types.Instant) {
	m.p.noteTx(tx, last)
	m.p.addAll(Span{From: from, To: allTo})
	m.p.addLate(Span{From: from, To: lateTo})
}

// AddWild folds an entry that carries no span: the member meets every window.
func (m *DiskMember) AddWild(tx types.Instant) {
	m.p.noteTx(tx, 0)
	m.p.flags |= spanWild
}

// FirstTx returns the lowest TxFrom of the entries folded (MergeFirstTx).
func (m DiskMember) FirstTx() types.Instant { return m.p.tx }

// Meets reports whether the member's rows may be valid somewhere in [start,
// end) at transaction time pin (0, or math.MaxInt64: no pin, every row).
func (m DiskMember) Meets(start, end, pin types.Instant) bool {
	var sp Span
	var ok bool
	if pin != 0 && pin < math.MaxInt64 && m.p.last > pin {
		sp, ok = m.p.envAll()
	} else {
		sp, ok = m.p.envLate()
	}
	return ok && sp.meets(start, end)
}
