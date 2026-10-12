package store

import (
	"math"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Valid-end index sentinels (backlog 51): ValidEndOpen is an end that is not
// recorded (an open segment), ValidEndNone an entity without a segment.
const (
	ValidEndOpen = types.Instant(math.MaxInt64)
	ValidEndNone = types.Instant(math.MinInt64)
)

// ValidEndCandidate is one entity a valid-end bounded effective scan reads
// (store.RelValidEndCapability): its ID (a types.RelID or types.NodeID value),
// the index's write generation for it, Stamp — every transaction stamp of its
// rows is at or below Stamp, so at a pin at or above Stamp its belief is the
// newest — and Bound, an upper bound on its effective valid end at such a pin
// (ValidEndOpen: unbounded). A candidate the index could not bound carries
// Stamp ValidEndOpen.
type ValidEndCandidate struct {
	ID           int64
	Gen          uint64
	Stamp, Bound types.Instant
}

// RelValidEndCapability is OPTIONAL (backlog 51): a RAM valid-end index over
// relationships, so a type scan bounded by valid end
// (temporal ForEachRelEffectiveByTypeFrom) reads the relationships still
// effective at or after a valid instant instead of the type's whole history.
//
// RelValidEndCandidates returns a SOUND SUPERSET of the relationships of type
// typeToken whose effective valid end at pin (the end of the last segment of
// RelEffectiveTimeline(id, pin); an open segment has no end) is open or at or
// after validFrom: every relationship that ever had the type and either an
// upper bound on its end at or after validFrom, or a row stamped after pin
// (its belief at pin may differ from the newest), or an index state the store
// must re-read. The bound folds every row's own ValidTo and every delete
// tombstone recorded after all the entity's earlier stamps (a delete ends
// every row recorded before it); it never models the chain resolver, which
// stays the authority: the caller reads each candidate's timeline and decides.
// The first call for a type tracks it (its ever-members from the K1
// membership sidecar, each read once: temporal fields only); from then on
// every row write of the type is folded, writes of untracked types change
// nothing; a removed or overwritten row moves its entity to a re-read on the
// next call. The order is unspecified.
//
// RefineRelValidEnd records end, the exact effective valid end of id read at a
// pin at or above the candidate's Stamp (ValidEndOpen when open, ValidEndNone
// without a segment), when the index still holds generation gen for id: no
// row of id was written since the candidate was listed. It lets the index drop
// an entity whose bound is loose (a valid-time close leaves the closed
// version's open predecessor in history) once its timeline was read; the next
// write to the entity restores the folded bound.
type RelValidEndCapability interface {
	RelValidEndCandidates(typeToken uint16, pin, validFrom types.Instant) ([]ValidEndCandidate, error)
	RefineRelValidEnd(id types.RelID, gen uint64, end types.Instant)
}

// NodeValidEndCapability is RelValidEndCapability for nodes by label: the
// candidates are the nodes that ever carried one of labelTokens (bounded by
// the node's whole timeline, not the labelled part).
type NodeValidEndCapability interface {
	NodeValidEndCandidates(labelTokens []uint16, pin, validFrom types.Instant) ([]ValidEndCandidate, error)
	RefineNodeValidEnd(id types.NodeID, gen uint64, end types.Instant)
}

// ValidEndStats describes one kind's valid-end index (a diagnostic): the
// type or label tokens it covers, the entities it bounds, those awaiting a
// re-read, its ordered entries, an estimate of its RAM, and Notes, the row
// writes that changed it since it was built.
type ValidEndStats struct {
	Tokens   int
	Entities int
	Unknown  int
	Entries  int
	Bytes    int
	Notes    uint64
}
