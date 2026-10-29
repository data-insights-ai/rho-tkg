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

// PropertyIndexValidEnd lists the on-disk property index definitions whose
// entries carry an upper bound on their node's effective valid end
// (VALIDENDPRUNE): label Label, property keys starting with KeyPrefix ("" every
// key of the label).
type PropertyIndexValidEnd struct {
	Label     string
	KeyPrefix string
}

// ValidEndProbe is what a pruning lookup knew of one node it returned: Bound,
// the stored upper bound on its effective valid end when Known, Seq, the
// store's write sequence read before the node's row, and Src, the store's own
// routing tag. RefineNodePropertyValidEnd takes Seq and Src back.
type ValidEndProbe struct {
	Bound types.Instant
	Known bool
	Seq   uint64
	Src   uint32
}

// PropertyIndexValidEndStats counts a store's pruning since open: entries
// pruned without reading their row, entries without a bound whose row was read,
// bounds refined, and refines skipped because the node was written since.
type PropertyIndexValidEndStats struct {
	Pruned        uint64
	UnknownRead   uint64
	Refined       uint64
	RefineSkipped uint64
}

// PropertyIndexValidEndCapability is OPTIONAL (VALIDENDPRUNE): the pruning
// lookup behind QueryOpts.ValidEndAfter. NodesByLabelAndPropertyKeysValidEnd
// is NodesByLabelAndPropertyKeys (opts.ValidEndAfter set, no time filter)
// minus the nodes a listed definition's entries bound below the bound, with a
// probe per node returned. The caller decides each node from its timeline and
// hands back its exact end (ValidEndOpen, ValidEndNone) with
// RefineNodePropertyValidEnd, which stores it unless the node was written since
// Seq; a refine writes index entries only, no row, stamp or change record.
type PropertyIndexValidEndCapability interface {
	NodesByLabelAndPropertyKeysValidEnd(labelToken uint16, key string, valueKeys []string, opts QueryOpts) ([]*types.Node, []ValidEndProbe, error)
	RefineNodePropertyValidEnd(n *types.Node, probe ValidEndProbe, end types.Instant)
	PropertyIndexValidEndStats() PropertyIndexValidEndStats
}

// PropertyIndexValidEndListedCapability is OPTIONAL (VALIDENDLISTED): whether a
// ValidEndAfter lookup on the property index (labelToken, key) prunes from its
// entries without reading rows (the definition is listed, on disk and its
// bounds trusted). In memory, no I/O; false for every other definition. A
// create of the index can turn it true, a drop or Clear false.
type PropertyIndexValidEndListedCapability interface {
	PropertyIndexValidEndListed(labelToken uint16, key string) bool
}

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
