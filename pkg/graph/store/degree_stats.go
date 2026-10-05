package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// RelTypeDegreeStats describes how one relationship type's current
// relationships spread over their endpoints: the planner input for an
// adjacency or CSR cost estimate that a hub breaks when only the mean is
// known (mean out-degree = Rels / Starts).
//
// MaxOut is the largest number of the type's relationships starting at one
// node (MaxOutNode, the smallest such node ID); MaxIn the mirror for ends. A
// self-loop counts once on each side. All zero for a type with no
// relationships.
//
// Exact is true when the relationship mutation epoch the stats are keyed on
// did not move while they were counted, so they describe one state of the
// store. It is false when a writer kept moving it across the bounded retries,
// or when the store exposes no epoch to key on (then nothing is cached either).
type RelTypeDegreeStats struct {
	Rels       int64
	Starts     int64
	Ends       int64
	MaxOut     int64
	MaxIn      int64
	MaxOutNode types.NodeID
	MaxInNode  types.NodeID
	Exact      bool
}

// RelTypeDegreeCapability is OPTIONAL: a store that can count a type's
// degrees from its adjacency index without decoding relationship rows.
// typeToken 0 means every type. ok=false declines (the graph layer then
// streams the type's relationships). The counts must describe the store's
// current relationships; MaxOutNode / MaxInNode are the smallest node IDs
// holding the maxima; Exact is set by the caller.
type RelTypeDegreeCapability interface {
	RelTypeDegreeStats(typeToken uint16) (stats RelTypeDegreeStats, ok bool, err error)
}

// DegreeStatsFromCounts folds per-node out- and in-counts into the stats
// (Exact left false).
func DegreeStatsFromCounts(rels int64, out, in map[types.NodeID]int64) RelTypeDegreeStats {
	st := RelTypeDegreeStats{Rels: rels, Starts: int64(len(out)), Ends: int64(len(in))}
	st.MaxOut, st.MaxOutNode = maxDegree(out)
	st.MaxIn, st.MaxInNode = maxDegree(in)
	return st
}

func maxDegree(m map[types.NodeID]int64) (int64, types.NodeID) {
	var best int64
	var node types.NodeID
	for id, n := range m {
		if n > best || (n == best && id < node) {
			best, node = n, id
		}
	}
	return best, node
}
