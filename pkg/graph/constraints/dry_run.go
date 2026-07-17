package constraints

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// Dry-run constraint validation types (sigma HP2.5). A proposed fact set is
// validated against the configured unique + temporal constraints and the
// violations reported WITHOUT asserting anything. See g.Constraints().DryRunValidate.

// DryRunNode is one proposed node fact to validate against unique constraints.
type DryRunNode struct {
	// Ref is the caller's tag for this fact, echoed in any violation and used for
	// intra-set duplicate detection. Optional.
	Ref string
	// ID is optional: a NON-ZERO ID validates a proposed UPDATE to an existing
	// node (its own held value is not a self-violation) AND lets a co-proposed
	// DryRunRel reference this node as an endpoint (by StartID/EndID == ID). Zero =
	// a NEW, un-referenceable node — any existing holder of a constrained value is
	// a violation.
	ID types.NodeID
	// Labels + Properties are the proposed node state (same shape as g.Nodes().Add).
	Labels     []string
	Properties map[string]any
}

// DryRunRel is one proposed relationship fact to validate against temporal
// constraints. An endpoint (StartID / EndID) resolves FIRST to a co-proposed
// DryRunNode with the same non-zero ID (using that node's PROPOSED interval),
// else to the live committed node; if neither exists it is reported as an
// "invalid" violation.
type DryRunRel struct {
	Ref        string
	TypeName   string
	StartID    types.NodeID
	EndID      types.NodeID
	Properties map[string]any
	// ValidFrom / ValidTo are the proposed relationship valid interval. ValidFrom
	// 0 means "validate as if asserted now".
	ValidFrom types.Instant
	ValidTo   types.Instant
}

// DryRunFacts is a proposed fact set to validate. Nodes are checked against
// unique constraints; Rels against temporal constraints.
type DryRunFacts struct {
	Nodes []DryRunNode
	Rels  []DryRunRel
}

// DryRunViolation is one constraint violation found during a dry run. Err is a
// sentinel-wrapping error (errors.Is-able — graph.ErrUniqueViolation /
// graph.ErrTemporalConstraint).
type DryRunViolation struct {
	Ref  string // the offending fact's Ref (may be empty)
	Kind string // "unique" | "temporal" | "invalid"
	Err  error
}
