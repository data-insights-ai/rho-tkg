package core

import (
	"context"
	"fmt"

	constraintspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Dry-run constraint validation. Validate a proposed fact set
// against the configured unique + temporal constraints and REPORT the violations
// WITHOUT asserting anything — no store writes, no ID mint, no events, no LSN
// burn, and (unlike the Tx+rollback emulation) no risk of leaving a permanent
// UniqueForever claim behind (a rolled-back tx does NOT release forever claims —
// this door never makes one). It reuses the exact read-only check paths the write
// doors use (nodeUniqueValueKeys + nodesByLabelAndProperty for UniqueCurrent, the
// new check-only checkForeverOwnership for UniqueForever, checkTemporalConstraints
// for the relationship-within-endpoints invariant), so a dry-run PASS is a
// faithful predictor of a real assert under the same committed state.

// dryRunProbeRelID is a fixed throwaway rel-ID for the temporal-constraint probe.
// It is never persisted and never minted from the generator (a dry-run burns no
// IDs); the probe always carries an explicit ValidFrom, so the ID's timestamp is
// never consulted.
const dryRunProbeRelID = types.RelID(1)

// DryRunValidate validates a proposed fact set against the configured constraints
// and returns every violation WITHOUT mutating anything. An empty result slice
// means the fact set would be accepted under the current committed state. It holds
// only read locks (c.mu.RLock); the inner checks are side-effect-free.
func (co *ConstraintOps) DryRunValidate(ctx context.Context, facts constraintspkg.DryRunFacts) ([]constraintspkg.DryRunViolation, error) {
	c := co.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	var violations []constraintspkg.DryRunViolation

	// --- Nodes → unique constraints (+ intra-set duplicates) ---
	seen := make(map[string]string) // seen value-key -> the Ref that first claimed it
	for _, dn := range facts.Nodes {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		// Structural validation the real create door runs unconditionally
		// (validateNodeCreateLabels — empty/oversized/too-many labels) BEFORE
		// buildDryRunProbeNode's property checks, mirroring the "validate
		// before generating IDs" ordering rule. A real Add rejects these
		// regardless of unique-constraint configuration, so a dry run must
		// too (BACKLOG 8b).
		if err := c.validateNodeCreateLabels(dn.Labels); err != nil {
			violations = append(violations, constraintspkg.DryRunViolation{Ref: dn.Ref, Kind: "invalid", Err: err})
			continue
		}
		probe, err := c.buildDryRunProbeNode(dn.ID, dn.Properties)
		if err != nil {
			violations = append(violations, constraintspkg.DryRunViolation{Ref: dn.Ref, Kind: "invalid", Err: err})
			continue
		}
		tuples, ferr := c.nodeUniqueValueKeys(probe, dn.Labels)
		if ferr != nil {
			violations = append(violations, constraintspkg.DryRunViolation{Ref: dn.Ref, Kind: "unique", Err: ferr})
			continue
		}
		nodeViolated := false
		for seenKey, tuple := range tuples {
			// (a) an earlier fact in THIS set already claims the value.
			if firstRef, ok := seen[seenKey]; ok {
				violations = append(violations, constraintspkg.DryRunViolation{Ref: dn.Ref, Kind: "unique",
					Err: fmt.Errorf("%w: label token %d key %q already claimed by fact %q earlier in this set",
						ErrUniqueViolation, tuple.labelTok, tuple.key, firstRef)})
				nodeViolated = true
				continue
			}
			// (b) a committed CURRENT node already holds it.
			matches, err := c.nodesByLabelAndProperty(tuple.labelTok, tuple.key, tuple.raw, storepkg.QueryOpts{})
			if err != nil {
				return nil, fmt.Errorf("graph: dry-run unique lookup: %w", err)
			}
			for _, m := range matches {
				if m.ID() == probe.ID() {
					continue // validating an update to this same node
				}
				violations = append(violations, constraintspkg.DryRunViolation{Ref: dn.Ref, Kind: "unique",
					Err: fmt.Errorf("%w: label token %d key %q already held by node %d",
						ErrUniqueViolation, tuple.labelTok, tuple.key, m.ID())})
				nodeViolated = true
				break
			}
			// (c) UniqueForever: a value can be owned by an entity that no longer
			// holds it (supersession / hard delete), which (b) misses. Check-only.
			if tuple.scope == constraintspkg.UniqueForever {
				if err := c.checkForeverOwnership(tuple.labelTok, tuple.key, tuple.valueKey, probe.ID()); err != nil {
					violations = append(violations, constraintspkg.DryRunViolation{Ref: dn.Ref, Kind: "unique", Err: err})
					nodeViolated = true
				}
			}
		}
		// A non-violating fact claims its values, so a later identical fact in the
		// set is reported against it (mirrors the batch pre-check).
		if !nodeViolated {
			for seenKey := range tuples {
				seen[seenKey] = dn.Ref
			}
		}
	}

	// --- Rels → structural validation (unconditional) + temporal constraints ---
	if len(facts.Rels) > 0 {
		// A rel's endpoint may be a node PROPOSED in the SAME fact set (not yet
		// asserted), so resolve endpoints from the proposed nodes FIRST (using their
		// proposed valid interval) and fall back to the live committed node. Only
		// nodes carrying a non-zero ID can be referenced by a rel; a malformed
		// proposed node is skipped here (already reported in the unique pass) so a
		// rel referencing it reports "not found".
		proposed := make(map[types.NodeID]*types.Node, len(facts.Nodes))
		for _, dn := range facts.Nodes {
			if dn.ID == 0 {
				continue
			}
			node, berr := c.buildDryRunEndpointNode(dn.ID, dn.Properties)
			if berr != nil {
				continue
			}
			proposed[dn.ID] = node
		}
		resolveEndpoint := func(id types.NodeID) (*types.Node, error) {
			if n, ok := proposed[id]; ok {
				return n, nil // co-proposed node — use its proposed interval
			}
			return c.getCurrentNode(id)
		}
		hasTemporalConstraints := c.constraints.Len() > 0
		for _, dr := range facts.Rels {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
			// Structural validation the real create kernel
			// (relationship_create_kernel.go's prepareRelCreate) runs
			// UNCONDITIONALLY, before any temporal-constraint check and
			// regardless of whether any constraint is configured — a
			// self-loop or malformed endpoint/type-name is rejected by a real
			// Add/AddByID no matter what. A dry run that only inspected
			// temporal constraints could report zero violations for a fact
			// set a real assert would reject (BACKLOG 8b).
			if err := validateRelationshipEndpointIDs(dr.StartID, dr.EndID); err != nil {
				violations = append(violations, constraintspkg.DryRunViolation{Ref: dr.Ref, Kind: "invalid", Err: err})
				continue
			}
			if dr.StartID == dr.EndID && !c.validation.AllowSelfLoops {
				violations = append(violations, constraintspkg.DryRunViolation{Ref: dr.Ref, Kind: "invalid", Err: ErrSelfLoop})
				continue
			}
			if err := c.validateName(dr.TypeName); err != nil {
				violations = append(violations, constraintspkg.DryRunViolation{Ref: dr.Ref, Kind: "invalid", Err: err})
				continue
			}
			if !hasTemporalConstraints {
				continue
			}
			startNode, err := resolveEndpoint(dr.StartID)
			if err != nil {
				violations = append(violations, constraintspkg.DryRunViolation{Ref: dr.Ref, Kind: "invalid",
					Err: fmt.Errorf("start node %d: %w", dr.StartID, err)})
				continue
			}
			endNode, err := resolveEndpoint(dr.EndID)
			if err != nil {
				violations = append(violations, constraintspkg.DryRunViolation{Ref: dr.Ref, Kind: "invalid",
					Err: fmt.Errorf("end node %d: %w", dr.EndID, err)})
				continue
			}
			validFrom := dr.ValidFrom
			if validFrom == 0 {
				validFrom = c.now() // validate as if asserted now
			}
			probe := c.newRelConstraintProbe(dryRunProbeRelID, dr.StartID, dr.EndID, validFrom, dr.ValidTo, 0)
			if err := c.checkTemporalConstraints(probe, startNode, endNode); err != nil {
				violations = append(violations, constraintspkg.DryRunViolation{Ref: dr.Ref, Kind: "temporal", Err: err})
			}
		}
	}

	return violations, nil
}

// buildDryRunEndpointNode builds a throwaway node for use as a PROPOSED rel
// endpoint: unlike buildDryRunProbeNode (unique-check role, which strips the
// temporal shadow keys) it KEEPS the proposed valid interval (tkg_valid_from /
// tkg_valid_to) so the rel-within-endpoints check sees the node's proposed
// bounds. A missing explicit ValidFrom falls back to the ID's snowflake timestamp
// (same as a real create), so a caller wanting a specific interval supplies
// tkg_valid_from.
func (c *Core) buildDryRunEndpointNode(id types.NodeID, props map[string]any) (*types.Node, error) {
	_, _, _, _, props, err := extractProvenance(props)
	if err != nil {
		return nil, err
	}
	validFrom, validTo, createdAt, _, props, err := extractTemporal(props)
	if err != nil {
		return nil, err
	}
	if err := c.validateProperties(props); err != nil {
		return nil, err
	}
	ps, err := types.NewOwnedPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: dry-run endpoint properties: %w", err)
	}
	n := types.NewNode(id, 0, nil)
	if err := n.SetOwnedProperties(ps); err != nil {
		return nil, fmt.Errorf("graph: dry-run endpoint properties: %w", err)
	}
	if validFrom != 0 || validTo != 0 || createdAt != 0 {
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: validFrom, ValidTo: validTo, CreatedAt: createdAt})
	}
	return n, nil
}

// buildDryRunProbeNode constructs a throwaway node carrying the proposed
// properties (shadow/provenance/temporal keys stripped and validated exactly as a
// real create would), so its indexable value-keys match what a real assert would
// index. id is the caller-supplied node ID (0 for a new node — matches no
// existing node, so any holder is a violation).
func (c *Core) buildDryRunProbeNode(id types.NodeID, props map[string]any) (*types.Node, error) {
	_, _, _, _, props, err := extractProvenance(props)
	if err != nil {
		return nil, err
	}
	_, _, _, _, props, err = extractTemporal(props)
	if err != nil {
		return nil, err
	}
	if err := c.validateProperties(props); err != nil {
		return nil, err
	}
	ps, err := types.NewOwnedPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: dry-run node properties: %w", err)
	}
	n := types.NewNode(id, 0, nil)
	if err := n.SetOwnedProperties(ps); err != nil {
		return nil, fmt.Errorf("graph: dry-run node properties: %w", err)
	}
	return n, nil
}
