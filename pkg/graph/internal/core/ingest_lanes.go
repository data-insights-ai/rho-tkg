package core

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// maxSnowflakeSlots is the size of the 5-bit snowflake node field (0..31). It
// mirrors the sharded backend's maxSlots — the two MUST agree, since a lane
// generator's node-field is exactly the sharded routing slot.
const maxSnowflakeSlots = 32

// buildLaneGenerators constructs the per-lane UNIFIED ID generators for
// Config.IngestLanes > 0 (ADR-0007 S4). It returns nil slices when lanes == 0
// (the legacy dual-generator model — zero behavior change).
//
// Each lane generator draws a DISTINCT node-field (slot) from 0..31, skipping
// the interactive pair {snowflakeNodeID*2, *2+1} that the standalone/tx/batch
// door already owns. A single generator per slot mints BOTH nodes and rels, so
// value-level ID uniqueness holds by construction: within a slot successive
// mints draw distinct (time, seq) tuples, and across slots the node-field
// differs. Fails closed if there are not enough free slots (2+lanes > 32).
func buildLaneGenerators(snowflakeNodeID int64, lanes uint8) ([]*snowflake.Node, []uint8, error) {
	if lanes == 0 {
		return nil, nil, nil
	}
	interactiveNode := snowflakeNodeID * 2
	interactiveRel := snowflakeNodeID*2 + 1
	if 2+int(lanes) > maxSnowflakeSlots {
		return nil, nil, fmt.Errorf(
			"graph: IngestLanes %d needs %d slots but only %d exist after the interactive pair (5-bit node field)",
			lanes, 2+int(lanes), maxSnowflakeSlots)
	}

	gens := make([]*snowflake.Node, 0, lanes)
	slots := make([]uint8, 0, lanes)
	for slot := int64(0); slot < maxSnowflakeSlots && len(gens) < int(lanes); slot++ {
		if slot == interactiveNode || slot == interactiveRel {
			continue
		}
		gen, err := snowflake.NewNode(slot,
			snowflake.WithEpoch(snowflakeEpoch),
			snowflake.WithMicroseconds(),
			snowflake.WithNodeBits(5),
			snowflake.WithStepBits(10),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("graph: lane %d ID generator (slot %d): %w", len(gens)+1, slot, err)
		}
		gens = append(gens, gen)
		slots = append(slots, uint8(slot))
	}
	return gens, slots, nil
}

// laneGeneratorIndex maps a session lane number to a laneGenerators index, or
// returns ok=false to signal "use the interactive generator". Lane 0 and any
// lane when IngestLanes==0 fall back to interactive. A nonzero session lane is
// folded into [0, len(laneGenerators)) so an unbounded lane counter (the ingest
// session assigns a monotonically increasing lane) always maps to a real slot.
func (c *Core) laneGeneratorIndex(lane uint16) (int, bool) {
	n := len(c.laneGenerators)
	if lane == 0 || n == 0 {
		return 0, false
	}
	return int((lane - 1) % uint16(n)), true
}

// nextNodeIDForLane mints a node ID for the given session lane. Lane 0 (and any
// lane when no lane generators exist) uses the interactive even-node-field
// generator; a nonzero lane uses its pinned unified generator.
func (c *Core) nextNodeIDForLane(lane uint16) types.NodeID {
	idx, ok := c.laneGeneratorIndex(lane)
	if !ok {
		return c.nextNodeID()
	}
	return types.NodeID(c.laneGenerators[idx].Generate())
}

// nextRelIDForLane mints a rel ID for the given session lane. Lane 0 uses the
// interactive odd-node-field generator; a nonzero lane uses the SAME unified
// generator as nextNodeIDForLane for that lane (the disciplineUnified contract).
func (c *Core) nextRelIDForLane(lane uint16) types.RelID {
	idx, ok := c.laneGeneratorIndex(lane)
	if !ok {
		return c.nextRelID()
	}
	return types.RelID(c.laneGenerators[idx].Generate())
}
