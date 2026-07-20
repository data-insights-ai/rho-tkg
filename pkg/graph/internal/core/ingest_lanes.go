package core

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
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

// validateShardedSlotCoverage cross-validates the slots New()'s ID generators
// will actually mint from (the interactive pair {SnowflakeNodeID*2, *2+1}
// plus every IngestLanes slot buildLaneGenerators picked) against a sharded
// store's own claimed range (BACKLOG 20h). Only applies when store is a
// *sharded.Store; every other backend has no slot-ownership concept and is a
// silent no-op. Without this, a BaseSlot/SlotCount that doesn't cover every
// slot the generators use is only discovered reactively — a routing error
// (or worse, a misrouted write to a foreign slot's shard) the first time an
// uncovered slot mints an ID — instead of failing closed at construction
// with a message naming exactly which slot and why.
func validateShardedSlotCoverage(store storepkg.MandatoryStore, snowflakeNodeID int64, laneSlots []uint8) error {
	shardedStore, ok := store.(*sharded.Store)
	if !ok {
		return nil
	}
	base, count := shardedStore.ClaimedSlotRange()
	covers := func(slot uint8) bool {
		return slot >= base && slot < base+count
	}
	interactiveNode := uint8(snowflakeNodeID * 2)
	interactiveRel := uint8(snowflakeNodeID*2 + 1)
	if !covers(interactiveNode) || !covers(interactiveRel) {
		return fmt.Errorf(
			"graph: sharded store claims slots [%d,%d) but SnowflakeNodeID %d needs its interactive pair {%d,%d}: reconfigure BaseSlot/SlotCount or SnowflakeNodeID",
			base, base+count, snowflakeNodeID, interactiveNode, interactiveRel)
	}
	for _, slot := range laneSlots {
		if !covers(slot) {
			return fmt.Errorf(
				"graph: sharded store claims slots [%d,%d) but IngestLanes needs slot %d: reconfigure BaseSlot/SlotCount, SnowflakeNodeID, or IngestLanes",
				base, base+count, slot)
		}
	}
	return nil
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
