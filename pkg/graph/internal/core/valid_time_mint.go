package core

import (
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// validTimeMinter returns the store's valid-time minter (ADR-0013) when the
// store mints, after checking that no generator of this core draws the field
// the store mints on: the interactive pair {snowflakeNodeID*2, *2+1} and every
// lane slot. Nil when the store does not mint.
func validTimeMinter(store storepkg.MandatoryStore, snowflakeNodeID int64, laneSlots []uint8) (storepkg.ValidTimeMintCapability, error) {
	m, ok := store.(storepkg.ValidTimeMintCapability)
	if !ok || !m.MintsAtValidTime() {
		return nil, nil
	}
	const field = storepkg.ValidTimeMintField
	if snowflakeNodeID*2+1 >= field {
		return nil, fmt.Errorf("graph: SnowflakeNodeID %d needs node field %d, which the store's MintAtValidFrom reserves: use 0-%d",
			snowflakeNodeID, snowflakeNodeID*2+1, (field-2)/2)
	}
	for _, slot := range laneSlots {
		if slot == field {
			return nil, fmt.Errorf("graph: IngestLanes %d needs node field %d, which the store's MintAtValidFrom reserves: use at most %d lanes",
				len(laneSlots), field, field-2)
		}
	}
	return m, nil
}

// validTimed reports whether a create's start (claimedStart) is an instant the
// store mints at: an untimed create starts at the whole axis
// (types.UnboundedValidFrom) or, on the legacy axis, at 0, which the store
// mints at its clock too.
func validTimed(validFrom types.Instant) bool { return validFrom != types.UnboundedValidFrom }

// mintNodeIDAt mints a generated node ID: the store's, at the node's ValidFrom
// (untimed: the store's clock), when it mints this label, else the lane's
// generator.
func (c *Core) mintNodeIDAt(lane, primaryLabel uint16, validFrom types.Instant) (types.NodeID, error) {
	if c.validTimeMint != nil {
		if id, ok, err := c.validTimeMint.MintNodeIDAt(primaryLabel, validFrom, validTimed(validFrom)); ok || err != nil {
			return id, err
		}
	}
	return c.nextNodeIDForLane(lane), nil
}

// mintRelIDAt mints a generated relationship ID: the store's, inside its start
// node's window, when it mints, else the lane's generator.
func (c *Core) mintRelIDAt(lane uint16, startID types.NodeID, validFrom types.Instant) (types.RelID, error) {
	if c.validTimeMint != nil {
		if id, ok, err := c.validTimeMint.MintRelIDAt(startID, validFrom, validTimed(validFrom)); ok || err != nil {
			return id, err
		}
	}
	return c.nextRelIDForLane(lane), nil
}
