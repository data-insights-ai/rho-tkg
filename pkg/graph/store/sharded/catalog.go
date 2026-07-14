package sharded

import (
	"errors"
	"fmt"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/vmihailenco/msgpack/v5"
)

// catalogFormatVersion is the on-disk version of the slot catalog blob. A blob
// written by a newer release (higher version) fails closed at open.
const catalogFormatVersion uint8 = 1

// ID-discipline markers. The sharded backend mints nodes AND relationships from
// ONE generator per slot (ADR-0007 §2, "the even/odd rel pairing is DROPPED in
// sharded mode"); a directory that recorded a different discipline fails closed.
const (
	disciplineUnified uint8 = 1 // single generator per slot (nodes + rels)
)

// catalogMetaKey is the anchor-shard MetaKV key under which the slot catalog is
// persisted. Distinct from every graph-layer marker key.
const catalogMetaKey = "sharded_slot_catalog"

// maxSlots is the size of the 5-bit snowflake node field (0..31).
const maxSlots = 32

// slotCatalog is the persisted routing table. It records the claimed slot range,
// the slot->shard-index map (default identity), the ID-discipline marker, and a
// format version. It is written atomically to the anchor shard at create and
// validated against the live config on every open (fail closed on conflict).
type slotCatalog struct {
	FormatVersion uint8         `msgpack:"v"`
	BaseSlot      uint8         `msgpack:"b"`
	SlotCount     uint8         `msgpack:"n"`
	Discipline    uint8         `msgpack:"d"`
	SlotShard     map[uint8]int `msgpack:"m"` // claimed slot -> shard index
}

// newIdentityCatalog builds a default identity catalog: claimed slot base+k maps
// to shard index k, for k in [0, count).
func newIdentityCatalog(base, count uint8) *slotCatalog {
	m := make(map[uint8]int, count)
	for k := uint8(0); k < count; k++ {
		m[base+k] = int(k)
	}
	return &slotCatalog{
		FormatVersion: catalogFormatVersion,
		BaseSlot:      base,
		SlotCount:     count,
		Discipline:    disciplineUnified,
		SlotShard:     m,
	}
}

// shardIndexForSlot returns the shard index owning slot, and whether the slot is
// claimed by this catalog.
func (c *slotCatalog) shardIndexForSlot(slot uint8) (int, bool) {
	idx, ok := c.SlotShard[slot]
	return idx, ok
}

func (c *slotCatalog) encode() ([]byte, error) {
	return msgpack.Marshal(c)
}

// decodeCatalog decodes a persisted catalog blob at the trust boundary.
func decodeCatalog(data []byte) (*slotCatalog, error) {
	var c slotCatalog
	if err := storeutil.SafeUnmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCatalogCorrupt, err)
	}
	return &c, nil
}

// validateAgainstConfig fails closed if a persisted catalog disagrees with the
// config the store is being opened under: a newer format version, an unknown ID
// discipline, or a different claimed range (base/count) — any of which would
// silently mis-route or corrupt IDs.
func (c *slotCatalog) validateAgainstConfig(base, count uint8) error {
	if c.FormatVersion > catalogFormatVersion {
		return fmt.Errorf("%w: catalog format version %d newer than supported %d",
			ErrCatalogConflict, c.FormatVersion, catalogFormatVersion)
	}
	if c.Discipline != disciplineUnified {
		return fmt.Errorf("%w: unknown ID discipline %d", ErrCatalogConflict, c.Discipline)
	}
	if c.BaseSlot != base {
		return fmt.Errorf("%w: catalog base slot %d != config base slot %d",
			ErrCatalogConflict, c.BaseSlot, base)
	}
	if c.SlotCount != count {
		return fmt.Errorf("%w: catalog slot count %d != config slot count %d",
			ErrCatalogConflict, c.SlotCount, count)
	}
	// Every claimed slot must map to a shard index within range.
	if len(c.SlotShard) != int(count) {
		return fmt.Errorf("%w: catalog maps %d slots, expected %d",
			ErrCatalogConflict, len(c.SlotShard), count)
	}
	for k := uint8(0); k < count; k++ {
		idx, ok := c.SlotShard[base+k]
		if !ok {
			return fmt.Errorf("%w: catalog does not map claimed slot %d", ErrCatalogConflict, base+k)
		}
		if idx < 0 || idx >= int(count) {
			return fmt.Errorf("%w: catalog slot %d maps to out-of-range shard index %d",
				ErrCatalogConflict, base+k, idx)
		}
	}
	return nil
}

// validateConfigRange enforces the slot-budget invariants: 1..maxSlots slots and
// base+count within the 5-bit field.
func validateConfigRange(base, count uint8) error {
	if count == 0 {
		return errors.New("graph: sharded.Config.SlotCount must be >= 1")
	}
	if count > maxSlots {
		return fmt.Errorf("graph: sharded.Config.SlotCount must be <= %d, got %d", maxSlots, count)
	}
	if int(base)+int(count) > maxSlots {
		return fmt.Errorf("graph: sharded.Config BaseSlot(%d)+SlotCount(%d) must be <= %d",
			base, count, maxSlots)
	}
	return nil
}
