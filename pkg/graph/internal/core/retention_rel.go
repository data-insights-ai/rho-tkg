package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"maps"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship retention watermarks (backlog 21). A relationship purge
// (PurgeExpiredRels) removes the relationships of ONE type, so it records a
// per-type watermark instead of raising the graph-wide node one: a read of type
// T, or of every type, pinned below T's watermark fails closed with
// ErrRetentionExpired, while reads of other types and of nodes are answered.
//
// Persistence mirrors the node family: retention_rel_watermark/<typeToken>
// (max-monotonic) and retention_rel_max_watermark (the max over types). In
// RAM the per-type values live in a copy-on-write map loaded at open (by
// registered type token, MetaKV has no enumeration), so the gate never reads
// MetaKV; retentionAnyMax = max(node max, rel max) keeps the no-retention
// path at one atomic load.

// retentionRelMaxWatermarkMeta holds the max over every per-type relationship
// retention watermark.
const retentionRelMaxWatermarkMeta = "retention_rel_max_watermark"

// retentionRelWatermarkKey is the per-type MetaKV key holding the mint-time
// boundary below which relationships of that type have been purged.
func retentionRelWatermarkKey(typeToken uint16) string {
	return fmt.Sprintf("retention_rel_watermark/%d", typeToken)
}

// retentionScope names what a read covers, so each purge family fails only
// the reads that could miss its rows.
type retentionScope struct {
	// nodes: the node-label purges (retention_max_watermark) apply.
	nodes bool
	// rels: the relationship-type purges apply — relType's watermark, or every
	// type's (retention_rel_max_watermark) when relType is 0.
	rels    bool
	relType uint16
}

var (
	// scopeNodes covers node reads: a relationship purge never fails them.
	scopeNodes = retentionScope{nodes: true}
	// scopeAllRels covers reads of every relationship type.
	scopeAllRels = retentionScope{nodes: true, rels: true}
	// scopeRelsOnly covers relationship reads the node gate never covered
	// (the valid-time and adjacency doors, backlog 21 finding c): only the
	// relationship family applies, so their node-purge behaviour is unchanged.
	scopeRelsOnly = retentionScope{rels: true}
)

// relTypeRetentionScope is the scope of a read of typeName: every type when
// typeName is empty, that type's watermark when it is registered, and the node
// family alone for an unregistered type (no relationship of it was ever
// purged).
func (c *Core) relTypeRetentionScope(typeName string) retentionScope {
	if typeName == "" {
		return scopeAllRels
	}
	tok, ok := c.relTypes.Lookup(typeName)
	if !ok {
		return scopeNodes
	}
	return retentionScope{nodes: true, rels: true, relType: tok}
}

// withoutNodes drops the node family from a scope (doors the node gate never
// covered).
func (s retentionScope) withoutNodes() retentionScope {
	s.nodes = false
	return s
}

// relRetentionWatermark is typeToken's watermark (0: never purged).
func (c *Core) relRetentionWatermark(typeToken uint16) int64 {
	if m := c.relRetentionWatermarks.Load(); m != nil {
		return (*m)[typeToken]
	}
	return 0
}

// raiseRetentionAnyMax lifts the one-load fast gate to w.
func (c *Core) raiseRetentionAnyMax(w int64) {
	for {
		cur := c.retentionAnyMax.Load()
		if w <= cur || c.retentionAnyMax.CompareAndSwap(cur, w) {
			return
		}
	}
}

// loadRelRetentionWatermarks rehydrates the relationship family at open: the
// max key, then (only when a relationship purge ever ran) every registered
// type's key. Fails closed on a corrupt blob.
func (c *Core) loadRelRetentionWatermarks() error {
	mk := c.metaKV
	if mk == nil {
		return nil
	}
	relMax, err := readWatermarkMeta(mk, retentionRelMaxWatermarkMeta)
	if err != nil {
		return fmt.Errorf("graph: read relationship retention watermark: %w", err)
	}
	c.retentionRelMaxWatermark.Store(relMax)
	c.relRetentionWatermarks.Store(nil)
	if relMax > 0 {
		m := make(map[uint16]int64)
		for tok := 1; tok <= c.relTypes.Len(); tok++ {
			wm, err := readWatermarkMeta(mk, retentionRelWatermarkKey(uint16(tok))) // #nosec G115 -- bounded by the registry's token capacity
			if err != nil {
				return fmt.Errorf("graph: read relationship retention watermark (type token %d): %w", tok, err)
			}
			if wm > 0 {
				m[uint16(tok)] = wm // #nosec G115 -- bounded by the registry's token capacity
			}
		}
		c.relRetentionWatermarks.Store(&m)
	}
	c.raiseRetentionAnyMax(relMax)
	return nil
}

// readWatermarkMeta decodes one 8-byte watermark key (0 when absent).
func readWatermarkMeta(mk storepkg.MetaKVCapability, key string) (int64, error) {
	v, err := mk.MetaGet(key)
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("%w: want 8 bytes, got %d", storepkg.ErrCorruptWire, len(v))
	}
	wm := int64(binary.BigEndian.Uint64(v)) // #nosec G115 — round-trips the stored int64
	if wm < 0 {
		return 0, fmt.Errorf("%w: negative watermark %d", storepkg.ErrCorruptWire, wm)
	}
	return wm, nil
}

// advanceRelRetentionWatermark records that relationships of typeToken have
// been purged below w: the per-type key (max-monotonic), the max key when w
// raises it, and the RAM gate. No as-of cache bump: the cache holds node
// columns only. Serialized with every other watermark advance.
func (c *Core) advanceRelRetentionWatermark(typeToken uint16, w types.Instant) error {
	if w <= 0 || typeToken == 0 {
		return nil
	}
	mk, err := c.retentionMetaKV()
	if err != nil {
		return err
	}
	c.retentionMu.Lock()
	defer c.retentionMu.Unlock()
	if int64(w) > c.relRetentionWatermark(typeToken) {
		if err := mk.MetaSet(retentionRelWatermarkKey(typeToken), encodeWatermark(w)); err != nil {
			return fmt.Errorf("graph: advance relationship retention watermark: %w", err)
		}
		next := make(map[uint16]int64)
		if m := c.relRetentionWatermarks.Load(); m != nil {
			maps.Copy(next, *m)
		}
		next[typeToken] = int64(w)
		c.relRetentionWatermarks.Store(&next)
	}
	if int64(w) > c.retentionRelMaxWatermark.Load() {
		if err := mk.MetaSet(retentionRelMaxWatermarkMeta, encodeWatermark(w)); err != nil {
			return fmt.Errorf("graph: advance relationship retention max watermark: %w", err)
		}
		c.retentionRelMaxWatermark.Store(int64(w))
	}
	c.raiseRetentionAnyMax(int64(w))
	return nil
}

// reapRelRetentionForReset clears the relationship family on Admin.Reset /
// ChangeClear: every registered type token's key (tokens are reused across a
// reset), the max key and the RAM gate. Caller holds c.mu.Lock.
func (c *Core) reapRelRetentionForReset() error {
	c.retentionRelMaxWatermark.Store(0)
	c.relRetentionWatermarks.Store(nil)
	mk := c.metaKV
	if mk == nil {
		return nil
	}
	if err := mk.MetaSet(retentionRelMaxWatermarkMeta, nil); err != nil {
		return fmt.Errorf("graph: reset relationship retention watermark: %w", err)
	}
	for tok := 1; tok <= c.relTypes.Len(); tok++ {
		if err := mk.MetaSet(retentionRelWatermarkKey(uint16(tok)), nil); err != nil { // #nosec G115 -- bounded by registry TokenCapacityMax
			return fmt.Errorf("graph: reset per-type relationship retention watermark (token %d): %w", tok, err)
		}
	}
	return nil
}

// checkRelTypePointRetention is the relationship-family point gate of the
// relationship point doors (RelAsOf, RelAtTx, RelEffectiveTimeline,
// RelsEffectiveByIDs): below the relationship max it fails when the
// relationship's type was purged above pin, or when the relationship is gone
// (a relationship purge may have removed it, so its type is unknown). One
// atomic load when no relationship purge ever ran.
func (c *Core) checkRelTypePointRetention(id types.RelID, pin types.Instant) error {
	if pin == 0 {
		return nil
	}
	relMax := c.retentionRelMaxWatermark.Load()
	// A relationship minted at or after every relationship watermark was never
	// a purge candidate (a purge takes mint < Before <= the max): its rows
	// are whole, so the door answers it normally.
	if int64(pin) >= relMax || int64(id.MintInstant()) >= relMax {
		return nil
	}
	tok, ok, err := c.relTypeTokenForRetention(id)
	if err != nil {
		return err
	}
	if !ok || int64(pin) < c.relRetentionWatermark(tok) {
		return ErrRetentionExpired
	}
	return nil
}

// relTypeTokenForRetention reads the relationship's type from its current row
// or, when deleted, from its history (the type is immutable). ok is false when
// neither exists.
func (c *Core) relTypeTokenForRetention(id types.RelID) (uint16, bool, error) {
	r, err := c.getCurrentRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return 0, false, err
	}
	if r == nil {
		history, herr := c.getRelHistory(id)
		if herr != nil {
			return 0, false, herr
		}
		if len(history) == 0 {
			return 0, false, nil
		}
		r = history[len(history)-1]
	}
	return r.TypeToken().Value(), true, nil
}

// checkRelsOnlyRetention is the relationship-family gate of the doors the node
// gate never covered, for one relationship type (or every type when empty).
func (c *Core) checkRelsOnlyRetention(typeName string, pin types.Instant) error {
	return c.checkScanRetentionAt(pin, c.relTypeRetentionScope(typeName).withoutNodes())
}

// checkRelTypesRetention is the relationship-family gate of a read of several
// types (every type when relTypes is empty).
func (c *Core) checkRelTypesRetention(relTypes []string, pin types.Instant) error {
	if len(relTypes) == 0 {
		return c.checkScanRetentionAt(pin, scopeRelsOnly)
	}
	for _, name := range relTypes {
		if err := c.checkRelsOnlyRetention(name, pin); err != nil {
			return err
		}
	}
	return nil
}

// retentionWatermarksByName returns both watermark families keyed by name for
// the export header (nil maps when no purge of that family ever ran).
func (c *Core) retentionWatermarksByName() (nodes, rels map[string]int64, err error) {
	if c.retentionMaxWatermark.Load() > 0 {
		for tok := 1; tok <= c.labels.Len(); tok++ {
			wm, werr := c.retentionWatermarkForLabel(uint16(tok)) // #nosec G115 -- bounded by the registry's token capacity
			if werr != nil {
				return nil, nil, werr
			}
			if wm > 0 {
				if nodes == nil {
					nodes = make(map[string]int64)
				}
				nodes[c.labels.Resolve(uint16(tok))] = wm // #nosec G115 -- bounded by the registry's token capacity
			}
		}
	}
	if m := c.relRetentionWatermarks.Load(); m != nil {
		for tok, wm := range *m {
			if rels == nil {
				rels = make(map[string]int64, len(*m))
			}
			rels[c.relTypes.Resolve(tok)] = wm
		}
	}
	return nodes, rels, nil
}

// importRelRetentionLocked raises this graph's relationship watermarks to the
// imported ones (max-monotonic), so a replica bootstrapped after a
// relationship purge refuses the pins its source refuses. A name the imported
// registry does not hold, or a non-positive value, is a corrupt export.
// Caller holds c.mu.Lock.
func (c *Core) importRelRetentionLocked(byName map[string]int64) error {
	for name, wm := range byName {
		tok, ok := c.relTypes.Lookup(name)
		if !ok || wm <= 0 {
			return fmt.Errorf("%w: relationship retention watermark %q = %d", ErrCorruptExport, name, wm)
		}
		if err := c.advanceRelRetentionWatermark(tok, types.Instant(wm)); err != nil {
			return fmt.Errorf("import: relationship retention watermark: %w", err)
		}
	}
	return nil
}

// relsAtTxPin is the pin of a bitemporal door: the transaction pin when set,
// else the valid instant (checkScanRetention's precedence).
func relsAtTxPin(validAt, txAt types.Instant) types.Instant {
	if txAt != 0 {
		return txAt
	}
	return validAt
}
