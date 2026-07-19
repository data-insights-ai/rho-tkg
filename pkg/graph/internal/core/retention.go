package core

import (
	"encoding/binary"
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Retention read-door plumbing (ADR-0008 stage R1 — fail-closed FIRST).
//
// Retention PURGE (a later stage) hard-removes whole entities below a per-label
// age boundary WITHOUT tombstones, so a temporal read pinned below that boundary
// cannot be answered completely. This file installs the fail-closed GUARD before
// the purge that needs it exists: a per-label retention watermark in MetaKV, a
// lock-free graph-max fast gate mirroring compactedThroughTx, and the point/scan
// read-door checks that turn a below-watermark read into ErrRetentionExpired
// instead of a silently-incomplete result. No purge advances the watermark yet;
// R2 wires the purge to advanceRetentionWatermark.
//
// Structure mirrors compaction.go deliberately: the graph-level maximum is the
// lock-free gate (0 ⇒ nothing to check), and the per-label watermarks are read
// on demand by exact MetaKV key (never scanned), exactly as compaction reads the
// per-entity stubs.

// retentionMaxWatermarkMeta holds the graph-level MAX over all per-label
// retention watermarks — the lock-free fast gate, rehydrated at open.
const retentionMaxWatermarkMeta = "retention_max_watermark"

// retentionWatermarkKey is the per-label MetaKV key holding the age boundary
// below which entities of that label have been purged.
func retentionWatermarkKey(labelToken uint16) string {
	return fmt.Sprintf("retention_watermark/%d", labelToken)
}

// loadRetentionWatermark rehydrates the graph retention-max atomic from MetaKV at
// open. Fail closed on a corrupt blob — a silently-dropped watermark would let a
// scan below purged knowledge return wrong data.
func (c *Core) loadRetentionWatermark() error {
	mk := c.metaKV
	if mk == nil {
		return nil
	}
	v, err := mk.MetaGet(retentionMaxWatermarkMeta)
	if err != nil {
		return fmt.Errorf("graph: read retention watermark: %w", err)
	}
	if len(v) == 0 {
		return nil
	}
	if len(v) != 8 {
		return fmt.Errorf("graph: read retention watermark: %w: want 8 bytes, got %d", storepkg.ErrCorruptWire, len(v))
	}
	wm := int64(binary.BigEndian.Uint64(v)) // #nosec G115 — round-trips the stored int64
	if wm < 0 {
		return fmt.Errorf("graph: read retention watermark: %w: negative watermark %d", storepkg.ErrCorruptWire, wm)
	}
	c.retentionMaxWatermark.Store(wm)
	return nil
}

// advanceRetentionWatermark records that entities of labelToken have been purged
// below w: it persists the per-label watermark (max-monotonic) and, when w
// exceeds the current graph max, the graph-max fast-gate key and atomic. Both
// writes go through the store-level MetaSet (on tiered/sharded these route to the
// reference/anchor shard, so the graph max lands globally on ONE shard). No purge
// exists in R1 — this is the seam R2's purge calls after a range is fully clean.
func (c *Core) advanceRetentionWatermark(labelToken uint16, w types.Instant) error {
	if w <= 0 {
		return nil
	}
	// A retention purge removes history below the boundary, which can change the
	// as-of belief at any txAt < w — invalidate the as-of column cache.
	c.asOfColumns.bump()
	mk, err := c.retentionMetaKV()
	if err != nil {
		return err
	}
	// Per-label watermark is max-monotonic (a purge never lowers a boundary).
	cur, err := c.retentionWatermarkForLabel(labelToken)
	if err != nil {
		return err
	}
	if int64(w) > cur {
		if err := mk.MetaSet(retentionWatermarkKey(labelToken), encodeWatermark(w)); err != nil {
			return fmt.Errorf("graph: advance retention watermark: %w", err)
		}
	}
	// Advance the graph-max gate when this raises it.
	if int64(w) > c.retentionMaxWatermark.Load() {
		if err := mk.MetaSet(retentionMaxWatermarkMeta, encodeWatermark(w)); err != nil {
			return fmt.Errorf("graph: advance retention max watermark: %w", err)
		}
		c.retentionMaxWatermark.Store(int64(w))
	}
	return nil
}

// retentionMetaKV resolves the store's MetaKV capability (every in-tree backend).
func (c *Core) retentionMetaKV() (storepkg.MetaKVCapability, error) {
	mk := c.metaKV
	if mk == nil {
		return nil, fmt.Errorf("graph: retention: %w", storepkg.ErrCapabilityNotSupported)
	}
	return mk, nil
}

// retentionWatermarkForLabel reads the per-label watermark by exact key (never a
// scan). 0 (nil error) when the label has no retention watermark.
func (c *Core) retentionWatermarkForLabel(labelToken uint16) (int64, error) {
	mk := c.metaKV
	if mk == nil {
		return 0, nil
	}
	v, err := mk.MetaGet(retentionWatermarkKey(labelToken))
	if err != nil {
		return 0, fmt.Errorf("graph: read label retention watermark: %w", err)
	}
	if len(v) == 0 {
		return 0, nil
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("graph: read label retention watermark: %w: want 8 bytes, got %d", storepkg.ErrCorruptWire, len(v))
	}
	wm := int64(binary.BigEndian.Uint64(v)) // #nosec G115 — round-trips the stored int64
	if wm < 0 {
		return 0, fmt.Errorf("graph: read label retention watermark: %w: negative watermark %d", storepkg.ErrCorruptWire, wm)
	}
	return wm, nil
}

// reapRetentionForReset clears the graph retention watermark AND every
// per-label retention watermark as part of Admin.Reset, so a reset graph does
// not inherit a pre-Clear watermark that would spuriously fail temporal reads
// (BACKLOG 13b).
//
// Unlike a compaction stub (keyed by ENTITY ID, and Reset destroys every
// entity — a stale stub for a now-nonexistent ID is simply never looked up
// again, since snowflake IDs are never reused), a retention watermark is keyed
// by LABEL TOKEN, and Admin.Reset deliberately PRESERVES the label registry
// (tokens ARE reused across a reset — same label name, same token number).
// Leaving a stale per-label watermark key in place after Reset zeroed only the
// graph-max gate is a genuine cross-label false-positive hazard: once ANY
// later post-reset purge on a DIFFERENT label raises the graph max again, the
// per-label check in checkNodePointRetention re-activates and consults every
// queried node's label tokens — including this label's STALE, pre-reset
// watermark, for a label that was NEVER purged post-reset. A point read on a
// brand-new post-reset entity of that label, pinned before the stale
// watermark, then fails closed with ErrRetentionExpired for no legitimate
// reason.
//
// MetaKVCapability has no key-enumeration primitive (get/set only), so this
// clears every CURRENTLY REGISTERED label token's watermark key by token
// number (1..c.labels.Len()) rather than tracking which ones were ever
// written — the registry is already the authoritative, bounded (<= 65535)
// enumeration of every token that could possibly hold a watermark key, so no
// additional durable tracking structure is needed. MetaSet(key, nil) mirrors
// the graph-max clear immediately below: retentionWatermarkForLabel already
// treats an empty value identically to an absent key (len(v)==0 → 0), so this
// is a safe "unset", not a hard delete, on every in-tree backend. Caller
// holds c.mu.Lock.
func (c *Core) reapRetentionForReset() error {
	c.retentionMaxWatermark.Store(0)
	mk := c.metaKV
	if mk == nil {
		return nil
	}
	if err := mk.MetaSet(retentionMaxWatermarkMeta, nil); err != nil {
		return fmt.Errorf("graph: reset retention watermark: %w", err)
	}
	for tok := 1; tok <= c.labels.Len(); tok++ {
		if err := mk.MetaSet(retentionWatermarkKey(uint16(tok)), nil); err != nil { // #nosec G115 -- bounded by registry TokenCapacityMax
			return fmt.Errorf("graph: reset per-label retention watermark (token %d): %w", tok, err)
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// Answerability checks (ADR-0008 §2.2)
// -----------------------------------------------------------------------------

// checkScanRetention fails a scan whose transaction/valid pin falls before the
// graph's maximum retention watermark: a scan cannot signal per entity, so a pin
// below the max means SOME relevant label's purged entities are missing and the
// whole scan fails closed with ErrRetentionExpired rather than returning a
// silently-incomplete set. Gated by the lock-free graph max so the common
// (no retention ever) path pays nothing.
func (c *Core) checkScanRetention(opts storepkg.QueryOpts) error {
	pin := opts.TxAt
	if opts.TxPin != 0 {
		pin = opts.TxPin
	}
	if pin == 0 {
		pin = opts.ValidAt
	}
	if pin == 0 {
		pin = opts.ValidStart
	}
	return c.checkScanRetentionAt(pin)
}

// checkScanRetentionAt is the pin-only scan gate (for the named txtime doors).
func (c *Core) checkScanRetentionAt(pin types.Instant) error {
	if pin == 0 {
		return nil
	}
	max := c.retentionMaxWatermark.Load()
	if max == 0 {
		return nil
	}
	if int64(pin) < max {
		return ErrRetentionExpired
	}
	return nil
}

// checkNodePointRetention returns ErrRetentionExpired when pin falls before the
// retention watermark of ANY of the queried node's labels. Gated by the graph
// max so the no-retention path pays nothing; only when the pin is below the max
// does it load the node's labels and consult the per-label watermarks. A node
// that no longer exists at all (fully purged / never existed) has no labels to
// consult — the door's own ErrNodeNotFound stands (R1: the scan gate is the
// whole-graph guard; the point gate protects surviving entities).
func (c *Core) checkNodePointRetention(id types.NodeID, pin types.Instant) error {
	if pin == 0 {
		return nil
	}
	max := c.retentionMaxWatermark.Load()
	if max == 0 || int64(pin) >= max {
		return nil
	}
	tokens, err := c.nodeLabelTokensForRetention(id)
	if err != nil || len(tokens) == 0 {
		return err
	}
	for _, tok := range tokens {
		wm, err := c.retentionWatermarkForLabel(tok)
		if err != nil {
			return err
		}
		if wm != 0 && int64(pin) < wm {
			return ErrRetentionExpired
		}
	}
	return nil
}

// checkRelPointRetention is the relationship point gate. Relationships carry a
// rel-type, not labels, and are purged transitively with their endpoint events,
// so R1 fails a below-max rel point read closed against the graph max (the
// conservative fail-closed direction — a rel below the retention boundary may
// have been purged with its event).
func (c *Core) checkRelPointRetention(pin types.Instant) error {
	return c.checkScanRetentionAt(pin)
}

// nodeLabelTokensForRetention returns the node's label tokens from its current
// row, falling back to the most recent history version when the current row is
// gone (a deleted-but-not-purged entity still answers historical reads). Returns
// nil (no error) when the node has neither.
func (c *Core) nodeLabelTokensForRetention(id types.NodeID) ([]uint16, error) {
	node, err := c.getCurrentNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}
	if node == nil {
		history, herr := c.getNodeHistory(id)
		if herr != nil {
			return nil, herr
		}
		if len(history) == 0 {
			return nil, nil
		}
		node = history[len(history)-1]
	}
	labelTokens := node.AllLabelTokens()
	tokens := make([]uint16, 0, len(labelTokens))
	for _, lt := range labelTokens {
		tokens = append(tokens, lt.Value())
	}
	return tokens, nil
}
