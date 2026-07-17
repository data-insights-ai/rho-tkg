package core

import (
	"fmt"
	"strings"

	"github.com/vmihailenco/msgpack/v5"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Named knowledge-time (Erkenntniszeit) marks
//
// A small durable alias registry mapping a documented name to a transaction-time
// instant, so a consumer can run AS OF SYSTEM TIME against a named, stable state
// (e.g. tagAsOf("oetk-statistik-2025", 2026-01-15 12:00)) instead of a bare
// magic number. The instant is a TRANSACTION time (knowledge time) — it feeds
// QueryOpts.TxAt / the NodeAtTx txAt argument, not valid time.
//
// Stored as a single msgpack map under the asof_tags MetaKV key. Durable across
// restart on a persistent backend; reaped by Clear() (the reset meta-key scan
// preserves only the LSN watermark). Not carried by Export/Import — tags are
// store-local metadata, like the graph epoch and the failover lease.
// =============================================================================

// asofTagsMeta is the MetaKV key holding the durable named-as-of-tag registry.
// Distinct from graphEpochMeta / replicaAppliedLSNMeta / idSlotLeaseMeta.
const asofTagsMeta = "asof_tags"

// maxAsOfTags bounds the durable tag registry. These are curated, documented
// marks (a handful in practice); the cap keeps the meta blob small and bounded.
const maxAsOfTags = 4096

// maxAsOfTagNameLen bounds a single tag name (bytes).
const maxAsOfTagNameLen = 256

func validateAsOfTagName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: name must be non-blank", ErrInvalidAsOfTag)
	}
	if len(name) > maxAsOfTagNameLen {
		return fmt.Errorf("%w: name exceeds %d bytes", ErrInvalidAsOfTag, maxAsOfTagNameLen)
	}
	return nil
}

// asofMetaKV returns the store's MetaKV capability or a wrapped
// ErrCapabilityNotSupported. Every in-tree backend implements it; a store that
// cannot persist metadata cannot hold durable tags.
func (c *Core) asofMetaKV() (storepkg.MetaKVCapability, error) {
	mk := c.metaKV
	if mk == nil {
		return nil, fmt.Errorf("graph: as-of tags: %w", storepkg.ErrCapabilityNotSupported)
	}
	return mk, nil
}

// loadAsOfTags reads and decodes the durable tag map. A missing key yields an
// empty (non-nil) map. Persisted bytes are untrusted (a corrupt disk row) so
// they are decoded through storeutil.SafeUnmarshal and fail closed.
func loadAsOfTags(mk storepkg.MetaKVCapability) (map[string]int64, error) {
	v, err := mk.MetaGet(asofTagsMeta)
	if err != nil {
		return nil, fmt.Errorf("graph: read as-of tags: %w", err)
	}
	tags := make(map[string]int64)
	if len(v) == 0 {
		return tags, nil
	}
	if err := storeutil.SafeUnmarshal(v, &tags); err != nil {
		return nil, fmt.Errorf("graph: decode as-of tags: %w", err)
	}
	// Re-validate the decoded entries against the same invariants the write door
	// (tagAsOf) enforces. The persisted bytes are untrusted (a corrupt disk row
	// or a foreign MetaKV writer): a non-positive instant is especially dangerous
	// because Instant(0) aliases the "no TX filter" sentinel, so a resolved tag
	// would silently return CURRENT belief instead of the named knowledge time.
	// This is a NEW key (no legacy data can hold an invalid entry), so the load
	// door mirrors the write door exactly and fails closed. See lesson 59.
	for name, at := range tags {
		if at <= 0 {
			return nil, fmt.Errorf("graph: decode as-of tags: %w: tag %q has non-positive instant %d", storepkg.ErrCorruptWire, name, at)
		}
		if err := validateAsOfTagName(name); err != nil {
			return nil, fmt.Errorf("graph: decode as-of tags: %w: %v", storepkg.ErrCorruptWire, err)
		}
	}
	return tags, nil
}

func storeAsOfTags(mk storepkg.MetaKVCapability, tags map[string]int64) error {
	b, err := msgpack.Marshal(tags)
	if err != nil {
		return fmt.Errorf("graph: encode as-of tags: %w", err)
	}
	if err := mk.MetaSet(asofTagsMeta, b); err != nil {
		return fmt.Errorf("graph: persist as-of tags: %w", err)
	}
	return nil
}

// tagAsOf registers (or overwrites) a named knowledge-time mark. Gated by
// checkWritable (a read-only replica tags on its primary). Serialized with
// concurrent writers via asofMu so a read-modify-write never loses an update.
func (c *Core) tagAsOf(name string, at types.Instant) error {
	if err := c.checkWritable(); err != nil {
		return err
	}
	if err := validateAsOfTagName(name); err != nil {
		return err
	}
	if at <= 0 {
		return fmt.Errorf("%w: instant must be positive", ErrInvalidAsOfTag)
	}
	mk, err := c.asofMetaKV()
	if err != nil {
		return err
	}
	c.asofMu.Lock()
	defer c.asofMu.Unlock()
	tags, err := loadAsOfTags(mk)
	if err != nil {
		return err
	}
	if _, exists := tags[name]; !exists && len(tags) >= maxAsOfTags {
		return fmt.Errorf("%w: at capacity (%d)", ErrTooManyAsOfTags, maxAsOfTags)
	}
	tags[name] = int64(at)
	return storeAsOfTags(mk, tags)
}

// resolveAsOf returns the instant registered under name (ok=false if absent).
// Lock-free read: MetaGet returns an atomic snapshot.
func (c *Core) resolveAsOf(name string) (types.Instant, bool, error) {
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	if err := validateAsOfTagName(name); err != nil {
		return 0, false, err
	}
	mk, err := c.asofMetaKV()
	if err != nil {
		return 0, false, err
	}
	tags, err := loadAsOfTags(mk)
	if err != nil {
		return 0, false, err
	}
	v, ok := tags[name]
	if !ok {
		return 0, false, nil
	}
	return types.Instant(v), true, nil
}

// asOfTags returns a copy of the full name→instant registry.
func (c *Core) asOfTags() (map[string]types.Instant, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	mk, err := c.asofMetaKV()
	if err != nil {
		return nil, err
	}
	raw, err := loadAsOfTags(mk)
	if err != nil {
		return nil, err
	}
	out := make(map[string]types.Instant, len(raw))
	for k, v := range raw {
		out[k] = types.Instant(v)
	}
	return out, nil
}

// reapAsOfTagsForReset clears the durable tag registry as part of Admin.Reset,
// so a reset graph does not leak pre-Clear knowledge marks (lesson 53). It is
// backend-independent: badger's Clear already scan-reaps meta, but memory's
// Clear preserves the whole metaKV — do it here so the contract holds on every
// store. No-op when the backend cannot persist metadata. Serialized with tag
// writers via asofMu (Reset holds c.mu.Lock, but tag writers do not take c.mu).
func (c *Core) reapAsOfTagsForReset() error {
	mk := c.metaKV
	if mk == nil {
		return nil
	}
	c.asofMu.Lock()
	defer c.asofMu.Unlock()
	if err := mk.MetaSet(asofTagsMeta, nil); err != nil {
		return fmt.Errorf("graph: reset as-of tags: %w", err)
	}
	return nil
}

// removeAsOfTag deletes a named mark. Idempotent: removing an absent tag is a
// no-op (nil error). Gated by checkWritable.
func (c *Core) removeAsOfTag(name string) error {
	if err := c.checkWritable(); err != nil {
		return err
	}
	if err := validateAsOfTagName(name); err != nil {
		return err
	}
	mk, err := c.asofMetaKV()
	if err != nil {
		return err
	}
	c.asofMu.Lock()
	defer c.asofMu.Unlock()
	tags, err := loadAsOfTags(mk)
	if err != nil {
		return err
	}
	if _, ok := tags[name]; !ok {
		return nil
	}
	delete(tags, name)
	return storeAsOfTags(mk, tags)
}

// -----------------------------------------------------------------------------
// TempOps surface (g.Temporal())
// -----------------------------------------------------------------------------

// TagAsOf registers (or overwrites) a named knowledge-time (Erkenntniszeit)
// mark: name → a transaction-time instant used with AS OF SYSTEM TIME / TxAt.
// The instant must be positive and the name non-blank; the registry is bounded
// (ErrTooManyAsOfTags). Requires a backend with MetaKV (every in-tree store);
// rejected on a read-only replica (tag on the primary). Durable across restart.
func (t *TempOps) TagAsOf(name string, at types.Instant) error { return t.c.tagAsOf(name, at) }

// ResolveAsOf returns the transaction-time instant registered under name.
// ok is false (nil error) when the tag does not exist.
func (t *TempOps) ResolveAsOf(name string) (types.Instant, bool, error) {
	return t.c.resolveAsOf(name)
}

// AsOfTags returns a snapshot copy of every registered name → instant mark.
func (t *TempOps) AsOfTags() (map[string]types.Instant, error) { return t.c.asOfTags() }

// RemoveAsOfTag deletes a named mark (idempotent — removing an absent tag is a
// no-op). Rejected on a read-only replica.
func (t *TempOps) RemoveAsOfTag(name string) error { return t.c.removeAsOfTag(name) }
