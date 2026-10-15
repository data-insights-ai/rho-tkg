package core

import (
	"context"
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Config.UntimedValidStart (backlog 73): where an entity's first version
// without a valid-from starts in valid time.
//
// The mode is decided once, when a first version is written (untimedStart):
// under UntimedUnbounded the row is stored with ValidFrom =
// types.UnboundedValidFrom, an ordinary explicit start. Every reader takes a
// row's start from the row (storeutil.EntityValidFrom: explicit ValidFrom,
// else TxFrom), so the resolver, the index envelopes (RAM, 0x0B keys, open
// image, tiered per-shard files), the DocValues start column, the valid-end
// candidates, the paged and counting doors and a replica or an export all see
// the mode without being told it, and no persisted derived start depends on
// the configuration. A later version without a valid-from starts at its
// TxFrom in both modes (the update doors clear ValidFrom).

// UntimedValidStart selects the start of an entity's first version written
// without a valid-from (backlog 73).
type UntimedValidStart uint8

const (
	// UntimedFromTxFrom (the default) starts it at its TxFrom (backlog 62).
	UntimedFromTxFrom UntimedValidStart = iota
	// UntimedUnbounded starts it at -inf (types.UnboundedValidFrom): the
	// entity is valid at every instant before its first change. A later
	// version without a valid-from still starts at its TxFrom.
	UntimedUnbounded
)

// untimedValidStartMeta is the MetaKV key persisting the mode a store's rows
// are written under: one byte, 0 = UntimedFromTxFrom, 1 = UntimedUnbounded,
// untimedConverting = a conversion to UntimedUnbounded started and did not
// finish (Admin().SetUntimedValidStart).
const untimedValidStartMeta = "untimed_valid_start"

const untimedConverting byte = 2

// untimedStart is THE place the mode decides: the valid-from a create stores.
// first reports that the row is the entity's first version (a generated ID,
// or an imported ID without an earlier life). An explicit validFrom is kept.
func (c *Core) untimedStart(validFrom types.Instant, first bool) types.Instant {
	if validFrom == 0 && first && c.untimedUnbounded.Load() {
		return types.UnboundedValidFrom
	}
	return validFrom
}

// stampQueuedUntimedStart applies the mode to a queued create (batch, ingest)
// at its commit, under the lock that excludes a conversion: a create queued
// before Admin().SetUntimedValidStart converted the store is written under
// the new mode. Reports whether it changed tm (its pre-encoded row is then
// stale).
func (c *Core) stampQueuedUntimedStart(tm *types.TemporalMetadata) bool {
	if tm == nil || tm.ValidFrom != 0 {
		return false
	}
	vf := c.untimedStart(0, true)
	tm.ValidFrom = vf
	return vf != 0
}

// resolveUntimedMode checks the configured mode against the one the store's
// rows were written under; persistUntimedMode writes it once New passed every
// other check. Rule: a store with rows keeps its mode; a configured mode that
// differs fails New with ErrUntimedModeMismatch. The mode of a store with rows
// is its key, or, without one (written before backlog 73), UntimedFromTxFrom:
// v4.54.0 and later started its untimed rows at their TxFrom, older releases
// at the ID's mint instant, and v4.54.0 already reads both as TxFrom rows (an
// untimed row stores no start). A store without rows takes the configured
// mode, also over a key, and so does a store without stable storage. A key
// naming an unfinished conversion opens only under UntimedUnbounded, which
// finishes it (resumeUntimedConversion).
func (c *Core) resolveUntimedMode() error {
	mk := c.metaKV
	if mk == nil {
		return nil
	}
	v, err := mk.MetaGet(untimedValidStartMeta)
	if err != nil {
		return fmt.Errorf("graph: read untimed valid-start mode: %w", err)
	}
	want := UntimedFromTxFrom
	if c.untimedUnbounded.Load() {
		want = UntimedUnbounded
	}
	hasKey := len(v) == 1
	switch {
	case len(v) > 1 || (hasKey && v[0] > untimedConverting):
		return fmt.Errorf("graph: untimed valid-start mode %x: %w", v, storepkg.ErrCorruptWire)
	case hasKey && v[0] == byte(want):
		return nil
	}
	if df, ok := c.store.(storepkg.DurableFlushCapability); !ok || !df.DurableFlushSupported() {
		c.untimedModeStamp = true // never reopened: no earlier mode
		return nil
	}
	_, found, err := c.sampleRowHashed()
	if err != nil {
		return fmt.Errorf("graph: untimed valid-start mode: %w", err)
	}
	switch {
	case !found:
		c.untimedModeStamp = true // an empty store takes the configured mode
	case hasKey && v[0] == untimedConverting:
		if want != UntimedUnbounded {
			return fmt.Errorf("graph: the store's conversion to UntimedUnbounded did not finish; open it with Config.UntimedValidStart = UntimedUnbounded to finish it: %w", ErrUntimedModeMismatch)
		}
		if c.readOnlyReplica {
			return fmt.Errorf("graph: the store's conversion to UntimedUnbounded did not finish; a writable open finishes it: %w", ErrUntimedModeMismatch)
		}
		c.untimedResume = true
	case hasKey:
		return untimedModeMismatch(UntimedValidStart(v[0]), want)
	case want != UntimedFromTxFrom:
		return untimedModeMismatch(UntimedFromTxFrom, want)
	default:
		c.untimedModeStamp = true
	}
	return nil
}

func untimedModeName(m UntimedValidStart) string {
	if m == UntimedUnbounded {
		return "UntimedUnbounded"
	}
	return "UntimedFromTxFrom"
}

func untimedModeMismatch(stored, want UntimedValidStart) error {
	hint := ""
	if stored == UntimedFromTxFrom {
		hint = "; convert it once with Admin().SetUntimedValidStart(UntimedUnbounded)"
	}
	return fmt.Errorf("graph: the store's rows are written under %s, Config.UntimedValidStart is %s%s: %w",
		untimedModeName(stored), untimedModeName(want), hint, ErrUntimedModeMismatch)
}

// resumeUntimedConversion finishes a conversion an earlier session started
// (resolveUntimedMode set untimedResume), after the registries are loaded.
func (c *Core) resumeUntimedConversion() error {
	if !c.untimedResume {
		return nil
	}
	if err := c.stampUntimedFirstVersions(context.Background()); err != nil {
		return fmt.Errorf("graph: finish the conversion to UntimedUnbounded: %w", err)
	}
	c.untimedModeStamp = true
	return nil
}

// persistUntimedMode writes the mode resolveUntimedMode decided to stamp,
// durably (rows of a tiered or sharded store land on other shards than the
// key). A read-only store cannot stamp; nothing is written under the mode.
func (c *Core) persistUntimedMode() error {
	if !c.untimedModeStamp {
		return nil
	}
	if err := c.writeUntimedMode(c.untimedModeByte()); err != nil {
		if errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			return nil
		}
		return fmt.Errorf("graph: persist untimed valid-start mode: %w", err)
	}
	return nil
}

func (c *Core) untimedModeByte() byte {
	if c.untimedUnbounded.Load() {
		return byte(UntimedUnbounded)
	}
	return byte(UntimedFromTxFrom)
}

func (c *Core) writeUntimedMode(b byte) error {
	if c.metaKV == nil {
		return nil
	}
	val := []byte{b}
	if d, ok := c.metaKV.(interface {
		MetaSetDurable(key string, value []byte) error
	}); ok {
		return d.MetaSetDurable(untimedValidStartMeta, val)
	}
	return c.metaKV.MetaSet(untimedValidStartMeta, val)
}

// restoreUntimedModeAfterReset re-persists the mode after Admin.Reset() or a
// replica's ChangeClear apply wiped the MetaKV keyspace (reapPolicyPreserve):
// the graph keeps writing under its mode. Caller holds c.mu.Lock.
func (c *Core) restoreUntimedModeAfterReset() error {
	if err := c.writeUntimedMode(c.untimedModeByte()); err != nil {
		return fmt.Errorf("graph: restore untimed valid-start mode after reset: %w", err)
	}
	return nil
}

// SetUntimedValidStart converts the store's untimed first versions to mode
// (backlog 73). The conversion is one way, from UntimedFromTxFrom to
// UntimedUnbounded: it stamps types.UnboundedValidFrom on every entity's
// first version (version 0) that has no valid-from, current or history, node
// and relationship, through the store doors that maintain every index
// (envelopes, the 0x0B keys, the DocValues columns), then persists the mode
// and writes later creates under it. Rows with a valid-from and later
// versions are untouched. The graph is locked for the duration (one row read
// per entity, one row write per untimed first version).
//
// Crash safety: the store is marked "converting" durably before the first
// row changes; a reopen under UntimedFromTxFrom then fails with
// ErrUntimedModeMismatch and one under UntimedUnbounded finishes the
// conversion. A store already in mode is stamped again (idempotent: rows
// copied in verbatim since, e.g. by a restore, are converted). Reopen the
// store with Config.UntimedValidStart = UntimedUnbounded afterwards.
//
// Errors: ErrInvalidUntimedValidStart (a mode out of range),
// ErrUntimedModeMismatch (UntimedFromTxFrom on an UntimedUnbounded graph),
// ErrGraphClosed, ErrReadOnlyReplica, ctx's error, a store error (the
// conversion stays marked unfinished; call again).
func (a *AdminOps) SetUntimedValidStart(ctx context.Context, mode UntimedValidStart) error {
	c := a.c
	if mode > UntimedUnbounded {
		return ErrInvalidUntimedValidStart
	}
	if err := c.checkWritable(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	if mode == UntimedFromTxFrom {
		if c.untimedUnbounded.Load() {
			return fmt.Errorf("graph: a store converted to UntimedUnbounded cannot go back: %w", ErrUntimedModeMismatch)
		}
		return nil
	}
	if !c.untimedUnbounded.Load() {
		if err := c.writeUntimedMode(untimedConverting); err != nil {
			return fmt.Errorf("graph: mark the untimed valid-start conversion: %w", err)
		}
	}
	// Rewritten history rows change answers at earlier pins: drop the cached
	// as-of columns and state cuts, also after a conversion cut short.
	defer c.asOfColumns.bump()
	if err := c.stampUntimedFirstVersions(ctx); err != nil {
		return err
	}
	c.untimedUnbounded.Store(true)
	if err := c.writeUntimedMode(byte(UntimedUnbounded)); err != nil {
		return fmt.Errorf("graph: persist untimed valid-start mode: %w", err)
	}
	return nil
}

// stampUntimedFirstVersions rewrites every untimed first version (node and
// relationship, current or history) with ValidFrom = types.UnboundedValidFrom.
// Caller holds c.mu.Lock (or runs inside New).
func (c *Core) stampUntimedFirstVersions(ctx context.Context) error {
	var nodeIDs []types.NodeID
	collect := func(id types.NodeID) bool { nodeIDs = append(nodeIDs, id); return true }
	if err := c.store.ForEachNodeID(collect); err != nil {
		return fmt.Errorf("iterate nodes: %w", err)
	}
	if err := c.store.ForEachNodeHistoryID(collect); err != nil {
		return fmt.Errorf("iterate node history: %w", err)
	}
	for _, id := range dedupIDs(nodeIDs) {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		if err := c.stampNodeFirstVersion(id); err != nil {
			return fmt.Errorf("node %d: %w", id, err)
		}
	}
	var relIDs []types.RelID
	collectRel := func(id types.RelID) bool { relIDs = append(relIDs, id); return true }
	if err := c.store.ForEachRelID(collectRel); err != nil {
		return fmt.Errorf("iterate relationships: %w", err)
	}
	if err := c.store.ForEachRelHistoryID(collectRel); err != nil {
		return fmt.Errorf("iterate relationship history: %w", err)
	}
	for _, id := range dedupIDs(relIDs) {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		if err := c.stampRelFirstVersion(id); err != nil {
			return fmt.Errorf("relationship %d: %w", id, err)
		}
	}
	return nil
}

func dedupIDs[T ~int64](ids []T) []T {
	seen := make(map[T]struct{}, len(ids))
	out := ids[:0]
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// untimedFirst reports whether row, the first row of its chain in write
// order, is a first version without a valid-from.
func untimedFirst(version uint32, tm *types.TemporalMetadata) bool {
	return version == 0 && tm != nil && tm.ValidFrom == 0 && !tm.Void
}

func (c *Core) stampNodeFirstVersion(id types.NodeID) error {
	history, err := c.getNodeHistory(id)
	if err != nil {
		return err
	}
	current, err := c.getCurrentNode(id)
	if errors.Is(err, storepkg.ErrNodeNotFound) {
		current = nil
	} else if err != nil {
		return err
	}
	chain := append([]*types.Node(nil), history...)
	if current != nil {
		chain = append(chain, current)
	}
	if len(chain) == 0 {
		return nil
	}
	first := versionOrdered(chain)[0]
	if !untimedFirst(first.Version(), first.Temporal()) {
		return nil
	}
	row := first.DeepCopy()
	row.Temporal().ValidFrom = types.UnboundedValidFrom
	if first == current {
		return c.store.ReplaceNode(row)
	}
	return c.store.PutNodeVersion(id, row.Version(), row)
}

func (c *Core) stampRelFirstVersion(id types.RelID) error {
	history, err := c.getRelHistory(id)
	if errors.Is(err, storepkg.ErrSlotNotLocal) {
		return nil
	}
	if err != nil {
		return err
	}
	current, err := c.getCurrentRelationship(id)
	if errors.Is(err, storepkg.ErrRelNotFound) || errors.Is(err, storepkg.ErrSlotNotLocal) {
		current = nil
	} else if err != nil {
		return err
	}
	chain := append([]*types.Relationship(nil), history...)
	if current != nil {
		chain = append(chain, current)
	}
	if len(chain) == 0 {
		return nil
	}
	first := versionOrdered(chain)[0]
	if !untimedFirst(first.Version(), first.Temporal()) {
		return nil
	}
	row := first.DeepCopy()
	row.Temporal().ValidFrom = types.UnboundedValidFrom
	if first == current {
		return c.store.ReplaceRelationship(row)
	}
	return c.store.PutRelVersion(id, row.Version(), row)
}
