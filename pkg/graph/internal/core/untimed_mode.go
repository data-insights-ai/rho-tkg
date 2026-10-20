package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Config.UntimedValidStart (backlog 73, round 11): what a write without a
// valid time claims, and how the store's rows record valid time.
//
// The legacy modes decide once, when a first version is written
// (untimedStart): under UntimedUnbounded the row is stored with ValidFrom =
// types.UnboundedValidFrom, an ordinary explicit start. Every reader takes a
// row's start from the row (storeutil.EntityValidFrom: explicit ValidFrom,
// else TxFrom), so the resolver, the index envelopes (RAM, 0x0B keys, open
// image, tiered per-shard files), the DocValues start column, the valid-end
// candidates, the paged and counting doors and a replica or an export all see
// the mode without being told it, and no persisted derived start depends on
// the configuration. A later version without a valid-from starts at its
// TxFrom in both legacy modes (the update doors clear ValidFrom).
//
// UntimedWholeAxis (round 11) is the clean semantics: no write derives a
// valid time from its transaction time, and the store's rows are on the
// literal valid axis (types.LiteralValidAxis): every stored ValidFrom and
// ValidTo is the instant it names (0 included), an open end is
// types.InfiniteValidTo. The axis is the store's, fixed when it opens from its
// persisted mode (store.ValidAxisCapability), never a per-row flag.

// UntimedValidStart selects what a write without a valid time claims
// (backlog 73, round 11).
type UntimedValidStart uint8

const (
	// UntimedDefault (the zero value, v4.57.0) keeps the mode an existing
	// store was written under (its persisted mode; UntimedFromTxFrom for a
	// store with rows and no persisted mode) and makes a new store an
	// UntimedUnbounded store. It is a Config value only:
	// Admin().SetUntimedValidStart names the target mode.
	//
	// Announced (round 11): in a later minor (held after v4.66.0, no
	// target release) a new store under
	// UntimedDefault is an UntimedWholeAxis store (when its store implements
	// store.ValidAxisCapability; memory, badger, tiered and sharded do).
	// Existing stores keep their persisted mode. Set UntimedUnbounded
	// explicitly to keep today's default for new stores.
	UntimedDefault UntimedValidStart = iota
	// UntimedFromTxFrom starts it at its TxFrom (backlog 62); the default
	// for new stores up to v4.56.x. Legacy valid axis.
	UntimedFromTxFrom
	// UntimedUnbounded starts it at -inf (types.UnboundedValidFrom): the
	// entity is valid at every instant before its first change. A later
	// version without a valid-from still starts at its TxFrom. The default
	// for new stores from v4.57.0. Legacy valid axis.
	UntimedUnbounded
	// UntimedWholeAxis (round 11, fixed at store creation) is the clean
	// semantics: a write without a valid time makes no valid-time claim, and
	// no claim means the whole axis, (-inf, +inf). An untimed create holds
	// over the whole axis (types.UnboundedValidFrom, types.InfiniteValidTo);
	// an untimed update applies to every valid-time segment of the entity and
	// keeps the segment boundaries (SQL:2011's UPDATE without FOR PORTION
	// OF): each segment gets a new version in the new belief, the old
	// versions end in transaction time only, existence does not change. An
	// untimed delete ends the entity in transaction time only (a
	// retraction). Writes with a valid time keep their rules. The store's
	// rows are on the literal valid axis (types.LiteralValidAxis): 0 is an
	// ordinary instant, claimable as a start and as an end.
	//
	// Named for what an untimed write claims (the whole axis), not for the
	// entity, which is temporal like any other (round 11 called it
	// UntimedAtemporal).
	UntimedWholeAxis
)

// untimedValidStartMeta is the MetaKV key persisting the mode a store's rows
// are written under: one byte, untimedByteTxFrom, untimedByteUnbounded,
// untimedConverting (a conversion to UntimedUnbounded started and did not
// finish, Admin().SetUntimedValidStart) or untimedByteWholeAxis. The bytes 0
// to 2 are the v4.56.0 encoding; they are not the UntimedValidStart values
// (v4.57.0 put UntimedDefault at 0). A store reads it at open to fix its
// valid axis (store.ValidAxisOfModeByte).
// It equals store.UntimedValidStartMetaKey (TestUntimedModeKeyIsTheStores),
// spelled here as a literal for the MetaKV reap-policy check.
const untimedValidStartMeta = "untimed_valid_start"

const (
	untimedByteTxFrom    byte = 0
	untimedByteUnbounded byte = 1
	untimedConverting    byte = 2
	untimedByteWholeAxis      = storepkg.UntimedWholeAxisModeByte // round 11
)

// untimedModeOfByte is the mode a persisted key byte (not converting) names.
func untimedModeOfByte(b byte) UntimedValidStart {
	switch b {
	case untimedByteTxFrom:
		return UntimedFromTxFrom
	case untimedByteWholeAxis:
		return UntimedWholeAxis
	}
	return UntimedUnbounded
}

// untimedByteValid reports whether b is a persisted mode byte this release
// reads.
func untimedByteValid(b byte) bool { return b <= untimedByteWholeAxis }

// untimedMode is the mode the graph writes under (UntimedFromTxFrom,
// UntimedUnbounded or UntimedWholeAxis), from the two flags; setUntimedMode
// sets both together with the valid axis.
func (c *Core) untimedMode() UntimedValidStart {
	if c.untimedWholeAxis.Load() {
		return UntimedWholeAxis
	}
	return untimedModeOf(c.untimedUnbounded.Load())
}

// setUntimedMode sets the graph's mode and its valid axis (the literal axis
// under UntimedWholeAxis). The store follows with syncStoreAxis once the mode
// is decided.
func (c *Core) setUntimedMode(mode UntimedValidStart) {
	c.untimedWholeAxis.Store(mode == UntimedWholeAxis)
	c.untimedUnbounded.Store(mode == UntimedUnbounded || mode == UntimedWholeAxis)
	if mode == UntimedWholeAxis {
		c.validAxis.Set(types.LiteralValidAxis)
	} else {
		c.validAxis.Set(types.LegacyValidAxis)
	}
}

// axis is the graph's valid axis (round 11): how its rows record valid time.
func (c *Core) axis() types.ValidAxis { return c.validAxis.Get() }

// syncStoreAxis hands the graph's valid axis to its store
// (store.ValidAxisCapability), after the mode is decided at open or changed
// on an empty store (an import adopting its source's mode, a replica
// following its primary). A store without the capability reads every row on
// the legacy axis, so it cannot hold an UntimedWholeAxis graph
// (ErrCapabilityNotSupported).
func (c *Core) syncStoreAxis() error {
	if va, ok := c.store.(storepkg.ValidAxisCapability); ok {
		va.SetValidAxis(c.axis())
		// Read back (pattern: a capability is what the call does, not the
		// method set): a store that does not keep the axis reads rows on
		// another one.
		if got := va.ValidAxis(); got != c.axis() {
			return fmt.Errorf("graph: the store kept valid axis %+v, the graph's is %+v: %w", got, c.axis(), storepkg.ErrCapabilityNotSupported)
		}
		return nil
	}
	if c.axis().Literal() {
		return fmt.Errorf("graph: Config.UntimedValidStart = UntimedWholeAxis needs a store that implements store.ValidAxisCapability: %w", storepkg.ErrCapabilityNotSupported)
	}
	return nil
}

// untimedStart is THE place the mode decides the valid-from of a create that
// claims none: first reports that the row is the entity's first version (a
// generated ID, or an imported ID without an earlier life). While a
// conversion is unfinished (untimedIncomplete) an untimed first version is
// refused: the store holds rows of both rules.
func (c *Core) untimedStart(first bool) (types.Instant, error) {
	if first {
		if err := c.checkUntimedComplete("create an untimed first version"); err != nil {
			return 0, err
		}
	}
	return c.untimedStartMode(first), nil
}

// untimedStartMode is untimedStart's mode rule without the
// unfinished-conversion guard (a dry run; a commit-time restamp whose batch
// passed precheckQueuedUntimedCreates): types.UnboundedValidFrom for every
// untimed create under UntimedWholeAxis (no claim is the whole axis, also for
// an imported ID with an earlier life) and for a first version under
// UntimedUnbounded, else 0 (the legacy axis's "no claim": the row starts at
// its TxFrom).
func (c *Core) untimedStartMode(first bool) types.Instant {
	if c.untimedWholeAxis.Load() || first && c.untimedUnbounded.Load() {
		return types.UnboundedValidFrom
	}
	return 0
}

// checkUntimedComplete fails closed with ErrUntimedModeMismatch while a
// conversion to UntimedUnbounded is unfinished (the store is marked
// converting, part of its untimed first versions may still start at their
// TxFrom). op names the refused operation.
func (c *Core) checkUntimedComplete(op string) error {
	if !c.untimedIncomplete.Load() {
		return nil
	}
	return fmt.Errorf("graph: %s: the conversion to UntimedUnbounded did not finish; call Admin().SetUntimedValidStart(ctx, UntimedUnbounded) again: %w", op, ErrUntimedModeMismatch)
}

// precheckQueuedUntimedCreates refuses a batch or ingest group, before any
// write, that would create an untimed first version while a conversion is
// unfinished (a create queued before the conversion was cut short).
func (c *Core) precheckQueuedUntimedCreates(nodes []pendingNode, rels []pendingRel) error {
	if !c.untimedIncomplete.Load() {
		return nil
	}
	a := c.axis()
	for i := range nodes {
		if tm := nodes[i].temporal; tm != nil && !a.Claimed(tm.ValidFrom) {
			return c.checkUntimedComplete("create an untimed first version")
		}
	}
	for i := range rels {
		if tm := rels[i].temporal; tm != nil && !a.Claimed(tm.ValidFrom) {
			return c.checkUntimedComplete("create an untimed first version")
		}
	}
	return nil
}

// stampQueuedUntimedStart applies the mode to a queued create (batch, ingest)
// at its commit, under the lock that excludes a conversion: a create queued
// before Admin().SetUntimedValidStart converted the store is written under
// the new mode. Reports whether it changed tm (its pre-encoded row is then
// stale). Only a legacy-axis create can be unclaimed.
func (c *Core) stampQueuedUntimedStart(tm *types.TemporalMetadata) bool {
	if tm == nil || c.axis().Claimed(tm.ValidFrom) {
		return false
	}
	vf := c.untimedStartMode(true)
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
//
// Under UntimedDefault (defaultMode) the mode is the store's: its key (a key
// naming an unfinished conversion is UntimedUnbounded, and the open finishes
// it), else UntimedFromTxFrom for a durable store with rows (written before
// v4.56.0), else UntimedUnbounded (a new store, or one without stable
// storage: the v4.57.0 default). A store without MetaKV cannot persist the
// mode and stays UntimedFromTxFrom. untimedUnbounded is set from the decision.
func (c *Core) resolveUntimedMode(defaultMode bool) error {
	mk := c.metaKV
	if mk == nil {
		// Without MetaKV the mode cannot be persisted, so no session can
		// tell a new store from one an earlier session wrote: the default
		// stays UntimedFromTxFrom (the pre-v4.57.0 zero value) in every
		// session, never a mix of both rules.
		if defaultMode {
			c.setUntimedMode(UntimedFromTxFrom)
		}
		if c.untimedWholeAxis.Load() {
			// A later session could not tell how the rows record valid
			// time (the store's axis is read from the persisted mode).
			return fmt.Errorf("graph: Config.UntimedValidStart = UntimedWholeAxis needs a store that persists the mode (store.MetaKVCapability): %w", storepkg.ErrCapabilityNotSupported)
		}
		return nil
	}
	v, err := mk.MetaGet(untimedValidStartMeta)
	if err != nil {
		return fmt.Errorf("graph: read untimed valid-start mode: %w", err)
	}
	hasKey := len(v) == 1
	if len(v) > 1 || (hasKey && !untimedByteValid(v[0])) {
		return fmt.Errorf("graph: untimed valid-start mode %x: %w", v, storepkg.ErrCorruptWire)
	}
	if defaultMode {
		mode := UntimedUnbounded
		if hasKey {
			mode = untimedModeOfByte(v[0])
		} else if df, ok := c.store.(storepkg.DurableFlushCapability); ok && df.DurableFlushSupported() {
			// A store without stable storage was never written by an
			// earlier session: it is new.
			_, found, err := c.sampleRowHashed()
			if err != nil {
				return fmt.Errorf("graph: untimed valid-start mode: %w", err)
			}
			if found {
				mode = UntimedFromTxFrom
			}
		}
		c.setUntimedMode(mode)
	}
	want := c.untimedMode()
	if hasKey && v[0] == untimedModeByteOf(want) {
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
		c.untimedIncomplete.Store(true) // until the conversion finishes
		if c.readOnlyReplica {
			return nil // the primary's feed carries the rest of the conversion
		}
		c.untimedResume = true
	case hasKey:
		return untimedModeMismatch(untimedModeOfByte(v[0]), want)
	case want != UntimedFromTxFrom:
		return untimedModeMismatch(UntimedFromTxFrom, want)
	default:
		c.untimedModeStamp = true
	}
	return nil
}

func untimedModeName(m UntimedValidStart) string {
	switch m {
	case UntimedUnbounded:
		return "UntimedUnbounded"
	case UntimedWholeAxis:
		return "UntimedWholeAxis"
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
	if err := c.flushUntimedRows(); err != nil {
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
	return untimedModeByteOf(c.untimedMode())
}

// untimedModeByteOf is the persisted key byte of mode (UntimedFromTxFrom or
// UntimedUnbounded).
func untimedModeByteOf(mode UntimedValidStart) byte {
	switch mode {
	case UntimedUnbounded:
		return untimedByteUnbounded
	case UntimedWholeAxis:
		return untimedByteWholeAxis
	}
	return untimedByteTxFrom
}

// writeUntimedMode persists the mode key b and, once it is written, mirrors
// it in untimedIncomplete (b == untimedConverting): the in-process guard and
// the persisted key change together.
func (c *Core) writeUntimedMode(b byte) error {
	return c.writeUntimedModeLogged(b, false)
}

// writeUntimedModeLogged is writeUntimedMode; logTxFrom also writes a
// UntimedFromTxFrom key through the change-logged path (without raising the
// format marker), for a store whose feed may already carry an unbounded
// ChangeMeta record (an import adopting a TxFrom export into a store whose
// first open persisted UntimedUnbounded): a replica must follow the switch.
func (c *Core) writeUntimedModeLogged(b byte, logTxFrom bool) error {
	if logTxFrom && b == untimedByteTxFrom && c.metaKV != nil {
		if fm, ok := c.metaKV.(storepkg.MetaFormatMarkerCapability); ok {
			if err := fm.MetaSetDurableRaising(untimedValidStartMeta, []byte{b}, 0); err != nil {
				return err
			}
			c.untimedIncomplete.Store(false)
			return nil
		}
	}
	if err := c.writeUntimedModeKey(b); err != nil {
		return err
	}
	c.untimedIncomplete.Store(b == untimedConverting)
	return nil
}

func (c *Core) writeUntimedModeKey(b byte) error {
	if c.metaKV == nil {
		return nil
	}
	val := []byte{b}
	// An unbounded (or converting) store raises the store format marker to 7
	// with the key, so a release that ignores the key refuses the store
	// instead of writing TxFrom-start creates into it; a TxFrom store keeps
	// its marker.
	if fm, ok := c.metaKV.(storepkg.MetaFormatMarkerCapability); ok && b != untimedByteTxFrom {
		return fm.MetaSetDurableRaising(untimedValidStartMeta, val, storeutil.UntimedUnboundedStoreFormatVersion)
	}
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
// conversion. While the mark holds (untimedIncomplete, also in a process
// whose call was cut short), exports, imports and creates of an untimed first
// version fail with ErrUntimedModeMismatch. A store already in mode is
// marked and stamped again (idempotent: rows
// copied in verbatim since, e.g. by a restore, are converted). Reopen the
// store with Config.UntimedValidStart = UntimedUnbounded afterwards.
//
// Errors: ErrInvalidUntimedValidStart (a mode out of range),
// ErrUntimedModeMismatch (UntimedFromTxFrom on an UntimedUnbounded graph),
// ErrGraphClosed, ErrReadOnlyReplica, ctx's error, a store error (the
// conversion stays marked unfinished; call again).
func (a *AdminOps) SetUntimedValidStart(ctx context.Context, mode UntimedValidStart) error {
	c := a.c
	if mode == UntimedDefault || mode > UntimedUnbounded {
		// UntimedWholeAxis is fixed at store creation: no conversion names it.
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
	if c.untimedWholeAxis.Load() {
		return fmt.Errorf("graph: an UntimedWholeAxis store keeps its mode (fixed at creation): %w", ErrUntimedModeMismatch)
	}
	if mode == UntimedFromTxFrom {
		if c.untimedUnbounded.Load() {
			return fmt.Errorf("graph: a store converted to UntimedUnbounded cannot go back: %w", ErrUntimedModeMismatch)
		}
		return c.checkUntimedComplete("go back to UntimedFromTxFrom")
	}
	// Marked on an UntimedUnbounded store too (a re-stamp of rows copied in
	// verbatim): a re-stamp cut short also leaves rows of both rules.
	if err := c.writeUntimedMode(untimedConverting); err != nil {
		return fmt.Errorf("graph: mark the untimed valid-start conversion: %w", err)
	}
	// Rewritten history rows change answers at earlier pins: drop the cached
	// as-of columns and state cuts, also after a conversion cut short.
	defer c.asOfColumns.bump()
	if err := c.stampUntimedFirstVersions(ctx); err != nil {
		return err
	}
	// The rewritten rows are on disk on every shard before the final key
	// says the conversion is done; a crash before it leaves the store marked
	// converting, and the next unbounded open finishes it.
	if err := c.flushUntimedRows(); err != nil {
		return fmt.Errorf("graph: flush the converted rows: %w", err)
	}
	c.untimedUnbounded.Store(true)
	if err := c.writeUntimedMode(untimedByteUnbounded); err != nil {
		return fmt.Errorf("graph: persist untimed valid-start mode: %w", err)
	}
	return nil
}

// flushUntimedRows makes the rows a conversion rewrote durable on every shard
// (store.DurableFlushCapability) before the final mode key is written; a
// store without stable storage flushes its write buffer, one without either
// has nothing buffered.
func (c *Core) flushUntimedRows() error {
	if df, ok := c.store.(storepkg.DurableFlushCapability); ok && df.DurableFlushSupported() {
		return df.DurableFlush()
	}
	if f, ok := c.store.(interface{ Flush() error }); ok {
		return f.Flush()
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
	return c.stampForeignIncomingStubs(ctx)
}

// foreignIncomingStubs is the sharded store's view of its Model-A incoming
// stubs (ADR-0010): rows carrying another machine's edge verbatim.
type foreignIncomingStubs interface {
	ForEachForeignIncoming(fn func(*types.Relationship) bool) error
	ReplaceForeignIncoming(r *types.Relationship) error
}

// stampForeignIncomingStubs stamps every stub of an untimed first version: the
// START machine's conversion stamps the authoritative edge the same way, so
// both machines read the edge from -inf. The slot-routed relationship pass
// above cannot reach a stub (its ID's slot is foreign).
func (c *Core) stampForeignIncomingStubs(ctx context.Context) error {
	fs, ok := c.store.(foreignIncomingStubs)
	if !ok {
		return nil
	}
	var stubs []*types.Relationship
	if err := fs.ForEachForeignIncoming(func(r *types.Relationship) bool {
		if untimedFirst(c.axis(), r.Version(), r.Temporal()) {
			stubs = append(stubs, r)
		}
		return true
	}); err != nil {
		return fmt.Errorf("iterate foreign-incoming stubs: %w", err)
	}
	for _, r := range stubs {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		row := r.DeepCopy()
		row.Temporal().ValidFrom = types.UnboundedValidFrom
		if err := fs.ReplaceForeignIncoming(row); err != nil {
			return fmt.Errorf("foreign-incoming stub %d: %w", r.ID(), err)
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
// order, is a first version without a valid-from (on the valid axis a: only
// the legacy axis has unclaimed starts).
func untimedFirst(a types.ValidAxis, version uint32, tm *types.TemporalMetadata) bool {
	return version == 0 && tm != nil && !a.Claimed(tm.ValidFrom) && !tm.Void
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
	chain := chainOf(history, current)
	if len(chain) == 0 {
		return nil
	}
	first := versionOrdered(chain)[0]
	if !untimedFirst(c.axis(), first.Version(), first.Temporal()) {
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
	chain := chainOf(history, current)
	if len(chain) == 0 {
		return nil
	}
	first := versionOrdered(chain)[0]
	if !untimedFirst(c.axis(), first.Version(), first.Temporal()) {
		return nil
	}
	row := first.DeepCopy()
	row.Temporal().ValidFrom = types.UnboundedValidFrom
	if first == current {
		return c.store.ReplaceRelationship(row)
	}
	return c.store.PutRelVersion(id, row.Version(), row)
}

// applyMetaLocked applies a replicated ChangeMeta record (a replica's apply,
// caller holds c.mu.Lock). A primary replicates the untimed valid-start mode
// (backlog 73), which the replica takes as its own and persists like the
// primary (key, format marker 7), with its valid axis (round 11). Any other
// key fails closed: this release does not know what applying it means.
func (c *Core) applyMetaLocked(body storeutil.MetaBody) error {
	if body.Key != untimedValidStartMeta {
		return fmt.Errorf("graph: apply: unknown replicated meta key %q: %w", body.Key, storepkg.ErrCorruptWire)
	}
	if len(body.Value) != 1 || !untimedByteValid(body.Value[0]) {
		return fmt.Errorf("graph: apply: untimed valid-start mode %x: %w", body.Value, storepkg.ErrCorruptWire)
	}
	v := body.Value[0]
	c.asOfColumns.bump() // the conversion's rewritten history rows follow in the feed
	// The persisted key and the converting flag first: a lock-free reader
	// (Admin().UntimedValidStart) may see a mode one moment stale, never a
	// converting replica as complete.
	return c.switchUntimedModeLocked(untimedModeOfByte(v), func() error { return c.writeUntimedMode(v) })
}

// UntimedValidStart answers the untimed valid-start mode the store's rows are
// written under (round 11): the mode persisted in the store and in force for
// this graph, UntimedFromTxFrom, UntimedUnbounded or UntimedWholeAxis (never UntimedDefault,
// which a Config resolves at open), and complete = false while a conversion
// to UntimedUnbounded is unfinished (Admin().SetUntimedValidStart cut short:
// the store holds untimed first versions of both rules, and mode is the
// conversion's target). A replica answers its own open-time mode until the
// primary's mode record arrives in the feed (a conversion, or a mode an import
// adopted); a TxFrom primary emits none, so open its replica with
// UntimedFromTxFrom. After Admin().Reset() during an unfinished conversion the
// empty store answers UntimedFromTxFrom, complete (the target is not kept).
// ErrGraphClosed on a closed graph.
func (a *AdminOps) UntimedValidStart() (mode UntimedValidStart, complete bool, err error) {
	c := a.c
	if c.closed.Load() {
		return 0, false, ErrGraphClosed
	}
	if c.untimedIncomplete.Load() {
		return UntimedUnbounded, false, nil
	}
	return c.untimedMode(), true, nil
}

// ValidAxis answers the valid axis the graph's rows are on (round 11):
// types.LiteralValidAxis under UntimedWholeAxis, else types.LegacyValidAxis.
// Fixed at open (the store's persisted mode); it changes only when an empty
// store adopts an import's or a primary's mode. ErrGraphClosed on a closed
// graph.
func (a *AdminOps) ValidAxis() (types.ValidAxis, error) {
	if a.c.closed.Load() {
		return types.ValidAxis{}, ErrGraphClosed
	}
	return a.c.axis(), nil
}

func untimedModeOf(unbounded bool) UntimedValidStart {
	if unbounded {
		return UntimedUnbounded
	}
	return UntimedFromTxFrom
}

// adoptImportUntimedModeLocked admits a full import's untimed mode src at
// its header, before the first row is written. A target that holds rows
// refuses a source of another mode (ErrUntimedModeMismatch): a store never
// mixes the two rules. An empty target takes src through
// switchUntimedModeLocked, persisted with a change-logged key so a replica
// tailing the feed switches before the rows too, and rollback restores the
// target's mode when the import fails later. An export without the header
// field (a UntimedFromTxFrom source, or one written before backlog 73) is a
// TxFrom export. Caller holds c.mu.Lock.
func (c *Core) adoptImportUntimedModeLocked(src UntimedValidStart, rollback *importRollback) error {
	if err := c.checkUntimedComplete("import"); err != nil {
		return err
	}
	prior := c.untimedMode()
	if src == prior {
		return nil
	}
	empty := rollback != nil && rollback.emptyTarget
	if rollback == nil {
		var err error
		if empty, err = c.importTargetEmptyLocked(); err != nil {
			return err
		}
	}
	if !empty {
		return fmt.Errorf("import: the export's rows are written under %s, this store's under %s: %w",
			untimedModeName(src), untimedModeName(prior), ErrUntimedModeMismatch)
	}
	if rollback != nil {
		rollback.priorMode, rollback.modeAdopted = prior, true
	}
	if err := c.switchUntimedModeLocked(src, func() error {
		return c.writeUntimedModeLogged(untimedModeByteOf(src), true)
	}); err != nil {
		return fmt.Errorf("import: adopt the source's untimed valid-start mode: %w", err)
	}
	return nil
}

// switchUntimedModeLocked is THE place the mode, and with it the valid axis,
// changes after open: an import adopting its source's mode, a replica
// applying its primary's ChangeMeta record, an import rollback restoring the
// target's mode. The stores build valid-time envelopes, history buckets and
// tx-member spans on their axis as each row is written and rebuild nothing in
// SetValidAxis, so an axis change needs an empty store: the switch runs
// before the first row, and a store with rows refuses it
// (ErrUntimedModeMismatch) before persist runs. persist (nil = none) writes
// the mode key; then the graph and its store take the mode. A store that
// does not keep the axis gets the previous mode back. Caller holds c.mu.Lock.
func (c *Core) switchUntimedModeLocked(mode UntimedValidStart, persist func() error) error {
	prior := c.untimedMode()
	if (mode == UntimedWholeAxis) != (prior == UntimedWholeAxis) {
		empty, err := c.importTargetEmptyLocked()
		if err != nil {
			return err
		}
		if !empty {
			return fmt.Errorf("graph: the store holds rows on the valid axis of %s, it cannot switch to %s: %w",
				untimedModeName(prior), untimedModeName(mode), ErrUntimedModeMismatch)
		}
	}
	if persist != nil {
		if err := persist(); err != nil {
			return err
		}
	}
	c.setUntimedMode(mode)
	if err := c.syncStoreAxis(); err != nil {
		c.setUntimedMode(prior)
		_ = c.syncStoreAxis()
		return err
	}
	return nil
}

// untimedModeOfHeader is the mode an export header names (uu, ua).
func untimedModeOfHeader(unbounded, wholeAxis bool) UntimedValidStart {
	if wholeAxis {
		return UntimedWholeAxis
	}
	return untimedModeOf(unbounded)
}
