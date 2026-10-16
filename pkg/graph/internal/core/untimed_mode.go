package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
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
	// UntimedDefault (the zero value, v4.57.0) keeps the mode an existing
	// store was written under (its persisted mode; UntimedFromTxFrom for a
	// store with rows and no persisted mode) and makes a new store an
	// UntimedUnbounded store. It is a Config value only:
	// Admin().SetUntimedValidStart names the target mode.
	UntimedDefault UntimedValidStart = iota
	// UntimedFromTxFrom starts it at its TxFrom (backlog 62); the default
	// for new stores up to v4.56.x.
	UntimedFromTxFrom
	// UntimedUnbounded starts it at -inf (types.UnboundedValidFrom): the
	// entity is valid at every instant before its first change. A later
	// version without a valid-from still starts at its TxFrom. The default
	// for new stores from v4.57.0.
	UntimedUnbounded
)

// untimedValidStartMeta is the MetaKV key persisting the mode a store's rows
// are written under: one byte, untimedByteTxFrom, untimedByteUnbounded or
// untimedConverting (a conversion to UntimedUnbounded started and did not
// finish, Admin().SetUntimedValidStart). The bytes are the v4.56.0 encoding;
// they are not the UntimedValidStart values (v4.57.0 put UntimedDefault at 0).
const untimedValidStartMeta = "untimed_valid_start"

const (
	untimedByteTxFrom    byte = 0
	untimedByteUnbounded byte = 1
	untimedConverting    byte = 2
)

// untimedModeOfByte is the mode a persisted key byte (not converting) names.
func untimedModeOfByte(b byte) UntimedValidStart {
	if b == untimedByteTxFrom {
		return UntimedFromTxFrom
	}
	return UntimedUnbounded
}

// untimedStart is THE place the mode decides: the valid-from a create stores.
// first reports that the row is the entity's first version (a generated ID,
// or an imported ID without an earlier life). An explicit validFrom is kept.
// While a conversion is unfinished (untimedIncomplete) an untimed first
// version is refused: the store holds rows of both rules.
func (c *Core) untimedStart(validFrom types.Instant, first bool) (types.Instant, error) {
	if validFrom == 0 && first {
		if err := c.checkUntimedComplete("create an untimed first version"); err != nil {
			return 0, err
		}
	}
	return c.untimedStartMode(validFrom, first), nil
}

// untimedStartMode is untimedStart's mode rule without the
// unfinished-conversion guard (a dry run; a commit-time restamp whose batch
// passed precheckQueuedUntimedCreates).
func (c *Core) untimedStartMode(validFrom types.Instant, first bool) types.Instant {
	if validFrom == 0 && first && c.untimedUnbounded.Load() {
		return types.UnboundedValidFrom
	}
	return validFrom
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
	for i := range nodes {
		if tm := nodes[i].temporal; tm != nil && tm.ValidFrom == 0 {
			return c.checkUntimedComplete("create an untimed first version")
		}
	}
	for i := range rels {
		if tm := rels[i].temporal; tm != nil && tm.ValidFrom == 0 {
			return c.checkUntimedComplete("create an untimed first version")
		}
	}
	return nil
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
	vf := c.untimedStartMode(0, true)
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
			c.untimedUnbounded.Store(false)
		}
		return nil
	}
	v, err := mk.MetaGet(untimedValidStartMeta)
	if err != nil {
		return fmt.Errorf("graph: read untimed valid-start mode: %w", err)
	}
	hasKey := len(v) == 1
	if len(v) > 1 || (hasKey && v[0] > untimedConverting) {
		return fmt.Errorf("graph: untimed valid-start mode %x: %w", v, storepkg.ErrCorruptWire)
	}
	if defaultMode {
		unbounded := true
		if hasKey {
			unbounded = v[0] != untimedByteTxFrom
		} else if df, ok := c.store.(storepkg.DurableFlushCapability); ok && df.DurableFlushSupported() {
			// A store without stable storage was never written by an
			// earlier session: it is new.
			_, found, err := c.sampleRowHashed()
			if err != nil {
				return fmt.Errorf("graph: untimed valid-start mode: %w", err)
			}
			unbounded = !found
		}
		c.untimedUnbounded.Store(unbounded)
	}
	want := UntimedFromTxFrom
	if c.untimedUnbounded.Load() {
		want = UntimedUnbounded
	}
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
	return untimedModeByteOf(untimedModeOf(c.untimedUnbounded.Load()))
}

// untimedModeByteOf is the persisted key byte of mode (UntimedFromTxFrom or
// UntimedUnbounded).
func untimedModeByteOf(mode UntimedValidStart) byte {
	if mode == UntimedUnbounded {
		return untimedByteUnbounded
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
		if untimedFirst(r.Version(), r.Temporal()) {
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

// applyMetaLocked applies a replicated ChangeMeta record (a replica's apply,
// caller holds c.mu.Lock). The only meta entry a primary replicates is the
// untimed valid-start mode (backlog 73): the replica takes it as its own and
// persists it like the primary (key, format marker 7). Any other key fails
// closed: this release does not know what applying it means.
func (c *Core) applyMetaLocked(body storeutil.MetaBody) error {
	if body.Key != untimedValidStartMeta {
		return fmt.Errorf("graph: apply: unknown replicated meta key %q: %w", body.Key, storepkg.ErrCorruptWire)
	}
	if len(body.Value) != 1 || body.Value[0] > untimedConverting {
		return fmt.Errorf("graph: apply: untimed valid-start mode %x: %w", body.Value, storepkg.ErrCorruptWire)
	}
	v := body.Value[0]
	c.untimedUnbounded.Store(v != untimedByteTxFrom)
	c.asOfColumns.bump() // the conversion's rewritten history rows follow in the feed
	return c.writeUntimedMode(v)
}

func untimedModeOf(unbounded bool) UntimedValidStart {
	if unbounded {
		return UntimedUnbounded
	}
	return UntimedFromTxFrom
}

// checkImportUntimedModeLocked refuses a full import whose source wrote its
// untimed rows under the other mode into a target that holds rows
// (ErrUntimedModeMismatch, before any record is written): a store never
// mixes the two rules. An empty target takes the source's mode at the end of
// the import (adoptImportUntimedModeLocked). An export without the header
// field (a UntimedFromTxFrom source, or one written before backlog 73) is a
// TxFrom export. Caller holds c.mu.Lock.
func (c *Core) checkImportUntimedModeLocked(srcUnbounded bool, rollback *importRollback) error {
	if err := c.checkUntimedComplete("import"); err != nil {
		return err
	}
	if srcUnbounded == c.untimedUnbounded.Load() {
		return nil
	}
	empty := rollback != nil && rollback.emptyTarget
	if rollback == nil {
		var err error
		if empty, err = c.importTargetEmptyLocked(); err != nil {
			return err
		}
	}
	if empty {
		return nil
	}
	return fmt.Errorf("import: the export's rows are written under %s, this store's under %s: %w",
		untimedModeName(untimedModeOf(srcUnbounded)), untimedModeName(untimedModeOf(c.untimedUnbounded.Load())), ErrUntimedModeMismatch)
}

// adoptImportUntimedModeLocked makes an empty import target take the source's
// mode (checkImportUntimedModeLocked admitted it) and persists it: later
// creates follow the imported rows' rule, and a reopen needs the adopted mode.
// Caller holds c.mu.Lock.
func (c *Core) adoptImportUntimedModeLocked(srcUnbounded bool) error {
	if srcUnbounded == c.untimedUnbounded.Load() {
		return nil
	}
	c.untimedUnbounded.Store(srcUnbounded)
	if err := c.writeUntimedModeLogged(untimedModeByteOf(untimedModeOf(srcUnbounded)), true); err != nil {
		return fmt.Errorf("import: persist the adopted untimed valid-start mode: %w", err)
	}
	return nil
}
