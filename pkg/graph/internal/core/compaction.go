package core

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/vmihailenco/msgpack/v5"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// History retention & compaction (ADR-0001, stages a-c)
//
// Compaction trims an entity's OLDEST version-history rows, keeping the newest
// belief and its recent predecessors, and records a per-entity DETACHED ANCHOR
// STUB describing what was removed. The stub is a virtual predecessor for the
// oldest retained version: Verify*Chain requires that version's PrevHash to
// equal the stub's LastTrimmedHash, so the retained segment keeps full tamper
// evidence WITHOUT rewriting any stored row (append-only, lesson 46). The stub
// carries a self-hash so an attacker who can write the stub cannot forge a clean
// truncation boundary (ADR Risks).
//
// The per-entity trim + stub commit in ONE store WriteBatch via the optional
// HistoryCompactionCapability (memory + badger commit them atomically; tiered
// routes the trim to the entity's owning shard and the stub to the global
// reference shard — see tieredstore_compaction.go). The GRAPH watermark is
// routed ONCE via the store-level MetaSet (advanceCompactionWatermark), not
// bundled into every per-entity batch, so on tiered it lands deterministically
// on the reference shard rather than scattering across owning shards.
// =============================================================================

// compactedThroughTxMeta is the MetaKV key holding the graph-level compaction
// watermark: max over all per-entity stubs of LastTrimmedTxTo. A transaction-time
// pin at or above it cannot require any trimmed version.
const compactedThroughTxMeta = "compacted_through_tx"

// compactStubNodePrefix / compactStubRelPrefix are the per-entity stub MetaKV key
// prefixes (v1 choice: MetaKV keyed per entity; a dedicated keyspace can come
// later per ADR Decision 2).
const (
	compactStubNodePrefix = "compact_stub_node/"
	compactStubRelPrefix  = "compact_stub_rel/"
)

// compactStubDomain separates the stub self-hash pre-image from any other
// SHA-256 use in the codebase.
const compactStubDomain = "tkg_compact_stub_v1"

// RetentionPolicy is the compaction bound. Both bounds are keeps: a history
// version is trimmable only when it fails BOTH (it is neither among the newest
// KeepVersions history versions NOR recorded at/after KeepSince). The current
// row and the newest history version are never trimmable.
type RetentionPolicy struct {
	// KeepVersions retains at most the newest N history versions per entity.
	// 0 = no version bound.
	KeepVersions int
	// KeepSince retains every history version whose TxFrom >= KeepSince.
	// 0 = no age bound.
	KeepSince types.Instant
}

// CompactReport summarizes a CompactHistory* run.
type CompactReport struct {
	// EntitiesCompacted is the number of entities that had at least one history
	// version trimmed.
	EntitiesCompacted int
	// VersionsTrimmed is the total number of history versions removed.
	VersionsTrimmed int
	// Watermark is the graph CompactedThroughTx after this run.
	Watermark types.Instant
}

// compactionStub is the durable per-entity detached-anchor record.
type compactionStub struct {
	EntityID              int64  `msgpack:"eid"`
	TrimmedThroughVersion uint32 `msgpack:"ttv"`
	LastTrimmedHash       string `msgpack:"lth"`
	LastTrimmedTxTo       int64  `msgpack:"ltt"`
	CompactedAtTx         int64  `msgpack:"cat"`
	StubHash              string `msgpack:"sh"`
}

// selfHash computes the stub self-hash over every field except StubHash.
func (s compactionStub) selfHash() string {
	h := sha256.New()
	h.Write([]byte(compactStubDomain))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(s.EntityID)) // #nosec G115 — id bits
	h.Write(buf[:])
	binary.BigEndian.PutUint32(buf[:4], s.TrimmedThroughVersion)
	h.Write(buf[:4])
	binary.BigEndian.PutUint64(buf[:], uint64(len(s.LastTrimmedHash))) // #nosec G115
	h.Write(buf[:])
	h.Write([]byte(s.LastTrimmedHash))
	binary.BigEndian.PutUint64(buf[:], uint64(s.LastTrimmedTxTo)) // #nosec G115
	h.Write(buf[:])
	binary.BigEndian.PutUint64(buf[:], uint64(s.CompactedAtTx)) // #nosec G115
	h.Write(buf[:])
	return hex.EncodeToString(h.Sum(nil))
}

// sealed returns a copy with StubHash set to the freshly computed self-hash.
func (s compactionStub) sealed() compactionStub {
	s.StubHash = s.selfHash()
	return s
}

// encode marshals the stub to msgpack for MetaKV storage.
func (s compactionStub) encode() ([]byte, error) {
	b, err := msgpack.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("graph: encode compaction stub: %w", err)
	}
	return b, nil
}

// decodeCompactionStub decodes and self-verifies a stub blob. Untrusted bytes go
// through SafeUnmarshal (fail closed, never panic); a self-hash mismatch is a
// forged/tampered boundary and fails closed with ErrCorruptWire.
func decodeCompactionStub(expectID int64, blob []byte) (compactionStub, error) {
	var s compactionStub
	if err := storeutil.SafeUnmarshal(blob, &s); err != nil {
		return compactionStub{}, fmt.Errorf("graph: decode compaction stub: %w", err)
	}
	if s.EntityID != expectID {
		return compactionStub{}, fmt.Errorf("graph: decode compaction stub: %w: entity id mismatch (stub %d, key %d)", storepkg.ErrCorruptWire, s.EntityID, expectID)
	}
	if s.StubHash != s.selfHash() {
		return compactionStub{}, fmt.Errorf("graph: decode compaction stub: %w: self-hash mismatch", storepkg.ErrCorruptWire)
	}
	return s, nil
}

func compactStubNodeKey(id types.NodeID) string {
	return compactStubNodePrefix + fmt.Sprintf("%d", int64(id.SnowflakeID()))
}

func compactStubRelKey(id types.RelID) string {
	return compactStubRelPrefix + fmt.Sprintf("%d", int64(id.SnowflakeID()))
}

// compactionMetaKV resolves the store's MetaKV capability (every in-tree
// backend). Without it, compaction and stub reads decline.
func (c *Core) compactionMetaKV() (storepkg.MetaKVCapability, error) {
	mk := c.metaKV
	if mk == nil {
		return nil, fmt.Errorf("graph: history compaction: %w", storepkg.ErrCapabilityNotSupported)
	}
	return mk, nil
}

// loadCompactionWatermark rehydrates the graph watermark atomic from MetaKV at
// open. Fail closed on a corrupt blob — a silently-dropped watermark would let a
// scan below compacted knowledge return wrong data.
func (c *Core) loadCompactionWatermark() error {
	mk := c.metaKV
	if mk == nil {
		return nil
	}
	v, err := mk.MetaGet(compactedThroughTxMeta)
	if err != nil {
		return fmt.Errorf("graph: read compaction watermark: %w", err)
	}
	if len(v) == 0 {
		return nil
	}
	if len(v) != 8 {
		return fmt.Errorf("graph: read compaction watermark: %w: want 8 bytes, got %d", storepkg.ErrCorruptWire, len(v))
	}
	wm := int64(binary.BigEndian.Uint64(v)) // #nosec G115 — round-trips the stored int64
	if wm < 0 {
		return fmt.Errorf("graph: read compaction watermark: %w: negative watermark %d", storepkg.ErrCorruptWire, wm)
	}
	c.compactedThroughTx.Store(wm)
	return nil
}

func encodeWatermark(wm types.Instant) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(wm)) // #nosec G115
	return buf
}

// advanceCompactionWatermark persists the graph compaction watermark ONCE via
// the store-level MetaSet (never bundled into a per-entity batch) and updates the
// in-memory atomic, when newWatermark exceeds the current value. On tiered,
// MetaSet routes to the reference shard, so the graph watermark lands globally on
// ONE shard instead of scattering across the owning shards of whichever entities
// happened to be compacted (a subsequent refShard MetaGet would otherwise miss it
// and a scan below compacted knowledge would not fail closed).
//
// SEAM — crash-window ordering (why this is written BEFORE the per-entity trims,
// not after). The point-door fast gate skips the per-entity stub check whenever
// the pin is at/above the watermark (checkNodePointCompaction: `txPin >= wm`
// returns "not compacted"). An UNDER-stated watermark therefore turns a compacted
// entity's below-boundary point read into a SILENTLY-INCOMPLETE answer — the exact
// hole ErrHistoryCompacted exists to close. So the fail-closed direction is to
// OVER-state: advance the watermark to the full run maximum BEFORE any trim
// commits. A crash between the watermark write and a not-yet-committed trim then
// leaves the watermark HIGH and some chains still intact — the gate over-rejects
// (a read that could have been answered fails closed with ErrHistoryCompacted),
// which is conservative, and an idempotent re-run finishes the outstanding trims
// while the already-correct watermark stays put (a re-run cannot recompute a
// trimmed entity's boundary, so an after-the-trims watermark write could never be
// repaired — another reason it goes first). This preserves memory/badger behavior:
// there the watermark already reached the run maximum on the FIRST per-entity
// batch (it was the same value bundled into every batch), i.e. before most trims.
func (c *Core) advanceCompactionWatermark(newWatermark types.Instant) error {
	if int64(newWatermark) <= c.compactedThroughTx.Load() {
		return nil
	}
	mk, err := c.compactionMetaKV()
	if err != nil {
		return err
	}
	if err := mk.MetaSet(compactedThroughTxMeta, encodeWatermark(newWatermark)); err != nil {
		return fmt.Errorf("graph: advance compaction watermark: %w", err)
	}
	c.compactedThroughTx.Store(int64(newWatermark))
	return nil
}

// loadNodeCompactionStub reads and self-verifies the stub for id. ok is false
// (nil error) when no stub exists.
func (c *Core) loadNodeCompactionStub(id types.NodeID) (compactionStub, bool, error) {
	mk := c.metaKV
	if mk == nil {
		return compactionStub{}, false, nil
	}
	v, err := mk.MetaGet(compactStubNodeKey(id))
	if err != nil {
		return compactionStub{}, false, fmt.Errorf("graph: read compaction stub: %w", err)
	}
	if len(v) == 0 {
		return compactionStub{}, false, nil
	}
	s, err := decodeCompactionStub(int64(id.SnowflakeID()), v)
	if err != nil {
		return compactionStub{}, false, err
	}
	return s, true, nil
}

// loadRelCompactionStub is the relationship mirror of loadNodeCompactionStub.
func (c *Core) loadRelCompactionStub(id types.RelID) (compactionStub, bool, error) {
	mk := c.metaKV
	if mk == nil {
		return compactionStub{}, false, nil
	}
	v, err := mk.MetaGet(compactStubRelKey(id))
	if err != nil {
		return compactionStub{}, false, fmt.Errorf("graph: read compaction stub: %w", err)
	}
	if len(v) == 0 {
		return compactionStub{}, false, nil
	}
	s, err := decodeCompactionStub(int64(id.SnowflakeID()), v)
	if err != nil {
		return compactionStub{}, false, err
	}
	return s, true, nil
}

// reapCompactionForReset clears the graph compaction watermark as part of
// Admin.Reset, so a reset graph does not inherit a pre-Clear watermark that would
// spuriously fail temporal reads with ErrHistoryCompacted. Backend-independent:
// badger's Clear already scan-reaps meta, but memory's Clear preserves the whole
// MetaKV. Per-entity stub keys are left in place — they are keyed by
// (never-reused, monotonically increasing) snowflake IDs, so no post-reset entity
// can ever read one, and the zeroed watermark fast-gates every temporal read past
// the stub probe. No-op when the backend cannot persist metadata. Caller holds
// c.mu.Lock (Reset's exclusion class).
func (c *Core) reapCompactionForReset() error {
	c.compactedThroughTx.Store(0)
	mk := c.metaKV
	if mk == nil {
		return nil
	}
	if err := mk.MetaSet(compactedThroughTxMeta, nil); err != nil {
		return fmt.Errorf("graph: reset compaction watermark: %w", err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Answerability checks (ADR Decision 3)
// -----------------------------------------------------------------------------

// checkNodePointCompaction returns ErrHistoryCompacted when txPin falls strictly
// before the entity's compacted knowledge boundary, so a point read cannot
// silently answer from an incomplete kept chain. txPin == 0 (no TX filter) is
// never compacted — the current row still answers. Gated by the lock-free graph
// watermark so the common (no compaction ever) path pays nothing.
func (c *Core) checkNodePointCompaction(id types.NodeID, txPin types.Instant) error {
	if txPin == 0 {
		return nil
	}
	wm := c.compactedThroughTx.Load()
	if wm == 0 || int64(txPin) >= wm {
		return nil
	}
	stub, ok, err := c.loadNodeCompactionStub(id)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if int64(txPin) < stub.LastTrimmedTxTo {
		return ErrHistoryCompacted
	}
	return nil
}

// checkRelPointCompaction is the relationship mirror of checkNodePointCompaction.
func (c *Core) checkRelPointCompaction(id types.RelID, txPin types.Instant) error {
	if txPin == 0 {
		return nil
	}
	wm := c.compactedThroughTx.Load()
	if wm == 0 || int64(txPin) >= wm {
		return nil
	}
	stub, ok, err := c.loadRelCompactionStub(id)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if int64(txPin) < stub.LastTrimmedTxTo {
		return ErrHistoryCompacted
	}
	return nil
}

// checkScanCompaction errors a scan whose transaction-time pin falls before the
// graph watermark (ADR Decision 3, scan behavior (i) — correctness of defaults).
// A scan cannot signal per entity, so it consults the graph-level watermark:
// when the pin is below it, some entity's trimmed knowledge is required and the
// whole scan fails closed with ErrHistoryCompacted rather than returning a
// silently-incomplete result set.
func (c *Core) checkScanCompaction(opts storepkg.QueryOpts) error {
	pin := opts.TxAt
	if opts.TxPin != 0 {
		pin = opts.TxPin
	}
	return c.checkScanCompactionAt(pin)
}

func (c *Core) checkScanCompactionAt(pin types.Instant) error {
	if pin == 0 {
		return nil
	}
	wm := c.compactedThroughTx.Load()
	if wm == 0 {
		return nil
	}
	if int64(pin) < wm {
		return ErrHistoryCompacted
	}
	return nil
}

// validateTemporalQueryOptsScan is the scan-door superset of
// validateTemporalQueryOpts: the cross-field TxPin conflict check plus the
// compaction watermark pre-check. Every generic scan door (ByLabel / ByType /
// All / property / vector, standalone and tx-side) validates through here.
func (c *Core) validateTemporalQueryOptsScan(opts storepkg.QueryOpts) error {
	if err := validateTemporalQueryOpts(opts); err != nil {
		return err
	}
	if err := c.checkScanCompaction(opts); err != nil {
		return err
	}
	// Retention (ADR-0008): a pin below the graph's max retention watermark means
	// some purged entity is missing — fail the whole scan closed.
	return c.checkScanRetention(opts)
}

// -----------------------------------------------------------------------------
// Policy math
// -----------------------------------------------------------------------------

// versionInfo is the per-version summary the policy math operates on,
// type-agnostic across node / relationship.
type versionInfo struct {
	version uint32
	txFrom  types.Instant
	hash    string
}

// validateRetentionPolicy rejects an empty or negative policy.
func validateRetentionPolicy(policy RetentionPolicy) error {
	if policy.KeepVersions < 0 || policy.KeepSince < 0 {
		return ErrInvalidRetentionPolicy
	}
	if policy.KeepVersions == 0 && policy.KeepSince == 0 {
		return ErrInvalidRetentionPolicy
	}
	return nil
}

// planTrim decides how many oldest history versions to trim. hist is ascending
// by version. The newest history version is never trimmed. Returns trimCount
// (0 = nothing to trim) and, when trimCount > 0, the boundary version (highest
// trimmed) and the oldest kept version.
//
// The kept set is a SUFFIX (the store trims newest-by-version), so trimming
// stops at the first version the policy keeps. History TxFrom is monotonic
// (updates stamp the system clock; §4.1 backfill is create-only), so the age
// bound keeps a clean suffix.
func planTrim(hist []versionInfo, policy RetentionPolicy) (trimCount int, boundary versionInfo, oldestKept versionInfo) {
	m := len(hist)
	if m < 2 {
		return 0, versionInfo{}, versionInfo{}
	}
	trim := 0
	for i := 0; i < m-1; i++ { // never trim the newest history version (index m-1)
		failsCount := true
		if policy.KeepVersions > 0 && (m-i) <= policy.KeepVersions {
			failsCount = false
		}
		failsAge := true
		if policy.KeepSince > 0 && hist[i].txFrom >= policy.KeepSince {
			failsAge = false
		}
		if failsCount && failsAge {
			trim = i + 1
			continue
		}
		break
	}
	if trim == 0 {
		return 0, versionInfo{}, versionInfo{}
	}
	return trim, hist[trim-1], hist[trim]
}

// -----------------------------------------------------------------------------
// AdminOps surface (g.Admin())
// -----------------------------------------------------------------------------

// entityPlan is one entity's computed compaction (planning pass output).
type entityPlan struct {
	keepVersions int
	trimmed      int
	stub         compactionStub
}

// preflightCompaction runs the shared refusal checks common to node and rel
// compaction and returns the minimum registered as-of tag instant (0 = no tags).
// It validates that the store has both MetaKV (for the stub/watermark) and the
// atomic HistoryCompactionCapability; the actual meta writes ride the compaction
// batch, so no MetaKV handle is threaded out.
func (c *Core) preflightCompaction(ctx context.Context, policy RetentionPolicy) (types.Instant, error) {
	if ctx == nil {
		return 0, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := c.checkWritable(); err != nil {
		return 0, err
	}
	if err := validateRetentionPolicy(policy); err != nil {
		return 0, err
	}
	if c.changeLogActive() {
		return 0, ErrCompactionChangeLogEnabled
	}
	if _, err := c.compactionMetaKV(); err != nil {
		return 0, err
	}
	if c.historyCompaction == nil {
		return 0, fmt.Errorf("graph: history compaction: %w", storepkg.ErrCapabilityNotSupported)
	}
	return c.minAsOfTagInstant()
}

// minAsOfTagInstant returns the smallest registered named as-of tag instant, or
// 0 when no tags are registered.
func (c *Core) minAsOfTagInstant() (types.Instant, error) {
	tags, err := c.asOfTags()
	if err != nil {
		return 0, err
	}
	var min types.Instant
	for _, at := range tags {
		if min == 0 || at < min {
			min = at
		}
	}
	return min, nil
}

// CompactHistoryNodes trims node version history per policy. See RetentionPolicy.
//
// Refuses (no writes) when: the change-log is enabled
// (ErrCompactionChangeLogEnabled), the backend implements neither MetaKV nor the
// HistoryCompactionCapability (ErrCapabilityNotSupported), the policy is empty
// (ErrInvalidRetentionPolicy), or a registered as-of tag pins knowledge the
// policy would trim (ErrCompactionProtectedTag). Rejected on a read-only
// replica. Runs on memory, badger, and tiered (tiered routes each entity's trim
// to its owning shard and the stub + global watermark to the reference shard —
// see tieredstore_compaction.go).
func (a *AdminOps) CompactHistoryNodes(ctx context.Context, policy RetentionPolicy) (CompactReport, error) {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return CompactReport{}, err
	}
	minTag, err := c.preflightCompaction(ctx, policy)
	if err != nil {
		return CompactReport{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return CompactReport{}, ErrGraphClosed
	}

	compactor := c.historyCompaction
	now := c.now()

	// Planning pass: compute each entity's plan; refuse on a protected tag
	// BEFORE any write so compaction is all-or-nothing on the protected-tag gate.
	ids, err := c.allNodeChainIDs()
	if err != nil {
		return CompactReport{}, err
	}
	plans := make(map[types.NodeID]entityPlan)
	var maxBoundary types.Instant
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return CompactReport{}, err
		}
		plan, ok, err := c.planNodeCompaction(id, policy, now)
		if err != nil {
			return CompactReport{}, err
		}
		if !ok {
			continue
		}
		boundary := types.Instant(plan.stub.LastTrimmedTxTo)
		if minTag != 0 && minTag < boundary {
			return CompactReport{}, ErrCompactionProtectedTag
		}
		if boundary > maxBoundary {
			maxBoundary = boundary
		}
		plans[id] = plan
	}
	if len(plans) == 0 {
		return CompactReport{Watermark: types.Instant(c.compactedThroughTx.Load())}, nil
	}

	newWatermark := types.Instant(c.compactedThroughTx.Load())
	if maxBoundary > newWatermark {
		newWatermark = maxBoundary
	}
	// Route the global watermark ONCE, BEFORE the per-entity trims (over-state =
	// fail-closed; see advanceCompactionWatermark's SEAM note).
	if err := c.advanceCompactionWatermark(newWatermark); err != nil {
		return CompactReport{}, err
	}

	report := CompactReport{Watermark: newWatermark}
	for _, id := range ids {
		plan, ok := plans[id]
		if !ok {
			continue
		}
		stubBytes, err := plan.stub.encode()
		if err != nil {
			return report, err
		}
		writes := []storepkg.MetaWrite{
			{Key: compactStubNodeKey(id), Value: stubBytes},
		}
		if err := compactor.CompactNodeHistory(id, plan.keepVersions, writes); err != nil {
			return report, err
		}
		report.EntitiesCompacted++
		report.VersionsTrimmed += plan.trimmed
	}
	return report, nil
}

// CompactHistoryRels is the relationship mirror of CompactHistoryNodes.
func (a *AdminOps) CompactHistoryRels(ctx context.Context, policy RetentionPolicy) (CompactReport, error) {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return CompactReport{}, err
	}
	minTag, err := c.preflightCompaction(ctx, policy)
	if err != nil {
		return CompactReport{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return CompactReport{}, ErrGraphClosed
	}

	compactor := c.historyCompaction
	now := c.now()

	ids, err := c.allRelChainIDs()
	if err != nil {
		return CompactReport{}, err
	}
	plans := make(map[types.RelID]entityPlan)
	var maxBoundary types.Instant
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return CompactReport{}, err
		}
		plan, ok, err := c.planRelCompaction(id, policy, now)
		if err != nil {
			return CompactReport{}, err
		}
		if !ok {
			continue
		}
		boundary := types.Instant(plan.stub.LastTrimmedTxTo)
		if minTag != 0 && minTag < boundary {
			return CompactReport{}, ErrCompactionProtectedTag
		}
		if boundary > maxBoundary {
			maxBoundary = boundary
		}
		plans[id] = plan
	}
	if len(plans) == 0 {
		return CompactReport{Watermark: types.Instant(c.compactedThroughTx.Load())}, nil
	}

	newWatermark := types.Instant(c.compactedThroughTx.Load())
	if maxBoundary > newWatermark {
		newWatermark = maxBoundary
	}
	// Route the global watermark ONCE, BEFORE the per-entity trims (over-state =
	// fail-closed; see advanceCompactionWatermark's SEAM note).
	if err := c.advanceCompactionWatermark(newWatermark); err != nil {
		return CompactReport{}, err
	}

	report := CompactReport{Watermark: newWatermark}
	for _, id := range ids {
		plan, ok := plans[id]
		if !ok {
			continue
		}
		stubBytes, err := plan.stub.encode()
		if err != nil {
			return report, err
		}
		writes := []storepkg.MetaWrite{
			{Key: compactStubRelKey(id), Value: stubBytes},
		}
		if err := compactor.CompactRelHistory(id, plan.keepVersions, writes); err != nil {
			return report, err
		}
		report.EntitiesCompacted++
		report.VersionsTrimmed += plan.trimmed
	}
	return report, nil
}

// planNodeCompaction loads a node's history, runs the policy math, and builds
// the (unsealed→sealed) stub. ok is false when nothing would be trimmed.
func (c *Core) planNodeCompaction(id types.NodeID, policy RetentionPolicy, now types.Instant) (entityPlan, bool, error) {
	history, err := c.getNodeHistory(id)
	if err != nil {
		return entityPlan{}, false, err
	}
	infos := make([]versionInfo, 0, len(history))
	for _, h := range history {
		infos = append(infos, nodeVersionInfo(h))
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].version < infos[j].version })
	trim, boundary, oldestKept := planTrim(infos, policy)
	if trim == 0 {
		return entityPlan{}, false, nil
	}
	stub := compactionStub{
		EntityID:              int64(id.SnowflakeID()),
		TrimmedThroughVersion: boundary.version,
		LastTrimmedHash:       boundary.hash,
		LastTrimmedTxTo:       int64(oldestKept.txFrom),
		CompactedAtTx:         int64(now),
	}.sealed()
	return entityPlan{keepVersions: len(infos) - trim, trimmed: trim, stub: stub}, true, nil
}

// planRelCompaction is the relationship mirror of planNodeCompaction.
func (c *Core) planRelCompaction(id types.RelID, policy RetentionPolicy, now types.Instant) (entityPlan, bool, error) {
	history, err := c.getRelHistory(id)
	if err != nil {
		return entityPlan{}, false, err
	}
	infos := make([]versionInfo, 0, len(history))
	for _, h := range history {
		infos = append(infos, relVersionInfo(h))
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].version < infos[j].version })
	trim, boundary, oldestKept := planTrim(infos, policy)
	if trim == 0 {
		return entityPlan{}, false, nil
	}
	stub := compactionStub{
		EntityID:              int64(id.SnowflakeID()),
		TrimmedThroughVersion: boundary.version,
		LastTrimmedHash:       boundary.hash,
		LastTrimmedTxTo:       int64(oldestKept.txFrom),
		CompactedAtTx:         int64(now),
	}.sealed()
	return entityPlan{keepVersions: len(infos) - trim, trimmed: trim, stub: stub}, true, nil
}

func nodeVersionInfo(n *types.Node) versionInfo {
	vi := versionInfo{version: n.Version()}
	if tm := n.Temporal(); tm != nil {
		vi.txFrom = tm.TxFrom
	}
	if ig := n.Integrity(); ig != nil {
		vi.hash = ig.Hash
	}
	return vi
}

func relVersionInfo(r *types.Relationship) versionInfo {
	vi := versionInfo{version: r.Version()}
	if tm := r.Temporal(); tm != nil {
		vi.txFrom = tm.TxFrom
	}
	if ig := r.Integrity(); ig != nil {
		vi.hash = ig.Hash
	}
	return vi
}

// allNodeChainIDs collects every node ID with a current row and/or history rows,
// sorted ascending. Caller holds c.mu.Lock.
func (c *Core) allNodeChainIDs() ([]types.NodeID, error) {
	seen := make(map[snowflake.ID]struct{})
	if err := c.forEachNodeID(func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}
	if err := c.forEachNodeHistoryIDByDepth(storepkg.DepthAll, func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}
	ids := make([]types.NodeID, 0, len(seen))
	for id := range seen {
		ids = append(ids, types.NodeID(id))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].SnowflakeID() < ids[j].SnowflakeID() })
	return ids, nil
}

// allRelChainIDs is the relationship mirror of allNodeChainIDs.
func (c *Core) allRelChainIDs() ([]types.RelID, error) {
	seen := make(map[snowflake.ID]struct{})
	if err := c.forEachRelID(func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}
	if err := c.forEachRelHistoryIDByDepth(storepkg.DepthAll, func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}
	ids := make([]types.RelID, 0, len(seen))
	for id := range seen {
		ids = append(ids, types.RelID(id))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].SnowflakeID() < ids[j].SnowflakeID() })
	return ids, nil
}
