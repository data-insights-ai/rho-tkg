package core

// This file is the SINGLE, EXPLICIT classification of every MetaKV key this
// package (pkg/graph/internal/core) persists directly via mk.MetaSet — BACKLOG
// 13l. Admin.Reset()/ChangeClear wipe the store's whole MetaKV keyspace
// (badger: DropAll or its LastLSNKey-preserving variants; see
// (*badger.Store).Clear); a key must then be explicitly EITHER re-persisted
// (Preserve) or left wiped (Reap) — there is no third, undecided option. A
// hand-maintained reap checklist with no enforcement lets a brand-new MetaKV-
// backed feature land without anyone being forced to make that choice — see
// reapCoreStateForClear's callers in admin.go. TestMetaKVReapPolicyCoversEveryKey
// (metakv_reap_policy_check_test.go) AST-walks this package's non-test source
// for every mk.MetaSet call and fails closed if its key (or, for a
// dynamically-built per-entity key, its BUILDER FUNCTION) is not registered
// below — so a future contributor adding a new MetaKV-backed feature cannot
// silently skip this decision.
//
// Classifications (each entry states WHY, not just what):
//
//   - asofTagsMeta (Reap): named as-of tags are a durable REGISTRY OVER THE
//     GRAPH'S OWN DATA — tags name knowledge-time positions in a chain Reset
//     just erased, so stale tag names pointing at erased history are
//     meaningless and must not survive.
//   - compactedThroughTxMeta (Reap): the compaction watermark answers "how
//     far has history been trimmed" for the erased data; a fresh graph has
//     nothing trimmed.
//   - graphEpochMeta (Reap): identifies THIS GRAPH'S DATA LINEAGE so a delta
//     cursor from a different lineage is rejected (ExportSince). A stale
//     epoch surviving Reset would let a PRE-RESET delta cursor look valid
//     against POST-RESET data it has no real relationship to — minting fresh
//     forces any old cursor to fail closed, which is the safe direction.
//   - replicaAppliedLSNMeta (Reap): a replica's "how far I've replayed"
//     watermark. If it survived Reset while the replicated DATA was wiped,
//     the replica would believe it is already caught up and never
//     re-bootstrap, silently serving an incomplete dataset.
//   - instantFloorMeta (Preserve): the durable commit-clock floor watermark.
//     Like idSlotLeaseMeta this is data-INDEPENDENT node state — this node's
//     transaction-time position, not a description of the graph's entities. If
//     Clear wiped it, transaction time could go BACKWARDS across the Clear: a
//     write burst leaves the floor above the wall clock, the wipe drops the
//     watermark, and after a reopen fresh writes stamp BELOW records the change
//     log already emitted before the Clear. Unlike the id-slot lease it needs no
//     capture-before-Clear, because the authoritative value is the in-memory
//     lastInstant (which Clear never lowers), not the persisted blob — see
//     restoreInstantFloorAfterReset (instant_floor.go).
//   - idSlotLeaseMeta (Preserve): an EXTERNAL ORCHESTRATOR's failover hint
//     (which snowflake slot this node is leased for) — data-independent
//     identity, unrelated to the graph's own entities. Wiping it on a data
//     Reset risks two nodes colliding on the same ID-generation slot after a
//     failover. See captureIDSlotLeaseForReset/restoreIDSlotLeaseAfterReset
//     (replication.go).
//   - uniqueForeverOwnersMeta (Reap): the durable value-ownership registry is
//     scoped to the graph's own entities/values, which Reset just erased.
//   - schemaVersionKey (Reap): gates a one-shot, EXPLICITLY IDEMPOTENT
//     migration (see migration_bitemporal.go's own doc comment). Wiping it is
//     harmless — the migration simply re-runs against the now-empty store and
//     re-sets the flag; there is no correctness reason to preserve it.
//   - retentionMaxWatermarkMeta / retentionWatermarkKey (Reap): retention
//     watermarks describe purge progress over the graph's own (now-erased)
//     entities.
//   - uniqueConstraintsMeta (Reap): constraint DEFINITIONS are graph-schema-
//     adjacent but are explicitly documented as Reset-cleared today (see
//     reapUniqueConstraintsForReset) — a fresh graph starts with none.
//   - compactStubNodeKey / compactStubRelKey (Reap): per-entity compaction
//     stubs are meaningless once Clear erases the entities they describe.
type reapPolicy int

const (
	// reapPolicyReap means Reset/ChangeClear leave the key wiped (the
	// default direction — Clear's own wipe already achieves this; a Reap
	// entry needs no additional code, only this explicit classification).
	reapPolicyReap reapPolicy = iota
	// reapPolicyPreserve means the key must survive Reset/ChangeClear —
	// captured before Clear and re-persisted after, mirroring
	// persistRegistries()'s pattern (see e.g. captureIDSlotLeaseForReset).
	reapPolicyPreserve
)

// metaKVFixedKeys classifies every LITERAL MetaKV key this package uses —
// i.e. every mk.MetaSet(keyConst, ...) call site where keyConst resolves to a
// package-level string constant (or, in principle, a string literal).
var metaKVFixedKeys = map[string]reapPolicy{
	asofTagsMeta:              reapPolicyReap,
	compactedThroughTxMeta:    reapPolicyReap,
	graphEpochMeta:            reapPolicyReap,
	replicaAppliedLSNMeta:     reapPolicyReap,
	idSlotLeaseMeta:           reapPolicyPreserve,
	instantFloorMeta:          reapPolicyPreserve,
	uniqueForeverOwnersMeta:   reapPolicyReap,
	schemaVersionKey:          reapPolicyReap,
	retentionMaxWatermarkMeta: reapPolicyReap,
	uniqueConstraintsMeta:     reapPolicyReap,
}

// metaKVKeyFuncs classifies every FUNCTION that builds a dynamic, per-entity
// MetaKV key — i.e. every mk.MetaSet(builderFunc(id), ...) call site. The
// check cannot statically compute a per-entity key's exact string, so a
// dynamic key is classified by which BUILDER FUNCTION produced it, keyed by
// the function's Go identifier name.
var metaKVKeyFuncs = map[string]reapPolicy{
	"retentionWatermarkKey": reapPolicyReap,
	"compactStubNodeKey":    reapPolicyReap,
	"compactStubRelKey":     reapPolicyReap,
}
