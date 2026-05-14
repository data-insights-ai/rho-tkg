# Weaker Suspicion Verification - 2026-05-14

Scope: follow-up code-inspection pass for weaker candidates from the hardening
review. No fixes were made and no tests were executed.

## Confirmed

### W1 - AsyncEventBus `PublishBatch` is not fully atomic under `BackpressureBlock`

Severity: Medium

`AsyncEventBus` documents `PublishBatch` as an atomic enqueue with one wake-up at
the end, so the dispatcher sees the full burst before its first priority scan:

- `pkg/graph/events/events.go:210-216`
- `pkg/graph/events/events.go:362-367`

The implementation mostly follows that shape, but `enqueueLocked` signals the
dispatcher before the end of the batch when `BackpressureBlock` hits a full
priority queue:

- `pkg/graph/events/events.go:416-430`

That early signal is intentional for liveness: the existing regression test
`TestAsyncEventBusPublishBatchBlockWakesBeforeFullQueueWait` covers the
otherwise-deadlocking case where a batch fills a queue and needs the dispatcher
to drain space:

- `pkg/graph/events/async_eventbus_test.go:325-357`

The remaining problem is the API contract. With `QueueSize < number of events in
a priority bucket`, the dispatcher can start draining before all batch events are
visible. That contradicts the "single wake-up at the end" / "full burst before
first scan" guarantee. In a mixed workload, a pre-existing lower-priority queued
event can be dispatched before later higher-priority events from the blocked
batch have been enqueued. For graph batch/transaction event flushes this is
unlikely at small batch sizes, but the default queue size is finite and large
mutation batches can exceed it.

## Dismissed Or Contract-Only

### W2 - Vector `SearchNearest` pagination looked page-size sensitive

Status: Dismissed as a correctness bug; API clarity could still improve.

`SearchNearest` fetches the top `k` candidates and then applies
`QueryOpts.After/Limit` in distance order:

- `pkg/graph/internal/core/vector_search.go:95-109`
- `pkg/graph/internal/core/vector_search.go:160`
- `pkg/graph/internal/core/vector_search.go:244`

This means callers must keep `k` large enough to cover the whole paginated
window, not just the page size. The test suite documents that intended usage by
calling `SearchNearest(..., k=5, Limit=2)` across all pages:

- `pkg/graph/internal/core/vector_correctness_test.go:338-392`

The implementation and tests are internally consistent. The weak suspicion was
not a bug unless the desired API is "k means per-page size", which the current
tests do not encode.

### W3 - `CloseVersion` seemed to write same-version history

Status: Dismissed.

The current contract is explicitly in-place close without incrementing the
version or adding a normal successor version:

- `docs/api.md:126`
- `docs/architecture.md:211`

The implementation preserves the current version and writes the prior state as
history under that version while setting `ValidTo` on the current row:

- `pkg/graph/internal/core/version_chain.go:146-178`
- `pkg/graph/internal/core/version_chain.go:368-400`

Tests cover the observable behavior: the closed entity remains directly
retrievable, has `ValidTo`, rejects later mutations, and preserves integrity
metadata:

- `pkg/graph/internal/core/version_chain_test.go:324-399`
- `pkg/graph/internal/core/version_chain_test.go:1034-1090`

### W4 - Index provider `Init` might deadlock through `GraphReader`

Status: Dismissed.

`RegisterProvider` mutates the provider registry under `c.mu.Lock`, then
releases that lock before calling `Initializable.Init(graphReaderView{g: c})`:

- `pkg/graph/internal/core/index_provider.go:174-194`
- `pkg/graph/internal/core/index_provider.go:213-221`

The `graphReaderView` read methods route back through the graph read APIs, so
they would be unsafe only if `Init` still ran under the write lock. It does not.
The tests cover initializable bulk load, rollback on init errors/panics, and
close/unregister waiting for in-flight init:

- `pkg/graph/internal/core/index_provider_test.go:569-818`

### W5 - Context cancellation after local mutation might persist partial changes

Status: Dismissed.

The update paths mutate store-returned deep copies, then check context again
before persistence. If cancellation happens in that window, no store replace has
run yet:

- `pkg/graph/internal/core/node_update.go:114-180`
- `pkg/graph/internal/core/relationship_update.go:114-190`

Store boundaries deep-copy on get/put, so the local object mutation is not a
shared pointer back into the store:

- `pkg/graph/store/memory/memorystore_node.go:74`
- `pkg/graph/store/memory/memorystore_rel.go:167`
- `pkg/graph/store/badger/badgerstore_node.go:99`
- `pkg/graph/store/badger/badgerstore_rel.go:113`

### W6 - Internal sub-ops lack nil receiver guards

Status: Dismissed as public API risk.

Some internal sub-ops assume a valid `*Core`, but the public wrappers construct
API accessors through `graph.New` and guard nil/typed-nil ops in each sub-API
`ready()` method. The nil-guard contract is therefore enforced at the public
surface rather than inside every internal method.

Examples:

- `pkg/graph/graph.go:92-106`
- `pkg/graph/nodes/api.go:38-53`
- `pkg/graph/rels/api.go:41-56`
- `pkg/graph/temporal/api.go:41-56`
