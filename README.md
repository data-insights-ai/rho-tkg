# tkg/v4

[![CI](https://github.com/data-insights-ai/rho-tkg/actions/workflows/ci.yml/badge.svg)](https://github.com/data-insights-ai/rho-tkg/actions/workflows/ci.yml)

**Temporal Knowledge Graph v4** is an embeddable, pure-Go graph engine for
**bitemporal knowledge graphs** with audit-grade integrity:

- **Append-only, hash-chained version history** — nothing is ever overwritten;
  every update or delete appends a new version, and every version is
  SHA-256-chained to its predecessor.
- **Valid-time × transaction-time (bitemporal) queries** — ask "what did we
  believe as of last Tuesday" independently of "what was actually true on that
  date."
- **Named as-of marks** — give a durable name to a transaction-time pin
  (`TagAsOf`) so a documented knowledge state stays addressable by name.
- **A durable CDC change-log** — every committed mutation is recorded with a
  monotonic LSN, tailable for audit trails or point-in-time recovery.
- **Byte-exact log-shipped read replicas** — a replica reproduces the
  primary's rows verbatim (same hash, same version, same `TxFrom`), not a
  re-derived copy.

It is a **library, not a server**: no main binary, no network protocol, no
query language. Embed it in a Go process for graph storage, temporal history,
and integrity guarantees; layer your own query language or API on top.

## Install

```bash
go get github.com/data-insights-ai/rho-tkg/v4
```

## Quickstart

```go
package main

import (
	"context"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

func main() {
	g, err := graph.New(graph.Config{}) // in-memory store by default — see Backends below
	if err != nil {
		panic(err)
	}
	defer g.Close()

	ctx := context.Background()
	alice, _ := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Alice"})
	bob, _ := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Bob"})
	_, _ = g.Rels().Add(ctx, "KNOWS", alice, bob, nil)

	people, err := g.Nodes().ByLabel("Person", graph.QueryOpts{})
	if err != nil {
		panic(err)
	}
	fmt.Println(len(people), "people in the graph") // 2
}
```

## Hero examples

### (a) Bitemporal read — belief state as of a named pin

<!-- Runnable twin: pkg/graph/example_asof_tags_test.go -->

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

func main() {
	ctx := context.Background()
	g, err := graph.New(graph.Config{})
	if err != nil {
		log.Fatal(err)
	}
	defer g.Close()

	product, _ := g.Nodes().Add(ctx, []string{"Product"}, map[string]any{"price": 100})

	// Pin "everything committed so far" and give the pin a durable name.
	pin, _ := g.Temporal().NowTx()
	_ = g.Temporal().TagAsOf("q1-close", pin)

	// The price changes later — the pinned belief state does not.
	_, _ = g.Nodes().Update(ctx, product.ID(), map[string]any{"price": 140})

	// Reconstruct the belief state as of the named mark, weeks or years later.
	asOf, _, _ := g.Temporal().ResolveAsOf("q1-close")
	asOfNodes, _ := g.Temporal().NodesAsOf(asOf)
	for _, n := range asOfNodes {
		if n.ID() == product.ID() {
			fmt.Println(n.PropertiesMap()["price"]) // 100, not 140
		}
	}
}
```

### (b) Version-chain integrity + time travel

<!-- Runnable twin: pkg/graph/example_temporal_test.go -->

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func main() {
	ctx := context.Background()
	g, err := graph.New(graph.Config{})
	if err != nil {
		log.Fatal(err)
	}
	defer g.Close()

	acct, _ := g.Nodes().Add(ctx, []string{"Account"}, map[string]any{"balance": 100})
	t0 := types.Instant(time.Now().UnixMilli())

	_, _ = g.Nodes().Update(ctx, acct.ID(), map[string]any{"balance": 250})
	_, _ = g.Nodes().Update(ctx, acct.ID(), map[string]any{"balance": 400})

	// Every version is hash-chained to its predecessor; a tampered or
	// corrupted row breaks the chain and VerifyNodeChain reports it.
	ok, err := g.Hash().VerifyNodeChain(acct.ID())
	if err != nil || !ok {
		log.Fatal("hash chain broken")
	}

	// Time travel: read the balance exactly as it stood at t0 — before
	// either update — with no special "history" API required.
	asOfT0, _ := g.Temporal().NodeAt(acct.ID(), t0)
	fmt.Println(asOfT0.PropertiesMap()["balance"]) // 100
}
```

### (c) Change-feed tail with a resume LSN

<!-- Runnable twin: pkg/graph/example_changefeed_test.go -->

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

func main() {
	ctx := context.Background()
	// SyncWrites makes every mutation (and its change-log record) durable
	// before the call returns — simplest way to read your own writes here.
	g, err := graph.New(graph.Config{BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		log.Fatal(err)
	}
	defer g.Close()

	_, _ = g.Nodes().Add(ctx, []string{"Order"}, map[string]any{"status": "placed"})
	_, _ = g.Nodes().Add(ctx, []string{"Order"}, map[string]any{"status": "shipped"})

	// Resume from the last durable checkpoint (0 on first run).
	resumeLSN, _ := g.Replication().AppliedLSN()

	_ = g.Replication().ForEachChange(resumeLSN, func(rec store.ChangeRecord) bool {
		fmt.Printf("lsn=%d tag=%v\n", rec.LSN, rec.Tag)
		resumeLSN = rec.LSN
		return true // keep tailing
	})

	// Persist the watermark so a restart resumes exactly here, not from LSN 0.
	_ = g.Replication().SetAppliedLSN(resumeLSN)
}
```

#### Watching changes

For a long-lived consumer, `g.Replication().Watch(ctx, fromLSN)` returns a Go
`iter.Seq2` that live-tails the change-log instead of one bounded batch: it
polls with ctx-aware backoff (25ms up to a 500ms cap) while idle, resets on
every record, and stops cleanly on `ctx` cancellation or a `break` — no manual
resume loop required. See
[`pkg/graph/example_watch_test.go`](pkg/graph/example_watch_test.go) for a
runnable example.

## Features

| Feature | What it gives you |
|---|---|
| Bitemporal queries | Valid-time × transaction-time point/interval queries (`NodeAtTx`, `NodesAsOf`, `NodeAt`, `RelsDuring`, …) |
| Append-only version history | Every update/delete keeps prior versions; stored rows are never mutated |
| Hash-chained integrity | SHA-256 chain per entity; `g.Hash().VerifyNodeChain`/`VerifyRelChain` detect tampering or corruption |
| Named as-of marks | `TagAsOf`/`ResolveAsOf` give a durable name to a transaction-time pin (§4.2) |
| Transaction-time backfill | `AddWithTx` reproduces a documented historical knowledge state at re-ingest, gated by `Config.AllowTxBackfill` (§4.1) |
| CDC change-log | Durable ordered op-log (`Config.ChangeLog`) with monotonic LSNs; tail via `g.Replication().ForEachChange` |
| Byte-exact read replicas | `Config.ReadOnlyReplica` + `ApplyChange`/`ApplyChanges` reproduce a primary's rows verbatim (Phase 1: log-shipped bootstrap + tail; orchestration/failover automation is external) |
| Delta backups | `g.IO().ExportSince`/`ImportMerge` ship and replay only what changed since a cursor |
| One-call backups | `g.IO().BackupTo`/`BackupDeltaTo` write deterministic, LSN-named backup files; `graph.RestoreInto` replays a full+delta set — see [Backups](#backups) |
| Property, temporal, and vector indexes | Property equality/range lookups, high-frequency temporal buckets, and k-NN vector search — approximate HNSW by default, with an exact brute-force escape hatch (`VectorIndexOptions.UseBruteForce` via `CreateVectorIndexWithOptions`) |
| Encryption at rest | `Config.EncryptionKey` (AES-128/192/256) encrypts every Badger-backed shard; requires `BlockCacheSize`/`IndexCacheSize` > 0 (validated at `New`, never a Badger panic) |
| Transactions & batches | `g.Tx()` (serializable-per-entity) and `g.Batch()` (bulk ops with partial-failure reporting) |
| Event bus | Sync/async hooks on every mutation, for building your own indexes or side effects |

## Backups

`g.IO().BackupTo(dir)` / `g.IO().BackupDeltaTo(dir, since)` wrap `Export`/`ExportSince` with deterministic, LSN-named files (no new wire format) — no manifest to keep in sync, no wall-clock filenames to fight over:

```go
base, _ := g.IO().BackupTo(backupDir)          // backupDir/backup-<lsn>-full.tkg
// ... later, after more mutations ...
next, _ := g.IO().BackupDeltaTo(backupDir, base) // backupDir/backup-<base>-to-<next>-delta.tkg

restored, err := graph.RestoreInto(graph.Config{}, backupDir) // full + every delta, gaplessly
```

`RestoreInto` validates the whole chain (full → delta → delta → …) before replaying anything, naming the file that broke it on a gap or a foreign lineage. See [`docs/persistence.md`](docs/persistence.md#backups-one-call-ergonomics) for the full semantics (empty deltas, overwrite refusal, change-log-less backends).

## Backends

| Backend | Package | Use it when |
|---|---|---|
| `memory.Store` | `pkg/graph/store/memory` | Tests, scratch graphs, no persistence needed |
| `badger.Store` | `pkg/graph/store/badger` | A single embedded on-disk graph, backed by Badger v4 |
| `tiered.Store` | `pkg/graph/store/tiered` | Time-sharded persistence at scale — hot/warm/cold/archive rotation across many Badger shards |
| `sharded.Store` | `pkg/graph/store/sharded` | EXPERIMENTAL (ADR-0007) — slot-topology persistence: N Badger shards routed by the snowflake node field. See [`docs/architecture.md`](docs/architecture.md) |

## Module

```
github.com/data-insights-ai/rho-tkg/v4
```

**Go:** 1.26.1
**License:** Apache-2.0
**Dependencies:** [`rho-snowflake-2026`](https://github.com/bds421/rho-snowflake-2026) (IDs), [`msgpack/v5`](https://github.com/vmihailenco/msgpack) (serialization), [`badger/v4`](https://github.com/dgraph-io/badger) (persistence)

Each concurrent graph instance uses `Config.SnowflakeNodeID` (0–15) to keep
minted IDs unique; see [`docs/architecture.md`](docs/architecture.md#snowflake-ids)
for the full ID layout and epoch.

**Stdlib aliasing convention.** `pkg/graph/hash` and `pkg/graph/io` shadow the
stdlib `hash` and `io` packages. Inside those packages no aliasing is needed.
At any consumer site that imports **both** the stdlib package and the local
one, alias the **local** one with a `tkg` prefix (`tkghash` / `tkgio`) and
leave the stdlib import unaliased.

## Documentation

- [`docs/api.md`](docs/api.md) — API & Core Types: graph layer, registries, validation limits, temporal queries, transactions, and shadow properties.
- [`docs/architecture.md`](docs/architecture.md) — Architecture & Concurrency: system boundaries, entity lock manager, multi-phase iteration, and thread safety.
- [`docs/persistence.md`](docs/persistence.md) — Storage interfaces, `badger.Store`, `tiered.Store`, and EXPERIMENTAL `sharded.Store` slot-topology persistence.
- [`docs/design.md`](docs/design.md) — Design invariants: protocol guarantees, referential integrity, defensive copying, and error sentinels.
- [`docs/stability.md`](docs/stability.md) — API Stability & Deprecation Policy: v4 stability promise, experimental surfaces, and release conventions.
- [`docs/SPEC.md`](docs/SPEC.md) — Formal specifications and algorithms.

## Tutorials

Progressive, runnable tutorials in [`tutorials/`](tutorials/), each a standalone `main.go`:

| Tutorial | Topic |
|----------|-------|
| `001_basic_graph` | Create nodes, relationships, and query the graph |
| `002_temporal` | Temporal metadata (ValidFrom/ValidTo, CreatedAt, UpdatedAt) |
| `003_badger_persistence` | On-disk `badger.Store`, close/reopen, registry persistence |
| `004_full_features` | Update operations, version history, hash chain integrity |
| `005_performance` | Benchmark `memory.Store` vs `badger.Store` (throughput, memory, storage) |

Run any tutorial: `go run ./tutorials/001_basic_graph/`

## Building & testing

```bash
make check   # pre-commit: vet + build + test
make test    # unit tests (short mode)
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full build/test/lint workflow,
including the Docker-based `lint`/`security`/`vulncheck` targets.

## Version history

See [CHANGELOG.md](CHANGELOG.md) for the full, dated release history — every
version back to v3.0.0, with the defect it fixed or the feature it added.

Current release: **v4.28.1** — relationship columns on the memory backend (closing
the asymmetry with badger), a bulk-scan path for badger rel column builds (large
allocation collapse on full-type rebuilds), deterministic replacements for the
last scheduler-dependent CI probes, and a patch so read-only graph transactions
skip registry metadata checkpoint/restore when the global registry state is
clean. Earlier 4.25–4.27 work (vector scored search, exact erasure, columnar
typed scans / zone maps / append-extend, sharded S4–S5, retention purge across
backends) is in `CHANGELOG.md`.

If you are upgrading from v3.x, see `CHANGELOG.md` `[4.0.0]` for the full
public-API migration recipe (context-first methods, `g.Tier` split from
`g.Admin`, sub-API accessor methods) and `[4.4.0]` for the module path change
(`gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4` → `github.com/data-insights-ai/rho-tkg/v4`).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
