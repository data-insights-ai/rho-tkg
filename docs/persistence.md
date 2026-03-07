# Persistence Documentation

## Persistence (Badger)

Configure with `Config.BadgerDir` (on-disk) or `Config.BadgerInMemory: true` (testing):

```go
g, err := graph.New(graph.Config{
    SnowflakeNodeID: 1,
    BadgerDir:       "/path/to/data",
})
// ... use graph ...
g.Close() // saves registries + closes DB
```

Data is serialized using msgpack. Keys use fixed-width binary encoding with single-byte prefix tags for correct sort order. Registries are persisted on `Close()` and restored on startup.

Sync writes: set `Config.SyncWrites: true` to eliminate the 100ms async flush window — each write is flushed to disk synchronously (Badger `WithSyncWrites(true)` + immediate `flush()` after every store call). This removes the in-memory buffer vulnerability at the cost of higher write latency. `FlushInterval` is forced to 0 and the background flush goroutine is not started when `SyncWrites` is true.

## Tiered Persistence (TieredStore)

For workloads with distinct reference data (Case, User) and high-volume events (Signal, Alert):

```go
ts, err := graph.NewTieredStore(graph.TieredStoreConfig{
    DataDir:     "/path/to/data",
    RefLabels:   []string{"Case", "Organization", "User"},
    ShardWindow: 7 * 24 * time.Hour, // weekly event shards
    ColdAfter:   30 * 24 * time.Hour, // demote warm→cold after 30 days
})
g, err := graph.New(graph.Config{
    SnowflakeNodeID: 1,
    Store:           ts,
})
```

Directory layout: `data/meta/` (catalog + registry), `data/reference/` (ref shard), `data/events/<window>/` (event shards), `data/archive/` (archived reference entities). Hot shard receives all new event writes. On window expiry, `RotateHotShard()` demotes hot→warm (read-only) and creates a new hot shard. Warm shards are recovered from catalog on restart. Cold shards are lazy-opened on first access and auto-closed after idle timeout.
