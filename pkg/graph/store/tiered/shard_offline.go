package tiered

import (
	"context"
	"fmt"
	"path/filepath"
)

// MoveShardOffline moves the reference shard ("reference") or the archive
// ("archive") of the closed store at cfg to volume (hot, warm or cold; the
// reference shard never to cold). Both are open, or may open, whenever the
// store is, so they move only while it is closed (ADR-0012, SHARD step 7);
// event shards move online (Store.MoveShard). It refuses while a process has
// the store open (badger's lock on the reference shard) and holds that lock
// for the move, so the store cannot open meanwhile. The move is the mover's:
// copy, rename, catalog save (the commit point), source removal; New removes
// the copy the catalog does not name. ctx stops it before the catalog save.
func MoveShardOffline(ctx context.Context, cfg Config, name, volume string) error {
	return moveShardOffline(ctx, cfg, name, volume, nil)
}

// moveShardOffline is MoveShardOffline with the mover's fault hook.
func moveShardOffline(ctx context.Context, cfg Config, name, volume string, fault func(step string) error) error {
	if name != "reference" && name != "archive" {
		return fmt.Errorf("graph: MoveShardOffline: %q is neither the reference shard nor the archive (event shards move online: MoveShard)", name)
	}
	vol, ok := parseVolume(volume)
	if !ok {
		return fmt.Errorf("graph: MoveShardOffline: %q is not a volume (hot, warm or cold)", volume)
	}
	if name == "reference" && vol == VolumeCold {
		return fmt.Errorf("graph: MoveShardOffline: the reference shard cannot be cold: every commit and unique check reads it")
	}
	if cfg.InMemory {
		return fmt.Errorf("graph: MoveShardOffline: an in-memory store has no volumes")
	}
	ts := &Store{dataDir: cfg.DataDir, moveFault: fault}
	if err := ts.volumeConfig(cfg); err != nil {
		return err
	}
	dstRoot, ok := ts.volumeDir(vol)
	if !ok {
		return fmt.Errorf("graph: MoveShardOffline: volume %s, which the config does not map (Config.WarmDir, Config.ColdDir)", volumeName(vol))
	}
	ts.catalog = NewShardCatalog(filepath.Join(cfg.DataDir, "meta", "shard_catalog.json"))
	if err := ts.catalog.Load(); err != nil {
		return fmt.Errorf("graph: MoveShardOffline: load catalog: %w", err)
	}
	if err := ts.checkCatalogVolumes(); err != nil {
		return err
	}
	entry, ok := ts.catalog.GetShard(name)
	if !ok {
		return fmt.Errorf("graph: MoveShardOffline: the store at %s has no %s", cfg.DataDir, name)
	}
	if entry.Volume == vol {
		return nil
	}
	unlock, err := lockShardDir(ts.shardDir("reference"))
	if err != nil {
		return fmt.Errorf("graph: MoveShardOffline: the store is open (its reference shard is locked): %w", err)
	}
	defer unlock()
	return ts.moveClosedShard(ctx, name, name, vol, dstRoot)
}
