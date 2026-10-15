package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Interval columns are a fresh layout: Initial.Groups is authenticated and is
// part of every chunk's generation hash. No full-prefix census is relabelled.
const graphFeedRetainedColumnLayout = "scalar-feed-checkpoint-i64-columns-v1"

func graphFeedRetainedColumnProfile(h *harness, p graphColumnPolicy) bool {
	return h != nil && validGraphFeedRetainedConfig(h.config) && p.Version == 2 && p.validate() == nil
}
func graphFeedRetainedColumnPolicy(p graphCostProfile) graphColumnPolicy {
	// Two generations (initial->C and checkpoint->final), plus bounded cache
	// and owner controls. Each original contributes one logical row, not one
	// row per physical write. Actual byte/work exhaustion remains a failure.
	return graphColumnPolicy{Version: 2, ChunkRows: 16, ChunkBytes: 64 << 10,
		MaxChunks: 2*((2*p.Facts+3+15)/16) + 16, MaxGenerations: 2,
		MaxRetainedBytes: (64 << 20) + p.Facts*(32<<10),
		MaxWorkRows:      2*p.Facts + 3, MaxWorkBytes: (64 << 20) + p.Facts*(64<<10)}
}
func graphColumnInterval(g graphColumnGeneration) bool { return g.Version == 2 }
func graphColumnIntervalRows(g graphColumnGeneration, end [2]uint64) (uint64, bool) {
	if end[0] < g.Initial.Groups[0] || end[1] < g.Initial.Groups[1] {
		return 0, false
	}
	return end[0] - g.Initial.Groups[0] + end[1] - g.Initial.Groups[1], true
}
func graphColumnGenerationVersion(s *graphColumnStore) uint8 {
	if s.binding.Version == 3 && s.binding.Policy.Version == 2 && strings.HasPrefix(s.h.config.Graph, graphFeedRetainedNamespace) {
		return 2
	}
	return 1
}

func graphFeedRetainedFillCache(ctx context.Context, s *graphColumnStore, id string, cache *os.Root, a *graphFeedRetainedArtifacts) error {
	m, err := s.manifest(id)
	if err != nil {
		return err
	}
	var chain [32]byte
	for ordinal := uint64(0); ordinal < m.Chunks; ordinal++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		body, _, err := graphCostParallelRead(ctx, s.h.db, func(tr fdb.ReadTransaction) ([]byte, error) {
			raw, err := tr.Get(s.chunkKey(id, ordinal)).Get()
			if err != nil {
				return nil, err
			}
			if len(raw) > s.binding.Policy.ChunkBytes {
				return nil, errLimit
			}
			if _, err = decodeGraphColumnChunk(s.h, m.Generation, ordinal, chain, raw, s.binding.Policy); err != nil {
				return nil, err
			}
			return bytes.Clone(raw), nil
		})
		if err != nil {
			return err
		}
		f, err := cache.OpenFile(graphColumnCacheName(m.Generation, ordinal), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, writeErr := f.Write(body)
		if err = errors.Join(writeErr, f.Sync(), f.Close()); err != nil {
			return err
		}
		if err = a.event(struct {
			Type       string
			Generation string
			Ordinal    uint64
			Body       []byte
		}{"cache-fill", id, ordinal, body}); err != nil {
			return err
		}
		chain = graphColumnChain(chain, body)
	}
	if chain != m.Chain {
		return errMismatch
	}
	return nil
}
