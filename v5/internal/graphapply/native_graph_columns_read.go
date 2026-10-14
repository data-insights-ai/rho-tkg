package graphapply

import (
	"bytes"
	"context"
	"errors"
)

func (s *nativeColumnStore) walk(ctx context.Context, id [16]byte, source *nativeColumnSource, at graphCut, cache *nativeColumnCache, emit func(nativeColumnRow) error) (nativeColumnManifest, error) {
	if source == nil || emit == nil || ctx == nil {
		return nativeColumnManifest{}, errInvalid
	}
	if err := s.begin(ctx); err != nil {
		return nativeColumnManifest{}, err
	}
	defer s.end()
	if err := errors.Join(ctx.Err(), source.ctx.Err()); err != nil {
		return nativeColumnManifest{}, err
	}
	p, err := s.existing(id, source)
	if err != nil {
		return nativeColumnManifest{}, err
	}
	if err = s.reserveWork(&p, source, source.openWork); err != nil {
		return nativeColumnManifest{}, err
	}
	certificates, err := source.at(ctx, at)
	if err != nil {
		return nativeColumnManifest{}, err
	}
	return s.read(ctx, id, source, certificates, cache, emit, nil)
}

// The cache may supply bytes but never source/cut certification. Every page
// rehydrates authentic originals at the bound native cut and compares the actual
// typed column chunk before emitting. Consumer callbacks run outside all mutexes.
// A prefix can escape before an error; only a returned manifest is complete EOF.
func (s *nativeColumnStore) read(ctx context.Context, id [16]byte, source *nativeColumnSource, at [2]graphCertificate, cache *nativeColumnCache, emit func(nativeColumnRow) error, copyChunk func(uint64, []byte) error) (nativeColumnManifest, error) {
	if source == nil || source.digest != s.digest || emit == nil {
		return nativeColumnManifest{}, errInvalid
	}
	p, err := s.existing(id, source)
	if err != nil {
		return nativeColumnManifest{}, err
	}
	manifest, err := s.checkManifest(id)
	if err != nil {
		return nativeColumnManifest{}, err
	}
	if !nativeColumnSame(manifest.Generation, p.Generation) {
		return nativeColumnManifest{}, errCorrupt
	}
	manifestWire, err := nativeColumnMetadata(manifest)
	if err != nil {
		return nativeColumnManifest{}, err
	}
	if cache != nil {
		cached, err := cache.get(nativeColumnCacheManifestName(manifest.Generation), nativeColumnMetadataBytes)
		if err != nil {
			return nativeColumnManifest{}, err
		}
		if !bytes.Equal(manifestWire, cached) {
			return nativeColumnManifest{}, errCorrupt
		}
	}
	positions := [2]uint64{source.initial[0].sourceIndex, source.initial[1].sourceIndex}
	var chain [32]byte
	var frames, rows, retained uint64
	for ordinal := uint64(0); ordinal < manifest.Chunks; ordinal++ {
		if err := errors.Join(ctx.Err(), source.ctx.Err()); err != nil {
			return nativeColumnManifest{}, err
		}
		if err = s.reserve(&p, source); err != nil {
			return nativeColumnManifest{}, err
		}
		key := nativeColumnChunkKey(id, ordinal)
		body, err := s.get(key, s.binding.Policy.ChunkBytes)
		if err != nil {
			return nativeColumnManifest{}, err
		}
		if cache != nil {
			cached, err := cache.get(nativeColumnCacheChunkName(manifest.Generation, ordinal), s.binding.Policy.ChunkBytes)
			if err != nil {
				return nativeColumnManifest{}, err
			}
			if !bytes.Equal(cached, body) {
				return nativeColumnManifest{}, errCorrupt
			}
		}
		decoded, err := decodeNativeColumnChunk(manifest.Generation, ordinal, chain, body, s.binding.Policy)
		if err != nil {
			return nativeColumnManifest{}, err
		}
		actual, next, err := source.page(ctx, positions)
		if err != nil {
			return nativeColumnManifest{}, err
		}
		genuine, err := encodeNativeColumnChunk(manifest.Generation, ordinal, chain, actual, s.binding.Policy)
		if err != nil || !bytes.Equal(body, genuine) {
			return nativeColumnManifest{}, errors.Join(errCorrupt, err)
		}
		positions = next
		frames += uint64(len(decoded))
		retained += uint64(len(key) + len(body))
		chain = nativeColumnChain(chain, body)
		if copyChunk != nil {
			if err := copyChunk(ordinal, body); err != nil {
				return nativeColumnManifest{}, err
			}
		}
		for _, frame := range decoded {
			group := -1
			for g, h := range source.hosts {
				if h.machine.ns.partition == frame.Partition {
					group = g
				}
			}
			if group < 0 {
				return nativeColumnManifest{}, errCorrupt
			}
			rows += uint64(len(frame.Rows))
			if frame.Index <= at[group].sourceIndex {
				for _, row := range frame.Rows {
					if err := emit(row); err != nil {
						return nativeColumnManifest{}, err
					}
				}
			}
		}
	}
	if chain != manifest.Chain || frames != manifest.Frames || rows != manifest.Rows || retained != manifest.Bytes || positions != manifest.Positions {
		return nativeColumnManifest{}, errCorrupt
	}
	// True EOF is inspected even for an empty generation, with the same owner,
	// cumulative grant and deadline. Missing source frames can never be empty.
	if err = s.reserve(&p, source); err != nil {
		return nativeColumnManifest{}, err
	}
	last, next, err := source.page(ctx, positions)
	if err != nil || len(last) != 0 || next != positions {
		return nativeColumnManifest{}, errors.Join(errCorrupt, err)
	}
	if err := errors.Join(ctx.Err(), source.ctx.Err()); err != nil {
		return nativeColumnManifest{}, err
	}
	return manifest, nil
}

func (s *nativeColumnStore) cache(ctx context.Context, id [16]byte, source *nativeColumnSource, cache *nativeColumnCache) error {
	if source == nil || cache == nil || ctx == nil {
		return errInvalid
	}
	if err := s.begin(ctx); err != nil {
		return err
	}
	defer s.end()
	manifest, err := s.checkManifest(id)
	if err != nil {
		return err
	}
	complete, err := s.read(ctx, id, source, source.final, nil, func(nativeColumnRow) error { return nil }, func(n uint64, b []byte) error {
		return cache.put(nativeColumnCacheChunkName(manifest.Generation, n), b)
	})
	if err != nil {
		return err
	}
	body, err := nativeColumnMetadata(complete)
	if err != nil {
		return err
	}
	if err := errors.Join(ctx.Err(), source.ctx.Err()); err != nil {
		return err
	}
	return cache.put(nativeColumnCacheManifestName(complete.Generation), body)
}
