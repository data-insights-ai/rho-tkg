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
type nativeColumnReadCursor struct {
	source                          *nativeColumnSource
	manifest                        nativeColumnManifest
	at                              [2]graphCertificate
	cache                           *nativeColumnCache
	positions                       [2]uint64
	chain                           [32]byte
	ordinal, frames, rows, retained uint64
	complete                        bool
}

func (s *nativeColumnStore) readCursor(ctx context.Context, id [16]byte, source *nativeColumnSource, at [2]graphCertificate, cache *nativeColumnCache) (*nativeColumnReadCursor, error) {
	if ctx == nil || source == nil || source.digest != s.digest {
		return nil, errInvalid
	}
	if err := errors.Join(ctx.Err(), source.ctx.Err()); err != nil {
		return nil, err
	}
	p, err := s.existing(id, source)
	if err != nil {
		return nil, err
	}
	manifest, err := s.checkManifest(id)
	if err != nil {
		return nil, err
	}
	if !nativeColumnSame(manifest.Generation, p.Generation) {
		return nil, errCorrupt
	}
	if cache != nil {
		wire, err := nativeColumnMetadata(manifest)
		if err != nil {
			return nil, err
		}
		cached, err := cache.get(nativeColumnCacheManifestName(manifest.Generation), nativeColumnMetadataBytes)
		if err != nil || !bytes.Equal(wire, cached) {
			return nil, errors.Join(errCorrupt, err)
		}
	}
	return &nativeColumnReadCursor{source: source, manifest: manifest, at: at, cache: cache, positions: [2]uint64{source.initial[0].sourceIndex, source.initial[1].sourceIndex}}, nil
}

func (s *nativeColumnStore) openReadCursor(ctx context.Context, id [16]byte, source *nativeColumnSource, at graphCut, cache *nativeColumnCache) (*nativeColumnReadCursor, error) {
	if source == nil || ctx == nil {
		return nil, errInvalid
	}
	if err := s.begin(ctx); err != nil {
		return nil, err
	}
	defer s.end()
	p, err := s.existing(id, source)
	if err != nil {
		return nil, err
	}
	if err = s.reserveWork(&p, source, source.openWork); err != nil {
		return nil, err
	}
	pair, err := source.at(ctx, at)
	if err != nil {
		return nil, err
	}
	return s.readCursor(ctx, id, source, pair, cache)
}

// Each step rehydrates one actual chunk using the original held source owner.
// Durable attempted-work grants advance even on failure; logical continuation
// commits only AFTER the complete consumer acceptance succeeds. No callback runs
// under store/source locks. Nested store operations refuse through begin.
func (s *nativeColumnStore) nextReadChunk(ctx context.Context, r *nativeColumnReadCursor, accept func(uint64, []byte, []nativeColumnFrame, bool) error) (bool, error) {
	if r == nil || accept == nil || ctx == nil {
		return false, errInvalid
	}
	if err := s.begin(ctx); err != nil {
		return false, err
	}
	defer s.end()
	return s.readChunk(ctx, r, accept)
}
func (s *nativeColumnStore) readChunk(ctx context.Context, r *nativeColumnReadCursor, accept func(uint64, []byte, []nativeColumnFrame, bool) error) (bool, error) {
	if r == nil || accept == nil || r.complete || r.source == nil || r.source.digest != s.digest {
		return false, errInvalid
	}
	source, id := r.source, r.manifest.Generation.ID
	if err := errors.Join(ctx.Err(), source.ctx.Err()); err != nil {
		return false, err
	}
	p, err := s.existing(id, source)
	if err != nil {
		return false, err
	}
	actualManifest, err := s.checkManifest(id)
	if err != nil || !nativeColumnSame(actualManifest, r.manifest) {
		return false, errors.Join(errCorrupt, err)
	}
	if err = s.reserve(&p, source); err != nil {
		return false, err
	}
	prospective := *r
	var body []byte
	var decoded []nativeColumnFrame
	if r.ordinal == r.manifest.Chunks {
		last, next, err := source.page(ctx, r.positions)
		if err != nil || len(last) != 0 || next != r.positions || r.positions != r.manifest.Positions || r.chain != r.manifest.Chain || r.frames != r.manifest.Frames || r.rows != r.manifest.Rows || r.retained != r.manifest.Bytes {
			return false, errors.Join(errCorrupt, err)
		}
		prospective.complete = true
	} else {
		if r.ordinal > r.manifest.Chunks {
			return false, errCorrupt
		}
		key := nativeColumnChunkKey(id, r.ordinal)
		body, err = s.get(key, s.binding.Policy.ChunkBytes)
		if err != nil {
			return false, err
		}
		if r.cache != nil {
			cached, err := r.cache.get(nativeColumnCacheChunkName(r.manifest.Generation, r.ordinal), s.binding.Policy.ChunkBytes)
			if err != nil || !bytes.Equal(cached, body) {
				return false, errors.Join(errCorrupt, err)
			}
		}
		decoded, err = decodeNativeColumnChunk(r.manifest.Generation, r.ordinal, r.chain, body, s.binding.Policy)
		if err != nil {
			return false, err
		}
		frames, next, err := source.page(ctx, r.positions)
		if err != nil {
			return false, err
		}
		genuine, err := encodeNativeColumnChunk(r.manifest.Generation, r.ordinal, r.chain, frames, s.binding.Policy)
		if err != nil || !bytes.Equal(genuine, body) || len(decoded) == 0 {
			return false, errors.Join(errCorrupt, err)
		}
		prospective.positions = next
		prospective.ordinal++
		prospective.frames += uint64(len(decoded))
		prospective.retained += uint64(len(key) + len(body))
		prospective.chain = nativeColumnChain(r.chain, body)
		for _, frame := range decoded {
			group := source.frameGroup(frame.Partition)
			if group < 0 {
				return false, errCorrupt
			}
			prospective.rows += uint64(len(frame.Rows))
		}
	}
	if err := errors.Join(ctx.Err(), source.ctx.Err()); err != nil {
		return false, err
	}
	if err := accept(r.ordinal, body, decoded, prospective.complete); err != nil {
		return false, err
	}
	if err := errors.Join(ctx.Err(), source.ctx.Err()); err != nil {
		return false, err
	}
	*r = prospective
	return r.complete, nil
}
func (s *nativeColumnSource) frameGroup(partition uint64) int {
	for g := range 2 {
		if s.covers(g) && s.final[g].scope.partition == partition {
			return g
		}
	}
	return -1
}
func (s *nativeColumnStore) read(ctx context.Context, id [16]byte, source *nativeColumnSource, at [2]graphCertificate, cache *nativeColumnCache, emit func(nativeColumnRow) error, copyChunk func(uint64, []byte) error) (nativeColumnManifest, error) {
	if emit == nil {
		return nativeColumnManifest{}, errInvalid
	}
	cursor, err := s.readCursor(ctx, id, source, at, cache)
	if err != nil {
		return nativeColumnManifest{}, err
	}
	for {
		complete, err := s.readChunk(ctx, cursor, func(n uint64, body []byte, frames []nativeColumnFrame, eof bool) error {
			if eof {
				return nil
			}
			if copyChunk != nil {
				if err := copyChunk(n, body); err != nil {
					return err
				}
			}
			for _, frame := range frames {
				group := source.frameGroup(frame.Partition)
				if group < 0 {
					return errCorrupt
				}
				if frame.Index <= at[group].sourceIndex {
					for _, row := range frame.Rows {
						if err := emit(row); err != nil {
							return err
						}
					}
				}
			}
			return nil
		})
		if err != nil {
			return nativeColumnManifest{}, err
		}
		if complete {
			return cursor.manifest, nil
		}
	}
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
