package graphapply

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// Separate embedding-declared DERIVED namespace. Its Pebble WAL commits column
// progress only; it cannot acknowledge graph truth or mutate the source log.
// Pebble's actual LOCK owns the directory. All files belong in total cost.
type nativeColumnStore struct {
	mu             sync.Mutex
	active, closed bool
	poison         error
	db             *pebble.DB
	binding        nativeColumnBinding
	digest         [32]byte
}

func openNativeColumnStore(dir string, fs vfs.FS, create bool, source *nativeColumnSource) (store *nativeColumnStore, err error) {
	if dir == "" || source == nil || source.binding.Policy.validate() != nil {
		return nil, errInvalid
	}
	if fs == nil {
		fs = vfs.Default
	}
	db, err := pebble.Open(dir, &pebble.Options{FS: fs, ErrorIfExists: create, ErrorIfNotExists: !create, CacheSize: source.binding.Policy.CacheBytes, MemTableSize: source.binding.Policy.MemTableBytes, MemTableStopWritesThreshold: 2, MaxOpenFiles: 32, FormatMajorVersion: pebble.FormatMinSupported, MaxManifestFileSize: 1 << 20})
	if err != nil {
		return nil, err
	}
	s := &nativeColumnStore{db: db, binding: source.binding, digest: source.digest}
	defer func() {
		if err != nil {
			err = errors.Join(err, db.Close())
		}
	}()
	wire, err := nativeColumnMetadata(s.binding)
	if err != nil {
		return nil, err
	}
	if create {
		ledger := nativeColumnLedger{Bytes: 2*nativeColumnMetadataBytes + uint64(len("binding")+len("ledger"))}
		l, e := nativeColumnMetadata(ledger)
		if e != nil {
			return nil, e
		}
		err = s.commit([]nativeColumnPut{{[]byte("binding"), wire}, {[]byte("ledger"), l}})
		if err != nil {
			return nil, err
		}
	} else {
		actual, e := s.get([]byte("binding"), nativeColumnMetadataBytes)
		if e != nil {
			return nil, e
		}
		if !bytes.Equal(actual, wire) {
			return nil, errInvalid
		}
		if _, e = s.ledger(); e != nil {
			return nil, e
		}
	}
	return s, nil
}
func (s *nativeColumnStore) begin(ctx context.Context) error {
	if s == nil || ctx == nil {
		return errInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return errInvalid
	}
	if s.poison != nil {
		return s.poison
	}
	if s.active {
		return errLimit
	}
	s.active = true
	return nil
}
func (s *nativeColumnStore) end() { s.mu.Lock(); s.active = false; s.mu.Unlock() }
func (s *nativeColumnStore) Close() error {
	if s == nil {
		return errInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if s.active {
		return errLimit
	}
	s.closed = true
	return s.db.Close()
}
func (s *nativeColumnStore) get(key []byte, maximum int) (body []byte, err error) {
	v, closer, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}
	if len(v) > maximum {
		return nil, errors.Join(errLimit, closer.Close())
	}
	body = bytes.Clone(v)
	err = closer.Close()
	if err != nil {
		body = nil
	}
	return body, err
}

type nativeColumnPut struct{ key, value []byte }

func (s *nativeColumnStore) commit(writes []nativeColumnPut) (err error) {
	if len(writes) < 1 || len(writes) > 4 {
		return errInvalid
	}
	b := s.db.NewBatch()
	defer func() {
		err = errors.Join(err, b.Close())
		if err != nil {
			s.mu.Lock()
			s.poison = errors.Join(errCorrupt, err)
			s.mu.Unlock()
			err = s.poison
		}
	}()
	for _, w := range writes {
		if e := b.Set(w.key, w.value, nil); e != nil {
			return e
		}
	}
	if err = b.Commit(pebble.Sync); err != nil {
		s.mu.Lock()
		s.poison = errors.Join(errCorrupt, err)
		s.mu.Unlock()
		return s.poison
	}
	return nil
}
func nativeColumnKey(id [16]byte, kind string) []byte {
	return []byte("generation/" + hex.EncodeToString(id[:]) + "/" + kind)
}
func nativeColumnChunkKey(id [16]byte, ordinal uint64) []byte {
	var n [8]byte
	for i := range 8 {
		n[7-i] = byte(ordinal >> (8 * i))
	}
	return nativeColumnKey(id, "chunk/"+hex.EncodeToString(n[:]))
}
func (s *nativeColumnStore) ledger() (nativeColumnLedger, error) {
	var l nativeColumnLedger
	b, err := s.get([]byte("ledger"), nativeColumnMetadataBytes)
	if err != nil {
		return l, err
	}
	if err = nativeColumnDecodeMetadata(b, &l); err != nil {
		return l, err
	}
	p := s.binding.Policy
	if l.Bytes < 2*nativeColumnMetadataBytes || l.Bytes > p.MaxRetainedBytes || l.Generations > p.MaxGenerations || l.Chunks > p.MaxChunks {
		return l, errCorrupt
	}
	return l, nil
}
func (s *nativeColumnStore) progress(id [16]byte) (nativeColumnProgress, error) {
	var p nativeColumnProgress
	b, err := s.get(nativeColumnKey(id, "progress"), nativeColumnMetadataBytes)
	if err != nil {
		return p, err
	}
	if err = nativeColumnDecodeMetadata(b, &p); err != nil {
		return p, err
	}
	limit := s.binding.Policy
	if p.Generation.ID != id || id == ([16]byte{}) || p.Generation.Binding != s.digest || p.Next > limit.MaxChunks || p.Rows > limit.MaxDecodedRows || p.Bytes > limit.MaxRetainedBytes || !p.Work.admit(nativeColumnWork{}, limit) || p.Frames < p.Next || p.Frames > p.Next*uint64(limit.ChunkRecords) || p.Rows > p.Frames*uint64(limit.ChunkRows) {
		return p, errCorrupt
	}
	var frames uint64
	for g, boundary := range p.Generation.Sources {
		initial, e := decodeGraphCertificate(boundary.Initial)
		if e != nil {
			return p, e
		}
		final, e := decodeGraphCertificate(boundary.Final)
		if e != nil {
			return p, e
		}
		if boundary.Generation == 0 || initial.scope != final.scope || initial.configuration != final.configuration || initial.round > final.round || initial.sourceIndex > final.sourceIndex || p.Positions[g] < initial.sourceIndex || p.Positions[g] > final.sourceIndex {
			return p, errCorrupt
		}
		frames += p.Positions[g] - initial.sourceIndex
		if p.EOF && p.Positions[g] != final.sourceIndex {
			return p, errCorrupt
		}
	}
	if frames != p.Frames || p.Next == 0 && (p.Chain != ([32]byte{}) || p.Bytes != 0 || p.Rows != 0) || p.Next > 0 && p.Chain == ([32]byte{}) {
		return p, errCorrupt
	}
	return p, nil
}

func (s *nativeColumnStore) ensure(id [16]byte, source *nativeColumnSource) (nativeColumnProgress, error) {
	if source == nil || source.digest != s.digest {
		return nativeColumnProgress{}, errInvalid
	}
	gen, err := source.generation(id)
	if err != nil {
		return nativeColumnProgress{}, err
	}
	prior, err := s.progress(id)
	if err == nil {
		if !nativeColumnSame(prior.Generation, gen) {
			return nativeColumnProgress{}, errInvalid
		}
		return prior, nil
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return nativeColumnProgress{}, err
	}
	l, err := s.ledger()
	if err != nil {
		return prior, err
	}
	reserved := uint64(2*nativeColumnMetadataBytes + len(nativeColumnKey(id, "progress")) + len(nativeColumnKey(id, "manifest")))
	if l.Generations == s.binding.Policy.MaxGenerations || reserved > s.binding.Policy.MaxRetainedBytes-l.Bytes {
		return prior, errLimit
	}
	prior = nativeColumnProgress{Generation: gen, Positions: [2]uint64{source.initial[0].sourceIndex, source.initial[1].sourceIndex}}
	l.Bytes += reserved
	l.Generations++
	progress, err := nativeColumnMetadata(prior)
	if err != nil {
		return prior, err
	}
	ledger, err := nativeColumnMetadata(l)
	if err != nil {
		return prior, err
	}
	if err = s.commit([]nativeColumnPut{{nativeColumnKey(id, "progress"), progress}, {[]byte("ledger"), ledger}}); err != nil {
		return nativeColumnProgress{}, err
	}
	return prior, nil
}

// Authenticated source construction is charged once for each actual owner,
// including a new original-At capture after physical publisher restart.
func (s *nativeColumnStore) reserve(p *nativeColumnProgress, source *nativeColumnSource) error {
	return s.reserveWork(p, source, source.grant)
}
func (s *nativeColumnStore) reserveWork(p *nativeColumnProgress, source *nativeColumnSource, grant nativeColumnWork) error {
	if p.Owner != source.owner {
		grant = grant.add(source.openWork)
	}
	if !p.Work.admit(grant, s.binding.Policy) {
		return errLimit
	}
	p.Work = p.Work.add(grant)
	p.Owner = source.owner
	body, err := nativeColumnMetadata(*p)
	if err != nil {
		return err
	}
	return s.commit([]nativeColumnPut{{nativeColumnKey(p.Generation.ID, "progress"), body}})
}
func (s *nativeColumnStore) publishPage(ctx context.Context, id [16]byte, source *nativeColumnSource) (nativeColumnManifest, error) {
	if err := s.begin(ctx); err != nil {
		return nativeColumnManifest{}, err
	}
	defer s.end()
	return s.page(ctx, id, source)
}
func (s *nativeColumnStore) page(ctx context.Context, id [16]byte, source *nativeColumnSource) (nativeColumnManifest, error) {
	p, err := s.ensure(id, source)
	if err != nil {
		return nativeColumnManifest{}, err
	}
	if p.EOF {
		return s.checkManifest(id)
	}
	if err = s.reserve(&p, source); err != nil {
		return nativeColumnManifest{}, err
	}
	frames, next, err := source.page(ctx, p.Positions)
	if err != nil {
		return nativeColumnManifest{}, err
	}
	if len(frames) == 0 {
		for g := range 2 {
			if next[g] != source.final[g].sourceIndex {
				return nativeColumnManifest{}, errCorrupt
			}
		}
		manifest := nativeColumnManifestFor(p)
		if err = s.reserveWork(&p, source, source.openWork); err != nil {
			return nativeColumnManifest{}, err
		}
		err = source.quiescent(ctx, func() error {
			p.EOF = true
			progress, e := nativeColumnMetadata(p)
			if e != nil {
				return e
			}
			wire, e := nativeColumnMetadata(manifest)
			if e != nil {
				return e
			}
			return s.commit([]nativeColumnPut{{nativeColumnKey(id, "progress"), progress}, {nativeColumnKey(id, "manifest"), wire}})
		})
		if err != nil {
			return nativeColumnManifest{}, err
		}
		if err = errors.Join(ctx.Err(), source.ctx.Err()); err != nil {
			return nativeColumnManifest{}, err
		}
		return manifest, nil
	}
	wire, err := encodeNativeColumnChunk(p.Generation, p.Next, p.Chain, frames, s.binding.Policy)
	if err != nil {
		return nativeColumnManifest{}, err
	}
	key := nativeColumnChunkKey(id, p.Next)
	l, err := s.ledger()
	if err != nil {
		return nativeColumnManifest{}, err
	}
	used := uint64(len(key) + len(wire))
	if l.Chunks == s.binding.Policy.MaxChunks || used > s.binding.Policy.MaxRetainedBytes-l.Bytes {
		return nativeColumnManifest{}, errLimit
	}
	if _, err := s.get(key, s.binding.Policy.ChunkBytes); err == nil {
		return nativeColumnManifest{}, errCorrupt
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return nativeColumnManifest{}, err
	}
	p.Next++
	p.Positions = next
	p.Frames += uint64(len(frames))
	p.Bytes += used
	p.Chain = nativeColumnChain(p.Chain, wire)
	for _, frame := range frames {
		p.Rows += uint64(len(frame.Rows))
	}
	l.Bytes += used
	l.Chunks++
	progress, err := nativeColumnMetadata(p)
	if err != nil {
		return nativeColumnManifest{}, err
	}
	ledger, err := nativeColumnMetadata(l)
	if err != nil {
		return nativeColumnManifest{}, err
	}
	if err = errors.Join(ctx.Err(), source.ctx.Err()); err != nil {
		return nativeColumnManifest{}, err
	}
	if err = s.commit([]nativeColumnPut{{key, wire}, {nativeColumnKey(id, "progress"), progress}, {[]byte("ledger"), ledger}}); err != nil {
		return nativeColumnManifest{}, err
	}
	return nativeColumnManifest{}, nil
}
func (s *nativeColumnStore) existing(id [16]byte, source *nativeColumnSource) (nativeColumnProgress, error) {
	if source == nil || source.digest != s.digest {
		return nativeColumnProgress{}, errInvalid
	}
	gen, err := source.generation(id)
	if err != nil {
		return nativeColumnProgress{}, err
	}
	p, err := s.progress(id)
	if errors.Is(err, pebble.ErrNotFound) {
		return nativeColumnProgress{}, errNativeColumnIncomplete
	}
	if err != nil {
		return nativeColumnProgress{}, err
	}
	if !nativeColumnSame(p.Generation, gen) {
		return nativeColumnProgress{}, errInvalid
	}
	return p, nil
}
func (s *nativeColumnStore) publish(ctx context.Context, id [16]byte, source *nativeColumnSource) (nativeColumnManifest, error) {
	if err := s.begin(ctx); err != nil {
		return nativeColumnManifest{}, err
	}
	defer s.end()
	for {
		manifest, err := s.page(ctx, id, source)
		if err != nil {
			return nativeColumnManifest{}, err
		}
		if manifest.Generation.ID != ([16]byte{}) {
			// A sealed replay still verifies every actual original. It cannot
			// adopt a cached summary as source completeness.
			return s.read(ctx, id, source, source.final, nil, func(nativeColumnRow) error { return nil }, nil)
		}
	}
}
func (s *nativeColumnStore) checkManifest(id [16]byte) (nativeColumnManifest, error) {
	var manifest nativeColumnManifest
	p, err := s.progress(id)
	if errors.Is(err, pebble.ErrNotFound) {
		return manifest, errNativeColumnIncomplete
	}
	if err != nil {
		return manifest, err
	}
	if !p.EOF {
		return manifest, errNativeColumnIncomplete
	}
	wire, err := s.get(nativeColumnKey(id, "manifest"), nativeColumnMetadataBytes)
	if errors.Is(err, pebble.ErrNotFound) {
		return manifest, errNativeColumnIncomplete
	}
	if err != nil {
		return manifest, err
	}
	if err = nativeColumnDecodeMetadata(wire, &manifest); err != nil {
		return manifest, err
	}
	if !nativeColumnSame(manifest, nativeColumnManifestFor(p)) {
		return nativeColumnManifest{}, errCorrupt
	}
	return manifest, nil
}
func (s *nativeColumnStore) manifest(id [16]byte) (nativeColumnManifest, error) {
	if err := s.begin(context.Background()); err != nil {
		return nativeColumnManifest{}, err
	}
	defer s.end()
	return s.checkManifest(id)
}
