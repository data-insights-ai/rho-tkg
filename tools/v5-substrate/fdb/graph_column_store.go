package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type graphColumnLedger struct{ Bytes, Generations, Chunks uint64 }

// MaxWorkBytes accounts encoded chunk bodies plus authenticated original,
// request/ID and physical history key/value bytes. Fixed binding/config/cut,
// progress/guard and graph metadata checks and SDK retries are excluded.
type graphColumnProgress struct {
	Generation                   graphColumnGeneration
	Metadata                     [2]metadata
	Next, Rows, Bytes, WorkBytes uint64
	Sequences, History           [2]uint64
	Chain                        [32]byte
	Previous                     outcome
	EOF                          bool
	// Historical full-source publication census remains distinct from physical
	// retained metadata after an explicitly selected source retirement.
	PublicationHistory [2]uint64 `json:",omitzero"`
}
type graphColumnManifest struct {
	Generation          graphColumnGeneration
	Metadata            [2]metadata
	Chunks, Rows, Bytes uint64
	History             [2]uint64
	Chain               [32]byte
}
type graphColumnStore struct {
	ctx     context.Context
	h       *harness
	binding graphColumnBinding
	digest  [32]byte
}

// Retain-all namespace has its own explicit quotas. No existing config bytes,
// KV admission or graph metadata are rewritten. Failed uploads keep charged
// immutable chunks/reservations; there is no expiry, GC, or capacity claim.
func newGraphColumnStore(h *harness, p graphColumnPolicy) (*graphColumnStore, error) {
	return newGraphColumnStoreContext(nil, h, p)
}
func newGraphColumnStoreContext(ctx context.Context, h *harness, p graphColumnPolicy) (*graphColumnStore, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if !graphColumnProfile(h, p) {
		return nil, errInvalid
	}
	binding := graphColumnBinding{Version: 1, Layout: graphColumnLayout, Schema: "node-presence;edge-life-bound;property-p:i64;canonical-original:v1", Config: h.config, Scope: h.config.CostScope, Policy: p}
	if h.config.Cost.Version == 4 {
		binding.Version, binding.Layout, binding.Schema = 2, graphColumnEdgeUniqueLayout, "node-life-create-close;edge-life-bound-create-close;property-p:i64:effective-unique;canonical-original:v1"
	}
	body, err := wire(binding)
	if err != nil {
		return nil, err
	}
	s := &graphColumnStore{ctx: ctx, h: &harness{db: h.db, config: h.config}, binding: binding, digest: sha256.Sum256(body)}
	_, err = s.write(func(tr fdb.Transaction) (any, error) {
		for g := uint8(0); g < 2; g++ {
			if err := h.checkConfig(tr, g); err != nil {
				return nil, err
			}
		}
		old, err := tr.Get(s.key("binding")).Get()
		if err != nil {
			return nil, err
		}
		if old != nil {
			if !bytes.Equal(old, body) {
				return nil, errMismatch
			}
			_, err := s.ledger(tr)
			return nil, err
		}
		rows, err := tr.GetRange(prefixRange(s.prefix()), fdb.RangeOptions{Limit: 1}).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if len(rows) != 0 {
			return nil, errUnavailable
		} // Partial/foreign namespace is never adopted.
		reserved := uint64(len(s.key("binding")) + len(body) + len(s.key("ledger")) + 256)
		if reserved > p.MaxRetainedBytes {
			return nil, errLimit
		}
		tr.Set(s.key("binding"), body)
		return nil, put(tr, s.key("ledger"), graphColumnLedger{Bytes: reserved})
	})
	if err != nil {
		return nil, graphColumnCommitError(err)
	}
	return s, nil
}
func (s *graphColumnStore) read(fn func(fdb.ReadTransaction) (any, error)) (any, error) {
	return graphCostReadContext(s.ctx, s.h.db, fn)
}
func (s *graphColumnStore) write(fn func(fdb.Transaction) (any, error)) (any, error) {
	return graphCostWriteContext(s.ctx, s.h.db, fn)
}
func (s *graphColumnStore) writeContext(ctx context.Context, fn func(fdb.Transaction) (any, error)) (any, error) {
	bounded, cancel := graphCostIOContext(s.ctx, ctx)
	defer cancel()
	return graphCostWriteContext(bounded, s.h.db, fn)
}
func (s *graphColumnStore) forContext(ctx context.Context) (*graphColumnStore, func()) {
	if s.ctx == nil {
		return s, func() {}
	}
	bounded, cancel := graphCostIOContext(s.ctx, ctx)
	// Only the ephemeral IO lifetime differs; the same immutable binding and
	// native reducers remain authoritative. Never replace an existing owner's s.
	own := *s
	own.ctx = bounded
	return &own, cancel
}
func (s *graphColumnStore) prefix() string          { return s.h.prefix(0) + "columns-v1/" }
func (s *graphColumnStore) key(name string) fdb.Key { return fdb.Key(s.prefix() + name) }
func (s *graphColumnStore) generationKey(id, kind string) fdb.Key {
	return s.key("generation/" + hex.EncodeToString([]byte(id)) + "/" + kind)
}
func (s *graphColumnStore) chunkKey(id string, n uint64) fdb.Key {
	return s.generationKey(id, fmt.Sprintf("chunk/%016x", n))
}
func (s *graphColumnStore) ledger(tr fdb.ReadTransaction) (graphColumnLedger, error) {
	l, found, err := get[graphColumnLedger](tr, s.key("ledger"))
	p := s.binding.Policy
	if err != nil {
		return l, err
	}
	if !found || l.Bytes > p.MaxRetainedBytes || l.Generations > p.MaxGenerations || l.Chunks > p.MaxChunks {
		return l, errInvalid
	}
	return l, nil
}
func graphColumnSame(a, b any) bool {
	left, e1 := wire(a)
	right, e2 := wire(b)
	return e1 == nil && e2 == nil && bytes.Equal(left, right)
}
func (s *graphColumnStore) check(tr fdb.ReadTransaction, g graphColumnGeneration) error {
	if g.Version != 1 || !validName(g.ID) || g.Binding != s.digest || !slices.Equal(g.Initial.Scope, []uint8{0, 1}) || !slices.Equal(g.Cut.Scope, []uint8{0, 1}) || g.Initial.Groups != [2]uint64{} || g.Initial.Round > g.Cut.Round {
		return errInvalid
	}
	for _, groups := range g.Cut.Groups {
		if groups > s.h.config.Limits.Transactions {
			return errInvalid
		}
	}
	if g.Cut.Groups[0]+g.Cut.Groups[1] > s.binding.Policy.MaxWorkRows {
		return errLimit
	}
	binding, err := tr.Get(s.key("binding")).Get()
	if err != nil {
		return err
	}
	body, err := wire(s.binding)
	if err != nil || !bytes.Equal(binding, body) {
		return errors.Join(errMismatch, err)
	}
	for group := uint8(0); group < 2; group++ {
		if err := s.h.checkConfig(tr, group); err != nil {
			return err
		}
	}
	for _, c := range []certificate{g.Initial, g.Cut} {
		if err := s.h.verifyCutIdentity(tr, cut{c}); err != nil {
			return err
		}
	}
	return nil
}
func (s *graphColumnStore) progress(tr fdb.ReadTransaction, g graphColumnGeneration, quiescent bool) (graphColumnProgress, error) {
	if err := s.check(tr, g); err != nil {
		return graphColumnProgress{}, err
	}
	state, found, err := get[graphColumnProgress](tr, s.generationKey(g.ID, "progress"))
	if err != nil {
		return state, err
	}
	p := s.binding.Policy
	if !found || !graphColumnSame(state.Generation, g) || state.Next > p.MaxChunks || state.Rows > p.MaxWorkRows || state.Bytes > p.MaxRetainedBytes || state.WorkBytes > p.MaxWorkBytes {
		return graphColumnProgress{}, errMismatch
	}
	if state.Rows != state.Sequences[0]+state.Sequences[1] || state.Rows < state.Next || state.Rows > state.Next*uint64(p.ChunkRows) || state.Next == 0 && (state.Bytes != 0 || state.WorkBytes != 0 || state.Chain != [32]byte{} || state.Previous != (outcome{})) || state.Next != 0 && (state.Chain == [32]byte{} || !state.Previous.Commit || state.Previous.Coordinator > 1 || state.Previous.Sequence != state.Sequences[state.Previous.Coordinator] || state.Previous.Round > g.Cut.Round) {
		return graphColumnProgress{}, errInvalid
	}
	for group := uint8(0); group < 2; group++ {
		if state.Sequences[group] > g.Cut.Groups[group] || state.History[group] > graphColumnPublicationHistory(state)[group] {
			return graphColumnProgress{}, errInvalid
		}
		if quiescent {
			actual, err := s.h.meta(tr, group)
			if err != nil {
				return graphColumnProgress{}, err
			}
			if actual != state.Metadata[group] || actual.Groups != g.Cut.Groups[group] || actual.Floor != g.Cut.Round {
				return graphColumnProgress{}, errMismatch
			}
		}
	}
	return state, nil
}

func (s *graphColumnStore) begin(g graphColumnGeneration) error {
	_, err := s.write(func(tr fdb.Transaction) (any, error) {
		if err := s.check(tr, g); err != nil {
			return nil, err
		}
		old, found, err := get[graphColumnProgress](tr, s.generationKey(g.ID, "progress"))
		if err != nil {
			return nil, err
		}
		if found {
			if !graphColumnSame(old.Generation, g) {
				return nil, errMismatch
			}
			return nil, nil
		}
		l, err := s.ledger(tr)
		if err != nil {
			return nil, err
		}
		// Reserve worst-case mutable progress and eventual small manifest bytes.
		reserved := uint64(8192 + len(s.generationKey(g.ID, "progress")) + len(s.generationKey(g.ID, "manifest")))
		if l.Generations == s.binding.Policy.MaxGenerations || reserved > s.binding.Policy.MaxRetainedBytes-l.Bytes {
			return nil, errLimit
		}
		state := graphColumnProgress{Generation: g}
		for group := uint8(0); group < 2; group++ {
			m, err := s.h.meta(tr, group)
			if err != nil {
				return nil, err
			}
			if m.Groups != g.Cut.Groups[group] || m.Floor != g.Cut.Round {
				return nil, errMismatch
			}
			state.Metadata[group] = m
		}
		census, err := s.h.historyPublicationIn(tr, state.Metadata)
		if err != nil {
			return nil, err
		}
		if census != [2]uint64{state.Metadata[0].Versions, state.Metadata[1].Versions} {
			state.PublicationHistory = census
		}
		l.Bytes += reserved
		l.Generations++
		if err := put(tr, s.key("ledger"), l); err != nil {
			return nil, err
		}
		return nil, put(tr, s.generationKey(g.ID, "progress"), state)
	})
	return graphColumnCommitError(err)
}

// Every logical column row must bind to the authentic retained original and
// BOTH request/ID records, plus every original physical history write (and its
// predecessor). Canonical original bytes are not a substitute for those reads.
func (s *graphColumnStore) original(tr fdb.ReadTransaction, g graphColumnGeneration, row graphColumnRow) (changeGroup, uint64, error) {
	if len(row.Original) > s.binding.Policy.ChunkBytes-graphColumnHeader-2-8*graphColumnNumeric-8 {
		return changeGroup{}, 0, errLimit
	}
	group, err := decode[changeGroup](row.Original)
	if err != nil {
		return group, 0, err
	}
	if err := s.h.validateChange(group); err != nil {
		return group, 0, err
	}
	o := group.Outcome
	if o.Round <= g.Initial.Round || o.Round > g.Cut.Round || o.Sequence > g.Cut.Groups[o.Coordinator] {
		return group, 0, errMismatch
	}
	bytesRead := uint64(0)
	missing := false
	check := func(key fdb.Key, expected []byte) error {
		missing = false
		actual, err := tr.Get(key).Get()
		if err != nil {
			return err
		}
		bytesRead += uint64(len(key) + len(actual))
		if !bytes.Equal(actual, expected) {
			missing = actual == nil
			return errUnavailable
		}
		return nil
	}
	if err := check(s.h.changeKey(o.Coordinator, o.Sequence, o.Request), row.Original); err != nil {
		return group, 0, err
	}
	body, err := wire(o)
	if err != nil {
		return group, 0, err
	}
	for _, key := range []fdb.Key{s.h.key(o.Coordinator, "id", o.ID), s.h.key(o.Coordinator, "request", o.Request)} {
		if err := check(key, body); err != nil {
			return group, 0, err
		}
	}
	for _, write := range group.Writes {
		body, err := wire(write.After)
		if err != nil {
			return group, 0, err
		}
		key := func(v value) fdb.Key {
			return fdb.Key(fmt.Sprintf("%shistory/%016x/%x", s.h.prefix(write.Group), v.Round, write.Key))
		}
		if err := check(key(write.After), body); err != nil {
			if !errors.Is(err, errUnavailable) || !missing {
				return group, 0, err
			}
			extra, err := s.h.historyLineageIn(tr, write.Group, write.Key, write.After)
			if err != nil {
				return group, 0, err
			}
			bytesRead += extra
		}
		if write.Before != nil {
			if write.Before.Round >= write.After.Round {
				return group, 0, errInvalid
			}
			body, err := wire(*write.Before)
			if err != nil {
				return group, 0, err
			}
			if err := check(key(*write.Before), body); err != nil {
				if !errors.Is(err, errUnavailable) || !missing {
					return group, 0, err
				}
				extra, err := s.h.historyLineageIn(tr, write.Group, write.Key, *write.Before)
				if err != nil {
					return group, 0, err
				}
				bytesRead += extra
			}
		}
	}
	return group, bytesRead, nil
}

func (s *graphColumnStore) upload(g graphColumnGeneration, ordinal uint64, previous [32]byte, body []byte) error {
	p := s.binding.Policy
	rows, err := decodeGraphColumnChunk(s.h, g, ordinal, previous, body, p)
	if err != nil {
		return err
	}
	// Clone before retry; caller buffers cannot change native commit identity.
	body = bytes.Clone(body)
	_, err = s.write(func(tr fdb.Transaction) (any, error) {
		state, err := s.progress(tr, g, true)
		if err != nil {
			return nil, err
		}
		key := s.chunkKey(g.ID, ordinal)
		prior, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		if prior != nil {
			if !bytes.Equal(prior, body) || ordinal >= state.Next {
				return nil, errMismatch
			}
			for _, row := range rows {
				if _, _, err := s.original(tr, g, row); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}
		if state.EOF || ordinal != state.Next || previous != state.Chain {
			return nil, errMismatch
		}
		l, err := s.ledger(tr)
		if err != nil {
			return nil, err
		}
		retained := uint64(len(key) + len(body))
		if l.Chunks == p.MaxChunks || retained > p.MaxRetainedBytes-l.Bytes || uint64(len(rows)) > p.MaxWorkRows-state.Rows {
			return nil, errLimit
		}
		work := uint64(len(body))
		for _, row := range rows {
			group, used, err := s.original(tr, g, row)
			if err != nil {
				return nil, err
			}
			o := group.Outcome
			if o.Sequence != state.Sequences[o.Coordinator]+1 || state.Previous.Round != 0 && graphCostChangeOrder(state.Previous, o) >= 0 {
				return nil, errMismatch
			}
			state.Sequences[o.Coordinator]++
			state.Previous = o
			for _, write := range group.Writes {
				state.History[write.Group]++
			}
			work += used
		}
		if work > p.MaxWorkBytes-state.WorkBytes {
			return nil, errLimit
		}
		state.Next++
		state.Rows += uint64(len(rows))
		state.Bytes += retained
		state.WorkBytes += work
		state.Chain = graphColumnChain(previous, body)
		stateBody, err := wire(state)
		if err != nil || len(stateBody) > 4096 {
			return nil, errors.Join(errLimit, err)
		}
		l.Bytes += retained
		l.Chunks++
		tr.Set(key, body)
		tr.Set(s.generationKey(g.ID, "progress"), stateBody)
		return nil, put(tr, s.key("ledger"), l)
	})
	return graphColumnCommitError(err)
}

func graphColumnComplete(p graphColumnProgress) bool {
	g := p.Generation
	if p.Sequences != g.Cut.Groups || p.Rows != g.Cut.Groups[0]+g.Cut.Groups[1] {
		return false
	}
	for group := range 2 {
		if p.Metadata[group].Groups != g.Cut.Groups[group] || p.Metadata[group].Floor != g.Cut.Round || p.History[group] != graphColumnPublicationHistory(p)[group] {
			return false
		}
	}
	return true
}
func graphColumnPublicationHistory(p graphColumnProgress) [2]uint64 {
	if p.PublicationHistory != [2]uint64{} {
		return p.PublicationHistory
	}
	return [2]uint64{p.Metadata[0].Versions, p.Metadata[1].Versions}
}
func graphColumnManifestFor(p graphColumnProgress) graphColumnManifest {
	return graphColumnManifest{Generation: p.Generation, Metadata: p.Metadata, Chunks: p.Next, Rows: p.Rows, Bytes: p.Bytes, History: p.History, Chain: p.Chain}
}

// The ordinary publication and genuine Commit-issued fault child share this
// exact native reducer. No external IO or observer runs in the transaction.
func (s *graphColumnStore) sealIn(tr fdb.Transaction, g graphColumnGeneration) (graphColumnManifest, error) {
	state, err := s.progress(tr, g, true)
	if err != nil {
		return graphColumnManifest{}, err
	}
	if !graphColumnComplete(state) {
		return graphColumnManifest{}, errUnavailable
	}
	manifest := graphColumnManifestFor(state)
	body, err := wire(manifest)
	if err != nil || len(body) > 4096 {
		return graphColumnManifest{}, errors.Join(errLimit, err)
	}
	old, err := tr.Get(s.generationKey(g.ID, "manifest")).Get()
	if err != nil {
		return graphColumnManifest{}, err
	}
	if old != nil && !bytes.Equal(old, body) {
		return graphColumnManifest{}, errMismatch
	}
	if err := s.ensureGuard(tr, manifest, old == nil); err != nil {
		return graphColumnManifest{}, err
	}
	state.EOF = true
	if err := put(tr, s.generationKey(g.ID, "progress"), state); err != nil {
		return graphColumnManifest{}, err
	}
	tr.Set(s.generationKey(g.ID, "manifest"), body)
	return manifest, nil
}
func (s *graphColumnStore) seal(g graphColumnGeneration) (graphColumnManifest, error) {
	result, err := s.write(func(tr fdb.Transaction) (any, error) { return s.sealIn(tr, g) })
	if err != nil {
		return graphColumnManifest{}, graphColumnCommitError(err)
	}
	return result.(graphColumnManifest), nil
}

func (s *graphColumnStore) manifestIn(tr fdb.ReadTransaction, id string) (graphColumnManifest, error) {
	if s == nil || !validName(id) {
		return graphColumnManifest{}, errInvalid
	}
	m, found, err := get[graphColumnManifest](tr, s.generationKey(id, "manifest"))
	if err != nil {
		return graphColumnManifest{}, err
	}
	if !found || m.Generation.ID != id {
		return graphColumnManifest{}, errUnavailable
	}
	state, err := s.progress(tr, m.Generation, false)
	if err != nil {
		return graphColumnManifest{}, err
	}
	if !state.EOF || !graphColumnComplete(state) || !graphColumnSame(m, graphColumnManifestFor(state)) {
		return graphColumnManifest{}, errUnavailable
	}
	return m, nil
}
func (s *graphColumnStore) manifest(id string) (graphColumnManifest, error) {
	if s == nil || !validName(id) {
		return graphColumnManifest{}, errInvalid
	}
	result, err := s.read(func(tr fdb.ReadTransaction) (any, error) { return s.manifestIn(tr, id) })
	if err != nil {
		return graphColumnManifest{}, err
	}
	return result.(graphColumnManifest), nil
}

// Source callback IO/SDK uploads occur only AFTER ReadTransact returns. No
// dataset map, full snapshot, caller-supplied completeness count or hash seal.
type graphColumnPublicationEvent struct {
	Stage       string
	Generation  graphColumnGeneration
	Ordinal     uint64
	ChunkSHA256 [32]byte
}
type graphColumnPublicationObserver func(graphColumnPublicationEvent) error

func (s *graphColumnStore) publish(ctx context.Context, id string, initial, final cut) (graphColumnManifest, error) {
	return s.publishObserved(ctx, id, initial, final, nil)
}

// Nil is the ordinary path. Observers own no native transaction and cannot
// supply completeness; errors withhold the manifest/EOF result. Their IO is
// after each upload/ReadTransact returns for chunk events; only the source-eof
// event follows verified source EOF. Neither event runs in an SDK retry.
func (s *graphColumnStore) publishObserved(ctx context.Context, id string, initial, final cut, observe graphColumnPublicationObserver) (graphColumnManifest, error) {
	if s == nil || ctx == nil || !validName(id) {
		return graphColumnManifest{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return graphColumnManifest{}, err
	}
	s, cancel := s.forContext(ctx)
	defer cancel()
	g := graphColumnGeneration{Version: 1, ID: id, Binding: s.digest, Initial: initial.record, Cut: final.record}
	body, err := wire(g)
	if err != nil {
		return graphColumnManifest{}, err
	}
	g, err = decode[graphColumnGeneration](body) // Own mutable scope arrays.
	if err != nil {
		return graphColumnManifest{}, err
	}
	if m, err := s.manifest(id); err == nil {
		if !graphColumnSame(m.Generation, g) {
			return graphColumnManifest{}, errMismatch
		}
		return s.walk(ctx, id, final, nil, func(graphColumnRow) error { return nil })
	} else if !errors.Is(err, errUnavailable) {
		return graphColumnManifest{}, err
	}
	if err := s.begin(g); err != nil {
		return graphColumnManifest{}, err
	}
	p := s.binding.Policy
	var rows []graphColumnRow
	var ordinal uint64
	var chain [32]byte
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		body, err := encodeGraphColumnChunk(s.h, g, ordinal, chain, rows, p)
		if err != nil {
			return err
		}
		if err := s.upload(g, ordinal, chain, body); err != nil {
			return err
		}
		if observe != nil {
			if err := observe(graphColumnPublicationEvent{"chunk-upload-returned", g, ordinal, sha256.Sum256(body)}); err != nil {
				return err
			}
		}
		chain = graphColumnChain(chain, body)
		ordinal++
		rows = nil
		return nil
	}
	_, err = s.h.graphCostWalkChangesContext(s.ctx, initial, final, graphCostStreamPolicy{PageRows: 1, PageBytes: 64 << 10}, func(raw graphCostRawRow) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(raw.Value) > p.ChunkBytes-graphColumnHeader-2-8*graphColumnNumeric-8 {
			return errLimit
		}
		row, err := s.h.graphColumnRow(raw.Value)
		if err != nil {
			return err
		}
		candidate := append(slices.Clone(rows), row)
		size := graphColumnHeader + len(candidate)*(2+8*graphColumnNumeric+8)
		for _, value := range candidate {
			size += len(value.Original)
		}
		if len(candidate) > p.ChunkRows || size > p.ChunkBytes {
			if err := flush(); err != nil {
				return err
			}
			candidate = []graphColumnRow{row}
			if graphColumnHeader+2+8*graphColumnNumeric+8+len(row.Original) > p.ChunkBytes {
				return errLimit
			}
		}
		rows = candidate
		return nil
	})
	if err != nil {
		return graphColumnManifest{}, err
	}
	if err := ctx.Err(); err != nil {
		return graphColumnManifest{}, err
	}
	if err := flush(); err != nil {
		return graphColumnManifest{}, err
	}
	if observe != nil {
		if err := observe(graphColumnPublicationEvent{Stage: "source-eof", Generation: g, Ordinal: ordinal}); err != nil {
			return graphColumnManifest{}, err
		}
	}
	// Actual source EOF already verified, and native durable progress separately
	// requires every original sequence AND physical history census at this cut.
	return s.seal(g)
}

func graphColumnCacheName(g graphColumnGeneration, ordinal uint64) string {
	hash, _ := graphColumnGenerationHash(g)
	return fmt.Sprintf("column-%x-%016x.bin", hash, ordinal)
}

// cache, when supplied, is a caller-owned os.Root with the ENTIRE generation.
// There is no cache-miss fallback or cache-only authority. Missing/foreign files,
// wrong lengths/ordinals, or a partial generation return a zero EOF result.
// Callbacks can receive a prefix before an error; only successful EOF is complete.
// Even callers without an external pin hold a durable physical read owner.
// This closes the race with later retention initialization on another client.
func (s *graphColumnStore) walk(ctx context.Context, id string, at cut, cache *os.Root, emit func(graphColumnRow) error) (end graphColumnManifest, err error) {
	if s == nil || ctx == nil || emit == nil {
		return end, errInvalid
	}
	owner, err := newGraphColumnRead(s, id, nil)
	if err != nil {
		return end, err
	}
	if err = owner.open(ctx); err != nil {
		if errors.Is(err, errUnknown) {
			err = errors.Join(err, columnReadCleanup(owner))
		}
		return end, err
	}
	defer func() {
		err = errors.Join(err, columnReadCleanup(owner))
		if err != nil {
			end = graphColumnManifest{}
		}
	}()
	return owner.walk(ctx, at, cache, emit)
}
func (s *graphColumnStore) walkOwned(ctx context.Context, id string, at cut, cache *os.Root, emit func(graphColumnRow) error, guard func(fdb.ReadTransaction) error) (graphColumnManifest, error) {
	if s == nil || ctx == nil || emit == nil || guard == nil {
		return graphColumnManifest{}, errInvalid
	}
	s, cancel := s.forContext(ctx)
	defer cancel()
	m, err := s.manifest(id)
	if err != nil {
		return graphColumnManifest{}, err
	}
	if !slices.Equal(at.record.Scope, []uint8{0, 1}) || at.record.Round < m.Generation.Initial.Round || at.record.Round > m.Generation.Cut.Round {
		return graphColumnManifest{}, errInvalid
	}
	for group := range 2 {
		if at.record.Groups[group] > m.Generation.Cut.Groups[group] {
			return graphColumnManifest{}, errInvalid
		}
	}
	var chain [32]byte
	var count, retained, work uint64
	var sequences, history [2]uint64
	var previous outcome
	for ordinal := uint64(0); ordinal < m.Chunks; ordinal++ {
		if err := ctx.Err(); err != nil {
			return graphColumnManifest{}, err
		}
		var cached []byte
		if cache != nil {
			// Read through a bounded handle; ReadFile alone would admit allocation
			// by attacker-controlled cache file length before the chunk guard.
			file, err := cache.Open(graphColumnCacheName(m.Generation, ordinal))
			if err != nil {
				return graphColumnManifest{}, err
			}
			cached, err = readGraphColumnCache(file, s.binding.Policy.ChunkBytes)
			if err = errors.Join(err, file.Close()); err != nil {
				return graphColumnManifest{}, err
			}
		}
		result, err := s.read(func(tr fdb.ReadTransaction) (any, error) {
			if err := guard(tr); err != nil {
				return nil, err
			}
			if err := s.check(tr, m.Generation); err != nil {
				return nil, err
			}
			if err := s.h.verifyCut(tr, at); err != nil {
				return nil, err
			}
			body, err := tr.Get(s.chunkKey(id, ordinal)).Get()
			if err != nil {
				return nil, err
			}
			if body == nil {
				return nil, errUnavailable
			}
			if cache != nil && !bytes.Equal(body, cached) {
				return nil, errMismatch
			}
			rows, err := decodeGraphColumnChunk(s.h, m.Generation, ordinal, chain, body, s.binding.Policy)
			if err != nil {
				return nil, err
			}
			used := uint64(len(body))
			for _, row := range rows {
				_, n, err := s.original(tr, m.Generation, row)
				if err != nil {
					return nil, err
				}
				used += n
			}
			return struct {
				Rows []graphColumnRow
				Body []byte
				Work uint64
			}{rows, bytes.Clone(body), used}, nil
		})
		if err != nil {
			return graphColumnManifest{}, err
		}
		page := result.(struct {
			Rows []graphColumnRow
			Body []byte
			Work uint64
		})
		p := s.binding.Policy
		if uint64(len(page.Rows)) > p.MaxWorkRows-count || page.Work > p.MaxWorkBytes-work {
			return graphColumnManifest{}, errLimit
		}
		count += uint64(len(page.Rows))
		work += page.Work
		retained += uint64(len(s.chunkKey(id, ordinal)) + len(page.Body))
		chain = graphColumnChain(chain, page.Body)
		for _, row := range page.Rows {
			group, err := decode[changeGroup](row.Original)
			if err != nil {
				return graphColumnManifest{}, err
			}
			o := group.Outcome
			if o.Sequence != sequences[o.Coordinator]+1 || previous.Round != 0 && graphCostChangeOrder(previous, o) >= 0 {
				return graphColumnManifest{}, errMismatch
			}
			sequences[o.Coordinator]++
			previous = o
			for _, write := range group.Writes {
				history[write.Group]++
			}
			inRound, inSequence := row.Round <= at.record.Round, row.Sequence <= at.record.Groups[row.Coordinator]
			if inRound != inSequence {
				return graphColumnManifest{}, errMismatch
			}
			if inRound {
				if err := emit(row); err != nil {
					return graphColumnManifest{}, err
				}
			}
		}
	}
	if chain != m.Chain || count != m.Rows || retained != m.Bytes || sequences != m.Generation.Cut.Groups || history != m.History {
		return graphColumnManifest{}, errUnavailable
	}
	// Empty generations still authenticate the requested cut and cancellation.
	if err := ctx.Err(); err != nil {
		return graphColumnManifest{}, err
	}
	_, err = s.read(func(tr fdb.ReadTransaction) (any, error) {
		if err := guard(tr); err != nil {
			return nil, err
		}
		if err := s.check(tr, m.Generation); err != nil {
			return nil, err
		}
		return nil, s.h.verifyCut(tr, at)
	})
	if err != nil {
		return graphColumnManifest{}, err
	}
	return m, nil
}

// point scans one retained generation under its cumulative work limits and
// holds at most one selected identity. It is not an indexed/O(1) point claim.
func (s *graphColumnStore) point(ctx context.Context, id string, at cut, owner uint64) (graphColumnRow, bool, error) {
	if owner == 0 {
		return graphColumnRow{}, false, errInvalid
	}
	var selected graphColumnRow
	_, err := s.walk(ctx, id, at, nil, func(row graphColumnRow) error {
		if row.Owner == owner && (selected.Round == 0 || selected.Round < row.Round) {
			selected = row
		}
		return nil
	})
	if err != nil {
		return graphColumnRow{}, false, err
	}
	return selected, selected.Round != 0, nil
}

func readGraphColumnCache(file *os.File, maximum int) ([]byte, error) {
	if file == nil || maximum < 1 || maximum > 64<<10 {
		return nil, errInvalid
	}
	body, err := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maximum {
		return nil, errLimit
	}
	return body, nil
}

// A failed native mutation future is not an authoritative ABORT. Retry the
// SAME layout/generation/chunk bytes; immutable progress resolves its outcome.
func graphColumnCommitError(err error) error {
	if _, native := errors.AsType[fdb.Error](err); native {
		return errors.Join(errUnknown, err)
	}
	return err
}
