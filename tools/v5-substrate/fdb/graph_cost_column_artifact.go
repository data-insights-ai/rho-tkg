package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"os"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphCostColumnFormat = "fdb-graph-cost-columns-v1"
const graphCostColumnStorageRows = 32
const graphCostColumnStorageBytes = graphCostColumnStorageRows * ((64 << 10) + 1024)

type graphCostColumnCounts struct{ Chunks, WalkRows, Points, Stages, Collections uint64 }
type graphCostColumnStorage struct {
	Records, Bytes, LiveChunkRecords, LiveChunkBytes uint64
	TombstoneRecords, TombstoneBytes                 uint64
	RetiredLineageRecords, RetiredLineageBytes       uint64
}
type graphCostColumnEnd struct {
	Format, SourceFormat, ReferenceLayout string
	PayloadSHA256, ArtifactSHA256         [32]byte
	Old, Final                            graphColumnManifest
	Counts                                graphCostColumnCounts
	Ledger                                graphColumnLedger
	PinLedger                             graphColumnPinLedger
	Storage                               graphCostColumnStorage
}

func (e graphCostColumnEnd) complete() bool {
	return (e.Format == graphCostColumnFormat && e.SourceFormat == graphCostRawFormat || e.Format == graphCostRetentionColumnFormat && e.SourceFormat == graphCostRetentionFormat) && e.ReferenceLayout == "native-logical-kv-reference" &&
		e.PayloadSHA256 != [32]byte{} && e.ArtifactSHA256 != [32]byte{} && e.Old.Generation.ID == "old" && e.Final.Generation.ID == "final" &&
		e.Counts.Chunks == e.Old.Chunks+e.Final.Chunks && e.Counts.WalkRows == 2*(e.Old.Rows+e.Final.Rows) && e.Counts.Points == 6 && e.Counts.Stages == 7 &&
		e.Counts.Collections == e.Old.Chunks && e.Old.Chunks > 0 && e.Ledger.Generations == 2 && e.Ledger.Chunks == e.Final.Chunks &&
		e.Storage.Records == e.Final.Chunks+22 && e.Storage.LiveChunkRecords == e.Final.Chunks && e.Storage.LiveChunkBytes == e.Final.Bytes && e.Storage.TombstoneRecords == 9 && e.Storage.RetiredLineageRecords == 3
}

type graphCostColumnLane struct {
	s       *graphColumnStore
	pins    *graphColumnPins
	root    *os.Root
	file    *os.File
	payload hash.Hash
	whole   hash.Hash
	initial cut
	old     cut
	held    *graphColumnRead
	token   graphColumnPinToken
	head    graphColumnReference
	end     graphCostColumnEnd
	closed  bool
	// Nil preserves historical artifact bytes and emits no new timing. The fresh
	// columns workload observes complete calls, never one sample per emitted row.
	timing     func(call, name string, at certificate, rows uint64, nanos int64) error
	pointValue func(id uint64, generation string) int64
}

func (l *graphCostColumnLane) timed(call, name string, at certificate, rows uint64, begin time.Time) error {
	if l.timing == nil {
		return nil
	}
	return l.timing(call, name, at, rows, time.Since(begin).Nanoseconds())
}

func (l *graphCostColumnLane) context() context.Context {
	if l.s.ctx != nil {
		return l.s.ctx
	}
	return context.Background() // Historical lane/default IO stays unchanged.
}

// These are explicit fixture admission policies, not measured capacity/RSS
// bounds. Work applies separately to each publication/read; fixed control and
// configuration checks and native callback retries are outside its accounting.
func graphCostColumnPolicies(p graphCostProfile) (graphColumnPolicy, graphColumnPinPolicy) {
	return graphColumnPolicy{ChunkRows: 32, ChunkBytes: 64 << 10, MaxChunks: 1_000_000, MaxGenerations: 2,
			MaxRetainedBytes: (64 << 20) + (6+3*p.Facts)*(64<<10), MaxWorkRows: 3 + 2*p.Facts,
			MaxWorkBytes: (64 << 20) + (3+2*p.Facts)*(512<<10)},
		graphColumnPinPolicy{MaxRecords: 32, MaxBytes: 256 << 10, PageChunks: 1, PageBytes: 128 << 10,
			MaxWorkChunks: 1, MaxWorkBytes: 128 << 10, MaxOutputBytes: 1024}
}

func newGraphCostColumnLane(h *harness, root *os.Root, initial cut) (*graphCostColumnLane, error) {
	if h == nil || root == nil || h.config.Graph != "graph-cost-columns-v1" && h.config.Graph != "graph-cost-parallel-columns-v1" && h.config.Graph != graphCostRetentionGraph("columns", h.config.Cost.Writers) || !validGraphCostProfile(h.config) {
		return nil, errInvalid
	}
	policy, retention := graphCostColumnPolicies(h.config.Cost)
	s, err := newGraphColumnStore(h, policy)
	if err != nil {
		return nil, err
	}
	pins, err := newGraphColumnPins(s, retention)
	if err != nil {
		return nil, err
	}
	file, _, err := openGraphCostArtifact(root, "fdb-cost-columns.jsonl")
	if err != nil {
		return nil, err
	}
	l := &graphCostColumnLane{s: s, pins: pins, root: root, file: file, payload: sha256.New(), whole: sha256.New(), initial: initial}
	l.end.Format, l.end.SourceFormat, l.end.ReferenceLayout = graphCostColumnFormat, graphCostRawFormat, "native-logical-kv-reference"
	if graphCostRetainedConfig(h.config) {
		l.end.Format, l.end.SourceFormat = graphCostRetentionColumnFormat, graphCostRetentionFormat
	}
	body, err := wire(initial.record)
	if err != nil {
		return nil, errors.Join(err, l.Close())
	}
	records, err := l.records([]fdb.Key{s.key("binding"), pins.key("binding"), h.key(0, "meta", "config"), h.key(1, "meta", "config"), h.key(0, "cut", hex.EncodeToString(body))})
	if err == nil {
		err = l.write(struct {
			Type, Format, SourceFormat, ReferenceLayout string
			Config                                      config
			InitialCut                                  certificate
			StoragePagePolicy                           graphCostStreamPolicy
			Records                                     []graphCostStoredRecord
		}{"header", l.end.Format, l.end.SourceFormat, l.end.ReferenceLayout, h.config, initial.record, graphCostStreamPolicy{graphCostColumnStorageRows, graphCostColumnStorageBytes}, records}, true)
	}
	if err != nil {
		return nil, errors.Join(err, l.Close())
	}
	return l, nil
}

// Raw SDK reads own their bounded bytes. Artifact/cache writes occur only
// AFTER ReadTransact returns; no retry callback can duplicate external output.
func (l *graphCostColumnLane) records(keys []fdb.Key) ([]graphCostStoredRecord, error) {
	if len(keys) < 1 || len(keys) > 20 {
		return nil, errInvalid
	}
	result, err := l.s.read(func(tr fdb.ReadTransaction) (any, error) {
		out := make([]graphCostStoredRecord, 0, len(keys))
		for _, key := range keys {
			body, err := tr.Get(key).Get()
			if err != nil {
				return nil, err
			}
			if len(key) > 1024 || len(body) == 0 || len(body) > 64<<10 {
				return nil, errUnavailable
			}
			out = append(out, graphCostStoredRecord{Key: bytes.Clone(key), Value: bytes.Clone(body)})
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return result.([]graphCostStoredRecord), nil
}

func (l *graphCostColumnLane) write(value any, payload bool) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(body)+1 > 512<<10 || l.closed {
		return errLimit
	}
	body = append(body, '\n')
	if _, err := l.file.Write(body); err != nil {
		return err
	}
	_, _ = l.whole.Write(body)
	if payload {
		_, _ = l.payload.Write(body)
	}
	return nil
}

func (l *graphCostColumnLane) stage(name string, generations ...string) error {
	keys := []fdb.Key{l.s.key("ledger"), l.pins.key("ledger")}
	keys = append(keys, l.s.h.key(0, "meta", "state"), l.s.h.key(1, "meta", "state"))
	for _, generation := range generations {
		keys = append(keys, l.s.generationKey(generation, "manifest"), l.s.generationKey(generation, "progress"), l.s.generationKey(generation, "owners"))
		m := l.end.Old
		if generation == "final" {
			m = l.end.Final
		}
		body, err := wire(m.Generation.Cut)
		if err != nil {
			return err
		}
		keys = append(keys, l.s.h.key(0, "cut", hex.EncodeToString(body)))
	}
	if l.held != nil {
		keys = append(keys, l.held.key(), columnPinKey(l.s, l.token.Request), l.pins.recordKey("reference", "head"))
		keys = append(keys, l.pins.recordKey("reference-request", "head-old"))
		if l.head.Version == 2 {
			keys = append(keys, l.pins.recordKey("reference-request", "head-final"))
		}
	}
	records, err := l.records(keys)
	if err != nil {
		return err
	}
	if err := l.write(struct {
		Type, Name string
		Records    []graphCostStoredRecord
	}{"stage", name, records}, true); err != nil {
		return err
	}
	l.end.Counts.Stages++
	return nil
}

func (l *graphCostColumnLane) holdOld(old cut) error {
	ctx := l.context()
	m, err := l.s.publish(ctx, "old", l.initial, old)
	if err != nil {
		return err
	}
	l.end.Old, l.old = m, old
	l.head, err = l.pins.reference(ctx, "head-old", "head", "old", 0)
	if err != nil {
		return err
	}
	l.token, err = l.pins.pin(ctx, "hold-old", "old", old)
	if err != nil {
		return err
	}
	l.held, err = l.pins.open(ctx, l.token)
	if err != nil {
		return err
	}
	return l.stage("old-held", "old")
}

func (l *graphCostColumnLane) walk(owner *graphColumnRead, at cut, cache *os.Root, name string) error {
	begin := time.Now()
	var count uint64
	m, err := owner.walk(l.context(), at, cache, func(row graphColumnRow) error {
		if err := l.write(struct {
			Type, Name, Generation string
			Row                    graphColumnRow
		}{"walk-row", name, owner.id.Generation, row}, true); err != nil {
			return err
		}
		count++
		return nil
	})
	if err != nil {
		return err
	}
	if count != m.Rows || !sameCertificate(at.record, m.Generation.Cut) {
		return errMismatch
	}
	if err := l.write(struct {
		Type, Name, Generation string
		Cut                    certificate
		Rows                   uint64
		Manifest               graphColumnManifest
	}{"walk-eof", name, owner.id.Generation, at.record, count, m}, true); err != nil {
		return err
	}
	l.end.Counts.WalkRows += count
	return l.timed("Walk", name, at.record, count, begin)
}

// Cache files are actual immutable SDK chunk bytes in an exclusive directory.
// Their filenames/body are independently checked against the hash-bound chunk
// records by the offline verifier; nested cache files need no inventory map.
func (l *graphCostColumnLane) chunks(owner *graphColumnRead, m graphColumnManifest) (cache *os.Root, err error) {
	begin := time.Now()
	name := "column-cache-" + m.Generation.ID
	if err := l.root.Mkdir(name, 0700); err != nil {
		return nil, err
	}
	cache, err = l.root.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, cache.Close())
			cache = nil
		}
	}()
	var chain [32]byte
	var count, retained uint64
	for ordinal := uint64(0); ordinal < m.Chunks; ordinal++ {
		key := l.s.chunkKey(m.Generation.ID, ordinal)
		result, e := l.s.read(func(tr fdb.ReadTransaction) (any, error) {
			if err := owner.check(tr); err != nil {
				return nil, err
			}
			if err := l.s.check(tr, m.Generation); err != nil {
				return nil, err
			}
			body, err := tr.Get(key).Get()
			if err != nil {
				return nil, err
			}
			rows, err := decodeGraphColumnChunk(l.s.h, m.Generation, ordinal, chain, body, l.s.binding.Policy)
			if err != nil {
				return nil, err
			}
			for _, row := range rows {
				if _, _, err := l.s.original(tr, m.Generation, row); err != nil {
					return nil, err
				}
			}
			return struct {
				Body []byte
				Rows uint64
			}{bytes.Clone(body), uint64(len(rows))}, nil
		})
		if e != nil {
			return nil, e
		}
		page := result.(struct {
			Body []byte
			Rows uint64
		})
		file, e := cache.OpenFile(graphColumnCacheName(m.Generation, ordinal), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			return nil, e
		}
		_, e = file.Write(page.Body)
		if e = errors.Join(e, file.Close()); e != nil {
			return nil, e
		}
		if e := l.write(struct {
			Type, Generation, CacheDirectory, CacheFile string
			Ordinal, Length, Rows                       uint64
			SHA256                                      [32]byte
			Record                                      graphCostStoredRecord
		}{"chunk", m.Generation.ID, name, graphColumnCacheName(m.Generation, ordinal), ordinal, uint64(len(page.Body)), page.Rows, sha256.Sum256(page.Body), graphCostStoredRecord{key, page.Body}}, true); e != nil {
			return nil, e
		}
		chain = graphColumnChain(chain, page.Body)
		count += page.Rows
		retained += uint64(len(key) + len(page.Body))
		l.end.Counts.Chunks++
	}
	if chain != m.Chain || count != m.Rows || retained != m.Bytes {
		return nil, errMismatch
	}
	_, err = l.s.read(func(tr fdb.ReadTransaction) (any, error) { return nil, owner.check(tr) })
	if err != nil {
		return nil, err
	}
	err = l.write(struct {
		Type, Generation string
		Manifest         graphColumnManifest
	}{"chunks-eof", m.Generation.ID, m}, true)
	if err == nil {
		err = l.timed("Cache", m.Generation.ID, m.Generation.Cut, count, begin)
	}
	return cache, err
}

func (l *graphCostColumnLane) export(owner *graphColumnRead, m graphColumnManifest, at cut) (err error) {
	cache, err := l.chunks(owner, m)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, cache.Close()) }()
	if err := l.walk(owner, at, nil, m.Generation.ID); err != nil {
		return err
	}
	if err := l.walk(owner, at, cache, m.Generation.ID+"-cache"); err != nil {
		return err
	}
	// The existing point API is a bounded generation scan, NOT an index. Three
	// selected witnesses per cut keep total work linear rather than N point scans.
	for _, id := range [3]uint64{7, 7 + 2*(l.s.h.config.Cost.Facts-1), 5} {
		begin := time.Now()
		row, found, err := l.s.point(l.context(), m.Generation.ID, at, id)
		if err != nil {
			return err
		}
		want := int64(6)
		if m.Generation.ID == "old" {
			want = 5
		}
		if l.pointValue != nil && id != 5 {
			want = l.pointValue(id, m.Generation.ID)
		}
		if found != (id != 5) || found && (row.Owner != id || row.Value != want) {
			return errMismatch
		}
		if err := l.write(struct {
			Type, Generation string
			Owner            uint64
			Found            bool
			Row              graphColumnRow
		}{"point", m.Generation.ID, id, found, row}, true); err != nil {
			return err
		}
		if err := l.timed("ScanPoint", fmt.Sprintf("%s-%d", m.Generation.ID, id), at.record, m.Rows, begin); err != nil {
			return err
		}
		l.end.Counts.Points++
	}
	return nil
}

func (l *graphCostColumnLane) refusal(name string, err, expected error) error {
	if !errors.Is(err, expected) {
		return errors.Join(errMismatch, err)
	}
	return l.write(struct{ Type, Name, Error string }{"refusal", name, expected.Error()}, true)
}

func (l *graphCostColumnLane) finish(final cut) (end graphCostColumnEnd, err error) {
	ctx := l.context()
	if l.held == nil {
		return end, errInvalid
	}
	l.end.Final, err = l.s.publish(ctx, "final", l.initial, final)
	if err != nil {
		return end, err
	}
	l.head, err = l.pins.reference(ctx, "head-final", "head", "final", l.head.Version)
	if err != nil {
		return end, err
	}
	if err := l.stage("final-head", "old", "final"); err != nil {
		return end, err
	}
	if err := l.pins.finish(ctx, "expire-old", l.token, true); err != nil {
		return end, err
	}
	_, refused := l.pins.open(ctx, l.token)
	if err := l.refusal("expired-new-open", refused, errColumnExpired); err != nil {
		return end, err
	}
	budget := graphColumnCollectBudget{Chunks: 1, Bytes: 128 << 10, OutputBytes: 1024}
	_, refused = l.pins.collect(ctx, "old", budget)
	if err := l.refusal("held-old-collection", refused, errColumnInUse); err != nil {
		return end, err
	}
	if err := l.stage("expired-held", "old", "final"); err != nil {
		return end, err
	}
	if err := l.export(l.held, l.end.Old, l.old); err != nil {
		return end, err
	}
	current, err := newGraphColumnRead(l.s, "final", nil)
	if err != nil {
		return end, err
	}
	if err := current.open(ctx); err != nil {
		return end, errors.Join(err, columnReadCleanup(current))
	}
	err = l.export(current, l.end.Final, final)
	if err = errors.Join(err, columnReadCleanup(current)); err != nil {
		return end, err
	}
	if err := l.stage("reads-complete", "old", "final"); err != nil {
		return end, err
	}
	if err := columnReadCleanup(l.held); err != nil {
		return end, err
	}
	if err := l.stage("old-closed", "old", "final"); err != nil {
		return end, err
	}
	for ordinal := uint64(0); ordinal < l.end.Old.Chunks; ordinal++ {
		page, err := l.pins.collect(ctx, "old", budget)
		if err != nil || page.Cursor != ordinal+1 || page.WorkChunks != 1 || page.Done != (ordinal+1 == l.end.Old.Chunks) {
			return end, errors.Join(errMismatch, err)
		}
		key := l.s.chunkKey("old", ordinal)
		result, err := l.s.read(func(tr fdb.ReadTransaction) (any, error) {
			raw, err := tr.Get(key).Get()
			if err != nil {
				return nil, err
			}
			if raw != nil {
				return nil, errMismatch
			}
			guard, found, err := l.s.guard(tr, l.end.Old)
			if err != nil || !found || guard.Cursor != page.Cursor {
				return nil, errors.Join(errMismatch, err)
			}
			body, err := wire(guard)
			return graphCostStoredRecord{Key: l.s.generationKey("old", "owners"), Value: body}, err
		})
		if err != nil {
			return end, err
		}
		if err := l.write(struct {
			Type   string
			Budget graphColumnCollectBudget
			Result graphColumnCollectResult
			Absent []byte
			Guard  graphCostStoredRecord
		}{"collect", budget, page, bytes.Clone(key), result.(graphCostStoredRecord)}, true); err != nil {
			return end, err
		}
		l.end.Counts.Collections++
	}
	_, refused = l.s.publish(ctx, "old", l.initial, l.old)
	if err := l.refusal("retired-publication", refused, errColumnExpired); err != nil {
		return end, err
	}
	if err := l.stage("old-collected", "old", "final"); err != nil {
		return end, err
	}
	// Final immutable generation stays reachable by the named head. Source KV,
	// original/history/CDC/cuts and all control tombstones remain retained.
	_, refused = l.pins.collect(ctx, "final", budget)
	if err := l.refusal("final-reference-collection", refused, errColumnInUse); err != nil {
		return end, err
	}
	if err := l.stage("terminal", "old", "final"); err != nil {
		return end, err
	}
	records, err := l.records([]fdb.Key{l.s.key("ledger"), l.pins.key("ledger")})
	if err != nil {
		return end, err
	}
	l.end.Ledger, err = decode[graphColumnLedger](records[0].Value)
	if err != nil {
		return end, err
	}
	l.end.PinLedger, err = decode[graphColumnPinLedger](records[1].Value)
	if err != nil {
		return end, err
	}
	if err := l.storage(); err != nil {
		return end, err
	}
	copy(l.end.PayloadSHA256[:], l.payload.Sum(nil))
	if err := l.write(struct {
		Type string
		End  graphCostColumnEnd
	}{"footer", l.end}, false); err != nil {
		return end, err
	}
	copy(l.end.ArtifactSHA256[:], l.whole.Sum(nil))
	if err := l.Close(); err != nil {
		return end, err
	}
	if !l.end.complete() {
		return end, fmt.Errorf("column artifact incomplete: %w", errMismatch)
	}
	return l.end, nil
}

// Actual retained logical key/value bytes, not the reservation ledger and not
// physical FDB replica/file allocation. Paging holds no complete namespace map.
func (l *graphCostColumnLane) storage() error {
	keyRange := prefixRange(l.s.prefix())
	begin, end := keyRange.Begin.(fdb.Key), keyRange.End.(fdb.Key)
	check := func(tr fdb.ReadTransaction) error {
		if err := l.s.check(tr, l.end.Final.Generation); err != nil {
			return err
		}
		ledger, err := l.s.ledger(tr)
		if err != nil || ledger != l.end.Ledger {
			return errors.Join(errMismatch, err)
		}
		pins, err := l.pins.ledger(tr)
		if err != nil || pins != l.end.PinLedger {
			return errors.Join(errMismatch, err)
		}
		return nil
	}
	for {
		result, err := l.s.read(func(tr fdb.ReadTransaction) (any, error) {
			if err := check(tr); err != nil {
				return nil, err
			}
			rows, err := tr.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Limit: graphCostColumnStorageRows}).GetSliceWithError()
			if err != nil {
				return nil, err
			}
			if len(rows) > graphCostColumnStorageRows {
				return nil, errLimit
			}
			out := make([]graphCostStoredRecord, 0, len(rows))
			used := 0
			var previous []byte
			for _, row := range rows {
				if len(row.Key) > 1024 || len(row.Value) == 0 || len(row.Value) > 64<<10 || bytes.Compare(row.Key, begin) < 0 || bytes.Compare(row.Key, end) >= 0 || previous != nil && bytes.Compare(previous, row.Key) >= 0 {
					return nil, errLimit
				}
				used += len(row.Key) + len(row.Value)
				if used > graphCostColumnStorageBytes {
					return nil, errLimit
				}
				out = append(out, graphCostStoredRecord{bytes.Clone(row.Key), bytes.Clone(row.Value)})
				previous = row.Key
			}
			return out, nil
		})
		if err != nil {
			return err
		}
		rows := result.([]graphCostStoredRecord)
		for _, row := range rows {
			size := uint64(len(row.Key) + len(row.Value))
			l.end.Storage.Records++
			l.end.Storage.Bytes += size
			if l.end.Storage.Records > l.end.Final.Chunks+22 || l.end.Storage.Bytes > l.s.binding.Policy.MaxRetainedBytes {
				return errLimit
			}
			switch {
			case bytes.HasPrefix(row.Key, l.s.generationKey("final", "chunk/")):
				l.end.Storage.LiveChunkRecords++
				l.end.Storage.LiveChunkBytes += size
			case bytes.HasPrefix(row.Key, l.s.generationKey("old", "")):
				l.end.Storage.RetiredLineageRecords++
				l.end.Storage.RetiredLineageBytes += size
			case bytes.HasPrefix(row.Key, l.s.key("read-owner/")), bytes.Equal(row.Key, columnPinKey(l.s, l.token.Request)):
				l.end.Storage.TombstoneRecords++
				l.end.Storage.TombstoneBytes += size
			}
			if err := l.write(struct {
				Type   string
				Record graphCostStoredRecord
			}{"storage-row", row}, true); err != nil {
				return err
			}
		}
		if len(rows) < graphCostColumnStorageRows {
			break
		}
		begin = fdb.Key(append(bytes.Clone(rows[len(rows)-1].Key), 0))
	}
	_, err := l.s.read(func(tr fdb.ReadTransaction) (any, error) { return nil, check(tr) })
	return err
}

func (l *graphCostColumnLane) Close() error {
	if l == nil || l.closed {
		return nil
	}
	var err error
	if l.held != nil {
		err = columnReadCleanup(l.held)
	}
	closeErr := l.file.Close()
	l.closed = true
	return errors.Join(err, closeErr)
}
