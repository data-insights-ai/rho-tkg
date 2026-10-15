package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"os"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type graphFeedRetainedEnd struct {
	Version                                               uint8
	Format                                                string
	Spec                                                  graphFeedWorkloadSpec
	Config                                                config
	Transport                                             graphTransportRecord
	Catalog                                               graphOwnerBinding
	Initial, Snapshot, BeforeGC, Checkpoint, Final        certificate
	Original                                              graphFeedOriginal
	Token                                                 graphFeedToken
	Transition                                            graphCostStoredRecord
	Retired                                               graphHistorySeal
	Clients                                               [32]uint64
	Attempts, Aborted, Committed, Issued, Pages, FeedRows uint64
	SetupNanos, BusinessNanos, TotalNanos                 int64
	Business                                              []graphFeedBusinessPhase // Exactly create, first-update, checkpoint-update, final-update.
	Processes                                             [2]int
	KilledReaped, ReplacementReaped                       bool
	EventSHA256                                           string
	Retention, Allocator                                  string
}
type graphFeedRetainedArtifacts struct {
	ctx              context.Context
	h                *harness
	root             *os.Root
	file             *os.File
	hash             hash.Hash
	mu               sync.Mutex
	end              graphFeedRetainedEnd
	started          time.Time
	written, maximum uint64
	closed           bool
}

func newGraphFeedRetainedArtifacts(ctx context.Context, h *harness, root *os.Root, spec graphFeedWorkloadSpec, c *graphOwnerCatalog, transport graphTransportRecord, initial cut) (*graphFeedRetainedArtifacts, error) {
	if ctx == nil || h == nil || root == nil || c == nil || c.binding.Transfer == nil || c.binding.Transfer.Version != 3 || !validGraphFeedRetainedConfig(h.config) || transport.Policy.Spec != spec {
		return nil, errInvalid
	}
	file, _, err := openGraphCostArtifact(root, "fdb-feed-retained-events.jsonl")
	if err != nil {
		return nil, err
	}
	// Encoded immutable intents, repeated raw native inventories, full point /
	// feed / columns evidence and actual retry frames. This is finite streamed
	// artifact admission, not an in-memory dataset or storage capacity verdict.
	a := &graphFeedRetainedArtifacts{ctx: ctx, h: h, root: root, file: file, hash: sha256.New(), started: time.Now(), maximum: (64 << 20) + spec.Facts*(8<<20)}
	a.end = graphFeedRetainedEnd{Version: 1, Format: graphFeedRetainedFormat, Spec: spec, Config: h.config, Transport: transport, Catalog: c.binding, Initial: initial.record,
		Retention: "snapshot C promised until authenticated irreversible transition; detached complete CDC/originals retained after physical source GC; generic At(C) follows actual floor; native counterpart conservatively keeps rolling ACK predecessor",
		Allocator: "genuine HOME reserve, recipient session install and actual local Cursor.Next/Node/Edge; not native installed-grant-reference factory"}
	if err = a.event(struct {
		Type   string
		Header graphFeedRetainedEnd
	}{"header", a.end}); err != nil {
		return nil, errors.Join(err, a.Close())
	}
	return a, nil
}
func (a *graphFeedRetainedArtifacts) event(v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(body) > 256<<10 {
		return errLimit
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return os.ErrClosed
	}
	if uint64(len(body)+1) > a.maximum-a.written {
		return errLimit
	}
	_, err = io.MultiWriter(a.file, a.hash).Write(append(body, '\n'))
	if err == nil {
		a.written += uint64(len(body) + 1)
	}
	return err
}
func (a *graphFeedRetainedArtifacts) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	return errors.Join(a.file.Sync(), a.file.Close())
}
func (a *graphFeedRetainedArtifacts) finish() error {
	a.end.TotalNanos = time.Since(a.started).Nanoseconds()
	a.end.EventSHA256 = hex.EncodeToString(a.hash.Sum(nil))
	if err := a.Close(); err != nil {
		return err
	}
	return feedChildWrite(a.root, "fdb-feed-retained-end.json", a.end)
}
func (a *graphFeedRetainedArtifacts) native(stage string) error {
	var before [2]metadata
	for g := uint8(0); g < 2; g++ {
		m, _, err := graphCostParallelRead(a.ctx, a.h.db, func(tr fdb.ReadTransaction) (metadata, error) { return a.h.meta(tr, g) })
		if err != nil {
			return err
		}
		before[g] = m
		prefix := a.h.prefix(g)
		begin := fdb.Key(prefix)
		end := prefixRange(prefix).End
		var count, used uint64
		for {
			rows, _, err := graphCostParallelRead(a.ctx, a.h.db, func(tr fdb.ReadTransaction) ([]fdb.KeyValue, error) {
				return tr.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Limit: 32}).GetSliceWithError()
			})
			if err != nil {
				return err
			}
			for _, r := range rows {
				if len(r.Key) > 2048 || len(r.Value) > 64<<10 || !bytes.HasPrefix(r.Key, []byte(prefix)) {
					return errLimit
				}
				if err := a.event(struct {
					Type, Stage string
					Group       uint8
					Record      graphCostStoredRecord
				}{"native", stage, g, graphCostStoredRecord{r.Key, r.Value}}); err != nil {
					return err
				}
				count++
				used += uint64(len(r.Key) + len(r.Value))
			}
			if len(rows) < 32 {
				break
			}
			begin = fdb.Key(append(bytes.Clone(rows[len(rows)-1].Key), 0))
		}
		if err := a.event(struct {
			Type, Stage string
			Group       uint8
			Rows, Bytes uint64
			Metadata    metadata
		}{"native-eof", stage, g, count, used, m}); err != nil {
			return err
		}
	}
	for g := uint8(0); g < 2; g++ {
		m, _, err := graphCostParallelRead(a.ctx, a.h.db, func(tr fdb.ReadTransaction) (metadata, error) { return a.h.meta(tr, g) })
		if err != nil {
			return err
		}
		if m != before[g] {
			return errMismatch
		}
	}
	return nil
}
func (a *graphFeedRetainedArtifacts) snapshot(stage string, s *graphSnapshot) error {
	c, err := s.Cut()
	if err != nil {
		return err
	}
	if err = a.event(struct {
		Type, Stage string
		Cut         certificate
	}{"snapshot-begin", stage, c}); err != nil {
		return err
	}
	end, err := s.Scan(a.ctx, func(row graphSnapshotRow) error {
		return a.event(struct {
			Type, Stage string
			Row         graphSnapshotRow
		}{"snapshot-row", stage, row})
	})
	if err != nil {
		return err
	}
	return a.event(struct {
		Type, Stage string
		End         graphSnapshotEnd
	}{"snapshot-eof", stage, end})
}
func (a *graphFeedRetainedArtifacts) columns(s *graphColumnStore, id, stage string, initial, final cut, cache *os.Root) error {
	if cache == nil {
		m, err := s.publish(a.ctx, id, initial, final)
		if err != nil {
			return err
		}
		if err = a.event(struct {
			Type, Stage string
			Binding     graphColumnBinding
			Manifest    graphColumnManifest
		}{"columns-manifest", stage, s.binding, m}); err != nil {
			return err
		}
	}
	m, err := s.walk(a.ctx, id, final, cache, func(row graphColumnRow) error {
		return a.event(struct {
			Type, Stage string
			Row         graphColumnRow
		}{"columns-row", stage, row})
	})
	if err != nil {
		return err
	}
	return a.event(struct {
		Type, Stage string
		Manifest    graphColumnManifest
	}{"columns-eof", stage, m})
}
