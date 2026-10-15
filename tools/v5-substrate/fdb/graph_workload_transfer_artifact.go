package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphTransferWorkloadFormat = "fdb-owner-transfer-workload-v1"

type graphTransferPhase struct {
	Phase                                                            string
	Operations, BusinessCrossTransactions, BusinessLocalTransactions uint64
	WallNanos                                                        int64
}
type graphTransferNativeCost struct {
	Rows, Bytes, FrozenRows, FrozenBytes uint64
	Scope                                string
}
type graphTransferAttempt struct {
	Operation                graphCostOperation
	ClientID, AttemptOrdinal int
	PreviousRequest          string
	Intent                   graphOwnerIntent
	Prepare                  graphTransferReply
	Submissions              []graphTransferReply
	Proof                    graphCostParallelProof
	Classification           string
	Nanos                    int64
}
type graphTransferEnd struct {
	Version                                      uint8
	Format                                       string
	Spec                                         graphFeedWorkloadSpec
	Config                                       config
	Binding                                      graphOwnerBinding
	Transport                                    graphTransportRecord
	Initial, Old, Final                          certificate
	Move                                         graphOwnerMove
	Handoff                                      graphOwnerCensusObservation
	Committed, Aborted, Attempts, Issued, Burned uint64
	Clients                                      [32]uint64
	Phases                                       [2]graphTransferPhase
	Sources                                      [3]graphCostStreamEnd
	AfterGC                                      graphCostStreamEnd
	FeedOriginal                                 graphFeedOriginal
	FeedToken                                    graphFeedToken
	FeedPages, FeedRows, ReplayedPages           uint64
	Retired                                      graphHistorySeal
	Columns                                      *graphCostColumnEnd `json:",omitempty"`
	Processes                                    []graphTransferProcessExit
	EventSHA256                                  string
	Storage                                      graphTransferNativeCost
}
type graphTransferArtifacts struct {
	ctx    context.Context
	h      *harness
	root   *os.Root
	file   *os.File
	hash   hash.Hash
	mu     sync.Mutex // One bounded artifact owner; workers never write an SDK callback.
	end    graphTransferEnd
	column *graphFeedWorkloadArtifacts // Genuine shared raw/column reducers only.
	closed bool
}

func newGraphTransferArtifacts(ctx context.Context, h *harness, root *os.Root, spec graphFeedWorkloadSpec, catalog *graphOwnerCatalog, transport graphTransportRecord, initial cut) (*graphTransferArtifacts, error) {
	if ctx == nil || h == nil || root == nil || catalog == nil || catalog.binding.Transfer == nil || catalog.binding.Transfer.Version != 2 || transport.Policy.Spec != spec || h.config != catalog.binding.Config {
		return nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, _, err := openGraphCostArtifact(root, "fdb-transfer-events.jsonl")
	if err != nil {
		return nil, err
	}
	a := &graphTransferArtifacts{ctx: ctx, h: h, root: root, file: file, hash: sha256.New(), end: graphTransferEnd{Version: 1, Format: graphTransferWorkloadFormat, Spec: spec, Config: h.config, Binding: catalog.binding, Transport: transport, Initial: initial.record}}
	a.column = &graphFeedWorkloadArtifacts{ctx: ctx, h: h, root: root, end: graphFeedWorkloadEnd{Format: graphTransferWorkloadFormat, Spec: spec, Config: h.config, Initial: initial.record}}
	if err = a.event(struct {
		Type      string
		Binding   graphOwnerBinding
		Transport graphTransportRecord
		Initial   certificate
	}{"header", catalog.binding, transport, initial.record}); err != nil {
		return nil, errors.Join(err, a.Close())
	}
	return a, nil
}
func (a *graphTransferArtifacts) event(value any) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return os.ErrClosed
	}
	return selectorsJSON(io.MultiWriter(a.file, a.hash), value)
}
func (a *graphTransferArtifacts) handoffTiming(name string, nanos int64) error {
	if nanos < 1 || name != "reserve" && name != "freeze" && name != "activate" {
		return errInvalid
	}
	return a.event(struct {
		Type, Name, Access string
		Nanos              int64
	}{"handoff-api", name, "complete native catalog API retry/wait; full diagnostic/certification census reported separately; no physical replica copy", nanos})
}
func (a *graphTransferArtifacts) attempt(value graphTransferAttempt) error {
	if len(value.Submissions) == 0 || len(value.Submissions) > 8 || value.AttemptOrdinal < 1 || value.AttemptOrdinal > 64 || value.ClientID < 0 || value.ClientID >= a.end.Spec.Writers {
		return errInvalid
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.end.Attempts >= a.h.config.Limits.Transactions {
		return errLimit
	}
	if err := selectorsJSON(io.MultiWriter(a.file, a.hash), struct {
		Type    string
		Attempt graphTransferAttempt
	}{"attempt", value}); err != nil {
		return err
	}
	a.end.Attempts++
	if value.Classification == "committed" {
		if !value.Proof.Outcome.Commit {
			return errMismatch
		}
		a.end.Committed++
		if value.Operation.Phase != "setup" {
			a.end.Clients[value.ClientID]++
		}
	} else if value.Classification == "aborted" {
		if value.Proof.Outcome.Commit {
			return errMismatch
		}
		a.end.Aborted++
	} else {
		return errUnknown
	}
	return nil
}
func (a *graphTransferArtifacts) Close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	var err error
	if a.column != nil {
		err = a.column.Close()
	}
	if a.file != nil {
		err = errors.Join(err, a.file.Close())
	}
	return err
}

// Reuse the complete native two-door scanner. The borrowed event sink is held
// only at drained old/final barriers and is never owned/closed by the adapter.
func (a *graphTransferArtifacts) inventory(stage string, at cut) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.column.events, a.column.eventHash = a.file, a.hash
	a.column.end.Attempts = a.end.Attempts
	defer func() { a.column.events, a.column.eventHash = nil, nil }()
	return a.column.inventory(stage, at)
}

func (a *graphTransferArtifacts) observe(name string, snapshot *graphSnapshot, want int64) error {
	if snapshot == nil || !validName(name) {
		return errInvalid
	}
	begin := time.Now()
	end, err := snapshot.Scan(a.ctx, func(row graphSnapshotRow) error {
		value := want
		if len(row.Key) >= 5 && row.Key[:5] == "node/" {
			value = 1
		}
		if row.Value.Deleted || row.Value.Value != value {
			return errMismatch
		}
		return a.event(struct {
			Type, Name string
			Row        graphSnapshotRow
		}{"snapshot-row", name, row})
	})
	if err != nil {
		return err
	}
	if end.Rows[0]+end.Rows[1] != 3+3*a.end.Spec.Facts {
		return errMismatch
	}
	if err = a.event(struct {
		Type, Name, Access string
		Selection          graphSnapshotSelection
		End                graphSnapshotEnd
		Work               graphSnapshotWork
		Nanos              int64
	}{"snapshot-eof", name, "complete retained semantic Scan through EOF and callback IO, not indexed latency", snapshot.selection, end, snapshot.Work(), time.Since(begin).Nanoseconds()}); err != nil {
		return err
	}
	begin = time.Now()
	points, err := snapshot.Points(a.ctx, []graphSnapshotKey{{Group: 0, Key: edgeKey("edge", a.h.config.Cost.edge(0, want))}, {Group: 0, Key: edgeKey("edge", a.h.config.Cost.edge(a.end.Spec.Facts-1, want))}, {Group: 0, Key: edgeKey("edge", a.h.config.Cost.edge(a.end.Spec.Facts+16, want))}})
	if err != nil {
		return err
	}
	if len(points) != 3 || !points[0].Found || !points[1].Found || points[2].Found || points[0].Value.Value != want || points[1].Value.Value != want {
		return errMismatch
	}
	return a.event(struct {
		Type, Name, Access string
		Points             []graphSnapshotPoint
		Work               graphSnapshotWork
		Nanos              int64
	}{"points", name, "one complete scan-based Points call including retries/wait; not per-key index latency", points, snapshot.Work(), time.Since(begin).Nanoseconds()})
}

func (a *graphTransferArtifacts) census(c *graphOwnerCatalog, authority graphOwnerAuthority, move graphOwnerMove, request string) error {
	begin := time.Now()
	original, err := c.PrepareCensus(a.ctx, authority, move, request)
	if err != nil {
		return err
	}
	if err = a.event(struct {
		Type     string
		Original graphOwnerCensusOriginal
	}{"census-original", original}); err != nil {
		return err
	}
	for ordinal := uint64(1); ordinal <= c.binding.Transfer.Census.MaxCalls; ordinal++ {
		page, err := c.CensusPage(a.ctx, authority, move, request, ordinal)
		if err != nil {
			return err
		}
		for _, record := range page.Rows {
			if err = a.event(struct {
				Type, Request string
				Ordinal       uint64
				Record        graphCostStoredRecord
			}{"census-row", request, ordinal, record}); err != nil {
				return err
			}
		}
		if err = a.event(struct {
			Type, Request string
			Page          graphOwnerCensusPage
			Replay        bool
		}{"census-page", request, page.Page, page.Replay}); err != nil {
			return err
		}
		if page.Page.EOF {
			return a.event(struct {
				Type, Request, Access string
				Nanos                 int64
			}{"census-eof", request, "chosen complete native diagnostic/certification census including retry/wait and streamed artifact IO; not physical data-copy or an inherent FDB owner-transfer requirement", time.Since(begin).Nanoseconds()})
		}
	}
	return errLimit
}

func (a *graphTransferArtifacts) raw(source *graphHistoryRetention, initial, old, final cut, retained bool) (err error) {
	read, err := source.OpenGraphSnapshot(a.ctx, graphSnapshotRequest{Mode: "At", Scope: []uint8{0, 1}, At: final.record}, graphFeedWorkloadPolicy(a.h.config).Read)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, read.Close()) }()
	headerOld := old
	if retained {
		headerOld = final
	}
	header, err := a.h.graphCostArtifactHeaderContext(a.ctx, initial, headerOld, final, graphCostStreamPolicy{32, 4 << 20})
	if err != nil {
		return err
	}
	if retained {
		// This is a PHYSICAL post-GC census, not a successful old At query.
		// Retained old C remains an immutable lineage certificate; authenticate
		// that actual record alongside unchanged metadata and completed seal.
		header, _, err = graphCostParallelRead(a.ctx, a.h.db, func(tr fdb.ReadTransaction) (graphCostArtifactHeader, error) {
			state, e := source.state(tr)
			if e != nil {
				return graphCostArtifactHeader{}, e
			}
			if state.Phase != historySealed || !graphColumnSame(state.Completed, a.end.Retired) {
				return graphCostArtifactHeader{}, errMismatch
			}
			for g := uint8(0); g < 2; g++ {
				m, e := a.h.meta(tr, g)
				if e != nil || m != header.Metadata[g] {
					return graphCostArtifactHeader{}, errors.Join(errMismatch, e)
				}
			}
			if e := a.h.verifyCutIdentity(tr, old); e != nil {
				return graphCostArtifactHeader{}, e
			}
			body, e := wire(old.record)
			if e != nil {
				return graphCostArtifactHeader{}, e
			}
			record, e := snapshotStored(tr, a.h.key(0, "cut", hex.EncodeToString(body)), 64<<10)
			if e != nil {
				return graphCostArtifactHeader{}, e
			}
			if !bytes.Equal(body, record.Value) {
				return graphCostArtifactHeader{}, errMismatch
			}
			header.OldCut, header.CutRecords[1] = old.record, record
			return header, nil
		})
		if err != nil {
			return err
		}
	}
	for i, name := range []string{"history", "current", "cdc"} {
		if retained && name != "history" {
			continue
		}
		header.Stream = name
		end, err := a.column.raw(header, func(emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
			switch name {
			case "history":
				rows, e := read.owner.walk(a.ctx, emit)
				return graphCostStreamEnd{Rows: rows, Groups: final.record.Groups, Metadata: header.Metadata}, e
			case "current":
				return a.h.graphCostWalkRowsContext(a.ctx, final, final, header.Policy, emit, false)
			default:
				return a.h.graphCostWalkChangesContext(a.ctx, initial, final, header.Policy, emit)
			}
		})
		if err != nil {
			return err
		}
		filename := "fdb-transfer-" + name + ".jsonl"
		if retained {
			filename = "fdb-transfer-after-gc-history.jsonl"
			a.end.AfterGC = end
		} else {
			a.end.Sources[i] = end
		}
		if err = a.root.Link("fdb-feed-"+name+".jsonl", filename); err != nil {
			return err
		}
		if err = a.root.Remove("fdb-feed-" + name + ".jsonl"); err != nil {
			return err
		}
	}
	return nil
}

func (a *graphTransferArtifacts) native(ctx context.Context, keys []fdb.Key) ([]graphCostStoredRecord, error) {
	if len(keys) < 1 || len(keys) > 20 {
		return nil, errInvalid
	}
	rows, _, err := graphCostParallelRead(ctx, a.h.db, func(tr fdb.ReadTransaction) ([]graphCostStoredRecord, error) {
		out := make([]graphCostStoredRecord, 0, len(keys))
		for _, key := range keys {
			r, e := snapshotStored(tr, key, 64<<10)
			if e != nil {
				return nil, e
			}
			out = append(out, r)
		}
		return out, nil
	})
	return rows, err
}

func (a *graphTransferArtifacts) complete() error {
	p := a.h.config.Cost
	if a.end.Committed != 3+2*p.Facts || a.end.Aborted < 1 || a.end.Attempts != a.end.Committed+a.end.Aborted || !a.end.Handoff.Seal.Complete || a.end.Retired.Deleted[0]+a.end.Retired.Deleted[1] != 3*p.Facts || a.end.FeedRows != p.Facts || a.end.FeedPages == 0 || a.end.ReplayedPages == 0 {
		return errMismatch
	}
	for client := range p.Writers {
		if a.end.Clients[client] < 2 {
			return errMismatch
		}
	}
	if p.Facts*2 != a.end.Phases[0].Operations+a.end.Phases[1].Operations || a.end.Phases[0].WallNanos < 1 || a.end.Phases[1].WallNanos < 1 {
		return errMismatch
	}
	want := [3]uint64{3 + 6*p.Facts, 3 + 3*p.Facts, 3 + 2*p.Facts}
	for i, source := range a.end.Sources {
		if source.Rows[0]+source.Rows[1] != want[i] || source.Groups != a.end.Final.Groups {
			return errMismatch
		}
	}
	if a.end.AfterGC.Rows[0]+a.end.AfterGC.Rows[1] != 3+3*p.Facts || a.end.AfterGC.Groups != a.end.Final.Groups || len(a.end.Processes) != 4 || a.end.Storage.Rows < 1 || a.end.Storage.Bytes < 1 || a.end.Issued != 2*p.Facts+9 || a.end.Burned != 7 || (a.end.Columns == nil) != (a.end.Spec.Layout == "kv") {
		return errMismatch
	}
	for _, process := range a.end.Processes {
		if !process.Reaped || process.PID < 1 || process.Signal == "" && process.Exit != 0 || process.Signal != "" && (process.Signal != "SIGKILL" || process.Exit != -1) {
			return errMismatch
		}
	}
	a.end.EventSHA256 = hex.EncodeToString(a.hash.Sum(nil))
	if err := a.event(struct {
		Type string
		End  graphTransferEnd
	}{"footer", a.end}); err != nil {
		return err
	}
	if err := a.Close(); err != nil {
		return err
	}
	return feedChildWrite(a.root, "fdb-transfer-result.json", a.end)
}
