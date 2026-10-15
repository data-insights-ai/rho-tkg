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
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type graphSelectorsEnd struct {
	Version                                                             uint8
	Format                                                              string
	Spec                                                                graphSelectorsSpec
	Config                                                              config
	Binding                                                             graphSelectorsBinding
	BindingRecord                                                       graphCostStoredRecord
	Initial, Old, Final                                                 certificate
	Attempts, Aborted, Committed, Setup, Creates, Updates, Observations uint64
	Clients                                                             [32]uint64
	Sources                                                             [3]graphCostStreamEnd
	Columns                                                             *graphCostColumnEnd `json:",omitempty"`
	AttemptSHA256, EventSHA256                                          string
}
type graphSelectorsArtifacts struct {
	h                           *harness
	root                        *os.Root
	attempts, events, latencies *os.File
	attemptHash, eventHash      hash.Hash
	end                         graphSelectorsEnd
	old, final                  cut
	receipt                     *graphSnapshotReceipt
	column                      *graphCostColumnLane
	closed                      bool
}

func newGraphSelectorsArtifacts(h *harness, root *os.Root, spec graphSelectorsSpec, binding graphCostStoredRecord, initial cut) (*graphSelectorsArtifacts, error) {
	a := &graphSelectorsArtifacts{h: h, root: root, attemptHash: sha256.New(), eventHash: sha256.New(), end: graphSelectorsEnd{Version: 1, Format: graphSelectorsFormat, Spec: spec, Config: h.config, Binding: graphSelectorsPolicy(h.config), BindingRecord: binding, Initial: initial.record}}
	var err error
	a.attempts, _, err = openGraphCostArtifact(root, "fdb-selectors-attempts.jsonl")
	if err != nil {
		return nil, err
	}
	a.events, _, err = openGraphCostArtifact(root, "fdb-selectors-events.jsonl")
	if err != nil {
		return nil, errors.Join(err, a.Close())
	}
	a.latencies, _, err = openGraphCostArtifact(root, "fdb-selectors-latency.jsonl")
	if err != nil {
		return nil, errors.Join(err, a.Close())
	}
	if err := a.event(struct {
		Type    string
		Format  string
		Spec    graphSelectorsSpec
		Config  config
		Binding graphSelectorsBinding
		Record  graphCostStoredRecord
		Initial certificate
	}{"header", graphSelectorsFormat, spec, h.config, a.end.Binding, binding, initial.record}); err != nil {
		return nil, errors.Join(err, a.Close())
	}
	return a, nil
}
func selectorsJSON(writer io.Writer, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(raw)+1 > 512<<10 {
		return errLimit
	}
	_, err = writer.Write(append(raw, '\n'))
	return err
}
func (a *graphSelectorsArtifacts) event(v any) error {
	return selectorsJSON(io.MultiWriter(a.events, a.eventHash), v)
}
func (a *graphSelectorsArtifacts) accept(e graphCostParallelEvent) error {
	if e.Attempt == nil {
		return nil
	}
	attempt := e.Attempt
	if a.end.Attempts >= a.h.config.Limits.Transactions {
		return errLimit
	}
	if err := selectorsJSON(io.MultiWriter(a.attempts, a.attemptHash), struct {
		Type    string
		Attempt *graphCostParallelAttempt
	}{"attempt", attempt}); err != nil {
		return err
	}
	a.end.Attempts++
	if attempt.Classification == "aborted" {
		a.end.Aborted++
		return nil
	}
	if attempt.Classification != "committed" {
		return errUnknown
	}
	if !e.Group.Outcome.Commit || e.Group.Outcome.ID != attempt.Transaction {
		return errMismatch
	}
	a.end.Committed++
	switch attempt.Operation.Phase {
	case "setup":
		a.end.Setup++
	case "create":
		a.end.Creates++
	case "update":
		a.end.Updates++
		if attempt.Operation.Fact == 0 {
			a.receipt = &graphSnapshotReceipt{e.Group.Tx, e.Group.Outcome}
		}
	default:
		return errInvalid
	}
	if attempt.ClientID < 0 || attempt.ClientID >= a.h.config.Cost.Writers {
		return errInvalid
	}
	if attempt.Operation.Phase != "setup" {
		a.end.Clients[attempt.ClientID]++
	}
	return a.event(struct {
		Type      string
		Operation graphCostOperation
		Original  graphCostStoredRecord
		Outcome   outcome
	}{"committed", attempt.Operation, attempt.Original, e.Group.Outcome})
}
func (a *graphSelectorsArtifacts) latency(phase string, fact uint64, client int, elapsed int64) error {
	if elapsed < 0 {
		return errInvalid
	}
	return selectorsJSON(a.latencies, struct {
		Phase  string
		Fact   uint64
		Client int
		Nanos  int64
		Access string
	}{phase, fact, client, elapsed, "actual admission through business attempt/replan/verification; not selector point latency"})
}
func (a *graphSelectorsArtifacts) Close() error {
	if a == nil || a.closed {
		return nil
	}
	a.closed = true
	var err error
	if a.column != nil {
		err = errors.Join(err, columnReadCleanup(a.column.held), a.column.Close())
	}
	for _, file := range []*os.File{a.attempts, a.events, a.latencies} {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
	}
	return err
}

// One genuine open includes native selection/acquisition retry and wait time.
// Re-observing a held owner does not invent a second open sample.
func (a *graphSelectorsArtifacts) open(source *graphHistoryRetention, name string, request graphSnapshotRequest) (*graphSnapshot, error) {
	begin := time.Now()
	r, err := source.OpenGraphSnapshot(context.Background(), request, a.end.Binding.Read)
	if err != nil {
		return nil, err
	}
	elapsed := time.Since(begin).Nanoseconds()
	if err := a.event(struct {
		Type, Name, Mode, Access string
		Cut                      certificate
		Nanos                    int64
	}{"selector-open", name, request.Mode, "actual Open through retained selection/acquisition including retries/wait; no data rows read", r.owner.id.Cut, elapsed}); err != nil {
		return nil, errors.Join(err, r.Close())
	}
	return r, nil
}

func (a *graphSelectorsArtifacts) observe(name string, read *graphSnapshot, want int64) error {
	if read == nil || !validName(name) {
		return errInvalid
	}
	selection := read.selection
	inventory := selection.Inventory
	selection.Inventory = nil
	if err := a.event(struct {
		Type, Name string
		Selection  graphSnapshotSelection
		Policy     graphSnapshotPolicy
	}{"selection", name, selection, read.policy}); err != nil {
		return err
	}
	for ordinal, record := range inventory {
		if err := a.event(struct {
			Type, Name string
			Ordinal    int
			Record     graphCostStoredRecord
		}{"certificate", name, ordinal, record}); err != nil {
			return err
		}
	}
	var count [2]uint64
	begin := time.Now()
	end, err := read.Scan(context.Background(), func(row graphSnapshotRow) error {
		value := want
		if strings.HasPrefix(row.Key, "node/") {
			value = 1
		}
		if want == 0 || row.Value.Value != value || row.Value.Deleted {
			return errMismatch
		}
		if err := a.event(struct {
			Type, Name string
			Row        graphSnapshotRow
		}{"snapshot-row", name, row}); err != nil {
			return err
		}
		count[row.Group]++
		return nil
	})
	if err != nil {
		return err
	}
	elapsed := time.Since(begin).Nanoseconds()
	var expected [2]uint64
	if want != 0 {
		expected = [2]uint64{2, 1}
		for fact := uint64(0); fact < a.h.config.Cost.Facts; fact++ {
			expected[0] += 2
			expected[a.h.config.Cost.edge(fact, want).Target.Owner]++
		}
	}
	for g := uint8(0); g < 2; g++ {
		included := false
		for _, scope := range end.Cut.Scope {
			included = included || scope == g
		}
		if !included {
			expected[g] = 0
		}
	}
	if count != expected || end.Rows != expected {
		return errMismatch
	}
	if err := a.event(struct {
		Type, Name, Access string
		End                graphSnapshotEnd
		Nanos              int64
	}{"snapshot-eof", name, "bounded retained semantic scan through EOF including retries/wait and artifact callback IO; not indexed latency", end, elapsed}); err != nil {
		return err
	}
	a.end.Observations++
	if name == "fresh-current" {
		keys := []graphSnapshotKey{{0, edgeKey("edge", a.h.config.Cost.edge(0, 6))}, {0, edgeKey("edge", a.h.config.Cost.edge(a.h.config.Cost.Facts-1, 6))}, {0, edgeKey("edge", a.h.config.Cost.edge(a.h.config.Cost.Facts+16, 6))}}
		begin := time.Now()
		points, err := read.Points(context.Background(), keys)
		if err != nil {
			return err
		}
		elapsed := time.Since(begin).Nanoseconds()
		if !points[0].Found || !points[1].Found || points[2].Found || points[0].Value.Value != 6 || points[1].Value.Value != 6 {
			return errMismatch
		}
		return a.event(struct {
			Type, Name, Access string
			Points             []graphSnapshotPoint
			Work               graphSnapshotWork
			Nanos              int64
		}{"points-eof", name, "ONE complete semantic Scan through EOF for three fixed probes including retries/wait; not an index", points, read.Work(), elapsed})
	}
	return nil
}

// Actual native ID inventory is separate from attempt emission, so a missing,
// foreign or additional durable paired original cannot disappear at EOF.
func (a *graphSelectorsArtifacts) inventory(stage string, at cut) error {
	var counts [2][2]uint64
	var actual [2]metadata
	var native [2]graphCostStoredRecord
	_, _, err := graphCostParallelRead(context.Background(), a.h.db, func(tr fdb.ReadTransaction) (bool, error) {
		if err := a.h.verifyCutIdentity(tr, at); err != nil {
			return false, err
		}
		for g := uint8(0); g < 2; g++ {
			var err error
			actual[g], err = a.h.meta(tr, g)
			if err != nil {
				return false, err
			}
			if actual[g].Groups != at.record.Groups[g] || actual[g].Floor != at.record.Round {
				return false, errMismatch
			}
			native[g], err = snapshotStored(tr, a.h.key(g, "meta", "state"), 4096)
			if err != nil {
				return false, err
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	if err := a.event(struct {
		Type, Stage string
		Cut         certificate
		Metadata    [2]metadata
		Records     [2]graphCostStoredRecord
	}{"originals-header", stage, at.record, actual, native}); err != nil {
		return err
	}
	check := func(tr fdb.ReadTransaction) error {
		if err := a.h.verifyCutIdentity(tr, at); err != nil {
			return err
		}
		for g := uint8(0); g < 2; g++ {
			m, err := a.h.meta(tr, g)
			if err != nil {
				return err
			}
			if m != actual[g] {
				return errMismatch
			}
		}
		return nil
	}
	for kindIndex, kind := range []string{"id", "request"} {
		for g := uint8(0); g < 2; g++ {
			bound := prefixRange(a.h.prefix(g) + kind + "/")
			begin := bound.Begin
			for {
				rows, _, err := graphCostParallelRead(context.Background(), a.h.db, func(tr fdb.ReadTransaction) ([][2]graphCostStoredRecord, error) {
					if err := check(tr); err != nil {
						return nil, err
					}
					raw, err := tr.GetRange(fdb.KeyRange{Begin: begin, End: bound.End}, fdb.RangeOptions{Limit: 32}).GetSliceWithError()
					if err != nil {
						return nil, err
					}
					var out [][2]graphCostStoredRecord
					used := 0
					for _, row := range raw {
						if len(row.Key) > 1024 || len(row.Value) == 0 || len(row.Value) > 64<<10 {
							return nil, errLimit
						}
						o, err := decode[outcome](row.Value)
						identity := o.ID
						if kind == "request" {
							identity = o.Request
						}
						if err != nil || o.Coordinator != g || !bytes.Equal(row.Key, a.h.key(g, kind, identity)) {
							return nil, errors.Join(errMismatch, err)
						}
						mateKind, mateIdentity := "request", o.Request
						if kind == "request" {
							mateKind, mateIdentity = "id", o.ID
						}
						request, err := snapshotStored(tr, a.h.key(g, mateKind, mateIdentity), 64<<10)
						if err != nil || !bytes.Equal(request.Value, row.Value) {
							return nil, errors.Join(errMismatch, err)
						}
						used += len(row.Key) + len(row.Value) + len(request.Key) + len(request.Value)
						if used > 4<<20 {
							return nil, errLimit
						}
						out = append(out, [2]graphCostStoredRecord{{bytes.Clone(row.Key), bytes.Clone(row.Value)}, request})
					}
					return out, nil
				})
				if err != nil {
					return err
				}
				for _, pair := range rows {
					counts[kindIndex][g]++
					if counts[kindIndex][g] > a.h.config.Limits.Transactions {
						return errLimit
					}
					if err := a.event(struct {
						Type, Stage, Kind string
						Group             uint8
						Records           [2]graphCostStoredRecord
					}{"original", stage, kind, g, pair}); err != nil {
						return err
					}
				}
				if len(rows) < 32 {
					break
				}
				begin = fdb.Key(append(bytes.Clone(rows[len(rows)-1][0].Key), 0))
			}
		}
	}
	_, _, err = graphCostParallelRead(context.Background(), a.h.db, func(tr fdb.ReadTransaction) (bool, error) { return true, check(tr) })
	if err != nil {
		return err
	}
	if counts[0] != counts[1] || counts[0][0] != actual[0].Transactions || counts[0][1] != actual[1].Transactions || counts[0][0]+counts[0][1] != a.end.Attempts {
		return errMismatch
	}
	return a.event(struct {
		Type, Stage string
		Rows        [2][2]uint64
		Metadata    [2]metadata
	}{"originals-eof", stage, counts, actual})
}

func (a *graphSelectorsArtifacts) raw(header graphCostArtifactHeader, walk func(func(graphCostRawRow) error) (graphCostStreamEnd, error)) (end graphCostStreamEnd, err error) {
	header.Format = graphSelectorsFormat
	file, _, err := openGraphCostArtifact(a.root, "fdb-selectors-"+header.Stream+".jsonl")
	if err != nil {
		return end, err
	}
	defer func() {
		err = errors.Join(err, file.Close())
		if err != nil {
			end = graphCostStreamEnd{}
		}
	}()
	digest := sha256.New()
	out := io.MultiWriter(file, digest)
	if err = selectorsJSON(out, header); err != nil {
		return end, err
	}
	var counts [2]uint64
	end, err = walk(func(row graphCostRawRow) error {
		if row.Group > 1 || counts[row.Group] >= header.Config.Limits.Versions {
			return errLimit
		}
		if err := selectorsJSON(out, graphCostArtifactRow{"row", row}); err != nil {
			return err
		}
		counts[row.Group]++
		return nil
	})
	if err != nil {
		return graphCostStreamEnd{}, err
	}
	if end.Rows != counts || end.Metadata != header.Metadata || end.Groups != header.FinalCut.Groups {
		return graphCostStreamEnd{}, errMismatch
	}
	err = selectorsJSON(file, graphCostArtifactFooter{"footer", counts, end.Groups, end.Metadata, hex.EncodeToString(digest.Sum(nil))})
	return end, err
}
func (a *graphSelectorsArtifacts) finish(source *graphHistoryRetention) (err error) {
	p := a.h.config.Cost
	if a.end.Setup != 3 || a.end.Creates != p.Facts || a.end.Updates != p.Facts || a.end.Committed != 3+2*p.Facts || a.end.Attempts != a.end.Committed+a.end.Aborted || a.end.Observations != 11 {
		return errMismatch
	}
	for client := 0; client < p.Writers; client++ {
		if a.end.Clients[client] < 2 {
			return errMismatch
		}
	}
	a.end.Old, a.end.Final = a.old.record, a.final.record
	if err = a.inventory("final", a.final); err != nil {
		return err
	}
	held, err := a.open(source, "raw-source-held", graphSnapshotRequest{Mode: "At", Scope: []uint8{0, 1}, At: a.final.record})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, held.Close()) }()
	if err = a.event(struct {
		Type      string
		Selection graphSnapshotSelection
		Policy    graphSnapshotPolicy
	}{"source-held", held.selection, held.policy}); err != nil {
		return err
	}
	policy := graphCostStreamPolicy{32, 4 << 20}
	header, err := a.h.graphCostArtifactHeader(cut{a.end.Initial}, a.old, a.final, policy)
	if err != nil {
		return err
	}
	for i, name := range []string{"history", "current", "cdc"} {
		header.Stream = name
		a.end.Sources[i], err = a.raw(header, func(emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
			switch name {
			case "history":
				rows, err := held.owner.walk(context.Background(), emit)
				if err != nil {
					return graphCostStreamEnd{}, err
				}
				return graphCostStreamEnd{Rows: rows, Groups: a.final.record.Groups, Metadata: header.Metadata}, nil
			case "current":
				return a.h.graphCostWalkCurrent(a.final, policy, emit)
			default:
				return a.h.graphCostWalkChanges(cut{a.end.Initial}, a.final, policy, emit)
			}
		})
		if err != nil {
			return err
		}
	}
	if a.end.Sources[0].Rows[0]+a.end.Sources[0].Rows[1] != 3+6*p.Facts || a.end.Sources[1].Rows[0]+a.end.Sources[1].Rows[1] != 3+3*p.Facts || a.end.Sources[2].Rows != a.final.record.Groups {
		return errMismatch
	}
	if err = held.Close(); err != nil {
		return err
	}
	closed, _, err := graphCostParallelRead(context.Background(), a.h.db, func(tr fdb.ReadTransaction) (graphCostStoredRecord, error) {
		raw, err := snapshotStored(tr, source.ownerKey(held.owner.id.Request), historyControlBytes)
		if err != nil {
			return raw, err
		}
		actual, err := decode[graphHistoryOwner](raw.Value)
		if err != nil {
			return raw, err
		}
		expected := held.owner.id
		expected.State = historyReleased
		expected.TerminalRequest = "close"
		if !graphColumnSame(actual, expected) {
			return raw, errMismatch
		}
		return raw, nil
	})
	if err != nil {
		return err
	}
	if err = a.event(struct {
		Type, Access string
		Owner        graphCostStoredRecord
		Work         graphSnapshotWork
	}{"source-closed", "Work covers selector opening/cleanup only; complete raw history uses the separately bound History policy", closed, held.Work()}); err != nil {
		return err
	}
	a.end.AttemptSHA256 = hex.EncodeToString(a.attemptHash.Sum(nil))
	a.end.EventSHA256 = hex.EncodeToString(a.eventHash.Sum(nil))
	if err = selectorsJSON(a.attempts, struct {
		Type                         string
		Attempts, Aborted, Committed uint64
		PayloadSHA256                string
	}{"footer", a.end.Attempts, a.end.Aborted, a.end.Committed, a.end.AttemptSHA256}); err != nil {
		return err
	}
	if err = selectorsJSON(a.events, struct {
		Type string
		End  graphSelectorsEnd
	}{"footer", a.end}); err != nil {
		return err
	}
	if err = a.Close(); err != nil {
		return err
	}
	file, _, err := openGraphCostArtifact(a.root, "fdb-selectors-result.json")
	if err != nil {
		return err
	}
	return errors.Join(selectorsJSON(file, a.end), file.Close())
}

func (a *graphSelectorsArtifacts) columnsStart() (err error) {
	policy, retention := graphCostColumnPolicies(a.h.config.Cost)
	store, err := newGraphColumnStore(a.h, policy)
	if err != nil {
		return err
	}
	pins, err := newGraphColumnPins(store, retention)
	if err != nil {
		return err
	}
	file, _, err := openGraphCostArtifact(a.root, "fdb-selectors-columns.jsonl")
	if err != nil {
		return err
	}
	l := &graphCostColumnLane{s: store, pins: pins, root: a.root, file: file, payload: sha256.New(), whole: sha256.New(), initial: cut{a.end.Initial}}
	l.end.Format, l.end.SourceFormat, l.end.ReferenceLayout = graphSelectorsFormat, graphSelectorsFormat, "native-logical-kv-reference"
	a.column = l
	records, err := l.records([]fdb.Key{store.key("binding"), pins.key("binding")})
	if err != nil {
		return err
	}
	if err = l.write(struct {
		Type, Format, SourceFormat, ReferenceLayout string
		Config                                      config
		InitialCut                                  certificate
		Records                                     []graphCostStoredRecord
	}{"header", graphSelectorsFormat, graphSelectorsFormat, "native-logical-kv-reference", a.h.config, a.end.Initial, records}, true); err != nil {
		return err
	}
	return l.holdOld(a.old)
}

// Genuine refusals retain their native post-call guard/owner/reference bytes;
// a printable error label alone supplies no ownership or collection authority.
func (a *graphSelectorsArtifacts) columnRefusal(name, generation string, input, result any, observed, expected error) error {
	l := a.column
	if !errors.Is(observed, expected) || errors.Is(observed, errUnknown) {
		return errors.Join(errMismatch, observed)
	}
	if _, ok := errors.AsType[fdb.Error](observed); ok {
		return observed
	}
	if _, ok := errors.AsType[*graphColumnReadPending](observed); ok {
		return observed
	}
	records, err := l.records([]fdb.Key{l.s.generationKey(generation, "manifest"), l.s.generationKey(generation, "progress"), l.s.generationKey(generation, "owners"), l.held.key(), columnPinKey(l.s, l.token.Request), l.pins.recordKey("reference", "head"), l.s.key("ledger"), l.pins.key("ledger"), a.h.key(0, "meta", "state"), a.h.key(1, "meta", "state")})
	if err != nil {
		return err
	}
	return l.write(struct {
		Type, Name, Generation, Error string
		Input, Result                 any
		Records                       []graphCostStoredRecord
	}{"refusal", name, generation, observed.Error(), input, result, records}, true)
}
func (a *graphSelectorsArtifacts) columnsFinish() (err error) {
	l := a.column
	ctx := context.Background()
	if l == nil || l.held == nil {
		return errInvalid
	}
	l.end.Final, err = l.s.publish(ctx, "final", l.initial, a.final)
	if err != nil {
		return err
	}
	l.head, err = l.pins.reference(ctx, "head-final", "head", "final", l.head.Version)
	if err != nil {
		return err
	}
	if err = l.stage("final-head", "old", "final"); err != nil {
		return err
	}
	if err = l.pins.finish(ctx, "expire-old", l.token, true); err != nil {
		return err
	}
	refusedRead, refused := l.pins.open(ctx, l.token)
	if refusedRead != nil {
		return errors.Join(errMismatch, columnReadCleanup(refusedRead))
	}
	if err = a.columnRefusal("expired-new-open", "old", l.token, refusedRead, refused, errColumnExpired); err != nil {
		return err
	}
	budget := graphColumnCollectBudget{Chunks: 1, Bytes: 128 << 10, OutputBytes: 1024}
	refusedPage, refused := l.pins.collect(ctx, "old", budget)
	if refusedPage != (graphColumnCollectResult{}) {
		return errMismatch
	}
	input := struct {
		Generation string
		Budget     graphColumnCollectBudget
	}{"old", budget}
	if err = a.columnRefusal("held-old-collection", "old", input, refusedPage, refused, errColumnInUse); err != nil {
		return err
	}
	if err = l.stage("expired-held", "old", "final"); err != nil {
		return err
	}
	// Existing export performs actual binary chunk/cache/typed walks and three
	// scan-based points at5(old)/6(final), with independently authenticated EOF.
	if err = l.export(l.held, l.end.Old, a.old); err != nil {
		return err
	}
	current, err := newGraphColumnRead(l.s, "final", nil)
	if err != nil {
		return err
	}
	if err = current.open(ctx); err != nil {
		return errors.Join(err, columnReadCleanup(current))
	}
	err = l.export(current, l.end.Final, a.final)
	if err = errors.Join(err, columnReadCleanup(current)); err != nil {
		return err
	}
	if err = l.stage("reads-complete", "old", "final"); err != nil {
		return err
	}
	if err = columnReadCleanup(l.held); err != nil {
		return err
	}
	if err = l.stage("old-closed", "old", "final"); err != nil {
		return err
	}
	for ordinal := uint64(0); ordinal < l.end.Old.Chunks; ordinal++ {
		page, err := l.pins.collect(ctx, "old", budget)
		if err != nil || page.Cursor != ordinal+1 || page.WorkChunks != 1 || page.Done != (ordinal+1 == l.end.Old.Chunks) {
			return errors.Join(errMismatch, err)
		}
		key := l.s.chunkKey("old", ordinal)
		guard, _, err := graphCostParallelRead(ctx, a.h.db, func(tr fdb.ReadTransaction) (graphCostStoredRecord, error) {
			raw, err := tr.Get(key).Get()
			if err != nil {
				return graphCostStoredRecord{}, err
			}
			if raw != nil {
				return graphCostStoredRecord{}, errMismatch
			}
			actual, found, err := l.s.guard(tr, l.end.Old)
			if err != nil || !found || actual.Cursor != page.Cursor {
				return graphCostStoredRecord{}, errors.Join(errMismatch, err)
			}
			return snapshotStored(tr, l.s.generationKey("old", "owners"), historyControlBytes)
		})
		if err != nil {
			return err
		}
		if err = l.write(struct {
			Type   string
			Budget graphColumnCollectBudget
			Result graphColumnCollectResult
			Absent []byte
			Guard  graphCostStoredRecord
		}{"collect", budget, page, bytes.Clone(key), guard}, true); err != nil {
			return err
		}
		l.end.Counts.Collections++
	}
	refusedManifest, refused := l.s.publish(ctx, "old", l.initial, a.old)
	if !graphColumnSame(refusedManifest, graphColumnManifest{}) {
		return errMismatch
	}
	publishInput := struct {
		Generation     string
		Initial, Final certificate
	}{"old", l.initial.record, a.old.record}
	if err = a.columnRefusal("retired-publication", "old", publishInput, refusedManifest, refused, errColumnExpired); err != nil {
		return err
	}
	refusedPage, refused = l.pins.collect(ctx, "final", budget)
	if refusedPage != (graphColumnCollectResult{}) {
		return errMismatch
	}
	input.Generation = "final"
	if err = a.columnRefusal("final-reference-collection", "final", input, refusedPage, refused, errColumnInUse); err != nil {
		return err
	}
	if err = l.stage("terminal", "old", "final"); err != nil {
		return err
	}
	records, err := l.records([]fdb.Key{l.s.key("ledger"), l.pins.key("ledger")})
	if err != nil {
		return err
	}
	l.end.Ledger, err = decode[graphColumnLedger](records[0].Value)
	if err != nil {
		return err
	}
	l.end.PinLedger, err = decode[graphColumnPinLedger](records[1].Value)
	if err != nil {
		return err
	}
	if err = l.storage(); err != nil {
		return err
	}
	if l.end.Counts.Chunks != l.end.Old.Chunks+l.end.Final.Chunks || l.end.Counts.WalkRows != 2*(l.end.Old.Rows+l.end.Final.Rows) || l.end.Counts.Points != 6 || l.end.Counts.Stages != 6 || l.end.Counts.Collections != l.end.Old.Chunks || l.end.Ledger.Chunks != l.end.Final.Chunks {
		return errMismatch
	}
	copy(l.end.PayloadSHA256[:], l.payload.Sum(nil))
	if err = l.write(struct {
		Type string
		End  graphCostColumnEnd
	}{"footer", l.end}, false); err != nil {
		return err
	}
	copy(l.end.ArtifactSHA256[:], l.whole.Sum(nil))
	if err = l.Close(); err != nil {
		return err
	}
	a.end.Columns = new(l.end)
	return nil
}
