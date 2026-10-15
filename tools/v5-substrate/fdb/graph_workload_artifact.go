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
	"strconv"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type graphContentionPoint struct {
	Name     string
	Cut      certificate
	Fact     uint64
	Key      []byte
	Found    bool
	Value    value
	Original graphCostParallelProof
}
type graphContentionEnd struct {
	Version                                                            uint8               `json:"version"`
	Format                                                             string              `json:"format"`
	Spec                                                               graphContentionSpec `json:"spec"`
	Config                                                             config
	Binding                                                            graphContentionBinding
	BindingRecord                                                      graphCostStoredRecord
	Initial, Old, Final                                                certificate
	Attempts, Aborted, Committed, HotAborted, ClaimAborted             uint64
	Setup, Seed, Hot, Claims, Submissions, SDKCallbacks, ReadCallbacks uint64
	ClientHot                                                          [32]uint64
	OldMetadata, FinalMetadata                                         [2]metadata
	Sources                                                            [3]graphCostStreamEnd
	Columns                                                            [2]graphColumnManifest
	Points                                                             []graphContentionPoint
	ProbeWalks                                                         uint64
	ProbeAccess                                                        string
	AttemptPayloadSHA256                                               string
	Transport                                                          *graphTransportRecord `json:",omitempty"`
}
type graphContentionArtifacts struct {
	ctx                       context.Context
	h                         *harness
	root                      *os.Root
	attempts, groups, latency *os.File
	payload                   hash.Hash
	end                       graphContentionEnd
	closed                    bool
}

func newGraphContentionArtifacts(h *harness, root *os.Root, spec graphContentionSpec, binding graphCostStoredRecord, initial cut) (a *graphContentionArtifacts, err error) {
	return newGraphContentionArtifactsAdmission(nil, h, root, spec, binding, initial, nil)
}
func newGraphContentionArtifactsAdmission(ctx context.Context, h *harness, root *os.Root, spec graphContentionSpec, binding graphCostStoredRecord, initial cut, transport *graphTransportRecord) (a *graphContentionArtifacts, err error) {
	admission, checkErr := graphTransportArtifact(h, graphFeedWorkloadSpec(spec), transport)
	if checkErr != nil {
		return nil, checkErr
	}
	if admission != nil && ctx == nil {
		return nil, errInvalid
	}

	if h == nil || root == nil || (spec.Version != 1 && spec.Version != 2) || spec.Family != graphContentionFamily || spec.profile() != h.config.Cost || !strings.HasPrefix(h.config.Graph, "graph-workload-contention-v"+strconv.Itoa(int(spec.Version))+"-"+spec.Layout+"-") || !validGraphContentionConfig(h.config) {
		return nil, errInvalid
	}
	a = &graphContentionArtifacts{ctx: ctx, h: h, root: root, payload: sha256.New(), end: graphContentionEnd{Version: spec.Version, Format: graphTransportFormatFor(spec.Version, graphContentionFormat), Spec: spec, Config: h.config, Binding: graphContentionPolicyAdmission(h.config, admission), BindingRecord: binding, Initial: initial.record, Transport: transport}}
	defer func() {
		if err != nil {
			err = errors.Join(err, a.Close())
			a = nil
		}
	}()
	a.attempts, _, err = openGraphCostArtifact(root, "fdb-workload-attempts.jsonl")
	if err != nil {
		return a, err
	}
	a.groups, _, err = openGraphCostArtifact(root, "fdb-workload-operations.jsonl")
	if err != nil {
		return a, err
	}
	a.latency, _, err = openGraphCostArtifact(root, "fdb-workload-latency.jsonl")
	if err != nil {
		return a, err
	}
	err = a.write(struct {
		Type, Format string
		Spec         graphContentionSpec
		Binding      graphContentionBinding
		Record       graphCostStoredRecord
		Initial      certificate
	}{"header", a.end.Format, spec, a.end.Binding, binding, initial.record})
	return a, err
}
func (a *graphContentionArtifacts) write(v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(body)+1 > 512<<10 {
		return errLimit
	}
	_, err = io.MultiWriter(a.attempts, a.payload).Write(append(body, '\n'))
	return err
}
func (a *graphContentionArtifacts) accept(event graphContentionEvent) error {
	if event.Attempt != nil {
		row := event.Attempt
		if err := a.write(struct {
			Type    string
			Attempt *graphContentionAttempt
		}{"attempt", row}); err != nil {
			return err
		}
		a.end.Attempts++
		a.end.ReadCallbacks += uint64(row.ReadCallbacks) // #nosec G115 -- bounded nonnegative native callback counts.
		for _, submit := range row.Submissions {
			a.end.Submissions++
			a.end.SDKCallbacks += uint64(submit.Callbacks)
		} // #nosec G115 -- bounded nonnegative callback counts.
		if a.end.Attempts > a.h.config.Limits.Transactions {
			return errLimit
		}
		if row.Classification == "aborted" {
			a.end.Aborted++
			if row.Operation.Phase == "hot" {
				a.end.HotAborted++
			}
			if row.Operation.Phase == "claim" {
				a.end.ClaimAborted++
			}
		}
		if row.Classification == "committed" {
			a.end.Committed++
			switch row.Operation.Phase {
			case "setup":
				a.end.Setup++
			case "seed":
				a.end.Seed++
			case "hot":
				a.end.Hot++
				a.end.ClientHot[row.ClientID]++
			case "claim":
				a.end.Claims++
			default:
				return errInvalid
			}
			if err := json.NewEncoder(a.groups).Encode(event.Group); err != nil {
				return err
			}
		}
	}
	if event.Done && event.Err == nil {
		if err := json.NewEncoder(a.latency).Encode(struct {
			Operation    graphCostOperation
			ClientID     int
			ElapsedNanos int64
		}{event.Operation, event.Client, event.ElapsedNanos}); err != nil {
			return err
		}
	}
	return event.Err
}
func (a *graphContentionArtifacts) metadata(c cut) ([2]metadata, error) {
	got, err := graphCostReadContext(a.ctx, a.h.db, func(tr fdb.ReadTransaction) (any, error) {
		var out [2]metadata
		if err := a.h.verifyCut(tr, c); err != nil {
			return nil, err
		}
		if err := a.h.graphContentionBindingIn(tr); err != nil {
			return nil, err
		}
		for g := uint8(0); g < 2; g++ {
			if err := a.h.checkConfig(tr, g); err != nil {
				return nil, err
			}
			m, err := a.h.meta(tr, g)
			if err != nil {
				return nil, err
			}
			if m.Groups != c.record.Groups[g] || m.Floor != c.record.Round {
				return nil, errMismatch
			}
			out[g] = m
		}
		if out[0].Transactions+out[1].Transactions != a.end.Attempts {
			return nil, errMismatch
		}
		return out, nil
	})
	if err != nil {
		return [2]metadata{}, err
	}
	return got.([2]metadata), nil
}
func (a *graphContentionArtifacts) barrier(name string, c cut) error {
	meta, err := a.metadata(c)
	if err != nil {
		return err
	}
	if name == "old" {
		a.end.Old, a.end.OldMetadata = c.record, meta
	} else if name == "final" {
		a.end.Final, a.end.FinalMetadata = c.record, meta
	} else {
		return errInvalid
	}
	return a.write(struct {
		Type, Name                   string
		Cut                          certificate
		Metadata                     [2]metadata
		Attempts, Committed, Aborted uint64
	}{"barrier", name, c.record, meta, a.end.Attempts, a.end.Committed, a.end.Aborted})
}
func (a *graphContentionArtifacts) Close() error {
	if a == nil || a.closed {
		return nil
	}
	a.closed = true
	var err error
	for _, file := range []*os.File{a.attempts, a.groups, a.latency} {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
	}
	return err
}
func graphContentionRawWrite(root *os.Root, header graphCostArtifactHeader, walk func(func(graphCostRawRow) error) (graphCostStreamEnd, error)) (end graphCostStreamEnd, err error) {
	if header.Format != graphTransportFormatFor(graphTransportConfigVersion(header.Config), graphContentionFormat) || header.Policy.validate() != nil || walk == nil {
		return end, errInvalid
	}
	file, _, err := openGraphCostArtifact(root, "fdb-workload-"+header.Stream+".jsonl")
	if err != nil {
		return end, err
	}
	defer func() {
		err = errors.Join(err, file.Close())
		if err != nil {
			end = graphCostStreamEnd{}
		}
	}()
	payload := sha256.New()
	writer := io.MultiWriter(file, payload)
	if err = json.NewEncoder(writer).Encode(header); err != nil {
		return end, err
	}
	var counts [2]uint64
	verified, err := walk(func(row graphCostRawRow) error {
		if row.Group > 1 || counts[row.Group] >= header.Config.Limits.Versions {
			return errLimit
		}
		body, err := json.Marshal(graphCostArtifactRow{"row", row})
		if err != nil {
			return err
		}
		if len(body)+1 > 512<<10 {
			return errLimit
		}
		if _, err = writer.Write(append(body, '\n')); err != nil {
			return err
		}
		counts[row.Group]++
		return nil
	})
	if err != nil {
		return end, err
	}
	if counts != verified.Rows || verified.Groups != header.FinalCut.Groups || verified.Metadata != header.Metadata {
		return end, errMismatch
	}
	if err = json.NewEncoder(file).Encode(graphCostArtifactFooter{"footer", counts, verified.Groups, verified.Metadata, hex.EncodeToString(payload.Sum(nil))}); err != nil {
		return end, err
	}
	return verified, nil
}
func (a *graphContentionArtifacts) current(final cut, policy graphCostStreamPolicy, emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
	stream, err := newGraphCostStreamContext(a.ctx, a.h, final, final, policy, emit)
	if err != nil {
		return graphCostStreamEnd{}, err
	}
	var counts [2]uint64
	for g := uint8(0); g < 2; g++ {
		interval := prefixRange(a.h.prefix(g) + "current/")
		begin, end := interval.Begin.(fdb.Key), interval.End.(fdb.Key)
		for {
			got, err := graphCostReadContext(a.ctx, a.h.db, func(tr fdb.ReadTransaction) (any, error) {
				if _, err := stream.check(tr, true); err != nil {
					return nil, err
				}
				rows, err := tr.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Limit: policy.PageRows}).GetSliceWithError()
				if err != nil {
					return nil, err
				}
				out := make([]graphCostRawRow, 0, len(rows))
				used := 0
				for _, row := range rows {
					if len(row.Key) > 1024 || len(row.Value) == 0 || len(row.Value) > 64<<10 || !bytes.HasPrefix(row.Key, []byte(a.h.prefix(g)+"current/")) || bytes.Compare(row.Key, begin) < 0 || bytes.Compare(row.Key, end) >= 0 {
						return nil, errLimit
					}
					logical, err := hex.DecodeString(string(row.Key[len(a.h.prefix(g)+"current/"):]))
					if err != nil || !validName(string(logical)) || !bytes.Equal(row.Key, a.h.key(g, "current", string(logical))) {
						return nil, errInvalid
					}
					v, err := decode[value](row.Value)
					if err != nil || !validGraphValue(v) || v.Deleted || v.Round > final.record.Round {
						return nil, errors.Join(errInvalid, err)
					}
					_, extra, err := a.h.historyOriginalIn(tr, g, string(logical), v)
					if err != nil {
						return nil, err
					}
					used += len(row.Key) + len(row.Value) + int(extra)
					if used > policy.PageBytes {
						return nil, errLimit
					} // #nosec G115 -- admitted bounded raw original envelopes.
					out = append(out, graphCostRawRow{Group: g, Key: bytes.Clone(row.Key), Value: bytes.Clone(row.Value)})
				}
				return out, nil
			})
			if err != nil {
				return graphCostStreamEnd{}, err
			}
			rows := got.([]graphCostRawRow)
			for _, row := range rows {
				counts[g]++
				if err := emit(row); err != nil {
					return graphCostStreamEnd{}, err
				}
			}
			if len(rows) < policy.PageRows {
				break
			}
			begin = fdb.Key(append(bytes.Clone(rows[len(rows)-1].Key), 0))
		}
	}
	return stream.finish(counts)
}

func (a *graphContentionArtifacts) inventory() error {
	var counts [2]uint64
	for g := uint8(0); g < 2; g++ {
		interval := prefixRange(a.h.prefix(g) + "id/")
		begin, end := interval.Begin.(fdb.Key), interval.End.(fdb.Key)
		for {
			got, err := graphCostReadContext(a.ctx, a.h.db, func(tr fdb.ReadTransaction) (any, error) {
				if err := a.h.graphContentionBindingIn(tr); err != nil {
					return nil, err
				}
				m, err := a.h.meta(tr, g)
				if err != nil || m != a.end.FinalMetadata[g] {
					return nil, errors.Join(errMismatch, err)
				}
				raw, err := tr.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Limit: 32}).GetSliceWithError()
				if err != nil {
					return nil, err
				}
				out := make([]graphCostParallelProof, 0, len(raw))
				used := 0
				for _, record := range raw {
					if len(record.Key) > 1024 || len(record.Value) > 64<<10 {
						return nil, errLimit
					}
					o, err := decode[outcome](record.Value)
					if err != nil {
						return nil, err
					}
					if o.Coordinator != g || !bytes.Equal(record.Key, a.h.key(g, "id", o.ID)) || !validName(o.Request) || o.Digest == [32]byte{} {
						return nil, errMismatch
					}
					requestKey := a.h.key(g, "request", o.Request)
					request, err := tr.Get(requestKey).Get()
					if err != nil {
						return nil, err
					}
					if !bytes.Equal(request, record.Value) {
						return nil, errMismatch
					}
					changeKey := a.h.changeKey(g, o.Sequence, o.Request)
					body, err := tr.Get(changeKey).Get()
					if err != nil {
						return nil, err
					}
					proof := graphCostParallelProof{Outcome: o, Pair: [2]graphCostStoredRecord{{bytes.Clone(record.Key), bytes.Clone(record.Value)}, {bytes.Clone(requestKey), bytes.Clone(request)}}, Original: graphCostStoredRecord{bytes.Clone(changeKey), bytes.Clone(body)}}
					if o.Commit {
						group, err := decode[changeGroup](body)
						if err != nil {
							return nil, err
						}
						if err = a.h.validateChange(group); err != nil {
							return nil, err
						}
						if group.Outcome != o {
							return nil, errMismatch
						}
						proof.Group = group
					} else if o.Round != 0 || o.Sequence != 0 || body != nil {
						return nil, errMismatch
					}
					used += len(record.Key) + len(record.Value) + len(requestKey) + len(request) + len(changeKey) + len(body)
					if used > 4<<20 {
						return nil, errLimit
					}
					out = append(out, proof)
				}
				return out, nil
			})
			if err != nil {
				return err
			}
			page := got.([]graphCostParallelProof)
			for _, proof := range page {
				counts[g]++
				if counts[g] > a.end.FinalMetadata[g].Transactions {
					return errLimit
				}
				if err = a.write(struct {
					Type  string
					Group uint8
					Proof graphCostParallelProof
				}{"original", g, proof}); err != nil {
					return err
				}
			}
			if len(page) < 32 {
				break
			}
			begin = fdb.Key(append(bytes.Clone(page[len(page)-1].Pair[0].Key), 0))
		}
	}
	if counts != [2]uint64{a.end.FinalMetadata[0].Transactions, a.end.FinalMetadata[1].Transactions} {
		return errMismatch
	}
	return a.write(struct {
		Type     string
		Rows     [2]uint64
		Metadata [2]metadata
	}{"originals-eof", counts, a.end.FinalMetadata})
}
func (a *graphContentionArtifacts) sources(source *graphHistoryRetention) (err error) {
	policy := graphCostStreamPolicy{PageRows: 32, PageBytes: 4 << 20}
	header, err := a.h.graphCostArtifactHeaderContext(a.ctx, cut{a.end.Initial}, cut{a.end.Old}, cut{a.end.Final}, policy)
	if err != nil {
		return err
	}
	header.Format = a.end.Format
	owner, _, cleanupOwner, err := graphTransportHeld(a.ctx, a.context(), source, cut{a.end.Final}, "workload-final")
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, cleanupOwner())
		if err != nil {
			a.end.Sources = [3]graphCostStreamEnd{}
		}
	}()

	for i, name := range []string{"history", "current", "cdc"} {
		header.Stream = name
		walk := func(emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
			if name == "history" {
				rows, err := a.probeWalk("final", owner, emit)
				if err != nil {
					return graphCostStreamEnd{}, err
				}
				meta, err := a.metadata(cut{a.end.Final})
				if err != nil {
					return graphCostStreamEnd{}, err
				}
				return graphCostStreamEnd{Rows: rows, Groups: a.end.Final.Groups, Metadata: meta}, nil
			}
			if name == "current" {
				return a.current(cut{a.end.Final}, policy, emit)
			}
			return a.h.graphCostWalkChangesContext(a.ctx, cut{a.end.Initial}, cut{a.end.Final}, policy, emit)
		}
		a.end.Sources[i], err = graphContentionRawWrite(a.root, header, walk)
		if err != nil {
			return err
		}
	}
	p := a.h.config.Cost
	if a.end.Sources[0].Rows[0]+a.end.Sources[0].Rows[1] != 3+6*p.Facts || a.end.Sources[1].Rows[0]+a.end.Sources[1].Rows[1] != 3+3*p.Facts || a.end.Sources[2].Rows != a.end.Final.Groups {
		return errMismatch
	}
	return nil
}

// Four sample observations share ONE authenticated scan. Final observations
// are folded into the required raw-history stream; this is not an index or a
// native point-lookup latency measurement.
func (a *graphContentionArtifacts) probeWalk(name string, owner *graphHistoryRead, emit func(graphCostRawRow) error) ([2]uint64, error) {
	if len(a.end.Points) > 4 || name != "old-held" && name != "final" || owner == nil || emit == nil {
		return [2]uint64{}, errInvalid
	}
	p := a.h.config.Cost
	var points [4]graphContentionPoint
	for i, fact := range [4]uint64{0, 9, p.Facts / 2, p.Facts + 16} {
		points[i] = graphContentionPoint{Name: name, Cut: owner.id.Cut, Fact: fact, Key: []byte(edgeKey("edge", p.edge(fact, 5)))}
	}
	rows, err := owner.walk(a.context(), func(row graphCostRawRow) error {
		key, v, err := a.h.graphHistoryRow(row, owner.id.Cut.Round)
		if err != nil {
			return err
		}
		for i := range points {
			if row.Group == 0 && key == string(points[i].Key) {
				points[i].Found = true
				points[i].Value = v
			}
		}
		return emit(row)
	})
	if err != nil {
		return [2]uint64{}, err
	}
	if rows[0]+rows[1] > a.h.config.Limits.Versions {
		return [2]uint64{}, errLimit
	}
	for _, out := range points {
		want := graphContentionExpectedValue(p, out.Fact, name == "old-held")
		if out.Found != (want != 0) || out.Found && out.Value.Value != want {
			return [2]uint64{}, errMismatch
		}
		if out.Found {
			result, err := graphCostReadContext(a.ctx, a.h.db, func(tr fdb.ReadTransaction) (any, error) {
				if err := owner.check(tr); err != nil {
					return nil, err
				}
				group, _, err := a.h.historyOriginalIn(tr, 0, string(out.Key), out.Value)
				return group, err
			})
			if err != nil {
				return [2]uint64{}, err
			}
			group := result.(changeGroup)
			body, err := wire(group.Tx)
			if err != nil {
				return [2]uint64{}, err
			}
			out.Original, _, err = a.h.graphCostReadParallelOriginal(a.context(), group.Tx, body)
			if err != nil {
				return [2]uint64{}, err
			}
		}
		a.end.Points = append(a.end.Points, out)
		if err = a.write(struct {
			Type  string
			Point graphContentionPoint
			Rows  [2]uint64
		}{"point-eof", out, rows}); err != nil {
			return [2]uint64{}, err
		}
	}
	a.end.ProbeWalks++
	a.end.ProbeAccess = "four fixed probes share one retained-history walk per cut; final probes fold into required history export; not indexed point latency"
	err = a.write(struct {
		Type, Name string
		Rows       [2]uint64
		Probes     uint64
	}{"probe-walk-eof", name, rows, 4})
	if err != nil {
		return [2]uint64{}, err
	}
	return rows, nil
}
func (a *graphContentionArtifacts) finish(source *graphHistoryRetention) error {
	p := a.h.config.Cost
	if a.end.Setup != 3 || a.end.Seed != p.Facts/2 || a.end.Hot != p.Facts || a.end.Claims != p.Facts/2 || a.end.Committed != 3+2*p.Facts || a.end.ClaimAborted != p.Facts/2*(graphContentionCompetitors(p)-1) || a.end.Attempts != a.end.Committed+a.end.Aborted || a.end.Aborted != a.end.HotAborted+a.end.ClaimAborted {
		return errMismatch
	}
	if err := a.inventory(); err != nil {
		return err
	}
	if err := a.sources(source); err != nil {
		return err
	}
	a.end.AttemptPayloadSHA256 = hex.EncodeToString(a.payload.Sum(nil))
	if err := json.NewEncoder(a.attempts).Encode(struct {
		Type string
		End  graphContentionEnd
	}{"footer", a.end}); err != nil {
		return err
	}
	if err := a.Close(); err != nil {
		return err
	}
	file, encoder, err := openGraphCostArtifact(a.root, "fdb-workload-result.json")
	if err != nil {
		return err
	}
	return errors.Join(encoder.Encode(a.end), file.Close())
}

// Reuse the production publisher, retained owners, raw chunk exporter, typed
// walks, cache and collector. Only the fresh family's semantic point oracle and
// completion grammar differ from the historical create/update lane.
func (a *graphContentionArtifacts) columnStart(initial, old cut) (l *graphCostColumnLane, err error) {
	policy, retention := graphCostColumnPolicies(a.h.config.Cost)
	s, err := newGraphColumnStoreContext(a.ctx, a.h, policy)
	if err != nil {
		return nil, err
	}
	pins, err := newGraphColumnPins(s, retention)
	if err != nil {
		return nil, err
	}
	file, _, err := openGraphCostArtifact(a.root, "fdb-workload-columns.jsonl")
	if err != nil {
		return nil, err
	}
	l = &graphCostColumnLane{s: s, pins: pins, root: a.root, file: file, payload: sha256.New(), whole: sha256.New(), initial: initial}
	l.end.Format, l.end.SourceFormat, l.end.ReferenceLayout = a.end.Format, a.end.Format, "native-logical-kv-reference"
	defer func() {
		if err != nil {
			err = errors.Join(err, columnReadCleanup(l.held), l.Close())
			l = nil
		}
	}()
	records, err := l.records([]fdb.Key{s.key("binding"), pins.key("binding")})
	if err != nil {
		return l, err
	}
	if err = l.write(struct {
		Type, Format string
		Config       config
		Records      []graphCostStoredRecord
	}{"header", a.end.Format, a.h.config, records}, true); err != nil {
		return l, err
	}
	err = l.holdOld(old)
	return l, err
}
func (a *graphContentionArtifacts) columnFinish(l *graphCostColumnLane, final cut) (err error) {
	ctx := a.context()
	l.end.Final, err = l.s.publish(ctx, "final", l.initial, final)
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
	if err = a.columnRefusal(l, "expired-new-open", "old", l.token, refusedRead, refused, errColumnExpired); err != nil {
		return err
	}
	budget := graphColumnCollectBudget{Chunks: 1, Bytes: 128 << 10, OutputBytes: 1024}
	refusedCollection, refused := l.pins.collect(ctx, "old", budget)
	if refusedCollection != (graphColumnCollectResult{}) {
		return errMismatch
	}
	collectInput := struct {
		Generation string
		Budget     graphColumnCollectBudget
	}{"old", budget}
	if err = a.columnRefusal(l, "held-old-collection", "old", collectInput, refusedCollection, refused, errColumnInUse); err != nil {
		return err
	}
	if err = l.stage("expired-held", "old", "final"); err != nil {
		return err
	}
	export := func(owner *graphColumnRead, m graphColumnManifest, at cut) (err error) {
		cache, err := l.chunks(owner, m)
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, cache.Close()) }()
		if err = l.walk(owner, at, nil, m.Generation.ID); err != nil {
			return err
		}
		if err = l.walk(owner, at, cache, m.Generation.ID+"-cache"); err != nil {
			return err
		}
		for _, fact := range []uint64{0, a.h.config.Cost.Facts / 2, a.h.config.Cost.Facts + 16} {
			id := a.h.config.Cost.edge(fact, 5).ID
			row, found, e := l.s.point(ctx, m.Generation.ID, at, id)
			if e != nil {
				return e
			}
			want := graphContentionExpectedValue(a.h.config.Cost, fact, m.Generation.ID == "old")
			if found != (want != 0) || found && (row.Owner != id || row.Value != want) {
				return errMismatch
			}
			if e = l.write(struct {
				Type, Generation string
				Owner            uint64
				Found            bool
				Row              graphColumnRow
			}{"point", m.Generation.ID, id, found, row}, true); e != nil {
				return e
			}
			l.end.Counts.Points++
		}
		return nil
	}
	if err = export(l.held, l.end.Old, l.old); err != nil {
		return err
	}
	current, err := newGraphColumnRead(l.s, "final", nil)
	if err != nil {
		return err
	}
	if err = current.open(ctx); err != nil {
		return errors.Join(err, columnReadCleanup(current))
	}
	err = export(current, l.end.Final, final)
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
		page, e := l.pins.collect(ctx, "old", budget)
		if e != nil || page.Cursor != ordinal+1 || page.WorkChunks != 1 || page.Done != (ordinal+1 == l.end.Old.Chunks) {
			return errors.Join(errMismatch, e)
		}
		key := l.s.chunkKey("old", ordinal)
		result, e := graphCostReadContext(a.ctx, a.h.db, func(tr fdb.ReadTransaction) (any, error) {
			raw, e := tr.Get(key).Get()
			if e != nil {
				return nil, e
			}
			if raw != nil {
				return nil, errMismatch
			}
			guard, found, e := l.s.guard(tr, l.end.Old)
			if e != nil || !found || guard.Cursor != page.Cursor {
				return nil, errors.Join(errMismatch, e)
			}
			guardKey := l.s.generationKey("old", "owners")
			body, e := tr.Get(guardKey).Get()
			if e != nil {
				return nil, e
			}
			return graphCostStoredRecord{bytes.Clone(guardKey), bytes.Clone(body)}, nil
		})
		if e != nil {
			return e
		}
		if e = l.write(struct {
			Type   string
			Budget graphColumnCollectBudget
			Result graphColumnCollectResult
			Absent []byte
			Guard  graphCostStoredRecord
		}{"collect", budget, page, bytes.Clone(key), result.(graphCostStoredRecord)}, true); e != nil {
			return e
		}
		l.end.Counts.Collections++
	}
	refusedPublication, refused := l.s.publish(ctx, "old", l.initial, l.old)
	if !graphColumnSame(refusedPublication, graphColumnManifest{}) {
		return errMismatch
	}
	publishInput := struct {
		Generation     string
		Initial, Final certificate
	}{"old", l.initial.record, l.old.record}
	if err = a.columnRefusal(l, "retired-publication", "old", publishInput, refusedPublication, refused, errColumnExpired); err != nil {
		return err
	}
	refusedCollection, refused = l.pins.collect(ctx, "final", budget)
	if refusedCollection != (graphColumnCollectResult{}) {
		return errMismatch
	}
	collectInput.Generation = "final"
	if err = a.columnRefusal(l, "final-reference-collection", "final", collectInput, refusedCollection, refused, errColumnInUse); err != nil {
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
	if l.end.Counts.Chunks != l.end.Old.Chunks+l.end.Final.Chunks || l.end.Counts.WalkRows != 2*(l.end.Old.Rows+l.end.Final.Rows) || l.end.Counts.Collections != l.end.Old.Chunks || l.end.Ledger.Chunks != l.end.Final.Chunks {
		return errMismatch
	}
	copy(l.end.PayloadSHA256[:], l.payload.Sum(nil))
	if err = l.write(struct {
		Type string
		End  graphCostColumnEnd
	}{"footer", l.end}, false); err != nil {
		return err
	}
	if err = l.Close(); err != nil {
		return err
	}
	a.end.Columns = [2]graphColumnManifest{l.end.Old, l.end.Final}
	return nil
}
func graphContentionExpectedValue(p graphCostProfile, fact uint64, old bool) int64 {
	if fact >= p.Facts || old && fact >= p.Facts/2 {
		return 0
	}
	want := int64(5)
	if old || fact >= p.Facts/2 {
		return want
	}
	for _, probe := range []uint64{0, 9} {
		target, err := graphContentionTarget(p, "hot", probe)
		if err != nil || target != fact {
			continue
		}
		count := p.Facts * (100 - p.CrossPercent) / 100
		if p.cross(probe) {
			count = p.Facts * p.CrossPercent / 100
		}
		// At0/100 both probes designate the same category; account it once.
		return want + int64(count) // #nosec G115 -- admitted Facts<=1M.
	}
	return want
}

// A refusal is the actual production result/error plus its observed native
// authority. Labels alone do not certify expiry, held ownership or collection.
func (a *graphContentionArtifacts) columnRefusal(l *graphCostColumnLane, name, generation string, input, result any, observed, expected error) error {
	if !errors.Is(observed, expected) || errors.Is(observed, errUnknown) {
		return errors.Join(errMismatch, observed)
	}
	if _, pending := errors.AsType[*graphColumnReadPending](observed); pending {
		return observed
	}
	if _, native := errors.AsType[fdb.Error](observed); native {
		return observed
	}
	records, err := l.records([]fdb.Key{l.s.generationKey(generation, "manifest"), l.s.generationKey(generation, "progress"), l.s.generationKey(generation, "owners"),
		l.held.key(), columnPinKey(l.s, l.token.Request), l.pins.recordKey("reference", "head"), l.s.key("ledger"), l.pins.key("ledger"), a.h.key(0, "meta", "state"), a.h.key(1, "meta", "state")})
	if err != nil {
		return err
	}
	return l.write(struct {
		Type, Name, Generation, Error string
		Input, Result                 any
		Records                       []graphCostStoredRecord
	}{"refusal", name, generation, observed.Error(), input, result, records}, true)
}

func (a *graphContentionArtifacts) context() context.Context {
	return graphTransportLegacyContext(a.ctx)
}
