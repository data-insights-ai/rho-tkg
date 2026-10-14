package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphCostRetentionFormat = "fdb-graph-cost-retained-v1"
const graphCostRetentionParallelFormat = "fdb-graph-cost-retained-attempts-v1"
const graphCostRetentionColumnFormat = "fdb-graph-cost-retained-columns-v1"

// The selector creates a separate persisted graph, never widens a recorded
// graph-cost-v1 binding, and is independent of the KV/derived-columns layout.
func graphCostRetentionSelected() (bool, error) {
	switch os.Getenv("RHO_GRAPH_COST_RETENTION") {
	case "", "off":
		return false, nil
	case "source-v1":
		return true, nil
	default:
		return false, errInvalid
	}
}
func graphCostRetentionGraph(layout string, writers int) string {
	graph := "graph-cost-retained-v1"
	if layout == "columns" {
		graph = "graph-cost-retained-columns-v1"
	}
	if writers != 1 {
		graph = "graph-cost-parallel-retained-v1"
		if layout == "columns" {
			graph = "graph-cost-parallel-retained-columns-v1"
		}
	}
	return graph
}
func graphCostRetainedConfig(c config) bool {
	return c.Graph == graphCostRetentionGraph("kv", c.Cost.Writers) || c.Graph == graphCostRetentionGraph("columns", c.Cost.Writers)
}

// A fresh fixture policy, not measured capacity or RSS. One cumulative work
// allowance covers both source census and deletion; it is not reset on resume.
// Named controls/tombstones retain a separate finite reservation ledger.
func graphCostHistoryPolicy(p graphCostProfile) graphHistoryPolicy {
	return graphHistoryPolicy{MaxOwners: 64, MaxControlBytes: 512 << 10,
		PageRows: 128, PageBytes: 4 << 20, MaxWorkRows: 16*p.Facts + 64,
		MaxWorkBytes:  (64 << 20) + (16*p.Facts+64)*(512<<10),
		MaxChainSteps: 8, MaxOutputBytes: 4096}
}

type graphCostRetentionEnd struct {
	Format, PayloadSHA256, ArtifactSHA256                  string
	Binding                                                graphHistoryBinding
	ArtifactPolicy                                         graphCostRetentionArtifactPolicy
	OldCut, FinalCut                                       certificate
	Seal                                                   graphHistorySeal
	Metadata                                               [2]metadata
	OldRows, FinalRows                                     [2]uint64
	OriginalRows                                           [2]uint64
	Pages, Resumes, GuardRefusals, Replays, AbortedReplays uint64
	Sources                                                [3]graphCostStreamEnd
}
type graphCostRetentionArtifactPolicy struct {
	LineBytes                      int
	OriginalPage                   graphCostStreamPolicy
	MaxOriginalRows, MaxReplayRows uint64
}

func graphCostRetentionArtifactsPolicy(c config) graphCostRetentionArtifactPolicy {
	p := graphCostHistoryPolicy(c.Cost)
	return graphCostRetentionArtifactPolicy{512 << 10, graphCostStreamPolicy{p.PageRows, p.PageBytes}, 2 * c.Limits.Transactions, 2 * c.Limits.Transactions}
}

func (e graphCostRetentionEnd) complete(p graphCostProfile) bool {
	if !validGraphCostProfile(e.Binding.Config) || !sameCertificate(e.Seal.Cut, e.FinalCut) || e.OldCut.Round >= e.FinalCut.Round || !validScope(e.OldCut.Scope) || len(e.OldCut.Scope) != 2 || e.OriginalRows != [2]uint64{e.Metadata[0].Transactions, e.Metadata[1].Transactions} || e.OriginalRows[0]+e.OriginalRows[1] > e.ArtifactPolicy.MaxOriginalRows {
		return false
	}
	for _, digest := range []string{e.PayloadSHA256, e.ArtifactSHA256} {
		if len(digest) != 64 {
			return false
		}
		if _, err := hex.DecodeString(digest); err != nil {
			return false
		}
	}
	for g := range 2 {
		before, after := e.Seal.Before[g], e.Metadata[g]
		if before.Versions < e.Seal.Deleted[g] || before.Bytes < e.Seal.DeletedBytes[g] || before.Versions-e.Seal.Deleted[g] != after.Versions || before.Bytes-e.Seal.DeletedBytes[g] != after.Bytes || before.Transactions != after.Transactions || before.Generation != after.Generation || before.Certificates != after.Certificates || before.Groups != after.Groups || before.Floor != after.Floor || after.Groups != e.FinalCut.Groups[g] || after.Floor != e.FinalCut.Round || after.Versions != e.FinalRows[g] || e.OldCut.Groups[g] > e.FinalCut.Groups[g] {
			return false
		}
	}
	for _, source := range e.Sources {
		if source.Metadata != e.Metadata || source.Groups != e.FinalCut.Groups {
			return false
		}
	}
	return e.Format == graphCostRetentionFormat && e.Binding.Config.Cost == p && graphCostRetainedConfig(e.Binding.Config) && e.ArtifactPolicy == graphCostRetentionArtifactsPolicy(e.Binding.Config) && e.Replays <= e.ArtifactPolicy.MaxReplayRows &&
		e.Binding.Policy == graphCostHistoryPolicy(p) && e.Binding.Policy.validate() == nil && e.Binding.Initial.Groups == [2]uint64{} &&
		e.OldRows[0]+e.OldRows[1] == 3+3*p.Facts && e.FinalRows == e.Sources[0].Rows && e.FinalRows[0]+e.FinalRows[1] == 3+3*p.Facts &&
		e.Pages >= 2 && e.Resumes == 1 && e.GuardRefusals == 2 && e.Replays == 3+2*p.Facts+e.AbortedReplays && e.Replays == e.Metadata[0].Transactions+e.Metadata[1].Transactions &&
		e.Seal.Request == "source-final" && sameCertificate(e.Seal.Cut, e.FinalCut) && e.Seal.After == e.Metadata &&
		e.Seal.Deleted[0]+e.Seal.Deleted[1] == 3*p.Facts && e.Seal.Originals[0]+e.Seal.Originals[1] == 3+6*p.Facts &&
		e.Seal.Anchors == e.FinalRows && e.Seal.CumulativeDeleted == e.Seal.Deleted &&
		e.Sources[1].Rows == e.FinalRows && e.Sources[2].Rows == e.FinalCut.Groups && len(e.PayloadSHA256) == 64 && len(e.ArtifactSHA256) == 64
}

type graphCostRetentionLane struct {
	s              *graphHistoryRetention
	root           *os.Root
	file           *os.File
	payload, whole hash.Hash
	held           *graphHistoryRead
	token          graphHistoryToken
	end            graphCostRetentionEnd
	closed         bool
}

func newGraphCostRetentionLane(h *harness, root *os.Root, initial cut) (*graphCostRetentionLane, error) {
	if h == nil || root == nil || !graphCostRetainedConfig(h.config) || !validGraphCostProfile(h.config) {
		return nil, errInvalid
	}
	s, err := newGraphHistoryRetention(h, initial, graphCostHistoryPolicy(h.config.Cost))
	if err != nil {
		return nil, err
	}
	file, _, err := openGraphCostArtifact(root, "fdb-cost-retention.jsonl")
	if err != nil {
		return nil, err
	}
	l := &graphCostRetentionLane{s: s, root: root, file: file, payload: sha256.New(), whole: sha256.New()}
	l.end.Format, l.end.Binding = graphCostRetentionFormat, s.binding
	l.end.ArtifactPolicy = graphCostRetentionArtifactsPolicy(h.config)
	records, err := l.records([]fdb.Key{h.historyKey("binding"), h.historyKey("ledger"), h.historyKey("state"), h.key(0, "meta", "config"), h.key(1, "meta", "config")})
	if err == nil {
		err = l.write(struct {
			Type, Format   string
			Binding        graphHistoryBinding
			ArtifactPolicy graphCostRetentionArtifactPolicy
			Records        []graphCostStoredRecord
		}{"header", graphCostRetentionFormat, s.binding, l.end.ArtifactPolicy, records}, true)
	}
	if err != nil {
		return nil, errors.Join(err, l.Close())
	}
	return l, nil
}
func (l *graphCostRetentionLane) write(v any, payload bool) error {
	if l == nil || l.closed {
		return errInvalid
	}
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(body)+1 > 512<<10 {
		return errLimit
	}
	body = append(body, '\n')
	if _, err = l.file.Write(body); err != nil {
		return err
	}
	_, _ = l.whole.Write(body)
	if payload {
		_, _ = l.payload.Write(body)
	}
	return nil
}
func (l *graphCostRetentionLane) records(keys []fdb.Key) ([]graphCostStoredRecord, error) {
	if len(keys) < 1 || len(keys) > 20 {
		return nil, errInvalid
	}
	result, err := l.s.h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		if _, err := l.s.state(tr); err != nil {
			return nil, err
		}
		out := make([]graphCostStoredRecord, 0, len(keys))
		for _, key := range keys {
			raw, err := tr.Get(key).Get()
			if err != nil {
				return nil, err
			}
			if len(key) > 1024 || len(raw) == 0 || len(raw) > 64<<10 {
				return nil, errUnavailable
			}
			out = append(out, graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)})
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return result.([]graphCostStoredRecord), nil
}
func (l *graphCostRetentionLane) stage(name string, extra ...fdb.Key) error {
	keys := []fdb.Key{l.s.h.historyKey("binding"), l.s.h.historyKey("ledger"), l.s.h.historyKey("state"), l.s.h.key(0, "meta", "state"), l.s.h.key(1, "meta", "state")}
	if l.held != nil {
		keys = append(keys, l.s.ownerKey(l.held.id.Request), l.s.ownerKey(l.token.Request))
	}
	keys = append(keys, extra...)
	records, err := l.records(keys)
	if err != nil {
		return err
	}
	return l.write(struct {
		Type, Name string
		Records    []graphCostStoredRecord
	}{"stage", name, records}, true)
}
func (l *graphCostRetentionLane) refusal(name string, actual, expected error) error {
	if !errors.Is(actual, expected) {
		return errors.Join(errMismatch, actual)
	}
	return l.write(struct{ Type, Name, Error string }{"refusal", name, actual.Error()}, true)
}
func (l *graphCostRetentionLane) holdOld(old cut) error {
	var secret [32]byte
	// The genuine issued secret is stored in the token; no wall-clock expiration.
	if _, err := rand.Read(secret[:]); err != nil {
		return err
	}
	l.token = graphHistoryToken{Request: "source-old", Cut: old.record, Secret: secret}
	if err := l.write(struct {
		Type  string
		Token graphHistoryToken
	}{"pin-intent", l.token}, true); err != nil {
		return err
	}
	token, err := l.s.pin("source-old", old, secret)
	if err != nil {
		return err
	}
	l.token = token
	l.held, err = l.s.open(context.Background(), token)
	if err != nil {
		return err
	}
	l.end.OldCut = old.record
	return l.stage("old-held")
}

// Exact typed fixed-slice validation is independent of counting. The source
// reader authenticates raw originals and every anchor at its own verified EOF.
func graphCostRetentionRow(h *harness, row graphCostRawRow, want int64) error {
	if h == nil || want != 5 && want != 6 {
		return errInvalid
	}
	key, v, err := h.graphHistoryRow(row, ^uint64(0))
	if err != nil {
		return err
	}
	if v.Deleted {
		return errMismatch
	}
	if strings.HasPrefix(key, "node/") {
		for _, node := range h.config.Cost.nodes() {
			if row.Group == node.Owner && key == lifeKey(node) && v.Value == 1 && v.Version == 1 {
				return nil
			}
		}
		return errMismatch
	}
	kind, _, ok := strings.Cut(key, "/")
	if !ok || kind != "edge" && kind != "out" && kind != "in" {
		return errInvalid
	}
	edge, err := parseGraphEdge(kind, key, v.Value, h.config)
	if err != nil {
		return err
	}
	if edge.ID < 7 || (edge.ID-7)%2 != 0 {
		return errMismatch
	}
	fact := (edge.ID - 7) / 2
	group := uint8(0)
	if kind == "in" {
		group = edge.Target.Owner
	}
	version := uint64(1)
	if want == 6 {
		version = 2
	}
	if fact >= h.config.Cost.Facts || row.Group != group || edge != h.config.Cost.edge(fact, want) || v.Version != version {
		return errMismatch
	}
	return nil
}
func (l *graphCostRetentionLane) oldWalk() error {
	rows, err := l.held.walk(context.Background(), func(row graphCostRawRow) error {
		if err := graphCostRetentionRow(l.s.h, row, 5); err != nil {
			return err
		}
		return l.write(struct {
			Type, Name string
			Row        graphCostRawRow
		}{"row", "old-held", row}, true)
	})
	if err != nil {
		return err
	}
	if rows[0]+rows[1] != 3+3*l.s.h.config.Cost.Facts {
		return errMismatch
	}
	l.end.OldRows = rows
	return l.write(struct {
		Type, Name string
		Cut        certificate
		Rows       [2]uint64
	}{"read-eof", "old-held", l.end.OldCut, rows}, true)
}

type graphCostRetentionCleanupPending struct {
	Lane  *graphCostRetentionLane
	Cause error
}

func (e *graphCostRetentionCleanupPending) Error() string {
	return "fdb-cost: source retention ownership cleanup pending"
}
func (e *graphCostRetentionCleanupPending) Unwrap() error { return e.Cause }
func (l *graphCostRetentionLane) Close() error {
	if l == nil {
		return nil
	}
	var err error
	if l.held != nil {
		err = historyReadCleanup(l.held)
	}
	if l.token.Request != "" {
		err = errors.Join(err, l.s.expire("source-old-expire", l.token))
	}
	if !l.closed {
		err = errors.Join(err, l.file.Close())
		l.closed = true
	}
	if err != nil {
		return &graphCostRetentionCleanupPending{l, err}
	}
	return nil
}

type graphCostRetentionObservation struct {
	Metadata  [2]metadata
	State     graphHistoryState
	Records   []graphCostStoredRecord
	Originals [2]graphCostStoredRecord
}

func (l *graphCostRetentionLane) observe(x *transaction) (graphCostRetentionObservation, error) {
	result, err := l.s.h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		var out graphCostRetentionObservation
		state, err := l.s.state(tr)
		if err != nil {
			return nil, err
		}
		out.State = state
		for g := uint8(0); g < 2; g++ {
			if err := l.s.h.checkConfig(tr, g); err != nil {
				return nil, err
			}
			m, err := l.s.h.meta(tr, g)
			if err != nil {
				return nil, err
			}
			out.Metadata[g] = m
			key := l.s.h.key(g, "meta", "state")
			raw, err := tr.Get(key).Get()
			if err != nil {
				return nil, err
			}
			out.Records = append(out.Records, graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)})
		}
		key := l.s.h.historyKey("state")
		raw, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		out.Records = append(out.Records, graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)})
		if x != nil {
			for i, key := range []fdb.Key{l.s.h.key(x.Coordinator, "id", x.ID), l.s.h.key(x.Coordinator, "request", x.Request)} {
				raw, err := tr.Get(key).Get()
				if err != nil {
					return nil, err
				}
				if raw != nil {
					return nil, errMismatch
				}
				out.Originals[i] = graphCostStoredRecord{bytes.Clone(key), nil}
			}
		}
		return out, nil
	})
	if err != nil {
		return graphCostRetentionObservation{}, err
	}
	return result.(graphCostRetentionObservation), nil
}

// A separately named non-business diagnostic, never a useful operation or ABORT.
// Only two genuine native submissions are admitted; both use the SAME frozen
// bytes and both original absences remain observations, not global authority.
func (l *graphCostRetentionLane) busyDiagnostic() error {
	p := l.s.h.config.Cost
	command := graphCommand{Kind: "edge-update", Edge: p.edge(0, 6)}
	before, _, err := l.s.h.graphCostPointSnapshot(command)
	if err != nil {
		return err
	}
	x := l.s.h.graphTransaction("source-retirement-diagnostic", 0, command, before)
	x.Lifecycle = &lifecycleMetadata{Provenance: 4 + p.Facts, EdgeLife: command.Edge.ID + 1, Definition: &lifecycleDefinition{ID: 6, Kind: "i64", I64: 6}}
	body, err := wire(x)
	if err != nil {
		return err
	}
	if err := l.s.h.validate(x); err != nil {
		return err
	}
	for ordinal := 1; ordinal <= 2; ordinal++ {
		prior, err := l.observe(&x)
		if err != nil {
			return err
		}
		if prior.State.Phase != historyCensus && prior.State.Phase != historyDeleting {
			return errMismatch
		}
		submitted := l.s.h.graphCostParallelSubmit(context.Background(), x, body)
		submitted.Ordinal = ordinal
		if !errors.Is(submitted.cause, errHistoryBusy) || submitted.Retryable || submitted.MaybeCommitted || submitted.Outcome != (outcome{}) || submitted.Callbacks < 1 || submitted.Callbacks > graphCostParallelCallbacks {
			return errors.Join(errMismatch, submitted.cause)
		}
		var nativeCode *int
		if native, ok := errors.AsType[fdb.Error](submitted.cause); ok {
			nativeCode = new(native.Code)
		}
		if nativeCode != nil {
			return errors.Join(errMismatch, submitted.cause)
		}
		after, err := l.observe(&x)
		if err != nil {
			return err
		}
		if !graphColumnSame(prior, after) {
			return errMismatch
		}
		if err := l.write(struct {
			Type, Classification string
			Diagnostic           bool
			Ordinal              int
			Body                 []byte
			BodySHA256           [32]byte
			Submission           graphCostParallelSubmission
			NativeCode           *int `json:",omitempty"`
			Before, After        graphCostRetentionObservation
		}{"guard-submission", "retirement-busy", true, ordinal, body, sha256.Sum256(body), submitted, nativeCode, prior, after}, true); err != nil {
			return err
		}
		l.end.GuardRefusals++
	}
	return nil
}
func (l *graphCostRetentionLane) collect(final cut) error {
	if err := l.s.begin("source-final", final); err != nil {
		return err
	}
	if err := l.stage("retirement-admitted", l.s.h.historyKey("intent/source-final")); err != nil {
		return err
	}
	if err := l.busyDiagnostic(); err != nil {
		return err
	}
	budget := graphHistoryBudget{Rows: l.s.binding.Policy.PageRows, Bytes: uint64(l.s.binding.Policy.PageBytes), OutputBytes: l.s.binding.Policy.MaxOutputBytes}
	for {
		page, err := l.s.collect(context.Background(), "source-final", budget)
		if err != nil {
			return err
		}
		observed, err := l.observe(nil)
		if err != nil {
			return err
		}
		if observed.State.Request != "source-final" || !sameCertificate(observed.State.Target, final.record) {
			return errMismatch
		}
		if err := l.write(struct {
			Type        string
			Budget      graphHistoryBudget
			Page        graphHistoryPage
			Observation graphCostRetentionObservation
		}{"collect", budget, page, observed}, true); err != nil {
			return err
		}
		l.end.Pages++
		if l.end.Resumes == 0 && page.Deleted[0]+page.Deleted[1] > 0 {
			if page.Done {
				return errMismatch
			}
			// Reconstruct the helper from genuine persisted binding/state. No
			// claim of a physical server/client restart or native process reopen.
			reopened, err := newGraphHistoryRetention(l.s.h, cut{l.s.binding.Initial}, l.s.binding.Policy)
			if err != nil {
				return err
			}
			l.s = reopened
			if err := l.s.begin("source-final", final); err != nil {
				return err
			}
			again, err := l.observe(nil)
			if err != nil {
				return err
			}
			if !graphColumnSame(again, observed) {
				return errMismatch
			}
			if err := l.write(struct {
				Type, Request string
				Before, After graphCostRetentionObservation
			}{"durable-resume", "source-final", observed, again}, true); err != nil {
				return err
			}
			l.end.Resumes++
		}
		if page.Done {
			if observed.State.Phase != historySealed || !graphColumnSame(observed.State.Completed, page.Seal) || observed.Metadata != page.Seal.After {
				return errMismatch
			}
			l.end.Seal, l.end.Metadata = page.Seal, observed.Metadata
			return l.stage("retirement-complete", l.s.h.historyKey("intent/source-final"), l.s.h.historyKey("seal/source-final"))
		}
	}
}
func (l *graphCostRetentionLane) ownedHistory(final cut, emit func(graphCostRawRow) error) (end graphCostStreamEnd, err error) {
	owner, err := l.s.newRead(final, nil)
	if err != nil {
		return end, err
	}
	_, err = l.s.h.db.Transact(func(tr fdb.Transaction) (any, error) { return nil, l.s.acquireIn(tr, owner.id) })
	if err != nil {
		err = graphHistoryCommitError(err)
		if errors.Is(err, errUnknown) {
			err = errors.Join(err, historyReadCleanup(owner))
		}
		return end, err
	}
	defer func() {
		err = errors.Join(err, historyReadCleanup(owner))
		if err != nil {
			end = graphCostStreamEnd{}
		}
	}()
	if err := l.stage("final-read-open", l.s.ownerKey(owner.id.Request)); err != nil {
		return end, err
	}
	rows, err := owner.walk(context.Background(), func(row graphCostRawRow) error {
		if err := graphCostRetentionRow(l.s.h, row, 6); err != nil {
			return err
		}
		return emit(row)
	})
	if err != nil {
		return end, err
	}
	observed, err := l.observe(nil)
	if err != nil {
		return end, err
	}
	if observed.Metadata != l.end.Metadata || !graphColumnSame(observed.State.Completed, l.end.Seal) {
		return end, errMismatch
	}
	if err := historyReadCleanup(owner); err != nil {
		return end, err
	}
	if err := l.stage("final-read-closed", l.s.ownerKey(owner.id.Request)); err != nil {
		return end, err
	}
	return graphCostStreamEnd{Rows: rows, Groups: final.record.Groups, Metadata: observed.Metadata}, nil
}

// Original replay uses closed real operation bytes, bounded before decoding.
// It reuses original IDs/request/body and proves no new metadata or CDC effects.
func (l *graphCostRetentionLane) replayOriginals() (err error) {
	file, err := l.root.Open("fdb-cost-operation-groups.jsonl")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), (64<<10)+1)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) > 64<<10 {
			return errLimit
		}
		group, err := decode[changeGroup](line)
		if err != nil {
			return err
		}
		if err := l.s.h.validateChange(group); err != nil {
			return err
		}
		body, err := wire(group.Tx)
		if err != nil {
			return err
		}
		before, err := l.observe(nil)
		if err != nil {
			return err
		}
		submitted := l.s.h.graphCostParallelSubmit(context.Background(), group.Tx, body)
		submitted.Ordinal = 1
		if submitted.cause != nil || submitted.Outcome != group.Outcome {
			return errors.Join(errMismatch, submitted.cause)
		}
		proof, _, err := l.s.h.graphCostReadParallelOriginal(context.Background(), group.Tx, body)
		if err != nil {
			return err
		}
		if !graphColumnSame(proof.Group, group) {
			return errMismatch
		}
		after, err := l.observe(nil)
		if err != nil {
			return err
		}
		if !graphColumnSame(before, after) {
			return errMismatch
		}
		if err := l.write(struct {
			Type          string
			Body          []byte
			BodySHA256    [32]byte
			Submission    graphCostParallelSubmission
			Proof         graphCostParallelProof
			Before, After graphCostRetentionObservation
		}{"original-replay", body, sha256.Sum256(body), submitted, proof, before, after}, true); err != nil {
			return err
		}
		l.end.Replays++
		if l.end.Replays > 3+2*l.s.h.config.Cost.Facts {
			return errLimit
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if l.end.Replays != 3+2*l.s.h.config.Cost.Facts {
		return errMismatch
	}
	return nil
}

func (l *graphCostRetentionLane) header(final cut) (graphCostArtifactHeader, error) {
	policy := graphCostStreamPolicy{l.s.binding.Policy.PageRows, l.s.binding.Policy.PageBytes}
	result, err := l.s.h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		state, err := l.s.state(tr)
		if err != nil {
			return nil, err
		}
		if state.Phase != historySealed || state.Owners != 0 || !graphColumnSame(state.Completed, l.end.Seal) {
			return nil, errMismatch
		}
		header := graphCostArtifactHeader{Type: "header", Format: graphCostRetentionFormat, Config: l.s.h.config, InitialCut: l.s.binding.Initial, OldCut: l.end.OldCut, FinalCut: final.record, Policy: policy, Metadata: l.end.Metadata}
		stored := func(key fdb.Key, decoded any) (graphCostStoredRecord, error) {
			raw, err := tr.Get(key).Get()
			if err != nil {
				return graphCostStoredRecord{}, err
			}
			body, err := wire(decoded)
			if err != nil || !bytes.Equal(raw, body) {
				return graphCostStoredRecord{}, errors.Join(errMismatch, err)
			}
			return graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)}, nil
		}
		for g := uint8(0); g < 2; g++ {
			if err := l.s.h.checkConfig(tr, g); err != nil {
				return nil, err
			}
			m, err := l.s.h.meta(tr, g)
			if err != nil || m != l.end.Metadata[g] {
				return nil, errors.Join(errMismatch, err)
			}
			header.ConfigRecords[g], err = stored(l.s.h.key(g, "meta", "config"), header.Config)
			if err != nil {
				return nil, err
			}
			header.MetadataRecords[g], err = stored(l.s.h.key(g, "meta", "state"), m)
			if err != nil {
				return nil, err
			}
		}
		for i, c := range []certificate{header.InitialCut, header.OldCut, header.FinalCut} {
			if err := l.s.h.verifyCutIdentity(tr, cut{c}); err != nil {
				return nil, err
			}
			body, err := wire(c)
			if err != nil {
				return nil, err
			}
			header.CutRecords[i], err = stored(l.s.h.key(c.Scope[0], "cut", hex.EncodeToString(body)), c)
			if err != nil {
				return nil, err
			}
		}
		return header, nil
	})
	if err != nil {
		return graphCostArtifactHeader{}, err
	}
	return result.(graphCostArtifactHeader), nil
}
func writeGraphCostRetainedRaw(root *os.Root, header graphCostArtifactHeader, walk func(func(graphCostRawRow) error) (graphCostStreamEnd, error)) (end graphCostStreamEnd, err error) {
	if root == nil || walk == nil || header.Type != "header" || header.Format != graphCostRetentionFormat || !graphCostRetainedConfig(header.Config) || !validGraphCostProfile(header.Config) || header.Policy.validate() != nil || header.Stream != "history" && header.Stream != "current" && header.Stream != "cdc" {
		return end, errInvalid
	}
	file, _, err := openGraphCostArtifact(root, "fdb-cost-"+header.Stream+".jsonl")
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
	if err := json.NewEncoder(writer).Encode(header); err != nil {
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
		if _, err := writer.Write(append(body, '\n')); err != nil {
			return err
		}
		counts[row.Group]++
		return nil
	})
	if err != nil {
		return end, err
	}
	if verified.Rows != counts || verified.Groups != header.FinalCut.Groups || verified.Metadata != header.Metadata {
		return end, errMismatch
	}
	footer := graphCostArtifactFooter{"footer", verified.Rows, verified.Groups, verified.Metadata, hex.EncodeToString(payload.Sum(nil))}
	if err := json.NewEncoder(file).Encode(footer); err != nil {
		return end, err
	}
	return verified, nil
}
func (l *graphCostRetentionLane) sources(final cut) ([3]graphCostStreamEnd, error) {
	var ends [3]graphCostStreamEnd
	header, err := l.header(final)
	if err != nil {
		return ends, err
	}
	policy := header.Policy
	for i, stream := range []struct {
		name string
		walk func(func(graphCostRawRow) error) (graphCostStreamEnd, error)
	}{
		{"history", func(emit func(graphCostRawRow) error) (graphCostStreamEnd, error) { return l.ownedHistory(final, emit) }},
		{"current", func(emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
			return l.s.h.graphCostWalkCurrent(final, policy, emit)
		}},
		{"cdc", func(emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
			return l.s.h.graphCostWalkChanges(cut{l.s.binding.Initial}, final, policy, emit)
		}},
	} {
		header.Stream = stream.name
		ends[i], err = writeGraphCostRetainedRaw(l.root, header, stream.walk)
		if err != nil {
			return [3]graphCostStreamEnd{}, err
		}
	}
	return ends, nil
}
func (l *graphCostRetentionLane) dropColumnReference(columns *graphCostColumnLane) error {
	if columns == nil {
		return nil
	}
	before, err := l.observe(nil)
	if err != nil {
		return err
	}
	prior := columns.head
	next, err := columns.pins.reference(context.Background(), "head-source-retirement", "head", "", prior.Version)
	if err != nil {
		return err
	}
	if next.Generation != "" || next.Version != prior.Version+1 {
		return errMismatch
	}
	columns.head = next
	after, err := l.observe(nil)
	if err != nil {
		return err
	}
	if before.Metadata != after.Metadata || before.State.Owners == 0 || after.State.Owners+1 != before.State.Owners {
		return errMismatch
	}
	refKey := columns.pins.recordKey("reference", "head")
	owner := historyColumnRequest("reference", fmt.Sprintf("%s/%d", prior.Name, prior.Version))
	records, err := l.records([]fdb.Key{refKey, columns.pins.recordKey("reference-request", "head-source-retirement"), columns.s.generationKey("final", "owners"), columns.pins.key("ledger"), l.s.ownerKey(owner)})
	if err != nil {
		return err
	}
	ref, err := decode[graphColumnReference](records[0].Value)
	if err != nil || !graphColumnSame(ref, next) {
		return errors.Join(errMismatch, err)
	}
	return l.write(struct {
		Type          string
		Before, After graphCostRetentionObservation
		Reference     graphColumnReference
		Records       []graphCostStoredRecord
	}{"column-reference-release", before, after, next, records}, true)
}
func (l *graphCostRetentionLane) finish(final cut, columns *graphCostColumnLane) (end graphCostRetentionEnd, err error) {
	if l == nil || l.held == nil || l.closed {
		return end, errInvalid
	}
	l.end.FinalCut = final.record
	if err := l.s.expire("source-old-expire", l.token); err != nil {
		return end, err
	}
	_, refused := l.s.open(context.Background(), l.token)
	if err := l.refusal("expired-new-open", refused, errHistoryExpired); err != nil {
		return end, err
	}
	if err := l.refusal("held-source-retirement", l.s.begin("source-final", final), errHistoryInUse); err != nil {
		return end, err
	}
	if err := l.stage("expired-held"); err != nil {
		return end, err
	}
	if err := l.oldWalk(); err != nil {
		return end, err
	}
	if err := historyReadCleanup(l.held); err != nil {
		return end, err
	}
	if err := l.stage("old-closed"); err != nil {
		return end, err
	}
	if err := l.dropColumnReference(columns); err != nil {
		return end, err
	}
	if err := l.collect(final); err != nil {
		return end, err
	}
	// A below-floor cut is expired independently of retained certificate/original
	// lineage. No empty/current fallback or whole-view materialization is admitted.
	if _, refused = l.s.newRead(cut{l.end.OldCut}, nil); !errors.Is(refused, errHistoryExpired) {
		return end, errors.Join(errMismatch, refused)
	}
	if err := l.refusal("below-floor-new-read", refused, errHistoryExpired); err != nil {
		return end, err
	}
	if err := l.replayOriginals(); err != nil {
		return end, err
	}
	if err := l.replayAborts(); err != nil {
		return end, err
	}
	if err := l.retainedOriginals(); err != nil {
		return end, err
	}
	l.end.Sources, err = l.sources(final)
	if err != nil {
		return end, err
	}
	l.end.FinalRows = l.end.Sources[0].Rows
	if err := l.stage("terminal", l.s.h.historyKey("seal/source-final")); err != nil {
		return end, err
	}
	l.end.PayloadSHA256 = hex.EncodeToString(l.payload.Sum(nil))
	// The artifact hash is published only after its exclusive file closes.
	candidate := l.end
	candidate.ArtifactSHA256 = strings.Repeat("0", 64)
	if !candidate.complete(l.s.h.config.Cost) {
		return end, errMismatch
	}
	if err := l.write(struct {
		Type string
		End  graphCostRetentionEnd
	}{"footer", l.end}, false); err != nil {
		return end, err
	}
	l.end.ArtifactSHA256 = hex.EncodeToString(l.whole.Sum(nil))
	if err := l.Close(); err != nil {
		return end, err
	}
	if !l.end.complete(l.s.h.config.Cost) {
		return end, errMismatch
	}
	return l.end, nil
}

func (l *graphCostRetentionLane) replayAborts() (err error) {
	if l.s.h.config.Cost.Writers == 1 {
		return nil
	}
	file, err := l.root.Open("fdb-cost-attempts.jsonl")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), (512<<10)+1)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) > 512<<10 {
			return errLimit
		}
		var tag struct{ Type string }
		if err := json.Unmarshal(line, &tag); err != nil {
			return err
		}
		if tag.Type != "attempt" {
			continue
		}
		var frame struct {
			Type    string
			Attempt graphCostParallelAttempt
		}
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&frame); err != nil {
			return err
		}
		a := frame.Attempt
		if a.Classification != "aborted" {
			continue
		}
		if len(a.Proposal) == 0 || len(a.Proposal) > 64<<10 || a.BodySHA256 != sha256.Sum256(a.Proposal) {
			return errMismatch
		}
		x, err := decode[transaction](a.Proposal)
		if err != nil {
			return err
		}
		o, err := l.s.h.graphCostParallelOriginal(x, a.Proposal, a.Originals)
		if err != nil || o.Commit || a.Request != x.Request || a.Transaction != x.ID {
			return errors.Join(errMismatch, err)
		}
		before, err := l.observe(nil)
		if err != nil {
			return err
		}
		submitted := l.s.h.graphCostParallelSubmit(context.Background(), x, a.Proposal)
		submitted.Ordinal = 1
		if submitted.cause != nil || submitted.Outcome != o {
			return errors.Join(errMismatch, submitted.cause)
		}
		proof, _, err := l.s.h.graphCostReadParallelOriginal(context.Background(), x, a.Proposal)
		if err != nil {
			return err
		}
		if proof.Outcome != o || !graphColumnSame(proof.Pair, a.Originals) || len(proof.Original.Value) != 0 {
			return errMismatch
		}
		after, err := l.observe(nil)
		if err != nil {
			return err
		}
		if !graphColumnSame(before, after) {
			return errMismatch
		}
		if err := l.write(struct {
			Type          string
			Body          []byte
			BodySHA256    [32]byte
			Submission    graphCostParallelSubmission
			Proof         graphCostParallelProof
			Before, After graphCostRetentionObservation
		}{"aborted-original-replay", a.Proposal, a.BodySHA256, submitted, proof, before, after}, true); err != nil {
			return err
		}
		l.end.Replays++
		l.end.AbortedReplays++
		if l.end.Replays > l.end.Metadata[0].Transactions+l.end.Metadata[1].Transactions {
			return errLimit
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if l.end.Replays != l.end.Metadata[0].Transactions+l.end.Metadata[1].Transactions {
		return errMismatch
	}
	return nil
}

// Enumerate genuine retained ID originals once in canonical native-key order,
// including outcome-only ABORTs. Paired requests and committed CDC are read back
// in the SAME native transaction. The verifier joins this unique inventory to
// replay/attempt frames; an external replay count alone grants no completeness.
func (l *graphCostRetentionLane) retainedOriginals() error {
	var counts [2]uint64
	for g := uint8(0); g < 2; g++ {
		range_ := prefixRange(l.s.h.prefix(g) + "id/")
		begin, end := range_.Begin.(fdb.Key), range_.End.(fdb.Key)
		for {
			result, err := l.s.h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
				state, err := l.s.state(tr)
				if err != nil {
					return nil, err
				}
				m, err := l.s.h.meta(tr, g)
				if err != nil || state.Phase != historySealed || m != l.end.Metadata[g] {
					return nil, errors.Join(errMismatch, err)
				}
				rows, err := tr.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Limit: l.s.binding.Policy.PageRows}).GetSliceWithError()
				if err != nil {
					return nil, err
				}
				out := make([]graphCostParallelProof, 0, len(rows))
				used := uint64(0)
				var previous []byte
				for _, row := range rows {
					used += uint64(len(row.Key) + len(row.Value))
					if used > uint64(l.s.binding.Policy.PageBytes) {
						return nil, errHistoryBudget
					}
					o, err := decode[outcome](row.Value)
					if err != nil {
						return nil, err
					}
					if o.Coordinator != g || !validName(o.ID) || !validName(o.Request) || o.Digest == [32]byte{} || !bytes.Equal(row.Key, l.s.h.key(g, "id", o.ID)) || bytes.Compare(row.Key, begin) < 0 || bytes.Compare(row.Key, end) >= 0 || previous != nil && bytes.Compare(previous, row.Key) >= 0 {
						return nil, errMismatch
					}
					requestKey := l.s.h.key(g, "request", o.Request)
					request, err := tr.Get(requestKey).Get()
					if err != nil {
						return nil, err
					}
					used += uint64(len(requestKey) + len(request))
					if !bytes.Equal(request, row.Value) {
						return nil, errMismatch
					}
					proof := graphCostParallelProof{Outcome: o, Pair: [2]graphCostStoredRecord{{bytes.Clone(row.Key), bytes.Clone(row.Value)}, {bytes.Clone(requestKey), bytes.Clone(request)}}}
					changeKey := l.s.h.changeKey(g, o.Sequence, o.Request)
					raw, err := tr.Get(changeKey).Get()
					if err != nil {
						return nil, err
					}
					used += uint64(len(changeKey) + len(raw))
					if used > uint64(l.s.binding.Policy.PageBytes) {
						return nil, errHistoryBudget
					}
					if o.Commit {
						group, err := decode[changeGroup](raw)
						if err != nil {
							return nil, err
						}
						if err := l.s.h.validateChange(group); err != nil {
							return nil, err
						}
						if group.Outcome != o {
							return nil, errMismatch
						}
						proof.Group, proof.Original = group, graphCostStoredRecord{bytes.Clone(changeKey), bytes.Clone(raw)}
					} else {
						if o.Round != 0 || o.Sequence != 0 || raw != nil || o.Reason != "generation" && o.Reason != "revision" && o.Reason != "endpoint-life" && o.Reason != "edge-identity" && o.Reason != "graph-shape" {
							return nil, errMismatch
						}
						proof.Original = graphCostStoredRecord{bytes.Clone(changeKey), nil}
					}
					out = append(out, proof)
					previous = row.Key
				}
				return out, nil
			})
			if err != nil {
				return err
			}
			page := result.([]graphCostParallelProof)
			for _, proof := range page {
				counts[g]++
				if counts[g] > l.end.Metadata[g].Transactions {
					return errLimit
				}
				if err := l.write(struct {
					Type  string
					Group uint8
					Proof graphCostParallelProof
				}{"retained-original", g, proof}, true); err != nil {
					return err
				}
			}
			if len(page) < l.s.binding.Policy.PageRows {
				break
			}
			begin = fdb.Key(append(bytes.Clone(page[len(page)-1].Pair[0].Key), 0))
		}
		if counts[g] != l.end.Metadata[g].Transactions {
			return errMismatch
		}
	}
	observed, err := l.observe(nil)
	if err != nil {
		return err
	}
	if observed.Metadata != l.end.Metadata {
		return errMismatch
	}
	l.end.OriginalRows = counts
	return l.write(struct {
		Type        string
		Rows        [2]uint64
		Observation graphCostRetentionObservation
	}{"originals-eof", counts, observed}, true)
}
