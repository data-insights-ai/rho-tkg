package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const historyFaultFormat = "fdb-source-history-faults-v1"
const historyFaultPages = 32

type historyFaultObservation struct {
	State            graphHistoryState
	Metadata         [2]metadata
	Records          []graphCostStoredRecord
	History, Current []graphCostRawRow
}
type historyFaultStage struct {
	Name        string
	Observation historyFaultObservation
	Rows        []graphCostRawRow
	EOF         [2]uint64
}
type historyFaultPage struct {
	Budget        graphHistoryBudget
	Result        graphHistoryPage
	Before, After historyFaultObservation
}
type historyFaultState struct {
	Version                uint8
	Format                 string
	Coordinator            uint8
	Config                 config
	Binding                graphHistoryBinding
	Initial, Old, Final    certificate
	Token                  graphHistoryToken
	Owner                  graphHistoryOwner
	Originals              []columnFaultOriginal
	Pending                *columnFaultOriginal  `json:",omitempty"`
	Failure                *graphPlatformAttempt `json:",omitempty"`
	Views                  []columnFaultView
	Stages                 []historyFaultStage
	Pages                  []historyFaultPage
	BeforeSources, Sources [3]graphCostStreamEnd
	GuardRefusals          []graphPlatformAttempt
	Diagnostic             []byte
	Replays                []graphPlatformAttempt
}

func historyFaultSettings() (config, uint8, error) {
	graph, coord := os.Getenv("FDB_HISTORY_FAULT_GRAPH"), os.Getenv("FDB_HISTORY_FAULT_COORDINATOR")
	if !validName(graph) || !strings.HasPrefix(graph, "source-history-fault-v1-") || coord != "A" && coord != "B" {
		return config{}, 0, errInvalid
	}
	return graphCostConfig(graph, graphCostProfile{Version: 1, Facts: 1, CrossPercent: 100, Writers: 1}), uint8(strings.Index("AB", coord)), nil // #nosec G115 -- exact A/B.
}
func historyFaultPolicy() graphHistoryPolicy {
	// Fixed finite fault fixture limits, not capacity or native-client RSS bounds.
	return graphHistoryPolicy{MaxOwners: 16, MaxControlBytes: 128 << 10, PageRows: 1, PageBytes: 128 << 10, MaxWorkRows: 1024, MaxWorkBytes: 8 << 20, MaxChainSteps: 8, MaxOutputBytes: 4096}
}
func historyFaultSave(root *os.Root, r historyFaultState) (err error) {
	body, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(body) > graphPlatformBytes {
		return errLimit
	}
	file, err := root.OpenFile("state.tmp", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(body)
	if err = errors.Join(writeErr, file.Close()); err != nil {
		return errors.Join(err, root.Remove("state.tmp"))
	}
	return root.Rename("state.tmp", "state.json")
}
func historyFaultLoad(root *os.Root) (r historyFaultState, err error) {
	file, err := root.Open("state.json")
	if err != nil {
		return r, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	body, err := io.ReadAll(io.LimitReader(file, graphPlatformBytes+1))
	if err != nil {
		return r, err
	}
	if len(body) == 0 || len(body) > graphPlatformBytes {
		return r, errLimit
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err = d.Decode(&r); err != nil {
		return r, err
	}
	canonical, err := json.Marshal(r)
	if err != nil || !bytes.Equal(canonical, body) {
		return r, errInvalid
	}
	c, coord, err := historyFaultSettings()
	if err != nil || r.Version != 1 || r.Format != historyFaultFormat || r.Config != c || r.Coordinator != coord || r.Binding.Config != c || r.Binding.Policy != historyFaultPolicy() || !sameCertificate(r.Binding.Initial, r.Initial) || len(r.Originals) < 4 || len(r.Originals) > 5 || len(r.Stages) > 12 || len(r.Pages) > historyFaultPages || r.Pending != nil || r.Failure != nil {
		return r, errInvalid
	}
	return r, nil
}
func historyFaultOwner(s *graphHistoryRetention, r historyFaultState) (*graphHistoryRead, error) {
	body, err := wire(r.Token)
	if s == nil || err != nil || !historyOwnerValid(r.Owner) || r.Owner.Kind != "read" || r.Owner.State != historyActive || !sameCertificate(r.Owner.Cut, r.Old) || !sameCertificate(r.Token.Cut, r.Old) || r.Owner.PinRequest != r.Token.Request || r.Owner.PinDigest != sha256.Sum256(body) || !sameCertificate(r.Owner.Required, r.Initial) {
		return nil, errInvalid
	}
	// This local reconstruction writes nothing. Native check/walk/Close must
	// authenticate this exact previously issued identity, including Required.
	return &graphHistoryRead{s: s, id: r.Owner}, nil
}

// Observation reads have their own fixed nine-history/six-current/128KiB
// control admission, outside the collector logical-work ledger. Post-SDK byte
// checks and the 2MiB artifact cap are not a native-client RSS or power-loss bound.
func historyFaultObserve(s *graphHistoryRetention, r historyFaultState) (historyFaultObservation, error) {
	got, err := s.h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		var out historyFaultObservation
		state, err := s.state(tr)
		if err != nil {
			return nil, err
		}
		out.State = state
		keys := []fdb.Key{s.h.historyKey("binding"), s.h.historyKey("ledger"), s.h.historyKey("state"), s.ownerKey(r.Token.Request), s.ownerKey(r.Owner.Request), s.h.historyKey("intent/process-final"), s.h.historyKey("seal/process-final")}
		for g := uint8(0); g < 2; g++ {
			if err := s.h.checkConfig(tr, g); err != nil {
				return nil, err
			}
			out.Metadata[g], err = s.h.meta(tr, g)
			if err != nil {
				return nil, err
			}
			keys = append(keys, s.h.key(g, "meta", "state"), s.h.key(g, "meta", "config"))
			for _, kind := range []string{"history", "current"} {
				raw, err := tr.GetRange(prefixRange(s.h.prefix(g)+kind+"/"), fdb.RangeOptions{Limit: 10}).GetSliceWithError()
				if err != nil {
					return nil, err
				}
				if len(raw) > 9 {
					return nil, errLimit
				}
				for _, row := range raw {
					if len(row.Key) > 1024 || len(row.Value) == 0 || len(row.Value) > 64<<10 {
						return nil, errLimit
					}
					item := graphCostRawRow{Group: g, Key: bytes.Clone(row.Key), Value: bytes.Clone(row.Value)}
					if err := s.h.graphCostValidateRaw(item, kind == "history", ^uint64(0)); err != nil {
						return nil, err
					}
					if kind == "history" {
						out.History = append(out.History, item)
					} else {
						out.Current = append(out.Current, item)
					}
				}
			}
		}
		if len(out.History) > 9 || len(out.Current) > 6 {
			return nil, errLimit
		}
		for _, c := range []certificate{r.Initial, r.Old, r.Final} {
			if c.Graph != "" {
				if err := s.h.verifyCutIdentity(tr, cut{c}); err != nil {
					return nil, err
				}
				body, err := wire(c)
				if err != nil {
					return nil, err
				}
				keys = append(keys, s.h.key(0, "cut", hex.EncodeToString(body)))
			}
		}
		used := 0
		for _, key := range keys {
			raw, err := tr.Get(key).Get()
			if err != nil {
				return nil, err
			}
			used += len(key) + len(raw)
			if len(raw) > 64<<10 || used > 128<<10 {
				return nil, errLimit
			}
			out.Records = append(out.Records, graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)})
		}
		return out, nil
	})
	if err != nil {
		return historyFaultObservation{}, err
	}
	return got.(historyFaultObservation), nil
}
func historyFaultStageAt(s *graphHistoryRetention, r *historyFaultState, name string, owner *graphHistoryRead) error {
	if len(r.Stages) >= 12 {
		return errLimit
	}
	stage := historyFaultStage{Name: name}
	if owner != nil {
		counts, err := owner.walk(context.Background(), func(row graphCostRawRow) error {
			if len(stage.Rows) >= 6 {
				return errLimit
			}
			if err := graphCostRetentionRow(s.h, row, 5); err != nil {
				return err
			}
			stage.Rows = append(stage.Rows, row)
			return nil
		})
		if err != nil {
			return err
		}
		if counts[0]+counts[1] != 6 {
			return errMismatch
		}
		stage.EOF = counts
	}
	var err error
	stage.Observation, err = historyFaultObserve(s, *r)
	if err != nil {
		return err
	}
	r.Stages = append(r.Stages, stage)
	return nil
}
func historyFaultApply(root *os.Root, h *harness, r *historyFaultState, phase string, i uint64) error {
	// columnFaultApply persists a different file grammar, so submit/capture here
	// while retaining the shared production transaction factory and original read.
	command := graphCommand{Kind: "edge-" + phase, Edge: h.config.Cost.edge(i, 5)}
	provenance := 4 + i
	coordinator := r.Coordinator
	if phase == "setup" {
		command = graphCommand{Kind: "node-create", Node: h.config.Cost.nodes()[i]}
		provenance = i + 1
		coordinator = command.Node.Owner
	}
	if phase == "update" {
		command.Edge.Value = 6
		provenance = 5 + i
	}
	before, _, err := h.graphCostPointSnapshot(command)
	if err != nil {
		return err
	}
	x := h.graphTransaction(fmt.Sprintf("history-process-%s-%d", phase, i), coordinator, command, before)
	x.Lifecycle = &lifecycleMetadata{Provenance: provenance}
	if phase != "setup" {
		x.Lifecycle.EdgeLife = command.Edge.ID + 1
		x.Lifecycle.Definition = &lifecycleDefinition{ID: uint64(command.Edge.Value), Kind: "i64", I64: command.Edge.Value}
	} // #nosec G115 -- exact5/6.
	body, err := wire(x)
	if err != nil {
		return err
	}
	r.Pending = &columnFaultOriginal{Proposal: body}
	if err = historyFaultSave(root, *r); err != nil {
		return err
	}
	o, callbacks, err := h.graphCostSubmit(x)
	if err != nil {
		a := graphPlatformAttempt{Request: x.Request, Transaction: x.ID, BodySHA256: sha256.Sum256(body), SubmitInvocations: 1, SDKCallbacks: callbacks}
		graphPlatformError(&a, err)
		r.Failure = &a
		return errors.Join(err, historyFaultSave(root, *r))
	}
	if !o.Commit {
		return errUnavailable
	}
	proof, _, err := h.graphCostReadParallelOriginal(context.Background(), x, body)
	if err != nil || proof.Outcome != o {
		return errors.Join(errMismatch, err)
	}
	r.Originals = append(r.Originals, columnFaultOriginal{body, o, proof.Pair, proof.Original})
	r.Pending = nil
	return historyFaultSave(root, *r)
}
func historyFaultPartial(page historyFaultPage) bool {
	return !page.Result.Done && page.Result.Phase == historyDeleting && page.Result.Deleted[0]+page.Result.Deleted[1] > 0 && page.After.State.Phase == historyDeleting && len(page.After.State.Cursor) > 0
}
func historyFaultCollect(s *graphHistoryRetention, r *historyFaultState) error {
	if len(r.Pages) >= historyFaultPages {
		return errLimit
	}
	before, err := historyFaultObserve(s, *r)
	if err != nil {
		return err
	}
	budget := graphHistoryBudget{Rows: 1, Bytes: 128 << 10, OutputBytes: 4096}
	page, err := s.collect(context.Background(), "process-final", budget)
	if err != nil {
		return err
	}
	after, err := historyFaultObserve(s, *r)
	if err != nil {
		return err
	}
	r.Pages = append(r.Pages, historyFaultPage{budget, page, before, after})
	return nil
}
func historyFaultMarker(root *os.Root, r historyFaultState, phase string) error {
	if err := historyFaultSave(root, r); err != nil {
		return err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Ready, Format string
		Coordinator   uint8
		StateSHA256   [32]byte
	}{phase, historyFaultFormat, r.Coordinator, sha256.Sum256(raw)})
}
func historyFaultRefuse(root *os.Root, s *graphHistoryRetention, r *historyFaultState) error {
	if _, err := s.h.at(cut{r.Old}, 1, nil); !errors.Is(err, errHistoryExpired) {
		return errors.Join(errMismatch, err)
	}
	if _, err := s.pin("below-floor", cut{r.Old}, r.Token.Secret); !errors.Is(err, errHistoryExpired) {
		return errors.Join(errMismatch, err)
	}
	command := graphCommand{Kind: "edge-update", Edge: s.h.config.Cost.edge(0, 6)}
	before, _, err := s.h.graphCostPointSnapshot(command)
	if err != nil {
		return err
	}
	x := s.h.graphTransaction("process-retirement-diagnostic", r.Coordinator, command, before)
	x.Lifecycle = &lifecycleMetadata{Provenance: 5, EdgeLife: command.Edge.ID + 1, Definition: &lifecycleDefinition{ID: 6, Kind: "i64", I64: 6}}
	r.Diagnostic, err = wire(x)
	if err != nil {
		return err
	}
	if err := historyFaultSave(root, *r); err != nil {
		return err
	}
	for range 2 {
		prior, err := historyFaultObserve(s, *r)
		if err != nil {
			return err
		}
		submit := s.h.graphCostParallelSubmit(context.Background(), x, r.Diagnostic)
		a := graphPlatformAttempt{Classification: "returned-outcome", Request: x.Request, Transaction: x.ID, BodySHA256: sha256.Sum256(r.Diagnostic), SubmitInvocations: 1, SDKCallbacks: submit.Callbacks, Outcome: submit.Outcome, Retryable: submit.Retryable, MaybeCommitted: submit.MaybeCommitted}
		if submit.cause != nil {
			graphPlatformError(&a, submit.cause)
		}
		if errors.Is(submit.cause, errHistoryBusy) {
			a.Classification = "retirement-busy"
		}
		r.GuardRefusals = append(r.GuardRefusals, a)
		if err := historyFaultSave(root, *r); err != nil {
			return errors.Join(submit.cause, err)
		}
		if !errors.Is(submit.cause, errHistoryBusy) || submit.Retryable || submit.MaybeCommitted || submit.Outcome != (outcome{}) || a.NativeCode != nil {
			return errors.Join(errMismatch, submit.cause)
		}
		got, err := s.h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
			var pair [2]graphCostStoredRecord
			for i, key := range []fdb.Key{s.h.key(x.Coordinator, "id", x.ID), s.h.key(x.Coordinator, "request", x.Request)} {
				raw, err := tr.Get(key).Get()
				if err != nil {
					return nil, err
				}
				if raw != nil {
					return nil, errMismatch
				}
				pair[i] = graphCostStoredRecord{bytes.Clone(key), nil}
			}
			return pair, nil
		})
		if err != nil {
			return err
		}
		a.Originals = got.([2]graphCostStoredRecord)
		after, err := historyFaultObserve(s, *r)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(prior, after) {
			return errMismatch
		}
		r.GuardRefusals[len(r.GuardRefusals)-1] = a
		if err := historyFaultSave(root, *r); err != nil {
			return err
		}
	}
	return nil
}

// This separate finite format uses actual bounded production walks. Its history
// EOF is the retained physical prefix, not the original publication effect count.
func historyFaultWrite(root *os.Root, header graphCostArtifactHeader, walk func(func(graphCostRawRow) error) (graphCostStreamEnd, error)) (end graphCostStreamEnd, err error) {
	if root == nil || walk == nil || header.Type != "header" || header.Format != historyFaultFormat || header.Policy.validate() != nil || header.Stream != "history" && header.Stream != "current" && header.Stream != "cdc" {
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
	if err = json.NewEncoder(writer).Encode(header); err != nil {
		return end, err
	}
	var counts [2]uint64
	verified, err := walk(func(row graphCostRawRow) error {
		if row.Group > 1 || counts[row.Group] >= 9 {
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
func historyFaultSources(root *os.Root, s *graphHistoryRetention, r *historyFaultState) (err error) {
	observed, err := historyFaultObserve(s, *r)
	if err != nil {
		return err
	}
	lane := &graphCostRetentionLane{s: s, end: graphCostRetentionEnd{OldCut: r.Old, Metadata: observed.Metadata, Seal: observed.State.Completed}}
	header, err := lane.header(cut{r.Final})
	if err != nil {
		return err
	}
	header.Format = historyFaultFormat
	var secret [32]byte
	if _, err = rand.Read(secret[:]); err != nil {
		return err
	}
	token, err := s.pin("final-proof", cut{r.Final}, secret)
	if err != nil {
		return err
	}
	owner, err := s.open(context.Background(), token)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, historyReadCleanup(owner), s.release("final-proof-close", token))
		if err != nil {
			r.Sources = [3]graphCostStreamEnd{}
		}
	}()
	for i, name := range []string{"history", "current", "cdc"} {
		header.Stream = name
		walk := func(emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
			switch name {
			case "history":
				rows, err := owner.walk(context.Background(), emit)
				if err != nil {
					return graphCostStreamEnd{}, err
				}
				after, err := historyFaultObserve(s, *r)
				if err != nil || after.Metadata != header.Metadata {
					return graphCostStreamEnd{}, errors.Join(errMismatch, err)
				}
				return graphCostStreamEnd{Rows: rows, Groups: r.Final.Groups, Metadata: after.Metadata}, nil
			case "current":
				return s.h.graphCostWalkCurrent(cut{r.Final}, header.Policy, emit)
			default:
				return s.h.graphCostWalkChanges(cut{r.Initial}, cut{r.Final}, header.Policy, emit)
			}
		}
		r.Sources[i], err = historyFaultWrite(root, header, walk)
		if err != nil {
			return err
		}
	}
	if r.Sources[0].Rows != observed.State.Completed.Anchors || r.Sources[1].Rows != r.Sources[0].Rows || r.Sources[2].Rows != r.Final.Groups {
		return errMismatch
	}
	return nil
}
func historyFaultReplay(s *graphHistoryRetention, r *historyFaultState) error {
	before, err := historyFaultObserve(s, *r)
	if err != nil {
		return err
	}
	for _, original := range r.Originals {
		x, err := decode[transaction](original.Proposal)
		if err != nil {
			return err
		}
		submitted := s.h.graphCostParallelSubmit(context.Background(), x, original.Proposal)
		if submitted.cause != nil || submitted.Outcome != original.Outcome {
			return errors.Join(errMismatch, submitted.cause)
		}
		proof, _, err := s.h.graphCostReadParallelOriginal(context.Background(), x, original.Proposal)
		if err != nil || proof.Outcome != original.Outcome || !graphColumnSame(proof.Pair, original.Originals) || !graphColumnSame(proof.Original, original.CDC) {
			return errors.Join(errMismatch, err)
		}
		r.Replays = append(r.Replays, graphPlatformAttempt{Classification: "original-commit", Request: x.Request, Transaction: x.ID, BodySHA256: sha256.Sum256(original.Proposal), SubmitInvocations: 1, SDKCallbacks: submitted.Callbacks, Outcome: proof.Outcome, Originals: proof.Pair, Original: proof.Original})
	}
	after, err := historyFaultObserve(s, *r)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(before, after) {
		return errMismatch
	}
	return nil
}
func runGraphHistoryFault(db fdb.Database, mode string) (err error) {
	c, coordinator, err := historyFaultSettings()
	if err != nil {
		return err
	}
	directory := os.Getenv("FDB_HISTORY_FAULT_ARTIFACT")
	if directory == "" {
		return errInvalid
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	h := &harness{db: db, config: c}
	if mode == "history-fault-seed" {
		h, err = newHarness(db, c)
		if err != nil {
			return err
		}
		initial, err := h.fresh([]uint8{0, 1}, nil)
		if err != nil {
			return err
		}
		s, err := newGraphHistoryRetention(h, initial, historyFaultPolicy())
		if err != nil {
			return err
		}
		r := historyFaultState{Version: 1, Format: historyFaultFormat, Coordinator: coordinator, Config: c, Binding: s.binding, Initial: initial.record}
		first, err := columnFaultSourceView(h, r.Initial)
		if err != nil {
			return err
		}
		if len(first.Values[0])+len(first.Values[1]) != 0 {
			return errMismatch
		}
		r.Views = append(r.Views, first)
		for i := range 3 {
			if err = historyFaultApply(root, h, &r, "setup", uint64(i)); err != nil {
				return err
			}
		}
		if err = historyFaultApply(root, h, &r, "create", 0); err != nil {
			return err
		}
		old, err := h.fresh([]uint8{0, 1}, nil)
		if err != nil {
			return err
		}
		r.Old = old.record
		view, err := columnFaultSourceView(h, r.Old)
		if err != nil {
			return err
		}
		if err = graphCostCheckView(view.View, c.Cost, 5); err != nil {
			return err
		}
		r.Views = append(r.Views, view)
		var secret [32]byte
		if _, err = rand.Read(secret[:]); err != nil {
			return err
		}
		r.Token, err = s.pin("held-old", old, secret)
		if err != nil {
			return err
		}
		owner, err := s.newRead(old, &r.Token)
		if err != nil {
			return err
		}
		r.Owner = owner.id
		// Actual factory identity is durably caller-owned BEFORE native acquisition.
		if err = historyFaultStageAt(s, &r, "factory-intent-before-acquisition", nil); err != nil {
			return err
		}
		if err = historyFaultSave(root, r); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	}
	r, err := historyFaultLoad(root)
	if err != nil {
		return err
	}
	s, err := newGraphHistoryRetention(h, cut{r.Initial}, r.Binding.Policy)
	if err != nil {
		return err
	}
	if !graphColumnSame(s.binding, r.Binding) {
		return errMismatch
	}
	switch mode {
	case "history-fault-owner-child":
		if len(r.Originals) != 4 || len(r.Stages) != 1 {
			return errInvalid
		}
		owner, err := historyFaultOwner(s, r)
		if err != nil {
			return err
		}
		if err = owner.open(context.Background(), r.Token); err != nil {
			return err
		}
		if err = historyFaultStageAt(s, &r, "owner-active-before-update", owner); err != nil {
			return err
		}
		if err = historyFaultApply(root, h, &r, "update", 0); err != nil {
			return err
		}
		final, err := h.fresh([]uint8{0, 1}, nil)
		if err != nil {
			return err
		}
		r.Final = final.record
		view, err := columnFaultSourceView(h, r.Final)
		if err != nil {
			return err
		}
		if err = graphCostCheckView(view.View, c.Cost, 6); err != nil {
			return err
		}
		r.Views = append(r.Views, view)
		if err = historyFaultStageAt(s, &r, "held-old-after-update", owner); err != nil {
			return err
		}
		if err = s.expire("expire-old", r.Token); err != nil {
			return err
		}
		if _, refused := s.open(context.Background(), r.Token); !errors.Is(refused, errHistoryExpired) {
			return errors.Join(errMismatch, refused)
		}
		if refused := s.begin("process-final", final); !errors.Is(refused, errHistoryInUse) {
			return errors.Join(errMismatch, refused)
		}
		if err = historyFaultStageAt(s, &r, "expired-new-open-refused-held-owner-live", owner); err != nil {
			return err
		}
		if err = root.Mkdir("before", 0700); err != nil {
			return err
		}
		beforeRoot, err := root.OpenRoot("before")
		if err != nil {
			return err
		}
		r.BeforeSources, err = h.graphCostArtifacts(beforeRoot, cut{r.Initial}, cut{r.Old}, final, graphCostStreamPolicy{PageRows: 8, PageBytes: 256 << 10})
		if err = errors.Join(err, beforeRoot.Close()); err != nil {
			return err
		}
		if err = historyFaultMarker(root, r, "source-owner-live-after-expiration"); err != nil {
			return err
		}
		select {}
	case "history-fault-recover-owner":
		if len(r.Stages) != 4 || len(r.Originals) != 5 {
			return errInvalid
		}
		owner, err := historyFaultOwner(s, r)
		if err != nil {
			return err
		}
		// No open/acquire: an expired pin grants no NEW authority. Genuine check
		// authenticates the already active exact owner retained across process death.
		if err = historyFaultStageAt(s, &r, "replacement-authenticated-held-old", owner); err != nil {
			return err
		}
		if err = owner.Close(); err != nil {
			return errors.Join(err, &graphHistoryReadPending{owner, err})
		}
		if err = historyFaultStageAt(s, &r, "original-owner-authenticated-close", nil); err != nil {
			return err
		}
		if err = s.begin("process-final", cut{r.Final}); err != nil {
			return err
		}
		if err = historyFaultStageAt(s, &r, "retirement-admitted", nil); err != nil {
			return err
		}
		if err = historyFaultRefuse(root, s, &r); err != nil {
			return err
		}
		if err = historyFaultStageAt(s, &r, "guard-refusals-unchanged", nil); err != nil {
			return err
		}
		if !reflect.DeepEqual(r.Stages[6].Observation, r.Stages[7].Observation) {
			return errMismatch
		}
		if err = historyFaultSave(root, r); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	case "history-fault-gc-child":
		if len(r.Stages) != 8 || len(r.Pages) != 0 {
			return errInvalid
		}
		for len(r.Pages) < historyFaultPages {
			if err = historyFaultCollect(s, &r); err != nil {
				return err
			}
			if err = historyFaultSave(root, r); err != nil {
				return err
			}
			page := r.Pages[len(r.Pages)-1]
			if page.Result.Done {
				return errMismatch
			}
			if historyFaultPartial(page) {
				if err = historyFaultMarker(root, r, "source-history-deletion-page-committed"); err != nil {
					return err
				}
				select {}
			}
		}
		return errLimit
	case "history-fault-terminal":
		if len(r.Stages) != 8 || len(r.Pages) == 0 || !historyFaultPartial(r.Pages[len(r.Pages)-1]) {
			return errInvalid
		}
		actual, err := historyFaultObserve(s, r)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(actual, r.Pages[len(r.Pages)-1].After) {
			return errMismatch
		}
		if err = s.begin("process-final", cut{r.Final}); err != nil {
			return err
		}
		again, err := historyFaultObserve(s, r)
		if err != nil || !reflect.DeepEqual(actual, again) {
			return errors.Join(errMismatch, err)
		}
		if err = historyFaultStageAt(s, &r, "replacement-exact-partial-cursor", nil); err != nil {
			return err
		}
		for len(r.Pages) < historyFaultPages {
			if err = historyFaultCollect(s, &r); err != nil {
				return err
			}
			if err = historyFaultSave(root, r); err != nil {
				return err
			}
			if r.Pages[len(r.Pages)-1].Result.Done {
				break
			}
		}
		last := r.Pages[len(r.Pages)-1]
		if !last.Result.Done || last.After.State.Phase != historySealed || last.Result.Seal.Deleted[0]+last.Result.Seal.Deleted[1] != 3 || last.Result.Seal.Originals[0]+last.Result.Seal.Originals[1] != 9 || len(last.After.History) != 6 {
			return errMismatch
		}
		if _, refused := h.at(cut{r.Old}, 1, nil); !errors.Is(refused, errHistoryExpired) {
			return errors.Join(errMismatch, refused)
		}
		if _, refused := s.pin("below-floor-after", cut{r.Old}, r.Token.Secret); !errors.Is(refused, errHistoryExpired) {
			return errors.Join(errMismatch, refused)
		}
		current, err := columnFaultSourceView(h, r.Final)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, r.Views[2]) {
			return errMismatch
		}
		groups, err := h.changes(cut{r.Initial}, cut{r.Final}, 1)
		if err != nil || len(groups) != 5 {
			return errors.Join(errMismatch, err)
		}
		for _, original := range r.Originals {
			found := 0
			for _, group := range groups {
				if group.Outcome == original.Outcome {
					found++
					body, err := wire(group)
					if err != nil || !bytes.Equal(body, original.CDC.Value) {
						return errors.Join(errMismatch, err)
					}
				}
			}
			if found != 1 {
				return errMismatch
			}
		}
		if err = historyFaultStageAt(s, &r, "sealed-original-lineage", nil); err != nil {
			return err
		}
		if err = historyFaultReplay(s, &r); err != nil {
			return err
		}
		if err = historyFaultSources(root, s, &r); err != nil {
			return err
		}
		if err = historyFaultStageAt(s, &r, "terminal-complete", nil); err != nil {
			return err
		}
		if err = historyFaultSave(root, r); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(struct {
			Format                            string
			SetupOperations, UsefulOperations int
			Record                            historyFaultState
			Scope                             string
		}{r.Format, 3, 2, r, "actual source-history SDK owner/collector process replacement; five COMMIT originals only, no ABORT/process-server/power-loss/matrix/capacity acceptance"})
	default:
		return fmt.Errorf("unsupported explicit source-history mode %s: %w", mode, errInvalid)
	}
}
