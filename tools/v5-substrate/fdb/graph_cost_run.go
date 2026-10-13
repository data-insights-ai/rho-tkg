package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type graphCostSample struct {
	Phase                                      string `json:"phase"`
	Fact                                       uint64 `json:"fact"`
	Cross                                      bool   `json:"cross"`
	ProposalNanos, AckNanos, VerificationNanos int64
	ProposalAttempts, CommitAttempts           int
	Outcome                                    outcome
}
type graphCostResult struct {
	Version                                                  int              `json:"version"`
	Engine                                                   string           `json:"engine"`
	Spec                                                     graphCostProfile `json:"spec"`
	Deployment                                               string           `json:"deployment"`
	SetupNodes                                               int
	Creates, Updates, CompletedCreates, CompletedUpdates     uint64
	SetupNanos, WritesNanos, VerificationNanos, TotalNanos   int64
	InitialCut, OldCut, FinalCut                             certificate
	Config                                                   config
	CDCGroups, LogicalComponentChanges, PhysicalGraphChanges uint64
	IDAdmission                                              string
}

func graphCostSpecFromEnv() (graphCostProfile, error) {
	var p graphCostProfile
	raw := os.Getenv("RHO_GRAPH_COST_SPEC")
	if len(raw) == 0 || len(raw) > 1024 {
		return p, errInvalid
	}
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&p); err != nil {
		return p, errors.Join(errInvalid, err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return p, errors.Join(errInvalid, err)
	}
	if !validGraphCostProfile(graphCostConfig("validation", p)) {
		return p, errInvalid
	}
	return p, nil
}

// Only the bounded actual metadata and declared dependency keys are captured.
// This avoids rescanning the complete growing current state per operation.
func (h *harness) graphCostPointSnapshot(command graphCommand) (snapshot, int, error) {
	attempts := 0
	result, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		attempts++
		s := snapshot{Values: [2]map[string]value{{}, {}}}
		keys := graphReadKeys(command)
		for g := uint8(0); g < 2; g++ {
			if err := h.checkConfig(tr, g); err != nil {
				return nil, err
			}
			m, err := h.meta(tr, g)
			if err != nil {
				return nil, err
			}
			s.Generations[g] = m.Generation
			for _, key := range keys[g] {
				v, found, err := get[value](tr, h.key(g, "current", key))
				if err != nil {
					return nil, err
				}
				if found && !validGraphValue(v) {
					return nil, errInvalid
				}
				s.Values[g][key] = v
			}
		}
		return s, nil
	})
	if err != nil {
		return snapshot{}, attempts, err
	}
	return result.(snapshot), attempts, nil
}

// Identical immutable validation and native commit implementation as submit;
// attempts counts actual retry callback invocations, with no callback output.
func (h *harness) graphCostSubmit(x transaction) (outcome, int, error) {
	if h == nil {
		return outcome{}, 0, errInvalid
	}
	if err := h.validate(x); err != nil {
		return outcome{}, 0, err
	}
	body, err := wire(x)
	if err != nil {
		return outcome{}, 0, err
	}
	x, err = decode[transaction](body)
	if err != nil {
		return outcome{}, 0, err
	}
	attempts := 0
	result, err := h.db.Transact(func(tr fdb.Transaction) (any, error) { attempts++; return h.execute(tr, x, sha256.Sum256(body)) })
	if err != nil {
		if _, ok := errors.AsType[fdb.Error](err); ok {
			return outcome{}, attempts, errors.Join(errUnknown, err)
		}
		return outcome{}, attempts, err
	}
	return result.(outcome), attempts, nil
}
func (h *harness) graphCostOriginal(x transaction, o outcome) (changeGroup, error) {
	rows, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		group, found, err := get[changeGroup](tr, h.changeKey(o.Coordinator, o.Sequence, o.Request))
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errUnavailable
		}
		if err := h.validateChange(group); err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(group.Tx, x) || group.Outcome != o {
			return nil, errMismatch
		}
		for _, key := range []fdb.Key{h.key(o.Coordinator, "id", o.ID), h.key(o.Coordinator, "request", o.Request)} {
			actual, found, err := get[outcome](tr, key)
			if err != nil {
				return nil, err
			}
			if !found || actual != o {
				return nil, errMismatch
			}
		}
		return group, nil
	})
	if err != nil {
		return changeGroup{}, err
	}
	return rows.(changeGroup), nil
}
func graphCostCheckView(view graphView, p graphCostProfile, want int64) error {
	nodes := p.nodes()
	if len(view.Nodes) != 3 || uint64(len(view.IdentityEdges)) != p.Facts || uint64(len(view.VisibleEdges)) != p.Facts || uint64(len(view.Outgoing)) != p.Facts || uint64(len(view.Incoming)) != p.Facts {
		return errMismatch
	}
	// Owner0 contains two distinct heads; compare declared node identities/lives.
	seen := map[endpoint]bool{}
	for _, n := range view.Nodes {
		seen[n] = true
	}
	for _, n := range nodes {
		if !seen[n] {
			return errMismatch
		}
	}
	for i := uint64(0); i < p.Facts; i++ {
		expected := p.edge(i, want)
		if view.IdentityEdges[i] != expected || view.VisibleEdges[i] != expected || view.Outgoing[i] != expected || view.Incoming[i] != expected {
			return errMismatch
		}
	}
	return nil
}
func runGraphCost(db fdb.Database, p graphCostProfile) (err error) {
	started := time.Now()
	c := graphCostConfig("graph-cost-v1", p)
	h, err := newHarness(db, c)
	if err != nil {
		return err
	}
	// Qualification exercises refusal against the actual immutable native config.
	// This read-only check is excluded from uninstrumented measurement cells.
	if os.Getenv("FDB_COST_QUALIFY") == "1" {
		stale := &harness{db: h.db, config: h.config}
		stale.config.Epochs[0]++
		if _, _, refused := stale.graphCostPointSnapshot(graphCommand{Kind: "node-create", Node: p.nodes()[0]}); !errors.Is(refused, errStale) {
			return errors.Join(errMismatch, refused)
		}
	}
	directory := os.Getenv("FDB_COST_ARTIFACT")
	if directory == "" {
		return errInvalid
	}
	artifactRoot, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, artifactRoot.Close()) }()
	open := func(name string) (*os.File, *json.Encoder, error) {
		return openGraphCostArtifact(artifactRoot, name)
	}
	latency, samples, err := open("fdb-cost-latency.jsonl")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, latency.Close()) }()
	cdc, records, err := open("fdb-cost-cdc.jsonl")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, cdc.Close()) }()
	result := graphCostResult{Version: 1, Engine: "foundationdb", Spec: p, Deployment: "single-machine distributed: four logical fault-domain containers, triple ssd", Config: c, SetupNodes: 3, Creates: p.Facts, Updates: p.Facts, IDAdmission: "fixed declared fixture IDs/ranges; this adapter has no rho allocator protocol"}
	initial, err := h.fresh([]uint8{0, 1}, nil)
	if err != nil {
		return err
	}
	result.InitialCut = initial.record
	apply := func(phase string, i uint64, command graphCommand, provenance uint64) error {
		begin := time.Now()
		before, attempts, err := h.graphCostPointSnapshot(command)
		if err != nil {
			return err
		}
		coordinator := uint8(0)
		if phase == "setup" {
			coordinator = command.Node.Owner
		}
		x := h.graphTransaction(fmt.Sprintf("cost-%s-%d", phase, i), coordinator, command, before)
		m := lifecycleMetadata{Provenance: provenance}
		if phase != "setup" {
			id := uint64(5)
			if phase == "update" {
				id = 6
			}
			m.EdgeLife = command.Edge.ID + 1
			m.Definition = &lifecycleDefinition{ID: id, Kind: "i64", I64: command.Edge.Value}
		}
		x.Lifecycle = &m
		sample := graphCostSample{Phase: phase, Fact: i, Cross: phase != "setup" && p.cross(i), ProposalNanos: time.Since(begin).Nanoseconds(), ProposalAttempts: attempts}
		begin = time.Now()
		o, attempts, err := h.graphCostSubmit(x)
		sample.AckNanos = time.Since(begin).Nanoseconds()
		sample.CommitAttempts = attempts
		sample.Outcome = o
		if err != nil {
			return err
		}
		if !o.Commit {
			return fmt.Errorf("cost actual operation refused %s/%d: %s: %w", phase, i, o.Reason, errUnavailable)
		}
		begin = time.Now()
		group, err := h.graphCostOriginal(x, o)
		if err != nil {
			return err
		}
		for _, row := range group.Writes {
			if phase == "update" {
				if row.Before == nil || row.Before.Deleted || row.Before.Value != 5 || row.After.Value != 6 || row.After.Deleted {
					return errMismatch
				}
			}
			if phase == "create" && (row.Before != nil || row.After.Value != 5 || row.After.Deleted) {
				return errMismatch
			}
			if phase == "setup" && (row.Before != nil || row.After.Value != 1 || row.After.Deleted) {
				return errMismatch
			}
		}
		if err := records.Encode(group); err != nil {
			return err
		}
		sample.VerificationNanos = time.Since(begin).Nanoseconds()
		if err := samples.Encode(sample); err != nil {
			return err
		}
		result.CDCGroups++
		result.PhysicalGraphChanges += uint64(len(group.Writes))
		logical := uint64(1)
		if phase == "create" {
			logical = 2
		}
		result.LogicalComponentChanges += logical
		if phase != "setup" {
			result.WritesNanos += sample.ProposalNanos + sample.AckNanos
			if phase == "create" {
				result.CompletedCreates++
			} else {
				result.CompletedUpdates++
			}
		}
		return nil
	}
	for i, n := range p.nodes() {
		if err := apply("setup", uint64(i), graphCommand{Kind: "node-create", Node: n}, uint64(i+1)); err != nil {
			return err
		}
	}
	result.SetupNanos = time.Since(started).Nanoseconds()
	var old cut
	for _, phase := range []string{"create", "update"} {
		for i := uint64(0); i < p.Facts; i++ {
			value, provenance := int64(5), 4+i
			if phase == "update" {
				value, provenance = 6, 4+p.Facts+i
			}
			if err := apply(phase, i, graphCommand{Kind: "edge-" + phase, Edge: p.edge(i, value)}, provenance); err != nil {
				return err
			}
			if (i+1)%100 == 0 || i+1 == p.Facts {
				if err := json.NewEncoder(os.Stdout).Encode(struct {
					Phase                              string
					CompletedCreates, CompletedUpdates uint64
					ElapsedNanos                       int64
				}{phase, result.CompletedCreates, result.CompletedUpdates, time.Since(started).Nanoseconds()}); err != nil {
					return err
				}
			}
		}
		certified, err := h.fresh([]uint8{0, 1}, nil)
		if err != nil {
			return err
		}
		if phase == "create" {
			old = certified
			result.OldCut = certified.record
		} else {
			result.FinalCut = certified.record
		}
	}
	begin := time.Now()
	for _, read := range []struct {
		cut   cut
		value int64
	}{{old, 5}, {cut{result.FinalCut}, 6}} {
		s, err := h.at(read.cut, 128, nil)
		if err != nil {
			return err
		}
		if uint64(len(s.Values[0])+len(s.Values[1])) != 3+3*p.Facts {
			return errMismatch
		}
		view, err := h.projectGraph(s)
		if err != nil {
			return err
		}
		if err := graphCostCheckView(view, p, read.value); err != nil {
			return err
		}
		for _, rows := range s.Values {
			for key := range rows {
				if strings.HasPrefix(key, fmt.Sprintf("edge/%016x/", uint64(5))) {
					return errMismatch
				}
			}
		}
		file, encoder, err := open(fmt.Sprintf("fdb-cost-view-%d.json", read.value))
		if err != nil {
			return err
		}
		writeErr := encoder.Encode(struct {
			Cut      certificate
			Snapshot snapshot
			View     graphView
		}{read.cut.record, s, view})
		if err := errors.Join(writeErr, file.Close()); err != nil {
			return err
		}
	}
	groups, err := h.changes(initial, cut{result.FinalCut}, 128)
	if err != nil {
		return err
	}
	if uint64(len(groups)) != 3+2*p.Facts || result.CDCGroups != 3+2*p.Facts || result.LogicalComponentChanges != 3+3*p.Facts || result.PhysicalGraphChanges != 3+6*p.Facts {
		return errMismatch
	}
	if _, err := h.current(); err != nil {
		return err
	}
	result.VerificationNanos = time.Since(begin).Nanoseconds()
	result.TotalNanos = time.Since(started).Nanoseconds()
	file, encoder, err := open("fdb-cost-result.json")
	if err != nil {
		return err
	}
	return errors.Join(encoder.Encode(result), file.Close())
}

// Artifact names remain scoped to the caller-selected directory handle.
// O_EXCL preserves originals; traversal and closed-root errors remain intact.
func openGraphCostArtifact(root *os.Root, name string) (*os.File, *json.Encoder, error) {
	file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, nil, err
	}
	return file, json.NewEncoder(file), nil
}
