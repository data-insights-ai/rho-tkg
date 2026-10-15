package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphCreateUpdateFamily = "partition-local-create-update"
const graphCreateUpdateFormat = "fdb-create-update-v1"

// The canonical family label denotes its strict-local0 baseline. Nonzero rows
// are actual mixed/cross graph transactions, never local-throughput results.
func graphCreateUpdateSemantics(cross uint64) string {
	if cross == 0 {
		return "strict-partition-local-create-update"
	}
	return "mixed-or-cross-partition-create-update"
}
func graphCreateUpdateConfig(spec graphSelectorsSpec, run string) config {
	version := "v1"
	if spec.Version == 2 {
		version = "v2"
	}
	return graphCostConfig("graph-workload-create-update-"+version+"-"+spec.Layout+"-"+run, spec.profile())
}
func validGraphCreateUpdateConfig(c config) bool {
	if c.Cost.Version != 9 {
		return false
	}
	proxy := c
	proxy.Cost.Version = 3
	proxy.Graph = strings.Replace(c.Graph, "graph-workload-create-update-", "graph-workload-selectors-", 1)
	return proxy.Graph != c.Graph && validGraphSelectorsConfig(proxy)
}
func graphCreateUpdateSpecFromEnv() (graphSelectorsSpec, string, error) {
	var spec graphSelectorsSpec
	raw, run := os.Getenv("RHO_GRAPH_WORKLOAD_SPEC"), os.Getenv("RHO_GRAPH_WORKLOAD_RUN")
	if len(raw) == 0 || len(raw) > 1024 {
		return spec, "", errInvalid
	}
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&spec); err != nil {
		return spec, "", errors.Join(errInvalid, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return spec, "", errors.Join(errInvalid, err)
	}
	if (spec.Version != 1 && spec.Version != 2) || spec.Family != graphCreateUpdateFamily || !validGraphCreateUpdateConfig(graphCreateUpdateConfig(spec, run)) {
		return spec, "", errInvalid
	}
	p, err := graphTransportAdmission(spec.Version, spec.Family, run)
	if err != nil || p != nil && p.Spec != graphFeedWorkloadSpec(spec) {
		return spec, "", errors.Join(errMismatch, err)
	}
	return spec, run, nil
}
func runGraphCreateUpdate(db fdb.Database, spec graphSelectorsSpec, run string) error {
	if spec.Family != graphCreateUpdateFamily {
		return errInvalid
	}
	return runGraphScalarReadWorkload(db, spec, run)
}

type graphCreateUpdatePhase struct {
	Phase                                                             string
	Attempts, Aborted, Committed, SingleParticipant, MultiParticipant uint64
	ReadParticipants, WriteParticipants                               [2]uint64
	StartedUnixNanos, FinishedUnixNanos, WallNanos                    int64
}
type graphCreateUpdateEnd struct {
	RowSemantics, IDAdmission, Coordination string
	SetupExcluded                           bool
	Phases                                  [2]graphCreateUpdatePhase
}

func graphCreateUpdateSummary(c config) *graphCreateUpdateEnd {
	return &graphCreateUpdateEnd{graphCreateUpdateSemantics(c.Cost.CrossPercent), "fixed declared fixture IDs; no allocator issuance claim", "one ordinary native FDB transaction per submission across the actual business prefixes; no rho 2PC; SDK retries/attempts recorded separately", true, [2]graphCreateUpdatePhase{{Phase: "create"}, {Phase: "update"}}}
}

// Derive locality from ALL actual graph reads/effects. Empty coordination-only
// participants cannot inflate the business ratio. Existing graph reducers own
// the keys/effects; this is observation, not a second mutation model.
func graphCreateUpdateFootprint(x transaction) (reads, writes [2]bool, err error) {
	if x.GraphChange == nil || x.GraphChange.Kind != "edge-create" && x.GraphChange.Kind != "edge-update" {
		return reads, writes, errInvalid
	}
	if x.GraphChange.Edge.Source.Owner != 0 || x.GraphChange.Edge.Target.Owner > 1 {
		return reads, writes, errInvalid
	}
	expected, keys := graphEffects(*x.GraphChange), graphReadKeys(*x.GraphChange)
	var seen [2]bool
	for _, p := range x.Participants {
		if p.Group > 1 || seen[p.Group] || len(p.Reads) == 0 || len(p.Effects) == 0 || !slices.Equal(p.Effects, expected[p.Group]) || len(p.Reads) != len(keys[p.Group]) {
			return reads, writes, errInvalid
		}
		seen[p.Group] = true
		for i, read := range p.Reads {
			if read.Key != keys[p.Group][i] {
				return reads, writes, errInvalid
			}
		}
		reads[p.Group], writes[p.Group] = true, true
	}
	for g := range 2 {
		if seen[g] != (len(expected[g]) > 0 || len(keys[g]) > 0) {
			return reads, writes, errInvalid
		}
	}
	return reads, writes, nil
}
func (a *graphSelectorsArtifacts) createUpdateAccept(e graphCostParallelEvent) error {
	if a.end.CreateUpdate == nil || e.Attempt == nil || e.Attempt.Operation.Phase == "setup" {
		return nil
	}
	attempt := e.Attempt
	if len(attempt.Proposal) == 0 || len(attempt.Proposal) > 64<<10 {
		return errLimit
	}
	x, err := decode[transaction](attempt.Proposal)
	if err != nil {
		return err
	}
	if err := a.h.validate(x); err != nil {
		return err
	}
	o, err := a.h.graphCostParallelOriginal(x, attempt.Proposal, attempt.Originals)
	if err != nil {
		return err
	}
	reads, writes, err := graphCreateUpdateFootprint(x)
	if err != nil {
		return err
	}
	phase := 0
	if attempt.Operation.Phase == "update" {
		phase = 1
	} else if attempt.Operation.Phase != "create" {
		return errInvalid
	}
	p := &a.end.CreateUpdate.Phases[phase]
	if p.WallNanos != 0 {
		return errMismatch
	}
	p.Attempts++
	if !o.Commit {
		p.Aborted++
		return nil
	}
	body, err := wire(e.Group.Tx)
	if err != nil || !bytes.Equal(body, attempt.Proposal) || e.Group.Outcome != o {
		return errors.Join(errMismatch, err)
	}
	p.Committed++
	if reads[0] && reads[1] || writes[0] && writes[1] {
		p.MultiParticipant++
	} else {
		p.SingleParticipant++
	}
	for g := range 2 {
		if reads[g] {
			p.ReadParticipants[g]++
		}
		if writes[g] {
			p.WriteParticipants[g]++
		}
	}
	return nil
}
func (a *graphSelectorsArtifacts) createUpdatePhase(name string, began time.Time) error {
	if a.end.CreateUpdate == nil {
		return nil
	}
	phase := 0
	if name == "update" {
		phase = 1
	} else if name != "create" {
		return errInvalid
	}
	p := &a.end.CreateUpdate.Phases[phase]
	n := a.h.config.Cost.Facts
	if p.WallNanos != 0 || p.Committed != n || p.Attempts != p.Committed+p.Aborted || p.MultiParticipant*100 != n*a.h.config.Cost.CrossPercent || p.SingleParticipant+p.MultiParticipant != n || p.ReadParticipants != [2]uint64{n, p.MultiParticipant} || p.WriteParticipants != p.ReadParticipants {
		return errMismatch
	}
	ended := time.Now()
	p.StartedUnixNanos, p.FinishedUnixNanos, p.WallNanos = began.UnixNano(), ended.UnixNano(), ended.Sub(began).Nanoseconds()
	if p.WallNanos <= 0 {
		return errInvalid
	}
	return a.event(struct {
		Type, RowSemantics string
		Phase              graphCreateUpdatePhase
	}{"business-phase", a.end.CreateUpdate.RowSemantics, *p})
}
