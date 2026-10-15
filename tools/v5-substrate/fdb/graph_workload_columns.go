package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphColumnsFamily = "immutable-columns"
const graphColumnsFormat = "fdb-immutable-columns-v1"

// The source graph is the same fixed scalar/life-bound slice, but this is an
// independently selected publication/scan workload, not a renamed selector run.
// Cost6 distinguishes its durable admission from all earlier graph formats.
func graphColumnsConfig(spec graphSelectorsSpec, run string) config {
	version := "v1"
	if spec.Version == 2 {
		version = "v2"
	}
	return graphCostConfig("graph-workload-columns-"+version+"-"+spec.Layout+"-"+run, spec.profile())
}
func validGraphColumnsConfig(c config) bool {
	if c.Cost.Version != 6 {
		return false
	}
	proxy := c
	proxy.Cost.Version = 3
	proxy.Graph = strings.Replace(c.Graph, "graph-workload-columns-", "graph-workload-selectors-", 1)
	return proxy.Graph != c.Graph && validGraphSelectorsConfig(proxy)
}
func graphColumnsSpecFromEnv() (graphSelectorsSpec, string, error) {
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
	if (spec.Version != 1 && spec.Version != 2) || spec.Family != graphColumnsFamily || !validGraphColumnsConfig(graphColumnsConfig(spec, run)) {
		return spec, "", errInvalid
	}
	admission, err := graphTransportAdmission(spec.Version, spec.Family, run)
	if err != nil || admission != nil && admission.Spec != graphFeedWorkloadSpec(spec) {
		return spec, "", errors.Join(errMismatch, err)
	}
	return spec, run, nil
}

// Only the new family installs this optional observation hook. Each event is
// emitted after the actual API returns/EOF, including native retries, waits and
// its caller callback IO. Errors never emit successful timing/EOF evidence.
func (a *graphSelectorsArtifacts) columnTiming(call, name string, at certificate, rows uint64, nanos int64) error {
	if a == nil || a.h == nil || a.end.Spec.Family != graphColumnsFamily && a.end.Spec.Family != graphCorrectionFamily || nanos <= 0 || !validName(name) || rows > 3+2*a.h.config.Cost.Facts {
		return errInvalid
	}
	return a.event(struct {
		Type, Call, Name, Access string
		Cut                      certificate
		Rows                     uint64
		Nanos                    int64
	}{"column-timing", call, name, "complete actual caller operation through EOF including retry/wait and artifact IO; scan-based points are not indexed latency", at, rows, nanos})
}

func runGraphColumns(db fdb.Database, spec graphSelectorsSpec, run string) (err error) {
	if spec.Family != graphColumnsFamily {
		return errInvalid
	}
	return runGraphScalarReadWorkload(db, spec, run)
}
func runGraphScalarReadWorkload(db fdb.Database, spec graphSelectorsSpec, run string) (err error) {
	c := graphColumnsConfig(spec, run)
	kind := "workload-columns-v1"
	if spec.Family == graphCorrectionFamily {
		c, kind = graphCorrectionConfig(spec, run), "workload-correction-v1"
	}
	if (spec.Version != 1 && spec.Version != 2) || spec.Family != graphColumnsFamily && spec.Family != graphCorrectionFamily || !validGraphCostProfile(c) {
		return errInvalid
	}
	admission, err := graphTransportAdmission(spec.Version, spec.Family, run)
	if err != nil || admission != nil && admission.Spec != graphFeedWorkloadSpec(spec) {
		return errors.Join(errMismatch, err)
	}
	seconds := uint64(180)
	if raw := os.Getenv("FDB_COST_SECONDS"); raw != "" && admission == nil {
		seconds, err = strconv.ParseUint(raw, 10, 64)
		if err != nil || seconds < 1 || seconds > 1800 {
			return errors.Join(errInvalid, err)
		}
	}
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	if admission != nil {
		deadline = time.Unix(0, admission.DeadlineUnixNanos)
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	// This is a fresh API: unlike historical selector wrappers, ordinary local
	// columns also use the bounded native IO door under the actual run context.
	h, err := newHarnessContext(ctx, db, c)
	if err != nil {
		return err
	}
	var transport *graphTransportRecord
	if admission != nil {
		stored, err := h.graphTransportInstall(ctx, *admission)
		if err != nil {
			return err
		}
		transport = &graphTransportRecord{*admission, stored}
	}
	binding, err := h.graphWorkloadInstallContext(ctx, admission, kind)
	if err != nil {
		return err
	}
	initial, err := graphTransportFresh(ctx, h)
	if err != nil {
		return err
	}
	source, err := newGraphHistoryRetentionContext(ctx, h, initial, graphSelectorsPolicy(c).History)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(os.Getenv("FDB_COST_ARTIFACT"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	a, err := newGraphWorkloadArtifacts(ctx, h, root, spec, binding, initial, transport)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, a.Close()) }()
	apply := func(phase string, fact uint64) error {
		begin := time.Now()
		var outputErr error
		err := h.graphSelectorsApply(ctx, phase, fact, 0, func(e graphCostParallelEvent) { outputErr = errors.Join(outputErr, a.accept(e)) })
		if err != nil || outputErr != nil {
			return errors.Join(err, outputErr)
		}
		return a.latency(phase, fact, 0, time.Since(begin).Nanoseconds())
	}
	for i := uint64(0); i < 3; i++ {
		if err := apply("setup", i); err != nil {
			return err
		}
	}
	if err := apply("create", 0); err != nil {
		return err
	}
	if err := h.graphSelectorsPhase(ctx, a, "create"); err != nil {
		return err
	}
	old, err := a.open(source, "old-fresh", graphSnapshotRequest{Mode: "Fresh", Scope: []uint8{0, 1}})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, old.Close()) }()
	a.old = cut{old.owner.id.Cut}
	if err := a.observe("old-fresh", old, 5); err != nil {
		return err
	}
	if spec.Layout == "columns" {
		begin := time.Now()
		if err := a.columnsStart(); err != nil {
			return err
		}
		if err := a.columnTiming("PublishAndHold", "old", a.old.record, a.column.end.Old.Rows, time.Since(begin).Nanoseconds()); err != nil {
			return err
		}
	}
	if err := a.inventory("old", a.old); err != nil {
		return err
	}
	// fact0's actual value6 is authoritative before parallel dependents start.
	if err := apply("update", 0); err != nil {
		return err
	}
	updates := c.Cost.Facts
	if spec.Family == graphCorrectionFamily {
		updates /= 2
	}
	if err := h.graphWorkloadPhase(ctx, a, "update", updates); err != nil {
		return err
	}
	if err := a.observe("held-old-after-update", old, 5); err != nil {
		return err
	}
	final, err := a.open(source, "fresh-current", graphSnapshotRequest{Mode: "Fresh", Scope: []uint8{0, 1}})
	if err != nil {
		return err
	}
	a.final = cut{final.owner.id.Cut}
	if err := a.observe("fresh-current", final, 6); err != nil {
		return errors.Join(err, final.Close())
	}
	if spec.Family == graphCorrectionFamily {
		if err := a.correctionIndexes(old, final); err != nil {
			return errors.Join(err, final.Close())
		}
	}
	if err := final.Close(); err != nil {
		return err
	}
	if err := old.Close(); err != nil {
		return err
	}
	if spec.Layout == "columns" {
		if err := a.columnsFinish(); err != nil {
			return err
		}
	}
	if err := a.finish(source); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(a.end)
}
