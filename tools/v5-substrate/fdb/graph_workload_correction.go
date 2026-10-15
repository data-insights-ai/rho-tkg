package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphCorrectionFamily = "late-correction-old-index"
const graphCorrectionFormat = "fdb-late-correction-old-index-v1"

func graphCorrectionConfig(spec graphSelectorsSpec, run string) config {
	version := "v1"
	if spec.Version == 2 {
		version = "v2"
	}
	return graphCostConfig("graph-workload-correction-"+version+"-"+spec.Layout+"-"+run, spec.profile())
}
func graphCorrectionLimit(p graphCostProfile) limits {
	// One create per fact, then authentic corrections for only the first half.
	// Retained ABORT pairs get finite attempt headroom, not invented useful work.
	originals := 3 + (p.Facts+p.Facts/2)*graphCostParallelAttempts
	return limits{Bytes: (64 << 20) + p.Facts*(64<<10) + originals*2048, Transactions: originals, Versions: 3 + 3*p.Facts + 3*(p.Facts/2), Certificates: 1024}
}
func validGraphCorrectionConfig(c config) bool {
	if c.Cost.Version != 7 || c.Cost.Writers != 1 && c.Cost.Writers != 8 && c.Cost.Writers != 32 {
		return false
	}
	if c.Cost.Facts/2 <= uint64(c.Cost.Writers) {
		return false
	} // #nosec G115 -- admitted writers1/8/32.
	proxy := c
	proxy.Cost.Version = 3
	proxy.Graph = strings.Replace(c.Graph, "graph-workload-correction-", "graph-workload-selectors-", 1)
	return proxy.Graph != c.Graph && validGraphSelectorsConfig(proxy)
}
func graphCorrectionValue(p graphCostProfile, fact uint64, generation string) int64 {
	if generation == "final" && fact < p.Facts/2 {
		return 6
	}
	return 5
}
func graphCorrectionSpecFromEnv() (graphSelectorsSpec, string, error) {
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
	if (spec.Version != 1 && spec.Version != 2) || spec.Family != graphCorrectionFamily || !validGraphCorrectionConfig(graphCorrectionConfig(spec, run)) {
		return spec, "", errInvalid
	}
	p, err := graphTransportAdmission(spec.Version, spec.Family, run)
	if err != nil || p != nil && p.Spec != graphFeedWorkloadSpec(spec) {
		return spec, "", errors.Join(errMismatch, err)
	}
	return spec, run, nil
}
func runGraphCorrection(db fdb.Database, spec graphSelectorsSpec, run string) error {
	if spec.Family != graphCorrectionFamily {
		return errInvalid
	}
	return runGraphScalarReadWorkload(db, spec, run)
}

func graphCorrectionPredicates() []graphIndexPredicate {
	return []graphIndexPredicate{{"out", 5}, {"out", 6}, {"out", 7}, {"in", 5}, {"in", 6}, {"in", 7}}
}

// The late correction is already committed before either index observation:
// both owners now see genuine current6 heads for corrected IDs, while old must
// traverse native original/history authority back to5. Six predicates share ONE
// complete source scan per owner. The family never calls that indexed latency.
func (a *graphSelectorsArtifacts) correctionIndexes(old, current *graphSnapshot) (err error) {
	if a == nil || a.h == nil || a.end.Spec.Family != graphCorrectionFamily || old == nil || current == nil {
		return errInvalid
	}
	file, _, err := openGraphCostArtifact(a.root, "fdb-correction-indexes.jsonl")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	digest := sha256.New()
	write := func(v any) error { return selectorsJSON(io.MultiWriter(file, digest), v) }
	predicates := graphCorrectionPredicates()
	if err := write(struct {
		Type, Format, Access string
		Spec                 graphSelectorsSpec
		Config               config
		Binding              graphCostStoredRecord
		Old, Final           certificate
		Predicates           []graphIndexPredicate
		Owners               [2]graphCostStoredRecord
	}{"header", a.end.Format, "declared out/in postings; batched scan-based selection, not a value index", a.end.Spec, a.h.config, a.end.BindingRecord, a.old.record, a.final.record, predicates, [2]graphCostStoredRecord{old.selection.AcquiredOwner, current.selection.AcquiredOwner}}); err != nil {
		return err
	}
	for i, read := range []*graphSnapshot{old, current} {
		name := "old"
		if i == 1 {
			name = "final"
		}
		var counts [8][2]uint64
		begin := time.Now()
		end, err := read.IndexScan(a.context(), predicates, func(match graphIndexMatch) error {
			if match.Selector >= 6 || match.Row.Group > 1 {
				return errMismatch
			}
			counts[match.Selector][match.Row.Group]++
			return write(struct {
				Type, Name string
				Match      graphIndexMatch
			}{"match", name, match})
		})
		if err != nil {
			return err
		}
		if end.Selectors != 6 || end.Matches != counts || counts[2] != [2]uint64{} || counts[5] != [2]uint64{} {
			return errMismatch
		}
		var total uint64
		for _, groups := range counts {
			total += groups[0] + groups[1]
		}
		if total != 2*a.h.config.Cost.Facts {
			return errMismatch
		}
		if err := write(struct {
			Type, Name, Access string
			End                graphIndexEnd
			Nanos              int64
		}{"index-eof", name, "ONE complete retained source Scan for six predicates including native retries/wait and caller row IO; not indexed latency", end, time.Since(begin).Nanoseconds()}); err != nil {
			return err
		}
	}
	return selectorsJSON(file, struct{ Type, PayloadSHA256 string }{"footer", hex.EncodeToString(digest.Sum(nil))})
}
