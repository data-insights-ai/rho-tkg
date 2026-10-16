package graphapply

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"hash"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativeLateCorrectionFamily = "late-correction-old-index"
const nativeLateCorrectionFormat = "native-late-correction-workload-v1"
const nativeLateCorrectionProcessFlag = "RHO_GRAPH_LATE_CORRECTION_PROCESS"

func nativeLateCorrectionCost(s nativeSelectorsSpec) nativeGraphCostSpec {
	return nativeGraphCostSpec{11, s.Facts, s.CrossPercent, s.Writers}
}
func nativeLateCorrectionValidate(s nativeGraphCostSpec) error {
	if s.Version != 11 {
		return errInvalid
	}
	s.Version = 9
	if err := nativeCreateUpdateValidate(s); err != nil {
		return err
	}
	if s.Facts/2 <= uint64(s.Writers) {
		return errInvalid
	} // #nosec G115 -- validated writers1/8/32.
	return nil
}
func nativeLateCorrectionSpecFromEnv() (s nativeSelectorsSpec, err error) {
	body := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_SPEC"))
	if len(body) == 0 || len(body) > 1024 || nativeFeedDecodeJSON(body, &s) != nil || s.Version != 1 || s.Family != nativeLateCorrectionFamily || s.Layout != "kv" && s.Layout != "columns" || nativeLateCorrectionValidate(nativeLateCorrectionCost(s)) != nil {
		return s, errInvalid
	}
	return s, nil
}
func nativeLateCorrectionAdmissionFromEnv(s nativeSelectorsSpec) (nativeSelectorsAdmission, [32]byte, error) {
	if s.Family != nativeLateCorrectionFamily || nativeLateCorrectionValidate(nativeLateCorrectionCost(s)) != nil {
		return nativeSelectorsAdmission{}, [32]byte{}, errInvalid
	}
	return nativeScalarReadAdmissionFromEnv(s, nativeLateCorrectionFamily)
}
func nativeLateCorrectionFrames(s nativeGraphCostSpec) (uint64, error) {
	if err := nativeLateCorrectionValidate(s); err != nil {
		return 0, err
	}
	// Explicit conservative native frame admission; not reported actual work.
	// Half updates do not imply that old/corrected/unchanged index scans are free.
	s.Version = 9
	return nativeCreateUpdateFrames(s)
}
func nativeLateCorrectionProfile(s nativeGraphCostSpec, init *declaredInitCommand, l *raftlog.Limits, p *raftlog.ApplicationPolicy, g *raftlog.ApplicationGenerationLimits) error {
	frames, err := nativeLateCorrectionFrames(s)
	if err != nil {
		return err
	}
	return nativeScalarReadProfile(s, frames, init, l, p, g)
}
func nativeLateCorrectionReadPolicy(s nativeSelectorsSpec, a nativeSelectorsAdmission) (nativeSelectorsReadPolicy, error) {
	if s.Family != nativeLateCorrectionFamily || a.Spec != s || a.Family != nativeLateCorrectionFamily {
		return nativeSelectorsReadPolicy{}, errInvalid
	}
	frames, err := nativeLateCorrectionFrames(nativeLateCorrectionCost(s))
	if err != nil {
		return nativeSelectorsReadPolicy{}, err
	}
	// Five actual query originals, plus four scan-equivalent passes for the
	// outgoing and two incoming endpoint ranges at BOTH retained cuts. This is
	// fresh11-only admission; neither native9 nor10 gets a changed allowance.
	p, err := nativeScalarReadAllowance(s.Facts, a.BudgetSeconds, a.DeadlineUnixNanos, frames, 9)
	p.Queries = 5
	return p, err
}
func nativeLateCorrectionColumnPolicy(h *allocationHost, s nativeSelectorsSpec, a nativeSelectorsAdmission, digest [32]byte) (nativeColumnPolicy, error) {
	if h == nil || h.machine == nil || h.machine.SemanticContractID() != durableColumnGraphSemanticContractID() || s.Family != nativeLateCorrectionFamily || a.Spec != s || a.Family != nativeLateCorrectionFamily || digest == ([32]byte{}) {
		return nativeColumnPolicy{}, errInvalid
	}
	frames, err := nativeLateCorrectionFrames(nativeLateCorrectionCost(s))
	if err != nil {
		return nativeColumnPolicy{}, err
	}
	return nativeScalarReadColumnPolicy(h, frames, a.BudgetSeconds, digest)
}
func newNativeLateCorrectionSessions(ctx context.Context, c *crossProcessCoordinator, local func(context.Context, graphCutQuery) (graphCutReply, error)) (*nativeSelectorsSessions, error) {
	s, err := nativeLateCorrectionSpecFromEnv()
	if err != nil {
		return nil, err
	}
	a, digest, err := nativeLateCorrectionAdmissionFromEnv(s)
	if err != nil {
		return nil, err
	}
	p, err := nativeLateCorrectionReadPolicy(s, a)
	if err != nil {
		return nil, err
	}
	return newNativeScalarQuerySessions(ctx, c, local, s, a, digest, p)
}
func nativeLateCorrectionChildEnv(s nativeSelectorsSpec, a nativeSelectorsAdmission, reopen bool) ([]string, error) {
	if s.Family != nativeLateCorrectionFamily || a.Spec != s || a.Family != nativeLateCorrectionFamily {
		return nil, errInvalid
	}
	env, err := nativeCreateUpdateChildEnv(s, a, reopen)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(nativeLateCorrectionCost(s))
	if err != nil {
		return nil, err
	}
	for i, v := range env {
		if strings.HasPrefix(v, "RHO_GRAPH_COST_SPEC=") {
			env[i] = "RHO_GRAPH_COST_SPEC=" + string(body)
		}
		if strings.HasPrefix(v, "RHO_GRAPH_CREATE_UPDATE_PROCESS=") {
			env[i] = "RHO_GRAPH_CREATE_UPDATE_PROCESS="
		}
	}
	return append(env, nativeLateCorrectionProcessFlag+"=1", nativeImmutableColumnsProcessFlag+"=", "RHO_GRAPH_FEED_RETAINED_PROCESS="), nil
}
func nativeLateCorrectionProcessProfile(s *nativeGraphCostSpec) (bool, error) {
	flag := os.Getenv(nativeLateCorrectionProcessFlag)
	if flag == "" {
		if s != nil && s.Version == 11 {
			return false, errInvalid
		}
		return false, nil
	}
	if flag != "1" || s == nil || nativeLateCorrectionValidate(*s) != nil || os.Getenv("RHO_GRAPH_CERTIFIED_PROCESS") != "1" || os.Getenv("RHO_GRAPH_SOURCE_PROCESS") != "1" {
		return false, errInvalid
	}
	for _, name := range []string{nativeImmutableColumnsProcessFlag, "RHO_GRAPH_CREATE_UPDATE_PROCESS", "RHO_GRAPH_SELECTORS_PROCESS", "RHO_GRAPH_FEED_PROCESS", "RHO_GRAPH_FEED_RETAINED_PROCESS", "RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_EDGE_UNIQUE_PROCESS", "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_NATIVE_WORKLOAD", "RHO_GRAPH_NATIVE_FAULT_FAMILY"} {
		if v := os.Getenv(name); v != "" && v != "0" {
			return false, errInvalid
		}
	}
	spec, err := nativeLateCorrectionSpecFromEnv()
	if err != nil || nativeLateCorrectionCost(spec) != *s {
		return false, errors.Join(errInvalid, err)
	}
	_, _, err = nativeLateCorrectionAdmissionFromEnv(spec)
	return err == nil, err
}
func nativeScalarCorrectedValue(s nativeGraphCostSpec, value int64, fact uint64) int64 {
	if s.Version == 11 && value == 6 && fact >= s.Facts/2 {
		return 5
	}
	return value
}

// Exactly three genuine endpoint-posting ranges serve six out/in scalar
// predicates. Continuations come only from the native view, never caller IDs.
type nativeLateCorrectionIndexCursor struct {
	endpoint     uint8
	continuation graphstore.PartitionIncidentContinuation
	hasCursor    bool
	counts       [3]uint64
}
type nativeLateCorrectionIndexReply struct {
	Endpoint              uint64
	Kind                  string
	Equals                [3]int64
	Matches               [3]uint64
	EndpointEOF, Complete bool
}

func (s *nativeSelectorsSessions) correctionIndexPage(ctx context.Context, owner *nativeSelectorsQuery) (nativeSelectorsReply, *nativeLateCorrectionIndexCursor, error) {
	var out nativeSelectorsReply
	if ctx == nil || s == nil || owner == nil || s.spec.Family != nativeLateCorrectionFamily || nativeLateCorrectionValidate(nativeLateCorrectionCost(s.spec)) != nil {
		return out, nil, errInvalid
	}
	state := nativeLateCorrectionIndexCursor{}
	if owner.correction != nil {
		state = *owner.correction
	}
	if state.endpoint >= 3 {
		return out, nil, errInvalid
	}
	endpoints := [3]uint64{1, 3, nativeLateCorrectionCost(s.spec).remoteNode()}
	predicate := graphstate.IncidentPredicate{Endpoint: graphstate.EntityID(endpoints[state.endpoint]), Window: owner.predicate.Window}
	var next graphstore.PartitionIncidentContinuation
	var err error
	out, next, err = s.pagePredicate(ctx, owner, nil, true, predicate, state.continuation, state.hasCursor, false)
	if err != nil {
		return nativeSelectorsReply{}, nil, err
	}
	result := nativeLateCorrectionIndexReply{Endpoint: endpoints[state.endpoint], Kind: "in", Equals: [3]int64{5, 6, 7}, EndpointEOF: out.EOF}
	if state.endpoint == 0 {
		result.Kind = "out"
	}
	for _, entity := range out.Points {
		if !entity.Found || !entity.Effective || entity.Kind != 2 || entity.Type != "edge" || !entity.Property.Present || entity.Property.Null || entity.Property.Kind != "i64" || result.Kind == "out" && entity.Source != result.Endpoint || result.Kind == "in" && entity.Target != result.Endpoint {
			return nativeSelectorsReply{}, nil, errCorrupt
		}
		matched := false
		for i, value := range result.Equals {
			if entity.Property.I64 == value {
				if state.counts[i] >= s.spec.Facts {
					return nativeSelectorsReply{}, nil, errCorrupt
				}
				state.counts[i]++
				matched = true
			}
		}
		if !matched {
			return nativeSelectorsReply{}, nil, errCorrupt
		}
	}
	result.Matches = state.counts
	if out.EOF {
		state.endpoint++
		state.continuation = graphstore.PartitionIncidentContinuation{}
		state.hasCursor = false
		state.counts = [3]uint64{}
	} else {
		state.continuation = next
		state.hasCursor = true
	}
	result.Complete = state.endpoint == 3
	out.Index = &result
	// Only serve's successful bounded JSON reply installs this private state.
	// Failed reads still debit the SAME actual owner's native work.
	return out, &state, nil
}

// Fixed16 observation filtering for useful correction footprint counters only.
// The independent original checker remains authority for read/write coverage.
func nativeLateCorrectionCount(s nativeSelectorsSpec, phase *nativeCreateUpdatePhase, sequence *uint64, q nativeSelectorsInput, r nativeSelectorsReply) error {
	if phase == nil || nativeLateCorrectionValidate(nativeLateCorrectionCost(s)) != nil {
		return errInvalid
	}
	if phase.Phase == "update" {
		points, ids := make([]nativeLifecycleEntity, 0, 16), make([]uint64, 0, 16)
		for i, p := range r.Points {
			if p.ID >= 7 && (p.ID-7)/2 < s.Facts/2 {
				if i >= len(r.Candidates) {
					return errCorrupt
				}
				points = append(points, p)
				ids = append(ids, r.Candidates[i])
			}
		}
		r.Points, r.Candidates = points, ids
	}
	return nativeCreateUpdateCount(s, phase, sequence, q, r)
}

func nativeLateCorrectionWorkload(t *testing.T) {
	t.Helper()
	nativeScalarCreateUpdateWorkload(t, nativeLateCorrectionFamily)
}

const nativeLateCorrectionIndexAccess = "complete actual native out/in incident-posting pages with typed scalar residuals and original-bound hydration; one shared candidate walk per endpoint for5/6/phantom7, not value-index latency"

type nativeLateCorrectionEnd struct {
	Version           uint8
	IndexObservations uint64
	SHA256            [32]byte
	Access            string
}
type nativeLateCorrectionIndexes struct {
	f      *os.File
	digest hash.Hash
	spec   nativeSelectorsSpec
	end    nativeLateCorrectionEnd
	closed bool
}

func newNativeLateCorrectionIndexes(directory string, s nativeSelectorsSpec, digest [32]byte) (*nativeLateCorrectionIndexes, error) {
	if s.Family != nativeLateCorrectionFamily || nativeLateCorrectionValidate(nativeLateCorrectionCost(s)) != nil || digest == ([32]byte{}) {
		return nil, errInvalid
	}
	f, err := os.OpenFile(filepath.Join(directory, "indexes.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	o := &nativeLateCorrectionIndexes{f: f, digest: sha256.New(), spec: s, end: nativeLateCorrectionEnd{Version: 1, Access: nativeLateCorrectionIndexAccess}}
	err = o.write(struct {
		Type, Format    string
		Spec            nativeSelectorsSpec
		AdmissionSHA256 [32]byte
	}{"header", nativeLateCorrectionFormat, s, digest})
	if err != nil {
		return nil, errors.Join(err, o.Close())
	}
	return o, nil
}
func (o *nativeLateCorrectionIndexes) write(value any) error {
	if o == nil || o.closed {
		return errInvalid
	}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(body) > 64<<10 {
		return errLimit
	}
	body = append(body, '\n')
	if _, err = o.f.Write(body); err != nil {
		return err
	}
	_, err = o.digest.Write(body)
	return err
}
func (o *nativeLateCorrectionIndexes) Close() error {
	if o == nil || o.closed {
		return nil
	}
	o.closed = true
	return o.f.Close()
}
func (o *nativeLateCorrectionIndexes) observe(t *testing.T, c *nativeSelectorsClient, handle uint64, at graphCut, sequence *uint64) {
	t.Helper()
	if o == nil || sequence == nil || o.end.IndexObservations >= 2 {
		t.Fatal("exact half-correction index observations")
	}
	name := []string{"old", "final"}[o.end.IndexObservations]
	endpoints := [3]uint64{1, 3, nativeLateCorrectionCost(o.spec).remoteNode()}
	var counts [6][2]uint64
	var work graphstore.PageWork
	endpoint := 0
	began := time.Now()
	for page := uint64(0); ; page++ {
		if page > 2*o.spec.Facts+3 || endpoint >= 3 {
			t.Fatal("bounded actual correction index continuation")
		}
		*sequence++
		r := c.invoke(nativeSelectorsInput{Action: "index", Handle: handle, Sequence: *sequence}, nil)
		if r.Index == nil || r.Index.Endpoint != endpoints[endpoint] || r.Index.Equals != ([3]int64{5, 6, 7}) || r.Index.EndpointEOF != r.EOF || r.Index.Complete != (endpoint == 2 && r.EOF) || len(r.Points) != len(r.Candidates) {
			t.Fatal("actual native index scope/complete page")
		}
		for i, p := range r.Points {
			if r.Candidates[i] != p.ID || p.ID < 7 || (p.ID-7)%2 != 0 {
				t.Fatal("actual incident candidate identity")
			}
			value := int64(5)
			if name == "final" && (p.ID-7)/2 < o.spec.Facts/2 {
				value = 6
			}
			if err := nativeCreateUpdateCheckPoint(nativeFeedSpec{Version: 1, Family: nativeLateCorrectionFamily, Facts: o.spec.Facts, CrossPercent: o.spec.CrossPercent, Writers: o.spec.Writers, Layout: o.spec.Layout}, nativeFeedPoint{Fact: (p.ID - 7) / 2, Entity: p}, value); err != nil {
				t.Fatal("exact corrected/unchanged retained index entity", err)
			}
		}
		if r.EOF {
			base, group := 0, 0
			if endpoint > 0 {
				base = 3
			}
			if endpoint == 2 {
				group = 1
			}
			for i, n := range r.Index.Matches {
				counts[base+i][group] += n
			}
			endpoint++
		}
		work = r.Work
		if r.Index.Complete {
			break
		}
	}
	if endpoint != 3 || counts[2] != ([2]uint64{}) || counts[5] != ([2]uint64{}) {
		t.Fatal("real exact-empty phantom predicates")
	}
	var total uint64
	for _, groups := range counts {
		total += groups[0] + groups[1]
	}
	if total != 2*o.spec.Facts {
		t.Fatal("complete outgoing/incoming posting census")
	}
	cut, err := nativeFeedCutWire(at)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.write(struct {
		Type, Name, Stage, Access string
		Handle                    uint64
		Cut                       []byte
		Matches                   [6][2]uint64
		Work                      graphstore.PageWork
		Nanos                     int64
	}{"index-eof", name, c.stage, nativeLateCorrectionIndexAccess, handle, cut, counts, work, time.Since(began).Nanoseconds()}); err != nil {
		t.Fatal(err)
	}
	o.end.IndexObservations++
}
func (o *nativeLateCorrectionIndexes) finish() (nativeLateCorrectionEnd, error) {
	if o == nil || o.closed || o.end.IndexObservations != 2 {
		return nativeLateCorrectionEnd{}, errCorrupt
	}
	copy(o.end.SHA256[:], o.digest.Sum(nil))
	err := json.NewEncoder(o.f).Encode(struct {
		Type string
		End  nativeLateCorrectionEnd
	}{"footer", o.end})
	return o.end, errors.Join(err, o.Close())
}
