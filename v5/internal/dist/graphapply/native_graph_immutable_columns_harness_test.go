package graphapply

import (
	"bytes"
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

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativeImmutableColumnsFamily = "immutable-columns"
const nativeImmutableColumnsFormat = "native-immutable-columns-workload-v1"
const nativeImmutableColumnsProcessFlag = "RHO_GRAPH_IMMUTABLE_COLUMNS_PROCESS"
const nativeImmutableColumnsControl = "NIC1"

type nativeImmutableColumnsRetireQuestion struct{ Cookie nativeGraphCostColumnCookie }
type nativeImmutableColumnsRecord struct{ Key, Value []byte }
type nativeImmutableColumnsRetireObservation struct {
	Cookie             nativeGraphCostColumnCookie
	Generation, Source [16]byte
	Binding            nativeColumnBinding
	Before, After      [4]nativeImmutableColumnsRecord
}

func nativeImmutableColumnsRetireInput(q nativeImmutableColumnsRetireQuestion) ([]byte, error) {
	if q.Cookie.Owner == 0 || q.Cookie.Request == ([16]byte{}) {
		return nil, errInvalid
	}
	body, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}
	return append([]byte(nativeImmutableColumnsControl), body...), nil
}

// This fresh diagnostic calls the SAME production Retire without the normal
// retire request's prior reader Close. A real held source must refuse; labels
// or a timeout do not supply that outcome. It cannot target a foreign owner.
func nativeImmutableColumnsRetireIdle(ctx context.Context, s *nativeGraphCostColumnSessions, r processRequest) ([]byte, error) {
	if ctx == nil || s == nil || s.spec == nil || s.spec.Version != 10 && s.spec.Version != 11 || s.coordinator == nil || r.op != processCostObserve || r.index != 0 || r.entity != 0 || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 || len(r.data) > 1024 || !bytes.HasPrefix(r.data, []byte(nativeImmutableColumnsControl)) {
		return nil, errInvalid
	}
	var q nativeImmutableColumnsRetireQuestion
	if err := nativeFeedDecodeJSON(r.data[len(nativeImmutableColumnsControl):], &q); err != nil {
		return nil, errInvalid
	}
	wire, err := nativeImmutableColumnsRetireInput(q)
	if err != nil || !bytes.Equal(wire, r.data) {
		return nil, errInvalid
	}
	s.mu.Lock()
	o := s.owner
	if s.pending || o == nil || o.cookie != q.Cookie || s.probes >= 4 {
		s.mu.Unlock()
		return nil, errInvalid
	}
	s.probes++
	s.mu.Unlock()
	if err := o.gate.lock(ctx); err != nil {
		return nil, err
	}
	defer o.gate.unlock()
	if err := errors.Join(ctx.Err(), s.ctx.Err(), o.ctx.Err()); err != nil {
		return nil, err
	}
	if err := o.renew(ctx); err != nil {
		return nil, err
	}
	if o.store == nil || o.source == nil || o.store.binding.Version != 2 || o.store.binding.Policy.Version != 1 || o.store.binding.Policy.Admission == ([32]byte{}) {
		return nil, errInvalid
	}
	o.store.mu.Lock()
	held := o.store.retained[o.source.owner] == o.generation
	o.store.mu.Unlock()
	if !held {
		return nil, replica.ErrUnavailable
	}
	observed := nativeImmutableColumnsRetireObservation{Cookie: o.cookie, Generation: o.generation, Source: o.source.owner, Binding: o.store.binding}
	keys := [4][]byte{nativeColumnKey(o.generation, "progress"), nativeColumnKey(o.generation, "manifest"), nativeColumnRetentionKey(o.generation), []byte("ledger")}
	read := func() (out [4]nativeImmutableColumnsRecord, err error) {
		for i, key := range keys {
			value, e := o.store.get(key, nativeColumnMetadataBytes)
			if e != nil {
				return out, e
			}
			out[i] = nativeImmutableColumnsRecord{bytes.Clone(key), value}
		}
		return out, nil
	}
	observed.Before, err = read()
	if err != nil {
		return nil, err
	}
	publisher, err := newRetainedPinPublisher(s.coordinator)
	if err != nil {
		return nil, err
	}
	refused := o.store.Retire(ctx, o.generation, publisher)
	if !errors.Is(refused, errLimit) {
		return nil, errors.Join(errCorrupt, refused)
	}
	observed.After, err = read()
	if err != nil {
		return nil, err
	}
	for i := range keys {
		if !bytes.Equal(observed.Before[i].Value, observed.After[i].Value) {
			return nil, errCorrupt
		}
	}
	body, err := json.Marshal(observed)
	if err != nil {
		return nil, err
	}
	return body, refused // Preserve the actual native errLimit/cause mask.
}

func nativeImmutableColumnsCost(s nativeSelectorsSpec) nativeGraphCostSpec {
	return nativeGraphCostSpec{10, s.Facts, s.CrossPercent, s.Writers}
}
func nativeImmutableColumnsValidate(s nativeGraphCostSpec) error {
	if s.Version != 10 {
		return errInvalid
	}
	s.Version = 9
	return nativeCreateUpdateValidate(s) // Dimension arithmetic only, never Cost9 authority.
}
func nativeImmutableColumnsSpecFromEnv() (s nativeSelectorsSpec, err error) {
	body := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_SPEC"))
	if len(body) == 0 || len(body) > 1024 || nativeFeedDecodeJSON(body, &s) != nil || s.Version != 1 || s.Family != nativeImmutableColumnsFamily || s.Layout != "kv" && s.Layout != "columns" || nativeImmutableColumnsValidate(nativeImmutableColumnsCost(s)) != nil {
		return s, errInvalid
	}
	return s, nil
}
func nativeImmutableColumnsAdmissionFromEnv(s nativeSelectorsSpec) (nativeSelectorsAdmission, [32]byte, error) {
	if s.Family != nativeImmutableColumnsFamily || nativeImmutableColumnsValidate(nativeImmutableColumnsCost(s)) != nil {
		return nativeSelectorsAdmission{}, [32]byte{}, errInvalid
	}
	return nativeScalarReadAdmissionFromEnv(s, nativeImmutableColumnsFamily)
}
func nativeImmutableColumnsProcessProfile(s *nativeGraphCostSpec) (bool, error) {
	flag := os.Getenv(nativeImmutableColumnsProcessFlag)
	if flag == "" {
		if s != nil && s.Version == 10 {
			return false, errInvalid
		}
		return false, nil
	}
	if flag != "1" || s == nil || nativeImmutableColumnsValidate(*s) != nil || os.Getenv("RHO_GRAPH_CERTIFIED_PROCESS") != "1" || os.Getenv("RHO_GRAPH_SOURCE_PROCESS") != "1" {
		return false, errInvalid
	}
	for _, name := range []string{"RHO_GRAPH_CREATE_UPDATE_PROCESS", "RHO_GRAPH_SELECTORS_PROCESS", "RHO_GRAPH_FEED_PROCESS", "RHO_GRAPH_FEED_RETAINED_PROCESS", "RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_EDGE_UNIQUE_PROCESS", "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_NATIVE_WORKLOAD", "RHO_GRAPH_NATIVE_FAULT_FAMILY"} {
		if v := os.Getenv(name); v != "" && v != "0" {
			return false, errInvalid
		}
	}
	spec, err := nativeImmutableColumnsSpecFromEnv()
	if err != nil || nativeImmutableColumnsCost(spec) != *s {
		return false, errors.Join(errInvalid, err)
	}
	_, _, err = nativeImmutableColumnsAdmissionFromEnv(spec)
	return err == nil, err
}
func nativeImmutableColumnsFrames(s nativeGraphCostSpec) (uint64, error) {
	if err := nativeImmutableColumnsValidate(s); err != nil {
		return 0, err
	}
	s.Version = 9
	return nativeCreateUpdateFrames(s)
}
func nativeImmutableColumnsProfile(s nativeGraphCostSpec, init *declaredInitCommand, l *raftlog.Limits, p *raftlog.ApplicationPolicy, g *raftlog.ApplicationGenerationLimits) error {
	frames, err := nativeImmutableColumnsFrames(s)
	if err != nil {
		return err
	}
	return nativeScalarReadProfile(s, frames, init, l, p, g)
}
func nativeImmutableColumnsReadPolicy(s nativeSelectorsSpec, a nativeSelectorsAdmission) (nativeSelectorsReadPolicy, error) {
	if s.Family != nativeImmutableColumnsFamily || nativeImmutableColumnsValidate(nativeImmutableColumnsCost(s)) != nil || a.Spec != s || a.Family != nativeImmutableColumnsFamily {
		return nativeSelectorsReadPolicy{}, errInvalid
	}
	frames, err := nativeImmutableColumnsFrames(nativeImmutableColumnsCost(s))
	if err != nil {
		return nativeSelectorsReadPolicy{}, err
	}
	return nativeScalarReadAllowance(s.Facts, a.BudgetSeconds, a.DeadlineUnixNanos, frames, 5)
}
func nativeImmutableColumnsColumnPolicy(h *allocationHost, s nativeSelectorsSpec, a nativeSelectorsAdmission, digest [32]byte) (nativeColumnPolicy, error) {
	if h == nil || h.machine == nil || h.machine.SemanticContractID() != durableColumnGraphSemanticContractID() || s.Family != nativeImmutableColumnsFamily || a.Spec != s || a.Family != nativeImmutableColumnsFamily || digest == ([32]byte{}) {
		return nativeColumnPolicy{}, errInvalid
	}
	frames, err := nativeImmutableColumnsFrames(nativeImmutableColumnsCost(s))
	if err != nil {
		return nativeColumnPolicy{}, err
	}
	// Eight prepaid full passes: the old generation needs at most publication,
	// native/fill/cache, held-old, two reopen modes and one scan-based Points.
	// No ordinary profile quota is increased; actual work remains in receipts.
	return nativeScalarReadColumnPolicy(h, frames, a.BudgetSeconds, digest)
}
func newNativeImmutableColumnsSessions(ctx context.Context, c *crossProcessCoordinator, local func(context.Context, graphCutQuery) (graphCutReply, error)) (*nativeSelectorsSessions, error) {
	s, err := nativeImmutableColumnsSpecFromEnv()
	if err != nil {
		return nil, err
	}
	a, digest, err := nativeImmutableColumnsAdmissionFromEnv(s)
	if err != nil {
		return nil, err
	}
	p, err := nativeImmutableColumnsReadPolicy(s, a)
	if err != nil {
		return nil, err
	}
	return newNativeScalarQuerySessions(ctx, c, local, s, a, digest, p)
}
func nativeImmutableColumnsChildEnv(s nativeSelectorsSpec, a nativeSelectorsAdmission, reopen bool) ([]string, error) {
	if s.Family != nativeImmutableColumnsFamily || a.Spec != s || a.Family != nativeImmutableColumnsFamily {
		return nil, errInvalid
	}
	// Reuse the exact original run/deadline/transport builder, replacing only
	// the explicitly different declared profile and process selector.
	env, err := nativeCreateUpdateChildEnv(s, a, reopen)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(nativeImmutableColumnsCost(s))
	if err != nil {
		return nil, err
	}
	for i, value := range env {
		if strings.HasPrefix(value, "RHO_GRAPH_COST_SPEC=") {
			env[i] = "RHO_GRAPH_COST_SPEC=" + string(body)
		}
		if strings.HasPrefix(value, "RHO_GRAPH_CREATE_UPDATE_PROCESS=") {
			env[i] = "RHO_GRAPH_CREATE_UPDATE_PROCESS="
		}
	}
	return append(env, nativeImmutableColumnsProcessFlag+"=1", "RHO_GRAPH_FEED_RETAINED_PROCESS="), nil
}

func nativeScalarCreateUpdateChildEnv(s nativeSelectorsSpec, a nativeSelectorsAdmission, reopen bool) ([]string, error) {
	switch s.Family {
	case nativeCreateUpdateFamily:
		return nativeCreateUpdateChildEnv(s, a, reopen)
	case nativeImmutableColumnsFamily:
		return nativeImmutableColumnsChildEnv(s, a, reopen)
	case nativeLateCorrectionFamily:
		return nativeLateCorrectionChildEnv(s, a, reopen)
	default:
		return nil, errInvalid
	}
}

const nativeImmutableColumnsAccess = "actual complete caller operation through native EOF including retry/wait and callback IO; batched scan-based Points are not indexed latency"

type nativeImmutableColumnsEnd struct {
	Version                                             uint8
	Access                                              string
	Publications, Scans, ScanPoints, HeldRetireRefusals uint64
	SHA256                                              [32]byte
	DerivedRetention                                    string
}
type nativeImmutableColumnsEvent struct {
	Type, Stage, Access string
	Generation          [16]byte
	Cut                 []byte
	Rows                [3]uint64
	Cached, Fill        bool
	Nanos               int64
	Points              *[3]nativeImmutableColumnsPoint `json:",omitzero"`
}
type nativeImmutableColumnsObservations struct {
	f                *os.File
	digest           hash.Hash
	spec             nativeSelectorsSpec
	end              nativeImmutableColumnsEnd
	closed, finished bool
}

func newNativeImmutableColumnsObservations(directory string, spec nativeSelectorsSpec, admission [32]byte) (*nativeImmutableColumnsObservations, error) {
	if spec.Family != nativeImmutableColumnsFamily || nativeImmutableColumnsValidate(nativeImmutableColumnsCost(spec)) != nil || admission == ([32]byte{}) {
		return nil, errInvalid
	}
	return newNativeScalarColumnObservations(directory, spec, admission)
}
func newNativeScalarColumnObservations(directory string, spec nativeSelectorsSpec, admission [32]byte) (*nativeImmutableColumnsObservations, error) {
	format := nativeImmutableColumnsFormat
	if spec.Family == nativeLateCorrectionFamily {
		if nativeLateCorrectionValidate(nativeLateCorrectionCost(spec)) != nil {
			return nil, errInvalid
		}
		format = nativeLateCorrectionFormat
	} else if spec.Family != nativeImmutableColumnsFamily || nativeImmutableColumnsValidate(nativeImmutableColumnsCost(spec)) != nil {
		return nil, errInvalid
	}
	if admission == ([32]byte{}) {
		return nil, errInvalid
	}
	f, err := os.OpenFile(filepath.Join(directory, "column-observations.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	o := &nativeImmutableColumnsObservations{f: f, digest: sha256.New(), spec: spec, end: nativeImmutableColumnsEnd{Version: 1, Access: nativeImmutableColumnsAccess, DerivedRetention: "native Retire terminates pin/journal; derived chunk bytes retained and counted; no physical derived/source GC"}}
	err = o.write(struct {
		Type, Format    string
		Spec            nativeSelectorsSpec
		AdmissionSHA256 [32]byte
	}{"header", format, spec, admission})
	if err != nil {
		return nil, errors.Join(err, o.Close())
	}
	return o, nil
}
func (o *nativeImmutableColumnsObservations) write(v any) error {
	if o == nil || o.closed || o.finished {
		return errInvalid
	}
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(body) > 512<<10 {
		return errLimit
	}
	body = append(body, '\n')
	if _, err = o.f.Write(body); err != nil {
		return err
	}
	_, err = o.digest.Write(body)
	return err
}
func (o *nativeImmutableColumnsObservations) Close() error {
	if o == nil || o.closed {
		return nil
	}
	o.closed = true
	return o.f.Close()
}
func (o *nativeImmutableColumnsObservations) published(t *testing.T, stage string, pair *nativeGraphCostColumnPair, began time.Time) {
	t.Helper()
	if o.end.Publications >= 2 || stage != []string{"old-seal", "final-seal"}[o.end.Publications] {
		t.Fatal("exact fresh generation schedule")
	}
	cut, err := nativeFeedCutWire(pair.Final)
	if err != nil {
		t.Fatal(err)
	}
	rows := [3]uint64{3 + o.spec.Facts, o.spec.Facts, 0}
	if pair.Updated {
		updates := o.spec.Facts
		if o.spec.Family == nativeLateCorrectionFamily {
			updates /= 2
		}
		rows[0] += updates
		rows[2] = updates
	}
	if err := o.write(nativeImmutableColumnsEvent{Type: "Publish", Stage: stage, Access: nativeImmutableColumnsAccess, Generation: pair.ID, Cut: cut, Rows: rows, Nanos: time.Since(began).Nanoseconds()}); err != nil {
		t.Fatal(err)
	}
	o.end.Publications++
}
func (o *nativeImmutableColumnsObservations) read(t *testing.T, stage string, pair *nativeGraphCostColumnPair, at graphCut, cached, fill bool, began time.Time, rows [3]uint64, probes *nativeImmutableColumnsProbes) {
	t.Helper()
	if o.end.Scans >= 11 && probes == nil || o.end.ScanPoints >= 2 && probes != nil {
		t.Fatal("finite fresh column observations")
	}
	cut, err := nativeFeedCutWire(at)
	if err != nil {
		t.Fatal(err)
	}
	e := nativeImmutableColumnsEvent{Type: "Scan", Stage: stage, Access: nativeImmutableColumnsAccess, Generation: pair.ID, Cut: cut, Rows: rows, Cached: cached, Fill: fill}
	if probes != nil {
		value := int64(5)
		if pair.Updated {
			value = 6
		}
		points, err := probes.complete(value)
		if err != nil {
			t.Fatal(err)
		}
		e.Type, e.Points = "ScanPoints", &points
		o.end.ScanPoints++
	} else {
		o.end.Scans++
	}
	e.Nanos = time.Since(began).Nanoseconds()
	if err := o.write(e); err != nil {
		t.Fatal(err)
	}
}
func (o *nativeImmutableColumnsObservations) retireRefused(t *testing.T, ctx context.Context, clients [2]*nativeGraphEdgeUniqueColumnClient) {
	t.Helper()
	for _, c := range clients {
		if o.end.HeldRetireRefusals >= 2 {
			t.Fatal("finite held-reader refusal scope")
		}
		wire, err := nativeImmutableColumnsRetireInput(nativeImmutableColumnsRetireQuestion{Cookie: c.cookie})
		if err != nil {
			t.Fatal(err)
		}
		began := time.Now()
		r, actual := nativeGraphCostExchange(ctx, c.p, processRequest{op: processCostObserve, data: wire})
		if !errors.Is(actual, errLimit) {
			t.Fatal("real native Retire did not refuse live source", actual)
		}
		var observed nativeImmutableColumnsRetireObservation
		if nativeFeedDecodeJSON(r.aux, &observed) != nil || observed.Cookie != c.cookie || !nativeColumnSame(observed.Binding, c.binding) {
			t.Fatal("actual held-reader native journal")
		}
		for i := range observed.Before {
			if !bytes.Equal(observed.Before[i].Key, observed.After[i].Key) || !bytes.Equal(observed.Before[i].Value, observed.After[i].Value) {
				t.Fatal("refused Retire mutated native journal")
			}
		}
		raw, err := encodeProcessResponse(r)
		if err != nil {
			t.Fatal(err)
		}
		if err := o.write(struct {
			Type            string
			Member          uint64
			Input, Response []byte
			Nanos           int64
		}{"HeldRetireRefused", c.member, wire, raw, time.Since(began).Nanoseconds()}); err != nil {
			t.Fatal(err)
		}
		o.end.HeldRetireRefusals++
	}
}
func (o *nativeImmutableColumnsObservations) finish() (nativeImmutableColumnsEnd, error) {
	if o == nil || o.closed || o.finished {
		return nativeImmutableColumnsEnd{}, errInvalid
	}
	expected := [4]uint64{2, 11, 2, 2}
	if o.spec.Layout == "kv" {
		expected = [4]uint64{}
	}
	if [4]uint64{o.end.Publications, o.end.Scans, o.end.ScanPoints, o.end.HeldRetireRefusals} != expected {
		return nativeImmutableColumnsEnd{}, errCorrupt
	}
	copy(o.end.SHA256[:], o.digest.Sum(nil))
	// The footer seals only complete native operations. Its own bytes are not
	// recursively included in the sealed prefix digest.
	err := json.NewEncoder(o.f).Encode(struct {
		Type string
		End  nativeImmutableColumnsEnd
	}{"footer", o.end})
	o.finished = true
	return o.end, errors.Join(err, o.Close())
}

// A fixed-three consumer of genuine complete typed column rows. This is a
// scan-based Points operation, not an index or a substitute graph backend.
// Each delivered predecessor must match the last ACTUAL cell for that probe.
type nativeImmutableColumnsPoint struct {
	Owner              uint64
	Found              bool
	Presence, Property nativeColumnRow
}
type nativeImmutableColumnsProbes struct {
	spec   nativeGraphCostSpec
	points [3]nativeImmutableColumnsPoint
}

func newNativeImmutableColumnsProbes(s nativeGraphCostSpec) (*nativeImmutableColumnsProbes, error) {
	err := nativeImmutableColumnsValidate(s)
	if s.Version == 11 {
		err = nativeLateCorrectionValidate(s)
	}
	if err != nil {
		return nil, err
	}
	return &nativeImmutableColumnsProbes{spec: s, points: [3]nativeImmutableColumnsPoint{{Owner: 7}, {Owner: 7 + 2*(s.Facts-1)}, {Owner: 5}}}, nil
}
func (p *nativeImmutableColumnsProbes) accept(row nativeColumnRow) error {
	if p == nil {
		return errInvalid
	}
	if _, err := nativeGraphCostColumnRow(p.spec, row); err != nil {
		return err
	}
	for i := range p.points {
		probe := &p.points[i]
		if probe.Owner != row.Owner {
			continue
		}
		previous := &probe.Presence
		if row.Component == 3 {
			previous = &probe.Property
		} else if row.Component != 1 {
			return errCorrupt
		}
		if row.Before != previous.After {
			return errCorrupt
		}
		row.Axis, row.Scope = bytes.Clone(row.Axis), bytes.Clone(row.Scope)
		*previous = row
		probe.Found = probe.Presence.After.Present && probe.Property.After.Present
	}
	return nil
}
func (p *nativeImmutableColumnsProbes) complete(value int64) ([3]nativeImmutableColumnsPoint, error) {
	if p == nil || value != 5 && value != 6 {
		return [3]nativeImmutableColumnsPoint{}, errInvalid
	}
	for i, point := range p.points {
		if i == 2 {
			if point.Found || point.Presence.Owner != 0 || point.Property.Owner != 0 {
				return [3]nativeImmutableColumnsPoint{}, errCorrupt
			}
			continue
		}
		fact := (point.Owner - 7) / 2
		selected := nativeScalarCorrectedValue(p.spec, value, fact)
		ordinal := 4 + fact + boolUint(selected == 6)*p.spec.Facts
		if !point.Found || point.Presence.After.Handle != point.Owner+1 || point.Presence.After.Provenance != ordinal || point.Property.After.Value != selected || point.Property.After.Handle != uint64(selected) || point.Property.After.Provenance != ordinal {
			return [3]nativeImmutableColumnsPoint{}, errCorrupt
		}
	}
	return p.points, nil
}
