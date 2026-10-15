package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

const nativeContentionFamily = "contention-empty-range"
const nativeContentionFormat = "native-contention-empty-range-v1"
const nativeContentionMarker = "NCW1"

const (
	nativeContentionAttempts     = 64
	nativeContentionSubmissions  = 8
	nativeContentionCallers      = 32
	nativeContentionActive       = 2
	nativeContentionWholeSeconds = 365 * 24 * 60 * 60 // fresh admission ceiling, never a required run duration
	nativeContentionColumnPasses = 8                  // dependency prepass plus the seven actual publication/read passes
	nativeContentionOwnerStarts  = nativeContentionWholeSeconds/60 + 7 + 1 + 4 + 1
)

type nativeContentionSpec struct {
	Version      int    `json:"version"`
	Family       string `json:"family"`
	Facts        uint64 `json:"facts"`
	CrossPercent uint64 `json:"crossPercent"`
	Writers      int    `json:"writers"`
	Layout       string `json:"layout"`
}

// Immutable fresh run admission. Dataset identity excludes these execution
// choices; the actual source policy binds their exact canonical digest.
type nativeContentionAdmission struct {
	Version             uint8
	Spec                nativeContentionSpec
	Run                 [16]byte
	Transport           nativeWorkloadTransportSpec
	BudgetSeconds       uint64
	CausalMinimumMillis uint64
	DeadlineUnixNanos   int64
}

func nativeContentionStarts(seconds uint64) uint64 { return seconds/60 + 13 }
func nativeContentionFailedPages(seconds uint64) uint64 {
	return 4*nativeContentionColumnPasses + seconds/60 + 4
}

func nativeContentionCausalMinimum(spec nativeContentionSpec, transport nativeWorkloadTransportSpec) (uint64, error) {
	if spec.validate() != nil || transport.validate() != nil {
		return 0, errInvalid
	}
	// Seed and empty-ID waves are sequential in the actual driver. Hot steps
	// depend on the previous value on their actual local/remote target; the
	// longest of those two chains cannot overlap itself. Every such useful step
	// waits for at least one authenticated proposal round trip. This lower
	// bound deliberately omits other native work and promises no throughput.
	remote := spec.Facts * spec.CrossPercent / 100 // admitted operands <=1M/100
	steps := spec.Facts + max(remote, spec.Facts-remote) + 3
	return steps * transport.TargetRTTMillis, nil // admitted RTT<=100ms; <2^31ms
}

func (a nativeContentionAdmission) validate() error {
	floor, err := nativeContentionCausalMinimum(a.Spec, a.Transport)
	if err != nil || a.Version != 1 || a.Run == ([16]byte{}) || a.BudgetSeconds == 0 || a.BudgetSeconds > nativeContentionWholeSeconds || a.CausalMinimumMillis != floor || a.BudgetSeconds*1000 <= floor || a.DeadlineUnixNanos <= 0 {
		return errors.Join(errInvalid, err)
	}
	return nil
}

func (a nativeContentionAdmission) digest() ([32]byte, error) {
	if err := a.validate(); err != nil {
		return [32]byte{}, err
	}
	body, err := json.Marshal(a)
	if err != nil || len(body) > 4096 {
		return [32]byte{}, errors.Join(errLimit, err)
	}
	return sha256.Sum256(append([]byte("rho-tkg:native-contention-run-admission:v1\x00"), body...)), nil
}

func nativeContentionBudgetSecondsFromEnv() (uint64, error) {
	wire := os.Getenv("RHO_GRAPH_WORKLOAD_SECONDS")
	n, err := strconv.ParseUint(wire, 10, 64)
	if err != nil || n == 0 || n > nativeContentionWholeSeconds || strconv.FormatUint(n, 10) != wire {
		return 0, errInvalid
	}
	return n, nil
}

func nativeContentionAdmissionFromEnv(spec nativeContentionSpec, run [16]byte) (nativeContentionAdmission, error) {
	a := nativeContentionAdmission{Version: 1, Spec: spec, Run: run}
	var err error
	a.BudgetSeconds, err = nativeContentionBudgetSecondsFromEnv()
	if err != nil {
		return a, err
	}
	deadline, err := nativeContentionDeadlineFromEnv()
	if err != nil {
		return a, err
	}
	a.DeadlineUnixNanos = deadline.UnixNano()
	wire := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_TRANSPORT"))
	if len(wire) > 1024 || json.Unmarshal(wire, &a.Transport) != nil {
		return a, errInvalid
	}
	canonical, err := json.Marshal(a.Transport)
	if err != nil || !bytes.Equal(canonical, wire) {
		return a, errors.Join(errInvalid, err)
	}
	a.CausalMinimumMillis, err = nativeContentionCausalMinimum(spec, a.Transport)
	return a, errors.Join(err, a.validate())
}

func (s nativeContentionSpec) validate() error {
	if s.Version != 1 || s.Family != nativeContentionFamily || s.Facts < 20 || s.Facts > 1_000_000 || s.Facts%20 != 0 || s.CrossPercent != 0 && s.CrossPercent != 10 && s.CrossPercent != 50 && s.CrossPercent != 100 || s.Writers != 1 && s.Writers != 8 && s.Writers != 32 || s.Layout != "kv" && s.Layout != "columns" {
		return errInvalid
	}
	return nil
}

func (s nativeContentionSpec) competitors() uint64 { return uint64(max(2, s.Writers)) } // #nosec G115 -- admitted1/8/32.
func (s nativeContentionSpec) homeCount() uint64   { return 5*s.Facts + 64 }
func (s nativeContentionSpec) remoteNode() uint64  { return s.homeCount() + 1 }
func (s nativeContentionSpec) cross(i uint64) bool {
	return (i+1)*s.CrossPercent/100 > i*s.CrossPercent/100
}
func (s nativeContentionSpec) edge(fact uint64) (owner, life, target, targetLife uint64) {
	target, targetLife = 3, 4
	if s.cross(fact) {
		target, targetLife = s.remoteNode(), s.remoteNode()+1
	}
	return 7 + 2*fact, 8 + 2*fact, target, targetLife
}
func (s nativeContentionSpec) target(phase string, fact uint64) (uint64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	switch phase {
	case "seed":
		if fact < s.Facts/2 {
			return fact, nil
		}
	case "claim":
		if fact < s.Facts/2 {
			return s.Facts/2 + fact, nil
		}
	case "hot":
		if fact >= s.Facts {
			break
		}
		for candidate := uint64(0); candidate < 10; candidate++ {
			if s.cross(candidate) == s.cross(fact) {
				return candidate, nil
			}
		}
	}
	return 0, errInvalid
}

func (s nativeContentionSpec) ordinal(phase string, fact, claimant uint64) (uint64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	switch phase {
	case "setup":
		if fact < 3 && claimant == 0 {
			return fact + 1, nil
		}
	case "seed":
		if fact < s.Facts/2 && claimant == 0 {
			return 4 + fact, nil
		}
	case "hot":
		if fact < s.Facts && claimant == 0 {
			return 4 + s.Facts/2 + fact, nil
		}
	case "claim":
		if fact < s.Facts/2 && claimant < s.competitors() {
			return 4 + 3*s.Facts/2 + fact*s.competitors() + claimant, nil
		}
	}
	return 0, errInvalid
}

func (s nativeContentionSpec) valueID(value int64) (uint64, error) {
	if value < 5 || value > int64(s.Facts)+5 {
		return 0, errInvalid
	} // #nosec G115 -- admitted<=1M.
	return 4*s.Facts + 32 + uint64(value), nil // #nosec G115 -- checked positive value within explicit HOME ID domain.
}

// Declared loop proof, not a measured record/byte count. Each useful hot step
// admits at most two competing originals plus one priority original AFTER both
// actual refusals/ABORTs. Each empty-ID wave has the same bound; late frozen
// callers encounter that SAME stale complete prefix before registration. Seed
// writes add one original each. Unknown replies retry the SAME original at most
// eight times; each invocation has at most six Register/Prepare/Decide/Resolve
// submissions, plus at most three native capacity-help submissions and one
// retried Prepare. Physical-reopen replay is of ONE original, not every logical
// operation, and admits at most eight invocations with two Resolve submissions.
// Fresh capture has four native cut controls per complete two-source cohort;
// at most two hot cohorts/useful step and two empty-ID cohorts/wave occur. Seed
// cohorts and all finite retained-owner lifecycle invocations are included too.
func (s nativeContentionSpec) retainedFrames() (uint64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	originals := 5*s.Facts + 3
	// At captures append no controls. Recovered live Publish authenticates and
	// skips installed originals; source expiry is not fresh log credit. Only
	// the four actual generation lifecycles and finite uncertain/terminal calls
	// contribute this separate conservative control allowance.
	controls := uint64(1024 * 32)
	return 16384 + originals*nativeContentionSubmissions*10 + 14*s.Facts + controls + nativeContentionSubmissions*2, nil
}

// Same immutable contiguous retain-all admission used by the child bootstrap
// and fresh column policy. Three native application records exist per frame;
// sparse AS7/collected profiles may not reuse this proof.
func (s nativeContentionSpec) applicationAdmission() (frames, records, bytes uint64, err error) {
	frames, err = s.retainedFrames()
	if err != nil || frames > (^uint64(0)-65536)/259 {
		return 0, 0, 0, errors.Join(errLimit, err)
	}
	// Every admitted native entry can install at most256 keyed writes plus its
	// three Root/Outcome/CDC frames. Quotas allocate no corresponding row map.
	// Retained bytes are an explicit logical resource policy, NOT theoretical
	// decoded-work grants or physical disk/RSS. Both banks fit unchanged1TiB cap;
	// reaching this finite policy remains a reported resource refusal.
	return frames, 259*frames + 65536, (64 << 20) + s.Facts*(512<<10), nil
}

// This separate fresh profile leaves ordinary COST, P05 and the compact durable
// column diagnostic profiles unchanged. Quotas allocate no history/RSS buffer.
func nativeContentionProfile(s *nativeContentionSpec, init *declaredInitCommand, l *raftlog.Limits, p *raftlog.ApplicationPolicy, generations *raftlog.ApplicationGenerationLimits) error {
	if s == nil {
		return nil
	}
	if init == nil || l == nil || p == nil || generations == nil {
		return errInvalid
	}
	if p.MaxInstallWrites != 256 || p.MaxImageBytes != 172 {
		return errInvalid
	}
	_, records, bytes, err := s.applicationAdmission()
	if err != nil {
		return err
	}
	// Three genuine application records per retained index, plus native keyed
	// graph/control state. No sparse/collected-source frame count is inferred.
	init.maxBlock = min(s.homeCount(), idalloc.MaxBlockSize)
	l.MaxRetainedBytes, l.MaxRetainedEntries = bytes, 1_000_000
	p.RetainedApplicationBytes, p.RetainedApplicationRecords = bytes, records
	*generations = raftlog.ApplicationGenerationLimits{MaxBytes: 2 * bytes, MaxRecords: 2 * records}
	return nil
}

func nativeContentionColumnsPolicy(h *allocationHost, spec nativeContentionSpec, admission nativeContentionAdmission) (nativeColumnPolicy, error) {
	if h == nil || h.machine == nil || h.machine.SemanticContractID() != durableColumnGraphSemanticContractID() || !h.machine.retainedPins() || spec.validate() != nil {
		return nativeColumnPolicy{}, errInvalid
	}
	frames, records, bytes, err := spec.applicationAdmission()
	application := h.machine.store.ApplicationLimits()
	if err != nil || application.MaxInstallWrites != 256 || application.MaxImageBytes != 172 || application.RetainedApplicationRecords != records || application.RetainedApplicationBytes != bytes {
		return nativeColumnPolicy{}, errors.Join(errInvalid, err)
	}
	if admission.Spec != spec || admission.validate() != nil {
		return nativeColumnPolicy{}, errInvalid
	}
	p, err := nativeContentionColumnAllowance(application, h.machine.limits, frames, admission.BudgetSeconds)
	if err == nil {
		p.Admission, err = admission.digest()
	}
	return p, err
}

// Whole-generation grants are additive: every successful page of every actual
// pass, plus a finite global failed/unknown page pool. They do not promise four
// lost replies at EVERY page. Ordinary invocation caps and SAME-sequence replay
// remain unchanged. Actual full-TTL expiry is the only renewal accounting event;
// short calls/cancellation consume the separate failed-construction allowance.
func nativeContentionColumnAllowance(application raftlog.ApplicationPolicy, l materializerLimits, frames, seconds uint64) (nativeColumnPolicy, error) {
	p := nativeColumnPolicy{Version: 1, ChunkRecords: 16, ChunkRows: 16, ChunkBytes: 64 << 10, FrameBytes: 32 << 10, MaxGenerations: 2, MaxChunks: 1 << 40, MaxRetainedBytes: 1 << 62, MaxWorkRecords: ^uint64(0), MaxWorkBytes: ^uint64(0), MaxDecodedRows: ^uint64(0), MaxOutputBytes: ^uint64(0), MaxRPCs: ^uint64(0), MaxWireBytes: ^uint64(0), CacheBytes: 2 << 20, MemTableBytes: 1 << 20, Lifetime: time.Minute}
	if seconds == 0 || seconds > nativeContentionWholeSeconds {
		return p, errInvalid
	}
	opening, grant, err := nativeColumnLimitAllowance(application, l, p)
	if err != nil {
		return p, err
	}
	opening, err = opening.scaled(2)
	if err != nil {
		return p, err
	}
	opening, err = nativeColumnPeerOpening(opening, l)
	if err != nil {
		return p, err
	}
	// This fresh factory has a separate complete issued-entry ceiling. Do not
	// turn its separately funded maximum keyed writes into fictitious log frames.
	starts := nativeContentionStarts(seconds)
	failedPages := nativeContentionFailedPages(seconds)
	if frames == 0 || frames >= 1<<39 || frames+1 > (^uint64(0)-failedPages)/nativeContentionColumnPasses {
		return p, errLimit
	}
	pages, err := grant.scaled((frames+1)*nativeContentionColumnPasses + failedPages)
	if err != nil {
		return p, err
	}
	// Every cursor renewal funds BOTH the replacement source capture and its
	// separate same-predicate At authentication before rebinding the private
	// cursor. Four real local generations can each require both effects.
	captures, err := opening.scaled(8*starts + 4*nativeContentionColumnPasses)
	if err != nil {
		return p, err
	}
	// Resume/Publish and the authentic live retained Open are distinct funded
	// invocations on each renewal. Neither source nor durable work is refunded.
	retention, err := nativeColumnRetentionWork(l).scaled(8*starts + 32)
	if err != nil {
		return p, err
	}
	budget, err := pages.plus(captures)
	if err != nil {
		return p, err
	}
	budget, err = budget.plus(retention)
	if err != nil {
		return p, err
	}
	p.MaxWorkRecords, p.MaxWorkBytes, p.MaxDecodedRows, p.MaxOutputBytes, p.MaxRPCs, p.MaxWireBytes = budget.Records, budget.Bytes, budget.DecodedRows, budget.OutputBytes, budget.RPCs, budget.WireBytes
	p.MaxChunks = 2 * frames
	perChunk := uint64(p.ChunkBytes + 128)
	if frames > ((1<<62)-8*nativeColumnMetadataBytes)/(2*perChunk) {
		return p, errLimit
	}
	p.MaxRetainedBytes = 8*nativeColumnMetadataBytes + 2*frames*perChunk
	return p, p.validate()
}

func nativeContentionSpecFromEnv() (*nativeContentionSpec, [16]byte, error) {
	var run [16]byte
	selector := os.Getenv("RHO_GRAPH_NATIVE_WORKLOAD")
	if selector == "" {
		return nil, run, nil
	}
	if selector != nativeContentionFormat {
		return nil, run, errInvalid
	}
	for _, name := range []string{"RHO_GRAPH_COST_SPEC", "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_NATIVE_FAULT_PROCESS", "RHO_GRAPH_FAULT_FAMILY_PROCESS", "RHO_GRAPH_DURABLE_MODEL", "RHO_GRAPH_DECISION_FAULT_PROCESS"} {
		if value := os.Getenv(name); value != "" && value != "0" && value != "off" {
			return nil, run, errInvalid
		}
	}
	raw := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_SPEC"))
	if len(raw) == 0 || len(raw) > 1024 {
		return nil, run, errInvalid
	}
	var spec nativeContentionSpec
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&spec); err != nil {
		return nil, run, errors.Join(errInvalid, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, run, errors.Join(errInvalid, err)
	}
	canonical, err := json.Marshal(spec)
	if err != nil || !bytes.Equal(canonical, raw) || spec.validate() != nil {
		return nil, run, errors.Join(errInvalid, err)
	}
	runText := os.Getenv("RHO_GRAPH_WORKLOAD_RUN")
	id, err := hex.DecodeString(runText)
	if err != nil || len(id) != len(run) || hex.EncodeToString(id) != runText {
		return nil, run, errInvalid
	}
	copy(run[:], id)
	if run == ([16]byte{}) {
		return nil, run, errInvalid
	}
	return &spec, run, nil
}

func nativeContentionDeadlineFromEnv() (time.Time, error) {
	seconds, err := nativeContentionBudgetSecondsFromEnv()
	if err != nil {
		return time.Time{}, err
	}
	wire := os.Getenv("RHO_GRAPH_WORKLOAD_DEADLINE")
	n, err := strconv.ParseInt(wire, 10, 64)
	if err != nil || n <= 0 || strconv.FormatInt(n, 10) != wire {
		return time.Time{}, errInvalid
	}
	deadline := time.Unix(0, n)
	remaining := time.Until(deadline)
	// seconds<=365d: Duration/nanos and all32 simultaneous caller timing
	// observations fit int64. A physical restart retains this exact deadline.
	if remaining <= 0 || remaining > time.Duration(seconds)*time.Second { // #nosec G115 -- admitted <=365d.
		return time.Time{}, errInvalid
	}
	return deadline, nil
}

func nativeContentionGrantPlan(s nativeContentionSpec, block, partition, sequence uint64) (first, count uint64, err error) {
	if s.validate() != nil || block < 2 || block > idalloc.MaxBlockSize || sequence == 0 {
		return 0, 0, errInvalid
	}
	switch partition {
	case 3:
		if sequence > 8 || (sequence-1)*block >= s.homeCount() {
			return 0, 0, errInvalid
		}
		first = 1 + (sequence-1)*block
		return first, min(block, s.homeCount()-first+1), nil
	case 8:
		if sequence == 1 {
			return s.remoteNode(), 2, nil
		}
	}
	return 0, 0, errInvalid
}

func nativeContentionGrantParameters(o *certifiedProcessOwners, s nativeContentionSpec, data []byte) (uint64, uint64, error) {
	if o == nil || o.host == nil || len(data) != 16 {
		return 0, 0, errInvalid
	}
	cfg, err := o.host.currentConfiguration()
	if err != nil {
		return 0, 0, err
	}
	partition := uint64(3)
	if o.reserve.ID == ([16]byte{2}) {
		partition = 8
	} else if o.reserve.ID != ([16]byte{1}) {
		return 0, 0, errInvalid
	}
	sequence, count := binary.BigEndian.Uint64(data[:8]), binary.BigEndian.Uint64(data[8:])
	_, want, err := nativeContentionGrantPlan(s, cfg.maxBlock, partition, sequence)
	if err != nil || count != want {
		return 0, 0, errors.Join(errInvalid, err)
	}
	return sequence, count, nil
}

type nativeContentionCookie struct {
	Owner   uint64
	Request [16]byte
}
type nativeContentionJob struct {
	Phase           string
	Fact, Claimant  uint64
	Client, Attempt int
	Previous        [16]byte
}
type nativeContentionOperation struct {
	Dataset                 [32]byte
	Family, Phase           string
	Fact, Claimant, Ordinal uint64
}

func (s nativeContentionSpec) operation(job nativeContentionJob) (nativeContentionOperation, error) {
	ordinal, err := s.ordinal(job.Phase, job.Fact, job.Claimant)
	if err != nil || job.Client < 0 || job.Client >= s.Writers || job.Attempt < 1 || job.Attempt > nativeContentionAttempts || (job.Attempt == 1) != (job.Previous == ([16]byte{})) {
		return nativeContentionOperation{}, errors.Join(errInvalid, err)
	}
	// Dataset deliberately excludes engine/writers/layout/family/RTT/run. Family
	// is a separate immutable operation coordinate, matching the fresh FDB grammar.
	var dataset [25]byte
	dataset[0] = 1
	binary.BigEndian.PutUint64(dataset[1:9], s.Facts)
	binary.BigEndian.PutUint64(dataset[9:17], s.CrossPercent)
	copy(dataset[17:], "graph-v2")
	return nativeContentionOperation{sha256.Sum256(dataset[:]), nativeContentionFamily, job.Phase, job.Fact, job.Claimant, ordinal}, nil
}

type nativeContentionCapture struct {
	Cookie                                  nativeContentionCookie
	Cut, Condition                          []byte
	Target, Life                            uint64
	Cell                                    nativeLifecycleCell
	Component                               []byte
	Range                                   graphstate.KeyPredicate
	Keys                                    []graphstate.ComponentKey
	RangeComplete, EntityAbsent, LifeAbsent bool
}
type nativeContentionFrozen struct {
	Job             nativeContentionJob
	Operation       nativeContentionOperation
	Request         [16]byte
	Business        []byte
	BodySHA256      [32]byte
	Capture         nativeContentionCapture
	ops             []graphstate.Operation
	claims          []freshBinding
	busy            bool
	calls           uint8
	terminal        []byte
	terminalAction  string
	replayed        bool
	factoryReplayed bool
	resume          crossProcessResume
}
type nativeContentionControl struct {
	Action              string
	Cookie              nativeContentionCookie
	Job                 nativeContentionJob
	Slot                uint64
	Cut, Column, Resume []byte
	Initial, Final      []byte
	Sequence            uint64
}
type nativeContentionReply struct {
	Run                                      [16]byte
	Cookie                                   nativeContentionCookie
	Slot                                     uint64
	Job                                      nativeContentionJob
	Operation                                nativeContentionOperation
	Request                                  [16]byte
	Business                                 []byte
	BodySHA256                               [32]byte
	Capture                                  nativeContentionCapture
	Result, Resume                           []byte
	Proof                                    *nativeGraphCostDecisionProof `json:",omitzero"`
	Class, Error                             string
	Calls                                    uint8
	Column                                   []byte
	Creation                                 *nativeColumnCreationFailure `json:",omitzero"`
	SourceWork                               graphstore.PageWork
	AdmittedSourceCalls, AdmittedSourceBytes int
	SourceOutputRemaining                    int
	DeadlineUnixNanos                        int64
	Source                                   *nativeContentionSourceReceipt `json:",omitzero"`
}
type nativeContentionSessions struct {
	mu                     sync.Mutex
	gate                   graphRequestGate
	ctx                    context.Context
	coordinator            *crossProcessCoordinator
	owners                 *certifiedProcessOwners
	columns                *nativeGraphCostColumnSessions
	spec                   nativeContentionSpec
	run                    [16]byte
	read                   *conditionalGraphRead
	condition              graphReadCondition
	cut                    graphCut
	cookie                 nativeContentionCookie
	conditionWire, cutWire []byte
	cancel                 context.CancelFunc
	next                   uint64
	busy, pending          int
	columnProbes           uint8
	frozen                 [nativeContentionCallers]*nativeContentionFrozen
	previous               [nativeContentionCallers]nativeContentionPrevious
	source                 *nativeContentionSourceOwner
	templates              [2]nativeContentionReadTemplate
	phase                  string
}

type nativeContentionPrevious struct {
	Job     nativeContentionJob
	Request [16]byte
	Class   string
}

type nativeContentionReadTemplate struct {
	phase   string
	owner   uint64
	capture nativeContentionCapture
	ops     []graphstate.Operation
	claims  []freshBinding
}

func nativeContentionInput(q nativeContentionControl) ([]byte, error) {
	body, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}
	if len(body) > 32<<10 {
		return nil, errLimit
	}
	return append([]byte(nativeContentionMarker), body...), nil
}
func decodeNativeContentionInput(body []byte) (q nativeContentionControl, err error) {
	if !bytes.HasPrefix(body, []byte(nativeContentionMarker)) || len(body) > 32<<10+len(nativeContentionMarker) {
		return q, errInvalid
	}
	d := json.NewDecoder(bytes.NewReader(body[len(nativeContentionMarker):]))
	d.DisallowUnknownFields()
	if err = d.Decode(&q); err != nil {
		return q, errors.Join(errInvalid, err)
	}
	if err = d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return q, errors.Join(errInvalid, err)
	}
	canonical, err := json.Marshal(q)
	if err != nil || !bytes.Equal(canonical, body[len(nativeContentionMarker):]) {
		return q, errors.Join(errInvalid, err)
	}
	switch q.Action {
	case "open", "close", "freeze", "frozen", "run", "resume", "columns", "column-owner", "records-open", "records-next", "records-close":
	default:
		return q, errInvalid
	}
	return q, nil
}

func (s *nativeContentionSessions) open(ctx context.Context) (reply nativeContentionReply, err error) {
	if !s.gate.tryLock() {
		return reply, errLimit
	}
	defer s.gate.unlock()
	s.mu.Lock()
	if s.read != nil || s.source != nil || s.pending != 0 || s.busy != 0 || s.next == ^uint64(0) {
		s.mu.Unlock()
		return reply, errLimit
	}
	s.pending++
	s.next++
	sequence := s.next
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.pending--; s.mu.Unlock() }()
	// One fixed cohort deadline covers capture, all32 factories and both Run
	// slots. Its SAME condition/source ledger is never reset between callers.
	owner, cancel := context.WithTimeout(s.ctx, 45*time.Second)
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	var read *conditionalGraphRead
	defer func() {
		if err != nil {
			cancel()
			if read != nil {
				err = errors.Join(err, read.Close())
			}
		}
	}()
	read, err = s.coordinator.OpenForUpdate(owner, graphReadRequest{mode: graphReadFresh})
	if err != nil {
		return reply, err
	}
	condition, err := read.Condition()
	if err != nil {
		return reply, err
	}
	cut, err := read.Cut()
	if err != nil {
		return reply, err
	}
	conditionWire, err := encodeGraphReadConditionValue(condition.value, s.owners.host.machine.limits)
	if err != nil {
		return reply, err
	}
	cutWire, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: graphReadAt, cut: cut})
	if err != nil {
		return reply, err
	}
	nonce, err := graphReadNonce()
	if err != nil {
		return reply, err
	}
	cookie := nativeContentionCookie{sequence, nonce}
	if err := errors.Join(ctx.Err(), owner.Err()); err != nil {
		return reply, err
	}
	s.mu.Lock()
	s.read, s.condition, s.cut, s.cookie, s.cancel = read, condition, cut, cookie, cancel
	s.cutWire, s.conditionWire = cutWire, conditionWire
	clear(s.frozen[:])
	clear(s.templates[:])
	s.phase = ""
	s.mu.Unlock()
	deadline, _ := owner.Deadline()
	return nativeContentionReply{Cookie: cookie, Class: "captured-native-prefix", Capture: nativeContentionCapture{Cookie: cookie, Cut: bytes.Clone(cutWire), Condition: bytes.Clone(conditionWire)}, DeadlineUnixNanos: deadline.UnixNano()}, nil
}

func (s *nativeContentionSessions) cohortReceipt(read *conditionalGraphRead, reply *nativeContentionReply) {
	reply.SourceWork = read.read.Work()
	read.budget.mu.Lock()
	reply.AdmittedSourceCalls, reply.AdmittedSourceBytes = read.budget.calls, read.budget.bytes
	reply.SourceOutputRemaining = read.budget.output.Remaining()
	read.budget.mu.Unlock()
	if deadline, ok := read.ctx.Deadline(); ok {
		reply.DeadlineUnixNanos = deadline.UnixNano()
	}
}

func (s *nativeContentionSessions) closeCohort(cookie nativeContentionCookie, force bool) (nativeContentionReply, error) {
	if !s.gate.tryLock() {
		return nativeContentionReply{}, errLimit
	}
	defer s.gate.unlock()
	s.mu.Lock()
	if s.read == nil {
		s.mu.Unlock()
		if force {
			return nativeContentionReply{}, nil
		}
		return nativeContentionReply{}, errInvalid
	}
	if !force && (s.cookie != cookie || s.busy != 0 || s.pending != 0) {
		s.mu.Unlock()
		return nativeContentionReply{}, errLimit
	}
	if !force {
		for _, frozen := range s.frozen {
			if frozen != nil && frozen.calls != 0 && len(frozen.terminal) == 0 {
				s.mu.Unlock()
				return nativeContentionReply{}, errCrossProcessPending
			}
		}
	}
	read, cancel, actual := s.read, s.cancel, s.cookie
	s.read = nil
	s.mu.Unlock()
	cancel()
	err := read.Close()
	reply := nativeContentionReply{Cookie: actual, Class: "closed-native-prefix"}
	s.cohortReceipt(read, &reply)
	return reply, err
}

func (s *nativeContentionSessions) runFrozen(ctx context.Context, q nativeContentionControl) (reply nativeContentionReply, err error) {
	s.mu.Lock()
	if s.read == nil || s.cookie != q.Cookie || q.Slot == 0 || q.Slot > nativeContentionCallers || s.pending != 0 || s.busy >= nativeContentionActive {
		s.mu.Unlock()
		return reply, errLimit
	}
	frozen := s.frozen[q.Slot-1]
	if frozen == nil || frozen.busy {
		s.mu.Unlock()
		return reply, errLimit
	}
	if len(frozen.terminal) != 0 {
		if frozen.replayed || q.Action != frozen.terminalAction {
			s.mu.Unlock()
			return reply, errLimit
		}
		// The factory prepaid one bounded replay of this exact completed slot.
		// No replay runs native work or grants another original invocation.
		frozen.replayed = true
		body := bytes.Clone(frozen.terminal)
		s.mu.Unlock()
		if err := json.Unmarshal(body, &reply); err != nil {
			return reply, errCorrupt
		}
		return reply, nil
	}
	if frozen.calls >= nativeContentionSubmissions {
		s.mu.Unlock()
		return reply, errLimit
	}
	frozen.busy = true
	frozen.calls++
	s.busy++
	read, condition := s.read, s.condition
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		frozen.busy = false
		s.busy--
		s.mu.Unlock()
	}()
	reply = nativeContentionReply{Cookie: q.Cookie, Slot: q.Slot, Job: frozen.Job, Operation: frozen.Operation, Request: frozen.Request, BodySHA256: frozen.BodySHA256, Calls: frozen.calls}
	// Only actual retained originals recover an unknown result. The private
	// typed operations/claims never change on repeated invocations of this slot.
	var result crossProcessResult
	var runErr error
	if q.Action == "resume" {
		if frozen.resume.request == (requestID{}) {
			return reply, errInvalid
		}
		result, runErr = s.coordinator.ResumeConditional(ctx, frozen.resume)
	} else {
		result, runErr = s.coordinator.RunIfUnchanged(ctx, requestID(frozen.Request), frozen.ops, frozen.Operation.Ordinal, frozen.claims, condition)
	}
	if result.resume.request != (requestID{}) {
		frozen.resume = result.resume
		reply.Resume, err = encodeP05Resume(result.resume)
		if err != nil {
			return reply, err
		}
	}
	reply.Result, err = encodeGraphProcessCoordinatorResult(result, runErr)
	if err != nil {
		return reply, err
	}
	if runErr != nil {
		reply.Error = runErr.Error()
		if q.Action == "run" && errors.Is(runErr, errGraphReadConflict) && !errors.Is(runErr, errCrossProcessPending) {
			reply.Class = "pre-prepare-stale"
		} else if errors.Is(runErr, errCrossProcessPending) || nativeGraphCostRetrySame(runErr) {
			reply.Class = "pending-or-admission"
		} else {
			reply.Class = "unavailable"
		}
	} else {
		if result.decision.phase != crossDecide || result.decision.tx != result.resume.tx || result.decision.commit && result.decision.round == 0 {
			return reply, errCorrupt
		}
		proofWire, err := nativeGraphCostCoordinatorProofWire(ctx, s.owners.host, result)
		if err != nil {
			return reply, err
		}
		var proof nativeGraphCostDecisionProof
		if err := json.Unmarshal(proofWire, &proof); err != nil {
			return reply, errors.Join(errCorrupt, err)
		}
		reply.Proof = &proof
		reply.Class = "native-abort"
		if result.decision.commit {
			reply.Class = "commit"
		}
	}
	s.cohortReceipt(read, &reply)
	if reply.Class == "commit" || reply.Class == "native-abort" || reply.Class == "pre-prepare-stale" {
		body, err := json.Marshal(reply)
		if err != nil || len(body) > 32<<10 {
			return reply, errors.Join(errLimit, err)
		}
		s.mu.Lock()
		frozen.terminal = body
		frozen.terminalAction = q.Action
		s.previous[q.Slot-1] = nativeContentionPrevious{frozen.Job, frozen.Request, reply.Class}
		s.mu.Unlock()
	}
	return reply, nil
}

// A claim is selected only from a genuine installed grant in this actual Host.
// A sequence calculation or serialized freshBinding supplies no native proof.
func nativeContentionFreshGrant(o *certifiedProcessOwners, spec nativeContentionSpec, id uint64) (grantReference, error) {
	if o == nil || o.host == nil || o.recipient == nil || id == 0 {
		return grantReference{}, errInvalid
	}
	cfg, err := o.host.currentConfiguration()
	if err != nil {
		return grantReference{}, err
	}
	if cfg.maxBlock == 0 {
		return grantReference{}, errCorrupt
	}
	sequence := uint64(1)
	if o.host.machine.ns.partition == 3 {
		sequence = (id-1)/cfg.maxBlock + 1
	}
	first, count, err := nativeContentionGrantPlan(spec, cfg.maxBlock, o.host.machine.ns.partition, sequence)
	if err != nil || id < first || id-first >= count {
		return grantReference{}, errors.Join(errInvalid, err)
	}
	q := allocationQuery{kind: observeGrant, session: o.recipient.session, sequence: sequence, count: count, nonce: [16]byte{1}}
	actual, err := o.host.machine.observeAllocation(q, o.host.driver.Applied())
	if err != nil {
		return grantReference{}, err
	}
	g := actual.grant
	if g.installed == 0 || g.grant.Request.Session != q.session || g.grant.Request.Sequence != sequence || g.grant.Reservation.First != first || g.grant.Reservation.Last != first+count-1 || !g.publication.Contains(id) {
		return grantReference{}, errCorrupt
	}
	return grantReference{session: g.grant.Request.Session, sequence: g.grant.Request.Sequence}, nil
}

func (s *nativeContentionSessions) freeze(ctx context.Context, q nativeContentionControl, sequence uint64) (reply nativeContentionReply, err error) {
	if !s.gate.tryLock() {
		return reply, errLimit
	}
	defer s.gate.unlock()
	operation, err := s.spec.operation(q.Job)
	if err != nil {
		return reply, err
	}
	s.mu.Lock()
	if s.read == nil || s.cookie != q.Cookie || s.busy != 0 || s.pending != 0 || q.Slot == 0 || q.Slot > nativeContentionCallers || s.frozen[q.Slot-1] != nil {
		s.mu.Unlock()
		return reply, errInvalid
	}
	if s.phase != "" && s.phase != q.Job.Phase {
		s.mu.Unlock()
		return reply, errInvalid
	}
	s.phase = q.Job.Phase
	if q.Job.Attempt > 1 {
		previous := s.previous[q.Slot-1]
		before, after := previous.Job, q.Job
		if previous.Request != after.Previous || before.Phase != after.Phase || before.Fact != after.Fact || before.Claimant != after.Claimant || before.Client != after.Client || before.Attempt != after.Attempt-1 || previous.Class != "native-abort" && previous.Class != "pre-prepare-stale" {
			s.mu.Unlock()
			return reply, errCrossProcessPending
		}
	}
	read, condition, cookie := s.read, s.condition, s.cookie
	cutWire, conditionWire := s.cutWire, s.conditionWire
	s.mu.Unlock()
	if err := read.active(); err != nil {
		return reply, err
	}
	// Own the actual factory/capture/retained slot before decoder/query allocation.
	// This slot retains typed operations/claims, its bounded factory receipt,
	// one terminal receipt and one replay copy. Admit ownership before reads.
	if err := reserveConditionBudget(read.budget, 256<<10); err != nil {
		return reply, err
	}
	view, err := read.ReadView()
	if err != nil {
		return reply, err
	}
	h := s.owners.host
	cfg, err := h.currentConfiguration()
	if err != nil {
		return reply, err
	}
	window, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return reply, err
	}
	point, err := temporal.RationalPosition(cfg.defaultAxis.Axis(), temporal.RationalInt64(0))
	if err != nil {
		return reply, err
	}
	capture := nativeContentionCapture{Cookie: cookie, Cut: bytes.Clone(cutWire), Condition: bytes.Clone(conditionWire)}
	var ops []graphstate.Operation
	var claims []freshBinding
	cached := false
	if q.Job.Phase == "setup" {
		owner := uint64(1)
		if q.Job.Fact == 1 {
			owner = 3
		} else if q.Job.Fact == 2 {
			owner = s.spec.remoteNode()
		}
		ops = []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: graphstate.EntityID(owner), Life: graphstate.LifeID(owner + 1), Scope: window, Record: graphstate.EntityRecord{Interpretation: graphstate.InterpretationObservation, TemporalRole: graphstate.TemporalRoleValidity}}}
		claims = []freshBinding{{role: entityBinding, id: owner}, {role: lifeBinding, owner: owner, id: owner + 1}}
	} else {
		target, err := s.spec.target(q.Job.Phase, q.Job.Fact)
		if err != nil {
			return reply, err
		}
		owner, life, endpoint, endpointLife := s.spec.edge(target)
		// One genuine certified read owner serves at most two actual hot targets
		// or one empty-ID target. Retain those immutable typed read results under
		// this SAME cookie/predicate; each independent caller still obtains its
		// own actual Host factory/original. This bounds SDK reads under the real
		// one-minute source lifetime without32 duplicate source traversals.
		for _, template := range s.templates {
			if template.phase == q.Job.Phase && template.owner == owner {
				capture = template.capture
				ops = append([]graphstate.Operation(nil), template.ops...)
				claims = append([]freshBinding(nil), template.claims...)
				cached = true
				break
			}
		}
		if !cached {
			capture.Target, capture.Life = owner, life
			value := int64(5)
			if q.Job.Phase == "hot" {
				key := graphstate.ComponentKey{Owner: graphstate.EntityID(owner), Life: graphstate.LifeID(life), Kind: graphstate.ScalarProperty, Name: "p"}
				page, err := view.ComponentPage(ctx, graphstate.ComponentQuery{Key: key, Window: window}, 0, graphstate.ReadBudget{Rows: 16, Bytes: 32 << 10})
				if err != nil || !page.Complete || page.Next != 0 {
					return reply, errors.Join(errLimit, err)
				}
				capture.Component, err = state.AppendState(nil, page.Data, state.CodecLimits{State: h.machine.limits.graph.Planner.Component, MaxEncodedBytes: 32 << 10})
				if err != nil {
					return reply, err
				}
				cell, err := page.Data.At(point, h.machine.limits.graph.Planner.Component)
				if err != nil {
					return reply, err
				}
				capture.Cell, err = nativeLifecycleCellFromState(ctx, view, key, cell)
				if err != nil || !capture.Cell.Present || capture.Cell.Null || capture.Cell.I64 < 5 || capture.Cell.I64 >= int64(s.spec.Facts)+5 {
					return reply, errors.Join(errCorrupt, err)
				} // #nosec G115 -- admitted<=1M.
				value = capture.Cell.I64 + 1
			} else {
				capture.Range = graphstate.KeyPredicate{Owner: graphstate.EntityID(owner)}
				keys, err := view.ComponentKeys(ctx, capture.Range, 0, graphstate.ReadBudget{Rows: 2, Bytes: 4096})
				if err != nil || !keys.Complete || keys.Next != 0 {
					return reply, errors.Join(errLimit, err)
				}
				entity, err := view.Entity(ctx, graphstate.EntityID(owner))
				if err != nil {
					return reply, err
				}
				actualLife, err := view.Life(ctx, graphstate.EntityID(owner), graphstate.LifeID(life))
				if err != nil {
					return reply, err
				}
				capture.Keys, capture.RangeComplete, capture.EntityAbsent, capture.LifeAbsent = keys.Keys, keys.Complete, !entity.Found, !actualLife.Found
				if len(keys.Keys) != 0 || entity.Found || actualLife.Found {
					return reply, errGraphReadConflict
				}
				ops = append(ops, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: graphstate.EntityID(owner), Life: graphstate.LifeID(life), Scope: window, Record: graphstate.EntityRecord{Type: "edge", Source: 1, Target: graphstate.EntityID(endpoint), Mode: graphstate.LifeBound, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleOccurrenceTime}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: graphstate.LifeID(endpointLife)}})
				claims = append(claims, freshBinding{role: entityBinding, id: owner}, freshBinding{role: lifeBinding, owner: owner, id: life})
			}
			valueID, err := s.spec.valueID(value)
			if err != nil {
				return reply, err
			}
			definition, err := view.Value(ctx, graphstate.ValueID(valueID))
			if err != nil {
				return reply, err
			}
			if definition.Found {
				actual, ok := definition.Value.Int64Value()
				if !ok || actual != value {
					return reply, errCorrupt
				}
			} else {
				claims = append(claims, freshBinding{role: valueBinding, id: valueID})
			}
			ops = append(ops, graphstate.Operation{Kind: graphstate.Set, Owner: graphstate.EntityID(owner), Life: graphstate.LifeID(life), Name: "p", Scope: window, Value: graphstate.I64(value), ValueID: graphstate.ValueID(valueID)})
		}
	}
	for i := range claims {
		claims[i].grant, err = nativeContentionFreshGrant(s.owners, s.spec, claims[i].id)
		if err != nil {
			return reply, err
		}
	}
	if q.Job.Phase != "setup" && !cached {
		stored := false
		for i := range s.templates {
			if s.templates[i].phase == "" {
				s.templates[i] = nativeContentionReadTemplate{q.Job.Phase, capture.Target, capture, append([]graphstate.Operation(nil), ops...), append([]freshBinding(nil), claims...)}
				stored = true
				break
			}
		}
		if !stored {
			return reply, errLimit
		}
	}
	id := requestID(s.owners.incarnation)
	binary.BigEndian.PutUint64(id[8:], sequence)
	proposal, err := h.PreparePartitionGraphOperations(id, ops, operation.Ordinal, claims)
	if err != nil || len(proposal.wire) > 8<<10 {
		return reply, errors.Join(errLimit, err)
	}
	if err := errors.Join(ctx.Err(), read.active()); err != nil {
		return reply, err
	}
	frozen := &nativeContentionFrozen{Job: q.Job, Operation: operation, Request: [16]byte(id), Business: proposal.wire, BodySHA256: sha256.Sum256(proposal.wire), Capture: capture, ops: ops, claims: claims}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.read != read || s.cookie != cookie || s.condition.stamp != condition.stamp || s.busy != 0 || s.frozen[q.Slot-1] != nil {
		return reply, errInvalid
	}
	s.frozen[q.Slot-1] = frozen
	return nativeContentionReply{Cookie: cookie, Slot: q.Slot, Job: q.Job, Operation: operation, Request: frozen.Request, Business: bytes.Clone(frozen.Business), BodySHA256: frozen.BodySHA256, Capture: capture}, nil
}

// Only this explicit fresh bootstrap creates a workload owner. No decoded body
// can select another policy/profile, private original, cut or live source owner.
func newNativeContentionSessions(ctx context.Context, c *crossProcessCoordinator, owners *certifiedProcessOwners, columns *nativeGraphCostColumnSessions, spec *nativeContentionSpec, run [16]byte) (*nativeContentionSessions, error) {
	if spec == nil {
		return nil, nil
	}
	if ctx == nil || c == nil || owners == nil || columns == nil || spec.validate() != nil || run == ([16]byte{}) || c.host != owners.host || !c.host.machine.retainedPins() {
		return nil, errInvalid
	}
	return &nativeContentionSessions{ctx: ctx, gate: newGraphRequestGate(), coordinator: c, owners: owners, columns: columns, spec: *spec, run: run}, nil
}

func (s *nativeContentionSessions) serve(ctx context.Context, r processRequest) ([]byte, error) {
	if s == nil || ctx == nil || r.index != 0 || r.entity != 0 || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 {
		return nil, errInvalid
	}
	q, err := decodeNativeContentionInput(r.data)
	if err != nil {
		return nil, err
	}
	// Compare the complete decoded shape with the one admitted shape for this
	// operation. Unknown/duplicate fields were refused by the canonical decoder.
	allowed := nativeContentionControl{Action: q.Action}
	switch q.Action {
	case "open":
	case "close":
		allowed.Cookie = q.Cookie
	case "freeze":
		allowed.Cookie, allowed.Slot, allowed.Job = q.Cookie, q.Slot, q.Job
	case "frozen", "run", "resume":
		allowed.Cookie, allowed.Slot = q.Cookie, q.Slot
	case "columns", "column-owner":
		allowed.Column = q.Column
	case "records-open":
		allowed.Initial, allowed.Final = q.Initial, q.Final
	case "records-next":
		allowed.Cookie, allowed.Sequence = q.Cookie, q.Sequence
	case "records-close":
		allowed.Cookie = q.Cookie
	}
	expected, err := nativeContentionInput(allowed)
	if err != nil || !bytes.Equal(expected, r.data) {
		return nil, errors.Join(errInvalid, err)
	}
	if err := errors.Join(ctx.Err(), s.ctx.Err()); err != nil {
		return nil, err
	}
	var reply nativeContentionReply
	switch q.Action {
	case "open":
		reply, err = s.open(ctx)
	case "close":
		reply, err = s.closeCohort(q.Cookie, false)
	case "freeze":
		reply, err = s.freeze(ctx, q, r.sequence)
	case "frozen":
		reply, err = s.frozenReceipt(q)
	case "run", "resume":
		reply, err = s.runFrozen(ctx, q)
	case "columns":
		if s.spec.Layout != "columns" {
			return nil, errInvalid
		}
		if err := s.columnInput(q.Column); err != nil {
			return nil, err
		}
		reply.Column, err = s.columns.serve(ctx, processRequest{data: q.Column})
		if failure, ok := errors.AsType[*nativeColumnCreationFailure](err); ok {
			reply.Creation = failure
		}
	case "column-owner":
		if err := s.columnInput(q.Column); err != nil {
			return nil, err
		}
		reply.Column, err = s.columnOwner(ctx, q.Column)
	case "records-open", "records-next", "records-close":
		reply.Source, err = s.records(ctx, q)
	default:
		return nil, errInvalid
	}
	// Preserve failure diagnostics alongside the actual error mask; an empty
	// body or a diagnostic never becomes success or permission to replace a pin.
	if err != nil {
		reply.Error = err.Error()
	}
	reply.Run = s.run
	if reply.Source != nil {
		reply.Source.Run = s.run
	}
	body, encodeErr := json.Marshal(reply)
	if encodeErr != nil || len(body) > nativeGraphCostPageBytes {
		return nil, errors.Join(err, errLimit, encodeErr)
	}
	return body, err
}

func (s *nativeContentionSessions) columnInput(body []byte) error {
	q, err := nativeGraphCostColumnDecode(processRequest{data: body})
	if err != nil {
		return err
	}
	canonical, err := nativeGraphCostColumnInput(q)
	if err != nil || !bytes.Equal(canonical, body) {
		return errors.Join(errInvalid, err)
	}
	if q.Action != nativeColumnsOpen {
		return nil
	}
	frames, err := s.spec.retainedFrames()
	if err != nil {
		return err
	}
	for _, wire := range [][]byte{q.Initial, q.Final} {
		cut, err := nativeGraphCostColumnAt(wire)
		if err != nil {
			return err
		}
		pair, err := nativeColumnPeerCutPair(s.coordinator, cut)
		if err != nil {
			return err
		}
		for _, cert := range pair {
			if cert.sourceIndex > frames {
				return errLimit
			}
		}
	}
	return nil
}

func (s *nativeContentionSessions) frozenReceipt(q nativeContentionControl) (nativeContentionReply, error) {
	if !s.gate.tryLock() {
		return nativeContentionReply{}, errLimit
	}
	defer s.gate.unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.read == nil || s.cookie != q.Cookie || q.Slot == 0 || q.Slot > nativeContentionCallers {
		return nativeContentionReply{}, errInvalid
	}
	f := s.frozen[q.Slot-1]
	if f == nil {
		// Authoritative only about this live child-local factory slot. It cannot
		// prove native absence or reconstruct an original after process loss.
		return nativeContentionReply{Cookie: q.Cookie, Slot: q.Slot, Class: "factory-absent"}, nil
	}
	if f.factoryReplayed {
		return nativeContentionReply{}, errLimit
	}
	f.factoryReplayed = true
	return nativeContentionReply{Cookie: q.Cookie, Slot: q.Slot, Job: f.Job, Operation: f.Operation, Request: f.Request, Business: bytes.Clone(f.Business), BodySHA256: f.BodySHA256, Capture: f.Capture}, nil
}

// A lost constructor reply first queries the actual SAME child owner. This is
// bounded observation of existing ownership, never reconstruction from a DTO.
func (s *nativeContentionSessions) columnOwner(ctx context.Context, body []byte) ([]byte, error) {
	if s.spec.Layout != "columns" {
		return nil, errInvalid
	}
	q, err := nativeGraphCostColumnDecode(processRequest{data: body})
	if err != nil || q.Action != nativeColumnsOpen || q.RetainedExpires == 0 {
		return nil, errors.Join(errInvalid, err)
	}
	s.mu.Lock()
	if s.columnProbes >= nativeColumnTerminalAttempts {
		s.mu.Unlock()
		return nil, errLimit
	}
	s.columnProbes++
	s.mu.Unlock()
	s.columns.mu.Lock()
	owner, pending := s.columns.owner, s.columns.pending
	s.columns.mu.Unlock()
	if owner == nil || pending {
		return nil, errCrossProcessPending
	}
	if err := owner.gate.lock(ctx); err != nil {
		return nil, err
	}
	defer owner.gate.unlock()
	if owner.generation != q.Generation {
		return nil, errInvalid
	}
	if owner.source == nil {
		if err := owner.renew(ctx); err != nil {
			return nil, err
		}
	}
	initial, err := nativeGraphCostColumnAt(q.Initial)
	if err != nil {
		return nil, err
	}
	final, err := nativeGraphCostColumnAt(q.Final)
	if err != nil {
		return nil, err
	}
	first, err := owner.source.cutPair(initial)
	if err != nil {
		return nil, err
	}
	last, err := owner.source.cutPair(final)
	if err != nil || first != owner.source.initial || last != owner.source.final {
		return nil, errors.Join(errInvalid, err)
	}
	r, err := owner.store.retentionRecord(q.Generation)
	if err != nil {
		return nil, err
	}
	m, err := decodeGraphPinManifest(r.Original)
	if err != nil || m.expires != q.RetainedExpires {
		return nil, errors.Join(errInvalid, err)
	}
	active := ctx.Err()
	if owner.store.binding.Policy.Version == 1 {
		active = errors.Join(active, owner.ctx.Err())
	} else {
		active = errors.Join(active, owner.source.ctx.Err())
	}
	if active != nil {
		return nil, active
	}
	receipt, err := owner.receipt(0)
	if err != nil {
		return nil, err
	}
	return nativeGraphCostColumnEncode(receipt)
}

// Called only after the shared worker WaitGroup has joined. Cancellation does
// not manufacture a terminal outcome; unresolved originals stay unresolved.
func (s *nativeContentionSessions) close() error {
	if s == nil {
		return nil
	}
	_, err := s.closeCohort(nativeContentionCookie{}, true)
	s.mu.Lock()
	source := s.source
	s.mu.Unlock()
	if source != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err = errors.Join(err, source.close(cleanup))
		cancel()
	}
	return err
}

// This exporter is raw verification IO for either layout; it publishes no
// immutable generation/cache. One live owner retains one source/page and one
// cumulative ledger across actual one-minute source expiry. Restart loses this
// private cursor and starts a separately charged genuine query at original At.
type nativeContentionSourceReceipt struct {
	Run                               [16]byte
	Cookie                            nativeContentionCookie
	Sequence                          uint64
	Binding                           nativeColumnBinding
	Initial, Final                    [2][]byte
	Positions                         [2]uint64
	Frames                            []nativeColumnFrame
	EOF, Closed                       bool
	Charged                           nativeColumnWork
	Starts, Expirations, FailedStarts uint64
	AttemptedPages, FailedPages       uint64
	RPCs, RequestBytes, ReplyBytes    uint64
	DeadlineUnixNanos                 int64
}
type nativeContentionSourceOwner struct {
	gate                              graphRequestGate
	ctx                               context.Context
	cancel                            context.CancelFunc
	coordinator                       *crossProcessCoordinator
	initial, final                    graphCut
	policy                            nativeColumnPolicy
	admission                         nativeContentionAdmission
	binding                           nativeColumnBinding
	input                             []byte
	openReplayed                      bool
	source                            *nativeColumnSource
	cookie                            nativeContentionCookie
	positions                         [2]uint64
	charged                           nativeColumnWork
	starts, expirations, failedStarts uint64
	attemptedPages, failedPages       uint64
	calls, requestBytes, replyBytes   uint64
	sequence                          uint64
	cached                            []byte
	replayed, eof, closed             bool
	closeErr                          error
}

func (o *nativeContentionSourceOwner) reserve(grant nativeColumnWork) error {
	if !o.charged.admit(grant, o.policy) {
		return errLimit
	}
	o.charged = o.charged.add(grant)
	return nil
}

func (o *nativeContentionSourceOwner) capture() error {
	seconds := o.admission.BudgetSeconds
	if o.starts >= nativeContentionStarts(seconds) || o.failedStarts >= 4 {
		return errLimit
	}
	opening, _, err := nativeColumnSourceAllowance([2]*allocationHost{o.coordinator.host, o.coordinator.host}, o.policy)
	if err != nil {
		return err
	}
	opening, err = nativeColumnPeerOpening(opening, o.coordinator.host.machine.limits)
	if err != nil {
		return err
	}
	if err := o.reserve(opening); err != nil {
		return err
	}
	o.starts++
	source, err := newPartitionNativeColumnSource(o.ctx, o.coordinator, o.initial, o.final, o.policy)
	if err != nil {
		o.failedStarts++ // uncertain/failing effects retain the complete opening grant
		if failure, ok := errors.AsType[*nativeColumnSourceFailure](err); ok {
			if !nativeColumnAdd(o.calls, failure.RPCs) || !nativeColumnAdd(o.requestBytes, failure.RequestBytes) || !nativeColumnAdd(o.replyBytes, failure.ReplyBytes) {
				return errors.Join(err, errLimit)
			}
			o.calls += failure.RPCs
			o.requestBytes += failure.RequestBytes
			o.replyBytes += failure.ReplyBytes
		}
		return err
	}
	// Select the actual fresh control/original grammar so real GPO entries are
	// authenticated rather than skipped. This raw owner issues no pin/generation;
	// a source binding is never itself durable-retention authority.
	if err := source.retainedBinding(); err != nil {
		o.failedStarts++
		o.source = source // once-only cleanup includes its actual transport meter
		return errors.Join(err, o.releaseSource())
	}
	if o.source != nil || o.binding.Version != 0 && !nativeColumnSame(o.binding, source.binding) {
		// A successful capture cannot replace an existing live owner. The gate
		// normally excludes this branch; still retain the attempted capture cost.
		err := source.Close()
		calls, requests, replies := source.meter.snapshot()
		if !nativeColumnAdd(o.calls, calls) || !nativeColumnAdd(o.requestBytes, requests) || !nativeColumnAdd(o.replyBytes, replies) {
			return errors.Join(errInvalid, err, errLimit)
		}
		o.calls += calls
		o.requestBytes += requests
		o.replyBytes += replies
		return errors.Join(errInvalid, err)
	}
	o.binding = source.binding
	o.source = source
	return nil
}

func (o *nativeContentionSourceOwner) releaseSource() error {
	if o.source == nil {
		return nil
	}
	source := o.source
	err := source.Close()
	if source.meter != nil {
		calls, requests, replies := source.meter.snapshot()
		if !nativeColumnAdd(o.calls, calls) || !nativeColumnAdd(o.requestBytes, requests) || !nativeColumnAdd(o.replyBytes, replies) {
			err = errors.Join(err, errLimit)
		} else {
			o.calls += calls
			o.requestBytes += requests
			o.replyBytes += replies
		}
	}
	o.source = nil
	return err
}

func (o *nativeContentionSourceOwner) receipt(sequence uint64) (nativeContentionSourceReceipt, error) {
	r := nativeContentionSourceReceipt{Cookie: o.cookie, Sequence: sequence, Binding: o.binding, Positions: o.positions, Charged: o.charged, Starts: o.starts, Expirations: o.expirations, FailedStarts: o.failedStarts, AttemptedPages: o.attemptedPages, FailedPages: o.failedPages, RPCs: o.calls, RequestBytes: o.requestBytes, ReplyBytes: o.replyBytes, EOF: o.eof, Closed: o.closed, Frames: []nativeColumnFrame{}}
	deadline, _ := o.ctx.Deadline()
	r.DeadlineUnixNanos = deadline.UnixNano()
	for g, cert := range o.initial.certificates {
		var err error
		r.Initial[g], err = encodeGraphCertificate(cert)
		if err != nil {
			return r, err
		}
	}
	for g, cert := range o.final.certificates {
		var err error
		r.Final[g], err = encodeGraphCertificate(cert)
		if err != nil {
			return r, err
		}
	}
	if o.source != nil {
		r.Binding = o.source.binding
		if o.source.meter != nil {
			calls, request, reply := o.source.meter.snapshot()
			if !nativeColumnAdd(r.RPCs, calls) || !nativeColumnAdd(r.RequestBytes, request) || !nativeColumnAdd(r.ReplyBytes, reply) {
				return r, errLimit
			}
			r.RPCs += calls
			r.RequestBytes += request
			r.ReplyBytes += reply
		}
	}
	return r, nil
}

func (s *nativeContentionSessions) records(ctx context.Context, q nativeContentionControl) (*nativeContentionSourceReceipt, error) {
	if q.Action == "records-open" {
		if !s.gate.tryLock() {
			return nil, errLimit
		}
		defer s.gate.unlock()
		input, err := nativeContentionInput(q)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		existing := s.source
		busy := s.read != nil || s.busy != 0 || s.pending != 0 || s.next == ^uint64(0)
		if !busy {
			s.next++
		}
		number := s.next
		s.mu.Unlock()
		if busy {
			return nil, errLimit
		}
		if existing != nil {
			if !existing.gate.tryLock() {
				return nil, errLimit
			}
			defer existing.gate.unlock()
			if existing.closed || existing.openReplayed || !bytes.Equal(existing.input, input) || existing.ctx.Err() != nil {
				return nil, errInvalid
			}
			existing.openReplayed = true
			receipt, err := existing.receipt(0)
			return &receipt, err
		}
		initial, err := nativeGraphCostColumnAt(q.Initial)
		if err != nil {
			return nil, err
		}
		final, err := nativeGraphCostColumnAt(q.Final)
		if err != nil {
			return nil, err
		}
		first, err := nativeColumnPeerCutPair(s.coordinator, initial)
		if err != nil {
			return nil, err
		}
		last, err := nativeColumnPeerCutPair(s.coordinator, final)
		if err != nil {
			return nil, err
		}
		frames, err := s.spec.retainedFrames()
		if err != nil {
			return nil, err
		}
		for _, cert := range last {
			if cert.sourceIndex > frames {
				return nil, errLimit
			}
		}
		policy, err := nativeContentionColumnsPolicy(s.coordinator.host, s.spec, s.owners.workloadAdmission)
		if err != nil {
			return nil, err
		}
		nonce, err := graphReadNonce()
		if err != nil {
			return nil, err
		}
		owner, cancel := context.WithTimeout(s.ctx, nativeContentionWholeSeconds*time.Second)
		stop := context.AfterFunc(ctx, cancel)
		o := &nativeContentionSourceOwner{gate: newGraphRequestGate(), ctx: owner, cancel: cancel, coordinator: s.coordinator, initial: initial, final: final, policy: policy, admission: s.owners.workloadAdmission, cookie: nativeContentionCookie{number, nonce}, input: input}
		if err := o.gate.lock(ctx); err != nil {
			stop()
			cancel()
			return nil, err
		}
		defer o.gate.unlock()
		for g, cert := range first {
			o.positions[g] = cert.sourceIndex
		}
		// Publish ownership before source effects. Even a failed/uncertain first
		// capture retains its precharged query ledger and original cut recipe.
		s.mu.Lock()
		s.source = o
		s.mu.Unlock()
		context.AfterFunc(owner, func() {
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Second)
			defer done()
			_ = o.close(cleanup)
		})
		err = o.capture()
		stop()
		if err == nil {
			err = errors.Join(ctx.Err(), owner.Err())
		}
		receipt, receiptErr := o.receipt(0)
		return &receipt, errors.Join(err, receiptErr)
	}
	s.mu.Lock()
	o := s.source
	s.mu.Unlock()
	if o == nil || o.cookie != q.Cookie {
		return nil, errInvalid
	}
	if q.Action == "records-close" {
		err := o.close(ctx)
		if err != nil {
			return nil, err
		}
		if err := o.gate.lock(ctx); err != nil {
			return nil, err
		}
		receipt, err := o.receipt(0)
		o.gate.unlock()
		s.mu.Lock()
		if s.source == o {
			s.source = nil
		}
		s.mu.Unlock()
		return &receipt, err
	}
	return o.next(ctx, q.Sequence)
}

func (o *nativeContentionSourceOwner) next(ctx context.Context, sequence uint64) (reply *nativeContentionSourceReceipt, err error) {
	if !o.gate.tryLock() {
		return nil, errLimit
	}
	defer o.gate.unlock()
	if o.closed || errors.Join(ctx.Err(), o.ctx.Err()) != nil {
		return nil, errors.Join(errInvalid, ctx.Err(), o.ctx.Err())
	}
	if sequence == o.sequence && o.cached != nil {
		if o.replayed {
			return nil, errLimit
		}
		o.replayed = true
		var reply nativeContentionSourceReceipt
		if err := json.Unmarshal(o.cached, &reply); err != nil {
			return nil, errors.Join(errCorrupt, err)
		}
		return &reply, nil
	}
	if o.eof || o.sequence == ^uint64(0) || sequence != o.sequence+1 {
		return nil, errInvalid
	}
	if o.failedPages >= nativeContentionFailedPages(o.admission.BudgetSeconds) || o.attemptedPages == ^uint64(0) {
		return nil, errLimit
	}
	if o.source != nil && o.source.ctx.Err() != nil {
		// Only this immutable owner context's actual TTL expiry permits another
		// capture. A shorter page call cancellation cannot masquerade as expiry.
		if !o.source.fullTTL || !errors.Is(o.source.ctx.Err(), context.DeadlineExceeded) || o.ctx.Err() != nil || o.expirations >= o.admission.BudgetSeconds/60 {
			return nil, errCrossProcessPending
		}
		if err := o.releaseSource(); err != nil {
			return nil, err
		}
		o.expirations++
	}
	if o.source == nil {
		if err := o.capture(); err != nil {
			return nil, err
		}
	}
	if err := o.reserve(o.source.grant); err != nil {
		return nil, err
	}
	o.attemptedPages++
	defer func() {
		if err != nil {
			o.failedPages++
		}
	}()
	frames, positions, err := o.source.page(ctx, o.positions)
	if err != nil {
		return nil, err
	}
	eof := len(frames) == 0
	if eof && positions != o.source.finalPositions() || !eof && positions == o.positions {
		return nil, errCorrupt
	}
	receipt, err := o.receipt(sequence)
	if err != nil {
		return nil, err
	}
	receipt.Positions, receipt.Frames, receipt.EOF = positions, frames, eof
	body, err := json.Marshal(receipt)
	if err != nil || len(body) > nativeGraphCostPageBytes {
		return nil, errors.Join(errLimit, err)
	}
	if err := errors.Join(ctx.Err(), o.ctx.Err()); err != nil {
		return nil, err
	}
	// Successful complete hydration/encoding precedes the only cursor commit.
	o.positions, o.eof, o.sequence, o.cached, o.replayed = positions, eof, sequence, body, false
	return &receipt, nil
}

func (o *nativeContentionSourceOwner) close(ctx context.Context) error {
	o.cancel()
	if err := o.gate.lock(ctx); err != nil {
		return err
	}
	defer o.gate.unlock()
	if o.closed {
		return o.closeErr
	}
	o.closed, o.eof = true, false
	o.closeErr = o.releaseSource()
	return o.closeErr
}
