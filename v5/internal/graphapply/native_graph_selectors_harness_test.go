package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativeSelectorsFamily = "snapshot-selectors"
const nativeSelectorsFormat = "native-snapshot-selectors-workload-v1"
const nativeSelectorsMarker = "GNSW1"
const nativeSelectorsQueries = 16

// Family identity is separate from the common scalar dataset. The actual
// business originals use that dataset's immutable ordinal and provenance.
type nativeSelectorsSpec struct {
	Version      int    `json:"version"`
	Family       string `json:"family"`
	Facts        uint64 `json:"facts"`
	CrossPercent uint64 `json:"crossPercent"`
	Writers      int    `json:"writers"`
	Layout       string `json:"layout"`
}

func (s nativeSelectorsSpec) cost() nativeGraphCostSpec {
	return nativeGraphCostSpec{Version: 6, Facts: s.Facts, CrossPercent: s.CrossPercent, Writers: s.Writers}
}
func nativeSelectorsValidate(s nativeGraphCostSpec) error {
	if s.Version != 6 {
		return errInvalid
	}
	// Shape parity only. No feed owner, feed allowance or feed authority is
	// obtained by validating the common dimensions through the existing guard.
	s.Version = 5
	return nativeFeedValidate(s)
}
func nativeSelectorsSpecFromEnv() (s nativeSelectorsSpec, err error) {
	body := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_SPEC"))
	if len(body) == 0 || len(body) > 1024 || nativeFeedDecodeJSON(body, &s) != nil || s.Version != 1 || s.Family != nativeSelectorsFamily || s.Layout != "kv" && s.Layout != "columns" || nativeSelectorsValidate(s.cost()) != nil {
		return s, errInvalid
	}
	return s, nil
}

type nativeSelectorsAdmission struct {
	Version                            uint8
	Family, Run                        string
	Spec                               nativeSelectorsSpec
	Transport                          nativeWorkloadTransportSpec
	BudgetSeconds, CausalMinimumMillis uint64
	DeadlineUnixNanos                  int64
	Graph                              [16]byte
	Topology                           uint64
	Declaration                        [32]byte
	BinarySHA256                       string
}

func nativeSelectorsAdmissionFromEnv(s nativeSelectorsSpec) (a nativeSelectorsAdmission, digest [32]byte, err error) {
	if nativeSelectorsValidate(s.cost()) != nil {
		return a, digest, errInvalid
	}
	return nativeScalarReadAdmissionFromEnv(s, nativeSelectorsFamily)
}

func nativeScalarReadAdmissionFromEnv(s nativeSelectorsSpec, family string) (a nativeSelectorsAdmission, digest [32]byte, err error) {
	deadline, err := nativeGraphEdgeUniqueDeadlineFromEnv()
	if err != nil {
		return a, digest, err
	}
	seconds, _ := strconv.ParseUint(os.Getenv("RHO_GRAPH_WORKLOAD_SECONDS"), 10, 32)
	run := os.Getenv("RHO_GRAPH_WORKLOAD_RUN")
	nonce, err := hex.DecodeString(run)
	if err != nil || len(nonce) != 16 || hex.EncodeToString(nonce) != run || bytes.Equal(nonce, make([]byte, 16)) {
		return a, digest, errInvalid
	}
	var transport nativeWorkloadTransportSpec
	wire := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_TRANSPORT"))
	if len(wire) == 0 || len(wire) > 1024 || nativeFeedDecodeJSON(wire, &transport) != nil || transport.validate() != nil || transport.Seed != 20261009 {
		return a, digest, errInvalid
	}
	declaration, err := nativeGraphEdgeUniqueTransportDeclaration()
	if err != nil {
		return a, digest, err
	}
	binary, err := nativeGraphEdgeUniqueExecutable()
	if err != nil {
		return a, digest, err
	}
	// Three anchor originals and the two value-definition fact0 originals
	// precede dependent work. This necessary traffic floor is not a duration
	// forecast; scans, replication and read closure incur additional real work.
	floor := uint64(5) * transport.TargetRTTMillis
	if seconds*1000 <= floor {
		return a, digest, errLimit
	}
	a = nativeSelectorsAdmission{1, family, run, s, transport, seconds, floor, deadline.UnixNano(), [16]byte(declaration.Graph()), declaration.TopologyEpoch(), declaration.Digest(), binary}
	body, err := json.Marshal(a)
	return a, sha256.Sum256(body), err
}

func nativeSelectorsProcessProfile(s *nativeGraphCostSpec) (bool, error) {
	flag := os.Getenv("RHO_GRAPH_SELECTORS_PROCESS")
	if flag == "" {
		if s != nil && s.Version == 6 {
			return false, errInvalid
		}
		return false, nil
	}
	if flag != "1" || s == nil || nativeSelectorsValidate(*s) != nil || os.Getenv("RHO_GRAPH_CERTIFIED_PROCESS") != "1" || os.Getenv("RHO_GRAPH_SOURCE_PROCESS") != "1" {
		return false, errInvalid
	}
	for _, name := range []string{"RHO_GRAPH_FEED_PROCESS", "RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_EDGE_UNIQUE_PROCESS", "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_NATIVE_WORKLOAD", "RHO_GRAPH_NATIVE_FAULT_FAMILY"} {
		if value := os.Getenv(name); value != "" && value != "0" {
			return false, errInvalid
		}
	}
	spec, err := nativeSelectorsSpecFromEnv()
	if err != nil || spec.cost() != *s {
		return false, errors.Join(errInvalid, err)
	}
	_, _, err = nativeSelectorsAdmissionFromEnv(spec)
	return err == nil, err
}

func nativeSelectorsOriginals(s nativeGraphCostSpec) uint64 { return 16*s.Facts + 3 }
func nativeSelectorsFrames(s nativeGraphCostSpec) (uint64, error) {
	if nativeSelectorsValidate(s) != nil {
		return 0, errInvalid
	}
	// The same eight local/single cross admission rule gives at most eight
	// concurrent local originals per useful install. Each of the two N-operation
	// phases has an enforced aggregate capture ceiling. Ten native stages per
	// original cover Register/two Prepare/Decide/two Resolve, capacity help and
	// one same-Prepare retry. Additional SAME invocations are a whole-cell64 pool,
	// not eight retries multiplied across every useful operation. The fixed4096
	// covers all sixteen bounded read closures, pin issuance/publication/cleanup,
	// two derived generations and genuine grant/original replay controls.
	return nativeSelectorsOriginals(s)*10 + 64*10 + 4096, nil
}
func nativeSelectorsGrantPlan(s nativeGraphCostSpec, block, partition, sequence uint64) (uint64, uint64, error) {
	if nativeSelectorsValidate(s) != nil || block < 2 || block > idalloc.MaxBlockSize || sequence == 0 {
		return 0, 0, errInvalid
	}
	return nativeScalarReadGrantPlan(s, block, partition, sequence)
}
func nativeScalarReadGrantPlan(s nativeGraphCostSpec, block, partition, sequence uint64) (uint64, uint64, error) {
	if block < 2 || block > idalloc.MaxBlockSize || sequence == 0 {
		return 0, 0, errInvalid
	}
	if partition == 3 {
		if sequence > (s.homeCount()+block-1)/block {
			return 0, 0, errInvalid
		}
		first := 1 + (sequence-1)*block
		return first, min(block, s.homeCount()-first+1), nil
	}
	if partition == 8 && sequence == 1 {
		return s.remoteNode(), 2, nil
	}
	return 0, 0, errInvalid
}
func nativeSelectorsProfile(s nativeGraphCostSpec, init *declaredInitCommand, l *raftlog.Limits, p *raftlog.ApplicationPolicy, g *raftlog.ApplicationGenerationLimits) error {
	if init == nil || l == nil || p == nil || g == nil || nativeSelectorsValidate(s) != nil || p.MaxInstallWrites != 256 || p.MaxImageBytes != 172 {
		return errInvalid
	}
	frames, err := nativeSelectorsFrames(s)
	if err != nil {
		return errLimit
	}
	return nativeScalarReadProfile(s, frames, init, l, p, g)
}
func nativeScalarReadProfile(s nativeGraphCostSpec, frames uint64, init *declaredInitCommand, l *raftlog.Limits, p *raftlog.ApplicationPolicy, g *raftlog.ApplicationGenerationLimits) error {
	if init == nil || l == nil || p == nil || g == nil || p.MaxInstallWrites != 256 || p.MaxImageBytes != 172 {
		return errInvalid
	}
	if frames > (^uint64(0)-65536)/260 || frames > (^uint64(0)-(64<<20))/(64<<10) {
		return errLimit
	}
	retainedBytes, retainedRecords := min(uint64(64<<20)+frames*(64<<10), uint64(1<<39)), uint64(65536)+260*frames
	if retainedRecords > 1<<39 {
		return errLimit
	}
	init.maxBlock = min(s.homeCount(), idalloc.MaxBlockSize)
	l.MaxRetainedBytes, l.MaxRetainedEntries = retainedBytes, 1_000_000
	p.RetainedApplicationBytes, p.RetainedApplicationRecords = retainedBytes, retainedRecords
	*g = raftlog.ApplicationGenerationLimits{MaxBytes: 2 * retainedBytes, MaxRecords: 2 * retainedRecords}
	return nil
}

// Each diagnostic query keeps its actual source, pin and private continuation.
// Replacement source invocations are prepaid from one finite owner allowance;
// no same-query renewal refunds an old arena, attempt or failed source grant.
type nativeSelectorsReadPolicy struct {
	Queries, Starts, Calls               uint64
	Rows, Bytes, Output, RPCs, WireBytes uint64
	DeadlineUnixNanos                    int64
}

func nativeSelectorsReadAllowance(s nativeSelectorsSpec, a nativeSelectorsAdmission) (nativeSelectorsReadPolicy, error) {
	if nativeSelectorsValidate(s.cost()) != nil || a.Spec != s || a.DeadlineUnixNanos <= 0 || a.BudgetSeconds == 0 || a.BudgetSeconds > 365*24*60*60 {
		return nativeSelectorsReadPolicy{}, errInvalid
	}
	frames, err := nativeSelectorsFrames(s.cost())
	if err != nil {
		return nativeSelectorsReadPolicy{}, err
	}
	return nativeScalarReadAllowance(s.Facts, a.BudgetSeconds, a.DeadlineUnixNanos, frames, nativeSelectorsQueries)
}
func nativeScalarReadAllowance(facts, seconds uint64, deadline int64, frames, queries uint64) (nativeSelectorsReadPolicy, error) {
	if facts < 20 || facts > 1_000_000 || seconds == 0 || seconds > 365*24*60*60 || deadline <= 0 || queries == 0 || queries > nativeSelectorsQueries {
		return nativeSelectorsReadPolicy{}, errInvalid
	}
	l := defaultMaterializerLimits()
	// Two complete physical cohorts also export all native records in pages of
	// four, independently of the semantic query scans. These are not discarded
	// from resource admission merely because they are diagnostic rather than
	// business latency. Actual payload/work is reported separately from grants.
	starts := queries*((facts+15)/16+seconds/60+16) + 4*((frames+3)/4) + 64
	unit := nativeColumnRetentionWork(l)
	grant, err := unit.scaled(starts)
	if err != nil {
		return nativeSelectorsReadPolicy{}, err
	}
	return nativeSelectorsReadPolicy{Queries: queries, Starts: starts, Calls: starts * 8, Rows: grant.Records, Bytes: grant.Bytes, Output: grant.OutputBytes, RPCs: grant.RPCs, WireBytes: grant.WireBytes, DeadlineUnixNanos: deadline}, nil
}

func nativeSelectorsColumnPolicy(h *allocationHost, s nativeSelectorsSpec, a nativeSelectorsAdmission, digest [32]byte) (nativeColumnPolicy, error) {
	if h == nil || h.machine == nil || h.machine.SemanticContractID() != durableColumnGraphSemanticContractID() || a.Spec != s || digest == ([32]byte{}) {
		return nativeColumnPolicy{}, errInvalid
	}
	p := nativeScalarReadColumnDefaults(digest)
	frames, err := nativeSelectorsFrames(s.cost())
	if err != nil {
		return p, err
	}
	return nativeScalarReadColumnPolicy(h, frames, a.BudgetSeconds, digest)
}
func nativeScalarReadColumnDefaults(digest [32]byte) nativeColumnPolicy {
	return nativeColumnPolicy{Version: 1, Admission: digest, ChunkRecords: 16, ChunkRows: 16, ChunkBytes: 64 << 10, FrameBytes: 32 << 10, MaxGenerations: 2, MaxChunks: 1 << 40, MaxRetainedBytes: 1 << 62, MaxWorkRecords: ^uint64(0), MaxWorkBytes: ^uint64(0), MaxDecodedRows: ^uint64(0), MaxOutputBytes: ^uint64(0), MaxRPCs: ^uint64(0), MaxWireBytes: ^uint64(0), CacheBytes: 2 << 20, MemTableBytes: 1 << 20, Lifetime: time.Minute}
}
func nativeScalarReadColumnPolicy(h *allocationHost, frames, seconds uint64, digest [32]byte) (nativeColumnPolicy, error) {
	p := nativeScalarReadColumnDefaults(digest)
	opening, page, err := nativeColumnLimitAllowance(h.machine.store.ApplicationLimits(), h.machine.limits, p)
	if err != nil {
		return p, err
	}
	opening, err = opening.scaled(2)
	if err != nil {
		return p, err
	}
	opening, err = nativeColumnPeerOpening(opening, h.machine.limits)
	if err != nil {
		return p, err
	}
	const passes = 8
	failed := uint64(32) + seconds/60 + 4
	if frames > (^uint64(0)-failed)/passes-1 {
		return p, errLimit
	}
	pages, err := page.scaled((frames+1)*passes + failed)
	if err != nil {
		return p, err
	}
	starts := seconds/60 + 13
	captures, err := opening.scaled(8*starts + 4*passes)
	if err != nil {
		return p, err
	}
	work, err := pages.plus(captures)
	if err != nil {
		return p, err
	}
	retention, err := nativeColumnRetentionWork(h.machine.limits).scaled(8*starts + 32)
	if err != nil {
		return p, err
	}
	work, err = work.plus(retention)
	if err != nil {
		return p, err
	}
	p.MaxWorkRecords, p.MaxWorkBytes, p.MaxDecodedRows, p.MaxOutputBytes, p.MaxRPCs, p.MaxWireBytes = work.Records, work.Bytes, work.DecodedRows, work.OutputBytes, work.RPCs, work.WireBytes
	p.MaxChunks = 2 * frames
	perChunk := uint64(p.ChunkBytes + 128)
	if frames > ((1<<62)-8*nativeColumnMetadataBytes)/(2*perChunk) {
		return p, errLimit
	}
	p.MaxRetainedBytes = 8*nativeColumnMetadataBytes + 2*frames*perChunk
	return p, p.validate()
}
