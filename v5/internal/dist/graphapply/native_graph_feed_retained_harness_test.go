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

const nativeFeedRetainedFormat = "native-snapshot-feed-retained-only-workload-v1"
const nativeFeedRetainedMarker = "GNFR7"
const nativeFeedRetainedContract = "snapshot-token-relinquished-native-rolling-ack-prefix-v1"

// Version3 explicitly selects a different minimum retention promise. Cost5/v1
// keeps snapshot C, and the FDB owned-transport v2 recipe is not this profile.
type nativeFeedRetainedSpec struct {
	Version      int    `json:"version"`
	Family       string `json:"family"`
	Facts        uint64 `json:"facts"`
	CrossPercent uint64 `json:"crossPercent"`
	Writers      int    `json:"writers"`
	Layout       string `json:"layout"`
}

func (s nativeFeedRetainedSpec) cost() nativeGraphCostSpec {
	return nativeGraphCostSpec{Version: 7, Facts: s.Facts, CrossPercent: s.CrossPercent, Writers: s.Writers}
}

func nativeFeedRetainedValidate(s nativeGraphCostSpec) error {
	if s.Version != 7 {
		return errInvalid
	}
	s.Version = 5 // dimension shape only; no Cost5 source/pin authority
	return nativeFeedValidate(s)
}

func nativeFeedRetainedSpecFromEnv() (s nativeFeedRetainedSpec, err error) {
	wire := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_SPEC"))
	if len(wire) == 0 || len(wire) > 1024 || nativeFeedDecodeJSON(wire, &s) != nil || s.Version != 3 || s.Family != nativeFeedFamily || s.Layout != "kv" && s.Layout != "columns" || nativeFeedRetainedValidate(s.cost()) != nil {
		return s, errInvalid
	}
	return s, nil
}

type nativeFeedRetainedAdmission struct {
	Version                            uint8
	Family, Run                        string
	Spec                               nativeFeedRetainedSpec
	Transport                          nativeWorkloadTransportSpec
	BudgetSeconds, CausalMinimumMillis uint64
	DeadlineUnixNanos                  int64
	Graph                              [16]byte
	Topology                           uint64
	Declaration                        [32]byte
	BinarySHA256                       string
}

func nativeFeedRetainedAdmissionFromEnv(s nativeFeedRetainedSpec) (a nativeFeedRetainedAdmission, digest [32]byte, err error) {
	if nativeFeedRetainedValidate(s.cost()) != nil || s.Version != 3 || s.Family != nativeFeedFamily || s.Layout != "kv" && s.Layout != "columns" {
		return a, digest, errInvalid
	}
	deadline, err := nativeGraphEdgeUniqueDeadlineFromEnv()
	if err != nil {
		return a, digest, err
	}
	seconds, err := strconv.ParseUint(os.Getenv("RHO_GRAPH_WORKLOAD_SECONDS"), 10, 32)
	if err != nil {
		return a, digest, err
	}
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
	// Same five actual anchor/value-definition dependencies as the scalar
	// family. Collection, publication and feed work add real traffic; this is
	// only a necessary causal floor, never a predicted completion time.
	floor := uint64(5) * transport.TargetRTTMillis
	if seconds*1000 <= floor {
		return a, digest, errLimit
	}
	a = nativeFeedRetainedAdmission{1, nativeFeedFamily, run, s, transport, seconds, floor, deadline.UnixNano(), [16]byte(declaration.Graph()), declaration.TopologyEpoch(), declaration.Digest(), binary}
	body, err := json.Marshal(a)
	return a, sha256.Sum256(body), err
}

func nativeFeedRetainedProcessProfile(s *nativeGraphCostSpec) (bool, error) {
	flag := os.Getenv("RHO_GRAPH_FEED_RETAINED_PROCESS")
	if flag == "" {
		if s != nil && s.Version == 7 {
			return false, errInvalid
		}
		return false, nil
	}
	if flag != "1" || s == nil || nativeFeedRetainedValidate(*s) != nil || os.Getenv("RHO_GRAPH_CERTIFIED_PROCESS") != "1" || os.Getenv("RHO_GRAPH_SOURCE_PROCESS") != "1" {
		return false, errInvalid
	}
	for _, name := range []string{"RHO_GRAPH_FEED_PROCESS", "RHO_GRAPH_CREATE_UPDATE_PROCESS", "RHO_GRAPH_SELECTORS_PROCESS", "RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_EDGE_UNIQUE_PROCESS", "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_NATIVE_WORKLOAD", "RHO_GRAPH_NATIVE_FAULT_FAMILY", "RHO_GRAPH_STABLE_STORAGE_PROCESS", "RHO_GRAPH_STABLE_STATE_PROCESS", "RHO_GRAPH_STATE_RETENTION_PROCESS"} {
		if value := os.Getenv(name); value != "" && value != "0" && value != "off" {
			return false, errInvalid
		}
	}
	spec, err := nativeFeedRetainedSpecFromEnv()
	if err != nil || spec.cost() != *s {
		return false, errors.Join(errInvalid, err)
	}
	_, _, err = nativeFeedRetainedAdmissionFromEnv(spec)
	return err == nil, err
}

func nativeFeedRetainedPages(s nativeGraphCostSpec) uint64 { return 64*s.Facts + 1024 }

func nativeFeedRetainedFrames(s nativeGraphCostSpec) (uint64, error) {
	if nativeFeedRetainedValidate(s) != nil {
		return 0, errInvalid
	}
	// Both update ranges together still contain exactly N useful updates. The
	// existing eight-slot charging invariant and explicit original capture cap
	// remain 16N+3. Page/ACK/admission cost five entries per keeper. Doubling the
	// previous page allowance funds both physical boundaries and the retained
	// prefix splice, without multiplying SAME retries across all facts. Native
	// Collect is local storage work, reported separately; it invents no entries.
	return (16*s.Facts+3)*10 + 64*10 + nativeFeedRetainedPages(s)*5 + 8*((s.Facts+15)/16) + 8192, nil
}

func nativeFeedRetainedGrantPlan(s nativeGraphCostSpec, block, partition, sequence uint64) (uint64, uint64, error) {
	if nativeFeedRetainedValidate(s) != nil || block < 2 || block > idalloc.MaxBlockSize || sequence == 0 {
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

func nativeFeedRetainedProfile(s nativeGraphCostSpec, init *declaredInitCommand, l *raftlog.Limits, p *raftlog.ApplicationPolicy, g *raftlog.ApplicationGenerationLimits) error {
	if init == nil || l == nil || p == nil || g == nil || nativeFeedRetainedValidate(s) != nil || p.MaxInstallWrites != 256 || p.MaxImageBytes != 172 {
		return errInvalid
	}
	frames, err := nativeFeedRetainedFrames(s)
	if err != nil || frames > (^uint64(0)-(64<<20))/(64<<10) || frames > (^uint64(0)-65536)/260 {
		return errors.Join(errLimit, err)
	}
	retainedBytes := min(uint64(64<<20)+frames*(64<<10), uint64(1<<39))
	retainedRecords := uint64(65536) + frames*260
	if retainedRecords > 1<<39 {
		return errLimit
	}
	init.maxBlock = min(s.homeCount(), idalloc.MaxBlockSize)
	l.MaxRetainedBytes, l.MaxRetainedEntries = retainedBytes, 1_000_000
	p.RetainedApplicationBytes, p.RetainedApplicationRecords = retainedBytes, retainedRecords
	*g = raftlog.ApplicationGenerationLimits{MaxBytes: 2 * retainedBytes, MaxRecords: 2 * retainedRecords}
	return nil
}

func nativeFeedRetainedPolicy(s nativeGraphCostSpec, deadline int64) retainedFeedPolicy {
	l := defaultMaterializerLimits()
	pages := nativeFeedRetainedPages(s)
	return retainedFeedPolicy{pageBytes: 64 << 10, pageRows: 16, maxPages: pages, maxAttempts: 4*pages + s.Facts/8 + 1024, operationRows: l.sourceRows, operationBytes: l.sourceBytes, operationOutput: l.outputBytes, deadlineUnixNanos: deadline}
}

// Fresh generation owners authenticate this exact admission before workers.
// Every generation declares its own genuine initial checkpoint; this policy
// cannot confer availability of an original C that has already been collected.
func nativeFeedRetainedColumnPolicy(h *allocationHost, s nativeFeedRetainedSpec, a nativeFeedRetainedAdmission, digest [32]byte) (nativeColumnPolicy, error) {
	if h == nil || h.machine == nil || h.machine.SemanticContractID() != retainedOnlyFeedSemanticContractID() || a.Spec != s || digest == ([32]byte{}) {
		return nativeColumnPolicy{}, errInvalid
	}
	p := nativeColumnPolicy{Version: 1, Admission: digest, ChunkRecords: 16, ChunkRows: 16, ChunkBytes: 64 << 10, FrameBytes: 32 << 10, MaxGenerations: 2, MaxChunks: 1 << 40, MaxRetainedBytes: 1 << 62, MaxWorkRecords: ^uint64(0), MaxWorkBytes: ^uint64(0), MaxDecodedRows: ^uint64(0), MaxOutputBytes: ^uint64(0), MaxRPCs: ^uint64(0), MaxWireBytes: ^uint64(0), CacheBytes: 2 << 20, MemTableBytes: 1 << 20, Lifetime: time.Minute}
	frames, err := nativeFeedRetainedFrames(s.cost())
	if err != nil {
		return p, err
	}
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
	failed := uint64(32) + a.BudgetSeconds/60 + 4
	if frames > (^uint64(0)-failed)/passes-1 {
		return p, errLimit
	}
	work, err := page.scaled((frames+1)*passes + failed)
	if err != nil {
		return p, err
	}
	starts := a.BudgetSeconds/60 + 13
	captures, err := opening.scaled(8*starts + 4*passes)
	if err != nil {
		return p, err
	}
	work, err = work.plus(captures)
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
	if p.validate() != nil {
		return p, errLimit
	}
	return p, nil
}

func nativeFeedRetainedPresenceRow(s nativeGraphCostSpec, row nativeColumnRow) (int, error) {
	if s.Version != 7 {
		return 0, errCorrupt
	}
	return nativeScalarGuardedPresenceRow(s, row)
}
