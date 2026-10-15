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

const nativeFeedFamily = "snapshot-to-feed"
const nativeFeedFormat = "native-snapshot-feed-workload-v1"
const nativeFeedMarker = "GNFW1"
const nativeFeedMaxOriginals = 64
const nativeFeedSameInvocations = 8
const nativeFeedExtraInvocations = 64

// The immutable workload admission describes actual work; it grants no pin,
// certificate, grant or native original authority to its copied fields.
type nativeFeedSpec struct {
	Version      int    `json:"version"`
	Family       string `json:"family"`
	Facts        uint64 `json:"facts"`
	CrossPercent uint64 `json:"crossPercent"`
	Writers      int    `json:"writers"`
	Layout       string `json:"layout"`
}

func (s nativeFeedSpec) cost() nativeGraphCostSpec {
	if s.Version == 2 {
		return nativeGraphCostSpec{Version: 13, Facts: s.Facts, CrossPercent: s.CrossPercent, Writers: s.Writers}
	}
	return nativeGraphCostSpec{Version: 5, Facts: s.Facts, CrossPercent: s.CrossPercent, Writers: s.Writers}
}
func nativeFeedValidate(s nativeGraphCostSpec) error {
	if s.Version != 5 || s.Facts < 20 || s.Facts > 1_000_000 || s.Facts%20 != 0 || s.CrossPercent != 0 && s.CrossPercent != 10 && s.CrossPercent != 50 && s.CrossPercent != 100 || s.Writers != 1 && s.Writers != 8 && s.Writers != 32 || s.Facts < uint64(s.Writers+1) {
		return errInvalid
	}
	return nil
}
func nativeFeedSpecFromEnv() (s nativeFeedSpec, err error) {
	body := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_SPEC"))
	if len(body) == 0 || len(body) > 1024 {
		return s, errInvalid
	}
	if err = nativeFeedDecodeJSON(body, &s); err != nil {
		return s, err
	}
	if s.Version != 1 || s.Family != nativeFeedFamily || s.Layout != "kv" && s.Layout != "columns" || nativeFeedValidate(s.cost()) != nil {
		return s, errInvalid
	}
	return s, nil
}

type nativeFeedAdmission struct {
	Version                            uint8
	Family, Run                        string
	Spec                               nativeFeedSpec
	Transport                          nativeWorkloadTransportSpec
	BudgetSeconds, CausalMinimumMillis uint64
	DeadlineUnixNanos                  int64
	Graph                              [16]byte
	Topology                           uint64
	Declaration                        [32]byte
	BinarySHA256                       string
}

func nativeFeedAdmissionFromEnv(s nativeFeedSpec) (a nativeFeedAdmission, digest [32]byte, err error) {
	if nativeFeedValidate(s.cost()) != nil {
		return a, digest, errInvalid
	}
	return nativeScalarFeedAdmissionFromEnv(s)
}

func nativeScalarFeedAdmissionFromEnv(s nativeFeedSpec) (a nativeFeedAdmission, digest [32]byte, err error) {
	deadline, err := nativeGraphEdgeUniqueDeadlineFromEnv()
	if err != nil {
		return a, digest, err
	}
	seconds, _ := strconv.ParseUint(os.Getenv("RHO_GRAPH_WORKLOAD_SECONDS"), 10, 32)
	run := os.Getenv("RHO_GRAPH_WORKLOAD_RUN")
	nonce, e := hex.DecodeString(run)
	if e != nil || len(nonce) != 16 || hex.EncodeToString(nonce) != run || bytes.Equal(nonce, make([]byte, 16)) {
		return a, digest, errInvalid
	}
	var transport nativeWorkloadTransportSpec
	wire := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_TRANSPORT"))
	if len(wire) > 1024 || nativeFeedDecodeJSON(wire, &transport) != nil || transport.validate() != nil || transport.Seed != 20261009 {
		return a, digest, errInvalid
	}
	declaration, e := nativeGraphEdgeUniqueTransportDeclaration()
	if e != nil {
		return a, digest, e
	}
	binary, e := nativeGraphEdgeUniqueExecutable()
	if e != nil {
		return a, digest, e
	}
	// Three sequential anchor creations and the two fact0 value-definition
	// originals each complete native replication before dependents are admitted.
	// Snapshot publication adds ordered participant work, omitted here: this is
	// only a necessary causal-delay floor, never a runtime prediction.
	floor := uint64(5) * transport.TargetRTTMillis
	if seconds*1000 <= floor {
		return a, digest, errLimit
	}
	a = nativeFeedAdmission{1, nativeFeedFamily, run, s, transport, seconds, floor, deadline.UnixNano(), [16]byte(declaration.Graph()), declaration.TopologyEpoch(), declaration.Digest(), binary}
	body, e := json.Marshal(a)
	return a, sha256.Sum256(body), e
}
func nativeFeedProcessProfile(s *nativeGraphCostSpec) (bool, error) {
	flag := os.Getenv("RHO_GRAPH_FEED_PROCESS")
	if flag == "2" {
		return nativeGuardedScalarProcessProfile(s, true)
	}
	if flag == "" {
		if s != nil && (s.Version == 5 || s.Version == 13) {
			return false, errInvalid
		}
		return false, nil
	}
	if flag != "1" || s == nil || nativeFeedValidate(*s) != nil || os.Getenv("RHO_GRAPH_CERTIFIED_PROCESS") != "1" || os.Getenv("RHO_GRAPH_SOURCE_PROCESS") != "1" {
		return false, errInvalid
	}
	for _, name := range []string{"RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_EDGE_UNIQUE_PROCESS", "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_NATIVE_WORKLOAD", "RHO_GRAPH_NATIVE_FAULT_FAMILY"} {
		if value := os.Getenv(name); value != "" && value != "0" {
			return false, errInvalid
		}
	}
	spec, err := nativeFeedSpecFromEnv()
	if err != nil || spec.cost() != *s {
		return false, errors.Join(errInvalid, err)
	}
	_, _, err = nativeFeedAdmissionFromEnv(spec)
	return err == nil, err
}
func nativeFeedPages(s nativeGraphCostSpec) uint64     { return 32*s.Facts + 256 }
func nativeFeedOriginals(s nativeGraphCostSpec) uint64 { return 16*s.Facts + 3 }
func nativeFeedFrames(s nativeGraphCostSpec) (uint64, error) {
	if nativeFeedValidate(s) != nil {
		return 0, errInvalid
	}
	// Cross proposals hold all8 local slots before capture. At most8 local
	// originals coexist: each useful local install can invalidate at most7
	// others. Two N-operation phases therefore admit <=16N+3 original captures,
	// with a separately enforced whole-cell ceiling and per-original64 limit.
	// A first Run emits<=6 phases +3 capacity-help +1 Prepare retry. SAME
	// invocations spend a separate whole-cell64 pool. Page/ACK emit five entries
	// per participant. After snapshot C, at most160N
	// business/protocol entries plus <=N/8 snapshot admissions must drain. Every
	// protocol page consumes32 and every useful envelope consumes>=1; hence
	// P <= N+(160N+N/8+5P+fixed)/32, safely below32N+256. Add snapshot admissions,
	// boundary controls, issuance and two derived generations' complete cleanup.
	// Original replay does not advance a second time. This is not measured cost.
	return nativeFeedOriginals(s)*10 + nativeFeedExtraInvocations*10 + nativeFeedPages(s)*5 + 4*((s.Facts+15)/16) + 4096, nil
}
func nativeFeedGrantPlan(s nativeGraphCostSpec, block, partition, sequence uint64) (uint64, uint64, error) {
	if nativeFeedValidate(s) != nil || block < 2 || block > idalloc.MaxBlockSize || sequence == 0 {
		return 0, 0, errInvalid
	}
	if partition == 3 {
		first := 1 + (sequence-1)*block
		if sequence > (s.homeCount()+block-1)/block {
			return 0, 0, errInvalid
		}
		return first, min(block, s.homeCount()-first+1), nil
	}
	if partition == 8 && sequence == 1 {
		return s.remoteNode(), 2, nil
	}
	return 0, 0, errInvalid
}
func nativeFeedProfile(s nativeGraphCostSpec, init *declaredInitCommand, l *raftlog.Limits, p *raftlog.ApplicationPolicy, g *raftlog.ApplicationGenerationLimits) error {
	if init == nil || l == nil || p == nil || g == nil || nativeFeedValidate(s) != nil || p.MaxInstallWrites != 256 || p.MaxImageBytes != 172 {
		return errInvalid
	}
	init.maxBlock = min(s.homeCount(), idalloc.MaxBlockSize)
	frames, err := nativeFeedFrames(s)
	if err != nil || frames > (^uint64(0)-(64<<20))/(64<<10) || frames > (^uint64(0)-65536)/260 {
		return errors.Join(errLimit, err)
	}
	// Installed bytes have an independent512GiB ceiling, leaving a second bank
	// inside the existing native1TiB total-generation ceiling. The
	// theoretical frame/output maxima are NOT a promise that every maximal
	// frame can be retained simultaneously; actual exhaustion fails the cell.
	retainedBytes := min(uint64(64<<20)+frames*(64<<10), uint64(1<<39))
	retainedRecords := uint64(65536) + frames*260
	if retainedBytes > 1<<39 || retainedRecords > 1<<39 {
		return errLimit
	}
	l.MaxRetainedBytes, l.MaxRetainedEntries = retainedBytes, 1_000_000
	p.RetainedApplicationBytes, p.RetainedApplicationRecords = retainedBytes, retainedRecords
	*g = raftlog.ApplicationGenerationLimits{MaxBytes: 2 * retainedBytes, MaxRecords: 2 * retainedRecords}
	return nil
}
func nativeFeedPolicy(s nativeGraphCostSpec, deadline int64) retainedFeedPolicy {
	l := defaultMaterializerLimits()
	return retainedFeedPolicy{pageBytes: 64 << 10, pageRows: 16, maxPages: nativeFeedPages(s), maxAttempts: 4*nativeFeedPages(s) + s.Facts/16 + 512, operationRows: l.sourceRows, operationBytes: l.sourceBytes, operationOutput: l.outputBytes, deadlineUnixNanos: deadline}
}

// The full fresh column policy uses the existing Version1 aggregate decoder
// allowance, moderate16/16 chunks and additive failed-page pool. All checked
// arithmetic refuses before stores/workers; defaults remain unchanged.
func nativeFeedColumnPolicy(h *allocationHost, s nativeFeedSpec, a nativeFeedAdmission, digest [32]byte) (nativeColumnPolicy, error) {
	if h == nil || h.machine == nil || h.machine.SemanticContractID() != retainedFeedSemanticContractID() || a.Spec != s || digest == ([32]byte{}) {
		return nativeColumnPolicy{}, errInvalid
	}
	p := nativeColumnPolicy{Version: 1, Admission: digest, ChunkRecords: 16, ChunkRows: 16, ChunkBytes: 64 << 10, FrameBytes: 32 << 10, MaxGenerations: 2, MaxChunks: 1 << 40, MaxRetainedBytes: 1 << 62, MaxWorkRecords: ^uint64(0), MaxWorkBytes: ^uint64(0), MaxDecodedRows: ^uint64(0), MaxOutputBytes: ^uint64(0), MaxRPCs: ^uint64(0), MaxWireBytes: ^uint64(0), CacheBytes: 2 << 20, MemTableBytes: 1 << 20, Lifetime: time.Minute}
	frames, err := nativeFeedFrames(s.cost())
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
	pages, err := page.scaled((frames+1)*passes + failed)
	if err != nil {
		return p, err
	}
	starts := a.BudgetSeconds/60 + 13
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
	if p.validate() != nil {
		return p, errLimit
	}
	return p, nil
}
