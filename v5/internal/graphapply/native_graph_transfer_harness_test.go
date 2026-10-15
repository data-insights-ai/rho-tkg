package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strconv"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativeTransferFamily = "transfer-retained-pins"
const nativeTransferFormat = "native-both-home-transfer-workload-v1"
const nativeTransferMarker = "GNTW1"
const nativeTransferProcessFlag = "RHO_GRAPH_TRANSFER_PROCESS"

// This explicit profile selects pooled range movement before native Open. The
// copied admission describes consumptive ceilings, never placement, a cursor,
// an original result or proof that the mandatory workload fits those ceilings.
type nativeTransferSpec struct {
	Version      int    `json:"version"`
	Family       string `json:"family"`
	Facts        uint64 `json:"facts"`
	CrossPercent uint64 `json:"crossPercent"`
	Writers      int    `json:"writers"`
	Layout       string `json:"layout"`
}

func (s nativeTransferSpec) cost() nativeGraphCostSpec {
	return nativeGraphCostSpec{12, s.Facts, s.CrossPercent, s.Writers}
}

func nativeTransferValidate(s nativeGraphCostSpec) error {
	if s.Version != 12 {
		return errInvalid
	}
	s.Version = 5 // Dimensions only; no retained-C Cost5 authority is borrowed.
	return nativeFeedValidate(s)
}

func nativeTransferSpecFromEnv() (s nativeTransferSpec, err error) {
	wire := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_SPEC"))
	if len(wire) == 0 || len(wire) > 1024 || nativeFeedDecodeJSON(wire, &s) != nil || s.Version != 2 || s.Family != nativeTransferFamily || s.Layout != "kv" && s.Layout != "columns" || nativeTransferValidate(s.cost()) != nil {
		return s, errInvalid
	}
	return s, nil
}

type nativeTransferGrants struct {
	MaxPages      uint64
	DescribeBytes uint64
	ImportBytes   uint64
	UndoBytes     uint64
}

func (g nativeTransferGrants) pools() rangeMovePools {
	return rangeMovePools{g.DescribeBytes, g.ImportBytes, g.UndoBytes}
}

func (g nativeTransferGrants) validate() error {
	if g.MaxPages == 0 || g.MaxPages > 4_000_000 || !g.pools().valid(true) {
		return errInvalid
	}
	// Native count/log reservations remain the full immutable per-step maxima.
	// Actual existing rows and first-move growth are checked again at Fence.
	p := g.MaxPages
	if 2*p+6 > 10_000_000 || 82086*p+1083890 > 1<<40 || g.DescribeBytes > (128<<10)*(p+1) || g.ImportBytes > ((64<<10)+16*(64<<10)+16384)*p || g.UndoBytes > ((64<<10)+16*(64<<10)+16384)*p {
		return errLimit
	}
	w := uint64(64 * (4 << 20))
	if g.DescribeBytes > (1<<40)-1376256-w || g.ImportBytes > (1<<40)-3473408-w || g.UndoBytes > (1<<40)-3473408-w-g.ImportBytes {
		return errLimit
	}
	return nil
}

type nativeTransferAdmission struct {
	Version                            uint8
	Family, Run                        string
	Spec                               nativeTransferSpec
	Transport                          nativeWorkloadTransportSpec
	BudgetSeconds, CausalMinimumMillis uint64
	DeadlineUnixNanos                  int64
	Graph                              [16]byte
	Topology                           uint64
	Declaration                        [32]byte
	BinarySHA256                       string
	Grants                             nativeTransferGrants
}

func nativeTransferAdmissionFromEnv(s nativeTransferSpec) (a nativeTransferAdmission, digest [32]byte, err error) {
	if s.Version != 2 || s.Family != nativeTransferFamily || s.Layout != "kv" && s.Layout != "columns" || nativeTransferValidate(s.cost()) != nil {
		return a, digest, errInvalid
	}
	deadline, err := nativeGraphEdgeUniqueDeadlineFromEnv()
	if err != nil {
		return a, digest, err
	}
	seconds, err := strconv.ParseUint(os.Getenv("RHO_GRAPH_WORKLOAD_SECONDS"), 10, 32)
	if err != nil || seconds == 0 || seconds > 365*24*60*60 {
		return a, digest, errors.Join(errInvalid, err)
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
	var grants nativeTransferGrants
	wire = []byte(os.Getenv("RHO_GRAPH_TRANSFER_GRANTS"))
	if len(wire) == 0 || len(wire) > 1024 || nativeFeedDecodeJSON(wire, &grants) != nil {
		return a, digest, errInvalid
	}
	if err := grants.validate(); err != nil {
		return a, digest, err
	}
	declaration, err := nativeGraphEdgeUniqueTransportDeclaration()
	if err != nil {
		return a, digest, err
	}
	binary, err := nativeGraphEdgeUniqueExecutable()
	if err != nil {
		return a, digest, err
	}
	// Three setup anchors, two scalar definitions, K, and two ordered moves
	// each require a genuine source/replication round. Page traffic is additional;
	// this necessary floor is not a completion-time or capacity prediction.
	floor := uint64(8) * transport.TargetRTTMillis
	if seconds*1000 <= floor {
		return a, digest, errLimit
	}
	a = nativeTransferAdmission{1, nativeTransferFamily, run, s, transport, seconds, floor, deadline.UnixNano(), [16]byte(declaration.Graph()), declaration.TopologyEpoch(), declaration.Digest(), binary, grants}
	body, err := json.Marshal(a)
	return a, sha256.Sum256(body), err
}

func nativeTransferOriginals(s nativeGraphCostSpec) uint64 { return 16*s.Facts + 8 }
func nativeTransferPages(s nativeGraphCostSpec) uint64     { return 64*s.Facts + 1024 }
func nativeTransferFrames(s nativeGraphCostSpec, g nativeTransferGrants) (uint64, error) {
	if nativeTransferValidate(s) != nil {
		return 0, errInvalid
	}
	if err := g.validate(); err != nil {
		return 0, err
	}
	// This is a ceiling on actual consumptive work/calls, not invented native
	// entries or prepaid storage discounts. SAME uncertainty spends a global64
	// pool; it does not silently multiply every successful operation by eight.
	return 10*nativeTransferOriginals(s) + 640 + 5*nativeTransferPages(s) + 4*g.MaxPages + 16*((s.Facts+15)/16) + 16384 + 128, nil
}

func nativeTransferGrantPlan(s nativeGraphCostSpec, block, partition, sequence uint64) (uint64, uint64, error) {
	if nativeTransferValidate(s) != nil || block != min(s.homeCount(), idalloc.MaxBlockSize) || sequence == 0 {
		return 0, 0, errInvalid
	}
	home := (s.homeCount() + block - 1) / block
	if partition == 3 {
		if sequence <= home {
			first := 1 + (sequence-1)*block
			return first, min(block, s.homeCount()-first+1), nil
		}
		if sequence == home+1 {
			return s.remoteNode() + 2, 2, nil
		} // real successor epoch grant, after remote2
	}
	if partition == 8 && sequence == 1 {
		return s.remoteNode(), 2, nil
	}
	return 0, 0, errInvalid
}

func nativeTransferProfile(s nativeGraphCostSpec, a nativeTransferAdmission, init *declaredInitCommand, l *raftlog.Limits, p *raftlog.ApplicationPolicy, generation *raftlog.ApplicationGenerationLimits) error {
	if init == nil || l == nil || p == nil || generation == nil || a.Spec.cost() != s || a.Spec.Version != 2 || a.Family != nativeTransferFamily || p.MaxInstallWrites != 256 || p.MaxImageBytes != 172 || p.MaxInstallBytes != 4<<20 || p.MaxTailEntries != 64 {
		return errInvalid
	}
	frames, err := nativeTransferFrames(s, a.Grants)
	if err != nil || frames > (^uint64(0)-65536-2*(256*a.Grants.MaxPages+336))/260 {
		return errors.Join(errLimit, err)
	}
	records := uint64(65536) + 260*frames + 2*(256*a.Grants.MaxPages+336)
	if records > 1<<40 {
		return errLimit
	}
	init.maxBlock = min(s.homeCount(), idalloc.MaxBlockSize)
	l.MaxRetainedBytes, l.MaxRetainedEntries = 1<<40, 10_000_000
	p.RetainedApplicationBytes, p.RetainedApplicationRecords = 1<<40, records
	// A second generation is admitted only against the actual native bank
	// counters. It is never automatically funded by doubling a one-TiB grant.
	*generation = raftlog.ApplicationGenerationLimits{MaxBytes: 1 << 40, MaxRecords: min(2*records, uint64(1<<40))}
	return nil
}

func nativeTransferColumnPolicy(h *allocationHost, s nativeTransferSpec, a nativeTransferAdmission, digest [32]byte) (nativeColumnPolicy, error) {
	if h == nil || h.machine == nil || h.machine.SemanticContractID() != rangeMovePooledSemanticContractID() || h.machine.store.ApplicationRecovery().Version != 4 || a.Spec != s || a.Family != nativeTransferFamily || digest == ([32]byte{}) {
		return nativeColumnPolicy{}, errInvalid
	}
	frames, err := nativeTransferFrames(s.cost(), a.Grants)
	if err != nil {
		return nativeColumnPolicy{}, err
	}
	return nativeScalarReadColumnPolicy(h, frames, a.BudgetSeconds, digest)
}
