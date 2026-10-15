package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphTransportFormat = "fdb-owned-ingress-transport-v2"
const graphFeedTransportFormat = "fdb-snapshot-feed-workload-v2"
const graphTransportMaxSeconds = uint64(365 * 24 * 60 * 60)
const graphTransportMaxSamples = uint64(31_536_002)
const graphTransportMaxArtifactBytes = uint64(1 << 44)
const graphTransportSampleBytes = uint64(2 << 20)
const graphTransportFixedBytes = uint64(16 << 20)
const graphTransportControlBytes = 16 << 10

type graphTransport struct {
	Kind            string `json:"kind"`
	TargetRTTMillis uint64 `json:"targetRTTMillis"`
	JitterMillis    uint64 `json:"jitterMillis"`
	Seed            uint64 `json:"seed"`
}
type graphTransportPeer struct {
	Role, Container, Image, IP, NetNS, Network string
	PID                                        uint64
}
type graphTransportTools struct {
	Version                      string
	TC, IP, Probe, Source, Build [32]byte
}

// This private policy binds the actual owned deployment, not the dataset. Its
// deadline is chosen before helpers/shaping/probes and never refreshed by a
// new SDK client. MaxSamples/Bytes admit STREAMED evidence, never allocation.
type graphTransportPolicy struct {
	Version                                                  uint8
	Spec                                                     graphFeedWorkloadSpec
	Run                                                      [16]byte
	Transport                                                graphTransport
	BudgetSeconds, CausalMinimumMillis                       uint64
	AdmittedUnixNanos, DeadlineUnixNanos                     int64
	QueuePackets, SampleMillis, MaxSamples, MaxArtifactBytes uint64
	ProbeSamples, ProbePayloadBytes, ProbeTimeoutMillis      uint64
	Tools                                                    graphTransportTools
	Peers                                                    [5]graphTransportPeer
	ArmedSHA256                                              [32]byte
}
type graphTransportRecord struct {
	Policy graphTransportPolicy
	Record graphCostStoredRecord
}

func graphFeedFormatFor(spec graphFeedWorkloadSpec) string {
	if spec.Version == 2 {
		return graphFeedTransportFormat
	}
	return graphFeedWorkloadFormat
}

func graphTransportCausalMinimum(spec graphFeedWorkloadSpec, t graphTransport) (uint64, error) {
	if spec.Facts < 20 || spec.Facts > 1_000_000 || spec.Facts%20 != 0 || spec.CrossPercent != 0 && spec.CrossPercent != 10 && spec.CrossPercent != 50 && spec.CrossPercent != 100 || spec.Writers != 1 && spec.Writers != 8 && spec.Writers != 32 || t.TargetRTTMillis > 100 {
		return 0, errInvalid
	}
	writers := uint64(spec.Writers) // #nosec G115 -- Exact positive allow-list above.
	waves := (spec.Facts - 1 + writers - 1) / writers
	pages := (spec.Facts + 7) / 8
	// Three setup commits, two seed+parallel phases, then complete page ACKs.
	// One mandatory client request/response per counted operation is a lower
	// bound only. Extra SDK reads, retries, scans and base latency are omitted.
	var rounds uint64
	if spec.Version == 3 && spec.Family == graphFeedWorkloadFamily {
		// Three serial anchors, complete create wave, first half-update wave,
		// one serial post-GC checkpoint update, remaining update wave, then
		// at least ceil(N/8) complete envelope Page/ACKs. Extra native/catalog/
		// GC/retry/probe IO is omitted, never inferred from configured RTT.
		half := spec.Facts / 2
		rounds = 4 + (spec.Facts+writers-1)/writers + (half+writers-1)/writers + (spec.Facts-half-1+writers-1)/writers + pages
		return rounds * t.TargetRTTMillis, nil
	}
	switch spec.Family {
	case graphFeedWorkloadFamily, graphOwnerTransferFamily:
		rounds = 5 + 2*waves + pages
	case graphSelectorsFamily, graphColumnsFamily, graphCreateUpdateFamily:
		rounds = 5 + 2*waves
	case graphCorrectionFamily:
		if spec.Facts/2 <= writers {
			return 0, errInvalid
		}
		// Real create and delayed HALF-correction phases each have their own
		// seed then independent callers. Retained scans/publication are omitted.
		rounds = 5 + waves + (spec.Facts/2-1+writers-1)/writers
	case graphContentionFamily:
		// Serial setup + seed half + isolated claim waves, and the longest
		// genuine increment dependency chain. Each counted step requires a
		// native request/response; capture/retry/query work is omitted.
		remote := spec.Facts * spec.CrossPercent / 100
		rounds = 3 + spec.Facts + max(remote, spec.Facts-remote)
	case graphEdgeUniqueFamily:
		// Seven serial lifecycle commits, N serial old-plan captures, N serial
		// original-close replays, plus the three barrier-separated worker phases.
		// Rejected stale/frozen-close legs and other SDK IO are omitted.
		rounds = 7 + 2*spec.Facts + (2*spec.Facts+writers-1)/writers + 2*((spec.Facts+writers-1)/writers)
	default:
		return 0, errInvalid
	}
	return rounds * t.TargetRTTMillis, nil
}
func transportHex(s string, size int) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == size && hex.EncodeToString(b) == s && !bytes.Equal(b, make([]byte, size))
}
func (p graphTransportPolicy) validate() error {
	s := p.Spec
	if !(p.Version == 2 && s.Version == 2 || p.Version == 3 && s.Version == 3 && s.Family == graphFeedWorkloadFamily && s.Facts >= 20 && s.Facts > uint64(s.Writers)) || !graphTransportFamily(s.Family) || s.Facts < 20 || s.Facts > 1_000_000 || s.Facts%20 != 0 || s.Writers != 1 && s.Writers != 8 && s.Writers != 32 || (s.Family == graphFeedWorkloadFamily || s.Family == graphSelectorsFamily || s.Family == graphColumnsFamily || s.Family == graphOwnerTransferFamily || s.Family == graphCreateUpdateFamily) && s.Facts < uint64(s.Writers)+1 || s.CrossPercent != 0 && s.CrossPercent != 10 && s.CrossPercent != 50 && s.CrossPercent != 100 || s.Layout != "kv" && s.Layout != "columns" || p.Run == [16]byte{} {
		return errInvalid
	}
	t := p.Transport
	if t.Seed != 20261009 || t.JitterMillis != 0 || !(t.Kind == "local" && t.TargetRTTMillis == 0 || t.Kind == "injected-rtt" && (t.TargetRTTMillis == 1 || t.TargetRTTMillis == 20 || t.TargetRTTMillis == 100)) {
		return errInvalid
	}
	minimum, err := graphTransportCausalMinimum(s, t)
	if err != nil || p.CausalMinimumMillis != minimum || p.BudgetSeconds < 1 || p.BudgetSeconds > graphTransportMaxSeconds || p.BudgetSeconds*1000 <= minimum || p.AdmittedUnixNanos <= 0 || p.AdmittedUnixNanos > math.MaxInt64-int64(p.BudgetSeconds)*int64(time.Second) || p.DeadlineUnixNanos != p.AdmittedUnixNanos+int64(p.BudgetSeconds)*int64(time.Second) {
		return errors.Join(errInvalid, err)
	}
	if p.QueuePackets < 1 || p.QueuePackets > 4096 || p.SampleMillis < 1000 || p.SampleMillis > 60000 || p.MaxSamples < (p.BudgetSeconds*1000+p.SampleMillis-1)/p.SampleMillis+2 || p.MaxSamples > graphTransportMaxSamples || p.MaxArtifactBytes < graphTransportFixedBytes || p.MaxArtifactBytes > graphTransportMaxArtifactBytes || p.MaxSamples > (p.MaxArtifactBytes-graphTransportFixedBytes)/graphTransportSampleBytes || p.ProbeSamples != 9 || p.ProbePayloadBytes != 64 || p.ProbeTimeoutMillis != 3000 {
		return errInvalid
	}
	if p.Tools.Version != "iproute2-6.12.0" || p.Tools.TC == [32]byte{} || p.Tools.IP == [32]byte{} || p.Tools.Probe == [32]byte{} || hex.EncodeToString(p.Tools.Source[:]) != "bbd141ef7b5d0127cc2152843ba61f274dc32814fa3e0f13e7d07a080bef53d9" || p.Tools.Build == [32]byte{} || p.ArmedSHA256 == [32]byte{} {
		return errInvalid
	}
	for i, peer := range p.Peers {
		role := "zone" + strconv.Itoa(i)
		if i == 4 {
			role = "client"
		}
		address, err := netip.ParseAddr(peer.IP)
		if peer.Role != role || !transportHex(peer.Container, 32) || !transportHex(peer.Network, 32) || len(peer.Image) != 71 || peer.Image[:7] != "sha256:" || !transportHex(peer.Image[7:], 32) || err != nil || !address.Is4() || address.String() != peer.IP || peer.PID == 0 || len(peer.NetNS) < 7 || len(peer.NetNS) > 64 || peer.NetNS[:5] != "net:[" || peer.NetNS[len(peer.NetNS)-1] != ']' {
			return errors.Join(errInvalid, err)
		}
		if _, err := strconv.ParseUint(peer.NetNS[5:len(peer.NetNS)-1], 10, 64); err != nil {
			return errors.Join(errInvalid, err)
		}
		for _, prior := range p.Peers[:i] {
			if prior.Container == peer.Container || prior.PID == peer.PID || prior.IP == peer.IP || prior.NetNS == peer.NetNS || prior.Network != peer.Network {
				return errInvalid
			}
		}
	}
	return nil
}
func (p graphTransportPolicy) digest() ([32]byte, error) {
	if err := p.validate(); err != nil {
		return [32]byte{}, err
	}
	body, err := wire(p)
	if err != nil || len(body) > graphTransportControlBytes {
		return [32]byte{}, errors.Join(errLimit, err)
	}
	return sha256.Sum256(body), nil
}

// The gate is a rooted, bounded, atomically linked controller observation. It
// runs BEFORE openDB. Seeing it grants no SDK success or ownership by itself.
func graphTransportFromEnv() (*graphTransportPolicy, error) {
	if os.Getenv("RHO_GRAPH_WORKLOAD_TRANSPORT") == "" {
		return nil, nil
	}
	if os.Getenv("RHO_GRAPH_WORKLOAD_TRANSPORT") != graphTransportFormat && os.Getenv("RHO_GRAPH_WORKLOAD_TRANSPORT") != graphFeedRetainedTransportFormat {
		return nil, errInvalid
	}
	root, err := os.OpenRoot(os.Getenv("FDB_COST_ARTIFACT"))
	if err != nil {
		return nil, err
	}
	defer root.Close()
	file, err := root.Open("transport-admission.json")
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(io.LimitReader(file, graphTransportControlBytes+1))
	err = errors.Join(err, file.Close())
	if err != nil || len(body) > graphTransportControlBytes {
		return nil, errors.Join(errLimit, err)
	}
	p, err := decode[graphTransportPolicy](body)
	if err != nil || p.validate() != nil || (p.Version == 3) != (os.Getenv("RHO_GRAPH_WORKLOAD_TRANSPORT") == graphFeedRetainedTransportFormat) {
		return nil, errors.Join(errInvalid, err)
	}
	if os.Getenv("RHO_GRAPH_WORKLOAD_SECONDS") != strconv.FormatUint(p.BudgetSeconds, 10) || os.Getenv("RHO_GRAPH_WORKLOAD_DEADLINE") != strconv.FormatInt(p.DeadlineUnixNanos, 10) || os.Getenv("RHO_GRAPH_WORKLOAD_RUN") != hex.EncodeToString(p.Run[:]) || time.Now().UnixNano() < p.AdmittedUnixNanos || time.Now().UnixNano() >= p.DeadlineUnixNanos {
		return nil, context.DeadlineExceeded
	}
	return &p, nil
}

func (h *harness) graphTransportInstall(ctx context.Context, p graphTransportPolicy) (graphCostStoredRecord, error) {
	if ctx == nil || h == nil || p.validate() != nil || h.config != graphTransportConfig(p.Spec, hex.EncodeToString(p.Run[:])) {
		return graphCostStoredRecord{}, errInvalid
	}
	body, err := wire(p)
	if err != nil {
		return graphCostStoredRecord{}, err
	}
	key := h.key(0, "workload-transport-v2", "binding")
	if len(key)+len(body) > graphTransportControlBytes {
		return graphCostStoredRecord{}, errLimit
	}
	_, err = graphCostWriteContext(ctx, h.db, func(tr fdb.Transaction) (any, error) {
		for g := uint8(0); g < 2; g++ {
			if err := h.checkConfig(tr, g); err != nil {
				return nil, err
			}
		}
		prior, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		if prior != nil && !bytes.Equal(prior, body) {
			return nil, errMismatch
		}
		tr.Set(key, body)
		return nil, nil
	})
	if err != nil {
		return graphCostStoredRecord{}, err
	}
	stored, _, err := graphCostParallelRead(ctx, h.db, func(tr fdb.ReadTransaction) (graphCostStoredRecord, error) {
		actual, err := snapshotStored(tr, key, graphTransportControlBytes)
		if err != nil || !bytes.Equal(actual.Value, body) {
			return actual, errors.Join(errMismatch, err)
		}
		return actual, nil
	})
	return stored, err
}

// Family dispatch is private to the explicit transported spec. Ordinary config
// constructors and Cost.Version meanings remain unchanged.
func graphTransportFamily(family string) bool {
	return family == graphFeedWorkloadFamily || family == graphContentionFamily || family == graphSelectorsFamily || family == graphEdgeUniqueFamily || family == graphColumnsFamily || family == graphCorrectionFamily || family == graphOwnerTransferFamily || family == graphCreateUpdateFamily
}
func graphTransportConfig(spec graphFeedWorkloadSpec, run string) config {
	if spec.Version == 3 && spec.Family == graphFeedWorkloadFamily {
		return graphFeedRetainedConfig(spec, run)
	}
	switch spec.Family {
	case graphCreateUpdateFamily:
		return graphCreateUpdateConfig(graphSelectorsSpec(spec), run)
	case graphOwnerTransferFamily:
		return graphOwnerTransferConfig(spec, run)
	case graphFeedWorkloadFamily:
		return graphFeedWorkloadConfig(spec, run)
	case graphContentionFamily:
		c := graphContentionConfig(graphContentionSpec(spec).profile(), spec.Layout, run)
		c.Graph = strings.Replace(c.Graph, "contention-v1-", "contention-v2-", 1)
		return c
	case graphSelectorsFamily:
		return graphSelectorsConfig(graphSelectorsSpec(spec), run)
	case graphColumnsFamily:
		return graphColumnsConfig(graphSelectorsSpec(spec), run)
	case graphCorrectionFamily:
		return graphCorrectionConfig(graphSelectorsSpec(spec), run)
	case graphEdgeUniqueFamily:
		c := graphEdgeUniqueConfig(graphEdgeUniqueSpec(spec).profile(), spec.Layout, run)
		c.Graph = strings.Replace(c.Graph, "unique-v1-", "unique-v2-", 1)
		return c
	}
	return config{}
}
func graphTransported(c config) bool {
	return strings.HasPrefix(c.Graph, graphFeedRetainedNamespace) || strings.HasPrefix(c.Graph, "graph-workload-contention-v2-") || strings.HasPrefix(c.Graph, "graph-workload-selectors-v2-") || strings.HasPrefix(c.Graph, "graph-workload-edge-unique-v2-") || strings.HasPrefix(c.Graph, "graph-workload-columns-v2-") || strings.HasPrefix(c.Graph, "graph-workload-correction-v2-") || strings.HasPrefix(c.Graph, "graph-workload-create-update-v2-") || strings.HasPrefix(c.Graph, graphOwnerTransportNamespace)
}

// Every new business capture/apply authenticates the SAME actual native policy
// inside its original transaction. A graph label alone grants no admission.
func (h *harness) graphTransportNative(tr fdb.ReadTransaction) (*graphTransportPolicy, error) {
	if h == nil {
		return nil, errInvalid
	}
	if !graphTransported(h.config) {
		return nil, nil
	}
	raw, err := tr.Get(h.key(0, "workload-transport-v2", "binding")).Get()
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || len(raw) > graphTransportControlBytes {
		return nil, errMismatch
	}
	p, err := decode[graphTransportPolicy](raw)
	if err != nil || p.validate() != nil || graphTransportConfig(p.Spec, hex.EncodeToString(p.Run[:])) != h.config {
		return nil, errors.Join(errMismatch, err)
	}
	return &p, nil
}
func graphTransportAdmission(version uint8, family, run string) (*graphTransportPolicy, error) {
	p, err := graphTransportFromEnv()
	if err != nil {
		return nil, err
	}
	if version == 1 && p == nil {
		return nil, nil
	}
	if version != 2 || p == nil || p.Spec.Family != family || hex.EncodeToString(p.Run[:]) != run {
		return nil, errMismatch
	}
	return p, nil
}
func graphTransportContext(p *graphTransportPolicy) (context.Context, context.CancelFunc) {
	if p == nil {
		return context.Background(), func() {}
	}
	return context.WithDeadline(context.Background(), time.Unix(0, p.DeadlineUnixNanos))
}
func graphTransportIO(ctx context.Context, p *graphTransportPolicy) context.Context {
	if p == nil {
		return nil
	}
	return ctx
}
func graphTransportFresh(ctx context.Context, h *harness) (cut, error) {
	if h == nil {
		return cut{}, errInvalid
	}
	if ctx == nil {
		return h.fresh([]uint8{0, 1}, nil)
	}
	got, err := graphCostWriteContext(ctx, h.db, func(tr fdb.Transaction) (any, error) { return h.freshIn(tr, []uint8{0, 1}, nil) })
	if err != nil {
		return cut{}, err
	}
	return got.(cut), nil
}
func graphTransportFormatFor(version uint8, legacy string) string {
	if version == 2 {
		return strings.TrimSuffix(legacy, "-v1") + "-v2"
	}
	return legacy
}
func graphTransportDigest(p *graphTransportPolicy) (digest [32]byte) {
	// Native and artifact callers validate the policy before deriving this
	// private comparable binding field; nil is the exact ordinary zero value.
	if p != nil {
		digest, _ = p.digest()
	}
	return digest
}
func graphTransportBindingKind(c config, legacy string) string {
	if graphTransported(c) {
		return strings.TrimSuffix(legacy, "-v1") + "-v2"
	}
	return legacy
}

func graphTransportLegacyContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// Fresh raw exports use the actual retained At owner and its independently
// prepaid bounded cleanup lifetime; no new pin is needed for an already held
// owner. Ordinary callers retain their literal historical pin/open/release.
func graphTransportHeld(ctx, caller context.Context, s *graphHistoryRetention, c cut, name string) (*graphHistoryRead, func() error, func() error, error) {
	if s == nil || caller == nil {
		return nil, nil, nil, errInvalid
	}
	if ctx != nil {
		view, err := s.OpenGraphSnapshot(ctx, graphSnapshotRequest{Mode: "At", Scope: []uint8{0, 1}, At: c.record}, graphSelectorsPolicy(s.h.config).Read)
		if err != nil {
			return nil, nil, nil, err
		}
		return view.owner, view.Close, view.Close, nil
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, nil, nil, err
	}
	token, err := s.pin(name, c, secret)
	if err != nil {
		return nil, nil, nil, err
	}
	owner, err := s.open(caller, token)
	if err != nil {
		return nil, nil, nil, err
	}
	closeOwner := func() error {
		if err := historyReadCleanup(owner); err != nil {
			return err
		}
		return s.release(name+"-close", token)
	}
	cleanupOwner := func() error { return errors.Join(historyReadCleanup(owner), s.release(name+"-close", token)) }
	return owner, closeOwner, cleanupOwner, nil
}
func graphTransportFamilyConfig(spec graphFeedWorkloadSpec, run string) config {
	if spec.Version == 2 {
		return graphTransportConfig(spec, run)
	}
	switch spec.Family {
	case graphCreateUpdateFamily:
		return graphCreateUpdateConfig(graphSelectorsSpec(spec), run)
	case graphContentionFamily:
		return graphContentionConfig(graphContentionSpec(spec).profile(), spec.Layout, run)
	case graphSelectorsFamily:
		return graphSelectorsConfig(graphSelectorsSpec(spec), run)
	case graphColumnsFamily:
		return graphColumnsConfig(graphSelectorsSpec(spec), run)
	case graphCorrectionFamily:
		return graphCorrectionConfig(graphSelectorsSpec(spec), run)
	case graphEdgeUniqueFamily:
		return graphEdgeUniqueConfig(graphEdgeUniqueSpec(spec).profile(), spec.Layout, run)
	}
	return config{}
}
func graphTransportNewHarness(ctx context.Context, db fdb.Database, c config) (*harness, error) {
	if ctx == nil {
		return newHarness(db, c)
	}
	return newHarnessContext(ctx, db, c)
}
func graphTransportArtifact(h *harness, spec graphFeedWorkloadSpec, record *graphTransportRecord) (*graphTransportPolicy, error) {
	if spec.Version == 1 && record == nil {
		return nil, nil
	}
	if spec.Version != 2 || record == nil || h == nil || record.Policy.Spec != spec || record.Policy.validate() != nil || graphTransportConfig(spec, hex.EncodeToString(record.Policy.Run[:])) != h.config {
		return nil, errMismatch
	}
	body, err := wire(record.Policy)
	if err != nil || !bytes.Equal(record.Record.Key, h.key(0, "workload-transport-v2", "binding")) || !bytes.Equal(record.Record.Value, body) {
		return nil, errors.Join(errMismatch, err)
	}
	return &record.Policy, nil
}

func graphTransportConfigVersion(c config) uint8 {
	if graphTransported(c) {
		return 2
	}
	return 1
}

// Shared SAME-body submit/original doors authenticate the fresh family policy
// even when execute returns its retained original before native graph predicates.
// Older graphs take the literal no-read branch.
func (h *harness) graphTransportCheckIn(tr fdb.ReadTransaction) error {
	if h == nil {
		return errInvalid
	}
	if h.config.Cost.Version == 9 {
		return h.graphWorkloadBindingIn(tr, "workload-create-update-v1")
	}
	if h.config.Cost.Version == 6 {
		// This new family's ordinary and transported original fast paths both
		// require its actual native admission before returning a retained reply.
		return h.graphWorkloadBindingIn(tr, "workload-columns-v1")
	}
	if h.config.Cost.Version == 7 {
		return h.graphWorkloadBindingIn(tr, "workload-correction-v1")
	}
	if !graphTransported(h.config) {
		return nil
	}
	switch h.config.Cost.Version {
	case 10:
		p, err := h.graphTransportNative(tr)
		if err != nil {
			return err
		}
		if p == nil || p.Version != 3 || p.Spec.Family != graphFeedWorkloadFamily {
			return errMismatch
		}
		return nil
	case 8:
		p, err := h.graphTransportNative(tr)
		if err != nil {
			return err
		}
		if p == nil || p.Spec.Family != graphOwnerTransferFamily {
			return errMismatch
		}
		return nil
	case 2:
		return h.graphContentionBindingIn(tr)
	case 3:
		return h.graphSelectorsBindingIn(tr)
	case 4:
		return h.graphEdgeUniqueBindingIn(tr)
	}
	return errInvalid
}
