package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net/netip"
	"os"
	"strconv"
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
	if spec.Facts < 20 || spec.Facts > 1_000_000 || spec.Writers != 1 && spec.Writers != 8 && spec.Writers != 32 || t.TargetRTTMillis > 100 {
		return 0, errInvalid
	}
	writers := uint64(spec.Writers) // #nosec G115 -- Exact positive allow-list above.
	waves := (spec.Facts - 1 + writers - 1) / writers
	pages := (spec.Facts + 7) / 8
	// Three setup commits, two seed+parallel phases, then complete page ACKs.
	// One mandatory client request/response per counted operation is a lower
	// bound only. Extra SDK reads, retries, scans and base latency are omitted.
	return (5 + 2*waves + pages) * t.TargetRTTMillis, nil
}
func transportHex(s string, size int) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == size && hex.EncodeToString(b) == s && !bytes.Equal(b, make([]byte, size))
}
func (p graphTransportPolicy) validate() error {
	s := p.Spec
	if p.Version != 2 || s.Version != 2 || s.Family != graphFeedWorkloadFamily || s.Facts < 20 || s.Facts > 1_000_000 || s.Facts%20 != 0 || s.Writers != 1 && s.Writers != 8 && s.Writers != 32 || s.Facts < uint64(s.Writers)+1 || s.CrossPercent != 0 && s.CrossPercent != 10 && s.CrossPercent != 50 && s.CrossPercent != 100 || s.Layout != "kv" && s.Layout != "columns" || p.Run == [16]byte{} {
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
	if os.Getenv("RHO_GRAPH_WORKLOAD_TRANSPORT") != graphTransportFormat {
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
	if err != nil || p.validate() != nil {
		return nil, errors.Join(errInvalid, err)
	}
	if os.Getenv("RHO_GRAPH_WORKLOAD_SECONDS") != strconv.FormatUint(p.BudgetSeconds, 10) || os.Getenv("RHO_GRAPH_WORKLOAD_DEADLINE") != strconv.FormatInt(p.DeadlineUnixNanos, 10) || os.Getenv("RHO_GRAPH_WORKLOAD_RUN") != hex.EncodeToString(p.Run[:]) || time.Now().UnixNano() < p.AdmittedUnixNanos || time.Now().UnixNano() >= p.DeadlineUnixNanos {
		return nil, context.DeadlineExceeded
	}
	return &p, nil
}

func (h *harness) graphTransportInstall(ctx context.Context, p graphTransportPolicy) (graphCostStoredRecord, error) {
	if ctx == nil || h == nil || p.validate() != nil || h.config != graphFeedWorkloadConfig(p.Spec, hex.EncodeToString(p.Run[:])) {
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
