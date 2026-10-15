package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
)

const nativeGraphEdgeUniqueTransportFormat = "native-edge-endpoint-unique-transport-v2"
const nativeGraphEdgeUniqueTransportEnv = "RHO_GRAPH_EDGE_TRANSPORT_PROFILE"

func nativeGraphEdgeUniqueTransportEnabled() (bool, error) {
	switch os.Getenv(nativeGraphEdgeUniqueTransportEnv) {
	case "":
		return false, nil
	case nativeGraphEdgeUniqueTransportFormat:
		return true, nil
	default:
		return false, errInvalid
	}
}

// Same fixed declared fixture as partitionCodecRequest. This describes the
// admitted graph, not source authority. Profile admission checks the ACTUAL
// constructor declaration, and the oracle binds the acquired native cut scopes.
func nativeGraphEdgeUniqueTransportDeclaration() (graphstore.OwnershipDeclaration, error) {
	n, _ := certifiedProcessNamespace(1)
	return graphstore.NewOwnershipDeclaration(graphstate.GraphID(n.graph), 9, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{4}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{8}}}, graphstore.DefaultLimits())
}

func nativeGraphEdgeUniqueCausalMinimum(spec nativeGraphCostSpec, transport nativeWorkloadTransportSpec) (uint64, error) {
	if nativeGraphEdgeUniqueValidate(spec) != nil || transport.validate() != nil {
		return 0, errInvalid
	}
	// FreezeOld walks N facts SEQUENTIALLY. For each, FreezePlan opens every
	// authenticated participant through an actual source request/reply BEFORE
	// the next fact. Three setup and four endpoint lifecycle Runs are likewise
	// sequential and each needs genuine remote authority. Each chain member
	// therefore waits for >=one full shaped source RTT. This necessary floor
	// deliberately omits all other Raft, retries, reads and column work.
	return (spec.Facts + 7) * transport.TargetRTTMillis, nil // <=100000700ms
}

// The running binary is read once per actual OS process. No caller-supplied
// digest substitutes for it. The controller independently hashes its executable.
var nativeGraphEdgeUniqueExecutable = sync.OnceValues(func() (string, error) {
	name, err := os.Executable()
	if err != nil {
		return "", err
	}
	f, err := os.Open(name)
	if err != nil {
		return "", err
	}
	info, err := f.Stat()
	if err != nil || info.Size() < 1 || info.Size() > 1<<30 || !info.Mode().IsRegular() {
		return "", errors.Join(errInvalid, err, f.Close())
	}
	hash := sha256.New()
	n, readErr := io.Copy(hash, io.LimitReader(f, info.Size()+1))
	if err := errors.Join(readErr, f.Close()); err != nil {
		return "", err
	}
	if n != info.Size() {
		return "", errInvalid
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
})

func nativeGraphEdgeUniqueOwnedAdmission(binding nativeGraphEdgeUniqueRunBinding) (nativeGraphEdgeUniqueRunBinding, error) {
	wire := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_TRANSPORT"))
	var transport nativeWorkloadTransportSpec
	if len(wire) > 1024 || json.Unmarshal(wire, &transport) != nil || transport.validate() != nil || transport.Seed != 20261009 {
		return binding, errInvalid
	}
	canonical, err := json.Marshal(transport)
	if err != nil || !bytes.Equal(canonical, wire) {
		return binding, errors.Join(errInvalid, err)
	}
	declaration, err := nativeGraphEdgeUniqueTransportDeclaration()
	if err != nil {
		return binding, err
	}
	binding.Version, binding.Transport = 2, nativeGraphEdgeUniqueTransportFormat
	binding.OwnedTransport = new(transport)
	binding.Graph, binding.Topology, binding.Declaration = [16]byte(declaration.Graph()), declaration.TopologyEpoch(), declaration.Digest()
	binding.CausalMinimumMillis, err = nativeGraphEdgeUniqueCausalMinimum(binding.Spec, transport)
	if err != nil || binding.BudgetSeconds*1000 <= binding.CausalMinimumMillis {
		return binding, errors.Join(errLimit, err)
	}
	binding.BinarySHA256, err = nativeGraphEdgeUniqueExecutable()
	return binding, err
}

// Install the EXISTING scheduler before mux goroutines/campaigns. Both cohorts
// get the same immutable spec/deadline and independent actual counter ledgers;
// the existing128-frame/8MiB hub owns payloads and cancellation throughout.
func attachNativeGraphEdgeUniqueNetwork(t *testing.T, n *certifiedProcessNetwork, spec nativeGraphCostSpec, deadline time.Time) {
	t.Helper()
	enabled, err := nativeGraphEdgeUniqueTransportEnabled()
	if err != nil {
		t.Fatal(err)
	}
	if !enabled {
		attachNativeColumnProcessNetwork(t, n, spec)
		return
	}
	binding, _, err := nativeGraphEdgeUniqueAdmission(spec)
	if err != nil || binding.Version != 2 || binding.OwnedTransport == nil || binding.DeadlineUnixNanos != deadline.UnixNano() {
		t.Fatal("immutable actual native transport admission", err)
	}
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	t.Cleanup(cancel)
	frames, err := nativeGraphEdgeUniqueFrames(spec)
	if err != nil || frames > (1<<41)/4096 || frames > uint64(^uint(0)>>1)/4096 {
		t.Fatal("finite native transport work", err)
	}
	transport, err := newNativeWorkloadTransport(ctx, *binding.OwnedTransport, frames*4096)
	if err != nil {
		t.Fatal(err)
	}
	attachGraphProcessMuxesWithWorkload(t, n.all, int(frames*4096), transport) // #nosec G115 -- exact checked native int bound above.
	n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: n.all[1], 2: n.all[2], 3: n.all[3]}}
	n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: n.all[4], 5: n.all[5], 6: n.all[6]}}
}
