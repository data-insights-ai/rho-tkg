package graphapply

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"
)

// Authored direct guards, not an executed transport/workload result. The same
// full process parent supplies actual lifecycle/original/column evidence.
func TestNativeGraphEdgeUniqueOwnedTransportAdmissionPreservesLocalBytes(t *testing.T) {
	spec := nativeGraphCostSpec{Version: 4, Facts: 20, CrossPercent: 100, Writers: 8}
	legacy := nativeGraphEdgeUniqueRunBinding{Version: 1, Family: nativeGraphEdgeUniqueFamily, Run: "01010101010101010101010101010101", Spec: spec, Transport: "local", BudgetSeconds: 1800, DeadlineUnixNanos: 1}
	wire, err := json.Marshal(legacy)
	const original = `{"Version":1,"Family":"cross-edge-endpoint-uniqueness","Run":"01010101010101010101010101010101","Spec":{"version":4,"facts":20,"crossPercent":100,"writers":8},"Transport":"local","BudgetSeconds":1800,"CausalMinimumMillis":0,"DeadlineUnixNanos":1}`
	if err != nil || string(wire) != original {
		t.Fatal("legacy local admission bytes changed", err, string(wire))
	}
	t.Setenv("RHO_GRAPH_WORKLOAD_RUN", legacy.Run)
	t.Setenv("RHO_GRAPH_WORKLOAD_SECONDS", "1800")
	t.Setenv("RHO_GRAPH_WORKLOAD_DEADLINE", strconv.FormatInt(time.Now().Add(1799*time.Second).UnixNano(), 10))
	t.Setenv("RHO_GRAPH_WORKLOAD_TRANSPORT", "local")
	t.Setenv(nativeGraphEdgeUniqueTransportEnv, "")
	local, hash, err := nativeGraphEdgeUniqueAdmission(spec)
	if err != nil || local.Version != 1 || local.Transport != "local" || local.OwnedTransport != nil || hash == ([32]byte{}) {
		t.Fatal("legacy local source profile", err)
	}
	t.Setenv(nativeGraphEdgeUniqueTransportEnv, nativeGraphEdgeUniqueTransportFormat)
	for _, rtt := range []uint64{0, 1, 20, 100} {
		transport := nativeWorkloadTransportSpec{Kind: "injected-rtt", TargetRTTMillis: rtt, Seed: 20261009}
		if rtt == 0 {
			transport.Kind = "local"
		}
		wire, err := json.Marshal(transport)
		if err != nil {
			t.Fatal(err)
		}
		t.Setenv("RHO_GRAPH_WORKLOAD_TRANSPORT", string(wire))
		got, digest, err := nativeGraphEdgeUniqueAdmission(spec)
		if err != nil || got.Version != 2 || got.OwnedTransport == nil || *got.OwnedTransport != transport || got.CausalMinimumMillis != 27*rtt || got.BinarySHA256 == "" || got.Graph == ([16]byte{}) || got.Topology != 9 || got.Declaration == ([32]byte{}) || digest == hash {
			t.Fatal("actual immutable transport admission", got, err)
		}
		again, same, err := nativeGraphEdgeUniqueAdmission(spec)
		if err != nil || same != digest || !bytes.Equal([]byte(again.BinarySHA256), []byte(got.BinarySHA256)) {
			t.Fatal("same original admission changed across reopen", err)
		}
	}
	for _, bad := range []string{"local", `{"kind":"injected-rtt","targetRTTMillis":2,"jitterMillis":0,"seed":20261009}`, `{"kind":"local","targetRTTMillis":0,"jitterMillis":1,"seed":20261009}`} {
		t.Setenv("RHO_GRAPH_WORKLOAD_TRANSPORT", bad)
		if _, _, err := nativeGraphEdgeUniqueAdmission(spec); !errors.Is(err, errInvalid) {
			t.Fatal("malformed profile accepted", err)
		}
	}
	t.Setenv("RHO_GRAPH_WORKLOAD_TRANSPORT", `{"kind":"injected-rtt","targetRTTMillis":100,"jitterMillis":0,"seed":20261009}`)
	t.Setenv("RHO_GRAPH_WORKLOAD_SECONDS", "2")
	t.Setenv("RHO_GRAPH_WORKLOAD_DEADLINE", strconv.FormatInt(time.Now().Add(time.Second).UnixNano(), 10))
	if _, _, err := nativeGraphEdgeUniqueAdmission(spec); !errors.Is(err, errLimit) {
		t.Fatal("impossible necessary causal floor admitted", err)
	}
	t.Setenv(nativeGraphEdgeUniqueTransportEnv, "caller-label")
	if _, err := nativeGraphEdgeUniqueTransportEnabled(); !errors.Is(err, errInvalid) {
		t.Fatal("unknown transport profile", err)
	}
}
