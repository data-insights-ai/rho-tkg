package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
	"time"
)

// Authored source guards only. No fixture here certifies a Linux qdisc or RTT.
func TestGraphTransportImmutableFiniteAdmission(t *testing.T) {
	spec := graphFeedWorkloadSpec{2, graphFeedWorkloadFamily, 1_000_000, 50, 1, "kv"}
	p := graphTransportPolicy{Version: 2, Spec: spec, Run: [16]byte{1}, Transport: graphTransport{"injected-rtt", 100, 0, 20261009}, BudgetSeconds: 31536000, AdmittedUnixNanos: 1, QueuePackets: 1024, SampleMillis: 60000, MaxSamples: 525602, MaxArtifactBytes: graphTransportMaxArtifactBytes, ProbeSamples: 9, ProbePayloadBytes: 64, ProbeTimeoutMillis: 3000, Tools: graphTransportTools{Version: "iproute2-6.12.0", TC: [32]byte{1}, IP: [32]byte{2}, Probe: [32]byte{3}, Build: [32]byte{5}}, ArmedSHA256: [32]byte{4}}
	source, _ := hex.DecodeString("bbd141ef7b5d0127cc2152843ba61f274dc32814fa3e0f13e7d07a080bef53d9")
	copy(p.Tools.Source[:], source)
	p.DeadlineUnixNanos = p.AdmittedUnixNanos + int64(p.BudgetSeconds)*int64(time.Second)
	p.CausalMinimumMillis, _ = graphTransportCausalMinimum(p.Spec, p.Transport)
	for i := range 5 {
		role := fmt.Sprintf("zone%d", i)
		if i == 4 {
			role = "client"
		}
		p.Peers[i] = graphTransportPeer{Role: role, Container: fmt.Sprintf("%064x", i+1), Image: "sha256:" + fmt.Sprintf("%064x", i+20), IP: fmt.Sprintf("10.0.0.%d", i+10), NetNS: fmt.Sprintf("net:[%d]", i+100), Network: fmt.Sprintf("%064x", 99), PID: uint64(i + 1)} // #nosec G115 -- Fixed i in[0,4].
	}
	if p.CausalMinimumMillis != 212500300 || p.validate() != nil {
		t.Fatal("actual finite arithmetic", p.CausalMinimumMillis, p.validate())
	}
	digest, err := p.digest()
	if err != nil || digest == [32]byte{} {
		t.Fatal(err)
	}
	for _, mutate := range []func(*graphTransportPolicy){
		func(p *graphTransportPolicy) { p.DeadlineUnixNanos++ },
		func(p *graphTransportPolicy) { p.BudgetSeconds++ },
		func(p *graphTransportPolicy) {
			p.BudgetSeconds = 1800
			p.DeadlineUnixNanos = p.AdmittedUnixNanos + 1800*int64(time.Second)
		},
		func(p *graphTransportPolicy) { p.MaxSamples = 1 },
		func(p *graphTransportPolicy) { p.MaxArtifactBytes = 1 << 20 },
		func(p *graphTransportPolicy) { p.QueuePackets = 0 },
		func(p *graphTransportPolicy) { p.Peers[1].IP = p.Peers[0].IP },
		func(p *graphTransportPolicy) { p.Tools.Version = "latest" },
	} {
		bad := p
		mutate(&bad)
		if !errors.Is(bad.validate(), errInvalid) {
			t.Fatal("foreign/reset/unfunded policy accepted", bad)
		}
	}
}
