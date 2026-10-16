package replica

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func TestApplicationPacketScopeCodecBoundsAndOwnership(t *testing.T) {
	binding := raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 7, Group: [16]byte{3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{5}}
	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "regular", true: "snapshot"}[snapshot], func(t *testing.T) {
			p := Packet{From: 1, To: 2, Payload: []byte("opaque consensus"), Snapshot: snapshot}
			need := applicationPacketHeaderBytes + len(p.Payload)
			if out, err := EncodeApplicationPacket(binding, p, need-1); !errors.Is(err, ErrLimit) || !reflect.DeepEqual(out, Packet{}) {
				t.Fatal(out, err)
			}
			encoded, err := EncodeApplicationPacket(binding, p, need)
			if err != nil || len(encoded.Payload) != need || cap(encoded.Payload) != need {
				t.Fatal(encoded, err)
			}
			decoded, err := DecodeApplicationPacket(binding, encoded, need)
			if err != nil || !reflect.DeepEqual(decoded, p) || cap(decoded.Payload) != len(p.Payload) {
				t.Fatal(decoded, err)
			}
			encoded.Payload[len(encoded.Payload)-1] ^= 1
			if bytes.Equal(decoded.Payload, encoded.Payload[applicationPacketHeaderBytes:]) {
				t.Fatal("decode aliases wire")
			}
			for _, change := range []func(*raftlog.ApplicationBinding){func(b *raftlog.ApplicationBinding) { b.Identity.Graph[0]++ }, func(b *raftlog.ApplicationBinding) { b.Identity.Partition++ }, func(b *raftlog.ApplicationBinding) { b.Identity.Group[0]++ }, func(b *raftlog.ApplicationBinding) { b.SemanticContractID[0]++ }} {
				other := binding
				change(&other)
				if out, err := DecodeApplicationPacket(other, encoded, need); !errors.Is(err, ErrInvalid) || !reflect.DeepEqual(out, Packet{}) {
					t.Fatal("cross-bound packet accepted", out, err)
				}
			}
			if _, err := DecodeApplicationPacket(binding, encoded, need-1); !errors.Is(err, ErrLimit) {
				t.Fatal(err)
			}
			for n := 0; n < applicationPacketHeaderBytes; n++ {
				bad := encoded
				bad.Payload = bad.Payload[:n]
				if _, err := DecodeApplicationPacket(binding, bad, need); !errors.Is(err, ErrInvalid) {
					t.Fatal(n, err)
				}
			}
			for _, at := range []int{0, 2, 76, 79} {
				bad := encoded
				bad.Payload = bytes.Clone(encoded.Payload)
				bad.Payload[at] ^= 1
				if _, err := DecodeApplicationPacket(binding, bad, need); !errors.Is(err, ErrInvalid) {
					t.Fatal(at, err)
				}
			}
		})
	}
	for _, limit := range []int{-1, 0, applicationPacketHeaderBytes - 1, applicationPacketMaxBytes + 1} {
		if _, err := EncodeApplicationPacket(binding, Packet{}, limit); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if _, err := DecodeApplicationPacket(binding, Packet{}, limit); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := EncodeApplicationPacket(raftlog.ApplicationBinding{}, Packet{}, 100); !errors.Is(err, ErrInvalid) || !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := DecodeApplicationPacket(raftlog.ApplicationBinding{}, Packet{}, 100); !errors.Is(err, ErrInvalid) || !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal(err)
	}
}

type semanticFenceMachine struct{ restored bool }

func (m *semanticFenceMachine) Restore(uint64, []byte) error { m.restored = true; return nil }
func (*semanticFenceMachine) Stage(Entry, raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	return raftlog.ApplicationBatch{}, raftlog.ErrInvalid
}
func TestApplicationPacketCodecDoesNotEnableBoundDriver(t *testing.T) {
	p := raftlog.DefaultApplicationPolicy(1)
	tc := raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 7, Group: [16]byte{3}}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()}
	s, err := raftlog.Open(raftlog.Config{Dir: "db", FS: vfs.NewMem(), Create: true, Application: p, Transfer: tc, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2000000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 1000000}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{1}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Initialize([]uint64{1, 2, 3}, nil); err != nil {
		t.Fatal(err)
	}
	machine := &semanticFenceMachine{}
	if _, err := Open(Config{ID: 1, Store: s, ApplicationMachine: machine}); !errors.Is(err, ErrInvalid) || machine.restored {
		t.Fatal("semantic codec enabled traffic", err, machine.restored)
	}
}
