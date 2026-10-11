package graphapply

import (
	"bytes"
	"errors"
	"testing"
)

// RED: the recovery admission door must bound a delivered PGQ2 frame before
// slicing its checksum. It is a verifier input, never an admitted proposal.
func TestCrossRecoveryTruncatedPGQ2MustNotPanic(t *testing.T) {
	n := newCrossPartitionNetwork(t)
	h := n.hosts[0][0]
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	_, err = h.machine.VerifyApplicationRecovery(t.Context(), []byte{'P', 'G', 'Q', 2}, view)
	if err == nil {
		t.Fatal("truncated PGQ2 admitted")
	}
}

func TestPartitionGraphVersionsShareExactOwnedAdmissionAndRejectBadChecksums(t *testing.T) {
	l := defaultMaterializerLimits()
	r := partitionGraphCommand{configuration: [32]byte{1}, declaration: [32]byte{2}, routing: [32]byte{3}, request: graphCodecRequest(t)}
	old, err := encodePartitionGraphCommand(r, l)
	if err != nil {
		t.Fatal(err)
	}
	r.version = 1
	explicit, err := encodePartitionGraphCommand(r, l)
	if err != nil || !bytes.Equal(old, explicit) {
		t.Fatal("old frame bytes changed", err)
	}
	owned := 0
	for _, version := range []byte{1, 2} {
		r.version = version
		wire, err := encodePartitionGraphCommand(r, l)
		if err != nil {
			t.Fatal(err)
		}
		admitted := 0
		decoded, err := decodePartitionGraphCommandOwned(wire, l, &admitted)
		if err != nil || decoded.request.id != r.request.id {
			t.Fatal(decoded, err)
		}
		if owned == 0 {
			owned = admitted
		} else if admitted != owned {
			t.Fatal("frame gained uncharged simultaneous backing", owned, admitted)
		}
		tight := l
		tight.commandOwnedBytes = admitted
		if _, err := decodePartitionGraphCommand(wire, tight); err != nil {
			t.Fatal("exact owned cap", version, admitted, err)
		}
		tight.commandOwnedBytes--
		if _, err := decodePartitionGraphCommand(wire, tight); !errors.Is(err, errLimit) {
			t.Fatal("one-short owned cap", version, err)
		}
		bad := bytes.Clone(wire)
		bad[len(bad)-1] ^= 1
		if _, err := decodePartitionGraphCommand(bad, l); !errors.Is(err, errCorrupt) {
			t.Fatal("bad checksum admitted", version, err)
		}
		for _, n := range []int{4, 5, 31, 32, len(wire) - 1} {
			if _, err := decodePartitionGraphCommand(wire[:n], l); !errors.Is(err, errCorrupt) {
				t.Fatal("truncated frame", version, n, err)
			}
		}
	}
}
