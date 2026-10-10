package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

func TestRegisteredGenesisOriginIndependentFramesAndBudgets(t *testing.T) {
	command := registeredStageHex(t, "474a5131000100000000f44a47533100010900000000000000000000000000000000000000000000010a00000000000000000000000000000001000000000000000100000000000000010000008c27ebac460514d5ca92d142aacb7292d997eeacd0c2a6132e559d72e5334454db000000000000000103000000000000000100000000000000020000000000000003358026a048d72e4de703da156369117d06cfa1877da82291f3d23416afd9626d0000000000000002000000000000040000000000001000000000000000010000000000000000100000000000002000000000000000001000000000000040000000000000001000000000000000010000")
	keyWant := registeredStageHex(t, "484b5331010900000000000000000000000000000000000000000000010a0000000000000000000000000000000001")
	valueWant := registeredStageHex(t, "48435231000101000900000000000000000000000000000000000000000000010a000000000000000000000000000000f14f6bcb17676555f19f3bfdd1671491418a4d121eec6146bb29e82d3c34416500010000000000000003000000fc4a47533100010900000000000000000000000000000000000000000000010a00000000000000000000000000000001000000000000000100000000000000010000008c27ebac460514d5ca92d142aacb7292d997eeacd0c2a6132e559d72e5334454db000000000000000103000000000000000100000000000000020000000000000003358026a048d72e4de703da156369117d06cfa1877da82291f3d23416afd9626d00000000000000020000000000000400000000000010000000000000000100000000000000001000000000000020000000000000000010000000000000400000000000000010000000000000000100000000000000000002d32809eaf3c72a77596598b6e599d2241c16c1563be01fdac2d4c7d63bc5d782")
	outcomeWant := registeredStageHex(t, "4a4f52310001040000000000000003000001484b5331010900000000000000000000000000000000000000000000010a000000000000000000000000000000000100000000000000030000000000000002f14f6bcb17676555f19f3bfdd1671491418a4d121eec6146bb29e82d3c344165a75627067ad614feb7ffc367af2c6267f901e8a21d1357dce28cf5b8f736c042")
	source, err := decodeGenesisCommand(command, registeredJournalBudget{3314})
	if err != nil {
		t.Fatal(err)
	}
	for _, amount := range []uint64{4085, 4086, 4087} {
		key, value, err := encodeRegisteredOrigin(source, 3, 2, registeredJournalBudget{amount})
		if amount == 4085 {
			if !errors.Is(err, errLimit) || key != nil || value != nil {
				t.Fatal("one-short Origin must be zero", err)
			}
			continue
		}
		if err != nil || !bytes.Equal(key, keyWant) || !bytes.Equal(value, valueWant) {
			t.Fatal("independent HKS/HCR", err)
		}
		key[0] = 0
		value[0] = 0
		if !bytes.Equal(command, registeredStageHex(t, "474a5131000100000000f44a47533100010900000000000000000000000000000000000000000000010a00000000000000000000000000000001000000000000000100000000000000010000008c27ebac460514d5ca92d142aacb7292d997eeacd0c2a6132e559d72e5334454db000000000000000103000000000000000100000000000000020000000000000003358026a048d72e4de703da156369117d06cfa1877da82291f3d23416afd9626d0000000000000002000000000000040000000000001000000000000000010000000000000000100000000000002000000000000000001000000000000040000000000000001000000000000000010000")) {
			t.Fatal("aliased command")
		}
	}
	for _, amount := range []uint64{6938, 6939, 6940} {
		origin, err := decodeRegisteredOrigin(valueWant, registeredJournalBudget{amount})
		if amount == 6938 {
			if !errors.Is(err, errLimit) || origin != (registeredOrigin{}) {
				t.Fatal("one-short decode must be zero", err)
			}
			continue
		}
		if err != nil || origin.source != source || origin.index != 3 || origin.term != 2 {
			t.Fatal("full Origin tuple", err)
		}
	}
	for _, amount := range []uint64{3339, 3340, 3341} {
		wire, err := encodeRegisteredGenesisOutcome(source, 3, 2, 3, 4, registeredJournalBudget{amount})
		if amount == 3339 {
			if !errors.Is(err, errLimit) || wire != nil {
				t.Fatal("one-short JOR must be zero", err)
			}
			continue
		}
		if err != nil || !bytes.Equal(wire, outcomeWant) {
			t.Fatal("independent Created JOR", err)
		}
	}
	for _, at := range []int{0, 4, 6, 7, 8, 48, 82, 90, 94, 345, 377} {
		bad := bytes.Clone(valueWant)
		bad[at] ^= 1
		origin, err := decodeRegisteredOrigin(bad, registeredJournalBudget{6939})
		if !errors.Is(err, errCorrupt) || origin != (registeredOrigin{}) {
			t.Fatal("bad immutable frame", at, err)
		}
	}
	for _, bad := range [][]byte{nil, valueWant[:377], append(bytes.Clone(valueWant), 0)} {
		origin, err := decodeRegisteredOrigin(bad, registeredJournalBudget{6939})
		if !errors.Is(err, errCorrupt) || origin != (registeredOrigin{}) {
			t.Fatal("bad length", err)
		}
	}
	for _, amount := range []uint64{0, (32 << 20) + 1} {
		key, value, err := encodeRegisteredOrigin(source, 3, 2, registeredJournalBudget{amount})
		if !errors.Is(err, errInvalid) || key != nil || value != nil {
			t.Fatal("invalid budget", err)
		}
	}
	for _, pair := range [][2]uint64{{0, 2}, {3, 0}, {1, 2}, {2, 2}} {
		key, value, err := encodeRegisteredOrigin(source, pair[0], pair[1], registeredJournalBudget{4086})
		if !errors.Is(err, errInvalid) || key != nil || value != nil {
			t.Fatal("invalid original position/term", pair, err)
		}
	}
	for _, status := range []byte{0, 3, 6, 255} {
		wire, err := encodeRegisteredGenesisOutcome(source, 3, 2, 3, status, registeredJournalBudget{3340})
		if !errors.Is(err, errInvalid) || wire != nil {
			t.Fatal("invalid Genesis status", status, err)
		}
	}
}

// Supplemental guards, frozen after the overall draft but before any guard refinement.
// The initial actual Driver RED remains the sole missing-behavior evidence.
func TestRegisteredGenesisOriginChecksumValidSemanticRefusals(t *testing.T) {
	value := registeredStageHex(t, "48435231000101000900000000000000000000000000000000000000000000010a000000000000000000000000000000f14f6bcb17676555f19f3bfdd1671491418a4d121eec6146bb29e82d3c34416500010000000000000003000000fc4a47533100010900000000000000000000000000000000000000000000010a00000000000000000000000000000001000000000000000100000000000000010000008c27ebac460514d5ca92d142aacb7292d997eeacd0c2a6132e559d72e5334454db000000000000000103000000000000000100000000000000020000000000000003358026a048d72e4de703da156369117d06cfa1877da82291f3d23416afd9626d00000000000000020000000000000400000000000010000000000000000100000000000000001000000000000020000000000000000010000000000000400000000000000010000000000000000100000000000000000002d32809eaf3c72a77596598b6e599d2241c16c1563be01fdac2d4c7d63bc5d782")
	for _, change := range []struct {
		name  string
		at    int
		value byte
		want  error
	}{
		{"family", 6, 2, errCorrupt}, {"flags", 7, 1, errCorrupt}, {"protocol", 81, 2, errCorrupt},
		{"too-early original", 89, 2, errCorrupt}, {"lineage", 48, 0, errCorrupt},
		{"outer graph differs from source", 8, 10, errCorrupt},
		{"source graph differs from outer", 100, 10, errCorrupt},
		{"zero owner", 200, 0, errInvalid}, {"source term not Initialize", 156, 2, errInvalid},
		{"Genesis term not later than Initialize", 345, 1, errCorrupt},
	} {
		t.Run(change.name, func(t *testing.T) {
			bad := bytes.Clone(value)
			bad[change.at] = change.value
			sum := sha256.Sum256(bad[:346])
			copy(bad[346:], sum[:])
			origin, err := decodeRegisteredOrigin(bad, registeredJournalBudget{6939})
			if !errors.Is(err, change.want) || origin != (registeredOrigin{}) {
				t.Fatal("checksum-valid contradiction must be zero", err)
			}
		})
	}
}

func TestRegisteredGenesisNilCallbackContracts(t *testing.T) {
	var m *registeredMaterializer
	if m.SemanticContractID() != (raftlog.ApplicationSemanticContractID{}) {
		t.Fatal("nil meaning must be zero")
	}
	if err := m.Restore(1, nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil Restore", err)
	}
	batch, err := m.Stage(replica.Entry{}, raftlog.ApplicationBudget{})
	if !errors.Is(err, errInvalid) || !reflect.DeepEqual(batch, raftlog.ApplicationBatch{}) {
		t.Fatal("nil Stage must be zero", err)
	}
	machine, err := newRegisteredMaterializer(nil, registeredOperationLimits{8 << 20, 8 << 20})
	if !errors.Is(err, errInvalid) || machine != nil {
		t.Fatal("nil Store constructor must be zero", err)
	}
}
