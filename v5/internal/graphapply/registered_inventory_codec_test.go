package graphapply

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"unsafe"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// External complete bytes are inert data; no durable cohort or live factory runs.
const inventoryOldPIJ = "50494a3100010001000000000000000000000000000000000000000000000102" +
	"000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc" +
	"852871231394fc9338d89693abf60f0001010300000000000000000000000000" +
	"000001358026a048d72e4de703da156369117d06cfa1877da82291f3d23416af" +
	"d9626d0000007a424453310001000000004c4447503100010000010a00000000" +
	"00000000000000000000000100010000000c63616d6572612d636c6f636b0000" +
	"000b6d696c6c697365636f6e640a000000000000000000000000000000000000" +
	"0104000000000000000000000000000000000000000000000100000000000000" +
	"4000"

const inventoryOldSIJ = "53494a3100010001000000000000000000000000000000000000000000000102" +
	"000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc" +
	"852871231394fc9338d89693abf60f0001010300000000000000000000000000" +
	"000001358026a048d72e4de703da156369117d06cfa1877da82291f3d23416af" +
	"d9626d0000007a424453310001000000004c4447503100010000010a00000000" +
	"00000000000000000000000100010000000c63616d6572612d636c6f636b0000" +
	"000b6d696c6c697365636f6e640a000000000000000000000000000000000000" +
	"0104000000000000000000000000000000000000000000000100000000000000" +
	"4000"

const inventoryOldGJQ = "474a5131000100000000f44a4753310001010000000000000000000000000000" +
	"0000000000000000010200000000000000000000000000000001000000000000" +
	"000100000000000000010000008c12cf5d257654d0fa3779b2b4026d838440ec" +
	"14e65c1ad4abda97080ecc01f86a000000000000000103000000000000000100" +
	"000000000000020000000000000003358026a048d72e4de703da156369117d06" +
	"cfa1877da82291f3d23416afd9626d0000000000000002000000000000040000" +
	"0000000010000000000000000100000000000000001000000000000020000000" +
	"00000000001000000000000040000000000000001000000000000000010000"

const inventoryNewPIJ = "50494a3100010001000000000000000000000000000000000000000000000102" +
	"000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc" +
	"852871231394fc9338d89693abf60f0001010300000000000000000000000000" +
	"0000011587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60" +
	"da51c70000007a424453310001000000004c4447503100010000010a00000000" +
	"00000000000000000000000100010000000c63616d6572612d636c6f636b0000" +
	"000b6d696c6c697365636f6e640a000000000000000000000000000000000000" +
	"0104000000000000000000000000000000000000000000000100000000000000" +
	"4000"

const inventoryNewSH = "26a28dedef197296c095d19f0334e2d51406b22efc1a4eed50cd6d38b13b53ca"

const inventoryNewPH = "47dbf9fa532d24438f384b551cf473352be09e20531df0d23f56f076dc3411f2"

func TestRegisteredInventoryCodecFirstContract(t *testing.T) {
	origin, key, spec, budget := registeredCodecFixture(t)
	oldPIJ := registeredFixtureHex(t, inventoryOldPIJ)
	oldSIJ := registeredFixtureHex(t, inventoryOldSIJ)
	oldGJQ := registeredFixtureHex(t, inventoryOldGJQ)
	encoded, err := encodePreparedBootstrap(origin, key, spec, budget)
	if err != nil || !bytes.Equal(encoded, oldPIJ) || cap(encoded) != len(oldPIJ) {
		t.Fatalf("OLD full PIJ contract: %v", err)
	}
	projected, err := submissionProjectionV1(oldPIJ, budget)
	if err != nil || !bytes.Equal(projected, oldSIJ) || cap(projected) != len(oldSIJ) {
		t.Fatalf("OLD full SIJ contract: %v", err)
	}
	source, err := decodeGenesisCommand(oldGJQ, registeredJournalBudget{32 << 20})
	if err != nil || source != journalGoldenSource {
		t.Fatalf("OLD full GJQ source: %v", err)
	}
	command, err := encodeGenesisCommand(source, registeredJournalBudget{32 << 20})
	if err != nil || !bytes.Equal(command, oldGJQ) || cap(command) != len(oldGJQ) {
		t.Fatalf("OLD full GJQ command: %v", err)
	}
	t.Log("OLD full PIJ/SIJ/GJQ baseline PASS before valid NEW call")
	newPIJ := registeredFixtureHex(t, inventoryNewPIJ)
	encoded, err = encodeInventoryPreparedBootstrap(origin, key, spec, budget)
	if err != nil {
		if !errors.Is(err, errRegisteredInventoryCodecUnavailable) || encoded != nil {
			t.Fatalf("valid NEW encoder unexpected cause/output: %v %x", err, encoded)
		}
		t.Fatalf("valid NEW inventory encoder contract missing: %v", err)
	}
	if !bytes.Equal(encoded, newPIJ) || cap(encoded) != len(newPIJ) {
		t.Fatalf("NEW complete independent PIJ differs: got%x want%x", encoded, newPIJ)
	}
	sh, ph, err := hashInventoryPreparedV1(newPIJ, budget)
	if err != nil || !bytes.Equal(sh[:], registeredFixtureHex(t, inventoryNewSH)) || !bytes.Equal(ph[:], registeredFixtureHex(t, inventoryNewPH)) {
		t.Fatalf("NEW independent full SH/PH contract: %v sh%x ph%x", err, sh, ph)
	}
}

// Retained only for the original frozen RED error classification; no production
// unavailable branch remains after implementation.
var errRegisteredInventoryCodecUnavailable = errors.New("registered inventory codec unavailable")

func TestRegisteredInventoryCompiledAgreementAndMetadata(t *testing.T) {
	old, current := registeredSemanticContractID(), registeredInventorySemanticContractID()
	if !bytes.Equal(old[:], registeredFixtureHex(t, "358026a048d72e4de703da156369117d06cfa1877da82291f3d23416afd9626d")) ||
		!bytes.Equal(current[:], registeredFixtureHex(t, "1587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60da51c7")) || len(registeredInventorySemanticDescriptor) != 601 {
		t.Fatal("compiled agreements differ from independent full descriptors")
	}
	// Variable strings/containers are separately charged; this is fixed logical
	// metadata plus source-check allowance, not an allocation/RSS measurement.
	fixed := unsafe.Sizeof(registeredParsed{}) + unsafe.Sizeof(registeredCursor{}) +
		unsafe.Sizeof(registeredLedger{}) + unsafe.Sizeof(registeredBootstrapSpec{}) +
		unsafe.Sizeof(registeredOriginScope{}) + unsafe.Sizeof(registeredFormatBudget{}) +
		2*256 + 8*24 + 2*32 + 96 + 128
	if fixed > registeredMetadataBytes {
		t.Fatalf("fixed metadata and selector comparison allowance%d exceeds2048", fixed)
	}
	t.Logf("fixed typed metadata/hash/slice/selector allowance=%d/2048", fixed)
}

func inventoryVariantRows(t *testing.T) map[string]registeredLiteral {
	t.Helper()
	raw, err := os.ReadFile("testdata/registered_inventory_bootstrap_fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows map[string]registeredLiteral
	if err = json.Unmarshal(raw, &rows); err != nil || len(rows) != 14 {
		t.Fatalf("independent14vectors: %v", err)
	}
	return rows
}

func TestRegisteredInventoryAllPreparedVariants(t *testing.T) {
	oldRows, rows := registeredLiterals(t), inventoryVariantRows(t)
	_, _, _, budget := registeredCodecFixture(t)
	for name, row := range rows {
		t.Run(name, func(t *testing.T) {
			wire := registeredFixtureHex(t, row.Prepared)
			oldWire := registeredFixtureHex(t, oldRows[name].Prepared)
			o, key, spec, err := decodePreparedBootstrap(oldWire, budget)
			if err != nil {
				t.Fatal("OLD validated input", err)
			}
			encoded, err := encodeInventoryPreparedBootstrap(o, key, spec, budget)
			if err != nil || !bytes.Equal(encoded, wire) || cap(encoded) != len(wire) {
				t.Fatalf("independent NEW encoding: %v", err)
			}
			if err = validateInventoryPreparedV1(wire, budget); err != nil {
				t.Fatal("NEW borrowed validation", err)
			}
			decodedO, decodedKey, decoded, err := decodeInventoryPreparedBootstrap(wire, budget)
			if err != nil || decodedO != o || decodedKey != key || !reflect.DeepEqual(decoded, spec) {
				t.Fatalf("full typed NEW decode: %v", err)
			}
			projection, err := inventorySubmissionProjectionV1(wire, budget)
			if err != nil || !bytes.Equal(projection, registeredFixtureHex(t, row.Submission)) || cap(projection) != len(wire) {
				t.Fatalf("independent SIJ: %v", err)
			}
			sh, ph, err := hashInventoryPreparedV1(wire, budget)
			if err != nil || !bytes.Equal(sh[:], registeredFixtureHex(t, row.SubmissionHash)) || !bytes.Equal(ph[:], registeredFixtureHex(t, row.PreparedHash)) {
				t.Fatalf("full independent hashes: %v", err)
			}
			if err = validatePreparedV1(wire, budget); !errors.Is(err, errInvalid) {
				t.Fatal("OLD must refuse NEW", err)
			}
			if err = validateInventoryPreparedV1(oldWire, budget); !errors.Is(err, errInvalid) {
				t.Fatal("NEW must refuse OLD", err)
			}
			clear(wire)
			if !reflect.DeepEqual(decoded, spec) || !bytes.Equal(encoded, registeredFixtureHex(t, row.Prepared)) || !bytes.Equal(projection, registeredFixtureHex(t, row.Submission)) {
				t.Fatal("owned data aliases borrowed wire")
			}
		})
	}
}

func inventoryExpectedRefusal(t *testing.T, wire []byte, budget registeredFormatBudget, expected raftlog.ApplicationSemanticContractID, want error) {
	t.Helper()
	if err := validatePreparedV1Expected(wire, budget, expected); !errors.Is(err, want) {
		t.Fatal("expected validation cause", err)
	}
	o, k, spec, err := decodePreparedBootstrapExpected(wire, budget, expected)
	if !errors.Is(err, want) || o != (registeredOriginScope{}) || k != ([16]byte{}) || !reflect.DeepEqual(spec, registeredBootstrapSpec{}) {
		t.Fatal("expected decode cause/zero", err)
	}
	projected, err := submissionProjectionV1Expected(wire, budget, expected)
	if !errors.Is(err, want) || projected != nil {
		t.Fatal("expected projection cause/zero", err)
	}
	sh, ph, err := hashPreparedV1Expected(wire, budget, expected)
	if !errors.Is(err, want) || sh != ([32]byte{}) || ph != ([32]byte{}) {
		t.Fatal("expected hashes cause/zero", err)
	}
}

func TestRegisteredInventoryExpectedAndFormatRefusals(t *testing.T) {
	o, k, spec, budget := registeredCodecFixture(t)
	wire := registeredFixtureHex(t, inventoryNewPIJ)
	for _, expected := range []raftlog.ApplicationSemanticContractID{{}, {8}, registeredSemanticContractID()} {
		inventoryExpectedRefusal(t, wire, budget, expected, errInvalid)
		if expected != registeredSemanticContractID() {
			out, err := encodePreparedBootstrapExpected(o, k, spec, budget, expected)
			if !errors.Is(err, errInvalid) || out != nil {
				t.Fatal("unknown encoder meaning cause/zero", err)
			}
		}
	}
	current := registeredInventorySemanticContractID()
	inventoryExpectedRefusal(t, registeredFixtureHex(t, inventoryOldPIJ), budget, current, errInvalid)
	for _, pos := range []int{0, 4, 6, 81, 98, 135, 257} {
		bad := bytes.Clone(wire)
		bad[pos] = 255
		inventoryExpectedRefusal(t, bad, budget, current, errInvalid)
	}
	for _, bad := range [][]byte{nil, wire[:135], append(bytes.Clone(wire), 0)} {
		inventoryExpectedRefusal(t, bad, budget, current, errInvalid)
	}
	short := budget
	short.preparedBytes = 257
	inventoryExpectedRefusal(t, wire, short, current, errLimit)
	for _, cap := range []int{0, 32<<20 + 1} {
		bad := budget
		bad.walkBytes = cap
		inventoryExpectedRefusal(t, wire, bad, current, errInvalid)
		out, err := encodeInventoryPreparedBootstrap(o, k, spec, bad)
		if !errors.Is(err, errInvalid) || out != nil {
			t.Fatal("encoder invalid budget", err)
		}
	}
	for _, change := range []func(*registeredBootstrapSpec){func(s *registeredBootstrapSpec) { s.kind = 3 }, func(s *registeredBootstrapSpec) { s.allocator.epoch = 0 }, func(s *registeredBootstrapSpec) { s.domain.axes[0].Reference = " " }} {
		_, _, bad, _ := registeredCodecFixture(t)
		change(&bad)
		out, err := encodeInventoryPreparedBootstrap(o, k, bad, budget)
		if !errors.Is(err, errInvalid) || out != nil {
			t.Fatal("invalid typed NEW input cause/zero", err)
		}
	}
}

func TestRegisteredInventoryExactPreparedWork(t *testing.T) {
	o, k, spec, budget := registeredCodecFixture(t)
	wire := registeredFixtureHex(t, inventoryNewPIJ)
	// Independent base arithmetic: parse2048+258+86+46+66=2504;
	// encode adds indices2 and owned/emitted258 twice to structural2504=3022;
	// decode adds281walk+87owned=2872; projection adds516=3020;
	// hashes add31+4+258+33+4+258=588 ->3092. Selector fits fixed2048.
	for _, tc := range []struct {
		name  string
		exact int
		call  func(registeredFormatBudget) (bool, bool, error)
	}{
		{"encode", 3022, func(b registeredFormatBudget) (bool, bool, error) {
			out, e := encodeInventoryPreparedBootstrap(o, k, spec, b)
			return bytes.Equal(out, wire) && cap(out) == 258, out == nil, e
		}},
		{"validate", 2504, func(b registeredFormatBudget) (bool, bool, error) {
			e := validateInventoryPreparedV1(wire, b)
			return e == nil, true, e
		}},
		{"decode", 2872, func(b registeredFormatBudget) (bool, bool, error) {
			a, z, out, e := decodeInventoryPreparedBootstrap(wire, b)
			return a == o && z == k && reflect.DeepEqual(out, spec), a == (registeredOriginScope{}) && z == ([16]byte{}) && reflect.DeepEqual(out, registeredBootstrapSpec{}), e
		}},
		{"projection", 3020, func(b registeredFormatBudget) (bool, bool, error) {
			out, e := inventorySubmissionProjectionV1(wire, b)
			return bytes.Equal(out, registeredFixtureHex(t, inventoryVariantRows(t)["base"].Submission)), out == nil, e
		}},
		{"hash", 3092, func(b registeredFormatBudget) (bool, bool, error) {
			sh, ph, e := hashInventoryPreparedV1(wire, b)
			return bytes.Equal(sh[:], registeredFixtureHex(t, inventoryNewSH)) && bytes.Equal(ph[:], registeredFixtureHex(t, inventoryNewPH)), sh == ([32]byte{}) && ph == ([32]byte{}), e
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, delta := range []int{-1, 0, 1} {
				b := budget
				b.walkBytes = tc.exact + delta
				valid, zero, err := tc.call(b)
				if delta < 0 {
					if !errors.Is(err, errLimit) || !zero {
						t.Fatal("one-short work must refuse", err)
					}
				} else if err != nil || !valid {
					t.Fatal("exact/+1 work contract", err)
				}
			}
		})
	}
}

const inventoryNewSource = "4a47533100010100000000000000000000000000000000000000000000010200" +
	"0000000000000000000000000000010000000000000001000000000000000100" +
	"00008c12cf5d257654d0fa3779b2b4026d838440ec14e65c1ad4abda97080ecc" +
	"01f86a0000000000000001030000000000000001000000000000000200000000" +
	"000000031587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc" +
	"60da51c700000000000000020000000000000400000000000010000000000000" +
	"0001000000000000000010000000000000200000000000000000100000000000" +
	"0040000000000000001000000000000000010000"

const inventoryNewGJQ = "474a5131000100000000f44a4753310001010000000000000000000000000000" +
	"0000000000000000010200000000000000000000000000000001000000000000" +
	"000100000000000000010000008c12cf5d257654d0fa3779b2b4026d838440ec" +
	"14e65c1ad4abda97080ecc01f86a000000000000000103000000000000000100" +
	"0000000000000200000000000000031587f6a6c97eab3777c2b5cafbf4013e10" +
	"b6be377d08d9b32cf348fc60da51c70000000000000002000000000000040000" +
	"0000000010000000000000000100000000000000001000000000000020000000" +
	"00000000001000000000000040000000000000001000000000000000010000"

const inventoryNewOrigin = "4843523100010100010000000000000000000000000000000000000000000001" +
	"02000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950" +
	"dc852871231394fc9338d89693abf60f00010000000000000003000000fc4a47" +
	"5331000101000000000000000000000000000000000000000000000102000000" +
	"0000000000000000000000000100000000000000010000000000000001000000" +
	"8c12cf5d257654d0fa3779b2b4026d838440ec14e65c1ad4abda97080ecc01f8" +
	"6a00000000000000010300000000000000010000000000000002000000000000" +
	"00031587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60da" +
	"51c7000000000000000200000000000004000000000000100000000000000001" +
	"0000000000000000100000000000002000000000000000001000000000000040" +
	"00000000000000100000000000000001000000000000000000022c87f8e7a241" +
	"c442b2946b845f72c37c412a3b21e4ea09bd8bc66ef76a67ecdb"

const inventoryCreatedJOR = "4a4f52310001040000000000000003000001484b533101010000000000000000" +
	"0000000000000000000000000000010200000000000000000000000000000000" +
	"0100000000000000030000000000000002cbb22d33699f795ed00440b5ee0af9" +
	"50dc852871231394fc9338d89693abf60f85d901e3bfaf8ca663c544619a3969" +
	"ab2f751ca9f691b2bf5c24d3ba8b573bc1"

const inventoryOriginalJOR = "4a4f52310001050000000000000005000001484b533101010000000000000000" +
	"0000000000000000000000000000010200000000000000000000000000000000" +
	"0100000000000000030000000000000002cbb22d33699f795ed00440b5ee0af9" +
	"50dc852871231394fc9338d89693abf60fdd39423c0cff46061678dae49c9b1c" +
	"5c59648aaec857ce649e562951d0b182e5"

func TestRegisteredInventoryJournalAdaptersAndWork(t *testing.T) {
	wire := registeredFixtureHex(t, inventoryNewGJQ)
	source, err := decodeGenesisCommand(wire, registeredJournalBudget{32 << 20})
	if err != nil || source.Meaning != [32]byte(registeredInventorySemanticContractID()) {
		t.Fatal("independent NEW source setup", err)
	}
	for _, tc := range []struct {
		name  string
		exact uint64
		call  func(registeredJournalBudget) (bool, bool, error)
	}{
		{"source", 3324, func(b registeredJournalBudget) (bool, bool, error) {
			out, e := encodeInventoryGenesisSource(source, b)
			return bytes.Equal(out, registeredFixtureHex(t, inventoryNewSource)) && cap(out) == 244, out == nil, e
		}},
		{"command", 3346, func(b registeredJournalBudget) (bool, bool, error) {
			out, e := encodeInventoryGenesisCommand(source, b)
			return bytes.Equal(out, wire) && cap(out) == 255, out == nil, e
		}},
		{"decode-command", 3091, func(b registeredJournalBudget) (bool, bool, error) {
			out, e := decodeInventoryGenesisCommand(wire, b)
			return out == source, out == (registeredGenesisSource{}), e
		}},
		{"origin", 4118, func(b registeredJournalBudget) (bool, bool, error) {
			key, out, e := encodeInventoryRegisteredOrigin(source, 3, 2, b)
			return bytes.Equal(key, registeredFixtureHex(t, "484b533101010000000000000000000000000000000000000000000001020000000000000000000000000000000001")) && bytes.Equal(out, registeredFixtureHex(t, inventoryNewOrigin)) && cap(key) == 47 && cap(out) == 378, key == nil && out == nil, e
		}},
		{"decode-origin", 6971, func(b registeredJournalBudget) (bool, bool, error) {
			out, e := decodeInventoryRegisteredOrigin(registeredFixtureHex(t, inventoryNewOrigin), b)
			return out.source == source && out.index == 3 && out.term == 2, out == (registeredOrigin{}), e
		}},
		{"outcome", 3372, func(b registeredJournalBudget) (bool, bool, error) {
			out, e := encodeInventoryRegisteredGenesisOutcome(source, 3, 2, 3, 4, b)
			return bytes.Equal(out, registeredFixtureHex(t, inventoryCreatedJOR)) && cap(out) == 145, out == nil, e
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, delta := range []int{-1, 0, 1} {
				valid, zero, e := tc.call(registeredJournalBudget{uint64(int(tc.exact) + delta)})
				if delta < 0 {
					if !errors.Is(e, errLimit) || !zero {
						t.Fatal("one-short must retain cause/zero", e)
					}
				} else if e != nil || !valid {
					t.Fatal("exact/+1 independent full output", e)
				}
			}
			for _, cap := range []uint64{0, 31, 32<<20 + 1} {
				want := errInvalid
				if cap == 31 {
					want = errLimit
				}
				_, zero, e := tc.call(registeredJournalBudget{cap})
				if !errors.Is(e, want) || !zero {
					t.Fatal("invalid/short selector allowance", e)
				}
			}
		})
	}
	out, err := encodeInventoryRegisteredGenesisOutcome(source, 3, 2, 5, 5, registeredJournalBudget{3372})
	if err != nil || !bytes.Equal(out, registeredFixtureHex(t, inventoryOriginalJOR)) {
		t.Fatal("OriginalE5 keepsS3/T2", err)
	}
	// Decoder returned fixed value owns its fields; changing the input cannot alias.
	clear(wire)
	if source.Meaning != [32]byte(registeredInventorySemanticContractID()) {
		t.Fatal("source aliases wire")
	}
}

func TestRegisteredInventoryJournalMixedAndMalformed(t *testing.T) {
	budget := registeredJournalBudget{32 << 20}
	old := journalGoldenSource
	for _, call := range []func() (bool, error){
		func() (bool, error) { out, e := encodeInventoryGenesisSource(old, budget); return out == nil, e },
		func() (bool, error) { out, e := encodeInventoryGenesisCommand(old, budget); return out == nil, e },
		func() (bool, error) {
			key, out, e := encodeInventoryRegisteredOrigin(old, 3, 2, budget)
			return key == nil && out == nil, e
		},
		func() (bool, error) {
			out, e := encodeInventoryRegisteredGenesisOutcome(old, 3, 2, 3, 4, budget)
			return out == nil, e
		},
		func() (bool, error) {
			out, e := decodeInventoryGenesisCommand(registeredFixtureHex(t, inventoryOldGJQ), budget)
			return out == (registeredGenesisSource{}), e
		},
	} {
		zero, e := call()
		if !errors.Is(e, errInvalid) || !zero {
			t.Fatal("wrong compiled journal meaning", e)
		}
	}
	oldKey, oldOrigin, err := encodeRegisteredOrigin(old, 3, 2, budget)
	if err != nil || len(oldKey) != 47 {
		t.Fatal("OLD inert origin setup", err)
	}
	decoded, err := decodeInventoryRegisteredOrigin(oldOrigin, budget)
	if !errors.Is(err, errInvalid) || decoded != (registeredOrigin{}) {
		t.Fatal("OLD origin is not NEW agreement", err)
	}
	for _, bad := range [][]byte{nil, registeredFixtureHex(t, inventoryNewGJQ)[:254]} {
		out, e := decodeInventoryGenesisCommand(bad, budget)
		if !errors.Is(e, errInvalid) || out != (registeredGenesisSource{}) {
			t.Fatal("malformed command cause/zero", e)
		}
	}
	corrupt := registeredFixtureHex(t, inventoryNewOrigin)
	corrupt[len(corrupt)-1] ^= 1
	decoded, err = decodeInventoryRegisteredOrigin(corrupt, budget)
	if !errors.Is(err, errCorrupt) || decoded != (registeredOrigin{}) {
		t.Fatal("checksum corruption cause/zero", err)
	}
	source, err := decodeGenesisCommand(registeredFixtureHex(t, inventoryNewGJQ), budget)
	if err != nil {
		t.Fatal(err)
	}
	source.InitialOwnerEpoch = 0
	for _, call := range []func() (bool, error){
		func() (bool, error) { out, e := encodeInventoryGenesisSource(source, budget); return out == nil, e },
		func() (bool, error) { out, e := encodeInventoryGenesisCommand(source, budget); return out == nil, e },
		func() (bool, error) {
			key, out, e := encodeInventoryRegisteredOrigin(source, 3, 2, budget)
			return key == nil && out == nil, e
		},
		func() (bool, error) {
			out, e := encodeInventoryRegisteredGenesisOutcome(source, 3, 2, 3, 4, budget)
			return out == nil, e
		},
	} {
		zero, e := call()
		if !errors.Is(e, errInvalid) || !zero {
			t.Fatal("invalid NEW source cause/zero", e)
		}
	}
}

func TestRegisteredInventoryBorrowedParserSelection(t *testing.T) {
	_, _, _, budget := registeredCodecFixture(t)
	oldQ, err := registeredStart(budget)
	if err != nil {
		t.Fatal(err)
	}
	old, err := registeredParse(registeredFixtureHex(t, inventoryOldPIJ), budget, oldQ)
	if err != nil {
		t.Fatal(err)
	}
	newQ, err := registeredStart(budget)
	if err != nil {
		t.Fatal(err)
	}
	current, err := registeredParseExpected(registeredFixtureHex(t, inventoryNewPIJ), budget, newQ, registeredInventorySemanticContractID())
	if err != nil || !reflect.DeepEqual(old, current) {
		t.Fatal("borrowed tuple differs across onlyMeaning", err)
	}
	invalidQ, err := registeredStart(budget)
	if err != nil {
		t.Fatal(err)
	}
	zero, err := registeredParseExpected(registeredFixtureHex(t, inventoryNewPIJ), budget, invalidQ, raftlog.ApplicationSemanticContractID{})
	if !errors.Is(err, errInvalid) || !reflect.DeepEqual(zero, registeredParsed{}) {
		t.Fatal("unknown parser expected meaning cause/zero", err)
	}
}

func TestRegisteredInventoryOnePassInspection(t *testing.T) {
	_, _, _, budget := registeredCodecFixture(t)
	wire := registeredFixtureHex(t, inventoryNewPIJ)
	inspected, err := inspectPreparedV1Expected(wire, budget, registeredInventorySemanticContractID())
	if err != nil || inspected.canonicalWork != 3092 || !bytes.Equal(inspected.submissionHash[:], registeredFixtureHex(t, inventoryNewSH)) || !bytes.Equal(inspected.preparedHash[:], registeredFixtureHex(t, inventoryNewPH)) {
		t.Fatal("one-pass fullhash/tally", err)
	}
	if len(inspected.parsed.domain) == 0 || &inspected.parsed.domain[0] != &wire[146] {
		t.Fatal("inspection must borrow actual domain, not owned decode")
	}
	// The returned borrowed inspection and local parsed value may coexist. The
	// phase bound includes both, contexts/cursors and96 source-comparison bytes.
	fixed := unsafe.Sizeof(registeredParsed{}) + unsafe.Sizeof(registeredPreparedInspection{}) +
		unsafe.Sizeof(registeredLedger{}) + unsafe.Sizeof(registeredFormatBudget{}) +
		2*unsafe.Sizeof(registeredCursor{}) + unsafe.Sizeof(registeredAxisView{}) +
		2*256 + 2*32 + 96 + 128
	if fixed > registeredMetadataBytes {
		t.Fatalf("inspect simultaneous fixed allowance%d exceeds2048", fixed)
	}
	t.Logf("inspection fixed phase allowance=%d/2048", fixed)
	for _, cap := range []int{3091, 3092, 3093} {
		b := budget
		b.walkBytes = cap
		out, e := inspectPreparedV1Expected(wire, b, registeredInventorySemanticContractID())
		if cap < 3092 {
			if !errors.Is(e, errLimit) || !reflect.DeepEqual(out, registeredPreparedInspection{}) {
				t.Fatal("inspection one-short cause/zero", e)
			}
		} else if e != nil || out.canonicalWork != 3092 {
			t.Fatal("inspection exact/+1 tally", e)
		}
	}
}

const alternateKey = "484b533101010000000000000000000000000000000000000000000001020000" +
	"000000000000000000000000000001"

const alternateOrigin = "4843523100010100010000000000000000000000000000000000000000000001" +
	"02000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950" +
	"dc852871231394fc9338d89693abf60f00010000000000000006000000fc4a47" +
	"5331000101000000000000000000000000000000000000000000000102000000" +
	"0000000000000000000000000100000000000000010000000000000001000000" +
	"8c12cf5d257654d0fa3779b2b4026d838440ec14e65c1ad4abda97080ecc01f8" +
	"6a00000000000000010300000000000000010000000000000002000000000000" +
	"00031587f6a6c97eab3777c2b5cafbf4013e10b6be377d08d9b32cf348fc60da" +
	"51c7000000000000000200000000000004000000000000100000000000000001" +
	"0000000000000000100000000000002000000000000000001000000000000040" +
	"000000000000001000000000000000010000000000000000000448ddde805ac6" +
	"f5c14d1012add7a8958b81eb97183b4386926af65fbc3acb71c1"

const alternateCreated = "4a4f52310001040000000000000006000001484b533101010000000000000000" +
	"0000000000000000000000000000010200000000000000000000000000000000" +
	"0100000000000000060000000000000004cbb22d33699f795ed00440b5ee0af9" +
	"50dc852871231394fc9338d89693abf60f3f88a2e99ca6ec194a21373de0f6b9" +
	"dca123ed1370e7e76ba5f91b34ece919b9"

const alternateOriginal = "4a4f52310001050000000000000008000001484b533101010000000000000000" +
	"0000000000000000000000000000010200000000000000000000000000000000" +
	"0100000000000000060000000000000004cbb22d33699f795ed00440b5ee0af9" +
	"50dc852871231394fc9338d89693abf60fe2589082a01a98bd03483bfd0dc69c" +
	"b1abc8e31d0ddba0bfe276d8871b45a046"

func TestRegisteredInventoryAlternateOriginalRoundtrip(t *testing.T) {
	budget := registeredJournalBudget{32 << 20}
	source, err := decodeGenesisCommand(registeredFixtureHex(t, inventoryNewGJQ), budget)
	if err != nil {
		t.Fatal("valid independent NEW source setup", err)
	}
	key, origin, err := encodeInventoryRegisteredOrigin(source, 6, 4, budget)
	expectedKey := registeredFixtureHex(t, alternateKey)
	if err != nil || !bytes.Equal(key, expectedKey) || !bytes.Equal(origin, registeredFixtureHex(t, alternateOrigin)) {
		t.Fatal("alternate full Origin/key", err)
	}
	decoded, err := decodeInventoryRegisteredOrigin(origin, budget)
	if err != nil || decoded.source != source || decoded.index != 6 || decoded.term != 4 {
		t.Fatal("alternate complete Origin roundtrip", err)
	}
	for _, tc := range []struct {
		status     byte
		evaluation uint64
		literal    string
	}{{4, 6, alternateCreated}, {5, 8, alternateOriginal}} {
		outcome, err := encodeInventoryRegisteredGenesisOutcome(source, 6, 4, tc.evaluation, tc.status, budget)
		if err != nil || !bytes.Equal(outcome, registeredFixtureHex(t, tc.literal)) || outcome[6] != tc.status || binary.BigEndian.Uint64(outcome[7:15]) != tc.evaluation || outcome[17] != 1 || binary.BigEndian.Uint64(outcome[65:73]) != 6 || binary.BigEndian.Uint64(outcome[73:81]) != 4 {
			t.Fatal("alternate Created/Original full JOR address/term", err)
		}
	}
}
