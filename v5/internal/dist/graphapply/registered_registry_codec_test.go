package graphapply

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Inert Graph01/Partition1/Group02 data only; no Driver or creation authority.
// This test breaks honest unavailable strict RJQ declarations after real baselines.
func TestRegisteredRegistryCodecFirstContract(t *testing.T) {
	type literal struct{ Hex string }
	type row struct{ PIJ, SubmissionHash, PreparedHash, RJQ literal }
	var fixture struct {
		OldRows map[string]row     `json:"old_full_rows"`
		NewRows map[string]row     `json:"new_full_rows"`
		OldSeed map[string]literal `json:"old_seed"`
		NewSeed map[string]literal `json:"new_seed_proposed_contract"`
	}
	data, err := os.ReadFile("testdata/registered_registry_fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	decode := func(value literal) []byte {
		t.Helper()
		b, err := hex.DecodeString(value.Hex)
		if err != nil || len(b) == 0 {
			t.Fatalf("complete independent literal missing: %v", err)
		}
		return b
	}
	format := registeredFormatBudget{preparedBytes: 4 << 20, domainBytes: 4 << 20, axes: 1024, descriptorBytes: 4096, schemas: 4096, schemaNameBytes: 256, walkBytes: 32 << 20}
	for _, baseline := range []struct {
		value row
		id    raftlog.ApplicationSemanticContractID
	}{
		{fixture.OldRows["base"], registeredSemanticContractID()},
		{fixture.NewRows["base"], registeredInventorySemanticContractID()},
	} {
		got, err := inspectPreparedV1Expected(decode(baseline.value.PIJ), format, baseline.id)
		if err != nil || got.canonicalWork != 3092 || !bytes.Equal(got.submissionHash[:], decode(baseline.value.SubmissionHash)) || !bytes.Equal(got.preparedHash[:], decode(baseline.value.PreparedHash)) {
			t.Fatalf("existing OLD/NEW inspection baseline: %v", err)
		}
	}
	for _, seed := range []map[string]literal{fixture.OldSeed, fixture.NewSeed} {
		wire := decode(seed["GJQ255"])
		source, err := decodeGenesisCommand(wire, registeredJournalBudget{walkBytes: 3059})
		if err != nil {
			t.Fatal("existing generic Genesis decode baseline:", err)
		}
		again, err := encodeGenesisCommand(source, registeredJournalBudget{walkBytes: 3314})
		if err != nil || !bytes.Equal(again, wire) {
			t.Fatalf("existing generic Genesis encode baseline: %v", err)
		}
	}
	t.Log("BASELINE_OLD_NEW_PIJ_GENERIC_GENESIS_PASS")
	want := fixture.NewRows["base"]
	pij := decode(want.PIJ)
	budget := registeredRegistryCodecBudget{workBytes: 46144850, ownedBytes: 4198929}
	wire, err := encodeRegistryCommand(pij, format, budget)
	if err != nil {
		t.Fatalf("STRICT_VALID_NEW_RJQ_UNAVAILABLE: %v", err)
	}
	if !bytes.Equal(wire, decode(want.RJQ)) || len(wire) != cap(wire) {
		t.Fatal("strict valid NEW RJQ full bytes/exact owned output differ")
	}
	got, err := decodeRegistryCommand(wire, format, budget)
	if err != nil || got.canonicalWork != 3092 || !bytes.Equal(got.submissionHash[:], decode(want.SubmissionHash)) || !bytes.Equal(got.preparedHash[:], decode(want.PreparedHash)) {
		t.Fatalf("strict NEW RJQ one-pass borrowed inspection: %v", err)
	}
}
