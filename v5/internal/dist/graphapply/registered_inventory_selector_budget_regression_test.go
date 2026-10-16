package graphapply

import (
	"errors"
	"testing"
)

func TestRegisteredInventorySelectorExhaustion(t *testing.T) {
	wire := registeredFixtureHex(t, inventoryNewGJQ)
	source, err := decodeGenesisCommand(wire, registeredJournalBudget{32 << 20})
	if err != nil {
		t.Fatal("valid independent NEW source setup", err)
	}
	origin := registeredFixtureHex(t, inventoryNewOrigin)
	for _, tc := range []struct {
		name string
		call func(registeredJournalBudget) (bool, error)
	}{
		{"source", func(b registeredJournalBudget) (bool, error) {
			out, e := encodeInventoryGenesisSource(source, b)
			return out == nil, e
		}},
		{"command", func(b registeredJournalBudget) (bool, error) {
			out, e := encodeInventoryGenesisCommand(source, b)
			return out == nil, e
		}},
		{"decode-command", func(b registeredJournalBudget) (bool, error) {
			out, e := decodeInventoryGenesisCommand(wire, b)
			return out == (registeredGenesisSource{}), e
		}},
		{"origin", func(b registeredJournalBudget) (bool, error) {
			key, out, e := encodeInventoryRegisteredOrigin(source, 3, 2, b)
			return key == nil && out == nil, e
		}},
		{"decode-origin", func(b registeredJournalBudget) (bool, error) {
			out, e := decodeInventoryRegisteredOrigin(origin, b)
			return out == (registeredOrigin{}), e
		}},
		{"outcome", func(b registeredJournalBudget) (bool, error) {
			out, e := encodeInventoryRegisteredGenesisOutcome(source, 3, 2, 3, 4, b)
			return out == nil, e
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, limit := range []uint64{31, 32, 33} {
				zero, e := tc.call(registeredJournalBudget{limit})
				if !errors.Is(e, errLimit) || !zero {
					t.Errorf("valid exhausted selector budget%d must return ErrLimit/zero: %v zero%t", limit, e, zero)
				}
			}
		})
	}
}
