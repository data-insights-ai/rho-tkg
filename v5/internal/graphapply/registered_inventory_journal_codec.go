package graphapply

// NEW fixed adapters are inert data codecs, never factory/readiness authority.
// Reserve the32-byte expected-meaning comparison before decoding or comparing.
// The unchanged generic codec receives the remaining checked budget.
func reserveInventoryJournalMeaning(b registeredJournalBudget) (registeredJournalBudget, error) {
	if b.walkBytes == 0 || b.walkBytes > 32<<20 {
		return registeredJournalBudget{}, errInvalid
	}
	if b.walkBytes <= 32 {
		return registeredJournalBudget{}, errLimit
	}
	return registeredJournalBudget{b.walkBytes - 32}, nil
}

func encodeInventoryGenesisSource(s registeredGenesisSource, b registeredJournalBudget) ([]byte, error) {
	remaining, err := reserveInventoryJournalMeaning(b)
	if err != nil {
		return nil, err
	}
	if s.Meaning != [32]byte(registeredInventorySemanticContractID()) {
		return nil, errInvalid
	}
	return encodeGenesisSource(s, remaining)
}

func encodeInventoryGenesisCommand(s registeredGenesisSource, b registeredJournalBudget) ([]byte, error) {
	remaining, err := reserveInventoryJournalMeaning(b)
	if err != nil {
		return nil, err
	}
	if s.Meaning != [32]byte(registeredInventorySemanticContractID()) {
		return nil, errInvalid
	}
	return encodeGenesisCommand(s, remaining)
}

func decodeInventoryGenesisCommand(wire []byte, b registeredJournalBudget) (registeredGenesisSource, error) {
	remaining, err := reserveInventoryJournalMeaning(b)
	if err != nil {
		return registeredGenesisSource{}, err
	}
	source, err := decodeGenesisCommand(wire, remaining)
	if err != nil {
		return registeredGenesisSource{}, err
	}
	if source.Meaning != [32]byte(registeredInventorySemanticContractID()) {
		return registeredGenesisSource{}, errInvalid
	}
	return source, nil
}

func encodeInventoryRegisteredOrigin(s registeredGenesisSource, index, term uint64, b registeredJournalBudget) ([]byte, []byte, error) {
	remaining, err := reserveInventoryJournalMeaning(b)
	if err != nil {
		return nil, nil, err
	}
	if s.Meaning != [32]byte(registeredInventorySemanticContractID()) {
		return nil, nil, errInvalid
	}
	return encodeRegisteredOrigin(s, index, term, remaining)
}

func decodeInventoryRegisteredOrigin(wire []byte, b registeredJournalBudget) (registeredOrigin, error) {
	remaining, err := reserveInventoryJournalMeaning(b)
	if err != nil {
		return registeredOrigin{}, err
	}
	origin, err := decodeRegisteredOrigin(wire, remaining)
	if err != nil {
		return registeredOrigin{}, err
	}
	if origin.source.Meaning != [32]byte(registeredInventorySemanticContractID()) {
		return registeredOrigin{}, errInvalid
	}
	return origin, nil
}

func encodeInventoryRegisteredGenesisOutcome(s registeredGenesisSource, index, term, evaluation uint64, status byte, b registeredJournalBudget) ([]byte, error) {
	remaining, err := reserveInventoryJournalMeaning(b)
	if err != nil {
		return nil, err
	}
	if s.Meaning != [32]byte(registeredInventorySemanticContractID()) {
		return nil, errInvalid
	}
	return encodeRegisteredGenesisOutcome(s, index, term, evaluation, status, remaining)
}
