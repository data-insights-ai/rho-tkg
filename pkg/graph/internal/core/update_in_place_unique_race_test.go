package core

// The in-place-update interleaving with CreateUnique's install is pinned by
// the update_in_place case of TestCreateUnique_BlocksInstallUntilInFlightWriteCompletes
// (unique_constraints_toctou_test.go).
