package types

import "testing"

func TestIsShadowKey(t *testing.T) {
	t.Parallel()

	shadowKeys := []string{
		ShadowLabels, ShadowPrimaryLabel, ShadowID,
		ShadowValidFrom, ShadowValidTo,
		ShadowTransactionFrom, ShadowTransactionTo,
		ShadowCreatedAt, ShadowUpdatedAt,
		ShadowVersion, ShadowPreviousVersion, ShadowNextVersion,
		ShadowBaseEntity, ShadowRelType, ShadowIntegrity,
	}

	for _, key := range shadowKeys {
		if !IsShadowKey(key) {
			t.Errorf("IsShadowKey(%q) = false, want true", key)
		}
	}

	if len(shadowKeys) != 15 {
		t.Fatalf("expected 15 shadow keys, got %d", len(shadowKeys))
	}
}

func TestIsShadowKeyRejectsNonShadow(t *testing.T) {
	t.Parallel()

	nonShadow := []string{"name", "age", "", "tkglabels", "TKG_labels", "shadow_key"}
	for _, key := range nonShadow {
		if IsShadowKey(key) {
			t.Errorf("IsShadowKey(%q) = true, want false", key)
		}
	}
}

func TestShadowKeyConstants(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		constant string
		want     string
	}{
		{"Labels", ShadowLabels, "tkg_labels"},
		{"PrimaryLabel", ShadowPrimaryLabel, "tkg_primary_label"},
		{"ID", ShadowID, "tkg_id"},
		{"ValidFrom", ShadowValidFrom, "tkg_valid_from"},
		{"ValidTo", ShadowValidTo, "tkg_valid_to"},
		{"TransactionFrom", ShadowTransactionFrom, "tkg_transaction_from"},
		{"TransactionTo", ShadowTransactionTo, "tkg_transaction_to"},
		{"CreatedAt", ShadowCreatedAt, "tkg_created_at"},
		{"UpdatedAt", ShadowUpdatedAt, "tkg_updated_at"},
		{"Version", ShadowVersion, "tkg_version"},
		{"PreviousVersion", ShadowPreviousVersion, "tkg_previous_version"},
		{"NextVersion", ShadowNextVersion, "tkg_next_version"},
		{"BaseEntity", ShadowBaseEntity, "tkg_base_entity"},
		{"RelType", ShadowRelType, "tkg_rel_type"},
		{"Integrity", ShadowIntegrity, "tkg_integrity"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.constant != tc.want {
				t.Errorf("Shadow%s = %q, want %q", tc.name, tc.constant, tc.want)
			}
		})
	}
}
