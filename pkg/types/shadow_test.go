package types

import "testing"

func TestIsShadowKey(t *testing.T) {
	t.Parallel()

	shadowKeys := []string{
		ShadowLabels, ShadowType,
		ShadowValidFrom, ShadowValidTo,
		ShadowTxFrom, ShadowTxTo,
		ShadowCreatedAt, ShadowUpdatedAt, ShadowDeletedAt,
		ShadowCreatedBy, ShadowUpdatedBy,
		ShadowVersion,
		ShadowHash, ShadowPrevHash,
		ShadowBaseEntity,
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
		{"Type", ShadowType, "tkg_type"},
		{"ValidFrom", ShadowValidFrom, "tkg_valid_from"},
		{"ValidTo", ShadowValidTo, "tkg_valid_to"},
		{"TxFrom", ShadowTxFrom, "tkg_tx_from"},
		{"TxTo", ShadowTxTo, "tkg_tx_to"},
		{"CreatedAt", ShadowCreatedAt, "tkg_created_at"},
		{"UpdatedAt", ShadowUpdatedAt, "tkg_updated_at"},
		{"DeletedAt", ShadowDeletedAt, "tkg_deleted_at"},
		{"CreatedBy", ShadowCreatedBy, "tkg_created_by"},
		{"UpdatedBy", ShadowUpdatedBy, "tkg_updated_by"},
		{"Version", ShadowVersion, "tkg_version"},
		{"Hash", ShadowHash, "tkg_hash"},
		{"PrevHash", ShadowPrevHash, "tkg_prev_hash"},
		{"BaseEntity", ShadowBaseEntity, "tkg_base_entity"},
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
