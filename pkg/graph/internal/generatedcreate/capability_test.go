package generatedcreate

import "testing"

func TestProofValidOnlyForFreshGraphID(t *testing.T) {
	if !FreshGraphID.Valid() {
		t.Fatal("FreshGraphID.Valid() = false, want true")
	}
	if (Proof{}).Valid() {
		t.Fatal("zero Proof.Valid() = true, want false")
	}
}
