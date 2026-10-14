package hash32

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestDecodeRoundTripAndRefusals(t *testing.T) {
	sum := sha256.Sum256([]byte("x"))
	s := hex.EncodeToString(sum[:])
	var b [32]byte
	if !Decode(s, &b) || b != sum || String(b[:]) != s || !Canonical(s) {
		t.Fatal("canonical hash does not round-trip")
	}
	for _, bad := range []string{"", s[:63], s + "0", strings.ToUpper(s), "g" + s[1:], "tampered"} {
		if Canonical(bad) {
			t.Fatalf("Canonical(%q) = true", bad)
		}
	}
}
