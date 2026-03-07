package types

import (
	"bytes"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

func TestCloneBytes_Nil(t *testing.T) {
	t.Parallel()
	if got := CloneBytes(nil); got != nil {
		t.Fatalf("CloneBytes(nil) = %v, want nil", got)
	}
}

func TestCloneBytes_Empty(t *testing.T) {
	t.Parallel()
	src := []byte{}
	got := CloneBytes(src)
	if got == nil || len(got) != 0 {
		t.Fatalf("CloneBytes([]byte{}) = %v, want empty non-nil slice", got)
	}
}

func TestCloneBytes_Isolation(t *testing.T) {
	t.Parallel()
	src := []byte{0xAA, 0xBB, 0xCC}
	clone := CloneBytes(src)
	if !bytes.Equal(clone, src) {
		t.Fatalf("CloneBytes content mismatch: got %x, want %x", clone, src)
	}
	clone[0] = 0xFF
	if src[0] != 0xAA {
		t.Fatal("CloneBytes shares backing array with original")
	}
}

func TestNodeIntegrity_DeepCopy(t *testing.T) {
	t.Parallel()
	orig := &NodeIntegrity{
		Hash:               "h1",
		PrevHash:           "ph1",
		AuthorID:           "author",
		Signature:          []byte{0xAA, 0xBB},
		AuthorizedBy:       "auth",
		AuthorizationLevel: 5,
	}
	cp := orig.DeepCopy()
	if cp == orig {
		t.Fatal("DeepCopy returned same pointer")
	}
	if cp.Hash != orig.Hash || cp.PrevHash != orig.PrevHash {
		t.Fatal("scalar fields differ")
	}
	if !bytes.Equal(cp.Signature, orig.Signature) {
		t.Fatal("Signature content differs")
	}
	// Mutate copy — must not affect original.
	cp.Signature[0] = 0xFF
	if orig.Signature[0] != 0xAA {
		t.Fatal("NodeIntegrity.DeepCopy shares Signature backing array")
	}
}

func TestNodeIntegrity_DeepCopy_NilSignature(t *testing.T) {
	t.Parallel()
	orig := &NodeIntegrity{Hash: "h1"}
	cp := orig.DeepCopy()
	if cp.Signature != nil {
		t.Fatal("expected nil Signature in copy")
	}
}

func TestRelIntegrity_DeepCopy(t *testing.T) {
	t.Parallel()
	orig := &RelIntegrity{
		Hash:               "rh1",
		PrevHash:           "rph1",
		FromNodeHash:       "fnh",
		ToNodeHash:         "tnh",
		AuthorID:           "author",
		Signature:          []byte{0xCC, 0xDD},
		AuthorizedBy:       "auth",
		AuthorizationLevel: 3,
	}
	cp := orig.DeepCopy()
	if cp == orig {
		t.Fatal("DeepCopy returned same pointer")
	}
	if !bytes.Equal(cp.Signature, orig.Signature) {
		t.Fatal("Signature content differs")
	}
	cp.Signature[0] = 0xFF
	if orig.Signature[0] != 0xCC {
		t.Fatal("RelIntegrity.DeepCopy shares Signature backing array")
	}
}

func TestRelIntegrity_DeepCopy_NilSignature(t *testing.T) {
	t.Parallel()
	orig := &RelIntegrity{Hash: "rh1"}
	cp := orig.DeepCopy()
	if cp.Signature != nil {
		t.Fatal("expected nil Signature in copy")
	}
}

func TestNodeDeepCopy_SignatureIsolation(t *testing.T) {
	t.Parallel()
	n := NewNode(snowflake.ID(100), 1, nil)
	n.SetIntegrity(&NodeIntegrity{
		Hash:      "h1",
		Signature: []byte{0xAA, 0xBB},
	})

	cp := n.DeepCopy()
	cp.Integrity().Signature[0] = 0xFF

	if n.Integrity().Signature[0] != 0xAA {
		t.Fatal("Node.DeepCopy shares integrity Signature backing array with original")
	}
}

func TestRelDeepCopy_SignatureIsolation(t *testing.T) {
	t.Parallel()
	r := NewRelationship(snowflake.ID(200), 1, snowflake.ID(10), snowflake.ID(20))
	r.SetIntegrity(&RelIntegrity{
		Hash:      "rh1",
		Signature: []byte{0xCC, 0xDD},
	})

	cp := r.DeepCopy()
	cp.Integrity().Signature[0] = 0xFF

	if r.Integrity().Signature[0] != 0xCC {
		t.Fatal("Relationship.DeepCopy shares integrity Signature backing array with original")
	}
}
