package types

import (
	"testing"
	"unsafe"
)

func TestNodeStructSize(t *testing.T) {
	t.Parallel()

	const want = 80
	got := unsafe.Sizeof(Node{})
	if got != want {
		t.Fatalf("Node struct size = %d bytes, want %d bytes", got, want)
	}
}

func TestRelationshipStructSize(t *testing.T) {
	t.Parallel()

	const want = 72
	got := unsafe.Sizeof(Relationship{})
	if got != want {
		t.Fatalf("Relationship struct size = %d bytes, want %d bytes", got, want)
	}
}

func TestTemporalMetadataStructSize(t *testing.T) {
	t.Parallel()

	const want = 96
	got := unsafe.Sizeof(TemporalMetadata{})
	if got != want {
		t.Fatalf("TemporalMetadata struct size = %d bytes, want %d bytes", got, want)
	}
}

func TestNodeIntegrityStructSize(t *testing.T) {
	t.Parallel()

	// NodeIntegrity layout (v3.0.47):
	//   Hash string         = 16B @ offset 0
	//   PrevHash string     = 16B @ offset 16
	//   AuthorID string     = 16B @ offset 32
	//   Signature []byte    = 24B @ offset 48
	//   AuthorizedBy string = 16B @ offset 72
	//   AuthorizationLevel uint8 = 1B @ offset 88 → padded to 96B (8-byte alignment)
	const want = 96
	got := unsafe.Sizeof(NodeIntegrity{})
	if got != want {
		t.Fatalf("NodeIntegrity struct size = %d bytes, want %d bytes", got, want)
	}
}

func TestRelIntegrityStructSize(t *testing.T) {
	t.Parallel()

	// RelIntegrity layout (v3.0.47):
	//   Hash string              = 16B @ offset 0
	//   PrevHash string          = 16B @ offset 16
	//   FromNodeHash string      = 16B @ offset 32
	//   ToNodeHash string        = 16B @ offset 48
	//   AuthorID string          = 16B @ offset 64
	//   Signature []byte         = 24B @ offset 80
	//   AuthorizedBy string      = 16B @ offset 104
	//   AuthorizationLevel uint8 = 1B  @ offset 120 → padded to 128B (8-byte alignment)
	const want = 128
	got := unsafe.Sizeof(RelIntegrity{})
	if got != want {
		t.Fatalf("RelIntegrity struct size = %d bytes, want %d bytes", got, want)
	}
}
