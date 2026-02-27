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

	const want = 32
	got := unsafe.Sizeof(NodeIntegrity{})
	if got != want {
		t.Fatalf("NodeIntegrity struct size = %d bytes, want %d bytes", got, want)
	}
}

func TestRelIntegrityStructSize(t *testing.T) {
	t.Parallel()

	const want = 32
	got := unsafe.Sizeof(RelIntegrity{})
	if got != want {
		t.Fatalf("RelIntegrity struct size = %d bytes, want %d bytes", got, want)
	}
}
