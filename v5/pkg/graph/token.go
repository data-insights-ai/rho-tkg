package graph

import "fmt"

// MaxTokenBytes bounds the encoded size of a Cut or Receipt.
const MaxTokenBytes = 4096

// Receipt is the opaque proof of a commit. The zero Receipt is invalid; there
// is no numeric LSN and no arithmetic on receipts. Bytes and ParseReceipt move
// it across processes (a consumer may persist it); the encoding is opaque and
// only the engine that minted it can interpret it.
type Receipt struct{ tok []byte }

// IsZero reports the invalid zero Receipt.
func (r Receipt) IsZero() bool { return len(r.tok) == 0 }

// Bytes returns a copy of the encoded receipt (nil for the zero Receipt).
func (r Receipt) Bytes() []byte { return cloneTok(r.tok) }

// ParseReceipt wraps bytes previously obtained from Receipt.Bytes. Empty and
// oversized input is ErrInvalidReceipt; the engine checks provenance on use.
func ParseReceipt(b []byte) (Receipt, error) {
	if err := checkTok(b); err != nil {
		return Receipt{}, fmt.Errorf("%w: %v", ErrInvalidReceipt, err)
	}
	return Receipt{tok: cloneTok(b)}, nil
}

// Cut is an opaque certified knowledge boundary, read from Snapshot.Cut and
// accepted by At. The zero Cut is invalid. Bytes and ParseCut let a consumer
// checkpoint a cut to disk (sigma persists its committed stamp); never expose
// ordering or arithmetic on it.
type Cut struct{ tok []byte }

// IsZero reports the invalid zero Cut.
func (c Cut) IsZero() bool { return len(c.tok) == 0 }

// Bytes returns a copy of the encoded cut (nil for the zero Cut).
func (c Cut) Bytes() []byte { return cloneTok(c.tok) }

// ParseCut wraps bytes previously obtained from Cut.Bytes. Empty and oversized
// input is ErrInvalidCut; the engine checks provenance on use.
func ParseCut(b []byte) (Cut, error) {
	if err := checkTok(b); err != nil {
		return Cut{}, fmt.Errorf("%w: %v", ErrInvalidCut, err)
	}
	return Cut{tok: cloneTok(b)}, nil
}

func checkTok(b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("empty")
	}
	if len(b) > MaxTokenBytes {
		return fmt.Errorf("longer than %d bytes", MaxTokenBytes)
	}
	return nil
}

func cloneTok(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}
