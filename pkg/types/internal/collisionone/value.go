package collision

// Value is a test fixture with the same wire type name as collisiontwo.Value.
type Value struct {
	V byte
}

// HashBytes returns a deterministic payload for collision tests.
func (v Value) HashBytes() []byte { return []byte{v.V} }

// DeepCopyValue returns the immutable fixture value.
func (v Value) DeepCopyValue() any { return v }
