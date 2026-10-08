package types

// Dense ordinals. A store that implements store.OrdinalCapability gives each
// node and each relationship a small integer, dense from 1 in creation order,
// that a consumer can index arrays by instead of keying maps by snowflake ID.
// The store assigns it; a value a caller sets on an entity it passes to a
// store write is ignored (the store sets its own). It is not part of the
// entity's content: not hashed, not persisted, not exported or replicated.
//
// Guarantees, within one open store (see the store package's
// OrdinalCapability for the per-backend detail):
//   - an entity's ordinal never changes while the entity has a current row;
//   - two different entities never carry the same ordinal, and an ordinal is
//     never handed to a second entity after the first is deleted;
//   - 0 means "no ordinal": a store without the capability, a row the store
//     cannot assign one to, or an entity built outside a store.
// Ordinals are not stable across a reopen, between a primary and a replica,
// or between stores.

// Ordinal returns the node's store-assigned dense ordinal; 0 = none.
func (n *Node) Ordinal() uint32 {
	if n == nil {
		return 0
	}
	return n.ordinal
}

// SetOrdinal sets the dense ordinal. For Store implementations, on the copy a
// store is about to keep or hand out; ErrFrozenNode on a frozen node,
// ErrNilNode on nil.
func (n *Node) SetOrdinal(ordinal uint32) error {
	if n == nil {
		return ErrNilNode
	}
	if n.frozen {
		return ErrFrozenNode
	}
	n.ordinal = ordinal
	return nil
}

// Ordinal returns the relationship's store-assigned dense ordinal; 0 = none.
func (r *Relationship) Ordinal() uint32 {
	if r == nil {
		return 0
	}
	return r.ordinal
}

// SetOrdinal is Node.SetOrdinal for relationships (ErrFrozenRelationship,
// ErrNilRelationship).
func (r *Relationship) SetOrdinal(ordinal uint32) error {
	if r == nil {
		return ErrNilRelationship
	}
	if r.frozen {
		return ErrFrozenRelationship
	}
	r.ordinal = ordinal
	return nil
}

// CompactFrozenCopyWithOrdinal is CompactFrozenCopy carrying ordinal instead
// of n's: the copy a store keeps. Nil-safe.
func (n *Node) CompactFrozenCopyWithOrdinal(ordinal uint32) *Node {
	cp := n.CompactFrozenCopy()
	if cp != nil {
		cp.ordinal = ordinal
	}
	return cp
}

// DeepCopyWithOrdinal is DeepCopy carrying ordinal instead of n's (a store's
// history copy). Nil-safe.
func (n *Node) DeepCopyWithOrdinal(ordinal uint32) *Node {
	cp := n.DeepCopy()
	if cp != nil {
		cp.ordinal = ordinal
	}
	return cp
}

// CompactFrozenCopyWithOrdinal is the relationship mirror. Nil-safe.
func (r *Relationship) CompactFrozenCopyWithOrdinal(ordinal uint32) *Relationship {
	cp := r.CompactFrozenCopy()
	if cp != nil {
		cp.ordinal = ordinal
	}
	return cp
}

// DeepCopyWithOrdinal is the relationship mirror. Nil-safe.
func (r *Relationship) DeepCopyWithOrdinal(ordinal uint32) *Relationship {
	cp := r.DeepCopy()
	if cp != nil {
		cp.ordinal = ordinal
	}
	return cp
}

// StartOrdinal returns the dense ordinal of the relationship's start node as
// the store recorded it on the current row it keeps or hands out; 0 = none:
// a store that numbers no nodes (tiered, badger with DisableOrdinals), a
// declared segment type's row, a history row, an endpoint without a current
// row in this store when the row was written (a foreign endpoint), or a
// relationship built outside a store. While the relationship has a current
// row its endpoints have theirs, and an ordinal never changes while its
// entity has a current row, so the value equals Lend(start).Ordinal(). Like
// the ordinal, it is not content: not hashed, persisted or exported.
func (r *Relationship) StartOrdinal() uint32 {
	if r == nil {
		return 0
	}
	return r.startOrdinal
}

// EndOrdinal is StartOrdinal for the end node.
func (r *Relationship) EndOrdinal() uint32 {
	if r == nil {
		return 0
	}
	return r.endOrdinal
}

// SetEndpointOrdinals sets the endpoints' dense ordinals. For Store
// implementations, on the copy a store is about to keep or hand out;
// ErrFrozenRelationship on a frozen relationship, ErrNilRelationship on nil.
func (r *Relationship) SetEndpointOrdinals(start, end uint32) error {
	if r == nil {
		return ErrNilRelationship
	}
	if r.frozen {
		return ErrFrozenRelationship
	}
	r.startOrdinal, r.endOrdinal = start, end
	return nil
}

// CompactFrozenCopyWithOrdinals is CompactFrozenCopy carrying the given
// ordinal and endpoint ordinals: the current row a store keeps. Nil-safe.
func (r *Relationship) CompactFrozenCopyWithOrdinals(ordinal, start, end uint32) *Relationship {
	cp := r.CompactFrozenCopy()
	if cp != nil {
		cp.ordinal, cp.startOrdinal, cp.endOrdinal = ordinal, start, end
	}
	return cp
}

// DeepCopyWithOrdinals is DeepCopy carrying the given ordinal and endpoint
// ordinals (a store's history copy passes 0, 0). Nil-safe.
func (r *Relationship) DeepCopyWithOrdinals(ordinal, start, end uint32) *Relationship {
	cp := r.DeepCopy()
	if cp != nil {
		cp.ordinal, cp.startOrdinal, cp.endOrdinal = ordinal, start, end
	}
	return cp
}
