package idalloc

// ValidateRecipientSession checks fixed recipient identity shape only. It does
// not establish current fencing, activation, delivery freshness or durability.
func ValidateRecipientSession(s RecipientSession) error {
	if s.ID == ([16]byte{}) || s.Incarnation == ([16]byte{}) || s.Epoch == 0 {
		return ErrInvalid
	}
	return nil
}

// ValidateGrantRequest checks recipient-local request shape. Its sequence is
// independent of the allocator's global reservation sequence.
func ValidateGrantRequest(r GrantRequest) error {
	if r.Graph == (GraphID{}) || r.Sequence == 0 || r.Count == 0 || r.Count > MaxBlockSize {
		return ErrInvalid
	}
	return ValidateRecipientSession(r.Session)
}

// ValidateGrant checks exact namespace/count/range bindings using the existing
// reservation invariant. It supplies no authority, acknowledgement or cursor.
func ValidateGrant(g Grant) error {
	if err := ValidateGrantRequest(g.Request); err != nil {
		return err
	}
	b := g.Reservation
	if b.Request.Graph != g.Request.Graph || b.Request.Count != g.Request.Count || !b.validFor(b.Request) {
		return ErrInvalid
	}
	return nil
}
