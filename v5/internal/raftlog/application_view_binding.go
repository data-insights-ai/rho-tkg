package raftlog

import "context"

// ApplicationBinding returns the immutable store namespace and semantic
// agreement through this live retained view. The fixed by-value result grants
// no readiness, cut, lease, activation or traffic authority. A retained older
// physical generation reports the same binding; unbound stores report zero.
func (v *ApplicationView) ApplicationBinding(ctx context.Context) (ApplicationBinding, error) {
	if v == nil || v.s == nil || v.index == 0 || ctx == nil {
		return ApplicationBinding{}, ErrInvalid
	}
	if err := v.lock(ctx); err != nil {
		return ApplicationBinding{}, err
	}
	defer v.unlock()
	id := v.s.meta.Rep.SemanticContractID
	if id == (ApplicationSemanticContractID{}) {
		return ApplicationBinding{}, nil
	}
	return ApplicationBinding{Identity: v.s.meta.Transfer.Identity, SemanticContractID: id}, nil
}
