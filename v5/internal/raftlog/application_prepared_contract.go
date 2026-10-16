package raftlog

import "context"

// ApplicationContract returns this genuine prepared owner's immutable shared
// policy by value. It exposes no Store, term, manifest or activation authority
// and changes no readers, claims or pins. Callers reserve 1024 logical metadata
// bytes before this call; this is not a physical allocation or RSS bound.
func (p *PreparedApplicationSnapshot) ApplicationContract(ctx context.Context) (ApplicationContract, error) {
	if p == nil || p.s == nil || p.i == nil || p.self != p || ctx == nil {
		return ApplicationContract{}, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return ApplicationContract{}, err
	}
	s := p.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := p.live(); err != nil {
		return ApplicationContract{}, err
	}
	if err := ctx.Err(); err != nil {
		return ApplicationContract{}, err
	}
	return ApplicationContractForPolicyWithRetention(s.meta.App.Policy, s.meta.Controls.Config, s.meta.Recovery.Config, s.meta.Retention.Config)
}
