package raftlog

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"
)

// RootAt returns one owned retained root within this exact live captured view.
// The index/generation/image grants no cut, readiness, or activation authority.
// maxImageBytes bounds returned ownership, not policy-bounded frame hashing.
func (v *ApplicationView) RootAt(ctx context.Context, index uint64, maxImageBytes int) (ApplicationRoot, error) {
	if ctx == nil || index == 0 || maxImageBytes < 0 {
		return ApplicationRoot{}, ErrInvalid
	}
	if err := v.lock(ctx); err != nil {
		return ApplicationRoot{}, err
	}
	defer v.unlock()
	p := v.s.meta.App.Policy
	if !p.Enabled() || index > v.index {
		return ApplicationRoot{}, ErrInvalid
	}
	if maxImageBytes > p.MaxImageBytes {
		return ApplicationRoot{}, ErrLimit
	}
	image, err := v.historyPayload(ctx, index, appRootTag, p.MaxImageBytes, maxImageBytes, 4)
	if err != nil {
		return ApplicationRoot{}, err
	}
	return ApplicationRoot{Generation: v.generation, Index: index, Image: image, ImageHash: sha256.Sum256(image)}, nil
}

// ApplicationRecord returns an owned change/outcome envelope from this view's
// bank at a retained local index. false selects changes, true outcomes. A
// present empty envelope remains nonnil. Errors return nil with original cause.
func (v *ApplicationView) ApplicationRecord(ctx context.Context, index uint64, outcome bool, maxBytes int) ([]byte, error) {
	if ctx == nil || index == 0 || maxBytes < 0 {
		return nil, ErrInvalid
	}
	if err := v.lock(ctx); err != nil {
		return nil, err
	}
	defer v.unlock()
	p := v.s.meta.App.Policy
	if !p.Enabled() || index > v.index {
		return nil, ErrInvalid
	}
	if maxBytes > p.MaxPageBytes {
		return nil, ErrLimit
	}
	tag, limit := appChangeTag, p.MaxChangeBytes
	if outcome {
		tag, limit = appOutcomeTag, p.MaxOutcomeBytes
	}
	return v.historyPayload(ctx, index, tag, limit, maxBytes, 3)
}

// historyPayload runs under the genuine view/store locks and owns no new view,
// generation or transfer pin. Source ceiling1082+factor*policy includes fixed
// metadata1024, key9, frame36+n, AV1 hash9+4+n, copy n and root-only hash n.
// The caller operation reserves that policy ceiling; output cap is separate.
func (v *ApplicationView) historyPayload(ctx context.Context, index uint64, tag byte, policyMax, ownedMax, factor int) ([]byte, error) {
	if policyMax < 0 || policyMax > (math.MaxInt-1082)/factor {
		return nil, ErrLimit
	}
	// Peak1069+2*n and owned1033+n are smaller than the source ceiling,
	// with factor3/4 and policyMax>=0; this also checks their integer sums.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := bankIndexKey(v.bank, tag, index)
	raw, closer, err := v.s.db.Get(key)
	if err != nil {
		return nil, v.fail(storedReadFailure(err))
	}
	data, deleted, inspectErr := inspectAppFrame(key, raw, policyMax)
	if deleted {
		inspectErr = ErrCorrupt
	}
	opErr := inspectErr
	if inspectErr == nil && len(data) > ownedMax {
		opErr = ErrLimit
	}
	// Cancellation observed with a borrowed value still releases its closer.
	// No output escapes before closer success, including a prior owned copy.
	opErr = errors.Join(opErr, ctx.Err())
	var owned []byte
	if opErr == nil {
		owned = copyApplicationBytes(data)
	}
	closeErr := closer.Close()
	if durableErr := errors.Join(inspectErr, closeErr); durableErr != nil {
		v.s.poison = durableErr
	}
	if err := errors.Join(opErr, closeErr); err != nil {
		return nil, err
	}
	return owned, nil
}
