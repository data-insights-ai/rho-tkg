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

// ApplicationHistoryReadWork reports admitted native history lookup, checksum
// and ownership work. On error it is diagnostic only; no payload is returned.
type ApplicationHistoryReadWork struct{ Rows, Bytes int }

// ApplicationRecordBounded reads the same retained envelope as ApplicationRecord,
// with an explicit source-work grant independent of returned ownership. It funds
// the lookup before access and the actual borrowed frame before hashing/copying;
// a small record need not reserve its configured maximum payload size.
func (v *ApplicationView) ApplicationRecordBounded(ctx context.Context, index uint64, outcome bool, maxBytes int, b ReadBudget) ([]byte, ApplicationHistoryReadWork, error) {
	var work ApplicationHistoryReadWork
	if ctx == nil || index == 0 || maxBytes < 0 || b.Rows < 1 || b.Bytes < 1 {
		return nil, work, ErrInvalid
	}
	if err := v.lock(ctx); err != nil {
		return nil, work, err
	}
	defer v.unlock()
	p := v.s.meta.App.Policy
	if !p.Enabled() || index > v.index {
		return nil, work, ErrInvalid
	}
	if maxBytes > p.MaxPageBytes {
		return nil, work, ErrLimit
	}
	tag, limit := appChangeTag, p.MaxChangeBytes
	if outcome {
		tag, limit = appOutcomeTag, p.MaxOutcomeBytes
	}
	if index < retentionFloor(v.retention, tag) {
		return nil, work, ErrApplicationExpired
	}
	return v.readRetainedHistoryPayload(ctx, index, tag, limit, maxBytes, 3, &b)
}

// historyPayload runs under the genuine view/store locks and owns no new view,
// generation or transfer pin. Source ceiling1082+factor*policy includes fixed
// metadata1024, key9, frame36+n, AV1 hash9+4+n, copy n and root-only hash n.
// The caller operation reserves that policy ceiling; output cap is separate.
func (v *ApplicationView) historyPayload(ctx context.Context, index uint64, tag byte, policyMax, ownedMax, factor int) ([]byte, error) {
	if index < retentionFloor(v.retention, tag) {
		return nil, ErrApplicationExpired
	}
	return v.retainedHistoryPayload(ctx, index, tag, policyMax, ownedMax, factor)
}

func (v *ApplicationView) historyPayloadAnchor(ctx context.Context, index uint64, policyMax, ownedMax int) ([]byte, error) {
	return v.retainedHistoryPayload(ctx, index, appRootTag, policyMax, ownedMax, 4)
}

func (v *ApplicationView) retainedHistoryPayload(ctx context.Context, index uint64, tag byte, policyMax, ownedMax, factor int) ([]byte, error) {
	data, _, err := v.readRetainedHistoryPayload(ctx, index, tag, policyMax, ownedMax, factor, nil)
	return data, err
}

func (v *ApplicationView) readRetainedHistoryPayload(ctx context.Context, index uint64, tag byte, policyMax, ownedMax, factor int, b *ReadBudget) ([]byte, ApplicationHistoryReadWork, error) {
	var work ApplicationHistoryReadWork
	if policyMax < 0 || policyMax > (math.MaxInt-1082)/factor {
		return nil, work, ErrLimit
	}
	// Peak1069+2*n and owned1033+n are smaller than the source ceiling,
	// with factor3/4 and policyMax>=0; this also checks their integer sums.
	if err := ctx.Err(); err != nil {
		return nil, work, err
	}
	if b != nil {
		if b.Bytes < 1082 {
			return nil, work, ErrLimit
		}
		work = ApplicationHistoryReadWork{Rows: 1, Bytes: 1082}
	}
	key := bankIndexKey(v.bank, tag, index)
	raw, closer, err := v.s.db.Get(key)
	if err != nil {
		return nil, work, v.fail(storedReadFailure(err))
	}
	// Length inspection is constant work. Never checksum an oversized record,
	// nor a policy-valid record whose actual hashing/copy work is unfunded.
	if b != nil {
		var preflightErr error
		if len(raw) < appFrameBytes || len(raw)-appFrameBytes > policyMax {
			preflightErr = ErrCorrupt
		} else if cost := factor * (len(raw) - appFrameBytes); cost > b.Bytes-work.Bytes {
			preflightErr = ErrLimit
		} else {
			work.Bytes += cost
		}
		preflightErr = errors.Join(preflightErr, ctx.Err())
		if preflightErr != nil {
			closeErr := closer.Close()
			durableErr := closeErr
			if errors.Is(preflightErr, ErrCorrupt) {
				durableErr = errors.Join(ErrCorrupt, closeErr)
			}
			if durableErr != nil {
				v.s.poison = durableErr
			}
			return nil, work, errors.Join(preflightErr, closeErr)
		}
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
		return nil, work, err
	}
	return owned, work, nil
}
