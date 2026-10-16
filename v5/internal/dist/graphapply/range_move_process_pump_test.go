package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
)

// The process pump returns an outcome only after the exact native proposal is
// present in the committed log. Cancellation belongs to this operation, never
// to an unrelated successful Raft event driving its progress.
func pollGraphProcessMoveSubmission(ctx context.Context, h *allocationHost, p allocationProposal, baseline uint64, scanned int) (uint64, int, []byte, error, error) {
	c, err := decodeRangeMoveCommand(p.wire)
	if err != nil {
		return baseline, scanned, nil, err, nil
	}
	intent, err := decodeRangeMoveIntent(c.intent)
	if err != nil {
		return baseline, scanned, nil, err, nil
	}
	for index := baseline + 1; index <= h.driver.Applied(); index++ {
		if scanned >= 1024 {
			return baseline, scanned, nil, errLimit, nil
		}
		scanned++
		baseline = index
		entries, e := h.machine.store.Entries(index, index+1, uint64(h.machine.limits.commandBytes))
		if e != nil {
			return baseline, scanned, nil, nil, e
		}
		if len(entries) != 1 || entries[0].GetIndex() != index {
			return baseline, scanned, nil, nil, errCorrupt
		}
		if !bytes.Equal(entries[0].GetData(), p.wire) {
			continue
		}
		body, e := h.machine.store.ApplicationRecord(ctx, index, true, 256)
		if e != nil {
			if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
				return baseline, scanned, nil, e, nil
			}
			return baseline, scanned, nil, nil, e
		}
		if err := ctx.Err(); err != nil {
			return baseline, scanned, nil, err, nil
		}
		status, e := decodeRangeMoveStatus(body)
		if e != nil || status.ns != (namespace{p.target.graph, p.target.partition}) || status.move != rangeMoveID(namespace{intent.source.graph, intent.source.partition}, intent.id) || status.intent != sha256.Sum256(c.intent) || status.index > index {
			return baseline, scanned, nil, nil, errors.Join(errInvalid, e)
		}
		return baseline, scanned, body, nil, nil
	}
	return baseline, scanned, nil, nil, nil
}
