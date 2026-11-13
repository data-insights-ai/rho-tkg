package badger

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// DiagnoseRelPropertyTxMembersDuring uses the ordinary worker, counting disk membership work.
// Errors retain partial counters; Completed includes a normal callback stop, separately marked.
func (bs *Store) DiagnoseRelPropertyTxMembersDuring(token uint16, key, value string, start, end, tx types.Instant, fn func(types.RelID, types.Instant) bool) (work storecontract.PropertyMemberWork, err error) {
	err = bs.forEachRelPropertyTxMemberDuring(token, key, value, start, end, tx, fn, &work)
	work.Completed = err == nil
	return
}

// DiagnoseNodePropertyTxMembersDuring is the node twin of the relationship diagnostic.
// Counters describe logical work, never physical I/O, encoded bytes or allocated heap.
func (bs *Store) DiagnoseNodePropertyTxMembersDuring(token uint16, key, value string, start, end, tx types.Instant, fn func(types.NodeID, types.Instant) bool) (work storecontract.PropertyMemberWork, err error) {
	err = bs.forEachNodePropertyTxMemberDuring(token, key, value, start, end, tx, fn, &work)
	work.Completed = err == nil
	return
}

func memberWorkCallback[ID any](fn func(ID, types.Instant) bool, work *storecontract.PropertyMemberWork) func(ID, types.Instant) bool {
	if work == nil || fn == nil {
		return fn
	}
	return func(id ID, tx types.Instant) bool {
		work.MembersEmitted++
		keep := fn(id, tx)
		if !keep {
			work.CallbackStopped = true
		}
		return keep
	}
}

func noteMemberOverlay[E any](ov *pendingKeyOverlay[E], key string, op writeOp, e E, work *storecontract.PropertyMemberWork) {
	ov.note(key, op, e)
	if work != nil {
		work.OverlayOpsMatched++
		work.OverlaySetsPeak = max(work.OverlaySetsPeak, uint64(len(ov.sets)))
		work.OverlayDeletesPeak = max(work.OverlayDeletesPeak, uint64(len(ov.dels)))
	}
}
