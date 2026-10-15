package graphapply

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
)

// Only this cost fixture selects a smaller block to qualify the real refill
// path on seventeen facts. The immutable allocator still enforces maxBlock.
func nativeGraphCostBlockSize(s nativeGraphCostSpec) (uint64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	block := min(s.homeCount(), idalloc.MaxBlockSize)
	if raw := os.Getenv("RHO_GRAPH_COST_BLOCK_SIZE"); raw != "" {
		if s.Version == 4 || s.Version == 5 || s.Version == 6 || s.Version == 7 || s.Version == 9 || s.Version == 10 {
			return 0, errInvalid
		} // fresh full-family bound fixes actual block geometry before effects
		var err error
		block, err = strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return 0, errors.Join(errInvalid, err)
		}
	}
	if block < 2 || block > min(s.homeCount(), idalloc.MaxBlockSize) || s.Version != 4 && s.Version != 5 && s.Version != 6 && s.Version != 7 && s.Version != 9 && s.Version != 10 && s.homeCount() > 2*block {
		return 0, errLimit
	}
	return block, nil
}

// Input correspondence only, not a reservation or authority. HOME blocks are
// requested before remote2 so the existing graph-qualified fixture IDs agree.
func nativeGraphCostGrantPlan(s nativeGraphCostSpec, block, partition, sequence uint64) (first, count uint64, err error) {
	if s.Version == 10 {
		if err := nativeImmutableColumnsValidate(s); err != nil {
			return 0, 0, err
		}
		return nativeScalarReadGrantPlan(s, block, partition, sequence)
	}
	if s.Version == 7 {
		return nativeFeedRetainedGrantPlan(s, block, partition, sequence)
	}
	if s.Version == 9 {
		if err := nativeCreateUpdateValidate(s); err != nil {
			return 0, 0, err
		}
		return nativeScalarReadGrantPlan(s, block, partition, sequence)
	}
	if s.Version == 6 {
		return nativeSelectorsGrantPlan(s, block, partition, sequence)
	}
	if s.Version == 5 {
		return nativeFeedGrantPlan(s, block, partition, sequence)
	}
	if s.Version == 4 {
		return nativeGraphEdgeUniqueGrantPlan(s, block, partition, sequence)
	}
	if block < 2 || block > idalloc.MaxBlockSize || s.homeCount() > 2*block || sequence == 0 {
		return 0, 0, errInvalid
	}
	switch partition {
	case 3:
		if sequence > 2 || (sequence-1)*block >= s.homeCount() {
			return 0, 0, errInvalid
		}
		first = 1 + (sequence-1)*block
		return first, min(block, s.homeCount()-first+1), nil
	case 8:
		if sequence == 1 {
			return s.remoteNode(), 2, nil
		}
	}
	return 0, 0, errInvalid
}

func nativeGraphCostGrantParameters(o *certifiedProcessOwners, data []byte) (uint64, uint64, error) {
	if o == nil || o.cost == nil || o.host == nil || len(data) != 16 {
		return 0, 0, errInvalid
	}
	cfg, err := o.host.currentConfiguration()
	if err != nil {
		return 0, 0, err
	}
	partition := uint64(3)
	if o.reserve.ID == ([16]byte{2}) {
		partition = 8
	} else if o.reserve.ID != ([16]byte{1}) {
		return 0, 0, errInvalid
	}
	sequence, count := binary.BigEndian.Uint64(data[:8]), binary.BigEndian.Uint64(data[8:])
	_, want, err := nativeGraphCostGrantPlan(*o.cost, cfg.maxBlock, partition, sequence)
	if err != nil || count != want {
		return 0, 0, errors.Join(errInvalid, err)
	}
	return sequence, count, nil
}

// The candidate sequence is checked against a genuine installed retained grant.
// This local inspection constructs no quorum proof/cursor: the Host still
// validates the complete fresh binding and current recipient at application.
func nativeGraphCostFreshGrant(o *certifiedProcessOwners, id uint64) (grantReference, error) {
	if o == nil || o.cost == nil || o.recipient == nil || o.host == nil || id == 0 {
		return grantReference{}, errInvalid
	}
	cfg, err := o.host.currentConfiguration()
	if err != nil {
		return grantReference{}, err
	}
	partition := o.host.machine.ns.partition
	sequence := uint64(1)
	if partition == 3 && cfg.maxBlock != 0 {
		sequence = (id-1)/cfg.maxBlock + 1
	}
	first, count, err := nativeGraphCostGrantPlan(*o.cost, cfg.maxBlock, partition, sequence)
	if err != nil || id < first || id-first >= count {
		return grantReference{}, errors.Join(errInvalid, err)
	}
	q := allocationQuery{kind: observeGrant, session: o.recipient.session, sequence: sequence, count: count, nonce: [16]byte{1}}
	actual, err := o.host.machine.observeAllocation(q, o.host.driver.Applied())
	if err != nil {
		return grantReference{}, err
	}
	g := actual.grant
	if g.installed == 0 || g.grant.Request.Session != q.session || g.grant.Request.Sequence != sequence || g.grant.Reservation.First != first || g.grant.Reservation.Last != first+count-1 || !g.publication.Contains(id) {
		return grantReference{}, errCorrupt
	}
	return grantReference{session: g.grant.Request.Session, sequence: g.grant.Request.Sequence}, nil
}

func nativeGraphCostInstallGrants(t *testing.T, n *certifiedProcessNetwork, s nativeGraphCostSpec, recipient, partition, block uint64, stream *json.Encoder) {
	t.Helper()
	grants := uint64(1)
	if partition == 3 && block != 0 {
		grants = (s.homeCount() + block - 1) / block
	}
	if s.Version != 4 && s.Version != 5 && s.Version != 6 && s.Version != 7 && s.Version != 9 && s.Version != 10 && grants > 2 {
		t.Fatal("cost setup exceeds two HOME blocks")
	}
	for sequence := uint64(1); sequence <= grants; sequence++ {
		_, count, err := nativeGraphCostGrantPlan(s, block, partition, sequence)
		if err != nil {
			t.Fatal(err)
		}
		n.read(t, 1, observeRecipient, recipient)
		n.read(t, 1, observeAllocator, 0)
		input := graphE17GrantInput(sequence, count)
		reserveWire, reserved := nativeGraphCostGrantAction(t, n, certifiedReserve, 0, input)
		n.group(1).event(t, 1, processRequest{op: processCertifiedRead, entity: uint64(observeGrant), data: input})
		if !n.all[1].call(t, processRequest{op: processStateOp}).ready {
			t.Fatal("actual cost grant ReadIndex incomplete", partition, sequence)
		}
		installWire, installed := nativeGraphCostGrantAction(t, n, certifiedInstallGrant, partition, nil)
		// Raw production originals. Requested range/count is correspondence,
		// not decoded cursor issuance or a fabricated stored grant observation.
		if err := stream.Encode(nativeGraphCostGrantReceipt{
			Partition: partition, Sequence: sequence, RequestedCount: count,
			ReserveOutcome: reserved.outcome, InstallOutcome: installed.outcome,
			ReserveCDC: reserved.changes, InstallCDC: installed.changes,
			ReserveProposal: reserveWire, InstallProposal: installWire,
		}); err != nil {
			t.Fatal(err)
		}
	}
}
