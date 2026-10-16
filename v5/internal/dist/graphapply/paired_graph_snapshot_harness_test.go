package graphapply

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// The same two-sender/one-import process registry used by the legacy snapshot
// fixture carries actual child-local capabilities. Only paired source children
// enable this embedding path; no copied handle is graph or activation authority.
func pairedSnapshotHandles(owners *processOwners, event allocationEvent) ([]uint64, error) {
	count := 0
	for _, p := range event.packets {
		if p.Snapshot {
			count++
		}
	}
	if count == 0 && len(event.snapshotSends) == 0 {
		return nil, nil
	}
	if owners == nil || count != len(event.snapshotSends) || count > len(owners.sends) {
		return nil, errInvalid
	}
	handles := make([]uint64, 0, count)
	for _, send := range event.snapshotSends {
		if send == nil {
			return nil, errInvalid
		}
		id, err := owners.sender(send, true)
		if err != nil {
			return nil, err
		}
		handles = append(handles, id)
	}
	return handles, nil
}

func pairedSnapshotProcessEvent(ctx context.Context, h *allocationHost, owners *processOwners, r processRequest) (response processResponse, event allocationEvent, err error) {
	if ctx == nil || h == nil || owners == nil || owners.closed {
		return response, event, errInvalid
	}
	switch r.op {
	case processOffers:
		var sends []*replica.SnapshotSend
		sends, err = h.driver.SnapshotSends()
		for _, send := range sends {
			if err != nil {
				break
			}
			var id uint64
			id, err = owners.sender(send, false)
			response.handles = append(response.handles, id)
		}
	case processBuild, processSeal, processManifest, processNext, processFeedback:
		send := owners.send(r.index)
		if send == nil {
			return response, event, errInvalid
		}
		switch r.op {
		case processBuild:
			response.ready, err = send.Build(ctx, raftlog.ReadBudget{Rows: 64, Bytes: 256 << 10})
		case processManifest:
			var manifest raftlog.ApplicationSnapshotManifest
			manifest, err = send.Manifest()
			if err == nil {
				response.aux, err = raftlog.EncodeApplicationSnapshotManifest(manifest)
			}
		case processNext:
			var chunk raftlog.ApplicationSnapshotChunk
			chunk, err = send.Next(ctx, raftlog.ReadBudget{Rows: 64, Bytes: 256 << 10})
			if err == nil {
				response.aux, err = encodeProcessChunk(chunk)
				response.ready = chunk.Final
			}
		case processSeal, processFeedback:
			// Host.event consumes read replies/pending maps under the same
			// event lock as Step/Tick; direct unguarded Driver calls do not.
			h.mu.Lock()
			if h.closed {
				err = replica.ErrStopped
			} else if r.op == processSeal {
				err = h.driver.SealSnapshotSend(send)
			} else {
				event, err = h.event(h.driver.ReportSnapshotSend(send, r.entity == 1))
				if err == nil {
					owners.removeSend(r.index)
				}
			}
			h.mu.Unlock()
		}
	case processBeginImport:
		if owners.imported != nil {
			return response, event, errLimit
		}
		var manifest raftlog.ApplicationSnapshotManifest
		manifest, err = raftlog.DecodeApplicationSnapshotManifestForBinding(r.data, h.machine.store.ApplicationLimits().MaxImageBytes, h.machine.store.ApplicationBinding())
		if err == nil && manifest.Version != 5 {
			err = raftlog.ErrInvalid
		}
		if err == nil {
			owners.imported, err = h.machine.store.BeginApplicationImport(ctx, manifest)
		}
		if err == nil {
			owners.importID, err = owners.id()
			response.handles = []uint64{owners.importID}
			if err == nil {
				var status raftlog.ApplicationImportStatus
				status, err = owners.imported.Status()
				response.aux = status.ManifestID[:]
			}
		}
	case processAppend, processVerify, processPrepare, processActivate, processImportClose:
		if owners.imported == nil || owners.importID != r.index {
			return response, event, errInvalid
		}
		switch r.op {
		case processAppend:
			var chunk raftlog.ApplicationSnapshotChunk
			chunk, err = decodePairedProcessChunk(r.data)
			if err == nil {
				err = owners.imported.Append(ctx, chunk)
			}
		case processVerify:
			err = owners.imported.Verify(ctx)
		case processPrepare:
			owners.prepared, err = owners.imported.Prepare()
		case processImportClose:
			err = owners.closeImport()
		case processActivate:
			h.mu.Lock()
			if h.closed {
				err = replica.ErrStopped
			} else {
				event, err = h.event(h.driver.StepApplicationSnapshot(r.packet, owners.prepared))
			}
			h.mu.Unlock()
		}
	default:
		err = errInvalid
	}
	return response, event, err
}

// AS5 uses the existing bounded IPC chunk layout. The legacy decoder's FV<=3
// restriction stays unchanged; this paired-only door admits exactly version5.
// Borrowed fields live only through synchronous Append; parent decoded fields
// are observations, never a Store/Driver capability.
func decodePairedProcessChunk(b []byte) (raftlog.ApplicationSnapshotChunk, error) {
	c := graphCursor{b: b}
	version := c.count(5, 0)
	chunk := raftlog.ApplicationSnapshotChunk{Version: uint32(version)} // #nosec G115 -- count checked <=5.
	copy(chunk.CutID[:], c.take(32))
	copy(chunk.ManifestID[:], c.take(32))
	chunk.Sequence, chunk.Visited, chunk.VisitedBytes = c.u64(), c.u64(), c.u64()
	flag := c.tag()
	if flag > 1 {
		c.err = errCorrupt
	}
	chunk.Final = flag == 1
	chunk.After = c.field(2*1024 + 11)
	chunk.Data = c.field(256 << 10)
	if chunk.Version != 5 || len(c.b) != 0 {
		c.err = errCorrupt
	}
	if c.err != nil {
		return raftlog.ApplicationSnapshotChunk{}, errors.Join(errCorrupt, c.err)
	}
	chunk.After, chunk.Data = bytes.Clone(chunk.After), bytes.Clone(chunk.Data)
	return chunk, nil
}

func (f *pairedGraphFaultNetwork) snapshotPage() {
	f.t.Helper()
	f.requireActive()
	f.snapshotPages++
	if f.snapshotPages > processDeliveries {
		f.t.Fatal("paired cumulative snapshot page bound", f.snapshotPages)
	}
}

// Snapshot completion belongs to the actual embedding, not Tick or copied
// checkpoint bytes. The 32-round caller, original sender/import quotas, shared
// packet allowance and one cumulative 4096-page ledger remain unchanged.
func (f *pairedGraphFaultNetwork) snapshotCatchup(leader uint64, sealed map[uint64]bool) {
	f.t.Helper()
	donor := f.n.all[leader]
	offers := f.call(donor, processRequest{op: processOffers})
	for _, owner := range offers.handles {
		if sealed[owner] {
			continue
		}
		for {
			f.snapshotPage()
			built := f.call(donor, processRequest{op: processBuild, index: owner})
			if built.ready {
				break
			}
		}
		f.call(donor, processRequest{op: processSeal, index: owner})
		sealed[owner] = true
	}
	group := f.n.group(leader)
	pending := group.snapshots
	group.snapshots = nil
	hub := donor.mux.hub
	hub.mu.Lock()
	for i := 0; i < len(hub.snapshots); {
		send := hub.snapshots[i]
		if send.packet.From != leader {
			i++
			continue
		}
		pending = append(pending, send)
		hub.snapshotBytes -= cap(send.packet.Payload) + 64
		hub.snapshots = append(hub.snapshots[:i], hub.snapshots[i+1:]...)
	}
	hub.mu.Unlock()
	if len(pending) > 2 {
		f.t.Fatal("paired pending snapshot owner bound", len(pending))
	}
	for _, send := range pending {
		f.transferSnapshot(leader, send)
		if nativeSnapshotSenderSide(f.nativeSnapshotState) != "" && nativeSnapshotSenderReaped(f, donor) {
			return // All remaining handles belong to this reaped sender; no ACK.
		}
	}
}

func (f *pairedGraphFaultNetwork) transferSnapshot(leader uint64, send processSnapshotPacket) {
	f.t.Helper()
	if send.packet.From != leader || !send.packet.Snapshot || send.owner == 0 || (send.packet.To <= 3) != (leader <= 3) {
		f.t.Fatal("paired actual sender/packet association")
	}
	f.mu.Lock()
	down := f.down[send.packet.To]
	f.mu.Unlock()
	if down {
		f.t.Fatal("paired snapshot receiver still down", send.packet.To)
	}
	donor, receiver := f.n.all[leader], f.n.all[send.packet.To]
	senderCase := nativeSnapshotSenderSide(f.nativeSnapshotState) != ""
	notice := func(b nativeFaultSnapshotBoundary) bool {
		if senderCase {
			b.SenderOwner = send.owner
		}
		return f.nativeSnapshotNotice(b)
	}
	var knownOwner uint64
	finished := false
	senderFinished := false
	if f.parent != nil {
		defer func() {
			if finished {
				return
			}
			// Only an actually returned Begin handle may be closed. Cleanup
			// survives caller expiry; it never invents a successful import/ACK.
			f.mu.Lock()
			reaped := f.down[send.packet.To] && receiver.closed && receiver.cmd.ProcessState != nil
			f.mu.Unlock()
			if knownOwner != 0 && !reaped {
				if _, err := f.cleanupExchange(receiver, processRequest{op: processImportClose, index: knownOwner}); err != nil {
					f.t.Error("interrupted actual import cleanup", err)
				}
			}
			if !senderFinished && !(senderCase && nativeSnapshotSenderReaped(f, donor)) {
				feedback, err := f.cleanupExchange(donor, processRequest{op: processFeedback, index: send.owner, entity: 0})
				if err != nil {
					f.t.Error("interrupted actual sender failure feedback", err)
				} else {
					group := f.n.group(leader)
					group.enqueue(f.t, feedback) // Actual packets only; expiry never fakes delivery.
				}
			}
		}()
	}
	wire := f.call(donor, processRequest{op: processManifest, index: send.owner}).aux
	spec := partitionCodecRequest(f.t, 1, defaultMaterializerLimits())
	ns, _ := certifiedProcessNamespace(leader)
	part, _ := spec.declaration.Partition(ns.partition)
	binding := raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(ns.graph), Partition: ns.partition, Group: part.Group}, SemanticContractID: certifiedGraphSemanticContractID()}
	if f.n.rangeMoves {
		binding.SemanticContractID = rangeMoveSemanticContractID()
	}
	manifest, err := raftlog.DecodeApplicationSnapshotManifestForBinding(wire, 172, binding)
	if err != nil || manifest.Version != 5 {
		f.t.Fatal("paired actual AS5 manifest", err, manifest.Version)
	}
	abandon := func() {
		if senderCase && nativeSnapshotSenderReaped(f, donor) {
			// The captured OS process is gone, so it cannot receive Feedback.
			// Abort ONLY the actual acknowledged receiver owner; no ACK or
			// replacement sender capability is synthesized from its old ID.
			if knownOwner == 0 {
				f.t.Fatal("sender loss before genuine receiver Begin")
			}
			closed, err := f.cleanupExchange(receiver, processRequest{op: processImportClose, index: knownOwner})
			if err != nil {
				f.t.Fatal("sender-loss actual receiver cleanup", err)
			}
			if !notice(nativeFaultSnapshotBoundary{Phase: "after-sender-close", Donor: leader, Receiver: send.packet.To, Owner: knownOwner, Manifest: wire, Packet: send.packet, Response: closed}) {
				f.t.Fatal("sender cleanup observation refused")
			}
			knownOwner, finished, senderFinished = 0, true, true
			return
		}
		group := f.n.group(leader)
		feedback := f.call(donor, processRequest{op: processFeedback, index: send.owner, entity: 0})
		senderFinished = true
		group.enqueue(f.t, feedback)
		f.drainGroup(group)
		finished = knownOwner == 0
	}
	if (f.nativeSnapshotState == "before-begin" || f.nativeSnapshotState == "before-close") && !notice(nativeFaultSnapshotBoundary{Phase: "before-begin", Donor: leader, Receiver: send.packet.To, Manifest: wire, Packet: send.packet}) {
		abandon()
		return // A lost Begin response supplies NO import handle.
	}
	started := f.call(receiver, processRequest{op: processBeginImport, data: wire})
	if len(started.handles) != 1 || len(started.aux) != 32 {
		f.t.Fatal("paired exact receiver import owner")
	}
	owner := started.handles[0]
	knownOwner = owner
	if f.nativeSnapshotState != "" && !notice(nativeFaultSnapshotBoundary{Phase: "after-begin", Donor: leader, Receiver: send.packet.To, Owner: owner, Manifest: wire, Packet: send.packet, Response: started}) {
		abandon()
		return
	}
	for {
		f.snapshotPage()
		page := f.call(donor, processRequest{op: processNext, index: send.owner})
		chunk, err := decodePairedProcessChunk(page.aux)
		if err != nil || chunk.Version != manifest.Version || chunk.CutID != manifest.CutID || !bytes.Equal(chunk.ManifestID[:], started.aux) || chunk.Visited > 64 || chunk.VisitedBytes > 256<<10 {
			f.t.Fatal("paired actual snapshot page bounds/identity", err)
		}
		if (f.nativeSnapshotState == "before-append" || senderCase && nativeSnapshotSenderSide(f.nativeSnapshotState) == "before") && !notice(nativeFaultSnapshotBoundary{Phase: "before-append", Donor: leader, Receiver: send.packet.To, Owner: owner, Manifest: wire, Chunk: page.aux, Packet: send.packet}) {
			abandon()
			return
		}
		appended := f.call(receiver, processRequest{op: processAppend, index: owner, data: page.aux})
		if !notice(nativeFaultSnapshotBoundary{Phase: "after-append", Donor: leader, Receiver: send.packet.To, Owner: owner, Manifest: wire, Chunk: page.aux, Packet: send.packet, Response: appended}) {
			abandon()
			return
		}
		if chunk.Final {
			break
		}
	}
	if f.nativeSnapshotState == "before-verify" && !notice(nativeFaultSnapshotBoundary{Phase: "before-verify", Donor: leader, Receiver: send.packet.To, Owner: owner, Manifest: wire, Packet: send.packet}) {
		abandon()
		return
	}
	f.call(receiver, processRequest{op: processVerify, index: owner})
	prepared := f.call(receiver, processRequest{op: processPrepare, index: owner})
	if prepared.stats[statPrepared] != 1 || prepared.stats[statImport] != 1 || prepared.stats[statVerified] != 1 {
		f.t.Fatal("paired genuine dormant prepared owner missing")
	}
	if f.nativeSnapshotState == "before-close" && !notice(nativeFaultSnapshotBoundary{Phase: "before-close", Donor: leader, Receiver: send.packet.To, Owner: owner, Manifest: wire, Packet: send.packet, Response: prepared}) {
		abandon()
		return // Only the selected fresh recipe abandons this UNCLAIMED owner.
	}
	if !notice(nativeFaultSnapshotBoundary{Phase: "before-activate", Donor: leader, Receiver: send.packet.To, Owner: owner, Manifest: wire, Packet: send.packet, Response: prepared}) {
		abandon()
		return
	}
	activated := f.call(receiver, processRequest{op: processActivate, index: owner, packet: send.packet})
	if activated.stats[statCheckpoint] != manifest.Index || activated.stats[statAuditIndex] != manifest.Index || activated.cutID != manifest.CutID || !bytes.Equal(activated.manifestID[:], started.aux) {
		f.t.Fatal("paired actual Raft Ready activation mismatch")
	}
	if !notice(nativeFaultSnapshotBoundary{Phase: "after-activate", Donor: leader, Receiver: send.packet.To, Owner: owner, Manifest: wire, Packet: send.packet, Response: activated}) {
		f.t.Fatal("completed activation cannot be withdrawn by a diagnostic")
	}
	// Actual receiver activation is proved before sender feedback; receiver ACK
	// packets stay held until feedback on the still-pending exact sender owner.
	group := f.n.group(leader)
	feedback := f.call(donor, processRequest{op: processFeedback, index: send.owner, entity: 1})
	senderFinished = true
	group.enqueue(f.t, feedback)
	group.enqueue(f.t, activated)
	f.drainGroup(group)
	if f.parent == nil {
		f.call(receiver, processRequest{op: processImportClose, index: owner})
	} else if _, err := f.cleanupExchange(receiver, processRequest{op: processImportClose, index: owner}); err != nil {
		f.t.Fatal("actual completed import cleanup", err)
	}
	knownOwner, finished = 0, true
	f.requireActive()
	f.t.Logf("paired actual AS5 snapshot sender%d receiver%d index%d cumulativePages%d", leader, send.packet.To, manifest.Index, f.snapshotPages)
}

func TestNativePairedGraphSnapshotChunkKeepsLegacyRestrictionAndOwnedBounds(t *testing.T) {
	valid := raftlog.ApplicationSnapshotChunk{Version: 5, CutID: [32]byte{1}, ManifestID: [32]byte{2}, Sequence: 3, Visited: 1, VisitedBytes: 20, After: []byte{1}, Data: []byte{2, 3}, Final: true}
	wire, err := encodeProcessChunk(valid)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodePairedProcessChunk(wire)
	if err != nil || !reflect.DeepEqual(got, valid) {
		t.Fatal(got, err)
	}
	if _, err := decodeProcessChunk(wire); !errors.Is(err, errCorrupt) {
		t.Fatal("legacy decoder silently admitted AS5", err)
	}
	clear(wire)
	if !bytes.Equal(got.Data, []byte{2, 3}) || !bytes.Equal(got.After, []byte{1}) {
		t.Fatal("paired chunk aliases caller wire")
	}
	for _, version := range []uint32{0, 1, 2, 3, 4, 6} {
		bad := valid
		bad.Version = version
		wire, err := encodeProcessChunk(bad)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodePairedProcessChunk(wire); !errors.Is(err, errCorrupt) {
			t.Fatal("unsupported paired chunk", version, err)
		}
	}
	wire, err = encodeProcessChunk(valid)
	if err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string][]byte{"nil": nil, "truncated": wire[:len(wire)-1], "extra": append(bytes.Clone(wire), 0)} {
		if _, err := decodePairedProcessChunk(bad); !errors.Is(err, errCorrupt) {
			t.Fatal(fmt.Sprint("malformed chunk ", name), err)
		}
	}
}

func TestNativePairedGraphSnapshotOwnersRefuseConstructorlessAndMismatchedEvents(t *testing.T) {
	for _, input := range []struct {
		owners *processOwners
		event  allocationEvent
	}{
		{nil, allocationEvent{packets: []replica.Packet{{Snapshot: true}}}},
		{new(processOwners), allocationEvent{packets: []replica.Packet{{Snapshot: true}}}},
		{new(processOwners), allocationEvent{packets: []replica.Packet{{Snapshot: true}}, snapshotSends: []*replica.SnapshotSend{nil}}},
	} {
		if handles, err := pairedSnapshotHandles(input.owners, input.event); len(handles) != 0 || !errors.Is(err, errInvalid) {
			t.Fatal("snapshot packet imported owner authority", handles, err)
		}
	}
	if handles, err := pairedSnapshotHandles(nil, allocationEvent{}); handles != nil || err != nil {
		t.Fatal("legacy ordinary event changed", handles, err)
	}
	if _, _, err := pairedSnapshotProcessEvent(t.Context(), nil, new(processOwners), processRequest{op: processOffers}); !errors.Is(err, errInvalid) {
		t.Fatal("nil host", err)
	}
	if _, audited := any((*declaredMaterializer)(nil)).(replica.ApplicationSnapshotAuditor); audited {
		t.Fatal("paired source machine now requires real Begin/Continue/Finish audit fixture; plain activation must refuse")
	}
}
