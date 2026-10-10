package graphapply

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func registeredSnapshotOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

type registeredSnapshotTransport struct {
	t                 *testing.T
	g                 *registeredStageTestGroup
	isolate           bool
	queue             []replica.Packet
	packet, active    replica.Packet
	send              *replica.SnapshotSend
	initial, previous replica.Output
}

func (n *registeredSnapshotTransport) enqueue(out replica.Output) {
	n.t.Helper()
	if len(out.Reads) != 0 || len(out.Packets) > cap(n.queue)-len(n.queue) {
		n.t.Fatal("bounded transport shape")
	}
	owned := 64*cap(n.queue) + 64 + 3*128 + cap(n.active.Payload) + cap(n.packet.Payload)
	for _, packets := range [][]replica.Packet{n.queue, n.initial.Packets, n.previous.Packets, out.Packets} {
		owned += 64 * cap(packets)
		for _, p := range packets {
			if cap(p.Payload) > 1<<20 {
				n.t.Fatal("packet allowance")
			}
			owned += cap(p.Payload)
		}
	}
	for _, offers := range [][]*replica.SnapshotSend{n.initial.SnapshotSends, n.previous.SnapshotSends, out.SnapshotSends} {
		owned += 8 * cap(offers)
	}
	if owned > 2<<20 {
		n.t.Fatal("simultaneous transport retention allowance")
	}
	at := 0
	for _, p := range out.Packets {
		if p.From < 1 || p.From > 3 || p.To < 1 || p.To > 3 {
			n.t.Fatal("packet scope")
		}
		if p.Snapshot {
			if n.isolate || n.send != nil || at >= len(out.SnapshotSends) || p.From != 1 || p.To != 3 {
				n.t.Fatal("one real owned snapshot to receiver3 required")
			}
			n.packet, n.send = p, out.SnapshotSends[at]
			at++
			continue
		}
		if n.isolate && (p.From == 3 || p.To == 3) {
			continue
		}
		n.queue = append(n.queue, p)
	}
	if at != len(out.SnapshotSends) {
		n.t.Fatal("real packet/offer pairing")
	}
}

func (n *registeredSnapshotTransport) run(out replica.Output) {
	n.t.Helper()
	n.initial, n.previous = out, out
	n.active = replica.Packet{}
	n.enqueue(out)
	for delivered := 0; len(n.queue) != 0; delivered++ {
		if delivered >= 1024 {
			n.t.Fatal("per-event delivery allowance")
		}
		n.active = n.queue[0]
		copy(n.queue, n.queue[1:])
		n.queue[len(n.queue)-1] = replica.Packet{}
		n.queue = n.queue[:len(n.queue)-1]
		next, err := n.g.drivers[n.active.To-1].Step(n.active)
		registeredSnapshotOK(n.t, err)
		n.enqueue(next)
		n.previous = next
	}
	n.active = replica.Packet{}
	n.initial, n.previous = replica.Output{}, replica.Output{}
}

func TestRegisteredGenesisAS4(t *testing.T) {
	cohort := os.Getenv("RHO_REGISTERED_GENESIS_SNAPSHOT_COHORT")
	var graph, group byte
	switch cohort {
	case "":
		t.Skip("controlled fresh-cohort lane: set RHO_REGISTERED_GENESIS_SNAPSHOT_COHORT=normal or race")
	case "normal":
		graph, group = 73, 74
	case "race":
		graph, group = 75, 76
	default:
		t.Fatalf("invalid controlled snapshot cohort %q", cohort)
	}
	t.Logf("non-skipped controlled snapshot cohort %s Graph%d/Group%d", cohort, graph, group)
	var fixtureSet map[string]json.RawMessage
	var fixture struct {
		Bytes map[string]struct {
			Hex string `json:"hex"`
		} `json:"bytes"`
		Page struct {
			After struct {
				Hex string `json:"hex"`
			} `json:"after_canonical_key"`
		} `json:"single_page_expectation"`
	}
	data, err := os.ReadFile(filepath.Join("testdata", "registered_genesis_snapshot_fixtures.json"))
	registeredSnapshotOK(t, err)
	registeredSnapshotOK(t, json.Unmarshal(data, &fixtureSet))
	selected, found := fixtureSet[cohort]
	if !found {
		t.Fatal("missing exact independent fixture")
	}
	registeredSnapshotOK(t, json.Unmarshal(selected, &fixture))
	literal := func(name string) []byte {
		t.Helper()
		value, found := fixture.Bytes[name]
		if !found {
			t.Fatal("missing independent literal", name)
		}
		return registeredStageHex(t, value.Hex)
	}
	image, command, key, origin := literal("GR2_image"), literal("GJQ255"), literal("HKS47"), literal("HCR378")
	g := newRegisteredStageTestGroup(t, graph, group)
	n := &registeredSnapshotTransport{t: t, g: g, queue: make([]replica.Packet, 0, 128), isolate: true}
	sourceView, err := g.stores[2].ApplicationView(1)
	registeredSnapshotOK(t, err)
	sourceRoot, err := sourceView.RootBounded(t.Context(), 140)
	registeredSnapshotOK(t, err)
	registeredSnapshotOK(t, sourceView.Close())
	term, err := g.stores[2].Term(1)
	registeredSnapshotOK(t, err)
	if sourceRoot.Index != 1 || term != 1 || !bytes.Equal(sourceRoot.Image, image) || !bytes.Equal(sourceRoot.ImageHash[:], literal("image_hash")) {
		t.Fatal("actual initialized source1/term1")
	}
	out, err := g.drivers[0].Propose(command)
	registeredSnapshotOK(t, err)
	n.run(out)
	for j := range 2 {
		cp, _, err := g.stores[j].Checkpoint()
		registeredSnapshotOK(t, err)
		actual, err := g.stores[j].Term(3)
		registeredSnapshotOK(t, err)
		hard, _, err := g.stores[j].InitialState()
		registeredSnapshotOK(t, err)
		if cp != 3 || g.drivers[j].Applied() != 3 || actual != 2 || hard.GetCommit() != 3 {
			t.Fatal("actual quorum Genesis3/term2")
		}
		g.retainedOrigin(j, key, origin)
	}
	receiverAt2 := func() {
		t.Helper()
		s := g.stores[2]
		cp, root, err := s.Checkpoint()
		registeredSnapshotOK(t, err)
		last, err := s.LastIndex()
		registeredSnapshotOK(t, err)
		hard, _, err := s.InitialState()
		registeredSnapshotOK(t, err)
		if cp != 2 || last != 2 || hard.GetCommit() != 2 || g.drivers[2].Applied() != 2 || s.ApplicationGeneration() != 1 || !bytes.Equal(root, image) {
			t.Fatal("isolated/prepared receiver changed active state")
		}
		v, err := s.ApplicationView(2)
		registeredSnapshotOK(t, err)
		_, found, _, err := v.GetControl(t.Context(), key, raftlog.ReadBudget{Rows: 2, Bytes: 4096})
		registeredSnapshotOK(t, err)
		registeredSnapshotOK(t, v.Close())
		if found {
			t.Fatal("receiver invented pre-activation Origin")
		}
		usage, err := s.ApplicationControlUsage()
		registeredSnapshotOK(t, err)
		if usage.Bytes != 0 || usage.Records != 0 || usage.TotalBytes != 550 || usage.TotalRecords != 6 {
			t.Fatal("receiver2 active ledger")
		}
	}
	receiverAt2()
	registeredSnapshotOK(t, g.stores[0].PublishSnapshot())
	first, err := g.stores[0].FirstIndex()
	registeredSnapshotOK(t, err)
	if first != 4 {
		t.Fatal("snapshot path not forced by actual prefix publication")
	}
	n.isolate = false
	var sealed *replica.SnapshotSend
	for range 24 {
		out, err := g.drivers[0].Tick()
		registeredSnapshotOK(t, err)
		n.run(out)
		if n.send != nil {
			break
		}
		offers, err := g.drivers[0].SnapshotSends()
		registeredSnapshotOK(t, err)
		for _, offer := range offers {
			if offer == sealed {
				continue
			}
			ready := false
			for page := 0; !ready && page < 32; page++ {
				ready, err = offer.Build(t.Context(), raftlog.ReadBudget{Rows: 16, Bytes: 4096})
				registeredSnapshotOK(t, err)
			}
			if !ready {
				t.Fatal("bounded manifest build incomplete")
			}
			registeredSnapshotOK(t, g.drivers[0].SealSnapshotSend(offer))
			sealed = offer
		}
	}
	if n.send == nil || n.send != sealed {
		t.Fatal("actual bound MsgSnap/owned verified offer absent")
	}
	manifest, err := n.send.Manifest()
	registeredSnapshotOK(t, err)
	wire, err := raftlog.EncodeApplicationSnapshotManifest(manifest)
	registeredSnapshotOK(t, err)
	if !bytes.Equal(wire, literal("AS4_manifest")) || manifest.Version != 4 || manifest.Index != 3 || manifest.Term != 2 || manifest.ConfState.AutoLeave != nil {
		t.Fatal("actual canonical AS4/full conf presence")
	}
	decoded, err := replica.DecodeApplicationPacket(g.stores[2].ApplicationBinding(), n.packet, 1<<20)
	registeredSnapshotOK(t, err)
	var message pb.Message
	registeredSnapshotOK(t, proto.Unmarshal(decoded.Payload, &message))
	if message.GetType() != pb.MsgSnap || message.GetFrom() != 1 || message.GetTo() != 3 || message.GetTerm() != 2 || message.GetSnapshot().GetMetadata().GetIndex() != 3 || message.GetSnapshot().GetMetadata().GetTerm() != 2 || !bytes.Equal(message.GetSnapshot().Data, literal("AD3_descriptor")) {
		t.Fatal("AD3 must come from REAL outgoing snapshot packet")
	}
	imported, err := g.stores[2].BeginApplicationImport(t.Context(), manifest)
	registeredSnapshotOK(t, err)
	defer func() { registeredSnapshotOK(t, imported.Abort()) }()
	final := false
	for page := 0; !final && page < 32; page++ {
		chunk, err := n.send.Next(t.Context(), raftlog.ReadBudget{Rows: 16, Bytes: 4096})
		registeredSnapshotOK(t, err)
		if page != 0 || chunk.Sequence != 0 || chunk.Version != 4 || !chunk.Final || chunk.Visited != 10 || chunk.VisitedBytes != 1560 || !bytes.Equal(chunk.Data, literal("canonical_stream")) || chunk.CutID != manifest.CutID || !bytes.Equal(chunk.ManifestID[:], literal("ManifestID")) || !bytes.Equal(chunk.After, registeredStageHex(t, fixture.Page.After.Hex)) {
			t.Fatal("full independent canonical stream/progress")
		}
		registeredSnapshotOK(t, imported.Append(t.Context(), chunk))
		final = chunk.Final
	}
	if !final {
		t.Fatal("bounded stream incomplete")
	}
	registeredSnapshotOK(t, imported.Verify(t.Context()))
	status, err := imported.Status()
	registeredSnapshotOK(t, err)
	if !status.Final || !status.Verified || !bytes.Equal(status.ManifestID[:], literal("ManifestID")) {
		t.Fatal("verified dormant manifest identity")
	}
	prepared, err := imported.Prepare()
	registeredSnapshotOK(t, err)
	defer func() { registeredSnapshotOK(t, prepared.Close()) }()
	receiverAt2()
	activated, err := g.drivers[2].StepApplicationSnapshot(n.packet, prepared)
	registeredSnapshotOK(t, err)
	if g.stores[2].ApplicationGeneration() != 2 || g.drivers[2].Applied() != 3 {
		t.Fatal("actual Ready must activate generation2/bank1 at3")
	}
	audit, err := g.stores[2].ApplicationActivationAudit()
	registeredSnapshotOK(t, err)
	if audit.Index != 3 || audit.Term != 2 || !bytes.Equal(audit.CutID[:], literal("CutID")) || !bytes.Equal(audit.ManifestID[:], literal("ManifestID")) {
		t.Fatal("actual synced activation audit")
	}
	feedback, err := g.drivers[0].ReportSnapshotSend(n.send, true)
	registeredSnapshotOK(t, err)
	n.send = nil
	n.packet = replica.Packet{}
	n.run(feedback)
	n.run(activated)
	registeredSnapshotOK(t, prepared.Close())
	registeredSnapshotOK(t, imported.Abort())
	receiverState := func(index uint64, wantOutcome []byte) {
		t.Helper()
		s := g.stores[2]
		registeredSnapshotOK(t, s.ScrubApplication(t.Context()))
		for at := uint64(1); at <= index; at++ {
			v, err := s.ApplicationView(at)
			registeredSnapshotOK(t, err)
			root, err := v.RootBounded(t.Context(), 140)
			registeredSnapshotOK(t, err)
			if root.Generation != 2 || root.Index != at || !bytes.Equal(root.Image, image) {
				t.Fatal("full retained root run/source1")
			}
			row, found, _, err := v.GetControl(t.Context(), key, raftlog.ReadBudget{Rows: 2, Bytes: 4096})
			registeredSnapshotOK(t, err)
			if found != (at >= 3) || found && (row.Index != 3 || !bytes.Equal(row.Value, origin)) {
				t.Fatal("retained original control history")
			}
			if at == index {
				empty, err := v.ProveNoApplicationData(t.Context())
				registeredSnapshotOK(t, err)
				if !sameBase(root, empty) {
					t.Fatal("current graph empty proof")
				}
			}
			registeredSnapshotOK(t, v.Close())
			changes, err := s.ApplicationRecord(t.Context(), at, false, 1)
			registeredSnapshotOK(t, err)
			result, err := s.ApplicationRecord(t.Context(), at, true, 145)
			registeredSnapshotOK(t, err)
			var expected []byte
			if at == 3 {
				expected = literal("CreatedJOR145")
			}
			if at == 5 {
				expected = wantOutcome
			}
			if len(changes) != 0 || !bytes.Equal(result, expected) {
				t.Fatal("full change/outcome history")
			}
		}
		g.retainedOrigin(2, key, origin)
		m, err := newRegisteredMaterializer(s, registeredOperationLimits{8 << 20, 8 << 20})
		registeredSnapshotOK(t, err)
		source, err := encodeGenesisSource(m.source, registeredJournalBudget{3292})
		registeredSnapshotOK(t, err)
		if !bytes.Equal(source, literal("source244")) {
			t.Fatal("actual durable original source244")
		}
		recovered, err := s.ApplicationActivationAudit()
		registeredSnapshotOK(t, err)
		if recovered != audit {
			t.Fatal("activation audit changed across tail/reopen")
		}
	}
	cleanOwners := func() {
		t.Helper()
		for j, s := range g.stores {
			usage, err := s.ApplicationTransferUsage()
			registeredSnapshotOK(t, err)
			app, err := s.ApplicationUsage()
			registeredSnapshotOK(t, err)
			offers, err := g.drivers[j].SnapshotSends()
			registeredSnapshotOK(t, err)
			if usage.Exports != 0 || usage.Import || usage.Prepared != 0 || usage.Claims != 0 || usage.Verifier || usage.PinnedLogicalBytes != 0 || app.Views != 0 || len(offers) != 0 {
				t.Fatal("transfer/view owners retained")
			}
		}
	}
	reopen := func(expectedApplied uint64) {
		t.Helper()
		registeredSnapshotOK(t, g.drivers[2].Close())
		g.drivers[2], g.stores[2] = nil, nil
		cfg := g.configs[2]
		cfg.Create = false
		s, err := raftlog.Open(cfg)
		registeredSnapshotOK(t, err)
		g.stores[2] = s
		m, err := newRegisteredMaterializer(s, registeredOperationLimits{8 << 20, 8 << 20})
		registeredSnapshotOK(t, err)
		d, err := replica.Open(replica.Config{ID: 3, Store: s, ApplicationMachine: m, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		registeredSnapshotOK(t, err)
		g.drivers[2] = d
		if s.ApplicationGeneration() != 2 || d.Applied() != expectedApplied {
			t.Fatal("actual reopened activation progress")
		}
	}
	receiverState(3, nil)
	cleanOwners()
	reopen(3)
	receiverState(3, nil)
	cleanOwners()
	for range 20 {
		for _, d := range g.drivers {
			out, err := d.Tick()
			registeredSnapshotOK(t, err)
			if len(out.Reads) != 0 || len(out.SnapshotSends) != 0 {
				t.Fatal("isolated logical Tick shape")
			}
		}
	}
	out, err = g.drivers[1].Campaign()
	registeredSnapshotOK(t, err)
	n.run(out)
	g.roles(4, 3)
	for _, s := range g.stores {
		entries, err := s.Entries(4, 5, uint64(s.Limits().MaxReadBytes))
		registeredSnapshotOK(t, err)
		if len(entries) != 1 || len(entries[0].GetData()) != 0 {
			t.Fatal("actual higher-term empty noop")
		}
	}
	out, err = g.drivers[1].Propose(command)
	registeredSnapshotOK(t, err)
	n.run(out)
	g.roles(5, 3)
	for j, s := range g.stores {
		g.retainedOrigin(j, key, origin)
		result, err := s.ApplicationRecord(t.Context(), 5, true, 145)
		registeredSnapshotOK(t, err)
		usage, err := s.ApplicationControlUsage()
		registeredSnapshotOK(t, err)
		if !bytes.Equal(result, literal("OriginalE5JOR145")) || usage.TotalBytes != 2175 || usage.TotalRecords != 16 {
			t.Fatal("real E5/term3 must retain original S3/term2")
		}
	}
	receiverState(5, literal("OriginalE5JOR145"))
	cleanOwners()
	reopen(5)
	receiverState(5, literal("OriginalE5JOR145"))
	cleanOwners()
	t.Logf("actual non-skipped AS4 Ready/reopen/replay PASS: cohort=%s generation2/bank1 audit3/term2 original3/term2", cohort)
}
