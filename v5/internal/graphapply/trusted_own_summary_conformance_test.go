package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestTrustedWriterWideSummaryAndRetainedRoot(t *testing.T) {
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	own := func(owner interface{ Close() error }) {
		t.Helper()
		t.Cleanup(func() {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	n := codecNamespace()
	var stores [3]*raftlog.Store
	var machines [3]*materializer
	var drivers [3]*replica.Driver
	for j := range 3 {
		id := uint64(j + 1)
		p := raftlog.DefaultApplicationPolicy(id)
		cfg := raftlog.Config{Dir: "summary", FS: vfs.NewMem(), Create: true, Application: p,
			Transfer:    raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(n.graph), Partition: n.partition, Group: [16]byte{91}}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()},
			Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2000000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 4096},
			Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: SemanticContractID()}
		s, err := raftlog.Open(cfg)
		check(err)
		stores[j] = s
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		check(graphstore.BootstrapBoundSinglePartition(s, s.ApplicationBinding(), 3, [3]uint64{1, 2, 3}))
		m, err := newMaterializer(s, n, 3, defaultMaterializerLimits())
		check(err)
		machines[j] = m
		d, err := replica.Open(replica.Config{ID: id, Store: s, ApplicationMachine: m, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		check(err)
		drivers[j] = d
		t.Cleanup(func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	pump := func(first replica.Output, minimum uint64) uint64 {
		t.Helper()
		queue := make([]replica.Packet, 0, 256)
		enqueue := func(out replica.Output) {
			t.Helper()
			if len(out.SnapshotSends) != 0 || len(out.Reads) != 0 || len(out.Packets) > cap(queue)-len(queue) {
				t.Fatal("unexpected owners or queue capacity")
			}
			size := 64 * (cap(queue) + cap(out.Packets))
			for _, p := range queue {
				size += cap(p.Payload)
			}
			for _, p := range out.Packets {
				size += cap(p.Payload)
			}
			if size > 8<<20 {
				t.Fatal("aggregate queue byte cap")
			}
			queue = append(queue, out.Packets...)
		}
		enqueue(first)
		delivered := 0
		for tick := 0; tick <= 20; tick++ {
			for len(queue) > 0 {
				if delivered >= 1000 {
					t.Fatal("phase delivery cap")
				}
				p := queue[0]
				copy(queue, queue[1:])
				queue[len(queue)-1] = replica.Packet{}
				queue = queue[:len(queue)-1]
				if p.Snapshot || p.From < 1 || p.From > 3 || p.To < 1 || p.To > 3 || cap(p.Payload) > 4<<20 {
					t.Fatal("invalid public packet")
				}
				delivered++
				out, err := drivers[p.To-1].Step(p)
				check(err)
				enqueue(out)
			}
			index := drivers[0].Applied()
			if index >= minimum && drivers[1].Applied() == index && drivers[2].Applied() == index {
				return index
			}
			if tick == 20 {
				t.Fatal("phase tick cap")
			}
			for _, d := range drivers {
				out, err := d.Tick()
				check(err)
				enqueue(out)
			}
		}
		t.Fatal("unreachable incomplete phase")
		return 0
	}
	out, err := drivers[0].Campaign()
	check(err)
	index := pump(out, 2)
	submit := func(wire []byte) {
		t.Helper()
		out, err := drivers[0].Propose(wire)
		check(err)
		index = pump(out, index+1)
		ident, _, err := commandIdentity(wire)
		check(err)
		var image []byte
		for j, s := range stores {
			actual, raw, err := s.Checkpoint()
			check(err)
			if actual != index || drivers[j].Applied() != index {
				t.Fatal("installed index disagreement")
			}
			if j == 0 {
				image = bytes.Clone(raw)
			} else if !bytes.Equal(image, raw) {
				t.Fatal("installed root disagreement")
			}
			view, err := s.ApplicationView(index)
			check(err)
			own(view)
			row, found, err := view.Get(t.Context(), outcomeKey(ident), 4096)
			check(err)
			check(view.Close())
			if !found {
				t.Fatal("no original installed decision")
			}
			decision, err := decodeAnyOutcome(row.Value, n)
			check(err)
			if decision.kind != ident.kind || decision.identity != ident.identity() || decision.index != index || decision.reason != reasonNone || decision.disposition != applied || decision.hash != sha256.Sum256(wire) {
				t.Fatal("command did not apply", decision)
			}
			if ident.kind == reserveGrant {
				sequence, first, last := uint64(1), uint64(1), uint64(16)
				switch ident.identity() {
				case [16]byte{4}:
				case [16]byte{5}:
					sequence, first, last = 2, 17, 32
				default:
					t.Fatal("unexpected fixture grant identity")
				}
				g := decision.grant
				if decision.grantIndex != index || g.Request.Session != codecSession() || g.Request.Sequence != sequence || g.Request.Count != 16 || g.Reservation.First != first || g.Reservation.Last != last {
					t.Fatal("actual original grant range", j, g, first, last)
				}
			}
		}
	}
	init := graphInit(t)
	init.schemas = nil
	wire, err := encodeGraphRequest(init, defaultMaterializerLimits())
	check(err)
	submit(wire)
	controls := codecRequests(t)
	controls[3].sequence = 1
	controls[3].count = 16
	for _, control := range controls[2:4] {
		wire, err = encodeRequest(control, defaultLimits())
		check(err)
		submit(wire)
	}
	second := controls[3]
	second.id, second.sequence = requestID{5}, 2
	wire, err = encodeRequest(second, defaultLimits())
	check(err)
	submit(wire)
	axis, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{91}, Profile: temporal.ProfileLexicographicQN, Version: 1, Reference: "wide-summary", CanonicalUnit: "step"}, temporal.DefaultLimits())
	check(err)
	m := new(big.Int).Lsh(big.NewInt(1), 2047)
	integer := func(offset int64) temporal.Integer {
		t.Helper()
		v, err := temporal.ParseInteger(new(big.Int).Add(m, big.NewInt(offset)).String(), temporal.DefaultLimits())
		check(err)
		return v
	}
	position := func(offset, micro int64) temporal.Position {
		t.Helper()
		q, err := temporal.Fraction(integer(offset), integer(offset+2), temporal.DefaultLimits())
		check(err)
		p, err := temporal.LexPosition(axis, q, integer(micro))
		check(err)
		return p
	}
	span := func(offset int64) temporal.Scope {
		t.Helper()
		lo, err := temporal.FiniteBound(position(offset, 0), true)
		check(err)
		hi, err := temporal.FiniteBound(position(offset, 2), false)
		check(err)
		scope, err := temporal.Span(axis, lo, hi, temporal.DefaultLimits())
		check(err)
		return scope
	}
	all, err := temporal.All(axis)
	check(err)
	revision, err := state.NewRevision(100, 1)
	check(err)
	request := graphRequest{ns: n, kind: graphOperations, id: requestID{91}, revision: revision,
		operations: []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 11, Life: 12, Scope: all}, {Kind: graphstate.CreateNode, Owner: 13, Life: 14, Scope: all},
			{Kind: graphstate.CreateRelationship, Owner: 15, Life: 16, Scope: span(1), Record: graphstate.EntityRecord{Type: "R", Source: 11, Target: 13, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 12, TargetLife: 14}},
			{Kind: graphstate.CreateRelationship, Owner: 17, Life: 18, Scope: span(5), Record: graphstate.EntityRecord{Type: "R", Source: 11, Target: 13, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 12, TargetLife: 14}}}}
	grant := grantReference{session: codecSession(), sequence: 1}
	for _, pair := range [][2]uint64{{11, 12}, {13, 14}, {15, 16}, {17, 18}} {
		claimGrant := grant
		if pair[0] == 17 {
			claimGrant.sequence = 2
		}
		request.claims = append(request.claims, freshBinding{role: entityBinding, id: pair[0], grant: claimGrant}, freshBinding{role: lifeBinding, owner: graphstate.EntityID(pair[0]), id: pair[1], grant: claimGrant})
	}
	wire, err = encodeGraphRequest(request, defaultMaterializerLimits())
	check(err)
	submit(wire)
	oldIndex := index
	held, err := openGraphRead(t.Context(), machines[2], oldIndex)
	check(err)
	own(held)
	defer func() { check(held.Close()) }()
	oldInterface, err := held.ReadView()
	check(err)
	old, ok := oldInterface.(*graphstore.ReadView)
	if !ok {
		t.Fatal("not the actual Full reader")
	}
	rawView, err := stores[2].ApplicationView(oldIndex)
	check(err)
	own(rawView)
	defer func() { check(rawView.Close()) }()
	oldRoot, err := rawView.Root()
	check(err)
	if oldRoot.Index != oldIndex || oldRoot.ImageHash != sha256.Sum256(oldRoot.Image) {
		t.Fatal("retained root binding")
	}
	descriptorKey := append([]byte{0x11}, n.graph[:]...)
	descriptorKey = binary.BigEndian.AppendUint64(descriptorKey, n.partition)
	descriptor, found, err := rawView.Get(t.Context(), descriptorKey, 8192)
	check(err)
	if !found || len(descriptor.Value) != 216 || !bytes.Equal(descriptor.Value[:4], []byte{'G', 'C', 1, 0x11}) || descriptor.Value[29] != 3 || descriptor.Value[161] != 1 || binary.BigEndian.Uint64(descriptor.Value[176:184]) != 4 {
		t.Fatal("not writer-produced format3 two-level/four-row root")
	}
	ownID := binary.BigEndian.Uint64(descriptor.Value[168:176])
	pageKey := append([]byte{0x15}, n.graph[:]...)
	pageKey = binary.BigEndian.AppendUint64(pageKey, n.partition)
	pageKey = binary.BigEndian.AppendUint64(pageKey, ownID)
	page, found, err := rawView.Get(t.Context(), pageKey, 8192)
	check(err)
	if !found || len(page.Value) < 43 || !bytes.Equal(page.Value[:28], append([]byte{'G', 'C', 1, 0x15}, descriptorKey[1:]...)) || binary.BigEndian.Uint64(page.Value[28:36]) != ownID || page.Value[37] != 1 || binary.BigEndian.Uint16(page.Value[38:40]) != 2 {
		t.Fatal("actual two-child root missing")
	}
	digest := sha256.Sum256(append([]byte("rho-tkg:own-presence-page:v1\x00"), page.Value...))
	if !bytes.Equal(digest[:], descriptor.Value[184:216]) {
		t.Fatal("actual root page hash not descriptor-bound")
	}
	probes := []temporal.Position{position(1, 1), position(5, 1), position(7, 1)}
	inspect := func(view *graphstore.ReadView, version uint64, corrected bool) {
		t.Helper()
		for p, at := range probes {
			var cursor graphstate.Cursor
			var got []graphstore.IncidentAtCandidate
			done := false
			for range 8 {
				result, err := view.IncidentAt(t.Context(), graphstore.IncidentAtQuery{Endpoint: 11, Life: 12, At: at, Mode: graphstate.LifeBound, Direction: graphstore.IncidentSource, Type: "R", Visible: graphstate.Declared}, cursor, graphstate.ReadBudget{Rows: 8, Bytes: 4096})
				check(err)
				if result.View != view.Identity() || result.Version != graphstate.ReadVersion(version) {
					t.Fatal("query retained identity/version")
				}
				got = append(got, result.Candidates...)
				if len(got) > 1 {
					t.Fatal("unexpected duplicate/candidate")
				}
				if result.Complete {
					if result.Next != 0 {
						t.Fatal("complete cursor")
					}
					done = true
					break
				}
				if result.Next == 0 || result.Next == cursor {
					t.Fatal("nonadvancing cursor")
				}
				cursor = result.Next
			}
			if !done {
				t.Fatal("query page cap")
			}
			var want []graphstore.IncidentAtCandidate
			if p == 0 && !corrected {
				want = []graphstore.IncidentAtCandidate{{Relationship: 15, Life: 16, Roles: graphstore.IncidentSource}}
			}
			if p == 1 {
				want = []graphstore.IncidentAtCandidate{{Relationship: 17, Life: 18, Roles: graphstore.IncidentSource}}
			}
			if len(got) != len(want) || len(got) > 0 && got[0] != want[0] {
				t.Fatal("literal exact answer", p, corrected, got, want)
			}
		}
	}
	for j := range 3 {
		if j == 2 {
			inspect(old, oldIndex, false)
			continue
		}
		current, err := openGraphRead(t.Context(), machines[j], oldIndex)
		check(err)
		own(current)
		read, err := current.ReadView()
		check(err)
		view, ok := read.(*graphstore.ReadView)
		if !ok {
			t.Fatal("pre-correction Full reader")
		}
		inspect(view, oldIndex, false)
		check(current.Close())
	}
	change := request
	change.id = requestID{92}
	change.claims = nil
	change.revision, err = state.NewRevision(101, 1)
	check(err)
	change.operations = []graphstate.Operation{{Kind: graphstate.Close, Owner: 15, Life: 16, Scope: span(1)}}
	wire, err = encodeGraphRequest(change, defaultMaterializerLimits())
	check(err)
	submit(wire)
	for j, s := range stores {
		current, err := openGraphRead(t.Context(), machines[j], index)
		check(err)
		own(current)
		read, err := current.ReadView()
		check(err)
		view, ok := read.(*graphstore.ReadView)
		if !ok {
			t.Fatal("corrected Full reader")
		}
		inspect(view, index, true)
		check(current.Close())
		backend, err := s.ApplicationView(index)
		check(err)
		own(backend)
		root, err := backend.Root()
		check(err)
		d, found, err := backend.Get(t.Context(), descriptorKey, 8192)
		check(err)
		check(backend.Close())
		if !found || len(d.Value) != 216 || d.Value[161] != 0 || binary.BigEndian.Uint64(d.Value[176:184]) != 2 || bytes.Equal(root.Image, oldRoot.Image) {
			t.Fatal("correction did not collapse actual root")
		}
		before, err := graphstore.DecodeRoot(oldRoot.Image)
		check(err)
		after, err := graphstore.DecodeRoot(root.Image)
		check(err)
		if before.SemanticEpoch() == after.SemanticEpoch() || before.EffectDigest() == after.EffectDigest() {
			t.Fatal("correction lost logical effect")
		}
	}
	inspect(old, oldIndex, false)
	retained, err := rawView.Root()
	check(err)
	if retained.Index != oldRoot.Index || retained.ImageHash != oldRoot.ImageHash || !bytes.Equal(retained.Image, oldRoot.Image) {
		t.Fatal("held immutable root changed")
	}
}
