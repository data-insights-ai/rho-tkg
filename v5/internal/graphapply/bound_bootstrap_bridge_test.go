package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Actual three serialized Drivers and bounded public packet delivery. No temp
// seed DB, direct graph Install/Persist, caller Ready, or quorum boolean.
func TestBoundBootstrapBridgeRealInitHistoryAndReopen(t *testing.T) {
	checkErr := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	n := codecNamespace()
	var stores [3]*raftlog.Store
	var machines [3]*materializer
	var drivers [3]*replica.Driver
	var configs [3]raftlog.Config
	for j := range 3 {
		id := uint64(j + 1)
		p := raftlog.DefaultApplicationPolicy(id)
		cfg := raftlog.Config{Dir: "bridge", FS: vfs.NewMem(), Create: true, Application: p, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(n.graph), Partition: n.partition, Group: [16]byte{9}}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2000000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 4096}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: SemanticContractID()}
		s, e := raftlog.Open(cfg)
		checkErr(e)
		stores[j] = s
		configs[j] = cfg
		t.Cleanup(func() {
			if e := s.Close(); e != nil {
				t.Error(e)
			}
		})
		if e := graphstore.BootstrapBoundSinglePartition(s, s.ApplicationBinding(), 3, [3]uint64{1, 2, 3}); e != nil {
			t.Fatal(e)
		}
		m, e := newMaterializer(s, n, 3, defaultMaterializerLimits())
		checkErr(e)
		machines[j] = m
		if read, e := openGraphRead(t.Context(), m, 1); !errors.Is(e, errNotInitialized) || read != nil {
			t.Fatal("seed falsely ready", e)
		}
		wire, e := encodeGraphRequest(graphCodecRequest(t), m.limits)
		checkErr(e)
		if _, e := m.Stage(replica.Entry{Generation: s.ApplicationGeneration(), Index: 2, Term: 2, Data: wire}, s.ApplicationBudget()); !errors.Is(e, errNotInitialized) {
			t.Fatal("mutation before co-init", e)
		}
		d, e := replica.Open(replica.Config{ID: id, Store: s, ApplicationMachine: m, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		checkErr(e)
		drivers[j] = d
		t.Cleanup(func() {
			if e := d.Close(); e != nil {
				t.Error(e)
			}
		})
	}
	reads := make([]replica.ReadResult, 0, 16)
	pump := func(out replica.Output) {
		t.Helper()
		queue := make([]replica.Packet, 0, 128)
		enqueue := func(next replica.Output) {
			t.Helper()
			n := 64 * (cap(queue) + cap(next.Packets))
			for _, p := range queue {
				n += cap(p.Payload)
			}
			for _, p := range next.Packets {
				n += cap(p.Payload)
			}
			if len(next.Packets) > cap(queue)-len(queue) || n > 8<<20 || len(next.Reads) > cap(reads)-len(reads) {
				t.Fatal("aggregate queue/read capacity exhausted")
			}
			queue = append(queue, next.Packets...)
			reads = append(reads, next.Reads...)
		}
		enqueue(out)
		delivered := 0
		for len(queue) > 0 {
			if delivered >= 512 || len(queue) > 128 {
				t.Fatal("bounded packet schedule exhausted")
			}
			delivered++
			packet := queue[0]
			copy(queue, queue[1:])
			queue[len(queue)-1] = replica.Packet{}
			queue = queue[:len(queue)-1]
			if packet.Snapshot || packet.To < 1 || packet.To > 3 || len(packet.Payload) > 4<<20 || cap(packet.Payload) > 4<<20 {
				t.Fatal("unexpected packet/owner")
			}
			next, e := drivers[packet.To-1].Step(packet)
			checkErr(e)
			enqueue(next)
		}
	}
	submit := func(wire []byte) outcome {
		t.Helper()
		out, e := drivers[0].Propose(wire)
		checkErr(e)
		pump(out)
		ident, _, e := commandIdentity(wire)
		checkErr(e)
		var original outcome
		for j, s := range stores {
			v, e := s.ApplicationView(drivers[j].Applied())
			checkErr(e)
			row, found, e := v.Get(t.Context(), outcomeKey(ident), 4096)
			e = errors.Join(e, v.Close())
			if e != nil || !found {
				t.Fatal("no installed original outcome", e)
			}
			o, e := decodeAnyOutcome(row.Value, n)
			if e != nil || o.reason != reasonNone || o.disposition != applied {
				t.Fatal(o, e)
			}
			if j == 0 {
				original = o
			} else if o != original {
				t.Fatal("replica outcome disagreement")
			}
		}
		return original
	}
	out, e := drivers[0].Campaign()
	checkErr(e)
	pump(out)
	init := graphInit(t)
	init.schemas = []graphstate.PropertyDefinition{{Name: "answer", Owner: graphstate.Node, Type: graphstate.ScalarScope, Cardinality: graphstate.ScalarCardinality}}
	wire, e := encodeGraphRequest(init, defaultMaterializerLimits())
	checkErr(e)
	initialized := submit(wire)
	const legacy = "47434401010000000000000000000000000000000000000000000007010000000000000001000000000000000100000000000000020000000100000006616e737765720106010000000000000000000000000000000000000000005d7302d08ff714058bdbba2824bb29c2e9d6fae606c2729558066119c817d3d1"
	want, e := hex.DecodeString(legacy)
	checkErr(e)
	for j, s := range stores {
		cdc, e := s.ApplicationRecord(t.Context(), initialized.index, false, 1<<20)
		checkErr(e)
		logical, e := decodeChangeEnvelope(cdc, initialized, machines[j].limits)
		checkErr(e)
		actual, e := encodeGraphChanges(logical, machines[j].limits)
		t.Logf("replica=%d GCD1 actualLen=%d goldenLen=%d actualSHA=%x goldenSHA=%x equal=%t", j+1, len(actual), len(want), sha256.Sum256(actual), sha256.Sum256(want), bytes.Equal(actual, want))
		if e != nil || !bytes.Equal(actual, want) {
			t.Fatal("legacy GCD1 bytes changed", e)
		}
		read, e := openGraphRead(t.Context(), machines[j], initialized.index)
		checkErr(e)
		view, e := read.ReadView()
		checkErr(e)
		schema, e := view.Property(t.Context(), graphstate.Node, "answer")
		if e != nil || !schema.Found || schema.Record != init.schemas[0] {
			t.Fatal(schema, e)
		}
		checkErr(read.Close())
		u, e := s.ApplicationUsage()
		if e != nil || u.Views != 0 || u.ViewBytes != 0 {
			t.Fatal(u, e)
		}
	}
	controls := codecRequests(t)
	for _, r := range []request{controls[2], controls[3]} {
		if r.kind == reserveGrant {
			r.sequence = 1
			r.count = 16
		}
		b, e := encodeRequest(r, defaultLimits())
		checkErr(e)
		submit(b)
	}
	create := graphCodecRequest(t)
	wire, e = encodeGraphRequest(create, defaultMaterializerLimits())
	checkErr(e)
	created := submit(wire)
	change := create
	change.id = requestID{30}
	change.revision, e = state.NewRevision(9, 7)
	checkErr(e)
	at, e := temporal.RationalPosition(create.operations[0].Scope.Axis(), temporal.RationalInt64(1))
	checkErr(e)
	point, e := temporal.Point(at)
	checkErr(e)
	change.operations = []graphstate.Operation{create.operations[1]}
	change.operations[0].Value = graphstate.ScopeValue(point)
	change.operations[0].ValueID = 14
	change.claims = []freshBinding{{role: valueBinding, id: 14, grant: create.claims[0].grant}}
	wire, e = encodeGraphRequest(change, defaultMaterializerLimits())
	checkErr(e)
	changed := submit(wire)
	out, e = drivers[0].ReadIndex([]byte("bootstrap-bridge-new-barrier"))
	checkErr(e)
	pump(out)
	if len(reads) != 1 || reads[0].Index < changed.index || !bytes.Equal(reads[0].Context, []byte("bootstrap-bridge-new-barrier")) {
		t.Fatal("actual applied barrier missing", reads)
	}
	check := func(s *raftlog.Store, m *materializer, index uint64, want temporal.Scope) {
		t.Helper()
		read, e := openGraphRead(t.Context(), m, index)
		checkErr(e)
		view, e := read.ReadView()
		checkErr(e)
		zero, e := temporal.RationalPosition(at.Axis(), temporal.RationalInt64(0))
		checkErr(e)
		p, e := graphstate.Project(t.Context(), view, 11, zero, graphstate.Declared, m.limits.graph.Planner)
		if e != nil || !p.Exists || !p.Active || p.Life != 12 || len(p.Properties) != 1 || p.Properties[0].Name != "answer" {
			t.Fatal(p, e)
		}
		scope, ok := p.Properties[0].Scalar.Scope()
		equal, e := scope.SameSupport(want, temporal.DefaultLimits())
		if !ok || e != nil || !equal || scope.Kind() != want.Kind() {
			t.Fatal("current substituted for historical value", scope, want, e)
		}
		phantom, e := view.Entity(t.Context(), 999)
		if e != nil || phantom.Found {
			t.Fatal("phantom", phantom, e)
		}
		checkErr(read.Close())
		u, e := s.ApplicationUsage()
		if e != nil || u.Views != 0 || u.ViewBytes != 0 {
			t.Fatal(u, e)
		}
	}
	originalAll, e := temporal.All(at.Axis())
	checkErr(e)
	for j, s := range stores {
		check(s, machines[j], created.index, originalAll)
		check(s, machines[j], changed.index, point)
	}
	for _, d := range drivers {
		checkErr(d.Close())
	}
	cfg := configs[0]
	cfg.Create = false
	s, e := raftlog.Open(cfg)
	checkErr(e)
	defer func() {
		if e := s.Close(); e != nil {
			t.Error(e)
		}
	}()
	m, e := newMaterializer(s, n, 3, defaultMaterializerLimits())
	checkErr(e)
	check(s, m, created.index, originalAll)
	check(s, m, changed.index, point)
}
