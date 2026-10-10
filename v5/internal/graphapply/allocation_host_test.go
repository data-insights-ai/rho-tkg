package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"maps"
	"math"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

type allocationTestNetwork struct {
	t       *testing.T
	d       graphstore.OwnershipDeclaration
	hosts   [2][3]*allocationHost
	configs [2][3]raftlog.Config
	pending map[[16]byte]allocationProof
	seq     byte
}

func newAllocationTestNetwork(t *testing.T) *allocationTestNetwork {
	t.Helper()
	d := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{4}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{4}}})
	n := &allocationTestNetwork{t: t, d: d, pending: make(map[[16]byte]allocationProof)}
	for g, partition := range []uint64{3, 8} {
		for j := range 3 {
			id := uint64(j + 1)
			p := raftlog.DefaultApplicationPolicy(id)
			p.MaxImageBytes, p.MaxInstallWrites = 172, 256
			p.RetainedApplicationRecords, p.RetainedApplicationBytes = 8192, 16<<20
			cfg := raftlog.Config{Dir: "allocation", FS: vfs.NewMem(), Create: true, Application: p, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(d.Graph()), Partition: partition, Group: [16]byte{4}}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 32 << 20, MaxRecords: 16384}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 4096}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: declaredSemanticContractID()}
			n.configs[g][j] = cfg
			s, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := graphstore.BootstrapBoundOwnership(s, s.ApplicationBinding(), d, [3]uint64{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
			m, err := newDeclaredMaterializer(s, namespace{graph: idalloc.GraphID(d.Graph()), partition: partition}, d, defaultMaterializerLimits())
			if err != nil {
				t.Fatal(err)
			}
			h, err := openAllocationHost(m, id, replica.Config{Store: s, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
			if err != nil {
				t.Fatal(err)
			}
			n.hosts[g][j] = h
			t.Cleanup(func() {
				if err := h.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		out, err := n.hosts[g][0].Campaign()
		if err != nil {
			t.Fatal(err)
		}
		n.pump(g, out)
	}
	schemas := []graphstate.PropertyDefinition{{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}}
	initial, err := n.hosts[0][0].PrepareInitialization(3, bootstrapAttemptID{1}, schemas, 16, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	n.submit(0, initial)
	genesis := n.read(0, allocationQuery{kind: observeConfiguration})
	second, err := n.hosts[0][0].PrepareInitialization(8, bootstrapAttemptID{2}, schemas, 16, genesis)
	if err != nil {
		t.Fatal(err)
	}
	n.submit(1, second)
	return n
}

func (n *allocationTestNetwork) requestID() requestID {
	n.t.Helper()
	if n.seq == 255 {
		n.t.Fatal("bounded request-ID fixture exhausted")
	}
	n.seq++
	return requestID{n.seq}
}

func (n *allocationTestNetwork) pump(group int, out allocationEvent) []allocationReply {
	n.t.Helper()
	queue := make([]replica.Packet, 0, 128)
	var replies []allocationReply
	enqueue := func(e allocationEvent) {
		n.t.Helper()
		if len(e.snapshotSends) != 0 || len(queue)+len(e.packets) > cap(queue) || len(replies)+len(e.replies) > 16 {
			n.t.Fatal("bounded allocation fixture owners exhausted")
		}
		cost := 64 * cap(queue)
		for _, p := range queue {
			cost += cap(p.Payload)
		}
		for _, p := range e.packets {
			cost += cap(p.Payload)
		}
		if cost > 2<<20 {
			n.t.Fatal("bounded packet bytes exhausted")
		}
		queue = append(queue, e.packets...)
		replies = append(replies, e.replies...)
	}
	enqueue(out)
	for count := 0; len(queue) != 0; count++ {
		if count >= 1024 {
			n.t.Fatal("bounded allocation packet schedule exhausted")
		}
		p := queue[0]
		copy(queue, queue[1:])
		queue[len(queue)-1] = replica.Packet{}
		queue = queue[:len(queue)-1]
		if p.Snapshot || p.To < 1 || p.To > 3 {
			n.t.Fatal("unexpected allocation packet")
		}
		e, err := n.hosts[group][p.To-1].Step(p)
		if err != nil {
			n.t.Fatal(err)
		}
		enqueue(e)
	}
	return replies
}

func (n *allocationTestNetwork) submit(group int, p allocationProposal) outcome {
	n.t.Helper()
	o := n.submitReason(group, p, reasonNone)
	return o
}

func (n *allocationTestNetwork) submitReason(group int, p allocationProposal, expected reason) outcome {
	n.t.Helper()
	e, err := n.hosts[group][0].Submit(p)
	if err != nil {
		n.t.Fatal(err)
	}
	n.pump(group, e)
	var original outcome
	for j, h := range n.hosts[group] {
		index := h.driver.Applied()
		wire, err := h.machine.store.ApplicationRecord(n.t.Context(), index, true, graphOutcomeBytes)
		if err != nil {
			n.t.Fatal(err)
		}
		o, err := decodeDeclaredOutcome(wire, h.machine.ns)
		if err != nil || o.hash != sha256.Sum256(p.wire) || o.kind != commandKind(p.wire[4]) {
			n.t.Fatal(o, err)
		}
		if j == 0 {
			original = o
		} else if o != original {
			n.t.Fatal("replica outcome mismatch")
		}
	}
	if original.reason != expected {
		n.t.Fatal("unexpected protocol rejection", original)
	}
	return original
}

func (n *allocationTestNetwork) read(group int, q allocationQuery) allocationProof {
	n.t.Helper()
	q.nonce = [16]byte(n.requestID())
	e, err := n.hosts[group][0].Read(q)
	if err != nil {
		n.t.Fatal(err)
	}
	replies := n.pump(group, e)
	if len(replies) != 1 || replies[0].id != e.readID || replies[0].question != q || replies[0].err != nil {
		n.t.Fatal("missing matching actual quorum read", replies)
	}
	return replies[0].proof
}

func (n *allocationTestNetwork) activate(group int, recipient [16]byte) *allocationRecipientHandle {
	n.t.Helper()
	current := n.read(group, allocationQuery{kind: observeConfiguration})
	return n.activateFrom(group, recipient, current)
}

func (n *allocationTestNetwork) activateFrom(group int, recipient [16]byte, current allocationProof) *allocationRecipientHandle {
	n.t.Helper()
	home := n.hosts[group][0]
	begin, err := home.BeginRecipient(n.requestID(), recipient, current)
	if err != nil {
		n.t.Fatal(err)
	}
	n.submit(0, begin)
	pending := n.read(0, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: recipient}})
	n.pending[recipient] = pending
	for _, targetGroup := range []int{0, group} {
		if targetGroup == 0 && group == 0 {
			continue
		}
		target := n.hosts[targetGroup][0].machine.ns.partition
		fence, err := n.hosts[0][0].RecipientTransition(n.requestID(), fenceDeclaredRecipient, target, pending)
		if err != nil {
			n.t.Fatal(err)
		}
		n.submit(targetGroup, fence)
	}
	if group == 0 {
		fence, err := n.hosts[0][0].RecipientTransition(n.requestID(), fenceDeclaredRecipient, 3, pending)
		if err != nil {
			n.t.Fatal(err)
		}
		n.submit(0, fence)
	}
	fencedHome := n.read(group, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: recipient}})
	ack, err := home.RecipientTransition(n.requestID(), acknowledgeDeclaredHome, 3, fencedHome)
	if err != nil {
		n.t.Fatal(err)
	}
	n.submit(0, ack)
	fencedSource := n.read(0, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: recipient}})
	active, err := n.hosts[0][0].RecipientTransition(n.requestID(), activateDeclaredRecipient, 3, fencedSource)
	if err != nil {
		n.t.Fatal(err)
	}
	n.submit(0, active)
	sourceActive := n.read(0, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: recipient}})
	publish, err := n.hosts[0][0].RecipientTransition(n.requestID(), publishDeclaredRecipient, home.machine.ns.partition, sourceActive)
	if err != nil {
		n.t.Fatal(err)
	}
	n.submit(group, publish)
	proof := n.read(group, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: recipient}})
	result, err := home.OpenRecipient(proof)
	if err != nil {
		n.t.Fatal(err)
	}
	return result
}

func (n *allocationTestNetwork) deliver(group int, request idalloc.GrantRequest, q allocationQuery) (allocationReply, error) {
	n.t.Helper()
	service := n.read(0, allocationQuery{kind: observeAllocator})
	reserve, err := n.hosts[0][0].AuthorizeReserve(n.requestID(), request, service)
	if err != nil {
		return allocationReply{}, err
	}
	n.submit(0, reserve)
	grant := n.read(0, allocationQuery{kind: observeGrant, session: request.Session, sequence: request.Sequence, count: request.Count})
	install, err := n.hosts[0][0].InstallGrant(n.requestID(), n.hosts[group][0].machine.ns.partition, grant)
	if err != nil {
		return allocationReply{}, err
	}
	n.submit(group, install)
	e, err := n.hosts[group][0].Read(q)
	if err != nil {
		return allocationReply{}, err
	}
	replies := n.pump(group, e)
	if len(replies) != 1 {
		return allocationReply{}, errInvalid
	}
	return replies[0], nil
}

func (n *allocationTestNetwork) acquire(group int, h *allocationRecipientHandle, sequence, count uint64) *idalloc.Cursor {
	n.t.Helper()
	cursor, err := h.Acquire(n.t.Context(), sequence, count, func(ctx context.Context, request idalloc.GrantRequest, q allocationQuery) (allocationReply, error) {
		if err := ctx.Err(); err != nil {
			return allocationReply{}, err
		}
		return n.deliver(group, request, q)
	})
	if err != nil {
		n.t.Fatal(err)
	}
	return cursor
}

func TestDeclaredAllocationTwoGroupsGloballyDisjointCachedBlocks(t *testing.T) {
	n := newAllocationTestNetwork(t)
	one, two := n.activate(0, [16]byte{1}), n.activate(1, [16]byte{2})
	first, second := n.acquire(0, one, 1, 4), n.acquire(1, two, 1, 4)
	issued := make(map[uint64]int)
	for group, c := range []*idalloc.Cursor{first, second} {
		for range 4 {
			id, err := c.Next()
			if err != nil {
				t.Fatal(err)
			}
			if _, found := issued[id]; found {
				t.Fatal("globally overlapping ID", id)
			}
			issued[id] = group
		}
	}
	if len(issued) != 8 {
		t.Fatal(issued)
	}
	for id := uint64(1); id <= 8; id++ {
		group, found := issued[id]
		if !found || group != int((id-1)/4) {
			t.Fatal("independent plain-map range oracle", issued)
		}
	}
	if _, err := one.Acquire(t.Context(), 1, 4, func(context.Context, idalloc.GrantRequest, allocationQuery) (allocationReply, error) {
		t.Fatal("retry recreated delivery")
		return allocationReply{}, nil
	}); !errors.Is(err, idalloc.ErrAlreadyReserved) {
		t.Fatal(err)
	}
	for _, group := range n.hosts {
		for _, h := range group {
			_, image, err := h.machine.store.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			root, err := graphstore.DecodeRoot(image)
			if err != nil || root.SemanticEpoch() != 1 {
				t.Fatal("control advanced graph semantics", root, err)
			}
		}
	}
	// Rho allocation/database state supplies no graph mutation or inference door.
	h := n.hosts[1][0]
	index, image, err := h.machine.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.machine.Stage(replica.Entry{Index: index + 1, Generation: h.machine.store.ApplicationGeneration(), Term: 2, Data: []byte("graph-write-not-admitted")}, h.machine.store.ApplicationBudget())
	if !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	_, after, err := h.machine.store.Checkpoint()
	if err != nil || !bytes.Equal(after, image) {
		t.Fatal("refused graph command changed root", err)
	}
}

func TestDeclaredAllocationLegacyFactoryAndStageRefuseWithoutMutation(t *testing.T) {
	n := newAllocationTestNetwork(t)
	for _, group := range n.hosts {
		for _, h := range group {
			s := h.machine.store
			index, image, err := s.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			usage, err := s.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			transfer, err := s.ApplicationTransferUsage()
			if err != nil {
				t.Fatal(err)
			}
			if m, err := newMaterializer(s, h.machine.ns, h.machine.owner, h.machine.limits); m != nil || !errors.Is(err, replica.ErrInvalid) || !errors.Is(err, errInvalid) {
				t.Fatal("legacy factory admitted declared semantic binding", m, err)
			}
			legacy := materializer{store: s, ns: h.machine.ns, owner: h.machine.owner, limits: h.machine.limits}
			for _, data := range [][]byte{nil, []byte("legacy-command")} {
				b, err := legacy.Stage(replica.Entry{Index: index + 1, Generation: s.ApplicationGeneration(), Term: 2, Data: data}, s.ApplicationBudget())
				if !errors.Is(err, graphstore.ErrTopologyUnsupported) || len(b.Image)+len(b.Outcome)+len(b.Changes)+len(b.Writes) != 0 {
					t.Fatal("legacy Stage admitted GR3", b, err)
				}
			}
			afterIndex, after, err := s.Checkpoint()
			if err != nil || afterIndex != index || !bytes.Equal(image, after) {
				t.Fatal("legacy refusal changed checkpoint", err)
			}
			afterUsage, err := s.ApplicationUsage()
			if err != nil || afterUsage != usage {
				t.Fatal("legacy refusal changed application records", err)
			}
			afterTransfer, err := s.ApplicationTransferUsage()
			if err != nil || afterTransfer != transfer {
				t.Fatal("legacy refusal changed handles", err)
			}
		}
	}
}

func TestDeclaredAllocationLargerRecordsUseExplicitReadAdmission(t *testing.T) {
	n := newAllocationTestNetwork(t)
	handle := n.activate(0, [16]byte{1})
	n.acquire(0, handle, 1, 4)
	h := n.hosts[0][0]
	index := h.driver.Applied()
	view, err := h.machine.store.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	base, err := view.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		name string
		key  []byte
		size int
	}{
		{"recipient", recipientKey(h.machine.ns, handle.session.ID), protocolRecipientMaxBytes},
		{"grant", grantKey(h.machine.ns, handle.session, 1), protocolGrantBytes},
	} {
		t.Run(row.name, func(t *testing.T) {
			l := defaultLimits()
			q := reader{ctx: t.Context(), view: view, ns: h.machine.ns, base: base, limits: l}
			if b, found, err := q.get(row.key); len(b) != 0 || found || !errors.Is(err, errLimit) || !errors.Is(err, raftlog.ErrLimit) {
				t.Fatal("legacy 273-byte admission changed", found, err)
			}
			for _, delta := range []int{-1, 0, 1} {
				q = reader{ctx: t.Context(), view: view, ns: h.machine.ns, base: base, limits: l}
				b, found, err := q.getBounded(row.key, row.size+delta)
				if delta < 0 {
					if len(b) != 0 || found || !errors.Is(err, errLimit) || !errors.Is(err, raftlog.ErrLimit) {
						t.Fatal("below record bound", err)
					}
				} else if err != nil || !found || len(b) != row.size || cap(b) != len(b) {
					t.Fatal("exact-cap record read", found, len(b), cap(b), err)
				}
			}
			q = reader{ctx: t.Context(), view: view, ns: h.machine.ns, base: base, limits: l}
			if _, _, err := q.getBounded(row.key, -1); !errors.Is(err, errInvalid) || q.rows != 0 {
				t.Fatal("invalid admission did work", err)
			}
			if _, _, err := q.getBounded(row.key, (16<<20)+1); !errors.Is(err, errInvalid) || q.rows != 0 {
				t.Fatal("oversized admission did work", err)
			}
		})
	}
}

func TestDeclaredAllocationRetryFencesTransferAndRetainedReopen(t *testing.T) {
	n := newAllocationTestNetwork(t)
	handle := n.activate(1, [16]byte{2})
	var roots [2][3][]byte
	for g, group := range n.hosts {
		for j, h := range group {
			_, image, err := h.machine.store.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			roots[g][j] = image
		}
	}
	service := n.read(0, allocationQuery{kind: observeAllocator})
	request := idalloc.GrantRequest{Graph: n.hosts[0][0].machine.ns.graph, Session: handle.session, Sequence: 9, Count: 4}
	reserve, err := n.hosts[0][0].AuthorizeReserve(n.requestID(), request, service)
	if err != nil {
		t.Fatal(err)
	}
	original := n.submit(0, reserve)
	grant := n.read(0, allocationQuery{kind: observeGrant, session: request.Session, sequence: 9, count: 4})
	if grant.observation.grant.source.index != original.index || grant.observation.grant.grant.Reservation.First != 1 || grant.observation.grant.grant.Reservation.Last != 4 {
		t.Fatal("original range coordinates", grant.observation.grant)
	}
	install, err := n.hosts[0][0].InstallGrant(n.requestID(), 8, grant)
	if err != nil {
		t.Fatal(err)
	}
	installed := n.submit(1, install)
	assertControlCDC := func(group int, o outcome, want bool) {
		t.Helper()
		for _, h := range n.hosts[group] {
			b, err := h.machine.store.ApplicationRecord(t.Context(), h.driver.Applied(), false, graphOutcomeBytes)
			if err != nil {
				t.Fatal(err)
			}
			if !want {
				if len(b) != 0 {
					t.Fatal("non-change emitted control CDC", o, b)
				}
				continue
			}
			decoded, err := decodeDeclaredOutcome(b, h.machine.ns)
			if err != nil || decoded != o {
				t.Fatal("typed lean control CDC", decoded, o, err)
			}
		}
	}
	assertControlCDC(0, original, true)
	assertControlCDC(1, installed, true)
	// A stale pending observation may arrive after active issuance. Each exact
	// fence remains idempotent without resetting phases, range or sequence.
	for group, target := range []uint64{3, 8} {
		before := n.read(group, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: handle.session.ID}})
		fence, err := n.hosts[0][0].RecipientTransition(n.requestID(), fenceDeclaredRecipient, target, n.pending[handle.session.ID])
		if err != nil {
			t.Fatal(err)
		}
		o := n.submit(group, fence)
		if o.disposition != controlRecovery {
			t.Fatal("delayed fence not recovery", o)
		}
		assertControlCDC(group, o, false)
		after := n.read(group, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: handle.session.ID}})
		if before.observation.recipient != after.observation.recipient {
			t.Fatal("delayed fence changed exact recipient", before.observation.recipient, after.observation.recipient)
		}
	}
	currentService := n.read(0, allocationQuery{kind: observeAllocator})
	transfer, err := n.hosts[0][0].TransferAllocator(n.requestID(), currentService)
	if err != nil {
		t.Fatal(err)
	}
	transferred := n.submit(0, transfer)
	assertControlCDC(0, transferred, true)
	replay := n.submit(0, reserve)
	if replay.disposition != requestReplay || replay.index != original.index {
		t.Fatal("retry checked current fence before original mapping", replay, original)
	}
	assertControlCDC(0, replay, false)
	newService := n.read(0, allocationQuery{kind: observeAllocator})
	recoveryProposal, err := n.hosts[0][0].AuthorizeReserve(n.requestID(), request, newService)
	if err != nil {
		t.Fatal(err)
	}
	recovery := n.submit(0, recoveryProposal)
	if recovery.disposition != grantRecovery {
		t.Fatal("same reference allocated fresh range", recovery)
	}
	assertControlCDC(0, recovery, false)
	changed := request
	changed.Count = 5
	mismatch, err := n.hosts[0][0].AuthorizeReserve(n.requestID(), changed, newService)
	if err != nil {
		t.Fatal(err)
	}
	rejected := n.submitReason(0, mismatch, reasonMismatch)
	assertControlCDC(0, rejected, false)
	homeReplay := n.submit(1, install)
	if homeReplay.disposition != requestReplay || homeReplay.index != installed.index {
		t.Fatal("home retry lost original coordinate", homeReplay, installed)
	}
	assertControlCDC(1, homeReplay, false)
	for g, group := range n.hosts {
		for j, h := range group {
			s := h.machine.store
			index, image, err := s.Checkpoint()
			if err != nil || !bytes.Equal(image, roots[g][j]) {
				t.Fatal("control changed graph epoch/digest/root", err)
			}
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			cfg := n.configs[g][j]
			cfg.Create = false
			reopened, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			m, err := newDeclaredMaterializer(reopened, h.machine.ns, n.d, h.machine.limits)
			if err != nil {
				t.Fatal(err)
			}
			query := allocationQuery{kind: observeGrant, session: request.Session, sequence: 9, count: 4, nonce: [16]byte{1}}
			o, err := m.observeAllocation(query, index)
			if err != nil || o.grant.source != grant.observation.grant.source || o.grant.grant != grant.observation.grant.grant {
				t.Fatal("reopened cached range changed", o.grant, err)
			}
			want := original
			if g == 1 {
				want = installed
			}
			wire, err := reopened.ApplicationRecord(t.Context(), want.index, true, graphOutcomeBytes)
			if err != nil {
				t.Fatal(err)
			}
			old, err := decodeDeclaredOutcome(wire, m.ns)
			if err != nil || old != want {
				t.Fatal("retained original outcome changed", old, want, err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestDeclaredAllocationRecipientQueryIsIDOnlyAndReadContextExact(t *testing.T) {
	id := allocationReadID{session: [16]byte{1}, sequence: 1}
	q := allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: [16]byte{2}}, nonce: [16]byte{3}}
	if err := q.validate(idalloc.GraphID{4}); err != nil {
		t.Fatal("ID-only recipient point query refused", err)
	}
	wire := encodeAllocationReadContext(id, q)
	if len(wire) != allocationReadContextBytes || cap(wire) != len(wire) {
		t.Fatal(len(wire), cap(wire))
	}
	gotID, got, err := decodeAllocationReadContext(wire)
	if err != nil || gotID != id || got != q {
		t.Fatal(gotID, got, err)
	}
	for _, field := range []string{"incarnation", "epoch", "sequence", "count", "nonce"} {
		bad := q
		switch field {
		case "incarnation":
			bad.session.Incarnation = [16]byte{1}
		case "epoch":
			bad.session.Epoch = 1
		case "sequence":
			bad.sequence = 1
		case "count":
			bad.count = 1
		case "nonce":
			bad.nonce = [16]byte{}
		}
		if err := bad.validate(idalloc.GraphID{4}); !errors.Is(err, errInvalid) {
			t.Fatal(field, err)
		}
	}
	for _, wire := range [][]byte{nil, {1}, make([]byte, allocationReadContextBytes)} {
		if _, _, err := decodeAllocationReadContext(wire); !errors.Is(err, errCorrupt) {
			t.Fatal(err)
		}
	}
}

func TestDeclaredAllocationWrongDeliveryNonceBurnsAttemptWithoutCursor(t *testing.T) {
	n := newAllocationTestNetwork(t)
	h := n.activate(1, [16]byte{2})
	var firstReply allocationReply
	cursor, err := h.Acquire(t.Context(), 1, 4, func(ctx context.Context, r idalloc.GrantRequest, q allocationQuery) (allocationReply, error) {
		reply, err := n.deliver(1, r, q)
		if err != nil {
			return allocationReply{}, err
		}
		firstReply = reply
		reply.question.nonce[0] ^= 1
		return reply, nil
	})
	if cursor != nil || !errors.Is(err, idalloc.ErrPayloadMismatch) {
		t.Fatal("wrong nonce created issuance authority", cursor, err)
	}
	if _, err := h.Acquire(t.Context(), 1, 4, func(context.Context, idalloc.GrantRequest, allocationQuery) (allocationReply, error) {
		t.Fatal("abandoned attempt recreated delivery/cursor")
		return allocationReply{}, nil
	}); !errors.Is(err, idalloc.ErrAlreadyReserved) {
		t.Fatal(err)
	}
	if firstReply.proof.observation.grant.grant.Reservation.First != 1 || firstReply.proof.observation.grant.grant.Reservation.Last != 4 {
		t.Fatal(firstReply)
	}
	// A later legitimate refill skips the abandoned attempt and cannot reuse
	// any of its reserved numbers. This is an independent expected range.
	second := n.acquire(1, h, 2, 4)
	for want := uint64(5); want <= 8; want++ {
		got, err := second.Next()
		if err != nil || got != want {
			t.Fatal(got, want, err)
		}
	}
	active := n.read(1, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: h.session.ID}})
	if _, err := n.hosts[1][0].OpenRecipient(active); !errors.Is(err, idalloc.ErrAlreadyReserved) {
		t.Fatal("reopen recreated same-incarnation cursor owner", err)
	}
}

func TestDeclaredAllocationFreshRangeAfterServiceRotationAndCanceledRefill(t *testing.T) {
	n := newAllocationTestNetwork(t)
	h := n.activate(1, [16]byte{2})
	old := n.acquire(1, h, 1, 4)
	before := n.read(0, allocationQuery{kind: observeAllocator})
	previous, err := idalloc.Inspect(&before.observation.allocator)
	if err != nil {
		t.Fatal(err)
	}
	transfer, err := n.hosts[0][0].TransferAllocator(n.requestID(), before)
	if err != nil {
		t.Fatal(err)
	}
	n.submit(0, transfer)
	current := n.read(0, allocationQuery{kind: observeAllocator})
	after, err := idalloc.Inspect(&current.observation.allocator)
	if err != nil || after.HighWater != 4 || after.Authority.Epoch() != previous.Authority.Epoch()+1 {
		t.Fatal(previous, after, err)
	}
	request := idalloc.GrantRequest{Graph: n.hosts[0][0].machine.ns.graph, Session: h.session, Sequence: 2, Count: 4}
	if p, err := n.hosts[0][0].AuthorizeReserve(n.requestID(), request, before); len(p.wire) != 0 || !errors.Is(err, idalloc.ErrStaleAuthority) {
		t.Fatal("old service authorized new range", p, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if c, err := h.Acquire(ctx, 2, 4, func(context.Context, idalloc.GrantRequest, allocationQuery) (allocationReply, error) {
		t.Fatal("canceled before delivery burned/transported attempt")
		return allocationReply{}, nil
	}); c != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(c, err)
	}
	fresh := n.acquire(1, h, 2, 4)
	for i := uint64(1); i <= 4; i++ {
		got, err := old.Next()
		if err != nil || got != i {
			t.Fatal("cached grant invalidated by service rotation", got, err)
		}
		got, err = fresh.Next()
		if err != nil || got != i+4 {
			t.Fatal("high water reset after service rotation", got, err)
		}
	}
	freshGrant := n.read(0, allocationQuery{kind: observeGrant, session: h.session, sequence: 2, count: 4})
	if freshGrant.observation.grant.grant.Reservation.Request.Authority != after.Authority {
		t.Fatal("new range retained stale allocation authority")
	}
}

func TestDeclaredAllocationHostNilClosedAndWrongProofContracts(t *testing.T) {
	var h *allocationHost
	for _, call := range []func() error{
		func() error { _, err := h.Campaign(); return err }, func() error { _, err := h.Tick(); return err }, func() error { _, err := h.Step(replica.Packet{}); return err }, func() error { _, err := h.Submit(allocationProposal{}); return err }, func() error { _, err := h.Read(allocationQuery{}); return err }, func() error { return h.Close() },
		func() error {
			_, err := h.PrepareInitialization(3, bootstrapAttemptID{1}, nil, 16, allocationProof{})
			return err
		}, func() error { _, err := h.BeginRecipient(requestID{1}, [16]byte{1}, allocationProof{}); return err }, func() error {
			_, err := h.RecipientTransition(requestID{1}, fenceDeclaredRecipient, 3, allocationProof{})
			return err
		}, func() error { _, err := h.TransferAllocator(requestID{1}, allocationProof{}); return err }, func() error {
			_, err := h.AuthorizeReserve(requestID{1}, idalloc.GrantRequest{}, allocationProof{})
			return err
		}, func() error { _, err := h.InstallGrant(requestID{1}, 8, allocationProof{}); return err }, func() error { _, err := h.OpenRecipient(allocationProof{}); return err },
	} {
		if err := call(); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := openAllocationHost(nil, 1, replica.Config{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	var handle *allocationRecipientHandle
	if _, err := handle.Acquire(t.Context(), 1, 1, nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	n := newAllocationTestNetwork(t)
	h = n.hosts[1][0]
	out, err := h.Tick()
	if err != nil {
		t.Fatal(err)
	}
	n.pump(1, out)
	foreign := n.read(0, allocationQuery{kind: observeConfiguration})
	if p, err := h.BeginRecipient(n.requestID(), [16]byte{1}, foreign); len(p.wire) != 0 || !errors.Is(err, errInvalid) {
		t.Fatal("foreign process proof accepted", p, err)
	}
	if p, err := h.Submit(allocationProposal{target: n.hosts[0][0].machine.scope(), wire: []byte{1}}); len(p.packets) != 0 || !errors.Is(err, errInvalid) {
		t.Fatal("foreign target scope accepted", p, err)
	}
	if _, err := h.Read(allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: [16]byte{1}, Incarnation: [16]byte{2}}, nonce: [16]byte{1}}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal("repeat close changed result", err)
	}
	for _, call := range []func() error{func() error { _, err := h.Campaign(); return err }, func() error { _, err := h.Tick(); return err }, func() error { _, err := h.Step(replica.Packet{}); return err }, func() error { _, err := h.Submit(allocationProposal{}); return err }, func() error { _, err := h.Read(allocationQuery{}); return err }} {
		if err := call(); !errors.Is(err, replica.ErrStopped) {
			t.Fatal(err)
		}
	}
}

func TestDeclaredAllocationRecipientReplacementRefusesDelayedOldDeliveries(t *testing.T) {
	n := newAllocationTestNetwork(t)
	old := n.activate(1, [16]byte{2})
	n.acquire(1, old, 1, 4)
	pending := n.pending[old.session.ID]
	oldActive := n.read(0, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: old.session.ID}})
	oldHome := n.read(1, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: old.session.ID}})
	next := n.activateFrom(1, old.session.ID, oldHome)
	if next.session.ID != old.session.ID || next.session.Epoch != old.session.Epoch+1 {
		t.Fatal("recipient epoch not independently replaced", old.session, next.session)
	}
	if c, err := old.Acquire(t.Context(), 2, 4, func(context.Context, idalloc.GrantRequest, allocationQuery) (allocationReply, error) {
		t.Fatal("fenced old recipient delivered new grant")
		return allocationReply{}, nil
	}); c != nil || !errors.Is(err, idalloc.ErrStaleAuthority) {
		t.Fatal(c, err)
	}
	for group, target := range []uint64{3, 8} {
		before := n.read(group, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: old.session.ID}})
		fence, err := n.hosts[0][0].RecipientTransition(n.requestID(), fenceDeclaredRecipient, target, pending)
		if err != nil {
			t.Fatal(err)
		}
		n.submitReason(group, fence, reasonStale)
		after := n.read(group, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: old.session.ID}})
		if before.observation.recipient != after.observation.recipient {
			t.Fatal("stale fence changed new session", before.observation.recipient, after.observation.recipient)
		}
	}
	publish, err := n.hosts[0][0].RecipientTransition(n.requestID(), publishDeclaredRecipient, 8, oldActive)
	if err != nil {
		t.Fatal(err)
	}
	n.submitReason(1, publish, reasonStale)
	fresh := n.acquire(1, next, 1, 4)
	for want := uint64(5); want <= 8; want++ {
		got, err := fresh.Next()
		if err != nil || got != want {
			t.Fatal("recipient replacement reset global allocator", got, want, err)
		}
	}
	// An old durable grant remains inspectable but cannot reopen the retired
	// home handle. Its immutable numeric range is permanently reserved.
	oldGrant := n.read(1, allocationQuery{kind: observeGrant, session: old.session, sequence: 1, count: 4})
	if oldGrant.observation.grant.grant.Reservation.First != 1 || oldGrant.observation.grant.grant.Reservation.Last != 4 {
		t.Fatal(oldGrant)
	}
	if h, err := n.hosts[1][0].OpenRecipient(oldHome); h != nil || !errors.Is(err, idalloc.ErrStaleAuthority) {
		t.Fatal("old proof reopened fenced recipient", h, err)
	}
}

func TestDeclaredAllocationReadQuotaAndReversedDistinctCompletions(t *testing.T) {
	n := newAllocationTestNetwork(t)
	h := n.hosts[1][0]
	q := allocationQuery{kind: observeConfiguration, nonce: [16]byte{99}}
	packets := make([]replica.Packet, 0, 128)
	ids := make(map[allocationReadID]bool, 16)
	for range 16 {
		e, err := h.Read(q)
		if err != nil {
			t.Fatal(err)
		}
		if e.readID == (allocationReadID{}) || ids[e.readID] || len(e.replies) != 0 || len(e.snapshotSends) != 0 || len(packets)+len(e.packets) > cap(packets) {
			t.Fatal("read invocation was reused/completed before routing", e)
		}
		ids[e.readID] = true
		packets = append(packets, e.packets...)
	}
	pending := maps.Clone(h.pending)
	sequence := h.sequence
	index, image, err := h.machine.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	usage, err := h.machine.store.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	if e, err := h.Read(q); e.readID != (allocationReadID{}) || len(e.packets)+len(e.replies) != 0 || !errors.Is(err, errLimit) {
		t.Fatal("over-quota read produced ownership/output", e, err)
	}
	if !maps.Equal(pending, h.pending) || sequence != h.sequence {
		t.Fatal("refusal consumed/crossed pending invocations")
	}
	afterIndex, afterImage, err := h.machine.store.Checkpoint()
	if err != nil || afterIndex != index || !bytes.Equal(image, afterImage) {
		t.Fatal("read quota refusal changed checkpoint", err)
	}
	afterUsage, err := h.machine.store.ApplicationUsage()
	if err != nil || afterUsage != usage {
		t.Fatal("read quota refusal changed retained outcomes", err)
	}
	// The question is identical, but every invocation has its own context and
	// owner. Reverse actual heartbeat packets before delivering real replies.
	slices.Reverse(packets)
	replies := n.pump(1, allocationEvent{packets: packets})
	if len(replies) != 16 || len(h.pending) != 0 {
		t.Fatal("bounded reads not all completed", len(replies), len(h.pending))
	}
	for _, reply := range replies {
		if !ids[reply.id] || reply.err != nil || reply.question != q || reply.proof.id != reply.id || reply.proof.owner != h || reply.proof.question != q {
			t.Fatal("distinct identical questions crossed proof owners", reply)
		}
		delete(ids, reply.id)
	}
	if len(ids) != 0 {
		t.Fatal("completion omission", ids)
	}
	// Counter exhaustion is a finite operational refusal, never an ID wrap.
	h.sequence = math.MaxUint64
	if e, err := h.Read(q); e.readID != (allocationReadID{}) || !errors.Is(err, errLimit) || len(h.pending) != 0 || h.sequence != math.MaxUint64 {
		t.Fatal("read sequence overflow", e, err)
	}
}

func TestDeclaredAllocationProducerRefusalsBeforeProposalAndPostInitAttempt(t *testing.T) {
	n := newAllocationTestNetwork(t)
	source, home := n.hosts[0][0], n.hosts[1][0]
	genesis := n.read(0, allocationQuery{kind: observeConfiguration})
	foreign := n.read(1, allocationQuery{kind: observeConfiguration})
	service := n.read(0, allocationQuery{kind: observeAllocator})
	schemas := []graphstate.PropertyDefinition{{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}}
	index, image, err := source.machine.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	usage, err := source.machine.store.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		name string
		call func() (allocationProposal, error)
		want error
	}{
		{"unknown-partition", func() (allocationProposal, error) {
			return source.PrepareInitialization(99, bootstrapAttemptID{20}, schemas, 16, genesis)
		}, errInvalid},
		{"home-cannot-mint-genesis", func() (allocationProposal, error) {
			return home.PrepareInitialization(8, bootstrapAttemptID{20}, schemas, 16, foreign)
		}, errInvalid},
		{"wrong-proof-kind", func() (allocationProposal, error) {
			return source.PrepareInitialization(8, bootstrapAttemptID{20}, schemas, 16, service)
		}, errInvalid},
		{"foreign-proof-owner", func() (allocationProposal, error) {
			return source.PrepareInitialization(8, bootstrapAttemptID{20}, schemas, 16, foreign)
		}, errInvalid},
		{"authority-init-with-proof", func() (allocationProposal, error) {
			return source.PrepareInitialization(3, bootstrapAttemptID{20}, schemas, 16, genesis)
		}, errInvalid},
		{"divergent-max-block", func() (allocationProposal, error) {
			return source.PrepareInitialization(8, bootstrapAttemptID{20}, schemas, 32, genesis)
		}, idalloc.ErrPayloadMismatch},
		{"divergent-schema", func() (allocationProposal, error) {
			return source.PrepareInitialization(8, bootstrapAttemptID{20}, nil, 16, genesis)
		}, idalloc.ErrPayloadMismatch},
		{"transfer-wrong-proof-kind", func() (allocationProposal, error) { return source.TransferAllocator(n.requestID(), genesis) }, errInvalid},
		{"reserve-wrong-proof-kind", func() (allocationProposal, error) {
			return source.AuthorizeReserve(n.requestID(), idalloc.GrantRequest{Graph: source.machine.ns.graph}, genesis)
		}, errInvalid},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			p, err := fixture.call()
			if len(p.wire) != 0 || p.target != (allocationScope{}) || !errors.Is(err, fixture.want) {
				t.Fatal("producer emitted rejected proposal", p, err)
			}
		})
	}
	afterIndex, afterImage, err := source.machine.store.Checkpoint()
	if err != nil || afterIndex != index || !bytes.Equal(image, afterImage) {
		t.Fatal("producer refusal changed store", err)
	}
	afterUsage, err := source.machine.store.ApplicationUsage()
	if err != nil || afterUsage != usage {
		t.Fatal("producer refusal created dedup/outcome", err)
	}
	repeated, err := source.PrepareInitialization(3, bootstrapAttemptID{21}, schemas, 16, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	n.submitReason(0, repeated, reasonAlreadyInitialized)
	_, afterImage, err = source.machine.store.Checkpoint()
	if err != nil || !bytes.Equal(image, afterImage) {
		t.Fatal("later init attempt changed graph root", err)
	}
	cdc, err := source.machine.store.ApplicationRecord(t.Context(), source.driver.Applied(), false, 4096)
	if err != nil || len(cdc) != 0 {
		t.Fatal("post-init rejection emitted logical changes", cdc, err)
	}
	handle := n.activate(1, [16]byte{1})
	active := n.read(0, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: handle.session.ID}})
	if p, err := source.RecipientTransition(n.requestID(), initDeclaredPartition, 8, active); len(p.wire) != 0 || !errors.Is(err, errInvalid) {
		t.Fatal("invalid phase operation proposed", p, err)
	}
}

func TestDeclaredAllocationEpochSwitchDuringDeliveryCannotCreateOldCursor(t *testing.T) {
	n := newAllocationTestNetwork(t)
	old := n.activate(1, [16]byte{2})
	var next *allocationRecipientHandle
	cursor, err := old.Acquire(t.Context(), 1, 4, func(ctx context.Context, r idalloc.GrantRequest, q allocationQuery) (allocationReply, error) {
		reply, err := n.deliver(1, r, q)
		if err != nil {
			return allocationReply{}, err
		}
		active := n.read(1, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: old.session.ID}})
		next = n.activateFrom(1, old.session.ID, active)
		return reply, nil
	})
	if cursor != nil || !errors.Is(err, idalloc.ErrStaleAuthority) {
		t.Fatal("epoch switched during trusted delivery but old cursor escaped/refusal lost stale identity", cursor, err)
	}
	if next == nil || next.session.Epoch != old.session.Epoch+1 {
		t.Fatal("replacement not completed")
	}
	fresh := n.acquire(1, next, 1, 4)
	for want := uint64(5); want <= 8; want++ {
		got, err := fresh.Next()
		if err != nil || got != want {
			t.Fatal("replacement reused abandoned old block", got, want, err)
		}
	}
}

func TestDeclaredAllocationSourceInspectionCannotOpenHomeCursor(t *testing.T) {
	n := newAllocationTestNetwork(t)
	handle := n.activate(1, [16]byte{2})
	source := n.hosts[0][0]
	inspection := n.read(0, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: handle.session.ID}})
	index, image, err := source.machine.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	usage, err := source.machine.store.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	if opened, err := source.OpenRecipient(inspection); opened != nil || !errors.Is(err, idalloc.ErrStaleAuthority) {
		t.Fatal("source observation became home issuance authority", opened, err)
	}
	if len(source.recipients) != 0 {
		t.Fatal("refusal published a source recipient owner")
	}
	afterIndex, afterImage, err := source.machine.store.Checkpoint()
	if err != nil || afterIndex != index || !bytes.Equal(image, afterImage) {
		t.Fatal("inspection refusal changed root", err)
	}
	afterUsage, err := source.machine.store.ApplicationUsage()
	if err != nil || afterUsage != usage {
		t.Fatal("inspection refusal wrote durable authority", err)
	}
}

func TestDeclaredAllocationFailedAndWrongHostDeliveryCannotCreateCursor(t *testing.T) {
	for _, mode := range []string{"transport-failure", "completed-source-proof"} {
		t.Run(mode, func(t *testing.T) {
			n := newAllocationTestNetwork(t)
			handle := n.activate(1, [16]byte{2})
			networkErr := errors.New("fixture transport unavailable")
			cursor, err := handle.Acquire(t.Context(), 1, 4, func(ctx context.Context, r idalloc.GrantRequest, q allocationQuery) (allocationReply, error) {
				if mode == "transport-failure" {
					return allocationReply{}, networkErr
				}
				if _, err := n.deliver(1, r, q); err != nil {
					return allocationReply{}, err
				}
				// Same genuine query/session/nonce and immutable range, but a real
				// SOURCE completed-read invocation cannot authorize a HOME cursor.
				e, err := n.hosts[0][0].Read(q)
				if err != nil {
					return allocationReply{}, err
				}
				replies := n.pump(0, e)
				if len(replies) != 1 {
					return allocationReply{}, errInvalid
				}
				return replies[0], nil
			})
			want := errInvalid
			if mode == "transport-failure" {
				want = idalloc.ErrUnknown
			}
			if cursor != nil || !errors.Is(err, want) || mode == "transport-failure" && !errors.Is(err, networkErr) {
				t.Fatal("failed/foreign delivery created cursor or lost cause", cursor, err)
			}
			if _, err := handle.Acquire(t.Context(), 1, 4, func(context.Context, idalloc.GrantRequest, allocationQuery) (allocationReply, error) {
				t.Fatal("abandoned attempt reused delivery")
				return allocationReply{}, nil
			}); !errors.Is(err, idalloc.ErrAlreadyReserved) {
				t.Fatal(err)
			}
			fresh := n.acquire(1, handle, 2, 4)
			first := uint64(1)
			if mode == "completed-source-proof" {
				first = 5
			}
			for want := first; want < first+4; want++ {
				got, err := fresh.Next()
				if err != nil || got != want {
					t.Fatal("fresh refill reused reserved/abandoned block", got, want, err)
				}
			}
		})
	}
}

func TestDeclaredMaterializerMalformedProtocolAndLocalDecodeLimitHaveNoEffects(t *testing.T) {
	n := newAllocationTestNetwork(t)
	source := n.hosts[0][0]
	service := n.read(0, allocationQuery{kind: observeAllocator})
	proposal, err := source.TransferAllocator(n.requestID(), service)
	if err != nil {
		t.Fatal(err)
	}
	s, m := source.machine.store, source.machine
	index, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	usage, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"malformed", "local-codec-budget"} {
		t.Run(mode, func(t *testing.T) {
			limited := *m
			wire := bytes.Clone(proposal.wire)
			want := errCorrupt
			if mode == "malformed" {
				wire[len(wire)-1] ^= 1
			} else {
				limited.limits.commandOwnedBytes = 1
				want = errLimit
			}
			b, err := limited.Stage(replica.Entry{Index: index + 1, Generation: s.ApplicationGeneration(), Term: 2, Data: wire}, s.ApplicationBudget())
			if !errors.Is(err, want) || len(b.Image)+len(b.Changes)+len(b.Outcome)+len(b.Writes) != 0 {
				t.Fatal("malformed/local limit manufactured outcome", b, err)
			}
			afterIndex, afterImage, err := s.Checkpoint()
			if err != nil || afterIndex != index || !bytes.Equal(image, afterImage) {
				t.Fatal("refusal changed captured root", err)
			}
			afterUsage, err := s.ApplicationUsage()
			if err != nil || afterUsage != usage {
				t.Fatal("refusal changed KV/outcome ledger", err)
			}
		})
	}
}
