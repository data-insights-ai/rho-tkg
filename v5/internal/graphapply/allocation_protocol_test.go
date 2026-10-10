package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"unsafe"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func TestDeclaredAllocationAgreementIndependentGoldenAndLegacySeparation(t *testing.T) {
	id := declaredSemanticContractID()
	// Independent hashlib vector over the explicitly reviewed descriptor bytes.
	if hex.EncodeToString(id[:]) != "38a23c7b8208a091d6887981aaa6921ef04879d826d0be523d02b05ac89c9aeb" {
		t.Fatalf("agreement: %x", id)
	}
	if id == SemanticContractID() {
		t.Fatal("declared allocation traffic aliases legacy graph-operation agreement")
	}
	changed := strings.Replace(declaredSemanticDescriptor, "genesis-first-v1", "genesis-first-v2", 1)
	if raftlog.ApplicationSemanticContractID(sha256.Sum256([]byte(changed))) == id {
		t.Fatal("authority rule change did not change agreement")
	}
}

func TestDeclaredAllocationScopesRemainPartitionQualified(t *testing.T) {
	d := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{4}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{4}}})
	cfg, err := newGenesisAllocationConfig(d, nil, 16, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	one := allocationScope{graph: cfg.graph, partition: 3, group: [16]byte{4}, ownership: 2, topology: 9, declaration: d.Digest(), semantic: declaredSemanticContractID()}
	two := one
	two.partition, two.ownership = 8, 3
	for _, s := range []allocationScope{one, two} {
		if err := s.check(d, cfg); err != nil {
			t.Fatal(err)
		}
		wire, err := encodeAllocationScope(s)
		if err != nil || len(wire) != allocationScopeBytes || cap(wire) != len(wire) {
			t.Fatalf("scope encoding: %d/%d %v", len(wire), cap(wire), err)
		}
		got, err := decodeAllocationScope(wire)
		if err != nil || got != s {
			t.Fatalf("scope round trip: %+v %v", got, err)
		}
		wire[4] ^= 1
		if got != s {
			t.Fatal("decoded scope aliases input")
		}
	}
	if one == two || one.group != two.group {
		t.Fatal("qualified separation fixture is not distinguishing")
	}
	wrong := one
	wrong.partition = 8
	if err := wrong.check(d, cfg); !errors.Is(err, errInvalid) {
		t.Fatalf("same-group wrong qualified owner: %v", err)
	}
	wrong = one
	wrong.semantic = SemanticContractID()
	if err := wrong.check(d, cfg); !errors.Is(err, errInvalid) {
		t.Fatalf("legacy semantics: %v", err)
	}
	if _, err := encodeAllocationScope(allocationScope{}); !errors.Is(err, errInvalid) {
		t.Fatalf("zero scope: %v", err)
	}
	for _, b := range [][]byte{nil, {1}, make([]byte, allocationScopeBytes)} {
		if _, err := decodeAllocationScope(b); !errors.Is(err, errCorrupt) {
			t.Fatalf("malformed scope: %v", err)
		}
	}
}

func TestDeclaredProtocolGrantPreservesSourceVersusHomeCoordinates(t *testing.T) {
	a, err := idalloc.NewAuthority([16]byte{7}, 2)
	if err != nil {
		t.Fatal(err)
	}
	state, err := idalloc.NewState(idalloc.GraphID{1}, a, 16)
	if err != nil {
		t.Fatal(err)
	}
	_, rangeRecord, replay, err := idalloc.Reserve(&state, idalloc.Request{Graph: idalloc.GraphID{1}, Authority: a, Sequence: 1, Count: 4})
	if err != nil || replay {
		t.Fatalf("pure range fixture: %v %v", replay, err)
	}
	scope := allocationScope{graph: idalloc.GraphID{1}, partition: 3, group: [16]byte{4}, ownership: 2, topology: 9, declaration: [32]byte{5}, semantic: declaredSemanticContractID()}
	g := protocolGrant{configuration: [32]byte{6}, source: allocationCoordinate{scope: scope, index: 17}, grant: idalloc.Grant{Request: idalloc.GrantRequest{Graph: idalloc.GraphID{1}, Session: idalloc.RecipientSession{ID: [16]byte{8}, Incarnation: [16]byte{9}, Epoch: 3}, Sequence: 9, Count: 4}, Reservation: rangeRecord}, installed: 2}
	n := namespace{graph: idalloc.GraphID{1}, partition: 8}
	wire, err := encodeProtocolGrant(n, g)
	if err != nil {
		t.Fatal(err)
	}
	// Independent field-layout arithmetic, not an encoder-returned size proxy.
	if protocolGrantBytes != 4+24+32+120+8+144+8+32 || len(wire) != protocolGrantBytes || cap(wire) != len(wire) {
		t.Fatalf("grant size/capacity: %d %d/%d", protocolGrantBytes, len(wire), cap(wire))
	}
	got, err := decodeProtocolGrant(wire, n)
	if err != nil || got != g || got.source.index <= got.installed {
		t.Fatalf("independent coordinates/range: %+v %v", got, err)
	}
	if got.grant.Reservation.First != 1 || got.grant.Reservation.Last != 4 || got.grant.Request.Sequence == got.grant.Reservation.Request.Sequence {
		t.Fatal("recipient/global sequence fixture is not distinguishing")
	}
	wire[4] ^= 1
	if got != g {
		t.Fatal("decoded grant aliases input")
	}
	if _, err := decodeProtocolGrant(wire, n); !errors.Is(err, errCorrupt) {
		t.Fatalf("checksum: %v", err)
	}
	if _, err := encodeProtocolGrant(n, protocolGrant{}); !errors.Is(err, errInvalid) {
		t.Fatalf("zero grant: %v", err)
	}
	if unsafe.Sizeof(g) > 512 {
		t.Fatalf("fixed grant metadata allowance insufficient: %d", unsafe.Sizeof(g))
	}
}

func TestDeclaredAllocationScopeRechecksummedInvalidMetadata(t *testing.T) {
	// A checksum-valid frame must still fail structural validation.
	scope := allocationScope{graph: idalloc.GraphID{1}, partition: 3, group: [16]byte{4}, ownership: 2, topology: 9, declaration: [32]byte{5}, semantic: declaredSemanticContractID()}
	wire, err := encodeAllocationScope(scope)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]int{{4, 20}, {20, 28}, {28, 44}, {44, 52}, {52, 60}, {60, 92}, {92, 124}} {
		mutant := bytes.Clone(wire)
		clear(mutant[pair[0]:pair[1]])
		hash := sha256.Sum256(mutant[:len(mutant)-32])
		copy(mutant[len(mutant)-32:], hash[:])
		if _, err := decodeAllocationScope(mutant); !errors.Is(err, errCorrupt) {
			t.Fatalf("rechecksummed scope fields %v: %v", pair, err)
		}
	}
}

func TestDeclaredRecipientPhasesDelayedFencesAndExactRecovery(t *testing.T) {
	source := allocationScope{graph: idalloc.GraphID{1}, partition: 3, group: [16]byte{4}, ownership: 2, topology: 9, declaration: [32]byte{5}, semantic: declaredSemanticContractID()}
	home := source
	home.partition, home.ownership = 8, 3
	pending := protocolRecipient{configuration: [32]byte{6}, session: idalloc.RecipientSession{ID: [16]byte{7}, Incarnation: [16]byte{8}, Epoch: 1}, home: 8, sourcePhase: recipientPending}
	created, changed, why, err := beginProtocolRecipient(nil, pending)
	if err != nil || !changed || why != reasonNone || created != pending {
		t.Fatal(created, changed, why, err)
	}
	fenced, changed, why, err := fenceProtocolSource(&created, pending, source, 17)
	if err != nil || !changed || why != reasonNone || fenced.sourceFence != 17 || fenced.homePhase != recipientAbsent {
		t.Fatal(fenced, changed, why, err)
	}
	homeFenced, changed, why, err := fenceProtocolHome(nil, pending, 2)
	if err != nil || !changed || why != reasonNone || homeFenced.homeFence != 2 || homeFenced.sourcePhase != recipientAbsent {
		t.Fatal(homeFenced, changed, why, err)
	}
	acked, changed, why, err := acknowledgeProtocolHome(&fenced, homeFenced, home)
	if err != nil || !changed || why != reasonNone || acked.homeAck != (allocationCoordinate{scope: home, index: 2}) {
		t.Fatal(acked, changed, why, err)
	}
	active, changed, why, err := activateProtocolRecipient(&acked, acked, acked.homeAck, source, 19)
	if err != nil || !changed || why != reasonNone || active.activation.index != 19 || active.sourcePhase != recipientActive {
		t.Fatal(active, changed, why, err)
	}
	published, changed, why, err := publishProtocolRecipient(&homeFenced, active)
	if err != nil || !changed || why != reasonNone || published.homePhase != recipientActive || published.activation != active.activation {
		t.Fatal(published, changed, why, err)
	}
	active.sourceSequence, published.sequence = 9, 7
	got, changed, why, err := fenceProtocolSource(&active, pending, source, 99)
	if err != nil || changed || why != reasonNone || got != active {
		t.Fatal("delayed source fence reset active issuance", got, changed, why, err)
	}
	got, changed, why, err = fenceProtocolHome(&published, pending, 100)
	if err != nil || changed || why != reasonNone || got != published {
		t.Fatal("delayed home fence reset active issuance", got, changed, why, err)
	}
	got, changed, why, err = beginProtocolRecipient(&active, pending)
	if err != nil || changed || why != reasonNone || got != active {
		t.Fatal("delayed begin reset active issuance", got, changed, why, err)
	}
	for _, r := range []protocolRecipient{pending, fenced, homeFenced, acked, active, published} {
		n := namespace{graph: source.graph, partition: 3}
		wire, err := encodeProtocolRecipient(n, r)
		if err != nil || cap(wire) != len(wire) {
			t.Fatal(r, err)
		}
		decoded, err := decodeProtocolRecipient(wire, n)
		if err != nil || decoded != r {
			t.Fatal(decoded, err)
		}
	}
	// Structural checks, independently of checksums: an active home cannot
	// coexist with an unactivated source phase in a combined same-partition row.
	invalid := active
	invalid.sourcePhase, invalid.sourceSequence, invalid.homePhase, invalid.homeFence = recipientFenced, 0, recipientActive, 20
	if _, err := encodeProtocolRecipient(namespace{graph: source.graph, partition: 3}, invalid); !errors.Is(err, errInvalid) {
		t.Fatal("impossible combined phase admitted", err)
	}
}

func TestDeclaredAllocationNestedObservationScopesBeforeRecovery(t *testing.T) {
	d := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{4}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{4}}})
	cfg, err := newGenesisAllocationConfig(d, nil, 16, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	digest, err := cfg.digest()
	if err != nil {
		t.Fatal(err)
	}
	source := allocationScope{graph: cfg.graph, partition: 3, group: [16]byte{4}, ownership: 2, topology: 9, declaration: d.Digest(), semantic: declaredSemanticContractID()}
	home := source
	home.partition, home.ownership = 8, 3
	authority, err := idalloc.NewAuthority([16]byte{7}, 1)
	if err != nil {
		t.Fatal(err)
	}
	state, err := idalloc.NewState(cfg.graph, authority, 16)
	if err != nil {
		t.Fatal(err)
	}
	_, block, _, err := idalloc.Reserve(&state, idalloc.Request{Graph: cfg.graph, Authority: authority, Sequence: 1, Count: 4})
	if err != nil {
		t.Fatal(err)
	}
	session := idalloc.RecipientSession{ID: [16]byte{1}, Incarnation: [16]byte{2}, Epoch: 1}
	grant := protocolGrant{configuration: digest, source: allocationCoordinate{scope: source, index: 17}, grant: idalloc.Grant{Request: idalloc.GrantRequest{Graph: cfg.graph, Session: session, Sequence: 1, Count: 4}, Reservation: block}, installed: 17}
	observation := allocationObservation{kind: observeGrant, source: allocationCoordinate{scope: source, index: 19}, configuration: digest, epoch: 1, effect: [32]byte{1}, grant: grant}
	a := allocationRecordReader{q: &reader{ns: namespace{graph: cfg.graph, partition: 8}, base: raftlog.ApplicationRoot{Index: 2}}, scope: home, config: cfg, declaration: d}
	if err := a.checkObservation(observation); err != nil {
		t.Fatal("qualified source/home indexes incorrectly ordered", err)
	}
	homeObservation := observation
	homeObservation.source = allocationCoordinate{scope: home, index: 2}
	homeObservation.grant.installed = 2
	if err := a.checkObservation(homeObservation); err != nil {
		t.Fatal("home observation compared unrelated source index", err)
	}
	for _, field := range []string{"group", "owner", "partition", "future-grant", "future-install"} {
		mutant := observation
		switch field {
		case "group":
			mutant.grant.source.scope.group = [16]byte{99}
		case "owner":
			mutant.grant.source.scope.ownership++
		case "partition":
			mutant.grant.source.scope.partition = 8
		case "future-grant":
			mutant.grant.source.index = 20
		case "future-install":
			mutant.grant.installed = 20
		}
		if err := a.checkObservation(mutant); !errors.Is(err, errInvalid) {
			t.Fatal(field, err)
		}
	}
	r := protocolRecipient{configuration: digest, session: session, home: 8, sourcePhase: recipientActive, sourceFence: 17, homeAck: allocationCoordinate{scope: home, index: 2}, activation: allocationCoordinate{scope: source, index: 18}}
	observedRecipient := allocationObservation{kind: observeRecipient, source: allocationCoordinate{scope: source, index: 19}, configuration: digest, epoch: 1, effect: [32]byte{1}, recipient: r}
	if err := a.checkObservation(observedRecipient); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"ack-owner", "activation-group", "activation-home", "future-activation"} {
		mutant := observedRecipient
		switch field {
		case "ack-owner":
			mutant.recipient.homeAck.scope.ownership++
		case "activation-group":
			mutant.recipient.activation.scope.group = [16]byte{99}
		case "activation-home":
			mutant.recipient.activation.scope = home
		case "future-activation":
			mutant.recipient.activation.index = 20
		}
		if err := a.checkObservation(mutant); !errors.Is(err, errInvalid) {
			t.Fatal(field, err)
		}
	}
}

func TestDeclaredRecipientPhaseOrderRefusalsPreserveInput(t *testing.T) {
	source := allocationScope{graph: idalloc.GraphID{1}, partition: 3, group: [16]byte{4}, ownership: 2, topology: 9, declaration: [32]byte{5}, semantic: declaredSemanticContractID()}
	home := source
	home.partition, home.ownership = 8, 3
	pending := protocolRecipient{configuration: [32]byte{6}, session: idalloc.RecipientSession{ID: [16]byte{7}, Incarnation: [16]byte{8}, Epoch: 1}, home: 8, sourcePhase: recipientPending}
	fenced := pending
	fenced.sourcePhase, fenced.sourceFence = recipientFenced, 17
	homeFenced := protocolRecipient{configuration: pending.configuration, session: pending.session, home: 8, homePhase: recipientFenced, homeFence: 2}
	acked := fenced
	acked.homeAck = allocationCoordinate{scope: home, index: 2}
	active := acked
	active.sourcePhase, active.activation = recipientActive, allocationCoordinate{scope: source, index: 19}
	published := homeFenced
	published.homePhase, published.activation = recipientActive, active.activation
	next := pending
	next.previous, next.session.Epoch = 1, 2
	badPending := pending
	badPending.sourceSequence = 1
	badFenced := acked
	badFenced.sourceSequence = 1
	badHome := homeFenced
	badHome.sequence = 1
	before := [...]protocolRecipient{pending, fenced, homeFenced, acked, active, published, next, badPending, badFenced, badHome}
	for _, fixture := range []struct {
		name     string
		call     func() (protocolRecipient, bool, reason, error)
		why      reason
		sentinel error
	}{
		{"begin-gap", func() (protocolRecipient, bool, reason, error) { return beginProtocolRecipient(nil, next) }, reasonStale, nil},
		{"begin-before-active", func() (protocolRecipient, bool, reason, error) { return beginProtocolRecipient(&fenced, next) }, reasonStale, nil},
		{"begin-corrupt", func() (protocolRecipient, bool, reason, error) { return beginProtocolRecipient(&badPending, pending) }, reasonNone, errCorrupt},
		{"source-fence-absent", func() (protocolRecipient, bool, reason, error) { return fenceProtocolSource(nil, pending, source, 20) }, reasonStale, nil},
		{"source-fence-corrupt", func() (protocolRecipient, bool, reason, error) {
			return fenceProtocolSource(&badPending, pending, source, 20)
		}, reasonNone, errCorrupt},
		{"home-fence-gap", func() (protocolRecipient, bool, reason, error) { return fenceProtocolHome(nil, next, 3) }, reasonStale, nil},
		{"home-fence-corrupt", func() (protocolRecipient, bool, reason, error) { return fenceProtocolHome(&badHome, pending, 3) }, reasonNone, errCorrupt},
		{"ack-before-source-fence", func() (protocolRecipient, bool, reason, error) {
			return acknowledgeProtocolHome(&pending, homeFenced, home)
		}, reasonStale, nil},
		{"ack-corrupt", func() (protocolRecipient, bool, reason, error) {
			return acknowledgeProtocolHome(&badFenced, homeFenced, home)
		}, reasonNone, errCorrupt},
		{"activate-without-ack", func() (protocolRecipient, bool, reason, error) {
			return activateProtocolRecipient(&fenced, fenced, acked.homeAck, source, 20)
		}, reasonStale, nil},
		{"activate-corrupt", func() (protocolRecipient, bool, reason, error) {
			return activateProtocolRecipient(&badFenced, acked, acked.homeAck, source, 20)
		}, reasonNone, errCorrupt},
		{"publish-without-fence", func() (protocolRecipient, bool, reason, error) { return publishProtocolRecipient(nil, active) }, reasonStale, nil},
		{"publish-corrupt", func() (protocolRecipient, bool, reason, error) { return publishProtocolRecipient(&badHome, active) }, reasonNone, errCorrupt},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			_, changed, why, err := fixture.call()
			if changed || why != fixture.why || fixture.sentinel != nil && !errors.Is(err, fixture.sentinel) || fixture.sentinel == nil && err != nil {
				t.Fatal(changed, why, err)
			}
		})
	}
	// Successful phase transitions and repeated publication/activation both
	// preserve the originally qualified coordinates, not later read indexes.
	got, changed, why, err := publishProtocolRecipient(&published, active)
	if err != nil || changed || why != reasonNone || got != published {
		t.Fatal(got, changed, why, err)
	}
	got, changed, why, err = activateProtocolRecipient(&active, acked, acked.homeAck, source, 100)
	if err != nil || changed || why != reasonNone || got != active {
		t.Fatal(got, changed, why, err)
	}
	got, changed, why, err = beginProtocolRecipient(&active, next)
	if err != nil || !changed || why != reasonNone || got != next {
		t.Fatal(got, changed, why, err)
	}
	after := [...]protocolRecipient{pending, fenced, homeFenced, acked, active, published, next, badPending, badFenced, badHome}
	if after != before {
		t.Fatal("pure transition changed complete prior input records", before, after)
	}
}

func TestDeclaredRecipientRechecksummedPhaseAndCoordinateMutants(t *testing.T) {
	source := allocationScope{graph: idalloc.GraphID{1}, partition: 3, group: [16]byte{4}, ownership: 2, topology: 9, declaration: [32]byte{5}, semantic: declaredSemanticContractID()}
	home := source
	home.partition, home.ownership = 8, 3
	r := protocolRecipient{configuration: [32]byte{6}, session: idalloc.RecipientSession{ID: [16]byte{7}, Incarnation: [16]byte{8}, Epoch: 1}, home: 8, sourcePhase: recipientActive, sourceFence: 17, homeAck: allocationCoordinate{scope: home, index: 2}, activation: allocationCoordinate{scope: source, index: 19}}
	n := namespace{graph: source.graph, partition: 3}
	wire, err := encodeProtocolRecipient(n, r)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"configuration", "session", "home", "source-phase", "home-phase", "flags", "ack-index", "activation-index"} {
		mutant := bytes.Clone(wire)
		switch field {
		case "configuration":
			clear(mutant[28:60])
		case "session":
			clear(mutant[60:76])
		case "home":
			clear(mutant[100:108])
		case "source-phase":
			mutant[116] = 99
		case "home-phase":
			mutant[117] = 99
		case "flags":
			mutant[150] = 4
		case "ack-index":
			clear(mutant[271:279])
		case "activation-index":
			clear(mutant[399:407])
		}
		sum := sha256.Sum256(mutant[:len(mutant)-32])
		copy(mutant[len(mutant)-32:], sum[:])
		if _, err := decodeProtocolRecipient(mutant, n); !errors.Is(err, errCorrupt) {
			t.Fatal(field, err)
		}
	}
	if unsafe.Sizeof(allocationProtocolCommand{})+unsafe.Sizeof(allocationObservation{})+unsafe.Sizeof(outcome{})+unsafe.Sizeof(reader{}) > allocationCommandMetadataBytes {
		t.Fatal("simultaneous command metadata allowance insufficient")
	}
}
