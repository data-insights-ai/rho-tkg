package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
)

// These direct guards exercise native capacity and exact owned encodings; they
// do not establish graph-pin publication, quorum completion or GC acceptance.
func TestApplicationTerminalBankBoundIdentityProjectionAndLegacyBytes(t *testing.T) {
	p, l := DefaultApplicationPolicy(1), DefaultLimits()
	makeOwner := func(id byte) ApplicationRecoveryReservation {
		return ApplicationRecoveryReservation{ID: [32]byte{id}, Digest: [32]byte{id, 9}, Credits: []ApplicationRecoveryCredit{
			{Phase: 1, CommandBytes: 128, RetainedBytes: 8192, RetainedRecords: 8},
			{Phase: 2, CommandBytes: 128, RetainedBytes: 8192, RetainedRecords: 8},
			{Phase: 3, CommandBytes: 128, RetainedBytes: 8192, RetainedRecords: 8},
		}}
	}
	a, b := makeOwner(2), makeOwner(1)
	m := applicationRecoveryMetadata{Config: ApplicationRecoveryConfig{Version: 2}, Reservation: makeOwner(90)}
	var err error
	m, err = m.reserveTerminal(a, p, l)
	if err != nil {
		t.Fatal(err)
	}
	m, err = m.reserveTerminal(b, p, l)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Terminal) != 2 || m.Terminal[0].ID != b.ID || m.Reservation.ID != ([32]byte{90}) {
		t.Fatal("terminal ordering changed transient owner")
	}
	a.Credits[0].RetainedBytes = 1
	if m.Terminal[1].Credits[0].RetainedBytes != 8192 {
		t.Fatal("reservation borrowed caller credits")
	}
	changed := makeOwner(2)
	changed.Digest[1]++
	if _, err := m.reserveTerminal(changed, p, l); !errors.Is(err, ErrInvalid) {
		t.Fatalf("changed original template: %v", err)
	}
	u := ApplicationRecoveryUse{ID: b.ID, Digest: b.Digest, CommandDigest: sha256.Sum256([]byte("release original")), Phase: 1}
	projected, credit, err := m.consumeTerminal(u, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(projected.Terminal) != 2 || len(projected.Terminal[0].Credits) != 2 || credit.RetainedBytes != 8192 || len(m.Terminal[0].Credits) != 3 {
		t.Fatal("projection released unused terminal alternatives or mutated prior state")
	}
	g, err := terminalGrowth(projected.Terminal, p, l)
	if err != nil || g.records != 5*8 {
		t.Fatalf("protected alternatives: %+v %v", g, err)
	}
	installed, _, err := m.consumeTerminal(u, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(installed.Terminal) != 1 || installed.Terminal[0].ID != ([32]byte{2}) || !sameRecoveryReservation(installed.Reservation, m.Reservation) {
		t.Fatal("terminal retired another owner or transient reservation")
	}
	wrong := u
	wrong.Digest[0]++
	if _, _, err := m.consumeTerminal(wrong, true); !errors.Is(err, ErrInvalid) {
		t.Fatalf("different template terminal: %v", err)
	}
	wrong = u
	wrong.RetireBefore = 1
	if _, err := m.terminalCredit(wrong); !errors.Is(err, ErrInvalid) {
		t.Fatalf("terminal partial retirement: %v", err)
	}
	root := appendNativeRecoveryRoot(m)
	r, bank, err := readNativeRecoveryRoot(root, p, l)
	if err != nil || !sameRecoveryReservation(r, m.Reservation) || !sameTerminalBank(bank, m.Terminal) || len(root) != nativeRecoveryRootBytes(m) || sha256.Sum256(root) != nativeRecoveryRootDigest(m) {
		t.Fatalf("exact fresh root: %v", err)
	}
	metadataBytes := appendRecoveryMetadata(nil, m)
	decoded, tail, err := decodeRecoveryMetadata(metadataBytes, p, l)
	if err != nil || len(tail) != 0 || decoded.Config != m.Config || !sameTerminalBank(decoded.Terminal, m.Terminal) || len(metadataBytes) != recoveryMetadataBytes(m) {
		t.Fatalf("exact owned metadata: %v", err)
	}
	for id := byte(3); id <= 16; id++ {
		m, err = m.reserveTerminal(makeOwner(id), p, l)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.reserveTerminal(makeOwner(17), p, l); !errors.Is(err, ErrLimit) {
		t.Fatalf("owner capacity: %v", err)
	}
	tooMany := makeOwner(20)
	tooMany.Credits = append(tooMany.Credits, ApplicationRecoveryCredit{Phase: 4, CommandBytes: 128, RetainedBytes: 8192, RetainedRecords: 8})
	if _, err := terminalGrowth(applicationTerminalBank{tooMany}, p, l); !errors.Is(err, ErrInvalid) {
		t.Fatalf("alternative bound: %v", err)
	}
	if _, err := recoveryFits(g, p.RetainedApplicationBytes, 0, 0, 0, p, l); !errors.Is(err, ErrLimit) {
		t.Fatalf("terminal credits minted quota: %v", err)
	}
	fullMeta := metadata{Recovery: m}
	fullMeta.Recovery.Reservation = makeOwner(99)
	fullMeta.Recovery.Reservation.Credits = make([]ApplicationRecoveryCredit, 2700)
	if err := terminalMetadataHeadroom(fullMeta, l); !errors.Is(err, ErrLimit) {
		t.Fatalf("terminal metadata escape: %v", err)
	}
	legacy := applicationRecoveryMetadata{Config: ApplicationRecoveryConfig{Version: 1}, Reservation: makeOwner(91)}
	if !bytes.Equal(appendNativeRecoveryRoot(legacy), appendRecoveryRoot(legacy.Reservation)) || nativeRecoveryRootDigest(legacy) != recoveryRootDigest(legacy.Reservation) {
		t.Fatal("RS1 bytes or hash changed")
	}
	if _, err := legacy.reserveTerminal(makeOwner(1), p, l); !errors.Is(err, ErrInvalid) {
		t.Fatalf("legacy terminal capability: %v", err)
	}
	contract, err := ApplicationContractForPolicyWithRecovery(p, ApplicationControlConfig{Version: 1}, ApplicationRecoveryConfig{Version: 2})
	if err != nil || contract.Version != 4 {
		t.Fatalf("fresh terminal contract: %v", err)
	}
}

func TestApplicationViewCheckTerminalBankOwnsActualCompleteHistoricalRoster(t *testing.T) {
	var nilView *ApplicationView
	if equal, _, err := nilView.CheckTerminalBank(t.Context(), nil, ReadBudget{1, 4096}); equal || !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil owner: %v", err)
	}
	if equal, _, err := nilView.CheckRetainedRootFeed(t.Context(), 1, [32]byte{1}, 1, ReadBudget{2, 4096}); equal || !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil retained source owner: %v", err)
	}
	s := boundedControlStore(t, func(c *Config) {
		c.Recovery = ApplicationRecoveryConfig{Version: 2}
		c.Application.MaxInstallBytes = 8192
	})
	old, err := s.ApplicationView(1)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	r := ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 128, RetainedBytes: 8192, RetainedRecords: 8}}}
	verifier := recoveryAdmissionTestVerifier(func(_ context.Context, data []byte, _ *ApplicationView) (ApplicationRecoveryPurpose, error) {
		if !bytes.Equal(data, []byte("publish-terminal")) {
			return ApplicationRecoveryPurpose{}, ErrInvalid
		}
		return ApplicationRecoveryPurpose{TerminalReserve: &r}, nil
	})
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), verifier); err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 2, ent(2, 2, "publish-terminal"))
	claim, err := s.RecoveryAdmissionForCommittedEntry(t.Context(), 2)
	if err != nil {
		t.Fatal(err)
	}
	_, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.InstallApplication(2, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: 1, BaseImageHash: sha256.Sum256(image), Image: image, TerminalReserve: &r, RecoveryAdmission: claim}); err != nil {
		t.Fatal(err)
	}
	now, err := s.ApplicationView(2)
	if err != nil {
		t.Fatal(err)
	}
	defer now.Close()
	budget := ReadBudget{1, s.ApplicationLimits().MaxPageBytes}
	if equal, work, err := now.CheckTerminalBank(t.Context(), []ApplicationRecoveryReservation{r}, budget); err != nil || !equal || work.Records != 1 || work.Bytes <= 0 {
		t.Fatalf("actual native owner: %v %+v %v", equal, work, err)
	}
	if equal, _, err := now.CheckTerminalBank(t.Context(), nil, budget); err != nil || equal {
		t.Fatalf("unknown extra owner hidden: %v %v", equal, err)
	}
	if equal, _, err := old.CheckTerminalBank(t.Context(), nil, budget); err != nil || !equal {
		t.Fatalf("old bank substituted current roster: %v %v", equal, err)
	}
	if equal, _, err := old.CheckTerminalBank(t.Context(), []ApplicationRecoveryReservation{r}, budget); err != nil || equal {
		t.Fatalf("expected values minted authority: %v %v", equal, err)
	}
	if equal, _, err := now.CheckTerminalBank(t.Context(), []ApplicationRecoveryReservation{r}, ReadBudget{1, 1}); equal || !errors.Is(err, ErrLimit) || s.poison != nil {
		t.Fatalf("shared read allowance: %v", err)
	}
	if equal, work, err := now.CheckRetainedRootFeed(t.Context(), 1, sha256.Sum256(image), 1, ReadBudget{2, budget.Bytes}); err != nil || !equal || work.Records != 2 {
		t.Fatalf("actual old root and present empty feed: %v %+v %v", equal, work, err)
	}
	if equal, _, err := now.CheckRetainedRootFeed(t.Context(), 1, [32]byte{8}, 1, ReadBudget{2, budget.Bytes}); err != nil || equal {
		t.Fatalf("inert hash minted root: %v %v", equal, err)
	}
	if equal, work, err := now.CheckRetainedRootFeed(t.Context(), 2, sha256.Sum256(image), 3, budget); err != nil || !equal || work.Records != 1 {
		t.Fatalf("authenticated next feed start: %v %+v %v", equal, work, err)
	}
	if equal, _, err := now.CheckRetainedRootFeed(t.Context(), 1, sha256.Sum256(image), 1, ReadBudget{2, 256}); equal || !errors.Is(err, ErrLimit) || s.poison != nil {
		t.Fatalf("actual-frame budget preflight: %v", err)
	}
	if err := now.Close(); err != nil {
		t.Fatal(err)
	}
	if equal, _, err := now.CheckTerminalBank(t.Context(), nil, budget); equal || !errors.Is(err, ErrClosed) {
		t.Fatalf("closed owner: %v", err)
	}
}
