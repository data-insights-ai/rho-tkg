package raftlog

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Stock public Ready/import fixture only: no Driver, quorum or live cohort.
func TestPreparedApplicationContractFirstOwnerContract(t *testing.T) {
	s, p, snapshot := preparedReadFixture(t, 4, nil)
	v, err := p.OpenReadView(t.Context(), snapshot.GetMetadata().GetIndex(), 8)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	}()
	root, err := v.RootBounded(t.Context(), 8)
	if err != nil || root.Index != 3 || !bytes.Equal(root.Image, []byte("r3")) {
		t.Fatal("actual prepared read-view/root baseline", err)
	}
	want := ApplicationContract{Version: 2, MaxKeyBytes: 1024, MaxValueBytes: 1 << 20, MaxImageBytes: 8, MaxPageRows: 4096, MaxPageBytes: 2 << 20, MaxInstallWrites: 4096, MaxInstallBytes: 4 << 20, MaxChangeBytes: 1 << 20, MaxOutcomeBytes: 64 << 10}
	owner, err := ApplicationContractForPolicyWithControls(s.ApplicationLimits(), s.ApplicationControls())
	if err != nil || owner != want || p.s != s || p.self != p || p.readers != 1 || p.claim != nil {
		t.Fatal("complete actual owner contract/config baseline", err)
	}
	metadata, err := encodeMeta(s.meta)
	if err != nil {
		t.Fatal(err)
	}
	usage, err := s.ApplicationTransferUsage()
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.ApplicationUsage()
	if err != nil || app.Views != 1 || usage.Prepared != 1 || usage.Claims != 0 {
		t.Fatal("actual view/pin baseline", err)
	}
	t.Log("PREPARED_OWNER_VIEW_CONTRACT_USAGE_BASELINE_PASS")
	got, err := p.ApplicationContract(t.Context())
	if err != nil || got != want {
		t.Fatalf("PREPARED_VALID_OWNER_CONTRACT_UNAVAILABLE: got=%+v error=%v", got, err)
	}
	got.MaxImageBytes = 1
	again, err := p.ApplicationContract(t.Context())
	if err != nil || again != want {
		t.Fatal("returned contract mutation changed owner", err)
	}
	metadataAfter, err := encodeMeta(s.meta)
	if err != nil || !bytes.Equal(metadata, metadataAfter) || p.readers != 1 || p.claim != nil {
		t.Fatal("metadata/readers/claim mutated", err)
	}
	usageAfter, err := s.ApplicationTransferUsage()
	if err != nil || usageAfter != usage {
		t.Fatal("transfer pin/owner usage mutated", err)
	}
	appAfter, err := s.ApplicationUsage()
	if err != nil || appAfter != app {
		t.Fatal("application view usage mutated", err)
	}
}

// Both peers receive the same changed shared policy BEFORE Open/import.
func preparedContractLegacyPolicy(t *testing.T) (*Store, *PreparedApplicationSnapshot) {
	t.Helper()
	open := func(voter uint64) *Store {
		policy, transfer := transferConfig(voter)
		policy.MaxImageBytes, policy.MaxOutcomeBytes = 16, 512
		transfer.Contract = ApplicationContractForPolicy(policy)
		cfg := Config{Dir: "db", FS: vfs.NewMem(), Create: true, Application: policy, Transfer: transfer, Generations: ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: ApplicationPublishedCutLimits{MaxTransferChunks: 10000}, Replication: ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}}
		store, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		if err := store.Initialize([]uint64{1, 2, 3}, []byte("initial")); err != nil {
			t.Fatal(err)
		}
		return store
	}
	donor, receiver := open(2), open(1)
	generationApply(t, donor, "incoming", KV{Key: []byte("a"), Value: []byte("incoming")})
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := donor.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := donor.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := export.Close(); err != nil {
			t.Error(err)
		}
	}()
	manifest := buildPublished(t, export, ReadBudget{Rows: 1, Bytes: 4096})
	if manifest.Version != 2 {
		t.Fatal("actual legacy AS2 expected", manifest.Version)
	}
	p, _ := semanticPrepared(t, receiver, manifest, collectPublished(t, export, ReadBudget{Rows: 1, Bytes: 4096}))
	return receiver, p
}

func TestPreparedApplicationContractOwnerPolicies(t *testing.T) {
	legacy, oldPrepared := preparedContractLegacyPolicy(t)
	controls, newPrepared, _ := preparedReadFixture(t, 4, nil)
	want := ApplicationContract{Version: 1, MaxKeyBytes: 1024, MaxValueBytes: 1 << 20, MaxImageBytes: 16, MaxPageRows: 4096, MaxPageBytes: 2 << 20, MaxInstallWrites: 4096, MaxInstallBytes: 4 << 20, MaxChangeBytes: 1 << 20, MaxOutcomeBytes: 512}
	got, err := oldPrepared.ApplicationContract(t.Context())
	if err != nil || got != want || oldPrepared.s != legacy {
		t.Fatal("complete matched AS2 owner policy", got, err)
	}
	want.Version, want.MaxImageBytes, want.MaxOutcomeBytes = 2, 8, 64<<10
	got, err = newPrepared.ApplicationContract(t.Context())
	if err != nil || got != want || newPrepared.s != controls || controls == legacy {
		t.Fatal("complete distinct AS4 actual owner policy", got, err)
	}
}

func TestPreparedApplicationContractGuardsAndClaim(t *testing.T) {
	s, p, snapshot := preparedReadFixture(t, 4, nil)
	check := func(owner *PreparedApplicationSnapshot, ctx context.Context, want error) {
		t.Helper()
		got, err := owner.ApplicationContract(ctx)
		if !errors.Is(err, want) || got != (ApplicationContract{}) {
			t.Fatal("guard whole-zero", got, err, want)
		}
	}
	copied := *p
	missingImport := *p
	missingImport.self, missingImport.i = &missingImport, nil
	missingStore := *p
	missingStore.self, missingStore.s = &missingStore, nil
	for _, bad := range []*PreparedApplicationSnapshot{nil, {}, &copied, &missingImport, &missingStore} {
		check(bad, t.Context(), ErrInvalid)
	}
	check(p, nil, ErrInvalid)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	check(p, ctx, context.Canceled)
	func() {
		s.poison = ErrCorrupt // Existing stock static poison refusal; no failing IO.
		defer func() { s.poison = nil }()
		got, err := p.ApplicationContract(t.Context())
		if !errors.Is(err, ErrPoisoned) || !errors.Is(err, ErrCorrupt) || got != (ApplicationContract{}) {
			t.Fatal("poison marker/cause and whole-zero", got, err)
		}
	}()
	claim, err := s.ClaimPreparedApplicationSnapshot(p, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := claim.Close(); err != nil {
			t.Error(err)
		}
	}()
	before, err := s.ApplicationTransferUsage()
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := readyMetadataFingerprint(t, s)
	want := ApplicationContractForPolicy(s.ApplicationLimits())
	want.Version = 2
	got, err := p.ApplicationContract(t.Context())
	if err != nil || got != want || p.claim != claim || p.readers != 0 {
		t.Fatal("metadata read while genuine claim live", got, err)
	}
	after, err := s.ApplicationTransferUsage()
	if err != nil || after != before || readyMetadataFingerprint(t, s) != fingerprint {
		t.Fatal("live claim metadata/owners mutated", err)
	}
	// Stock public Ready unit activation only; never quorum or Driver evidence.
	if _, err := s.PersistApplicationReady(raft.Ready{Snapshot: snapshot, HardState: &pb.HardState{Term: new(snapshot.GetMetadata().GetTerm()), Commit: new(snapshot.GetMetadata().GetIndex())}}, claim); err != nil {
		t.Fatal(err)
	}
	check(p, t.Context(), ErrClosed)
}

func TestPreparedApplicationContractClosedAndMetadataLayout(t *testing.T) {
	_, p, _ := preparedReadFixture(t, 3, nil)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := p.ApplicationContract(t.Context()); !errors.Is(err, ErrClosed) || got != (ApplicationContract{}) {
		t.Fatal("closed owner whole-zero", got, err)
	}
	if unsafe.Sizeof(ApplicationContract{}) != 80 || unsafe.Sizeof([10]uint64{}) != 80 {
		t.Fatal("complete metadata widths")
	}
	// Conservative declared-value phases, never ABI copy-elision/heap/RSS proof.
	common := 6*unsafe.Sizeof(ApplicationContract{}) + unsafe.Sizeof(ApplicationControlConfig{}) + unsafe.Sizeof(context.Context(nil)) + 4*unsafe.Sizeof(error(nil)) + 3*unsafe.Sizeof((*Store)(nil)) + 64
	emission := common + 2*unsafe.Sizeof(ApplicationPolicy{})
	validation := common + unsafe.Sizeof(ApplicationPolicy{}) + unsafe.Sizeof([]uint64{}) + unsafe.Sizeof([10]uint64{})
	if emission > 1024 || validation > 1024 {
		t.Fatal("caller/callee/return/validation/numbers metadata phases", emission, validation)
	}
	t.Log("metadata logical1024 phases", emission, validation)
}
