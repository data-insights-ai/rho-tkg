package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// This explicit small OS-child fixture is not a benchmark profile. Constructor
// exclusivity does not remove the fresh agreement's inherited range/P05 APIs.
func nativeColumnProcessProfile() (bool, error) {
	switch os.Getenv("RHO_GRAPH_DURABLE_COLUMN_PROCESS") {
	case "":
		return false, nil
	case "1":
	default:
		return false, errInvalid
	}
	if os.Getenv("RHO_GRAPH_CERTIFIED_PROCESS") != "1" || os.Getenv("RHO_GRAPH_SOURCE_PROCESS") != "1" {
		return false, errInvalid
	}
	for _, name := range []string{"RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS"} {
		if os.Getenv(name) != "" && os.Getenv(name) != "0" {
			return false, errInvalid
		}
	}
	spec, err := nativeGraphCostSpecFromEnv()
	if err != nil || spec == nil || spec.Facts > 17 || spec.Writers != 1 {
		return false, errors.Join(errInvalid, err)
	}
	return true, nil
}

func nativeColumnProcessPolicy(h *allocationHost) (nativeColumnPolicy, error) {
	if h == nil || h.machine == nil || h.machine.SemanticContractID() != durableColumnGraphSemanticContractID() {
		return nativeColumnPolicy{}, errInvalid
	}
	// Fixed finite source-authored correctness allowance, not the retain-all
	// cost profile's continuous-prefix count or a measured capacity claim.
	return nativeColumnGuardPolicy(), nil
}

// The fresh process parent must authenticate its own agreement. The ordinary
// cost read helper intentionally refuses this ID and remains unchanged.
func nativeColumnProcessOpen(t *testing.T, n *certifiedProcessNetwork, mode graphReadMode, cut graphCut) (uint64, graphCut) {
	t.Helper()
	wire, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: mode, cut: cut})
	if err != nil {
		t.Fatal(err)
	}
	response := n.all[2].call(t, processRequest{op: processSourceCertifiedOpen, data: wire})
	handle, actual, err := decodeGraphE15ReadToken(response.aux)
	if err != nil || handle == 0 || len(actual.certificates) != 2 {
		t.Fatal("fresh process actual certified cut", err)
	}
	for _, certificate := range actual.certificates {
		if certificate.scope.semantic != durableColumnGraphSemanticContractID() {
			t.Fatal("fresh process cut agreement")
		}
	}
	return handle, actual
}

func nativeColumnProcessSubmit(t *testing.T, n *certifiedProcessNetwork, spec nativeGraphCostSpec, phase, fact uint64) nativeGraphCostSample {
	t.Helper()
	if spec.Version != 1 || spec.Facts > 17 || spec.Writers != 1 || spec.validate() != nil || n == nil || n.all[1] == nil || !n.all[1].durableColumns {
		t.Fatal("fresh small process profile")
	}
	// A one-participant operation still runs through genuine cross authority;
	// no legacy local-control helper silently changes the semantic agreement.
	sample, err := nativeGraphCostClientSubmit(t.Context(), n, spec, phase, fact, true, 0, nil, nil)
	if err != nil {
		t.Fatal("fresh process original Submit", err)
	}
	return sample
}

type nativeColumnProcessRemoteError struct{ processRemoteError }

func (e nativeColumnProcessRemoteError) Unwrap() error { return e.processRemoteError }
func (e nativeColumnProcessRemoteError) Is(target error) bool {
	return e.code == 7 && target == errGraphPinExpired || e.code == 8 && target == errGraphPinReleased || e.code == 9 && (target == errGraphHistoryExpired || target == raftlog.ErrApplicationExpired)
}

// Code8 includes a local generation-closed refusal. Only a separate genuine
// original-token Open can prove that the native keeper rejects that token.
func nativeColumnProcessErrorCode(err error) byte {
	switch graphRetentionStatus(err) {
	case 1:
		return 7
	case 2:
		return 8
	case 3:
		return 9
	default:
		return 0
	}
}

// Descriptive bounded IPC evidence only. Neither these journal bytes nor a
// receipt may be used as a retained source, live pin or native cleanup grant.
type nativeColumnProcessRetentionReceipt struct {
	Version, State, Attempts byte
	Generation               [16]byte
	Pin                      [32]byte
	Original, Token          []byte
}

func (r nativeColumnProcessRetentionReceipt) validate(terminal bool) error {
	if r.Version != 1 || r.Generation == ([16]byte{}) || r.Pin == ([32]byte{}) || r.State < nativeColumnPinLive || r.State > nativeColumnRetired || r.Attempts > nativeColumnTerminalAttempts || len(r.Original)+len(r.Token) > nativeColumnMetadataBytes || sha256.Sum256(r.Original) != r.Pin || terminal && r.State != nativeColumnRetired {
		return errCorrupt
	}
	if _, err := decodeGraphPinManifest(r.Original); err != nil {
		return errors.Join(errCorrupt, err)
	}
	if len(r.Token) != 0 {
		token, err := decodeRetainedPinToken(r.Token)
		if err != nil || !bytes.Equal(token.manifest, r.Original) {
			return errors.Join(errCorrupt, err)
		}
	} else if r.State == nativeColumnPinLive {
		return errCorrupt
	}
	return nil
}

func nativeColumnProcessRetention(s *nativeColumnStore, id [16]byte) (nativeColumnProcessRetentionReceipt, error) {
	r, err := s.retentionRecord(id)
	if err != nil {
		return nativeColumnProcessRetentionReceipt{}, err
	}
	return nativeColumnProcessRetentionReceipt{1, r.State, r.TerminalAttempts, id, r.Generation.Pin, r.Original, r.Token}, nil
}

func decodeNativeColumnProcessRetirement(wire []byte) (r nativeColumnProcessRetentionReceipt, err error) {
	if len(wire) > nativeColumnMetadataBytes {
		return r, errLimit
	}
	d := json.NewDecoder(bytes.NewReader(wire))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return r, errors.Join(errCorrupt, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return r, errors.Join(errCorrupt, err)
	}
	return r, r.validate(true)
}

func (s *nativeGraphCostColumnSessions) recoverRetirement(ctx context.Context, id [16]byte) (body []byte, err error) {
	policy, err := s.policy()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.pending || s.owner != nil {
		s.mu.Unlock()
		return nil, errLimit
	}
	s.pending = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.pending = false; s.mu.Unlock() }()
	dir := filepath.Join(s.dir, "native-columns", fmt.Sprintf("%x", id))
	// This authenticates saved originals and orders native cancellation on all
	// real keepers. A copied journal or local all-absent observation cannot finish it.
	if err := retireDurablePartitionNativeColumns(ctx, dir, vfs.Default, s.coordinator, id, policy); err != nil {
		return nil, err
	}
	store, err := openNativeColumnStoreBinding(dir, vfs.Default, false, nativeColumnBinding{Policy: policy}, [32]byte{})
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	r, err := nativeColumnProcessRetention(store, id)
	if err != nil {
		return nil, err
	}
	body, err = json.Marshal(r)
	if err == nil && len(body) > nativeColumnMetadataBytes {
		return nil, errLimit
	}
	return body, err
}

func (s *nativeGraphCostColumnSessions) probeOriginalPin(ctx context.Context, id [16]byte, wire []byte) (body []byte, err error) {
	policy, err := s.policy()
	if err != nil {
		return nil, err
	}
	if len(wire) == 0 || len(wire) > nativeColumnMetadataBytes {
		return nil, errInvalid
	}
	token, err := decodeRetainedPinToken(wire)
	if err != nil {
		return nil, err
	}
	manifest, err := decodeGraphPinManifest(token.manifest)
	if err != nil || manifest.publisher.semantic != s.coordinator.host.machine.SemanticContractID() || !durableColumnGraphAgreement(manifest.publisher.semantic) || manifest.publisher.graph != s.coordinator.host.machine.ns.graph {
		return nil, errors.Join(errInvalid, err)
	}
	s.mu.Lock()
	if s.pending || s.owner != nil || s.probes == 4 {
		s.mu.Unlock()
		return nil, errLimit
	}
	s.pending = true
	s.probes++
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.pending = false; s.mu.Unlock() }()
	dir := filepath.Join(s.dir, "native-columns", fmt.Sprintf("%x", id))
	store, err := openNativeColumnStoreBinding(dir, vfs.Default, false, nativeColumnBinding{Policy: policy}, [32]byte{})
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	r, err := store.retentionRecord(id)
	if err != nil || !bytes.Equal(r.Token, wire) {
		return nil, errors.Join(errInvalid, err)
	}
	// A small four-attempt diagnostic owner bounds this process; the actual
	// native invocation grant is durably debited from the generation's existing
	// cumulative Work before constructing it. Process replacement resets no
	// durable work, and cannot spend the separately prepaid terminal reserve.
	if err := store.fundRetention(id, nil, s.coordinator.host.machine.limits); err != nil {
		return nil, err
	}
	publisher, err := newRetainedPinPublisher(s.coordinator)
	if err != nil {
		return nil, err
	}
	held, err := publisher.Open(ctx, token)
	if err != nil {
		return nil, err
	}
	if err := held.Close(); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Version byte
		Pin     [32]byte
	}{1, r.Generation.Pin})
}

// Existing cost setup helpers deliberately require the old agreement. This
// small bootstrap uses the same real action/read/packet APIs with the exact new
// agreement instead of weakening those helpers or forging recipient/grant state.
func newNativeColumnProcessNetwork(t *testing.T, spec nativeGraphCostSpec) *certifiedProcessNetwork {
	t.Helper()
	wire, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(os.Getenv("RHO_GRAPH_PROCESS_ARTIFACT"), "rho-native-durable-columns-")
	if err != nil {
		t.Fatal(err)
	}
	n := &certifiedProcessNetwork{all: make(map[uint64]*graphProcess), dir: dir, leaders: [2]uint64{1, 4}}
	t.Log("durable native column process directories", dir)
	for id := uint64(1); id <= 6; id++ {
		n.all[id] = startGraphProcess(t, filepath.Join(n.dir, fmt.Sprint(id)), id, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_DURABLE_COLUMN_PROCESS=1", "RHO_GRAPH_COST_SPEC="+string(wire))
	}
	attachNativeColumnProcessNetwork(t, n, spec)
	t.Cleanup(func() {
		for _, p := range n.all {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := p.shutdown(ctx, t.Failed())
			cancel()
			if err != nil && !t.Failed() {
				t.Error(err)
			}
		}
	})
	for g, leader := range n.leaders {
		n.groups[g].event(t, leader, processRequest{op: processCampaign})
	}
	nativeColumnProcessAction(t, n, 1, certifiedInitialize, 3, nil)
	n.read(t, 1, observeConfiguration, 0)
	nativeColumnProcessAction(t, n, 1, certifiedInitialize, 8, nil)
	for _, r := range []struct{ actor, recipient, partition uint64 }{{1, 1, 3}, {4, 2, 8}} {
		n.read(t, r.actor, observeConfiguration, 0)
		nativeColumnProcessAction(t, n, r.actor, certifiedBeginRecipient, r.recipient, nil)
		n.read(t, 1, observeRecipient, r.recipient)
		nativeColumnProcessAction(t, n, 1, certifiedRecipientTransition, 3, []byte{byte(fenceDeclaredRecipient)})
		if r.partition != 3 {
			nativeColumnProcessAction(t, n, 1, certifiedRecipientTransition, r.partition, []byte{byte(fenceDeclaredRecipient)})
		}
		n.read(t, r.actor, observeRecipient, r.recipient)
		nativeColumnProcessAction(t, n, r.actor, certifiedRecipientTransition, 3, []byte{byte(acknowledgeDeclaredHome)})
		n.read(t, 1, observeRecipient, r.recipient)
		nativeColumnProcessAction(t, n, 1, certifiedRecipientTransition, 3, []byte{byte(activateDeclaredRecipient)})
		n.read(t, 1, observeRecipient, r.recipient)
		nativeColumnProcessAction(t, n, 1, certifiedRecipientTransition, r.partition, []byte{byte(publishDeclaredRecipient)})
		n.read(t, r.actor, observeRecipient, r.recipient)
		nativeColumnProcessAction(t, n, r.actor, certifiedOpenRecipient, 0, nil)
		n.read(t, 1, observeRecipient, r.recipient)
		n.read(t, 1, observeAllocator, 0)
		count := spec.homeCount()
		if r.partition == 8 {
			count = 2
		}
		input := graphE17GrantInput(1, count)
		nativeColumnProcessAction(t, n, 1, certifiedReserve, 0, input)
		n.group(1).event(t, 1, processRequest{op: processCertifiedRead, entity: uint64(observeGrant), data: input})
		if !n.all[1].call(t, processRequest{op: processStateOp}).ready {
			t.Fatal("actual fresh-profile grant observation incomplete")
		}
		nativeColumnProcessAction(t, n, 1, certifiedInstallGrant, r.partition, nil)
	}
	return n
}

func attachNativeColumnProcessNetwork(t *testing.T, n *certifiedProcessNetwork, spec nativeGraphCostSpec) {
	t.Helper()
	attachGraphProcessMuxesWithLimit(t, n.all, spec.deliveries())
	n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: n.all[1], 2: n.all[2], 3: n.all[3]}}
	n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: n.all[4], 5: n.all[5], 6: n.all[6]}}
}

func nativeColumnProcessAction(t *testing.T, n *certifiedProcessNetwork, actor, kind, index uint64, data []byte) {
	t.Helper()
	r := n.all[actor].call(t, processRequest{op: processCertifiedAction, entity: kind, index: index, data: data})
	if kind == certifiedOpenRecipient {
		if len(r.aux) != 0 {
			t.Fatal("recipient opened with proposal")
		}
		return
	}
	p, err := decodeCertifiedProcessProposal(r.aux)
	if err != nil || p.target.semantic != durableColumnGraphSemanticContractID() {
		t.Fatal("fresh native source proposal", err)
	}
	g := 0
	if p.target.partition == 8 {
		g = 1
	} else if p.target.partition != 3 {
		t.Fatal("foreign column bootstrap partition")
	}
	leader := n.leaders[g]
	n.groups[g].event(t, leader, processRequest{op: processPropose, data: r.aux})
	state := n.all[leader].call(t, processRequest{op: processStateOp})
	record := n.all[leader].call(t, processRequest{op: processEntryRecord, index: state.applied})
	ns, _ := certifiedProcessNamespace(leader)
	o, err := decodeDeclaredOutcome(record.outcome, ns)
	if err != nil || o.index != state.applied || o.hash != sha256.Sum256(p.wire) || o.reason != reasonNone || o.disposition != applied && (kind != certifiedInstallGrant || o.disposition != controlRecovery) {
		t.Fatal("actual fresh-profile bootstrap original", o, err)
	}
	for _, peer := range n.groups[g].nodes {
		actual := peer.call(t, processRequest{op: processEntryRecord, index: state.applied})
		if !bytes.Equal(actual.outcome, record.outcome) || !bytes.Equal(actual.changes, record.changes) {
			t.Fatal("fresh-profile voter originals differ")
		}
	}
}

func restartNativeColumnProcesses(t *testing.T, n *certifiedProcessNetwork, spec nativeGraphCostSpec, stream *json.Encoder) {
	t.Helper()
	var before [6]processResponse
	for id := uint64(1); id <= 6; id++ {
		before[id-1] = n.all[id].call(t, processRequest{op: processStateOp})
	}
	if nativeGraphCostStop(t, n, 1, stream) != 6 {
		t.Fatal("incomplete original cohort reap")
	}
	wire, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	n.all = make(map[uint64]*graphProcess)
	for id := uint64(1); id <= 6; id++ {
		n.all[id] = startGraphProcess(t, filepath.Join(n.dir, fmt.Sprint(id)), id, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_DURABLE_COLUMN_PROCESS=1", "RHO_GRAPH_PROCESS_REOPEN=1", "RHO_GRAPH_COST_SPEC="+string(wire))
	}
	attachNativeColumnProcessNetwork(t, n, spec)
	for id := uint64(1); id <= 6; id++ {
		actual, old := n.all[id].call(t, processRequest{op: processStateOp}), before[id-1]
		if actual.incarnation == old.incarnation || actual.applied != old.applied || !bytes.Equal(actual.image, old.image) || actual.stats[statViews] != 0 || actual.stats[statPinnedBytes] != 0 {
			t.Fatal("actual pre-election same-directory restore differs", id)
		}
	}
	for g, leader := range n.leaders {
		n.groups[g].event(t, leader, processRequest{op: processCampaign})
		n.read(t, leader, observeConfiguration, 0)
	}
}
