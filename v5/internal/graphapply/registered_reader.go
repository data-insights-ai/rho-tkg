package graphapply

import (
	"bytes"
	"context"
	"errors"
	"slices"

	"go.etcd.io/raft/v3"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Logical work/owned representation allowances; never physical memory or RSS.
type registeredOperationLimits struct{ sourceBytes, outputBytes int }

func (l registeredOperationLimits) reserve(s *raftlog.Store, extra int) error {
	if s == nil || extra < 0 || l.sourceBytes < 1 || l.outputBytes < 1 || l.sourceBytes > 8<<20 || l.outputBytes > 8<<20 {
		return errInvalid
	}
	if extra > l.sourceBytes || extra > l.outputBytes {
		return errLimit
	}
	p := s.ApplicationLimits()
	if !p.Enabled() || p.MaxImageBytes < 140 || p.MaxImageBytes > 64<<20 {
		return errInvalid
	}
	// Before Checkpoint/View: reserve configured image maximum for checkpoint,
	// current view and retained source view, plus6 separate140-byte copies/scans.
	//8192 fixed metadata includes two source DTOs, codecs, roots/ConfState/scratch.
	// Three separately charged Term inspections cover entry, original S and source1.
	// ConfState clone: pinned64KiB metadata cap, at most one uint64 per encoded
	// byte (8-byte owned fields), plus separately bounded unknown bytes.
	// Read4096, pure codecs and all output capacities are reserved pessimistically.
	for _, limit := range [2]int{l.sourceBytes, l.outputBytes} {
		remaining := limit
		for _, charge := range [11]int{65536*8 + 65536, 8192, p.MaxImageBytes, p.MaxImageBytes, p.MaxImageBytes, 6*140 + extra, s.Limits().MaxEntryBytes, s.Limits().MaxEntryBytes, s.Limits().MaxEntryBytes, 172 + 4096 + 6939 + 4086 + 3340 + 3059, 64 + 47 + 378 + 145} {
			if charge > remaining {
				return errLimit
			}
			remaining -= charge
		}
	}
	return nil
}

func registeredCurrentState(s *raftlog.Store, l registeredOperationLimits, extra int) (base raftlog.ApplicationRoot, root graphstore.Root, origin registeredOrigin, err error) {
	defer func() {
		if err != nil {
			base = raftlog.ApplicationRoot{}
			root = graphstore.Root{}
			origin = registeredOrigin{}
		}
	}()
	if err = l.reserve(s, extra); err != nil {
		return base, root, origin, err
	}
	index, image, err := s.Checkpoint()
	if err != nil {
		return base, root, origin, err
	}
	if len(image) != 140 || !bytes.Equal(image[:4], []byte{'G', 'R', 2, 2}) {
		return base, root, origin, errCorrupt
	}
	v, err := s.ApplicationView(index)
	if err != nil {
		return base, root, origin, err
	}
	defer func() { err = errors.Join(err, v.Close()) }()
	binding, err := v.ApplicationBinding(context.Background())
	if err != nil {
		return base, root, origin, err
	}
	if binding.Validate() != nil || binding.SemanticContractID != registeredSemanticContractID() || s.ApplicationControls().Version != 1 {
		return base, root, origin, errInvalid
	}
	base, err = v.RootBounded(context.Background(), 140)
	if err != nil {
		return base, root, origin, err
	}
	proof, err := v.ProveNoApplicationData(context.Background())
	if err != nil {
		return base, root, origin, err
	}
	if !sameBase(base, proof) || !bytes.Equal(image, base.Image) || len(base.Image) != 140 || !bytes.Equal(base.Image[:4], []byte{'G', 'R', 2, 2}) {
		return base, root, origin, errCorrupt
	}
	root, err = graphstore.DecodeRoot(base.Image)
	if err != nil {
		return base, root, origin, err
	}
	topology, err := root.SinglePartition()
	if err != nil {
		return base, root, origin, err
	}
	n := root.Namespace()
	expected, err := graphstore.NewRoot(n, root.OwnershipEpoch())
	if err != nil {
		return base, root, origin, err
	}
	if [16]byte(n.Graph) != binding.Identity.Graph || n.Partition != binding.Identity.Partition || root.OwnershipEpoch() == 0 || topology.TopologyEpoch != 1 || topology.SchemaVersion != 1 || topology.IndexVersion != 0 || root.SemanticEpoch() != 0 || root.NextPhysicalID() != 1 || root.EffectDigest() != expected.EffectDigest() {
		return base, root, origin, errCorrupt
	}
	origin, err = readRegisteredOrigin(v, s, base, root, binding)
	return base, root, origin, err
}

func registeredCurrentRoot(s *raftlog.Store, l registeredOperationLimits, extra int) (raftlog.ApplicationRoot, graphstore.Root, error) {
	base, root, origin, err := registeredCurrentState(s, l, extra)
	if err == nil && origin.index != 0 {
		err = errInvalid
	}
	if err != nil {
		return raftlog.ApplicationRoot{}, graphstore.Root{}, err
	}
	return base, root, nil
}

func registeredKnownTerm(s *raftlog.Store, index, term uint64) error {
	actual, err := s.Term(index)
	if err == nil {
		if actual != term {
			return errCorrupt
		}
		return nil
	}
	if !errors.Is(err, raft.ErrCompacted) {
		return err
	}
	first, e := s.FirstIndex()
	if e != nil {
		return e
	}
	if index >= first {
		return errCorrupt
	}
	return nil
}

func readRegisteredOrigin(v *raftlog.ApplicationView, s *raftlog.Store, base raftlog.ApplicationRoot, root graphstore.Root, binding raftlog.ApplicationBinding) (out registeredOrigin, err error) {
	defer func() {
		if err != nil {
			out = registeredOrigin{}
		}
	}()
	key, _ := registeredGenesisIdentity(registeredGenesisSource{Scope: binding.Identity})
	record, found, _, err := v.GetControl(context.Background(), key[:], raftlog.ReadBudget{Rows: 2, Bytes: 4096})
	if err != nil {
		return out, err
	}
	usage, err := s.ApplicationControlUsage()
	if err != nil {
		return out, err
	}
	if !found {
		if usage.Bytes != 0 || usage.Records != 0 {
			return out, errCorrupt
		}
		return out, nil
	}
	out, err = decodeRegisteredOrigin(record.Value, registeredJournalBudget{6939})
	if err != nil {
		return out, err
	}
	physical := uint64(len(key) + bytes.Count(key[:], []byte{0}) + 11 + 36 + 378) // #nosec G115 -- key is [47]byte; zero count0..47 bounds subtotal472..519 before widening.
	if record.Index != out.index || out.index > base.Index || !bytes.Equal(record.Key, key[:]) || usage.Records != 1 || usage.Bytes != physical {
		return out, errCorrupt
	}
	source := out.source
	if source.Scope != binding.Identity || source.Meaning != [32]byte(binding.SemanticContractID) || source.ImageHash != base.ImageHash || source.InitialOwnerEpoch != root.OwnershipEpoch() {
		return out, errCorrupt
	}
	_, cs, err := s.InitialState()
	if err != nil {
		return out, err
	}
	if cs == nil || len(cs.ProtoReflect().GetUnknown()) != 0 || len(cs.GetVoters()) != 3 || len(cs.GetVotersOutgoing()) != 0 || len(cs.GetLearners()) != 0 || len(cs.GetLearnersNext()) != 0 || cs.GetAutoLeave() || !slices.Equal(cs.GetVoters(), source.Voters[:]) || !slices.Contains(cs.GetVoters(), s.ApplicationLimits().LocalVoter) {
		return out, errCorrupt
	}
	contract, err := raftlog.ApplicationContractForPolicyWithControls(s.ApplicationLimits(), s.ApplicationControls())
	if err != nil {
		return out, err
	}
	actual := [10]int{int(contract.Version), contract.MaxKeyBytes, contract.MaxValueBytes, contract.MaxImageBytes, contract.MaxPageRows, contract.MaxPageBytes, contract.MaxInstallWrites, contract.MaxInstallBytes, contract.MaxChangeBytes, contract.MaxOutcomeBytes}
	for i, n := range actual {
		if n < 1 || n > 64<<20 || uint64(n) != source.SharedContract[i] {
			return out, errCorrupt
		}
	} // #nosec G115 -- each positive bounded actual contract field is checked before widening.
	if err = registeredKnownTerm(s, out.index, out.term); err != nil {
		return out, err
	}
	if err = registeredKnownTerm(s, source.SourcePosition, source.SourceTerm); err != nil {
		return out, err
	}
	retained, err := s.ApplicationView(source.SourcePosition)
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, retained.Close()) }()
	initial, err := retained.RootBounded(context.Background(), 140)
	if err != nil {
		return out, err
	}
	if initial.ImageHash != source.ImageHash || !bytes.Equal(initial.Image, base.Image) {
		return out, errCorrupt
	}
	return out, nil
}

func captureRegisteredGenesisSource(s *raftlog.Store, l registeredOperationLimits) (source registeredGenesisSource, err error) {
	base, root, err := registeredCurrentRoot(s, l, 0)
	if err != nil {
		return source, err
	}
	hard, cs, err := s.InitialState()
	if err != nil {
		return source, err
	}
	first, err := s.FirstIndex()
	if err != nil {
		return source, err
	}
	last, err := s.LastIndex()
	if err != nil {
		return source, err
	}
	term, err := s.Term(base.Index)
	if err != nil {
		return source, err
	}
	if base.Index != 1 || term != 1 || hard == nil || hard.GetTerm() != 1 || hard.GetCommit() != 1 || hard.GetVote() != 0 || first != 2 || last != 1 || cs == nil || len(cs.ProtoReflect().GetUnknown()) != 0 || len(cs.GetVoters()) != 3 || len(cs.GetVotersOutgoing()) != 0 || len(cs.GetLearners()) != 0 || len(cs.GetLearnersNext()) != 0 || cs.GetAutoLeave() || !slices.Contains(cs.GetVoters(), s.ApplicationLimits().LocalVoter) {
		return source, errInvalid
	}
	v, err := s.ApplicationView(1)
	if err != nil {
		return source, err
	}
	defer func() {
		if e := v.Close(); e != nil {
			source = registeredGenesisSource{}
			err = errors.Join(err, e)
		}
	}()
	retained, err := v.RootBounded(context.Background(), 140)
	if err != nil {
		return source, err
	}
	if !sameBase(base, retained) {
		return source, errInvalid
	}
	binding, err := v.ApplicationBinding(context.Background())
	if err != nil {
		return source, err
	}
	contract, err := raftlog.ApplicationContractForPolicyWithControls(s.ApplicationLimits(), s.ApplicationControls())
	if err != nil {
		return source, err
	}
	source = registeredGenesisSource{Scope: binding.Identity, SourceKind: 1, SourcePosition: base.Index, SourceTerm: term, ImageLength: 140, ImageHash: retained.ImageHash, InitialOwnerEpoch: root.OwnershipEpoch(), Meaning: [32]byte(binding.SemanticContractID)}
	copy(source.Voters[:], cs.GetVoters())
	source.SharedContract[0] = uint64(contract.Version)
	numbers := [9]int{contract.MaxKeyBytes, contract.MaxValueBytes, contract.MaxImageBytes, contract.MaxPageRows, contract.MaxPageBytes, contract.MaxInstallWrites, contract.MaxInstallBytes, contract.MaxChangeBytes, contract.MaxOutcomeBytes}
	for i, n := range numbers {
		if n < 1 || n > 64<<20 {
			return registeredGenesisSource{}, errInvalid
		}
		source.SharedContract[i+1] = uint64(n) // #nosec G115 -- positive actual contract fields bounded64MiB before widening.
	}
	if err := validateRegisteredGenesisSource(source); err != nil {
		return registeredGenesisSource{}, err
	}
	return source, nil
}
