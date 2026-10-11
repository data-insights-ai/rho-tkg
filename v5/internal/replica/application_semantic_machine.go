package replica

import (
	"context"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// ApplicationSemanticProvider reports the machine's explicit database-meaning
// agreement. It grants no lifetime, coverage, cut or traffic authority.
type ApplicationSemanticProvider interface {
	SemanticContractID() raftlog.ApplicationSemanticContractID
}

// ValidateApplicationMachineBinding checks capability before Restore or Stage.
// Exact zero preserves nonnil legacy machines without a semantic provider. Any
// partial/nonzero binding must validate and match a nonzero provider ID. This
// pure check neither restores state nor authorizes replicated application traffic.
func ValidateApplicationMachineBinding(binding raftlog.ApplicationBinding, machine ApplicationMachine) error {
	if isNilMachine(machine) {
		return ErrInvalid
	}
	if binding == (raftlog.ApplicationBinding{}) {
		return nil
	}
	if err := binding.Validate(); err != nil {
		return errors.Join(ErrInvalid, err)
	}
	provider, ok := machine.(ApplicationSemanticProvider)
	if !ok || provider.SemanticContractID() != binding.SemanticContractID {
		return ErrInvalid
	}
	return nil
}

// ApplicationAuditBudget bounds ONE logical operation, not the entire journal.
// The auditor owns accounting within its loan; Driver retains its own owners.
type ApplicationAuditBudget struct{ WorkBytes, OwnedBytes uint64 }

// ApplicationSnapshotContext is derived from the actual checked Driver input.
// It carries no protobuf, caller-selected bank, readiness or activation flag.
type ApplicationSnapshotContext struct {
	Index, Term                       uint64
	Voters                            [3]uint64
	DescriptorHash, CutID, ManifestID [32]byte
}

// ApplicationSnapshotAuditor starts a bounded audit of a genuine prepared snapshot.
type ApplicationSnapshotAuditor interface {
	BeginApplicationSnapshotAudit(*raftlog.PreparedApplicationSnapshot, ApplicationSnapshotContext, ApplicationAuditBudget) (ApplicationSnapshotAudit, error)
}

// ApplicationSnapshotAudit verifies a prepared snapshot through bounded phases.
// Finish releases readers/pages/large ownership and retains ONLY fixed sealed
// candidate/auditor state<=4096 logical bytes. Claim and actual Ready still
// authenticate activation; a successful Finish is not readiness. Close discards
// unused evidence and closes each owner once, preserving original errors.
type ApplicationSnapshotAudit interface {
	Advance(context.Context, ApplicationAuditBudget) (bool, error)
	Finish(context.Context, ApplicationAuditBudget) error
	Close() error
}

// ApplicationCleanup owns callback resources released outside Driver.mu.
type ApplicationCleanup interface{ Close() error }

// ApplicationCleanupProvider detaches ONE bounded aggregate owner without I/O
// or allocation. Stage and Restore must not close owners under Driver.mu.
// Each callback's
// existing operation ledger includes this owner and its release reservation.
type ApplicationCleanupProvider interface{ TakeApplicationCleanup() ApplicationCleanup }

func takeApplicationCleanup(m ApplicationMachine) ApplicationCleanup {
	if provider, ok := m.(ApplicationCleanupProvider); ok && !isNilMachine(provider) {
		return provider.TakeApplicationCleanup()
	}
	return nil
}
func closeApplicationCleanup(owner ApplicationCleanup) error {
	if isNilMachine(owner) {
		return nil
	}
	return owner.Close()
}

// ApplicationCallbackLimits is a compiled module's immutable logical envelope,
// not caller-requested admission or heap/RSS. Reporting performs no I/O/mutation.
// Work covers its complete reads/hashes/emission; owned covers retained and
// simultaneous callback/cleanup owners. Driver funds its own buffers separately.
type ApplicationCallbackLimits struct {
	StageWork, StageOwned, RestoreWork, RestoreOwned, CleanupWork, CleanupOwned uint64
}

// ApplicationResourceProvider reports immutable, compiled callback limits.
type ApplicationResourceProvider interface {
	ApplicationCallbackLimits() ApplicationCallbackLimits
}

func applicationSum(values ...uint64) (uint64, error) {
	var sum uint64
	for _, n := range values {
		if n > math.MaxUint64-sum {
			return 0, ErrLimit
		}
		sum += n
	}
	return sum, nil
}
func applicationProduct(a, b uint64) (uint64, error) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, ErrLimit
	}
	return a * b, nil
}
func applicationCallbackProfile(machine ApplicationMachine) (ApplicationCallbackLimits, error) {
	_, audit := machine.(ApplicationSnapshotAuditor)
	_, cleanup := machine.(ApplicationCleanupProvider)
	if !audit && !cleanup {
		return ApplicationCallbackLimits{}, nil
	}
	provider, ok := machine.(ApplicationResourceProvider)
	if !ok || isNilMachine(provider) {
		return ApplicationCallbackLimits{}, ErrInvalid
	}
	c := provider.ApplicationCallbackLimits()
	fields := [6]uint64{c.StageWork, c.StageOwned, c.RestoreWork, c.RestoreOwned, c.CleanupWork, c.CleanupOwned}
	for _, n := range fields {
		if n == 0 {
			return ApplicationCallbackLimits{}, ErrInvalid
		}
	}
	if _, err := applicationSum(fields[:]...); err != nil {
		return ApplicationCallbackLimits{}, err
	}
	return c, nil
}
