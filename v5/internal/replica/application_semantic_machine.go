package replica

import (
	"errors"

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
