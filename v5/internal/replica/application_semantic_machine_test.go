package replica

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

type semanticMachineCalls struct{ provider, restore, stage int }
type bindingMachine struct {
	calls *semanticMachineCalls
	id    raftlog.ApplicationSemanticContractID
}

func (m *bindingMachine) SemanticContractID() raftlog.ApplicationSemanticContractID {
	m.calls.provider++
	return m.id
}
func (m *bindingMachine) Restore(uint64, []byte) error { m.calls.restore++; return nil }
func (m *bindingMachine) Stage(Entry, raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	m.calls.stage++
	return raftlog.ApplicationBatch{}, nil
}

type legacyBindingMachine struct{ calls *semanticMachineCalls }

func (m *legacyBindingMachine) Restore(uint64, []byte) error { m.calls.restore++; return nil }
func (m *legacyBindingMachine) Stage(Entry, raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	m.calls.stage++
	return raftlog.ApplicationBatch{}, nil
}

func TestValidateApplicationMachineBindingDirect(t *testing.T) {
	binding := raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 7, Group: [16]byte{3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{9}}
	for _, name := range []string{"legacy", "legacy-provider", "matching", "missing-provider", "zero-provider", "mismatch", "nil-legacy", "nil-bound", "typed-nil-legacy", "typed-nil-bound", "identity-only", "semantic-only", "missing-graph", "missing-partition", "missing-group"} {
		t.Run(name, func(t *testing.T) {
			calls := &semanticMachineCalls{}
			b := binding
			var machine ApplicationMachine = &bindingMachine{calls: calls, id: binding.SemanticContractID}
			wantErr, wantProvider := true, 0
			switch name {
			case "legacy":
				b = raftlog.ApplicationBinding{}
				machine = &legacyBindingMachine{calls}
				wantErr = false
			case "legacy-provider":
				b = raftlog.ApplicationBinding{}
				wantErr = false
			case "matching":
				wantErr = false
				wantProvider = 1
			case "missing-provider":
				machine = &legacyBindingMachine{calls}
			case "zero-provider":
				machine = &bindingMachine{calls: calls}
				wantProvider = 1
			case "mismatch":
				machine = &bindingMachine{calls: calls, id: raftlog.ApplicationSemanticContractID{8}}
				wantProvider = 1
			case "nil-legacy":
				b = raftlog.ApplicationBinding{}
				machine = nil
			case "nil-bound":
				machine = nil
			case "typed-nil-legacy":
				b = raftlog.ApplicationBinding{}
				machine = (*bindingMachine)(nil)
			case "typed-nil-bound":
				machine = (*bindingMachine)(nil)
			case "identity-only":
				b.SemanticContractID = raftlog.ApplicationSemanticContractID{}
			case "semantic-only":
				b.Identity = raftlog.ApplicationIdentity{}
			case "missing-graph":
				b.Identity.Graph = [16]byte{}
			case "missing-partition":
				b.Identity.Partition = 0
			case "missing-group":
				b.Identity.Group = [16]byte{}
			}
			before := b
			err := ValidateApplicationMachineBinding(b, machine)
			if wantErr && !errors.Is(err, ErrInvalid) || !wantErr && err != nil {
				t.Fatal(err)
			}
			if b != before || *calls != (semanticMachineCalls{provider: wantProvider}) {
				t.Fatal("binding or callbacks changed", b, *calls)
			}
			if name == "identity-only" || name == "semantic-only" || name == "missing-graph" || name == "missing-partition" || name == "missing-group" {
				if !errors.Is(err, raftlog.ErrInvalid) {
					t.Fatal("binding sentinel lost", err)
				}
			}
		})
	}
}
