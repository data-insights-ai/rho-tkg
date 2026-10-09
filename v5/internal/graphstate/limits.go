package graphstate

import (
	"cmp"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

// Limits bounds transaction-local work, never total lifetime/history size.
// A large history remains page-readable; a large transaction explicitly errors.
// Byte ledgers count conservative fixed record headers, variable names, canonical
// scopes/values and axis definitions. They are not Go heap/RSS measurements;
// embedding storage independently budgets decoded pages and referenced payloads.
type Limits struct {
	Component                                                                                    state.Limits
	MaxOperations, MaxPages, MaxRows, MaxReadBytes, MaxDeltaBytes, MaxDependencies, MaxNameBytes int
}

// DefaultLimits returns provisional bounded planning/materialization policy.
func DefaultLimits() Limits {
	return Limits{Component: state.DefaultLimits(), MaxOperations: 4096, MaxPages: 4096, MaxRows: 65536, MaxReadBytes: 16 << 20, MaxDeltaBytes: 16 << 20, MaxDependencies: 16384, MaxNameBytes: 256}
}

// Validate checks overrides before any reader calls or working-map allocation.
func (l Limits) Validate() error { _, err := l.resolve(); return err }
func (l Limits) resolve() (Limits, error) {
	if err := l.Component.Validate(); err != nil {
		return Limits{}, err
	}
	d := DefaultLimits()
	l.MaxOperations = cmp.Or(l.MaxOperations, d.MaxOperations)
	l.MaxPages = cmp.Or(l.MaxPages, d.MaxPages)
	l.MaxRows = cmp.Or(l.MaxRows, d.MaxRows)
	l.MaxReadBytes = cmp.Or(l.MaxReadBytes, d.MaxReadBytes)
	l.MaxDeltaBytes = cmp.Or(l.MaxDeltaBytes, d.MaxDeltaBytes)
	l.MaxDependencies = cmp.Or(l.MaxDependencies, d.MaxDependencies)
	l.MaxNameBytes = cmp.Or(l.MaxNameBytes, d.MaxNameBytes)
	for _, n := range []int{l.MaxOperations, l.MaxPages, l.MaxRows, l.MaxDependencies, l.MaxNameBytes} {
		if n < 1 || n > 1<<20 {
			return Limits{}, ErrInvalidInput
		}
	}
	for _, n := range []int{l.MaxReadBytes, l.MaxDeltaBytes} {
		if n < 1 || n > 64<<20 {
			return Limits{}, ErrInvalidInput
		}
	}
	return l, nil
}
