package assertion

import (
	"bytes"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func validName(s string) bool { return utf8.ValidString(s) && strings.TrimSpace(s) != "" }

func validComponent(k graphstate.ComponentKey) bool {
	if k.Owner == 0 {
		return false
	}
	switch k.Kind {
	case graphstate.Presence:
		return k.Life == 0 && k.Name == "" && k.Member == 0
	case graphstate.Label, graphstate.ScalarProperty:
		return k.Life != 0 && validName(k.Name) && k.Member == 0
	case graphstate.SetMember:
		return k.Life != 0 && validName(k.Name) && k.Member != 0
	default:
		return false
	}
}

func validateSpec(s Spec, l Limits) error {
	if s.Ref.Graph == (graphstate.GraphID{}) || s.Ref.ID == 0 || s.Revision.ID() == 0 || s.Interpretation < Occurrence || s.Interpretation > AssertedRelation {
		return ErrInvalid
	}
	if s.Previous == s.Revision.ID() || s.Retracted && s.Previous == 0 {
		return ErrPredecessor
	}
	// Refuse oversized names before any UTF-8/whitespace scan or child work.
	if len(s.Role) > l.MaxRoleBytes || len(s.Role) > l.MaxRecordBytes || len(s.Target.Component.Name) > l.MaxRecordBytes-len(s.Role) {
		return ErrResourceLimit
	}
	if recordFixedBytes > l.MaxRecordBytes-len(s.Role)-len(s.Target.Component.Name) {
		return ErrResourceLimit
	}
	switch s.Target.Kind {
	case EntityTarget:
		if s.Target.Entity == 0 || s.Target.Component != (graphstate.ComponentKey{}) {
			return ErrInvalid
		}
	case ComponentTarget:
		if s.Target.Entity != 0 || !validComponent(s.Target.Component) {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	hasNative := s.Placement.Native.Kind() != temporal.ScopeInvalid
	hasSymbolic := s.Placement.Symbolic.SupportLevel() != temporal.DescriptorSupportInvalid
	switch s.Placement.Kind {
	case NoAssociation:
		if s.Role != "" || hasNative || hasSymbolic {
			return ErrInvalid
		}
	case NativePlacement:
		if !validName(string(s.Role)) || !hasNative || hasSymbolic {
			return ErrInvalid
		}
	case SymbolicPlacement:
		if !validName(string(s.Role)) || hasNative || !hasSymbolic {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	switch s.Knowledge.Kind {
	case NoKnowledge:
		if s.Knowledge.Point.Kind() != temporal.PointKnowledgeInvalid {
			return ErrInvalid
		}
	case PointKnowledge:
		if s.Placement.Kind != NativePlacement {
			return ErrKnowledgeAssociation
		}
		a, b := s.Placement.Native.Axis(), s.Knowledge.Point.Axis()
		if a.Descriptor() != b.Descriptor() || a.DefinitionHash() != b.DefinitionHash() {
			return errors.Join(ErrKnowledgeAssociation, temporal.ErrAxisMismatch)
		}
	default:
		return ErrInvalid
	}
	return nil
}

func own(s Spec) Record {
	r := Record{ref: s.Ref, target: s.Target, revision: s.Revision, previous: s.Previous, interpretation: s.Interpretation, role: TemporalRole(strings.Clone(string(s.Role))), placement: s.Placement.Kind, native: s.Placement.Native, retracted: s.Retracted}
	r.target.Component.Name = strings.Clone(s.Target.Component.Name)
	if s.Placement.Kind == SymbolicPlacement {
		r.symbolic = new(s.Placement.Symbolic)
	}
	if s.Knowledge.Kind == PointKnowledge {
		r.knowledge = new(s.Knowledge.Point)
	}
	return r
}

// New validates the complete record/child budgets before owning any metadata.
// It does not verify target existence, history uniqueness or allocator authority.
func New(s Spec, l Limits) (Record, error) {
	l, err := l.resolve()
	if err != nil {
		return Record{}, err
	}
	if _, err := encodeSpec(s, l); err != nil {
		return Record{}, err
	}
	return own(s), nil
}

// Spec returns value metadata and immutable temporal children; their accessors
// return defensive copies of mutable bytes/slices. A zero Record returns zero.
func (r Record) Spec() Spec {
	if r.ref == (Ref{}) {
		return Spec{}
	}
	s := Spec{Ref: r.ref, Target: r.target, Revision: r.revision, Previous: r.previous, Interpretation: r.interpretation, Role: r.role, Placement: Placement{Kind: r.placement, Native: r.native}, Knowledge: Knowledge{Kind: NoKnowledge}, Retracted: r.retracted}
	if r.symbolic != nil {
		s.Placement.Symbolic = *r.symbolic
	}
	if r.knowledge != nil {
		s.Knowledge = Knowledge{Kind: PointKnowledge, Point: *r.knowledge}
	}
	return s
}

// Apply checks only the immediate/current revision. Exact current replay compares
// complete canonical content, never a hash alone. Historical revision-ID reuse,
// target existence, allocator claims, commit order and multi-partition consistency
// require the future store. Refusal returns zero output and preserves before.
func Apply(before Record, next Spec, l Limits) (Transition, error) {
	l, err := l.resolve()
	if err != nil {
		return Transition{}, err
	}
	nextBytes, err := encodeSpec(next, l)
	if err != nil {
		return Transition{}, err
	}
	if before.ref == (Ref{}) {
		if next.Previous != 0 {
			return Transition{}, ErrPredecessor
		}
		return Transition{after: own(next)}, nil
	}
	previous := before.Spec()
	beforeBytes, err := encodeSpec(previous, l)
	if err != nil {
		return Transition{}, err
	}
	if previous.Ref.Graph != next.Ref.Graph {
		return Transition{}, ErrNamespace
	}
	if previous.Ref != next.Ref || previous.Target != next.Target {
		return Transition{}, ErrRebinding
	}
	if previous.Revision.ID() == next.Revision.ID() {
		if !bytes.Equal(beforeBytes, nextBytes) {
			return Transition{}, ErrRevisionReuse
		}
		return Transition{before: before, after: before, hasBefore: true, replay: true}, nil
	}
	if next.Previous != previous.Revision.ID() {
		return Transition{}, ErrPredecessor
	}
	return Transition{before: before, after: own(next), hasBefore: true}, nil
}

// Before returns the retained immediate predecessor, absent for initial records.
func (t Transition) Before() (Record, bool) { return t.before, t.hasBefore }

// After returns the complete immutable current association, zero on refusal.
func (t Transition) After() Record { return t.after }

// Replay reports exact current-record replay, not durable request deduplication.
func (t Transition) Replay() bool { return t.replay }
