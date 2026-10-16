package graph

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

type refKind uint8

const (
	refNode refKind = iota + 1
	refRel
)

// Ref names one node or one relationship. The zero Ref is invalid.
type Ref struct {
	kind refKind
	node types.NodeID
	rel  types.RelID
}

// NodeRef refers to a node; zero and negative IDs are ErrInvalidID.
func NodeRef(id types.NodeID) (Ref, error) {
	if err := checkNode(id); err != nil {
		return Ref{}, err
	}
	return Ref{kind: refNode, node: id}, nil
}

// RelRef refers to a relationship; zero and negative IDs are ErrInvalidID.
func RelRef(id types.RelID) (Ref, error) {
	if id <= 0 {
		return Ref{}, fmt.Errorf("%w: relationship", ErrInvalidID)
	}
	return Ref{kind: refRel, rel: id}, nil
}

// IsRel reports a relationship ref.
func (r Ref) IsRel() bool { return r.kind == refRel }

// Node returns the node ID (zero for a relationship ref).
func (r Ref) Node() types.NodeID { return r.node }

// Rel returns the relationship ID (zero for a node ref).
func (r Ref) Rel() types.RelID { return r.rel }

// Validate reports ErrInvalidID for the zero Ref or a bad ID.
func (r Ref) Validate() error {
	switch r.kind {
	case refNode:
		return checkNode(r.node)
	case refRel:
		if r.rel <= 0 {
			return fmt.Errorf("%w: relationship", ErrInvalidID)
		}
		return nil
	}
	return fmt.Errorf("%w: empty ref", ErrInvalidID)
}

func checkNode(id types.NodeID) error {
	if id <= 0 {
		return fmt.Errorf("%w: node", ErrInvalidID)
	}
	return nil
}
