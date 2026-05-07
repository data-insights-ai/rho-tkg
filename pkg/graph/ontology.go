package graph

import (
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
)

// EntityClass and OntologyMapping classify labels for shard routing in
// the TieredStore. The canonical declaration lives in
// `pkg/graph/internal/index`; these aliases are the public API surface.

type (
	// EntityClass distinguishes reference entities from event entities.
	EntityClass = indexpkg.EntityClass
	// OntologyMapping classifies entity labels as reference or event.
	OntologyMapping = indexpkg.OntologyMapping
)

// EntityClass constants.
const (
	ClassEvent     = indexpkg.ClassEvent
	ClassReference = indexpkg.ClassReference
)

// NewOntologyMapping creates an OntologyMapping that classifies the given
// label names as ClassReference. All other labels default to ClassEvent.
func NewOntologyMapping(refLabels []string) *OntologyMapping {
	return indexpkg.NewOntologyMapping(refLabels)
}
