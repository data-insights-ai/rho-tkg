package graph

import "sync"

// EntityClass distinguishes reference entities (long-lived, low-churn) from
// event entities (high-volume, time-windowed). Unknown labels default to
// ClassEvent so that only explicitly configured reference labels are routed
// to the reference shard.
type EntityClass byte

const (
	// ClassEvent is the default classification for entities whose labels
	// are not registered as reference labels.
	ClassEvent EntityClass = 0
	// ClassReference classifies entities as long-lived reference data
	// (e.g., Case, Organization, User). Reference entities live in a
	// dedicated shard that is always hot.
	ClassReference EntityClass = 1
)

// OntologyMapping classifies entity labels as reference or event.
// It maintains a name-based map from configuration and a lazily-populated
// token-based cache for hot-path lookups.
type OntologyMapping struct {
	byName   map[string]EntityClass // label name -> class (from config)
	byToken  map[uint16]EntityClass // cached token -> class (populated lazily)
	mu       sync.RWMutex           // protects byToken lazy cache
	labelReg *labelRegistry         // shared ref, set after Graph.New()
}

// NewOntologyMapping creates an OntologyMapping that classifies the given
// label names as ClassReference. All other labels default to ClassEvent.
func NewOntologyMapping(refLabels []string) *OntologyMapping {
	byName := make(map[string]EntityClass, len(refLabels))
	for _, name := range refLabels {
		byName[name] = ClassReference
	}
	return &OntologyMapping{
		byName:  byName,
		byToken: make(map[uint16]EntityClass),
	}
}

// ClassifyByName returns the entity class for the given label name.
// Unknown labels return ClassEvent.
func (om *OntologyMapping) ClassifyByName(name string) EntityClass {
	if c, ok := om.byName[name]; ok {
		return c
	}
	return ClassEvent
}

// ClassifyByToken returns the entity class for the given label token.
// Uses a lazy cache backed by the label registry. Token 0 (reserved)
// returns ClassEvent. If no label registry is set, returns ClassEvent.
func (om *OntologyMapping) ClassifyByToken(token uint16) EntityClass {
	if token == 0 {
		return ClassEvent
	}

	// Fast path: check cache under read lock.
	om.mu.RLock()
	c, ok := om.byToken[token]
	om.mu.RUnlock()
	if ok {
		return c
	}

	// Slow path: resolve via label registry, cache result.
	if om.labelReg == nil {
		return ClassEvent
	}

	name := om.labelReg.Resolve(token)
	if name == "" {
		return ClassEvent
	}

	cls := om.ClassifyByName(name)

	om.mu.Lock()
	om.byToken[token] = cls
	om.mu.Unlock()

	return cls
}

// SetLabelRegistry wires the ontology to the graph's label registry for
// token-based classification. Called by Graph.New() after construction.
func (om *OntologyMapping) SetLabelRegistry(reg *labelRegistry) {
	om.labelReg = reg
}

// RefLabels returns the set of reference label names. Used for catalog metadata.
func (om *OntologyMapping) RefLabels() []string {
	out := make([]string, 0, len(om.byName))
	for name, cls := range om.byName {
		if cls == ClassReference {
			out = append(out, name)
		}
	}
	return out
}
