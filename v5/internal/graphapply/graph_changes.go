package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

// graphChanges is an internal retained semantic envelope. Existing payload
// references resolve through the immutable dictionary at the original applied
// root. It is not yet a standalone portable public CDC feed or erase contract.
// Readiness/schema initialization is a semantic effect, even with no entities.
type graphChanges struct {
	ns                             namespace
	initialized                    bool
	topology, schema, indexVersion uint64
	schemas                        []graphstate.PropertyDefinition
	entities                       []graphstate.EntityRecord
	lives                          []graphstate.LifeRecord
	values                         []graphstate.ValueWrite
	groups                         []graphstore.ComponentChangeGroup
}

func (g graphChanges) nonempty() bool {
	return g.initialized || len(g.entities) > 0 || len(g.lives) > 0 || len(g.values) > 0 || len(g.groups) > 0
}
func changesAxes(g graphChanges, l materializerLimits) (axisTable, error) {
	var t axisTable
	for _, e := range g.entities {
		if err := t.add(e.Axis, l.maxAxes); err != nil {
			return nil, err
		}
	}
	for _, v := range g.values {
		if s, ok := v.Value.Scope(); ok {
			if err := t.add(s.Axis(), l.maxAxes); err != nil {
				return nil, err
			}
		}
	}
	for _, group := range g.groups {
		if err := t.add(group.Owned.Axis(), l.maxAxes); err != nil {
			return nil, err
		}
	}
	return t, nil
}
func writeComponentKey(w *boundedWriter, k graphstate.ComponentKey) {
	w.u64(uint64(k.Owner))
	w.u64(uint64(k.Life))
	w.tag(byte(k.Kind))
	w.text(k.Name)
	w.u64(uint64(k.Member))
}
func readComponentKey(c *graphCursor, l materializerLimits) graphstate.ComponentKey {
	return graphstate.ComponentKey{Owner: graphstate.EntityID(c.u64()), Life: graphstate.LifeID(c.u64()), Kind: graphstate.ComponentKind(c.tag()), Name: string(c.field(l.catalog.MaxNameBytes)), Member: graphstate.ValueID(c.u64())}
}
func validComponentKey(k graphstate.ComponentKey, l materializerLimits) bool {
	if k.Owner == 0 {
		return false
	}
	switch k.Kind {
	case graphstate.Presence:
		return k.Life == 0 && k.Name == "" && k.Member == 0
	case graphstate.Label:
		return k.Life != 0 && validGraphName(k.Name, l) && k.Member == 0
	case graphstate.ScalarProperty:
		return k.Life != 0 && validGraphName(k.Name, l) && k.Member == 0
	case graphstate.SetMember:
		return k.Life != 0 && validGraphName(k.Name, l) && k.Member != 0
	}
	return false
}
func validateGraphChanges(g graphChanges, l materializerLimits) error {
	if !g.ns.valid() {
		return errInvalid
	}
	if g.initialized {
		if g.topology != 1 || g.schema != 1 || g.indexVersion != 2 || len(g.entities)+len(g.lives)+len(g.values)+len(g.groups) != 0 {
			return errInvalid
		}
		for i, d := range g.schemas {
			if !validSchema(d, l) {
				return errInvalid
			}
			if i > 0 {
				old := g.schemas[i-1]
				if old.Owner > d.Owner || old.Owner == d.Owner && old.Name >= d.Name {
					return errInvalid
				}
			}
		}
	} else if g.topology != 0 || g.schema != 0 || g.indexVersion != 0 || len(g.schemas) != 0 {
		return errInvalid
	}
	if len(g.schemas) > l.maxSchemas || len(g.entities) > l.maxClaims || len(g.lives) > l.maxClaims || len(g.values) > l.maxClaims || len(g.groups) > l.graph.Pages.MaxPatches {
		return errLimit
	}
	for _, e := range g.entities {
		if e.ID == 0 || e.Kind != graphstate.Node && e.Kind != graphstate.Relationship {
			return errInvalid
		}
		if e.Kind == graphstate.Node && (e.Type != "" || e.Source != 0 || e.Target != 0 || e.Mode != 0) || e.Kind == graphstate.Relationship && (!validGraphName(e.Type, l) || e.Source == 0 || e.Target == 0 || e.Mode != graphstate.LifeBound && e.Mode != graphstate.IdentityReference) {
			return errInvalid
		}
	}
	for _, life := range g.lives {
		if life.Owner == 0 || life.Life == 0 {
			return errInvalid
		}
	}
	for _, v := range g.values {
		if v.ID == 0 || v.Value.Kind() == graphstate.ScalarInvalid {
			return errInvalid
		}
	}
	for _, group := range g.groups {
		if !validComponentKey(group.Key, l) || len(group.Changes) == 0 {
			return errInvalid
		}
	}
	return nil
}
func emitGraphChanges(w *boundedWriter, g graphChanges, t axisTable, l materializerLimits) {
	w.add([]byte{'G', 'C', 'D', 1})
	w.add(g.ns.graph[:])
	w.u64(g.ns.partition)
	if g.initialized {
		w.tag(1)
	} else {
		w.tag(0)
	}
	w.u64(g.topology)
	w.u64(g.schema)
	w.u64(g.indexVersion)
	w.u32(len(g.schemas))
	for _, d := range g.schemas {
		writeSchema(w, d)
	}
	w.u32(len(t))
	for _, a := range t {
		writeAxis(w, a, l)
	}
	w.u32(len(g.entities))
	for _, e := range g.entities {
		writeEntity(w, e, t)
	}
	w.u32(len(g.lives))
	for _, life := range g.lives {
		writeLife(w, life)
	}
	w.u32(len(g.values))
	for _, v := range g.values {
		w.u64(uint64(v.ID))
		writeScalar(w, v.Value, t, l)
	}
	w.u32(len(g.groups))
	for _, group := range g.groups {
		writeComponentKey(w, group.Key)
		writeScope(w, group.Owned, t, l)
		available := w.max - w.n - 4
		if available < 1 {
			w.err = errLimit
			return
		}
		wire, err := state.AppendChanges(nil, group.Owned.Axis(), group.Changes, state.CodecLimits{State: l.graph.Planner.Component, MaxEncodedBytes: min(l.changeBytes, available)})
		if err != nil {
			w.err = err
			return
		}
		w.field(wire)
	}
}
func encodeGraphChanges(g graphChanges, l materializerLimits) ([]byte, error) {
	if err := l.validate(); err != nil {
		return nil, err
	}
	if err := validateGraphChanges(g, l); err != nil {
		return nil, err
	}
	t, err := changesAxes(g, l)
	if err != nil {
		return nil, err
	}
	return boundedEncoding(l.changeBytes, func(w *boundedWriter) { emitGraphChanges(w, g, t, l) })
}
func decodeGraphChanges(b []byte, n namespace, l materializerLimits) (graphChanges, error) {
	if err := l.validate(); err != nil {
		return graphChanges{}, err
	}
	body, err := graphBody(b, "GCD\x01", l.changeBytes)
	if err != nil {
		return graphChanges{}, err
	}
	c := graphCursor{b: body, maxOwnedBytes: l.outputBytes}
	if !c.charge(512 + 8*len(b)) {
		return graphChanges{}, c.err
	}
	g := graphChanges{ns: namespace{graph: n.graph, partition: n.partition}}
	if c.array() != [16]byte(n.graph) || c.u64() != n.partition {
		return graphChanges{}, errCorrupt
	}
	flag := c.tag()
	if flag > 1 {
		return graphChanges{}, errCorrupt
	}
	g.initialized = flag == 1
	g.topology, g.schema, g.indexVersion = c.u64(), c.u64(), c.u64()
	count := c.count(l.maxSchemas, 8)
	if !c.charge(64 * count) {
		return graphChanges{}, c.err
	}
	g.schemas = make([]graphstate.PropertyDefinition, count)
	for i := range g.schemas {
		g.schemas[i] = readSchema(&c, l)
	}
	count = c.count(l.maxAxes, 27)
	if !c.charge(axisMetadataBytes * count) {
		return graphChanges{}, c.err
	}
	t := make(axisTable, count)
	for i := range t {
		t[i] = readAxis(&c, l)
	}
	count = c.count(l.maxClaims, 32)
	if !c.charge(256 * count) {
		return graphChanges{}, c.err
	}
	g.entities = make([]graphstate.EntityRecord, count)
	for i := range g.entities {
		g.entities[i] = readEntity(&c, t, l)
	}
	count = c.count(l.maxClaims, 32)
	if !c.charge(64 * count) {
		return graphChanges{}, c.err
	}
	g.lives = make([]graphstate.LifeRecord, count)
	for i := range g.lives {
		g.lives[i] = readLife(&c)
	}
	count = c.count(l.maxClaims, 16)
	if !c.charge(256 * count) {
		return graphChanges{}, c.err
	}
	g.values = make([]graphstate.ValueWrite, count)
	for i := range g.values {
		g.values[i] = graphstate.ValueWrite{ID: graphstate.ValueID(c.u64()), Value: readScalar(&c, t, l)}
	}
	count = c.count(l.graph.Pages.MaxPatches, 29)
	if !c.charge(512 * count) {
		return graphChanges{}, c.err
	}
	g.groups = make([]graphstore.ComponentChangeGroup, count)
	for i := range g.groups {
		group := graphstore.ComponentChangeGroup{Key: readComponentKey(&c, l), Owned: readScope(&c, t, l)}
		wire := c.field(l.changeBytes)
		if c.err != nil {
			return graphChanges{}, c.err
		}
		if !preflightChangesBacking(&c, wire, l) {
			return graphChanges{}, c.err
		}
		changes, _, err := state.DecodeChanges(wire, group.Owned.Axis(), state.CodecLimits{State: l.graph.Planner.Component, MaxEncodedBytes: l.changeBytes})
		if err != nil {
			return graphChanges{}, errors.Join(errCorrupt, err)
		}
		group.Changes = changes
		g.groups[i] = group
	}
	if c.err != nil {
		return graphChanges{}, c.err
	}
	if len(c.b) != 0 {
		return graphChanges{}, errCorrupt
	}
	if err := validateGraphChanges(g, l); err != nil {
		return graphChanges{}, errCorrupt
	}
	canonical, err := encodeGraphChanges(g, l)
	if err != nil {
		return graphChanges{}, err
	}
	if !bytes.Equal(b, canonical) {
		return graphChanges{}, errCorrupt
	}
	return g, nil
}

// Digest chains only logical schema/readiness/catalog/ordered component effects.
// Physical handles, descriptor rewrites, local generation and applied index are
// deliberately absent from this portable semantic input.
func graphEffectDigest(previous [32]byte, changes []byte) [32]byte {
	b := append([]byte("rho-tkg:graph-effects:v1\x00"), previous[:]...)
	b = binary.BigEndian.AppendUint64(b, uint64(len(changes)))
	h := sha256.New()
	_, _ = h.Write(b)
	_, _ = h.Write(changes)
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

// GCE1 binds retained logical CDC to the original request/application outcome.
// Its applied index is recovery metadata; only GCD1 enters EffectDigest.
func encodeChangeEnvelope(o outcome, logical []byte, l materializerLimits) ([]byte, error) {
	if o.reason != reasonNone || o.disposition != applied || len(logical) == 0 {
		return nil, errInvalid
	}
	return boundedEncoding(l.changeBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'C', 'E', 1})
		w.tag(byte(o.kind))
		w.add(o.identity[:])
		w.add(o.hash[:])
		w.u64(o.index)
		w.field(logical)
	})
}
func decodeChangeEnvelope(b []byte, o outcome, l materializerLimits) (graphChanges, error) {
	body, err := graphBody(b, "GCE\x01", l.changeBytes)
	if err != nil {
		return graphChanges{}, err
	}
	c := graphCursor{b: body}
	kind := commandKind(c.tag())
	id := c.array()
	var hash [32]byte
	copy(hash[:], c.take(32))
	index := c.u64()
	logical := c.field(l.changeBytes)
	if c.err != nil || len(c.b) != 0 || kind != o.kind || id != o.identity || hash != o.hash || index != o.index {
		return graphChanges{}, errCorrupt
	}
	return decodeGraphChanges(logical, o.ns, l)
}
