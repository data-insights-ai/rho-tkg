package graphapply

import (
	"fmt"
	"slices"
	"testing"
)

// This oracle intentionally uses no Plan/Project/state.State, production value
// decoder or predicate algebra. This pilot uses rational Q at position 0,
// not a Z-domain proof; applied versions are independent immutable map snapshots.
type processFact struct {
	Exists, Active bool
	Life           uint64
	Labels         []string
	Answer         string
}
type tinyGraphOracle struct {
	versions map[uint64]map[uint64]processFact
}

func newTinyGraphOracle() *tinyGraphOracle {
	return &tinyGraphOracle{versions: make(map[uint64]map[uint64]processFact)}
}
func (o *tinyGraphOracle) created(index uint64) {
	o.versions[index] = map[uint64]processFact{
		1: {Exists: true, Active: true, Life: 2, Labels: []string{"X"}, Answer: "old"},
		3: {Exists: true, Active: true, Life: 4, Labels: []string{"X"}, Answer: "old"},
	}
}
func (o *tinyGraphOracle) changed(index uint64) {
	o.versions[index] = map[uint64]processFact{
		1: {Exists: true}, // Immutable identity remains; closed projection has no life/components.
		3: {Exists: true, Active: true, Life: 4, Labels: []string{"Y"}, Answer: "new"},
	}
}
func (o *tinyGraphOracle) tail(index uint64) {
	o.versions[index] = map[uint64]processFact{1: {Exists: true}, 3: {Exists: true, Active: true, Life: 4, Labels: []string{"Y"}, Answer: "tail"}}
}

// A retry/current-term no-op is another applied coordinate, not another graph effect.
func (o *tinyGraphOracle) retrySame(index uint64) {
	o.versions[index] = map[uint64]processFact{1: {Exists: true}, 3: {Exists: true, Active: true, Life: 4, Labels: []string{"Y"}, Answer: "new"}}
}
func compareProcessFact(got, want processFact) error {
	if got.Exists != want.Exists || got.Active != want.Active || got.Life != want.Life || got.Answer != want.Answer || !slices.Equal(got.Labels, want.Labels) {
		return fmt.Errorf("exact fact mismatch: got %+v want %+v", got, want)
	}
	return nil
}
func (o *tinyGraphOracle) compare(index, id uint64, got processFact) error {
	version, ok := o.versions[index]
	if !ok {
		return fmt.Errorf("unknown oracle version %d", index)
	}
	return compareProcessFact(got, version[id])
}
func TestReplicatedSerialOracleRejectsCurrentAsHistory(t *testing.T) {
	o := newTinyGraphOracle()
	o.created(10)
	o.changed(11)
	for _, id := range []uint64{1, 3} {
		if err := o.compare(10, id, o.versions[11][id]); err == nil {
			t.Fatal("current-state substitution accepted as history", id)
		}
		if err := o.compare(10, id, o.versions[10][id]); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.compare(11, 99, processFact{Exists: true}); err == nil {
		t.Fatal("phantom accepted")
	}
	if err := o.compare(11, 3, processFact{Exists: true, Active: true, Life: 4, Labels: []string{"X", "Y"}, Answer: "new"}); err == nil {
		t.Fatal("extra label accepted")
	}
}

// The process acceptance oracle is literal map semantics, independent of the
// planner, reducer, projection, physical codecs and participant/fence guards.
// It covers these fixed client inputs, not general graph serializability.
type certifiedProcessFact struct {
	Exists, Active       bool
	Life, Source, Target uint64
	Labels               []string
	Incident             []uint64
}
type certifiedProcessOracle struct {
	facts map[uint64]certifiedProcessFact
}

func newCertifiedProcessOracle() *certifiedProcessOracle {
	return &certifiedProcessOracle{facts: map[uint64]certifiedProcessFact{
		1:  {Exists: true, Active: true, Life: 2},
		17: {Exists: true, Active: true, Life: 18},
	}}
}
func (o *certifiedProcessOracle) healthy() {
	for _, id := range []uint64{1, 17} {
		f := o.facts[id]
		f.Labels = []string{"healthy"}
		o.facts[id] = f
	}
}
func (o *certifiedProcessOracle) crossEdge() {
	o.facts[3] = certifiedProcessFact{Exists: true, Active: true, Life: 4, Source: 1, Target: 17}
	for _, id := range []uint64{1, 17} {
		f := o.facts[id]
		f.Incident = []uint64{3}
		o.facts[id] = f
	}
}
func (o *certifiedProcessOracle) compare(id uint64, got certifiedProcessFact) error {
	want := o.facts[id]
	if got.Exists != want.Exists || got.Active != want.Active || got.Life != want.Life || got.Source != want.Source || got.Target != want.Target || !slices.Equal(got.Labels, want.Labels) || !slices.Equal(got.Incident, want.Incident) {
		return fmt.Errorf("exact certified process fact %d: got %+v want %+v", id, got, want)
	}
	return nil
}
func encodeCertifiedProcessFact(f certifiedProcessFact) ([]byte, error) {
	if len(f.Labels) > 16 || len(f.Incident) > 16 {
		return nil, errLimit
	}
	for _, s := range f.Labels {
		if len(s) > 256 {
			return nil, errLimit
		}
	}
	return processEncoding(func(w *boundedWriter) {
		var flags byte
		if f.Exists {
			flags |= 1
		}
		if f.Active {
			flags |= 2
		}
		w.tag(flags)
		w.u64(f.Life)
		w.u64(f.Source)
		w.u64(f.Target)
		w.u32(len(f.Labels))
		for _, s := range f.Labels {
			w.text(s)
		}
		w.u32(len(f.Incident))
		for _, id := range f.Incident {
			w.u64(id)
		}
	})
}
func decodeCertifiedProcessFact(wire []byte) (certifiedProcessFact, error) {
	c := graphCursor{b: wire}
	flags := c.tag()
	f := certifiedProcessFact{Exists: flags&1 != 0, Active: flags&2 != 0, Life: c.u64(), Source: c.u64(), Target: c.u64()}
	if flags > 3 {
		c.err = errCorrupt
	}
	n := c.count(16, 4)
	if c.err != nil {
		return certifiedProcessFact{}, c.err
	}
	f.Labels = make([]string, n)
	for i := range f.Labels {
		f.Labels[i] = string(c.field(256))
	}
	n = c.count(16, 8)
	if c.err != nil {
		return certifiedProcessFact{}, c.err
	}
	f.Incident = make([]uint64, n)
	for i := range f.Incident {
		f.Incident[i] = c.u64()
	}
	if len(c.b) != 0 {
		c.err = errCorrupt
	}
	return f, c.err
}
func TestReplicatedCertifiedProcessOracleRejectsMissingCrossEffects(t *testing.T) {
	o := newCertifiedProcessOracle()
	before := o.facts[1]
	o.crossEdge()
	if err := o.compare(1, before); err == nil {
		t.Fatal("missing outgoing posting accepted")
	}
	if err := o.compare(17, certifiedProcessFact{Exists: true, Active: true, Life: 18}); err == nil {
		t.Fatal("missing incoming posting accepted")
	}
	if err := o.compare(3, certifiedProcessFact{}); err == nil {
		t.Fatal("missing cross relationship accepted")
	}
	if err := o.compare(99, o.facts[3]); err == nil {
		t.Fatal("phantom accepted")
	}
	for _, id := range []uint64{1, 3, 17, 99} {
		wire, err := encodeCertifiedProcessFact(o.facts[id])
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeCertifiedProcessFact(wire)
		if err != nil {
			t.Fatal(err)
		}
		if err := o.compare(id, got); err != nil {
			t.Fatal(err)
		}
	}
}
