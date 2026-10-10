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
