package graphstore

import (
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func postingTestKey(kind recordKind, n uint64) postingKey {
	switch kind {
	case uniquePostingRecord:
		return postingKey{family: kind, owner: graphstate.Node, scalar: graphstate.ScalarI64, name: "a\x00å", value: 99, component: graphstate.ComponentKey{Owner: graphstate.EntityID(n), Life: 11, Kind: graphstate.SetMember, Name: "a\x00å", Member: 100}}
	case canonicalIncidentRecord:
		return postingKey{family: kind, endpoint: 1, mode: graphstate.LifeBound, relationship: graphstate.EntityID(n), roles: 1}
	default:
		return postingKey{family: kind, endpoint: 1, bound: 11, relationship: graphstate.EntityID(n), life: 31, roles: 1}
	}
}
func TestPostingCodecAllTypedFamiliesAdversarialBranches(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	c := f.catalog(t, f.index)
	limits := defaultComponentKeyTreeLimits()
	root := c.root
	root.next = 16
	for _, kind := range []recordKind{uniquePostingRecord, canonicalIncidentRecord, declaredIncidentRecord} {
		first, second := postingTestKey(kind, 1), postingTestKey(kind, 2)
		nodes := []postingTreeNode{{kind: kind, id: 1, keys: []postingKey{first, second}}, {kind: kind, id: 1, level: 1, children: []postingTreeChild{{id: 2, count: 1, first: first, last: first}, {id: 3, count: 1, first: second, last: second}}}}
		for _, node := range nodes {
			wire, err := encodePostingKeyTreeNode(node, c, limits)
			if err != nil {
				t.Fatal(err)
			}
			decode := func(wire []byte, l postingTreeLimits) error {
				key := physicalKey(root.namespace, kind, 1)
				q := &reader{c: &Catalog{root: root, limits: c.limits}, ctx: t.Context(), pending: map[string]raftlog.KV{string(key): {Key: key, Value: wire}}}
				p := pageReader{q: q, limits: DefaultPageLimits()}
				_, err := p.postingTreeNode(1, kind, l)
				return err
			}
			if err := decode(wire, limits); err != nil {
				t.Fatal(err)
			}
			for n := range len(wire) {
				if err := decode(wire[:n], limits); !errors.Is(err, ErrCorrupt) {
					t.Fatal("truncated typed page accepted", kind, n, err)
				}
			}
			if err := decode(append(slices.Clone(wire), 0), limits); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
			bad := slices.Clone(wire)
			binary.BigEndian.PutUint32(bad[37:41], math.MaxUint32)
			if err := decode(bad, limits); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
			tight := limits
			tight.maxPageBytes = 64
			if err := decode(wire, tight); !errors.Is(err, ErrResourceLimit) {
				t.Fatal("tight policy became corruption", err)
			}
			if err := decode(wire, postingTreeLimits{}); !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
		}
		for _, node := range []postingTreeNode{{kind: kind, id: 0}, {kind: kind, id: 1, level: 8}, {kind: kind, id: 1, keys: []postingKey{second, first}}, {kind: kind, id: 1, keys: []postingKey{first, first}}, {kind: kind, id: 1, keys: []postingKey{{family: kind}}}, {kind: kind, id: 1, level: 1, children: []postingTreeChild{{id: 1, count: 1, first: first, last: first}, {id: 3, count: 1, first: second, last: second}}}, {kind: kind, id: 1, level: 1, children: []postingTreeChild{{id: 2, count: 1, first: first, last: first}, {id: 2, count: 1, first: second, last: second}}}} {
			if _, err := encodePostingKeyTreeNode(node, c, limits); !errors.Is(err, ErrInvalid) {
				t.Fatal("invalid typed page encoded", kind, node, err)
			}
		}
	}
}
func TestPostingSplitsDuplicateIdentityAndLateFailuresAreAtomic(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	c := f.catalog(t, f.index)
	v, err := OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	for _, kind := range []recordKind{uniquePostingRecord, canonicalIncidentRecord, declaredIncidentRecord} {
		s, err := newFullStage(t.Context(), c, v.descriptor, DefaultPageLimits())
		if err != nil {
			t.Fatal(err)
		}
		limits := postingTreeLimits{maxKeys: 2, maxChildren: 3, maxLevels: 5, maxPageBytes: 4096}
		var tree postingTreeRoot
		err = s.operation(t.Context(), func(base *reader) error {
			p := pageStage{&pageReader{q: base, limits: DefaultPageLimits()}, c.root}
			p.allocation = &p.root
			var e error
			tree, e = p.newPostingKeyTree(kind, limits)
			if e != nil {
				return e
			}
			for n := uint64(1); n <= 12; n++ {
				tree, e = p.insertPostingKey(tree, postingTestKey(kind, n), limits)
				if e != nil {
					return e
				}
			}
			base.full.root = p.root
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if tree.level < 2 || tree.count != 12 {
			t.Fatal("posting split lost membership", tree)
		}
		before, _ := s.Writes()
		authority := *s.full
		err = s.operation(t.Context(), func(base *reader) error {
			p := pageStage{&pageReader{q: base, limits: DefaultPageLimits()}, s.full.root}
			p.allocation = &p.root
			n, err := p.postingTreeRoot(tree, limits)
			if err != nil {
				return err
			}
			if n.summary().count != 12 {
				return ErrCorrupt
			}
			found, err := p.hasPostingKey(tree, postingTestKey(kind, 1), limits)
			if err != nil || !found {
				return ErrCorrupt
			}
			next, err := p.insertPostingKey(tree, postingTestKey(kind, 1), limits)
			if err != nil || next != tree {
				return ErrCorrupt
			}
			if _, err := p.insertPostingKey(tree, postingTestKey(kind, 13), limits); err != nil {
				return err
			}
			_, err = p.insertPostingKey(tree, postingKey{}, limits)
			return err
		})
		after, _ := s.Writes()
		if !errors.Is(err, ErrInvalid) || !sameWrites(before, after) || *s.full != authority {
			t.Fatal("late invalid posting changed existing staging", kind, err)
		}
		if kind != uniquePostingRecord {
			err = s.operation(t.Context(), func(base *reader) error {
				p := pageStage{&pageReader{q: base, limits: DefaultPageLimits()}, s.full.root}
				p.allocation = &p.root
				wrong := postingTestKey(kind, 1)
				wrong.roles = 2
				_, err := p.insertPostingKey(tree, wrong, limits)
				return err
			})
			if !errors.Is(err, ErrRebinding) {
				t.Fatal("immutable role payload rebound", err)
			}
		}
		_ = s.Close()
	}
}

func TestPostingDecoderChargesActualCountOnceBeforeOwnership(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	c := f.catalog(t, f.index)
	defer c.view.Close()
	root := c.root
	root.next = 100
	limits := defaultComponentKeyTreeLimits()
	limits.maxPageBytes = 64 << 10
	for _, kind := range []recordKind{uniquePostingRecord, canonicalIncidentRecord, declaredIncidentRecord} {
		for _, branch := range []bool{false, true} {
			for _, count := range []int{2, 64} {
				node := postingTreeNode{kind: kind, id: 1}
				variable := 0
				for i := range count {
					key := postingTestKey(kind, uint64(i+1))
					variable += len(key.name) + len(key.component.Name)
					if branch {
						node.level = 1
						node.children = append(node.children, postingTreeChild{uint64(i + 2), 1, key, key})
						variable += len(key.name) + len(key.component.Name)
					} else {
						node.keys = append(node.keys, key)
					}
				}
				wire, err := encodePostingKeyTreeNode(node, c, limits)
				if err != nil {
					t.Fatal(err)
				}
				key := physicalKey(root.namespace, kind, 1)
				delivered := cap(key) + cap(wire) + 64
				fixed := 128 + 320*count
				decode := func(capBytes int) (postingTreeNode, *reader, error) {
					q := &reader{c: &Catalog{root: root, limits: c.limits}, ctx: t.Context(), maxBytes: capBytes, pending: map[string]raftlog.KV{string(key): {Key: key, Value: wire}}}
					p := &pageReader{q: q, limits: DefaultPageLimits()}
					n, err := p.postingTreeNode(1, kind, limits)
					return n, q, err
				}
				n, q, err := decode(delivered + fixed + variable)
				if err != nil || n.summary() != node.summary() || q.bytes != delivered+fixed+variable || q.rows != 1 {
					t.Fatal("actual decoded ownership charged twice or omitted", kind, branch, count, q.bytes, delivered, fixed, variable, err)
				}
				n, q, err = decode(delivered + fixed - 1)
				if !errors.Is(err, ErrResourceLimit) || n.id != 0 || q.bytes != delivered || q.rows != 1 {
					t.Fatal("fixed backing not refused before ownership", kind, branch, count, q.bytes, err)
				}
				if branch {
					// A duplicate far from its first occurrence must be rejected
					// by the bounded previous-child scan after a real decode.
					bad := slices.Clone(wire)
					entryBytes := 16 + 2*len(appendPostingKey(nil, node.children[0].first))
					binary.BigEndian.PutUint64(bad[41+(count-1)*entryBytes:], node.children[0].id)
					original := wire
					wire = bad
					_, _, err = decode(1 << 20)
					wire = original
					if !errors.Is(err, ErrCorrupt) {
						t.Fatal("far duplicate child accepted", kind, count, err)
					}
				}
			}
		}
	}
}
