package graphstore

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strings"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// componentKeyReader supplies only private KeysOnly iteration. It is deliberately
// not a ReadView. Stable handles are resolved at its retained MVCC root; the
// reader never scans application keys, including keys created after that root.
type componentKeyReader struct {
	mu          sync.Mutex
	c           *Catalog
	limits      PageLimits
	descriptor  componentIndexDescriptor
	id          graphstate.ViewID
	index       uint64
	cursors     map[graphstate.Cursor]componentKeyContinuation
	cursorBytes int
	closed      bool
	last        PageWork
}
type componentKeyContinuation struct {
	query [32]byte
	last  graphstate.ComponentKey
	bytes int
}

func newComponentKeyReader(c *Catalog, l PageLimits) (*componentKeyReader, error) {
	if c == nil {
		return nil, ErrInvalid
	}
	l, err := l.resolve()
	if err != nil {
		return nil, err
	}
	q, err := c.reader(context.Background())
	if err != nil {
		return nil, err
	}
	if c.root.topology != keysOnlyTopology {
		return nil, ErrTopologyUnsupported
	}
	q.maxRows, q.maxBytes = l.MaxWorkRecords, l.MaxWorkBytes
	d, found, err := q.componentIndexDescriptor(c.root)
	if err == nil && !found {
		err = ErrCorrupt
	}
	if err != nil {
		return nil, c.failure(err)
	}
	p := pageReader{q: q, limits: l}
	if _, err := p.componentKeyTreeRoot(d.tree, keyTreeLimits(l)); err != nil {
		return nil, c.failure(err)
	}
	// Fixed handle initialization has its own charged descriptor/image hashing
	// scratch allowance; this is not included in subsequent page LastWork.
	if err := q.materialize(512 + c.rootImageBytes); err != nil {
		return nil, err
	}
	root, err := c.view.Root()
	if err != nil {
		return nil, err
	}
	wire, err := encodeComponentIndexDescriptor(d, c.root, c.limits)
	if err != nil {
		return nil, err
	}
	id := componentKeysViewIdentity(wire, root)
	return &componentKeyReader{c: c, limits: l, descriptor: d, id: id, index: root.Index, cursors: make(map[graphstate.Cursor]componentKeyContinuation)}, nil
}
func componentKeysViewIdentity(descriptor []byte, root raftlog.ApplicationRoot) graphstate.ViewID {
	b := append([]byte("rho-tkg:component-keys-view:v1\x00"), descriptor...)
	b = binary.BigEndian.AppendUint64(b, root.Generation)
	b = binary.BigEndian.AppendUint64(b, root.Index)
	b = append(b, root.ImageHash[:]...)
	hash := sha256.Sum256(b)
	var id graphstate.ViewID
	copy(id[:], hash[:16])
	return id
}
func (p *componentKeyReader) Close() error {
	if p == nil {
		return ErrInvalid
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.cursors = nil
	p.cursorBytes = 0
	return nil
}
func keyPredicateMatches(k graphstate.ComponentKey, p graphstate.KeyPredicate) bool {
	return k.Owner == p.Owner && (p.Life == 0 || k.Life == p.Life) && (p.Kind == 0 || k.Kind == p.Kind) && (p.Name == "" || k.Name == p.Name)
}

// Prefix comparison stops at the first unspecified dimension. Later exact
// fields become filters rather than unsafe ordering fences.
func compareKeyPrefix(k graphstate.ComponentKey, p graphstate.KeyPredicate) int {
	if order := cmp.Compare(k.Owner, p.Owner); order != 0 {
		return order
	}
	if p.Life == 0 {
		return 0
	}
	if order := cmp.Compare(k.Life, p.Life); order != 0 {
		return order
	}
	if p.Kind == 0 {
		return 0
	}
	if order := cmp.Compare(k.Kind, p.Kind); order != 0 {
		return order
	}
	if p.Name == "" {
		return 0
	}
	return cmp.Compare(k.Name, p.Name)
}
func keyPrefixStart(p graphstate.KeyPredicate) graphstate.ComponentKey {
	k := graphstate.ComponentKey{Owner: p.Owner}
	if p.Life != 0 {
		k.Life = p.Life
		if p.Kind != 0 {
			k.Kind = p.Kind
			k.Name = p.Name
		}
	}
	return k
}

// walkComponentKeys reuses decoded ancestors/leaves for the whole page. A
// visitor refusal leaves the last fully checked key as the continuation anchor.
func (q *pageReader) walkComponentKeys(n componentKeyTreeNode, p graphstate.KeyPredicate, after graphstate.ComponentKey, inclusive bool, l componentKeyTreeLimits, visit func(graphstate.ComponentKey) (bool, error)) (bool, error) {
	if n.level == 0 {
		for _, key := range n.keys {
			order := compareComponentKeys(key, after)
			if order < 0 || !inclusive && order == 0 {
				continue
			}
			prefix := compareKeyPrefix(key, p)
			if prefix < 0 {
				continue
			}
			if prefix > 0 {
				return true, nil
			}
			keep, err := visit(key)
			if err != nil || !keep {
				return false, err
			}
		}
		return true, nil
	}
	for i, child := range n.children {
		order := compareComponentKeys(child.last, after)
		if order < 0 || !inclusive && order == 0 || compareKeyPrefix(child.last, p) < 0 {
			continue
		}
		if compareKeyPrefix(child.first, p) > 0 {
			return true, nil
		}
		next, err := q.componentKeyTreeChild(n, i, l)
		if err != nil {
			return false, err
		}
		complete, err := q.walkComponentKeys(next, p, after, inclusive, l, visit)
		if err != nil || !complete {
			return false, err
		}
	}
	return true, nil
}

// ComponentKeys fills a bounded page from the exact longest contiguous prefix.
// Sparse exact filters can produce empty advancing pages. Work exhaustion after
// checked candidates ends a successful incomplete page at the last checked key;
// a candidate too large to make any progress refuses with zero output. Errors
// never mutate continuations or return partial data.
func (p *componentKeyReader) ComponentKeys(ctx context.Context, predicate graphstate.KeyPredicate, token graphstate.Cursor, budget graphstate.ReadBudget) (graphstate.KeyPage, error) {
	if p == nil {
		return graphstate.KeyPage{}, ErrInvalid
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.last = PageWork{}
	if p.closed {
		return graphstate.KeyPage{}, ErrClosed
	}
	if predicate.Owner == 0 || predicate.Kind > graphstate.SetMember || predicate.Name != "" && !validName(predicate.Name, p.c.limits) || budget.Rows < 1 || budget.Bytes < 1 {
		return graphstate.KeyPage{}, ErrInvalid
	}
	if ctx == nil {
		return graphstate.KeyPage{}, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return graphstate.KeyPage{}, err
	}
	scratchBytes := 45 + len(predicate.Name)
	effectiveBytes := min(budget.Bytes, p.limits.MaxWorkBytes, p.c.limits.MaxReadBytes)
	if scratchBytes+256+p.c.limits.MaxNameBytes > effectiveBytes-p.c.rootImageBytes {
		return graphstate.KeyPage{}, ErrResourceLimit
	}
	scratch := make([]byte, 0, scratchBytes)
	canonical := appendComponent(append(scratch, p.id[:]...), graphstate.ComponentKey{Owner: predicate.Owner, Life: predicate.Life, Kind: predicate.Kind, Name: predicate.Name})
	hash := sha256.Sum256(canonical)
	old := componentKeyContinuation{}
	after := keyPrefixStart(predicate)
	inclusive := true
	if token != 0 {
		var found bool
		old, found = p.cursors[token]
		if !found || old.query != hash {
			return graphstate.KeyPage{}, ErrInvalid
		}
		after = old.last
		inclusive = false
	}
	base, err := p.c.reader(ctx)
	if err != nil {
		return graphstate.KeyPage{}, err
	}
	base.maxRows = min(budget.Rows, p.limits.MaxWorkRecords)
	base.maxBytes = min(budget.Bytes, p.limits.MaxWorkBytes)
	q := pageReader{q: base, limits: p.limits}
	defer func() { _ = q.budget(); p.last = q.work }()
	// Reserve bounded continuation ownership before reading any candidate.
	if err := base.materialize(len(canonical) + 256 + p.c.limits.MaxNameBytes); err != nil {
		return graphstate.KeyPage{}, err
	}
	n, err := q.componentKeyTreeRoot(p.descriptor.tree, keyTreeLimits(p.limits))
	if err != nil {
		return graphstate.KeyPage{}, p.c.failure(err)
	}
	out := graphstate.KeyPage{View: p.id, Version: graphstate.ReadVersion(p.index)}
	var last graphstate.ComponentKey
	visited := 0
	out.Complete, err = q.walkComponentKeys(n, predicate, after, inclusive, keyTreeLimits(p.limits), func(key graphstate.ComponentKey) (bool, error) {
		// This key was just reached through validated root/count/level/fence links.
		// readMeta still verifies existence and all catalog binding references.
		q.member = &key
		m, found, err := q.readMeta(key)
		if err != nil {
			return false, err
		}
		if !found {
			return false, ErrCorrupt
		}
		d, err := q.directory(m.Root, key, m.Axis)
		if err != nil {
			return false, err
		}
		if d.Owned.Kind() != temporal.ScopeAll {
			return false, ErrCorrupt
		}
		if keyPredicateMatches(key, predicate) {
			if len(out.Keys) == cap(out.Keys) {
				capacity := min(max(1, 2*cap(out.Keys)), budget.Rows)
				if err := base.materialize(64 * (capacity - cap(out.Keys))); err != nil {
					return false, err
				}
				keys := make([]graphstate.ComponentKey, len(out.Keys), capacity)
				copy(keys, out.Keys)
				out.Keys = keys
			}
			if err := base.materialize(len(key.Name)); err != nil {
				return false, err
			}
			key.Name = strings.Clone(key.Name)
			out.Keys = append(out.Keys, key)
		}
		last = key
		visited++
		return true, q.budget()
	})
	if err != nil {
		if !errors.Is(err, ErrResourceLimit) || visited == 0 {
			return graphstate.KeyPage{}, p.c.failure(err)
		}
		out.Complete = false
	}
	cost := 0
	if !out.Complete {
		cost = 128 + len(last.Name)
		count := len(p.cursors)
		if token != 0 {
			count--
		}
		if count >= p.limits.MaxCursors || cost > p.limits.MaxCursorBytes-(p.cursorBytes-old.bytes) {
			return graphstate.KeyPage{}, ErrResourceLimit
		}
		out.Next, err = allocatePageCursor()
		if err != nil {
			return graphstate.KeyPage{}, err
		}
	}
	if token != 0 {
		delete(p.cursors, token)
		p.cursorBytes -= old.bytes
	}
	if out.Next != 0 {
		last.Name = strings.Clone(last.Name)
		p.cursors[out.Next] = componentKeyContinuation{hash, last, cost}
		p.cursorBytes += cost
	}
	return out, nil
}
