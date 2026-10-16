package graph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Directory and uniqueness sentinels.
var (
	// ErrLocked reports an Open of a directory that another open Graph holds,
	// in this process or another. Close releases the directory.
	ErrLocked = errors.New("graph: directory is open in another graph")
	// ErrUniqueDeclared reports a DeclareUnique of a property that is already
	// unique (a Config seed or an earlier declaration).
	ErrUniqueDeclared = errors.New("graph: property is already unique")
	// ErrUniqueDuplicates reports a DeclareUnique refused because existing data
	// holds an equal value on two nodes over overlapping valid time. The error
	// names the key and two node IDs, never the value, and also matches
	// ErrUniqueViolation. Nothing is declared.
	ErrUniqueDuplicates = errors.New("graph: existing values hold a duplicate claim")
)

// uniqueDeclPrefix is the engine row listing a node property declared unique at
// runtime, so a reopen knows it without the Config seeds.
var uniqueDeclPrefix = []byte("rho-tkg:v5/pkg/graph:unique-decl:v1:")

func uniqueDeclKey(key string) []byte { return append(bytes.Clone(uniqueDeclPrefix), key...) }

// DeclareUnique makes the node property key unique over all nodes on a graph
// that already holds data, in one commit (gap rows 6-7; a Config seed can only
// do this at genesis). kind is the property's kind. A key never written is
// declared fresh. A key already written (declared by its first write) keeps
// its kind, which must equal kind; its existing values are validated first and
// every one is entered into the unique index, so Tx.GetOrCreate may use the key
// from then on and the engine refuses a second holder in any later Update. A
// duplicate over overlapping valid time is ErrUniqueDuplicates and declares
// nothing; disjoint valid time is not a duplicate. The declaration survives
// Close and reopen. An already unique key is ErrUniqueDeclared; an empty key is
// ErrInvalidOp; a zero, float or unknown kind, or a kind other than the
// declared one, is ErrInvalidValue. Cost: one read of every value of key and a
// single commit, which must fit one install (ErrInvalidRequest otherwise).
func (g *Graph) DeclareUnique(ctx context.Context, key string, kind ValueKind) (Receipt, error) {
	if g == nil {
		return Receipt{}, ErrClosed
	}
	if ctx == nil {
		return Receipt{}, fmt.Errorf("%w: nil context", ErrInvalidRequest)
	}
	if g.eng == nil {
		return Receipt{}, ErrClosed
	}
	if err := nonEmpty("property key", key); err != nil {
		return Receipt{}, err
	}
	if _, err := kindToScalar(kind); err != nil || kind == KindFloat {
		return Receipt{}, fmt.Errorf("%w: a unique property needs a non-float kind", ErrInvalidValue)
	}
	return g.eng.declareUnique(ctx, key, kind)
}

func (e *pebbleEngine) declareUnique(ctx context.Context, key string, kind ValueKind) (Receipt, error) {
	sk, _ := kindToScalar(kind)
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.check(); err != nil {
		return Receipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	base, prev, err := e.store.Checkpoint()
	if err != nil {
		return Receipt{}, err
	}
	next := base + 1
	d, found, err := e.definition(ctx, base, graphstate.Node, key)
	if err != nil {
		return Receipt{}, err
	}
	if found {
		if d.Cardinality != graphstate.ScalarCardinality || d.Type != sk {
			return Receipt{}, fmt.Errorf("%w: the property is declared with another kind", ErrInvalidValue)
		}
		if d.Unique != graphstate.UniqueNone {
			return Receipt{}, ErrUniqueDeclared
		}
	}
	def := graphstate.PropertyDefinition{Name: key, Owner: graphstate.Node, Type: sk, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueScalar}
	view, err := e.store.ApplicationView(base)
	if err != nil {
		return Receipt{}, err
	}
	defer view.Close()
	effects, _, err := graphstore.DeclareUniquePartition(ctx, view, e.decl, e.binding, e.conf, def,
		graphstore.Limits{MaxStageRecords: 4096, MaxReadRows: 65536, MaxReadBytes: 64 << 20},
		graphstore.GraphLimits{MaxOutputBytes: 64 << 20, MaxSourceRows: 1 << 20, MaxSourceBytes: 64 << 20, Pages: graphstore.PageLimits{MaxPatches: 4096, MaxTailAtoms: 4096, MaxWorkRecords: 65536, MaxWorkBytes: 32 << 20}},
		readBudget(), graphstate.Limits{MaxRows: 1 << 20, MaxReadBytes: 64 << 20})
	if err != nil {
		return Receipt{}, mapDeclareErr(key, err)
	}
	digest := sha256.Sum256(binary.BigEndian.AppendUint64(append([]byte("rho-v5 declare unique:"), prev...), next))
	root, err := effects.Root.AdvanceEffects(digest)
	if err != nil {
		return Receipt{}, err
	}
	img, err := graphstore.EncodeRoot(root)
	if err != nil {
		return Receipt{}, err
	}
	inst := e.nextInstant()
	writes := slices.Clone(effects.Writes)
	writes = append(writes, instantRow(next, inst), raftlog.KV{Key: uniqueDeclKey(key), Value: []byte{1}})
	slices.SortFunc(writes, func(a, b raftlog.KV) int { return bytes.Compare(a.Key, b.Key) })
	if err := e.commit(next, prev, img, writes); err != nil {
		return Receipt{}, err
	}
	e.last.Store(inst)
	e.node[key] = def
	return Receipt{tok: e.receiptToken(next, inst)}, nil
}

func mapDeclareErr(key string, err error) error {
	var conflict *graphstate.UniqueConflict
	switch {
	case errors.As(err, &conflict):
		return fmt.Errorf("%w: %w: key %q is held by nodes %d and %d", ErrUniqueDuplicates, ErrUniqueViolation, key, conflict.A, conflict.B)
	case errors.Is(err, graphstore.ErrAlreadyUnique):
		return ErrUniqueDeclared
	}
	return mapStageErr(err)
}

// loadUniqueDecls enters every property declared unique at runtime into the
// engine's definitions (Tx.GetOrCreate reads them from there).
func (e *pebbleEngine) loadUniqueDecls(ctx context.Context, view *raftlog.ApplicationView, c *graphstore.Catalog) error {
	end := bytes.Clone(uniqueDeclPrefix)
	end[len(end)-1]++
	res, err := view.Scan(ctx, uniqueDeclPrefix, end, nil, raftlog.ReadBudget{Rows: 4096, Bytes: 1 << 20})
	if err != nil {
		return err
	}
	for _, row := range res.Rows {
		name := string(row.Key[len(uniqueDeclPrefix):])
		d, found, err := c.Property(ctx, graphstate.Node, name)
		if err != nil || !found || d.Unique != graphstate.UniqueScalar {
			return fmt.Errorf("%w: stored unique declaration has no matching schema", ErrInvalidConfig)
		}
		e.node[name] = d
	}
	return nil
}
