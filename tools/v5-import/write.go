package importmap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
)

// Write-path ambiguity kinds (a value or entity the graph could not take as it was).
const (
	KindKeyKindConflict = "key_kind_conflict" // a later value of another kind than the key's first (dropped under DropKindConflicts)
	KindNullOnlyKey     = "null_only_key"     // every value of the key is null: nothing can declare it, the nulls are not written
	KindAlreadyExists   = "already_exists"    // the graph already held the ID: the entity is left as it was
)

// metaKey is the engine Meta row holding the import's progress. It is written
// in the same Update as every batch, so "how far did the import get" is never
// a guess: there is no window between a commit and its bookkeeping.
const metaKey = "rho.import"

// Options tunes Import. The zero value is fine.
type Options struct {
	// MaxOps caps the ops of one Update (default 256). A batch the store refuses as
	// too large is halved until it fits.
	MaxOps int
	// AfterBatch runs after each committed batch with the number of ops applied
	// so far; an error stops the import (the graph keeps what is committed).
	AfterBatch func(applied int) error
	// DropKindConflicts drops a value whose kind differs from its key's (reported
	// as KindKeyKindConflict) instead of refusing the import.
	DropKindConflicts bool
}

// ErrTooManyExisting stops an import whose list of pre-existing IDs no longer fits one Meta row.
var ErrTooManyExisting = errors.New("importmap: too many pre-existing IDs, import into a fresh graph")

// Report lists what the write path did not take as it was.
type Report struct {
	Ambiguities []Ambiguity
}

type ref struct {
	kind string
	id   int64
}

func isCreate(op string) bool { return op == "create_node" || op == "create_rel" }

// check refuses a stream the write path cannot apply: an ID that is zero or
// negative, a node and a relationship on one ID (they share one ID space), a
// life created twice or before the life before it ended, an op on an entity
// nothing created, a relationship to a node nothing created.
func (r *Result) check() error {
	if r == nil {
		return errors.New("importmap: nil result")
	}
	lives := map[ref]int{}   // newest life created
	closed := map[ref]bool{} // the newest life has an End or Retract
	nodes := map[int64]bool{}
	rels := map[int64]bool{}
	for i, o := range r.Ops {
		if o.Entity != "node" && o.Entity != "rel" {
			return fmt.Errorf("importmap: op %d: entity %q", i, o.Entity)
		}
		if o.ID <= 0 || o.ID >= math.MaxInt64 {
			return fmt.Errorf("importmap: op %d: %s id %d is not a positive v4 ID", i, o.Entity, o.ID)
		}
		if o.Life < 1 {
			return fmt.Errorf("importmap: op %d: life %d", i, o.Life)
		}
		k := ref{o.Entity, o.ID}
		switch o.Op {
		case "create_node", "create_rel":
			if (o.Op == "create_node") != (o.Entity == "node") {
				return fmt.Errorf("importmap: op %d: %s on a %s", i, o.Op, o.Entity)
			}
			if o.Entity == "node" && rels[o.ID] || o.Entity == "rel" && nodes[o.ID] {
				return fmt.Errorf("importmap: op %d: node and rel share ID %d", i, o.ID)
			}
			if lives[k] >= o.Life {
				return fmt.Errorf("importmap: op %d: %s %d life %d is created twice", i, o.Entity, o.ID, o.Life)
			}
			if o.Life != lives[k]+1 {
				return fmt.Errorf("importmap: op %d: %s %d life %d follows life %d", i, o.Entity, o.ID, o.Life, lives[k])
			}
			if o.Life > 1 && !closed[k] {
				return fmt.Errorf("importmap: op %d: %s %d life %d starts before life %d has ended", i, o.Entity, o.ID, o.Life, o.Life-1)
			}
			if o.Op == "create_rel" && (!nodes[o.Start] || !nodes[o.End]) {
				return fmt.Errorf("importmap: op %d: rel %d runs between nodes %d and %d, not both created", i, o.ID, o.Start, o.End)
			}
			lives[k], closed[k] = o.Life, false
			if o.Entity == "node" {
				nodes[o.ID] = true
			} else {
				rels[o.ID] = true
			}
		case "correct", "unset", "end", "retract", "add_label", "remove_label":
			if o.Life != lives[k] {
				return fmt.Errorf("importmap: op %d: %s on %s %d life %d, which nothing created", i, o.Op, o.Entity, o.ID, o.Life)
			}
			if (o.Op == "add_label" || o.Op == "remove_label") && o.Entity != "node" {
				return fmt.Errorf("importmap: op %d: %s on a rel", i, o.Op)
			}
			if o.Op == "end" || o.Op == "retract" {
				closed[k] = true
			}
		default:
			return fmt.Errorf("importmap: op %d: unknown op %q", i, o.Op)
		}
	}
	return nil
}

// Seed is the largest v4 ID of the stream: the allocator is seeded here so no
// new entity is ever minted on a v4 ID.
func (r *Result) Seed() uint64 {
	var m int64
	for _, o := range r.Ops {
		m = max(m, o.ID)
	}
	return uint64(m)
}

// plan is the stream as written: values the typed schema cannot hold are
// refused (or dropped and reported under DropKindConflicts), and the schema
// seeds the graph at genesis so a null first value or an import order never
// decides a key's kind.
type plan struct {
	ops    []Op
	node   []graph.PropertySchema
	rel    []graph.PropertySchema
	report []Ambiguity
}

func (r *Result) plan(o Options) (*plan, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	p := &plan{}
	kinds := map[[2]string]graph.ValueKind{}
	firstAt := map[[2]string]int64{} // the ID whose value declared the key
	nulls := map[[2]string]Op{}
	var order, nullOrder [][2]string
	for _, op := range r.Ops {
		for _, k := range sortedPropKeys(op.Props) {
			v := op.Props[k]
			sk := [2]string{op.Entity, k}
			if v.Kind() == graph.KindNull {
				if _, ok := nulls[sk]; !ok {
					nulls[sk] = op
					nullOrder = append(nullOrder, sk)
				}
				continue
			}
			if _, ok := kinds[sk]; !ok {
				kinds[sk], firstAt[sk] = v.Kind(), op.ID
				order = append(order, sk)
			}
		}
	}
	// A key that is only ever null declares nothing: its nulls are not written.
	nullOnly := map[[2]string]bool{}
	for _, sk := range nullOrder {
		if _, ok := kinds[sk]; !ok {
			nullOnly[sk] = true
			op := nulls[sk]
			p.report = append(p.report, Ambiguity{Entity: op.Entity, ID: op.ID, Version: op.Version, Key: sk[1], Kind: KindNullOnlyKey})
		}
	}
	for _, sk := range order {
		ps := graph.PropertySchema{Key: sk[1], Kind: kinds[sk]}
		if sk[0] == "node" {
			p.node = append(p.node, ps)
		} else {
			p.rel = append(p.rel, ps)
		}
	}
	for _, op := range r.Ops {
		if len(op.Props) > 0 {
			props := map[string]PropValue{}
			for _, k := range sortedPropKeys(op.Props) {
				v := op.Props[k]
				sk := [2]string{op.Entity, k}
				switch {
				case nullOnly[sk]:
				case v.Kind() == graph.KindNull || fits(kinds[sk], v.Value):
					props[k] = v
				case o.DropKindConflicts:
					p.report = append(p.report, Ambiguity{Entity: op.Entity, ID: op.ID, Version: op.Version, Key: k, Kind: KindKeyKindConflict})
				default:
					return nil, fmt.Errorf("importmap: %s %d: key %q holds a %s value, but the key is %s since %s %d (Options.DropKindConflicts drops such values)",
						op.Entity, op.ID, k, kindName(v.Kind()), kindName(kinds[sk]), op.Entity, firstAt[sk])
				}
			}
			if len(props) == 0 {
				props = nil
			}
			if op.Op == "correct" && props == nil {
				continue // nothing left to correct
			}
			op.Props = props
		}
		p.ops = append(p.ops, op)
	}
	return p, nil
}

func kindName(k graph.ValueKind) string {
	switch k {
	case graph.KindString:
		return "string"
	case graph.KindBool:
		return "bool"
	case graph.KindInt:
		return "int"
	case graph.KindFloat:
		return "float"
	case graph.KindBytes:
		return "bytes"
	case graph.KindList:
		return "list"
	}
	return "null"
}

func sortedPropKeys(m map[string]PropValue) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// fits reports whether the engine takes v under a key of kind want: the same
// kind, or an exact numeric (an integral float into an int key, an int a float holds exactly).
func fits(want graph.ValueKind, v graph.Value) bool {
	if v.Kind() == want {
		return true
	}
	switch {
	case want == graph.KindInt && v.Kind() == graph.KindFloat:
		f, _ := v.AsFloat()
		return f == math.Trunc(f) && f >= -(1<<63) && f < 1<<63
	case want == graph.KindFloat && v.Kind() == graph.KindInt:
		n, _ := v.AsInt()
		f := float64(n)
		return f < 1<<63 && int64(f) == n
	}
	return false
}

// Config returns base with the allocator seeded above every v4 ID and the
// property schema of the stream. A graph written by Import must be opened with it
// (and reopened with it: the seed is stored, the schema is checked). It refuses
// a stream Import would refuse (o is the Options Import gets).
func (r *Result) Config(base graph.Config, o Options) (graph.Config, error) {
	p, err := r.plan(o)
	if err != nil {
		return graph.Config{}, err
	}
	merge := func(have, add []graph.PropertySchema) []graph.PropertySchema {
		out := slices.Clone(have)
		for _, a := range add {
			if !slices.ContainsFunc(out, func(h graph.PropertySchema) bool { return h.Key == a.Key }) {
				out = append(out, a)
			}
		}
		return out
	}
	base.Allocation.Seed = r.Seed()
	base.NodeProperties = merge(base.NodeProperties, p.node)
	base.RelProperties = merge(base.RelProperties, p.rel)
	return base, base.Validate()
}

// progress is the Meta row: how many ops of the plan are applied, which
// stream it is, and the entities the graph already held (left as they were).
type progress struct {
	Applied     int        `json:"applied"`
	Fingerprint string     `json:"fingerprint"`
	Skipped     []skipPair `json:"skipped,omitempty"`
}

type skipPair struct {
	Kind    string `json:"kind"`
	ID      int64  `json:"id"`
	Version uint32 `json:"version,omitempty"`
}

// Import applies the op stream to g in batches (each one Update, under the
// store's stage-record limit), keeping every v4 ID and life. cfg must be the
// Config the graph was opened with (Result.Config). An import that stopped is
// resumed by calling Import again with the same stream: progress is the
// graph's Meta row, written inside every batch, so nothing is applied twice.
// An ID the graph already holds is left as it is and reported.
func (r *Result) Import(ctx context.Context, g *graph.Graph, cfg graph.Config, o Options) (*Report, error) {
	p, err := r.plan(o)
	if err != nil {
		return nil, err
	}
	if cfg.Allocation.Seed < r.Seed() {
		return nil, fmt.Errorf("importmap: allocator seed %d is not above the largest v4 ID %d (use Result.Config)", cfg.Allocation.Seed, r.Seed())
	}
	if o.MaxOps <= 0 {
		o.MaxOps = 256
	}
	w := &writer{ctx: ctx, g: g, cfg: cfg, p: p, skipped: map[ref]bool{}, rep: Report{Ambiguities: slices.Clone(p.report)}}
	sum := sha256.Sum256((&Result{Ops: p.ops}).OpsJSON())
	w.print = hex.EncodeToString(sum[:])
	if err := w.resume(); err != nil {
		return nil, err
	}
	for w.applied < len(p.ops) {
		n := w.cut(min(o.MaxOps, len(p.ops)-w.applied))
		for {
			err := w.batch(w.applied, n)
			if err == nil {
				break
			}
			switch {
			case errors.Is(err, graph.ErrInvalidRequest) && n > 1, errors.Is(err, graph.ErrAlreadyExists) && n > 1:
				n = w.cut((n + 1) / 2)
				continue
			case errors.Is(err, graph.ErrAlreadyExists) && isCreate(p.ops[w.applied].Op):
				// One create names an ID the graph holds: leave that entity as it is.
				op := p.ops[w.applied]
				w.skipped[ref{op.Entity, op.ID}] = true
				w.skipOrder = append(w.skipOrder, skipPair{op.Entity, op.ID, op.Version})
				w.rep.Ambiguities = append(w.rep.Ambiguities, Ambiguity{Entity: op.Entity, ID: op.ID, Version: op.Version, Kind: KindAlreadyExists})
				continue
			}
			return nil, err
		}
		w.applied += n
		if o.AfterBatch != nil {
			if err := o.AfterBatch(w.applied); err != nil {
				return nil, fmt.Errorf("importmap: stopped after %d ops: %w", w.applied, err)
			}
		}
	}
	return &w.rep, nil
}

type writer struct {
	ctx       context.Context
	g         *graph.Graph
	cfg       graph.Config
	p         *plan
	print     string
	applied   int
	skipped   map[ref]bool
	skipOrder []skipPair
	rep       Report
}

// cut shortens a batch of n ops from applied so that no entity gets ops
// recorded at two different instants in one commit: a commit is one revision
// of the entity, and its evidence must be one instant.
func (w *writer) cut(n int) int {
	rec := map[ref]int64{}
	for i := 0; i < n; i++ {
		o := w.p.ops[w.applied+i]
		k := ref{o.Entity, o.ID}
		if at, ok := rec[k]; ok && at != o.Recorded {
			return max(i, 1)
		}
		rec[k] = o.Recorded
	}
	return n
}

// resume reads the Meta row: where the import stopped.
func (w *writer) resume() error {
	s, err := w.g.OpenSnapshot(w.ctx, graph.Fresh())
	if err != nil {
		return err
	}
	defer s.Close()
	raw, ok, err := s.Meta(w.ctx, metaKey)
	if err != nil || !ok {
		return err
	}
	var pr progress
	if err := json.Unmarshal(raw, &pr); err != nil {
		return fmt.Errorf("importmap: the import's Meta row is unreadable: %w", err)
	}
	if pr.Fingerprint != w.print {
		return errors.New("importmap: the graph holds an import of another op stream")
	}
	if pr.Applied < 0 || pr.Applied > len(w.p.ops) {
		return fmt.Errorf("importmap: Meta says %d ops applied of %d", pr.Applied, len(w.p.ops))
	}
	for _, k := range pr.Skipped {
		r := ref{k.Kind, k.ID}
		w.skipped[r] = true
		w.skipOrder = append(w.skipOrder, k)
		w.rep.Ambiguities = append(w.rep.Ambiguities, Ambiguity{Entity: k.Kind, ID: k.ID, Version: k.Version, Kind: KindAlreadyExists})
	}
	w.applied = pr.Applied
	return nil
}

// batch applies ops [from, from+n) and the progress row in one Update.
func (w *writer) batch(from, n int) error {
	_, err := w.g.Update(w.ctx, func(tx graph.Tx) error {
		for _, op := range w.p.ops[from : from+n] {
			if w.skipped[ref{op.Entity, op.ID}] {
				continue
			}
			gops, err := op.Graphs(w.cfg)
			if err != nil {
				return err
			}
			for _, gop := range gops {
				if err := applyOp(tx, gop); err != nil {
					return fmt.Errorf("importmap: %s on %s %d: %w", op.Op, op.Entity, op.ID, err)
				}
			}
		}
		pr := progress{Applied: from + n, Fingerprint: w.print}
		pr.Skipped = w.skipOrder
		raw, err := json.Marshal(pr)
		if err != nil {
			return err
		}
		if len(raw) > graph.MaxMetaValueBytes {
			return ErrTooManyExisting
		}
		return tx.SetMeta(metaKey, raw)
	})
	return err
}

func applyOp(tx graph.Tx, op graph.Op) error {
	var err error
	switch x := op.(type) {
	case graph.CreateNode:
		_, err = tx.CreateNode(x)
	case graph.CreateRel:
		_, err = tx.CreateRel(x)
	case graph.Reopen:
		err = tx.Reopen(x)
	case graph.Correct:
		err = tx.Correct(x)
	case graph.Unset:
		err = tx.Unset(x)
	case graph.AddLabel:
		err = tx.AddLabel(x)
	case graph.RemoveLabel:
		err = tx.RemoveLabel(x)
	case graph.End:
		err = tx.End(x)
	case graph.Retract:
		err = tx.Retract(x)
	default:
		err = fmt.Errorf("importmap: op %T", op)
	}
	return err
}
