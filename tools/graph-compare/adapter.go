package slice

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Adapter owns a private native graph, never an external entity/value oracle.
// Calls count actual public native API invocations, not internal storage writes.
type Adapter struct {
	mu              sync.Mutex
	g               *graph.Graph
	backend, dir    string
	closed          bool
	uniformEntity   bool
	Calls           map[string]uint64
	RangeCandidates uint64
	Visited         int
	maxVisited      int
}

func openAdapter(backend, dir string) (*Adapter, error) {
	if backend != "memory" && backend != "badger" || backend == "badger" && dir == "" {
		return nil, ErrContract
	}
	cfg := graph.Config{Validation: graph.ValidationLimits{AllowSelfLoops: true}}
	if backend == "badger" {
		cfg.BadgerDir = dir
		cfg.SyncWrites = true
	}
	g, err := graph.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Adapter{g: g, backend: backend, dir: dir, uniformEntity: true, Calls: make(map[string]uint64), maxVisited: defaultLimits().MaxVisited}, nil
}
func (a *Adapter) call(name string) { a.Calls[name]++ }
func (a *Adapter) check(ctx context.Context) error {
	if a == nil || ctx == nil {
		return ErrContract
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.closed {
		return graph.ErrGraphClosed
	}
	return nil
}

// Close releases the private native graph; repeated calls are harmless.
func (a *Adapter) Close() error {
	if a == nil {
		return ErrContract
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.call("graph_close")
	err := a.g.Close()
	a.closed = true
	return err
}

// Reopen reopens the owned synchronous Badger store; memory is unsupported.
func (a *Adapter) Reopen() error {
	if a == nil {
		return ErrContract
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.backend != "badger" {
		return ErrUnsupported
	}
	if !a.closed {
		a.call("graph_close")
		if err := a.g.Close(); err != nil {
			return err
		}
		a.closed = true
	}
	a.call("graph_reopen")
	g, err := graph.New(graph.Config{BadgerDir: a.dir, SyncWrites: true, Validation: graph.ValidationLimits{AllowSelfLoops: true}})
	if err != nil {
		return err
	}
	a.g = g
	a.closed = false
	return nil
}
func validateEntity(e Entity) (int64, map[string]any, error) {
	id, err := nativeID(e.ID, e.Kind)
	if err != nil {
		return 0, nil, err
	}
	props, err := nativeProperties(e.Properties)
	if err != nil {
		return 0, nil, err
	}
	if e.Kind == "node" {
		if e.Source != "" || e.Target != "" || e.Type != "" || len(e.Labels) < 1 || len(e.Labels) > 64 {
			return 0, nil, ErrContract
		}
		seen := make(map[string]bool)
		for _, l := range e.Labels {
			if l == "" || len(l) > 256 || seen[l] {
				return 0, nil, ErrContract
			}
			seen[l] = true
		}
	} else {
		if len(e.Labels) != 0 || e.Type == "" || len(e.Type) > 256 {
			return 0, nil, ErrContract
		}
		if _, err := nativeID(e.Source, "node"); err != nil {
			return 0, nil, err
		}
		if _, err := nativeID(e.Target, "node"); err != nil {
			return 0, nil, err
		}
	}
	return id, props, nil
}
func (a *Adapter) load(ctx context.Context, e Entity) error {
	id, props, err := validateEntity(e)
	if err != nil {
		return err
	}
	if e.Kind == "node" {
		a.uniformEntity = a.uniformEntity && slices.Contains(e.Labels, "Entity")
		a.call("nodes_import")
		_, err = a.g.Nodes().Import(ctx, types.NodeID(id), e.Labels, props)
		return err
	}
	s, _ := nativeID(e.Source, "node")
	d, _ := nativeID(e.Target, "node")
	a.call("nodes_get")
	start, err := a.g.Nodes().Get(ctx, types.NodeID(s))
	if err != nil {
		return err
	}
	a.call("nodes_get")
	end, err := a.g.Nodes().Get(ctx, types.NodeID(d))
	if err != nil {
		return err
	}
	a.call("rels_import")
	_, err = a.g.Rels().Import(ctx, types.RelID(id), e.Type, start, end, props)
	return err
}

// Load imports a checked external identity and complete row into the native graph.
func (a *Adapter) Load(ctx context.Context, e Entity) error {
	if a == nil {
		return ErrContract
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.check(ctx); err != nil {
		return err
	}
	return a.load(ctx, e)
}

// BuildIndexes installs the declared native index when its label scope is uniform.
func (a *Adapter) BuildIndexes() error {
	if a == nil {
		return ErrContract
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.check(context.Background()); err != nil {
		return err
	}
	if !a.uniformEntity {
		return nil
	}
	a.call("node_index_create")
	return a.g.Index().CreateProperty("Entity", "p_i64")
}

// Apply applies one supported change event through native graph operations.
func (a *Adapter) Apply(ctx context.Context, e Event) (retErr error) {
	if a == nil {
		return ErrContract
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.check(ctx); err != nil {
		return err
	}
	if e.Revision < 1 {
		return ErrContract
	}
	if e.Op == "append" {
		return a.load(ctx, e.Row)
	}
	if e.Op == "upsert" {
		id, props, err := validateEntity(e.Row)
		if err != nil {
			return err
		}
		if e.Row.Kind != "node" {
			return ErrUnsupported
		}
		a.call("nodes_get")
		prior, err := a.g.Nodes().Get(ctx, types.NodeID(id))
		if errors.Is(err, graph.ErrNodeNotFound) {
			return a.load(ctx, e.Row)
		}
		if err != nil {
			return err
		}
		updates := props
		for key := range prior.PropertiesMap() {
			if _, exists := props[key]; !exists {
				updates[key] = nil
			}
		}
		labels := a.g.Nodes().Labels(prior)
		a.call("node_labels")
		a.call("tx_begin")
		tx, err := a.g.Tx().Begin()
		if err != nil {
			return err
		}
		committed := false
		defer func() {
			if !committed {
				a.call("tx_rollback")
				retErr = errors.Join(retErr, tx.Rollback())
			}
		}()
		a.call("tx_update_node")
		if _, err := tx.UpdateNode(types.NodeID(id), updates); err != nil {
			return err
		}
		for _, l := range e.Row.Labels {
			if !slices.Contains(labels, l) {
				a.call("tx_add_label")
				if err := tx.AddNodeLabel(types.NodeID(id), l); err != nil {
					return err
				}
			}
		}
		for _, l := range labels {
			if !slices.Contains(e.Row.Labels, l) {
				a.call("tx_remove_label")
				if err := tx.RemoveNodeLabel(types.NodeID(id), l); err != nil {
					return err
				}
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		a.call("tx_commit")
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
		a.uniformEntity = a.uniformEntity && slices.Contains(e.Row.Labels, "Entity")
		return nil
	}
	id, err := nativeID(e.ID, e.Kind)
	if err != nil {
		return err
	}
	if e.Op == "delete" {
		if e.Kind == "node" {
			incident := false
			a.call("outgoing_scan")
			err = a.g.Rels().ForEachOutgoing(types.NodeID(id), "", func(*types.Relationship) bool { incident = true; return false })
			if err != nil {
				return err
			}
			a.call("incoming_scan")
			err = a.g.Rels().ForEachIncoming(types.NodeID(id), "", func(*types.Relationship) bool { incident = true; return false })
			if err != nil {
				return err
			}
			if incident {
				return ErrContract
			}
			a.call("nodes_delete")
			return a.g.Nodes().Delete(ctx, types.NodeID(id))
		}
		a.call("rels_delete")
		return a.g.Rels().Delete(ctx, types.RelID(id))
	}
	if e.Op != "update" {
		return ErrUnsupported
	}
	updates, err := nativeProperties(e.Set)
	if err != nil {
		return err
	}
	seen := make(map[string]bool)
	for _, key := range e.Remove {
		if key == "" || seen[key] {
			return ErrContract
		}
		if _, exists := updates[key]; exists {
			return ErrContract
		}
		seen[key] = true
		updates[key] = nil
	}
	if e.Kind == "node" {
		a.call("nodes_get")
		n, err := a.g.Nodes().Get(ctx, types.NodeID(id))
		if err != nil {
			return err
		}
		for _, key := range e.Remove {
			if _, ok := n.GetProperty(key); !ok {
				return ErrContract
			}
		}
		a.call("nodes_update")
		_, err = a.g.Nodes().Update(ctx, types.NodeID(id), updates)
		return err
	}
	a.call("rels_get")
	r, err := a.g.Rels().Get(ctx, types.RelID(id))
	if err != nil {
		return err
	}
	for _, key := range e.Remove {
		if _, ok := r.GetProperty(key); !ok {
			return ErrContract
		}
	}
	a.call("rels_update")
	_, err = a.g.Rels().Update(ctx, types.RelID(id), updates)
	return err
}
func nativeCells(props map[string]any) (map[string]Cell, error) {
	out := make(map[string]Cell, len(props))
	for k, v := range props {
		c, err := cellFromNative(v)
		if err != nil {
			return nil, err
		}
		out[k] = c
	}
	return out, nil
}
func (a *Adapter) node(n *types.Node) (Entity, error) {
	if n == nil || n.ID() <= 0 {
		return Entity{}, ErrContract
	}
	p, err := nativeCells(n.PropertiesMap())
	if err != nil {
		return Entity{}, err
	}
	a.call("node_labels")
	labels := a.g.Nodes().Labels(n)
	slices.Sort(labels)
	return Entity{Kind: "node", ID: externalID(int64(n.ID()), "node"), Labels: labels, Properties: p}, nil
}
func (a *Adapter) rel(r *types.Relationship) (Entity, error) {
	if r == nil || r.ID() <= 0 {
		return Entity{}, ErrContract
	}
	p, err := nativeCells(r.PropertiesMap())
	if err != nil {
		return Entity{}, err
	}
	a.call("rel_type")
	return Entity{Kind: "edge", ID: externalID(int64(r.ID()), "edge"), Source: externalID(int64(r.StartNodeID()), "node"), Target: externalID(int64(r.EndNodeID()), "node"), Type: a.g.Rels().Type(r), Properties: p}, nil
}
func cellMatches(v Cell, r Request) (bool, error) {
	n, err := v.Native()
	if err != nil {
		return false, err
	}
	if r.Op == "equality" {
		w, err := r.Value.Native()
		return err == nil && v.Type == r.Value.Type && n == w, err
	}
	lo, err := r.Low.Native()
	if err != nil {
		return false, err
	}
	hi, err := r.High.Native()
	if err != nil {
		return false, err
	}
	if r.Low.Type != r.High.Type {
		return false, ErrContract
	}
	if v.Type != r.Low.Type {
		return false, nil
	}
	switch n := n.(type) {
	case int64:
		l, ok := lo.(int64)
		h, ok2 := hi.(int64)
		if !ok || !ok2 || l > h {
			return false, ErrContract
		}
		return n >= l && n <= h, nil
	case float64:
		l, ok := lo.(float64)
		h, ok2 := hi.(float64)
		if !ok || !ok2 || l > h {
			return false, ErrContract
		}
		return n >= l && n <= h, nil
	default:
		return false, ErrUnsupported
	}
}
func projection(e Entity, keys []string) Projection {
	p := Projection{Kind: e.Kind, ID: e.ID, Columns: make(map[string]Column, len(keys))}
	for _, k := range keys {
		if c, ok := e.Properties[k]; ok {
			p.Columns[k] = Column{Presence: "present", Value: new(c)}
		} else {
			p.Columns[k] = Column{Presence: "absent"}
		}
	}
	return p
}

// Query streams complete native current answers under the adapter callback cap.
func (a *Adapter) Query(ctx context.Context, r Request, emit func(any) error) error {
	return a.query(ctx, r, emit, defaultLimits().MaxVisited)
}

func (a *Adapter) query(ctx context.Context, r Request, emit func(any) error, maxVisited int) error {
	if a == nil || emit == nil {
		return ErrContract
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.check(ctx); err != nil {
		return err
	}
	a.Visited = 0
	a.maxVisited = maxVisited
	if r.Op == "label" {
		if r.Kind != "" && r.Kind != "node" {
			return ErrContract
		}
		r.Kind = "node"
	}
	if r.Op == "type" {
		if r.Kind != "" && r.Kind != "edge" {
			return ErrContract
		}
		r.Kind = "edge"
	}
	if r.Op == "history" {
		return ErrUnsupported
	}
	if err := validateRequest(r); err != nil {
		return err
	}
	var callbackErr error
	visit := func() bool {
		if callbackErr != nil {
			return false
		}
		if err := ctx.Err(); err != nil {
			callbackErr = err
			return false
		}
		a.Visited++
		if a.Visited > a.maxVisited {
			callbackErr = ErrLimit
			return false
		}
		return true
	}
	consume := func(e Entity, err error) bool {
		if err != nil {
			callbackErr = err
			return false
		}
		switch r.Op {
		case "scan":
			callbackErr = emit(e)
		case "projection":
			callbackErr = emit(projection(e, r.Keys))
		case "label":
			if slices.Contains(e.Labels, r.Label) {
				callbackErr = emit(IDRow{e.Kind, e.ID})
			}
		case "type":
			if e.Type == r.Type {
				callbackErr = emit(IDRow{e.Kind, e.ID})
			}
		case "equality", "range":
			if c, ok := e.Properties[r.Key]; ok {
				match, err := cellMatches(c, r)
				if err != nil {
					callbackErr = err
					return false
				}
				if match {
					callbackErr = emit(IDRow{e.Kind, e.ID})
				}
			}
		default:
			callbackErr = ErrUnsupported
		}
		return callbackErr == nil
	}
	if r.Op == "lookup" {
		if !visit() {
			return callbackErr
		}
		id, err := nativeID(r.ID, r.Kind)
		if err != nil {
			return err
		}
		if r.Kind == "node" {
			a.call("nodes_get")
			n, err := a.g.Nodes().Get(ctx, types.NodeID(id))
			if errors.Is(err, graph.ErrNodeNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			e, err := a.node(n)
			if err != nil {
				return err
			}
			return emit(e)
		}
		a.call("rels_get")
		rel, err := a.g.Rels().Get(ctx, types.RelID(id))
		if errors.Is(err, graph.ErrRelNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		e, err := a.rel(rel)
		if err != nil {
			return err
		}
		return emit(e)
	}
	if r.Op == "adjacency" || r.Op == "expand" {
		id, err := nativeID(r.ID, "node")
		if err != nil {
			return err
		}
		if r.Op == "expand" {
			if r.MaxDepth < 1 || r.MaxDepth > 4 {
				return ErrContract
			}
			a.call("nodes_get")
			_, err := a.g.Nodes().Get(ctx, types.NodeID(id))
			if errors.Is(err, graph.ErrNodeNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			var walk func(types.NodeID, []string, []string, int) error
			walk = func(current types.NodeID, edges, nodes []string, remaining int) error {
				if remaining == 0 {
					return nil
				}
				a.call("outgoing_scan")
				err := a.g.Rels().ForEachOutgoing(current, "", func(rel *types.Relationship) bool {
					if !visit() {
						return false
					}
					nextE := append(slices.Clone(edges), externalID(int64(rel.ID()), "edge"))
					nextN := append(slices.Clone(nodes), externalID(int64(rel.EndNodeID()), "node"))
					callbackErr = emit(Walk{Start: r.ID, Depth: len(nextE), EdgeIDs: nextE, NodeIDs: nextN})
					if callbackErr == nil {
						callbackErr = walk(rel.EndNodeID(), nextE, nextN, remaining-1)
					}
					return callbackErr == nil
				})
				return errors.Join(err, callbackErr)
			}
			return walk(types.NodeID(id), nil, []string{r.ID}, r.MaxDepth)
		}
		fn := func(rel *types.Relationship) bool {
			if !visit() {
				return false
			}
			a.call("rel_type")
			callbackErr = emit(Adjacency{externalID(int64(rel.ID()), "edge"), externalID(int64(rel.StartNodeID()), "node"), externalID(int64(rel.EndNodeID()), "node"), a.g.Rels().Type(rel)})
			return callbackErr == nil
		}
		switch r.Direction {
		case "out":
			a.call("outgoing_scan")
			err = a.g.Rels().ForEachOutgoing(types.NodeID(id), "", fn)
		case "in":
			a.call("incoming_scan")
			err = a.g.Rels().ForEachIncoming(types.NodeID(id), "", fn)
		default:
			return ErrContract
		}
		return errors.Join(err, callbackErr)
	}
	if r.Kind != "node" && r.Kind != "edge" {
		return ErrContract
	}
	if r.Op == "range" && r.Kind == "node" && r.Key == "p_i64" && a.uniformEntity {
		lo, err := r.Low.Native()
		if err != nil {
			return err
		}
		hi, err := r.High.Native()
		if err != nil {
			return err
		}
		l, ok := lo.(int64)
		h, ok2 := hi.(int64)
		if !ok || !ok2 || l > h {
			return ErrContract
		}
		a.call("node_range_candidates")
		err = a.g.Nodes().ForEachByLabelPropertyRange("Entity", r.Key, float64(l), float64(h), true, true, store.QueryOpts{}, func(n *types.Node) bool {
			if !visit() {
				return false
			}
			a.RangeCandidates++
			e, err := a.node(n)
			return consume(e, err)
		})
		if !errors.Is(err, graph.ErrIndexNotFound) {
			return errors.Join(err, callbackErr)
		}
		a.call("range_fallback")
	}
	if r.Kind == "node" {
		a.call("nodes_scan")
		err := a.g.Nodes().ForEach(store.QueryOpts{}, func(n *types.Node) bool {
			if !visit() {
				return false
			}
			if r.Op == "range" {
				a.RangeCandidates++
			}
			e, err := a.node(n)
			return consume(e, err)
		})
		return errors.Join(err, callbackErr)
	}
	a.call("rels_scan")
	err := a.g.Rels().ForEach(store.QueryOpts{}, func(rel *types.Relationship) bool {
		if !visit() {
			return false
		}
		e, err := a.rel(rel)
		return consume(e, err)
	})
	return errors.Join(err, callbackErr)
}
