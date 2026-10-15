package main

// An explicitly selected deterministic cost fixture. This remains test support:
// three fixed endpoints plus a declared edge/life interval, not an ID allocator.
// Value-only fields preserve canonical config equality and legacy zero encoding.
type graphCostProfile struct {
	Version      uint8  `json:"version"`
	Facts        uint64 `json:"facts"`
	CrossPercent uint64 `json:"crossPercent"`
	Writers      int    `json:"writers"`
}

func (p graphCostProfile) nodes() [3]endpoint {
	return [3]endpoint{{0, 1, 2}, {0, 3, 4}, {1, 7 + 2*p.Facts, 8 + 2*p.Facts}}
}
func (p graphCostProfile) cross(i uint64) bool {
	return (i+1)*p.CrossPercent/100 > i*p.CrossPercent/100
}
func (p graphCostProfile) edge(i uint64, value int64) graphEdge {
	nodes := p.nodes()
	target := nodes[1]
	if p.cross(i) {
		target = nodes[2]
	}
	return graphEdge{ID: 7 + 2*i, Source: nodes[0], Target: target, Value: value}
}
func graphCostLimit(c config) limits {
	if c.Cost.Version == 6 {
		return graphSelectorsLimit(c.Cost)
	}
	if c.Cost.Version == 5 {
		return graphFeedWorkloadLimit(c.Cost)
	}
	if c.Cost.Version == 3 {
		return graphSelectorsLimit(c.Cost)
	}
	if c.Cost.Version == 4 {
		return graphEdgeUniqueLimit(c.Cost)
	}
	if c.Cost.Version == 2 {
		return graphContentionLimit(c.Cost)
	}
	if c.Cost == (graphCostProfile{}) {
		return limits{Bytes: 16 << 20, Transactions: 1024, Versions: 65536, Certificates: 1024}
	}
	count := 2*c.Cost.Facts + 3
	bytes := (64 << 20) + c.Cost.Facts*(64<<10)
	if c.Cost.Writers == 8 || c.Cost.Writers == 32 {
		count = 3 + 2*c.Cost.Facts*graphCostParallelAttempts
		bytes += 2 * c.Cost.Facts * (graphCostParallelAttempts - 1) * 2048
	}
	return limits{Bytes: bytes, Transactions: count, Versions: 6*c.Cost.Facts + 3, Certificates: 1024}
}
func graphCostConfig(graph string, p graphCostProfile) config {
	c := defaults()
	nodes := p.nodes()
	c.Graph, c.Nodes, c.Cost = graph, [2]uint64{nodes[0].Node, nodes[2].Node}, p
	c.CostScope = lifecycleConfig("validation").Lifecycle.Scope
	c.Limits = graphCostLimit(c)
	return c
}
func validGraphCostProfile(c config) bool {
	if c.Cost.Version == 6 {
		return validGraphColumnsConfig(c)
	}
	if c.Cost.Version == 5 {
		return validGraphFeedWorkloadConfig(c)
	}
	if c.Cost.Version == 3 {
		return validGraphSelectorsConfig(c)
	}
	if c.Cost.Version == 4 {
		return validGraphEdgeUniqueConfig(c)
	}
	if c.Cost.Version == 2 {
		return validGraphContentionConfig(c)
	}
	p := c.Cost
	parallel := (p.Writers == 8 || p.Writers == 32) && (c.Graph == "graph-cost-parallel-v1" || c.Graph == "graph-cost-parallel-columns-v1" || graphCostRetainedConfig(c))
	if p.Version != 1 || p.Facts < 1 || p.Facts > 1_000_000 || p.Writers != 1 && !parallel || p.CrossPercent != 0 && p.CrossPercent != 10 && p.CrossPercent != 50 && p.CrossPercent != 100 || c.Lifecycle != (lifecycleProfile{}) || c.CostScope != lifecycleConfig("validation").Lifecycle.Scope {
		return false
	}
	n := p.nodes()
	return c.Nodes == [2]uint64{n[0].Node, n[2].Node}
}
func (c config) graphNodeOwner(id uint64) uint8 {
	if c.Cost.Version == 4 {
		for _, n := range graphEdgeUniqueNodes(c.Cost) {
			if n.Node == id {
				return n.Owner
			}
		}
		return 2
	}
	if c.Cost != (graphCostProfile{}) {
		for _, n := range c.Cost.nodes() {
			if n.Node == id {
				return n.Owner
			}
		}
		return 2
	}
	if id == c.Nodes[0] {
		return 0
	}
	if id == c.Nodes[1] {
		return 1
	}
	return 2
}
func (h *harness) validateGraphCostLifecycle(x transaction) error {
	if h.config.Cost.Version == 4 {
		return h.validateGraphEdgeUniqueShape(x)
	}
	if h.config.Cost.Version == 2 {
		return h.validateGraphContention(x)
	}
	if !validGraphCostProfile(h.config) || x.GraphChange == nil || x.Lifecycle == nil {
		return errInvalid
	}
	p, g, m := h.config.Cost, *x.GraphChange, *x.Lifecycle
	if g.Kind == "node-create" {
		for i, n := range p.nodes() {
			if g.Node == n && m.Provenance == uint64(i+1) && m.EdgeLife == 0 && m.Definition == nil {
				return nil
			}
		}
		return errInvalid
	}
	if g.Kind != "edge-create" && g.Kind != "edge-update" || g.Edge.ID < 7 || (g.Edge.ID-7)%2 != 0 {
		return errInvalid
	}
	i := (g.Edge.ID - 7) / 2
	if i >= p.Facts || g.Edge != p.edge(i, g.Edge.Value) || m.EdgeLife != g.Edge.ID+1 || m.Definition == nil {
		return errInvalid
	}
	value, id, provenance := int64(5), uint64(5), 4+i
	if g.Kind == "edge-update" {
		value, id, provenance = 6, 6, 4+p.Facts+i
	}
	if g.Edge.Value != value || *m.Definition != (lifecycleDefinition{ID: id, Kind: "i64", I64: value}) || m.Provenance != provenance {
		return errInvalid
	}
	return nil
}
