package main

// This is a fixed comparison contract, not a generic rho schema or backend.
// Every field is a value so config equality remains structural after decode.
type lifecycleScope struct {
	Kind          string `json:"kind"`
	AxisProfile   string `json:"axisProfile"`
	AxisVersion   uint16 `json:"axisVersion"`
	CanonicalUnit string `json:"canonicalUnit"`
	Reference     string `json:"reference"`
}
type lifecycleDefinition struct {
	ID   uint64
	Kind string
	I64  int64
}
type lifecycleProfile struct {
	Version        uint8
	Scope          lifecycleScope
	Edge, EdgeLife uint64
	NodeLives      [2]uint64
	ReopenedLife   uint64
	// Declared input definitions, not observed dictionary membership. Only an
	// actual committed transaction definition introduces NewValues in a trace.
	Values               [2]lifecycleDefinition
	Type, Mode, Property string
}
type lifecycleMetadata struct {
	Provenance uint64
	EdgeLife   uint64               `json:",omitzero"`
	Definition *lifecycleDefinition `json:",omitempty"`
}

func lifecycleConfig(graph string) config {
	c := graphConfig(graph)
	c.Lifecycle = lifecycleProfile{Version: 1,
		Scope: lifecycleScope{Kind: "all", AxisProfile: "Q", AxisVersion: 1, CanonicalUnit: "millisecond", Reference: "posix-unix-epoch:1970-01-01T00:00:00Z:v1"},
		Edge:  9, EdgeLife: 4, NodeLives: [2]uint64{11, 22}, ReopenedLife: 12,
		Values: [2]lifecycleDefinition{{5, "i64", 5}, {6, "i64", 6}},
		Type:   "edge", Mode: "life-bound", Property: "p"}
	return c
}
func validLifecycleProfile(c config) bool {
	p := c.Lifecycle
	if p == (lifecycleProfile{}) {
		return true
	}
	if p.Version != 1 || p.Scope != lifecycleConfig("validation").Lifecycle.Scope || p.Type != "edge" || p.Mode != "life-bound" || p.Property != "p" || c.Nodes[0] == 0 || c.Nodes[1] == 0 {
		return false
	}
	ids := []uint64{c.Nodes[0], c.Nodes[1], p.Edge, p.EdgeLife, p.NodeLives[0], p.NodeLives[1], p.ReopenedLife, p.Values[0].ID, p.Values[1].ID}
	seen := map[uint64]bool{}
	for _, id := range ids {
		if id == 0 || id > 32 || seen[id] {
			return false
		}
		seen[id] = true
	}
	return p.Values[0].Kind == "i64" && p.Values[1].Kind == "i64" && p.Values[0].I64 != p.Values[1].I64
}
func (h *harness) validateLifecycle(x transaction) error {
	if h.config.Cost != (graphCostProfile{}) {
		return h.validateGraphCostLifecycle(x)
	}
	p := h.config.Lifecycle
	if p == (lifecycleProfile{}) {
		if x.Lifecycle != nil {
			return errInvalid
		}
		return nil
	}
	m := x.Lifecycle
	if m == nil || m.Provenance == 0 || x.GraphChange == nil {
		return errInvalid
	}
	g := *x.GraphChange
	switch g.Kind {
	case "node-create", "node-close":
		if g.Node.Owner > 1 || g.Node.Life != p.NodeLives[g.Node.Owner] && (g.Node.Owner != 0 || g.Node.Life != p.ReopenedLife) || m.EdgeLife != 0 || m.Definition != nil {
			return errInvalid
		}
	case "edge-create", "edge-update", "edge-close":
		if g.Edge.ID != p.Edge || m.EdgeLife != p.EdgeLife || g.Edge.Source.Life != p.NodeLives[0] || g.Edge.Target.Life != p.NodeLives[1] {
			return errInvalid
		}
		if g.Kind == "edge-close" {
			if m.Definition != nil {
				return errInvalid
			}
		} else if m.Definition == nil || m.Definition.Kind != "i64" || m.Definition.I64 != g.Edge.Value || *m.Definition != p.Values[0] && *m.Definition != p.Values[1] {
			return errInvalid
		}
	default:
		return errInvalid
	}
	return nil
}
