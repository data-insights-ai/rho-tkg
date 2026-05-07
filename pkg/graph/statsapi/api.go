// Package statsapi is a sub-API accessor for graph counter / count-by-label
// statistics. The full GraphStats struct (atomic operation counters + cache
// metrics) is reachable via Graph.Stats; statsapi exposes the count helpers
// that don't pull pkg/graph types into the sub-API import.
package statsapi

// Core is the subset of *graph.Graph stat methods the statsapi sub-API forwards to.
type Core interface {
	NodeCount() (int, error)
	RelationshipCount() (int, error)
	NodeCountByLabel(label string) (int, error)
	RelCountByType(typeName string) (int, error)
	AllLabelCounts() (map[string]int, error)
	AllRelTypeCounts() (map[string]int, error)
}

// API is the stats sub-API accessor.
type API struct{ c Core }

// New constructs a stats sub-API.
func New(c Core) *API { return &API{c: c} }

// NodeCount returns the total node count. Forwards to Graph.NodeCount.
func (a *API) NodeCount() (int, error) { return a.c.NodeCount() }

// RelCount returns the total relationship count. Forwards to Graph.RelationshipCount.
func (a *API) RelCount() (int, error) { return a.c.RelationshipCount() }

// NodeCountByLabel returns the count of nodes carrying the label. Forwards to Graph.NodeCountByLabel.
func (a *API) NodeCountByLabel(label string) (int, error) { return a.c.NodeCountByLabel(label) }

// RelCountByType returns the count of relationships of the given type. Forwards to Graph.RelCountByType.
func (a *API) RelCountByType(typeName string) (int, error) { return a.c.RelCountByType(typeName) }

// AllLabelCounts returns counts per label. Forwards to Graph.AllLabelCounts.
func (a *API) AllLabelCounts() (map[string]int, error) { return a.c.AllLabelCounts() }

// AllRelTypeCounts returns counts per relationship type. Forwards to Graph.AllRelTypeCounts.
func (a *API) AllRelTypeCounts() (map[string]int, error) { return a.c.AllRelTypeCounts() }
