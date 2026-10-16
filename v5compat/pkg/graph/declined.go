package graph

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/admin"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/constraints"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/events"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/index"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/ingest"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/replication"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/stats"
)

// declined builds the ErrNotSupportedInV5 error of one door.
func declined(door string) error { return fmt.Errorf("%w: %s", ErrNotSupportedInV5, door) }

// The sub-APIs below keep the v4 accessors and method sets (their packages are in
// pkg/graph/<name>) so a consumer compiles; each method reports
// ErrNotSupportedInV5 naming the door (the X list, or not in the first batch).

// Stats returns the statistics sub-API.
func (g *Graph) Stats() *stats.API { return &stats.API{} }

// Index returns the index DDL and vector sub-API.
func (g *Graph) Index() *index.API { return &index.API{} }

// Constraints returns the constraint sub-API.
func (g *Graph) Constraints() *constraints.API { return &constraints.API{} }

// Admin returns the admin sub-API (DecomposeNodeID, IDSlotLease, tier admin).
func (g *Graph) Admin() *admin.API { return &admin.API{} }

// Replication returns the change-feed and LSN-follower sub-API.
func (g *Graph) Replication() *replication.API { return &replication.API{} }

// IO returns the v4 export/import sub-API.
func (g *Graph) IO() *io.API { return &io.API{} }

// Events returns the event-bus sub-API.
func (g *Graph) Events() *events.API { return &events.API{} }

// Ingest returns the ingest-session sub-API.
func (g *Graph) Ingest() *ingest.API { return &ingest.API{} }
