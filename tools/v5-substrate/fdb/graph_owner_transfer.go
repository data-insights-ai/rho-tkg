package main

// Fresh ownership/allocator admission for the transfer workload. This is
// deliberately separate from catalog1's small direct-API control grant.
import (
	"bytes"
	"context"
	"encoding/hex"
	"math"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const graphOwnerTransferNamespace = "graph-owned-v2-"
const graphOwnerTransportNamespace = "graph-owned-v3-"
const graphOwnerTransferFamily = "transfer-retained-pins"
const graphOwnerMaxBlock = uint64(1 << 20)

type graphOwnerTransferPolicy struct {
	Version                                       uint8
	Config                                        config
	Run                                           [16]byte
	BudgetSeconds                                 uint64
	AdmittedUnixNanos, DeadlineUnixNanos          int64
	MaxRecords, MaxControlBytes, MaxProposalBytes uint64
	MaxGrants, MaxBlock                           uint64
	MaxAttempts, MaxSubmissions, MaxCallbacks     uint64
	CleanupMillis, MaxCleanupCalls                uint64
	Admission                                     *graphTransportPolicy   `json:",omitempty"`
	Census                                        *graphOwnerCensusPolicy `json:",omitempty"`
}

func graphOwnerTransferLimit(p graphCostProfile) limits {
	originals := 3 + 2*p.Facts*graphCostParallelAttempts
	return limits{Bytes: (64 << 20) + p.Facts*(64<<10) + originals*2048, Transactions: originals, Versions: 3 + 6*p.Facts, Certificates: 1024}
}
func graphOwnerTransferConfig(spec graphFeedWorkloadSpec, run string) config {
	prefix := graphOwnerTransferNamespace
	if spec.Version == 2 {
		prefix = graphOwnerTransportNamespace
	}
	return graphCostConfig(prefix+spec.Layout+"-"+run, graphCostProfile{8, spec.Facts, spec.CrossPercent, spec.Writers})
}
func validGraphOwnerTransferConfig(c config) bool {
	p := c.Cost
	if p.Version != 8 || p.Facts < 20 || p.Facts > 1_000_000 || p.Facts%20 != 0 || p.Writers != 1 && p.Writers != 8 && p.Writers != 32 || p.Facts <= uint64(p.Writers) || p.CrossPercent != 0 && p.CrossPercent != 10 && p.CrossPercent != 50 && p.CrossPercent != 100 || c.Lifecycle != (lifecycleProfile{}) || c.CostScope != lifecycleConfig("validation").Lifecycle.Scope {
		return false
	}
	name, found := strings.CutPrefix(c.Graph, graphOwnerTransferNamespace)
	if !found {
		name, found = strings.CutPrefix(c.Graph, graphOwnerTransportNamespace)
	}
	if !found {
		return false
	}
	run, found := strings.CutPrefix(name, "kv-")
	if !found {
		run, found = strings.CutPrefix(name, "columns-")
	}
	if !found || !transportHex(run, 16) {
		return false
	}
	n := p.nodes()
	return c.Nodes == [2]uint64{n[0].Node, n[2].Node}
}
func (p graphOwnerTransferPolicy) validate() error {
	legacy := p.Version == 1 && p.Admission == nil && p.Census == nil && strings.HasPrefix(p.Config.Graph, graphOwnerTransferNamespace)
	fresh := p.Version == 2 && p.Admission != nil && p.Census != nil && p.Admission.validate() == nil && p.Admission.Spec.Family == graphOwnerTransferFamily && p.Census.validate() == nil && strings.HasPrefix(p.Config.Graph, graphOwnerTransportNamespace)
	if (!legacy && !fresh) || !validGraphOwnerTransferConfig(p.Config) || p.Run == [16]byte{} || !strings.HasSuffix(p.Config.Graph, hex.EncodeToString(p.Run[:])) || p.BudgetSeconds < 1 || p.BudgetSeconds > graphTransportMaxSeconds || p.AdmittedUnixNanos <= 0 || p.AdmittedUnixNanos > math.MaxInt64-int64(p.BudgetSeconds)*int64(time.Second) || p.DeadlineUnixNanos != p.AdmittedUnixNanos+int64(p.BudgetSeconds)*int64(time.Second) || p.DeadlineUnixNanos > math.MaxInt64-int64(30*time.Second) || p.CleanupMillis != 30_000 || p.MaxCleanupCalls != 8 || p.MaxAttempts != 64 || p.MaxSubmissions != 8 || p.MaxCallbacks != 16 || p.MaxProposalBytes < 2048 || p.MaxProposalBytes > 32<<10 || p.MaxGrants < 4 || p.MaxGrants > 64 || p.MaxBlock < 1 || p.MaxBlock > graphOwnerMaxBlock {
		return errInvalid
	}
	if p.Admission != nil {
		a := p.Admission
		if a.Run != p.Run || a.BudgetSeconds != p.BudgetSeconds || a.AdmittedUnixNanos != p.AdmittedUnixNanos || a.DeadlineUnixNanos != p.DeadlineUnixNanos || graphTransportConfig(a.Spec, hex.EncodeToString(a.Run[:])) != p.Config {
			return errMismatch
		}
		// Explicit grant for the declared complete useful schedule: source
		// history/current/CDC, both original+intent pairs and first-use IDs.
		// ABORT/control/column/page headroom is additional caller-owned admission.
		if p.Census.MaxRows < 33+21*p.Config.Cost.Facts {
			return errInvalid
		}
	}
	// Enough records for the useful schedule, with the actual chosen extra
	// attempt headroom explicit. Limits may still refuse a genuinely exhausted
	// cell; this is never a promise that every 64-attempt worst case fits.
	minimum := 2*(3+2*p.Config.Cost.Facts) + 2*p.Config.Cost.Facts + 4*p.MaxGrants + 64
	grants := (2*p.Config.Cost.Facts+6+p.MaxBlock-1)/p.MaxBlock + (2+p.MaxBlock-1)/p.MaxBlock + 1
	if grants > p.MaxGrants || p.MaxRecords < minimum || p.MaxRecords > 1<<32 || p.MaxControlBytes < 512<<10 || p.MaxControlBytes > 1<<50 || p.MaxRecords > p.MaxControlBytes/128 {
		return errInvalid
	}
	return nil
}

func (c *graphOwnerCatalog) transportIn(tr fdb.ReadTransaction) error {
	if p := c.binding.Transfer; p != nil && p.Admission != nil {
		body, err := wire(p.Admission)
		if err != nil {
			return err
		}
		raw, err := tr.Get(c.h.key(0, "workload-transport-v2", "binding")).Get()
		if err != nil {
			return err
		}
		if !bytes.Equal(raw, body) {
			return errMismatch
		}
	}
	return nil
}

// A ticket is reserved with the immutable move/grant BEFORE acquisition. Its
// full-width record and eight three-RPC invocations (admit/effect/observation,
// each <=16 callbacks) are separate from ordinary admission. Failed pre-admit
// RPC/decode work is observed work, not claimed durable spending. Once native
// admission commits, even an effect error/unknown result never refunds it.
type graphOwnerCleanup struct {
	Request           string
	DeadlineUnixNanos int64
	Calls             uint64
}

func (c *graphOwnerCatalog) reserveCleanupIn(tr fdb.Transaction, request string) error {
	if c.binding.Transfer == nil {
		return nil
	}
	if !validName(request) {
		return errInvalid
	}
	return c.createIn(tr, c.key("cleanup", request), graphOwnerCleanup{Request: request})
}

func (p graphOwnerTransferPolicy) cleanupDebit(t graphOwnerCleanup, now int64) (graphOwnerCleanup, error) {
	if p.validate() != nil || !validName(t.Request) || t.Calls > p.MaxCleanupCalls || t.DeadlineUnixNanos < 0 || (t.Calls == 0) != (t.DeadlineUnixNanos == 0) {
		return t, errMismatch
	}
	ceiling := p.DeadlineUnixNanos + int64(p.CleanupMillis)*int64(time.Millisecond)
	if now < p.AdmittedUnixNanos || now >= ceiling || t.DeadlineUnixNanos > ceiling {
		return t, context.DeadlineExceeded
	}
	if t.Calls == p.MaxCleanupCalls {
		return t, errLimit
	}
	if t.DeadlineUnixNanos == 0 {
		t.DeadlineUnixNanos = now + min(ceiling-now, int64(p.CleanupMillis)*int64(time.Millisecond))
	}
	if now >= t.DeadlineUnixNanos {
		return t, context.DeadlineExceeded
	}
	t.Calls++
	return t, nil
}

// SAME original cleanup alone may outlive normal run cancellation. Replacement
// cannot renew its absolute ceiling or first-close deadline, create a ticket,
// acquire an owner, install a grant or issue a cursor through this door.
func (c *graphOwnerCatalog) terminalContext(ctx context.Context, request string, authenticate func(fdb.ReadTransaction) error) (context.Context, context.CancelFunc, error) {
	if c == nil || ctx == nil || !validName(request) || authenticate == nil {
		return nil, nil, errInvalid
	}
	if c.binding.Transfer == nil {
		return c.operationContext(ctx)
	}
	p := *c.binding.Transfer
	if p.validate() != nil {
		return nil, nil, errMismatch
	}
	now := time.Now().UnixNano()
	ceiling := p.DeadlineUnixNanos + int64(p.CleanupMillis)*int64(time.Millisecond)
	if now < p.AdmittedUnixNanos || now >= ceiling {
		return nil, nil, context.DeadlineExceeded
	}
	bounded, cancel := context.WithDeadline(context.WithoutCancel(ctx), time.Unix(0, now+min(ceiling-now, int64(p.CleanupMillis)*int64(time.Millisecond))))
	ticket, _, err := historyTransactContext(bounded, c.h.db, func() error { return nil }, func(tr fdb.Transaction) (graphOwnerCleanup, error) {
		actual, err := c.h.ownerCatalogIn(tr)
		if err != nil {
			return graphOwnerCleanup{}, err
		}
		if !graphColumnSame(actual.binding, c.binding) {
			return graphOwnerCleanup{}, errMismatch
		}
		if err := authenticate(tr); err != nil {
			return graphOwnerCleanup{}, err
		}
		t, found, err := get[graphOwnerCleanup](tr, c.key("cleanup", request))
		if err != nil {
			return t, err
		}
		if !found || t.Request != request {
			return t, errUnavailable
		}
		t, err = p.cleanupDebit(t, time.Now().UnixNano())
		if err != nil {
			return t, err
		}
		return t, put(tr, c.key("cleanup", request), t)
	})
	if err != nil {
		cancel()
		return nil, nil, err
	}
	terminal, terminalCancel := context.WithDeadline(bounded, time.Unix(0, ticket.DeadlineUnixNanos))
	return terminal, func() { terminalCancel(); cancel() }, nil
}

func (c *graphOwnerCatalog) operationContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if c == nil || ctx == nil {
		return nil, nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if c.binding.Transfer == nil {
		return ctx, func() {}, nil
	}
	p := c.binding.Transfer
	if p.validate() != nil {
		return nil, nil, errMismatch
	}
	if err := c.checkTime(); err != nil {
		return nil, nil, err
	}
	bounded, cancel := context.WithDeadline(ctx, time.Unix(0, p.DeadlineUnixNanos))
	return bounded, cancel, nil
}
func (c *graphOwnerCatalog) checkTime() error {
	if p := c.binding.Transfer; p != nil {
		if now := time.Now().UnixNano(); now < p.AdmittedUnixNanos || now >= p.DeadlineUnixNanos {
			return context.DeadlineExceeded
		}
	}
	return nil
}

// Retired controls remain charged: no control/highwater reset or capacity claim.
// Immutable intents use actual encoded bytes under catalog2; mutable terminal
// records retain full-width prepaid space so ordinary traffic cannot steal it.
func (c *graphOwnerCatalog) recordCharge(key, body []byte, v any) uint64 {
	if c.binding.Version == 2 {
		switch v.(type) {
		case graphOwnerIntent, graphOwnerAllocationGrant, graphOwnerIssuedID:
			return uint64(len(key) + len(body))
		}
	}
	return uint64(len(key) + graphOwnerRecordBytes)
}
