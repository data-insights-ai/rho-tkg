package main

// A bounded native handoff census, not a prefix-copy protocol. Source keys stay
// put. The full two-prefix row stream and every mutable control head belong to
// one genuine frozen move; activation seals them in its OWN native transaction.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type graphOwnerCensusPolicy struct {
	// MaxWork covers admitted page RPC callback allowance. Prepare, refused
	// pre-admission RPCs and final observation remain separately observed bounded
	// native IO; these counters are not total process/storage/RSS accounting.
	PageRows, PageBytes, HeadRows, HeadBytes               int
	MaxRows, MaxBytes, MaxCalls, MaxWorkRows, MaxWorkBytes uint64
}

func (p graphOwnerCensusPolicy) validate() error {
	if p.PageRows < 1 || p.PageRows > 128 || p.PageBytes < 64<<10 || p.PageBytes > 4<<20 || p.HeadRows < 8 || p.HeadRows > 512 || p.HeadBytes < 4096 || p.HeadBytes > 48<<10 || p.MaxRows < 1 || p.MaxRows > 1<<40 || p.MaxBytes < 1<<20 || p.MaxBytes > 1<<55 || p.MaxCalls < 8 || p.MaxCalls > 1<<32 || p.MaxWorkRows < p.MaxRows || p.MaxWorkRows > 1<<48 || p.MaxWorkBytes < p.MaxBytes || p.MaxWorkBytes > 1<<62 {
		return errInvalid
	}
	return nil
}

type graphOwnerCensusOriginal struct {
	Request string
	Move    graphOwnerMove
	Frozen  graphOwnerMoveProgress
	Binding [32]byte
	Heads   []graphCostStoredRecord
}
type graphOwnerCensusPosition struct {
	Group  uint8
	Cursor []byte
}
type graphOwnerCensusPage struct {
	Request     string
	Ordinal     uint64
	From, Next  graphOwnerCensusPosition
	Rows, Bytes uint64
	Hash        [32]byte
	EOF         bool
}
type graphOwnerCensusProgress struct {
	Original                                [32]byte
	Position                                graphOwnerCensusPosition
	Rows, Bytes, Calls, WorkRows, WorkBytes uint64
	Pages                                   uint64
	Hash                                    [32]byte
	Pending                                 string
	Invocations                             uint8
	Last                                    graphOwnerCensusPage
	EOF                                     bool
}
type graphOwnerCensusSeal struct {
	Original      [32]byte
	Before, After graphOwnerState
	Cut           certificate
	Rows, Bytes   uint64
	Hash, Heads   [32]byte
	Complete      bool
}
type graphOwnerCensusDelivery struct {
	Page   graphOwnerCensusPage
	Rows   []graphCostStoredRecord
	Replay bool
}
type graphOwnerCensusObservation struct {
	Original          graphOwnerCensusOriginal
	Progress          graphOwnerCensusProgress
	Seal              graphOwnerCensusSeal
	Controls          [3]graphCostStoredRecord
	AllRows, AllBytes uint64
}

func censusProgressValid(p graphOwnerCensusProgress, limit graphOwnerCensusPolicy) bool {
	if p.Position.Group > 2 || len(p.Position.Cursor) > 1024 || p.Position.Group == 2 && len(p.Position.Cursor) != 0 || p.Rows > limit.MaxRows || p.Bytes > limit.MaxBytes || p.Calls > limit.MaxCalls || p.Pages > p.Calls || p.Last.Ordinal != p.Pages || p.WorkRows > limit.MaxWorkRows || p.WorkBytes > limit.MaxWorkBytes || p.Invocations > 8 || p.EOF != (p.Position.Group == 2) {
		return false
	}
	if p.Pending != "" && (p.Pending != fmt.Sprintf("page-%016x", p.Pages+1) || p.EOF) {
		return false
	}
	if p.Pages == 0 {
		return p.Rows == 0 && p.Bytes == 0 && p.Hash == [32]byte{} && p.Position.Group == 0 && len(p.Position.Cursor) == 0 && graphColumnSame(p.Last, graphOwnerCensusPage{})
	}
	return p.Last.Request == fmt.Sprintf("page-%016x", p.Pages) && p.Last.Rows <= uint64(limit.PageRows) && p.Last.Bytes <= uint64(limit.PageBytes) && p.Last.Rows <= p.Rows && p.Last.Bytes <= p.Bytes && p.Last.EOF == p.EOF && censusPositionEqual(p.Last.Next, p.Position) && p.Last.From.Group <= p.Last.Next.Group && p.Last.From.Group < 2 && len(p.Last.From.Cursor) <= 1024
}

func (c *graphOwnerCatalog) censusKey(request, kind string) fdb.Key {
	return c.h.key(0, "owner-census-v1", request+"/"+kind)
}
func (c *graphOwnerCatalog) censusOwn(request string, key []byte) bool {
	for _, kind := range []string{"original", "progress", "seal"} {
		if bytes.Equal(key, c.censusKey(request, kind)) {
			return true
		}
	}
	return false
}

// All current mutable classes are enumerated here. Immutable large classes are
// streamed separately; unknown classes are NOT silently treated as immutable.
// HeadRows/HeadBytes bound the complete native inventory, independent of facts.
// Column generation prefix jumps skip chunk bodies without a history-sized read.
func (c *graphOwnerCatalog) censusHeadsIn(tr fdb.ReadTransaction) ([]graphCostStoredRecord, error) {
	p := c.binding.Transfer.Census
	var result []graphCostStoredRecord
	add := func(key fdb.Key, raw []byte) error {
		if len(key) > 1024 || len(raw) == 0 || len(raw) > 64<<10 || len(result) >= p.HeadRows {
			return errLimit
		}
		if len(key)+len(raw) > p.HeadBytes {
			return errLimit
		}
		candidate := append(slices.Clone(result), graphCostStoredRecord{Key: key, Value: raw})
		body, err := wire(candidate)
		if err != nil || len(body) > p.HeadBytes {
			return errors.Join(errLimit, err)
		}
		result = append(result, graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)})
		return nil
	}
	point := func(key fdb.Key, required bool) error {
		raw, err := tr.Get(key).Get()
		if err != nil {
			return err
		}
		if raw == nil {
			if required {
				return errUnavailable
			}
			return nil
		}
		return add(key, raw)
	}
	rangeHead := func(prefix string) error {
		rows, err := tr.GetRange(prefixRange(prefix), fdb.RangeOptions{Limit: p.HeadRows - len(result) + 1}).GetSliceWithError()
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := add(row.Key, row.Value); err != nil {
				return err
			}
		}
		return nil
	}
	present := func(prefix string) (bool, error) {
		key, err := tr.GetKey(fdb.FirstGreaterOrEqual(fdb.Key(prefix))).Get()
		if err != nil {
			return false, err
		}
		return bytes.HasPrefix(key, []byte(prefix)), nil
	}
	for g := uint8(0); g < 2; g++ {
		for _, name := range []string{"config", "state"} {
			if err := point(c.h.key(g, "meta", name), true); err != nil {
				return nil, err
			}
		}
	}
	for _, kind := range []string{"binding", "state", "ledger", "allocation-state"} {
		name := "current"
		if kind == "binding" {
			name = "immutable"
		}
		if kind == "ledger" {
			name = "controls"
		}
		if err := point(c.key(kind, name), true); err != nil {
			return nil, err
		}
	}
	for _, kind := range []string{"move-progress", "allocation-slot", "cleanup"} {
		if err := rangeHead(string(c.key(kind, ""))); err != nil {
			return nil, err
		}
	}
	for _, name := range []string{"binding", "ledger", "state"} {
		if err := point(c.h.historyKey(name), true); err != nil {
			return nil, err
		}
	}
	if err := rangeHead((&graphHistoryRetention{h: c.h}).ownerPrefix()); err != nil {
		return nil, err
	}
	if err := point(c.h.key(0, "workload-transport-v2", "binding"), true); err != nil {
		return nil, err
	}
	feed, err := present(c.h.prefix(0) + "feed-v1/")
	if err != nil {
		return nil, err
	}
	if feed {
		key := c.h.key(0, "feed-v1/meta", "binding")
		binding, found, err := get[graphFeedBinding](tr, key)
		if err != nil {
			return nil, err
		}
		if !found || binding.Version != 3 || binding.Config != c.h.config || !graphColumnSame(binding.Source, c.binding.Source) || binding.Retention == nil || !graphColumnSame(*binding.Retention, c.binding) || binding.Admission != nil || binding.Policy.validate() != nil || binding.Selector != "full-scope;fixed-scalar-cross-edge;unfiltered-complete-before-after:v1" || binding.Order != "Round,Coordinator,Request;independent-coordinator-sequences:v1" {
			return nil, errMismatch
		}
		ledger, found, err := get[graphFeedLedger](tr, c.h.key(0, "feed-v1/meta", "ledger"))
		if err != nil {
			return nil, err
		}
		if !found || ledger.Bytes > binding.Policy.MaxControlBytes || ledger.Records > 4*binding.Policy.MaxPages+128 || ledger.CleanupTickets > 64 {
			return nil, errMismatch
		}
		for _, name := range []string{"binding", "ledger"} {
			if err := point(c.h.key(0, "feed-v1/meta", name), true); err != nil {
				return nil, err
			}
		}
	}
	for _, kind := range []string{"cursor", "cleanup", "retained-reader", "retained-phase"} {
		prefix := c.h.prefix(0) + "feed-v1/" + kind + "/"
		if err := rangeHead(prefix); err != nil {
			return nil, err
		}
	}
	columns := c.h.prefix(0) + "columns-v1/"
	hasColumns, err := present(columns)
	if err != nil {
		return nil, err
	}
	if hasColumns {
		binding, found, err := get[graphColumnBinding](tr, fdb.Key(columns+"binding"))
		if err != nil {
			return nil, err
		}
		if !found || binding.Version != 1 || binding.Config != c.h.config || binding.Layout != graphColumnLayout || binding.Schema != "node-presence;edge-life-bound;property-p:i64;canonical-original:v1" || binding.Scope != c.h.config.CostScope || !graphColumnProfile(c.h, binding.Policy) {
			return nil, errMismatch
		}
		ledger, found, err := get[graphColumnLedger](tr, fdb.Key(columns+"ledger"))
		if err != nil {
			return nil, err
		}
		if !found || ledger.Bytes > binding.Policy.MaxRetainedBytes || ledger.Generations > binding.Policy.MaxGenerations || ledger.Chunks > binding.Policy.MaxChunks {
			return nil, errMismatch
		}
		for _, name := range []string{"binding", "ledger"} {
			if err := point(fdb.Key(columns+name), true); err != nil {
				return nil, err
			}
		}
	}
	hasRetention, err := present(columns + "retention-v1/")
	if err != nil {
		return nil, err
	}
	if hasRetention {
		binding, found, err := get[graphColumnPinBinding](tr, fdb.Key(columns+"retention-v1/binding"))
		if err != nil {
			return nil, err
		}
		columnBody, err := tr.Get(fdb.Key(columns + "binding")).Get()
		if err != nil {
			return nil, err
		}
		if !found || binding.Version != 1 || binding.Columns != sha256.Sum256(columnBody) || binding.Policy.validate() != nil {
			return nil, errMismatch
		}
		ledger, found, err := get[graphColumnPinLedger](tr, fdb.Key(columns+"retention-v1/ledger"))
		if err != nil {
			return nil, err
		}
		if !found || ledger.Records > binding.Policy.MaxRecords || ledger.Bytes > binding.Policy.MaxBytes || ledger.ReservedRecords > binding.Policy.MaxRecords-ledger.Records || ledger.ReservedBytes > binding.Policy.MaxBytes-ledger.Bytes {
			return nil, errMismatch
		}
		for _, name := range []string{"binding", "ledger"} {
			if err := point(fdb.Key(columns+"retention-v1/"+name), true); err != nil {
				return nil, err
			}
		}
	}
	for _, kind := range []string{"read-owner/", "retention-v1/pin/", "retention-v1/reference/"} {
		if err := rangeHead(columns + kind); err != nil {
			return nil, err
		}
	}
	begin := []byte(columns + "generation/")
	end := prefixRange(string(begin)).End
	for {
		key, err := tr.GetKey(fdb.FirstGreaterOrEqual(fdb.Key(begin))).Get()
		if err != nil {
			return nil, err
		}
		if len(key) == 0 || bytes.Compare(key, end) >= 0 {
			break
		}
		if len(key) > 1024 {
			return nil, errLimit
		}
		suffix, ok := strings.CutPrefix(string(key), columns+"generation/")
		if !ok {
			return nil, errMismatch
		}
		id, _, ok := strings.Cut(suffix, "/")
		raw, err := hex.DecodeString(id)
		if !ok || err != nil || !validName(string(raw)) || hex.EncodeToString(raw) != id {
			return nil, errMismatch
		}
		prefix := columns + "generation/" + id + "/"
		for _, name := range []string{"progress", "owners"} {
			if err := point(fdb.Key(prefix+name), name == "progress"); err != nil {
				return nil, err
			}
		}
		// A malformed generation with neither native head is not a skippable chunk.
		if len(result) == 0 || !bytes.HasPrefix(result[len(result)-1].Key, []byte(prefix)) {
			return nil, errUnavailable
		}
		begin = bytes.Clone(prefixRange(prefix).End)
	}
	slices.SortFunc(result, func(a, b graphCostStoredRecord) int { return bytes.Compare(a.Key, b.Key) })
	for i := 1; i < len(result); i++ {
		if bytes.Equal(result[i-1].Key, result[i].Key) {
			return nil, errMismatch
		}
	}
	return result, nil
}

func (c *graphOwnerCatalog) censusClass(key []byte) (bool, error) {
	g := uint8(0)
	suffix, ok := strings.CutPrefix(string(key), c.h.prefix(0))
	if !ok {
		g = 1
		suffix, ok = strings.CutPrefix(string(key), c.h.prefix(1))
	}
	if !ok {
		return false, errMismatch
	}
	kind, name, ok := strings.Cut(suffix, "/")
	if !ok || name == "" {
		return false, errUnavailable
	}
	switch kind {
	case "current", "history", "changes", "id", "request", "cut":
		return true, nil
	case "meta":
		raw, err := hex.DecodeString(name)
		if err != nil {
			return false, errMismatch
		}
		if string(raw) == "config" || string(raw) == "state" {
			return true, nil
		}
		return false, errUnavailable
	case "source-owner-v1":
		if g != 0 {
			return false, errMismatch
		}
		return true, nil
	case "source-retention-v1", "workload-transport-v2", "owner-census-v1":
		if g != 0 {
			return false, errMismatch
		}
		raw, err := hex.DecodeString(name)
		if err != nil {
			return false, errMismatch
		}
		s := string(raw)
		if kind == "source-retention-v1" && (s == "binding" || s == "ledger" || s == "state" || strings.HasPrefix(s, "intent/") || strings.HasPrefix(s, "seal/")) {
			return true, nil
		}
		if kind == "workload-transport-v2" && s == "binding" {
			return true, nil
		}
		if kind == "owner-census-v1" {
			i := strings.LastIndexByte(s, '/')
			if i > 0 && (s[i+1:] == "original" || s[i+1:] == "progress" || s[i+1:] == "seal") {
				return true, nil
			}
		}
		return false, errUnavailable
	case "owner-catalog-v1":
		if g != 0 {
			return false, errMismatch
		}
		decoded, err := hex.DecodeString(name)
		if err != nil {
			return false, errMismatch
		}
		which, tail, ok := strings.Cut(string(decoded), "/")
		if !ok {
			return false, errUnavailable
		}
		if which == "binding" && tail == "immutable" || which == "state" && tail == "current" || which == "ledger" && tail == "controls" || which == "allocation-state" && tail == "current" {
			return true, nil
		}
		switch which {
		case "move", "move-progress", "cleanup", "intent-request", "intent-id", "allocation-grant", "allocation-index", "allocation-slot", "allocation-issued":
			return true, nil
		}
		return false, errUnavailable
	case "feed-v1":
		if g != 0 {
			return false, errMismatch
		}
		which, tail, ok := strings.Cut(name, "/")
		if !ok {
			return false, errUnavailable
		}
		if which == "meta" {
			raw, err := hex.DecodeString(tail)
			if err != nil {
				return false, errMismatch
			}
			if string(raw) == "binding" || string(raw) == "ledger" {
				return true, nil
			}
			return false, errUnavailable
		}
		switch which {
		case "original", "cursor", "reader", "cleanup", "retained-phase", "retained-reader", "action", "page", "ack":
			return true, nil
		}
		return false, errUnavailable
	case "columns-v1":
		if g != 0 {
			return false, errMismatch
		}
		which, rest, ok := strings.Cut(name, "/")
		if !ok {
			if name == "binding" || name == "ledger" {
				return true, nil
			}
			return false, errUnavailable
		}
		switch which {
		case "read-owner":
			if raw, err := hex.DecodeString(rest); err == nil && validName(string(raw)) && hex.EncodeToString(raw) == rest {
				return true, nil
			}
		case "retention-v1":
			k, id, nested := strings.Cut(rest, "/")
			if !nested && (k == "binding" || k == "ledger") {
				return true, nil
			}
			switch k {
			case "pin", "reference", "reference-request":
				if raw, err := hex.DecodeString(id); nested && err == nil && validName(string(raw)) && hex.EncodeToString(raw) == id {
					return true, nil
				}
			}
		case "generation":
			id, tail, ok := strings.Cut(rest, "/")
			if !ok {
				return false, errUnavailable
			}
			raw, err := hex.DecodeString(id)
			if err != nil || !validName(string(raw)) || hex.EncodeToString(raw) != id {
				return false, errMismatch
			}
			if tail == "progress" || tail == "owners" || tail == "manifest" {
				return true, nil
			}
			if ordinal, ok := strings.CutPrefix(tail, "chunk/"); ok && len(ordinal) == 16 && transportHex(ordinal, 8) {
				return true, nil
			}
		}
		return false, errUnavailable
	}
	return false, errUnavailable
}

func (c *graphOwnerCatalog) censusOriginalIn(tr fdb.ReadTransaction, move graphOwnerMove, request string) (graphOwnerCensusOriginal, graphOwnerCensusProgress, error) {
	var o graphOwnerCensusOriginal
	var progress graphOwnerCensusProgress
	actual, err := c.h.ownerCatalogIn(tr)
	if err != nil {
		return o, progress, err
	}
	if actual == nil || actual.binding.Transfer == nil || actual.binding.Transfer.Version != 2 || !graphColumnSame(actual.binding, c.binding) {
		return o, progress, errMismatch
	}
	frozen, err := c.moveIn(tr, move)
	if err != nil {
		return o, progress, err
	}
	if frozen.Phase != 1 || frozen.Census != request {
		return o, progress, errStale
	}
	if _, err := c.heldIn(tr, move, frozen); err != nil {
		return o, progress, err
	}
	state, err := c.stateIn(tr)
	if err != nil {
		return o, progress, err
	}
	if state.Moving != move.Request || state.Version != move.Before.Version || state.Routes != move.Before.Routes {
		return o, progress, errStale
	}
	stored, found, err := get[graphOwnerCensusOriginal](tr, c.censusKey(request, "original"))
	if err != nil {
		return o, progress, err
	}
	if !found {
		return o, progress, errUnavailable
	}
	o = stored
	binding, err := wire(c.binding)
	if err != nil {
		return o, progress, err
	}
	if o.Request != request || !graphColumnSame(o.Move, move) || !graphColumnSame(o.Frozen, frozen) || o.Binding != sha256.Sum256(binding) {
		return o, progress, errMismatch
	}
	body, err := wire(o)
	if err != nil {
		return o, progress, err
	}
	progress, found, err = get[graphOwnerCensusProgress](tr, c.censusKey(request, "progress"))
	if err != nil {
		return o, progress, err
	}
	if !found || progress.Original != sha256.Sum256(body) {
		return o, progress, errUnavailable
	}
	if !censusProgressValid(progress, *c.binding.Transfer.Census) {
		return o, progress, errMismatch
	}
	heads, err := c.censusHeadsIn(tr)
	if err != nil {
		return o, progress, err
	}
	if !graphColumnSame(heads, o.Heads) {
		return o, progress, errOwnerBusy
	}
	return o, progress, nil
}

// PrepareCensus reserves the complete immutable original and two full-width
// mutable slots before any page. Head/counter bounds are encoded admission,
// not a client RSS or matrix capacity claim. No data-sized transaction/map.
func (c *graphOwnerCatalog) PrepareCensus(ctx context.Context, a graphOwnerAuthority, move graphOwnerMove, request string) (graphOwnerCensusOriginal, error) {
	var zero graphOwnerCensusOriginal
	if c == nil || c.binding.Transfer == nil || c.binding.Transfer.Version != 2 || !validName(request) {
		return zero, errInvalid
	}
	owned, err := graphOwnerMoveInput(move)
	if err != nil {
		return zero, err
	}
	move = owned
	ctx, cancel, err := c.operationContext(ctx)
	if err != nil {
		return zero, err
	}
	defer cancel()
	_, _, err = historyTransactContext(ctx, c.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		actual, err := c.h.ownerCatalogIn(tr)
		if err != nil {
			return false, err
		}
		if err := actual.authenticate(a); err != nil {
			return false, err
		}
		if a.ID != move.From {
			return false, errStale
		}
		frozen, err := c.moveIn(tr, move)
		if err != nil {
			return false, err
		}
		if frozen.Phase != 1 {
			return false, errStale
		}
		if frozen.Census != "" {
			if frozen.Census != request {
				return false, errOwnerBusy
			}
			_, _, err := c.censusOriginalIn(tr, move, request)
			return true, err
		}
		if _, err := c.heldIn(tr, move, frozen); err != nil {
			return false, err
		}
		state, err := c.stateIn(tr)
		if err != nil {
			return false, err
		}
		if state.Moving != move.Request || state.Routes != move.Before.Routes || state.Version != move.Before.Version {
			return false, errStale
		}
		if _, err := c.allocationStateIn(tr); err != nil {
			return false, err
		}
		frozen.Census = request
		if err := put(tr, c.key("move-progress", move.Request), frozen); err != nil {
			return false, err
		}
		// These writes are private to this same transaction: no placeholder is
		// observable. Reserve their maximum widths BEFORE capturing ledger heads.
		for _, kind := range []string{"original", "progress", "seal"} {
			if raw, err := tr.Get(c.censusKey(request, kind)).Get(); err != nil {
				return false, err
			} else if raw != nil {
				return false, errMismatch
			}
			if err := c.createIn(tr, c.censusKey(request, kind), struct{}{}); err != nil {
				return false, err
			}
		}
		heads, err := c.censusHeadsIn(tr)
		if err != nil {
			return false, err
		}
		binding, err := wire(c.binding)
		if err != nil {
			return false, err
		}
		o := graphOwnerCensusOriginal{request, move, frozen, sha256.Sum256(binding), heads}
		body, err := wire(o)
		if err != nil {
			return false, err
		}
		tr.Set(c.censusKey(request, "original"), body)
		p := graphOwnerCensusProgress{Original: sha256.Sum256(body)}
		if err := put(tr, c.censusKey(request, "progress"), p); err != nil {
			return false, err
		}
		return true, put(tr, c.censusKey(request, "seal"), graphOwnerCensusSeal{Original: p.Original})
	})
	if err != nil {
		return zero, err
	}
	o, _, err := graphCostParallelRead(ctx, c.h.db, func(tr fdb.ReadTransaction) (graphOwnerCensusOriginal, error) {
		if err := c.authenticate(a); err != nil {
			return zero, err
		}
		o, _, err := c.censusOriginalIn(tr, move, request)
		return o, err
	})
	return o, err
}

func censusPositionEqual(a, b graphOwnerCensusPosition) bool {
	return a.Group == b.Group && bytes.Equal(a.Cursor, b.Cursor)
}
func (c *graphOwnerCatalog) censusRowsIn(tr fdb.ReadTransaction, request string, from graphOwnerCensusPosition) ([]graphCostStoredRecord, graphOwnerCensusPosition, error) {
	p := c.binding.Transfer.Census
	pos := graphOwnerCensusPosition{from.Group, bytes.Clone(from.Cursor)}
	var out []graphCostStoredRecord
	used := 0
	for pos.Group < 2 && len(out) < p.PageRows {
		prefix := c.h.prefix(pos.Group)
		begin := []byte(prefix)
		if len(pos.Cursor) > 0 {
			if !bytes.HasPrefix(pos.Cursor, begin) {
				return nil, pos, errMismatch
			}
			begin = append(bytes.Clone(pos.Cursor), 0)
		}
		rows, err := tr.GetRange(fdb.KeyRange{Begin: fdb.Key(begin), End: prefixRange(prefix).End}, fdb.RangeOptions{Limit: p.PageRows - len(out) + 3}).GetSliceWithError()
		if err != nil {
			return nil, pos, err
		}
		if len(rows) == 0 {
			pos.Group++
			pos.Cursor = nil
			continue
		}
		for _, row := range rows {
			if len(row.Key) > 1024 || len(row.Value) == 0 || len(row.Value) > 64<<10 {
				return nil, pos, errLimit
			}
			if ok, err := c.censusClass(row.Key); err != nil || !ok {
				return nil, pos, errors.Join(errUnavailable, err)
			}
			if c.censusOwn(request, row.Key) {
				pos.Cursor = bytes.Clone(row.Key)
				continue
			}
			if len(out) == p.PageRows || len(row.Key)+len(row.Value) > p.PageBytes-used {
				if len(out) == 0 {
					return nil, pos, errLimit
				}
				return out, pos, nil
			}
			used += len(row.Key) + len(row.Value)
			out = append(out, graphCostStoredRecord{bytes.Clone(row.Key), bytes.Clone(row.Value)})
			pos.Cursor = bytes.Clone(row.Key)
		}
	}
	return out, pos, nil
}

// Page admission is committed separately, so a source-side error cannot refund
// spent native callback/work credit. Only the last immutable page descriptor is
// retained: SAME recovery rereads its exact raw range under the frozen heads.
// Callers must persist an emitted page before requesting the next one; there is
// no resume promise after losing older caller-owned output.
func (c *graphOwnerCatalog) CensusPage(ctx context.Context, a graphOwnerAuthority, move graphOwnerMove, original string, ordinal uint64) (graphOwnerCensusDelivery, error) {
	var zero graphOwnerCensusDelivery
	if c == nil || c.binding.Transfer == nil || c.binding.Transfer.Version != 2 || !validName(original) || ordinal == 0 {
		return zero, errInvalid
	}
	request := fmt.Sprintf("page-%016x", ordinal)
	owned, err := graphOwnerMoveInput(move)
	if err != nil {
		return zero, err
	}
	move = owned
	ctx, cancel, err := c.operationContext(ctx)
	if err != nil {
		return zero, err
	}
	defer cancel()
	_, _, err = historyTransactContext(ctx, c.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		if err := c.authenticate(a); err != nil {
			return false, err
		}
		if a.ID != move.From {
			return false, errStale
		}
		_, progress, err := c.censusOriginalIn(tr, move, original)
		if err != nil {
			return false, err
		}
		p := c.binding.Transfer.Census
		rows := uint64(4*p.HeadRows+p.PageRows+64) * graphCostParallelCallbacks
		// SDK GetRange delivers its admitted row window before PageBytes can
		// reject output. Charge the complete maximum delivered native window,
		// including prefix/own-record skips, rather than just emitted bytes.
		work := uint64(4*p.HeadBytes+4*p.HeadRows*1024+(p.PageRows+12)*((64<<10)+1024)+(256<<10)) * graphCostParallelCallbacks
		if progress.Calls == p.MaxCalls || rows > p.MaxWorkRows-progress.WorkRows || work > p.MaxWorkBytes-progress.WorkBytes {
			return false, errLimit
		}
		if progress.Pending != "" && progress.Pending != request {
			return false, errOwnerBusy
		}
		if ordinal == progress.Pages && progress.Last.Request == request {
			if progress.Invocations == 8 {
				return false, errLimit
			}
			progress.Invocations++
		} else {
			if ordinal != progress.Pages+1 {
				return false, errStale
			}
			if progress.EOF {
				return false, errHistoryExpired
			}
			if progress.Pending == "" {
				progress.Pending = request
				progress.Invocations = 0
			}
			if progress.Invocations == 8 {
				return false, errLimit
			}
			progress.Invocations++
		}
		progress.Calls++
		progress.WorkRows += rows
		progress.WorkBytes += work
		return true, put(tr, c.censusKey(original, "progress"), progress)
	})
	if err != nil {
		return zero, err
	}
	delivery, _, err := historyTransactContext(ctx, c.h.db, func() error { return nil }, func(tr fdb.Transaction) (graphOwnerCensusDelivery, error) {
		if err := c.authenticate(a); err != nil {
			return zero, err
		}
		if a.ID != move.From {
			return zero, errStale
		}
		_, progress, err := c.censusOriginalIn(tr, move, original)
		if err != nil {
			return zero, err
		}
		replay := progress.Last.Request == request
		from := progress.Position
		if replay {
			from = progress.Last.From
		} else if progress.Pending != request {
			return zero, errMismatch
		}
		rows, next, err := c.censusRowsIn(tr, original, from)
		if err != nil {
			return zero, err
		}
		page := graphOwnerCensusPage{Request: request, Ordinal: ordinal, From: from, Next: next, Rows: uint64(len(rows)), EOF: next.Group == 2}
		for _, row := range rows {
			page.Bytes += uint64(len(row.Key) + len(row.Value))
			page.Hash = historyChain(page.Hash, row.Key, row.Value)
		}
		if replay {
			if !graphColumnSame(page, progress.Last) {
				return zero, errMismatch
			}
			return graphOwnerCensusDelivery{page, rows, true}, nil
		}
		p := c.binding.Transfer.Census
		if page.Rows > p.MaxRows-progress.Rows || page.Bytes > p.MaxBytes-progress.Bytes {
			return zero, errLimit
		}
		progress.Rows += page.Rows
		progress.Bytes += page.Bytes
		for _, row := range rows {
			progress.Hash = historyChain(progress.Hash, row.Key, row.Value)
		}
		progress.Position, progress.Last, progress.EOF = next, page, page.EOF
		progress.Pending = ""
		progress.Pages++
		if err := put(tr, c.censusKey(original, "progress"), progress); err != nil {
			return zero, err
		}
		return graphOwnerCensusDelivery{page, rows, false}, nil
	})
	return delivery, err // External artifact IO begins only after this returns.
}

// Called ONLY from Activate's genuine native transaction, before source-owner
// release and catalog routing writes. A raw hash/count without the complete
// current mutable inventory, authentic frozen owner and both EOFs is insufficient.
func (c *graphOwnerCatalog) censusActivateIn(tr fdb.Transaction, move graphOwnerMove, frozen graphOwnerMoveProgress, after graphOwnerState) error {
	if frozen.Census == "" {
		return errUnavailable
	}
	o, p, err := c.censusOriginalIn(tr, move, frozen.Census)
	if err != nil {
		return err
	}
	if !p.EOF || p.Position.Group != 2 || p.Pending != "" || p.Last.Request == "" || !p.Last.EOF {
		return errUnavailable
	}
	headBody, err := wire(o.Heads)
	if err != nil {
		return err
	}
	seal := graphOwnerCensusSeal{Original: p.Original, Before: move.Before, After: after, Cut: frozen.Cut, Rows: p.Rows, Bytes: p.Bytes, Hash: p.Hash, Heads: sha256.Sum256(headBody), Complete: true}
	old, found, err := get[graphOwnerCensusSeal](tr, c.censusKey(o.Request, "seal"))
	if err != nil {
		return err
	}
	if !found || old.Original != p.Original || old.Complete {
		return errMismatch
	}
	return put(tr, c.censusKey(o.Request, "seal"), seal)
}

func (c *graphOwnerCatalog) ObserveCensus(ctx context.Context, a graphOwnerAuthority, move graphOwnerMove, request string) (graphOwnerCensusObservation, error) {
	var zero graphOwnerCensusObservation
	if c == nil || c.binding.Transfer == nil || c.binding.Transfer.Version != 2 || !validName(request) {
		return zero, errInvalid
	}
	owned, err := graphOwnerMoveInput(move)
	if err != nil {
		return zero, err
	}
	move = owned
	ctx, cancel, err := c.operationContext(ctx)
	if err != nil {
		return zero, err
	}
	defer cancel()
	result, _, err := graphCostParallelRead(ctx, c.h.db, func(tr fdb.ReadTransaction) (graphOwnerCensusObservation, error) {
		actual, err := c.h.ownerCatalogIn(tr)
		if err != nil {
			return zero, err
		}
		if err := actual.authenticate(a); err != nil {
			return zero, err
		}
		if !graphColumnSame(actual.binding, c.binding) {
			return zero, errMismatch
		}
		frozen, err := c.moveIn(tr, move)
		if err != nil {
			return zero, err
		}
		if frozen.Phase != 2 || frozen.Census != request {
			return zero, errUnavailable
		}
		var out graphOwnerCensusObservation
		for i, kind := range []string{"original", "progress", "seal"} {
			out.Controls[i], err = snapshotStored(tr, c.censusKey(request, kind), 64<<10)
			if err != nil {
				return zero, err
			}
		}
		out.Original, err = decode[graphOwnerCensusOriginal](out.Controls[0].Value)
		if err != nil {
			return zero, err
		}
		out.Progress, err = decode[graphOwnerCensusProgress](out.Controls[1].Value)
		if err != nil {
			return zero, err
		}
		out.Seal, err = decode[graphOwnerCensusSeal](out.Controls[2].Value)
		if err != nil {
			return zero, err
		}
		p, s := out.Progress, out.Seal
		binding, err := wire(actual.binding)
		if err != nil {
			return zero, err
		}
		heads, err := wire(out.Original.Heads)
		if err != nil {
			return zero, err
		}
		before := frozen
		before.Phase = 1
		after := move.Before
		after.Version++
		after.Routes[move.Bucket] = graphOwnerRoute{move.To, move.Before.Routes[move.Bucket].Epoch + 1}
		if !censusProgressValid(p, *actual.binding.Transfer.Census) || !s.Complete || !p.EOF || p.Pending != "" || s.Original != sha256.Sum256(out.Controls[0].Value) || p.Original != s.Original || s.Rows != p.Rows || s.Bytes != p.Bytes || s.Hash != p.Hash || out.Original.Request != request || out.Original.Binding != sha256.Sum256(binding) || !graphColumnSame(out.Original.Move, move) || !graphColumnSame(out.Original.Frozen, before) || s.Before != move.Before || s.After != after || !sameCertificate(s.Cut, frozen.Cut) || s.Heads != sha256.Sum256(heads) || len(heads) > actual.binding.Transfer.Census.HeadBytes || len(out.Original.Heads) > actual.binding.Transfer.Census.HeadRows {
			return zero, errMismatch
		}
		out.AllRows, out.AllBytes = s.Rows+3, s.Bytes
		for _, r := range out.Controls {
			n := uint64(len(r.Key) + len(r.Value))
			if n > math.MaxUint64-out.AllBytes {
				return zero, errLimit
			}
			out.AllBytes += n
		}
		return out, nil
	})
	return result, err
}
