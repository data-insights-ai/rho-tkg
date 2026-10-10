package vendorcompare

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	GraphName      = "VCGraph20261010"
	NodeType       = "VCNode20261010"
	ImageReference = "tigergraph/tigergraph:4.2.5@sha256:78a3d62604527ba8686930465554fc3a419bf4b4de5c3d2d50202825a5308d49"
	ProjectName    = "rho-vendor-tg-20261010"
	ContainerName  = "rho-vendor-tg-20261010-db"
	Endpoint       = "http://127.0.0.1:19240/restpp"
)

var propertyTypes = map[string]string{"p_bool": "bool", "p_i64": "i64", "p_f64": "f64", "p_text": "text", "p_optional": "i64"}
var logicalTypes = []string{"HOP", "REL1", "REL2"}

func edgeType(kind string, reverse bool) (string, error) {
	if !slices.Contains(logicalTypes, kind) {
		return "", ErrUnsupported
	}
	prefix := "VC"
	if reverse {
		prefix = "VCR"
	}
	return prefix + kind + "20261010", nil
}
func logicalType(kind string) (string, bool, error) {
	for _, logical := range logicalTypes {
		for _, reverse := range []bool{false, true} {
			native, _ := edgeType(logical, reverse)
			if native == kind {
				return logical, reverse, nil
			}
		}
	}
	return "", false, ErrContract
}
func validID(id, kind string) bool {
	prefix := "n:"
	if kind == "edge" {
		prefix = "e:"
	} else if kind != "node" {
		return false
	}
	if len(id) != 18 || !strings.HasPrefix(id, prefix) {
		return false
	}
	for _, c := range id[2:] {
		if c < '0' || c > '9' {
			if c < 'a' || c > 'f' {
				return false
			}
		}
	}
	return true
}
func validateRow(row Entity) error {
	if !validID(row.ID, row.Kind) || row.Properties == nil || len(row.Properties) > len(propertyTypes) {
		return ErrContract
	}
	for key, cell := range row.Properties {
		if propertyTypes[key] != cell.Type {
			return ErrUnsupported
		}
		if _, err := cell.Native(); err != nil {
			return err
		}
	}
	if row.Kind == "node" {
		if row.Source != "" || row.Target != "" || row.Type != "" || len(row.Labels) == 0 || len(row.Labels) > 64 {
			return ErrContract
		}
		seen := map[string]bool{}
		for _, label := range row.Labels {
			if label == "" || len(label) > 256 || seen[label] {
				return ErrContract
			}
			seen[label] = true
		}
	} else {
		if len(row.Labels) > 0 || !validID(row.Source, "node") || !validID(row.Target, "node") {
			return ErrContract
		}
		if _, err := edgeType(row.Type, false); err != nil {
			return err
		}
	}
	return nil
}

type attribute struct {
	Value any `json:"value"`
}

func attributes(row Entity) (map[string]attribute, error) {
	if err := validateRow(row); err != nil {
		return nil, err
	}
	out := make(map[string]attribute)
	for key, kind := range propertyTypes {
		cell, present := row.Properties[key]
		var value any
		switch kind {
		case "bool":
			value = false
		case "i64":
			value = int64(0)
		case "f64":
			value = float64(0)
		case "text":
			value = ""
		}
		if present {
			var err error
			value, err = cell.Native()
			if err != nil {
				return nil, err
			}
		}
		out[key] = attribute{value}
		out["has_"+key] = attribute{present}
	}
	if row.Kind == "node" {
		out["vc_labels"] = attribute{slices.Clone(row.Labels)}
	} else {
		out["vc_edge_id"] = attribute{row.ID} // Native multi-edge discriminator.
		out["vc_id"] = attribute{row.ID}      // Explicit ordinary attribute for export identity.
	}
	return out, nil
}
func nativeBool(data json.RawMessage) (bool, error) {
	switch string(data) {
	case "true", "1":
		return true, nil
	case "false", "0":
		return false, nil
	default:
		return false, ErrContract
	}
}
func nativeProperties(attrs map[string]json.RawMessage) (map[string]Cell, error) {
	out := make(map[string]Cell)
	for key, kind := range propertyTypes {
		flag, ok := attrs["has_"+key]
		if !ok {
			return nil, ErrContract
		}
		present, err := nativeBool(flag)
		if err != nil {
			return nil, err
		}
		raw, ok := attrs[key]
		if !ok {
			return nil, ErrContract
		}
		if !present {
			continue
		}
		var value any
		switch kind {
		case "bool":
			value, err = nativeBool(raw)
		case "text":
			if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				return nil, ErrContract
			}
			var s string
			err = strictJSON(raw, &s)
			value = s
		case "i64":
			var number json.Number
			err = strictJSON(raw, &number)
			if err == nil {
				var n int64
				n, err = strconv.ParseInt(number.String(), 10, 64)
				if err == nil && strconv.FormatInt(n, 10) != number.String() {
					err = ErrContract
				}
				value = n
			}
		case "f64":
			var number json.Number
			err = strictJSON(raw, &number)
			if err == nil {
				var n float64
				n, err = strconv.ParseFloat(number.String(), 64)
				if math.IsNaN(n) || math.IsInf(n, 0) {
					err = ErrContract
				}
				value = n
			}
		}
		if err != nil {
			return nil, errors.Join(ErrContract, err)
		}
		cell, err := cellFromNative(value)
		if err != nil {
			return nil, err
		}
		out[key] = cell
	}
	return out, nil
}

type nativeNode struct {
	ID         string                     `json:"v_id"`
	Type       string                     `json:"v_type"`
	Attributes map[string]json.RawMessage `json:"attributes"`
}
type nativeEdge struct {
	Type          string                     `json:"e_type"`
	Source        string                     `json:"from_id"`
	SourceType    string                     `json:"from_type"`
	Target        string                     `json:"to_id"`
	TargetType    string                     `json:"to_type"`
	Directed      bool                       `json:"directed"`
	Attributes    map[string]json.RawMessage `json:"attributes"`
	Discriminator json.RawMessage            `json:"discriminator,omitempty"`
}

func decodeNode(node nativeNode) (Entity, error) {
	if node.Type != NodeType || !validID(node.ID, "node") {
		return Entity{}, ErrContract
	}
	props, err := nativeProperties(node.Attributes)
	if err != nil {
		return Entity{}, err
	}
	var labels []string
	if err := strictJSON(node.Attributes["vc_labels"], &labels); err != nil {
		return Entity{}, err
	}
	slices.Sort(labels)
	row := Entity{Kind: "node", ID: node.ID, Labels: labels, Properties: props}
	return row, validateRow(row)
}
func decodeEdge(edge nativeEdge) (Entity, bool, error) {
	if !edge.Directed || edge.SourceType != NodeType || edge.TargetType != NodeType || !validID(edge.Source, "node") || !validID(edge.Target, "node") {
		return Entity{}, false, ErrContract
	}
	kind, reverse, err := logicalType(edge.Type)
	if err != nil {
		return Entity{}, false, err
	}
	props, err := nativeProperties(edge.Attributes)
	if err != nil {
		return Entity{}, false, err
	}
	var id string
	if err := strictJSON(edge.Attributes["vc_id"], &id); err != nil {
		return Entity{}, false, err
	}
	// The ordinary identity is mandatory. If the discriminator is also returned
	// as an attribute, it must agree; neither is inferred from endpoint pairs.
	if raw, ok := edge.Attributes["vc_edge_id"]; ok {
		var other string
		if err := strictJSON(raw, &other); err != nil || other != id {
			return Entity{}, false, ErrContract
		}
	}
	source, target := edge.Source, edge.Target
	if reverse {
		source, target = target, source
	}
	row := Entity{Kind: "edge", ID: id, Source: source, Target: target, Type: kind, Properties: props}
	return row, reverse, validateRow(row)
}

type restEnvelope struct {
	Error   *bool           `json:"error"`
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Results json.RawMessage `json:"results"`
}

// Traffic is application HTTP work, not host-network or physical engine cost.
type Traffic struct {
	Requests      uint64            `json:"requests"`
	RequestBytes  uint64            `json:"request_bytes"`
	ResponseBytes uint64            `json:"response_bytes"`
	ByMethod      map[string]uint64 `json:"requests_by_method"`
}

// TigerGraph is a serial functional adapter over actual native reads. Callbacks
// must not reenter it. It retains no mirror of entity values or expected answers.
type TigerGraph struct {
	mu      sync.Mutex
	base    string
	client  *http.Client
	traffic Traffic
}

// NewTigerGraph admits only a literal local endpoint and disables redirects.
func NewTigerGraph(endpoint string, client *http.Client) (*TigerGraph, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.Port() == "" || u.User != nil || (u.Path != "" && u.Path != "/restpp") || u.RawQuery != "" || u.Fragment != "" {
		return nil, ErrContract
	}
	if n, err := strconv.Atoi(u.Port()); err != nil || n < 1 || n > 65535 {
		return nil, ErrContract
	}
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if copyClient.Timeout == 0 {
		copyClient.Timeout = 30 * time.Second
	}
	return &TigerGraph{base: endpoint, client: &copyClient, traffic: Traffic{ByMethod: map[string]uint64{}}}, nil
}
func (t *TigerGraph) request(ctx context.Context, method, path string, body any) (json.RawMessage, error) {
	if ctx == nil {
		return nil, ErrContract
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	if body == nil {
		data = nil
	}
	req, err := http.NewRequestWithContext(ctx, method, t.base+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if method == http.MethodPost {
		req.Header.Set("gsql-atomic-level", "atomic")
	}
	t.traffic.Requests++
	t.traffic.RequestBytes += uint64(len(data))
	t.traffic.ByMethod[method]++
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	b, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20+1))
	closeErr := resp.Body.Close()
	t.traffic.ResponseBytes += uint64(len(b))
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	if resp.StatusCode != http.StatusOK || len(b) > 8<<20 {
		return nil, fmt.Errorf("%w: REST status=%d or response limit", ErrContract, resp.StatusCode)
	}
	// Envelope permits documented version/timing fields, but duplicate keys,
	// invalid numbers, excessive nesting and multiple JSON values still refuse.
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	if err := walkJSON(decoder, 0); err != nil {
		return nil, errors.Join(ErrContract, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, ErrContract
	}
	var envelope restEnvelope
	if err := json.Unmarshal(b, &envelope); err != nil {
		return nil, err
	}
	if envelope.Error == nil || *envelope.Error || len(envelope.Results) == 0 || string(envelope.Results) == "null" {
		return nil, fmt.Errorf("%w: REST refused code=%s", ErrContract, envelope.Code)
	}
	return envelope.Results, nil
}
func graphPath() string { return "/graph/" + GraphName }
func (t *TigerGraph) nodes(ctx context.Context) ([]Entity, error) {
	data, err := t.request(ctx, http.MethodGet, graphPath()+"/vertices/"+NodeType, nil)
	if err != nil {
		return nil, err
	}
	var native []nativeNode
	if err := strictJSON(data, &native); err != nil {
		return nil, err
	}
	if len(native) > 128 {
		return nil, ErrLimit
	}
	rows := make([]Entity, 0, len(native))
	seen := map[string]bool{}
	for _, node := range native {
		row, err := decodeNode(node)
		if err != nil {
			return nil, err
		}
		if seen[row.ID] {
			return nil, ErrContract
		}
		seen[row.ID] = true
		rows = append(rows, row)
	}
	slices.SortFunc(rows, func(a, b Entity) int { return strings.Compare(a.ID, b.ID) })
	return rows, nil
}
func (t *TigerGraph) adjacency(ctx context.Context, id string, reverse bool) ([]Entity, error) {
	if !validID(id, "node") {
		return nil, ErrContract
	}
	var rows []Entity
	seen := map[string]bool{}
	for _, kind := range logicalTypes {
		nativeType, _ := edgeType(kind, reverse)
		data, err := t.request(ctx, http.MethodGet, graphPath()+"/edges/"+NodeType+"/"+url.PathEscape(id)+"/"+nativeType, nil)
		if err != nil {
			return nil, err
		}
		var native []nativeEdge
		if err := strictJSON(data, &native); err != nil {
			return nil, err
		}
		for _, edge := range native {
			row, isReverse, err := decodeEdge(edge)
			if err != nil {
				return nil, err
			}
			if isReverse != reverse || row.Type != kind || (!reverse && row.Source != id) || (reverse && row.Target != id) || seen[row.ID] {
				return nil, ErrContract
			}
			seen[row.ID] = true
			rows = append(rows, row)
			if len(rows) > 256 {
				return nil, ErrLimit
			}
		}
	}
	slices.SortFunc(rows, func(a, b Entity) int { return strings.Compare(a.ID, b.ID) })
	return rows, nil
}
func (t *TigerGraph) edges(ctx context.Context) ([]Entity, error) {
	nodes, err := t.nodes(ctx)
	if err != nil {
		return nil, err
	}
	var rows []Entity
	seen := map[string]bool{}
	for _, node := range nodes {
		edges, err := t.adjacency(ctx, node.ID, false)
		if err != nil {
			return nil, err
		}
		for _, edge := range edges {
			if seen[edge.ID] {
				return nil, ErrContract
			}
			seen[edge.ID] = true
			rows = append(rows, edge)
			if len(rows) > 256 {
				return nil, ErrLimit
			}
		}
	}
	slices.SortFunc(rows, func(a, b Entity) int { return strings.Compare(a.ID, b.ID) })
	return rows, nil
}
func (t *TigerGraph) lookup(ctx context.Context, kind, id string) (Entity, bool, error) {
	if !validID(id, kind) {
		return Entity{}, false, ErrContract
	}
	var rows []Entity
	var err error
	if kind == "node" {
		rows, err = t.nodes(ctx)
	} else {
		rows, err = t.edges(ctx)
	}
	if err != nil {
		return Entity{}, false, err
	}
	for _, row := range rows {
		if row.ID == id {
			return row, true, nil
		}
	}
	return Entity{}, false, nil
}
func (t *TigerGraph) write(ctx context.Context, row Entity, appendOnly bool) error {
	attrs, err := attributes(row)
	if err != nil {
		return err
	}
	params := url.Values{"ack": {"all"}}
	var body any
	if row.Kind == "node" {
		if appendOnly {
			params.Set("new_vertex_only", "true")
		} else {
			params.Set("update_vertex_only", "true")
		}
		body = map[string]any{"vertices": map[string]any{NodeType: map[string]any{row.ID: attrs}}}
	} else {
		params.Set("vertex_must_exist", "true")
		nativeType, _ := edgeType(row.Type, false)
		body = map[string]any{"edges": map[string]any{NodeType: map[string]any{row.Source: map[string]any{nativeType: map[string]any{NodeType: map[string]any{row.Target: []any{attrs}}}}}}}
	}
	results, err := t.request(ctx, http.MethodPost, graphPath()+"?"+params.Encode(), body)
	if err != nil {
		return err
	}
	var counts []struct {
		Vertices        *int `json:"accepted_vertices"`
		Edges           *int `json:"accepted_edges"`
		SkippedVertices int  `json:"skipped_vertices"`
		SkippedEdges    int  `json:"skipped_edges"`
	}
	// Other documented diagnostic fields are allowed; exact acceptance counts
	// and a native readback are mandatory rather than assuming HTTP 200 is success.
	if err := json.Unmarshal(results, &counts); err != nil || len(counts) != 1 || counts[0].SkippedVertices != 0 || counts[0].SkippedEdges != 0 {
		return ErrContract
	}
	if row.Kind == "node" {
		if counts[0].Vertices == nil || *counts[0].Vertices != 1 {
			return ErrContract
		}
	} else {
		if counts[0].Edges == nil || *counts[0].Edges != 1 {
			return ErrContract
		}
	}
	got, found, err := t.lookup(ctx, row.Kind, row.ID)
	if err != nil {
		return err
	}
	row.Labels = slices.Sorted(slices.Values(row.Labels))
	if !found || !equalEntity(row, got) {
		return ErrContract
	}
	return nil
}
func equalEntity(a, b Entity) bool {
	if a.Kind != b.Kind || a.ID != b.ID || a.Source != b.Source || a.Target != b.Target || a.Type != b.Type || !slices.Equal(a.Labels, b.Labels) || len(a.Properties) != len(b.Properties) {
		return false
	}
	for key, cell := range a.Properties {
		other, ok := b.Properties[key]
		if !ok || cell.Type != other.Type {
			return false
		}
		left, e1 := cell.Native()
		right, e2 := other.Native()
		if e1 != nil || e2 != nil || left != right {
			return false
		}
	}
	return true
}

// Load creates one absent identity and confirms its complete native readback.
func (t *TigerGraph) Load(ctx context.Context, row Entity) error {
	if t == nil || ctx == nil {
		return ErrContract
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := validateRow(row); err != nil {
		return err
	}
	if _, found, err := t.lookup(ctx, row.Kind, row.ID); err != nil {
		return err
	} else if found {
		return ErrContract
	}
	return t.write(ctx, row, true)
}

// Apply executes one fixture mutation. Separate events are not a stage transaction.
func (t *TigerGraph) Apply(ctx context.Context, event Event) error {
	if t == nil || ctx == nil || event.Revision < 1 {
		return ErrContract
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if event.Op == "append" {
		if _, found, err := t.lookup(ctx, event.Row.Kind, event.Row.ID); err != nil {
			return err
		} else if found {
			return ErrContract
		}
		return t.write(ctx, event.Row, true)
	}
	if event.Op == "upsert" {
		if event.Row.Kind != "node" {
			return ErrUnsupported
		}
		_, found, err := t.lookup(ctx, "node", event.Row.ID)
		if err != nil {
			return err
		}
		return t.write(ctx, event.Row, !found)
	}
	prior, found, err := t.lookup(ctx, event.Kind, event.ID)
	if err != nil {
		return err
	}
	if !found {
		return ErrContract
	}
	switch event.Op {
	case "update":
		prior.Properties = maps.Clone(prior.Properties)
		seen := map[string]bool{}
		for _, key := range event.Remove {
			if seen[key] {
				return ErrContract
			}
			seen[key] = true
			if _, ok := prior.Properties[key]; !ok {
				return ErrContract
			}
			if _, ok := event.Set[key]; ok {
				return ErrContract
			}
			delete(prior.Properties, key)
		}
		maps.Copy(prior.Properties, event.Set)
		return t.write(ctx, prior, false)
	case "delete":
		path := graphPath()
		if event.Kind == "node" {
			for _, reverse := range []bool{false, true} {
				rows, err := t.adjacency(ctx, event.ID, reverse)
				if err != nil {
					return err
				}
				if len(rows) > 0 {
					return ErrContract
				}
			}
			path += "/vertices/" + NodeType + "/" + url.PathEscape(event.ID)
		} else {
			kind, _ := edgeType(prior.Type, false)
			path += "/edges/" + NodeType + "/" + url.PathEscape(prior.Source) + "/" + kind + "/" + NodeType + "/" + url.PathEscape(prior.Target) + "/" + url.PathEscape(event.ID)
		}
		if _, err := t.request(ctx, http.MethodDelete, path, nil); err != nil {
			return err
		}
		if _, found, err := t.lookup(ctx, event.Kind, event.ID); err != nil {
			return err
		} else if found {
			return ErrContract
		}
		return nil
	default:
		return ErrUnsupported
	}
}
func matches(cell Cell, request Request) (bool, error) {
	value, err := cell.Native()
	if err != nil {
		return false, err
	}
	if request.Op == "equality" {
		other, err := request.Value.Native()
		return err == nil && cell.Type == request.Value.Type && value == other, err
	}
	if cell.Type != request.Low.Type {
		return false, nil
	}
	low, err := request.Low.Native()
	if err != nil {
		return false, err
	}
	high, err := request.High.Native()
	if err != nil {
		return false, err
	}
	switch v := value.(type) {
	case int64:
		l, ok := low.(int64)
		h, ok2 := high.(int64)
		return ok && ok2 && v >= l && v <= h, nil
	case float64:
		l, ok := low.(float64)
		h, ok2 := high.(float64)
		return ok && ok2 && v >= l && v <= h, nil
	}
	return false, ErrUnsupported
}

// Query emits complete current rows. Filtering is client-side over bounded native
// scans; expansion recursively reads native adjacency and never deduplicates walks.
func (t *TigerGraph) Query(ctx context.Context, request Request, emit func(any) error) error {
	if t == nil || ctx == nil || emit == nil {
		return ErrContract
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if request.Op == "history" {
		return ErrUnsupported
	}
	if request.Op == "label" {
		if request.Kind != "" && request.Kind != "node" {
			return ErrContract
		}
		request.Kind = "node"
	}
	if request.Op == "type" {
		if request.Kind != "" && request.Kind != "edge" {
			return ErrContract
		}
		request.Kind = "edge"
	}
	if err := validateRequest(request); err != nil {
		return err
	}
	send := func(row any) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return emit(row)
	}
	if request.Op == "lookup" {
		row, found, err := t.lookup(ctx, request.Kind, request.ID)
		if err != nil {
			return err
		}
		if found {
			return send(row)
		}
		return nil
	}
	if request.Op == "adjacency" {
		rows, err := t.adjacency(ctx, request.ID, request.Direction == "in")
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := send(Adjacency{row.ID, row.Source, row.Target, row.Type}); err != nil {
				return err
			}
		}
		return nil
	}
	if request.Op == "expand" {
		if _, found, err := t.lookup(ctx, "node", request.ID); err != nil {
			return err
		} else if !found {
			return nil
		}
		var walk func(string, []string, []string, int) error
		walk = func(current string, edges, nodes []string, left int) error {
			if left == 0 {
				return nil
			}
			rows, err := t.adjacency(ctx, current, false)
			if err != nil {
				return err
			}
			for _, row := range rows {
				nextE := append(slices.Clone(edges), row.ID)
				nextN := append(slices.Clone(nodes), row.Target)
				if err := send(Walk{request.ID, len(nextE), nextE, nextN}); err != nil {
					return err
				}
				if err := walk(row.Target, nextE, nextN, left-1); err != nil {
					return err
				}
			}
			return nil
		}
		return walk(request.ID, nil, []string{request.ID}, request.MaxDepth)
	}
	var rows []Entity
	var err error
	if request.Kind == "node" {
		rows, err = t.nodes(ctx)
	} else {
		rows, err = t.edges(ctx)
	}
	if err != nil {
		return err
	}
	for _, row := range rows {
		var result any
		switch request.Op {
		case "scan":
			result = row
		case "label":
			if slices.Contains(row.Labels, request.Label) {
				result = IDRow{row.Kind, row.ID}
			}
		case "type":
			if row.Type == request.Type {
				result = IDRow{row.Kind, row.ID}
			}
		case "equality", "range":
			if cell, ok := row.Properties[request.Key]; ok {
				match, err := matches(cell, request)
				if err != nil {
					return err
				}
				if match {
					result = IDRow{row.Kind, row.ID}
				}
			}
		case "projection":
			columns := make(map[string]Column, len(request.Keys))
			for _, key := range request.Keys {
				if value, ok := row.Properties[key]; ok {
					columns[key] = Column{Presence: "present", Value: new(value)}
				} else {
					columns[key] = Column{Presence: "absent"}
				}
			}
			result = Projection{row.Kind, row.ID, columns}
		default:
			return ErrUnsupported
		}
		if result != nil {
			if err := send(result); err != nil {
				return err
			}
		}
	}
	return nil
}

// Traffic returns a defensive snapshot of observed application-level HTTP costs.
func (t *TigerGraph) Traffic() (Traffic, error) {
	if t == nil {
		return Traffic{}, ErrContract
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := t.traffic
	out.ByMethod = maps.Clone(out.ByMethod)
	return out, nil
}

// PhysicalInventory counts actual native forward/reverse rows, including helpers.
func (t *TigerGraph) PhysicalInventory(ctx context.Context) (Inventory, error) {
	if t == nil || ctx == nil {
		return Inventory{}, ErrContract
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	nodes, err := t.nodes(ctx)
	if err != nil {
		return Inventory{}, err
	}
	forward, err := t.edges(ctx)
	if err != nil {
		return Inventory{}, err
	}
	reverse := 0
	seen := map[string]bool{}
	actual := map[string]Entity{}
	for _, row := range forward {
		actual[row.ID] = row
	}
	for _, node := range nodes {
		rows, err := t.adjacency(ctx, node.ID, true)
		if err != nil {
			return Inventory{}, err
		}
		for _, row := range rows {
			native, ok := actual[row.ID]
			if !ok || seen[row.ID] || !equalEntity(row, native) {
				return Inventory{}, ErrContract
			}
			seen[row.ID] = true
		}
		reverse += len(rows)
	}
	if reverse != len(forward) {
		return Inventory{}, ErrContract
	}
	return Inventory{LogicalNodes: len(nodes), LogicalEdges: len(forward), ForwardRows: len(forward), ReverseRows: reverse, DeclaredCopies: 1, DeclaredPartitions: 1,
		NodeAttributesPerRow: 11, EdgeAttributesPerRow: 12, Scope: "Native row enumeration; fixed-schema defaults/presence/labels/duplicate external-ID attributes included. No native retained revision inventory."}, nil
}
