package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"unicode/utf8"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

var (
	errInvalid     = errors.New("fdb-spike: invalid input")
	errMismatch    = errors.New("fdb-spike: immutable binding mismatch")
	errStale       = errors.New("fdb-spike: stale graph/topology/epoch")
	errUnknown     = errors.New("fdb-spike: unknown outcome")
	errUnavailable = errors.New("fdb-spike: certificate unavailable")
	errLimit       = errors.New("fdb-spike: retained resource limit")
)

type limits struct{ Bytes, Transactions, Versions, Certificates uint64 }
type config struct {
	Graph    string
	Topology uint64
	Epochs   [2]uint64
	Limits   limits
}
type read struct {
	Key     string
	Version uint64
}
type effect struct {
	Key    string
	Value  int64
	Delete bool
}
type participant struct {
	Group             uint8
	Epoch, Generation uint64
	Reads             []read
	Effects           []effect
}
type transaction struct {
	Graph        string
	Topology     uint64
	ID, Request  string
	Coordinator  uint8
	Dependency   uint64
	Participants []participant
}
type value struct {
	Value          int64
	Version, Round uint64
	Deleted        bool
	TxID           string
}
type outcome struct {
	Commit              bool
	Round               uint64
	Digest              [32]byte
	ID, Request, Reason string
	Coordinator         uint8
}
type metadata struct{ Floor, Generation, Versions, Transactions, Certificates, Bytes uint64 }
type certificate struct {
	Graph    string
	Topology uint64
	Epochs   [2]uint64
	Scope    []uint8
	Round    uint64
}
type cut struct{ record certificate }
type snapshot struct {
	Values      [2]map[string]value
	Generations [2]uint64
}
type harness struct {
	db     fdb.Database
	config config
}

func defaults() config {
	return config{"fixture", 1, [2]uint64{7, 11}, limits{4 << 20, 256, 4096, 256}}
}
func validName(s string) bool { return s != "" && len(s) <= 128 && utf8.ValidString(s) }
func wire(v any) ([]byte, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return nil, e
	}
	if len(b) > 64<<10 {
		return nil, errLimit
	}
	return b, nil
}
func decode[T any](b []byte) (T, error) {
	var v T
	if len(b) == 0 || len(b) > 64<<10 {
		return v, errInvalid
	}
	depth := 0
	quoted, escaped := false, false
	for _, c := range b {
		if quoted {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		switch c {
		case '"':
			quoted = true
		case '{', '[':
			depth++
			if depth > 16 {
				return v, errInvalid
			}
		case '}', ']':
			depth--
			if depth < 0 {
				return v, errInvalid
			}
		}
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e := d.Decode(&v); e != nil {
		return v, errors.Join(errInvalid, e)
	}
	canonical, e := wire(v)
	if e != nil || !bytes.Equal(b, canonical) {
		return v, errInvalid
	}
	return v, nil
}
func get[T any](tr fdb.ReadTransaction, k fdb.Key) (T, bool, error) {
	var z T
	b, e := tr.Get(k).Get()
	if e != nil {
		return z, false, e
	}
	if b == nil {
		return z, false, nil
	}
	v, e := decode[T](b)
	return v, true, e
}
func (h *harness) prefix(g uint8) string {
	return fmt.Sprintf("g%d/%s/", g, hex.EncodeToString([]byte(h.config.Graph)))
}
func (h *harness) key(g uint8, kind, name string) fdb.Key {
	return fdb.Key(h.prefix(g) + kind + "/" + hex.EncodeToString([]byte(name)))
}
func prefixRange(p string) fdb.KeyRange {
	return fdb.KeyRange{Begin: fdb.Key(p), End: fdb.Key(p[:len(p)-1] + "0")}
}
func (h *harness) meta(tr fdb.ReadTransaction, g uint8) (metadata, error) {
	m, ok, e := get[metadata](tr, h.key(g, "meta", "state"))
	if e != nil {
		return m, e
	}
	if !ok || m.Bytes < h.metadataReservation(g) || m.Bytes > h.config.Limits.Bytes || m.Versions > h.config.Limits.Versions || m.Transactions > h.config.Limits.Transactions || m.Certificates > h.config.Limits.Certificates {
		return m, errInvalid
	}
	return m, nil
}

// Fixed config plus worst-width mutable metadata are reserved conservatively.
// User KV changes are charged exactly; physical FDB/page/replica overhead is
// measured separately and is NOT bounded by this application admission quota.
func (h *harness) metadataReservation(g uint8) uint64 {
	b, _ := wire(h.config)
	m, _ := wire(metadata{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64})
	return uint64(len(h.key(g, "meta", "config")) + len(b) + len(h.key(g, "meta", "state")) + len(m))
}
func (h *harness) checkConfig(tr fdb.ReadTransaction, g uint8) error {
	c, ok, e := get[config](tr, h.key(g, "meta", "config"))
	if e != nil {
		return e
	}
	if !ok || c != h.config {
		return errStale
	}
	return nil
}
func put(tr fdb.Transaction, k fdb.Key, v any) error {
	b, e := wire(v)
	if e != nil {
		return e
	}
	tr.Set(k, b)
	return nil
}
func newHarness(db fdb.Database, c config) (*harness, error) {
	l := c.Limits
	if !validName(c.Graph) || c.Topology == 0 || c.Epochs[0] == 0 || c.Epochs[1] == 0 || l.Bytes < 8192 || l.Bytes > 16<<20 || l.Transactions < 1 || l.Transactions > 1024 || l.Versions < 1 || l.Versions > 65536 || l.Certificates < 1 || l.Certificates > 1024 {
		return nil, errInvalid
	}
	h := &harness{db, c}
	_, e := db.Transact(func(tr fdb.Transaction) (any, error) {
		for g := uint8(0); g < 2; g++ {
			old, ok, e := get[config](tr, h.key(g, "meta", "config"))
			if e != nil {
				return nil, e
			}
			if ok {
				if old != c {
					return nil, errStale
				}
				if _, e = h.meta(tr, g); e != nil {
					return nil, e
				}
				continue
			}
			if e = put(tr, h.key(g, "meta", "config"), c); e != nil {
				return nil, e
			}
			if e = put(tr, h.key(g, "meta", "state"), metadata{Bytes: h.metadataReservation(g)}); e != nil {
				return nil, e
			}
		}
		return nil, nil
	})
	if e != nil {
		return nil, e
	}
	return h, nil
}
func (h *harness) validate(x transaction) error {
	if !validName(x.Graph) || !validName(x.ID) || !validName(x.Request) || x.Topology == 0 || x.Coordinator > 1 || len(x.Participants) < 1 || len(x.Participants) > 2 {
		return errInvalid
	}
	if x.Graph != h.config.Graph || x.Topology != h.config.Topology {
		return errStale
	}
	found := false
	for i, p := range x.Participants {
		if p.Group > 1 || p.Epoch == 0 || i > 0 && x.Participants[i-1].Group >= p.Group || len(p.Reads) > 128 || len(p.Effects) > 128 {
			return errInvalid
		}
		if p.Epoch != h.config.Epochs[p.Group] {
			return errStale
		}
		if p.Group == x.Coordinator {
			found = true
		}
		for j, r := range p.Reads {
			if !validName(r.Key) || j > 0 && p.Reads[j-1].Key >= r.Key {
				return errInvalid
			}
		}
		for j, e := range p.Effects {
			if !validName(e.Key) || e.Delete && e.Value != 0 || j > 0 && p.Effects[j-1].Key >= e.Key {
				return errInvalid
			}
		}
	}
	if !found {
		return errInvalid
	}
	return nil
}
func (h *harness) submit(x transaction) (outcome, error) {
	if h == nil {
		return outcome{}, errInvalid
	}
	if e := h.validate(x); e != nil {
		return outcome{}, e
	}
	b, e := wire(x)
	if e != nil {
		return outcome{}, e
	}
	x, e = decode[transaction](b)
	if e != nil {
		return outcome{}, e
	}
	result, e := h.db.Transact(func(tr fdb.Transaction) (any, error) { return h.execute(tr, x, sha256.Sum256(b)) })
	if e != nil {
		if _, ok := errors.AsType[fdb.Error](e); ok {
			return outcome{}, errors.Join(errUnknown, e)
		}
		return outcome{}, e
	}
	return result.(outcome), nil
}
func (h *harness) execute(tr fdb.Transaction, x transaction, digest [32]byte) (outcome, error) {
	if e := h.checkConfig(tr, x.Coordinator); e != nil {
		return outcome{}, e
	}
	req := h.key(x.Coordinator, "request", x.Request)
	id := h.key(x.Coordinator, "id", x.ID)
	old, ok, e := get[outcome](tr, req)
	if e != nil {
		return outcome{}, e
	}
	binding, idOK, e := get[outcome](tr, id)
	if e != nil {
		return outcome{}, e
	}
	if ok {
		if old.Digest != digest || old.ID != x.ID || !idOK || binding != old {
			return outcome{}, errMismatch
		}
		return old, nil
	}
	if idOK {
		return outcome{}, errMismatch
	}
	ms := [2]metadata{}
	current := [2]map[string]value{}
	floor := x.Dependency
	reason := ""
	for _, p := range x.Participants {
		g := p.Group
		if e := h.checkConfig(tr, g); e != nil {
			return outcome{}, e
		}
		m, e := h.meta(tr, g)
		if e != nil {
			return outcome{}, e
		}
		ms[g] = m
		floor = max(floor, m.Floor)
		// BEFORE writes: complete conservative predicate/absence conflict footprint.
		if e = tr.AddReadConflictRange(prefixRange(h.prefix(g) + "current/")); e != nil {
			return outcome{}, e
		}
		if p.Generation != m.Generation {
			reason = "generation"
		}
		current[g] = map[string]value{}
		for _, r := range p.Reads {
			v, _, e := get[value](tr, h.key(g, "current", r.Key))
			if e != nil {
				return outcome{}, e
			}
			if r.Version != v.Version {
				reason = "revision"
			}
			current[g][r.Key] = v
		}
		for _, f := range p.Effects {
			if _, ok := current[g][f.Key]; !ok {
				v, _, e := get[value](tr, h.key(g, "current", f.Key))
				if e != nil {
					return outcome{}, e
				}
				current[g][f.Key] = v
			}
		}
	}
	if ms[x.Coordinator].Transactions >= h.config.Limits.Transactions {
		return outcome{}, errLimit
	}
	o := outcome{Commit: reason == "", Digest: digest, ID: x.ID, Request: x.Request, Reason: reason, Coordinator: x.Coordinator}
	if o.Commit {
		if floor == math.MaxUint64 {
			return outcome{}, errLimit
		}
		o.Round = floor + 1
		for _, p := range x.Participants {
			m := ms[p.Group]
			if uint64(len(p.Effects)) > h.config.Limits.Versions-m.Versions || len(p.Effects) > 0 && m.Generation == math.MaxUint64 {
				return outcome{}, errLimit
			}
			for _, f := range p.Effects {
				if current[p.Group][f.Key].Version == math.MaxUint64 {
					return outcome{}, errLimit
				}
			}
		}
	}
	// Writes and exact encoded KV-byte quota deltas share the native commit.
	account := func(g uint8, k fdb.Key, v any, prior any, hadPrior bool) error {
		b, e := wire(v)
		if e != nil {
			return e
		}
		size := uint64(len(k) + len(b))
		m := ms[g]
		if hadPrior {
			old, e := wire(prior)
			if e != nil {
				return e
			}
			oldSize := uint64(len(k) + len(old))
			if m.Bytes < oldSize {
				return errInvalid
			}
			m.Bytes -= oldSize
		}
		if size > h.config.Limits.Bytes-m.Bytes {
			return errLimit
		}
		m.Bytes += size
		ms[g] = m
		tr.Set(k, b)
		return nil
	}
	if o.Commit {
		for _, p := range x.Participants {
			g := p.Group
			for _, f := range p.Effects {
				prev := current[g][f.Key]
				v := value{f.Value, prev.Version + 1, o.Round, f.Delete, x.ID}
				if e = account(g, h.key(g, "current", f.Key), v, prev, prev.Version != 0); e != nil {
					return outcome{}, e
				}
				history := fdb.Key(fmt.Sprintf("%shistory/%016x/%s", h.prefix(g), o.Round, hex.EncodeToString([]byte(f.Key))))
				if e = account(g, history, v, nil, false); e != nil {
					return outcome{}, e
				}
			}
			m := ms[g]
			m.Versions += uint64(len(p.Effects))
			if len(p.Effects) > 0 {
				m.Generation++
			}
			m.Floor = o.Round
			ms[g] = m
		}
	}
	if e = account(x.Coordinator, req, o, nil, false); e != nil {
		return outcome{}, e
	}
	if e = account(x.Coordinator, id, o, nil, false); e != nil {
		return outcome{}, e
	}
	m := ms[x.Coordinator]
	m.Transactions++
	ms[x.Coordinator] = m
	for _, p := range x.Participants {
		if e = put(tr, h.key(p.Group, "meta", "state"), ms[p.Group]); e != nil {
			return outcome{}, e
		}
	}
	return o, nil
}
func (h *harness) recover(g uint8, request string) (outcome, error) {
	if h == nil || g > 1 || !validName(request) {
		return outcome{}, errInvalid
	}
	r, e := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		o, ok, e := get[outcome](tr, h.key(g, "request", request))
		if e != nil {
			return nil, e
		}
		if !ok {
			return nil, errUnknown
		}
		return o, nil
	})
	if e != nil {
		return outcome{}, e
	}
	return r.(outcome), nil
}
func validScope(scope []uint8) bool {
	return len(scope) > 0 && len(scope) <= 2 && scope[0] <= 1 && (len(scope) == 1 || scope[0] == 0 && scope[1] == 1)
}
func (h *harness) fresh(scope []uint8, after *outcome) (cut, error) {
	if h == nil || !validScope(scope) {
		return cut{}, errInvalid
	}
	scope = slices.Clone(scope)
	result, e := h.db.Transact(func(tr fdb.Transaction) (any, error) {
		round := uint64(1)
		ms := [2]metadata{}
		if after != nil {
			if !after.Commit || after.Coordinator > 1 {
				return nil, errInvalid
			}
			o, ok, e := get[outcome](tr, h.key(after.Coordinator, "request", after.Request))
			if e != nil {
				return nil, e
			}
			if !ok || o != *after {
				return nil, errUnknown
			}
			round = max(round, o.Round)
		}
		for _, g := range scope {
			if e := h.checkConfig(tr, g); e != nil {
				return nil, e
			}
			m, e := h.meta(tr, g)
			if e != nil {
				return nil, e
			}
			ms[g] = m
			round = max(round, m.Floor)
		}
		cert := certificate{h.config.Graph, h.config.Topology, h.config.Epochs, scope, round}
		b, e := wire(cert)
		if e != nil {
			return nil, e
		}
		name := hex.EncodeToString(b)
		k := h.key(scope[0], "cut", name)
		prior, ok, e := get[certificate](tr, k)
		if e != nil {
			return nil, e
		}
		if ok {
			if !sameCertificate(prior, cert) {
				return nil, errMismatch
			}
			return cut{cert}, nil
		}
		m := ms[scope[0]]
		if m.Certificates >= h.config.Limits.Certificates || uint64(len(k)+len(b)) > h.config.Limits.Bytes-m.Bytes {
			return nil, errLimit
		}
		m.Certificates++
		m.Bytes += uint64(len(k) + len(b))
		ms[scope[0]] = m
		for _, g := range scope {
			m := ms[g]
			m.Floor = round
			if e = put(tr, h.key(g, "meta", "state"), m); e != nil {
				return nil, e
			}
		}
		tr.Set(k, b)
		return cut{cert}, nil
	})
	if e != nil {
		return cut{}, e
	}
	return result.(cut), nil
}
func sameCertificate(a, b certificate) bool {
	return a.Graph == b.Graph && a.Topology == b.Topology && a.Epochs == b.Epochs && a.Round == b.Round && slices.Equal(a.Scope, b.Scope)
}
func (h *harness) verifyCut(tr fdb.ReadTransaction, c cut) error {
	r := c.record
	if !validScope(r.Scope) || r.Round == 0 {
		return errInvalid
	}
	if r.Graph != h.config.Graph || r.Topology != h.config.Topology || r.Epochs != h.config.Epochs {
		return errStale
	}
	b, e := wire(r)
	if e != nil {
		return e
	}
	stored, ok, e := get[certificate](tr, h.key(r.Scope[0], "cut", hex.EncodeToString(b)))
	if e != nil {
		return e
	}
	if !ok || !sameCertificate(stored, r) {
		return errUnavailable
	}
	return nil
}

// at pages immutable retained history in FRESH native transactions. Fencing
// forbids later history <=B; no FDB native read-version pin is held or assumed.
func (h *harness) at(c cut, pageSize int, between func()) (snapshot, error) {
	s := snapshot{Values: [2]map[string]value{{}, {}}}
	if h == nil || pageSize < 1 || pageSize > 128 || !validScope(c.record.Scope) {
		return s, errInvalid
	}
	for _, g := range c.record.Scope {
		begin := fdb.Key(h.prefix(g) + "history/")
		end := fdb.Key(fmt.Sprintf("%shistory/%016x/", h.prefix(g), c.record.Round))
		// Include the complete round without incrementing MaxUint64.
		end = append(end[:len(end)-1], '0')
		count := uint64(0)
		for {
			r, e := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
				if e := h.verifyCut(tr, c); e != nil {
					return nil, e
				}
				return tr.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Limit: pageSize}).GetSliceWithError()
			})
			if e != nil {
				return snapshot{}, e
			}
			rows := r.([]fdb.KeyValue)
			for _, row := range rows {
				count++
				if count > h.config.Limits.Versions {
					return snapshot{}, errLimit
				}
				v, e := decode[value](row.Value)
				if e != nil {
					return snapshot{}, e
				}
				if v.Round == 0 || v.Round > c.record.Round || v.Version == 0 {
					return snapshot{}, errInvalid
				}
				offset := len(h.prefix(g)+"history/") + 17
				if len(row.Key) < offset || len(row.Key) > offset+256 {
					return snapshot{}, errInvalid
				}
				tail := string(row.Key[offset:])
				key, e := hex.DecodeString(tail)
				if e != nil || !validName(string(key)) {
					return snapshot{}, errInvalid
				}
				if v.Deleted {
					delete(s.Values[g], string(key))
				} else {
					s.Values[g][string(key)] = v
				}
			}
			if len(rows) < pageSize {
				break
			}
			begin = append(bytes.Clone(rows[len(rows)-1].Key), 0)
			if between != nil {
				between()
			}
		}
	}
	// Verify even an empty/invalid scope before returning success.
	_, e := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) { return nil, h.verifyCut(tr, c) })
	return s, e
}
func (h *harness) current() (snapshot, error) {
	if h == nil {
		return snapshot{}, errInvalid
	}
	r, e := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		s := snapshot{Values: [2]map[string]value{{}, {}}}
		for g := uint8(0); g < 2; g++ {
			if e := h.checkConfig(tr, g); e != nil {
				return nil, e
			}
			m, e := h.meta(tr, g)
			if e != nil {
				return nil, e
			}
			s.Generations[g] = m.Generation
			// #nosec G115 -- newHarness admits Versions <= 65,536, fitting every supported Go int.
			rows, e := tr.GetRange(prefixRange(h.prefix(g)+"current/"), fdb.RangeOptions{Limit: int(h.config.Limits.Versions) + 1}).GetSliceWithError()
			if e != nil {
				return nil, e
			}
			if uint64(len(rows)) > h.config.Limits.Versions {
				return nil, errLimit
			}
			for _, row := range rows {
				v, e := decode[value](row.Value)
				if e != nil {
					return nil, e
				}
				key, e := hex.DecodeString(string(row.Key[len(h.prefix(g)+"current/"):]))
				if e != nil || !validName(string(key)) || v.Version == 0 {
					return nil, errInvalid
				}
				if !v.Deleted {
					s.Values[g][string(key)] = v
				}
			}
		}
		return s, nil
	})
	if e != nil {
		return snapshot{}, e
	}
	return r.(snapshot), nil
}
