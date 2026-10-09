package txnproto

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

type coordinator struct {
	Local                      bool
	Tx                         Tx
	Votes                      map[uint8]Vote
	Decision                   *Decision
	ReserveBytes, ReserveSlots int
}
type intent struct {
	Tx                         Tx
	Vote                       Vote
	Decision                   *Decision
	Installed                  bool
	ReserveBytes, ReserveSlots int
}
type state struct {
	Coordinators             map[string]*coordinator
	Requests                 map[string]string
	Intents                  map[string]*intent
	Rows                     map[string][]Value
	Certificates             map[uint64]Certificate
	Floor, Fence, Generation uint64
	Lock                     string
	Versions                 int
}
type record struct {
	Index uint64
	Data  []byte
}
type image struct {
	Version int
	Config  Config
	Applied uint64
	Journal []record
}

// Machine is a deterministic reducer. Applying it directly is tentative and
// never yields Proof or a quorum receipt. Host is the authoritative read seam.
// Rejections are deterministic application outcomes, not replica-stop errors;
// malformed wire/recovery state stops the replica rather than guessing.
type Machine struct {
	mu           sync.Mutex
	config       Config
	state        state
	journal      []record
	journalBytes int
	applied      uint64
	last         error
}

// New creates an empty tentative reducer with finite resource bounds.
func New(c Config) (*Machine, error) {
	if c.Limits == (Limits{}) {
		c.Limits = DefaultLimits()
	}
	if err := checkConfig(c); err != nil {
		return nil, err
	}
	m := &Machine{config: c}
	m.reset()
	return m, nil
}
func (m *Machine) reset() {
	m.state = state{Coordinators: map[string]*coordinator{}, Requests: map[string]string{}, Intents: map[string]*intent{}, Rows: map[string][]Value{}, Certificates: map[uint64]Certificate{}}
	m.journal = nil
	m.journalBytes = 0
	m.applied = 0
	m.last = nil
}

func decode[T any](b []byte, maxBytes int) (T, error) {
	var v T
	if len(b) == 0 || len(b) > maxBytes {
		return v, ErrInvalid
	}
	// Typed schema has no recursive fields. Guard nesting before JSON parsing;
	// even an unknown adversarial field cannot create an enormous parser stack.
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
				return v, ErrInvalid
			}
		case '}', ']':
			depth--
			if depth < 0 {
				return v, ErrInvalid
			}
		}

	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&v); err != nil {
		return v, errors.Join(ErrInvalid, err)
	}
	canonical, err := json.Marshal(v)
	if err != nil || !bytes.Equal(canonical, b) {
		return v, ErrInvalid
	}
	return v, nil
}
func (m *Machine) checkTx(t Tx) error {
	if err := validateTx(t); err != nil {
		return err
	}
	c := m.config
	if t.Graph != c.Graph || t.Topology != c.Topology {
		return ErrStale
	}
	for _, p := range t.Participants {
		if p.Epoch != c.Epochs[p.Group] {
			return ErrStale
		}
	}
	return nil
}
func (m *Machine) checkView(v *View, kind string) error {
	if v == nil || v.Kind != kind || v.Index == 0 || v.Group > 1 || v.Graph != m.config.Graph || v.Topology != m.config.Topology || v.Epoch != m.config.Epochs[v.Group] {
		return ErrStale
	}
	if v.Tx == nil {
		return ErrInvalid
	}
	expected := View{Graph: v.Graph, Topology: v.Topology, Group: v.Group, Epoch: v.Epoch, Index: v.Index, Kind: v.Kind, Tx: v.Tx}
	switch kind {
	case Registration:
	case Prepared:
		if v.Vote == nil {
			return ErrInvalid
		}
		expected.Vote = v.Vote
	case Decided:
		if v.Decision == nil {
			return ErrInvalid
		}
		expected.Decision = v.Decision
	default:
		return ErrInvalid
	}
	if digest(*v) != digest(expected) {
		return ErrInvalid
	}
	return m.checkTx(*v.Tx)
}
func reserve(s state) (b, n, versions int) {
	for _, c := range s.Coordinators {
		b += c.ReserveBytes
		n += c.ReserveSlots
	}
	for _, i := range s.Intents {
		b += i.ReserveBytes
		n += i.ReserveSlots
		if i.ReserveSlots > 0 {
			p, _ := part(i.Tx, i.Vote.Group)
			versions += len(p.Effects)
		}
	}
	return
}
func (m *Machine) budget(s state, extraBytes, extraSlots int) error {
	b, n, v := reserve(s)
	l := m.config.Limits
	if m.journalBytes+extraBytes+b > l.JournalBytes || len(m.journal)+extraSlots+n > l.Transitions || len(s.Coordinators) > l.Transactions || len(s.Intents) > l.Transactions || s.Versions+v > l.Versions || len(s.Certificates) > l.Certificates {
		return ErrLimit
	}
	return nil
}

// Apply accepts committed application records. Configuration-entry gaps are
// legal: replica.Machine does not receive those entries. Empty records are no-ops.
func (m *Machine) Apply(e replica.Entry) error {
	if m == nil {
		return ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if e.Index <= m.applied || e.Term == 0 {
		return ErrInvalid
	}
	if len(e.Data) == 0 {
		m.applied = e.Index
		m.last = nil
		return nil
	}
	c, err := decode[command](e.Data, m.config.Limits.CommandBytes)
	if err != nil {
		return err
	}
	if err := validateCommand(c); err != nil {
		return err
	}
	m.apply(c, e.Index, e.Data)
	return nil
}
func validateCommand(c command) error {
	if c.Version != 1 {
		return ErrInvalid
	}
	// Canonical shape prevents hidden ignored payloads changing a request digest.
	expected := command{Version: 1, Kind: c.Kind}
	switch c.Kind {
	case "register", "local":
		expected.Tx = c.Tx
		if c.Tx == nil {
			return ErrInvalid
		}
	case "prepare":
		expected.Remote = c.Remote
		expected.Prior = c.Prior
		if c.Remote == nil {
			return ErrInvalid
		}
	case "vote", "resolve":
		expected.Remote = c.Remote
		if c.Remote == nil {
			return ErrInvalid
		}
	case "decide":
		expected.Remote = c.Remote
		expected.Commit = c.Commit
		if c.Remote == nil {
			return ErrInvalid
		}
	case "fence", "certify":
		expected.Round = c.Round
		if c.Round == 0 {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	if digest(c) != digest(expected) {
		return ErrInvalid
	}
	return nil
}
func (m *Machine) apply(c command, index uint64, data []byte) {
	s := clone(m.state)
	changed, err := m.transition(&s, c, index, len(data)+32)
	if err == nil && changed {
		err = m.budget(s, len(data)+32, 1)
	}
	if err == nil && changed {
		m.state = s
		m.journal = append(m.journal, record{index, bytes.Clone(data)})
		m.journalBytes += len(data) + 32
	}
	m.last = err
	m.applied = index
}
func matching(t Tx, v *View) bool { return v != nil && v.Tx != nil && digest(t) == digest(*v.Tx) }
func (m *Machine) bound(s *state, t Tx) error {
	if err := m.checkTx(t); err != nil {
		return err
	}
	if t.Coordinator != m.config.Group {
		return ErrStale
	}
	if id, ok := s.Requests[t.Request]; ok && id != t.ID {
		return ErrMismatch
	}
	if existing := s.Coordinators[t.ID]; existing != nil && digest(existing.Tx) != digest(t) {
		return ErrMismatch
	}
	return nil
}
func (m *Machine) validate(s *state, p Participant, id string) error {
	if s.Lock != "" && s.Lock != id {
		return ErrRetry
	}
	if p.Generation != s.Generation {
		return ErrRetry
	}
	for _, r := range p.Reads {
		h := s.Rows[r.Key]
		var v uint64
		if len(h) > 0 {
			v = h[len(h)-1].Version
		}
		if v != r.Version {
			return ErrRetry
		}
	}
	return nil
}
func (m *Machine) transition(s *state, c command, index uint64, wireBytes int) (bool, error) {
	switch c.Kind {
	case "register", "local":
		t := *c.Tx
		if err := m.bound(s, t); err != nil {
			return false, err
		}
		if s.Coordinators[t.ID] != nil {
			return false, nil
		}
		if len(s.Coordinators) >= m.config.Limits.Transactions {
			return false, ErrLimit
		}
		r := &coordinator{Tx: t, Votes: map[uint8]Vote{}}
		if c.Kind == "local" {
			r.Local = true
			if len(t.Participants) != 1 || t.Participants[0].Group != m.config.Group {
				return false, ErrInvalid
			}
			p := t.Participants[0]
			proofView := View{Graph: t.Graph, Topology: t.Topology, Group: t.Coordinator, Epoch: p.Epoch, Index: math.MaxUint64, Kind: Decided, Tx: &t, Decision: &Decision{Commit: true, Round: math.MaxUint64, Digest: digest(t), Reason: ErrRetry.Error()}}
			proofWire, _ := json.Marshal(proofView)
			if len(proofWire) > m.config.Limits.CommandBytes {
				return false, ErrLimit
			}
			err := m.validate(s, p, t.ID)
			d := &Decision{Digest: digest(t)}
			if err != nil {
				d.Reason = err.Error()
			} else {
				floor := max(s.Floor, s.Fence, t.Dependency)
				if floor == math.MaxUint64 {
					return false, ErrLimit
				}
				if s.Versions+len(p.Effects) > m.config.Limits.Versions {
					return false, ErrLimit
				}
				d.Commit = true
				d.Round = floor + 1
				if err := install(s, t, p, d.Round); err != nil {
					return false, err
				}
			}
			r.Decision = d
		} else {
			coordBytes, _, err := recoveryBudgets(t, m.config.Limits.CommandBytes)
			if err != nil {
				return false, err
			}
			r.ReserveBytes = coordBytes
			r.ReserveSlots = len(t.Participants) + 1
		}
		s.Coordinators[t.ID] = r
		s.Requests[t.Request] = t.ID
		return true, nil
	case "prepare":
		if err := m.checkView(c.Remote, Registration); err != nil {
			return false, err
		}
		t := *c.Remote.Tx
		if c.Remote.Group != t.Coordinator {
			return false, ErrStale
		}
		_, intentBytes, err := recoveryBudgets(t, m.config.Limits.CommandBytes)
		if err != nil {
			return false, err
		}
		p, ok := part(t, m.config.Group)
		if !ok {
			return false, ErrStale
		}
		if local := s.Coordinators[t.ID]; local != nil && local.Local && local.Decision != nil {
			return false, ErrMismatch
		}
		if old := s.Intents[t.ID]; old != nil {
			if digest(old.Tx) != digest(t) {
				return false, ErrMismatch
			}
			return false, nil
		}
		pos := 0
		for j, x := range t.Participants {
			if x.Group == p.Group {
				pos = j
			}
		}
		if pos > 0 {
			if err := m.checkView(c.Prior, Prepared); err != nil {
				return false, err
			}
			prev := t.Participants[pos-1]
			if !matching(t, c.Prior) || c.Prior.Group != prev.Group || c.Prior.Vote == nil || !validVote(t, *c.Prior.Vote) || !c.Prior.Vote.Yes {
				return false, ErrMismatch
			}
		} else if c.Prior != nil {
			return false, ErrInvalid
		}
		if len(s.Intents) >= m.config.Limits.Transactions {
			return false, ErrLimit
		}
		v := Vote{Group: p.Group, Epoch: p.Epoch, Digest: digest(t), Effects: digest(p), Floor: max(s.Floor, s.Fence, t.Dependency)}
		err = m.validate(s, p, t.ID)
		v.Yes = err == nil
		if err != nil {
			v.Reason = err.Error()
		}
		i := &intent{Tx: t, Vote: v}
		if v.Yes {
			i.ReserveBytes = intentBytes
			i.ReserveSlots = 1
			s.Lock = t.ID
		}
		s.Intents[t.ID] = i
		return true, nil
	case "vote":
		if err := m.checkView(c.Remote, Prepared); err != nil {
			return false, err
		}
		t := *c.Remote.Tx
		r := s.Coordinators[t.ID]
		if r == nil {
			return false, ErrUnavailable
		}
		if !matching(r.Tx, c.Remote) || c.Remote.Vote == nil || c.Remote.Group != c.Remote.Vote.Group || !validVote(t, *c.Remote.Vote) {
			return false, ErrMismatch
		}
		v := *c.Remote.Vote
		if old, ok := r.Votes[v.Group]; ok {
			if old != v {
				return false, ErrMismatch
			}
			return false, nil
		}
		if r.Decision != nil {
			return false, ErrMismatch
		}
		r.Votes[v.Group] = v
		r.ReserveBytes -= wireBytes
		r.ReserveSlots--
		if r.ReserveBytes < 0 || r.ReserveSlots < 1 {
			return false, ErrLimit
		}
		return true, nil
	case "decide":
		if err := m.checkView(c.Remote, Registration); err != nil {
			return false, err
		}
		t := *c.Remote.Tx
		r := s.Coordinators[t.ID]
		if r == nil {
			return false, ErrUnavailable
		}
		if !matching(r.Tx, c.Remote) {
			return false, ErrMismatch
		}
		if t.Coordinator != m.config.Group {
			return false, ErrStale
		}
		if r.Decision != nil {
			if r.Decision.Commit != c.Commit {
				return false, ErrMismatch
			}
			return false, nil
		}
		d := &Decision{Commit: c.Commit, Digest: digest(t)}
		floor := max(s.Floor, s.Fence, t.Dependency)
		for _, p := range t.Participants {
			v, ok := r.Votes[p.Group]
			if c.Commit && (!ok || !v.Yes) {
				return false, ErrPending
			}
			if ok {
				d.Votes = append(d.Votes, v)
				floor = max(floor, v.Floor)
			}
		}
		if c.Commit {
			if floor == math.MaxUint64 {
				return false, ErrLimit
			}
			d.Round = floor + 1
			s.Floor = max(s.Floor, d.Round)
		}
		r.Decision = d
		r.ReserveBytes = 0
		r.ReserveSlots = 0
		return true, nil
	case "resolve":
		if err := m.checkView(c.Remote, Decided); err != nil {
			return false, err
		}
		t := *c.Remote.Tx
		d := c.Remote.Decision
		if c.Remote.Group != t.Coordinator || d == nil {
			return false, ErrMismatch
		}
		if local := s.Coordinators[t.ID]; local != nil && local.Local && local.Decision != nil && matching(local.Tx, c.Remote) && digest(local.Decision) == digest(d) {
			return false, nil
		}
		if !validDecision(t, *d) {
			return false, ErrMismatch
		}
		p, ok := part(t, m.config.Group)
		if !ok {
			return false, ErrStale
		}
		i := s.Intents[t.ID]
		if i == nil {
			if d.Commit {
				return false, ErrUnavailable
			}
			if len(s.Intents) >= m.config.Limits.Transactions {
				return false, ErrLimit
			}
			// Terminal abort blocks a delayed old registration from reacquiring locks.
			i = &intent{Tx: t, Vote: Vote{Group: p.Group, Epoch: p.Epoch, Digest: digest(t), Effects: digest(p), Reason: "aborted before prepare"}}
			s.Intents[t.ID] = i
		}
		if digest(i.Tx) != digest(t) {
			return false, ErrMismatch
		}
		if i.Decision != nil {
			if digest(i.Decision) != digest(d) {
				return false, ErrMismatch
			}
			return false, nil
		}
		if d.Commit {
			if !i.Vote.Yes {
				return false, ErrMismatch
			}
			found := false
			for _, v := range d.Votes {
				if v == i.Vote {
					found = true
				}
			}
			if !found {
				return false, ErrMismatch
			}
			if err := install(s, t, p, d.Round); err != nil {
				return false, err
			}
		}
		i.Decision = d
		i.Installed = true
		i.ReserveBytes = 0
		i.ReserveSlots = 0
		if s.Lock == t.ID {
			s.Lock = ""
		}
		return true, nil
	case "fence":
		if c.Round <= s.Fence {
			return false, nil
		}
		s.Fence = c.Round
		s.Floor = max(s.Floor, c.Round)
		return true, nil
	case "certify":
		if c.Round > s.Fence {
			return false, ErrPending
		}
		if _, ok := s.Certificates[c.Round]; ok {
			return false, nil
		}
		for _, i := range s.Intents {
			if i.Vote.Yes && i.Vote.Floor < c.Round && !i.Installed {
				return false, ErrPending
			}
		}
		s.Certificates[c.Round] = Certificate{c.Round, index}
		return true, nil
	}
	return false, ErrInvalid
}
func validVote(t Tx, v Vote) bool {
	p, ok := part(t, v.Group)
	return ok && v.Epoch == p.Epoch && v.Digest == digest(t) && v.Effects == digest(p) && v.Floor >= t.Dependency && (v.Yes && v.Reason == "" || !v.Yes && v.Reason != "")
}
func validDecision(t Tx, d Decision) bool {
	if d.Digest != digest(t) {
		return false
	}
	if !d.Commit {
		return d.Round == 0 && len(d.Votes) <= len(t.Participants)
	}
	if d.Round <= t.Dependency || len(d.Votes) != len(t.Participants) {
		return false
	}
	for j, v := range d.Votes {
		if v.Group != t.Participants[j].Group || !validVote(t, v) || !v.Yes || d.Round <= v.Floor {
			return false
		}
	}
	return true
}
func install(s *state, t Tx, p Participant, round uint64) error {
	if len(p.Effects) > 0 && s.Generation == math.MaxUint64 {
		return ErrLimit
	}
	for _, e := range p.Effects {
		h := s.Rows[e.Key]
		if len(h) > 0 && h[len(h)-1].Version == math.MaxUint64 {
			return ErrLimit
		}
	}
	for _, e := range p.Effects {
		h := s.Rows[e.Key]
		version := uint64(1)
		if len(h) > 0 {
			version = h[len(h)-1].Version + 1
		}
		s.Rows[e.Key] = append(h, Value{e.Value, version, round, e.Delete, t.ID})
		s.Versions++
	}
	if len(p.Effects) > 0 {
		s.Generation++
	}
	s.Floor = max(s.Floor, round)
	return nil
}

// Checkpoint retains the complete bounded successful-command journal, including
// NO votes and terminal aborts. No log reclamation/expiry semantics are invented.
func (m *Machine) Checkpoint(maxBytes int) ([]byte, error) {
	if m == nil {
		return nil, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if maxBytes < 1 {
		return nil, ErrLimit
	}
	b, err := json.Marshal(image{1, m.config, m.applied, m.journal})
	if err != nil {
		return nil, err
	}
	if len(b) > maxBytes {
		return nil, ErrLimit
	}
	return b, nil
}

// Restore validates canonical versioned bytes and replays within finite quotas.
// An empty index 0/1 image is the adapter's synthetic initialization only.
// The driver checkpoint position can exceed the last application record because
// configuration records bypass Machine.Apply. Unresolved locks/floors survive.
func (m *Machine) Restore(index uint64, b []byte) error {
	if m == nil {
		return ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(b) == 0 {
		if index > 1 {
			return ErrInvalid
		}
		m.reset()
		m.applied = index
		return nil
	}
	if bytes.Count(b, []byte(`"Index":`)) > m.config.Limits.Transitions {
		return ErrInvalid
	}
	im, err := decode[image](b, 2*m.config.Limits.JournalBytes+4096)
	if err != nil {
		return err
	}
	if im.Version != 1 || im.Config != m.config || im.Applied > index || len(im.Journal) > m.config.Limits.Transitions {
		return ErrInvalid
	}
	next := &Machine{config: m.config}
	next.reset()
	for _, r := range im.Journal {
		if r.Index <= next.applied || r.Index > im.Applied || len(r.Data) > m.config.Limits.CommandBytes {
			return ErrInvalid
		}
		c, err := decode[command](r.Data, m.config.Limits.CommandBytes)
		if err != nil {
			return err
		}
		if err := validateCommand(c); err != nil {
			return err
		}
		before := len(next.journal)
		next.apply(c, r.Index, r.Data)
		if next.last != nil || len(next.journal) != before+1 {
			return ErrInvalid
		}
	}
	m.state = next.state
	m.journal = next.journal
	m.journalBytes = next.journalBytes
	m.applied = index
	m.last = nil
	return nil
}
