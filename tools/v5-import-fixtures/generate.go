package fixtures

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
)

// FixedExportedAt replaces the header's wall-clock ExportedAt so an export is
// byte-reproducible. An importer must ignore the field.
const FixedExportedAt int64 = 1_700_000_000_000

// Marker is the prefix of the string property every hashed scenario carries;
// flipping a byte of it must make v4 Import fail the integrity check.
const Marker = "canary-"

// Scenario is one fixture: a store configuration and the build that fills it.
type Scenario struct {
	Name      string
	Unbounded bool // Config.UntimedValidStart = UntimedUnbounded, else UntimedFromTxFrom
	Hashes    bool // integrity hashes on
	build     func(ctx context.Context, b *builder) error
}

func (s Scenario) config() graph.Config {
	c := graph.Config{AllowTxBackfill: true, DisableIntegrityHashes: !s.Hashes}
	if s.Unbounded {
		c.UntimedValidStart = graph.UntimedUnbounded
	}
	return c
}

func (s Scenario) info() Config {
	m := "tx_from"
	if s.Unbounded {
		m = "unbounded"
	}
	return Config{UntimedValidStart: m, IntegrityHashes: s.Hashes}
}

// Scenarios lists the fixtures in a fixed order.
func Scenarios() []Scenario {
	var out []Scenario
	for _, u := range []bool{false, true} {
		for _, h := range []bool{true, false} {
			name := "txfrom"
			if u {
				name = "unbounded"
			}
			if h {
				name += "_hashed"
			} else {
				name += "_nohash"
			}
			out = append(out, Scenario{Name: name, Unbounded: u, Hashes: h, build: buildStandard})
		}
	}
	return out
}

// Generate builds the store, exports it and records v4's answers.
func Generate(s Scenario) (Output, error) {
	ctx := context.Background()
	g, err := graph.New(s.config())
	if err != nil {
		return Output{}, err
	}
	defer g.Close()
	b := &builder{g: g, s: s}
	if err := s.build(ctx, b); err != nil {
		return Output{}, fmt.Errorf("build: %w", err)
	}
	var raw bytes.Buffer
	if err := g.IO().Export(&raw); err != nil {
		return Output{}, fmt.Errorf("export: %w", err)
	}
	exp, err := pinHeader(raw.Bytes())
	if err != nil {
		return Output{}, err
	}
	e, err := answer(ctx, g, s.Name, s.info(), b.query())
	if err != nil {
		return Output{}, err
	}
	js, err := marshal(e)
	if err != nil {
		return Output{}, err
	}
	return Output{Export: exp, Expected: js}, nil
}

func importInto(s Scenario, export []byte) error {
	_, err := importGraph(s, export)
	return err
}

func importGraph(s Scenario, export []byte) (*graph.Graph, error) {
	g, err := graph.New(s.config())
	if err != nil {
		return nil, err
	}
	if err := g.IO().Import(bytes.NewReader(export), tkgio.ImportOptions{}); err != nil {
		_ = g.Close()
		return nil, err
	}
	return g, nil
}

// Verify imports export into a fresh v4 store and checks that v4 gives exactly
// the answers recorded in expected for every ID, pin and timeline it lists.
func Verify(s Scenario, export, expected []byte) error {
	var want Expected
	if err := json.Unmarshal(expected, &want); err != nil {
		return fmt.Errorf("expected file: %w", err)
	}
	if want.Scenario != s.Name || want.Config != s.info() {
		return fmt.Errorf("expected file is for %q %+v, scenario is %q %+v", want.Scenario, want.Config, s.Name, s.info())
	}
	g, err := importGraph(s, export)
	if err != nil {
		return fmt.Errorf("v4 import: %w", err)
	}
	defer g.Close()
	q := queryOf(&want)
	q.declare = true
	got, err := answer(context.Background(), g, s.Name, s.info(), q)
	if err != nil {
		return err
	}
	a, err := marshal(&want)
	if err != nil {
		return err
	}
	b, err := marshal(got)
	if err != nil {
		return err
	}
	if !bytes.Equal(a, b) {
		return fmt.Errorf("v4 answers after import differ from the expected file: %s", firstDiff(a, b))
	}
	return nil
}

func firstDiff(a, b []byte) string {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	lo := max(0, i-60)
	return fmt.Sprintf("at byte %d: expected ...%s... got ...%s...", i, a[lo:min(len(a), i+80)], b[lo:min(len(b), i+80)])
}

// records splits an export stream into (tag, body offset, body length).
func records(b []byte) (tags []byte, offs, lens []int, err error) {
	for p := 0; p < len(b); {
		if p+5 > len(b) {
			return nil, nil, nil, errors.New("truncated record header")
		}
		n := int(binary.BigEndian.Uint32(b[p+1 : p+5]))
		if p+5+n > len(b) {
			return nil, nil, nil, errors.New("truncated record body")
		}
		tags = append(tags, b[p])
		offs = append(offs, p+5)
		lens = append(lens, n)
		p += 5 + n
	}
	return
}

// pinHeader overwrites the header record's "at" (ExportedAt) value with
// FixedExportedAt. The 8-byte msgpack integer keeps the record length.
func pinHeader(b []byte) ([]byte, error) {
	tags, offs, lens, err := records(b)
	if err != nil || len(tags) == 0 || tags[0] != 0x01 {
		return nil, fmt.Errorf("export does not start with a header record: %v", err)
	}
	body := b[offs[0] : offs[0]+lens[0]]
	i := bytes.Index(body, []byte{0xa2, 'a', 't'})
	if i < 0 || i+3+9 > len(body) || (body[i+3] != 0xcf && body[i+3] != 0xd3) {
		return nil, errors.New("header ExportedAt is not an 8-byte msgpack integer")
	}
	out := append([]byte(nil), b...)
	binary.BigEndian.PutUint64(out[offs[0]+i+4:], uint64(FixedExportedAt))
	return out, nil
}

// payloadOffsets returns the offset of every Marker occurrence inside a node,
// relationship or history record body.
func payloadOffsets(b []byte) []int {
	tags, offs, lens, err := records(b)
	if err != nil {
		return nil
	}
	var out []int
	for i, t := range tags {
		if t < 0x03 || t > 0x06 {
			continue
		}
		body := b[offs[i] : offs[i]+lens[i]]
		for from := 0; ; {
			j := bytes.Index(body[from:], []byte(Marker))
			if j < 0 {
				break
			}
			out = append(out, offs[i]+from+j)
			from += j + len(Marker)
		}
	}
	return out
}
