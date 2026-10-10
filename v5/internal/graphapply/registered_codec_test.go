package graphapply

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Copied from the independently derived framing-refinement fixture, not Go output.
// These bytes describe intent data; they supply no live Store or creation authority.
const registeredPreparedFixtureHex = "50494a3100010001000000000000000000000000000000000000000000000102" +
	"000000000000000000000000000000cbb22d33699f795ed00440b5ee0af950dc" +
	"852871231394fc9338d89693abf60f0001010300000000000000000000000000" +
	"000001358026a048d72e4de703da156369117d06cfa1877da82291f3d23416af" +
	"d9626d0000007a424453310001000000004c4447503100010000010a00000000" +
	"00000000000000000000000100010000000c63616d6572612d636c6f636b0000" +
	"000b6d696c6c697365636f6e640a000000000000000000000000000000000000" +
	"0104000000000000000000000000000000000000000000000100000000000000" +
	"4000"

func registeredFixtureHex(t *testing.T, value string) []byte {
	t.Helper()
	b, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal("independent fixture hex:", err)
	}
	return b
}

func registeredAssertReadRefusal(t *testing.T, wire []byte, limits registeredFormatBudget, want error) {
	t.Helper()
	if err := validatePreparedV1(wire, limits); !errors.Is(err, want) {
		t.Fatal("validation refusal cause differs", err)
	}
	o, k, s, err := decodePreparedBootstrap(wire, limits)
	if !errors.Is(err, want) || o != (registeredOriginScope{}) || k != ([16]byte{}) || !reflect.DeepEqual(s, registeredBootstrapSpec{}) {
		t.Fatal("decode refusal cause/zero output differs", err)
	}
	projection, err := submissionProjectionV1(wire, limits)
	if !errors.Is(err, want) || len(projection) != 0 {
		t.Fatal("projection refusal cause/zero output differs", err)
	}
	sh, ph, err := hashPreparedV1(wire, limits)
	if !errors.Is(err, want) || sh != ([32]byte{}) || ph != ([32]byte{}) {
		t.Fatal("hash refusal cause/zero output differs", err)
	}
}

func TestRegisteredReadBudgetAndSizePriority(t *testing.T) {
	_, _, _, limits := registeredCodecFixture(t)
	wire := registeredFixtureHex(t, registeredPreparedFixtureHex)
	bad := limits
	bad.walkBytes = 0
	registeredAssertReadRefusal(t, wire, bad, errInvalid)
	short := limits
	short.preparedBytes = 257
	registeredAssertReadRefusal(t, wire, short, errLimit)
	exhausted := limits
	exhausted.walkBytes = 1
	registeredAssertReadRefusal(t, wire, exhausted, errLimit)
	registeredAssertReadRefusal(t, nil, exhausted, errInvalid)
	registeredAssertReadRefusal(t, wire[:135], exhausted, errInvalid)
}

func TestRegisteredTypedAxisRefusalsAndWorkExhaustion(t *testing.T) {
	for name, change := range map[string]func(*registeredBootstrapSpec){
		"blank-reference":        func(s *registeredBootstrapSpec) { s.domain.axes[0].Reference = " " },
		"blank-unit":             func(s *registeredBootstrapSpec) { s.domain.axes[0].CanonicalUnit = " " },
		"invalid-reference-UTF8": func(s *registeredBootstrapSpec) { s.domain.axes[0].Reference = "\xff" },
		"invalid-unit-UTF8":      func(s *registeredBootstrapSpec) { s.domain.axes[0].CanonicalUnit = "\xff" },
		"zero-axis-ID":           func(s *registeredBootstrapSpec) { s.domain.axes[0].ID = temporal.AxisID{} },
		"unknown-axis-version":   func(s *registeredBootstrapSpec) { s.domain.axes[0].Version = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			o, k, s, limits := registeredCodecFixture(t)
			change(&s)
			b, err := encodePreparedBootstrap(o, k, s, limits)
			if !errors.Is(err, errInvalid) || len(b) != 0 {
				t.Fatal("typed axis refusal must preserve cause/zero bytes", err)
			}
		})
	}
	o, k, s, limits := registeredCodecFixture(t)
	// Metadata2048 + structural258 + origin86 =2392; axis text/hash112 =>2504.
	limits.walkBytes = 2503
	b, err := encodePreparedBootstrap(o, k, s, limits)
	if !errors.Is(err, errLimit) || len(b) != 0 {
		t.Fatal("typed axis work exhaustion must return zero bytes", err)
	}
}

func TestRegisteredBorrowedDefaultRescanRefusals(t *testing.T) {
	_, _, _, limits := registeredCodecFixture(t)
	wire := registeredFixtureHex(t, registeredLiterals(t)["two-axes-default11"].Prepared)
	// Independent wire: n300; before rescan structural264, text76, hashes124,
	// origin86, canonical comparison32 + metadata2048 =2630.
	// First traversal50 ->2680; ID comparison32 ->2712; second traversal42
	// ->2754; ID comparison32 ->2786; selected hash58 ->2844.
	for _, cap := range []int{2650, 2690, 2843} {
		budget := limits
		budget.walkBytes = cap
		registeredAssertReadRefusal(t, wire, budget, errLimit)
	}
	for _, defaultID := range []byte{12, 0} {
		malformed := bytes.Clone(wire)
		malformed[247] = defaultID // Independent two-axis default spans [247:263].
		registeredAssertReadRefusal(t, malformed, limits, errInvalid)
	}
}

func TestRegisteredWireOrderingAndDuplicatesRefuseWithoutDecodedData(t *testing.T) {
	rows := registeredLiterals(t)
	_, _, _, limits := registeredCodecFixture(t)
	for _, name := range []string{"two-axes-default10", "node-and-relationship-schema"} {
		literal := registeredFixtureHex(t, rows[name].Prepared)
		for _, mutation := range []string{"swapped", "duplicate"} {
			t.Run(name+"/"+mutation, func(t *testing.T) {
				wire := bytes.Clone(literal)
				if name == "two-axes-default10" {
					// Independent fixture: descriptors [155:205] and [205:247].
					if mutation == "swapped" {
						swapped := append(bytes.Clone(literal[205:247]), literal[155:205]...)
						copy(wire[155:247], swapped)
					} else {
						copy(wire[205:221], literal[155:171]) // Rebound duplicate AxisID.
					}
				} else {
					// Independent fixture: owner-qualified records [224:236], [236:248].
					if mutation == "swapped" {
						copy(wire[224:236], literal[236:248])
						copy(wire[236:248], literal[224:236])
					} else {
						copy(wire[236:248], literal[224:236])
					}
				}
				if err := validatePreparedV1(wire, limits); !errors.Is(err, errInvalid) {
					t.Fatal("noncanonical ordering/duplicate accepted", err)
				}
				o, k, s, err := decodePreparedBootstrap(wire, limits)
				if !errors.Is(err, errInvalid) || o != (registeredOriginScope{}) || k != ([16]byte{}) || !reflect.DeepEqual(s, registeredBootstrapSpec{}) {
					t.Fatal("noncanonical wire returned decoded data", err)
				}
			})
		}
	}
}

func TestRegisteredDirectOperationLedgerBoundaries(t *testing.T) {
	_, _, _, limits := registeredCodecFixture(t)
	wire := registeredFixtureHex(t, registeredPreparedFixtureHex)
	operations := []struct {
		name  string
		exact int
		run   func(*testing.T, registeredFormatBudget) error
	}{
		{"validation", 2504, func(t *testing.T, l registeredFormatBudget) error { return validatePreparedV1(wire, l) }},
		{"decode", 2872, func(t *testing.T, l registeredFormatBudget) error {
			o, k, s, err := decodePreparedBootstrap(wire, l)
			if err != nil && (o != (registeredOriginScope{}) || k != ([16]byte{}) || !reflect.DeepEqual(s, registeredBootstrapSpec{})) {
				t.Fatal("decode budget refusal returned partial owned data")
			}
			return err
		}},
		{"projection", 3020, func(t *testing.T, l registeredFormatBudget) error {
			b, err := submissionProjectionV1(wire, l)
			if err != nil && len(b) != 0 {
				t.Fatal("projection budget refusal returned partial owned bytes")
			}
			return err
		}},
		{"hashes", 3092, func(t *testing.T, l registeredFormatBudget) error {
			sh, ph, err := hashPreparedV1(wire, l)
			if err != nil && (sh != ([32]byte{}) || ph != ([32]byte{})) {
				t.Fatal("hash budget refusal returned partial digests")
			}
			return err
		}},
	}
	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			for _, delta := range []int{-1, 0} {
				budget := limits
				budget.walkBytes = op.exact + delta
				err := op.run(t, budget)
				if delta < 0 && !errors.Is(err, errLimit) || delta == 0 && err != nil {
					t.Fatalf("ledger exact=%d delta=%d: %v", op.exact, delta, err)
				}
			}
		})
	}
}

type registeredLiteral struct {
	Prepared       string `json:"prepared_hex"`
	Submission     string `json:"submission_hex"`
	PreparedHash   string `json:"prepared_hash"`
	SubmissionHash string `json:"submission_hash"`
	DefinitionHash string `json:"default_definition_hash"`
}

func registeredLiterals(t *testing.T) map[string]registeredLiteral {
	t.Helper()
	b, err := os.ReadFile("testdata/registered_bootstrap_fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows map[string]registeredLiteral
	if err := json.Unmarshal(b, &rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func registeredVariant(t *testing.T, name string, literal registeredLiteral) (registeredOriginScope, [16]byte, registeredBootstrapSpec, registeredFormatBudget) {
	t.Helper()
	o, k, s, l := registeredCodecFixture(t)
	switch name {
	case "allocator-owner5":
		s.allocator.owner = [16]byte{5}
	case "allocator-epoch2":
		s.allocator.epoch = 2
	case "allocator-max128":
		s.allocator.maxBlock = 128
	case "changed-reference":
		s.domain.axes[0].Reference = "alternate-clock"
	case "changed-unit":
		s.domain.axes[0].CanonicalUnit = "tick"
	case "rational-profile", "declared-POSIX-rational-ms":
		s.domain.axes[0].Profile = temporal.ProfileRationalQ
	case "lexicographic-profile":
		s.domain.axes[0].Profile = temporal.ProfileLexicographicQN
	case "two-axes-default10", "two-axes-default11":
		s.domain.axes = append(s.domain.axes, temporal.AxisDescriptor{ID: temporal.AxisID{11}, Profile: temporal.ProfileRationalQ, Version: 1, Reference: "other-clock", CanonicalUnit: "tick"})
		if name == "two-axes-default11" {
			s.domain.defaultValidityAxis = temporal.AxisID{11}
		}
		slices.Reverse(s.domain.axes)
	case "node-and-relationship-schema":
		s.schemas = []graphstate.PropertyDefinition{{Name: "name", Owner: graphstate.Relationship, Type: graphstate.ScalarString, Cardinality: graphstate.SetCardinality, Unique: graphstate.UniqueMembers}, {Name: "name", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}}
	case "sixty-five-schemas":
		for i := range 65 {
			s.schemas = append(s.schemas, graphstate.PropertyDefinition{Name: "p" + string([]byte{byte('0' + i/10), byte('0' + i%10)}), Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality})
		}
	case "base", "declared-POSIX-identity-ms":
	default:
		t.Fatalf("unrecognized independent fixture %q", name)
	}
	if strings.HasPrefix(name, "declared-POSIX") {
		s.kind = 2
		s.domain.mapping = registeredMappingSpec{tag: 1, version: 1, sourceReference: "POSIX/Unix/millisecond/1970-01-01T00:00:00Z", targetAxis: s.domain.defaultValidityAxis, rule: 1}
		copy(s.domain.mapping.targetDefinitionHash[:], registeredFixtureHex(t, literal.DefinitionHash))
	}
	return o, k, s, l
}

func TestRegisteredIndependentVariantsBindEveryBootstrapField(t *testing.T) {
	for name, literal := range registeredLiterals(t) {
		t.Run(name, func(t *testing.T) {
			o, k, s, l := registeredVariant(t, name, literal)
			for _, descriptor := range s.domain.axes {
				if descriptor.ID == s.domain.defaultValidityAxis {
					axis, err := temporal.NewAxis(descriptor, temporal.Limits{MaxDescriptorBytes: 4096})
					digest := axis.DefinitionHash()
					if err != nil || !bytes.Equal(digest[:], registeredFixtureHex(t, literal.DefinitionHash)) {
						t.Fatal("Axis.NewAxis differs from independent definition hash", err)
					}
				}
			}
			want := registeredFixtureHex(t, literal.Prepared)
			got, err := encodePreparedBootstrap(o, k, s, l)
			if err != nil || !bytes.Equal(got, want) || cap(got) != len(got) {
				t.Fatalf("complete independent PIJ differs: got=%x err=%v want=%x", got, err, want)
			}
			if err := validatePreparedV1(want, l); err != nil {
				t.Fatal("literal validation:", err)
			}
			projection, err := submissionProjectionV1(want, l)
			if err != nil || !bytes.Equal(projection, registeredFixtureHex(t, literal.Submission)) || cap(projection) != len(projection) {
				t.Fatal("complete SIJ differs", err)
			}
			sh, ph, err := hashPreparedV1(want, l)
			if err != nil || !bytes.Equal(sh[:], registeredFixtureHex(t, literal.SubmissionHash)) || !bytes.Equal(ph[:], registeredFixtureHex(t, literal.PreparedHash)) {
				t.Fatal("two independent hashes differ", err)
			}
			decodedOrigin, decodedKey, decodedSpec, err := decodePreparedBootstrap(want, l)
			if err != nil || decodedOrigin != o || decodedKey != k {
				t.Fatal("decoded complete origin/key differ", err)
			}
			clear(want) // All 14 decoded variants must own every input-backed field.
			again, err := encodePreparedBootstrap(decodedOrigin, decodedKey, decodedSpec, l)
			if err != nil || !bytes.Equal(again, registeredFixtureHex(t, literal.Prepared)) {
				t.Fatal("decoder dropped independent intent fields", err)
			}
		})
	}
}

func TestRegisteredRefusesMalformedWireWithoutPartialData(t *testing.T) {
	_, _, _, l := registeredCodecFixture(t)
	literal := registeredFixtureHex(t, registeredPreparedFixtureHex)
	for n := range len(literal) {
		if err := validatePreparedV1(literal[:n], l); !errors.Is(err, errInvalid) {
			t.Fatalf("truncated prefix %d accepted: %v", n, err)
		}
	}
	cases := map[string]func([]byte) []byte{
		"magic":                  func(b []byte) []byte { b[0] = 'X'; return b },
		"version":                func(b []byte) []byte { b[5] = 2; return b },
		"flags":                  func(b []byte) []byte { b[6] = 1; return b },
		"graph":                  func(b []byte) []byte { clear(b[7:23]); return b },
		"group-lineage":          func(b []byte) []byte { b[31] = 3; return b },
		"lineage":                func(b []byte) []byte { b[47] ^= 1; return b },
		"protocol":               func(b []byte) []byte { b[80] = 2; return b },
		"class":                  func(b []byte) []byte { b[81] = 2; return b },
		"key":                    func(b []byte) []byte { clear(b[82:98]); return b },
		"meaning":                func(b []byte) []byte { b[99] ^= 1; return b },
		"length":                 func(b []byte) []byte { b[134]--; return b },
		"body-version":           func(b []byte) []byte { b[140] = 2; return b },
		"body-flags":             func(b []byte) []byte { b[141] = 1; return b },
		"domain-count-expansion": func(b []byte) []byte { b[153] = 255; return b },
		"descriptor-length":      func(b []byte) []byte { b[174] = 255; return b },
		"utf8":                   func(b []byte) []byte { b[178] = 255; return b },
		"schema-count":           func(b []byte) []byte { b[222] = 255; return b },
		"allocator-arm":          func(b []byte) []byte { b[224] = 2; return b },
		"revision":               func(b []byte) []byte { b[257] = 1; return b },
		"trailing":               func(b []byte) []byte { return append(b, 0) },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			wire := change(bytes.Clone(literal))
			err := validatePreparedV1(wire, l)
			if !errors.Is(err, errInvalid) && !errors.Is(err, errLimit) {
				t.Fatal("malformed borrowed bytes accepted", err)
			}
			o, k, s, err := decodePreparedBootstrap(wire, l)
			if err == nil || o != (registeredOriginScope{}) || k != ([16]byte{}) || len(s.domain.axes)+len(s.schemas) != 0 {
				t.Fatal("partial decoded output", err)
			}
			b, err := submissionProjectionV1(wire, l)
			if err == nil || len(b) != 0 {
				t.Fatal("partial projection", err)
			}
			sh, ph, err := hashPreparedV1(wire, l)
			if err == nil || sh != ([32]byte{}) || ph != ([32]byte{}) {
				t.Fatal("partial hashes", err)
			}
		})
	}
}

func TestRegisteredMappingSchemaAndOriginRefusals(t *testing.T) {
	rows := registeredLiterals(t)
	for name, change := range map[string]func(*registeredBootstrapSpec){
		"wrong-source":     func(s *registeredBootstrapSpec) { s.domain.mapping.sourceReference = "Unix" },
		"wrong-rule":       func(s *registeredBootstrapSpec) { s.domain.mapping.rule = 2 },
		"wrong-version":    func(s *registeredBootstrapSpec) { s.domain.mapping.version = 2 },
		"wrong-target":     func(s *registeredBootstrapSpec) { s.domain.mapping.targetAxis = temporal.AxisID{9} },
		"wrong-definition": func(s *registeredBootstrapSpec) { s.domain.mapping.targetDefinitionHash[0] ^= 1 },
		"QN-mapping":       func(s *registeredBootstrapSpec) { s.domain.axes[0].Profile = temporal.ProfileLexicographicQN },
		"non-ms-mapping":   func(s *registeredBootstrapSpec) { s.domain.axes[0].CanonicalUnit = "tick" },
		"duplicate-schema": func(s *registeredBootstrapSpec) {
			s.schemas = []graphstate.PropertyDefinition{{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: 1}, {Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: 1}}
		},
		"invalid-uniqueness": func(s *registeredBootstrapSpec) {
			s.schemas = []graphstate.PropertyDefinition{{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarScope, Cardinality: 1, Unique: graphstate.UniqueMembers}}
		},
		"blank-schema": func(s *registeredBootstrapSpec) {
			s.schemas = []graphstate.PropertyDefinition{{Name: " ", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: 1}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			o, k, s, l := registeredVariant(t, "declared-POSIX-identity-ms", rows["declared-POSIX-identity-ms"])
			change(&s)
			b, err := encodePreparedBootstrap(o, k, s, l)
			if !errors.Is(err, errInvalid) || len(b) != 0 {
				t.Fatal("invalid mapping/schema accepted", err)
			}
		})
	}
	for _, change := range []func(*registeredOriginScope, *[16]byte){
		func(o *registeredOriginScope, k *[16]byte) { o.partition = 0 },
		func(o *registeredOriginScope, k *[16]byte) { o.graph = [16]byte{} },
		func(o *registeredOriginScope, k *[16]byte) { o.group = [16]byte{} },
		func(o *registeredOriginScope, k *[16]byte) { o.protocol = 2 },
		func(o *registeredOriginScope, k *[16]byte) { o.lineage = [32]byte{} },
		func(o *registeredOriginScope, k *[16]byte) { *k = [16]byte{} },
	} {
		o, k, s, l := registeredCodecFixture(t)
		change(&o, &k)
		b, err := encodePreparedBootstrap(o, k, s, l)
		if !errors.Is(err, errInvalid) || len(b) != 0 {
			t.Fatal("invalid origin/key accepted", err)
		}
	}
}

func TestRegisteredExactFormatBudgetsAndOwnedOutput(t *testing.T) {
	o, k, s, l := registeredCodecFixture(t)
	for _, change := range []func(*registeredFormatBudget, int){
		func(l *registeredFormatBudget, d int) { l.preparedBytes = 258 + d },
		func(l *registeredFormatBudget, d int) { l.domainBytes = 76 + d },
		func(l *registeredFormatBudget, d int) { l.descriptorBytes = 50 + d },
		func(l *registeredFormatBudget, d int) { l.walkBytes = 3022 + d },
	} {
		for _, delta := range []int{-1, 0, 1} {
			budget := l
			change(&budget, delta)
			b, err := encodePreparedBootstrap(o, k, s, budget)
			if delta < 0 {
				if !errors.Is(err, errLimit) || len(b) != 0 {
					t.Fatal("one-short cap", err)
				}
			} else if err != nil || len(b) != 258 {
				t.Fatal("exact/plus-one cap", err)
			}
		}
	}
	for _, change := range []func(*registeredFormatBudget){
		func(l *registeredFormatBudget) { l.preparedBytes = 0 },
		func(l *registeredFormatBudget) { l.domainBytes = 0 },
		func(l *registeredFormatBudget) { l.axes = 0 },
		func(l *registeredFormatBudget) { l.schemas = 0 },
		func(l *registeredFormatBudget) { l.descriptorBytes = 4097 },
		func(l *registeredFormatBudget) { l.walkBytes = (32 << 20) + 1 },
	} {
		budget := l
		change(&budget)
		b, err := encodePreparedBootstrap(o, k, s, budget)
		if !errors.Is(err, errInvalid) || len(b) != 0 {
			t.Fatal("malformed budget accepted", err)
		}
	}
	b, err := encodePreparedBootstrap(o, k, s, l)
	if err != nil {
		t.Fatal(err)
	}
	_, _, decoded, err := decodePreparedBootstrap(b, l)
	if err != nil {
		t.Fatal(err)
	}
	s.domain.axes[0].Reference = "changed"
	if !bytes.Equal(b, registeredFixtureHex(t, registeredPreparedFixtureHex)) {
		t.Fatal("encoded output changed after caller axis mutation")
	}
	b[178] = 'X'
	if decoded.domain.axes[0].Reference != "camera-clock" {
		t.Fatal("decoded strings alias caller bytes")
	}
	if bytes.Equal(b, registeredFixtureHex(t, registeredPreparedFixtureHex)) {
		t.Fatal("ownership probe did not mutate source")
	}
	if cap(b) != len(b) || cap(decoded.domain.axes) != len(decoded.domain.axes) {
		t.Fatal("spare output capacity")
	}
}

func registeredCodecFixture(t *testing.T) (registeredOriginScope, [16]byte, registeredBootstrapSpec, registeredFormatBudget) {
	t.Helper()
	origin := registeredOriginScope{graph: [16]byte{1}, partition: 1, group: [16]byte{2}, protocol: 1}
	copy(origin.lineage[:], registeredFixtureHex(t, "cbb22d33699f795ed00440b5ee0af950dc852871231394fc9338d89693abf60f"))
	spec := registeredBootstrapSpec{
		kind: 1, revisionTag: 0,
		domain: registeredDomainPolicy{
			version: 1, defaultValidityAxis: temporal.AxisID{10},
			axes: []temporal.AxisDescriptor{{
				ID: temporal.AxisID{10}, Profile: temporal.ProfileIntegerZ, Version: 1,
				Reference: "camera-clock", CanonicalUnit: "millisecond",
			}},
		},
		allocator: registeredAllocatorIntent{tag: 1, owner: [16]byte{4}, epoch: 1, maxBlock: 64},
	}
	budget := registeredFormatBudget{
		preparedBytes: 4 << 20, domainBytes: 4 << 20, axes: 1024, descriptorBytes: 4096,
		schemas: 4096, schemaNameBytes: 256, walkBytes: 32 << 20,
	}
	return origin, [16]byte{3}, spec, budget
}

func TestRegisteredBootstrapCompletePIJHasNoOmittedFields(t *testing.T) {
	origin, key, spec, budget := registeredCodecFixture(t)
	want := registeredFixtureHex(t, registeredPreparedFixtureHex)
	if len(want) != 258 {
		t.Fatalf("independent fixture length=%d, want 258", len(want))
	}
	meaning := registeredSemanticContractID()
	if !bytes.Equal(meaning[:], registeredFixtureHex(t, "358026a048d72e4de703da156369117d06cfa1877da82291f3d23416afd9626d")) {
		t.Fatal("compiled meaning differs from independent descriptor hash")
	}
	got, err := encodePreparedBootstrap(origin, key, spec, budget)
	if err != nil {
		if len(got) != 0 {
			t.Fatalf("failed encoder leaked %d bytes: %v", len(got), err)
		}
		t.Fatalf("valid explicit Bootstrap reached encoder: got %v and zero bytes; want full independent PIJ258", err)
	}
	if !bytes.Equal(got, want) || cap(got) != len(got) {
		t.Fatalf("complete domain/default/allocator PIJ differs: got=%x len=%d cap=%d want=%x", got, len(got), cap(got), want)
	}
}

func TestRegisteredBootstrapRejectsStrictUnionDomainDefaultAllocator(t *testing.T) {
	cases := []struct {
		name   string
		change func(*registeredBootstrapSpec)
	}{
		{"unknown-kind", func(s *registeredBootstrapSpec) { s.kind = 3 }},
		{"component-revision", func(s *registeredBootstrapSpec) { s.revisionTag = 1 }},
		{"unknown-allocator-arm", func(s *registeredBootstrapSpec) { s.allocator.tag = 2 }},
		{"native-mapping-arm", func(s *registeredBootstrapSpec) { s.domain.mapping.tag = 1 }},
		{"mapped-kind-without-mapping", func(s *registeredBootstrapSpec) { s.kind = 2 }},
		{"absent-mapping-has-tail", func(s *registeredBootstrapSpec) { s.domain.mapping.sourceReference = "ignored" }},
		{"unknown-domain-version", func(s *registeredBootstrapSpec) { s.domain.version = 2 }},
		{"reserved-domain-flags", func(s *registeredBootstrapSpec) { s.domain.flags = 1 }},
		{"missing-domain-axes", func(s *registeredBootstrapSpec) { s.domain.axes = nil }},
		{"missing-default", func(s *registeredBootstrapSpec) { s.domain.defaultValidityAxis = temporal.AxisID{} }},
		{"dangling-default", func(s *registeredBootstrapSpec) { s.domain.defaultValidityAxis = temporal.AxisID{99} }},
		{"unknown-native-profile", func(s *registeredBootstrapSpec) { s.domain.axes[0].Profile = 4 }},
		{"rebound-duplicate-axis", func(s *registeredBootstrapSpec) {
			other := s.domain.axes[0]
			other.Reference = "different-clock"
			s.domain.axes = append(s.domain.axes, other)
		}},
		{"missing-allocator-owner", func(s *registeredBootstrapSpec) { s.allocator.owner = [16]byte{} }},
		{"zero-allocator-epoch", func(s *registeredBootstrapSpec) { s.allocator.epoch = 0 }},
		{"zero-allocator-block", func(s *registeredBootstrapSpec) { s.allocator.maxBlock = 0 }},
		{"oversized-allocator-block", func(s *registeredBootstrapSpec) { s.allocator.maxBlock = (1 << 20) + 1 }},
		{"unknown-schema-owner", func(s *registeredBootstrapSpec) {
			s.schemas = []graphstate.PropertyDefinition{{Name: "p", Owner: 3, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			origin, key, spec, budget := registeredCodecFixture(t)
			tc.change(&spec)
			got, err := encodePreparedBootstrap(origin, key, spec, budget)
			if !errors.Is(err, errInvalid) || len(got) != 0 {
				t.Fatalf("invalid complete union must refuse with zero bytes: got=%x err=%v", got, err)
			}
		})
	}
}
