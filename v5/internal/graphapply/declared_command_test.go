package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"unsafe"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
)

func declaredTestDeclaration(t *testing.T, partitions []graphstore.PartitionOwnership) graphstore.OwnershipDeclaration {
	t.Helper()
	d, err := graphstore.NewOwnershipDeclaration(graphstate.GraphID{1}, 9, partitions, graphstore.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestDeclaredGenesisConfigurationIndependentGoldenAndFixedBacking(t *testing.T) {
	c := genesisAllocationConfig{graph: idalloc.GraphID{1}, topology: 9, declaration: [32]byte{2}, home: 3, maxBlock: 16, schemas: [32]byte{4}}
	// Independent Python struct.pack/SHA256 vector, not candidate re-encoding.
	const golden = "47414301010000000000000000000000000000000000000000000009020000000000000000000000000000000000000000000000000000000000000000000000000000030000000000000010040000000000000000000000000000000000000000000000000000000000000018c54e5df80a37a8b44682f8e84c874226595d04b83a5bac1e67e7ea5982908b"
	wire, err := encodeGenesisAllocationConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) != genesisConfigBytes || cap(wire) != len(wire) || hex.EncodeToString(wire) != golden {
		t.Fatalf("wire/capacity changed: %x len/cap %d/%d", wire, len(wire), cap(wire))
	}
	got, err := decodeGenesisAllocationConfig(wire, c.graph)
	if err != nil || got != c {
		t.Fatalf("decode: %+v %v", got, err)
	}
	hash, err := c.digest()
	if err != nil || hex.EncodeToString(hash[:]) != "9e5c95eb4f37c11e51244e028d0e452ff40112214d22f1e380d88071ab8362f1" {
		t.Fatalf("digest: %x %v", hash, err)
	}
	wire[20] ^= 1
	if got != c {
		t.Fatal("decoded configuration aliases input")
	}
	// The complete binding increases the in-memory type, not GAC1 wire bytes.
	// Two fixed config values plus64 codec-header bytes fit the512 metadata;
	// descriptor strings/hash/encoder backing are separately priced by8*wire.
	if 2*unsafe.Sizeof(c)+64 > genesisCodecMetadataBytes {
		t.Fatalf("fixed configuration allowance insufficient: %d", unsafe.Sizeof(c))
	}
}

func TestDeclaredGenesisConfigurationChosenOnceAndQualified(t *testing.T) {
	d := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 8, OwnershipEpoch: 2, Group: [16]byte{8}}, {Partition: 3, OwnershipEpoch: 1, Group: [16]byte{3}}})
	l := defaultMaterializerLimits()
	c, err := newGenesisAllocationConfig(d, nil, 16, l)
	if err != nil || c.home != 3 || c.graph != (idalloc.GraphID{1}) || c.topology != 9 || c.declaration != d.Digest() {
		t.Fatalf("genesis selection: %+v %v", c, err)
	}
	if hex.EncodeToString(c.schemas[:]) != "51dc3628a31232f5870ac0036770b0e1185e30e89c3a7640fbb973f67ff03b1e" {
		t.Fatalf("empty schema vector: %x", c.schemas)
	}
	if err := c.checkGenesisDeclaration(d); err != nil {
		t.Fatal(err)
	}
	added := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 1, OwnershipEpoch: 1, Group: [16]byte{1}}, {Partition: 3, OwnershipEpoch: 1, Group: [16]byte{3}}, {Partition: 8, OwnershipEpoch: 2, Group: [16]byte{8}}})
	if err := c.checkGenesisDeclaration(added); !errors.Is(err, errInvalid) {
		t.Fatalf("unsupported topology/home change: %v", err)
	}
	if c.home != 3 || c.declaration != d.Digest() {
		t.Fatal("validation silently selected new allocation home")
	}
	wire, err := encodeGenesisAllocationConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(wire)
	if _, err := decodeGenesisAllocationConfig(wire, idalloc.GraphID{2}); !errors.Is(err, errCorrupt) {
		t.Fatalf("wrong graph: %v", err)
	}
	if !bytes.Equal(before, wire) {
		t.Fatal("refusal mutated configuration")
	}
	if got, err := sameGenesisConfiguration(wire, before, c.graph); err != nil || got != c {
		t.Fatalf("matching configuration: %+v %v", got, err)
	}
	changed := c
	changed.maxBlock++
	other, err := encodeGenesisAllocationConfig(changed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sameGenesisConfiguration(wire, other, c.graph); !errors.Is(err, errInvalid) || !errors.Is(err, idalloc.ErrPayloadMismatch) {
		t.Fatalf("different configuration: %v", err)
	}
}

func TestDeclaredGenesisConfigurationRejectsRechecksummedHostileShapes(t *testing.T) {
	c := genesisAllocationConfig{graph: idalloc.GraphID{1}, topology: 9, declaration: [32]byte{2}, home: 3, maxBlock: 16, schemas: [32]byte{4}}
	wire, err := encodeGenesisAllocationConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		lo, hi int
	}{{"graph", 4, 20}, {"topology", 20, 28}, {"declaration", 28, 60}, {"home", 60, 68}, {"max-block", 68, 76}, {"schemas", 76, 108}} {
		t.Run(tc.name, func(t *testing.T) {
			mutant := bytes.Clone(wire)
			clear(mutant[tc.lo:tc.hi])
			hash := sha256.Sum256(mutant[:108])
			copy(mutant[108:], hash[:])
			if _, err := decodeGenesisAllocationConfig(mutant, c.graph); !errors.Is(err, errCorrupt) {
				t.Fatalf("rechecksummed shape: %v", err)
			}
		})
	}
	for _, b := range [][]byte{nil, wire[:len(wire)-1], append(bytes.Clone(wire), 0)} {
		if _, err := decodeGenesisAllocationConfig(b, c.graph); !errors.Is(err, errCorrupt) {
			t.Fatalf("framing: %v", err)
		}
	}
	zero := genesisAllocationConfig{}
	if _, err := encodeGenesisAllocationConfig(zero); !errors.Is(err, errInvalid) {
		t.Fatalf("zero encode: %v", err)
	}
	if _, err := zero.digest(); !errors.Is(err, errInvalid) {
		t.Fatalf("zero digest: %v", err)
	}
}

func TestDeclaredGenesisConfigurationInputAndRepresentationBoundaries(t *testing.T) {
	d := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 1, Group: [16]byte{3}}})
	l := defaultMaterializerLimits()
	for _, block := range []uint64{0, idalloc.MaxBlockSize + 1} {
		if _, err := newGenesisAllocationConfig(d, nil, block, l); !errors.Is(err, errInvalid) {
			t.Fatalf("block %d: %v", block, err)
		}
	}
	if _, err := newGenesisAllocationConfig(graphstore.OwnershipDeclaration{}, nil, 1, l); !errors.Is(err, errInvalid) {
		t.Fatalf("zero declaration: %v", err)
	}
	bad := l
	bad.sourceRows = 0
	if _, err := newGenesisAllocationConfig(d, nil, 1, bad); !errors.Is(err, errInvalid) {
		t.Fatalf("invalid policy: %v", err)
	}
	definition := graphstate.PropertyDefinition{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}
	for _, tc := range []struct {
		name        string
		definitions []graphstate.PropertyDefinition
		policy      materializerLimits
		want        error
	}{
		{"bad shape", []graphstate.PropertyDefinition{{Name: "p"}}, l, errInvalid},
		{"duplicate", []graphstate.PropertyDefinition{definition, definition}, l, errInvalid},
		{"count", []graphstate.PropertyDefinition{definition}, func() materializerLimits { x := l; x.maxSchemas = 1; return x }(), nil},
		{"count over", []graphstate.PropertyDefinition{definition, definition}, func() materializerLimits { x := l; x.maxSchemas = 1; return x }(), errLimit},
		{"name over", []graphstate.PropertyDefinition{func() graphstate.PropertyDefinition { x := definition; x.Name = "pp"; return x }()}, func() materializerLimits { x := l; x.catalog.MaxNameBytes = 1; return x }(), errLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newGenesisAllocationConfig(d, tc.definitions, 1, tc.policy)
			if tc.want == nil {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
	minimumWire := len("rho-tkg:genesis-schemas:v1\x00") + 16 + 4 + sha256.Size
	for _, dimension := range []string{"wire", "owned"} {
		for _, delta := range []int{-1, 0, 1} {
			x := l
			if dimension == "wire" {
				x.commandBytes = minimumWire + delta
			} else {
				x.commandOwnedBytes = genesisCodecMetadataBytes + 2*minimumWire + delta
			}
			_, err := newGenesisAllocationConfig(d, nil, 1, x)
			if delta < 0 {
				if !errors.Is(err, errLimit) {
					t.Fatalf("%s/%d: %v", dimension, delta, err)
				}
			} else if err != nil {
				t.Fatalf("%s/%d: %v", dimension, delta, err)
			}
		}
	}
	// Independent fixed-live-set check; this allowance is not a heap/RSS bound.
	fixed := unsafe.Sizeof(genesisAllocationConfig{}) + 2*unsafe.Sizeof(boundedWriter{}) + unsafe.Sizeof(graphCursor{}) + 2*unsafe.Sizeof([32]byte{}) + unsafe.Sizeof([]byte{})
	if fixed > genesisCodecMetadataBytes {
		t.Fatalf("codec fixed allowance: %d", fixed)
	}
}

func TestDeclaredInitCodecAuthorityAndSourceObservationVariants(t *testing.T) {
	d := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{4}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{4}}})
	l := defaultMaterializerLimits()
	a, err := idalloc.NewAuthority([16]byte{7}, 2)
	if err != nil {
		t.Fatal(err)
	}
	schemas := []graphstate.PropertyDefinition{{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}}
	cfg, err := newGenesisAllocationConfig(d, schemas, 16, l)
	if err != nil {
		t.Fatal(err)
	}
	// This is codec inspection data, not an allocationProof or quorum receipt.
	g := genesisObservation{source: allocationCoordinate{scope: allocationScope{graph: cfg.graph, partition: cfg.home, group: [16]byte{4}, ownership: 2, topology: 9, declaration: d.Digest(), semantic: declaredSemanticContractID()}, index: 17}, configuration: cfg, epoch: 1, effect: [32]byte{9}}
	observation, err := encodeGenesisObservation(g)
	if err != nil || len(observation) != 4+120+8+140+8+32+32 || cap(observation) != len(observation) {
		t.Fatalf("observation geometry: %d/%d %v", len(observation), cap(observation), err)
	}
	decoded, err := decodeGenesisObservation(observation)
	if err != nil || decoded != g {
		t.Fatalf("observation round trip: %+v %v", decoded, err)
	}
	for _, r := range []declaredInitCommand{
		{ns: namespace{graph: cfg.graph, partition: cfg.home}, attempt: bootstrapAttemptID{1}, declaration: d, authority: a, maxBlock: 16, schemas: schemas},
		{ns: namespace{graph: cfg.graph, partition: 8}, attempt: bootstrapAttemptID{2}, declaration: d, maxBlock: 16, schemas: schemas, genesis: g},
	} {
		wire, err := encodeDeclaredInit(r, l)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(wire[:5], []byte{'G', 'R', 'Q', 4, 8}) || cap(wire) != len(wire) {
			t.Fatalf("strict framing/capacity: %x %d/%d", wire[:5], len(wire), cap(wire))
		}
		got, err := decodeDeclaredInit(wire, l)
		if err != nil || got.ns != r.ns || got.attempt != r.attempt || got.authority != r.authority || got.maxBlock != r.maxBlock || got.declaration.Digest() != d.Digest() || got.genesis != r.genesis || len(got.schemas) != 1 || got.schemas[0] != schemas[0] {
			t.Fatalf("initialization round trip: %+v %v", got, err)
		}
		if _, err := decodeGraphRequest(wire, l); !errors.Is(err, errCorrupt) {
			t.Fatalf("legacy GRQ2/3 door accepted GRQ4: %v", err)
		}
		wire[len(wire)-1] ^= 1
		if got.schemas[0] != schemas[0] || got.declaration.Digest() != d.Digest() {
			t.Fatal("decoded input aliases caller wire")
		}
		if _, err := decodeDeclaredInit(wire, l); !errors.Is(err, errCorrupt) {
			t.Fatalf("damaged command: %v", err)
		}
	}
	// A home's schema/configuration must match before its initialization command
	// can be produced; successful activation later cannot repair divergent genesis.
	wrong := declaredInitCommand{ns: namespace{graph: cfg.graph, partition: 8}, attempt: bootstrapAttemptID{3}, declaration: d, maxBlock: 16, genesis: g}
	if _, err := encodeDeclaredInit(wrong, l); !errors.Is(err, errInvalid) || !errors.Is(err, idalloc.ErrPayloadMismatch) {
		t.Fatalf("divergent home schemas: %v", err)
	}
	wrong.schemas = schemas
	wrong.authority = a
	if _, err := encodeDeclaredInit(wrong, l); !errors.Is(err, errInvalid) {
		t.Fatalf("home supplied independent allocator authority: %v", err)
	}
	wrong.authority = idalloc.Authority{}
	wrong.genesis = genesisObservation{}
	if _, err := encodeDeclaredInit(wrong, l); !errors.Is(err, errInvalid) {
		t.Fatalf("home omitted source observation: %v", err)
	}
	if unsafe.Sizeof(declaredInitCommand{})+unsafe.Sizeof(genesisAllocationConfig{})+2*unsafe.Sizeof(boundedWriter{})+unsafe.Sizeof(graphCursor{}) > declaredInitMetadataBytes {
		t.Fatal("initialization fixed representation allowance is insufficient")
	}
}

func TestDeclaredInitHostAdmissionUsesReplicaDecoderHeadroom(t *testing.T) {
	d := declaredTestDeclaration(t, []graphstore.PartitionOwnership{{Partition: 3, OwnershipEpoch: 1, Group: [16]byte{3}}})
	a, err := idalloc.NewAuthority([16]byte{7}, 1)
	if err != nil {
		t.Fatal(err)
	}
	r := declaredInitCommand{ns: namespace{graph: idalloc.GraphID{1}, partition: 3}, attempt: bootstrapAttemptID{1}, declaration: d, authority: a, maxBlock: 16}
	l := defaultMaterializerLimits()
	wire, err := encodeDeclaredInit(r, l)
	if err != nil {
		t.Fatal(err)
	}
	// Independently use the documented decoder ledger. An encoder-only 2x
	// allowance used to admit commands that this same-policy decoder refused.
	required := 1088 + 8*len(wire) + 64*d.Len()
	for _, dimension := range []string{"owned", "wire"} {
		for _, delta := range []int{-1, 0, 1} {
			t.Run(dimension+string(rune('b'+delta)), func(t *testing.T) {
				x := l
				if dimension == "owned" {
					x.commandOwnedBytes = required + delta
				} else {
					x.commandBytes = len(wire) + delta
				}
				encoded, err := encodeDeclaredInit(r, x)
				if delta < 0 {
					if !errors.Is(err, errLimit) || encoded != nil {
						t.Fatalf("host must refuse without a proposal: %x %v", encoded, err)
					}
					if allocations := testing.AllocsPerRun(10, func() { _, _ = encodeDeclaredInit(r, x) }); allocations != 0 {
						t.Fatalf("underfunded host input allocated: %g", allocations)
					}
					return
				}
				if err != nil || !bytes.Equal(encoded, wire) {
					t.Fatalf("host admission: %x %v", encoded, err)
				}
				decoded, err := decodeDeclaredInit(encoded, x)
				if err != nil || decoded.ns != r.ns || decoded.attempt != r.attempt || decoded.authority != a || decoded.declaration.Digest() != d.Digest() {
					t.Fatalf("host-admitted command refused by replica: %+v %v", decoded, err)
				}
			})
		}
	}
}
