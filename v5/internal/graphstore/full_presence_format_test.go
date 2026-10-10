package graphstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"testing"
)

func TestFullPresenceFreshFormat3HasFifthRealRoot(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	topology, err := f.root.SinglePartition()
	if err != nil || topology.IndexVersion != 3 || f.root.NextPhysicalID() != 6 {
		t.Fatalf("fresh Full needs format3 and five real roots: topology=%+v next=%d error=%v", topology, f.root.NextPhysicalID(), err)
	}
}

func TestFullPhysicalFormat2And3RootAndDescriptorGoldens(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	c := f.catalog(t, f.index)
	defer c.view.Close()
	q, err := c.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	descriptor, found, err := q.fullDescriptor(c.root)
	if err != nil || !found {
		t.Fatal(err)
	}
	type golden struct{ name, root, descriptor string }
	expected := []golden{{"archived-format2", "4752020201000000000000000000000000000000000000000000000700000000000000030000000000000001000000000000000512a0f65cb25738c3251f2ddfab7129fb80de0f7f05e3e105ccac2f2b71076e9d0000000000000001000000000000000100000000000000022520f702574a8ce9a2507e58dc1e0aca815a40434f8c84f8849cc249f918f2e9", "47430111010000000000000000000000000000000000000000000007020200000000000000000003000000000000000100000000000000010000000000000002100000000000000000000000000000010000000000000000120000000000000000000000000000020000000000000000130000000000000000000000000000030000000000000000140000000000000000000000000000040000000000000000"}, {"fresh-format3", "4752020201000000000000000000000000000000000000000000000700000000000000030000000000000001000000000000000612a0f65cb25738c3251f2ddfab7129fb80de0f7f05e3e105ccac2f2b71076e9d0000000000000001000000000000000100000000000000030e1ae71709b75ec47589ac7742e0d5f0096720f2b54a68fe65a179ebbaa26ce2", "47430111010000000000000000000000000000000000000000000007020300000000000000000003000000000000000100000000000000010000000000000003100000000000000000000000000000010000000000000000120000000000000000000000000000020000000000000000130000000000000000000000000000030000000000000000140000000000000000000000000000040000000000000000150000000000000000000000000000050000000000000000fe9769a55c1b93a5e18ecfbac0ee7422f7d190955df98216c20299d753772d81"}}
	var actual []golden
	for _, format := range []uint64{2, 3} {
		root, d := f.root, descriptor
		if format == 2 {
			root.topology = legacyFullTopology
			root.next = 5
			d.format = 2
			d.own = currentPresenceTreeRoot{}
		}
		image, err := EncodeRoot(root)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := encodeFullDescriptor(d, root, c.limits)
		if err != nil {
			t.Fatal(err)
		}
		actual = append(actual, golden{expected[len(actual)].name, hex.EncodeToString(image), hex.EncodeToString(wire)})
		decoded, err := DecodeRoot(image)
		if err != nil || decoded != root {
			t.Fatal("physical root changed on decode", format, err)
		}
		imageAgain, err := EncodeRoot(decoded)
		if err != nil || !bytes.Equal(image, imageAgain) {
			t.Fatal("physical reencode changed", format, err)
		}
		if root.EffectDigest() != f.root.EffectDigest() || root.SemanticEpoch() != f.root.SemanticEpoch() {
			t.Fatal("physical format changed logical root")
		}
	}
	for _, golden := range actual {
		t.Logf("PHYSICAL_GOLDEN %s root=%s descriptor=%s", golden.name, golden.root, golden.descriptor)
	}
	for i, got := range actual {
		if got != expected[i] {
			t.Fatal("physical golden changed", got.name)
		}
	}
	unknown, _ := hex.DecodeString(actual[1].root)
	binary.BigEndian.PutUint64(unknown[100:108], 4)
	sum := sha256.Sum256(unknown[:108])
	copy(unknown[108:], sum[:])
	if _, err := DecodeRoot(unknown); !errors.Is(err, ErrCorrupt) {
		t.Fatal("unknown physical capability accepted", err)
	}
}
