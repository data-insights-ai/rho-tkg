package core

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// BACKLOG 12d: verifyImportedNodeHash/verifyImportedRelHash previously
// EXEMPTED a row from verification whenever its integrity block was entirely
// absent or its Hash was empty ("Rows without integrity state are exempt").
// Every entity this library itself ever produces has a non-empty Hash (every
// create/import/replica-apply path always calls SetIntegrity with a freshly
// computed hash), so a row imported with NO hash at all is not a legitimate
// case — it is exactly what a forged or corrupted record looks like if
// whatever populated the integrity block (or an attacker) blanked the one
// field this check exists to verify. These tests had zero direct coverage
// before this fix — neither the "missing hash" case (the actual gap) nor the
// "wrong hash" case (the mechanism the exemption sat right next to).

func TestVerifyImportedNodeHash_MissingHashRejected(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)
	n := types.NewNode(types.NodeID(1), 1, nil)
	// No SetIntegrity call: n.Integrity() is nil, exactly the "blanked
	// integrity block" shape a forged/corrupted import record would have.
	err := c.verifyImportedNodeHash(n, 1, "node")
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("verifyImportedNodeHash(no integrity) = %v, want ErrCorruptExport", err)
	}
}

func TestVerifyImportedNodeHash_EmptyHashStringRejected(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)
	n := types.NewNode(types.NodeID(1), 1, nil)
	// Integrity block present but Hash explicitly blanked — a subtler forgery
	// than omitting the block entirely (e.g. other fields like AuthorID set).
	n.SetIntegrity(&types.NodeIntegrity{Hash: "", AuthorID: "someone"})
	err := c.verifyImportedNodeHash(n, 1, "node")
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("verifyImportedNodeHash(empty hash) = %v, want ErrCorruptExport", err)
	}
}

func TestVerifyImportedNodeHash_WrongHashRejected(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)
	n := types.NewNode(types.NodeID(1), 1, nil)
	n.SetIntegrity(&types.NodeIntegrity{Hash: "not-the-real-hash"})
	err := c.verifyImportedNodeHash(n, 1, "node")
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("verifyImportedNodeHash(wrong hash) = %v, want ErrCorruptExport", err)
	}
}

func TestVerifyImportedNodeHash_CorrectHashAccepted(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)
	tok, err := c.labels.GetOrCreate("Person")
	if err != nil {
		t.Fatalf("labels.GetOrCreate: %v", err)
	}
	n := types.NewNode(types.NodeID(1), tok, nil)
	hash, err := integrity.ComputeNodeHashChecked(n, []string{"Person"})
	if err != nil {
		t.Fatalf("ComputeNodeHashChecked: %v", err)
	}
	n.SetIntegrity(&types.NodeIntegrity{Hash: hash})
	if err := c.verifyImportedNodeHash(n, 1, "node"); err != nil {
		t.Fatalf("verifyImportedNodeHash(correct hash) = %v, want nil", err)
	}
}

func TestVerifyImportedRelHash_MissingHashRejected(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)
	r := types.NewRelationship(types.RelID(1), 1, types.NodeID(10), types.NodeID(20))
	err := c.verifyImportedRelHash(r, 1, "relationship")
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("verifyImportedRelHash(no integrity) = %v, want ErrCorruptExport", err)
	}
}

func TestVerifyImportedRelHash_EmptyHashStringRejected(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)
	r := types.NewRelationship(types.RelID(1), 1, types.NodeID(10), types.NodeID(20))
	r.SetIntegrity(&types.RelIntegrity{Hash: ""})
	err := c.verifyImportedRelHash(r, 1, "relationship")
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("verifyImportedRelHash(empty hash) = %v, want ErrCorruptExport", err)
	}
}

func TestVerifyImportedRelHash_WrongHashRejected(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)
	r := types.NewRelationship(types.RelID(1), 1, types.NodeID(10), types.NodeID(20))
	r.SetIntegrity(&types.RelIntegrity{Hash: "not-the-real-hash"})
	err := c.verifyImportedRelHash(r, 1, "relationship")
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("verifyImportedRelHash(wrong hash) = %v, want ErrCorruptExport", err)
	}
}

func TestVerifyImportedRelHash_CorrectHashAccepted(t *testing.T) {
	t.Parallel()
	c := newTestGraph(t)
	tok, err := c.relTypes.GetOrCreate("KNOWS")
	if err != nil {
		t.Fatalf("relTypes.GetOrCreate: %v", err)
	}
	r := types.NewRelationship(types.RelID(1), tok, types.NodeID(10), types.NodeID(20))
	hash, err := integrity.ComputeRelHashChecked(r, "KNOWS")
	if err != nil {
		t.Fatalf("ComputeRelHashChecked: %v", err)
	}
	r.SetIntegrity(&types.RelIntegrity{Hash: hash})
	if err := c.verifyImportedRelHash(r, 1, "relationship"); err != nil {
		t.Fatalf("verifyImportedRelHash(correct hash) = %v, want nil", err)
	}
}

// TestImport_RejectsNodeRecordWithBlankedIntegrityBlock is an end-to-end
// proof that a full g.IO.Import() stream containing a node record whose
// integrity block is entirely absent is rejected. NOTE: for the full-Import
// path specifically this is defense-in-depth-redundant with the pre-existing
// final whole-chain verify pass at the end of ImportWithOptions (import.go,
// "Final trust-boundary pass" — verifyNodeChainLocked already independently
// rejects any row with a missing/empty hash before the import can succeed,
// even with the BACKLOG 12d fix reverted; confirmed by stashing import.go's
// verifyImportedNodeHash change and re-running this test — it still passes).
// This test's genuine value is the unit-level TestVerifyImportedNodeHash_*
// tests above (confirmed RED via git-stash without the fix) plus an accurate
// end-to-end illustration that a full Import never persists such a row.
// The path where this fix IS the ONLY safety net — replica change-log apply,
// which has no whole-graph final verify pass — is covered by
// TestApplyChange_RejectsNodePutWithBlankedIntegrityBlock below.
func TestImport_RejectsNodeRecordWithBlankedIntegrityBlock(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	var stream bytes.Buffer
	writeImportMsgpackRecord(t, &stream, exportTagHeader, exportHeader{Version: exportFormatVersion, NodeCount: 1})
	writeImportMsgpackRecord(t, &stream, exportTagRegistry, tiered.RegistryFileData{
		Labels:   []string{"", "Person"},
		RelTypes: []string{"", "KNOWS"},
	})
	writeImportMsgpackRecord(t, &stream, exportTagNode, storeutil.NodeWire{
		ID:           100,
		PrimaryLabel: 1,
		Version:      0,
		// No Hash set: a forged or corrupted record.
	})

	err := g.IO.Import(bytes.NewReader(stream.Bytes()), tkgio.ImportOptions{})
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("Import(blanked integrity node) = %v, want ErrCorruptExport", err)
	}
	if _, err := g.store.GetNode(types.NodeID(100)); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("GetNode(100) after rejected import: got %v, want ErrNodeNotFound (nothing should have landed)", err)
	}
}

// TestImport_RejectsRelRecordWithBlankedIntegrityBlock mirrors the node
// case for relationships (rule 2).
func TestImport_RejectsRelRecordWithBlankedIntegrityBlock(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	ctx := context.Background()
	start, err := g.Nodes.Import(ctx, types.NodeID(100), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("seed start: %v", err)
	}
	end, err := g.Nodes.Import(ctx, types.NodeID(200), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("seed end: %v", err)
	}
	storedStart, err := g.store.GetNode(start.ID())
	if err != nil {
		t.Fatalf("GetNode(start): %v", err)
	}
	storedEnd, err := g.store.GetNode(end.ID())
	if err != nil {
		t.Fatalf("GetNode(end): %v", err)
	}

	var stream bytes.Buffer
	writeImportMsgpackRecord(t, &stream, exportTagHeader, exportHeader{Version: exportFormatVersion, NodeCount: 2, RelCount: 1})
	writeImportMsgpackRecord(t, &stream, exportTagRegistry, tiered.RegistryFileData{
		Labels:   []string{"", "Person"},
		RelTypes: []string{"", "KNOWS"},
	})
	writeImportMsgpackRecord(t, &stream, exportTagNode, storeutil.NodeToWire(storedStart))
	writeImportMsgpackRecord(t, &stream, exportTagNode, storeutil.NodeToWire(storedEnd))
	writeImportMsgpackRecord(t, &stream, exportTagRel, storeutil.RelWire{
		ID:      300,
		RelType: 1,
		StartID: int64(start.ID().SnowflakeID()),
		EndID:   int64(end.ID().SnowflakeID()),
		// No Hash set: a forged or corrupted record.
	})

	err = g.IO.Import(bytes.NewReader(stream.Bytes()), tkgio.ImportOptions{})
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("Import(blanked integrity rel) = %v, want ErrCorruptExport", err)
	}
	if _, err := g.store.GetRelationship(types.RelID(300)); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("GetRelationship(300) after rejected import: got %v, want ErrRelNotFound", err)
	}
}

// mustMarshalChangePayload msgpack-encodes a change-log record body for use
// as a hand-built storepkg.ChangeRecord.Payload in these tests. Production
// code never marshals directly (writers use the shared body builders and the
// backend's own WriteBatch), but a replica's ApplyChange is a pure decode
// door over exactly this wire shape, so encoding it directly here is the
// correct way to drive an adversarial (Tag, Payload) pair through it.
func mustMarshalChangePayload(t *testing.T, body any) []byte {
	t.Helper()
	b, err := msgpack.Marshal(body)
	if err != nil {
		t.Fatalf("msgpack.Marshal: %v", err)
	}
	return b
}

// TestApplyChange_RejectsNodePutWithBlankedIntegrityBlock is the genuinely
// load-bearing BACKLOG 12d proof: unlike g.IO.Import() (which has an
// independent final whole-chain verify pass as a safety net — see the
// comment on TestImport_RejectsNodeRecordWithBlankedIntegrityBlock above),
// the replica change-log apply path (g.Repl.ApplyChange, applyNodePutLocked)
// writes the decoded row straight to the store with NO subsequent chain
// verification of any kind. verifyImportedNodeHash is the ONLY check standing
// between a change-log record with a blanked integrity block and a silently
// unverified row landing on a replica. Confirmed load-bearing: stashing the
// BACKLOG 12d fix in import.go and re-running this test turns it RED (the
// row is accepted and persisted).
func TestApplyChange_RejectsNodePutWithBlankedIntegrityBlock(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	if _, err := g.labels.GetOrCreate("Person"); err != nil {
		t.Fatalf("labels.GetOrCreate: %v", err)
	}

	body := storeutil.NodePutBody{
		Wire: storeutil.NodeWire{
			ID:           100,
			PrimaryLabel: 1,
			Version:      0,
			// No Hash set: a forged or corrupted change-log record.
		},
	}
	rec := storepkg.ChangeRecord{
		LSN:     1,
		Tag:     storepkg.ChangeNodePut,
		Payload: mustMarshalChangePayload(t, body),
	}

	err := g.Repl.ApplyChange(rec)
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("ApplyChange(blanked integrity node put) = %v, want ErrCorruptExport", err)
	}
	if _, err := g.store.GetNode(types.NodeID(100)); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("GetNode(100) after rejected apply: got %v, want ErrNodeNotFound (nothing should have landed)", err)
	}
}

// TestApplyChange_RejectsRelPutWithBlankedIntegrityBlock mirrors the node
// case for relationships (rule 2) on the same non-redundant replica-apply
// path.
func TestApplyChange_RejectsRelPutWithBlankedIntegrityBlock(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	ctx := context.Background()
	start, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("seed start: %v", err)
	}
	end, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("seed end: %v", err)
	}
	if _, err := g.relTypes.GetOrCreate("KNOWS"); err != nil {
		t.Fatalf("relTypes.GetOrCreate: %v", err)
	}

	body := storeutil.RelPutBody{
		Wire: storeutil.RelWire{
			ID:      300,
			RelType: 1,
			StartID: int64(start.ID().SnowflakeID()),
			EndID:   int64(end.ID().SnowflakeID()),
			// No Hash set: a forged or corrupted change-log record.
		},
	}
	rec := storepkg.ChangeRecord{
		LSN:     2,
		Tag:     storepkg.ChangeRelPut,
		Payload: mustMarshalChangePayload(t, body),
	}

	err = g.Repl.ApplyChange(rec)
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("ApplyChange(blanked integrity rel put) = %v, want ErrCorruptExport", err)
	}
	if _, err := g.store.GetRelationship(types.RelID(300)); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("GetRelationship(300) after rejected apply: got %v, want ErrRelNotFound", err)
	}
}
