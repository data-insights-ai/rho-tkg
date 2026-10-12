package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const graphE17NativeVoterRows = 64
const graphE17NativeVoterBytes = 64 << 10
const graphE17NativeVoterFrameBytes = (128 << 10) + 2048
const graphE17NativeVoterPages = 128
const graphE17NativeVoterTotalRows = graphE17NativeVoterRows * graphE17NativeVoterPages
const graphE17NativeVoterTotalBytes = 64 << 20

type graphE17NativeVoterReply struct {
	root    raftlog.ApplicationRoot
	binding raftlog.ApplicationBinding
	page    raftlog.ApplicationPage
}

func encodeGraphE17NativeVoterRequest(cert graphCertificate, after []byte) ([]byte, error) {
	if len(after) > graphE17NativeVoterBytes {
		return nil, errLimit
	}
	wire, err := encodeGraphCertificate(cert)
	if err != nil {
		return nil, err
	}
	return boundedEncoding(graphE17NativeVoterBytes+1024, func(w *boundedWriter) {
		w.add([]byte("GVP1"))
		w.field(wire)
		w.field(after)
	})
}

func decodeGraphE17NativeVoterRequest(wire []byte) (graphCertificate, []byte, error) {
	body, err := graphBody(wire, "GVP1", graphE17NativeVoterBytes+1024)
	if err != nil {
		return graphCertificate{}, nil, err
	}
	c := graphCursor{b: body}
	certificate, after := c.field(512), c.field(graphE17NativeVoterBytes)
	if c.err != nil || len(c.b) != 0 {
		return graphCertificate{}, nil, errors.Join(errCorrupt, c.err)
	}
	cert, err := decodeGraphCertificate(certificate)
	if err != nil {
		return graphCertificate{}, nil, err
	}
	return cert, after, nil
}

func graphE17NativeVoterPageValid(page raftlog.ApplicationPage, after []byte) bool {
	if page.Visited < 0 || page.Visited > graphE17NativeVoterRows || page.Bytes < 0 || page.Bytes > graphE17NativeVoterBytes || len(page.Rows) > page.Visited || len(page.Next) > graphE17NativeVoterBytes {
		return false
	}
	if page.Complete {
		if len(page.Next) != 0 {
			return false
		}
	} else if page.Visited == 0 || len(page.Next) == 0 || bytes.Compare(page.Next, after) <= 0 {
		return false
	}
	cost := 64*len(page.Rows) + len(page.Next)
	previous := after
	for _, row := range page.Rows {
		if row.Deleted || len(row.Key) == 0 || bytes.Compare(row.Key, previous) <= 0 {
			return false
		}
		cost += len(row.Key) + len(row.Value)
		previous = row.Key
	}
	return cost <= page.Bytes && (page.Complete || len(page.Rows) == 0 || bytes.Compare(page.Next, previous) >= 0)
}

// Each child independently checks its persisted certificate through the actual
// Host. AtGraphCertificate closes its current validation owner and returns an
// owned view at cert.sourceIndex; this helper closes that source on every path.
// These returned bytes are diagnostics, never transferable quorum authority.
func graphE17NativeVoterPage(ctx context.Context, h *allocationHost, wire []byte) (result []byte, err error) {
	if ctx == nil || h == nil {
		return nil, errInvalid
	}
	cert, after, err := decodeGraphE17NativeVoterRequest(wire)
	if err != nil {
		return nil, err
	}
	view, err := h.AtGraphCertificate(ctx, cert)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, view.Close())
		if err != nil {
			result = nil
		}
	}()
	root, err := view.RootBounded(ctx, 172)
	if err != nil {
		return nil, err
	}
	binding, err := view.ApplicationBinding(ctx)
	if err != nil {
		return nil, err
	}
	if !graphE17NativeVoterCoordinate(cert, root, binding) {
		return nil, errInvalid
	}
	limits := view.ReadLimits()
	if limits.Rows < graphE17NativeVoterRows || limits.Bytes < graphE17NativeVoterBytes {
		return nil, errLimit
	}
	page, err := view.Scan(ctx, nil, nil, after, raftlog.ReadBudget{Rows: graphE17NativeVoterRows, Bytes: graphE17NativeVoterBytes})
	if err != nil {
		return nil, err
	}
	if !graphE17NativeVoterPageValid(page, after) {
		return nil, errCorrupt
	}
	pageWire, err := encodeGraphSourcePage(page, graphE17NativeVoterFrameBytes)
	if err != nil {
		return nil, err
	}
	return boundedEncoding(graphE17NativeVoterFrameBytes, func(w *boundedWriter) {
		w.add([]byte("GVB1"))
		emitSourceRoot(w, root)
		w.add(binding.Identity.Graph[:])
		w.u64(binding.Identity.Partition)
		w.add(binding.Identity.Group[:])
		w.add(binding.SemanticContractID[:])
		w.field(pageWire)
	})
}

func graphE17NativeVoterCoordinate(cert graphCertificate, root raftlog.ApplicationRoot, binding raftlog.ApplicationBinding) bool {
	return root.Generation > 0 && root.Index == cert.sourceIndex && len(root.Image) == 172 && root.ImageHash == sha256.Sum256(cert.image[:]) && bytes.Equal(root.Image, cert.image[:]) && binding.Validate() == nil && binding.Identity.Graph == [16]byte(cert.scope.graph) && binding.Identity.Partition == cert.scope.partition && binding.Identity.Group == cert.scope.group && binding.SemanticContractID == cert.scope.semantic
}

func decodeGraphE17NativeVoterReply(wire []byte, cert graphCertificate, after []byte) (graphE17NativeVoterReply, error) {
	body, err := graphBody(wire, "GVB1", graphE17NativeVoterFrameBytes)
	if err != nil {
		return graphE17NativeVoterReply{}, err
	}
	c := graphCursor{b: body}
	r := graphE17NativeVoterReply{root: readSourceRoot(&c)}
	r.binding.Identity.Graph = c.array()
	r.binding.Identity.Partition = c.u64()
	r.binding.Identity.Group = c.array()
	copy(r.binding.SemanticContractID[:], c.take(32))
	page := c.field(graphE17NativeVoterFrameBytes)
	if c.err != nil || len(c.b) != 0 || !graphE17NativeVoterCoordinate(cert, r.root, r.binding) {
		return graphE17NativeVoterReply{}, errors.Join(errCorrupt, c.err)
	}
	r.page, err = decodeGraphSourcePage(page, raftlog.ReadBudget{Rows: graphE17NativeVoterRows, Bytes: graphE17NativeVoterBytes})
	if err != nil || !graphE17NativeVoterPageValid(r.page, after) {
		return graphE17NativeVoterReply{}, errors.Join(errCorrupt, err)
	}
	r.root.Image = bytes.Clone(r.root.Image)
	return r, nil
}

// Scan every actual child at the SAME certificate. Physical pages may differ:
// future keys/tombstones can consume a page with no visible rows, so comparison
// is ordered row-by-row across borders, and continuation follows actual Next.
func graphE17WalkNativeVoter(t *testing.T, child *graphProcess, cert graphCertificate, op byte, visit func(raftlog.KV)) {
	t.Helper()
	if child == nil || visit == nil {
		t.Fatal("native voter probe missing child/consumer")
	}
	var after, previous []byte
	visited, workBytes, retainedBytes := 0, 0, 0
	for pageNumber := range graphE17NativeVoterPages {
		request, err := encodeGraphE17NativeVoterRequest(cert, after)
		if err != nil {
			t.Fatal(err)
		}
		response := child.call(t, processRequest{op: op, data: request})
		r, err := decodeGraphE17NativeVoterReply(response.aux, cert, after)
		if err != nil {
			t.Fatal("actual voter certificate/page", pageNumber, err)
		}
		visited += r.page.Visited
		workBytes += r.page.Bytes
		if visited > graphE17NativeVoterTotalRows || workBytes > graphE17NativeVoterTotalBytes {
			t.Fatal("native voter scan consumed cumulative work allowance")
		}
		for _, row := range r.page.Rows {
			if len(previous) > 0 && bytes.Compare(row.Key, previous) <= 0 {
				t.Fatal("native voter output duplicated/reordered key")
			}
			retainedBytes += 64 + len(row.Key) + len(row.Value)
			if retainedBytes > graphE17NativeVoterTotalBytes {
				t.Fatal("native voter comparison exceeded owned-output bound")
			}
			visit(row)
			previous = row.Key
		}
		if r.page.Complete {
			return
		}
		after = r.page.Next
	}
	t.Fatal("native voter physical continuation did not finish within finite pages")
}

// This supplements the public typed graph oracle; matching replicas alone is
// not an independent semantic oracle. Scan includes all graph KV and replicated
// application metadata in each dedicated native store, without guessed prefixes.
// Phantom keys must come from real existing key APIs; nil leaves that assertion
// with the full public phantom oracle in the calling process acceptance test.
func graphE17AssertNativeVoters(t *testing.T, n *certifiedProcessNetwork, cut graphCut, op byte, phantomKeys [][]byte) {
	t.Helper()
	if n == nil || len(cut.certificates) != 2 || len(phantomKeys) > graphE17NativeVoterRows {
		t.Fatal("native voter comparison requires actual two-group certificate")
	}
	n.e17VoterUsed++
	if n.e17VoterUsed > n.e17VoterPhases {
		t.Fatal("undeclared E17 native voter oracle phase")
	}
	seen := make(map[uint64]bool, 2)
	for _, cert := range cut.certificates {
		leader := uint64(1)
		if cert.scope.partition == 8 {
			leader = 4
		} else if cert.scope.partition != 3 {
			t.Fatal("native voter certificate outside actual fixture")
		}
		if seen[cert.scope.partition] {
			t.Fatal("duplicate native voter certificate")
		}
		seen[cert.scope.partition] = true
		ns, voters := certifiedProcessNamespace(leader)
		if cert.scope.graph != ns.graph || cert.scope.partition != ns.partition {
			t.Fatal("native voter scope differs from real child topology")
		}
		// Reserve the complete fixed backing, including slots never filled.
		// The native conservative64-byte KV header exceeds the actual supported
		// header size. Retained byte capacities are additional to this backing.
		ownedBytes := 64 * graphE17NativeVoterTotalRows
		if ownedBytes > graphE17NativeVoterTotalBytes {
			t.Fatal("native baseline backing exceeds owned allowance")
		}
		expected := make([]raftlog.KV, 0, graphE17NativeVoterTotalRows)
		graphE17WalkNativeVoter(t, n.all[voters[0]], cert, op, func(row raftlog.KV) {
			cost := cap(row.Key) + cap(row.Value)
			if cost > graphE17NativeVoterTotalBytes-ownedBytes {
				t.Fatal("native baseline backing and retained byte capacities exceed owned allowance")
			}
			ownedBytes += cost
			for _, phantom := range phantomKeys {
				if len(phantom) == 0 {
					t.Fatal("empty phantom key is not an actual key")
				}
				if bytes.Equal(row.Key, phantom) {
					t.Fatalf("phantom key returned native visible value: %x", row.Key)
				}
			}
			expected = append(expected, row)
		})
		for _, voter := range voters[1:] {
			position := 0
			graphE17WalkNativeVoter(t, n.all[voter], cert, op, func(row raftlog.KV) {
				if position >= len(expected) || row.Deleted != expected[position].Deleted || !bytes.Equal(row.Key, expected[position].Key) || !bytes.Equal(row.Value, expected[position].Value) {
					t.Fatalf("voter%d differs at native visible row%d key=%x", voter, position, row.Key)
				}
				position++
			})
			if position != len(expected) {
				t.Fatal("voter omitted complete native visible rows", voter, position, len(expected))
			}
		}
	}
}
