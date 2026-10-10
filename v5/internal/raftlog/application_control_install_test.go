package raftlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func newControlStore(t *testing.T, fs vfs.FS, voter uint64) (*Store, Config) {
	t.Helper()
	if fs == nil {
		fs = vfs.NewMem()
	}
	p, tc := transferConfig(voter)
	ctrl := ApplicationControlConfig{Version: 1}
	contract, err := ApplicationContractForPolicyWithControls(p, ctrl)
	if err != nil {
		t.Fatal(err)
	}
	tc.Contract = contract
	cfg := Config{Dir: "control", FS: fs, Create: true, Application: p, Transfer: tc, Controls: ctrl, Generations: ApplicationGenerationLimits{2 << 30, 2000000}, PublishedCuts: ApplicationPublishedCutLimits{10000}, Replication: ApplicationReplicationConfig{[3]uint64{1, 2, 3}}, SemanticContractID: ApplicationSemanticContractID{41}}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil && s.poison == nil {
			t.Error(err)
		}
	})
	if err := s.Initialize([]uint64{1, 2, 3}, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	return s, cfg
}

func boundedControlStore(t *testing.T, edit func(*Config)) *Store {
	t.Helper()
	p, tc := transferConfig(1)
	p.MaxInstallWrites, p.MaxInstallBytes = 1, 1664
	ctrl := ApplicationControlConfig{Version: 1}
	contract, err := ApplicationContractForPolicyWithControls(p, ctrl)
	if err != nil {
		t.Fatal(err)
	}
	tc.Contract = contract
	cfg := Config{Dir: "bounded-control", FS: vfs.NewMem(), Create: true, Application: p, Transfer: tc, Controls: ctrl, Generations: ApplicationGenerationLimits{2 << 30, 2000000}, PublishedCuts: ApplicationPublishedCutLimits{10000}, Replication: ApplicationReplicationConfig{[3]uint64{1, 2, 3}}, SemanticContractID: ApplicationSemanticContractID{41}}
	edit(&cfg)
	cfg.Transfer.Contract, err = ApplicationContractForPolicyWithControls(cfg.Application, ctrl)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Application.Validate(DefaultLimits()); err != nil {
		t.Fatal("fixture is not a legal policy", err)
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Initialize([]uint64{1, 2, 3}, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestControlLegalAggregateAdmissionAndPendingBoundaries(t *testing.T) {
	// Installed total is 333/7. Two reserved installations each charge
	// 1664 bytes and (one joint write + three envelopes) four records.
	// These limits exceed the independent policy minima 3328/8.
	for _, generation := range []bool{false, true} {
		for _, records := range []bool{false, true} {
			for _, delta := range []uint64{0, 1} {
				t.Run(fmt.Sprintf("generation=%v/records=%v/short=%d", generation, records, delta), func(t *testing.T) {
					s := boundedControlStore(t, func(c *Config) {
						if generation {
							if records {
								c.Generations.MaxRecords = 15 - delta
							} else {
								c.Generations.MaxBytes = 3661 - delta
							}
						} else if records {
							c.Application.RetainedApplicationRecords = 15 - delta
						} else {
							c.Application.RetainedApplicationBytes = 3661 - delta
						}
					})
					installControl(t, s, "seed", ApplicationControlPut{Key: []byte{'A', 0}, Value: []byte("reg")})
					before := readyMetadataFingerprint(t, s)
					err := s.AdmitApplication(0)
					if delta == 0 && err != nil || delta != 0 && !errors.Is(err, ErrLimit) {
						t.Fatal("aggregate proposal reservation", err)
					}
					if readyMetadataFingerprint(t, s) != before || s.poison != nil {
						t.Fatal("pure admission changed state")
					}
					persist(t, s, 2, 3, ent(3, 2, "pending-one"))
					before = readyMetadataFingerprint(t, s)
					err = s.Persist(raft.Ready{Entries: []*pb.Entry{ent(4, 2, "pending-two")}})
					if delta == 0 && err != nil || delta != 0 && !errors.Is(err, ErrLimit) {
						t.Fatal("aggregate pending reservation", err)
					}
					if delta != 0 && readyMetadataFingerprint(t, s) != before || s.poison != nil {
						t.Fatal("pending refusal changed durable state")
					}
					u, err := s.ApplicationControlUsage()
					if err != nil || u != (ApplicationControlUsage{53, 1, 333, 7}) {
						t.Fatal("pending altered installed ledgers", u, err)
					}
				})
			}
		}
	}
}

func TestControlLegalInstallWorkExactFitAndOneShort(t *testing.T) {
	for _, delta := range []int{0, 1} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			s := boundedControlStore(t, func(c *Config) { c.Application.MaxInstallBytes = 1664 - delta })
			index, batch := controlBatch(t, s, "seed", []ApplicationControlPut{{Key: []byte{'A', 0}, Value: []byte("reg")}})
			before := readyMetadataFingerprint(t, s)
			err := s.InstallApplication(index, batch)
			if delta == 0 {
				if err != nil {
					t.Fatal("literal 1664 work refused", err)
				}
				u, err := s.ApplicationControlUsage()
				if err != nil || u != (ApplicationControlUsage{53, 1, 333, 7}) {
					t.Fatal(u, err)
				}
			} else if !errors.Is(err, ErrLimit) || readyMetadataFingerprint(t, s) != before || s.poison != nil {
				t.Fatal("one-short install was not atomic refusal", err)
			}
		})
	}
}
func controlBatch(t *testing.T, s *Store, image string, puts []ApplicationControlPut, writes ...KV) (uint64, ApplicationBatch) {
	t.Helper()
	index, _, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	base, err := v.Root()
	if err != nil {
		t.Fatal(err)
	}
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, index+1, ent(index+1, 2, "control"))
	return index + 1, ApplicationBatch{BaseGeneration: base.Generation, BaseIndex: base.Index, BaseImageHash: base.ImageHash, Image: []byte(image), Changes: []byte("c"), Outcome: []byte("o"), ControlPuts: puts, Writes: writes}
}
func installControl(t *testing.T, s *Store, image string, puts ...ApplicationControlPut) uint64 {
	t.Helper()
	index, b := controlBatch(t, s, image, puts)
	if err := s.InstallApplication(index, b); err != nil {
		t.Fatal(err)
	}
	return index
}
func TestControlSameBatchIndependentLedgersAndReopen(t *testing.T) {
	fs := vfs.NewMem()
	s, cfg := newControlStore(t, fs, 1)
	old := viewApplication(t, s, 1)
	index := installControl(t, s, "seed", ApplicationControlPut{Key: []byte{'A', 0}, Value: []byte("reg")})
	u, err := s.ApplicationUsage()
	if err != nil || u.RetainedBytes != 333 || u.RetainedRecords != 7 || u.GraphBytes != 280 || u.GraphRecords != 6 || u.ControlBytes != 53 || u.ControlRecords != 1 {
		t.Fatal(u, err)
	}
	cu, err := s.ApplicationControlUsage()
	if err != nil || cu.Bytes != 53 || cu.Records != 1 || cu.TotalBytes != 333 || cu.TotalRecords != 7 {
		t.Fatal(cu, err)
	}
	row, found, _, err := old.GetControl(t.Context(), []byte{'A', 0}, ReadBudget{2, 4096})
	if err != nil || found || row.Value != nil {
		t.Fatal("old root substituted current", row, found, err)
	}
	now := viewApplication(t, s, index)
	row, found, _, err = now.GetControl(t.Context(), []byte{'A', 0}, ReadBudget{2, 4096})
	if err != nil || !found || string(row.Value) != "reg" || row.Index != 2 {
		t.Fatal(row, found, err)
	}
	if _, err := now.ProveNoApplicationData(t.Context()); err != nil {
		t.Fatal("control broke independent graph empty proof", err)
	}
	if err := s.ScrubApplication(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if err := now.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	r, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	reopened := viewApplication(t, r, 2)
	row, found, _, err = reopened.GetControl(t.Context(), []byte{'A', 0}, ReadBudget{2, 4096})
	if err != nil || !found || string(row.Value) != "reg" {
		t.Fatal(row, found, err)
	}
	bad := cfg
	bad.Controls = ApplicationControlConfig{}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(bad); !errors.Is(err, ErrInvalid) {
		t.Fatal("legacy reopen accepted controls", err)
	}
}
func TestControlLateImmutableConflictRejectsWholeInstallation(t *testing.T) {
	s, _ := newControlStore(t, nil, 1)
	installControl(t, s, "seed", ApplicationControlPut{Key: []byte("z"), Value: []byte("original")})
	index, b := controlBatch(t, s, "new", []ApplicationControlPut{{Key: []byte("a"), Value: []byte("must-not-install")}, {Key: []byte("z"), Value: []byte("replacement")}}, KV{Key: []byte("graph"), Value: []byte("must-not-install")})
	before, _ := encodeMeta(s.meta)
	if err := s.InstallApplication(index, b); !errors.Is(err, ErrInvalid) {
		t.Fatal("overwrite accepted", err)
	}
	after, _ := encodeMeta(s.meta)
	if !bytes.Equal(before, after) || s.poison != nil {
		t.Fatal("refusal changed metadata or poisoned")
	}
	v := viewApplication(t, s, index-1)
	if _, found, _, err := v.GetControl(t.Context(), []byte("a"), ReadBudget{2, 4096}); err != nil || found {
		t.Fatal(found, err)
	}
	if _, found, err := v.Get(t.Context(), []byte("graph"), 4096); err != nil || found {
		t.Fatal(found, err)
	}
	b.ControlPuts = b.ControlPuts[:1]
	if err := s.InstallApplication(index, b); err != nil {
		t.Fatal("valid retry refused", err)
	}
	now := viewApplication(t, s, index)
	if _, err := now.ProveNoApplicationData(t.Context()); !errors.Is(err, ErrInvalid) {
		t.Fatal("graph KV ignored by proof", err)
	}
	root, _ := now.Root()
	if string(root.Image) != "new" || root.ImageHash != sha256.Sum256([]byte("new")) {
		t.Fatal(root)
	}
}

func TestControlLegalGenerationStagingIncludesControlAndPendingReservations(t *testing.T) {
	donor := boundedControlStore(t, func(*Config) {})
	m, chunks := controlStream(t, donor)
	for _, records := range []bool{false, true} {
		for _, short := range []uint64{0, 1} {
			t.Run(fmt.Sprintf("records=%v/short=%d", records, short), func(t *testing.T) {
				s := boundedControlStore(t, func(c *Config) {
					if records {
						c.Generations.MaxRecords = 14 - short
					} else {
						c.Generations.MaxBytes = 2136 - short
					}
				})
				installControl(t, s, "seed", ApplicationControlPut{Key: []byte{'A', 0}, Value: []byte("reg")})
				persist(t, s, 2, 3, ent(3, 2, "pending"))
				before := readyMetadataFingerprint(t, s)
				// Independently333/7 installed +1664/4 pending +139/3
				// complete staged initial graph =2136 bytes and14 records.
				i, err := s.BeginApplicationImport(t.Context(), m)
				if short != 0 {
					if i != nil || !errors.Is(err, ErrLimit) || readyMetadataFingerprint(t, s) != before || s.poison != nil {
						t.Fatal("one-short staging reservation", err)
					}
					return
				}
				if err != nil {
					t.Fatal("literal full reservation refused", err)
				}
				before = readyMetadataFingerprint(t, s)
				if err := s.Persist(raft.Ready{Entries: []*pb.Entry{ent(4, 2, "steals-staging")}}); !errors.Is(err, ErrLimit) || readyMetadataFingerprint(t, s) != before {
					t.Fatal("pending stole reserved stage", err)
				}
				for _, c := range chunks {
					if err := i.Append(t.Context(), c); err != nil {
						t.Fatal(err)
					}
				}
				if err := i.Verify(t.Context()); err != nil {
					t.Fatal(err)
				}
				if err := i.Abort(); err != nil {
					t.Fatal(err)
				}
				u, err := s.ApplicationControlUsage()
				if err != nil || u != (ApplicationControlUsage{53, 1, 333, 7}) {
					t.Fatal(u, err)
				}
			})
		}
	}
}

func TestControlLegalPublishedPinExactFitAndOneShort(t *testing.T) {
	for _, short := range []uint64{0, 1} {
		t.Run(fmt.Sprint(short), func(t *testing.T) {
			s := boundedControlStore(t, func(c *Config) {
				c.Transfer.Limits.MaxExports = 8
				c.Transfer.Limits.MaxPinnedLogicalBytes = 76468 - short
			})
			installControl(t, s, "seed", ApplicationControlPut{Key: []byte{'A', 0}, Value: []byte("reg")})
			if err := s.PublishSnapshot(); err != nil {
				t.Fatal(err)
			}
			cut, err := s.PublishedApplicationCut()
			if err != nil {
				t.Fatal(err)
			}
			// Per handle333+4+4+1012+3+4+304+1024+4*2059=10924.
			// Seven handles76468 exceeds legal admission minimum68608.
			for n := range 7 {
				before := readyMetadataFingerprint(t, s)
				pins := s.pinnedApplicationBytes
				e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
				if n == 6 && short != 0 {
					if e != nil || !errors.Is(err, ErrLimit) || s.pinnedApplicationBytes != pins || readyMetadataFingerprint(t, s) != before || s.poison != nil {
						t.Fatal("one-short pin admission", err)
					}
					continue
				}
				if err != nil {
					t.Fatal("exact pin reservation", n, err)
				}
				defer e.Close()
				if e.pinBytes != 10924 || s.pinnedApplicationBytes != uint64(n+1)*10924 {
					t.Fatal("pin arithmetic", e.pinBytes, s.pinnedApplicationBytes)
				}
			}
		})
	}
}

func TestControlReadyHeadroomLiteralFreshOpenBoundary(t *testing.T) {
	for _, short := range []int{0, 1} {
		t.Run(fmt.Sprint(short), func(t *testing.T) {
			p, tc := transferConfig(1)
			ctrl := ApplicationControlConfig{1}
			contract, err := ApplicationContractForPolicyWithControls(p, ctrl)
			if err != nil {
				t.Fatal(err)
			}
			tc.Contract = contract
			l := DefaultLimits()
			l.MaxEntryBytes = 256
			l.MaxReadBytes = 512
			l.MaxReadyBytes = 1502 - short
			cfg := Config{Dir: "headroom", FS: vfs.NewMem(), Create: true, Limits: l, Application: p, Transfer: tc, Controls: ctrl, Generations: ApplicationGenerationLimits{2 << 30, 2000000}, PublishedCuts: ApplicationPublishedCutLimits{10000}, Replication: ApplicationReplicationConfig{[3]uint64{1, 2, 3}}, SemanticContractID: ApplicationSemanticContractID{41}}
			// Worst metadata1054 +Ready128 +AD3fixed312 +membership8=1502.
			// This is actual fresh-Open headroom, not fabricated Initialize work.
			s, err := Open(cfg)
			if short != 0 {
				if s != nil || !errors.Is(err, ErrLimit) {
					if s != nil {
						_ = s.Close()
					}
					t.Fatal("one-short Ready metadata admitted", err)
				}
				return
			}
			if err != nil {
				t.Fatal("exact Ready headroom refused", err)
			}
			defer s.Close()
			if err := s.Initialize([]uint64{1, 2, 3}, []byte("seed")); err != nil {
				t.Fatal(err)
			}
			raw, err := encodeMeta(s.meta)
			if err != nil || len(raw) != 1012 {
				t.Fatal("literal initialized metadata", len(raw), err)
			}
			installControl(t, s, "seed", ApplicationControlPut{Key: []byte{'A', 0}, Value: []byte("reg")})
		})
	}
}
