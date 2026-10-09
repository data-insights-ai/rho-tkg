package replica

import (
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/confchange"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// preflightWire validates the small fixed consensus schema without allocating
// protobuf objects. Network messages cannot contain local storage Responses;
// unknown fields/groups and duplicate singular fields fail closed. Recursion is
// structurally bounded to snapshot membership or configuration-change children.
func preflightWire(b []byte, kind int, l raftlog.Limits) error {
	var seen [15]bool
	var counts [15]int
	total := 0
	var entryType uint64
	var entryData []byte
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 || num < 1 || num > 14 {
			return ErrInvalid
		}
		b = b[n:]
		repeated := kind == 0 && num == 7 || kind == 4 && num <= 4 || kind == 7 && num == 2
		if seen[num] && !repeated {
			return ErrInvalid
		}
		seen[num] = true
		var v uint64
		var blob []byte
		switch typ {
		case protowire.VarintType:
			v, n = protowire.ConsumeVarint(b)
		case protowire.BytesType:
			blob, n = protowire.ConsumeBytes(b)
		default:
			return ErrInvalid
		}
		if n < 0 {
			return ErrInvalid
		}
		b = b[n:]
		switch kind {
		case 0: // Message
			switch num {
			case 1, 2, 3, 4, 5, 6, 8, 10, 11, 13:
				if typ != protowire.VarintType {
					return ErrInvalid
				}
				if num == 1 && (v > 30) {
					return ErrInvalid
				}
			case 7:
				if typ != protowire.BytesType {
					return ErrInvalid
				}
				counts[num]++
				total += len(blob)
				if counts[num] > l.MaxReadEntries || total > l.MaxReadBytes {
					return ErrLimit
				}
				if err := preflightWire(blob, 1, l); err != nil {
					return err
				}
			case 9:
				if typ != protowire.BytesType {
					return ErrInvalid
				}
				if err := preflightWire(blob, 2, l); err != nil {
					return err
				}
			case 12:
				if typ != protowire.BytesType {
					return ErrInvalid
				}
				if len(blob) > 1024 {
					return ErrLimit
				}
			default:
				return ErrInvalid
			}
		case 1: // Entry
			if num <= 3 {
				if typ != protowire.VarintType {
					return ErrInvalid
				}
				if num == 1 {
					entryType = v
				}
				if num == 1 && v > 2 {
					return ErrInvalid
				}
			} else if num == 4 {
				if typ != protowire.BytesType {
					return ErrInvalid
				}
				entryData = blob
				if len(blob) > l.MaxEntryBytes-74 {
					return ErrLimit
				}
			} else {
				return ErrInvalid
			}
		case 2: // Snapshot
			if typ != protowire.BytesType {
				return ErrInvalid
			}
			switch num {
			case 1:
				if len(blob) > l.MaxSnapshotBytes {
					return ErrLimit
				}
			case 2:
				if err := preflightWire(blob, 3, l); err != nil {
					return err
				}
			default:
				return ErrInvalid
			}
		case 3: // SnapshotMetadata
			switch num {
			case 1:
				if typ != protowire.BytesType {
					return ErrInvalid
				}
				if err := preflightWire(blob, 4, l); err != nil {
					return err
				}
			case 2, 3:
				if typ != protowire.VarintType {
					return ErrInvalid
				}
			default:
				return ErrInvalid
			}
		case 4: // ConfState accepts either packed or unpacked repeated uint64.
			if num <= 4 {
				switch typ {
				case protowire.VarintType:
					counts[num]++
				case protowire.BytesType:
					for len(blob) > 0 {
						_, n := protowire.ConsumeVarint(blob)
						if n < 0 {
							return ErrInvalid
						}
						blob = blob[n:]
						counts[num]++
						if counts[num] > 256 {
							return ErrLimit
						}
					}
				default:
					return ErrInvalid
				}
				if counts[num] > 256 {
					return ErrLimit
				}
			} else if num == 5 {
				if typ != protowire.VarintType {
					return ErrInvalid
				}
			} else {
				return ErrInvalid
			}
		case 6: // ConfChange V1: deprecated ID remains a supported field.
			if num <= 3 {
				if typ != protowire.VarintType || num == 2 && v > uint64(pb.ConfChangeAddLearnerNode) {
					return ErrInvalid
				}
			} else if num != 4 || typ != protowire.BytesType {
				return ErrInvalid
			}
		case 7: // ConfChange V2: bound repeated children before protobuf expansion.
			switch num {
			case 1:
				if typ != protowire.VarintType || v > uint64(pb.ConfChangeTransitionJointExplicit) {
					return ErrInvalid
				}
			case 2:
				if typ != protowire.BytesType {
					return ErrInvalid
				}
				counts[num]++
				if counts[num] > 256 {
					return ErrLimit
				}
				if err := preflightWire(blob, 8, l); err != nil {
					return err
				}
			case 3:
				if typ != protowire.BytesType {
					return ErrInvalid
				}
			default:
				return ErrInvalid
			}
		case 8: // ConfChangeSingle
			if num > 2 || typ != protowire.VarintType || num == 1 && v > uint64(pb.ConfChangeAddLearnerNode) {
				return ErrInvalid
			}
		default:
			return ErrInvalid
		}
	}
	// Entry.Data is otherwise opaque. Config payloads need a bounded schema
	// walk before protobuf allocates their repeated child objects downstream.
	if kind == 1 {
		switch pb.EntryType(entryType) {
		case pb.EntryConfChange:
			return preflightWire(entryData, 6, l)
		case pb.EntryConfChangeV2:
			return preflightWire(entryData, 7, l)
		}
	}
	return nil
}

// validateIncoming checks only fields interpreted by the pinned Raft handlers.
// Proposals have no transport term; follower forwarding preserves this. Appends
// must describe a consecutive log slice, not merely a list of valid entries.
func validateIncoming(m *pb.Message, cs *pb.ConfState, applied uint64, leader bool, l raftlog.Limits) error {
	// send attaches a term to network traffic except forwarded proposals and
	// reads. Network reads are already rejected by Step's admission policy.
	if m.GetTerm() > replicaTermCeiling || m.GetType() != pb.MsgProp && m.GetTerm() == 0 {
		return ErrInvalid
	}
	switch m.GetType() {
	case pb.MsgProp:
		if len(m.GetEntries()) == 0 || m.GetTerm() != 0 {
			return ErrInvalid
		}
	case pb.MsgApp:
		if m.GetIndex() == math.MaxUint64 || m.GetLogTerm() > m.GetTerm() ||
			(m.GetIndex() == 0) != (m.GetLogTerm() == 0) || uint64(len(m.GetEntries())) > math.MaxUint64-1-m.GetIndex() {
			return ErrInvalid
		}
		term := m.GetLogTerm()
		for offset, e := range m.GetEntries() {
			if e == nil || e.GetIndex() != m.GetIndex()+1+uint64(offset) || e.GetTerm() == 0 || e.GetTerm() < term || e.GetTerm() > m.GetTerm() {
				return ErrInvalid
			}
			term = e.GetTerm()
		}
	case pb.MsgSnap:
		md := m.GetSnapshot().GetMetadata()
		if md == nil || md.GetIndex() == 0 || md.GetIndex() == math.MaxUint64 || md.GetTerm() == 0 || md.GetTerm() > m.GetTerm() {
			return ErrInvalid
		}
		return validateSnapshotConf(md.GetConfState(), md.GetIndex())
	default:
		return nil
	}
	for _, e := range m.GetEntries() {
		if e == nil {
			return ErrInvalid
		}
		cc, err := decodeIncomingChange(e, l)
		if err != nil {
			return err
		}
		if cc != nil && m.GetType() == pb.MsgProp && leader {
			if err := validateChange(cs, cc, applied); err != nil {
				return err
			}
		}
	}
	return nil
}

func decodeIncomingChange(e *pb.Entry, l raftlog.Limits) (*pb.ConfChangeV2, error) {
	var cc *pb.ConfChangeV2
	switch e.GetType() {
	case pb.EntryNormal:
		return nil, nil
	case pb.EntryConfChange:
		if err := preflightWire(e.GetData(), 6, l); err != nil {
			return nil, err
		}
		v1 := &pb.ConfChange{}
		if err := (proto.UnmarshalOptions{RecursionLimit: 3}).Unmarshal(e.GetData(), v1); err != nil {
			return nil, errors.Join(ErrInvalid, err)
		}
		cc = v1.AsV2()
	case pb.EntryConfChangeV2:
		if err := preflightWire(e.GetData(), 7, l); err != nil {
			return nil, err
		}
		cc = &pb.ConfChangeV2{}
		if err := (proto.UnmarshalOptions{RecursionLimit: 3}).Unmarshal(e.GetData(), cc); err != nil {
			return nil, errors.Join(ErrInvalid, err)
		}
	default:
		return nil, ErrInvalid
	}
	if err := validateChangeSchema(cc); err != nil {
		return nil, err
	}
	return cc, nil
}

func validateSnapshotConf(cs *pb.ConfState, index uint64) error {
	if cs == nil || len(cs.GetVoters()) == 0 {
		return ErrInvalid
	}
	for _, ids := range [][]uint64{cs.Voters, cs.VotersOutgoing, cs.Learners, cs.LearnersNext} {
		if len(ids) > 256 {
			return ErrLimit
		}
		seen := make(map[uint64]bool, len(ids))
		for _, id := range ids {
			if id == 0 || raft.IsLocalMsgTarget(id) || seen[id] {
				return ErrInvalid
			}
			seen[id] = true
		}
	}
	t := tracker.MakeProgressTracker(1, 1)
	cfg, progress, err := confchange.Restore(confchange.Changer{Tracker: t, LastIndex: index}, cs)
	if err != nil {
		return errors.Join(ErrInvalid, err)
	}
	t.Config, t.Progress = cfg, progress
	if err := cs.Equivalent(t.ConfState()); err != nil {
		return errors.Join(ErrInvalid, err)
	}
	return nil
}
