package replica

import (
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"google.golang.org/protobuf/encoding/protowire"
)

// preflightWire validates the small fixed consensus schema without allocating
// protobuf objects. Network messages cannot contain local storage Responses;
// unknown fields/groups and duplicate singular fields fail closed. Recursion is
// structurally bounded to Message -> Snapshot -> Metadata -> ConfState.
func preflightWire(b []byte, kind int, l raftlog.Limits) error {
	var seen [15]bool
	var counts [15]int
	total := 0
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 || num < 1 || num > 14 {
			return ErrInvalid
		}
		b = b[n:]
		repeated := kind == 0 && num == 7 || kind == 4 && num <= 4
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
				if num == 1 && v > 2 {
					return ErrInvalid
				}
			} else if num == 4 {
				if typ != protowire.BytesType {
					return ErrInvalid
				}
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
				if typ != protowire.VarintType || v > 1 {
					return ErrInvalid
				}
			} else {
				return ErrInvalid
			}
		default:
			return ErrInvalid
		}
	}
	return nil
}
