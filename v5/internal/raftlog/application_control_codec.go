package raftlog

import (
	"bytes"
	"encoding/binary"
)

const controlTag byte = 18
const controlMetaBytes = 20

// ApplicationControlConfig selects fresh-store immutable control storage.
// Version one opts in; zero preserves legacy bytes and never upgrades a store.
type ApplicationControlConfig struct{ Version uint32 }

func (c ApplicationControlConfig) enabled() bool { return c.Version != 0 }

type controlMetadata struct {
	Config         ApplicationControlConfig
	Bytes, Records uint64
}

func appendControlMeta(b []byte, c controlMetadata) []byte {
	b = append(b, 'C', 'M', 1, 0)
	b = binary.BigEndian.AppendUint64(b, c.Bytes)
	return binary.BigEndian.AppendUint64(b, c.Records)
}
func decodeControlMeta(b []byte) (controlMetadata, []byte, error) {
	if len(b) < controlMetaBytes || !bytes.Equal(b[:4], []byte{'C', 'M', 1, 0}) {
		return controlMetadata{}, nil, ErrCorrupt
	}
	return controlMetadata{ApplicationControlConfig{1}, binary.BigEndian.Uint64(b[4:12]), binary.BigEndian.Uint64(b[12:20])}, b[20:], nil
}
func controlBankTag(bank byte) byte { return controlTag + bank }
func controlPrefix(bank byte, key []byte) []byte {
	b := appPrefix(key)
	b[0] = controlBankTag(bank)
	return b
}
func controlVersionKey(bank byte, key []byte, index uint64) []byte {
	b := appVersionKey(key, index)
	b[0] = controlBankTag(bank)
	return b
}
func decodeControlKey(bank byte, b []byte, limit int) ([]byte, uint64, error) {
	if bank > 1 {
		return nil, 0, ErrCorrupt
	}
	return decodeTaggedAppKey(b, limit, controlBankTag(bank))
}
func applicationTotals(m metadata) (uint64, uint64) {
	return m.App.Bytes + m.Controls.Bytes, m.App.Records + m.Controls.Records
}
func (c ApplicationControlConfig) validate(m metadata) error {
	if !c.enabled() {
		if c != (ApplicationControlConfig{}) || m.Controls != (controlMetadata{}) || m.Transfer.Contract.Version == 2 {
			return ErrInvalid
		}
		return nil
	}
	if c.Version != 1 || !m.App.Policy.Enabled() || !m.Rep.Config.enabled() || m.Rep.SemanticContractID == (ApplicationSemanticContractID{}) || m.Transfer.Contract.Version != 2 || !m.Gen.Limits.enabled() || !m.Gen.Publication.Limits.enabled() {
		return ErrInvalid
	}
	return nil
}
func validateControlMeta(m metadata) error {
	if err := m.Controls.Config.validate(m); err != nil {
		return err
	}
	c, p := m.Controls, m.App.Policy
	if !c.Config.enabled() {
		return nil
	}
	if p.RetainedApplicationBytes < 1 || p.RetainedApplicationBytes > 1<<40 || p.RetainedApplicationRecords < 1 || p.RetainedApplicationRecords > 1<<40 {
		return ErrInvalid
	}
	if m.App.Bytes > p.RetainedApplicationBytes || m.App.Records > p.RetainedApplicationRecords {
		return ErrLimit
	}
	if c.Bytes > p.RetainedApplicationBytes || c.Records > p.RetainedApplicationRecords || c.Records > c.Bytes/(12+appFrameBytes) || c.Records == 0 && c.Bytes != 0 {
		return ErrCorrupt
	}
	total, records := applicationTotals(m)
	if total > p.RetainedApplicationBytes || records > p.RetainedApplicationRecords {
		return ErrLimit
	}
	return nil
}

// ApplicationContractForPolicyWithControls checks shared bounds and returns a
// zero contract on error. The result grants no Store or control-mode authority;
// local voter identity and Raft limits remain separate configuration checks.
func ApplicationContractForPolicyWithControls(p ApplicationPolicy, c ApplicationControlConfig) (ApplicationContract, error) {
	if c.Version > 1 {
		return ApplicationContract{}, ErrInvalid
	}
	contract := ApplicationContractForPolicy(p)
	if c.enabled() {
		contract.Version = 2
	}
	if err := contract.validate(); err != nil {
		return ApplicationContract{}, err
	}
	return contract, nil
}

// ApplicationControls inspects the immutable configured mode; nil returns zero.
// Inspection does not grant a live Store or graph admission authority.
func (s *Store) ApplicationControls() ApplicationControlConfig {
	if s == nil {
		return ApplicationControlConfig{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta.Controls.Config
}
