package raftlog

// ApplicationBatchUsage is the logical retained growth of one installation:
// immutable KV versions and root/change/outcome envelopes, including keys and
// frames. It excludes checkpoint copies and physical disk/RSS consumption.
type ApplicationBatchUsage struct {
	RetainedBytes, RetainedRecords uint64
}

// Preflight validates batch shape and conservative installation work using the
// same accounting as InstallApplication. Limits must be concrete and valid
// (use DefaultLimits explicitly for defaults); the policy must be enabled and
// valid for those limits. Errors return zero usage.
//
// This pure check neither mutates batch nor reserves capacity. It does not
// check BaseIndex/BaseImageHash, committed-entry eligibility or remaining store
// quota. AdmitApplication separately checks proposal headroom within the
// serialized driver path.
// InstallApplication remains the authority for installation. Callers must keep
// batch unchanged between checking and use and must not mutate it concurrently.
func (p ApplicationPolicy) Preflight(batch ApplicationBatch, limits Limits) (ApplicationBatchUsage, error) {
	if err := limits.Validate(); err != nil {
		return ApplicationBatchUsage{}, err
	}
	if !p.Enabled() {
		return ApplicationBatchUsage{}, ErrInvalid
	}
	if err := p.validate(limits); err != nil {
		return ApplicationBatchUsage{}, err
	}
	retained, records, err := p.measure(batch)
	if err != nil {
		return ApplicationBatchUsage{}, err
	}
	return ApplicationBatchUsage{RetainedBytes: retained, RetainedRecords: records}, nil
}
