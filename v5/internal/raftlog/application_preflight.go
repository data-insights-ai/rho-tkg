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

// ApplicationControlBatchUsage separates the immutable control subtotal from
// aggregate growth while preserving the original two-field result type.
type ApplicationControlBatchUsage struct {
	ApplicationBatchUsage
	ControlBytes, ControlRecords uint64
}

// PreflightControlBatch is the same pure joint installation check as Preflight,
// with the control subtotal additionally reported. It grants no mode, absence,
// reservation, commit or authority; every error returns zero usage.
func (p ApplicationPolicy) PreflightControlBatch(batch ApplicationBatch, limits Limits) (ApplicationControlBatchUsage, error) {
	aggregate, err := p.Preflight(batch, limits)
	if err != nil {
		return ApplicationControlBatchUsage{}, err
	}
	cb, cr, _, err := controlGrowth(p, batch)
	if err != nil {
		return ApplicationControlBatchUsage{}, err
	}
	return ApplicationControlBatchUsage{ApplicationBatchUsage: aggregate, ControlBytes: cb, ControlRecords: cr}, nil
}
