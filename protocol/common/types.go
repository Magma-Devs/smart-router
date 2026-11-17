package common

// CrossValidationParams holds the cross-validation configuration parameters
// Note: Whether cross-validation is enabled is determined by the Selection type (CrossValidation),
// not by these parameters. These parameters only store the values when cross-validation is active.
type CrossValidationParams struct {
	MaxParticipants    int // Maximum number of providers to query
	AgreementThreshold int // Number of matching responses needed for consensus
	// MinGroups is the minimum number of distinct provider groups the agreeing responses must span.
	// 1 means no group-diversity requirement (the backwards-compatible default). Set by per-method
	// policy; there is no request header for it.
	MinGroups int
	// PerGroupQuorum upgrades the diversity requirement from "one cross-group consensus spanning MinGroups
	// groups" (the MinGroups default) to "each of MinGroups groups independently reaches its own internal
	// quorum of AgreementThreshold matching responses, and the per-group winners agree". Opt-in via
	// per-method policy; there is no request header for it. Implies selection front-loads
	// AgreementThreshold sessions per group (see GetSessionsOptions.PerGroupTarget).
	PerGroupQuorum bool
	// IncludeBackups is set when the policy's max-participants resolved from the "all" sentinel
	// (MAG-2783): selection must top up a primary-only shortfall from the backup tier, up to
	// MaxParticipants, rather than treating backups as the emergency-only fallback they are for every
	// other request. There is no request header for it; it is policy-only, like MinGroups.
	IncludeBackups bool
}

// DefaultCrossValidationParams are used when cross-validation is not enabled (Selection != CrossValidation)
var DefaultCrossValidationParams = CrossValidationParams{
	MaxParticipants:    1,
	AgreementThreshold: 1,
	MinGroups:          1,
}
