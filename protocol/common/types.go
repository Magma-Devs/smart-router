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
}

// DefaultCrossValidationParams are used when cross-validation is not enabled (Selection != CrossValidation)
var DefaultCrossValidationParams = CrossValidationParams{
	MaxParticipants:    1,
	AgreementThreshold: 1,
	MinGroups:          1,
}
