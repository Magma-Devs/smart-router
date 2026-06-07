package rpcsmartrouter

import (
	"fmt"
	"strings"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// Per-method cross-validation policy (Phase 1.1).
//
// A policy lives in SmartRouter config, scoped by (chain-id, api-interface, method), and tunes the
// cross-validation knobs for that method. Each knob resolves against the caller's request headers via
// clamp(caller, floor, cap):
//
//   - floor — operator minimum: a caller may always ask stricter (higher) and get it, up to cap.
//   - cap   — operator maximum: it overrides a stricter caller request (clamps it down). This is the
//     only mechanism that can make the router validate less strictly than a caller asked, and it is an
//     explicit, documented operator decision.
//   - floor == cap expresses an exact/authoritative value.
//
// With no policy for a method, behavior is exactly caller-driven (backwards compatible). A policy with
// Enabled=true forces cross-validation on even when the caller sent no headers; Enabled=false means the
// operator does not mandate CV for the method (caller headers still work).
//
// This type and resolver are pure: they do not touch the relay hot path. Wiring the resolver into the
// state machine's selection decision is a separate step.

// Default knob values applied when an enabled policy specifies neither a caller value nor a floor.
const (
	defaultEnabledMaxParticipants    = 3
	defaultEnabledAgreementThreshold = 2
	defaultEnabledMinGroups          = 1
)

// Bound is an optional [Floor, Cap] range for one cross-validation knob. A nil side is unbounded.
type Bound struct {
	Floor *int `yaml:"floor,omitempty" json:"floor,omitempty" mapstructure:"floor,omitempty"`
	Cap   *int `yaml:"cap,omitempty" json:"cap,omitempty" mapstructure:"cap,omitempty"`
}

// CrossValidationPolicy is a per-method cross-validation policy.
type CrossValidationPolicy struct {
	Enabled            bool  `yaml:"enabled,omitempty" json:"enabled,omitempty" mapstructure:"enabled,omitempty"`
	MaxParticipants    Bound `yaml:"max-participants,omitempty" json:"max-participants,omitempty" mapstructure:"max-participants,omitempty"`
	AgreementThreshold Bound `yaml:"agreement-threshold,omitempty" json:"agreement-threshold,omitempty" mapstructure:"agreement-threshold,omitempty"`
	MinGroups          Bound `yaml:"min-groups,omitempty" json:"min-groups,omitempty" mapstructure:"min-groups,omitempty"`
}

// CrossValidationConfig is the parsed `cross-validation:` config block: chain-id -> api-interface ->
// method -> policy.
type CrossValidationConfig struct {
	Policies map[string]map[string]map[string]CrossValidationPolicy `yaml:"policies,omitempty" json:"policies,omitempty" mapstructure:"policies,omitempty"`
}

// CrossValidationPolicyResolver resolves the effective cross-validation params for a request. It is
// immutable after construction and safe for concurrent use.
type CrossValidationPolicyResolver struct {
	policies map[string]CrossValidationPolicy // keyed by policyKey(chain, api, method)
}

// policyKey builds the canonical lookup key. chain-id and api-interface are matched case-insensitively
// (they are deployment identifiers); method names are matched exactly (RPC methods are case-sensitive).
func policyKey(chainID, apiInterface, method string) string {
	return strings.ToLower(chainID) + "\x00" + strings.ToLower(apiInterface) + "\x00" + method
}

// NewCrossValidationPolicyResolver flattens and validates the nested config into a resolver. An empty or
// nil config yields a resolver with no policies (every request resolves to pure caller-driven behavior).
func NewCrossValidationPolicyResolver(cfg CrossValidationConfig) (*CrossValidationPolicyResolver, error) {
	r := &CrossValidationPolicyResolver{policies: map[string]CrossValidationPolicy{}}
	for chainID, byAPI := range cfg.Policies {
		for apiInterface, byMethod := range byAPI {
			for method, policy := range byMethod {
				if err := policy.Validate(); err != nil {
					return nil, fmt.Errorf("invalid cross-validation policy for %s/%s/%s: %w", chainID, apiInterface, method, err)
				}
				r.policies[policyKey(chainID, apiInterface, method)] = policy
			}
		}
	}
	return r, nil
}

// HasPolicies reports whether any policy is configured (used to keep the no-policy path identical to today).
func (r *CrossValidationPolicyResolver) HasPolicies() bool {
	return r != nil && len(r.policies) > 0
}

// Resolve returns the effective cross-validation params for a request and whether cross-validation
// applies. callerParams/callerPresent come from the request's cross-validation headers.
//
// Precedence: no policy or a disabled policy => pure caller-driven (backwards compatible). An enabled
// policy => CV applies, with each knob = clamp(caller-or-floor-or-default, floor, cap). The structural
// invariants agreement-threshold <= max-participants and min-groups <= max-participants are enforced on
// the final values so the resolved quorum shape is always satisfiable.
func (r *CrossValidationPolicyResolver) Resolve(chainID, apiInterface, method string, callerParams common.CrossValidationParams, callerPresent bool) (common.CrossValidationParams, bool) {
	if r == nil {
		return callerParams, callerPresent
	}
	policy, hasPolicy := r.policies[policyKey(chainID, apiInterface, method)]
	if !hasPolicy || !policy.Enabled {
		// No policy, or operator does not mandate CV here: caller headers alone decide.
		return callerParams, callerPresent
	}

	eff := common.CrossValidationParams{
		MaxParticipants:    resolveKnob(callerParams.MaxParticipants, callerPresent, policy.MaxParticipants, defaultEnabledMaxParticipants),
		AgreementThreshold: resolveKnob(callerParams.AgreementThreshold, callerPresent, policy.AgreementThreshold, defaultEnabledAgreementThreshold),
		// There is no caller header for min-groups, so it is resolved purely from the policy.
		MinGroups: resolveKnob(0, false, policy.MinGroups, defaultEnabledMinGroups),
	}

	// Keep the quorum shape satisfiable regardless of caller/cap interaction.
	if eff.AgreementThreshold > eff.MaxParticipants {
		eff.AgreementThreshold = eff.MaxParticipants
	}
	if eff.MinGroups > eff.MaxParticipants {
		eff.MinGroups = eff.MaxParticipants
	}
	return eff, true
}

// resolveKnob computes one knob's effective value: start from the caller value (if present), else the
// floor (if set), else the default; then clamp into [floor, cap].
func resolveKnob(callerVal int, callerPresent bool, b Bound, def int) int {
	base := def
	switch {
	case callerPresent:
		base = callerVal
	case b.Floor != nil:
		base = *b.Floor
	}
	if b.Floor != nil && base < *b.Floor {
		base = *b.Floor
	}
	if b.Cap != nil && base > *b.Cap {
		base = *b.Cap
	}
	return base
}

// ValidateNoStatefulPolicies rejects an enabled policy whose method is stateful (a write /
// CONSISTENCY_SELECT_ALL_PROVIDERS method). Cross-validating a write response is a no-op (the response is
// a deterministic acknowledgement, not an observation), and selection-mode precedence would route such a
// method into CrossValidation — so an enabled policy on a write is a configuration error. isStateful is
// injected (it needs loaded specs) and is called per enabled policy at startup.
func (r *CrossValidationPolicyResolver) ValidateNoStatefulPolicies(isStateful func(chainID, apiInterface, method string) bool) error {
	if r == nil || isStateful == nil {
		return nil
	}
	for key, policy := range r.policies {
		if !policy.Enabled {
			continue
		}
		chainID, apiInterface, method := splitPolicyKey(key)
		if isStateful(chainID, apiInterface, method) {
			return fmt.Errorf("cross-validation policy on stateful method %s/%s/%s is not allowed: cross-validating a transaction-submission response is a no-op (see UC-3); strengthen write paths via the stateful fan-out instead", chainID, apiInterface, method)
		}
	}
	return nil
}

// splitPolicyKey reverses policyKey for diagnostics. chain-id and api-interface come back lowercased.
func splitPolicyKey(key string) (chainID, apiInterface, method string) {
	parts := strings.SplitN(key, "\x00", 3)
	for len(parts) < 3 {
		parts = append(parts, "")
	}
	return parts[0], parts[1], parts[2]
}

// Validate checks a single policy's internal consistency (config-load time, no spec/provider context).
func (p CrossValidationPolicy) Validate() error {
	for name, b := range map[string]Bound{
		"max-participants":    p.MaxParticipants,
		"agreement-threshold": p.AgreementThreshold,
		"min-groups":          p.MinGroups,
	} {
		if b.Floor != nil && *b.Floor < 1 {
			return fmt.Errorf("%s floor must be >= 1, got %d", name, *b.Floor)
		}
		if b.Cap != nil && *b.Cap < 1 {
			return fmt.Errorf("%s cap must be >= 1, got %d", name, *b.Cap)
		}
		if b.Floor != nil && b.Cap != nil && *b.Floor > *b.Cap {
			return fmt.Errorf("%s floor %d cannot exceed cap %d", name, *b.Floor, *b.Cap)
		}
	}
	// Cross-knob unsatisfiability: a required threshold/min-groups floor that exceeds the
	// max-participants cap can never be met.
	if p.AgreementThreshold.Floor != nil && p.MaxParticipants.Cap != nil && *p.AgreementThreshold.Floor > *p.MaxParticipants.Cap {
		return fmt.Errorf("agreement-threshold floor %d exceeds max-participants cap %d (unsatisfiable)", *p.AgreementThreshold.Floor, *p.MaxParticipants.Cap)
	}
	if p.MinGroups.Floor != nil && p.MaxParticipants.Cap != nil && *p.MinGroups.Floor > *p.MaxParticipants.Cap {
		return fmt.Errorf("min-groups floor %d exceeds max-participants cap %d (unsatisfiable)", *p.MinGroups.Floor, *p.MaxParticipants.Cap)
	}
	return nil
}
