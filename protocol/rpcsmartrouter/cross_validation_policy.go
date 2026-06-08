package rpcsmartrouter

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
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

// CrossValidationPolicyEntry is one entry in the `cross-validation.policies` list. method is a string
// value (not a map key) so its casing is preserved — viper lower-cases map keys, which would corrupt
// case-sensitive RPC method names. This list shape also matches the existing `direct-rpc:` / `endpoints:`
// config style.
type CrossValidationPolicyEntry struct {
	ChainID               string `yaml:"chain-id,omitempty" json:"chain-id,omitempty" mapstructure:"chain-id"`
	ApiInterface          string `yaml:"api-interface,omitempty" json:"api-interface,omitempty" mapstructure:"api-interface"`
	Method                string `yaml:"method,omitempty" json:"method,omitempty" mapstructure:"method"`
	CrossValidationPolicy `yaml:",inline" mapstructure:",squash"`
}

// CrossValidationConfig is the parsed `cross-validation:` config block.
type CrossValidationConfig struct {
	Policies []CrossValidationPolicyEntry `yaml:"policies,omitempty" json:"policies,omitempty" mapstructure:"policies,omitempty"`
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
	for i, entry := range cfg.Policies {
		if entry.ChainID == "" || entry.ApiInterface == "" || entry.Method == "" {
			return nil, fmt.Errorf("cross-validation policy #%d must set chain-id, api-interface, and method", i)
		}
		if err := entry.CrossValidationPolicy.Validate(); err != nil {
			return nil, fmt.Errorf("invalid cross-validation policy for %s/%s/%s: %w", entry.ChainID, entry.ApiInterface, entry.Method, err)
		}
		key := policyKey(entry.ChainID, entry.ApiInterface, entry.Method)
		if _, dup := r.policies[key]; dup {
			return nil, fmt.Errorf("duplicate cross-validation policy for %s/%s/%s", entry.ChainID, entry.ApiInterface, entry.Method)
		}
		r.policies[key] = entry.CrossValidationPolicy
	}
	return r, nil
}

// HasPolicies reports whether any policy is configured (used to keep the no-policy path identical to today).
func (r *CrossValidationPolicyResolver) HasPolicies() bool {
	return r != nil && len(r.policies) > 0
}

// NumPolicies returns how many per-method policies are configured (for startup logging).
func (r *CrossValidationPolicyResolver) NumPolicies() int {
	if r == nil {
		return 0
	}
	return len(r.policies)
}

// MaxResolvedMinGroups returns the largest no-caller resolved min-groups among ENABLED policies for the
// given chain/api (0 if none). Used by the startup capacity check: if it exceeds the number of distinct
// provider groups configured for the endpoint, no request can ever satisfy that policy. min-groups has no
// caller header, so the no-caller value is the maximum a request will ever require.
func (r *CrossValidationPolicyResolver) MaxResolvedMinGroups(chainID, apiInterface string) int {
	if r == nil {
		return 0
	}
	wantChain, wantAPI := strings.ToLower(chainID), strings.ToLower(apiInterface)
	maxMinGroups := 0
	for key, policy := range r.policies {
		if !policy.Enabled {
			continue
		}
		kc, ka, _ := splitPolicyKey(key)
		if kc != wantChain || ka != wantAPI {
			continue
		}
		if mg := resolveKnob(0, false, policy.MinGroups, defaultEnabledMinGroups); mg > maxMinGroups {
			maxMinGroups = mg
		}
	}
	return maxMinGroups
}

// ParseCrossValidationConfig reads the optional top-level `cross-validation:` block from viper config.
// An absent key yields an empty config (fully backwards compatible). Each knob accepts either the object
// form `{floor: N, cap: M}` or the scalar shorthand `N` (meaning `{floor: N}`).
func ParseCrossValidationConfig(v *viper.Viper) (CrossValidationConfig, error) {
	var cfg CrossValidationConfig
	if v == nil || !v.IsSet(common.CrossValidationConfigName) {
		return cfg, nil
	}
	hook := viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(boundScalarShorthandHook()))
	if err := v.UnmarshalKey(common.CrossValidationConfigName, &cfg, hook); err != nil {
		return cfg, fmt.Errorf("could not unmarshal %q config: %w", common.CrossValidationConfigName, err)
	}
	return cfg, nil
}

// boundScalarShorthandHook lets a Bound be written as a bare number: `agreement-threshold: 2` is
// decoded as `{floor: 2}`. The object form (a map) is passed through untouched for normal decoding.
func boundScalarShorthandHook() mapstructure.DecodeHookFuncType {
	boundType := reflect.TypeOf(Bound{})
	return func(from reflect.Type, to reflect.Type, data interface{}) (interface{}, error) {
		if to != boundType {
			return data, nil
		}
		switch from.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64, reflect.String:
			n, err := scalarToInt(data)
			if err != nil {
				return nil, fmt.Errorf("invalid cross-validation knob value %v: %w", data, err)
			}
			return Bound{Floor: &n}, nil
		default:
			return data, nil // object form: let mapstructure decode the map into Bound
		}
	}
}

// scalarToInt converts a YAML scalar (int / float / numeric string) to an int for the Bound shorthand.
func scalarToInt(data interface{}) (int, error) {
	switch x := data.(type) {
	case int:
		return x, nil
	case int8:
		return int(x), nil
	case int16:
		return int(x), nil
	case int32:
		return int(x), nil
	case int64:
		return int(x), nil
	case uint:
		return int(x), nil
	case uint8:
		return int(x), nil
	case uint16:
		return int(x), nil
	case uint32:
		return int(x), nil
	case uint64:
		return int(x), nil
	case float32:
		return floatToInt(float64(x))
	case float64:
		return floatToInt(x)
	case string:
		return strconv.Atoi(strings.TrimSpace(x))
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", data)
	}
}

// floatToInt converts a YAML float to an int, rejecting fractional values: cross-validation knobs are
// positive integers, so `agreement-threshold: 2.9` is a config error rather than a silent truncation.
func floatToInt(f float64) (int, error) {
	if f != math.Trunc(f) {
		return 0, fmt.Errorf("value %v must be a whole number", f)
	}
	return int(f), nil
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

	// Keep the quorum shape satisfiable. Validate() guarantees the no-caller shape already satisfies
	// threshold/min-groups <= max-participants, so this only fires when a caller asked for more
	// participants than an operator cap allows (the approved cap-loosening): we then lower the caller's
	// threshold to the capped participant count. It never reduces below an operator floor, because the
	// capped participant count is itself >= max-participants' floor >= the threshold/min-groups floors.
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
	// An ENABLED policy forces cross-validation even with no caller headers, so its no-caller resolved
	// shape must be satisfiable on its own. Compute that shape with the same resolveKnob logic Resolve
	// uses and reject if agreement-threshold or min-groups would exceed max-participants — otherwise
	// Resolve would silently clamp them down, violating "floor = operator minimum". (A disabled policy
	// never resolves on its own, so this does not apply.)
	if p.Enabled {
		noCallerMax := resolveKnob(0, false, p.MaxParticipants, defaultEnabledMaxParticipants)
		noCallerThreshold := resolveKnob(0, false, p.AgreementThreshold, defaultEnabledAgreementThreshold)
		noCallerMinGroups := resolveKnob(0, false, p.MinGroups, defaultEnabledMinGroups)
		if noCallerThreshold > noCallerMax {
			return fmt.Errorf("agreement-threshold resolves to %d with no caller but max-participants resolves to %d; an enabled policy must be satisfiable without caller headers (raise max-participants or lower agreement-threshold)", noCallerThreshold, noCallerMax)
		}
		if noCallerMinGroups > noCallerMax {
			return fmt.Errorf("min-groups resolves to %d with no caller but max-participants resolves to %d; an enabled policy must be satisfiable without caller headers (raise max-participants or lower min-groups)", noCallerMinGroups, noCallerMax)
		}
	}
	return nil
}
