package provideroptimizer

import (
	"fmt"
	"strings"
)

// SelectionPriority is a named preset over the four QoS weights that feed
// CalculateScore. It answers "what should the router optimise for", while
// SelectionMode answers "how is the winner picked from those scores" — the two are
// orthogonal and compose:
//
//	--qos-selection-priority fastest --qos-selection-mode best
//	  → sort providers by latency and always send to the head of the queue
//
//	--qos-selection-priority fastest --qos-selection-mode weighted_random
//	  → a lottery biased heavily toward the fastest providers
//
// The preset only sets weights; it adds no scoring machinery. Operators who want
// something other than the four presets can still set the individual
// --qos-*-weight flags, which take precedence over whatever the preset chose.
type SelectionPriority int

const (
	// SelectionPriorityBalanced is the zero value and keeps the historical default
	// weights, so an unset flag changes nothing for existing deployments.
	SelectionPriorityBalanced SelectionPriority = iota

	// SelectionPriorityMostReliable favours the provider that answers successfully
	// most often.
	SelectionPriorityMostReliable

	// SelectionPriorityFastest favours the provider that answers quickest.
	SelectionPriorityFastest

	// SelectionPriorityFreshest favours the provider closest to the head of the chain.
	SelectionPriorityFreshest
)

// priorityWeights holds the four QoS weights a preset applies. Each set sums to 1.0,
// which keeps NewEndpointSelector's normaliser from rescaling them.
type priorityWeights struct {
	availability float64
	latency      float64
	sync         float64
	stake        float64
}

// presetWeights maps each priority to its weights.
//
// The three axis presets are deliberately dominant rather than exclusive: the chosen
// axis takes 0.70, but availability keeps real weight in every one of them so a
// fast-but-flaky or fresh-but-flaky provider cannot outrank a fast-and-solid peer.
// A pure 1.0 on one axis would make availability irrelevant everywhere above
// score.MinAcceptableAvailability (below it the composite collapse in CalculateScore
// still applies, so the floor is never at risk — but 0.80-to-1.00 would stop counting).
//
// The axis presets zero the stake weight. In a static-provider deployment
// CalcWeightsByStake hands every provider an identical weight, so the stake term is the
// same constant for all candidates: it cannot change the ordering, and under
// SelectionModeWeightedRandom it actively flattens the lottery by putting every provider
// on the same pedestal. Spending 20% of the score on a constant would just dilute the
// axis the operator asked for. Balanced keeps its 0.20 stake weight because it is the
// default: changing it would silently move every existing deployment.
var presetWeights = map[SelectionPriority]priorityWeights{
	SelectionPriorityBalanced:     {availability: 0.30, latency: 0.30, sync: 0.20, stake: 0.20},
	SelectionPriorityMostReliable: {availability: 0.70, latency: 0.15, sync: 0.15, stake: 0.00},
	SelectionPriorityFastest:      {availability: 0.20, latency: 0.70, sync: 0.10, stake: 0.00},
	SelectionPriorityFreshest:     {availability: 0.20, latency: 0.10, sync: 0.70, stake: 0.00},
}

func (p SelectionPriority) String() string {
	switch p {
	case SelectionPriorityBalanced:
		return "balanced"
	case SelectionPriorityMostReliable:
		return "most-reliable"
	case SelectionPriorityFastest:
		return "fastest"
	case SelectionPriorityFreshest:
		return "freshest"
	}

	return ""
}

// SelectionPriorityNames lists the accepted spellings, for flag help text.
func SelectionPriorityNames() []string {
	return []string{
		SelectionPriorityBalanced.String(),
		SelectionPriorityMostReliable.String(),
		SelectionPriorityFastest.String(),
		SelectionPriorityFreshest.String(),
	}
}

// ParseSelectionPriority resolves the CLI/config spelling of a priority. Hyphens and
// underscores are interchangeable, so both "most-reliable" and "most_reliable" work.
//
// Unknown values are rejected rather than defaulted, for the same reason as
// ParseSelectionMode: silently falling back to balanced would leave an operator who
// asked for "fastest" running the default weights without ever being told.
//
// An empty string is the one exception — it means "not specified" and resolves to the
// default. Rejecting it would tie startup to the flag being viper-bound: any call site
// that has not bound the flag reads "" and would abort with a confusing "invalid
// selection priority:" rather than simply using the default.
func ParseSelectionPriority(str string) (SelectionPriority, error) {
	if strings.TrimSpace(str) == "" {
		return SelectionPriorityBalanced, nil
	}

	normalized := strings.ReplaceAll(str, "_", "-")
	for _, priority := range []SelectionPriority{
		SelectionPriorityBalanced,
		SelectionPriorityMostReliable,
		SelectionPriorityFastest,
		SelectionPriorityFreshest,
	} {
		if strings.EqualFold(normalized, priority.String()) {
			return priority, nil
		}
	}

	return SelectionPriorityBalanced, fmt.Errorf("invalid selection priority: %s (valid: %s)", str, strings.Join(SelectionPriorityNames(), "|"))
}

// ApplyTo returns cfg with the preset's four QoS weights applied. Every other field —
// SelectionMode, MinSelectionChance, Strategy, the adaptive-max wiring — is left
// untouched, so a priority never silently changes anything but the weighting.
//
// Callers that also honour explicit --qos-*-weight flags must apply those AFTER this,
// so a hand-set weight overrides the preset.
func (p SelectionPriority) ApplyTo(cfg EndpointSelectorConfig) EndpointSelectorConfig {
	weights, ok := presetWeights[p]
	if !ok {
		// Unreachable via ParseSelectionPriority, which rejects unknown values. Guard
		// anyway so a hand-constructed priority degrades to the default rather than
		// zeroing every weight and tripping the normaliser's fallback.
		weights = presetWeights[SelectionPriorityBalanced]
	}

	cfg.AvailabilityWeight = weights.availability
	cfg.LatencyWeight = weights.latency
	cfg.SyncWeight = weights.sync
	cfg.StakeWeight = weights.stake

	return cfg
}
