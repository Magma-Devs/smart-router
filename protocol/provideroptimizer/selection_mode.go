package provideroptimizer

import (
	"fmt"
	"strings"
)

// SelectionMode determines how a winner is picked from the scored candidate list.
// It is orthogonal to Strategy: Strategy shapes *how providers are scored* (which QoS
// axis is emphasised, see applyStrategyAdjustments), while SelectionMode governs only
// *how the winner is drawn* from those scores. Both modes share the identical scoring
// pipeline in CalculateUpstreamScores.
type SelectionMode int

const (
	// SelectionModeWeightedRandom draws a provider with probability proportional to its
	// SelectionWeight (WRS). This is the zero value and the historical behaviour.
	SelectionModeWeightedRandom SelectionMode = iota

	// SelectionModeBest always picks the highest-scoring provider, breaking ties
	// uniformly at random. Deterministic given the scores, so all organic traffic
	// concentrates on the current leader.
	SelectionModeBest
)

func (m SelectionMode) String() string {
	switch m {
	case SelectionModeWeightedRandom:
		return "weighted_random"
	case SelectionModeBest:
		return "best"
	}

	return ""
}

// SelectionModeNames lists the accepted spellings, for flag help text.
func SelectionModeNames() []string {
	return []string{SelectionModeWeightedRandom.String(), SelectionModeBest.String()}
}

// ParseSelectionMode resolves the CLI/config spelling of a selection mode. Hyphens and
// underscores are interchangeable, so both "weighted-random" and "weighted_random" work.
//
// Unknown values are rejected rather than silently defaulted: the mode dispatch treats
// anything that is not SelectionModeBest as weighted-random, so a typo'd flag would
// otherwise leave an operator who asked for "best" quietly running the old policy.
//
// An empty string is the one exception — it means "not specified" and resolves to the
// default. Rejecting it would tie startup to the flag being viper-bound: any call site
// that has not bound the flag reads "" and would abort with a confusing "invalid
// selection mode:" rather than simply using the default.
func ParseSelectionMode(str string) (SelectionMode, error) {
	if strings.TrimSpace(str) == "" {
		return SelectionModeWeightedRandom, nil
	}

	normalized := strings.ReplaceAll(str, "-", "_")
	for _, mode := range []SelectionMode{SelectionModeWeightedRandom, SelectionModeBest} {
		if strings.EqualFold(normalized, mode.String()) {
			return mode, nil
		}
	}

	return SelectionModeWeightedRandom, fmt.Errorf("invalid selection mode: %s (valid: %s)", str, strings.Join(SelectionModeNames(), "|"))
}
