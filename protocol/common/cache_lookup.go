package common

import "strings"

// Cache tier and lookup-outcome values. Defined here rather than in protocol/metrics
// because two consumers now share them — the cache_tier/outcome metric labels and the
// per-request Lava-Cache-Tier / Lava-Cache-Outcome headers (MAG-3540) — and a metric
// label that disagreed with the header on the same request would defeat the point of
// having both. protocol/metrics aliases these; protocol/common must not import it.
const (
	CacheTierPrimary   = "primary"
	CacheTierSecondary = "secondary"
	// CacheTierNone is header-only: no tier served this response. It is emitted
	// explicitly rather than by omitting the header, so an absent Lava-Cache-Tier can
	// only mean "router too old / directive not honored" and never "a provider served
	// it". Without it a test could pass having measured nothing.
	CacheTierNone = "none"

	// The four lookup outcomes, shared verbatim with the smartrouter_cache_* metric
	// labels. A tier reporting one of these was actually consulted on this request.
	CacheOutcomeHit     = "hit"
	CacheOutcomeMiss    = "miss"
	CacheOutcomeError   = "error"
	CacheOutcomeTimeout = "timeout"

	// The three not-consulted values. Header-only — no lookup happened, so there is
	// nothing to count and none of these ever labels a metric. They are kept distinct
	// rather than collapsed into one "n/a" because each sends the reader somewhere
	// different when a cache assertion fails.
	//
	// CacheOutcomeOff: the tier is unconfigured or its client reports itself
	// disconnected. The overwhelmingly common cause of a surprising secondary-cache
	// result, and the one an operator can fix.
	CacheOutcomeOff = "off"
	// CacheOutcomeSkipped: the tier is active but was not asked on this request —
	// either a bypass rule applied to both tiers (cross-validation, stateful,
	// lava-force-cache-refresh, a non-cacheable requested block), or the primary
	// already hit and the secondary is only consulted after a primary non-hit.
	CacheOutcomeSkipped = "skipped"
	// CacheOutcomeUnknown: this response was never built on a path that performs a
	// cache lookup at all (a fail-fast result, a synthesized error). Reported rather
	// than guessed "off": the tier may well be live, this response simply carries no
	// observation of it.
	CacheOutcomeUnknown = "unknown"
)

// CacheLookupReport is what the cache tiers did on the attempt that produced a given
// RelayResult, carried on the result so it can be read at reply time.
//
// It exists because the only pre-existing answer to "which tier served request X" was
// the cache_tier metric label, incremented on a detached goroutine AFTER the reply is
// written (`go RecordCacheResult(...)`). Nothing orders the two, so a test reading the
// counter straight after its reply races it and has to poll — which makes correctness a
// matter of timing rather than of router behaviour (MAG-3540). A response header is
// immune by construction: it IS the reply, so "read after the reply" and "read the
// header" are the same instant.
//
// Per-ATTEMPT, not per-request. Retries and hedges each run their own cache lookup in
// their own sendRelayToEndpoint call, and each stamps its own results. The report a
// caller sees therefore describes the lookups made on the attempt whose response won.
// That is what makes it race-free without a lock: every field is written before the
// attempt's relay goroutines start, and read-only thereafter.
type CacheLookupReport struct {
	// ServedTier is CacheTierPrimary, CacheTierSecondary, or CacheTierNone when a
	// provider (or nothing) answered.
	ServedTier string
	// PrimaryOutcome and SecondaryOutcome are one of the four lookup outcomes when the
	// tier was consulted, or one of the three not-consulted values when it was not.
	PrimaryOutcome   string
	SecondaryOutcome string
}

// ServedBy returns a copy naming the tier that answered. A copy rather than a mutation
// so the caller's running report — which the request keeps using when this tier did not
// serve — cannot be left claiming a hit it did not produce.
func (r CacheLookupReport) ServedBy(tier string) CacheLookupReport {
	r.ServedTier = tier
	return r
}

// TierHeaderValue renders the Lava-Cache-Tier value, mapping the zero value onto an
// explicit "none" so the header is never emitted empty.
func (r CacheLookupReport) TierHeaderValue() string {
	if r.ServedTier == "" {
		return CacheTierNone
	}
	return r.ServedTier
}

// OutcomeHeaderValue renders the Lava-Cache-Outcome value as "primary=<x>,secondary=<y>".
//
// Both tiers are always named, always in that order, so a caller can assert the whole
// string rather than parsing for the field it cares about. An unset field renders as
// "unknown" — a result built off the cache path carries no observation of either tier,
// and reporting "off" there would state as fact something this response cannot know.
func (r CacheLookupReport) OutcomeHeaderValue() string {
	var b strings.Builder
	b.WriteString(CacheTierPrimary)
	b.WriteString("=")
	b.WriteString(outcomeOrUnknown(r.PrimaryOutcome))
	b.WriteString(",")
	b.WriteString(CacheTierSecondary)
	b.WriteString("=")
	b.WriteString(outcomeOrUnknown(r.SecondaryOutcome))
	return b.String()
}

func outcomeOrUnknown(outcome string) string {
	if outcome == "" {
		return CacheOutcomeUnknown
	}
	return outcome
}
