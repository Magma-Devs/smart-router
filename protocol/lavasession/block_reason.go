package lavasession

import "time"

// BlockReason names WHY a provider was removed from routing (MAG-2599).
//
// Every provider block in this package carries one. Before these existed, all five block paths
// emitted the same log line and stored the same thing — an address in a []string — so an operator
// looking at an idle provider could see THAT it was out but never WHY, and the answer differed
// enough between paths to change what you should do about it: a provider blocked for
// `too-many-dead-sessions` is never reported, so the 30-second reconnect loop can never release it,
// while one blocked for `all-endpoints-disabled` is.
//
// The strings are operator-facing: they appear as a `reason` metric label, as the `block_reason`
// log field, and in /debug/provider-routing. Two rules keep them coherent as the list grows:
//
//   - A reason says what HAPPENED, not which counter tripped. "too-many-dead-sessions", not
//     "blocklisted-session-cap" — nobody outside this package knows what a blocklisted session is.
//   - They are part of the operator contract. Renaming one breaks dashboards and log queries, so
//     prefer adding a new value over redefining an existing one.
type BlockReason string

const (
	// BlockReasonAllEndpointsDisabled — every URL this provider has failed too many times in a
	// row, so there is nothing left to dial. Reported, so the reconnect loop can release it.
	BlockReasonAllEndpointsDisabled BlockReason = "all-endpoints-disabled"

	// BlockReasonTooManyDeadSessions — too many of this provider's sessions were retired, hitting
	// the per-provider allowance. Deliberately NOT reported, so the reconnect loop never sees it.
	BlockReasonTooManyDeadSessions BlockReason = "too-many-dead-sessions"

	// BlockReasonNeverServed — a session failed past its error budget and the provider has never
	// completed a successful relay. Narrow by construction: a provider that served earlier and is
	// failing everything now is not caught by this.
	BlockReasonNeverServed BlockReason = "never-served-successfully"

	// BlockReasonExplicitSignal — a caller returned BlockProviderError / ReportAndBlockProviderError
	// and asked for the block directly. No production code produces those sentinels today, so this
	// appearing in a log or on a dashboard means someone added a producer.
	BlockReasonExplicitSignal BlockReason = "explicit-block-signal"

	// BlockReasonPreviousEpoch — carried across an epoch boundary rather than newly decided. Only
	// used when the original record could not be recovered; the normal path preserves the real
	// reason and records the carry-over in BlockRecord.Detail instead.
	BlockReasonPreviousEpoch BlockReason = "blocked-in-previous-epoch"

	// BlockReasonUnspecified — a block was recorded without a reason. This is a bug: every call
	// site names one. It exists so a missing reason is visibly wrong rather than silently empty.
	BlockReasonUnspecified BlockReason = "unspecified"
)

// allBlockReasons is the shared backing array. A package-level var rather than a fresh slice per
// call because the state-size publisher reads it on every tick, per chain and api-interface.
var allBlockReasons = []BlockReason{
	BlockReasonAllEndpointsDisabled,
	BlockReasonTooManyDeadSessions,
	BlockReasonNeverServed,
	BlockReasonExplicitSignal,
	BlockReasonPreviousEpoch,
	BlockReasonUnspecified,
}

// AllBlockReasons lists every reason a block can carry, so the per-reason gauge can publish a zero
// for the ones not currently in use and stay self-correcting. Order is stable for test readability.
//
// Keep in sync with the constants above — a reason missing here is a series that never returns to 0
// once it has fired, which is the exact failure the gauge is shaped to avoid.
// TestBlockReasons_ListCoversEveryDeclaredConstant guards that.
//
// The returned slice is shared; callers must not mutate it.
func AllBlockReasons() []BlockReason { return allBlockReasons }

// ReleaseRoute names WHO returned a blocked provider to routing (MAG-2599).
//
// There are more ways back into the pool than out of it, and they can undo each other — the health
// probe in particular can release a provider on cheap-poll evidence moments after real relay traffic
// blocked it. Naming the route is what makes that visible in a log instead of appearing as a
// provider that mysteriously recovered.
//
// The route names the ACTOR, not the state change: "health-probe", not "probe-restored-provider".
// The question it answers is "who did this to my routing".
type ReleaseRoute string

const (
	// ReleaseSecondChanceTimer — the 3-minute timer blockProvider starts when it grants a second
	// chance instead of reporting.
	ReleaseSecondChanceTimer ReleaseRoute = "second-chance-timer"

	// ReleaseHealthProbe — the proactive prober re-enabled an endpoint and restored the provider.
	// The fastest route back, and the only one whose evidence is polls rather than real relays.
	ReleaseHealthProbe ReleaseRoute = "health-probe"

	// ReleaseReconnectLoop — the 30-second reported-providers reconnect ticker.
	ReleaseReconnectLoop ReleaseRoute = "reconnect-loop"

	// ReleaseSuccessfulRelay — the provider was tried as a last resort and served, proving itself.
	ReleaseSuccessfulRelay ReleaseRoute = "successful-relay"

	// ReleaseEpochNotReported — epoch transition: re-blocked from the previous epoch but never
	// reported, so unblocked immediately without a probe.
	ReleaseEpochNotReported ReleaseRoute = "epoch-not-reported"

	// ReleaseEpochProbe — epoch transition: re-blocked and reported, then passed its probe.
	ReleaseEpochProbe ReleaseRoute = "epoch-probe"

	// ReleasePoolEmpty — the last resort. Nothing (primary or backup) could serve the request, so
	// the whole blocked list was released at once.
	ReleasePoolEmpty ReleaseRoute = "pool-empty-release"

	// ReleaseOperatorReset — /debug/reset-all.
	ReleaseOperatorReset ReleaseRoute = "operator-reset"

	// ReleaseEpochRebuild — the pairing was rebuilt for a new epoch. Not a decision about this
	// provider: everything returns to the valid list first, and the carried-over blocks are then
	// re-applied on top.
	ReleaseEpochRebuild ReleaseRoute = "epoch-rebuild"
)

// BlockRecord is everything known at the moment a provider was blocked.
//
// Reported and SecondChanceGranted are stored rather than recomputed because they decide WHICH
// recovery routes can ever fire, which is the immediate follow-up question after "why":
//
//	Reported == false            → the 30-second reconnect loop will never look at it
//	SecondChanceGranted == true  → it comes back on its own in retrySecondChanceAfter
//
// Guarded by ConsumerSessionManager.lock, like the blocked lists it describes.
type BlockRecord struct {
	// Reason is why the provider was blocked. Never empty in production.
	Reason BlockReason
	// Since is when this block was decided. Preserved across epoch carry-over, so it measures how
	// long the provider has really been out rather than how long since the last epoch tick.
	Since time.Time
	// Detail is the discriminating number from the call site ("disconnections=50",
	// "consecutiveErrors=16"), plus a carry-over marker when the block survived an epoch. Free-form
	// and for humans — never parse it.
	Detail string
	// Reported records whether the provider was ACTUALLY added to the reported-providers register —
	// not merely whether reporting was requested. A first offence that takes a second chance is not
	// reported, so the two differ, and Reported and SecondChanceGranted are mutually exclusive.
	Reported bool
	// SecondChanceGranted records whether a second-chance timer was actually started — not merely
	// whether one was allowed. A provider that had already used its second chance is reported
	// instead, and does not come back on a timer.
	SecondChanceGranted bool
	// Backup is true when the provider was blocked as a backup (blockedBackupProviders) rather than
	// as a regular provider (currentlyBlockedProviderAddresses). A provider configured in both
	// pools can be blocked in either — and when one of the two is released, this is updated to
	// describe the block that still stands.
	Backup bool
	// Carries counts how many epoch transitions this block has survived. A counter rather than a
	// trail appended to Detail: a long-lived block would otherwise grow that string without bound,
	// and it is rendered in /debug/provider-routing.
	Carries uint32
}

// blockedFor reports how long the provider has been blocked as of now. Zero when Since is unset,
// so a record from before this field existed reads as 0 rather than as ~55 years.
func (r BlockRecord) blockedFor(now time.Time) time.Duration {
	if r.Since.IsZero() {
		return 0
	}
	return now.Sub(r.Since)
}

// withCarryOver returns a copy of the record marked as having survived an epoch transition. The
// original Reason and Since are preserved: the provider has been out since it was first blocked,
// and saying "blocked-in-previous-epoch" would replace the real answer with a description of the
// bookkeeping.
//
// Detail is deliberately left alone. An earlier version appended "carried into epoch N" here, which
// grows without bound for a long-lived block — currently masked only because the epoch releases
// almost everything within ~500ms, so nothing carries twice. Fixing that (the epoch clean-slate
// defect) would have turned the growth on. Carries answers the same question in fixed space.
func (r BlockRecord) withCarryOver() BlockRecord {
	if r.Reason == "" {
		r.Reason = BlockReasonPreviousEpoch
	}
	r.Carries++
	return r
}
