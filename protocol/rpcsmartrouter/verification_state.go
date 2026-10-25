package rpcsmartrouter

import (
	"slices"
	"time"

	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/utils"
)

// trackFailures records what one provider is failing now, each failure with the time it
// started (metrics.VerificationFailuresSince), and logs every change (MAG-3950): one line
// when a check starts failing, an error, or a warning for a Warning check, and one when
// it passes again. A check that takes another's place in the same scope and service, a
// provider out of the pairing that now fails chain-id where it failed connect, is one
// line, naming the check it replaced. Callers hold RPCSmartRouter.mu.
func (inputs *chainReverifyInputs) trackFailures(tier reverifyTier, name string, failures []metrics.ProviderVerificationFailure, now time.Time) []metrics.TimedVerificationFailure {
	key := retryKey(tier, name)
	previous := inputs.failing[key]
	next := metrics.VerificationFailuresSince(previous, failures, now)
	if inputs.failing == nil {
		inputs.failing = map[string][]metrics.TimedVerificationFailure{}
	}
	if len(next) == 0 {
		delete(inputs.failing, key)
	} else {
		inputs.failing[key] = next
	}

	attrs := func(f metrics.TimedVerificationFailure) []utils.Attribute {
		return []utils.Attribute{
			utils.LogAttr("chain", inputs.rpcEndpoint.ChainID),
			utils.LogAttr("apiInterface", inputs.rpcEndpoint.ApiInterface),
			utils.LogAttr("tier", tier.String()),
			utils.LogAttr("provider", name),
			utils.LogAttr("scope", f.Scope),
			utils.LogAttr("service", f.Service),
			utils.LogAttr("check", f.Check),
			utils.LogAttr("severity", f.Severity),
			utils.LogAttr("since", f.Since.UTC().Format(time.RFC3339)),
		}
	}
	for _, f := range next {
		if failingIn(previous, f) {
			continue
		}
		line := attrs(f)
		if replaced, ok := takesThePlaceOf(f, previous, next); ok {
			line = append(line, utils.LogAttr("previousCheck", replaced.Check))
		}
		if f.Severity == metrics.VerificationSeverityWarning {
			utils.LavaFormatWarning("verification check failing", nil, line...)
		} else {
			utils.LavaFormatError("verification check failing", nil, line...)
		}
	}
	for _, f := range previous {
		if failingIn(next, f) {
			continue
		}
		if _, ok := takesThePlaceOf(f, next, previous); ok {
			continue // logged as the check that replaced it
		}
		utils.LavaFormatInfo("verification check passes again", append(attrs(f), utils.LogAttr("failedFor", now.Sub(f.Since).Round(time.Second).String()))...)
	}
	return next
}

func failingIn(set []metrics.TimedVerificationFailure, f metrics.TimedVerificationFailure) bool {
	return slices.ContainsFunc(set, func(o metrics.TimedVerificationFailure) bool {
		return o.ProviderVerificationFailure == f.ProviderVerificationFailure
	})
}

// takesThePlaceOf finds, in others, a failure of another check in f's scope and service
// that is not in mine: the one f replaced, or the one that replaced f. Warnings fail
// independently, so none takes another's place.
func takesThePlaceOf(f metrics.TimedVerificationFailure, others, mine []metrics.TimedVerificationFailure) (metrics.TimedVerificationFailure, bool) {
	if f.Scope == metrics.VerificationScopeNone {
		return metrics.TimedVerificationFailure{}, false
	}
	for _, o := range others {
		if o.Scope == f.Scope && o.Service == f.Service && o.Check != f.Check && !failingIn(mine, o) {
			return o, true
		}
	}
	return metrics.TimedVerificationFailure{}, false
}

// verificationDebugCheck is one failing check on GET /debug/verification, as
// smartrouter_provider_verification_failed publishes it.
type verificationDebugCheck struct {
	Scope    string    `json:"scope"`
	Service  string    `json:"service,omitempty"`
	Check    string    `json:"check"`
	Severity string    `json:"severity"`
	Since    time.Time `json:"since"`
}

// verificationDebugRetry is a provider's place on the retry schedule.
type verificationDebugRetry struct {
	Attempts     int       `json:"attempts"`
	PendingSince time.Time `json:"pendingSince"`
	NextRetryAt  time.Time `json:"nextRetryAt"`
}

type verificationDebugProvider struct {
	Provider  string                   `json:"provider"`
	Tier      string                   `json:"tier"`
	InPairing bool                     `json:"inPairing"`
	Failing   []verificationDebugCheck `json:"failing"`
	Retry     *verificationDebugRetry  `json:"retry,omitempty"`
}

type verificationDebugChain struct {
	Chain        string                      `json:"chain"`
	ApiInterface string                      `json:"apiInterface"`
	Providers    []verificationDebugProvider `json:"providers"`
}

// verificationDebugState is GET /debug/verification (MAG-3950): per chain, what each
// configured provider is failing, with the start times the gauge publishes, and when
// its next verification retry is due. It reads what the router tracks whether or not
// metrics are published.
func (rpsr *RPCSmartRouter) verificationDebugState() map[string]any {
	rpsr.mu.Lock()
	defer rpsr.mu.Unlock()
	chainKeys := make([]string, 0, len(rpsr.reverifyInputs))
	for chainKey := range rpsr.reverifyInputs {
		chainKeys = append(chainKeys, chainKey)
	}
	slices.Sort(chainKeys)
	chains := make([]verificationDebugChain, 0, len(chainKeys))
	for _, chainKey := range chainKeys {
		inputs := rpsr.reverifyInputs[chainKey]
		if inputs == nil || inputs.rpcEndpoint == nil {
			continue
		}
		scheduled := map[string]retryEntry{}
		for _, entry := range inputs.retries.snapshot() {
			scheduled[retryKey(entry.tier, entry.name)] = entry
		}
		chain := verificationDebugChain{Chain: inputs.rpcEndpoint.ChainID, ApiInterface: inputs.rpcEndpoint.ApiInterface, Providers: []verificationDebugProvider{}}
		for _, tier := range []struct {
			tier     reverifyTier
			sessions map[uint64]*lavasession.ConsumerSessionsWithProvider
		}{
			{reverifyTierStatic, rpsr.providerSessions[chainKey]},
			{reverifyTierBackup, rpsr.backupProviderSessions[chainKey]},
		} {
			active := byName(tier.sessions)
			for _, p := range inputs.configuredFor(tier.tier) {
				key := retryKey(tier.tier, p.Name)
				row := verificationDebugProvider{Provider: p.Name, Tier: tier.tier.String(), Failing: []verificationDebugCheck{}}
				_, row.InPairing = active[p.Name]
				for _, f := range inputs.failing[key] {
					row.Failing = append(row.Failing, verificationDebugCheck{Scope: f.Scope, Service: f.Service, Check: f.Check, Severity: f.Severity, Since: f.Since})
				}
				if entry, ok := scheduled[key]; ok {
					row.Retry = &verificationDebugRetry{Attempts: entry.attempts, PendingSince: entry.since, NextRetryAt: entry.due}
				}
				chain.Providers = append(chain.Providers, row)
			}
		}
		chains = append(chains, chain)
	}
	return map[string]any{
		"retryDelay":    VerificationRetryDelay.String(),
		"retryMaxDelay": VerificationRetryMaxDelay.String(),
		"chains":        chains,
	}
}
