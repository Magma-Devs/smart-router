package rpcsmartrouter

import (
	"slices"
	"strings"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/magma-Devs/smart-router/utils"
)

// trackFailures records what one provider is failing now, each failure with the time it
// started (metrics.VerificationFailuresSince), and logs every change (MAG-3950): one line
// when a check starts failing, an error, or a warning for a Warning check and for a
// missing websocket url, which the config sets and no retry can fix (decided on #602),
// and one when it passes again. A check that takes another's place in the same scope and service, a
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
		if f.Severity == metrics.VerificationSeverityWarning || f.Check == chainlib.CheckMissingURL {
			utils.FormatWarning("verification check failing", nil, line...)
		} else {
			utils.FormatError("verification check failing", nil, line...)
		}
	}
	for _, f := range previous {
		if failingIn(next, f) {
			continue
		}
		if _, ok := takesThePlaceOf(f, next, previous); ok {
			continue // logged as the check that replaced it
		}
		utils.FormatInfo("verification check passes again", append(attrs(f), utils.LogAttr("failedFor", now.Sub(f.Since).Round(time.Second).String()))...)
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

// verificationDebugRetry is one check of a provider on the retry schedule: the spec
// verification's name and the add-on and extension its value covers, or wholePass for a
// failure no single check owns (a url that could not be dialed), which re-runs them all.
type verificationDebugRetry struct {
	Check        string    `json:"check,omitempty"`
	Addon        string    `json:"addon,omitempty"`
	Extension    string    `json:"extension,omitempty"`
	WholePass    bool      `json:"wholePass,omitempty"`
	Attempts     int       `json:"attempts"`
	PendingSince time.Time `json:"pendingSince"`
	NextRetryAt  time.Time `json:"nextRetryAt"`
}

type verificationDebugProvider struct {
	Provider  string                   `json:"provider"`
	Tier      string                   `json:"tier"`
	InPairing bool                     `json:"inPairing"`
	Failing   []verificationDebugCheck `json:"failing"`
	// Retries is each check of the provider on the retry schedule, the earliest due first.
	Retries []verificationDebugRetry `json:"retries"`
}

type verificationDebugChain struct {
	Chain        string                      `json:"chain"`
	ApiInterface string                      `json:"apiInterface"`
	Providers    []verificationDebugProvider `json:"providers"`
}

// verificationDebugState is GET /debug/verification (MAG-3950): per chain, what each
// configured provider is failing, with the start times the gauge publishes, and when
// each check it is failing is next retried. It reads what the router tracks whether or
// not metrics are published.
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
		scheduled := map[string][]retryEntry{}
		for _, entry := range inputs.retries.snapshot() {
			key := retryKey(entry.tier, entry.name)
			scheduled[key] = append(scheduled[key], entry)
		}
		chain := verificationDebugChain{Chain: inputs.rpcEndpoint.ChainID, ApiInterface: inputs.rpcEndpoint.ApiInterface, Providers: []verificationDebugProvider{}}
		for _, tier := range []struct {
			tier     reverifyTier
			sessions map[uint64]*routersession.ConsumerSessionsWithProvider
		}{
			{reverifyTierStatic, rpsr.providerSessions[chainKey]},
			{reverifyTierBackup, rpsr.backupProviderSessions[chainKey]},
		} {
			active := byName(tier.sessions)
			for _, p := range inputs.configuredFor(tier.tier) {
				key := retryKey(tier.tier, p.Name)
				row := verificationDebugProvider{Provider: p.Name, Tier: tier.tier.String(), Failing: []verificationDebugCheck{}, Retries: []verificationDebugRetry{}}
				_, row.InPairing = active[p.Name]
				for _, f := range inputs.failing[key] {
					row.Failing = append(row.Failing, verificationDebugCheck{Scope: f.Scope, Service: f.Service, Check: f.Check, Severity: f.Severity, Since: f.Since})
				}
				entries := scheduled[key]
				slices.SortFunc(entries, func(a, b retryEntry) int {
					if c := a.due.Compare(b.due); c != 0 {
						return c
					}
					return strings.Compare(a.check.Name+"\x00"+a.check.Addon+"\x00"+a.check.Extension, b.check.Name+"\x00"+b.check.Addon+"\x00"+b.check.Extension)
				})
				for _, entry := range entries {
					row.Retries = append(row.Retries, verificationDebugRetry{
						Check: entry.check.Name, Addon: entry.check.Addon, Extension: entry.check.Extension, WholePass: entry.check.WholePass(),
						Attempts: entry.attempts, PendingSince: entry.since, NextRetryAt: entry.due,
					})
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
