package rpcsmartrouter

import (
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/stretchr/testify/require"
)

// A failing check whose severity is Warning switches nothing off, and is published
// on the alarm metric as scope=none, severity=Warning, so it is seen. It used to be
// logged only (MAG-3950). The Solana node without the token-owner index fails the
// spec's Warning check tokens-owner-indexed.
func TestValidateProviderCollections_AWarningIsPublishedAndSwitchesNothingOff(t *testing.T) {
	up := newFakeUpstreams(t)
	parser := liveParser(t, "SOLANA", true)
	provider := staticProvider("SOLANA", commonlib.NodeUrl{Url: up.httpURL("/n/" + upSolana)})

	admission, err := validateProviderCollections(t.Context(), provider, parser, 20*time.Second)
	require.NoError(t, err)
	require.False(t, admission.Any(), "nothing is refused")
	require.True(t, admission.Equal(chainlib.ProviderAdmission{}), "a warning never moves a session")
	require.Equal(t, []chainlib.Warning{{Check: "tokens-owner-indexed"}}, admission.Warnings())
	require.Equal(t, []metrics.ProviderVerificationFailure{
		{Scope: metrics.VerificationScopeNone, Check: "tokens-owner-indexed", Severity: metrics.VerificationSeverityWarning},
	}, refusalsOf(parser, provider, admission))

	// Recorded, a first admission holding only a warning has not moved the pairing;
	// the stored admission keeps it for the metric.
	admissions := newAdmissionRegistry()
	require.False(t, admissions.record(provider, admission))
	require.Equal(t, admission.Warnings(), admissions.admissionFor(provider).Warnings())
	require.False(t, admissions.recordReverified(provider, chainlib.ProviderAdmission{}), "the warning clearing does not move it either")
	require.Empty(t, admissions.admissionFor(provider).Warnings())

	const spec = "VERIFYWARNING"
	const chainKey = spec + "jsonrpc" // RPCEndpoint.Key()
	inputs := &chainReverifyInputs{
		configuredStatic: []*lavasession.RPCStaticProviderEndpoint{provider},
		refusedFor: func(p *lavasession.RPCStaticProviderEndpoint) []metrics.ProviderVerificationFailure {
			return refusalsOf(parser, p, admission)
		},
	}
	rpsr := newVerificationFailedRouter(t, spec, inputs)
	rpsr.providerSessions[chainKey] = sessionsFor(provider.Name)
	rpsr.publishServingTierLocked(chainKey)
	require.Equal(t, []string{provider.Name + "|static|none||tokens-owner-indexed|Warning"}, failingSeries(t, spec))
}
