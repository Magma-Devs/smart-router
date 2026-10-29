package rpcsmartrouter

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

// A provider with no websocket url on a chain that subscribes boots into the pairing
// and raises one alarm: an error line naming the websocket's missing-url, and the same
// failure on GET /debug/verification (MAG-4505 comment 30715 point 5). It is not put on
// the retry schedule, since no retry can give it a url, and an epoch pass keeps it
// serving without logging the failure again.
func TestNoWebsocketURL_ServesAndRaisesOneAlarm(t *testing.T) {
	utils.EnableDebugLogBuffer(1000)
	defer utils.ClearDebugLogBuffer()
	utils.ClearDebugLogBuffer()
	up := newFakeUpstreams(t)
	provider := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL("/nows-boot/" + upArchive)})
	provider.Name = "https-only"
	rig := newPairedRetryRig(t, provider)
	rig.inputs.refusedFor = func(p *lavasession.RPCStaticProviderEndpoint) []metrics.ProviderVerificationFailure {
		return refusalsOf(rig.inputs.chainParser, p, rig.admissions.admissionFor(p))
	}
	require.False(t, rig.pending(), "a missing url is configuration: no retry can fix it")

	rig.rpsr.mu.Lock()
	rig.rpsr.publishServingTierLocked(rig.chainKey)
	rig.rpsr.mu.Unlock()

	lines := verificationLogLines(t, provider.Name)
	require.Len(t, lines, 1)
	require.Equal(t, "error", lines[0]["level"])
	require.Equal(t, metrics.VerificationScopeService, lines[0]["scope"])
	require.Equal(t, chainlib.WebSocketExtension, lines[0]["service"])
	require.Equal(t, chainlib.CheckMissingURL, lines[0]["check"])
	require.Equal(t, metrics.VerificationSeverityPartialFail, lines[0]["severity"])

	state := getVerificationDebugState(t, rig.rpsr)
	require.Len(t, state.Chains, 1)
	require.Len(t, state.Chains[0].Providers, 1)
	served := state.Chains[0].Providers[0]
	require.True(t, served.InPairing, "it serves over http")
	require.Len(t, served.Failing, 1)
	require.Equal(t, chainlib.WebSocketExtension, served.Failing[0].Service)
	require.Equal(t, chainlib.CheckMissingURL, served.Failing[0].Check)
	require.Nil(t, served.Retry)

	rig.rpsr.updateEpoch(context.Background(), rig.rpsr.epochTimer.GetCurrentEpoch())
	require.NotNil(t, rig.session(t, provider.Name), "the epoch keeps it serving")
	require.Len(t, verificationLogLines(t, provider.Name), 1, "still missing, so no new line")
	require.False(t, rig.pending())
}

// health admits a provider with no websocket url on a chain that subscribes, and each
// of its admitted rows says it serves no subscriptions, with the flag or without it. It
// used to exclude the provider unless health ran with the flag.
func TestHealth_NoWebsocketURLIsAdmittedWithoutSubscriptions(t *testing.T) {
	up := newFakeUpstreams(t)
	for _, skipWebsocket := range []bool{false, true} {
		rows := healthProbe(t, "ETH1", skipWebsocket, 20*time.Second, commonlib.NodeUrl{Url: up.httpURL("/nows-health/" + upArchive)})
		require.Len(t, rows, 1)
		require.True(t, rows[0].Ok, "the url passes its own checks")
		require.Equal(t, routerAdmitted, rows[0].Router.Verdict, "skip-websocket-verification=%t", skipWebsocket)
		require.Equal(t, noWebsocketReason, rows[0].Router.Reason)

		rows = healthProbe(t, "ETH1", skipWebsocket, 20*time.Second,
			commonlib.NodeUrl{Url: up.httpURL("/nows-health/" + upArchive)},
			commonlib.NodeUrl{Url: up.wsURL("/nows-health/" + upArchive)},
		)
		require.Len(t, rows, 2)
		for _, row := range rows {
			require.Equal(t, routerAdmitted, row.Router.Verdict)
			require.Empty(t, row.Router.Reason, "a provider with a socket serves its subscriptions")
		}
	}

	rows := healthProbe(t, "ETH1", false, 20*time.Second, commonlib.NodeUrl{Url: up.httpURL("/nows-health/" + upPolygon)})
	require.Equal(t, routerExcluded, rows[0].Router.Verdict, "a wrong chain is still excluded")
	require.NotContains(t, rows[0].Router.Reason, noWebsocketReason)
}

// A missing websocket url is not a failed run, since no check ran: the counter leaves it
// out, while the gauge holds it for as long as it lasts.
func TestCountRefusals_LeavesOutAMissingWebsocketURL(t *testing.T) {
	up := newFakeUpstreams(t)
	parser := liveParser(t, "ETH1", true)
	var counted []string
	inputs := &chainReverifyInputs{
		chainParser: parser,
		countFailedCheck: func(_ reverifyTier, provider string, failure metrics.ProviderVerificationFailure) {
			counted = append(counted, provider+"|"+failure.Scope+"|"+failure.Service+"|"+failure.Check)
		},
	}
	provider := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL("/nows-count/" + upPruned), Addons: []string{"archive"}})
	admission, err := validateProviderCollections(t.Context(), provider, parser, 20*time.Second)
	require.NoError(t, err)
	require.Len(t, refusalsOf(parser, provider, admission), 2, "archive is refused, and there is no websocket url")

	inputs.countRefusals(reverifyTierStatic, provider, admission)
	require.Equal(t, []string{"p|service|archive|pruning"}, counted)
}
