package rpcsmartrouter

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

// verificationLogLines returns the buffered log records about one provider's
// verification checks, oldest first.
func verificationLogLines(t *testing.T, provider string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, raw := range utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 0) {
		var record map[string]any
		require.NoError(t, json.Unmarshal(raw, &record))
		if record["provider"] != provider {
			continue
		}
		if message, _ := record["message"].(string); message == "verification check failing" || message == "verification check passes again" {
			out = append(out, record)
		}
	}
	return out
}

// One line when a check starts failing and one when it passes again, not one per pass. A
// check that takes another's place is one line naming it, and the failure keeps its start
// (MAG-4505 comment 30715: "one error log line when a check starts failing, one info line
// when it passes again").
func TestTrackFailures_LogsEachTransitionOnce(t *testing.T) {
	utils.EnableDebugLogBuffer(1000)
	defer utils.ClearDebugLogBuffer()
	utils.ClearDebugLogBuffer()
	const provider = "track-logs-provider"
	inputs := &chainReverifyInputs{rpcEndpoint: bootTestEndpoint()}
	start := time.Date(2026, 10, 25, 20, 0, 0, 0, time.UTC)
	out := func(check string) []metrics.ProviderVerificationFailure {
		return []metrics.ProviderVerificationFailure{{Scope: metrics.VerificationScopeProvider, Check: check}}
	}

	inputs.trackFailures(reverifyTierStatic, provider, out(chainlib.CheckConnect), start)
	inputs.trackFailures(reverifyTierStatic, provider, out(chainlib.CheckConnect), start.Add(time.Minute))
	tracked := inputs.trackFailures(reverifyTierStatic, provider, out("chain-id"), start.Add(2*time.Minute))
	require.Equal(t, start, tracked[0].Since, "a provider that moves to another check keeps when it started failing")
	inputs.trackFailures(reverifyTierStatic, provider, nil, start.Add(5*time.Minute))
	inputs.trackFailures(reverifyTierStatic, provider, []metrics.ProviderVerificationFailure{
		{Scope: metrics.VerificationScopeNone, Check: "tokens-owner-indexed", Severity: metrics.VerificationSeverityWarning},
	}, start.Add(6*time.Minute))

	lines := verificationLogLines(t, provider)
	require.Len(t, lines, 4)
	require.Equal(t, "error", lines[0]["level"])
	require.Equal(t, "verification check failing", lines[0]["message"])
	require.Equal(t, chainlib.CheckConnect, lines[0]["check"])

	require.Equal(t, "chain-id", lines[1]["check"], "the change of check is one line")
	require.Equal(t, chainlib.CheckConnect, lines[1]["previousCheck"])
	require.Equal(t, start.Format(time.RFC3339), lines[1]["since"])

	require.Equal(t, "info", lines[2]["level"])
	require.Equal(t, "verification check passes again", lines[2]["message"])
	require.Equal(t, "chain-id", lines[2]["check"])
	require.Equal(t, "5m0s", lines[2]["failedFor"])

	require.Equal(t, "warn", lines[3]["level"], "a Warning check failing is a warning, not an error")
	require.Empty(t, inputs.failing["static|"+provider][0].Service)
}

// A provider out of the pairing is listed by the check that took it out alone, but it
// did not pass what else it failed (review of #602). Its missing websocket url logs no
// "passes again" when it goes out and no second line when it is back, and keeps its
// start; an archive check that it passes when it is back passes again then, counted
// from its own start.
func TestTrackFailures_AProviderOutOfThePairingPassesNothingItFailed(t *testing.T) {
	utils.EnableDebugLogBuffer(1000)
	defer utils.ClearDebugLogBuffer()
	utils.ClearDebugLogBuffer()
	const provider = "track-held-provider"
	inputs := &chainReverifyInputs{rpcEndpoint: bootTestEndpoint()}
	start := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	missing := metrics.ProviderVerificationFailure{Scope: metrics.VerificationScopeService, Service: chainlib.WebSocketExtension, Check: chainlib.CheckMissingURL}
	archive := metrics.ProviderVerificationFailure{Scope: metrics.VerificationScopeService, Service: "archive", Check: "pruning"}
	out := []metrics.ProviderVerificationFailure{{Scope: metrics.VerificationScopeProvider, Check: "chain-id"}}

	inputs.trackFailures(reverifyTierStatic, provider, []metrics.ProviderVerificationFailure{missing, archive}, start)
	tracked := inputs.trackFailures(reverifyTierStatic, provider, out, start.Add(20*time.Minute))
	require.Len(t, tracked, 1, "while it is out, the provider is published by the check that took it out")
	inputs.trackFailures(reverifyTierStatic, provider, out, start.Add(21*time.Minute))
	tracked = inputs.trackFailures(reverifyTierStatic, provider, []metrics.ProviderVerificationFailure{missing}, start.Add(25*time.Minute))

	require.Len(t, tracked, 1)
	require.Equal(t, chainlib.CheckMissingURL, tracked[0].Check)
	require.Equal(t, start, tracked[0].Since, "the url is missing since the start, not since the readmit")
	require.Empty(t, inputs.heldWhileOut, "nothing is held once the provider is back")

	lines := verificationLogLines(t, provider)
	checks := make([]any, 0, len(lines))
	for _, line := range lines {
		checks = append(checks, line["check"])
	}
	require.Equal(t, []any{chainlib.CheckMissingURL, "pruning", "chain-id", "chain-id", "pruning"}, checks,
		"the start fails missing-url and pruning, the demote fails chain-id alone, the readmit passes chain-id and pruning")
	require.Equal(t, "verification check failing", lines[2]["message"])
	require.Nil(t, lines[2]["previousCheck"], "chain-id takes no part's place")
	require.Equal(t, "verification check passes again", lines[3]["message"])
	require.Equal(t, "5m0s", lines[3]["failedFor"])
	require.Equal(t, "verification check passes again", lines[4]["message"])
	require.Equal(t, "25m0s", lines[4]["failedFor"], "pruning failed from the start until the readmit")
}

// GET /debug/verification shows, per configured provider, what it is failing with the
// start the gauge publishes, whether it is in the pairing, and each check's next retry.
func TestDebugVerification_ShowsFailingChecksAndRetries(t *testing.T) {
	const spec = "VERIFYDEBUG"
	const chainKey = spec + "jsonrpc" // RPCEndpoint.Key()
	out, served := &routersession.RPCStaticProviderEndpoint{Name: "out"}, &routersession.RPCStaticProviderEndpoint{Name: "served"}
	inputs := &chainReverifyInputs{
		configuredStatic: []*routersession.RPCStaticProviderEndpoint{out, served},
		refusedFor: func(p *routersession.RPCStaticProviderEndpoint) []metrics.ProviderVerificationFailure {
			if p.Name == "served" {
				return []metrics.ProviderVerificationFailure{{Scope: metrics.VerificationScopeService, Service: "archive", Check: "pruning"}}
			}
			return nil
		},
		retries: newRetrySchedule(),
	}
	rpsr := newVerificationFailedRouter(t, spec, inputs)
	rpsr.providerSessions[chainKey] = sessionsFor("served")
	inputs.noteFailure(reverifyTierStatic, "out", &chainlib.VerificationFailure{Check: "chain-id", Err: errors.New("0x89")})
	chainID := chainlib.CheckID{Name: "chain-id"}
	inputs.retries.reconcile(reverifyTierStatic, "out", nil, []chainlib.CheckID{chainID}, time.Time{}, true)
	inputs.retries.reconcile(reverifyTierStatic, "served", nil, []chainlib.CheckID{{Name: "pruning", Extension: "archive"}, {}}, time.Time{}, true)
	rpsr.publishServingTierLocked(chainKey)

	state := getVerificationDebugState(t, rpsr)
	require.Equal(t, VerificationRetryDelay.String(), state.RetryDelay)
	require.Len(t, state.Chains, 1)
	providers := state.Chains[0].Providers
	require.Len(t, providers, 2)

	require.Equal(t, "out", providers[0].Provider)
	require.False(t, providers[0].InPairing)
	require.Len(t, providers[0].Failing, 1)
	require.Equal(t, "chain-id", providers[0].Failing[0].Check)
	require.Equal(t, metrics.VerificationSeverityFail, providers[0].Failing[0].Severity)
	require.Len(t, providers[0].Retries, 1)
	require.Equal(t, "chain-id", providers[0].Retries[0].Check)
	require.Equal(t, 1, providers[0].Retries[0].Attempts)
	require.False(t, providers[0].Retries[0].WholePass)

	require.Equal(t, "served", providers[1].Provider)
	require.True(t, providers[1].InPairing)
	require.Equal(t, "archive", providers[1].Failing[0].Service)
	require.Len(t, providers[1].Retries, 2, "each check on its own schedule")
	var archive, whole bool
	for _, retry := range providers[1].Retries {
		archive = archive || (retry.Check == "pruning" && retry.Extension == "archive" && retry.Addon == "")
		whole = whole || (retry.WholePass && retry.Check == "")
	}
	require.True(t, archive, "the archive check, named by its verification and extension")
	require.True(t, whole, "a failure no check owns retries the whole pass")

	gauge := verificationFailedSeries(t, spec)
	require.Equal(t, float64(providers[0].Failing[0].Since.Unix()), gauge["out|static|provider||chain-id|Fail"], "the debug endpoint and the gauge agree on the start")
	require.Equal(t, float64(providers[1].Failing[0].Since.Unix()), gauge["served|static|service|archive|pruning|PartialFail"])
}

// With metrics off (--metrics-listen-address disabled, the default), the router still
// tracks what its providers fail: the logs and the debug endpoint do not depend on it.
func TestDebugVerification_WorksWithMetricsOff(t *testing.T) {
	rpsr := createTestRPCSmartRouter()
	_, endpoint := createTestSessionManager("BSC", "jsonrpc")
	chainKey := endpoint.Key()
	inputs := &chainReverifyInputs{rpcEndpoint: endpoint, configuredStatic: bootTestProviders("out")}
	rpsr.reverifyInputs = map[string]*chainReverifyInputs{chainKey: inputs}
	inputs.noteFailure(reverifyTierStatic, "out", &chainlib.VerificationFailure{Check: chainlib.CheckConnect, Err: errors.New("refused")})
	require.Nil(t, rpsr.smartRouterMetricsManager)
	rpsr.publishServingTierLocked(chainKey)

	state := getVerificationDebugState(t, rpsr)
	require.Len(t, state.Chains, 1)
	require.Equal(t, chainlib.CheckConnect, state.Chains[0].Providers[0].Failing[0].Check)
}

type verificationDebugBody struct {
	RetryDelay    string                   `json:"retryDelay"`
	RetryMaxDelay string                   `json:"retryMaxDelay"`
	Chains        []verificationDebugChain `json:"chains"`
}

func getVerificationDebugState(t *testing.T, rpsr *RPCSmartRouter) verificationDebugBody {
	t.Helper()
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, router: rpsr})
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/debug/verification", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	var body verificationDebugBody
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &body))
	return body
}
