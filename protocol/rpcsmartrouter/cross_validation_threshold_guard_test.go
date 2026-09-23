package rpcsmartrouter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// The threshold guard in sendRelayToEndpoint fires when GetSessions fills fewer sessions than the
// agreement threshold. GetSessions has already registered those sessions in UsedProviders by then,
// and the guard used to return without releasing them. Nothing downstream ever did, because no
// relay goroutine is launched for them, so the state machine — which only hands the error back
// once CurrentlyUsed() == 0 — sat on a verdict it already had until the 30-second processing
// deadline fired (MAG-3286). The sibling guard in sendRelayToDirectEndpoints had the same defect
// and the same fix; this is the test that site never got.
//
// The stall itself lives in the state machine, so this function returns promptly either way.
// What gates the stall is CurrentlyUsed, and that is what this asserts — together with the session
// lock and the reserved compute units, which distinguish OnSessionDiscarded from a bare Free.
func TestSendRelayToEndpoint_ThresholdGuardReleasesGatheredSessions(t *testing.T) {
	// Session ids are drawn from the seeded rand package; run alone, nothing else has seeded it.
	rand.InitRandomSeed()
	ctx := context.Background()

	// A real parser and a parsed message: sendRelayToEndpoint reads the api collection, the
	// requested block and the directive headers off the message before it asks for sessions.
	noopHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(
		ctx, "LAVA", spectypes.APIInterfaceRest, noopHandler, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	const restPath = "/cosmos/base/tendermint/v1beta1/blocks/latest"
	chainMessage, err := chainParser.ParseMsg(restPath, nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	relayData := lavaprotocol.NewRelayData(ctx, http.MethodGet, restPath, nil, 0, spectypes.LATEST_BLOCK,
		"rest", chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage),
		common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMsg := chainlib.NewProtocolMessage(chainMessage, nil, relayData, "test", "1.2.3.4")

	// One healthy upstream. It is never asked for a relay — the guard returns first — but it has to
	// answer, so the probe UpdateAllProviders starts in the background cannot take the provider out
	// from under GetSessions.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"block":{"header":{"height":"200"}}}`))
	}))
	defer upstream.Close()
	directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL}, 5, "")
	require.NoError(t, err)

	const providerAddress = "lava@only-provider"
	provider := &lavasession.ConsumerSessionsWithProvider{
		PublicLavaAddress: providerAddress,
		Endpoints: []*lavasession.Endpoint{{
			NetworkAddress:    upstream.URL,
			Enabled:           true,
			Connections:       []*lavasession.EndpointConnection{},
			DirectConnections: []lavasession.DirectRPCConnection{directConn},
		}},
		Sessions:        map[int64]*lavasession.SingleConsumerSession{},
		MaxComputeUnits: 200,
		PairingEpoch:    1,
	}
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "LAVA", ApiInterface: "rest"}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "LAVA")
	sessionManager := lavasession.NewConsumerSessionManager(
		rpcEndpoint, optimizer, nil, "test-router",
		lavasession.NewActiveSubscriptionProvidersStorage())
	require.NoError(t, sessionManager.UpdateAllProviders(1,
		map[uint64]*lavasession.ConsumerSessionsWithProvider{0: provider}, nil))

	// The caller wants a quorum of three. GetSessions fills one, returns it with a nil error, and
	// the guard fires on 1 < 3.
	cvParams := &common.CrossValidationParams{MaxParticipants: 3, AgreementThreshold: 3}
	usedProviders := lavasession.NewUsedProviders(nil)
	sm := &cvGuardStateMachine{usedProviders: usedProviders, cvParams: cvParams, protocolMessage: protocolMsg}
	metricsStub := cvGuardMetrics{}
	relayProcessor := relaycore.NewRelayProcessor(
		ctx, cvParams, metricsStub, metricsStub, lavaprotocol.NewRelayRetriesManager(), sm)

	rpcss := &RPCSmartRouterServer{
		listenEndpoint:    rpcEndpoint,
		chainParser:       chainParser,
		consistencyConfig: relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:    sessionManager,
	}

	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	sendErr := rpcss.sendRelayToEndpoint(callCtx, cvParams.MaxParticipants,
		relaycore.GetEmptyRelayState(callCtx, protocolMsg), relayProcessor, nil, nil)

	require.Error(t, sendErr)
	require.Truef(t, errors.Is(sendErr, lavasession.PairingListEmptyError),
		"the error must wrap PairingListEmptyError so the relay policy stops the state machine instead of retrying with one provider; got: %v", sendErr)
	require.Equal(t, common.CrossValidationReasonInsufficientCapacity, relayProcessor.GetCrossValidationFailFastReason())

	// Prove the guard fired on a session it had actually gathered — an empty GetSessions would take
	// the error branch above the guard, register nothing, and pass the leak assertions for free.
	require.Equal(t, []string{providerAddress}, relayProcessor.GetCrossValidationQueriedProviders(),
		"setup: GetSessions must have handed the guard exactly the one provider")
	require.Len(t, provider.Sessions, 1, "setup: GetSessions must have opened a session on the provider")

	require.Equal(t, 0, usedProviders.CurrentlyUsed(),
		"the gathered session leaked into CurrentlyUsed: validateReturnCondition blocks on this and the request stalls for the full processingTimeout (MAG-3286)")
	require.Equal(t, 0, usedProviders.SessionsLatestBatch(),
		"SessionsLatestBatch leaked: RelayProcessor.checkEndProcessing would wait for a response that is never coming")

	// The session is unlocked again and the compute units GetSessions reserved are back with the
	// provider. A bare Session.Free would satisfy the first and fail the second.
	for _, session := range provider.Sessions {
		_, ok := session.TryUseSession()
		require.True(t, ok, "the gathered session is still locked; it will never be usable again")
		session.Free(nil)
	}
	require.Zero(t, provider.UsedComputeUnits,
		"the compute units reserved for the discarded session were not returned to the provider")
}
