package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/qos"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// archiveRequest asks for the archive extension. When no archive provider is left,
// sendRelayToEndpoint degrades to regular providers and takes the sessions under the plain key.
type archiveRequest struct{ *MockProtocolMessage }

func (archiveRequest) GetExtensions() []*spectypes.Extension {
	return []*spectypes.Extension{{Name: "archive"}}
}

// rejectEveryDegradedSession takes two sessions for msg under the plain key, as a request that
// degraded does, and sends them with both endpoints far behind the chain tip. The consistency
// filter rejects both and no relay is launched, so the release is the only thing that touches the
// request's dispatch history.
func rejectEveryDegradedSession(t *testing.T, msg archiveRequest) *relaycore.RelayProcessor {
	t.Helper()
	ctx := context.Background()

	noopHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(
		ctx, "LAVA", spectypes.APIInterfaceRest, noopHandler, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)

	chainTip := seedChainTip(200)
	endpointtip.Default().Reset()
	staleEp1 := &lavasession.Endpoint{NetworkAddress: "http://stale1:8545"}
	seedEndpointTip("LAVA", "rest", "http://stale1:8545", 50)
	staleEp2 := &lavasession.Endpoint{NetworkAddress: "http://stale2:8545"}
	seedEndpointTip("LAVA", "rest", "http://stale2:8545", 50)

	mkSession := func(addr string, ep *lavasession.Endpoint) *lavasession.SingleConsumerSession {
		return &lavasession.SingleConsumerSession{
			Parent:     &lavasession.ConsumerSessionsWithProvider{PublicLavaAddress: addr, Endpoints: []*lavasession.Endpoint{ep}},
			Connection: &lavasession.DirectRPCSessionConnection{Endpoint: ep},
			QoSManager: qos.NewQoSManager(),
		}
	}
	sess1 := mkSession("lava@provider1", staleEp1)
	sess2 := mkSession("lava@provider2", staleEp2)
	for _, s := range []*lavasession.SingleConsumerSession{sess1, sess2} {
		_, ok := s.TryUseSession()
		require.True(t, ok, "test setup: failed to lock session")
	}

	// In GetSessions order: the key goes on each session first, then AddUsed files the providers
	// under it. The key is the plain one, because the request degraded.
	usedProviders := lavasession.NewUsedProviders(nil)
	plainKey := lavasession.NewRouterKey(nil)
	require.NoError(t, sess1.SetUsageForSession(0, nil, usedProviders, plainKey))
	require.NoError(t, sess2.SetUsageForSession(0, nil, usedProviders, plainKey))
	sessionsMap := lavasession.ConsumerSessionsMap{
		"lava@provider1": &lavasession.SessionInfo{Session: sess1},
		"lava@provider2": &lavasession.SessionInfo{Session: sess2},
	}
	usedProviders.AddUsed(sessionsMap, nil)
	require.ElementsMatch(t, []string{"lava@provider1", "lava@provider2"}, usedProviders.DispatchedProviders())

	require.NotEqual(t, plainKey.String(), lavasession.NewRouterKeyFromExtensions(msg.GetExtensions()).String(),
		"setup: the request's key must differ from the key its sessions were taken under, or this measures nothing")

	sm := &budgetCallSiteStateMachine{usedProviders: usedProviders, protocolMessage: msg}
	relayProcessor := relaycore.NewRelayProcessor(ctx, nil, cvGuardMetrics{}, cvGuardMetrics{}, sm)
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "LAVA", ApiInterface: "rest"}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "LAVA")
	rpcss := &RPCSmartRouterServer{
		listenEndpoint:    rpcEndpoint,
		chainParser:       chainParser,
		consistencyConfig: relaycore.DefaultConsistencyValidationConfig(),
		chainState:        chainTip,
		sessionManager: lavasession.NewConsumerSessionManager(
			rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage()),
	}

	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = rpcss.sendRelayToDirectEndpoints(callCtx, sessionsMap, msg, relayProcessor, nil, nil, common.CacheLookupReport{})
	require.ErrorIs(t, err, lavasession.ConsistencyPreValidationError, "setup: both sessions must have been rejected as stale")
	return relayProcessor
}

// A session the consistency filter rejects was never asked, so it must leave the request's
// dispatch history. UsedProviders filed it under the key it was taken with, and on the degraded
// path that is the plain key, not the request's. A release under the request's key found nothing,
// so the provider stayed in the history, and Lava-Retries then counted an attempt at a node that
// never received the request (MAG-3762).
func TestSendRelayToDirectEndpoints_ReleasesADegradedSessionUnderItsOwnKey(t *testing.T) {
	relayProcessor := rejectEveryDegradedSession(t, archiveRequest{&MockProtocolMessage{
		api:            &spectypes.Api{Name: "/cosmos/base/tendermint/v1beta1/blocks/latest"},
		requestedBlock: spectypes.LATEST_BLOCK,
		userData:       common.UserData{DappId: "test", ConsumerIp: "1.2.3.4"},
	}})

	usedProviders := relayProcessor.GetUsedProviders()
	require.Empty(t, usedProviders.DispatchedProviders(),
		"a rejected session was never asked, so it must not stay in the history Lava-Retries counts")
	require.Equal(t, 0, usedProviders.SessionsLatestBatch())
	require.Equal(t, 0, usedProviders.CurrentlyUsed())
}

// The write verdict reads the same history, through SessionsDispatched. With the rejected nodes
// left in it, a write that reached no node read as one sent to two nodes that never answered, and
// once the budget ran out it came back as "transaction status unclear": a 500 telling the client
// the transaction may already be on chain. Neither node received it.
func TestWriteOutcomeIsUnknown_ADegradedWriteTheFilterKeptFromEveryNode(t *testing.T) {
	msg := archiveRequest{&MockProtocolMessage{
		api: &spectypes.Api{
			Name:     "/cosmos/tx/v1beta1/txs",
			Category: spectypes.SpecCategory{Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS},
		},
		requestedBlock: spectypes.LATEST_BLOCK,
		userData:       common.UserData{DappId: "test", ConsumerIp: "1.2.3.4"},
	}}
	relayProcessor := rejectEveryDegradedSession(t, msg)
	relayProcessor.SetStopReason(relaycore.StopReasonProcessingTimeout)

	require.Zero(t, relayProcessor.GetUsedProviders().SessionsDispatched(),
		"the filter kept the write from both nodes, so the verdict must count none as asked")
	require.False(t, writeOutcomeIsUnknown(msg, relayProcessor),
		"no node received the write, so none can be holding it: this is a failure, not an unclear write")
}
