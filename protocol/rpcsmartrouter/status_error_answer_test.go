package rpcsmartrouter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/qos"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relayprotocol"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// bitcoindError is the reply bitcoind sends a JSON-RPC 1.0 request it refuses, under HTTP 500.
func bitcoindError(code int, message string) string {
	return fmt.Sprintf(`{"result":null,"error":{"code":%d,"message":%q},"id":1}`, code, message)
}

// The requests MAG-5674's replies answer, and the replies, as bitcoind sends them.
const (
	sendUndecodableTx   = `{"jsonrpc":"1.0","id":1,"method":"sendrawtransaction","params":["00"]}`
	getBlockHashOfText  = `{"jsonrpc":"1.0","id":1,"method":"getblockhash","params":["abc"]}`
	verifyMessageBadKey = `{"jsonrpc":"1.0","id":1,"method":"verifymessage","params":["not-an-address","c2ln","msg"]}`
	getBlockHeaderBad   = `{"jsonrpc":"1.0","id":1,"method":"getblockheader","params":["00",true]}`
)

var (
	txDecodeFailed      = bitcoindError(-22, "TX decode failed")
	stringNotNumber     = bitcoindError(-3, "JSON value of type string is not of expected type number")
	invalidAddress      = bitcoindError(-5, "Invalid address")
	invalidParams       = bitcoindError(-32602, "Only one of address and nodeid should be provided.")
	bitcoindInternalErr = bitcoindError(-32603, "internal error")
)

// MAG-5674: which JSON-RPC 5xx the session release reads as the answer in its body, and with which
// flags. Each error is the sender's own: the status error, classified and wrapped.
func TestAnswerInsideStatusError(t *testing.T) {
	senderErr := func(family common.ChainFamily, status int, body string) error {
		return classifyAndWrap(&routersession.HTTPStatusError{
			StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), Body: []byte(body),
		}, family, common.TransportJsonRPC)
	}
	btc, evm := common.ChainFamilyBitcoin, common.ChainFamilyEVM
	type answer struct{ nonRetryable, dataScope, capability, rateLimited bool }
	for _, tc := range []struct {
		name   string
		family common.ChainFamily
		err    error
		want   *answer // nil: the release reads the error, a failure
	}{
		{"bitcoind -22 inside a 500", btc, senderErr(btc, 500, txDecodeFailed), &answer{nonRetryable: true}},
		{"bitcoind -3 inside a 500", btc, senderErr(btc, 500, stringNotNumber), &answer{dataScope: true}},
		{"bitcoind -5 invalid address inside a 500", btc, senderErr(btc, 500, invalidAddress), &answer{nonRetryable: true}},
		{"-32602 inside a 500", btc, senderErr(btc, 500, invalidParams), &answer{nonRetryable: true}},
		{"a method this node lacks inside a 500", btc, senderErr(btc, 500, bitcoindError(-32601, "Method not found")), &answer{capability: true}},
		{"a not-found body inside a 503", evm, senderErr(evm, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"block not found"}}`), &answer{dataScope: true}},
		{"a rate limit inside a 503", evm, senderErr(evm, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":429,"message":"Too Many Requests"}}`), &answer{rateLimited: true}},

		{"an internal error inside a 500", btc, senderErr(btc, 500, bitcoindInternalErr), nil},
		{"a code no row knows inside a 500", btc, senderErr(btc, 500, bitcoindError(-12345, "something undocumented")), nil},
		{"a gateway's own page", evm, senderErr(evm, 502, `<html>Bad Gateway</html>`), nil},
		{"a 500 with no body", btc, senderErr(btc, 500, ``), nil},
		{"the status error relayInnerDirect makes for a REST 5xx", btc, httpStatusRelayError(500, &pairingtypes.RelayReply{Data: []byte(txDecodeFailed)}), nil},
		{"a 429, read by its status alone", btc, httpStatusRelayError(429, &pairingtypes.RelayReply{Data: []byte(invalidParams)}), nil},
		{"a transport error", btc, classifyAndWrap(errors.New("connection reset by peer"), btc, common.TransportJsonRPC), nil},
		{"no error", btc, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, status := answerInsideStatusError(tc.err, tc.family)
			if tc.want == nil {
				require.Nil(t, got)
				require.Zero(t, status)
				return
			}
			require.NotNil(t, got)
			var httpErr *routersession.HTTPStatusError
			require.True(t, errors.As(tc.err, &httpErr))
			require.Equal(t, httpErr.StatusCode, status, "the 5xx's own status, for the release's reason")
			require.Equal(t, http.StatusOK, got.StatusCode, "read as inside a 2xx")
			require.True(t, got.IsNodeError)
			require.False(t, got.IsNodeAtFault)
			require.Equal(t, *tc.want, answer{got.IsNonRetryable, got.IsDataScope, got.IsNodeCapability, got.IsRateLimited})
		})
	}
}

// directRelayScore is what one direct relay left behind: the optimizer's samples, the session's
// consecutive-error streak, which starts at 1, and the endpoint's strikes.
type directRelayScore struct {
	successes, failures, syncs int32
	streak                     int
	strikes                    uint64
}

// scoreOneBitcoindRelay sends request through the real dispatcher to one BTC provider whose upstream
// answers every request with status and reply, and returns what the session release recorded.
func scoreOneBitcoindRelay(t *testing.T, request string, status int, reply string) directRelayScore {
	t.Helper()
	ctx := context.Background()
	const chainID = "BTC"

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(reply))
	}))
	defer upstream.Close()
	// A known tip for the endpoint, so a sync sample has something to report.
	seedEndpointTip(chainID, "jsonrpc", upstream.URL, 700)
	t.Cleanup(func() { endpointtip.Default().Remove(endpointtip.Key(chainID, "jsonrpc", upstream.URL)) })

	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(
		ctx, chainID, spectypes.APIInterfaceJsonRPC,
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
		nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	body := []byte(request)
	chainMsg, err := chainParser.ParseMsg("", body, http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	relayData := relayprotocol.NewRelayData(ctx, http.MethodPost, "", body, 0, 16, "jsonrpc",
		chainMsg.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMsg), common.GetExtensionNames(chainMsg.GetExtensions()))
	protocolMsg := chainlib.NewProtocolMessage(chainMsg, nil, relayData, "test", "1.2.3.4")

	const provider = "provider@btc"
	endpoint := &routersession.Endpoint{NetworkAddress: upstream.URL}
	directConn, err := routersession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL}, 5, "")
	require.NoError(t, err)
	session := &routersession.SingleConsumerSession{
		Parent: &routersession.ConsumerSessionsWithProvider{
			PublicAddress: provider,
			Endpoints:     []*routersession.Endpoint{endpoint},
		},
		Connection: &routersession.DirectRPCSessionConnection{
			DirectConnection: directConn,
			EndpointAddress:  upstream.URL,
			Endpoint:         endpoint,
		},
		QoSManager: qos.NewQoSManager(),
	}
	_, ok := session.TryUseSession()
	require.True(t, ok, "setup: failed to lock session")
	session.ConsecutiveErrors = []error{errors.New("an earlier failure")}

	usedProviders := routersession.NewUsedProviders(nil)
	sessions := routersession.ConsumerSessionsMap{provider: &routersession.SessionInfo{Session: session}}
	usedProviders.AddUsed(sessions, nil)
	require.NoError(t, session.SetUsageForSession(0, nil, usedProviders, routersession.NewRouterKey(nil)))

	spy := &scoringSpyOptimizer{
		ProviderOptimizer: provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, chainID),
	}
	rpcEndpoint := &routersession.RPCEndpoint{ChainID: chainID, ApiInterface: "jsonrpc"}
	rpcss := &RPCSmartRouterServer{
		listenEndpoint:    rpcEndpoint,
		chainParser:       chainParser,
		consistencyConfig: relaycore.DefaultConsistencyValidationConfig(),
		sessionManager: routersession.NewConsumerSessionManager(rpcEndpoint, spy, nil, "test-router",
			routersession.NewActiveSubscriptionProvidersStorage()),
	}

	sm := &budgetCallSiteStateMachine{usedProviders: usedProviders, protocolMessage: protocolMsg}
	relayProcessor := relaycore.NewRelayProcessor(ctx, nil, cvGuardMetrics{}, cvGuardMetrics{}, sm)
	require.NoError(t, rpcss.sendRelayToDirectEndpoints(ctx, sessions, protocolMsg, relayProcessor, nil, nil, common.CacheLookupReport{}))

	waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWait()
	relayProcessor.WaitForResults(waitCtx)

	// Holding the lock again means the dispatcher has released the session, and any optimizer sample
	// it records has already been handed off.
	require.Eventually(t, func() bool {
		_, ok := session.TryUseSession()
		return ok
	}, 5*time.Second, 10*time.Millisecond, "the dispatcher never released the session")
	streak := len(session.ConsecutiveErrors)
	session.Free(nil)

	// Samples are handed off asynchronously; give them time to land.
	time.Sleep(200 * time.Millisecond)
	return directRelayScore{spy.successes.Load(), spy.failures.Load(), spy.syncs.Load(), streak, endpoint.ConnectionRefusals}
}

// MAG-5674: bitcoind sends every error to a JSON-RPC 1.0 request under HTTP 500, and the same reply
// inside a 200 to a JSON-RPC 2.0 one. A reply the endpoint is not at fault for now scores the same
// either way, through the real dispatcher: an answer is a success, which ends the error streak, and
// a reply that this node cannot serve the request scores on sync only. A reply that is the node's
// fault stays a failure and a strike under both.
func TestAStatusErrorAnswerScoresAsTheSameAnswerInsideA2xx(t *testing.T) {
	success := directRelayScore{successes: 1, streak: 0}
	// sendrawtransaction is a hanging api, which never gives the optimizer a latency sample.
	writeSuccess := directRelayScore{streak: 0}
	syncOnly := directRelayScore{syncs: 1, streak: 1}
	failure := directRelayScore{failures: 1, streak: 2, strikes: 1}
	for _, tc := range []struct {
		name    string
		request string
		reply   string
		want    directRelayScore
	}{
		{"a transaction that does not decode (-22)", sendUndecodableTx, txDecodeFailed, writeSuccess},
		{"a parameter of the wrong type (-3)", getBlockHashOfText, stringNotNumber, syncOnly},
		{"an invalid address (-5)", verifyMessageBadKey, invalidAddress, success},
		{"invalid params (-32602)", getBlockHeaderBad, invalidParams, success},
		{"control: an internal error (-32603)", getBlockHashOfText, bitcoindInternalErr, failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inside500 := scoreOneBitcoindRelay(t, tc.request, http.StatusInternalServerError, tc.reply)
			inside200 := scoreOneBitcoindRelay(t, tc.request, http.StatusOK, tc.reply)

			require.Equal(t, tc.want, inside500, "inside a 500")
			require.Equal(t, inside200, inside500, "the same reply scores the same inside a 200 and a 500")
		})
	}
}

// newBitcoindBurstServer builds a BTC router whose providers all answer every request with status and
// reply, through the real session manager, the real optimizer (behind a spy) and the real relay loop.
func newBitcoindBurstServer(t *testing.T, chainParser chainlib.ChainParser, providers int, status int, reply string) (*RPCSmartRouterServer, []*routersession.Endpoint, *scoringSpyOptimizer, *atomic.Int64) {
	t.Helper()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	rpcEndpoint := &routersession.RPCEndpoint{ChainID: "BTC", ApiInterface: spectypes.APIInterfaceJsonRPC}
	spy := &scoringSpyOptimizer{
		ProviderOptimizer: provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "BTC"),
	}
	csm := routersession.NewConsumerSessionManager(rpcEndpoint, spy, nil, "test-router", routersession.NewActiveSubscriptionProvidersStorage())

	hits := &atomic.Int64{}
	pairing := map[uint64]*routersession.ConsumerSessionsWithProvider{}
	endpoints := make([]*routersession.Endpoint, 0, providers)
	for i := range providers {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(reply))
		}))
		t.Cleanup(upstream.Close)
		connection, err := routersession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL}, 5, "")
		require.NoError(t, err)
		endpoint := &routersession.Endpoint{NetworkAddress: upstream.URL, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{connection}}
		provider := routersession.NewConsumerSessionWithProvider(fmt.Sprintf("btc-%d", i), []*routersession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		pairing[uint64(i)] = provider
		endpoints = append(endpoints, endpoint)
	}
	require.NoError(t, csm.UpdateAllProviders(1, pairing, nil))

	return &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        chainParser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     csm,
		rpcSmartRouterLogs: logs,
	}, endpoints, spy, hits
}

// MAG-5674's done-when, through the whole relay: a burst of each request bitcoind refuses with a
// reply the endpoint is not at fault for leaves every endpoint enabled with no strike, every
// provider in rotation, and no failure sample in the optimizer. The burst is longer than both
// --bench-after's default (50 strikes) and the 16 consecutive errors that block a provider. The
// control shows the harness sees a strike when the reply is the node's fault.
func TestABurstOfBitcoindRefusalsLeavesEveryProviderOn(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "BTC", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	const providers, burst = 3, 60
	for _, tc := range []struct {
		name    string
		request string
		reply   string
	}{
		{"sendrawtransaction that does not decode (-22, a write to every provider)", sendUndecodableTx, txDecodeFailed},
		{"getblockhash of a string (-3)", getBlockHashOfText, stringNotNumber},
		{"verifymessage with an invalid address (-5)", verifyMessageBadKey, invalidAddress},
		{"invalid params (-32602)", getBlockHeaderBad, invalidParams},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, endpoints, spy, hits := newBitcoindBurstServer(t, chainParser, providers, http.StatusInternalServerError, tc.reply)

			for range burst {
				_, sendErr := sendRefusalTestRequestBody(t, ctx, chainParser, server, []byte(tc.request))
				require.Error(t, sendErr, "every provider refuses the request")
			}

			require.GreaterOrEqual(t, hits.Load(), int64(burst), "every request reached an upstream")
			for _, endpoint := range endpoints {
				require.True(t, endpoint.IsEnabled(), "endpoint %s switched off", endpoint.NetworkAddress)
				require.Zero(t, endpoint.ConnectionRefusals, "endpoint %s took a strike", endpoint.NetworkAddress)
			}
			snapshot := server.sessionManager.ProviderRoutingSnapshot()
			require.Empty(t, snapshot.Blocked, "a provider was blocked")
			require.Len(t, snapshot.ValidAddresses, providers, "every provider stays in rotation")
			require.Zero(t, spy.failures.Load(), "a provider took an availability failure")
		})
	}

	t.Run("control: an internal error is a strike", func(t *testing.T) {
		server, endpoints, _, _ := newBitcoindBurstServer(t, chainParser, providers, http.StatusInternalServerError, bitcoindInternalErr)

		for range 5 {
			_, sendErr := sendRefusalTestRequestBody(t, ctx, chainParser, server, []byte(getBlockHashOfText))
			require.Error(t, sendErr)
		}

		var strikes uint64
		for _, endpoint := range endpoints {
			strikes += endpoint.ConnectionRefusals
		}
		require.Positive(t, strikes, "the node's own fault still counts against it")
	})
}
