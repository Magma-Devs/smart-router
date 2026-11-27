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
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/qos"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relayprotocol"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// scoringSpyOptimizer counts the relay samples the session manager hands the optimizer, and
// forwards them to the real one.
type scoringSpyOptimizer struct {
	*provideroptimizer.ProviderOptimizer
	successes atomic.Int32
	failures  atomic.Int32
	syncs     atomic.Int32
}

func (s *scoringSpyOptimizer) AppendRelayData(provider string, latency time.Duration, cu, syncBlock uint64) {
	s.successes.Add(1)
	s.ProviderOptimizer.AppendRelayData(provider, latency, cu, syncBlock)
}

func (s *scoringSpyOptimizer) AppendRelayDataConsensus(provider string, latency time.Duration, cu, syncBlock uint64, syncRef provideroptimizer.SyncReference) {
	s.successes.Add(1)
	s.ProviderOptimizer.AppendRelayDataConsensus(provider, latency, cu, syncBlock, syncRef)
}

func (s *scoringSpyOptimizer) AppendRelayFailure(provider string) {
	s.failures.Add(1)
	s.ProviderOptimizer.AppendRelayFailure(provider)
}

func (s *scoringSpyOptimizer) AppendSyncData(provider string, syncBlock uint64, syncRef provideroptimizer.SyncReference) {
	s.syncs.Add(1)
	s.ProviderOptimizer.AppendSyncData(provider, syncBlock, syncRef)
}

// An endpoint that answers "I do not hold this data" answers fast. Scored as a success, that gave a
// node hundreds of blocks behind the tip an availability and latency sample it did not earn. It must
// get a sync sample only: its tip is still real evidence of lag. This drives the real dispatcher
// against a real upstream reply; the controls show the harness sees each kind of sample.
func TestDataScopeAnswerScoresSyncOnly(t *testing.T) {
	for _, tc := range []struct {
		name          string
		chainID       string // "" is ETH1
		status        int    // the upstream's HTTP status; 0 is 200
		reply         string
		wantSuccesses int32
		wantFailures  int32
		wantSyncs     int32
		wantStreak    int
	}{
		{
			name:      "block not found",
			reply:     `{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"block not found"}}`,
			wantSyncs: 1,
			// The streak is left as it was: neither reset by a success nor extended by a failure.
			wantStreak: 1,
		},
		{
			// No message text: the code alone classifies it, as for any error without one.
			name:       "block not found, empty message",
			reply:      `{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":""}}`,
			wantSyncs:  1,
			wantStreak: 1,
		},
		{
			name:       "missing trie node",
			reply:      `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"missing trie node 1a2b (path )"}}`,
			wantSyncs:  1,
			wantStreak: 1,
		},
		{
			// MAG-4448: bitcoind answers a JSON-RPC 1.0 request's error with HTTP 500. The body
			// decides here too, as it does for the endpoint's health and the retry: a -8 or -5 inside
			// the 500 scores as it would inside a 200, on sync only.
			name:       "bitcoind -8 inside a 500",
			chainID:    "BTC",
			status:     http.StatusInternalServerError,
			reply:      `{"result":null,"error":{"code":-8,"message":"Block height out of range"},"id":1}`,
			wantSyncs:  1,
			wantStreak: 1,
		},
		{
			name:       "bitcoind -5 inside a 500",
			chainID:    "BTC",
			status:     http.StatusInternalServerError,
			reply:      `{"result":null,"error":{"code":-5,"message":"No such mempool or blockchain transaction. Use gettransaction for wallet transactions."},"id":1}`,
			wantSyncs:  1,
			wantStreak: 1,
		},
		{
			// A 5xx whose body explains nothing stays a failure.
			name:         "control: bitcoind code no row knows inside a 500",
			chainID:      "BTC",
			status:       http.StatusInternalServerError,
			reply:        `{"result":null,"error":{"code":-12345,"message":"something the node never documented"},"id":1}`,
			wantFailures: 1,
			wantStreak:   2,
		},
		{
			name:          "control: a result",
			reply:         `{"jsonrpc":"2.0","id":1,"result":"0x10"}`,
			wantSuccesses: 1,
			wantStreak:    0,
		},
		{
			name:         "control: an internal error",
			reply:        `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`,
			wantFailures: 1,
			wantStreak:   2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			chainID, status := tc.chainID, tc.status
			if chainID == "" {
				chainID = "ETH1"
			}
			if status == 0 {
				status = http.StatusOK
			}

			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(tc.reply))
			}))
			defer upstream.Close()
			// A known tip for the endpoint, so a sync sample has something to report.
			seedEndpointTip(chainID, "jsonrpc", upstream.URL, 700)
			t.Cleanup(func() { endpointtip.Default().Remove(endpointtip.Key(chainID, "jsonrpc", upstream.URL)) })

			chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(
				ctx, chainID, spectypes.APIInterfaceJsonRPC,
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
				nil, "../../", nil)
			if closeServer != nil {
				defer closeServer()
			}
			require.NoError(t, err)

			body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x0000000000000000000000000000000000000000","0x10"]}`)
			if chainID == "BTC" {
				body = []byte(`{"jsonrpc":"1.0","id":1,"method":"getblockhash","params":[900000]}`)
			}
			chainMsg, err := chainParser.ParseMsg("", body, http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			relayData := relayprotocol.NewRelayData(ctx, http.MethodPost, "", body, 0, 16, "jsonrpc",
				chainMsg.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMsg), common.GetExtensionNames(chainMsg.GetExtensions()))
			protocolMsg := chainlib.NewProtocolMessage(chainMsg, nil, relayData, "test", "1.2.3.4")

			const provider = "provider@behind"
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

			// Holding the lock again means the dispatcher has released the session, and any
			// optimizer sample it records has already been handed off.
			require.Eventually(t, func() bool {
				_, ok := session.TryUseSession()
				return ok
			}, 5*time.Second, 10*time.Millisecond, "the dispatcher never released the session")
			streak := len(session.ConsecutiveErrors)
			session.Free(nil)

			// Samples are handed off asynchronously, so wait for the expected one, then give a stray
			// one time to land.
			require.Eventually(t, func() bool {
				return spy.successes.Load() == tc.wantSuccesses && spy.failures.Load() == tc.wantFailures &&
					spy.syncs.Load() == tc.wantSyncs
			}, 5*time.Second, 10*time.Millisecond,
				"successes=%d failures=%d syncs=%d", spy.successes.Load(), spy.failures.Load(), spy.syncs.Load())
			time.Sleep(100 * time.Millisecond)
			require.Equal(t, tc.wantSuccesses, spy.successes.Load(), "optimizer success samples")
			require.Equal(t, tc.wantFailures, spy.failures.Load(), "optimizer failure samples")
			require.Equal(t, tc.wantSyncs, spy.syncs.Load(), "optimizer sync-only samples")
			require.Equal(t, tc.wantStreak, streak, "consecutive error streak")
		})
	}
}

// TestIsDataScopeRelayOutcome pins which completed relays score on sync only. The flags come from
// the production classifier, fed a real error.
func TestIsDataScopeRelayOutcome(t *testing.T) {
	classified := func(code int, message string) *common.RelayResult {
		result := &common.RelayResult{StatusCode: http.StatusOK, IsNodeError: true}
		result.ApplyNodeErrorClassification(common.ChainFamilyEVM, common.TransportJsonRPC, code, message)
		return result
	}
	// A JSON-RPC 5xx as the direct sender returns it: the status error, classified and wrapped.
	statusErr := func(family common.ChainFamily, status int, body string) error {
		return classifyAndWrap(&routersession.HTTPStatusError{StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), Body: []byte(body)}, family, common.TransportJsonRPC)
	}
	// A REST or gRPC answer is classified by its transport status, as the direct senders do
	// (sendRESTRelay / sendGRPCRelay call ApplyNodeErrorClassification with the response code).
	classifiedStatus := func(family common.ChainFamily, transport common.TransportType, status int, message string) *common.RelayResult {
		result := &common.RelayResult{StatusCode: status, IsNodeError: true}
		result.ApplyNodeErrorClassification(family, transport, status, message)
		return result
	}

	for _, tc := range []struct {
		name   string
		err    error
		result *common.RelayResult
		want   bool
	}{
		{name: "block not found", result: classified(-32001, "block not found"), want: true},
		{name: "missing trie node", result: classified(-32000, "missing trie node 1a2b (path )"), want: true},
		{name: "transaction not found", result: classified(-32000, "transaction not found"), want: true},
		{name: "internal error", result: classified(-32603, "internal error"), want: false},
		{name: "rate limited", result: classified(-32000, "rate limit exceeded, please slow down"), want: false},
		{name: "a result", result: &common.RelayResult{StatusCode: http.StatusOK}, want: false},
		// Since REST non-2xx became node errors, a REST "not found" takes this path too,
		// including a 404 for a route the node does not serve.
		{name: "REST 404", result: classifiedStatus(common.ChainFamilyEVM, common.TransportREST, http.StatusNotFound, "not found"), want: true},
		{name: "REST 404 on a Cosmos chain", result: classifiedStatus(common.ChainFamilyCosmosSDK, common.TransportREST, http.StatusNotFound, "not found"), want: true},
		{name: "REST 410", result: classifiedStatus(common.ChainFamilyEVM, common.TransportREST, http.StatusGone, "gone"), want: true},
		{name: "REST 403 stays out", result: classifiedStatus(common.ChainFamilyEVM, common.TransportREST, http.StatusForbidden, "forbidden"), want: false},
		{name: "REST 400 stays out", result: classifiedStatus(common.ChainFamilyEVM, common.TransportREST, http.StatusBadRequest, "bad request"), want: false},
		{name: "gRPC NOT_FOUND", result: classifiedStatus(common.ChainFamilyCosmosSDK, common.TransportGRPC, 5, "block not found"), want: true},
		{name: "no result", result: nil, want: false},
		{
			name:   "data scope behind a 5xx stays a failure",
			result: &common.RelayResult{StatusCode: http.StatusBadGateway, IsNodeError: true, IsDataScope: true},
			want:   false,
		},
		// MAG-4448: a JSON-RPC 5xx whose body is data scope is read by its body, as the sender's
		// wrapped verdict carries it. A 5xx the body does not explain stays a failure.
		{name: "bitcoind -8 inside a 500", err: statusErr(common.ChainFamilyBitcoin, 500, `{"result":null,"error":{"code":-8,"message":"Block height out of range"},"id":1}`), want: true},
		{name: "bitcoind -5 inside a 500", err: statusErr(common.ChainFamilyBitcoin, 500, `{"result":null,"error":{"code":-5,"message":"Transaction not in mempool"},"id":1}`), want: true},
		{name: "a not-found body inside a 503", err: statusErr(common.ChainFamilyEVM, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"block not found"}}`), want: true},
		{name: "an internal error inside a 500 stays a failure", err: statusErr(common.ChainFamilyBitcoin, 500, `{"result":null,"error":{"code":-32603,"message":"internal error"},"id":1}`), want: false},
		{name: "a 500 with no body stays a failure", err: statusErr(common.ChainFamilyBitcoin, 500, ``), want: false},
		{name: "a code no row knows inside a 500 stays a failure", err: statusErr(common.ChainFamilyEVM, 500, `{"jsonrpc":"2.0","id":1,"error":{"code":-8,"message":"Block height out of range"}}`), want: false},
		{
			name:   "data scope with a transport error stays a failure",
			err:    errors.New("connection reset"),
			result: &common.RelayResult{StatusCode: http.StatusOK, IsNodeError: true, IsDataScope: true},
			want:   false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isDataScopeRelayOutcome(tc.err, tc.result))
		})
	}
}
