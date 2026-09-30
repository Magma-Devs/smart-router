package rpcsmartrouter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/qos"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
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

			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.reply))
			}))
			defer upstream.Close()
			// A known tip for the endpoint, so a sync sample has something to report.
			seedEndpointTip("ETH1", "jsonrpc", upstream.URL, 700)
			t.Cleanup(func() { endpointtip.Default().Remove(endpointtip.Key("ETH1", "jsonrpc", upstream.URL)) })

			chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(
				ctx, "ETH1", spectypes.APIInterfaceJsonRPC,
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
				nil, "../../", nil)
			if closeServer != nil {
				defer closeServer()
			}
			require.NoError(t, err)

			body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x0000000000000000000000000000000000000000","0x10"]}`)
			chainMsg, err := chainParser.ParseMsg("", body, http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			relayData := lavaprotocol.NewRelayData(ctx, http.MethodPost, "", body, 0, 16, "jsonrpc",
				chainMsg.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMsg), common.GetExtensionNames(chainMsg.GetExtensions()))
			protocolMsg := chainlib.NewProtocolMessage(chainMsg, nil, relayData, "test", "1.2.3.4")

			const provider = "lava@behind"
			endpoint := &lavasession.Endpoint{NetworkAddress: upstream.URL}
			directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL}, 5, "")
			require.NoError(t, err)
			session := &lavasession.SingleConsumerSession{
				Parent: &lavasession.ConsumerSessionsWithProvider{
					PublicLavaAddress: provider,
					Endpoints:         []*lavasession.Endpoint{endpoint},
				},
				Connection: &lavasession.DirectRPCSessionConnection{
					DirectConnection: directConn,
					EndpointAddress:  upstream.URL,
					Endpoint:         endpoint,
				},
				QoSManager: qos.NewQoSManager(),
			}
			_, ok := session.TryUseSession()
			require.True(t, ok, "setup: failed to lock session")
			session.ConsecutiveErrors = []error{errors.New("an earlier failure")}

			usedProviders := lavasession.NewUsedProviders(nil)
			sessions := lavasession.ConsumerSessionsMap{provider: &lavasession.SessionInfo{Session: session}}
			usedProviders.AddUsed(sessions, nil)
			require.NoError(t, session.SetUsageForSession(0, nil, usedProviders, lavasession.NewRouterKey(nil)))

			spy := &scoringSpyOptimizer{
				ProviderOptimizer: provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "ETH1"),
			}
			rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"}
			rpcss := &RPCSmartRouterServer{
				listenEndpoint:    rpcEndpoint,
				chainParser:       chainParser,
				consistencyConfig: relaycore.DefaultConsistencyValidationConfig(),
				sessionManager: lavasession.NewConsumerSessionManager(rpcEndpoint, spy, nil, "test-router",
					lavasession.NewActiveSubscriptionProvidersStorage()),
			}

			sm := &budgetCallSiteStateMachine{usedProviders: usedProviders, protocolMessage: protocolMsg}
			relayProcessor := relaycore.NewRelayProcessor(ctx, nil, cvGuardMetrics{}, cvGuardMetrics{},
				lavaprotocol.NewRelayRetriesManager(), sm)
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
		{name: "no result", result: nil, want: false},
		{
			name:   "data scope behind a 5xx stays a failure",
			result: &common.RelayResult{StatusCode: http.StatusBadGateway, IsNodeError: true, IsDataScope: true},
			want:   false,
		},
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
