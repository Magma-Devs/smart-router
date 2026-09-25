package rpcsmartrouter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MAG-3762. Lava-Retries counts every attempt the router sent, a hedge's loser included
// (MAG-1818). Since an attempt outlives the window that dispatches the hedge, the loser is still
// running when the hedge wins, so it has no result when the reply is built. Counting results
// alone reported the race as zero retries and named only the winner.

// dispatchedTo records one batch per provider, in order, the way a request's first attempt and
// each later retry or hedge go out.
func dispatchedTo(providers ...string) *lavasession.UsedProviders {
	usedProviders := lavasession.NewUsedProviders(nil)
	for _, provider := range providers {
		usedProviders.AddUsed(lavasession.ConsumerSessionsMap{provider: &lavasession.SessionInfo{}}, nil)
	}
	return usedProviders
}

func replyHeader(metadata []pairingtypes.Metadata, name string) (string, bool) {
	for _, entry := range metadata {
		if entry.Name == name {
			return entry.Value, true
		}
	}
	return "", false
}

func TestAttemptsWithoutResult(t *testing.T) {
	from := func(addr string) common.RelayResult {
		return common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: addr}}
	}
	failedAt := func(addr string) relaycore.RelayError {
		return relaycore.RelayError{ProviderInfo: common.ProviderInfo{ProviderAddress: addr}}
	}

	for _, tc := range []struct {
		name           string
		dispatched     []string
		successes      []common.RelayResult
		nodeErrors     []common.RelayResult
		protocolErrors []relaycore.RelayError
		want           []string
	}{
		{
			name: "nothing dispatched",
		},
		{
			name:           "every attempt reported back",
			dispatched:     []string{"p1", "p2"},
			successes:      []common.RelayResult{from("p2")},
			protocolErrors: []relaycore.RelayError{failedAt("p1")},
		},
		{
			name:       "a hedge won while the first attempt was still running",
			dispatched: []string{"p1", "p2"},
			successes:  []common.RelayResult{from("p2")},
			want:       []string{"p1"},
		},
		{
			name:       "the first attempt won while two hedges were still running",
			dispatched: []string{"p1", "p2", "p3"},
			successes:  []common.RelayResult{from("p1")},
			want:       []string{"p2", "p3"},
		},
		{
			name:       "a node error is a result",
			dispatched: []string{"p1", "p2", "p3"},
			nodeErrors: []common.RelayResult{from("p2")},
			successes:  []common.RelayResult{from("p3")},
			want:       []string{"p1"},
		},
		{
			name:           "a provider asked twice and answered once still has an attempt out",
			dispatched:     []string{"p1", "p2", "p1"},
			successes:      []common.RelayResult{from("p2")},
			protocolErrors: []relaycore.RelayError{failedAt("p1")},
			want:           []string{"p1"},
		},
		{
			name:       "a cache hit was never dispatched, so it settles no attempt",
			dispatched: []string{"p1"},
			successes:  []common.RelayResult{from("")},
			want:       []string{"p1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, attemptsWithoutResult(tc.dispatched, tc.successes, tc.nodeErrors, tc.protocolErrors))
		})
	}
}

func TestRetryCountHeader_CountsAttemptsWithoutResult(t *testing.T) {
	ctx := context.Background()
	from := func(addr string) common.RelayResult {
		return common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: addr}}
	}
	// callerProtocolErrors is the count SendParsedRelay reads off the processor before it calls
	// appendHeadersToRelayResult. It normally equals the snapshot's own list length.
	headersFor := func(t *testing.T, rp *MockRelayProcessorForHeaders, resolver string, callerProtocolErrors uint64) []pairingtypes.Metadata {
		t.Helper()
		relayResult := &common.RelayResult{
			ProviderInfo: common.ProviderInfo{ProviderAddress: resolver},
			Reply:        &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
		}
		(&RPCSmartRouterServer{}).appendHeadersToRelayResult(ctx, relayResult, callerProtocolErrors, rp,
			&MockProtocolMessage{api: &spectypes.Api{Name: "eth_blockNumber"}}, "eth_blockNumber", nil, true)
		return relayResult.Reply.Metadata
	}

	for _, tc := range []struct {
		name          string
		rp            *MockRelayProcessorForHeaders
		resolver      string
		wantRetries   string // "" means the header must be absent
		wantProviders string
	}{
		{
			// The ticket's reproduction: the pinned provider is slow, the ticker hedges to a peer,
			// the peer answers, and the pinned attempt is still running.
			name: "a hedge wins while the first attempt is still running",
			rp: &MockRelayProcessorForHeaders{
				selection:      relaycore.Stateless,
				usedProviders:  dispatchedTo("p1", "p2"),
				successResults: []common.RelayResult{from("p2")},
			},
			resolver:      "p2",
			wantRetries:   "1",
			wantProviders: "p1,p2",
		},
		{
			// MAG-1818's own example: P1 dispatched, P2 lost the race, P3 won.
			name: "a three-attempt hedge race",
			rp: &MockRelayProcessorForHeaders{
				selection:      relaycore.Stateless,
				usedProviders:  dispatchedTo("p1", "p2", "p3"),
				successResults: []common.RelayResult{from("p3")},
			},
			resolver:      "p3",
			wantRetries:   "2",
			wantProviders: "p1,p2,p3",
		},
		{
			// Every provider slow: the first attempt answers after two hedges went out. The resolver
			// stays last (MAG-1871) even though it went out first.
			name: "the first attempt wins while two hedges are still running",
			rp: &MockRelayProcessorForHeaders{
				selection:      relaycore.Stateless,
				usedProviders:  dispatchedTo("p1", "p2", "p3"),
				successResults: []common.RelayResult{from("p1")},
			},
			resolver:      "p1",
			wantRetries:   "2",
			wantProviders: "p2,p3,p1",
		},
		{
			name: "a loser that reported back before the reply is counted once",
			rp: &MockRelayProcessorForHeaders{
				selection:      relaycore.Stateless,
				usedProviders:  dispatchedTo("p1", "p2"),
				successResults: []common.RelayResult{from("p2")},
				protocolErrors: []relaycore.RelayError{{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1"}}},
			},
			resolver:      "p2",
			wantRetries:   "1",
			wantProviders: "p1,p2",
		},
		{
			// An attempt still out is listed with the protocol errors, where its cancellation
			// would be recorded, ahead of the node errors.
			name: "an attempt still out sits between the protocol errors and the node errors",
			rp: &MockRelayProcessorForHeaders{
				selection:      relaycore.Stateless,
				usedProviders:  dispatchedTo("p1", "p2", "p3", "p4"),
				protocolErrors: []relaycore.RelayError{{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1"}}},
				nodeErrors:     []common.RelayResult{from("p3")},
				successResults: []common.RelayResult{from("p4")},
			},
			resolver:      "p4",
			wantRetries:   "3",
			wantProviders: "p1,p2,p3,p4",
		},
		{
			// The hedge was served from the cache while the first attempt was still running.
			name: "a cache hit wins while the first attempt is still running",
			rp: &MockRelayProcessorForHeaders{
				selection:      relaycore.Stateless,
				usedProviders:  dispatchedTo("p1"),
				successResults: []common.RelayResult{from("")},
			},
			resolver:      "",
			wantRetries:   "1",
			wantProviders: "p1,Cached",
		},
		{
			// Unchanged: a stateful fan-out is one batch whose losers are expected, never retries.
			name: "a stateful fan-out still absorbs the providers that did not answer",
			rp: &MockRelayProcessorForHeaders{
				selection:      relaycore.Stateful,
				usedProviders:  dispatchedTo("p1", "p2", "p3"),
				successResults: []common.RelayResult{from("p1")},
			},
			resolver:      "p1",
			wantProviders: "p1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := headersFor(t, tc.rp, tc.resolver, uint64(len(tc.rp.protocolErrors)))

			retries, found := replyHeader(metadata, common.RETRY_COUNT_HEADER_NAME)
			if tc.wantRetries == "" {
				require.False(t, found, "no retry header expected, got %q", retries)
			} else {
				require.True(t, found, "the retry header is missing")
				require.Equal(t, tc.wantRetries, retries)
			}
			providers, found := replyHeader(metadata, common.PROVIDER_ADDRESS_HEADER_NAME)
			require.True(t, found)
			require.Equal(t, tc.wantProviders, providers)
		})
	}

	// On a failure path a reader can still be draining the responses when the reply is built,
	// so a result can be recorded after the caller read its protocol-error count and before the
	// header takes its snapshot. The attempt then has a result, so it is not "still out", and
	// the caller's count missed it. It must still be counted exactly once.
	t.Run("a result recorded after the caller read its count is counted once", func(t *testing.T) {
		rp := &MockRelayProcessorForHeaders{
			selection:      relaycore.Stateless,
			usedProviders:  dispatchedTo("p1", "p2"),
			protocolErrors: []relaycore.RelayError{{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1"}}},
			nodeErrors:     []common.RelayResult{from("p2")},
		}
		metadata := headersFor(t, rp, "p2", 0)

		retries, found := replyHeader(metadata, common.RETRY_COUNT_HEADER_NAME)
		require.True(t, found, "p1's attempt fell between the two reads and was counted zero times")
		require.Equal(t, "1", retries)
		providers, _ := replyHeader(metadata, common.PROVIDER_ADDRESS_HEADER_NAME)
		require.Equal(t, "p1,p2", providers)
	})
}

// The same race end to end: REST listener, parser, session selection, the state machine's hedge
// ticker, the dispatcher, and the header path. The first attempt is pinned to an upstream that
// never answers, and the relay timeout is short, so the ticker hedges to the other upstream, which
// answers at once. Before the fix the reply said a hedge fired and, in the same reply, that
// nothing was retried, and it named only the upstream that answered.
func TestRESTListener_HedgedLoserCountsAsARetry(t *testing.T) {
	rand.InitRandomSeed()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const path = "/cosmos/base/tendermint/v1beta1/blocks/17"
	var slowRelays, fastRelays atomic.Int32
	loserCancelled := make(chan struct{})
	markLoserCancelled := sync.OnceFunc(func() { close(loserCancelled) })

	parser, _, _, closeServer, slowNode, err := chainlib.CreateChainLibMocks(
		ctx, "LAVA", spectypes.APIInterfaceRest, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != path {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			slowRelays.Add(1)
			// Never answers. The attempt ends only when the router gives up on it.
			<-r.Context().Done()
			markLoserCancelled()
		}), nil, "../../", nil)
	require.NoError(t, err)
	defer closeServer()

	fastNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		fastRelays.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"block":{"header":{"height":"17"}}}`))
	}))
	defer fastNode.Close()

	upstream := func(name string, nodeURL common.NodeUrl) *lavasession.ConsumerSessionsWithProvider {
		conn, err := lavasession.NewDirectRPCConnection(ctx, nodeURL, 5, spectypes.APIInterfaceRest)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		provider := lavasession.NewConsumerSessionWithProvider(name, []*lavasession.Endpoint{{
			NetworkAddress:    nodeURL.Url,
			Enabled:           true,
			DirectConnections: []lavasession.DirectRPCConnection{conn},
		}}, 100000, 1, 1)
		provider.StaticProvider = true
		return provider
	}
	sessionManager, rpcEndpoint := createTestSessionManager("LAVA", spectypes.APIInterfaceRest)
	rpcEndpoint.NetworkAddress = "127.0.0.1:0"
	require.NoError(t, sessionManager.UpdateAllProviders(1, map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: upstream("slow-upstream", slowNode.NodeUrls[0]),
		1: upstream("fast-upstream", common.NodeUrl{Url: fastNode.URL}),
	}, nil))
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	server := &RPCSmartRouterServer{
		chainParser: parser, sessionManager: sessionManager, listenEndpoint: rpcEndpoint,
		rpcSmartRouterLogs: logs, relayRetriesManager: lavaprotocol.NewRelayRetriesManager(),
		consistencyConfig: relaycore.DefaultConsistencyValidationConfig(),
	}
	listener := chainlib.NewRestChainListener(ctx, rpcEndpoint, server, nil, logs)
	listenerDone := make(chan struct{})
	go func() {
		defer close(listenerDone)
		listener.Serve(ctx, common.ConsumerCmdFlags{})
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		assert.NoError(t, listener.Shutdown(shutdownCtx))
		select {
		case <-listenerDone:
		case <-shutdownCtx.Done():
			t.Error("REST listener did not stop")
		}
	})
	require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, time.Second, time.Millisecond)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+listener.GetListeningAddress()+path, nil)
	require.NoError(t, err)
	req.Header.Set(common.SELECT_PROVIDER_HEADER_NAME, "slow-upstream")
	req.Header.Set(common.RELAY_TIMEOUT_HEADER_NAME, "300ms")
	response, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, response.StatusCode, "body=%s", body)
	require.Equal(t, "true", response.Header.Get(common.LAVA_HEDGE_TRIGGERED_HEADER),
		"setup: the ticker must have hedged, or this measures nothing")
	require.EqualValues(t, 1, slowRelays.Load(), "setup: the pinned upstream must have been asked first")
	require.EqualValues(t, 1, fastRelays.Load(), "setup: the hedge must have gone to the other upstream")

	require.Equal(t, "1", response.Header.Get(common.RETRY_COUNT_HEADER_NAME),
		"two attempts went out, so the reply must report one retry")
	require.Equal(t, "slow-upstream,fast-upstream", response.Header.Get(common.PROVIDER_ADDRESS_HEADER_NAME),
		"the losing attempt must be named, with the upstream that answered last")

	// The router cut the losing attempt off, rather than the upstream answering it. (That the
	// attempt had no result when the reply was built is what the retry header above checks:
	// without the fix that header is absent.)
	select {
	case <-loserCancelled:
	case <-ctx.Done():
		t.Fatal("the router never cancelled the losing attempt")
	}
}
