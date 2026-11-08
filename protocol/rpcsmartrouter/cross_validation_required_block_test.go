package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// MAG-2786. Nothing checked that a provider holds the block it is asked for. An EVM node asked for a
// block above its tip answers {"result": null} with HTTP 200, which is no node error, so it is counted
// as a vote like any other answer, and two lagging nodes agree with each other. A cross-validated
// request for a block near the tip came back "not found" with lava-cross-validation-status: success.
//
// Every case runs the enabled policy defaults, three participants and agreement two, on the method the
// request calls. A provider's last known block is what the optimizer has observed of it; a provider
// listed with knownBlock 0 has never been observed.

const (
	// requiredBlockNull is what a node answers for a block it does not hold.
	requiredBlockNull = `{"jsonrpc":"2.0","id":1,"result":null}`
	// requiredBlockData is what a node that holds the block answers.
	requiredBlockData = `{"jsonrpc":"2.0","id":1,"result":{"hash":"0x01"}}`
)

type requiredBlockUpstream struct {
	name       string
	knownBlock uint64
	reply      string
}

func TestCrossValidation_ProvidersBelowTheRequestedBlockAreNotSelected(t *testing.T) {
	const (
		// requested is the block the caller names. A lagging provider is one block short of it.
		requested = 1000
		lagging   = requested - 1
	)
	for _, tc := range []struct {
		name      string
		upstreams []requiredBlockUpstream
		// wantData is set when enough providers hold the block to reach agreement; otherwise the
		// request must fail insufficient-capacity.
		wantData bool
	}{
		{
			// The ticket's case. Only one provider holds the block, one short of agreement.
			name: "two lagging providers answer null, one holds the block",
			upstreams: []requiredBlockUpstream{
				{"lagging-1", lagging, requiredBlockNull},
				{"lagging-2", lagging, requiredBlockNull},
				{"synced", requested, requiredBlockData},
			},
		},
		{
			name: "enough providers hold the block",
			upstreams: []requiredBlockUpstream{
				{"lagging-1", lagging, requiredBlockNull},
				{"lagging-2", lagging, requiredBlockNull},
				{"synced-1", requested, requiredBlockData},
				{"synced-2", requested, requiredBlockData},
			},
			wantData: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hits, result, err := sendRequiredBlockRequest(t, "eth_getBlockByNumber",
				fmt.Sprintf(`["0x%x",false]`, requested), 0, tc.upstreams)

			require.False(t, err == nil && replyResultIsNull(t, result),
				"a null answer must never pass cross-validation as success")
			for _, upstream := range tc.upstreams {
				if upstream.knownBlock < requested {
					require.Zero(t, hits[upstream.name].Load(), "%s is known to be below block %d and was asked for it", upstream.name, requested)
				}
			}
			if tc.wantData {
				require.NoError(t, err)
				require.Equal(t, "success", pinOverrideReplyHeader(result, common.CROSS_VALIDATION_STATUS_HEADER_NAME))
				require.JSONEq(t, requiredBlockData, string(result.Reply.Data))
				return
			}
			require.Error(t, err)
			require.NotNil(t, result)
			require.Equal(t, common.CrossValidationReasonInsufficientCapacity, result.CrossValidationFailureReason)
		})
	}
}

// Historical reads keep the providers behind the tip. A provider fifty blocks behind is still right
// about a block a thousand below the tip, and one never observed is not known to be behind at all, so
// both are asked; filtering on lag would drop them.
func TestCrossValidation_HistoricalReadKeepsProvidersBehindTheTip(t *testing.T) {
	const (
		tip       = 10_000
		behind    = tip - 50
		requested = tip - 1000
	)
	upstreams := []requiredBlockUpstream{
		{"at-tip", tip, requiredBlockData},
		{"behind", behind, requiredBlockData},
		{"never-observed", 0, requiredBlockData},
	}
	hits, result, err := sendRequiredBlockRequest(t, "eth_getBlockByNumber", fmt.Sprintf(`["0x%x",false]`, requested), 0, upstreams)
	require.NoError(t, err)
	require.Equal(t, "success", pinOverrideReplyHeader(result, common.CROSS_VALIDATION_STATUS_HEADER_NAME))
	// A cross-validation relay that loses the race keeps running after the reply goes out, so the
	// per-upstream count is awaited rather than read once.
	require.Eventually(t, func() bool {
		return hits["behind"].Load() == 1 && hits["never-observed"].Load() == 1
	}, 5*time.Second, 10*time.Millisecond, "a provider that holds the block, or may, must still be asked")
}

// A latest request is not held to a block. By the time it is sent, its requested block has been
// resolved to the chain tip, which reads exactly like a request naming the tip, and holding it to that
// would turn away every provider a block behind. It is left to the consistency filter's lag threshold.
func TestCrossValidation_LatestIsNotHeldToTheTip(t *testing.T) {
	const tip = 10_000
	upstreams := []requiredBlockUpstream{
		{"at-tip", tip, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`},
		{"one-behind-1", tip - 1, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`},
		{"one-behind-2", tip - 1, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`},
	}
	hits, result, err := sendRequiredBlockRequest(t, "eth_getBalance", `["0x0000000000000000000000000000000000000001","latest"]`, tip, upstreams)
	require.NoError(t, err, "a provider one block behind the tip must still serve a latest request")
	require.Equal(t, "success", pinOverrideReplyHeader(result, common.CROSS_VALIDATION_STATUS_HEADER_NAME))
	require.Eventually(t, func() bool {
		return hits["one-behind-1"].Load() == 1 && hits["one-behind-2"].Load() == 1
	}, 5*time.Second, 10*time.Millisecond)
}

// sendRequiredBlockRequest sends one cross-validated request for method with params through
// SendParsedRelay, to a router pairing upstreams under an operator policy that enables cross-validation
// on method with its defaults. seenBlock is the chain tip the request was parsed against, 0 for none.
// It returns each upstream's request count with the outcome.
func sendRequiredBlockRequest(t *testing.T, method, params string, seenBlock int64, upstreams []requiredBlockUpstream) (map[string]*atomic.Int64, *common.RelayResult, error) {
	t.Helper()
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)

	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "ETH1")
	csm := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())
	hits := make(map[string]*atomic.Int64, len(upstreams))
	providers := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(upstreams))
	for i, upstream := range upstreams {
		counter := &atomic.Int64{}
		hits[upstream.name] = counter
		reply := upstream.reply
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			counter.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(reply))
		}))
		t.Cleanup(server.Close)
		connection, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: server.URL}, 5, "")
		require.NoError(t, err)
		endpoint := &lavasession.Endpoint{NetworkAddress: server.URL, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{connection}}
		provider := lavasession.NewConsumerSessionWithProvider(upstream.name, []*lavasession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		providers[uint64(i)] = provider
		if upstream.knownBlock > 0 {
			optimizer.AppendSyncData(upstream.name, upstream.knownBlock, provideroptimizer.SyncReference{})
		}
	}
	require.NoError(t, csm.UpdateAllProviders(1, providers, nil))

	resolver, err := NewCrossValidationPolicyResolver(CrossValidationConfig{Policies: []CrossValidationPolicyEntry{{
		ChainID:               "ETH1",
		ApiInterface:          spectypes.APIInterfaceJsonRPC,
		Method:                method,
		CrossValidationPolicy: CrossValidationPolicy{Enabled: true},
	}}})
	require.NoError(t, err)
	rpcss := &RPCSmartRouterServer{
		listenEndpoint:          rpcEndpoint,
		chainParser:             chainParser,
		consistencyConfig:       relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:          csm,
		crossValidationResolver: resolver,
		rpcSmartRouterLogs:      logs,
	}

	body := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":%q,"params":%s,"id":1}`, method, params))
	chainMessage, err := chainParser.ParseMsg("", body, http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: uint64(seenBlock)})
	require.NoError(t, err)
	requestedBlock, _ := chainMessage.RequestedBlock()
	relayData := lavaprotocol.NewRelayData(ctx, http.MethodPost, "", body, seenBlock, requestedBlock, spectypes.APIInterfaceJsonRPC,
		chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, nil, relayData, "dapp", "1.2.3.4")

	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result, err := rpcss.SendParsedRelay(callCtx, nil, protocolMessage)
	return hits, result, err
}

// replyResultIsNull reports whether the reply's JSON-RPC result is null.
func replyResultIsNull(t *testing.T, result *common.RelayResult) bool {
	t.Helper()
	require.NotNil(t, result)
	require.NotNil(t, result.Reply)
	var envelope struct {
		Result json.RawMessage `json:"result"`
	}
	require.NoError(t, json.Unmarshal(result.Reply.Data, &envelope))
	return string(envelope.Result) == "null"
}
