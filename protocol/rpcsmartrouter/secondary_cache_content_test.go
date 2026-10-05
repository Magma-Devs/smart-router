package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	ecocache "github.com/magma-Devs/smart-router/ecosystem/cache"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/performance"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relaycoretest"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// The REST harness above cannot exercise a content check, since a REST body carries no
// envelope; these helpers build the same scene on Ethereum JSON-RPC, with the real spec.
func secondaryEthParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	noop := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	require.NoError(t, err)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	return chainParser
}

func secondaryEthMessage(t *testing.T, chainParser chainlib.ChainParser, body string, seenBlock int64) chainlib.ProtocolMessage {
	t.Helper()
	chainMsg, err := chainParser.ParseMsg("", []byte(body), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	reqBlock, _ := chainMsg.RequestedBlock()
	relayData := lavaprotocol.NewRelayData(context.Background(), http.MethodPost, "", []byte(body), seenBlock, reqBlock, spectypes.APIInterfaceJsonRPC, chainMsg.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMsg), common.GetExtensionNames(chainMsg.GetExtensions()))
	return chainlib.NewProtocolMessage(chainMsg, nil, relayData, "test-dapp", "127.0.0.1")
}

func newSecondaryEthTestServer(chainParser chainlib.ChainParser, primary *performance.Cache, secondary performance.CacheReader) *RPCSmartRouterServer {
	return &RPCSmartRouterServer{
		cache:                 primary,
		secondaryCache:        secondary,
		secondaryCacheTimeout: 100 * time.Millisecond,
		chainParser:           chainParser,
		listenEndpoint:        &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC},
	}
}

// runSecondaryEthLookup is runSecondaryLookupWithReport for the ETH1 JSON-RPC scene.
func runSecondaryEthLookup(t *testing.T, rpcss *RPCSmartRouterServer, protocolMessage chainlib.ProtocolMessage, requestedBlockForCache int64) (bool, *common.RelayResult, common.CacheLookupReport) {
	t.Helper()
	ctx := context.Background()
	hashKey, outputFormatter, err := protocolMessage.HashCacheRequest("ETH1")
	require.NoError(t, err)

	usedProviders := lavasession.NewUsedProviders(nil)
	stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, &SmartRouterRelaySenderMock{retValue: nil}, protocolMessage, nil, false)
	require.NoError(t, err)
	relayProcessor := relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayRetriesManagerInstance, stateMachine)

	waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
	defer waitCancel()
	waitDone := make(chan struct{})
	go func() { _ = relayProcessor.WaitForResults(waitCtx); close(waitDone) }()

	cacheReport := common.CacheLookupReport{ServedTier: common.CacheTierNone, PrimaryOutcome: common.CacheOutcomeMiss, SecondaryOutcome: common.CacheOutcomeSkipped}
	served := rpcss.trySecondaryCacheLookup(ctx, protocolMessage, protocolMessage.RelayPrivateData(), relayProcessor, nil, hashKey, outputFormatter, requestedBlockForCache, &cacheReport)
	if !served {
		waitCancel()
		<-waitDone
		return false, nil, cacheReport
	}
	<-waitDone
	result, processingErr := relayProcessor.ProcessingResult()
	require.NoError(t, processingErr)
	require.NotNil(t, result)
	return true, result, cacheReport
}

func directGetETH(rcs *ecocache.RelayerCacheServer, hashKey []byte, block, seenBlock int64) *pairingtypes.CacheRelayReply {
	reply, _ := rcs.GetRelay(context.Background(), &pairingtypes.RelayCacheGet{RequestHash: hashKey, ChainId: "ETH1", RequestedBlock: block, SeenBlock: seenBlock})
	return reply
}

const secondaryEthRequest = `{"jsonrpc":"2.0","id":7,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","0x64"]}`

// MAG-3597, symptom 1: the entry's contents decide its kind. A zone whose build predates
// the label writes an error body with no label and no placeholder; it must reach the
// caller as the node error it is, header and all, and never be backfilled as a success.
// The success control shows the same path serving a good entry, id restored.
func TestSecondaryEntryKindComesFromItsContents(t *testing.T) {
	primary, rcs := startCacheServerForTest(t)
	chainParser := secondaryEthParser(t)
	protocolMessage := secondaryEthMessage(t, chainParser, secondaryEthRequest, 100)
	hashKey, _, err := protocolMessage.HashCacheRequest("ETH1")
	require.NoError(t, err)

	t.Run("an error body with neither signal is served as a node error and never backfilled", func(t *testing.T) {
		fake := &fakeCacheReader{active: true, reply: &pairingtypes.CacheRelayReply{
			Reply:      &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"execution reverted"}}`), LatestBlock: 100},
			SeenBlock:  100,
			StatusCode: http.StatusOK,
		}}
		rpcss := newSecondaryEthTestServer(chainParser, primary, fake)
		served, result, report := runSecondaryEthLookup(t, rpcss, protocolMessage, 100)
		require.True(t, served, "a well-formed error is served, exactly like a labelled one")
		require.True(t, result.IsNodeError, "the header comes from the contents when the label is missing")
		require.Equal(t, metrics.CacheOutcomeHit, report.SecondaryOutcome)
		require.Equal(t, `7`, gjson.GetBytes(result.Reply.Data, "id").Raw, "the caller's id is restored on the error too")

		time.Sleep(700 * time.Millisecond)
		require.Nil(t, directGetETH(rcs, hashKey, 100, 100).GetReply(), "an error is never written into the primary as a success")
	})

	t.Run("a success body is served as a success, id restored", func(t *testing.T) {
		fake := &fakeCacheReader{active: true, reply: &pairingtypes.CacheRelayReply{
			Reply:      &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x64"}`), LatestBlock: 100},
			SeenBlock:  100,
			StatusCode: http.StatusOK,
		}}
		rpcss := newSecondaryEthTestServer(chainParser, primary, fake)
		served, result, _ := runSecondaryEthLookup(t, rpcss, protocolMessage, 100)
		require.True(t, served)
		require.False(t, result.IsNodeError)
		require.Equal(t, `7`, gjson.GetBytes(result.Reply.Data, "id").Raw)
		require.Equal(t, `"0x64"`, gjson.GetBytes(result.Reply.Data, "result").Raw)
	})
}

// MAG-3597, symptom 2 and the empty-entry case: an entry that is not a reply at all is
// never served, whatever label it carries. It is dropped as a secondary error, nothing
// is composed from its bytes, and nothing reaches the primary.
func TestSecondaryEntryThatIsNotAReplyIsNotServed(t *testing.T) {
	primary, rcs := startCacheServerForTest(t)
	chainParser := secondaryEthParser(t)
	protocolMessage := secondaryEthMessage(t, chainParser, secondaryEthRequest, 100)
	hashKey, _, err := protocolMessage.HashCacheRequest("ETH1")
	require.NoError(t, err)

	cases := []struct {
		name string
		data string
	}{
		{"a web page", `<html>502 Bad Gateway</html>`},
		{"a truncated envelope", `{"jsonrpc":"2.0","resu`},
		{"no bytes at all", ``},
		{"a bare value", `"0x64"`},
		{"an empty object", `{}`},
		{"an envelope with no payload", `{"jsonrpc":"2.0","id":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeCacheReader{active: true, reply: &pairingtypes.CacheRelayReply{
				Reply:      &pairingtypes.RelayReply{Data: []byte(tc.data), LatestBlock: 100},
				SeenBlock:  100,
				StatusCode: http.StatusOK,
			}}
			rpcss := newSecondaryEthTestServer(chainParser, primary, fake)
			served, _, report := runSecondaryEthLookup(t, rpcss, protocolMessage, 100)
			require.False(t, served, "not a reply, so not served as one")
			require.Equal(t, metrics.CacheOutcomeError, report.SecondaryOutcome, "recorded as an error, not a miss: the tier answered, this router refused it")

			time.Sleep(300 * time.Millisecond)
			require.Nil(t, directGetETH(rcs, hashKey, 100, 100).GetReply(), "nothing reaches the primary")
		})
	}
}

// A batch is judged element by element. One sibling that answers does not vouch for one
// that answers nothing: such a batch used to be served as a success and backfilled into
// the primary, because the batch classifier let the good element mask the empty one
// (Codex review of #412).
func TestSecondaryBatchWithAnElementThatAnswersNothingIsNotServed(t *testing.T) {
	primary, rcs := startCacheServerForTest(t)
	chainParser := secondaryEthParser(t)
	protocolMessage := secondaryEthMessage(t, chainParser, `[{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","0x64"]},{"jsonrpc":"2.0","id":2,"method":"eth_getBalance","params":["0x2222222222222222222222222222222222222222","0x64"]}]`, 100)
	hashKey, _, err := protocolMessage.HashCacheRequest("ETH1")
	require.NoError(t, err)
	fake := &fakeCacheReader{active: true, reply: &pairingtypes.CacheRelayReply{
		Reply:      &pairingtypes.RelayReply{Data: []byte(`[{"jsonrpc":"2.0","id":1,"result":"0x64"},{"jsonrpc":"2.0","id":2}]`), LatestBlock: 100},
		SeenBlock:  100,
		StatusCode: http.StatusOK,
	}}
	rpcss := newSecondaryEthTestServer(chainParser, primary, fake)
	served, _, report := runSecondaryEthLookup(t, rpcss, protocolMessage, 100)
	require.False(t, served, "an element with no payload makes the batch unusable")
	require.Equal(t, metrics.CacheOutcomeError, report.SecondaryOutcome)

	time.Sleep(300 * time.Millisecond)
	require.Nil(t, directGetETH(rcs, hashKey, 100, 100).GetReply(), "nothing reaches the primary")
}
