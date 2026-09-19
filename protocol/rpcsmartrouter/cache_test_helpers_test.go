package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"

	ecocache "github.com/magma-Devs/smart-router/ecosystem/cache"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/performance"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// Shared by the cache tests (MAG-3461, MAG-3460, MAG-3462). Each of those changes adds this
// file byte for byte, so whichever lands first carries it and the others merge onto it.

// ethJsonRPCParser builds the real Ethereum JSON-RPC chain parser from the spec file, so
// these tests read the flags and block parsing a deployed router reads.
func ethJsonRPCParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	noop := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	require.NoError(t, err)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	return chainParser
}

// ethProtocolMessage parses body the way the listener does and stamps seenBlock as the
// parse-time tip, so the message carries exactly what the cache paths read.
func ethProtocolMessage(t *testing.T, chainParser chainlib.ChainParser, body string, seenBlock int64) chainlib.ProtocolMessage {
	t.Helper()
	chainMsg, err := chainParser.ParseMsg("", []byte(body), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	reqBlock, _ := chainMsg.RequestedBlock()
	relayData := lavaprotocol.NewRelayData(context.Background(), http.MethodPost, "", []byte(body), seenBlock, reqBlock, spectypes.APIInterfaceJsonRPC, chainMsg.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMsg), common.GetExtensionNames(chainMsg.GetExtensions()))
	return chainlib.NewProtocolMessage(chainMsg, nil, relayData, "test-dapp", "127.0.0.1")
}

func ethCacheTestServer(chainParser chainlib.ChainParser, primary *performance.Cache) *RPCSmartRouterServer {
	return &RPCSmartRouterServer{
		cache:          primary,
		chainParser:    chainParser,
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC},
	}
}

// directGetOn is directGet for a chain other than the REST harness's LAVA.
func directGetOn(rcs *ecocache.RelayerCacheServer, chainID string, hashKey []byte, block, seenBlock int64) *pairingtypes.CacheRelayReply {
	reply, _ := rcs.GetRelay(context.Background(), &pairingtypes.RelayCacheGet{
		RequestHash:    hashKey,
		ChainId:        chainID,
		RequestedBlock: block,
		SeenBlock:      seenBlock,
		Finalized:      false,
	})
	return reply
}
