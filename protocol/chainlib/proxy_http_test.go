package chainlib

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// headerRecorder is a node that answers every request with body and records the headers each
// request arrived with.
type headerRecorder struct {
	mu      sync.Mutex
	headers []http.Header
	body    string
}

func (h *headerRecorder) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		h.headers = append(h.headers, r.Header.Clone())
		h.mu.Unlock()
		w.Header().Set("X-Node-Header", "node")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, h.body)
	}
}

func (h *headerRecorder) received() []http.Header {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]http.Header(nil), h.headers...)
}

func appendMessageHeaders(t *testing.T, chainMessage ChainMessage, headers []pairingtypes.Metadata) {
	t.Helper()
	appender, ok := chainMessage.GetRPCMessage().(interface {
		AppendHeader([]pairingtypes.Metadata)
	})
	require.True(t, ok, "the parsed message carries no headers")
	appender.AppendHeader(headers)
}

// TestRpcClientSenders_NullifiedHeaderIsDropped sends a JSON-RPC batch and a Tendermint JSON-RPC
// call whose spec nullifies one header and sets another. The nullified header must not reach the
// node, and the set one must not outlive its call on the pooled client: both senders used to set
// an empty value, which goes on the wire as an empty header, and the single JSON-RPC call already
// deleted it.
func TestRpcClientSenders_NullifiedHeaderIsDropped(t *testing.T) {
	for _, tc := range []struct {
		name, apiInterface, connectionType, request, nodeReply string
	}{
		{
			"jsonrpc batch", spectypes.APIInterfaceJsonRPC, http.MethodPost,
			`[{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]},{"jsonrpc":"2.0","id":2,"method":"eth_chainId","params":[]}]`,
			`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"result":"0x1"}]`,
		},
		{
			"tendermintrpc call", spectypes.APIInterfaceTendermintRPC, "",
			`{"jsonrpc":"2.0","id":1,"method":"status","params":[]}`,
			`{"jsonrpc":"2.0","id":1,"result":{}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			node := &headerRecorder{body: tc.nodeReply}
			specIndex := "ETH1"
			if tc.apiInterface == spectypes.APIInterfaceTendermintRPC {
				specIndex = "LAVA"
			}
			chainParser, chainRouter, _, closeServer, _, err := CreateChainLibMocks(ctx, specIndex, tc.apiInterface, node.handler(), nil, "../../", nil)
			require.NoError(t, err)
			if closeServer != nil {
				defer closeServer()
			}

			chainMessage, err := chainParser.ParseMsg("", []byte(tc.request), tc.connectionType, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			appendMessageHeaders(t, chainMessage, []pairingtypes.Metadata{{Name: "X-Nullified", Value: ""}, {Name: "X-Spec-Header", Value: "spec"}})
			_, _, _, err = chainRouter.SendNodeMsg(ctx, chainMessage, nil)
			require.NoError(t, err)

			// Later calls without the headers, on clients the pool hands out again.
			for range 8 {
				plain, err := chainParser.ParseMsg("", []byte(tc.request), tc.connectionType, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
				require.NoError(t, err)
				_, _, _, err = chainRouter.SendNodeMsg(ctx, plain, nil)
				require.NoError(t, err)
			}

			received := node.received()
			require.Len(t, received, 9)
			require.Equal(t, "spec", received[0].Get("X-Spec-Header"))
			_, nullifiedSent := received[0]["X-Nullified"]
			require.False(t, nullifiedSent, "a nullified header went on the wire, empty")
			for i, header := range received[1:] {
				_, leaked := header["X-Spec-Header"]
				require.False(t, leaked, "call %d inherited the first call's header from the pooled client", i+1)
			}
		})
	}
}

// TestHTTPProxies_ConcurrentFirstSendsShareOneClient sends concurrently through a REST proxy that
// has not sent before. Each proxy used to create its HTTP client on the first send without a lock,
// which the race detector reports; run with -race.
func TestHTTPProxies_ConcurrentFirstSendsShareOneClient(t *testing.T) {
	ctx := context.Background()
	node := &headerRecorder{body: `{"block":{}}`}
	chainParser, chainRouter, _, closeServer, _, err := CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, node.handler(), nil, "../../", nil)
	require.NoError(t, err)
	if closeServer != nil {
		defer closeServer()
	}

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		chainMessage, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/latest", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _, errs[i] = chainRouter.SendNodeMsg(ctx, chainMessage, nil)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, node.received(), 8)
}

// TestHTTPProxies_ReplyStatusAndHeaders pins what each HTTP-client proxy hands back from a node
// that answers 200 with a header: REST and GraphQL pass the node's status and headers on, and
// Tendermint's URI path answers like its JSON-RPC path, 200 and no node headers.
func TestHTTPProxies_ReplyStatusAndHeaders(t *testing.T) {
	for _, tc := range []struct {
		name, specIndex, apiInterface, url, body, connectionType string
		wantNodeHeader                                           bool
	}{
		{"rest", "LAVA", spectypes.APIInterfaceRest, "/cosmos/base/tendermint/v1beta1/blocks/latest", "", http.MethodGet, true},
		{"tendermintrpc uri", "LAVA", spectypes.APIInterfaceTendermintRPC, "status", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			node := &headerRecorder{body: `{"jsonrpc":"2.0","id":1,"result":{}}`}
			chainParser, chainRouter, _, closeServer, _, err := CreateChainLibMocks(ctx, tc.specIndex, tc.apiInterface, node.handler(), nil, "../../", nil)
			require.NoError(t, err)
			if closeServer != nil {
				defer closeServer()
			}
			var body []byte
			if tc.body != "" {
				body = []byte(tc.body)
			}
			chainMessage, err := chainParser.ParseMsg(tc.url, body, tc.connectionType, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			reply, _, _, err := chainRouter.SendNodeMsg(ctx, chainMessage, nil)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, reply.StatusCode)
			var nodeHeader string
			for _, md := range reply.RelayReply.Metadata {
				if md.Name == "X-Node-Header" {
					nodeHeader = md.Value
				}
			}
			if tc.wantNodeHeader {
				require.Equal(t, "node", nodeHeader)
			} else {
				require.Empty(t, reply.RelayReply.Metadata)
			}
		})
	}
}
