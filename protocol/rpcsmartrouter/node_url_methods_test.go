package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// MAG-5667, MAG-5587's reproduction: one provider with three node urls, B and C restricted to
// eth_call. Each of four methods is sent 12 times. eth_call reaches only B and C, every other
// method only A, whatever order the urls are listed in.

// methodNode is a fake ETH1 node that answers every method and counts each method it receives.
type methodNode struct {
	url  string
	mu   sync.Mutex
	seen map[string]int
}

func newMethodNode(t *testing.T) *methodNode {
	t.Helper()
	node := &methodNode{seen: map[string]int{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var request map[string]any
		_ = json.Unmarshal(raw, &request)
		method, _ := request["method"].(string)
		node.mu.Lock()
		node.seen[method]++
		node.mu.Unlock()
		answer := reply(upArchive, request)
		if _, failed := answer["error"]; failed {
			// The boot's checks get the archive node's answers; the methods under test get a result.
			answer = map[string]any{"jsonrpc": "2.0", "id": request["id"], "result": "0x1"}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(answer)
	}))
	t.Cleanup(server.Close)
	node.url = server.URL
	return node
}

func (n *methodNode) counts() map[string]int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return maps.Clone(n.seen)
}

func relayMethod(t *testing.T, server *RPCSmartRouterServer, method, params string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":%s}`, method, params)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+server.GetListeningAddress()+"/", strings.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode, method)
}

func TestNodeUrlMethodsListPartitionsRelaysByMethod(t *testing.T) {
	const address = `"0x1111111111111111111111111111111111111111"`
	params := map[string]string{
		"eth_call":                `[{"to":` + address + `,"data":"0x"},"latest"]`,
		"eth_getBalance":          `[` + address + `,"latest"]`,
		"eth_getCode":             `[` + address + `,"latest"]`,
		"eth_getTransactionCount": `[` + address + `,"latest"]`,
	}
	methods := []string{"eth_call", "eth_getBalance", "eth_getCode", "eth_getTransactionCount"}
	const perMethod = 12

	for _, order := range []struct {
		name string
		urls func(a, b, c common.NodeUrl) []common.NodeUrl
	}{
		{"A, then B and C with methods (run 1)", func(a, b, c common.NodeUrl) []common.NodeUrl { return []common.NodeUrl{a, b, c} }},
		{"B with methods first, then A, then C (run 4)", func(a, b, c common.NodeUrl) []common.NodeUrl { return []common.NodeUrl{b, a, c} }},
	} {
		t.Run(order.name, func(t *testing.T) {
			a, b, c := newMethodNode(t), newMethodNode(t), newMethodNode(t)
			server := bootETH1Endpoint(t, []*routersession.RPCStaticProviderEndpoint{{
				Name: "fakeprovider1", ChainID: "ETH1", ApiInterface: "jsonrpc",
				NodeUrls: order.urls(
					common.NodeUrl{Url: a.url},
					common.NodeUrl{Url: b.url, Methods: []string{"eth_call"}},
					common.NodeUrl{Url: c.url, Methods: []string{"eth_call"}},
				),
			}}, nil, nil)
			before := [3]map[string]int{a.counts(), b.counts(), c.counts()}

			for range perMethod {
				for _, method := range methods {
					relayMethod(t, server, method, params[method])
				}
			}

			after := [3]map[string]int{a.counts(), b.counts(), c.counts()}
			got := map[string][3]int{}
			for _, method := range methods {
				got[method] = [3]int{
					after[0][method] - before[0][method],
					after[1][method] - before[1][method],
					after[2][method] - before[2][method],
				}
			}
			call := got["eth_call"]
			require.Zero(t, call[0], "A has no methods list, and eth_call goes to the urls that name it")
			require.Equal(t, perMethod, call[1]+call[2], "B and C take every eth_call between them")
			for _, method := range methods[1:] {
				require.Equal(t, [3]int{perMethod, 0, 0}, got[method], "[A, B, C] %s: B and C take only eth_call", method)
			}
		})
	}
}
