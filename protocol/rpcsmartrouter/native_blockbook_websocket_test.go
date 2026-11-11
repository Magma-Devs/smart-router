package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestNativeBlockbookConfiguration(t *testing.T) {
	v := viper.New()
	endpoints, err := parseNativeBlockbookEndpoints(v, "BTCT4", "jsonrpc")
	require.NoError(t, err)
	require.Empty(t, endpoints)
	config := `direct-rpc:
 - name: indexer
   chain-id: BTCT4
   api-interface: jsonrpc
   node-urls:
    - url: https://rpc.example.invalid/
      addons: [blockbook]
    - url: wss://native.example.invalid/
      addons: [blockbook]
      auth-config:
       auth-headers:
        x-api-key: fixture
    - url: wss://native.example.invalid/
      addons: [archive, blockbook]
      auth-config:
       auth-headers:
        x-api-key: fixture
    - url: wss://ordinary.example.invalid/
 - name: native-only
   chain-id: BTCT4
   api-interface: jsonrpc
   node-urls:
    - url: wss://other.example.invalid/
      addons: [blockbook]
backup-direct-rpc:
 - name: backup
   chain-id: BTCT4
   api-interface: jsonrpc
   node-urls:
    - url: wss://backup.example.invalid/
      addons: [blockbook]
`
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(config)))
	before := v.AllSettings()
	endpoints, err = parseNativeBlockbookEndpoints(v, "BTCT4", "jsonrpc")
	require.NoError(t, err)
	require.Len(t, endpoints, 3, "archive variants must not duplicate a native connection candidate")
	require.Equal(t, "indexer", endpoints[0].Name)
	require.Equal(t, "fixture", endpoints[0].NodeURL.GetAuthHeaders()["x-api-key"])
	require.Equal(t, "backup", endpoints[2].Name)
	endpoints, err = parseNativeBlockbookEndpoints(v, "SEP1", "jsonrpc")
	require.NoError(t, err)
	require.Empty(t, endpoints)
	endpoints, err = parseNativeBlockbookEndpoints(v, "BTCT4", "rest")
	require.NoError(t, err)
	require.Empty(t, endpoints)
	raw, err := ParseStaticProviderEndpoints(v, common.DirectRPCConfigName)
	require.NoError(t, err)
	ordinary := ordinaryRPCProviderEndpoints(raw)
	require.Len(t, ordinary, 1, "native-only providers do not participate in ordinary RPC polling")
	require.Len(t, ordinary[0].NodeUrls, 2)
	require.Equal(t, "https://rpc.example.invalid/", ordinary[0].NodeUrls[0].Url)
	require.Equal(t, "wss://ordinary.example.invalid/", ordinary[0].NodeUrls[1].Url)
	require.Len(t, raw[0].NodeUrls, 4, "filtering must not mutate the parsed provider or Viper")
	require.Equal(t, before, v.AllSettings())
	for _, raw := range []any{
		"invalid-with-secret",
		[]any{map[string]any{"chain-id": "BTCT4", "api-interface": "jsonrpc", "name": "test", "node-urls": []any{map[string]any{"url": "wss://%SECRET", "addons": []string{"blockbook"}}}}},
		[]any{nil},
	} {
		v.Set(common.DirectRPCConfigName, raw)
		_, err := parseNativeBlockbookEndpoints(v, "BTCT4", "jsonrpc")
		require.Error(t, err)
		require.NotContains(t, err.Error(), "secret")
		require.NotContains(t, err.Error(), "SECRET")
	}
}

func TestNativeBlockbookCandidateLimitIsPerListener(t *testing.T) {
	v := viper.New()
	urls := make([]common.NodeUrl, 9)
	for i := range urls {
		urls[i] = common.NodeUrl{Url: fmt.Sprintf("wss://node-%d.example.invalid/", i), Addons: []string{"blockbook"}}
	}
	v.Set(common.DirectRPCConfigName, []lavasession.RPCStaticProviderEndpoint{{Name: "indexer", ChainID: "BTCT4", ApiInterface: "jsonrpc", NodeUrls: urls}})
	_, err := parseNativeBlockbookEndpoints(v, "BTCT4", "jsonrpc")
	require.Error(t, err)
	endpoints, err := parseNativeBlockbookEndpoints(v, "SEP1", "jsonrpc")
	require.NoError(t, err)
	require.Empty(t, endpoints)
}

func TestNativeBlockbookHealthUsesOrdinaryURLs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yml")
	require.NoError(t, os.WriteFile(path, []byte(`direct-rpc:
 - name: indexer
   chain-id: BTCT4
   api-interface: jsonrpc
   node-urls:
    - url: https://rpc.example.invalid/
      addons: [blockbook]
    - url: wss://native.example.invalid/
      addons: [blockbook]
`), 0600))
	providers, err := collectHealthProviders([]string{path}, false, common.AuthConfig{})
	require.NoError(t, err)
	require.Len(t, providers, 1)
	require.Len(t, providers[0].nodeUrls, 1)
	require.Equal(t, "https://rpc.example.invalid/", providers[0].nodeUrls[0].Url)
}

func nativeTestSession(t *testing.T, endpoint string, send func(context.Context, []byte) bool, disconnected func()) *nativeBlockbookSession {
	t.Helper()
	server := &RPCSmartRouterServer{nativeBlockbookEndpoints: []nativeBlockbookEndpoint{{ChainID: "BTCT4", Name: "test", NodeURL: common.NodeUrl{Url: endpoint}}}}
	s, ok := server.NewNativeWebSocketSession(context.Background(), send, disconnected, nil).(*nativeBlockbookSession)
	require.True(t, ok)
	t.Cleanup(s.Close)
	return s
}

func nativeTestObject(t *testing.T, value any) map[string]any {
	t.Helper()
	object, ok := value.(map[string]any)
	require.True(t, ok)
	return object
}

func receiveNativeFrame(t *testing.T, frames <-chan []byte) map[string]any {
	t.Helper()
	select {
	case f := <-frames:
		var result map[string]any
		require.NoError(t, json.Unmarshal(f, &result))
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("native frame timed out")
		return nil
	}
}

func TestNativeBlockbookPreservesAckEventsAndClientIsolation(t *testing.T) {
	var opened, closed atomic.Int32
	upgrader := websocket.Upgrader{}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		opened.Add(1)
		defer closed.Add(1)
		defer conn.Close()
		for {
			_, frame, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var req struct {
				ID     string `json:"id"`
				Method string `json:"method"`
				Params struct {
					Addresses []string `json:"addresses"`
				} `json:"params"`
			}
			if json.Unmarshal(frame, &req) != nil {
				return
			}
			_ = conn.WriteJSON(map[string]any{"id": req.ID, "data": map[string]any{"subscribed": true}})
			_ = conn.WriteJSON(map[string]any{"id": req.ID, "data": map[string]any{"addresses": req.Params.Addresses, "height": 123}})
		}
	}))
	defer backend.Close()
	url := "ws" + strings.TrimPrefix(backend.URL, "http")
	a, b := make(chan []byte, 4), make(chan []byte, 4)
	sink := func(ch chan []byte) func(context.Context, []byte) bool {
		return func(ctx context.Context, f []byte) bool {
			select {
			case ch <- f:
				return true
			case <-ctx.Done():
				return false
			}
		}
	}
	s1 := nativeTestSession(t, url, sink(a), func() {})
	s2 := nativeTestSession(t, url, sink(b), func() {})
	for i, session := range []*nativeBlockbookSession{s1, s2} {
		address := []string{"address-a", "address-b"}[i]
		handled, err := session.Handle([]byte(`{"id":"same-client-id","method":"subscribeAddresses","params":{"addresses":["` + address + `"]}}`))
		require.True(t, handled)
		require.NoError(t, err)
	}
	for i, ch := range []chan []byte{a, b} {
		ack := receiveNativeFrame(t, ch)
		require.Equal(t, "same-client-id", ack["id"])
		require.Equal(t, true, nativeTestObject(t, ack["data"])["subscribed"])
		event := receiveNativeFrame(t, ch)
		require.Equal(t, []any{[]string{"address-a", "address-b"}[i]}, nativeTestObject(t, event["data"])["addresses"])
	}
	require.EqualValues(t, 2, opened.Load())
	s1.Close()
	s2.Close()
	require.Eventually(t, func() bool { return closed.Load() == 2 }, time.Second, 10*time.Millisecond)
}

func TestNativeBlockbookKeepsUnrecognizedAndWriteMethodsOffNativePath(t *testing.T) {
	s := nativeTestSession(t, "ws://127.0.0.1:1", func(context.Context, []byte) bool { t.Fatal("unexpected reply"); return false }, func() {})
	for _, method := range []string{"eth_subscribe", "bb_getAddress", "bb_subscribe", "alchemy_subscribeSubscribe", "importaddress", "rescanblockchain", "sendTransaction", "bb_sendTransaction"} {
		handled, err := s.Handle([]byte(`{"id":"1","method":"` + method + `","params":{}}`))
		require.False(t, handled, method)
		require.NoError(t, err)
	}
	require.Nil(t, s.conn)
}

func TestNativeBlockbookRejectsBadShapeAndMasksDialFailure(t *testing.T) {
	frames := make(chan []byte, 4)
	s := nativeTestSession(t, "ws://127.0.0.1:1/private-key-value", func(_ context.Context, f []byte) bool { frames <- f; return true }, func() {})
	for _, frame := range []string{
		`{"id":1,"method":"subscribeNewBlock","params":{}}`,
		`{"id":"1","method":"subscribeAddresses","params":[]}`,
		`{"id":"1","method":"subscribeNewBlock","params":{}}`,
	} {
		handled, err := s.Handle([]byte(frame))
		require.True(t, handled)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "private-key-value")
		ack := receiveNativeFrame(t, frames)
		data, _ := json.Marshal(ack)
		require.NotContains(t, string(data), "private-key-value")
	}
}

func TestNativeBlockbookUpstreamLossDisconnectsInsteadOfSilentlyRestoring(t *testing.T) {
	upgrader := websocket.Upgrader{}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"id":"1","data":{"subscribed":true}}`))
	}))
	defer backend.Close()
	frames := make(chan []byte, 1)
	disconnected := make(chan struct{}, 1)
	s := nativeTestSession(t, "ws"+strings.TrimPrefix(backend.URL, "http"), func(_ context.Context, f []byte) bool { frames <- f; return true }, func() { disconnected <- struct{}{} })
	handled, err := s.Handle([]byte(`{"id":"1","method":"subscribeNewBlock","params":{}}`))
	require.True(t, handled)
	require.NoError(t, err)
	receiveNativeFrame(t, frames)
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("upstream loss was hidden from the client")
	}
}

func TestNativeBlockbookUsesConfiguredAuthAndCountsProviderErrorAsFailure(t *testing.T) {
	upgrader := websocket.Upgrader{}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-fixture-auth") != "fixture" || r.URL.Query().Get("fixture") != "one" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":"1","error":{"code":-32600,"message":"method unavailable"}}`))
		_, _, _ = conn.ReadMessage()
	}))
	defer backend.Close()
	frames := make(chan []byte, 1)
	recorded := make(chan error, 1)
	s := nativeTestSession(t, "ws"+strings.TrimPrefix(backend.URL, "http"), func(_ context.Context, f []byte) bool { frames <- f; return true }, func() {})
	s.endpoints[0].NodeURL.AuthConfig.AuthHeaders = map[string]string{"x-fixture-auth": "fixture"}
	s.endpoints[0].NodeURL.AuthConfig.AuthQuery = "fixture=one"
	s.record = func(method, provider string, latency time.Duration, err error) {
		require.Equal(t, "getInfo", method)
		require.Equal(t, "test", provider)
		require.Positive(t, latency)
		recorded <- err
	}
	handled, err := s.Handle([]byte(`{"id":"1","method":"getInfo","params":{}}`))
	require.True(t, handled)
	require.NoError(t, err)
	frame := receiveNativeFrame(t, frames)
	require.EqualValues(t, -32600, nativeTestObject(t, frame["error"])["code"])
	select {
	case err := <-recorded:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("provider error was not accounted for")
	}
}

func TestNativeBlockbookCloseCancelsBlockedDownstream(t *testing.T) {
	upgrader := websocket.Upgrader{}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"id":"1","data":{"subscribed":true}}`))
		_, _, _ = conn.ReadMessage()
	}))
	defer backend.Close()
	blocked := make(chan struct{})
	s := nativeTestSession(t, "ws"+strings.TrimPrefix(backend.URL, "http"), func(ctx context.Context, _ []byte) bool {
		close(blocked)
		<-ctx.Done()
		return false
	}, func() {})
	handled, err := s.Handle([]byte(`{"id":"1","method":"subscribeNewBlock","params":{}}`))
	require.True(t, handled)
	require.NoError(t, err)
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("upstream acknowledgement did not arrive")
	}
	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("closing a slow client leaked its native session")
	}
}

func TestNativeBlockbookFactoryIsInertWithoutConfiguration(t *testing.T) {
	server := &RPCSmartRouterServer{}
	require.Nil(t, server.NewNativeWebSocketSession(context.Background(), nil, nil, nil))
}
