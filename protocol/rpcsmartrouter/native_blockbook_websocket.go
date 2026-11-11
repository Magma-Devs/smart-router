package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/spf13/viper"
	"github.com/tidwall/gjson"
)

// This is a separate protocol surface: Blockbook replies and pushes both echo
// the request id in {id,data}. They have no JSON-RPC subscription id to rewrite.
// In particular, sharing an upstream would let subscribeAddresses replace another
// customer's address list. One downstream therefore owns one upstream connection.
type nativeBlockbookEndpoint struct {
	ChainID string         `mapstructure:"chain-id"`
	Name    string         `mapstructure:"name"`
	NodeURL common.NodeUrl `mapstructure:"node-url"`
}

func parseNativeBlockbookEndpoints(v *viper.Viper, chainID, apiInterface string) ([]nativeBlockbookEndpoint, error) {
	endpoints := make([]nativeBlockbookEndpoint, 0, 8)
	seen := map[string]bool{}
	for _, key := range []string{common.DirectRPCConfigName, common.BackupDirectRPCConfigName} {
		var configured []*lavasession.RPCStaticProviderEndpoint
		if err := v.UnmarshalKey(key, &configured); err != nil {
			return nil, errors.New("native Blockbook: invalid provider configuration shape")
		}
		for i, provider := range configured {
			if provider == nil {
				return nil, errors.New("native Blockbook: provider entry must not be null")
			}
			for _, node := range provider.NodeUrls {
				if !isNativeBlockbookURL(provider, node) {
					continue
				}
				u, err := url.Parse(node.Url)
				if err != nil || u.Hostname() == "" || u.Fragment != "" || strings.TrimSpace(provider.Name) == "" || provider.ChainID == "" {
					return nil, fmt.Errorf("native Blockbook: %s entry %d requires a chain-id, name and valid ws(s) URL", key, i)
				}
				if !strings.EqualFold(provider.ChainID, chainID) || apiInterface != "jsonrpc" {
					continue
				}
				// Charts can emit archive/non-archive variants of one URL. The
				// native transport needs one candidate per URL and authentication.
				identity, _ := json.Marshal([]any{provider.Name, node.Url, node.AuthConfig})
				if seen[string(identity)] {
					continue
				}
				seen[string(identity)] = true
				endpoints = append(endpoints, nativeBlockbookEndpoint{ChainID: provider.ChainID, Name: provider.Name, NodeURL: node})
			}
		}
	}
	if len(endpoints) > 8 {
		return nil, errors.New("native Blockbook: at most 8 WebSocket endpoints per listener are allowed")
	}
	return endpoints, nil
}

func isNativeBlockbookURL(provider *lavasession.RPCStaticProviderEndpoint, node common.NodeUrl) bool {
	if provider.ApiInterface != "jsonrpc" {
		return false
	}
	scheme := strings.ToLower(node.Url)
	if !strings.HasPrefix(scheme, "wss:") && !strings.HasPrefix(scheme, "ws:") {
		return false
	}
	for _, addon := range node.Addons {
		if strings.EqualFold(addon, "blockbook") {
			return true
		}
	}
	return false
}

// Keep native URLs out of JSON-RPC boot verification, polling and relays.
// Copy each provider so the shared Viper configuration retains its native URLs.
func ordinaryRPCProviderEndpoints(providers []*lavasession.RPCStaticProviderEndpoint) []*lavasession.RPCStaticProviderEndpoint {
	ordinary := make([]*lavasession.RPCStaticProviderEndpoint, 0, len(providers))
	for _, provider := range providers {
		copy := *provider
		copy.NodeUrls = make([]common.NodeUrl, 0, len(provider.NodeUrls))
		for _, node := range provider.NodeUrls {
			if !isNativeBlockbookURL(provider, node) {
				copy.NodeUrls = append(copy.NodeUrls, node)
			}
		}
		if len(copy.NodeUrls) > 0 {
			ordinary = append(ordinary, &copy)
		}
	}
	return ordinary
}

// Only documented native Blockbook reads and subscriptions enter this path.
// Writes stay on the router's existing transaction path, with its write guards.
var nativeBlockbookMethods = map[string]bool{
	"getInfo": true, "getBlockHash": true, "getBlock": true,
	"getAccountInfo": true, "getAccountUtxo": true, "getBalanceHistory": true,
	"getTransaction": true, "getTransactionSpecific": true, "estimateFee": true,
	"getCurrentFiatRates": true, "getFiatRatesForTimestamps": true, "getFiatRatesTickersList": true,
	"subscribeNewBlock": true, "unsubscribeNewBlock": true,
	"subscribeAddresses": true, "unsubscribeAddresses": true,
	"subscribeNewTransaction": true, "unsubscribeNewTransaction": true,
	"subscribeFiatRates": true, "unsubscribeFiatRates": true, "ping": true,
}

type nativeBlockbookSession struct {
	ctx        context.Context
	cancel     context.CancelFunc
	endpoints  []nativeBlockbookEndpoint
	send       func(context.Context, []byte) bool
	disconnect func()
	mu         sync.Mutex
	conn       *websocket.Conn
	done       chan struct{}
	started    bool
	provider   string
	requests   map[string]nativeBlockbookRequest
	record     func(string, string, time.Duration, error)
}

type nativeBlockbookRequest struct {
	method string
	start  time.Time
	acked  bool
}

func (rpcss *RPCSmartRouterServer) NewNativeWebSocketSession(ctx context.Context, send func(context.Context, []byte) bool, disconnect func(), record func(string, string, time.Duration, error)) chainlib.NativeWebSocketSession {
	if len(rpcss.nativeBlockbookEndpoints) == 0 {
		return nil
	}
	child, cancel := context.WithCancel(ctx)
	return &nativeBlockbookSession{ctx: child, cancel: cancel, endpoints: rpcss.nativeBlockbookEndpoints, send: send, disconnect: disconnect, done: make(chan struct{}), requests: map[string]nativeBlockbookRequest{}, record: record}
}

func (s *nativeBlockbookSession) Handle(frame []byte) (bool, error) {
	if len(frame) > 64*1024 {
		method := gjson.GetBytes(frame, "method").Str
		if !nativeBlockbookMethods[method] {
			return false, nil
		}
		err := errors.New("native Blockbook frames must not exceed 64 KiB")
		s.replyError(nil, err.Error())
		s.recordFailure("", method, err)
		return true, err
	}
	var request struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if json.Unmarshal(frame, &request) != nil || !nativeBlockbookMethods[request.Method] {
		return false, nil
	}
	var id string
	if json.Unmarshal(request.ID, &id) != nil || id == "" || len(id) > 128 || (len(request.Params) != 0 && request.Params[0] != '{') {
		err := errors.New("native Blockbook requests require a nonempty string id and object params (maximum frame 64 KiB)")
		s.replyError(request.ID, err.Error())
		s.recordFailure(id, request.Method, err)
		return true, err
	}
	conn, err := s.connection()
	if err != nil {
		s.replyError(request.ID, "Blockbook WebSocket upstream is unavailable")
		s.recordFailure(id, request.Method, err)
		return true, err
	}
	s.mu.Lock()
	if _, exists := s.requests[id]; !exists && len(s.requests) >= 1024 {
		s.mu.Unlock()
		s.replyError(request.ID, "too many outstanding Blockbook request ids")
		err := errors.New("too many outstanding Blockbook request ids")
		s.recordFailure(id, request.Method, err)
		return true, err
	}
	if strings.HasPrefix(request.Method, "subscribe") {
		for oldID, old := range s.requests {
			if old.method == request.Method && oldID != id {
				delete(s.requests, oldID)
			}
		}
	}
	s.requests[id] = nativeBlockbookRequest{method: request.Method, start: time.Now()}
	s.mu.Unlock()
	if conn.SetWriteDeadline(time.Now().Add(10*time.Second)) != nil || conn.WriteMessage(websocket.TextMessage, frame) != nil {
		err := errors.New("blockbook WebSocket upstream write failed; reconnect and resubscribe")
		s.recordFailure(id, request.Method, err)
		s.cancel()
		_ = conn.Close()
		s.disconnect()
		return true, err
	}
	return true, nil
}

func (s *nativeBlockbookSession) replyError(id json.RawMessage, message string) {
	if !json.Valid(id) {
		id = json.RawMessage(`null`)
	}
	frame, _ := json.Marshal(struct {
		ID   json.RawMessage `json:"id"`
		Data any             `json:"data"`
	}{id, map[string]any{"error": map[string]string{"message": message}}})
	s.send(s.ctx, frame)
}

func (s *nativeBlockbookSession) connection() (*websocket.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return nil, errors.New("blockbook WebSocket session is closed")
	}
	if s.conn != nil {
		return s.conn, nil
	}
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	for _, ep := range s.endpoints {
		headers := http.Header{}
		for k, v := range ep.NodeURL.GetAuthHeaders() {
			headers.Set(k, v)
		}
		endpoint := ep.NodeURL.AuthConfig.AddAuthPath(ep.NodeURL.Url)
		dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
		conn, response, err := dialer.DialContext(ctx, endpoint, headers)
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		if err != nil {
			continue
		}
		conn.SetReadLimit(2 * 1024 * 1024)
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(90 * time.Second)) })
		s.conn, s.started, s.provider = conn, true, ep.Name
		go s.read(conn)
		go s.keepAlive(conn)
		go func() {
			select {
			case <-s.ctx.Done():
				_ = conn.Close()
			case <-s.done:
			}
		}()
		return conn, nil
	}
	return nil, errors.New("no configured Blockbook WebSocket upstream accepted the connection")
}

func (s *nativeBlockbookSession) keepAlive(conn *websocket.Conn) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.done:
			return
		case <-ticker.C:
			if conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)) != nil {
				_ = conn.Close()
				return
			}
		}
	}
}

func (s *nativeBlockbookSession) read(conn *websocket.Conn) {
	defer close(s.done)
	defer conn.Close()
	for {
		kind, frame, err := conn.ReadMessage()
		if err != nil {
			if s.ctx.Err() == nil {
				s.disconnect()
			}
			return
		}
		if kind != websocket.TextMessage || !json.Valid(frame) {
			s.disconnect()
			return
		}
		s.recordFrame(frame)
		if !s.send(s.ctx, frame) {
			return
		}
	}
}

func (s *nativeBlockbookSession) recordFrame(frame []byte) {
	var response struct {
		ID    string          `json:"id"`
		Data  json.RawMessage `json:"data"`
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(frame, &response) != nil {
		return
	}
	var data struct {
		Subscribed bool            `json:"subscribed"`
		Error      json.RawMessage `json:"error"`
	}
	_ = json.Unmarshal(response.Data, &data)
	failed := len(response.Error) > 0 && string(response.Error) != "null" || len(data.Error) > 0 && string(data.Error) != "null"
	s.mu.Lock()
	request, exists := s.requests[response.ID]
	wasAcked := request.acked
	if exists {
		if !failed && (data.Subscribed || request.acked) && strings.HasPrefix(request.method, "subscribe") {
			request.acked = true
			s.requests[response.ID] = request
		} else {
			delete(s.requests, response.ID)
			if strings.HasPrefix(request.method, "unsubscribe") {
				for id, sub := range s.requests {
					if sub.method == strings.TrimPrefix(request.method, "un") {
						delete(s.requests, id)
					}
				}
			}
		}
	}
	provider := s.provider
	s.mu.Unlock()
	if exists && s.record != nil {
		var err error
		if failed {
			err = errors.New("blockbook upstream returned an error")
		}
		latency := time.Duration(0)
		if !wasAcked {
			latency = time.Since(request.start)
		}
		s.record(request.method, provider, latency, err)
	}
}

func (s *nativeBlockbookSession) recordFailure(id, method string, err error) {
	s.mu.Lock()
	delete(s.requests, id)
	provider := s.provider
	s.mu.Unlock()
	if s.record != nil {
		s.record(method, provider, 0, err)
	}
}

func (s *nativeBlockbookSession) Close() {
	s.cancel()
	s.mu.Lock()
	conn, started := s.conn, s.started
	s.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	if started {
		<-s.done
	}
}
