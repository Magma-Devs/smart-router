// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rpcclient

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/common"
)

const (
	wsReadBuffer       = 1024
	wsWriteBuffer      = 1024
	wsPingInterval     = 60 * time.Second
	wsPingWriteTimeout = 5 * time.Second
	wsPongTimeout      = 30 * time.Second
	wsMessageSizeLimit = 15 * 1024 * 1024
)

var wsBufferPool = new(sync.Pool)

type wsHandshakeError struct {
	err        error
	status     string
	statusCode int
	retryAfter time.Duration
}

func (e wsHandshakeError) Error() string {
	s := e.err.Error()
	if e.status != "" {
		s += " (HTTP status " + e.status + ")"
	}
	return s
}

// Unwrap presents a 429 upgrade rejection as the typed rate-limit error, so
// errors.Is(err, common.StatusCodeError429) and common.RetryAfterFrom read the same on a
// refused WS handshake as on every HTTP transport. Any other status unwraps to the dial
// error itself.
func (e wsHandshakeError) Unwrap() error {
	if e.statusCode == http.StatusTooManyRequests {
		return &common.RateLimitedError{RetryAfter: e.retryAfter}
	}
	return e.err
}

// DialWebsocketWithDialer creates a new RPC client that communicates with a JSON-RPC server
// that is listening on the given endpoint using the provided dialer.
func DialWebsocketWithDialer(ctx context.Context, endpoint string, dialer websocket.Dialer, headers map[string]string) (*Client, error) {
	endpoint, header, err := wsClientHeaders(endpoint, headers)
	if err != nil {
		return nil, err
	}
	return newClient(ctx, func(ctx context.Context) (ServerCodec, error) {
		conn, resp, err := dialer.DialContext(ctx, endpoint, header)
		if err != nil {
			hErr := wsHandshakeError{err: err}
			if resp != nil {
				// Capture the rejection while the response is in scope — the status code
				// types a 429 (see Unwrap), and Retry-After is what any backoff needs.
				hErr.status = resp.Status
				hErr.statusCode = resp.StatusCode
				hErr.retryAfter, _ = common.ParseRetryAfter(resp.Header, time.Now())
			}
			return nil, hErr
		}
		return newWebsocketCodec(conn, endpoint, header), nil
	})
}

// DialWebsocket creates a new RPC client that communicates with a JSON-RPC server
// that is listening on the given endpoint.
//
// The context is used for the initial connection establishment. It does not
// affect subsequent interactions with the client.
func DialWebsocket(ctx context.Context, endpoint string, headers map[string]string) (*Client, error) {
	dialer := websocket.Dialer{
		ReadBufferSize:  wsReadBuffer,
		WriteBufferSize: wsWriteBuffer,
		WriteBufferPool: wsBufferPool,
	}
	return DialWebsocketWithDialer(ctx, endpoint, dialer, headers)
}

func wsClientHeaders(endpoint string, headers map[string]string) (string, http.Header, error) {
	endpointURL, err := url.Parse(endpoint)
	if err != nil {
		return endpoint, nil, err
	}
	header := make(http.Header)
	for headerKey, headerValue := range headers {
		header.Add(headerKey, headerValue)
	}
	if endpointURL.User != nil {
		b64auth := base64.StdEncoding.EncodeToString([]byte(endpointURL.User.String()))
		header.Add("authorization", "Basic "+b64auth)
		endpointURL.User = nil
	}
	return endpointURL.String(), header, nil
}

type websocketCodec struct {
	*jsonCodec
	conn *websocket.Conn
	info PeerInfo

	wg        sync.WaitGroup
	pingReset chan struct{}
}

func newWebsocketCodec(conn *websocket.Conn, host string, req http.Header) ServerCodec {
	conn.SetReadLimit(wsMessageSizeLimit)
	conn.SetPongHandler(func(appData string) error {
		conn.SetReadDeadline(time.Time{})
		return nil
	})
	newCodec := NewFuncCodec(conn, conn.WriteJSON, conn.ReadJSON)
	codec, ok := newCodec.(*jsonCodec)
	if !ok {
		panic("newWebsocketCodec - newCodec.(*jsonCodec) - type assertion failed, type:" + fmt.Sprintf("%s", newCodec))
	}

	wc := &websocketCodec{
		jsonCodec: codec,
		conn:      conn,
		pingReset: make(chan struct{}, 1),
		info: PeerInfo{
			Transport:  "ws",
			RemoteAddr: conn.RemoteAddr().String(),
		},
	}
	// Fill in connection details.
	wc.info.HTTP.Host = host
	wc.info.HTTP.Origin = req.Get("Origin")
	wc.info.HTTP.UserAgent = req.Get("User-Agent")
	// Start pinger.
	wc.wg.Add(1)
	go wc.pingLoop()
	return wc
}

func (wc *websocketCodec) close() {
	wc.jsonCodec.close()
	wc.wg.Wait()
}

func (wc *websocketCodec) peerInfo() PeerInfo {
	return wc.info
}

func (wc *websocketCodec) writeJSON(ctx context.Context, v interface{}) error {
	err := wc.jsonCodec.writeJSON(ctx, v)
	if err == nil {
		// Notify pingLoop to delay the next idle ping.
		select {
		case wc.pingReset <- struct{}{}:
		default:
		}
	}
	return err
}

// pingLoop sends periodic ping frames when the connection is idle.
func (wc *websocketCodec) pingLoop() {
	timer := time.NewTimer(wsPingInterval)
	defer wc.wg.Done()
	defer timer.Stop()

	for {
		select {
		case <-wc.closed():
			return
		case <-wc.pingReset:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(wsPingInterval)
		case <-timer.C:
			wc.jsonCodec.encMu.Lock()
			wc.conn.SetWriteDeadline(time.Now().Add(wsPingWriteTimeout))
			wc.conn.WriteMessage(websocket.PingMessage, nil)
			wc.conn.SetReadDeadline(time.Now().Add(wsPongTimeout))
			wc.jsonCodec.encMu.Unlock()
			timer.Reset(wsPingInterval)
		}
	}
}
