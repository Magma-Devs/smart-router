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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/goccy/go-json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
)

const (
	maxRequestContentLength = 1024 * 1024 * 5
	contentType             = "application/json"
)

type httpConn struct {
	client    *http.Client
	url       string
	closeOnce sync.Once
	closeCh   chan interface{}
	mu        utils.LavaMutex // protects headers
	headers   http.Header
}

// httpConn implements ServerCodec, but it is treated specially by Client
// and some methods don't work. The panic() stubs here exist to ensure
// this special treatment is correct.

func (hc *httpConn) writeJSON(context.Context, interface{}) error {
	panic("writeJSON called on httpConn")
}

func (hc *httpConn) peerInfo() PeerInfo {
	panic("peerInfo called on httpConn")
}

func (hc *httpConn) remoteAddr() string {
	return hc.url
}

func (hc *httpConn) readBatch() ([]*JsonrpcMessage, bool, error) {
	<-hc.closeCh
	return nil, false, io.EOF
}

func (hc *httpConn) close() {
	hc.closeOnce.Do(func() { close(hc.closeCh) })
}

func (hc *httpConn) closed() <-chan interface{} {
	return hc.closeCh
}

// HTTPTimeouts represents the configuration params for the HTTP RPC server.
type HTTPTimeouts struct {
	// ReadTimeout is the maximum duration for reading the entire
	// request, including the body.
	//
	// Because ReadTimeout does not let Handlers make per-request
	// decisions on each request body's acceptable deadline or
	// upload rate, most users will prefer to use
	// ReadHeaderTimeout. It is valid to use them both.
	ReadTimeout time.Duration

	// WriteTimeout is the maximum duration before timing out
	// writes of the response. It is reset whenever a new
	// request's header is read. Like ReadTimeout, it does not
	// let Handlers make decisions on a per-request basis.
	WriteTimeout time.Duration

	// IdleTimeout is the maximum amount of time to wait for the
	// next request when keep-alives are enabled. If IdleTimeout
	// is zero, the value of ReadTimeout is used. If both are
	// zero, ReadHeaderTimeout is used.
	IdleTimeout time.Duration
}

// DefaultHTTPTimeouts represents the default timeout values used if further
// configuration is not provided.
var DefaultHTTPTimeouts = HTTPTimeouts{
	ReadTimeout:  30 * time.Second,
	WriteTimeout: 30 * time.Second,
	IdleTimeout:  120 * time.Second,
}

// DialHTTPWithClient creates a new RPC client that connects to an RPC server over HTTP
// using the provided HTTP Client.
func DialHTTPWithClient(endpoint string, client *http.Client) (*Client, error) {
	// Sanity check URL so we don't end up with a client that will fail every request.
	_, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}

	initctx := context.Background()
	headers := make(http.Header, 2)
	headers.Set("accept", contentType)
	headers.Set("content-type", contentType)
	return newClient(initctx, func(context.Context) (ServerCodec, error) {
		hc := &httpConn{
			client:  client,
			headers: headers,
			url:     endpoint,
			closeCh: make(chan interface{}),
		}
		return hc, nil
	})
}

// DialHTTP creates a new RPC client that connects to an RPC server over HTTP.
// Uses a shared HTTP transport to maximize connection reuse and TLS session caching.
func DialHTTP(endpoint string) (*Client, error) {
	optimizedClient := &http.Client{
		Timeout:   common.DefaultHTTPTimeout, // 5 minute timeout for entire request/response cycle
		Transport: common.SharedHttpTransport(),
	}
	return DialHTTPWithClient(endpoint, optimizedClient)
}

func (c *Client) sendHTTP(ctx context.Context, op *requestOp, msg interface{}, isJsonRPC bool, strict bool) error {
	hc, ok := c.writeConn.(*httpConn)
	if !ok {
		return fmt.Errorf("sendHTTP - c.writeConn.(*httpConn) - type assertion failed %s", c.writeConn)
	}
	respBody, err := hc.doRequest(ctx, msg, isJsonRPC, strict)
	if err != nil {
		return err
	}
	defer respBody.Close()

	var respmsg JsonrpcMessage
	if err := json.NewDecoder(respBody).Decode(&respmsg); err != nil {
		return err
	}
	op.resp <- &respmsg
	return nil
}

func (c *Client) sendBatchHTTP(ctx context.Context, op *requestOp, msgs []*JsonrpcMessage, strict bool) error {
	hc, ok := c.writeConn.(*httpConn)
	if !ok {
		return fmt.Errorf("sendBatchHTTP - c.writeConn.(*httpConn) - type assertion failed, type: %s", c.writeConn)
	}
	respBody, err := hc.doRequest(ctx, msgs, true, strict)
	if err != nil {
		return err
	}
	defer respBody.Close()

	dec := json.NewDecoder(respBody)

	// Read start of array '['
	t, err := dec.Token()
	if err != nil {
		return err
	}
	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("expected start of array, got %v", t)
	}

	// Stream decode each message one by one to avoid buffering entire array in memory
	for dec.More() {
		var respmsg JsonrpcMessage
		if err := dec.Decode(&respmsg); err != nil {
			return err
		}
		select {
		case op.resp <- &respmsg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Read end of array ']'
	t, err = dec.Token()
	if err != nil {
		return err
	}
	if delim, ok := t.(json.Delim); !ok || delim != ']' {
		return fmt.Errorf("expected end of array, got %v", t)
	}

	return nil
}

func (hc *httpConn) doRequest(ctx context.Context, msg interface{}, isJsonRPC bool, strict bool) (io.ReadCloser, error) {
	body, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", hc.url, io.NopCloser(bytes.NewReader(body)))
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(len(body))
	req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }

	// set headers
	hc.mu.Lock()
	req.Header = hc.headers.Clone()
	hc.mu.Unlock()

	// do request
	resp, err := hc.client.Do(req)
	if resp != nil {
		// resp can be non nil on error
		metadata.AppendToOutgoingContext(ctx, common.StatusCodeMetadataKey, strconv.Itoa(resp.StatusCode))
		trailer := metadata.Pairs(common.StatusCodeMetadataKey, strconv.Itoa(resp.StatusCode))
		grpc.SetTrailer(ctx, trailer) // we ignore this error here since this code can be triggered not from grpc
	}
	if err != nil {
		return nil, err
	}

	err = ValidateStatusCodes(resp.StatusCode, strict)
	if err != nil {
		// Attach the upstream's Retry-After while the response is still in scope. Passes
		// through untouched for anything that is not a rate-limit.
		return nil, common.WithRetryAfter(err, resp.Header, time.Now())
	}

	if isJsonRPC && (resp.StatusCode < 200 || resp.StatusCode >= 300) {
		var buf bytes.Buffer
		var body []byte
		if _, err := buf.ReadFrom(resp.Body); err == nil {
			body = buf.Bytes()
		}

		return nil, HTTPError{
			Status:     resp.Status,
			StatusCode: resp.StatusCode,
			Body:       body,
		}
	}
	return resp.Body, nil
}

func ValidateStatusCodes(statusCode int, strict bool) error {
	if statusCode == http.StatusGatewayTimeout {
		return common.StatusCodeError504
	} else if statusCode == http.StatusTooManyRequests {
		return common.StatusCodeError429
	}
	if strict {
		if (statusCode != 0 && statusCode < 200) || statusCode > 300 {
			return common.StatusCodeErrorStrict
		}
	}
	return nil
}
