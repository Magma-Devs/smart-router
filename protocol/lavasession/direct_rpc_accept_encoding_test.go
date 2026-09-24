package lavasession

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// encodingUpstream answers every request with the same JSON body, gzipped when
// gzipReply says so for the Accept-Encoding the request carried, and records what
// each request carried.
type encodingUpstream struct {
	plain, compressed []byte
	status            int
	gzipReply         func(acceptEncoding string) bool

	mu              sync.Mutex
	acceptEncodings []string
	protoMajors     []int
}

func newEncodingUpstream(t *testing.T, gzipReply func(acceptEncoding string) bool) *encodingUpstream {
	plain := []byte(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":449994332},"value":42}}`)
	return &encodingUpstream{plain: plain, compressed: gzipBytes(t, plain), status: http.StatusOK, gzipReply: gzipReply}
}

func (u *encodingUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	acceptEncoding := r.Header.Get("Accept-Encoding")
	u.mu.Lock()
	u.acceptEncodings = append(u.acceptEncodings, acceptEncoding)
	u.protoMajors = append(u.protoMajors, r.ProtoMajor)
	u.mu.Unlock()

	body := u.plain
	w.Header().Set("Content-Type", "application/json")
	if u.gzipReply(acceptEncoding) {
		body = u.compressed
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(u.status)
	_, _ = w.Write(body)
}

func (u *encodingUpstream) seen() (acceptEncodings []string, protoMajors []int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string{}, u.acceptEncodings...), append([]int{}, u.protoMajors...)
}

func gzipWhenAsked(acceptEncoding string) bool { return acceptEncoding == common.AcceptEncodingGzip }
func gzipAlways(string) bool                   { return true }
func gzipNever(string) bool                    { return false }

func newHTTPConnection(t *testing.T, nodeUrl common.NodeUrl) *HTTPDirectRPCConnection {
	t.Helper()
	conn, err := NewDirectRPCConnection(context.Background(), nodeUrl, 1, "")
	require.NoError(t, err)
	h, ok := conn.(*HTTPDirectRPCConnection)
	require.True(t, ok, "an http url must get an *HTTPDirectRPCConnection")
	return h
}

// requireInflatedReply asserts a reply reached the caller as the upstream's JSON,
// with headers that say so: no gzip coding, and no length of the compressed bytes.
func requireInflatedReply(t *testing.T, want, body []byte, header map[string][]string) {
	t.Helper()
	require.Equal(t, string(want), string(body))
	require.Empty(t, http.Header(header).Values("Content-Encoding"),
		"the headers travel to the client with the body; a gzip coding there would label plain JSON as gzip")
	require.Empty(t, http.Header(header).Values("Content-Length"),
		"the upstream's Content-Length counts the compressed bytes")
	require.Equal(t, "application/json", http.Header(header).Get("Content-Type"))
}

// A url that opted in (MAG-3844) asks for gzip on both send paths, whatever the
// caller's headers say, and hands back the reply inflated.
func TestHTTPDirectRPCConnection_AcceptEncodingGzip(t *testing.T) {
	upstream := newEncodingUpstream(t, gzipWhenAsked)
	srv := httptest.NewServer(upstream)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := newHTTPConnection(t, common.NodeUrl{Url: srv.URL, AcceptEncoding: "gzip"})

	// SendRequest: the chain tracker's polls and the recovery probe. A caller header
	// cannot replace the url's choice.
	sendResp, err := h.SendRequest(ctx, []byte(`{"jsonrpc":"2.0","id":1}`), map[string]string{"Accept-Encoding": "identity"})
	require.NoError(t, err)
	requireInflatedReply(t, upstream.plain, sendResp.Data, sendResp.Metadata)

	// DoHTTPRequest: every JSON-RPC and REST relay. Neither a spec header nor its
	// delete form (empty value) can replace the url's choice.
	for _, header := range []pairingtypes.Metadata{{Name: "Accept-Encoding", Value: "br"}, {Name: "Accept-Encoding", Value: ""}} {
		doResp, err := h.DoHTTPRequest(ctx, HTTPRequestParams{
			Method:      http.MethodPost,
			URL:         srv.URL,
			Body:        []byte(`{"jsonrpc":"2.0","id":1}`),
			Headers:     []pairingtypes.Metadata{header},
			ContentType: "application/json",
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, doResp.StatusCode)
		requireInflatedReply(t, upstream.plain, doResp.Body, doResp.Headers)
	}

	acceptEncodings, _ := upstream.seen()
	require.Equal(t, []string{"gzip", "gzip", "gzip"}, acceptEncodings)
}

// The option is case- and space-insensitive, like the header it sets.
func TestHTTPDirectRPCConnection_AcceptEncodingGzip_ValueIsNormalized(t *testing.T) {
	upstream := newEncodingUpstream(t, gzipWhenAsked)
	srv := httptest.NewServer(upstream)
	defer srv.Close()

	h := newHTTPConnection(t, common.NodeUrl{Url: srv.URL, AcceptEncoding: " GZip "})
	resp, err := h.SendRequest(context.Background(), []byte(`{}`), nil)
	require.NoError(t, err)
	requireInflatedReply(t, upstream.plain, resp.Data, resp.Metadata)

	acceptEncodings, _ := upstream.seen()
	require.Equal(t, []string{"gzip"}, acceptEncodings)
}

// Asking is not getting: an upstream may answer identity anyway, and its reply
// comes back as it arrived.
func TestHTTPDirectRPCConnection_AcceptEncodingGzip_UpstreamAnswersIdentity(t *testing.T) {
	upstream := newEncodingUpstream(t, gzipNever)
	srv := httptest.NewServer(upstream)
	defer srv.Close()

	h := newHTTPConnection(t, common.NodeUrl{Url: srv.URL, AcceptEncoding: common.AcceptEncodingGzip})
	resp, err := h.SendRequest(context.Background(), []byte(`{}`), nil)
	require.NoError(t, err)
	require.Equal(t, string(upstream.plain), string(resp.Data))
	require.Equal(t, strconv.Itoa(len(upstream.plain)), http.Header(resp.Metadata).Get("Content-Length"),
		"nothing was inflated, so the upstream's length still describes the body")
}

// Every url that did not opt in keeps asking for identity, whatever the caller's
// headers say. If its upstream gzips regardless, the reply is inflated: before,
// the raw gzip reached the JSON-RPC relay path and failed as malformed JSON.
func TestHTTPDirectRPCConnection_DefaultUrl_InflatesUnaskedGzip(t *testing.T) {
	upstream := newEncodingUpstream(t, gzipAlways)
	srv := httptest.NewServer(upstream)
	defer srv.Close()

	ctx := context.Background()
	h := newHTTPConnection(t, common.NodeUrl{Url: srv.URL})

	sendResp, err := h.SendRequest(ctx, []byte(`{}`), map[string]string{"Accept-Encoding": "gzip"})
	require.NoError(t, err)
	requireInflatedReply(t, upstream.plain, sendResp.Data, sendResp.Metadata)

	doResp, err := h.DoHTTPRequest(ctx, HTTPRequestParams{
		Method:  http.MethodGet,
		URL:     srv.URL,
		Headers: []pairingtypes.Metadata{{Name: "Accept-Encoding", Value: "gzip"}},
	})
	require.NoError(t, err)
	requireInflatedReply(t, upstream.plain, doResp.Body, doResp.Headers)

	acceptEncodings, _ := upstream.seen()
	require.Equal(t, []string{"identity", "identity"}, acceptEncodings)
}

// An error status keeps its body for the node-error classifiers, so a gzipped
// one has to arrive inflated too.
func TestHTTPDirectRPCConnection_AcceptEncodingGzip_ErrorStatusBodyIsInflated(t *testing.T) {
	upstream := newEncodingUpstream(t, gzipWhenAsked)
	upstream.status = http.StatusTooManyRequests
	srv := httptest.NewServer(upstream)
	defer srv.Close()

	h := newHTTPConnection(t, common.NodeUrl{Url: srv.URL, AcceptEncoding: common.AcceptEncodingGzip})
	resp, err := h.SendRequest(context.Background(), []byte(`{}`), nil)
	var statusErr *HTTPStatusError
	require.True(t, errors.As(err, &statusErr), "a 429 is an HTTPStatusError, got %v", err)
	require.Equal(t, string(upstream.plain), string(statusErr.Body))
	requireInflatedReply(t, upstream.plain, resp.Data, resp.Metadata)
}

// Production upstreams answer over HTTP/2, where net/http's transparent decoding is
// a separate implementation (http2gzipReader, the MAG-1589 hot path). Asking for
// gzip by hand keeps it off there too, so each reply is inflated exactly once.
func TestHTTPDirectRPCConnection_AcceptEncodingGzip_OverHTTP2(t *testing.T) {
	upstream := newEncodingUpstream(t, gzipWhenAsked)
	srv := httptest.NewUnstartedServer(upstream)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	// The test server's certificate is trusted only by its own client, so the
	// connection gets that client instead of the shared transport.
	h := &HTTPDirectRPCConnection{
		nodeUrl:  common.NodeUrl{Url: srv.URL, AcceptEncoding: common.AcceptEncodingGzip},
		protocol: DirectRPCProtocolHTTPS,
		client:   srv.Client(),
	}

	ctx := context.Background()
	sendResp, err := h.SendRequest(ctx, []byte(`{}`), nil)
	require.NoError(t, err)
	requireInflatedReply(t, upstream.plain, sendResp.Data, sendResp.Metadata)

	doResp, err := h.DoHTTPRequest(ctx, HTTPRequestParams{Method: http.MethodGet, URL: srv.URL})
	require.NoError(t, err)
	requireInflatedReply(t, upstream.plain, doResp.Body, doResp.Headers)

	acceptEncodings, protoMajors := upstream.seen()
	require.Equal(t, []string{"gzip", "gzip"}, acceptEncodings)
	require.Equal(t, []int{2, 2}, protoMajors, "both requests must have gone over HTTP/2")
}
