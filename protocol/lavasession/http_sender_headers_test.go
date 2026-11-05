package lavasession

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// The two HTTP senders set headers by one set of rules (MAG-4278 item 17): a caller's
// Content-Type wins over the default, the client's address is forwarded when the url asks for
// it, and an empty caller value deletes the header. SendRequest used to set application/json
// after the caller's headers, skip IP forwarding and send an empty value as an empty header.
func TestHTTPSenders_SetHeadersByTheSameRules(t *testing.T) {
	senders := map[string]func(h *HTTPDirectRPCConnection, ctx context.Context, url string, headers map[string]string) error{
		"SendRequest": func(h *HTTPDirectRPCConnection, ctx context.Context, _ string, headers map[string]string) error {
			_, err := h.SendRequest(ctx, []byte(`{}`), headers)
			return err
		},
		"DoHTTPRequest": func(h *HTTPDirectRPCConnection, ctx context.Context, url string, headers map[string]string) error {
			params := HTTPRequestParams{Method: http.MethodPost, URL: url, Body: []byte(`{}`), ContentType: "application/json"}
			for name, value := range headers {
				params.Headers = append(params.Headers, pairingtypes.Metadata{Name: name, Value: value})
			}
			_, err := h.DoHTTPRequest(ctx, params)
			return err
		},
	}

	// sendAndSee sends one request with the caller's headers and returns what the node received.
	sendAndSee := func(t *testing.T, send func(*HTTPDirectRPCConnection, context.Context, string, map[string]string) error, headers map[string]string) (http.Header, *HTTPDirectRPCConnection) {
		var seen http.Header
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seen = r.Header.Clone()
			_, _ = w.Write([]byte(`{}`))
		}))
		defer srv.Close()

		h := newHTTPConnection(t, common.NodeUrl{
			Url:          srv.URL,
			IpForwarding: true,
			AuthConfig:   common.AuthConfig{AuthHeaders: map[string]string{"X-Api-Key": "secret"}},
		})
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(common.IP_FORWARDING_HEADER_NAME, "203.0.113.7"))
		require.NoError(t, send(h, ctx, srv.URL, headers))
		return seen, h
	}

	for name, send := range senders {
		t.Run(name, func(t *testing.T) {
			t.Run("defaults", func(t *testing.T) {
				seen, h := sendAndSee(t, send, nil)
				require.Equal(t, "application/json", seen.Get("Content-Type"))
				require.Equal(t, "secret", seen.Get("X-Api-Key"), "the url's auth header")
				require.Equal(t, h.nodeUrl.UpstreamAcceptEncoding(), seen.Get("Accept-Encoding"))
			})
			t.Run("the client's address is forwarded when the url asks for it", func(t *testing.T) {
				seen, _ := sendAndSee(t, send, nil)
				require.Equal(t, "203.0.113.7", seen.Get(common.IP_FORWARDING_HEADER_NAME))
			})
			t.Run("a caller's content type wins over the default", func(t *testing.T) {
				seen, _ := sendAndSee(t, send, map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
				require.Equal(t, "application/x-www-form-urlencoded", seen.Get("Content-Type"))
			})
			t.Run("an empty caller value deletes the header", func(t *testing.T) {
				seen, _ := sendAndSee(t, send, map[string]string{"X-Api-Key": "", "X-Spec-Directive": "on"})
				_, present := seen["X-Api-Key"]
				require.False(t, present, "sent as %q", seen.Values("X-Api-Key"))
				require.Equal(t, "on", seen.Get("X-Spec-Directive"))
			})
			t.Run("no caller header overrides Accept-Encoding", func(t *testing.T) {
				seen, h := sendAndSee(t, send, map[string]string{"Accept-Encoding": "gzip"})
				require.Equal(t, h.nodeUrl.UpstreamAcceptEncoding(), seen.Get("Accept-Encoding"))
			})
		})
	}
}

// Every producer of an HTTPStatusError builds it the same way, so a rejection reads the same
// whichever path saw it: the code, the upstream's own status line, the body and Retry-After.
func TestNewHTTPStatusError_MatchesWhatTheSenderReturns(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`slow down`))
	}))
	defer srv.Close()

	h := newHTTPConnection(t, common.NodeUrl{Url: srv.URL})
	resp, sendErr := h.SendRequest(context.Background(), []byte(`{}`), nil)
	require.Error(t, sendErr)

	built := NewHTTPStatusError(resp.StatusCode, "429 Too Many Requests", resp.Metadata, resp.Data)
	require.Equal(t, built, sendErr)
	require.Equal(t, 7*time.Second, built.RetryAfter)
	require.Equal(t, "429 Too Many Requests", built.Status)
	require.Equal(t, []byte(`slow down`), built.Body)
}
