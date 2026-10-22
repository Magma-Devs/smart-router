package lavasession

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// The upstream's own status line rides on the error and on the REST response, so the client
// can be shown what the node said (MAG-4218).
func TestHTTPDirectRPCConnection_StatusLineReachesTheCaller(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`upstream down`))
	}))
	defer srv.Close()

	ctx := context.Background()
	h := newHTTPConnection(t, common.NodeUrl{Url: srv.URL})

	_, sendErr := h.SendRequest(ctx, []byte(`{}`), nil)
	var httpErr *HTTPStatusError
	require.True(t, errors.As(sendErr, &httpErr), "a 503 is an HTTPStatusError, got %v", sendErr)
	require.Equal(t, http.StatusServiceUnavailable, httpErr.StatusCode)
	require.Equal(t, "503 Service Unavailable", httpErr.Status)
	require.Equal(t, []byte(`upstream down`), httpErr.Body)
	require.Equal(t, "HTTP 503 Service Unavailable", httpErr.Error(), "the code is not printed twice")

	doResp, err := h.DoHTTPRequest(ctx, HTTPRequestParams{Method: http.MethodGet, URL: srv.URL})
	require.NoError(t, err, "DoHTTPRequest returns error statuses as a response, not an error")
	require.Equal(t, http.StatusServiceUnavailable, doResp.StatusCode)
	require.Equal(t, "503 Service Unavailable", doResp.Status)
}

// Error() is one text per status code whatever the upstream wrote on its status line: the
// best-error picker groups failed providers by it and the classifier scans it (MAG-4218).
func TestHTTPStatusError_ErrorIsCanonicalPerStatus(t *testing.T) {
	cases := []struct {
		code   int
		status string
		want   string
	}{
		{503, "503 Service Unavailable", "HTTP 503 Service Unavailable"},
		{503, "503 Service Temporarily Unavailable", "HTTP 503 Service Unavailable"},
		{503, "503 Backend fetch failed", "HTTP 503 Service Unavailable"},
		{503, "503", "HTTP 503 Service Unavailable"},
		{503, "", "HTTP 503 Service Unavailable"},
		{504, "504 Gateway Time-out", "HTTP 504 Gateway Timeout"},
		{429, "429 Too Many Requests", "HTTP 429 Too Many Requests"},
		// A status Go has no text for (Cloudflare's 52x) is the code alone, no trailing space.
		{520, "520 ", "HTTP 520"},
		{522, "522 Origin Connection Time-out", "HTTP 522"},
	}
	for _, tc := range cases {
		err := &HTTPStatusError{StatusCode: tc.code, Status: tc.status}
		require.Equal(t, tc.want, err.Error(), "status line %q", tc.status)
		require.Equal(t, tc.status, err.Status, "the upstream's own line is kept for the client")
	}
}
