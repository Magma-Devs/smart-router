package rpcclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// A 429 on the WS upgrade must surface as the typed rate-limit error with the upstream's
// Retry-After attached — the handshake used to keep only the status string and drop the
// headers, leaving the rejection invisible to every consumer.
func TestDialWebsocket_429UpgradeIsTypedRateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "120")
		http.Error(w, "too many requests", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := DialWebsocket(ctx, wsURL, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429)
	d, ok := common.RetryAfterFrom(err)
	require.True(t, ok, "Retry-After must survive the handshake error")
	require.Equal(t, 2*time.Minute, d)
}

// A non-429 rejection keeps today's shape: the dial error, status in the message, no
// rate-limit typing.
func TestDialWebsocket_ServerErrorUpgradeIsNotRateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := DialWebsocket(ctx, wsURL, nil)
	require.Error(t, err)
	require.NotErrorIs(t, err, common.StatusCodeError429)
	_, ok := common.RetryAfterFrom(err)
	require.False(t, ok)
}
