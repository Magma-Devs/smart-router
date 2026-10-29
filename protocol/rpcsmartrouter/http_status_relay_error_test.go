package rpcsmartrouter

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// relayInnerDirect fails a >=500/429 relay with httpStatusRelayError. A 429 must carry
// the typed sentinel and the upstream's Retry-After through the error, because the
// discarded result is the only other place they existed.
func TestHTTPStatusRelayError(t *testing.T) {
	t.Run("429 with Retry-After header", func(t *testing.T) {
		reply := &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{
			{Name: "Content-Type", Value: "application/json"},
			{Name: "Retry-After", Value: "45"},
		}}
		err := httpStatusRelayError(429, reply)
		require.ErrorIs(t, err, common.StatusCodeError429)
		d, ok := common.RetryAfterFrom(err)
		require.True(t, ok)
		require.Equal(t, 45*time.Second, d)
	})

	t.Run("429 without header stays typed, no duration", func(t *testing.T) {
		err := httpStatusRelayError(429, &pairingtypes.RelayReply{})
		require.ErrorIs(t, err, common.StatusCodeError429)
		_, ok := common.RetryAfterFrom(err)
		require.False(t, ok)
	})

	t.Run("429 with nil reply does not panic", func(t *testing.T) {
		err := httpStatusRelayError(429, nil)
		require.ErrorIs(t, err, common.StatusCodeError429)
	})

	t.Run("5xx is not a rate limit", func(t *testing.T) {
		err := httpStatusRelayError(503, &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{
			{Name: "Retry-After", Value: "45"},
		}})
		require.NotErrorIs(t, err, common.StatusCodeError429)
		_, ok := common.RetryAfterFrom(err)
		require.False(t, ok)
		require.EqualError(t, err, "HTTP 503 Service Unavailable", "one canonical text per status, as on the JSON-RPC sender")
	})

	t.Run("carries the upstream's answer", func(t *testing.T) {
		err := httpStatusRelayError(503, &pairingtypes.RelayReply{Data: []byte("<html/>"), Metadata: []pairingtypes.Metadata{
			{Name: "Content-Type", Value: "text/html"},
		}})
		var httpErr *lavasession.HTTPStatusError
		require.ErrorAs(t, err, &httpErr)
		require.Equal(t, 503, httpErr.StatusCode)
		require.Equal(t, []byte("<html/>"), httpErr.Body)
		require.Equal(t, "text/html", httpErr.ContentType)
	})
}

// The relay path used to fail with a plain "HTTP <code>" error; it now carries the upstream's
// answer. The registry must read both the same, on every transport the results manager or the
// direct path classifies with (MAG-4481).
func TestHTTPStatusRelayError_ClassifiesAsThePlainStatusDid(t *testing.T) {
	for code := 400; code < 600; code++ {
		var plain error = fmt.Errorf("HTTP %d", code)
		if code == 429 {
			plain = fmt.Errorf("HTTP %d: %w", code, common.StatusCodeError429)
		}
		carried := httpStatusRelayError(code, &pairingtypes.RelayReply{Data: []byte(`{"code":429,"message":"503 vendor phrase"}`)})
		for _, transport := range []common.TransportType{common.TransportGRPC, common.TransportREST, common.TransportJsonRPC} {
			before := common.ClassifyError(common.DetectConnectionError(plain), common.ChainFamilyUnknown, transport, 0, plain.Error())
			after := common.ClassifyError(common.DetectConnectionError(carried), common.ChainFamilyUnknown, transport, 0, carried.Error())
			require.Equal(t, before.Name, after.Name, "status %d on %v", code, transport)
		}
		require.Equal(t, errors.Is(plain, common.StatusCodeError429), errors.Is(carried, common.StatusCodeError429), "status %d", code)
	}
}
