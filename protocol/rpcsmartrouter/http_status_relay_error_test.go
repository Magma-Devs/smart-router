package rpcsmartrouter

import (
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
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
		require.EqualError(t, err, "HTTP 503")
	})
}
