package lavasession

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// A corroborated gRPC rate limit must come out of handleGRPCError typed — errors.Is on
// the sentinel and RetryAfterFrom reading the metadata delay — exactly like the HTTP
// transports.
func TestHandleGRPCError_CorroboratedRateLimitIsTyped(t *testing.T) {
	g := &GRPCDirectRPCConnection{}
	md := metadata.MD{"retry-after": []string{"30"}}

	resp, err := g.handleGRPCError(context.Background(), status.Error(codes.ResourceExhausted, "rate limit exceeded"), md)
	require.NotNil(t, resp, "the error response is still returned for the caller to inspect")
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429)
	d, ok := common.RetryAfterFrom(err)
	require.True(t, ok)
	require.Equal(t, 30*time.Second, d)
}

// RESOURCE_EXHAUSTED without corroboration is how grpc-go reports an oversized message —
// it must not read as a rate limit.
func TestHandleGRPCError_ResourceExhaustedAloneIsNotRateLimit(t *testing.T) {
	g := &GRPCDirectRPCConnection{}

	_, err := g.handleGRPCError(context.Background(), status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5000000 vs. 4194304)"), nil)
	require.Error(t, err)
	require.NotErrorIs(t, err, common.StatusCodeError429)
	// The original cause stays reachable when no rate limit re-scopes Unwrap.
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}

// A vendor's HTTP edge leaves the 429 text inside codes.Unavailable — the one shape with
// no status code and no metadata.
func TestHandleGRPCError_UnavailableWith429TextIsTyped(t *testing.T) {
	g := &GRPCDirectRPCConnection{}

	_, err := g.handleGRPCError(context.Background(), status.Error(codes.Unavailable, "unexpected HTTP status code received from server: 429 (Too Many Requests)"), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429)
	_, ok := common.RetryAfterFrom(err)
	require.False(t, ok, "no delay was offered")
}
