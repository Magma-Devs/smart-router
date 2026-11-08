package lavasession

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// A corroborated gRPC rate limit must come out of handleGRPCError typed — errors.Is on
// the sentinel and RetryAfterFrom reading the metadata delay — exactly like the HTTP
// transports.
func TestHandleGRPCError_CorroboratedRateLimitIsTyped(t *testing.T) {
	g := &GRPCDirectRPCConnection{}
	md := metadata.MD{"retry-after": []string{"30"}}

	resp, err := g.handleGRPCError(context.Background(), status.Error(codes.ResourceExhausted, "rate limit exceeded"), md, nil, true)
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

	_, err := g.handleGRPCError(context.Background(), status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5000000 vs. 4194304)"), nil, nil, true)
	require.Error(t, err)
	require.NotErrorIs(t, err, common.StatusCodeError429)
	// The original cause stays reachable when no rate limit re-scopes Unwrap.
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}

// A vendor's HTTP edge leaves the 429 text inside codes.Unavailable — the one shape with
// no status code and no metadata.
func TestHandleGRPCError_UnavailableWith429TextIsTyped(t *testing.T) {
	g := &GRPCDirectRPCConnection{}

	_, err := g.handleGRPCError(context.Background(), status.Error(codes.Unavailable, "unexpected HTTP status code received from server: 429 (Too Many Requests)"), nil, nil, true)
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429)
	_, ok := common.RetryAfterFrom(err)
	require.False(t, ok, "no delay was offered")
}

// rateLimitingHealthServer refuses every health check with RESOURCE_EXHAUSTED, sending header and
// trailer as the reply's header and trailer metadata: a node with a schema, for the JSON path.
type rateLimitingHealthServer struct {
	healthpb.UnimplementedHealthServer
	header, trailer metadata.MD
}

func (s rateLimitingHealthServer) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if err := grpc.SetHeader(ctx, s.header); err != nil {
		return nil, err
	}
	if err := grpc.SetTrailer(ctx, s.trailer); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.ResourceExhausted, "RESOURCE_EXHAUSTED")
}

// A node's wait is read whichever block of its reply carries it, on both of SendRequest's paths. A
// reply that is only a status has no header block at all (trailers-only), and grpc-go's own retry
// reads grpc-retry-pushback-ms from the trailer; a wait sent there was never seen, so the refusal
// was not even a rate limit (MAG-4915). The reply's metadata is still the header block alone: the
// wait is the router's to act on, not the client's to read.
func TestSendRequest_RateLimitWaitIsReadFromHeaderOrTrailer(t *testing.T) {
	const pushbackKey = "grpc-retry-pushback-ms"
	asked := time.Minute // the wait the MAG-4915 reproduction asked for
	pushback := metadata.Pairs(pushbackKey, strconv.FormatInt(asked.Milliseconds(), 10))
	blocks := []struct {
		name            string
		header, trailer metadata.MD
	}{
		{"header", pushback, nil},
		{"trailer", nil, pushback},
	}
	requireRateLimited := func(t *testing.T, resp *DirectRPCResponse, err error, header metadata.MD) {
		t.Helper()
		require.ErrorIs(t, err, common.StatusCodeError429, "RESOURCE_EXHAUSTED with a wait is a rate limit")
		wait, ok := common.RetryAfterFrom(err)
		require.True(t, ok, "the wait reaches the hold-off")
		require.Equal(t, asked, wait)
		require.NotNil(t, resp)
		require.Equal(t, header[pushbackKey], resp.Metadata[pushbackKey], "the reply carries the header block only")
	}
	for _, block := range blocks {
		t.Run(block.name+"/binary body", func(t *testing.T) {
			lis := bufconn.Listen(1024 * 1024)
			srv := grpc.NewServer(grpc.ForceServerCodec(rawProtoCodec{}), grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
				var body []byte
				if err := stream.RecvMsg(&body); err != nil {
					return err
				}
				if err := stream.SetHeader(block.header); err != nil {
					return err
				}
				stream.SetTrailer(block.trailer)
				return status.Error(codes.ResourceExhausted, "RESOURCE_EXHAUSTED")
			}))
			go func() { _ = srv.Serve(lis) }()
			t.Cleanup(srv.Stop)
			g := directConnOver(t, lis)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := g.SendRequest(ctx, []byte{0x08, 0x01}, map[string]string{GRPCMethodHeader: "test.v1.Node/Call"})
			requireRateLimited(t, resp, err, block.header)
		})
		t.Run(block.name+"/JSON body", func(t *testing.T) {
			lis := bufconn.Listen(1024 * 1024)
			srv := grpc.NewServer()
			healthpb.RegisterHealthServer(srv, rateLimitingHealthServer{header: block.header, trailer: block.trailer})
			reflection.Register(srv)
			go func() { _ = srv.Serve(lis) }()
			t.Cleanup(srv.Stop)
			g := directConnOver(t, lis)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := g.SendRequest(ctx, []byte(`{"service":"x"}`), map[string]string{GRPCMethodHeader: "grpc.health.v1.Health/Check"})
			requireRateLimited(t, resp, err, block.header)
		})
	}
}
