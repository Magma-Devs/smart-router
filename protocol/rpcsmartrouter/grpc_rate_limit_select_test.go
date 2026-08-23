package rpcsmartrouter

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// gRPC streaming subscriptions consult the same hold-off as ws: held-off endpoints are
// skipped while something ready remains, and the full tier stays when nothing is.
func TestGRPCSelectFromTier_SkipsHeldOffEndpoints(t *testing.T) {
	reg := withFreshRelayHoldoff(t)
	dgm := &DirectGRPCSubscriptionManager{} // no optimizer: first-non-ignored selection
	tier := []*common.NodeUrl{{Url: "grpc-a.example:443"}, {Url: "grpc-b.example:443"}}

	reg.RecordRateLimit("grpc-a.example:443", "grpc-a.example:443", time.Minute)
	ep, err := dgm.selectFromTier(context.Background(), tier, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "grpc-b.example:443", ep.Url)

	reg.RecordRateLimit("grpc-b.example:443", "grpc-b.example:443", time.Minute)
	ep, err = dgm.selectFromTier(context.Background(), tier, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, ep, "with everything held off the tier still serves")
}
