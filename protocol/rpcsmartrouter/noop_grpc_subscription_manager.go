package rpcsmartrouter

import (
	"context"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"google.golang.org/grpc"
)

// noopGRPCSubscriptionManager is the community/no-license stub that satisfies
// GRPCSubscriptionManager. It mirrors NoOpWSSubscriptionManager: never connects
// upstream, returns the same "no gRPC subscription manager configured" error
// for reflection that the server's nil-check used to return, and reports
// "not streaming" for IsStreamingMethod so that any unguarded call site does
// not spuriously reject a regular gRPC unary call.
//
// Community edition rejects gRPC at the API-interface gate (§3.3.5), so in
// practice this type is rarely reached at runtime — it exists to keep the
// interface always implementable rather than as a runtime fallback.
type noopGRPCSubscriptionManager struct {
	chainID      string
	apiInterface string
}

func newNoopGRPCSubscriptionManager(chainID, apiInterface string) *noopGRPCSubscriptionManager {
	return &noopGRPCSubscriptionManager{
		chainID:      chainID,
		apiInterface: apiInterface,
	}
}

func (n *noopGRPCSubscriptionManager) GetReflectionConnection(ctx context.Context) (*grpc.ClientConn, func(), error) {
	return nil, nil, fmt.Errorf("gRPC reflection not available: no gRPC subscription manager configured")
}

func (n *noopGRPCSubscriptionManager) IsStreamingMethod(ctx context.Context, methodPath string) (bool, *desc.MethodDescriptor, error) {
	return false, nil, nil
}

var _ GRPCSubscriptionManager = (*noopGRPCSubscriptionManager)(nil)
