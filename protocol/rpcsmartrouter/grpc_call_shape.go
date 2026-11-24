package rpcsmartrouter

import (
	"strings"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var _ chainlib.GRPCCallShapes = (*RPCSmartRouterServer)(nil)

// GRPCCallShape implements chainlib.GRPCCallShapes from the method definitions the router already
// holds: each gRPC endpoint's reflection snapshot, then the methods its connection has looked up.
// It asks no node and waits for nothing, so a call whose method it cannot find goes ahead and is
// judged by what it does (MAG-5133).
func (rpcss *RPCSmartRouterServer) GRPCCallShape(method string) (clientStreams, serverStreams, known bool) {
	if rpcss.sessionManager == nil {
		return false, false, false
	}
	name := protoreflect.FullName(strings.ReplaceAll(method, "/", "."))
	for _, endpoint := range rpcss.sessionManager.GetAllDirectRPCEndpoints() {
		if snapshotter, ok := endpoint.DirectConnection.(routersession.GRPCReflectionSnapshotter); ok {
			if snapshot := snapshotter.PeekReflectionSnapshot(); snapshot != nil {
				if descriptor, err := snapshot.FindDescriptorByName(name); err == nil {
					if methodDesc, ok := descriptor.(protoreflect.MethodDescriptor); ok {
						return methodDesc.IsStreamingClient(), methodDesc.IsStreamingServer(), true
					}
				}
			}
		}
		if provider, ok := endpoint.DirectConnection.(routersession.GRPCDescriptorProvider); ok {
			if methodDesc := provider.GetCachedMethodDescriptor(method); methodDesc != nil {
				return methodDesc.IsClientStreaming(), methodDesc.IsServerStreaming(), true
			}
		}
	}
	return false, false, false
}
