package grpcproxy

import (
	"context"
	"strings"

	"google.golang.org/grpc"
)

// RequestObserver starts a server-owned request before decoding and selection.
// The returned context follows both streaming dispatch and unary fallback.
type RequestObserver func(context.Context, string) (context.Context, func(error))

type observedRequestStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *observedRequestStream) Context() context.Context { return s.ctx }

func observeProxyRequests(handler grpc.StreamHandler, observer RequestObserver) grpc.StreamHandler {
	return func(srv interface{}, stream grpc.ServerStream) (err error) {
		method, _ := grpc.MethodFromServerStream(stream)
		ctx, finish := observer(stream.Context(), strings.TrimPrefix(method, "/"))
		defer func() { finish(err) }()
		return handler(srv, &observedRequestStream{ServerStream: stream, ctx: ctx})
	}
}
