package common

import (
	"context"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

// GRPCStatusFrame records whether a gRPC call's status came from the server: whether the client
// read the frame that ends a reply and carries grpc-status (the trailers, or a trailers-only
// reply).
//
// grpc-go hands the caller a status either way. When the connection breaks during the call (a
// reset, a closed socket, a stream reset), it writes one of its own, UNAVAILABLE for a broken
// connection, the code a node also sends when it is overloaded. The code and the call options
// cannot tell the two apart: an empty trailer block is not kept, so grpc.Trailer reads the same
// for a node's bare status as for no status at all. The stats handler can: grpc-go reports an
// InTrailer event for every status frame it reads, and for nothing else (MAG-5130).
type GRPCStatusFrame struct {
	received atomic.Bool
}

// Received reports whether the server's status frame arrived. False on a nil recorder.
func (f *GRPCStatusFrame) Received() bool {
	return f != nil && f.received.Load()
}

type grpcStatusFrameKey struct{}

// WithGRPCStatusFrame returns ctx carrying a new recorder for one call. Calls on a connection
// dialed with GRPCStatusFrameDialOption fill it in.
func WithGRPCStatusFrame(ctx context.Context) (context.Context, *GRPCStatusFrame) {
	frame := &GRPCStatusFrame{}
	return context.WithValue(ctx, grpcStatusFrameKey{}, frame), frame
}

// GRPCStatusFrameDialOption installs the handler that fills a call's GRPCStatusFrame. A call
// whose context carries no recorder costs one context lookup per stats event.
func GRPCStatusFrameDialOption() grpc.DialOption {
	return grpc.WithStatsHandler(grpcStatusFrameHandler{})
}

type grpcStatusFrameHandler struct{}

func (grpcStatusFrameHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (grpcStatusFrameHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	if _, ok := s.(*stats.InTrailer); !ok {
		return
	}
	if frame, ok := ctx.Value(grpcStatusFrameKey{}).(*GRPCStatusFrame); ok {
		frame.received.Store(true)
	}
}

func (grpcStatusFrameHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (grpcStatusFrameHandler) HandleConn(context.Context, stats.ConnStats) {}
