package grpcproxy

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionalpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/grpc/status"
)

// ReflectionRequestObserver observes a client reflection stream's establishment.
// Upstream reflection snapshots do not traverse this server and are never counted.
// One stream is one request, resolved by its first response (or an earlier failure).
type ReflectionRequestObserver func(context.Context, string) func(error)

type observedReflectionStream struct {
	grpc.ServerStream
	once   sync.Once
	finish func(error)
}

func (s *observedReflectionStream) complete(err error) { s.once.Do(func() { s.finish(err) }) }
func (s *observedReflectionStream) SendMsg(message interface{}) error {
	transportErr := s.ServerStream.SendMsg(message)
	err := transportErr
	if err == nil {
		switch response := message.(type) {
		case *reflectionv1.ServerReflectionResponse:
			if failure := response.GetErrorResponse(); failure != nil {
				err = status.Error(codes.Code(failure.ErrorCode), failure.ErrorMessage)
			}
		case *reflectionalpha.ServerReflectionResponse:
			if failure := response.GetErrorResponse(); failure != nil {
				err = status.Error(codes.Code(failure.ErrorCode), failure.ErrorMessage)
			}
		}
	}
	s.complete(err)
	// Report the original transport verdict to gRPC. Observing an error inside a
	// reflection response must not interrupt this valid bidirectional stream.
	return transportErr
}

func reflectionObserverOption(observer ReflectionRequestObserver) grpc.ServerOption {
	return grpc.StreamInterceptor(func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapped := &observedReflectionStream{ServerStream: stream, finish: observer(stream.Context(), info.FullMethod)}
		err := handler(srv, wrapped)
		terminalErr := err
		if terminalErr == nil || terminalErr == io.EOF {
			terminalErr = status.Error(codes.Canceled, "reflection stream closed before its first response")
		}
		wrapped.complete(terminalErr)
		return err
	})
}
