package grpcproxy

import (
	"errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionalpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/grpc/status"
	"testing"
)

type reflectionSendingStream struct {
	grpc.ServerStream
	err  error
	sent int
}

func (s *reflectionSendingStream) SendMsg(interface{}) error { s.sent++; return s.err }
func TestReflectionTelemetryPreservesTransportVerdictAndCompletesOnce(t *testing.T) {
	for _, tc := range []struct {
		name         string
		message      interface{}
		transportErr error
		wantCode     codes.Code
	}{
		{name: "v1 success", message: &reflectionv1.ServerReflectionResponse{}, wantCode: codes.OK},
		{name: "v1 application error", message: &reflectionv1.ServerReflectionResponse{MessageResponse: &reflectionv1.ServerReflectionResponse_ErrorResponse{ErrorResponse: &reflectionv1.ErrorResponse{ErrorCode: int32(codes.NotFound), ErrorMessage: "missing"}}}, wantCode: codes.NotFound},
		{name: "alpha application error", message: &reflectionalpha.ServerReflectionResponse{MessageResponse: &reflectionalpha.ServerReflectionResponse_ErrorResponse{ErrorResponse: &reflectionalpha.ErrorResponse{ErrorCode: int32(codes.NotFound), ErrorMessage: "missing"}}}, wantCode: codes.NotFound},
		{name: "transport failure", message: &reflectionv1.ServerReflectionResponse{}, transportErr: status.Error(codes.Unavailable, "disconnected"), wantCode: codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			underlying := &reflectionSendingStream{err: tc.transportErr}
			calls := 0
			var observed error
			stream := &observedReflectionStream{ServerStream: underlying, finish: func(err error) { calls++; observed = err }}
			require.Equal(t, tc.transportErr, stream.SendMsg(tc.message))
			require.Equal(t, tc.wantCode, status.Code(observed))
			require.Equal(t, 1, calls)
			_ = stream.SendMsg(tc.message)
			stream.complete(errors.New("later stream failure"))
			require.Equal(t, 1, calls)
			require.Equal(t, 2, underlying.sent, "observing an application error must not stop reflection")
		})
	}
}
