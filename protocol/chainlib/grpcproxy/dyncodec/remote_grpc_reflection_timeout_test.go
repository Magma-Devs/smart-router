package dyncodec

import (
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy/testproto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
)

// hangingReflectionServer accepts the reflection stream and then never answers.
//
// That is the shape of the upstream this guards against: a gateway that serves
// gRPC methods normally but does not implement reflection behind it does NOT
// refuse the stream — it accepts it and stays silent, so the client waits on a
// reply that never comes.
type hangingReflectionServer struct {
	grpc_reflection_v1alpha.UnimplementedServerReflectionServer
}

func (s *hangingReflectionServer) ServerReflectionInfo(stream grpc_reflection_v1alpha.ServerReflection_ServerReflectionInfoServer) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

func hangingReflectionConn(t *testing.T) *grpc.ClientConn {
	t.Helper()
	grpcSrv := grpc.NewServer()
	grpc_reflection_v1alpha.RegisterServerReflectionServer(grpcSrv, &hangingReflectionServer{})
	return testproto.InMemoryClientConn(t, grpcSrv)
}

// TestReflectionFailsAtItsDeadlineRatherThanHanging is the regression for
// MAG-3371: both reflection calls used context.Background(), so a silent server
// blocked until some outer context tore the connection down. On the startup
// verification path that was BootValidateTimeout, and the failure then surfaced
// as a closed connection pool instead of as reflection never answering.
//
// The elapsed-time bound is the assertion that matters — an unbounded call fails
// it by never returning.
func TestReflectionFailsAtItsDeadlineRatherThanHanging(t *testing.T) {
	const timeout = 150 * time.Millisecond

	for _, tc := range []struct {
		name string
		call func(r *GRPCReflectionProtoFileRegistry) error
	}{
		{
			name: "ProtoFileByPath",
			call: func(r *GRPCReflectionProtoFileRegistry) error {
				_, err := r.ProtoFileByPath("google/protobuf/descriptor.proto")
				return err
			},
		},
		{
			name: "ProtoFileContainingSymbol",
			call: func(r *GRPCReflectionProtoFileRegistry) error {
				_, err := r.ProtoFileContainingSymbol("google.protobuf.DescriptorProto")
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote := NewGRPCReflectionProtoFileRegistryFromConn(hangingReflectionConn(t), timeout)
			defer remote.Close()

			start := time.Now()
			err := tc.call(remote)
			elapsed := time.Since(start)

			require.Error(t, err, "a server that never answers must produce an error")
			require.GreaterOrEqual(t, elapsed, timeout,
				"must wait for its own deadline rather than failing early")
			require.Less(t, elapsed, 5*time.Second,
				"must fail at the reflection deadline, not hang until an outer context cancels it")
		})
	}
}

// TestZeroReflectionTimeoutTakesTheDefault pins the invariant that an unset
// timeout means the default, never "no deadline" — the state this file was in
// before MAG-3371. Asserted on the field because exercising it for real would
// cost the full default in test time.
func TestZeroReflectionTimeoutTakesTheDefault(t *testing.T) {
	conn := hangingReflectionConn(t)

	for _, given := range []time.Duration{0, -1 * time.Second} {
		remote := NewGRPCReflectionProtoFileRegistryFromConn(conn, given)
		require.Equal(t, defaultReflectionTimeout, remote.timeout,
			"a non-positive timeout must fall back to the default, not disable the deadline")
	}

	remote := NewGRPCReflectionProtoFileRegistryFromConn(conn, 2*time.Second)
	require.Equal(t, 2*time.Second, remote.timeout, "a configured timeout must be honoured")
}
