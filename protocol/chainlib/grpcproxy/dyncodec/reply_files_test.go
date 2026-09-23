package dyncodec

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy/testproto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/protobuf/reflect/protoreflect"

	// Registers google/protobuf/api.proto and its imports (type.proto,
	// source_context.proto, any.proto) for the reflection server below to serve.
	_ "google.golang.org/protobuf/types/known/apipb"
)

// TestOneReflectionRequestResolvesAFileWithItsImports is the regression for
// MAG-3828. google.protobuf.Api sits in a file whose imports have imports of their
// own; a remote that kept only the first file of each reply fetched every import
// with a request of its own, four in all, where the one reply already held them.
func TestOneReflectionRequestResolvesAFileWithItsImports(t *testing.T) {
	var streams atomic.Int32
	grpcSrv := grpc.NewServer(grpc.StreamInterceptor(
		func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			streams.Add(1)
			return handler(srv, ss)
		}))
	reflection.Register(grpcSrv)
	conn := testproto.InMemoryClientConn(t, grpcSrv)

	for _, tc := range []struct {
		name   string
		remote func() ProtoFileRegistry
	}{
		{
			name:   "grpc remote",
			remote: func() ProtoFileRegistry { return NewGRPCReflectionProtoFileRegistryFromConn(conn, 0) },
		},
		{
			name: "relayer remote",
			remote: func() ProtoFileRegistry {
				return NewRelayerRemote(func(ctx context.Context, method string, req []byte) ([]byte, metadata.MD, error) {
					var resp []byte
					err := conn.Invoke(ctx, "/"+method, req, &resp, grpc.CustomCodecCallOption{Codec: grpcproxy.RawBytesCodec{}})
					return resp, make(metadata.MD), err
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			streams.Store(0)
			registry := NewRegistry(tc.remote())

			desc, err := registry.FindDescriptorByName("google.protobuf.Api")
			require.NoError(t, err)
			api, ok := desc.(protoreflect.MessageDescriptor)
			require.True(t, ok)

			// Every import resolved to its real file, two levels deep, not to a placeholder.
			sourceContext := api.Fields().ByName("source_context").Message()
			require.False(t, sourceContext.IsPlaceholder(), "source_context.proto must resolve")
			option := api.Fields().ByName("options").Message()
			require.False(t, option.IsPlaceholder(), "type.proto must resolve")
			anyValue := option.Fields().ByName("value").Message()
			require.False(t, anyValue.IsPlaceholder(), "any.proto, imported by type.proto, must resolve")
			require.Equal(t, protoreflect.FullName("google.protobuf.Any"), anyValue.FullName())

			require.Equal(t, int32(1), streams.Load(),
				"one reflection request carries the file and all its imports")
		})
	}
}

// TestReplyWithoutFilesIsAnError covers a reply that names no file: it must be an
// error for the caller, not an index out of range.
func TestReplyWithoutFilesIsAnError(t *testing.T) {
	var replies replyFiles
	_, err := replies.keep(&grpc_reflection_v1alpha.ServerReflectionResponse{
		MessageResponse: &grpc_reflection_v1alpha.ServerReflectionResponse_FileDescriptorResponse{
			FileDescriptorResponse: &grpc_reflection_v1alpha.FileDescriptorResponse{},
		},
	})
	require.Error(t, err)
}
