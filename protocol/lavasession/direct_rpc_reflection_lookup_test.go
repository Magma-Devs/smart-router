package lavasession

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fullstorydev/grpcurl"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"
)

// startAnyPayloadNode serves review419.Echo, whose method takes and returns
// google.protobuf.Any, and describes review419/payload.proto too, which the
// service's file does not import: a node describes every file it links in.
func startAnyPayloadNode(t *testing.T) *slowReflectionServer {
	t.Helper()
	payload := messageFile(t, "review419/payload.proto", "review419", "Payload", "value", "")
	echo := linkFile(t, &descriptorpb.FileDescriptorProto{
		Name: proto.String("review419/echo.proto"), Package: proto.String("review419"), Syntax: proto.String("proto3"),
		Dependency: []string{anypb.File_google_protobuf_any_proto.Path()},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: proto.String("Echo"),
			Method: []*descriptorpb.MethodDescriptorProto{{
				Name: proto.String("Call"), InputType: proto.String(".google.protobuf.Any"), OutputType: proto.String(".google.protobuf.Any"),
			}},
		}},
	}, anypb.File_google_protobuf_any_proto)
	described := new(protoregistry.Files)
	for _, fd := range []protoreflect.FileDescriptor{anypb.File_google_protobuf_any_proto, payload, echo} {
		require.NoError(t, described.RegisterFile(fd))
	}

	s := &slowReflectionServer{}
	server := grpc.NewServer(grpc.StreamInterceptor(slowReflectionInterceptor(&s.reflectionStream, 0)))
	server.RegisterService(&grpc.ServiceDesc{ServiceName: "review419.Echo", HandlerType: (*any)(nil)}, struct{}{})
	reflectionv1.RegisterServerReflectionServer(server,
		reflection.NewServerV1(reflection.ServerOptions{Services: server, DescriptorResolver: described}))
	s.conn = serveOverBufconn(t, server)
	return s
}

// encodeAnyThroughRouter parses body as review419.Echo/Call's request with grpcurl,
// resolving every type through the router's reflection alone, as grpcurl does
// without -proto, and returns the Any it encoded.
func encodeAnyThroughRouter(t *testing.T, router *grpc.ClientConn, body string) (typeURL string, value []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := grpcreflect.NewClientAuto(ctx, router)
	defer client.Reset()
	source := grpcurl.DescriptorSourceFromServer(ctx, client)

	symbol, err := source.FindSymbol("review419.Echo")
	require.NoError(t, err)
	service, ok := symbol.(*desc.ServiceDescriptor)
	require.True(t, ok)
	method := service.FindMethodByName("Call")
	require.NotNil(t, method)
	request := dynamic.NewMessage(method.GetInputType())
	parser, _, err := grpcurl.RequestParserAndFormatter(grpcurl.FormatJSON, source, strings.NewReader(body), grpcurl.FormatOptions{})
	require.NoError(t, err)
	require.NoError(t, parser.Next(request))
	typeURL, ok = request.GetFieldByName("type_url").(string)
	require.True(t, ok)
	value, ok = request.GetFieldByName("value").([]byte)
	require.True(t, ok)
	return typeURL, value
}

// Review of #419: an Any names its payload type only at run time, so no service's
// files import it and a snapshot of those files cannot hold it. grpcurl resolves
// the type URL through reflection before it sends anything; the router looks the
// name up on the snapshot's node and keeps the file, so the request encodes.
func TestReflectionSnapshot_AnAnyPayloadNoServiceImportsResolvesThroughTheRouter(t *testing.T) {
	node := startAnyPayloadNode(t)
	g := newGRPCConnOverServer(t, node)
	router := serveSnapshotReflection(t, g.AwaitReflectionSnapshot)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	snapshot, err := g.AwaitReflectionSnapshot(ctx)
	require.NoError(t, err)
	require.True(t, snapshot.Complete)
	_, err = snapshot.FindDescriptorByName("review419.Payload")
	require.Error(t, err, "no listed service imports the payload's file")

	const body = `{"@type":"type.googleapis.com/review419.Payload","value":"hello"}`
	typeURL, value := encodeAnyThroughRouter(t, router, body)
	require.Equal(t, "type.googleapis.com/review419.Payload", typeURL)
	require.Equal(t, []byte{0x0a, 0x05, 'h', 'e', 'l', 'l', 'o'}, value, "Payload{value: \"hello\"}")

	streams := node.reflectionStream.Load()
	encodeAnyThroughRouter(t, router, body)
	require.Equal(t, streams, node.reflectionStream.Load(), "the file looked up is kept, so a second stream costs the node nothing")
}

// A lookup runs in a later session than the snapshot, and the node may have changed
// in between. What it returns is kept only when every file it needs agrees with
// the builds the snapshot holds; a name not added is not asked again within
// reflectionSnapshotRetry.
func TestReflectionSnapshot_ALookupThatDisagreesWithTheSnapshotAddsNothing(t *testing.T) {
	previous := reflectionSnapshotRetry
	reflectionSnapshotRetry = 100 * time.Millisecond
	t.Cleanup(func() { reflectionSnapshotRetry = previous })

	heldPayload := payloadBuild(t, "old_value")
	snapshot, err := buildReflectionSnapshot(fakeDescriptorSource{
		listed: []string{"x.S1"},
		services: map[string]*desc.ServiceDescriptor{
			"x.S1": wrappedService(t, serviceFileImporting(t, "S1", heldPayload, ".x.P"), "x.S1"),
		},
	})
	require.NoError(t, err)
	var lookups atomic.Int32
	answer := wrapperOver(t, payloadBuild(t, "new_value"))
	snapshot.lookup = func(context.Context, string) (protoreflect.FileDescriptor, error) {
		lookups.Add(1)
		return answer, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = snapshot.LookupDescriptor(ctx, "x.W")
	require.Error(t, err)
	_, err = snapshot.FindFileByPath("x/wrapper.proto")
	require.Error(t, err, "nothing of a disagreeing answer is kept")
	_, err = snapshot.LookupDescriptor(ctx, "x.W")
	require.Error(t, err)
	require.Equal(t, int32(1), lookups.Load(), "a miss is not asked again at once")

	answer = wrapperOver(t, heldPayload)
	time.Sleep(120 * time.Millisecond)
	d, err := snapshot.LookupDescriptor(ctx, "x.W")
	require.NoError(t, err)
	require.Equal(t, protoreflect.FullName("x.W"), d.FullName())
	require.Equal(t, int32(2), lookups.Load())

	_, err = snapshot.LookupDescriptor(ctx, "x.W")
	require.NoError(t, err)
	_, err = snapshot.LookupDescriptor(ctx, "grpc.reflection.v1.ServerReflection")
	require.Error(t, err)
	require.Equal(t, int32(2), lookups.Load(), "a name held, and the router's reflection services, are never looked up")
}
