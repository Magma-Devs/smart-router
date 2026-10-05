package dyncodec

import (
	"sync"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy/testproto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/reflect/protoreflect"

	// Registers api.proto, struct.proto and their imports for the reflection server.
	_ "google.golang.org/protobuf/types/known/apipb"
	_ "google.golang.org/protobuf/types/known/structpb"
)

// TestRegistryResolvesConcurrently is the regression for MAG-3889. Concurrent relays
// parse replies through one chain's registry. Resolving types the registry did not
// hold yet read and wrote its maps at the same time, which Go treats as a fatal
// error that ends the process. Run it with -race.
func TestRegistryResolvesConcurrently(t *testing.T) {
	grpcSrv := grpc.NewServer()
	reflection.Register(grpcSrv)
	conn := testproto.InMemoryClientConn(t, grpcSrv)
	registry := NewRegistry(NewGRPCReflectionProtoFileRegistryFromConn(conn, 0))

	names := []protoreflect.FullName{"google.protobuf.Api", "google.protobuf.Struct", "google.protobuf.Type", "google.protobuf.Method"}
	const callers = 32
	got := make([]protoreflect.MessageType, callers)
	errs := make([]error, callers)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			got[i], errs[i] = registry.FindMessageByName(names[i%len(names)])
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < callers; i++ {
		require.NoError(t, errs[i])
		require.Equal(t, names[i%len(names)], got[i].Descriptor().FullName())
		// Resolutions of one name that raced all get the one type the registry holds.
		require.True(t, got[i%len(names)].Descriptor() == got[i].Descriptor(), "every caller gets the registered %s", names[i%len(names)])
	}
}
