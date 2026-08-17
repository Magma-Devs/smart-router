package chainlib

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// startReflectionGRPCServer brings up a bare gRPC server that serves server
// reflection on a random loopback port. Reflection is the only thing this test
// needs: setupForProvider builds its registry from the reflection connection.
func startReflectionGRPCServer(t *testing.T) string {
	t.Helper()
	srv := grpc.NewServer()
	reflection.Register(srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func TestGetChainRouter_CloneProtectsLiveGrpcParser(t *testing.T) {
	addr := startReflectionGRPCServer(t)
	endpoint := &lavasession.RPCProviderEndpoint{
		ChainID:      "TEST",
		ApiInterface: "grpc",
		NodeUrls:     []common.NodeUrl{{Url: addr}},
	}

	// Part 1 — the hazard is real. Passing the LIVE parser mutates its registry.
	direct, err := NewGrpcChainParser()
	require.NoError(t, err)
	direct.SetSpec(spectypes.Spec{})
	require.Nil(t, direct.registry, "fresh parser starts with no registry")

	ctx1, cancel1 := context.WithTimeout(context.Background(), 15*time.Second)
	_, err = GetChainRouter(ctx1, 1, endpoint, direct)
	require.NoError(t, err)
	require.NotNil(t, direct.registry,
		"precondition: handing the live parser to GetChainRouter rebinds its registry")
	cancel1()

	// Part 2 — the clone absorbs the mutation, live parser untouched.
	live, err := NewGrpcChainParser()
	require.NoError(t, err)
	live.SetSpec(spectypes.Spec{})
	require.Nil(t, live.registry)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()
	_, err = GetChainRouter(ctx2, 1, endpoint, CloneChainParserForValidation(live))
	require.NoError(t, err)
	require.Nil(t, live.registry,
		"MAG-2538: verification must not rebind the live parser's registry")
}
