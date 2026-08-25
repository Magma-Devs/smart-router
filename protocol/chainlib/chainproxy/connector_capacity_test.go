package chainproxy

import (
	"context"
	"sync"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Pool capacity used to live in the package-global NumberOfParallelConnections,
// which every constructor wrote and GRPCConnector.ReturnRpc read back. Two
// connectors existing at once was enough to break that in both directions: the
// second constructor silently redefined the first's idle-trim threshold, and the
// write raced the read outright. MAG-2538's recovery test is what surfaced it —
// validateProvider tears its verification connector down while the next connector
// is being built — but nothing about the hazard is specific to that path.

// TestGRPCConnectorCapacityIsPerInstance pins the ownership half: a connector's
// trim threshold is the nConns IT was built with, and building another connector
// does not move it.
func TestGRPCConnectorCapacityIsPerInstance(t *testing.T) {
	server := createGRPCServer(t)
	defer server.Stop()

	ctx := context.Background()
	url := common.NodeUrl{Url: listenerAddressGrpc}

	small, err := NewGRPCConnector(ctx, ctx, 1, url)
	require.NoError(t, err)
	defer small.Close()

	// Built second and larger: under the global this reassigned the threshold that
	// `small` had already been constructed with.
	big, err := NewGRPCConnector(ctx, ctx, numberOfClients, url)
	require.NoError(t, err)
	defer big.Close()

	require.Equal(t, uint(1), small.capacity, "the first connector must keep its own capacity")
	require.Equal(t, uint(numberOfClients), big.capacity)
	// Nothing here asserts NumberOfParallelConnections survived construction: it is a
	// const, so "no constructor rewrites it" is enforced by the compiler for every
	// caller rather than by one test for the two constructors it happens to exercise.
}

// TestGRPCConnectorReturnRpcTrimsAgainstOwnCapacity is the same ownership property
// asserted through behaviour rather than the field: identical pool state, two
// capacities, opposite decisions about whether a returned client is worth keeping.
func TestGRPCConnectorReturnRpcTrimsAgainstOwnCapacity(t *testing.T) {
	ctx := context.Background()

	for _, tt := range []struct {
		name     string
		capacity uint
		wantFree int
	}{
		// freeClients already holds 2 and the borrow count drops to 0 on return, so
		// the surplus test is `2 > 0 + capacity`.
		{name: "capacity below the surplus trims", capacity: 1, wantFree: 2},
		{name: "capacity above the surplus keeps", capacity: 5, wantFree: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// A real client to hand back, since the trim path closes it. Dialled
			// standalone rather than borrowed from a connector: a borrow this test
			// never returns would leave that connector's Close waiting on it forever.
			returned, err := grpc.DialContext(ctx, listenerAddressGrpc,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = returned.Close() })

			// Hand-built so pool state is identical across the two cases and capacity
			// is the only variable. Only len(freeClients) feeds the decision, so the
			// placeholders are never dereferenced.
			connector := &GRPCConnector{
				capacity:    tt.capacity,
				usedClients: 1,
				freeClients: []*grpc.ClientConn{nil, nil},
			}
			connector.ReturnRpc(returned)

			require.Len(t, connector.freeClients, tt.wantFree)
			require.Equal(t, 0, connector.numberOfUsedClients())
		})
	}
}

// TestConnectorConstructionDoesNotRaceReturnRpc pins the race half. It is only
// meaningful under -race: before capacity moved onto the connector, constructing
// any connector wrote the global that ReturnRpc was reading.
//
// Both constructors are exercised. NewConnector is HTTP and ignores nConns for its
// own purposes, but it wrote the same global "for the gRPC connector", so building
// an HTTP connector raced a gRPC connector's teardown just as readily.
func TestConnectorConstructionDoesNotRaceReturnRpc(t *testing.T) {
	server := createGRPCServer(t)
	defer server.Stop()

	ctx := context.Background()
	url := common.NodeUrl{Url: listenerAddressGrpc}

	serving, err := NewGRPCConnector(ctx, ctx, numberOfClients, url)
	require.NoError(t, err)
	defer serving.Close()

	const rounds = 25
	var wg sync.WaitGroup

	// Borrowers: keep ReturnRpc reading the trim threshold throughout.
	for range 4 {
		wg.Go(func() {
			for range rounds {
				rpc, err := serving.GetRpc(ctx, true)
				if err != nil {
					return
				}
				serving.ReturnRpc(rpc)
			}
		})
	}

	// Builders: construct and tear down connectors alongside those returns.
	wg.Go(func() {
		for i := range rounds {
			transient, err := NewGRPCConnector(ctx, ctx, uint(1+i%3), url)
			if err != nil {
				continue
			}
			transient.Close()
		}
	})

	wg.Go(func() {
		for range rounds {
			httpConnector, err := NewConnector(ctx, 7, common.NodeUrl{Url: listenerAddressTcp})
			if err != nil {
				continue
			}
			httpConnector.Close()
		}
	})

	wg.Wait()

	require.Equal(t, uint(numberOfClients), serving.capacity,
		"the serving connector's capacity must survive every connector built alongside it")
}
