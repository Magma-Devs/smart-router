package routersession

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const healthCheckPath = "grpc.health.v1.Health/Check"

// startHealthNode serves the gRPC health service, with server reflection or without it: the second
// is a node that answers its methods but gives the router no definition of them.
func startHealthNode(t *testing.T, withReflection bool) *bufconn.Listener {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, health.NewServer())
	if withReflection {
		reflection.Register(srv)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis
}

// lookupCounter counts the lookups a connection runs, through its reflection observer.
type lookupCounter struct{ n atomic.Int32 }

func (c *lookupCounter) observe(error) { c.n.Add(1) }

func resolve(t *testing.T, g *GRPCDirectRPCConnection) (*desc.MethodDescriptor, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return g.ResolveMethodDescriptor(ctx, healthCheckPath)
}

// A provider whose node offers no lookup is read with the definition another provider of the chain
// gave (MAG-5134): resolved, cached for the tracker, and used to convert a JSON request it then
// serves.
func TestChainDescriptors_ProviderWithoutLookupUsesAnotherProvidersDefinition(t *testing.T) {
	chain := NewChainDescriptors()
	withLookup := directConnOver(t, startHealthNode(t, true))
	withoutLookup := directConnOver(t, startHealthNode(t, false))
	withLookup.SetChainDescriptors(chain)
	withoutLookup.SetChainDescriptors(chain)

	_, err := resolve(t, withoutLookup)
	require.ErrorIs(t, err, ErrNoMethodDefinition, "no provider has given a definition yet")
	require.Nil(t, withoutLookup.GetCachedMethodDescriptor(healthCheckPath))

	shared, err := resolve(t, withLookup)
	require.NoError(t, err)

	got, err := resolve(t, withoutLookup)
	require.NoError(t, err, "the other provider's definition is used")
	require.Same(t, shared, got)
	require.Same(t, shared, withoutLookup.GetCachedMethodDescriptor(healthCheckPath), "the tracker reads it too")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := withoutLookup.SendRequest(ctx, []byte(`{"service":""}`), map[string]string{GRPCMethodHeader: healthCheckPath})
	require.NoError(t, err, "a JSON request is converted with the shared definition and served by the node")
	require.NotNil(t, resp)
}

// A provider's own definition comes first: with the chain holding one already, the provider still
// looks the method up on its own node and uses what it got. Providers on different builds can
// differ, and the shared definition is only the fallback.
func TestChainDescriptors_OwnDefinitionComesFirst(t *testing.T) {
	chain := NewChainDescriptors()
	first := directConnOver(t, startHealthNode(t, true))
	first.SetChainDescriptors(chain)
	shared, err := resolve(t, first)
	require.NoError(t, err)

	second := directConnOver(t, startHealthNode(t, true))
	second.SetChainDescriptors(chain)
	lookups := &lookupCounter{}
	second.SetReflectionObserver(lookups.observe)

	own, err := resolve(t, second)
	require.NoError(t, err)
	require.EqualValues(t, 1, lookups.n.Load(), "the provider looked the method up on its own node")
	require.NotSame(t, shared, own, "and used its own node's definition")
}

// A failed lookup is not run again for reflectionSnapshotRetry: a node that offers no lookup is
// asked once per window, not on every request (MAG-5134). After the window it is asked again.
func TestResolver_FailedLookupIsHeldOffThenRetried(t *testing.T) {
	g := directConnOver(t, startHealthNode(t, false))
	now := time.Now()
	g.resolver.now = func() time.Time { return now }
	lookups := &lookupCounter{}
	g.SetReflectionObserver(lookups.observe)

	_, err := resolve(t, g)
	require.ErrorIs(t, err, ErrNoMethodDefinition)
	require.EqualValues(t, 1, lookups.n.Load())

	for i := 0; i < 3; i++ {
		_, err = resolve(t, g)
		require.ErrorIs(t, err, ErrNoMethodDefinition)
	}
	require.EqualValues(t, 1, lookups.n.Load(), "held off: no lookup within the window")

	now = now.Add(reflectionSnapshotRetry)
	_, err = resolve(t, g)
	require.ErrorIs(t, err, ErrNoMethodDefinition)
	require.EqualValues(t, 2, lookups.n.Load(), "looked up again once the window passed")
}

// A node that limits reflection and names its wait is asked again only after that wait, when it is
// longer than reflectionSnapshotRetry (MAG-5134). The wait is read from the reflection stream's
// trailer, as a relay's is.
func TestResolver_RateLimitedLookupIsHeldOffForTheWaitItNamed(t *testing.T) {
	const named = 2 * time.Minute
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		stream.SetTrailer(metadata.Pairs("grpc-retry-pushback-ms", strconv.FormatInt(named.Milliseconds(), 10)))
		return status.Error(codes.ResourceExhausted, "reflection quota exhausted")
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	g := directConnOver(t, lis)
	now := time.Now()
	g.resolver.now = func() time.Time { return now }
	lookups := &lookupCounter{}
	g.SetReflectionObserver(lookups.observe)

	_, err := resolve(t, g)
	require.ErrorIs(t, err, ErrNoMethodDefinition)
	require.True(t, errors.Is(err, common.StatusCodeError429), "the refusal is typed as a rate limit")
	require.EqualValues(t, 1, lookups.n.Load())

	now = now.Add(reflectionSnapshotRetry)
	_, _ = resolve(t, g)
	require.EqualValues(t, 1, lookups.n.Load(), "still within the wait the node named")

	now = now.Add(named)
	_, _ = resolve(t, g)
	require.EqualValues(t, 2, lookups.n.Load(), "asked again after the wait")
}

// The failure record starts over once full: a client can name any service, and each failed lookup
// of a name it made up would otherwise stay held forever.
func TestResolver_FailureRecordIsBounded(t *testing.T) {
	r := NewGRPCDescriptorResolver(context.Background(), &common.GrpcConfig{}, "grpc://node", &common.SafeSyncMap[string, *desc.MethodDescriptor]{}, nil)
	failed := errors.New("not found")
	for i := 0; i <= reflectionLookupMisses; i++ {
		r.noteLookupOutcome("svc"+strconv.Itoa(i), failed)
	}
	require.Len(t, r.failed, 1, "the record started over at reflectionLookupMisses")

	r.noteLookupOutcome("svc"+strconv.Itoa(reflectionLookupMisses), nil)
	require.Empty(t, r.failed, "a success clears the service")
}

// A lookup the connection's closing ended is not the node's doing and is not held against it.
func TestResolver_ClosingIsNotHeldAgainstTheService(t *testing.T) {
	r := NewGRPCDescriptorResolver(context.Background(), &common.GrpcConfig{}, "grpc://node", &common.SafeSyncMap[string, *desc.MethodDescriptor]{}, nil)
	r.noteLookupOutcome("svc", ErrGRPCConnectionClosed)
	require.Empty(t, r.failed)
}

// The chain's store is safe under the concurrent publishing and reading its providers do.
func TestChainDescriptors_ConcurrentUse(t *testing.T) {
	chain := NewChainDescriptors()
	g := directConnOver(t, startHealthNode(t, true))
	g.SetChainDescriptors(chain)
	methodDescriptor, err := resolve(t, g)
	require.NoError(t, err)
	service := g.GetCachedMethodDescriptor(healthCheckPath).GetService()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); chain.AddService(service) }()
		go func() {
			defer wg.Done()
			got, ok := chain.Method("grpc.health.v1.Health", "Check")
			if assert.True(t, ok) {
				assert.Equal(t, methodDescriptor.GetFullyQualifiedName(), got.GetFullyQualifiedName())
			}
		}()
	}
	wg.Wait()

	var none *ChainDescriptors
	_, ok := none.Method("grpc.health.v1.Health", "Check")
	require.False(t, ok, "a nil store shares nothing")
	none.AddService(service)
}

// The warm-up sweep, the snapshot it takes and each on-demand lookup are reported to the reflection
// observer, which counts them as the router's own operations (MAG-4563).
func TestReflectionObserver_SeesTheWarmUp(t *testing.T) {
	g := directConnOver(t, startHealthNode(t, true))
	var outcomes []error
	var mu sync.Mutex
	g.SetReflectionObserver(func(err error) {
		mu.Lock()
		defer mu.Unlock()
		outcomes = append(outcomes, err)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, g.Prewarm(ctx))

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, outcomes, "the sweep was observed")
	for _, err := range outcomes {
		require.NoError(t, err)
	}
}
