package chainlib

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// fiberChainListener is what the shutdown tests drive on each listener that serves through fiber.
type fiberChainListener interface {
	Serve(ctx context.Context, cmdFlags common.ConsumerCmdFlags)
	Shutdown(ctx context.Context) error
	GetListeningAddress() string
}

// fiberChainListeners builds one of each fiber listener on a free local port. None of the tests
// relays, so the relay sender is nil.
func fiberChainListeners(t *testing.T, ctx context.Context) map[string]func() fiberChainListener {
	t.Helper()
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	endpoint := func(chainID, apiInterface string) *routersession.RPCEndpoint {
		return &routersession.RPCEndpoint{
			NetworkAddress:  "127.0.0.1:0",
			ChainID:         chainID,
			ApiInterface:    apiInterface,
			HealthCheckPath: common.DEFAULT_HEALTH_PATH,
		}
	}
	return map[string]func() fiberChainListener{
		"jsonrpc": func() fiberChainListener {
			return NewJrpcChainListener(ctx, endpoint("ETH1", spectypes.APIInterfaceJsonRPC), nil, alwaysHealthyReporter{}, logger, nil, nil)
		},
		"tendermintrpc": func() fiberChainListener {
			return NewTendermintRpcChainListener(ctx, endpoint("COS5", spectypes.APIInterfaceTendermintRPC), nil, alwaysHealthyReporter{}, logger, nil, nil)
		},
		"rest": func() fiberChainListener {
			return NewRestChainListener(ctx, endpoint("LAV1", spectypes.APIInterfaceRest), nil, alwaysHealthyReporter{}, logger)
		},
		"graphql": func() fiberChainListener {
			return NewGraphQLChainListener(ctx, endpoint("SUI", spectypes.APIInterfaceGraphQL), nil, alwaysHealthyReporter{}, logger)
		},
	}
}

// serveInBackground runs Serve and returns a channel closed when it returns.
func serveInBackground(ctx context.Context, listener fiberChainListener) <-chan struct{} {
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		listener.Serve(ctx, common.ConsumerCmdFlags{})
	}()
	return returned
}

// requireStopped fails unless Serve returned and addr refuses connections. The serve context is
// never cancelled in these tests, so Shutdown alone has to stop the listener: the serve loop
// would otherwise bind again after RetryListeningInterval.
func requireStopped(t *testing.T, returned <-chan struct{}, addr string) {
	t.Helper()
	select {
	case <-returned:
	case <-time.After(3 * time.Second):
		t.Fatal("Serve still running after Shutdown")
	}
	if addr == "" {
		return
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + common.DEFAULT_HEALTH_PATH)
	if err == nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err, "a request after Shutdown must be refused, got HTTP %v", resp)
}

// A Shutdown that runs before Serve has started leaves nothing to drain. Serve must then return
// without serving instead of binding the port (MAG-5357).
func TestFiberChainListeners_ShutdownBeforeServe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for name, build := range fiberChainListeners(t, ctx) {
		t.Run(name, func(t *testing.T) {
			listener := build()
			shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelShutdown()
			require.NoError(t, listener.Shutdown(shutdownCtx))

			returned := serveInBackground(ctx, listener)
			requireStopped(t, returned, "")
			require.Empty(t, listener.GetListeningAddress(), "a listener shut down before Serve must never publish an address")
		})
	}
}

// A Shutdown of a listener that is serving stops it: Serve returns and the port refuses
// connections.
func TestFiberChainListeners_ShutdownWhileServing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for name, build := range fiberChainListeners(t, ctx) {
		t.Run(name, func(t *testing.T) {
			listener := build()
			returned := serveInBackground(ctx, listener)

			// The address is published before fiber starts serving; a health check that
			// answers is what proves the server took the socket.
			require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, 3*time.Second, 10*time.Millisecond)
			addr := listener.GetListeningAddress()
			client := &http.Client{Timeout: 2 * time.Second}
			require.Eventually(t, func() bool {
				resp, err := client.Get("http://" + addr + common.DEFAULT_HEALTH_PATH)
				if err != nil {
					return false
				}
				_ = resp.Body.Close()
				return resp.StatusCode == http.StatusOK
			}, 3*time.Second, 10*time.Millisecond)
			client.CloseIdleConnections()

			shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelShutdown()
			require.NoError(t, listener.Shutdown(shutdownCtx))
			requireStopped(t, returned, addr)
		})
	}
}

// fiber runs its OnListen hooks inside Listener, after the socket is bound and before fasthttp's
// Serve takes it: the window in which fasthttp's own Shutdown finds no listener and returns
// without stopping anything. A Shutdown there must still stop the server (MAG-5357).
func TestListenWithRetry_ShutdownBetweenBindAndServe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Get(common.DEFAULT_HEALTH_PATH, func(c *fiber.Ctx) error { return c.SendString("served") })
	var gate serveGate
	shutdownErr := make(chan error, 1)
	app.Hooks().OnListen(func(fiber.ListenData) error {
		shutdownErr <- gate.close(context.Background())
		return nil // a hook error panics inside fiber
	})

	addrCh := make(chan string, 1)
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		listenWithRetry(ctx, app, "127.0.0.1:0", common.NewSafeChannelSender(ctx, addrCh), &gate)
	}()

	var addr string
	select {
	case addr = <-addrCh:
	case <-time.After(3 * time.Second):
		t.Fatal("listenWithRetry never bound")
	}
	select {
	case err := <-shutdownErr:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("the OnListen hook never ran")
	}
	requireStopped(t, returned, addr)
}
