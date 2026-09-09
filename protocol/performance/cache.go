package performance

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/protocol/lavasession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type relayerCacheClientStore struct {
	client       pairingtypes.RelayerCacheClient
	conn         *grpc.ClientConn // stored so we can close it on reconnect or shutdown
	lock         sync.RWMutex
	ctx          context.Context
	address      string
	reconnecting atomic.Bool
	closed       atomic.Bool // set by close(); no reconnect may spawn afterwards
}

const (
	reconnectInterval = 5 * time.Second
)

func newRelayerCacheClientStore(ctx context.Context, address string) (*relayerCacheClientStore, error) {
	clientStore := &relayerCacheClientStore{
		client:  nil,
		ctx:     ctx,
		address: address,
	}
	return clientStore, clientStore.connectClient()
}

// peekConnState reports the live connectivity of the installed connection WITHOUT
// touching it. It exists because getClient() is not a read: it spawns
// `go reconnectClient()` whenever the client is nil, so an observer that called it
// to ask "is the cache up?" would dial the cache as a side effect of asking.
// /debug/cache-state is documented read-only and must not do that.
//
// The second return distinguishes "no connection installed" from a state reading.
// grpc.ClientConn.GetState() is a plain accessor — unlike Connect() and
// WaitForStateChange() it neither triggers nor waits on a transition.
func (r *relayerCacheClientStore) peekConnState() (connectivity.State, bool) {
	if r == nil {
		return connectivity.Shutdown, false
	}
	r.lock.RLock()
	defer r.lock.RUnlock()
	if r.client == nil || r.conn == nil {
		return connectivity.Shutdown, false
	}
	return r.conn.GetState(), true
}

func (r *relayerCacheClientStore) getClient() pairingtypes.RelayerCacheClient {
	if r == nil {
		return nil
	}

	r.lock.RLock()
	defer r.lock.RUnlock()

	if r.client == nil && !r.closed.Load() {
		go r.reconnectClient()
	}

	return r.client // might be nil
}

func (r *relayerCacheClientStore) connectGRPCConnectionToRelayerCacheService() (pairingtypes.RelayerCacheClient, *grpc.ClientConn, error) {
	connectCtx, cancel := context.WithTimeout(r.ctx, 3*time.Second)
	defer cancel()

	conn, err := lavasession.ConnectGRPCClient(connectCtx, r.address, false, true)
	if err != nil {
		return nil, nil, err
	}

	c := pairingtypes.NewRelayerCacheClient(conn)
	return c, conn, nil
}

func (r *relayerCacheClientStore) connectClient() error {
	relayerCacheClient, conn, err := r.connectGRPCConnectionToRelayerCacheService()
	if err == nil {
		utils.LavaFormatInfo("cache service connected successfully", utils.LogAttr("address", r.address))
		func() {
			r.lock.Lock()
			defer r.lock.Unlock()
			// A dial that was in flight when close() ran must not resurrect the
			// client — release the fresh connection instead of installing it.
			if r.closed.Load() {
				if closeErr := conn.Close(); closeErr != nil {
					utils.LavaFormatWarning("failed closing cache gRPC connection dialed after close", closeErr, utils.LogAttr("address", r.address))
				}
				return
			}
			// Close the old connection before replacing it to prevent goroutine leaks.
			// Each *grpc.ClientConn spawns internal goroutines (reader, writer, callback serializers)
			// that only exit when conn.Close() is called.
			if r.conn != nil {
				utils.LavaFormatDebug("closing previous cache gRPC connection before replacing", utils.LogAttr("address", r.address))
				if err := r.conn.Close(); err != nil {
					utils.LavaFormatWarning("failed to close previous cache gRPC connection", err, utils.LogAttr("address", r.address))
				}
			}
			r.client = relayerCacheClient
			r.conn = conn
		}()

		r.reconnecting.Store(false)
		return nil // connected
	}

	utils.LavaFormatDebug("cache service connection attempt failed", utils.LogAttr("address", r.address), utils.LogAttr("error", err))
	return err
}

func (r *relayerCacheClientStore) reconnectClient() {
	// This is a simple atomic operation to ensure that only one goroutine is reconnecting at a time.
	// reconnecting.CompareAndSwap(false, true):
	// if reconnecting == false {
	// 	reconnecting = true
	// 	return true -> reconnect
	// }
	// return false -> already reconnecting
	if !r.reconnecting.CompareAndSwap(false, true) {
		return
	}

	utils.LavaFormatInfo("cache service reconnection loop started", utils.LogAttr("address", r.address))

	for {
		if r.closed.Load() {
			utils.LavaFormatInfo("cache service reconnection loop exiting (client closed)", utils.LogAttr("address", r.address))
			return
		}
		// Dial first, sleep only between failed attempts. Otherwise a caller
		// that just discovered client == nil waits a full reconnectInterval
		// before any retry happens, leaving the cache silently skipped.
		if r.connectClient() == nil {
			utils.LavaFormatInfo("cache service reconnection succeeded, exiting reconnect loop", utils.LogAttr("address", r.address))
			return
		}
		select {
		case <-r.ctx.Done():
			utils.LavaFormatInfo("cache service reconnection loop exiting (context cancelled)", utils.LogAttr("address", r.address))
			return
		case <-time.After(reconnectInterval):
		}
	}
}

// close permanently shuts the store: no further reconnect attempts spawn, and
// the live gRPC connection (if any) is released. Safe to race an in-flight
// reconnect — connectClient re-checks closed under the same lock before
// installing a fresh connection. Idempotent and nil-safe.
func (r *relayerCacheClientStore) close() error {
	if r == nil || !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	var err error
	if r.conn != nil {
		err = r.conn.Close()
	}
	r.client = nil
	r.conn = nil
	return err
}

// resetOnConnectionError clears the client when a gRPC connection-level error is detected,
// allowing the next getClient() call to trigger reconnection.
func (r *relayerCacheClientStore) resetOnConnectionError(err error) {
	if err == nil {
		return
	}
	code := status.Code(err)
	if code != codes.Unavailable {
		return
	}
	utils.LavaFormatWarning("cache service connection error detected, triggering reconnection", err, utils.LogAttr("address", r.address))
	r.lock.Lock()
	r.client = nil
	r.lock.Unlock()
	go r.reconnectClient()
}

type Cache struct {
	// The configured address deliberately lives in exactly ONE place —
	// clientStore.address, the string actually dialled — and is read back through
	// BackendEndpoint. Cache used to carry a second copy that InitCache kept in sync
	// by assigning the same argument to both; two accessors for one fact is how they
	// eventually disagree.
	clientStore *relayerCacheClientStore
	serviceCtx  context.Context
}

func InitCache(ctx context.Context, addr string) (*Cache, error) {
	clientStore, err := newRelayerCacheClientStore(ctx, addr)
	cache := &Cache{
		clientStore: clientStore,
		serviceCtx:  ctx,
	}
	if err != nil {
		// Initial dial failed. Start the reconnect loop eagerly so the
		// cache becomes live as soon as the backend is reachable, instead
		// of staying cold until the first relay triggers getClient().
		go clientStore.reconnectClient()
	}
	return cache, err
}

func (cache *Cache) GetEntry(ctx context.Context, relayCacheGet *pairingtypes.RelayCacheGet) (reply *pairingtypes.CacheRelayReply, err error) {
	if cache == nil {
		return nil, NotInitializedError
	}

	client := cache.clientStore.getClient()
	if client == nil {
		return nil, NotConnectedError
	}

	reply, err = client.GetRelay(ctx, relayCacheGet)
	if err != nil {
		cache.clientStore.resetOnConnectionError(err)
	}
	return reply, err
}

func (cache *Cache) CacheActive() bool {
	return cache != nil && cache.clientStore.getClient() != nil
}

// DebugCacheState reports this tier for GET /debug/cache-state. Read-only: it
// reads the connection's state rather than asking for a client, so observing a
// down cache does not dial it.
//
// Reachability comes from the gRPC connectivity state, NOT from "a client pointer
// is installed" (what CacheActive answers). The installed pointer is a latch: it is
// cleared only by resetOnConnectionError, which returns early for every status code
// except Unavailable and fires only from a relay's own GetEntry/SetEntry/Flush. So
// on an idle router a cache-be killed an hour ago still reads as active, while the
// connection has long since left Ready.
//
// The remaining gap is deliberate and documented rather than papered over: a cache-be
// that ACCEPTS connections but hangs keeps its transport in Ready, so this reports
// reachable while every operation times out. That is the honest reading of a
// connectivity state — the backend is reachable and unresponsive, which are different
// faults — and distinguishing them needs an active probe this read-only endpoint
// must not perform.
func (cache *Cache) DebugCacheState() DebugCacheState {
	// A typed-nil *Cache is how an unconfigured cache travels inside the
	// CacheBackend interface (see the wiring convention on CacheBackend), and a
	// typed nil still satisfies this interface — so "configured" must be decided
	// here, on the receiver, and never by the caller's `!= nil` on the interface.
	//
	// Both the configured test and the reported value read BackendEndpoint, so
	// there is exactly one source for the address.
	address := cache.BackendEndpoint()
	if address == "" {
		return DebugCacheState{Engine: CacheEngineGRPC}
	}
	state := DebugCacheState{
		Configured: true,
		Engine:     CacheEngineGRPC,
		Address:    address,
		// A gRPC tier that is not reachable is SKIPPED, not attempted: GetEntry
		// returns NotConnectedError before any I/O, so an unreachable primary costs
		// nothing per relay. The RESP tier does the opposite, which is why this is
		// reported rather than assumed.
		WhenUnreachable: CacheWhenUnreachableSkipped,
	}
	connState, connected := cache.clientStore.peekConnState()
	if !connected {
		state.Reachable = boolPtr(false)
		state.Detail = "no connection installed"
		return state
	}
	state.Detail = connState.String()
	switch connState {
	case connectivity.Ready, connectivity.Idle:
		// Idle is a healthy connection with no active transport (gRPC drops one
		// after an idle period); it dials on the next call. Reporting it as
		// unreachable would make a quiet router look broken.
		state.Reachable = boolPtr(true)
	case connectivity.Connecting:
		// Genuinely undetermined — the dial is in flight. Left nil rather than
		// guessed either way.
	default: // TransientFailure, Shutdown
		state.Reachable = boolPtr(false)
	}
	return state
}

func (cache *Cache) SetEntry(ctx context.Context, cacheSet *pairingtypes.RelayCacheSet) error {
	if cache == nil {
		return NotInitializedError
	}

	client := cache.clientStore.getClient()
	if client == nil {
		return NotConnectedError
	}

	_, err := client.SetRelay(ctx, cacheSet)
	if err != nil {
		cache.clientStore.resetOnConnectionError(err)
	}
	return err
}

// Flush asks the cache-be server to drop every entry it holds. Reached only
// from the router's /debug/reset-all handler — never from the relay hot path.
// Returns NotInitializedError when --cache-be is not configured and
// NotConnectedError while the gRPC client is reconnecting; callers can treat
// both as "no external cache to flush" and proceed.
func (cache *Cache) Flush(ctx context.Context) error {
	if cache == nil {
		return NotInitializedError
	}

	client := cache.clientStore.getClient()
	if client == nil {
		return NotConnectedError
	}

	_, err := client.FlushCache(ctx, &emptypb.Empty{})
	if err != nil {
		cache.clientStore.resetOnConnectionError(err)
	}
	return err
}

// BackendEndpoint returns the cache-be address this client is configured
// against. Unlike the RESP backend there is nothing dynamic to report — the
// cache sidecar is a single endpoint.
func (cache *Cache) BackendEndpoint() string {
	if cache == nil || cache.clientStore == nil {
		return ""
	}
	return cache.clientStore.address
}

// Close permanently shuts the client down: the background reconnect loop stops,
// the gRPC connection closes, and the cache reads as inactive (operations
// return NotConnectedError). Nil-safe and idempotent; reached from the router's
// graceful shutdown, never the relay hot path.
func (cache *Cache) Close() error {
	if cache == nil {
		return nil
	}
	return cache.clientStore.close()
}

// SetEndpointObservation publishes one successful poll of an upstream endpoint to the cache
// backend so peer pods can borrow it (the fleet tracker gate, MAG-2981). Fire-and-forget at the
// call site: a failed publish only costs a peer one real poll.
func (cache *Cache) SetEndpointObservation(ctx context.Context, set *pairingtypes.EndpointObservationSet) error {
	if cache == nil {
		return NotInitializedError
	}

	client := cache.clientStore.getClient()
	if client == nil {
		return NotConnectedError
	}

	_, err := client.SetEndpointObservation(ctx, set)
	if err != nil {
		cache.clientStore.resetOnConnectionError(err)
	}
	return err
}

// GetEndpointObservation reads the freshest peer observation of an upstream endpoint. A miss
// is a reply with Found=false, not an error.
func (cache *Cache) GetEndpointObservation(ctx context.Context, get *pairingtypes.EndpointObservationGet) (*pairingtypes.EndpointObservationReply, error) {
	if cache == nil {
		return nil, NotInitializedError
	}

	client := cache.clientStore.getClient()
	if client == nil {
		return nil, NotConnectedError
	}

	reply, err := client.GetEndpointObservation(ctx, get)
	if err != nil {
		cache.clientStore.resetOnConnectionError(err)
	}
	return reply, err
}
