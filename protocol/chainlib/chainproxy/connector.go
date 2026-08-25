package chainproxy

//
// Right now this is only for Ethereum
// TODO: make this into a proper connection pool that supports
// the chainproxy interface

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/sigs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	ParallelConnectionsFlag                    = "parallel-connections"
	GRPCUseTls                                 = "use-tls"
	GRPCAllowInsecureConnection                = "allow-insecure-connection"
	MaximumNumberOfParallelConnectionsAttempts = 10
	MaxCallRecvMsgSize                         = 1024 * 1024 * 512 // setting receive size to 512mb for large debug responses
)

// NumberOfParallelConnections is the DEFAULT pool size — the default value of the
// --parallel-connections flag. It is configuration, not state, and const is what
// makes that a guarantee rather than a convention: no future caller can reintroduce
// the write below, because the assignment no longer compiles.
//
// It used to be both. NewConnector and NewGRPCConnector each wrote it with their own
// nConns, and GRPCConnector.ReturnRpc read it back as "the number we started with",
// so a connector's idle-trim threshold was whichever connector happened to be built
// last, process-wide. With one connector that is invisible; with two it is a data
// race (MAG-2538 tears a verification connector down while the next is constructed),
// and across differing nConns it trims the wrong pool. Capacity belongs to a
// connector, so it now lives on the connector.
const NumberOfParallelConnections uint = 10

// Connector manages HTTP/JSON-RPC connections to blockchain nodes.
// For HTTP connections, a single shared rpcclient.Client is used since
// http.Client is goroutine-safe and handles connection pooling internally.
// This design eliminates lock contention and maximizes connection reuse.
type Connector struct {
	client        *rpcclient.Client // Single shared client - goroutine safe
	nodeUrl       common.NodeUrl
	hashedNodeUrl string
	closed        atomic.Int32 // atomic flag to track if connector is closed
}

func HashURL(url string) string {
	// Convert the URL string to bytes
	urlBytes := []byte(url)

	// Hash the URL using the HashMsg function
	hashedBytes := sigs.HashMsg(urlBytes)

	// Encode the hashed bytes to a hex string for easier sharing
	return hex.EncodeToString(hashedBytes)
}

// NewConnector creates a new HTTP/JSON-RPC connector.
// Note: The nConns parameter is ignored for HTTP connections since a single
// shared client is used (http.Client is goroutine-safe and handles connection
// pooling internally). The parameter is kept for API compatibility with
// gRPC connector which still uses connection pooling.
func NewConnector(ctx context.Context, nConns uint, nodeUrl common.NodeUrl) (*Connector, error) {
	// nConns is deliberately not recorded anywhere: HTTP pools inside http.Transport.
	// This used to assign NumberOfParallelConnections "for the gRPC connector", which
	// made building an HTTP connector silently reset an unrelated gRPC connector's
	// idle-trim threshold — and race its ReturnRpc. That constant is now const, so
	// the assignment cannot come back.
	connector := &Connector{
		nodeUrl:       nodeUrl,
		hashedNodeUrl: HashURL(nodeUrl.Url),
	}

	// Create a single shared client - it's goroutine-safe and handles
	// connection pooling via the shared http.Transport
	rpcClient, err := connector.createConnection(ctx, nodeUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w, url: %s", err, nodeUrl.UrlStr())
	}

	connector.client = rpcClient
	utils.LavaFormatInfo("Created HTTP connector with shared client",
		utils.Attribute{Key: "url", Value: connector.nodeUrl.String()})

	// Start the connector loop to handle graceful shutdown
	go connector.connectorLoop(ctx)

	return connector, nil
}

func (connector *Connector) getRpcClient(ctx context.Context, nodeUrl common.NodeUrl) (*rpcclient.Client, error) {
	authPathNodeUrl := nodeUrl.AuthConfig.AddAuthPath(nodeUrl.Url)
	// origin used for auth header in the websocket case
	authHeaders := nodeUrl.GetAuthHeaders()
	rpcClient, err := rpcclient.DialContext(ctx, authPathNodeUrl, authHeaders)
	if err != nil {
		return nil, err
	}
	nodeUrl.SetAuthHeaders(ctx, rpcClient.SetHeader)
	return rpcClient, nil
}

func (connector *Connector) createConnection(ctx context.Context, nodeUrl common.NodeUrl) (*rpcclient.Client, error) {
	var rpcClient *rpcclient.Client
	var err error

	for attempt := 1; attempt <= MaximumNumberOfParallelConnectionsAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		timeout := common.AverageWorldLatency * (1 + time.Duration(attempt))
		nctx, cancel := nodeUrl.LowerContextTimeoutWithDuration(ctx, timeout)
		rpcClient, err = connector.getRpcClient(nctx, nodeUrl)
		cancel()

		if err == nil {
			return rpcClient, nil
		}

		utils.LavaFormatDebug("Failed to create connection, retrying",
			utils.Attribute{Key: "attempt", Value: attempt},
			utils.Attribute{Key: "error", Value: err.Error()},
			utils.Attribute{Key: "url", Value: nodeUrl.UrlStr()})
	}

	return nil, utils.LavaFormatError("Failed to create connection after max attempts",
		err, utils.Attribute{Key: "url", Value: nodeUrl.UrlStr()})
}

func (connector *Connector) connectorLoop(ctx context.Context) {
	<-ctx.Done()
	utils.LavaFormatDebug("HTTP connector shutting down", utils.Attribute{Key: "url", Value: connector.nodeUrl.String()})
	connector.Close()
}

// Close closes the connector and its underlying client.
func (connector *Connector) Close() {
	// Use atomic to ensure we only close once
	if !connector.closed.CompareAndSwap(0, 1) {
		return // Already closed
	}

	if connector.client != nil {
		connector.client.Close()
	}
}

// getting hashed url from connection. this is never changed. so its not locked.
func (connector *Connector) GetUrlHash() string {
	return connector.hashedNodeUrl
}

// GetRpc returns the shared RPC client.
// The client is goroutine-safe and handles connection pooling internally,
// so no locking or pool management is needed.
// The 'block' parameter is kept for API compatibility but is not used.
func (connector *Connector) GetRpc(ctx context.Context, block bool) (*rpcclient.Client, error) {
	if connector.closed.Load() == 1 {
		return nil, errors.New("connector is closed")
	}
	return connector.client, nil
}

// ReturnRpc is a no-op for HTTP connections.
// The shared client is goroutine-safe and doesn't need to be "returned".
// This method exists for API compatibility.
func (connector *Connector) ReturnRpc(rpc *rpcclient.Client) {
	// No-op: HTTP clients are goroutine-safe and shared.
	// Connection pooling is handled by the underlying http.Transport.
}

type GRPCConnector struct {
	lock        sync.RWMutex
	freeClients []*grpc.ClientConn
	usedClients int64
	credentials credentials.TransportCredentials
	nodeUrl     common.NodeUrl

	// capacity is the pool size this connector was built with — the nConns its
	// caller asked for, not whatever the last-constructed connector asked for.
	// ReturnRpc trims idle clients against it.
	//
	// Written once in NewGRPCConnector before the connector is published and never
	// again, so readers need no synchronization beyond the happens-before that
	// publishing already gives them.
	capacity uint

	// closed is set by Close and never cleared. Unlike the HTTP Connector's atomic
	// flag it is guarded by lock, because every reader already holds lock: that
	// makes "is the pool alive" and "take a client" a single atomic step, which a
	// separate atomic could not guarantee.
	closed bool
}

// ErrGRPCConnectorClosed is returned by GetRpc once Close has run. The HTTP
// Connector has always reported this; GRPCConnector had no closed state at all,
// so callers racing a Close either blocked forever on a pool that would never
// refill or had their retry goroutines repopulate it (MAG-2808).
var ErrGRPCConnectorClosed = errors.New("grpc connector is closed")

// NewGRPCConnector builds a pooled gRPC connector.
//
// The two contexts are deliberately separate, because a single one was three
// different things at once and callers could only get two of them right
// (MAG-2808):
//
//	lifetimeCtx  owns the pool. connectorLoop parks on it and closes the connector
//	             when it ends, and the async fill dials under it — that fill is
//	             background work which should finish, not work that belongs to
//	             whoever happened to call this.
//	dialCtx      bounds ONLY the initial blocking dial below, which runs in the
//	             caller's critical path. It is the caller's own deadline, so a
//	             relay does not wait materially past the point it would have given
//	             up anyway.
//
// Callers with nothing to distinguish (a startup path, where the process's
// context is both) pass the same context twice. Callers building a pool inside a
// request — the direct-RPC path — must NOT: passing the request's context as
// lifetimeCtx tears the pool down the moment that request returns.
func NewGRPCConnector(lifetimeCtx, dialCtx context.Context, nConns uint, nodeUrl common.NodeUrl) (*GRPCConnector, error) {
	connector := &GRPCConnector{
		freeClients: make([]*grpc.ClientConn, 0, nConns),
		nodeUrl:     nodeUrl,
		capacity:    nConns,
	}

	// MAG-2218: warn once per connector, not per dial. createConnection may still upgrade an
	// unconfigured endpoint to TLS on retry, so this reflects the configured intent.
	nodeUrl.TokenOverInsecureWarning(nodeUrl.AuthConfig.GetUseTls() || strings.HasPrefix(nodeUrl.Url, "grpcs://"))

	rpcClient, err := connector.createConnection(dialCtx, nodeUrl, connector.numberOfFreeClients())
	if err != nil {
		return nil, utils.LavaFormatError("grpc failed to create the first connection", err, utils.Attribute{Key: "address", Value: nodeUrl.UrlStr()})
	}
	connector.addClient(rpcClient)
	go addClientsAsynchronouslyGrpc(lifetimeCtx, connector, nConns-1, nodeUrl)
	return connector, nil
}

// grpcDialOptions assembles the options shared by every dial this connector makes: the
// transport credentials the caller decided on, the receive-size cap, and (MAG-2218) the
// endpoint's configured auth-headers. All three dial sites in this file go through it, so
// the auth attachment cannot be present on one path and missing on another — which is the
// class of bug MAG-2218 was.
func (connector *GRPCConnector) grpcDialOptions(transport grpc.DialOption) []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithBlock(),
		transport,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxCallRecvMsgSize)),
	}
	return append(opts, connector.nodeUrl.GrpcAuthDialOptions()...)
}

func getTlsConf(nodeUrl common.NodeUrl) *tls.Config {
	var tlsConf tls.Config
	cacert := nodeUrl.AuthConfig.GetCaCertificateParams()
	if cacert != "" {
		utils.LavaFormatDebug("Loading ca certificate from local path", utils.Attribute{Key: "cacert", Value: cacert})
		caCert, err := os.ReadFile(cacert)
		if err == nil {
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConf.RootCAs = caCertPool
			tlsConf.InsecureSkipVerify = true
		} else {
			utils.LavaFormatError("Failed loading CA certificate, continuing with a default certificate", err)
		}
	} else {
		keyPem, certPem := nodeUrl.AuthConfig.GetLoadingCertificateParams()
		if keyPem != "" && certPem != "" {
			utils.LavaFormatDebug("Loading certificate from local path", utils.Attribute{Key: "certPem", Value: certPem}, utils.Attribute{Key: "keyPem", Value: keyPem})
			cert, err := tls.LoadX509KeyPair(certPem, keyPem)
			if err != nil {
				utils.LavaFormatError("Failed setting up tls certificate from local path, continuing with dynamic certificates", err)
			} else {
				tlsConf.Certificates = []tls.Certificate{cert}
			}
		}
	}
	if nodeUrl.AuthConfig.AllowInsecure {
		tlsConf.InsecureSkipVerify = true
	}
	return &tlsConf
}

func (connector *GRPCConnector) setCredentials(credentials credentials.TransportCredentials) {
	connector.lock.Lock() // add connection to free list.
	defer connector.lock.Unlock()
	connector.credentials = credentials
}

func (connector *GRPCConnector) getCredentials() credentials.TransportCredentials {
	connector.lock.RLock() // add connection to free list.
	defer connector.lock.RUnlock()
	return connector.credentials
}

func (connector *GRPCConnector) getTransportCredentials() grpc.DialOption {
	creds := connector.getCredentials()
	if creds != nil {
		return grpc.WithTransportCredentials(creds)
	}
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

// grpcDialAddress converts a configured node URL into what grpc.DialContext expects.
// DialContext takes host:port; the grpc:// / grpcs:// prefixes are a config-time
// convention enforced by the direct-RPC validator (see
// protocol/lavasession/direct_rpc_connection.go validateURL). The grpcs:// form also
// implies TLS.
//
// EVERY dial site must go through this. increaseNumberOfClients used to dial the raw
// configured URL while createConnection stripped the scheme locally, so a grpcs://
// endpoint opened its first connection and then could never refill its pool: GetRpc
// spawns a refill every 50ms while a caller waits, each one failed on the unresolvable
// scheme-prefixed target, and the caller blocked until its context expired. On a
// one-connection pool whose single client is held for reflection, that is every relay
// (MAG-2333).
func grpcDialAddress(url string) (addr string, impliesTLS bool) {
	if after, ok := strings.CutPrefix(url, "grpcs://"); ok {
		return after, true
	}
	return strings.TrimPrefix(url, "grpc://"), false
}

func (connector *GRPCConnector) increaseNumberOfClients(ctx context.Context, numberOfFreeClients int) {
	utils.LavaFormatDebug("increasing number of clients", utils.Attribute{Key: "numberOfFreeClients", Value: numberOfFreeClients},
		utils.Attribute{Key: "url", Value: connector.nodeUrl.Url})
	if connector.isClosed() {
		return // the pool is being torn down; dialing now would only resurrect it
	}
	var grpcClient *grpc.ClientConn
	var err error
	for connectionAttempt := range MaximumNumberOfParallelConnectionsAttempts {
		nctx, cancel := connector.nodeUrl.LowerContextTimeoutWithDuration(ctx, common.AverageWorldLatency*2)
		dialAddr, _ := grpcDialAddress(connector.nodeUrl.Url)
		grpcClient, err = grpc.DialContext(nctx, dialAddr, connector.grpcDialOptions(connector.getTransportCredentials())...)
		if err != nil {
			utils.LavaFormatDebug("increaseNumberOfClients, Could not connect to the node, retrying", []utils.Attribute{{Key: "err", Value: err.Error()}, {Key: "Number Of Attempts", Value: connectionAttempt}, {Key: "nodeUrl", Value: connector.nodeUrl.UrlStr()}}...)
			cancel()
			continue
		}
		cancel()

		connector.lock.Lock() // add connection to free list.
		defer connector.lock.Unlock()
		if connector.closed {
			// Close drained the pool while this dial was in flight. Appending now
			// would hand a live client to a closed connector that nobody will ever
			// borrow from again, so close it here instead.
			grpcClient.Close()
			return
		}
		connector.freeClients = append(connector.freeClients, grpcClient)
		return
	}
	utils.LavaFormatDebug("increasing number of clients failed")
}

func (connector *GRPCConnector) GetRpc(ctx context.Context, block bool) (*grpc.ClientConn, error) {
	connector.lock.Lock()
	defer connector.lock.Unlock()

	if connector.closed {
		return nil, ErrGRPCConnectorClosed
	}

	numberOfFreeClients := len(connector.freeClients)
	if numberOfFreeClients <= int(connector.usedClients) { // if we reached half of the free clients start creating new connections
		go connector.increaseNumberOfClients(ctx, numberOfFreeClients) // increase asynchronously the free list.
	}

	if numberOfFreeClients == 0 {
		if !block {
			return nil, errors.New("out of clients")
		}
		// Wait for the async fill to produce a client. Both exits below are new:
		// this loop used to spin unconditionally, so a caller waiting on a pool
		// that would never refill was stuck forever even after its context was
		// cancelled, and the goroutines it kept spawning could repopulate a pool
		// Close had just drained (MAG-2808).
		for {
			connector.lock.Unlock()
			// if we reached 0 connections we need to create more connections
			// before sleeping, increase asynchronously the free list.
			go connector.increaseNumberOfClients(ctx, numberOfFreeClients)

			var ctxDone bool
			select {
			case <-ctx.Done():
				ctxDone = true
			case <-time.After(50 * time.Millisecond):
			}

			connector.lock.Lock()
			if ctxDone {
				return nil, fmt.Errorf("waiting for a free grpc client: %w", ctx.Err())
			}
			if connector.closed {
				return nil, ErrGRPCConnectorClosed
			}
			numberOfFreeClients = len(connector.freeClients)
			if numberOfFreeClients != 0 {
				break
			}
		}
	}

	ret := connector.freeClients[0]
	connector.usedClients++
	connector.freeClients = connector.freeClients[1:]

	return ret, nil
}

func (connector *GRPCConnector) ReturnRpc(rpc *grpc.ClientConn) {
	connector.lock.Lock()
	defer connector.lock.Unlock()

	// The decrement happens on every path below, including the nil handback: Close
	// is blocked waiting for usedClients to reach zero, so skipping it would leave
	// teardown spinning forever.
	connector.usedClients--
	if rpc == nil {
		// Defensive — a successful GetRpc never yields nil, and callers must not
		// hand back what they did not borrow. Checked once here so no path below
		// dereferences it.
		return
	}
	if connector.closed {
		// Drop the conn rather than appending it to a pool that is being torn down,
		// which would leave a live client behind.
		rpc.Close()
		return
	}
	if len(connector.freeClients) > (int(connector.usedClients) + int(connector.capacity) /* the number THIS connector started with */) {
		rpc.Close() // close connection
		return      // return without appending back to decrease idle connections
	}
	connector.freeClients = append(connector.freeClients, rpc)
}

func (connector *GRPCConnector) connectorLoop(ctx context.Context) {
	<-ctx.Done()
	log.Println("connectorLoop ctx.Done")
	connector.Close()
}

func (connector *GRPCConnector) Close() {
	for i := 0; ; i++ {
		connector.lock.Lock()
		// Mark closed under the same lock GetRpc, ReturnRpc and
		// increaseNumberOfClients take. From here on no client can be handed out
		// and no in-flight dial can refill the pool we are about to drain, so the
		// usedClients wait below is guaranteed to make progress.
		connector.closed = true
		for i := 0; i < len(connector.freeClients); i++ {
			connector.freeClients[i].Close()
		}
		connector.freeClients = []*grpc.ClientConn{}

		if connector.usedClients > 0 {
			if i > 10 {
				utils.LavaFormatError("stuck while closing grpc connector", nil, utils.LogAttr("freeClients", connector.freeClients), utils.LogAttr("usedClients", connector.usedClients))
			}
			connector.lock.Unlock()
			time.Sleep(100 * time.Millisecond)
		} else {
			connector.lock.Unlock()
			break
		}
	}
}

// addClientsAsynchronouslyGrpc fills the pool behind the caller and then hands the
// connector to connectorLoop. Both halves run under lifetimeCtx, never the dial
// context: the fill is background work that should finish even after the caller
// has moved on, and connectorLoop parked on a caller's context is precisely the
// bug MAG-2808 fixed.
func addClientsAsynchronouslyGrpc(lifetimeCtx context.Context, connector *GRPCConnector, nConns uint, nodeUrl common.NodeUrl) {
	for range nConns {
		if connector.isClosed() {
			break // Close is draining; further dials would only be thrown away
		}
		rpcClient, err := connector.createConnection(lifetimeCtx, nodeUrl, connector.numberOfFreeClients())
		if err != nil {
			break
		}
		connector.addClient(rpcClient)
	}
	if (connector.numberOfFreeClients() + connector.numberOfUsedClients()) == 0 {
		if connector.isClosed() {
			// A connector closed this early legitimately has no clients — Close
			// drained them. Reaching the fatal below would turn an ordinary
			// teardown during startup into a process exit.
			utils.LavaFormatWarning("gRPC connector closed before the async fill finished", nil,
				utils.LogAttr("address", nodeUrl.UrlStr()),
			)
		} else if lifetimeCtx.Err() != nil {
			// Probe-scoped ctx (validateProvider, etc.) was cancelled before
			// the async fill produced any client. Caller will treat the
			// returned error as a probe failure; the process must not exit.
			utils.LavaFormatWarning("gRPC connector aborted before any connection was built", lifetimeCtx.Err(),
				utils.LogAttr("address", nodeUrl.UrlStr()),
			)
		} else {
			utils.LavaFormatFatal("Could not create any connections to the node check address", nil,
				utils.Attribute{Key: "address", Value: nodeUrl.UrlStr()},
			)
		}
	}
	// Read the count through the locked accessor: addClient / GetRpc mutate freeClients under the lock
	// concurrently, so a bare len(connector.freeClients) here is a data race.
	utils.LavaFormatInfo("Finished adding clients asynchronously", utils.LogAttr("count", connector.numberOfFreeClients()))
	go connector.connectorLoop(lifetimeCtx)
}

func (connector *GRPCConnector) addClient(client *grpc.ClientConn) {
	connector.lock.Lock()
	defer connector.lock.Unlock()
	if connector.closed {
		// The startup fill is still running while Close drains. Appending here
		// would refill the pool behind Close's back, exactly as the retry path in
		// increaseNumberOfClients could (MAG-2808).
		client.Close()
		return
	}
	connector.freeClients = append(connector.freeClients, client)
}

func (connector *GRPCConnector) isClosed() bool {
	connector.lock.RLock()
	defer connector.lock.RUnlock()
	return connector.closed
}

func (connector *GRPCConnector) numberOfFreeClients() int {
	connector.lock.RLock()
	defer connector.lock.RUnlock()
	return len(connector.freeClients)
}

func (connector *GRPCConnector) numberOfUsedClients() int {
	// Read under the lock, NOT via atomic: every other access to usedClients (GetRpc/ReturnRpc/Close)
	// is a plain read/write under connector.lock. A lone atomic.Load here doesn't synchronize with
	// those mutex-protected writes (atomic and mutex are different happens-before domains), so it
	// raced GetRpc's usedClients++ under -race. Match numberOfFreeClients().
	connector.lock.RLock()
	defer connector.lock.RUnlock()
	return int(connector.usedClients)
}

func (connector *GRPCConnector) createConnection(ctx context.Context, nodeUrl common.NodeUrl, currentNumberOfConnections int) (*grpc.ClientConn, error) {
	// Auto-enable TLS on the local nodeUrl copy when the scheme implies it, so config
	// can rely on the scheme alone.
	addr, impliesTLS := grpcDialAddress(nodeUrl.Url)
	if impliesTLS {
		nodeUrl.AuthConfig.UseTLS = true
	}
	var rpcClient *grpc.ClientConn
	var err error
	numberOfConnectionAttempts := 0

	var credentialsToConnect credentials.TransportCredentials = nil
	// in the case the grpc server needs to connect using tls, but we haven't set credentials yet, should only happen once
	if nodeUrl.AuthConfig.GetUseTls() && connector.getCredentials() == nil {
		// this will allow us to use self signed certificates in development.
		tlsConf := getTlsConf(nodeUrl)
		connector.setCredentials(credentials.NewTLS(tlsConf))
	}

	for {
		numberOfConnectionAttempts += 1
		if numberOfConnectionAttempts > MaximumNumberOfParallelConnectionsAttempts {
			err = utils.LavaFormatError("Reached maximum number of parallel connections attempts, consider decreasing number of connections",
				nil, utils.Attribute{Key: "Currently Connected", Value: currentNumberOfConnections})
			return nil, err
		}
		if ctx.Err() != nil {
			// Don't Close() the connector — that wipes the existing pool.
			// One probe's cancellation must not destroy connections that
			// other callers are still using. Just stop dialling more and
			// return the error.
			return nil, ctx.Err()
		}
		nctx, cancel := connector.nodeUrl.LowerContextTimeoutWithDuration(ctx, common.AverageWorldLatency*2)
		rpcClient, err = grpc.DialContext(nctx, addr, connector.grpcDialOptions(connector.getTransportCredentials())...)
		cancel()
		if err == nil {
			return rpcClient, nil
		}

		// in case the provider didn't set TLS config and there are no active connections will do a retry with secure connection in case the endpoint is secure
		if connector.getCredentials() == nil && connector.numberOfFreeClients()+connector.numberOfUsedClients() == 0 {
			if credentialsToConnect == nil {
				tlsConf := getTlsConf(nodeUrl)
				credentialsToConnect = credentials.NewTLS(tlsConf)
			}
			nctx, cancel := connector.nodeUrl.LowerContextTimeoutWithDuration(ctx, common.AverageWorldLatency*2)
			var errNew error
			rpcClient, errNew = grpc.DialContext(nctx, addr, connector.grpcDialOptions(grpc.WithTransportCredentials(credentialsToConnect))...)
			cancel()
			if errNew == nil {
				// this means our endpoint is TLS, and we support upgrading even if the config didn't explicitly say it
				utils.LavaFormatDebug("upgraded TLS connection for grpc instead of insecure", utils.LogAttr("address", nodeUrl.String()))
				connector.setCredentials(credentialsToConnect)
				return rpcClient, nil
			}
		}
		utils.LavaFormatWarning("grpc could not connect to the node, retrying", err, []utils.Attribute{{
			Key: "Current Number Of Connections", Value: currentNumberOfConnections,
		}, {Key: "Number Of Attempts Remaining", Value: numberOfConnectionAttempts}, {Key: "nodeUrl", Value: connector.nodeUrl.UrlStr()}}...)
	}
}
