package redisstore

import (
	"context"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/stretchr/testify/require"
)

type proxyMode int

const (
	proxyPass      proxyMode = iota
	proxyHang                // accepts, forwards nothing: a hung process
	proxyBlackhole           // accepts nothing, forwards nothing: a lost node
	proxyRefuse              // closed: a stopped process
)

// proxyBacklog is the accept queue of a faultProxy. It is small so that a
// handful of unaccepted connections fills it, after which the kernel drops
// every SYN, and wide enough that a burst of dials in pass mode never does.
const proxyBacklog = 16

// faultProxy fronts one fake node, so a test can take that node away the three
// ways a real one goes. The address stays the same throughout, which is what
// the client and the Sentinels keep naming.
type faultProxy struct {
	t        *testing.T
	listener net.Listener
	backend  string

	mu      sync.Mutex
	changed *sync.Cond
	mode    proxyMode
	held    []net.Conn
}

func newFaultProxy(t *testing.T, backend string) *faultProxy {
	t.Helper()
	// net.Listen takes its backlog from somaxconn, thousands of connections
	// deep, so the listening socket is made by hand.
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	file := os.NewFile(uintptr(fd), "fault-proxy")
	defer file.Close()
	require.NoError(t, syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}))
	require.NoError(t, syscall.Listen(fd, proxyBacklog))
	listener, err := net.FileListener(file)
	require.NoError(t, err)

	proxy := &faultProxy{t: t, listener: listener, backend: backend}
	proxy.changed = sync.NewCond(&proxy.mu)
	t.Cleanup(proxy.refuse)
	go proxy.accept()
	return proxy
}

func (p *faultProxy) addr() string { return p.listener.Addr().String() }

func (p *faultProxy) accept() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		p.held = append(p.held, conn)
		// A lost node completes no handshake: stop taking connections off the
		// queue, so the queue fills and the kernel drops what follows.
		for p.mode == proxyBlackhole {
			p.changed.Wait()
		}
		mode := p.mode
		p.mu.Unlock()
		switch mode {
		case proxyPass:
			go p.serve(conn)
		case proxyRefuse:
			return
		}
	}
}

func (p *faultProxy) serve(client net.Conn) {
	backend, err := net.Dial("tcp", p.backend)
	if err != nil {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	p.held = append(p.held, backend)
	p.mu.Unlock()
	go p.pump(backend, client)
	go p.pump(client, backend)
}

// pump forwards one direction while the proxy passes traffic, and parks on
// the first byte that arrives while it does not.
func (p *faultProxy) pump(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if !p.forwarding() {
				return
			}
			if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
				err = writeErr
			}
		}
		if err != nil {
			if p.forwarding() {
				_ = dst.Close()
				_ = src.Close()
			}
			return
		}
	}
}

// forwarding blocks while the node is silent, and reports whether traffic
// passes once it no longer is.
func (p *faultProxy) forwarding() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.mode == proxyHang || p.mode == proxyBlackhole {
		p.changed.Wait()
	}
	return p.mode == proxyPass
}

func (p *faultProxy) setMode(mode proxyMode) {
	p.mu.Lock()
	p.mode = mode
	p.mu.Unlock()
	p.changed.Broadcast()
}

func (p *faultProxy) hang() { p.setMode(proxyHang) }

func (p *faultProxy) blackhole() {
	p.t.Helper()
	p.setMode(proxyBlackhole)
	for range 4 * proxyBacklog {
		conn, err := net.DialTimeout("tcp", p.addr(), 100*time.Millisecond)
		if err != nil {
			return // the queue is full: this SYN was dropped
		}
		p.mu.Lock()
		p.held = append(p.held, conn)
		p.mu.Unlock()
	}
	p.t.Fatal("the proxy's accept queue never filled")
}

func (p *faultProxy) refuse() {
	p.setMode(proxyRefuse)
	_ = p.listener.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, conn := range p.held {
		_ = conn.Close()
	}
	p.held = nil
}

type fakeNode struct {
	server *miniredis.Miniredis
	proxy  *faultProxy
}

// sentinelDeployment is three fake Sentinels and two data nodes, each behind
// a faultProxy. The Sentinels are miniredis servers that also answer the two
// SENTINEL questions the client asks, from one shared notion of the primary.
type sentinelDeployment struct {
	t         *testing.T
	sentinels []*fakeNode
	data      []*fakeNode
	primary   atomic.Pointer[fakeNode]
}

const (
	testMasterName       = "mymaster"
	testDataPassword     = "placeholder-data-credential"
	testSentinelPassword = "placeholder-sentinel-credential"
)

func newSentinelDeployment(t *testing.T) *sentinelDeployment {
	t.Helper()
	deployment := &sentinelDeployment{t: t}
	for range 2 {
		node := &fakeNode{server: miniredis.RunT(t)}
		node.server.RequireAuth(testDataPassword)
		node.proxy = newFaultProxy(t, node.server.Addr())
		deployment.data = append(deployment.data, node)
	}
	deployment.primary.Store(deployment.data[0])
	for range 3 {
		node := &fakeNode{server: miniredis.RunT(t)}
		node.server.RequireAuth(testSentinelPassword)
		node.proxy = newFaultProxy(t, node.server.Addr())
		deployment.sentinels = append(deployment.sentinels, node)
	}
	for _, sentinel := range deployment.sentinels {
		require.NoError(t, sentinel.server.Server().Register("SENTINEL", deployment.answer(sentinel)))
	}
	return deployment
}

func (d *sentinelDeployment) answer(self *fakeNode) server.Cmd {
	return func(peer *server.Peer, _ string, args []string) {
		if len(args) != 2 || args[1] != testMasterName {
			peer.WriteError("ERR unknown sentinel question")
			return
		}
		switch args[0] {
		case "get-master-addr-by-name":
			host, port, _ := net.SplitHostPort(d.primary.Load().proxy.addr())
			peer.WriteStrings([]string{host, port})
		case "sentinels":
			peer.WriteLen(len(d.sentinels) - 1)
			for _, other := range d.sentinels {
				if other == self {
					continue
				}
				host, port, _ := net.SplitHostPort(other.proxy.addr())
				peer.WriteStrings([]string{"ip", host, "port", port})
			}
		default:
			peer.WriteError("ERR unknown sentinel question")
		}
	}
}

func (d *sentinelDeployment) config(poolSize int) Config {
	addresses := make([]string, 0, len(d.sentinels))
	for _, sentinel := range d.sentinels {
		addresses = append(addresses, sentinel.proxy.addr())
	}
	return Config{
		Topology:             TopologySentinel,
		Addresses:            addresses,
		MasterName:           testMasterName,
		PasswordFile:         writeTempFile(d.t, "data-pass", testDataPassword+"\n"),
		SentinelPasswordFile: writeTempFile(d.t, "sentinel-pass", testSentinelPassword+"\n"),
		PoolSize:             poolSize,
	}
}

// attached is the Sentinel the client holds its subscription on, which is the
// one it asks for the primary.
func (d *sentinelDeployment) attached() *fakeNode {
	d.t.Helper()
	var found *fakeNode
	require.Eventually(d.t, func() bool {
		for _, sentinel := range d.sentinels {
			if sentinel.server.PubSubNumSub("+switch-master")["+switch-master"] > 0 {
				found = sentinel
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "the client subscribes to the Sentinel it attached to")
	return found
}

// A router attached to a Sentinel that stops answering leaves it and follows
// the primary the others name, whatever the pool size. The Sentinel goes silent
// the two ways one does: its node is lost, so nothing completes a handshake, or
// its process hangs, so the kernel still accepts and nothing replies.
//
// The limits are in failed probes: one to leave the Sentinel, and for a node
// lost with its primary one more for the idle connection to that primary,
// which has to time out before the client dials at all.
func TestClientLeavesASilentSentinel(t *testing.T) {
	for _, tc := range []struct {
		name        string
		silence     func(*faultProxy)
		loseOldData func(*faultProxy)
		limit       time.Duration
	}{
		{"sentinel on a lost node", (*faultProxy).blackhole, (*faultProxy).refuse, 2 * failedOutageProbe},
		{"sentinel process hung", (*faultProxy).hang, (*faultProxy).refuse, 2 * failedOutageProbe},
		{"node lost with its primary", (*faultProxy).blackhole, (*faultProxy).blackhole, 3 * failedOutageProbe},
	} {
		for _, pool := range []struct {
			name string
			size int
		}{{"pool-size 20", 20}, {"default pool size", 0}} {
			t.Run(tc.name+"/"+pool.name, func(t *testing.T) {
				t.Parallel()
				deployment := newSentinelDeployment(t)
				store, err := New(deployment.config(pool.size))
				require.NoError(t, err)
				t.Cleanup(func() { _ = store.Close() })
				require.NoError(t, store.SetHeight(context.Background(), core.HeightKey("ETH1", "before"), 1, time.Hour))

				attached := deployment.attached()
				oldPrimary, newPrimary := deployment.data[0], deployment.data[1]
				deployment.primary.Store(newPrimary)
				tc.silence(attached.proxy)
				tc.loseOldData(oldPrimary.proxy)

				elapsed := recoveryTime(t, store, newPrimary.server.Exists, tc.limit)
				t.Logf("recovered in %s", elapsed.Round(10*time.Millisecond))
			})
		}
	}
}

// The three lines go-redis prints when the primary or the Sentinel changes are
// logged at info with their values as attributes. This runs the real library,
// so a release that rewords one of them fails here.
func TestClientLogBridge(t *testing.T) {
	deployment := newSentinelDeployment(t)
	cfg := deployment.config(0)
	cfg.Addresses = cfg.Addresses[:1]
	seed, primary := cfg.Addresses[0], deployment.primary.Load().proxy.addr()

	logged := captureLog(t, func() {
		store, err := New(cfg)
		require.NoError(t, err)
		defer store.Close()
		require.NoError(t, store.Ping(context.Background()))
	})

	lines := logLines(t, logged)
	attached := findLogLine(lines, "resp-cache client attached to a sentinel")
	require.NotNil(t, attached, "log: %s", logged)
	require.Equal(t, "info", attached["level"])
	require.Equal(t, seed, attached["sentinel"])
	require.Equal(t, primary, attached["primary"])

	follows := findLogLine(lines, "resp-cache client follows a new primary")
	require.NotNil(t, follows, "log: %s", logged)
	require.Equal(t, "info", follows["level"])
	require.Equal(t, testMasterName, follows["master"])
	require.Equal(t, primary, follows["address"])

	discovered := findLogLine(lines, "resp-cache client discovered a sentinel")
	require.NotNil(t, discovered, "log: %s", logged)
	require.Equal(t, "info", discovered["level"])
	require.Equal(t, testMasterName, discovered["master"])
}
