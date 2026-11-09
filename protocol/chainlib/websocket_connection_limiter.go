package chainlib

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/magma-Devs/smart-router/protocol/common"
)

// WebsocketConnection defines the interface for websocket connections
type WebsocketConnection interface {
	// Add only the methods you need to mock
	RemoteAddr() net.Addr
	Locals(key string) interface{}
	WriteMessage(messageType int, data []byte) error
}

// Will limit a certain amount of connections per IP
type WebsocketConnectionLimiter struct {
	ipToNumberOfActiveConnections map[string]int64
	lock                          sync.RWMutex
}

func (wcl *WebsocketConnectionLimiter) HandleFiberRateLimitFlags(c *fiber.Ctx) {
	// X-Forwarded-For as it arrived, "" when absent. CanOpenConnection reads it only through the
	// trusted proxies (websocketClientAddress).
	c.Locals(common.IP_FORWARDING_HEADER_NAME, forwardedForList(c))

	rateLimitString := c.Get(WebSocketRateLimitHeader)
	rateLimit, err := strconv.ParseInt(rateLimitString, 10, 64)
	if err != nil {
		rateLimit = 0
	}
	c.Locals(WebSocketRateLimitHeader, rateLimit)

	connectionLimitString := c.Get(WebSocketOpenConnectionsLimitHeader)
	connectionLimit, err := strconv.ParseInt(connectionLimitString, 10, 64)
	if err != nil {
		connectionLimit = 0
	}
	c.Locals(WebSocketOpenConnectionsLimitHeader, connectionLimit)
}

// forwardedForList is the request's X-Forwarded-For list: every line of the header, in order,
// joined with commas, "" when there is none. A list sent as several lines is one list (RFC 9110,
// section 5.3), and a proxy may add its own line instead of appending to the one it received, so
// what it wrote can be in the last line; c.Get returns the first line only (MAG-3083). The lines
// are views into fasthttp's reusable request buffer, and the list is read later, in the websocket
// handler, so they are copied (MAG-3881).
func forwardedForList(c *fiber.Ctx) string {
	lines := c.Request().Header.PeekAll(common.IP_FORWARDING_HEADER_NAME)
	entries := make([]string, len(lines))
	for i, line := range lines {
		entries[i] = string(line)
	}
	return strings.Join(entries, ",")
}

func (wcl *WebsocketConnectionLimiter) getConnectionLimit(websocketConn WebsocketConnection) int64 {
	connectionLimitHeaderValue, ok := websocketConn.Locals(WebSocketOpenConnectionsLimitHeader).(int64)
	if !ok || connectionLimitHeaderValue < 0 {
		connectionLimitHeaderValue = 0
	}
	// Do not allow header to overwrite flag value if its set.
	if MaximumNumberOfParallelWebsocketConnectionsPerIp > 0 && connectionLimitHeaderValue > MaximumNumberOfParallelWebsocketConnectionsPerIp {
		return MaximumNumberOfParallelWebsocketConnectionsPerIp
	}
	// Return the larger of the global limit (if set) or the header value
	return max(MaximumNumberOfParallelWebsocketConnectionsPerIp, connectionLimitHeaderValue)
}

func (wcl *WebsocketConnectionLimiter) CanOpenConnection(websocketConn WebsocketConnection) (bool, func()) {
	// Check which connection limit is higher and use that.
	connectionLimit := wcl.getConnectionLimit(websocketConn)
	decreaseIpConnectionCallback := func() {}
	if connectionLimit > 0 { // 0 is disabled.
		forwardedFor, _ := websocketConn.Locals(common.IP_FORWARDING_HEADER_NAME).(string)
		key := websocketClientAddress(websocketConn.RemoteAddr(), forwardedFor, GetWebSocketTrustedProxies())

		// Check current connections before incrementing
		currentConnections := wcl.getCurrentAmountOfConnections(key)
		// If already at or exceeding limit, deny the connection
		if currentConnections >= connectionLimit {
			websocketConn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, fmt.Sprintf("Too Many Open Connections, limited to %d", connectionLimit)))
			return false, decreaseIpConnectionCallback
		}
		// If under limit, increment and return cleanup function
		wcl.addIpConnection(key)
		decreaseIpConnectionCallback = func() { wcl.decreaseIpConnection(key) }
	}
	return true, decreaseIpConnectionCallback
}

func (wcl *WebsocketConnectionLimiter) getCurrentAmountOfConnections(key string) int64 {
	wcl.lock.RLock()
	defer wcl.lock.RUnlock()
	return wcl.ipToNumberOfActiveConnections[key]
}

func (wcl *WebsocketConnectionLimiter) addIpConnection(key string) {
	wcl.lock.Lock()
	defer wcl.lock.Unlock()
	// wether it exists or not we add 1.
	wcl.ipToNumberOfActiveConnections[key] += 1
}

func (wcl *WebsocketConnectionLimiter) decreaseIpConnection(key string) {
	wcl.lock.Lock()
	defer wcl.lock.Unlock()
	// it must exist as we dont get here without adding it prior
	wcl.ipToNumberOfActiveConnections[key] -= 1
	if wcl.ipToNumberOfActiveConnections[key] == 0 {
		delete(wcl.ipToNumberOfActiveConnections, key)
	}
}

// DefaultWebSocketTrustedProxies are the proxies the per-client websocket connection limit trusts to
// name the client in X-Forwarded-For when --websocket-trusted-proxies is not set: loopback and the
// private ranges, where a gateway in the router's own cluster lives. A CDN in front of that gateway
// has public addresses, which the operator adds to the list. Only a proxy that appends to
// X-Forwarded-For the address it received the request from belongs in the list.
const DefaultWebSocketTrustedProxies = "127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,::1/128,fc00::/7"

var webSocketTrustedProxies = func() *atomic.Pointer[[]netip.Prefix] {
	prefixes, err := ParseTrustedProxies(DefaultWebSocketTrustedProxies)
	if err != nil {
		panic(err) // a constant; the test of ParseTrustedProxies reads it
	}
	trusted := &atomic.Pointer[[]netip.Prefix]{}
	trusted.Store(&prefixes)
	return trusted
}()

// SetWebSocketTrustedProxies sets the proxies websocketClientAddress reads X-Forwarded-For through.
func SetWebSocketTrustedProxies(prefixes []netip.Prefix) {
	webSocketTrustedProxies.Store(&prefixes)
}

func GetWebSocketTrustedProxies() []netip.Prefix {
	return *webSocketTrustedProxies.Load()
}

// ParseTrustedProxies reads a comma-separated list of IP addresses and CIDR ranges. An empty list
// trusts no proxy. An IPv4-mapped IPv6 address or range names its IPv4 address or range, as the
// addresses it is matched against do: they are unmapped, and an IPv6 range never contains an IPv4
// address.
func ParseTrustedProxies(value string) ([]netip.Prefix, error) {
	prefixes := []netip.Prefix{}
	for _, entry := range strings.Split(value, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if prefix, err := netip.ParsePrefix(entry); err == nil {
			if prefix.Addr().Is4In6() && prefix.Bits() >= 96 {
				prefix = netip.PrefixFrom(prefix.Addr().Unmap(), prefix.Bits()-96)
			}
			prefixes = append(prefixes, prefix.Masked())
			continue
		}
		addr, err := netip.ParseAddr(entry)
		if err != nil {
			return nil, fmt.Errorf("trusted proxy %q is neither an IP address nor a CIDR range", entry)
		}
		addr = addr.Unmap()
		prefixes = append(prefixes, netip.PrefixFrom(addr, addr.BitLen()))
	}
	return prefixes, nil
}

// websocketClientAddress is the address the per-client connection limit counts a connection
// against (MAG-3083). It is the address the connection came from unless that is a trusted proxy;
// then X-Forwarded-For is read from the right, through each trusted proxy, to the first address
// that is not one. Each proxy appends the address it received the request from, so every entry up
// to there was written by a proxy the router trusts, and every entry to the left of it is the
// client's own claim, which is never read. An entry that is not an address stops the walk at the
// last trusted hop. Nothing else the client sends, its User-Agent included, is part of the key:
// a client that could change the key could open as many connections as it liked, and could fill
// another client's allowance by claiming its address.
func websocketClientAddress(remote net.Addr, forwardedFor string, trusted []netip.Prefix) string {
	client, ok := remoteAddress(remote)
	if !ok {
		if remote == nil {
			return ""
		}
		return remote.String()
	}
	hops := strings.Split(forwardedFor, ",")
	for i := len(hops) - 1; i >= 0 && isTrustedProxy(client, trusted); i-- {
		hop, ok := forwardedAddress(hops[i])
		if !ok {
			break
		}
		client = hop
	}
	return client.String()
}

func isTrustedProxy(addr netip.Addr, trusted []netip.Prefix) bool {
	for _, prefix := range trusted {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

func remoteAddress(remote net.Addr) (netip.Addr, bool) {
	if remote == nil {
		return netip.Addr{}, false
	}
	if tcp, ok := remote.(*net.TCPAddr); ok {
		addr, ok := netip.AddrFromSlice(tcp.IP)
		return addr.Unmap(), ok
	}
	return forwardedAddress(remote.String())
}

// forwardedAddress reads one X-Forwarded-For entry: an address, with or without a port.
func forwardedAddress(entry string) (netip.Addr, bool) {
	entry = strings.TrimSpace(entry)
	if addr, err := netip.ParseAddr(entry); err == nil {
		return addr.Unmap(), true
	}
	if addrPort, err := netip.ParseAddrPort(entry); err == nil {
		return addrPort.Addr().Unmap(), true
	}
	return netip.Addr{}, false
}
