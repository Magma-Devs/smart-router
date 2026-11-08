package chainlib

import (
	"net"
	"net/netip"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

func tcpPeer(ip string) net.Addr {
	return &net.TCPAddr{IP: net.ParseIP(ip), Port: 41000}
}

// TestWebsocketClientAddress pins how the per-client websocket connection limit names a client
// (MAG-3083): the connection's own address, or, behind trusted proxies, the first address read from
// the right of X-Forwarded-For that is not a trusted proxy. 203.0.113.0/24 and 198.51.100.0/24 are
// documentation ranges standing in for public clients; 173.245.48.0/20 is one of Cloudflare's.
func TestWebsocketClientAddress(t *testing.T) {
	defaults, err := ParseTrustedProxies(DefaultWebSocketTrustedProxies)
	require.NoError(t, err)
	withCDN, err := ParseTrustedProxies(DefaultWebSocketTrustedProxies + ",173.245.48.0/20")
	require.NoError(t, err)

	cases := []struct {
		name         string
		remote       net.Addr
		forwardedFor string
		trusted      []netip.Prefix
		want         string
	}{
		{name: "a client that connects directly is its own address, whatever it forwards", remote: tcpPeer("203.0.113.7"), forwardedFor: "198.51.100.1", trusted: defaults, want: "203.0.113.7"},
		{name: "behind a trusted proxy, the address the proxy appended", remote: tcpPeer("10.0.0.5"), forwardedFor: "203.0.113.7", trusted: defaults, want: "203.0.113.7"},
		{name: "what the client wrote before the proxy's entry is never read", remote: tcpPeer("10.0.0.5"), forwardedFor: "198.51.100.1, 203.0.113.7", trusted: defaults, want: "203.0.113.7"},
		{name: "a chain of trusted proxies is read through", remote: tcpPeer("10.0.0.5"), forwardedFor: "203.0.113.7, 10.0.0.9", trusted: defaults, want: "203.0.113.7"},
		{name: "a CDN that is not trusted is the client the router can see", remote: tcpPeer("10.0.0.5"), forwardedFor: "203.0.113.7, 173.245.48.10", trusted: defaults, want: "173.245.48.10"},
		{name: "a trusted CDN is read through", remote: tcpPeer("10.0.0.5"), forwardedFor: "198.51.100.1, 203.0.113.7, 173.245.48.10", trusted: withCDN, want: "203.0.113.7"},
		{name: "a trusted proxy that forwards nothing is its own client", remote: tcpPeer("10.0.0.5"), forwardedFor: "", trusted: defaults, want: "10.0.0.5"},
		{name: "an entry no proxy writes stops the walk at the last trusted hop", remote: tcpPeer("10.0.0.5"), forwardedFor: "203.0.113.7, unknown", trusted: defaults, want: "10.0.0.5"},
		{name: "every hop trusted: the leftmost", remote: tcpPeer("127.0.0.1"), forwardedFor: "10.0.0.9", trusted: defaults, want: "10.0.0.9"},
		{name: "no trusted proxy: the connection's own address", remote: tcpPeer("10.0.0.5"), forwardedFor: "203.0.113.7", trusted: nil, want: "10.0.0.5"},
		{name: "a port and an IPv4-mapped address name the same client", remote: tcpPeer("::ffff:10.0.0.5"), forwardedFor: "203.0.113.7:5555", trusted: defaults, want: "203.0.113.7"},
		{name: "IPv6", remote: tcpPeer("fd00::1"), forwardedFor: "[2001:db8::7]:443", trusted: defaults, want: "2001:db8::7"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, websocketClientAddress(tc.remote, tc.forwardedFor, tc.trusted))
		})
	}
}

// TestWebsocketConnectionLimiter_AClientCannotRaiseItsOwnLimit pins MAG-3083 through
// CanOpenConnection, with the default trusted proxies: the limit was counted per address,
// X-Forwarded-For and User-Agent, so a client that changed either header got a fresh allowance.
func TestWebsocketConnectionLimiter_AClientCannotRaiseItsOwnLimit(t *testing.T) {
	previousLimit := MaximumNumberOfParallelWebsocketConnectionsPerIp
	MaximumNumberOfParallelWebsocketConnectionsPerIp = 2
	t.Cleanup(func() { MaximumNumberOfParallelWebsocketConnectionsPerIp = previousLimit })

	ctrl := gomock.NewController(t)
	open := func(wcl *WebsocketConnectionLimiter, remote, forwardedFor, userAgent string) bool {
		t.Helper()
		conn := NewMockWebsocketConnection(ctrl)
		conn.EXPECT().Locals(WebSocketOpenConnectionsLimitHeader).Return(int64(0)).AnyTimes()
		conn.EXPECT().Locals(common.IP_FORWARDING_HEADER_NAME).Return(forwardedFor).AnyTimes()
		conn.EXPECT().Locals("User-Agent").Return(userAgent).AnyTimes()
		conn.EXPECT().RemoteAddr().Return(tcpPeer(remote)).AnyTimes()
		conn.EXPECT().WriteMessage(gomock.Any(), gomock.Any()).AnyTimes()
		allowed, _ := wcl.CanOpenConnection(conn)
		return allowed
	}
	newLimiter := func() *WebsocketConnectionLimiter {
		return &WebsocketConnectionLimiter{ipToNumberOfActiveConnections: map[string]int64{}}
	}

	t.Run("a new User-Agent is the same client", func(t *testing.T) {
		wcl := newLimiter()
		got := []bool{
			open(wcl, "203.0.113.7", "", "agent-a"),
			open(wcl, "203.0.113.7", "", "agent-b"),
			open(wcl, "203.0.113.7", "", "agent-c"),
		}
		require.Equal(t, []bool{true, true, false}, got)
	})

	t.Run("X-Forwarded-For from a client that is not a trusted proxy is not read", func(t *testing.T) {
		wcl := newLimiter()
		got := []bool{
			open(wcl, "203.0.113.7", "198.51.100.1", "agent"),
			open(wcl, "203.0.113.7", "198.51.100.2", "agent"),
			open(wcl, "203.0.113.7", "198.51.100.3", "agent"),
		}
		require.Equal(t, []bool{true, true, false}, got)
	})

	t.Run("clients behind a trusted proxy each have their own allowance", func(t *testing.T) {
		wcl := newLimiter()
		got := []bool{
			open(wcl, "10.0.0.5", "203.0.113.20", "agent"),
			open(wcl, "10.0.0.5", "203.0.113.20", "agent"),
			open(wcl, "10.0.0.5", "203.0.113.21", "agent"),
			open(wcl, "10.0.0.5", "203.0.113.20", "agent"),
		}
		require.Equal(t, []bool{true, true, true, false}, got)
	})

	t.Run("a client cannot spend another client's allowance by claiming its address", func(t *testing.T) {
		wcl := newLimiter()
		got := []bool{
			open(wcl, "198.51.100.66", "203.0.113.30", "agent"),
			open(wcl, "198.51.100.66", "203.0.113.30", "agent"),
			open(wcl, "10.0.0.5", "203.0.113.30", "agent"),
		}
		require.Equal(t, []bool{true, true, true}, got, "the claimed client's own connection through the proxy is still allowed")
	})
}

func TestParseTrustedProxies(t *testing.T) {
	defaults, err := ParseTrustedProxies(DefaultWebSocketTrustedProxies)
	require.NoError(t, err)
	require.Len(t, defaults, 6)

	parsed, err := ParseTrustedProxies(" 10.1.2.3 , 2001:db8::1,::ffff:10.0.0.1, 192.168.7.9/16 ,")
	require.NoError(t, err)
	require.Equal(t, []netip.Prefix{
		netip.MustParsePrefix("10.1.2.3/32"),
		netip.MustParsePrefix("2001:db8::1/128"),
		netip.MustParsePrefix("10.0.0.1/32"),
		netip.MustParsePrefix("192.168.0.0/16"),
	}, parsed)

	none, err := ParseTrustedProxies("")
	require.NoError(t, err)
	require.Empty(t, none, "an empty list trusts no proxy")

	for _, invalid := range []string{"not-an-address", "10.0.0.0/33", "10.0.0.1:80"} {
		_, err := ParseTrustedProxies(invalid)
		require.Error(t, err, invalid)
	}
}
