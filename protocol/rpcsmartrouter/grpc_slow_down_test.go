package rpcsmartrouter

import (
	"context"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// MAG-5130: a server asking the router to slow down is a rate limit, as before: the endpoint is
// held off, not counted as unreachable. grpc-go words it two ways. A stream reset with
// ENHANCE_YOUR_CALM, and, for a call still in flight when the server sent a GOAWAY with that code
// and closed the connection, an UNAVAILABLE naming the GOAWAY. The second must not be read as a cut.

// slowDownNode is an HTTP/2 node, framed by hand, that reads each call and then asks the router to
// slow down: with a stream reset, or with a GOAWAY and then a closed connection. Reflection streams
// are left unanswered.
type slowDownNode struct {
	goAway bool
	calls  atomic.Int32
}

func startSlowDownNode(t *testing.T, goAway bool) (string, *slowDownNode) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })
	node := &slowDownNode{goAway: goAway}
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go node.serve(conn)
		}
	}()
	return lis.Addr().String(), node
}

func (n *slowDownNode) serve(conn net.Conn) {
	defer conn.Close()
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(conn, preface); err != nil {
		return
	}
	framer := http2.NewFramer(conn, conn)
	framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	if err := framer.WriteSettings(); err != nil {
		return
	}
	paths := map[uint32]string{}
	for {
		frame, err := framer.ReadFrame()
		if err != nil {
			return
		}
		switch frame := frame.(type) {
		case *http2.SettingsFrame:
			if !frame.IsAck() {
				_ = framer.WriteSettingsAck()
			}
		case *http2.PingFrame:
			if !frame.IsAck() {
				_ = framer.WritePing(true, frame.Data)
			}
		case *http2.MetaHeadersFrame:
			paths[frame.StreamID] = frame.PseudoValue("path")
		case *http2.DataFrame:
			if !frame.StreamEnded() || strings.Contains(paths[frame.StreamID], "ServerReflection") {
				continue
			}
			n.calls.Add(1)
			if !n.goAway {
				_ = framer.WriteRSTStream(frame.StreamID, http2.ErrCodeEnhanceYourCalm)
				continue
			}
			// The call is at or below the GOAWAY's last stream id, so grpc-go keeps it open
			// until the connection closes under it.
			_ = framer.WriteGoAway(frame.StreamID, http2.ErrCodeEnhanceYourCalm, []byte("too_many_pings"))
			time.Sleep(50 * time.Millisecond)
			return
		}
	}
}

var slowDownForms = []struct {
	name   string
	goAway bool
}{
	{"a stream reset with ENHANCE_YOUR_CALM", false},
	{"a GOAWAY with ENHANCE_YOUR_CALM, then a close", true},
}

// The relay reads both forms as the node's rate-limit answer, and a call to the endpoint does not
// count toward switching it off.
func TestSendGRPCRelay_SlowDownIsARateLimitNotACut(t *testing.T) {
	for _, tc := range slowDownForms {
		t.Run(tc.name, func(t *testing.T) {
			addr, node := startSlowDownNode(t, tc.goAway)
			result, err := sendGRPCRelayTo(t, addr, 5*time.Second)

			require.NoError(t, err, "the node's answer, not a connection fault")
			require.NotNil(t, result)
			require.True(t, result.IsNodeError)
			require.True(t, result.IsRateLimited, "healthy but busy: held off, not scored")
			require.Equal(t, int32(1), node.calls.Load())
		})
	}
}

func TestRelayInnerDirect_SlowDownIsNotCountedAsUnreachable(t *testing.T) {
	for _, tc := range slowDownForms {
		t.Run(tc.name, func(t *testing.T) {
			benchAfter(t, 1)
			addr, _ := startSlowDownNode(t, tc.goAway)
			nodeUrl := common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true, ReflectionTimeout: time.Second}}
			directConn, err := routersession.NewDirectRPCConnection(context.Background(), nodeUrl, 5, "")
			require.NoError(t, err)
			t.Cleanup(func() { _ = directConn.Close() })
			endpoint := &routersession.Endpoint{NetworkAddress: nodeUrl.Url, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{directConn}}
			h := &directRelayHarness{
				rpcss: &RPCSmartRouterServer{listenEndpoint: &routersession.RPCEndpoint{ChainID: "GRPCTEST", ApiInterface: "grpc"}},
				session: &routersession.SingleConsumerSession{
					Parent:     &routersession.ConsumerSessionsWithProvider{PublicAddress: "grpc-node-under-test", PairingEpoch: 100, Endpoints: []*routersession.Endpoint{endpoint}},
					Connection: &routersession.DirectRPCSessionConnection{DirectConnection: directConn, EndpointAddress: nodeUrl.Url, Endpoint: endpoint},
				},
				endpoint: endpoint,
			}
			const method = "test.v1.Node/Call"
			msg := &mockChainMessage{
				apiInterface: "grpc",
				api:          &spectypes.Api{Name: method},
				rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: method, Msg: []byte{0x08, 0x05}},
			}
			_, _ = h.rpcss.relayInnerDirect(context.Background(), h.session, &common.RelayResult{}, 5*time.Second, 30*time.Second,
				msg, msg.requestData, func() bool { return false }, nil, false)

			require.Zero(t, h.endpoint.ConnectionRefusals, "a request to slow down is not a refused connection")
			require.True(t, h.endpoint.IsEnabled(), "still on at --bench-after 1")
		})
	}
}

// A write asked to slow down by every provider is still sent once to each.
func TestGRPCListener_SlowDownWriteIsSentOncePerProvider(t *testing.T) {
	for _, tc := range slowDownForms {
		t.Run(tc.name, func(t *testing.T) {
			addrA, nodeA := startSlowDownNode(t, tc.goAway)
			addrB, nodeB := startSlowDownNode(t, tc.goAway)
			client := startGRPCRouterOverNodes(t, replyLostAPI(common.CONSISTENCY_SELECT_ALL_PROVIDERS), grpcReplyLostNode{"node-a", addrA}, grpcReplyLostNode{"node-b", addrB})

			_, _, err := callRouter(t, client, "")
			require.Error(t, err)
			time.Sleep(200 * time.Millisecond)
			require.Equal(t, int32(1), nodeA.calls.Load(), "one copy to each provider")
			require.Equal(t, int32(1), nodeB.calls.Load(), "one copy to each provider")
		})
	}
}
