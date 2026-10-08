package rpcsmartrouter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// MAG-3536: which failed attempts are protocol failures. Wire errors are wrapped with
// classifyAndWrap exactly as the senders wrap them, so each case is a shape relayInnerDirect
// actually receives, not a synthetic stand-in.
func TestIsProtocolFailure(t *testing.T) {
	_, refused := net.DialTimeout("tcp", "127.0.0.1:1", time.Second)
	require.Error(t, refused, "port 1 must refuse the dial for the first case to mean anything")
	wire := func(err error) error { return classifyAndWrap(err, common.ChainFamilyEVM, common.TransportJsonRPC) }
	grpcWire := func(err error) error { return classifyAndWrap(err, common.ChainFamilyEVM, common.TransportGRPC) }
	jsonrpc, rest, grpc := common.TransportJsonRPC, common.TransportREST, common.TransportGRPC
	notExpired := func() bool { return false }
	expired := func() bool { return true }

	for _, tc := range []struct {
		name           string
		transport      common.TransportType
		err            error
		isClientCancel bool
		budgetExpired  func() bool
		want           bool
	}{
		{"connection refused", jsonrpc, wire(fmt.Errorf("http request failed: %w", refused)), false, notExpired, true},
		{"body cut off mid-read", jsonrpc, wire(fmt.Errorf("failed reading response: %w", io.ErrUnexpectedEOF)), false, notExpired, true},
		{"REST body cut off mid-read", rest, wire(fmt.Errorf("failed reading response: %w", io.ErrUnexpectedEOF)), false, notExpired, true},
		{"attempt timed out", jsonrpc, wire(fmt.Errorf("http request failed: %w", context.DeadlineExceeded)), false, notExpired, true},
		{"body is not valid JSON", jsonrpc, wire(errors.New("malformed JSON-RPC response: body is not valid JSON")), false, notExpired, true},
		{"a JSON-RPC 503 is an answer", jsonrpc, wire(&lavasession.HTTPStatusError{StatusCode: 503, Status: "503"}), false, notExpired, false},
		{"a JSON-RPC 501 is an answer", jsonrpc, wire(&lavasession.HTTPStatusError{StatusCode: 501, Status: "501"}), false, notExpired, false},
		{"refused before dialling", jsonrpc, fmt.Errorf("failed to build REST URL: %w", errors.New("bad path")), false, notExpired, false},
		{"an HTTP request the client could not build, though the sender wraps it", jsonrpc, wire(fmt.Errorf("%w: %w", lavasession.ErrBuildHTTPRequest, errors.New("invalid method"))), false, notExpired, false},
		{"race loser or client hang-up", jsonrpc, wire(context.Canceled), true, notExpired, false},
		{"hang past the request budget", jsonrpc, wire(context.Canceled), true, expired, true},
		{"no error", jsonrpc, nil, false, notExpired, false},
		// The gRPC sender wraps its connection's pre-invoke refusals like wire errors, so gRPC is out.
		{"gRPC: a body the connection could not parse", grpc, grpcWire(errors.New("failed to parse gRPC request body")), false, notExpired, false},
		{"gRPC: even a real transport failure", grpc, grpcWire(fmt.Errorf("dial: %w", refused)), false, notExpired, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isProtocolFailure(tc.transport, tc.err, tc.isClientCancel, tc.budgetExpired))
		})
	}
}
