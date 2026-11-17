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
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// MAG-4718 (and MAG-3536): which failed attempts are protocol failures. Wire errors are wrapped with
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
		silent         bool // silentThroughWindow: still no answer when the attempt's window ran out
		want           bool
	}{
		{"connection refused", jsonrpc, wire(fmt.Errorf("http request failed: %w", refused)), false, notExpired, false, true},
		{"body cut off mid-read", jsonrpc, wire(fmt.Errorf("failed reading response: %w", io.ErrUnexpectedEOF)), false, notExpired, false, true},
		{"REST body cut off mid-read", rest, wire(fmt.Errorf("failed reading response: %w", io.ErrUnexpectedEOF)), false, notExpired, false, true},
		{"attempt timed out", jsonrpc, wire(fmt.Errorf("http request failed: %w", context.DeadlineExceeded)), false, notExpired, false, true},
		{"body is not valid JSON", jsonrpc, wire(errors.New("malformed JSON-RPC response: body is not valid JSON")), false, notExpired, false, true},
		{"a JSON-RPC 503 is an answer", jsonrpc, wire(&routersession.HTTPStatusError{StatusCode: 503, Status: "503"}), false, notExpired, false, false},
		{"a JSON-RPC 501 is an answer", jsonrpc, wire(&routersession.HTTPStatusError{StatusCode: 501, Status: "501"}), false, notExpired, false, false},
		{"refused before dialling", jsonrpc, fmt.Errorf("failed to build REST URL: %w", errors.New("bad path")), false, notExpired, false, false},
		{"an HTTP request the client could not build, though the sender wraps it", jsonrpc, wire(fmt.Errorf("%w: %w", routersession.ErrBuildHTTPRequest, errors.New("invalid method"))), false, notExpired, false, false},
		{"race loser or client hang-up", jsonrpc, wire(context.Canceled), true, notExpired, false, false},
		{"hang past the request budget", jsonrpc, wire(context.Canceled), true, expired, false, true},
		{"no error", jsonrpc, nil, false, notExpired, false, false},
		// The gRPC sender wraps its connection's pre-invoke refusals like wire errors, so gRPC is out.
		{"gRPC: a body the connection could not parse", grpc, grpcWire(errors.New("failed to parse gRPC request body")), false, notExpired, false, false},
		{"gRPC: even a real transport failure", grpc, grpcWire(fmt.Errorf("dial: %w", refused)), false, notExpired, false, false},
		// MAG-4718 criterion 2 (review 626-4): an attempt still silent when its own window ran out is
		// a protocol error even though a retry then answered and the race cancelled it. Cancelled
		// inside its window it stays exempt (the "race loser or client hang-up" row above).
		{"a race loser still silent when its window ran out", jsonrpc, wire(context.Canceled), true, notExpired, true, true},
		{"a timed-out attempt past its window counts as it always did", jsonrpc, wire(fmt.Errorf("http request failed: %w", context.DeadlineExceeded)), false, notExpired, true, true},
		{"past its window, a JSON-RPC 503 is still an answer", jsonrpc, wire(&routersession.HTTPStatusError{StatusCode: 503, Status: "503"}), false, notExpired, true, false},
		{"past its window, gRPC is still out", grpc, grpcWire(context.Canceled), true, notExpired, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isProtocolFailure(tc.transport, tc.err, tc.isClientCancel, tc.budgetExpired, tc.silent))
		})
	}
}
