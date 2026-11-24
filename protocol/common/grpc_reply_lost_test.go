package common

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRedactSocketAddresses(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"read tcp 10.42.7.9:51234->34.160.1.1:443: read: connection reset by peer", "read tcp [address]->[address]: read: connection reset by peer"},
		{"read tcp [fd00::7]:51234->[2600:1901::1]:443: read: connection reset by peer", "read tcp [address]->[address]: read: connection reset by peer"},
		{"dial tcp 127.0.0.1: connect", "dial tcp [address]: connect"},
		{"error reading from server: EOF", "error reading from server: EOF"},
		{"stream terminated by RST_STREAM with error code: INTERNAL_ERROR", "stream terminated by RST_STREAM with error code: INTERNAL_ERROR"},
		{"block 1.2.3 of version 10.0", "block 1.2.3 of version 10.0"},
	} {
		require.Equal(t, tc.want, RedactSocketAddresses(tc.in), tc.in)
	}
}

// MAG-5130: the wrapper keeps grpc-go's code, drops the addresses from its text, and is what the
// classifier reads as a connection fault on a request that may have reached the node.
func TestGRPCReplyLostError(t *testing.T) {
	err := error(NewGRPCReplyLostError(status.Error(codes.Unavailable, "error reading from server: read tcp 10.42.7.9:51234->34.160.1.1:443: read: connection reset by peer")))

	require.Equal(t, codes.Unavailable, status.Code(err))
	require.NotContains(t, err.Error(), "10.42.7.9")
	require.NotContains(t, err.Error(), "34.160.1.1")
	require.NotContains(t, err.Error(), "51234")
	require.Contains(t, err.Error(), "connection reset by peer")
	var lost *GRPCReplyLostError
	require.True(t, errors.As(err, &lost))

	require.Equal(t, RouterErrorConnectionReset, DetectConnectionError(err))
	require.Equal(t, RouterErrorConnectionClosed, DetectConnectionError(NewGRPCReplyLostError(status.Error(codes.Unavailable, "error reading from server: EOF"))))
	require.False(t, IsClientCancellation(err, nil), "the endpoint is not excused")
}
