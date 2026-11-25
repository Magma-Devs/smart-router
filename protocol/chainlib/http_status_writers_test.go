package chainlib

import (
	"errors"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MAG-5324. The HTTP status rows read a status only where the router writes one. Each error here
// is built by the code that writes it, so a writer whose text the rows stop reading fails here.
func TestHTTPStatusRows_ReadEveryStatusTheRouterWrites(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		transport common.TransportType
		want      *common.RouterError
	}{
		{
			"routersession.HTTPStatusError", &routersession.HTTPStatusError{StatusCode: 503, Status: "503 Service Temporarily Unavailable"},
			common.TransportJsonRPC, common.RouterErrorNodeServiceUnavailable,
		},
		{
			"routersession.HTTPStatusError with a JSON-RPC body", &routersession.HTTPStatusError{StatusCode: 503, Body: []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"boom"}}`)},
			common.TransportJsonRPC, common.RouterErrorNodeServiceUnavailable,
		},
		{
			"rpcclient.HTTPError",
			rpcclient.HTTPError{StatusCode: 503, Status: "503 Service Temporarily Unavailable", Body: []byte("<html>busy</html>")},
			common.TransportJsonRPC, common.RouterErrorNodeServiceUnavailable,
		},
		{
			"grpc-go's status for a gateway's reply", status.Error(codes.Unknown, "unexpected HTTP status code received from server: 500 (Internal Server Error)"),
			common.TransportGRPC, common.RouterErrorNodeInternalError,
		},
		{
			"common.RateLimited", common.RateLimited(errors.New("reply error"), 0),
			common.TransportJsonRPC, common.RouterErrorNodeRateLimited,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Same(t, tc.want, ClassifyNodeError(tc.err, common.ChainFamilyUnknown, tc.transport), tc.err.Error())
		})
	}
}

// The same rows on a node's own gRPC status, whose text carries a balance: no status there.
func TestHTTPStatusRows_ANodesGRPCTextIsNotAStatus(t *testing.T) {
	err := status.Error(codes.Unknown, "spendable balance 100uatom is smaller than 500uatom: insufficient funds")
	got := ClassifyNodeError(err, common.ChainFamilyCosmosSDK, common.TransportGRPC)
	require.NotSame(t, common.RouterErrorNodeInternalError, got)
	require.False(t, got.EndpointAtFault())
}
