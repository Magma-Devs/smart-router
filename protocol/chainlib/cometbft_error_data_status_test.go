package chainlib

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// CometBFT answers a GET URI request's failure with HTTP 500 and the body a POST gets inside a 200
// (rpc/jsonrpc/server/http_uri_handler.go). The direct sender turns the 500 into a status error, which
// is read here, so an internal error's data must reach this reading as it reaches the 2xx one
// (MAG-5325, for when MAG-5267 sends a client's GET as a GET).
func TestExtractNodeErrorDetails_CometBFTInternalErrorUnderA500(t *testing.T) {
	for _, tc := range []struct {
		data string
		want *common.RouterError
	}{
		{"tx already exists in cache", common.RouterErrorChainTxAlreadyKnown},
		{"height 5 is not available, lowest height is 1000", common.RouterErrorChainStatePruned},
		{"height 9999999 must be less than or equal to the current blockchain height 100", common.RouterErrorChainBlockNotFound},
		{"tx (ABCDEF) not found", common.RouterErrorChainTxNotFound},
		{"failed to load state at height 429", common.RouterErrorNodeInternalError},
	} {
		t.Run(tc.data, func(t *testing.T) {
			body := []byte(`{"jsonrpc":"2.0","id":-1,"error":{"code":-32603,"message":"Internal error","data":"` + tc.data + `"}}`)
			statusError := &routersession.HTTPStatusError{StatusCode: 500, Status: "500 Internal Server Error", Body: body}

			code, message := ExtractNodeErrorDetails(statusError)
			require.Equal(t, -32603, code)
			require.Equal(t, "HTTP 500: Internal error: "+tc.data, message)
			for _, family := range []common.ChainFamily{common.ChainFamilyCosmosSDK, common.ChainFamilyUnknown, common.ChainFamilyEVM} {
				require.Same(t, tc.want, ClassifyNodeError(statusError, family, common.TransportJsonRPC), "family %d", family)
			}
		})
	}

	t.Run("no data", func(t *testing.T) {
		statusError := &routersession.HTTPStatusError{StatusCode: 500, Body: []byte(`{"jsonrpc":"2.0","id":-1,"error":{"code":-32603,"message":"Internal error"}}`)}
		_, message := ExtractNodeErrorDetails(statusError)
		require.Equal(t, "HTTP 500: Internal error", message)
		require.Same(t, common.RouterErrorNodeInternalError, ClassifyNodeError(statusError, common.ChainFamilyUnknown, common.TransportJsonRPC))
	})
}
