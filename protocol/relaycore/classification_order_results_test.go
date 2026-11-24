package relaycore

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// restProtocolMessage is a REST read (a Cosmos balance query) as the results manager sees it.
func restProtocolMessage(t *testing.T) chainlib.ProtocolMessage {
	t.Helper()
	ctx := context.Background()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	chainMsg, err := chainParser.ParseMsg("/cosmos/bank/v1beta1/balances/lava@1x", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	return chainlib.NewProtocolMessage(chainMsg, nil, nil, "", "")
}

// The results manager runs the prefix (PRD §3.2 steps 1 to 3; MAG-5597) on every node error ahead of
// the rows, and leaves the rest to the rows with the inputs they read today.
func TestResultsManagerRunsThePrefixAheadOfTheRows(t *testing.T) {
	fileNodeError := func(t *testing.T, protocolMessage chainlib.ProtocolMessage, status int, body string) *common.RouterError {
		t.Helper()
		rm := NewResultsManager(1, "LAVA")
		nodeErr := rm.SetResponse(&RelayResponse{
			RelayResult: common.RelayResult{
				Reply:       &pairingtypes.RelayReply{Data: []byte(body)},
				StatusCode:  status,
				IsNodeError: true,
				Finalized:   true,
			},
		}, protocolMessage)
		require.Error(t, nodeErr)
		inst, ok := rm.(*ResultsManagerInst)
		require.True(t, ok)
		require.Len(t, inst.nodeResponseErrors.RelayErrors, 1)
		return inst.nodeResponseErrors.RelayErrors[0].RouterError
	}

	t.Run("a 429 is a rate limit whatever its body says", func(t *testing.T) {
		classified := fileNodeError(t, restProtocolMessage(t), http.StatusTooManyRequests, `{"message":"route not found"}`)
		require.Equal(t, common.RouterErrorNodeRateLimited.Name, classified.Name)
		require.Equal(t, common.RouterErrorNodeEndpointNotFound.Name, common.ClassifyError(nil, common.ChainFamilyUnknown, common.TransportREST, http.StatusTooManyRequests, "route not found").Name,
			"the rows alone read the text first; the prefix decided")
	})
	t.Run("a connection fault in the reply's text is no reply", func(t *testing.T) {
		classified := fileNodeError(t, restProtocolMessage(t), http.StatusBadGateway, `connection reset by peer`)
		require.Equal(t, common.RouterErrorConnectionReset.Name, classified.Name)
	})
	t.Run("a gateway's 429 inside a gRPC status reaches the prefix as a 429 (D12)", func(t *testing.T) {
		body := `{"error_message":"unexpected HTTP status code received from server: 429 (Too Many Requests); transport: received unexpected content-type \"text/html\"","error_code":14}`
		classified := fileNodeError(t, grpcProtocolMessage(), int(codes.Unavailable), body)
		require.Equal(t, common.RouterErrorNodeRateLimited.Name, classified.Name)
	})
	t.Run("any other status is left to the rows, with today's inputs", func(t *testing.T) {
		classified := fileNodeError(t, restProtocolMessage(t), http.StatusBadRequest, `{"message":"route not found"}`)
		require.Equal(t, common.RouterErrorNodeEndpointNotFound.Name, classified.Name, "the text row, ahead of the 400 code row, as on main")
		classified = fileNodeError(t, restProtocolMessage(t), http.StatusForbidden, `{"message":"forbidden"}`)
		require.Equal(t, common.RouterErrorNodeAccessDenied.Name, classified.Name, "the 403 code row (MAG-5254 item 3 makes it the status's)")
	})
}
