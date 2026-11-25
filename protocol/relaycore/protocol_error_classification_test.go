package relaycore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestSetErrorResponse_ProtocolErrorClassification pins how a protocol error (no reply from the
// node) is classified: by connection shape and message text, with no chain family and the gRPC
// transport's matchers. These are the verdicts ClassifyLegacyError's fall-through produced before
// MAG-4243 inlined it into setErrorResponse; its tests were deleted with it, so this is what keeps
// them from moving. Classifying with the JSON-RPC matchers would turn "nonce too low" into a chain
// error and "not implemented" into UNKNOWN; classifying with the chain's family would turn a
// Solana "blockhash not found" into a chain error.
func TestSetErrorResponse_ProtocolErrorClassification(t *testing.T) {
	cases := []struct {
		name    string
		chainID string
		err     error
		want    *common.RouterError
	}{
		{"gRPC-only text matcher", "ETH1", errors.New("not implemented"), common.RouterErrorNodeUnimplemented},
		{"JSON-RPC-only text matcher", "ETH1", errors.New("nonce too low"), common.RouterErrorUnknown},
		{"no chain family", "SOLANA", errors.New("blockhash not found"), common.RouterErrorUnknown},
		{"HTTP status text", "ETH1", errors.New("HTTP status 503: Service Unavailable"), common.RouterErrorNodeServiceUnavailable},
		{"context deadline", "ETH1", context.DeadlineExceeded, common.RouterErrorContextDeadline},
		{"EOF", "ETH1", fmt.Errorf("post: %w", io.EOF), common.RouterErrorConnectionClosed},
		{"DNS", "ETH1", fmt.Errorf("dial: %w", &net.DNSError{Err: "no such host", Name: "node.invalid", IsNotFound: true}), common.RouterErrorDNSFailure},
		{"session error", "ETH1", routersession.PairingListEmptyError, common.RouterErrorUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rm := NewResultsManager(1, tc.chainID)
			require.NoError(t, rm.SetResponse(&RelayResponse{Err: tc.err}, nil))
			_, _, protocolErrors := rm.GetResultsData()
			require.Len(t, protocolErrors, 1)
			require.Same(t, tc.want, protocolErrors[0].RouterError, "got %v", protocolErrors[0].RouterError)
		})
	}
}

// interfaceChainMessage is restReplyChainMessage on the api interface it names.
type interfaceChainMessage struct {
	restReplyChainMessage
	apiInterface string
}

func (m interfaceChainMessage) GetApiCollection() *spectypes.ApiCollection {
	return &spectypes.ApiCollection{CollectionData: spectypes.CollectionData{ApiInterface: m.apiInterface}}
}

// A REST attempt that failed on its upstream's status is recorded by that status on the REST table
// when the text rows name none (MAG-5490). The text rows name only the statuses httpStatusRows lists,
// so a 5xx no status row names was UNKNOWN_ERROR in the record (the coded log line, the error metric,
// relay_end) while the sender filed it as NODE_UNLISTED_SERVER_ERROR. Every other verdict stays: a
// status the text rows name, a status neither table names, a text that only reads like a status, a
// connection error, and every interface but REST.
func TestSetErrorResponse_RESTUpstreamStatusIsReadByItsStatus_MAG5490(t *testing.T) {
	status := func(code int) error { return &routersession.HTTPStatusError{StatusCode: code} }
	nginx := []byte(`<html><head><title>507 Insufficient Storage</title></head><body><center><h1>507 Insufficient Storage</h1></center><hr><center>nginx</center></body></html>`)
	cases := []struct {
		name         string
		apiInterface string
		err          error
		want         *common.RouterError
	}{
		{"REST 507", spectypes.APIInterfaceRest, status(507), common.RouterErrorNodeUnlistedServerError},
		{"REST 507 with a proxy's page", spectypes.APIInterfaceRest, &routersession.HTTPStatusError{StatusCode: 507, Body: nginx, ContentType: "text/html"}, common.RouterErrorNodeUnlistedServerError},
		{"REST 529", spectypes.APIInterfaceRest, status(529), common.RouterErrorNodeUnlistedServerError},
		{"REST 599", spectypes.APIInterfaceRest, status(599), common.RouterErrorNodeUnlistedServerError},
		{"REST 507 wrapped", spectypes.APIInterfaceRest, fmt.Errorf("relay failed: %w", status(507)), common.RouterErrorNodeUnlistedServerError},

		{"REST 503 keeps its text row", spectypes.APIInterfaceRest, status(503), common.RouterErrorNodeServiceUnavailable},
		{"REST 502 keeps its text row", spectypes.APIInterfaceRest, status(502), common.RouterErrorNodeBadGateway},
		{"REST 429 keeps its text row", spectypes.APIInterfaceRest, status(429), common.RouterErrorNodeRateLimited},
		{"REST 501 keeps its text row", spectypes.APIInterfaceRest, status(501), common.RouterErrorNodeUnimplemented},
		{"REST 600 has no row on either table", spectypes.APIInterfaceRest, status(600), common.RouterErrorUnknown},
		{"REST text that only reads like a status", spectypes.APIInterfaceRest, errors.New("HTTP 507 Insufficient Storage"), common.RouterErrorUnknown},
		{"REST connection closed", spectypes.APIInterfaceRest, fmt.Errorf("post: %w", io.EOF), common.RouterErrorConnectionClosed},
		{"JSON-RPC 507", spectypes.APIInterfaceJsonRPC, status(507), common.RouterErrorUnknown},
		{"Tendermint RPC 507", spectypes.APIInterfaceTendermintRPC, status(507), common.RouterErrorUnknown},
		{"gRPC 507", spectypes.APIInterfaceGrpc, status(507), common.RouterErrorUnknown},
		{"GraphQL 507", spectypes.APIInterfaceGraphQL, status(507), common.RouterErrorUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			message := chainlib.NewProtocolMessage(interfaceChainMessage{apiInterface: tc.apiInterface}, nil,
				&pairingtypes.RelayPrivateData{ApiUrl: "/cosmos/bank/v1beta1/balances/cosmos1demo"}, "dapp", "127.0.0.1")
			rm := NewResultsManager(1, "COSMOSHUB")
			require.NoError(t, rm.SetResponse(&RelayResponse{Err: tc.err}, message))
			_, _, protocolErrors := rm.GetResultsData()
			require.Len(t, protocolErrors, 1)
			require.Same(t, tc.want, protocolErrors[0].RouterError, "got %v", protocolErrors[0].RouterError)
		})
	}

	t.Run("without a protocol message the text decides", func(t *testing.T) {
		rm := NewResultsManager(1, "COSMOSHUB")
		require.NoError(t, rm.SetResponse(&RelayResponse{Err: status(507)}, nil))
		_, _, protocolErrors := rm.GetResultsData()
		require.Len(t, protocolErrors, 1)
		require.Same(t, common.RouterErrorUnknown, protocolErrors[0].RouterError)
	})

	// A write answered with such a status stays unclear: the unclear-write check reads this verdict's
	// MayHaveReachedNode, as it read UNKNOWN_ERROR's.
	require.True(t, common.RouterErrorNodeUnlistedServerError.MayHaveReachedNode)
}
