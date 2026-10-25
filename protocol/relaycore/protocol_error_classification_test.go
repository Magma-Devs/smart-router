package relaycore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
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
		want    *common.LavaError
	}{
		{"gRPC-only text matcher", "ETH1", errors.New("not implemented"), common.LavaErrorNodeUnimplemented},
		{"JSON-RPC-only text matcher", "ETH1", errors.New("nonce too low"), common.LavaErrorUnknown},
		{"no chain family", "SOLANA", errors.New("blockhash not found"), common.LavaErrorUnknown},
		{"HTTP status text", "ETH1", errors.New("HTTP status 503: Service Unavailable"), common.LavaErrorNodeServiceUnavailable},
		{"context deadline", "ETH1", context.DeadlineExceeded, common.LavaErrorContextDeadline},
		{"EOF", "ETH1", fmt.Errorf("post: %w", io.EOF), common.LavaErrorConnectionClosed},
		{"DNS", "ETH1", fmt.Errorf("dial: %w", &net.DNSError{Err: "no such host", Name: "node.invalid", IsNotFound: true}), common.LavaErrorDNSFailure},
		{"session error", "ETH1", lavasession.PairingListEmptyError, common.LavaErrorUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rm := NewResultsManager(1, tc.chainID)
			require.NoError(t, rm.SetResponse(&RelayResponse{Err: tc.err}, nil))
			_, _, protocolErrors := rm.GetResultsData()
			require.Len(t, protocolErrors, 1)
			require.Same(t, tc.want, protocolErrors[0].LavaError, "got %v", protocolErrors[0].LavaError)
		})
	}
}
