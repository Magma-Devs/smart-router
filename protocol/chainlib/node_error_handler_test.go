package chainlib

import (
	"errors"
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestIsUnsupportedMethodError pins that no node refusal reads as a router-terminal unsupported
// method. Since MAG-2771 a node saying it does not serve a method, route or verb — 404, 405, gRPC
// Unimplemented, -32601, "method not found" — is node-capability and retried on another provider,
// so every one of those shapes below must come back false. A code re-tagged
// SubCategoryUnsupportedMethod would turn them true and stop the retry.
func TestIsUnsupportedMethodError(t *testing.T) {
	t.Run("Nil error returns false", func(t *testing.T) {
		require.False(t, IsUnsupportedMethodError(nil))
	})

	t.Run("Error message patterns", func(t *testing.T) {
		tests := []struct {
			name     string
			err      error
			expected bool
		}{
			{
				name:     "Method not found is the node's refusal, not an unsupported method",
				err:      errors.New("method not found"),
				expected: false,
			},
			{
				name:     "Generic error",
				err:      errors.New("internal server error"),
				expected: false,
			},
			{
				name:     "Wrapped method not found is the node's refusal too",
				err:      fmt.Errorf("request failed: %w", errors.New("method not found")),
				expected: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := IsUnsupportedMethodError(tt.err)
				require.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("HTTP status codes", func(t *testing.T) {
		tests := []struct {
			name     string
			err      rpcclient.HTTPError
			expected bool
		}{
			{
				name:     "404 Not Found is the node's refusal",
				err:      rpcclient.HTTPError{StatusCode: 404, Status: "404 Not Found"},
				expected: false,
			},
			{
				name:     "405 Method Not Allowed is the node's refusal",
				err:      rpcclient.HTTPError{StatusCode: 405, Status: "405 Method Not Allowed"},
				expected: false,
			},
			{
				name:     "500 Internal Server Error",
				err:      rpcclient.HTTPError{StatusCode: 500, Status: "500 Internal Server Error"},
				expected: false,
			},
			{
				name:     "200 OK",
				err:      rpcclient.HTTPError{StatusCode: 200, Status: "200 OK"},
				expected: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := IsUnsupportedMethodError(tt.err)
				require.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("gRPC status codes", func(t *testing.T) {
		tests := []struct {
			name     string
			err      error
			expected bool
		}{
			{
				name:     "Unimplemented status is the node's refusal",
				err:      status.Error(codes.Unimplemented, "method not implemented"),
				expected: false,
			},
			{
				name:     "NotFound status",
				err:      status.Error(codes.NotFound, "resource not available"),
				expected: false,
			},
			{
				name:     "Internal status",
				err:      status.Error(codes.Internal, "internal error"),
				expected: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := IsUnsupportedMethodError(tt.err)
				require.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("JSON-RPC error recovery", func(t *testing.T) {
		// This tests the TryRecoverNodeErrorFromClientError path
		// Creating a mock HTTP error with JSON-RPC error body
		jsonRPCError := `{"jsonrpc":"2.0","error":{"code":-32601,"message":"Method not found"},"id":1}`
		httpErr := rpcclient.HTTPError{
			StatusCode: 200,
			Status:     "200 OK",
			Body:       []byte(jsonRPCError),
		}

		result := IsUnsupportedMethodError(httpErr)
		require.False(t, result, "a JSON-RPC -32601 is the node's refusal, retried on another provider")
	})

	t.Run("Combined error scenarios", func(t *testing.T) {
		// Test a gRPC error with method not found message
		err := status.Error(codes.Internal, "method not found")
		require.False(t, IsUnsupportedMethodError(err), "a method-not-found message is the node's refusal whatever the status code")

		// Test HTTP error with method not found only in body (not in error message)
		httpErr := rpcclient.HTTPError{
			StatusCode: 200,
			Status:     "200 OK",
			Body:       []byte("Method not found: eth_newMethod"),
		}
		// HTTPError.Error() includes the body, so this classifies by the "method not found" in
		// it: the node's refusal, not an unsupported method
		require.False(t, IsUnsupportedMethodError(httpErr))
	})
}

func BenchmarkIsUnsupportedMethodError(b *testing.B) {
	testErrors := []error{
		errors.New("method not found"),
		rpcclient.HTTPError{StatusCode: 404},
		status.Error(codes.Unimplemented, "not implemented"),
		errors.New("generic error"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, err := range testErrors {
			_ = IsUnsupportedMethodError(err)
		}
	}
}

// TestIsUnsupportedMethodError_SmartContractErrors verifies that smart contract errors are not read
// as the node refusing the method (prevents false positives on reverts). Since MAG-2771 such a false
// positive would send a revert to another provider instead of returning it, so the matchers must
// stay as tight as they were: a revert classifies as no node-refusal code, and a real refusal
// classifies as the code its wording names. Neither reads as a router-terminal unsupported method.
func TestIsUnsupportedMethodError_SmartContractErrors(t *testing.T) {
	refusalCodes := map[uint32]bool{
		common.LavaErrorNodeMethodNotFound.Code:   true,
		common.LavaErrorNodeUnimplemented.Code:    true,
		common.LavaErrorNodeEndpointNotFound.Code: true,
		common.LavaErrorNodeMethodNotAllowed.Code: true,
	}
	// The transports IsUnsupportedMethodError consults, in its order.
	classifyAll := func(err error) []*common.LavaError {
		var out []*common.LavaError
		for _, transport := range []common.TransportType{common.TransportJsonRPC, common.TransportREST, common.TransportGRPC} {
			if classified := ClassifyNodeError(err, -1, transport); classified != nil {
				out = append(out, classified)
			}
		}
		return out
	}

	tests := []struct {
		name    string
		message string
		refusal *common.LavaError // the node refusal the message names, nil for a revert
	}{
		// CRITICAL: Smart contract errors must not read as a node refusal
		{"Smart contract NFT not found", "execution reverted: NFT not found", nil},
		{"Smart contract User not found", "execution reverted: User not found", nil},
		{"Smart contract Token not found", "execution reverted: Token not found", nil},
		{"Smart contract identity not found", "execution reverted: identity not found", nil},
		{"Smart contract IdentityRegistry specific", "execution reverted: IdentityRegistry: identity not found", nil},
		{"Smart contract Record not found", "execution reverted: Record not found in database", nil},
		{"Generic not found without execution reverted", "user not found", nil},
		{"Item not found", "item not found", nil},
		// Real refusals still classify as the code their wording names
		{"Actual method not found", "method not found", common.LavaErrorNodeMethodNotFound},
		{"Actual endpoint not found", "endpoint not found", common.LavaErrorNodeEndpointNotFound},
		{"Actual route not found", "route not found", common.LavaErrorNodeEndpointNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.message)
			require.False(t, IsUnsupportedMethodError(err), "Message: %s", tt.message)
			classified := classifyAll(err)
			if tt.refusal == nil {
				for _, c := range classified {
					require.False(t, refusalCodes[c.Code], "Message: %s classified as %s", tt.message, c.Name)
				}
				return
			}
			require.Contains(t, classified, tt.refusal, "Message: %s", tt.message)
		})
	}
}
