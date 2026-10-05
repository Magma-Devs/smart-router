package chainlib

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
)

// TestRESTRowsDoNotBreakTheUnsupportedMethodCarveOut guards the reach of the REST status rows
// beyond ClassifyError.
//
// TestRESTStatusRowsDoNotReachOtherTransports only exercises ClassifyError with an explicit
// transport, and that is not the whole story: IsUnsupportedMethodError here, and
// common.IsUnsupportedMethodError / IsNonRetryableNodeError, LOOP over every transport including
// REST whatever the caller's transport is. ExtractNodeErrorDetails returns a bare
// rpcclient.HTTPError's status as the error code, so a gateway refusing a route with an HTTP 400
// reaches the REST table's CodeEquals rows from the protocol-error path in relay_processor.
//
// When the status rows were declared ABOVE the REST table's message rows, that is exactly what
// went wrong: a 400 whose body said "method not allowed" or "route not found" classified as a
// plain bad request, and the unsupported-method carve-out (zero CU, response caching, the
// permanent-protocol-error stop) silently stopped applying to it.
func TestRESTRowsDoNotBreakTheUnsupportedMethodCarveOut(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
	}{
		{400, "method not allowed"},
		{400, "route not found"},
		{400, `{"message":"endpoint not found"}`},
		{404, "route not found"},
		{405, `{"message":"method not allowed","error_code":"web_framework_error"}`},
	} {
		t.Run(fmt.Sprintf("%d %.24s", tc.status, tc.body), func(t *testing.T) {
			err := rpcclient.HTTPError{
				StatusCode: tc.status,
				Status:     fmt.Sprintf("%d", tc.status),
				Body:       []byte(tc.body),
			}
			require.True(t, IsUnsupportedMethodError(err),
				"a %d saying %q is the route not existing, whatever its status", tc.status, tc.body)
			require.True(t, common.IsUnsupportedMethodError("", tc.status, tc.body),
				"and the common helper must agree")
		})
	}
}

// TestRESTAccessDeniedRowDoesNotLeakUnsupportedMethod: NODE_ACCESS_DENIED carries
// SubCategoryNodeCapability so the endpoint is not blamed. That label must NOT be
// SubCategoryUnsupportedMethod, which ShouldRetryErrorWithContext hard-stops on regardless of the
// Retryable flag — a 403 has to keep travelling to another provider.
//
// Asserted on the REST transport explicitly, because that is the only transport whose table has
// these rows. ShouldRetryError is hardcoded to TransportJsonRPC and never reads the REST table, so
// it is not the helper that decides a REST retry; the REST relay path takes its verdict from
// RelayResult.ApplyNodeErrorClassification, which passes common.TransportREST.
func TestRESTAccessDeniedRowDoesNotLeakUnsupportedMethod(t *testing.T) {
	for _, status := range []int{401, 402, 403, 407, 426, 451} {
		err := rpcclient.HTTPError{StatusCode: status, Status: fmt.Sprintf("%d", status), Body: []byte("denied")}
		require.False(t, IsUnsupportedMethodError(err), "REST %d is not an unsupported method", status)
		require.True(t, ShouldRetryErrorWithContext(err, -1, common.TransportREST),
			"REST %d must still be retried on another provider", status)
	}
}
