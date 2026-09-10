package rpcsmartrouter

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"syscall"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// The reason recorded on a disable is only worth having if it is right, and nothing else pins the
// mapping — the behavioural tests in lavasession pass a reason in and assert it comes back out, so
// they would agree with any mapping at all, including an inverted one.
func TestEndpointDisableReasonFor_MapsRegistryCategory(t *testing.T) {
	t.Run("internal means the request never got an answer", func(t *testing.T) {
		require.Equal(t, lavasession.EndpointDisableUnreachable,
			endpointDisableReasonFor(common.LavaErrorConnectionRefused))
		require.Equal(t, lavasession.EndpointDisableUnreachable,
			endpointDisableReasonFor(common.LavaErrorConnectionTimeout))
	})

	t.Run("external means the node answered and the answer was its own failure", func(t *testing.T) {
		require.Equal(t, common.CategoryExternal, common.LavaErrorNodeInternalError.Category,
			"precondition: this fixture must be the External case")
		require.Equal(t, lavasession.EndpointDisableNodeError,
			endpointDisableReasonFor(common.LavaErrorNodeInternalError))
	})

	t.Run("nil is a bug, and must be visibly wrong rather than empty", func(t *testing.T) {
		require.Equal(t, lavasession.EndpointDisableUnspecified, endpointDisableReasonFor(nil))
	})
}

// classifyThroughRelayPath mirrors what the disable site actually runs
// (rpcsmartrouter_server.go: classified = ClassifyError(DetectConnectionError(err), ...)), so this
// table measures the reason a real fault produces rather than the reason a hand-built LavaError does.
func classifyThroughRelayPath(err error, transport common.TransportType) *common.LavaError {
	errorCode := 0
	var httpErr *lavasession.HTTPStatusError
	if errors.As(err, &httpErr) {
		errorCode = httpErr.StatusCode
	}
	return common.ClassifyError(common.DetectConnectionError(err), common.ChainFamilyEVM, transport, errorCode, err.Error())
}

// Characterisation test: this pins what a real transport fault is labelled TODAY, end to end.
//
// Several rows below are marked KNOWN-WRONG. They are asserted as-is deliberately — the reason is
// wrong for them today, and a test that asserted the desired value would fail on main and tell
// nobody anything. Pinned here, the wrongness is visible in the source and this test turns red the
// moment the classifier is fixed, which is the prompt to update the expectations.
//
// Cause: common.DetectConnectionError has no branch for *net.DNSError, x509 errors or io.EOF, and
// the string-fallback table has no matching row, so those faults classify as LavaErrorUnknown —
// which is CategoryExternal, and every External maps to node-error. That is a pre-existing gap in
// the shared classifier (it predates the disable-reason work and is used by retry, QoS and backoff
// as well), so it is fixed in its own ticket rather than here. See MAG-3563.
func TestEndpointDisableReasonFor_RealTransportFaults(t *testing.T) {
	refused := syscall.ECONNREFUSED
	cases := []struct {
		name       string
		err        error
		transport  common.TransportType
		want       lavasession.EndpointDisableReason
		knownWrong lavasession.EndpointDisableReason // non-empty = what it SHOULD be once MAG-3563 lands
	}{
		{
			name: "connection refused", err: &net.OpError{Op: "dial", Net: "tcp", Err: &refused},
			transport: common.TransportJsonRPC, want: lavasession.EndpointDisableUnreachable,
		},
		{
			name: "context deadline exceeded", err: context.DeadlineExceeded,
			transport: common.TransportJsonRPC, want: lavasession.EndpointDisableUnreachable,
		},
		{
			name: "connection reset (string fallback)", err: errors.New("read tcp: connection reset by peer"),
			transport: common.TransportJsonRPC, want: lavasession.EndpointDisableUnreachable,
		},
		{
			name: "node answered 500", err: &lavasession.HTTPStatusError{StatusCode: 500, Status: "500"},
			transport: common.TransportJsonRPC, want: lavasession.EndpointDisableNodeError,
		},
		// ---- KNOWN-WRONG below: all of these are infrastructure faults, none reached the node ----
		{
			name: "DNS: no such host", knownWrong: lavasession.EndpointDisableUnreachable,
			err:       &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "rpc.example", IsNotFound: true}},
			transport: common.TransportJsonRPC, want: lavasession.EndpointDisableNodeError,
		},
		{
			name: "TLS: unknown authority", knownWrong: lavasession.EndpointDisableUnreachable,
			err:       &net.OpError{Op: "dial", Net: "tcp", Err: x509.UnknownAuthorityError{}},
			transport: common.TransportJsonRPC, want: lavasession.EndpointDisableNodeError,
		},
		{
			name: "TLS: certificate expired", knownWrong: lavasession.EndpointDisableUnreachable,
			err:       &net.OpError{Op: "dial", Net: "tcp", Err: x509.CertificateInvalidError{Reason: x509.Expired}},
			transport: common.TransportJsonRPC, want: lavasession.EndpointDisableNodeError,
		},
		{
			name: "io.EOF", knownWrong: lavasession.EndpointDisableUnreachable,
			err:       io.EOF,
			transport: common.TransportJsonRPC, want: lavasession.EndpointDisableNodeError,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			classified := classifyThroughRelayPath(tc.err, tc.transport)
			require.NotNil(t, classified, "ClassifyError never returns nil, so the nil branch is unreachable in production")

			atFault, _ := classifyEndpointHealth(classified, false)
			require.True(t, atFault, "precondition: this fault must reach the disable site at all")

			got := endpointDisableReasonFor(classified)
			if tc.knownWrong != "" {
				require.Equalf(t, tc.want, got,
					"KNOWN-WRONG row changed: %q now reports %q. If the classifier was fixed, this row should "+
						"become %q and the knownWrong marker removed.", tc.name, got, tc.knownWrong)
				return
			}
			require.Equal(t, tc.want, got)
		})
	}
}

// One upstream 5xx must produce ONE reason, whichever api-interface carried it.
//
// The two disable sites reach the status by different routes: JSON-RPC's sender wraps it into an
// HTTPStatusError that arrives as an error, while REST returns (result, nil) with StatusCode set, so
// it lands on the status branch instead. An earlier draft of this change let the second site name a
// reason from the raw code (`http-server-error`), which split one incident into two reasons purely
// by customer configuration — a dashboard grouped by disable_reason would have shown two
// half-incidents and neither would have matched the outage. Both sites now classify through the
// registry, so this pins the convergence rather than the divergence.
func TestEndpointDisableReason_FiveHundredAgreesAcrossTransports(t *testing.T) {
	for _, statusCode := range []int{500, 502, 503} {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			// Site 1's route: the sender wrapped the status into an error.
			viaJSONRPC := endpointDisableReasonFor(classifyThroughRelayPath(
				&lavasession.HTTPStatusError{StatusCode: statusCode, Status: http.StatusText(statusCode)},
				common.TransportJsonRPC))

			// Site 2's route: (result, nil) with StatusCode set, classified from the status alone.
			// Mirrors the call in relayInnerDirect exactly — nil connection error, empty message.
			viaREST := endpointDisableReasonFor(
				common.ClassifyError(nil, common.ChainFamilyEVM, common.TransportREST, statusCode, ""))

			require.Equal(t, lavasession.EndpointDisableNodeError, viaJSONRPC,
				"a 5xx means the node answered and the answer was its own failure")
			require.Equal(t, viaJSONRPC, viaREST,
				"one upstream incident, one reason — the label must describe the fault, not the transport")
		})
	}
}

// 429 must never reach the disable path: a rate-limited endpoint is not an unhealthy one, and
// benching it would remove capacity precisely when the upstream is asking for less load. Guarded
// here because the enclosing status branch matches `>= 500 || == 429`, so the whole carve-out lives
// in classifyHTTPStatus and nothing else pinned it.
func TestClassifyHTTPStatus_RateLimitDoesNotDisable(t *testing.T) {
	shouldMarkUnhealthy, needsBackoff := classifyHTTPStatus(http.StatusTooManyRequests)
	require.False(t, shouldMarkUnhealthy, "a 429 must not disable the endpoint")
	require.True(t, needsBackoff, "but it must still back off")

	shouldMarkUnhealthy, _ = classifyHTTPStatus(http.StatusServiceUnavailable)
	require.True(t, shouldMarkUnhealthy, "control: a 5xx does disable")
}
