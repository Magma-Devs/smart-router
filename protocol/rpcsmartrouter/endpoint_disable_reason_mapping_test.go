package rpcsmartrouter

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
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

// The two disable sites derive the reason by different mechanisms — site 1 through the registry,
// site 2 from the raw HTTP status — so the same upstream 5xx is labelled differently depending on
// which api-interface the customer configured. Pinned so the divergence is a decision on the record
// rather than a surprise on a dashboard grouped by disable_reason.
func TestEndpointDisableReason_FiveHundredDivergesByTransport(t *testing.T) {
	viaJSONRPC := endpointDisableReasonFor(
		classifyThroughRelayPath(&lavasession.HTTPStatusError{StatusCode: 503, Status: "503"}, common.TransportJsonRPC))

	// REST never reaches endpointDisableReasonFor at all: sendRESTRelay returns (result, nil) with
	// StatusCode set, so the status branch at the call site hardcodes this constant instead.
	viaREST := lavasession.EndpointDisableServerError

	require.Equal(t, lavasession.EndpointDisableNodeError, viaJSONRPC,
		"a 5xx arrives at the JSON-RPC site already wrapped as an error, so it is classified by the registry")
	require.NotEqual(t, viaJSONRPC, viaREST,
		"documented divergence: one upstream 5xx incident splits into two reasons by api-interface")
}
