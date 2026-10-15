package common

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestClassifyError_EOFDNSAndTLSAreAtFault pins MAG-4242 item 5 through the real path. An upstream
// EOF, a DNS failure and an http-served-as-https endpoint used to classify as UNKNOWN_ERROR
// (CategoryExternal, nobody's fault) while their sibling, a connection reset, was internal and at
// fault, so retry, QoS, health tracking and the write outcome all read the wrong category. The
// three registry codes declared for them were never assigned.
func TestClassifyError_EOFDNSAndTLSAreAtFault(t *testing.T) {
	dnsNotFound := &net.DNSError{Err: "no such host", Name: "rpc.example", IsNotFound: true}
	tests := []struct {
		name string
		err  error
		want *LavaError
	}{
		{"bare EOF", io.EOF, LavaErrorConnectionClosed},
		{"EOF wrapped by the sender", fmt.Errorf("http request failed: %w", io.EOF), LavaErrorConnectionClosed},
		{"EOF inside url.Error", &url.Error{Op: "Post", URL: "https://rpc.example/", Err: io.EOF}, LavaErrorConnectionClosed},
		{"unexpected EOF mid-body", fmt.Errorf("failed reading response: %w", io.ErrUnexpectedEOF), LavaErrorConnectionClosed},
		{"DNS not found", dnsNotFound, LavaErrorDNSFailure},
		{"DNS not found inside url.Error and OpError", &url.Error{Op: "Post", URL: "https://rpc.example/", Err: &net.OpError{Op: "dial", Net: "tcp", Err: dnsNotFound}}, LavaErrorDNSFailure},
		{"DNS failure wrapped without %w", errors.New(`Post "https://rpc.example/": dial tcp: lookup rpc.example: no such host`), LavaErrorDNSFailure},
		{"http served as https", &url.Error{Op: "Post", URL: "https://rpc.example/", Err: errors.New("http: server gave HTTP response to HTTPS client")}, LavaErrorTLSMismatch},
		// The node error handler's rewording of the same three shapes.
		{"rewritten EOF", errors.New("Provider Side Failed Sending Message, Reason: End of input stream reached"), LavaErrorConnectionClosed},
		{"rewritten DNS", errors.New("Provider Side Failed Sending Message, Reason: DNS resolution failed"), LavaErrorDNSFailure},
		{"rewritten TLS", errors.New("Provider Side Failed Sending Message, Reason: misconfigured http endpoint as https"), LavaErrorTLSMismatch},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			classified := ClassifyError(DetectConnectionError(tc.err), ChainFamilyEVM, TransportJsonRPC, 0, tc.err.Error())
			require.Equal(t, tc.want, classified, "got %v", classified)
			require.True(t, classified.EndpointAtFault(), "a connect or transport failure is the endpoint's fault")
			require.True(t, classified.Retryable, "another endpoint has every chance")
		})
	}
}

// A DNS timeout is still a timeout: the typed timeout check comes first.
func TestDetectConnectionError_DNSTimeoutStaysATimeout(t *testing.T) {
	err := &url.Error{Op: "Post", URL: "https://rpc.example/", Err: &net.DNSError{Err: "i/o timeout", Name: "rpc.example", IsTimeout: true}}
	require.Equal(t, LavaErrorConnectionTimeout, DetectConnectionError(err))
}

// TestDetectConnectionErrorInReplyText_KeepsTheTransportOnlyRowsOffNodeReplies: the rows MAG-4242
// item 5 added describe Go's own transport errors, so a node's reply text that happens to say
// "unexpected EOF" or "no such host" stays the node's answer. The older rows read reply text as
// they always have, and the transport path still reads every row.
func TestDetectConnectionErrorInReplyText_KeepsTheTransportOnlyRowsOffNodeReplies(t *testing.T) {
	for _, text := range []string{
		`{"code":3,"message":"unexpected EOF","details":[]}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"lookup sequencer.internal: no such host"}}`,
		`server gave HTTP response to HTTPS client`,
		`Provider Side Failed Sending Message, Reason: DNS resolution failed`,
	} {
		require.Nil(t, DetectConnectionErrorInReplyText(text), text)
		require.NotNil(t, DetectConnectionError(errors.New(text)), "the transport path still reads %q", text)
	}
	require.Equal(t, LavaErrorConnectionRefused, DetectConnectionErrorInReplyText("dial tcp: connection refused"),
		"a row that predates item 5 reads reply text exactly as before")
}
