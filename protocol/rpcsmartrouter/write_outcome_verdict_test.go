package rpcsmartrouter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relayprotocol"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// What one copy's failure proves about a write (MAG-5232 Expected 1, 2 and 5): 401, 402, 403 and 429
// refuse it; a failure before the request was written proves it was not sent; anything else may
// have run, a reset and a timeout above all, which arrive after the request was on the wire.
func TestProtocolErrorEvidence(t *testing.T) {
	wrapped := func(routerError *common.RouterError) relaycore.RelayError {
		return relaycore.RelayError{Err: common.NewRouterErrorWrapping(routerError, errors.New("cause")), RouterError: routerError}
	}
	status := func(code int) relaycore.RelayError {
		return relaycore.RelayError{Err: fmt.Errorf("wrapped: %w", &routersession.HTTPStatusError{StatusCode: code})}
	}
	for _, c := range []struct {
		name  string
		error relaycore.RelayError
		want  copyEvidence
	}{
		{"401", status(http.StatusUnauthorized), copyRefused},
		{"402", status(http.StatusPaymentRequired), copyRefused},
		{"403", status(http.StatusForbidden), copyRefused},
		{"429", status(http.StatusTooManyRequests), copyRefused},
		{"a typed 429 with no status", relaycore.RelayError{Err: common.RateLimited(errors.New("handshake"), 0)}, copyRefused},
		{"400", status(http.StatusBadRequest), copyMayHaveRun},
		{"404", status(http.StatusNotFound), copyMayHaveRun},
		{"500", status(http.StatusInternalServerError), copyMayHaveRun},
		{"502", status(http.StatusBadGateway), copyMayHaveRun},
		{"503", status(http.StatusServiceUnavailable), copyMayHaveRun},
		{"refused connection", wrapped(common.RouterErrorConnectionRefused), copyNotSent},
		{"dns failure", wrapped(common.RouterErrorDNSFailure), copyNotSent},
		{"tls mismatch", wrapped(common.RouterErrorTLSMismatch), copyNotSent},
		{"network unreachable", wrapped(common.RouterErrorNetworkUnreachable), copyNotSent},
		{"connection reset", wrapped(common.RouterErrorConnectionReset), copyMayHaveRun},
		{"connection closed", wrapped(common.RouterErrorConnectionClosed), copyMayHaveRun},
		{"timeout", wrapped(common.RouterErrorConnectionTimeout), copyMayHaveRun},
		{"deadline", wrapped(common.RouterErrorContextDeadline), copyMayHaveRun},
		{"unclassified", relaycore.RelayError{Err: errors.New("something")}, copyMayHaveRun},
	} {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, protocolErrorEvidence(c.error))
		})
	}
	// Every failure the evidence calls not sent is one the registry says cannot have reached a node.
	for _, routerError := range []*common.RouterError{common.RouterErrorConnectionRefused, common.RouterErrorDNSFailure, common.RouterErrorTLSMismatch, common.RouterErrorNetworkUnreachable} {
		require.False(t, routerError.MayHaveReachedNode, routerError.Name)
	}
}

// Solana writes ids in base58, Bitcoin's alphabet: the standard vectors, leading zeros included.
func TestBase58(t *testing.T) {
	for _, c := range []struct {
		data    []byte
		encoded string
	}{
		{[]byte("Hello World!"), "2NEpo7TZRRrLZSi2U"},
		{[]byte("The quick brown fox jumps over the lazy dog."), "USm3fpXnKG5EUBx2ndxBDMPVciP5hGey2Jh4NDv6gmeo1LkMeiKrLJUUBk6Z"},
		{[]byte{0x00, 0x00, 0x28, 0x7f, 0xb4, 0xcd}, "11233QC4"},
		{[]byte{0x00}, "1"},
	} {
		require.Equal(t, c.encoded, base58Encode(c.data))
		decoded, err := base58Decode(c.encoded)
		require.NoError(t, err)
		require.Equal(t, c.data, decoded)
	}
	_, err := base58Decode("0OIl")
	require.ErrorIs(t, err, errNotBase58)
}

// Solana's compact-u16: seven bits a byte, low bits first, at most three bytes.
func TestDecodeCompactU16(t *testing.T) {
	for _, c := range []struct {
		data        []byte
		value, size int
		ok          bool
	}{
		{[]byte{0x01, 0xff}, 1, 1, true},
		{[]byte{0x7f}, 127, 1, true},
		{[]byte{0x80, 0x01}, 128, 2, true},
		{[]byte{0xff, 0xff, 0x03}, 65535, 3, true},
		{[]byte{0x80}, 0, 0, false},
		{nil, 0, 0, false},
	} {
		value, size, ok := decodeCompactU16(c.data)
		require.Equal(t, c.ok, ok, "%x", c.data)
		require.Equal(t, c.value, value, "%x", c.data)
		require.Equal(t, c.size, size, "%x", c.data)
	}
}

// Part I's sentence names the hash when the router has it, and tells the client how to get it
// otherwise.
func TestUnknownWriteMessage(t *testing.T) {
	require.Equal(t, "Status unknown - this transaction may have been broadcast. Look up 0xabc before signing anything new.", unknownWriteMessage("0xabc"))
	require.Equal(t, "Status unknown - this transaction may have been broadcast. Look up this transaction by the hash of your signed bytes before signing anything new.", unknownWriteMessage(""))
}

// parsedWrite is tc's request as SendParsedRelay gets it.
func parsedWrite(t *testing.T, tc writeInterfaceCase, chainParser chainlib.ChainParser) chainlib.ProtocolMessage {
	t.Helper()
	chainMessage, err := chainParser.ParseMsg(tc.path, []byte(tc.body), tc.method, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	relayData := relayprotocol.NewRelayData(context.Background(), tc.method, tc.path, []byte(tc.body), 0, spectypes.NOT_APPLICABLE, tc.apiInterface, nil, "", nil)
	return chainlib.NewProtocolMessage(chainMessage, nil, relayData, "dapp", "1.2.3.4")
}

// A batch carries no single transaction, so it has no hash, even when the batch holds a write.
func TestWriteTxHash_BatchHasNone(t *testing.T) {
	tc := jsonRPCBatchWithAWrite(t)
	chainParser := tc.parser(t)
	server := &RPCSmartRouterServer{listenEndpoint: &routersession.RPCEndpoint{ChainID: tc.chainID, ApiInterface: tc.apiInterface}}
	protocolMessage := parsedWrite(t, tc, chainParser)
	require.True(t, protocolMessage.IsBatch())
	require.Empty(t, server.writeTxHash(protocolMessage))

	single := writeInterfaceCases[0]
	require.Equal(t, "0x5fe7f977e71dba2ea1a68e21057beebb9be2ac30c6410aa38d4f3fbe41dcffd2", server.writeTxHash(parsedWrite(t, single, chainParser)),
		"keccak-256 of the raw transaction 0x01")
	require.Empty(t, requestGUID(context.Background()))
}

// The router's reply to a write is HTTP 200 on the result itself, not only in the body the listener
// forwards: relay_end and the request metrics read the result's status (MAG-5232 Expected 4 and 5).
func TestSendParsedRelay_WriteReplyResultIsHTTP200(t *testing.T) {
	rand.InitRandomSeed()
	tc := writeInterfaceCases[0]
	for _, c := range []struct {
		name     string
		upstream writeUpstream
		outcome  string
	}{
		{"unknown", writeUpstream{status: http.StatusServiceUnavailable, body: "down"}, writeOutcomeUnknown},
		{"not sent", writeUpstream{refuse: true}, writeOutcomeNotSent},
	} {
		t.Run(c.name, func(t *testing.T) {
			chainParser := tc.parser(t)
			a, b := c.upstream, c.upstream
			a.name, b.name = "a-"+t.Name(), "b-"+t.Name()
			hits := make([]int64, 2)
			server := newWriteServer(t, tc, chainParser, []writeUpstream{a, b}, nil, hits)

			result, err := sendWrite(t, tc, chainParser, server)
			require.Error(t, err)
			require.NotNil(t, result)
			require.Equal(t, http.StatusOK, result.StatusCode)
			require.Equal(t, http.StatusOK, result.GetClientFacing().UpstreamStatus)
			outcome, _ := replyHeader(result.GetReply().GetMetadata(), common.WRITE_OUTCOME_HEADER_NAME)
			require.Equal(t, c.outcome, outcome)
		})
	}
}
