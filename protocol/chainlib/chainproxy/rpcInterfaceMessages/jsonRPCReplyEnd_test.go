package rpcInterfaceMessages

import (
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// replyChecks are the checks that read a whole reply for a node error, each with the name its
// message gives a malformed reply.
func replyChecks() map[string]struct {
	check func(data []byte, httpStatusCode int) (bool, string)
	kind  string
	batch bool
} {
	return map[string]struct {
		check func(data []byte, httpStatusCode int) (bool, string)
		kind  string
		batch bool
	}{
		"JSON-RPC":         {JsonrpcMessage{}.CheckResponseError, "JSON-RPC", false},
		"XRPL":             {JsonrpcResponseErrorChecker(common.ChainFamilyXRP, &JsonrpcMessage{}), "JSON-RPC", false},
		"Tendermint RPC":   {TendermintrpcMessage{}.CheckResponseError, "Tendermint RPC", false},
		"JSON-RPC batch":   {CheckResponseErrorForJsonRpcBatch, "JSON-RPC batch", true},
		"Tendermint batch": {CheckResponseErrorForTendermintBatch, "JSON-RPC batch", true},
	}
}

// A reply followed by other bytes is a malformed reply to every check, whatever the reply before
// the bytes says: a result is not an answer, and a node's error is not the node's error. The
// status makes no difference; under a 2xx the sender's json.Valid refuses such a body first, and
// under any other status this is the only verdict it gets (MAG-5819).
func TestReplyChecks_AReplyFollowedByOtherBytesIsMalformed(t *testing.T) {
	quietChecks(t)
	previous := BatchNodeErrorOnAny
	defer func() { BatchNodeErrorOnAny = previous }()

	single := map[string]string{
		"a result":     `{"jsonrpc":"2.0","id":1,"result":"0x1"}`,
		"a node error": `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}`,
	}
	batch := map[string]string{
		"results":       `[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"result":"0x2"}]`,
		"a node error":  `[{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}]`,
		"an empty one":  `[]`,
		"mixed replies": `[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"boom"}}]`,
	}
	tails := []string{`<html><body>appended</body></html>`, `{"jsonrpc":"2.0","id":9,"result":"0x9"}`, `[]`, `x`}

	for name, reader := range replyChecks() {
		replies := single
		if reader.batch {
			replies = batch
		}
		for what, reply := range replies {
			for _, tail := range tails {
				for _, status := range []int{200, 403, 404} {
					for _, onAny := range []bool{false, true} {
						BatchNodeErrorOnAny = onAny
						hasError, message := reader.check([]byte(reply+tail), status)
						require.True(t, hasError, "%s, %s followed by %q, status %d", name, what, tail, status)
						require.Equal(t,
							fmt.Sprintf("malformed %s response: expected end of input at position %d, got %q", reader.kind, len(reply), tail[0]),
							message, "%s, %s followed by %q, status %d", name, what, tail, status)
					}
				}
				require.False(t, IsJsonrpcReply([]byte(reply+tail)), "%s followed by %q is not a JSON-RPC reply", what, tail)
			}
		}
	}
}

// Whitespace after a reply changes nothing: every check gives the verdict and the message it gives
// the reply alone, and IsJsonrpcReply answers as it does for the reply alone.
func TestReplyChecks_AReplyFollowedByWhitespaceIsReadAsTheReply(t *testing.T) {
	quietChecks(t)
	previous := BatchNodeErrorOnAny
	defer func() { BatchNodeErrorOnAny = previous }()

	replies := []string{
		`{"jsonrpc":"2.0","id":1,"result":"0x1"}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}`,
		`{"jsonrpc":"2.0","id":1,"result":{"response":{"code":5,"log":"app refused"}}}`,
		`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"result":"0x2"}]`,
		`[{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}]`,
		`[]`,
	}
	for name, reader := range replyChecks() {
		for _, reply := range replies {
			for _, tail := range []string{"\n", "\r\n", ` `, "\t", " \r\n\t \n"} {
				for _, onAny := range []bool{false, true} {
					BatchNodeErrorOnAny = onAny
					wantError, wantMessage := reader.check([]byte(reply), 200)
					hasError, message := reader.check([]byte(reply+tail), 200)
					require.Equal(t, wantError, hasError, "%s, %q followed by %q", name, reply, tail)
					require.Equal(t, wantMessage, message, "%s, %q followed by %q", name, reply, tail)
				}
				require.Equal(t, IsJsonrpcReply([]byte(reply)), IsJsonrpcReply([]byte(reply+tail)), "%q followed by %q", reply, tail)
			}
		}
	}
	// The verdicts above are not all one answer: the comparison has both kinds to tell apart.
	clean, _ := JsonrpcMessage{}.CheckResponseError([]byte(replies[0]+"\n"), 200)
	require.False(t, clean)
	failed, message := JsonrpcMessage{}.CheckResponseError([]byte(replies[1]+"\n"), 200)
	require.True(t, failed)
	require.Equal(t, "boom", message)
	require.True(t, IsJsonrpcReply([]byte(replies[0]+"\r\n")))
}
