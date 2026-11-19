package rpcsmartrouter

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// noRowsGraphQLWriteCase is the GraphQL write case on a chain whose write rows have not shipped: an
// index no chain family claims. Its copies keep today's handling, with the status and transport rows
// on top (GraphQL interface chapter, section 5, "No write rows yet"), and it has no digest formula.
func noRowsGraphQLWriteCase(t *testing.T) writeInterfaceCase {
	t.Helper()
	tc := graphQLWriteCase(t)
	tc.name, tc.chainID = "graphql without write rows", "GQLNOROWS"
	tc.parser = func(t *testing.T) chainlib.ChainParser { return graphQLWriteParser(t, tc.chainID) }
	return tc
}

// clientReply is the status and body the listener sends for result: the upstream's own answer a
// failed relay carries, or the reply the relay served.
func clientReply(t *testing.T, result *common.RelayResult) (int, []byte) {
	t.Helper()
	require.NotNil(t, result)
	if cf := result.GetClientFacing(); cf != nil && cf.UpstreamStatus != 0 {
		return cf.UpstreamStatus, cf.UpstreamBody
	}
	require.NotNil(t, result.Reply)
	return result.StatusCode, result.Reply.Data
}

// TestGraphQLWrite_EveryCopyRefusedByStatusIsRejected is MAG-5122 T15: when every copy is refused by
// the vendor's status (429, 401, 402, 403), the client gets the vendor's reply as is, with
// Lava-Write-Outcome: rejected. The status rows apply on a chain without write rows too.
func TestGraphQLWrite_EveryCopyRefusedByStatusIsRejected(t *testing.T) {
	rand.InitRandomSeed()
	const vendorRefusal = `{"message":"Too Many Requests"}`
	for _, tc := range []writeInterfaceCase{suiWriteOf(t), noRowsGraphQLWriteCase(t)} {
		for _, statuses := range [][2]int{
			{http.StatusTooManyRequests, http.StatusTooManyRequests},
			{http.StatusUnauthorized, http.StatusForbidden},
			{http.StatusPaymentRequired, http.StatusTooManyRequests},
		} {
			t.Run(fmt.Sprintf("%s/%d and %d", tc.name, statuses[0], statuses[1]), func(t *testing.T) {
				chainParser := tc.parser(t)
				a := writeUpstream{name: "a-" + t.Name(), status: statuses[0], body: vendorRefusal}
				b := writeUpstream{name: "b-" + t.Name(), status: statuses[1], body: vendorRefusal}
				hits, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{a, b})

				result, _ := sendWrite(t, tc, chainParser, server)
				require.Equal(t, [2]int64{1, 1}, [2]int64{atomic.LoadInt64(&hits[0]), atomic.LoadInt64(&hits[1])}, "one copy per provider, never re-sent")
				status, body := clientReply(t, result)
				require.Contains(t, statuses, status, "a vendor's refusal goes back with its own status")
				require.Equal(t, vendorRefusal, string(body), "and its own body")
				require.Equal(t, "rejected", writeOutcomeHeader(t, result))
				if tc.chainID == "SUI" {
					require.Equal(t, suiSnapshotDigest, resultHeader(result, common.TX_HASH_HEADER_NAME))
				} else {
					require.Empty(t, resultHeader(result, common.TX_HASH_HEADER_NAME), "no digest formula, so no Lava-Tx-Hash")
				}
			})
		}
	}
}

// TestGraphQLWrite_NoCopySentIsNotSent is MAG-5122 T16: when no connection was made on any copy, no
// node reply exists, and the router answers 502 with its own GraphQL error document, write_outcome:
// not_sent and the digest, and Lava-Write-Outcome: not_sent. The document names no url, address or
// connection error: the caller learns only that no provider received the transaction.
func TestGraphQLWrite_NoCopySentIsNotSent(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range []writeInterfaceCase{suiWriteOf(t), noRowsGraphQLWriteCase(t)} {
		t.Run(tc.name, func(t *testing.T) {
			chainParser := tc.parser(t)
			a := writeUpstream{name: "a-" + t.Name(), refuse: true}
			b := writeUpstream{name: "b-" + t.Name(), refuse: true}
			_, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{a, b})

			result, err := sendWrite(t, tc, chainParser, server)
			require.Error(t, err, "no provider received the write")
			require.NotNil(t, result)
			require.Equal(t, http.StatusBadGateway, result.StatusCode)
			status, body := clientReply(t, result)
			require.Equal(t, http.StatusBadGateway, status)
			require.Equal(t, common.JSONContentType, result.GetClientFacing().UpstreamContentType)

			var document struct {
				Data   json.RawMessage `json:"data"`
				Errors []struct {
					Message    string            `json:"message"`
					Extensions map[string]string `json:"extensions"`
				} `json:"errors"`
			}
			require.NoError(t, json.Unmarshal(body, &document), string(body))
			require.Nil(t, document.Data, "a router reply has no data key")
			require.Len(t, document.Errors, 1)
			require.Equal(t, "no answer from upstream: not sent", document.Errors[0].Message)
			require.Equal(t, "not_sent", document.Errors[0].Extensions["write_outcome"])
			for _, leak := range []string{"127.0.0.1", "http://", "dial", "refused"} {
				require.NotContains(t, string(body), leak)
			}

			require.Equal(t, "not_sent", writeOutcomeHeader(t, result))
			if tc.chainID == "SUI" {
				require.Equal(t, suiSnapshotDigest, document.Errors[0].Extensions["tx_hash"])
				require.Equal(t, suiSnapshotDigest, resultHeader(result, common.TX_HASH_HEADER_NAME))
			} else {
				require.NotContains(t, document.Errors[0].Extensions, "tx_hash")
				require.Empty(t, resultHeader(result, common.TX_HASH_HEADER_NAME))
			}
		})
	}
}

// TestGraphQLWrite_OutcomeAcrossCopies is MAG-5122 Sending point 4 (Part I R-L: accepted > unknown >
// rejected > not_sent) on the copies a write can mix.
func TestGraphQLWrite_OutcomeAcrossCopies(t *testing.T) {
	rand.InitRandomSeed()
	const vendorRefusal = `{"message":"Too Many Requests"}`
	refused := writeUpstream{refuse: true}
	rateLimited := writeUpstream{status: http.StatusTooManyRequests, body: vendorRefusal}
	unavailable := writeUpstream{status: http.StatusServiceUnavailable, body: "upstream down"}
	nodeAnswer := writeUpstream{status: http.StatusOK, body: `{"data":null,"errors":[{"message":"cannot submit"}]}`}
	for _, c := range []struct {
		name   string
		tc     writeInterfaceCase
		a, b   writeUpstream
		header string
	}{
		{name: "a refusal beside a copy not sent is rejected", tc: suiWriteOf(t), a: rateLimited, b: refused, header: "rejected"},
		{name: "a copy not sent beside one that may have run is unknown", tc: suiWriteOf(t), a: refused, b: unavailable, header: "unknown"},
		// The handshake failed, but the router reads that only from Go's text, which proves nothing:
		// with write rows, a failure the transport cannot prove happened before any byte left may
		// have run, though it carries no MayHaveReachedNode flag.
		{name: "a failed TLS handshake with no proof may have run", tc: suiWriteOf(t), a: writeUpstream{overTLS: true}, b: writeUpstream{overTLS: true}, header: "unknown"},
		{name: "without write rows, a failed TLS handshake keeps today's reply, with no outcome", tc: noRowsGraphQLWriteCase(t), a: writeUpstream{overTLS: true}, b: writeUpstream{overTLS: true}},
		{name: "without write rows, a refusal beside a copy not sent is rejected", tc: noRowsGraphQLWriteCase(t), a: rateLimited, b: refused, header: "rejected"},
		{name: "without write rows, a 5xx keeps today's reply, with no outcome", tc: noRowsGraphQLWriteCase(t), a: refused, b: unavailable},
		{name: "without write rows, a node's 2xx body is forwarded as is, with no outcome", tc: noRowsGraphQLWriteCase(t), a: nodeAnswer, b: nodeAnswer},
		{name: "without write rows, a served write is forwarded as is, with no outcome", tc: noRowsGraphQLWriteCase(t), a: writeUpstream{status: http.StatusOK, body: noRowsGraphQLWriteCase(t).success}, b: refused},
	} {
		t.Run(c.name, func(t *testing.T) {
			chainParser := c.tc.parser(t)
			a, b := c.a, c.b
			a.name, b.name = "a-"+t.Name(), "b-"+t.Name()
			_, server := newTwoPrimaryWriteServer(t, c.tc, chainParser, [2]writeUpstream{a, b})

			result, _ := sendWrite(t, c.tc, chainParser, server)
			require.NotNil(t, result)
			require.Equal(t, c.header, writeOutcomeHeader(t, result))
			if c.header == "rejected" {
				status, body := clientReply(t, result)
				require.Equal(t, http.StatusTooManyRequests, status, "the refusal goes back as is")
				require.Equal(t, vendorRefusal, string(body))
			}
		})
	}
}

// TestGraphQLWrite_NeverSentCarriesNotSent: a write no copy of which was dispatched keeps the
// router's own reply, with Lava-Write-Outcome: not_sent (Part I R-L, a refusal before any copy left).
func TestGraphQLWrite_NeverSentCarriesNotSent(t *testing.T) {
	rand.InitRandomSeed()
	tc := suiWriteOf(t)
	chainParser := tc.parser(t)
	server := newWriteServer(t, tc, chainParser, nil, nil, nil)

	result, err := sendWrite(t, tc, chainParser, server)
	require.Error(t, err)
	require.NotNil(t, result)
	require.Equal(t, "not_sent", writeOutcomeHeader(t, result))
	require.Equal(t, suiSnapshotDigest, resultHeader(result, common.TX_HASH_HEADER_NAME))
}

// TestWriteCopyNotSent pins what proves a write copy was not sent (Part I R-L): the router's own
// dialer failing before a connection opened, read from Go's own error types. A timeout and reply
// text never do.
func TestWriteCopyNotSent(t *testing.T) {
	refusedDial := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	clientError := &url.Error{Op: "Post", URL: "http://node", Err: refusedDial}
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"a refused dial", refusedDial, true},
		{"a refused dial inside the HTTP client's error", clientError, true},
		{"a refused dial the relay classified and wrapped", common.NewRouterErrorWrapping(common.RouterErrorConnectionRefused, fmt.Errorf("http request failed: %w", clientError)), true},
		{"an unreachable network at dial", &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ENETUNREACH)}, true},
		{"a name that does not resolve", &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "node", IsNotFound: true}}, true},
		{"a bare DNS failure", &net.DNSError{Err: "no such host", Name: "node", IsNotFound: true}, true},
		{"a proxy's dial that failed", &net.OpError{Op: "proxyconnect", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}, true},
		{"a dial that timed out", &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}, false},
		{"a DNS lookup that timed out", &net.DNSError{Err: "i/o timeout", Name: "node", IsTimeout: true}, false},
		{"a reset on an open connection", &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}, false},
		{"an unreachable network on an open connection", &net.OpError{Op: "write", Net: "tcp", Err: os.NewSyscallError("write", syscall.ENETUNREACH)}, false},
		{"text that says the connection was refused", errors.New("dial tcp 10.0.0.1:443: connect: connection refused"), false},
		{"no error", nil, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, writeCopyNotSent(c.err))
		})
	}
}

// TestGraphQLWrite_NeverLogsSignedBytes is MAG-5122 test 8 on the relay: a write's signed bytes reach
// no log line, relay_end included, even when a node's reply echoes them back: in a node error's
// message, in a failed status's body, or in a served reply.
func TestGraphQLWrite_NeverLogsSignedBytes(t *testing.T) {
	rand.InitRandomSeed()
	utils.EnableDebugLogBuffer(5000)
	t.Cleanup(utils.DisableDebugLogBuffer)

	rawSignature := make([]byte, 97)
	for i := range rawSignature {
		rawSignature[i] = byte(i*7 + 3)
	}
	signature := base64.StdEncoding.EncodeToString(rawSignature)
	tc := graphQLWriteCase(t)
	tc.body = `{"query":"mutation Submit($tx: Base64!, $sigs: [Base64!]!) { executeTransaction(transactionDataBcs: $tx, signatures: $sigs) { effects { status } } }","variables":{"tx":"` + suiSnapshotTransactionBcs + `","sigs":["` + signature + `"]}}`
	echoedRefusal := `{"data":null,"errors":[{"message":"Invalid BCS-encoded TransactionData: ` + suiSnapshotTransactionBcs + `","path":["executeTransaction"],"extensions":{"code":"BAD_USER_INPUT"}}]}`
	// Not a GraphQL reply, so the relay fails the copy by its status, and the attempt's failure line
	// carries the body (upstream_body).
	echoedFault := `upstream down while sending ` + suiSnapshotTransactionBcs + ` signed ` + signature
	echoedRun := `{"data":{"executeTransaction":{"effects":{"status":"SUCCESS"},"transaction":{"bcs":"` + suiSnapshotTransactionBcs + `","signatures":["` + signature + `"]}}}}`
	for _, c := range []struct {
		name string
		a, b writeUpstream
	}{
		{"a refusal that echoes the transaction", writeUpstream{status: http.StatusOK, body: echoedRefusal}, writeUpstream{status: http.StatusOK, body: echoedRefusal}},
		{"a failed status whose body echoes it", writeUpstream{status: http.StatusServiceUnavailable, body: echoedFault}, writeUpstream{status: http.StatusOK, body: echoedRefusal}},
		{"a served reply that echoes it", writeUpstream{status: http.StatusOK, body: echoedRun}, writeUpstream{refuse: true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			utils.ClearDebugLogBuffer()
			chainParser := tc.parser(t)
			a, b := c.a, c.b
			a.name, b.name = "a-"+t.Name(), "b-"+t.Name()
			_, server := newTwoPrimaryWriteServer(t, tc, chainParser, [2]writeUpstream{a, b})
			result, _ := sendWrite(t, tc, chainParser, server)
			require.NotNil(t, result)

			logged := ""
			for _, line := range utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 0) {
				logged += string(line) + "\n"
			}
			require.Contains(t, logged, `"event":"relay_end"`, "setup: relay_end is in the ring")
			for _, secret := range []string{suiSnapshotTransactionBcs, signature} {
				for start := 0; start+16 <= len(secret); start++ {
					require.NotContains(t, logged, secret[start:start+16], "a piece of the signed bytes reached a log line")
				}
			}
		})
	}
}
