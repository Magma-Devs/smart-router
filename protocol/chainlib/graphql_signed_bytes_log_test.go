package chainlib

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

// TestGraphQLListener_NeverLogsSignedBytes is MAG-5122 test 8 on the listener: a GraphQL write's
// signed bytes reach none of its log lines, --debug-relays included. That covers the request line,
// the failure lines of a write that failed, and the debug-level in/out line of one that was served,
// whose reply here echoes the transaction back.
func TestGraphQLListener_NeverLogsSignedBytes(t *testing.T) {
	utils.EnableDebugLogBuffer(1000)
	t.Cleanup(utils.DisableDebugLogBuffer)

	const transaction = "AAACAAgqAAAAAAAAAAAgp7AycDh4qnTDEmk1eJ/R1NfhEdWRGwkkfWljBhwxK1oCAgABAQAAAQECAAABAQD8zJpCG7sTwaZqGqmPCtdQKe3pSFd3nGkVtE+UBouSHgHhFa5/+bVpRPr+9v0BXVEKL9ljttQuyKYmZAnA6kD7NgMAAAAAAAAAIEzrkQF+I9IJ4x0c30+YdFP63Y1T+vAeA9YRM4e19EVk/MyaQhu7E8GmahqpjwrXUCnt6UhXd5xpFbRPlAaLkh7oAwAAAAAAAADyBSoBAAAAAA=="
	rawSignature := make([]byte, 97)
	for i := range rawSignature {
		rawSignature[i] = byte(i*7 + 3)
	}
	signature := base64.StdEncoding.EncodeToString(rawSignature)
	body := `{"query":"mutation Submit($tx: Base64!, $sigs: [Base64!]!) { executeTransaction(transactionDataBcs: $tx, signatures: $sigs) { effects { status } } }","variables":{"tx":"` + transaction + `","sigs":["` + signature + `"]}}`
	echo := `{"data":{"executeTransaction":{"effects":{"status":"SUCCESS"},"transaction":{"bcs":"` + transaction + `","signatures":["` + signature + `"]}}}}`

	newGraphQL := func(ctx context.Context, endpoint *routersession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
		return NewGraphQLChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger)
	}
	for _, debugRelays := range []bool{false, true} {
		for _, relay := range []struct {
			name   string
			sender RelaySender
		}{
			{"failed", failingRelay{}},
			{"failed with an upstream's answer", upstreamAnswerRelay{status: http.StatusServiceUnavailable, body: []byte(echo)}},
			{"served", &stubRelaySender{reply: []byte(echo)}},
		} {
			name := relay.name
			if debugRelays {
				name += " with --debug-relays"
			}
			t.Run(name, func(t *testing.T) {
				utils.ClearDebugLogBuffer()
				addr := serveWithFlags(t, newGraphQL, spectypes.APIInterfaceGraphQL, relay.sender, common.ConsumerCmdFlags{DebugRelays: debugRelays})
				resp, err := http.Post("http://"+addr+"/", "application/json", strings.NewReader(body))
				require.NoError(t, err)
				_, _ = io.Copy(io.Discard, resp.Body)
				require.NoError(t, resp.Body.Close())

				lines := utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 0)
				logged := ""
				for _, line := range lines {
					logged += string(line) + "\n"
				}
				require.Contains(t, logged, "Consumer received a new GraphQL request", "setup: the request line is in the ring")
				require.Contains(t, logged, "graphql in/out", "setup: the in/out line is in the ring")
				require.Contains(t, logged, "[REDACTED:", "the bytes are replaced, not dropped silently")
				for _, secret := range []string{transaction, signature} {
					for start := 0; start+16 <= len(secret); start++ {
						require.NotContains(t, logged, secret[start:start+16], "a piece of the signed bytes reached a log line")
					}
				}
			})
		}
	}
}

// upstreamAnswerRelay fails every request with an upstream's own answer, which the listener sends
// back as is (MAG-4481) and writes on its failure line.
type upstreamAnswerRelay struct {
	failingRelay
	status int
	body   []byte
}

func (r upstreamAnswerRelay) SendRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, analytics *metrics.RelayMetrics, metadataValues []pairingtypes.Metadata) (*common.RelayResult, error) {
	return &common.RelayResult{ClientFacing: &common.ClientFacingError{UpstreamStatus: r.status, UpstreamBody: r.body}}, errors.New("no upstream served the request")
}
