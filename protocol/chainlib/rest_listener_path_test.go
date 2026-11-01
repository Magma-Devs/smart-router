package chainlib

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// TestRestChainListener_LeadingSlashesCollapseBeforeTheRelay pins the listener half of
// MAG-3970. A client whose base URL ends in "/" sends "//cosmos/tx/v1beta1/txs". The
// parser reads the path with url.Parse, which takes "cosmos" as a host and matches
// "/tx/v1beta1/txs" (the Default read API); the sender appends the whole string to the
// node URL. The listener must hand both the same collapsed path, so the write matches
// its spec API and the node receives the path the spec names.
func TestRestChainListener_LeadingSlashesCollapseBeforeTheRelay(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stub := &restHealthRelayStub{}
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	endpoint := &lavasession.RPCEndpoint{
		NetworkAddress:  "127.0.0.1:0",
		ChainID:         "LAV1",
		ApiInterface:    "rest",
		HealthCheckPath: common.DEFAULT_HEALTH_PATH,
	}
	listener := NewRestChainListener(ctx, endpoint, stub, alwaysHealthyReporter{}, logger)
	go listener.Serve(ctx, common.ConsumerCmdFlags{})
	require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, 3*time.Second, 20*time.Millisecond,
		"listener never reported a listening address")
	addr := listener.GetListeningAddress()
	httpClient := &http.Client{Timeout: 3 * time.Second}

	for _, tc := range []struct {
		method      string
		requestPath string
		wantRelayed string // the url the relay sender receives: path plus the query marker
	}{
		{http.MethodPost, "//cosmos/tx/v1beta1/txs", "/cosmos/tx/v1beta1/txs?"},
		{http.MethodGet, "//cosmos/bank/v1beta1/balances/addr1?height=7", "/cosmos/bank/v1beta1/balances/addr1?height=7"},
		{http.MethodGet, "///cosmos/base/tendermint/v1beta1/blocks/latest", "/cosmos/base/tendermint/v1beta1/blocks/latest?"},
		// Only the leading slashes collapse; a host-shaped first segment is still path text.
		{http.MethodPost, "//127.0.0.1:9/cosmos/tx/v1beta1/txs", "/127.0.0.1:9/cosmos/tx/v1beta1/txs?"},
		{http.MethodGet, "/cosmos/bank/v1beta1/balances/addr1", "/cosmos/bank/v1beta1/balances/addr1?"},
	} {
		t.Run(tc.method+" "+tc.requestPath, func(t *testing.T) {
			before := len(stub.seen())
			var body io.Reader
			if tc.method == http.MethodPost {
				body = bytes.NewReader([]byte(`{"tx_bytes":"AA==","mode":"BROADCAST_MODE_SYNC"}`))
			}
			req, err := http.NewRequestWithContext(ctx, tc.method, "http://"+addr+tc.requestPath, body)
			require.NoError(t, err)
			require.Equal(t, tc.requestPath, req.URL.RequestURI(), "the client must send the path as written")
			response, err := httpClient.Do(req)
			require.NoError(t, err)
			responseBody, err := io.ReadAll(response.Body)
			response.Body.Close()
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, response.StatusCode, string(responseBody))

			seen := stub.seen()
			require.Len(t, seen, before+1, "exactly one relay per request")
			require.Equal(t, tc.wantRelayed, seen[before])
		})
	}
}
