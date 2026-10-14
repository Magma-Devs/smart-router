package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

const consistencyRejectedAllLine = "consistency pre-validation rejected every endpoint selected for this attempt"

// The line an operator reads when every endpoint picked for an attempt is behind the tip. The
// caller picks again, so it must not read as a failed request, and it must name the endpoint and
// its lag: the per-endpoint INFO line is hidden at the chart's default warn level.
func TestConsistencyRejectionLine(t *testing.T) {
	endpointtip.Default().Reset()
	utils.EnableDebugLogBuffer(1000)
	t.Cleanup(utils.DisableDebugLogBuffer)
	utils.ClearDebugLogBuffer()

	endpoint := &lavasession.Endpoint{NetworkAddress: "http://behind:8545"}
	seedEndpointTip("ETH1", "jsonrpc", "http://behind:8545", 700)
	sessions := lavasession.ConsumerSessionsMap{
		"lava@behind": &lavasession.SessionInfo{
			Session: &lavasession.SingleConsumerSession{
				Connection: &lavasession.DirectRPCSessionConnection{Endpoint: endpoint},
			},
		},
	}
	rpcss := &RPCSmartRouterServer{
		listenEndpoint:    &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		consistencyConfig: relaycore.DefaultConsistencyValidationConfig(),
		chainState:        seedChainTip(1000),
	}
	protocolMsg := &MockProtocolMessage{
		api:            &spectypes.Api{Name: "eth_getBalance"},
		requestedBlock: spectypes.LATEST_BLOCK,
		userData:       common.UserData{DappId: "test", ConsumerIp: "1.2.3.4"},
	}

	_, failed, err := rpcss.filterEndpointsByConsistency(context.Background(), sessions, protocolMsg)
	require.ErrorIs(t, err, lavasession.ConsistencyPreValidationError, "the caller keys on the sentinel to pick again")
	require.Len(t, failed, 1)
	// Cross-validation hands this error to the client: the log names the node, the error must not.
	require.Contains(t, err.Error(), "consistency pre-validation")
	require.NotContains(t, err.Error(), "lava@behind")
	require.NotContains(t, err.Error(), "700")

	var line map[string]any
	for _, raw := range utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 1000) {
		record := map[string]any{}
		if json.Unmarshal(raw, &record) != nil {
			continue
		}
		message, _ := record["message"].(string)
		if record["level"] == "error" {
			require.NotContains(t, message, "consistency pre-validation", "a rejection is not a failed request")
		}
		if message == consistencyRejectedAllLine {
			line = record
		}
	}
	require.NotNil(t, line, "the rejection line was not logged")
	require.Equal(t, "warn", line["level"])
	require.Equal(t, "1", fmt.Sprint(line["selected"]), "selected counts this attempt's pick")
	require.Equal(t, "lava@behind latest=700 lag=300", line["rejected"])
	require.Equal(t, "1000", fmt.Sprint(line["chainTip"]))
	require.Equal(t, "10", fmt.Sprint(line["threshold"]))
	for key := range line {
		require.False(t, strings.EqualFold(key, "totalEndpoints"), "totalEndpoints read as the router's endpoint count")
	}
}
