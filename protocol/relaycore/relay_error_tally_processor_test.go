package relaycore

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// tallyingGetter is a ChainIdAndApiInterfaceGetter that keeps the router's own node-error count,
// as the router server does (it implements NodeErrorRecorder through the embedded tally).
type tallyingGetter struct{ *RelayErrorTally }

func (tallyingGetter) GetChainIdAndApiInterface() (string, string) { return "testId", "testInterface" }

// MAG-4718: each node error the relay processor meters is also counted in the router's own tally,
// under the same rule as the metric: not on a stateful request, where X-1 of X errors is normal. The
// router's own health check is not a client request and stays out of the tally (review 626-5).
func TestRelayProcessor_CountsEachNodeErrorInTheRoutersOwnTally(t *testing.T) {
	for _, tc := range []struct {
		name        string
		selection   Selection
		healthCheck bool
		counted     bool
	}{
		{"stateless", Stateless, false, true},
		{"stateful is not counted", Stateful, false, false},
		{"the router's own health check is not counted", Stateless, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
			chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, handler, nil, "../../", nil)
			if closeServer != nil {
				defer closeServer()
			}
			require.NoError(t, err)
			chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "", "")
			usedProviders := routersession.NewUsedProviders(nil)
			if tc.healthCheck {
				usedProviders.MarkRouterHealthCheck()
			}
			tally := NewRelayErrorTally()

			relayProcessor := NewRelayProcessor(ctx, nil, RelayProcessorMetrics, tallyingGetter{tally},
				newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, tc.selection))

			lockCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
			defer cancel()
			require.Nil(t, usedProviders.TryLockSelection(lockCtx))
			usedProviders.AddUsed(routersession.ConsumerSessionsMap{"provider@test1": &routersession.SessionInfo{}}, nil)

			// Responses are handled while WaitForResults runs, so the count is read after it returns.
			go sendNodeErrorWithData(relayProcessor, "provider@test1", time.Millisecond,
				[]byte(`{"message":"invalid argument 0: hex string has length 3","code":400}`))
			waitCtx, waitCancel := context.WithTimeout(ctx, 200*time.Millisecond)
			defer waitCancel()
			require.NoError(t, relayProcessor.WaitForResults(waitCtx))

			if !tc.counted {
				require.Empty(t, tally.Counts())
				return
			}
			require.Equal(t, []RelayErrorCount{{
				ChainID: "testId", ApiInterface: "testInterface", ProviderAddress: "provider@test1",
				Method: protocolMessage.GetApi().Name, NodeErrors: 1,
			}}, tally.Counts())
		})
	}
}
