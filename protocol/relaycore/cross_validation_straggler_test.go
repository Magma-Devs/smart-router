package relaycore

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestWatchCrossValidationStragglers pins the MAG-2187 async compare path: responses that arrive after
// the quorum early-exit reply are classified against the reached consensus and reported exactly once per
// pending provider — agreed / disagreed / node-error / protocol-error when a response lands, and
// not-received when the watcher deadline fires first. The watcher runs on the test goroutine; responses
// are staged on the buffered channel up front.
func TestWatchCrossValidationStragglers(t *testing.T) {
	ctx := context.Background()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)
	chainMsg, err := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xDEAD","latest"],"id":1}`), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "127.0.0.1")

	consensusBody := []byte(`{"jsonrpc":"2.0","id":1,"result":"0xAAAA"}`)
	consensusHash := canonicalResponseHash(consensusBody)

	newCVProcessor := func() *RelayProcessor {
		usedProviders := lavasession.NewUsedProviders(nil)
		return NewRelayProcessor(ctx, &common.CrossValidationParams{MaxParticipants: 3, AgreementThreshold: 2}, NewConsistency("ETH1"), RelayProcessorMetrics, RelayProcessorMetrics, RelayRetriesManagerInstance, newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, CrossValidation))
	}
	successResponse := func(provider, group string, body []byte) *RelayResponse {
		return &RelayResponse{
			RelayResult: common.RelayResult{
				Reply:        &pairingtypes.RelayReply{Data: body},
				ProviderInfo: common.ProviderInfo{ProviderAddress: provider, ProviderGroup: group},
				StatusCode:   200,
			},
		}
	}
	collect := func(rp *RelayProcessor, pending []string, maxWait time.Duration) []CrossValidationStragglerResult {
		results := []CrossValidationStragglerResult{}
		rp.WatchCrossValidationStragglers(ctx, pending, consensusHash, maxWait, func(result CrossValidationStragglerResult) {
			results = append(results, result)
		})
		return results
	}

	t.Run("late matching response agrees", func(t *testing.T) {
		rp := newCVProcessor()
		rp.SetResponse(successResponse("p3", "g3", consensusBody))
		results := collect(rp, []string{"p3"}, time.Second)
		require.Len(t, results, 1)
		require.Equal(t, "p3", results[0].ProviderAddress)
		require.Equal(t, "g3", results[0].ProviderGroup)
		require.Equal(t, common.CrossValidationStragglerOutcomeAgreed, results[0].Outcome)
	})

	t.Run("key order does not break agreement (canonicalization parity)", func(t *testing.T) {
		rp := newCVProcessor()
		rp.SetResponse(successResponse("p3", "g3", []byte(`{"result":"0xAAAA","id":1,"jsonrpc":"2.0"}`)))
		results := collect(rp, []string{"p3"}, time.Second)
		require.Len(t, results, 1)
		require.Equal(t, common.CrossValidationStragglerOutcomeAgreed, results[0].Outcome)
	})

	t.Run("late conflicting response disagrees", func(t *testing.T) {
		rp := newCVProcessor()
		rp.SetResponse(successResponse("p3", "g3", []byte(`{"jsonrpc":"2.0","id":1,"result":"0xBBBB"}`)))
		results := collect(rp, []string{"p3"}, time.Second)
		require.Len(t, results, 1)
		require.Equal(t, common.CrossValidationStragglerOutcomeDisagreed, results[0].Outcome)
	})

	t.Run("late node error is not content dissent", func(t *testing.T) {
		rp := newCVProcessor()
		rp.SetResponse(successResponse("p3", "g3", []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}`)))
		results := collect(rp, []string{"p3"}, time.Second)
		require.Len(t, results, 1)
		require.Equal(t, common.CrossValidationStragglerOutcomeNodeError, results[0].Outcome)
	})

	t.Run("late relay failure is a protocol error", func(t *testing.T) {
		rp := newCVProcessor()
		rp.SetResponse(&RelayResponse{
			RelayResult: common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: "p3", ProviderGroup: "g3"}},
			Err:         errors.New("connection reset"),
		})
		results := collect(rp, []string{"p3"}, time.Second)
		require.Len(t, results, 1)
		require.Equal(t, common.CrossValidationStragglerOutcomeProtocolError, results[0].Outcome)
	})

	t.Run("deadline reports not-received", func(t *testing.T) {
		rp := newCVProcessor()
		results := collect(rp, []string{"p2", "p3"}, 50*time.Millisecond)
		require.Len(t, results, 2)
		for _, result := range results {
			require.Equal(t, common.CrossValidationStragglerOutcomeNotReceived, result.Outcome)
		}
	})

	t.Run("cancelled context reports not-received", func(t *testing.T) {
		rp := newCVProcessor()
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()
		results := []CrossValidationStragglerResult{}
		rp.WatchCrossValidationStragglers(cancelledCtx, []string{"p3"}, consensusHash, time.Minute, func(result CrossValidationStragglerResult) {
			results = append(results, result)
		})
		require.Len(t, results, 1)
		require.Equal(t, common.CrossValidationStragglerOutcomeNotReceived, results[0].Outcome)
	})

	t.Run("response buffered at the deadline is classified, not dropped", func(t *testing.T) {
		rp := newCVProcessor()
		rp.SetResponse(successResponse("p3", "g3", consensusBody))
		// maxWait 0 means the timer is (effectively) already expired: even when select takes the
		// timer case over the ready channel, the give-up path must first drain the buffered
		// response and classify it by content — never misreport an arrived response as
		// not-received (select picks among ready cases pseudo-randomly).
		results := collect(rp, []string{"p3"}, 0)
		require.Len(t, results, 1)
		require.Equal(t, common.CrossValidationStragglerOutcomeAgreed, results[0].Outcome)
	})

	t.Run("cancelled context still classifies buffered responses", func(t *testing.T) {
		rp := newCVProcessor()
		rp.SetResponse(successResponse("p2", "g2", []byte(`{"jsonrpc":"2.0","id":1,"result":"0xBBBB"}`)))
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()
		results := []CrossValidationStragglerResult{}
		rp.WatchCrossValidationStragglers(cancelledCtx, []string{"p2", "p3"}, consensusHash, time.Minute, func(result CrossValidationStragglerResult) {
			results = append(results, result)
		})
		require.Len(t, results, 2)
		outcomes := map[string]string{}
		for _, result := range results {
			outcomes[result.ProviderAddress] = result.Outcome
		}
		require.Equal(t, common.CrossValidationStragglerOutcomeDisagreed, outcomes["p2"], "buffered dissent must be classified even on cancel")
		require.Equal(t, common.CrossValidationStragglerOutcomeNotReceived, outcomes["p3"], "only the provider with no response is not-received")
	})

	t.Run("non-pending responses are consumed and dropped", func(t *testing.T) {
		rp := newCVProcessor()
		// An earlier-batch straggler not covered by the pending header must not produce a record.
		rp.SetResponse(successResponse("earlier-batch-provider", "g0", []byte(`{"jsonrpc":"2.0","id":1,"result":"0xCCCC"}`)))
		rp.SetResponse(successResponse("p3", "g3", consensusBody))
		results := collect(rp, []string{"p3"}, time.Second)
		require.Len(t, results, 1)
		require.Equal(t, "p3", results[0].ProviderAddress)
		require.Equal(t, common.CrossValidationStragglerOutcomeAgreed, results[0].Outcome)
	})

	t.Run("response consumed by another reader is resolved from recorded results", func(t *testing.T) {
		rp := newCVProcessor()
		// Simulate the dying state-machine reader (sole-reader race): it consumed the straggler's
		// response off the channel and recorded it via handleResponse before the watcher started.
		rp.SetResponse(successResponse("p3", "g3", []byte(`{"jsonrpc":"2.0","id":1,"result":"0xBBBB"}`)))
		require.Len(t, rp.NodeResults(), 1) // drains the channel into the results manager
		// The channel is now empty, but the watcher must classify p3 from the recorded result —
		// immediately, not after burning maxWait — instead of reporting not-received.
		results := collect(rp, []string{"p3"}, time.Minute)
		require.Len(t, results, 1)
		require.Equal(t, "p3", results[0].ProviderAddress)
		require.Equal(t, common.CrossValidationStragglerOutcomeDisagreed, results[0].Outcome)
	})

	t.Run("multiple stragglers each resolve once", func(t *testing.T) {
		rp := newCVProcessor()
		rp.SetResponse(successResponse("p2", "g2", []byte(`{"jsonrpc":"2.0","id":1,"result":"0xBBBB"}`)))
		rp.SetResponse(successResponse("p3", "g3", consensusBody))
		results := collect(rp, []string{"p2", "p3"}, time.Second)
		require.Len(t, results, 2)
		outcomes := map[string]string{}
		for _, result := range results {
			outcomes[result.ProviderAddress] = result.Outcome
		}
		require.Equal(t, common.CrossValidationStragglerOutcomeDisagreed, outcomes["p2"])
		require.Equal(t, common.CrossValidationStragglerOutcomeAgreed, outcomes["p3"])
	})
}
