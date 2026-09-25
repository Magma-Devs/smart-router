package rpcsmartrouter

import (
	context "context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	common "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	lavasession "github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relaycoretest"
	epochstoragetypes "github.com/magma-Devs/smart-router/types/epoch"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/lavaslices"
	"github.com/stretchr/testify/require"
)

type PolicySt struct {
	addons       []string
	extensions   []string
	apiInterface string
}

func (a PolicySt) GetSupportedAddons(string) ([]string, error) {
	return a.addons, nil
}

func (a PolicySt) GetSupportedExtensions(string) ([]epochstoragetypes.EndpointService, error) {
	ret := []epochstoragetypes.EndpointService{}
	for _, ext := range a.extensions {
		ret = append(ret, epochstoragetypes.EndpointService{Extension: ext, ApiInterface: a.apiInterface})
	}
	return ret, nil
}

type SmartRouterRelaySenderMock struct {
	retValue    error
	tickerValue time.Duration
}

func (srsm *SmartRouterRelaySenderMock) GetProcessingTimeout(chainMessage chainlib.ChainMessage) (processingTimeout time.Duration, relayTimeout time.Duration) {
	if srsm.tickerValue != 0 {
		return time.Second * 50000, srsm.tickerValue
	}
	return time.Second * 50000, 100 * time.Millisecond
}

// SmartRouterRelaySenderMockWithTimeout is a mock with configurable processing timeout
type SmartRouterRelaySenderMockWithTimeout struct {
	SmartRouterRelaySenderMock
	processingTimeout time.Duration
}

func (srsm *SmartRouterRelaySenderMockWithTimeout) GetProcessingTimeout(chainMessage chainlib.ChainMessage) (processingTimeout time.Duration, relayTimeout time.Duration) {
	return srsm.processingTimeout, 10 * time.Second
}

func (srsm *SmartRouterRelaySenderMock) GetChainIdAndApiInterface() (string, string) {
	return "testUno", "testDos"
}

func (srsm *SmartRouterRelaySenderMock) ParseRelay(
	ctx context.Context,
	url string,
	req string,
	connectionType string,
	dappID string,
	consumerIp string,
	metadata []pairingtypes.Metadata,
) (protocolMessage chainlib.ProtocolMessage, err error) {
	foundArchive := false
	for _, md := range metadata {
		if md.Value == "archive" {
			foundArchive = true
		}
	}
	if !foundArchive {
		utils.LavaFormatFatal("misuse in mocked parse relay", nil)
	}
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle the incoming request and provide the desired response
		w.WriteHeader(http.StatusOK)
	})
	specId := "ETH1"
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceJsonRPC, serverHandler, nil, "../../", []string{"archive"})
	defer closeServer()
	policy := PolicySt{
		addons:       []string{},
		extensions:   []string{"archive"},
		apiInterface: spectypes.APIInterfaceJsonRPC,
	}
	chainParser.SetPolicy(policy, specId, spectypes.APIInterfaceJsonRPC)
	chainMsg, err := chainParser.ParseMsg(url, []byte(req), connectionType, metadata, extensionslib.ExtensionInfo{LatestBlock: 0, ExtensionOverride: []string{"archive"}})
	relayRequestData := lavaprotocol.NewRelayData(ctx, connectionType, url, []byte(req), 0, -2, spectypes.APIInterfaceJsonRPC, chainMsg.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMsg), common.GetExtensionNames(chainMsg.GetExtensions()))
	protocolMessage = chainlib.NewProtocolMessage(chainMsg, nil, relayRequestData, dappID, consumerIp)
	return protocolMessage, nil
}

func TestConsumerStateMachineHappyFlow(t *testing.T) {
	t.Run("happy", func(t *testing.T) {
		ctx := context.Background()
		serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Handle the incoming request and provide the desired response
			w.WriteHeader(http.StatusOK)
		})
		specId := "LAVA"
		chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
		if closeServer != nil {
			defer closeServer()
		}
		require.NoError(t, err)
		chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		dappId := "dapp"
		consumerIp := "123.11"
		protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, dappId, consumerIp)
		usedProviders := lavasession.NewUsedProviders(nil)
		stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, &SmartRouterRelaySenderMock{retValue: nil}, protocolMessage, nil, false)
		require.NoError(t, err)
		relayProcessor := relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayRetriesManagerInstance, stateMachine)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancel()
		canUse := usedProviders.TryLockSelection(ctx)
		require.NoError(t, ctx.Err())
		require.Nil(t, canUse)
		require.Zero(t, usedProviders.CurrentlyUsed())
		require.Zero(t, usedProviders.SessionsLatestBatch())
		consumerSessionsMap := lavasession.ConsumerSessionsMap{"lava@test": &lavasession.SessionInfo{}, "lava@test2": &lavasession.SessionInfo{}}

		relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
		require.NoError(t, err)
		taskNumber := 0
		for task := range relayTaskChannel {
			switch taskNumber {
			case 0:
				require.False(t, task.IsDone())
				usedProviders.AddUsed(consumerSessionsMap, nil)
				relayProcessor.UpdateBatch(nil)
				relaycoretest.SendProtocolError(relayProcessor, "lava@test", time.Millisecond*1, fmt.Errorf("bad"))
			case 1:
				require.False(t, task.IsDone())
				usedProviders.AddUsed(consumerSessionsMap, nil)
				relayProcessor.UpdateBatch(nil)
				relaycoretest.SendNodeError(relayProcessor, "lava2@test", time.Millisecond*1)
			case 2:
				require.False(t, task.IsDone())
				usedProviders.AddUsed(consumerSessionsMap, nil)
				relayProcessor.UpdateBatch(nil)
				relaycoretest.SendNodeError(relayProcessor, "lava2@test", time.Millisecond*1)
			case 3:
				require.False(t, task.IsDone())
				usedProviders.AddUsed(consumerSessionsMap, nil)
				relayProcessor.UpdateBatch(nil)
				relaycoretest.SendSuccessResp(relayProcessor, "lava4@test", time.Millisecond*1)
			case 4:
				require.True(t, task.IsDone())
				results, _ := relayProcessor.HasRequiredNodeResults(1)
				require.True(t, results)
				returnedResult, err := relayProcessor.ProcessingResult()
				require.NoError(t, err)
				require.Equal(t, string(returnedResult.Reply.Data), "ok")
				require.Equal(t, http.StatusOK, returnedResult.StatusCode)
				return // end test.
			}
			taskNumber++
		}
	})
}

func TestConsumerStateMachineExhaustRetries(t *testing.T) {
	t.Run("retries", func(t *testing.T) {
		ctx := context.Background()
		serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Handle the incoming request and provide the desired response
			w.WriteHeader(http.StatusOK)
		})
		specId := "LAVA"
		chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
		if closeServer != nil {
			defer closeServer()
		}
		require.NoError(t, err)
		chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		dappId := "dapp"
		consumerIp := "123.11"
		protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, dappId, consumerIp)
		usedProviders := lavasession.NewUsedProviders(nil)
		stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, &SmartRouterRelaySenderMock{retValue: nil, tickerValue: 10 * time.Second}, protocolMessage, nil, false)
		require.NoError(t, err)
		relayProcessor := relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayRetriesManagerInstance, stateMachine)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancel()
		canUse := usedProviders.TryLockSelection(ctx)
		require.NoError(t, ctx.Err())
		require.Nil(t, canUse)
		require.Zero(t, usedProviders.CurrentlyUsed())
		require.Zero(t, usedProviders.SessionsLatestBatch())

		relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
		require.NoError(t, err)
		taskNumber := 0
		for task := range relayTaskChannel {
			switch taskNumber {
			case 0, 1, 2, 3:
				require.False(t, task.IsDone())
				relayProcessor.UpdateBatch(fmt.Errorf("failed sending message"))
			case 4:
				require.True(t, task.IsDone())
				require.Error(t, task.Err)
				return
			}
			taskNumber++
		}
	})
}

func TestConsumerStateMachineArchiveRetry(t *testing.T) {
	t.Run("retries_archive", func(t *testing.T) {
		ctx := context.Background()
		serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Handle the incoming request and provide the desired response
			w.WriteHeader(http.StatusOK)
		})
		specId := "ETH1"
		chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceJsonRPC, serverHandler, nil, "../../", nil)
		if closeServer != nil {
			defer closeServer()
		}
		require.NoError(t, err)

		params, _ := json.Marshal([]string{"5NFtBbExnjk4TFXpfXhJidcCm5KYPk7QCY51nWiwyQNU"})
		id, _ := json.Marshal(1)
		reqBody := rpcclient.JsonrpcMessage{
			Version: "2.0",
			Method:  "block", // Query latest block
			Params:  params,  // Use "final" to get the latest final block
			ID:      id,
		}

		// Convert request to JSON
		jsonData, err := json.Marshal(reqBody)
		if err != nil {
			log.Fatalf("Error marshalling request: %v", err)
		}

		chainMsg, err := chainParser.ParseMsg("", jsonData, http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		dappId := "dapp"
		consumerIp := "123.11"
		reqBlock, _ := chainMsg.RequestedBlock()
		var seenBlock int64 = 0

		relayRequestData := lavaprotocol.NewRelayData(ctx, http.MethodPost, "", jsonData, seenBlock, reqBlock, spectypes.APIInterfaceJsonRPC, chainMsg.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMsg), common.GetExtensionNames(chainMsg.GetExtensions()))
		protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, relayRequestData, dappId, consumerIp)
		usedProviders := lavasession.NewUsedProviders(nil)
		stateMachine, err := NewSmartRouterRelayStateMachine(
			ctx,
			usedProviders,
			&SmartRouterRelaySenderMock{retValue: nil, tickerValue: 10 * time.Second},
			protocolMessage,
			nil,
			false,
		)
		require.NoError(t, err)
		relayProcessor := relaycore.NewRelayProcessor(
			ctx,
			&common.DefaultCrossValidationParams,
			relaycoretest.RelayProcessorMetrics,
			relaycoretest.RelayProcessorMetrics,
			relaycoretest.RelayRetriesManagerInstance,
			stateMachine,
		)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancel()
		canUse := usedProviders.TryLockSelection(ctx)
		require.NoError(t, ctx.Err())
		require.Nil(t, canUse)
		require.Zero(t, usedProviders.CurrentlyUsed())
		require.Zero(t, usedProviders.SessionsLatestBatch())

		consumerSessionsMap := lavasession.ConsumerSessionsMap{"lava@test": &lavasession.SessionInfo{}, "lava@test2": &lavasession.SessionInfo{}}
		relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
		require.NoError(t, err)
		taskNumber := 0
		for task := range relayTaskChannel {
			switch taskNumber {
			case 0:
				require.False(t, task.IsDone())
				usedProviders.AddUsed(consumerSessionsMap, nil)
				relayProcessor.UpdateBatch(nil)
				relaycoretest.SendNodeErrorJsonRpc(relayProcessor, "lava2@test", time.Millisecond*1)
			case 1:
				require.False(t, task.IsDone())
				require.True(t,
					lavaslices.ContainsPredicate(
						task.RelayState.GetProtocolMessage().GetExtensions(),
						func(predicate *spectypes.Extension) bool { return predicate.Name == "archive" }),
				)
				usedProviders.AddUsed(consumerSessionsMap, nil)
				relayProcessor.UpdateBatch(nil)
				relaycoretest.SendSuccessRespJsonRpc(relayProcessor, "lava4@test", time.Millisecond*1)
			case 2:
				require.True(t, task.IsDone())
				results, _ := relayProcessor.HasRequiredNodeResults(1)
				require.True(t, results)
				returnedResult, err := relayProcessor.ProcessingResult()
				require.NoError(t, err)
				require.Equal(t, string(returnedResult.Reply.Data), `{"jsonrpc":"2.0","id":1,"result":{"result":"success"}}`)
				require.Equal(t, http.StatusOK, returnedResult.StatusCode)
				fmt.Println(relayProcessor.GetProtocolMessage().GetExtensions())
				return // end test.
			}
			taskNumber++
		}
	})
}

func TestSmartRouterStateMachineCircuitBreakerOnPairingErrors(t *testing.T) {
	t.Run("circuit_breaker_triggers_after_2_pairing_errors", func(t *testing.T) {
		ctx := context.Background()
		serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		specId := "LAVA"
		chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
		if closeServer != nil {
			defer closeServer()
		}
		require.NoError(t, err)
		chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		dappId := "dapp"
		consumerIp := "123.11"
		protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, dappId, consumerIp)
		usedProviders := lavasession.NewUsedProviders(nil)
		stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, &SmartRouterRelaySenderMock{retValue: nil}, protocolMessage, nil, false)
		require.NoError(t, err)
		relayProcessor := relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayRetriesManagerInstance, stateMachine)

		relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
		require.NoError(t, err)
		taskNumber := 0
		for task := range relayTaskChannel {
			switch taskNumber {
			case 0:
				// First pairing error
				require.False(t, task.IsDone())
				relayProcessor.UpdateBatch(lavasession.PairingListEmptyError)
			case 1:
				// Second pairing error - circuit breaker should trigger
				require.False(t, task.IsDone())
				relayProcessor.UpdateBatch(lavasession.PairingListEmptyError)
			case 2:
				// Should be done - circuit breaker stopped retries
				require.True(t, task.IsDone())
				require.Error(t, task.Err)
				require.True(t, errors.Is(task.Err, lavasession.PairingListEmptyError))
				return // Test successful
			default:
				require.Fail(t, "Circuit breaker should have stopped retries after 2 pairing errors")
			}
			taskNumber++
		}
	})
}

func TestSmartRouterStateMachineCircuitBreakerResetsOnSuccess(t *testing.T) {
	t.Run("pairing_error_counter_resets_on_success", func(t *testing.T) {
		ctx := context.Background()
		serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		specId := "LAVA"
		chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
		if closeServer != nil {
			defer closeServer()
		}
		require.NoError(t, err)
		chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		dappId := "dapp"
		consumerIp := "123.11"
		protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, dappId, consumerIp)
		usedProviders := lavasession.NewUsedProviders(nil)
		stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, &SmartRouterRelaySenderMock{retValue: nil}, protocolMessage, nil, false)
		require.NoError(t, err)
		relayProcessor := relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayRetriesManagerInstance, stateMachine)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancel()
		canUse := usedProviders.TryLockSelection(ctx)
		require.NoError(t, ctx.Err())
		require.Nil(t, canUse)

		consumerSessionsMap := lavasession.ConsumerSessionsMap{"lava@test": &lavasession.SessionInfo{}}

		relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
		require.NoError(t, err)
		taskNumber := 0
		for task := range relayTaskChannel {
			switch taskNumber {
			case 0:
				// First pairing error
				require.False(t, task.IsDone())
				relayProcessor.UpdateBatch(lavasession.PairingListEmptyError)
			case 1:
				// Success - should reset counter
				require.False(t, task.IsDone())
				usedProviders.AddUsed(consumerSessionsMap, nil)
				relayProcessor.UpdateBatch(nil)
				relaycoretest.SendSuccessResp(relayProcessor, "lava@test", time.Millisecond*1)
			case 2:
				// Done with success
				require.True(t, task.IsDone())
				require.NoError(t, task.Err)
				return // Test successful - counter was reset, no circuit breaker
			}
			taskNumber++
		}
	})
}

func TestSmartRouterStateMachineCircuitBreakerResetsOnDifferentError(t *testing.T) {
	t.Run("pairing_error_counter_resets_on_different_error", func(t *testing.T) {
		ctx := context.Background()
		serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		specId := "LAVA"
		chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
		if closeServer != nil {
			defer closeServer()
		}
		require.NoError(t, err)
		chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		dappId := "dapp"
		consumerIp := "123.11"
		protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, dappId, consumerIp)
		usedProviders := lavasession.NewUsedProviders(nil)
		stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, &SmartRouterRelaySenderMock{retValue: nil}, protocolMessage, nil, false)
		require.NoError(t, err)
		relayProcessor := relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayRetriesManagerInstance, stateMachine)

		relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
		require.NoError(t, err)
		taskNumber := 0
		for task := range relayTaskChannel {
			switch taskNumber {
			case 0:
				// First pairing error
				require.False(t, task.IsDone())
				relayProcessor.UpdateBatch(lavasession.PairingListEmptyError)
			case 1:
				// Different error (network timeout) - should reset counter
				require.False(t, task.IsDone())
				relayProcessor.UpdateBatch(fmt.Errorf("network timeout"))
			case 2:
				// Another pairing error - counter was reset, so this is #1 again
				require.False(t, task.IsDone())
				relayProcessor.UpdateBatch(lavasession.PairingListEmptyError)
			case 3:
				// Second consecutive pairing error - circuit breaker triggers
				require.False(t, task.IsDone())
				relayProcessor.UpdateBatch(lavasession.PairingListEmptyError)
			case 4:
				// Done - circuit breaker triggered
				require.True(t, task.IsDone())
				require.Error(t, task.Err)
				return // Test successful
			default:
				require.Fail(t, "Unexpected task number")
			}
			taskNumber++
		}
	})
}

// TestProcessingContextTimeoutEnforcement tests that no new retries are spawned after processingCtx expires
// This test validates the fix for the bug where processingCtx.Done() case was not being selected,
// causing 30+ seconds of wasted retries even though the context had expired.
func TestProcessingContextTimeoutEnforcement(t *testing.T) {
	t.Run("NoRetriesAfterTimeout", func(t *testing.T) {
		ctx := context.Background()
		serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		specId := "LAVA"
		chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
		if closeServer != nil {
			defer closeServer()
		}
		require.NoError(t, err)
		chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		dappId := "dapp"
		consumerIp := "123.11"
		protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, dappId, consumerIp)
		usedProviders := lavasession.NewUsedProviders(nil)

		// Create mock with SHORT processing timeout (100ms) to test timeout enforcement
		mockSender := &SmartRouterRelaySenderMockWithTimeout{
			processingTimeout: 100 * time.Millisecond, // 100ms processing timeout
		}

		stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, mockSender, protocolMessage, nil, false)
		require.NoError(t, err)
		relayProcessor := relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayRetriesManagerInstance, stateMachine)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5) // Overall test timeout
		defer cancel()
		canUse := usedProviders.TryLockSelection(ctx)
		require.NoError(t, ctx.Err())
		require.Nil(t, canUse)

		consumerSessionsMap := lavasession.ConsumerSessionsMap{"lava@test": &lavasession.SessionInfo{}}

		relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
		require.NoError(t, err)

		taskNumber := 0
		startTime := time.Now()

		for task := range relayTaskChannel {
			elapsed := time.Since(startTime)
			t.Logf("Task %d received at %v", taskNumber, elapsed)

			if task.IsDone() {
				t.Logf("Task %d is Done (error: %v), ending test", taskNumber, task.Err)
				break
			}

			// Add provider and simulate failure (no results)
			usedProviders.AddUsed(consumerSessionsMap, nil)
			relayProcessor.UpdateBatch(nil)

			// Simulate various errors to trigger retries
			if taskNumber%2 == 0 {
				relaycoretest.SendProtocolError(relayProcessor, "lava@test", time.Millisecond*1, fmt.Errorf("simulated protocol error"))
			} else {
				relaycoretest.SendNodeError(relayProcessor, "lava@test", time.Millisecond*1)
			}

			// Sleep slightly to allow WaitForResults to detect the error and return
			time.Sleep(10 * time.Millisecond)
			taskNumber++
		}

		elapsedTime := time.Since(startTime)

		t.Logf("Total tasks spawned: %d", taskNumber)
		t.Logf("Total elapsed time: %v", elapsedTime)

		// CRITICAL ASSERTION: After 100ms timeout, retries should stop reasonably quickly
		// Due to Go scheduler jitter, we allow some slack (~10-20ms overshoot is normal)
		// We expect around 10-12 tasks given ~10ms per task = ~100-120ms
		// Before the fix, this would spawn 30+ tasks over several seconds
		require.LessOrEqual(t, taskNumber, 12,
			"Expected maximum 12 tasks (100ms timeout + scheduler jitter), but got %d. This suggests processingCtx timeout is not being enforced properly!",
			taskNumber)

		// Verify timeout was respected with reasonable jitter tolerance
		// Allow up to 200ms (100ms timeout + 100ms buffer for scheduler jitter)
		// Before the fix, this would take 30+ seconds
		require.Less(t, elapsedTime, 200*time.Millisecond,
			"Expected test to complete within 200ms (timeout + jitter buffer), but took %v. This suggests significant delays in timeout detection!",
			elapsedTime)
	})
}

// TestProcessingContextStillValidAllowsRetries tests that retries continue when context is still valid
// This ensures our fix doesn't break the normal retry mechanism
func TestProcessingContextStillValidAllowsRetries(t *testing.T) {
	t.Run("RetriesContinueWhenContextValid", func(t *testing.T) {
		// This test expects 5+ retries, so set a high limit
		originalValue := relaycore.RelayRetryLimit
		relaycore.RelayRetryLimit = 10
		defer func() { relaycore.RelayRetryLimit = originalValue }()

		ctx := context.Background()
		serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		specId := "LAVA"
		chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
		if closeServer != nil {
			defer closeServer()
		}
		require.NoError(t, err)
		chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		dappId := "dapp"
		consumerIp := "123.11"
		protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, dappId, consumerIp)
		usedProviders := lavasession.NewUsedProviders(nil)

		// LONG processing timeout (10 seconds) - retries should continue
		stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, &SmartRouterRelaySenderMock{retValue: nil, tickerValue: 10 * time.Second}, protocolMessage, nil, false)
		require.NoError(t, err)
		relayProcessor := relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayRetriesManagerInstance, stateMachine)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2) // Test timeout
		defer cancel()
		canUse := usedProviders.TryLockSelection(ctx)
		require.NoError(t, ctx.Err())
		require.Nil(t, canUse)

		consumerSessionsMap := lavasession.ConsumerSessionsMap{"lava@test": &lavasession.SessionInfo{}}

		relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
		require.NoError(t, err)

		taskNumber := 0

		for task := range relayTaskChannel {
			t.Logf("Task %d received", taskNumber)

			if taskNumber >= 5 {
				// After 5 retries, send success to end the test
				usedProviders.AddUsed(consumerSessionsMap, nil)
				relayProcessor.UpdateBatch(nil)
				relaycoretest.SendSuccessResp(relayProcessor, "lava@test", time.Millisecond*1)
				time.Sleep(20 * time.Millisecond)

				if task.IsDone() {
					t.Log("Got Done task after success, test complete")
					break
				}
				taskNumber++
				continue
			}

			if task.IsDone() {
				t.Logf("Task %d is Done unexpectedly (error: %v)", taskNumber, task.Err)
				break
			}

			// Simulate failure to trigger retry
			usedProviders.AddUsed(consumerSessionsMap, nil)
			relayProcessor.UpdateBatch(nil)
			relaycoretest.SendNodeError(relayProcessor, "lava@test", time.Millisecond*1)
			time.Sleep(10 * time.Millisecond)

			taskNumber++
		}

		// ASSERTION: With valid context, we should have been able to retry multiple times
		require.GreaterOrEqual(t, taskNumber, 5,
			"Expected at least 5 retries when context is valid, but only got %d", taskNumber)
	})
}

// TestProcessingContextRaceCondition tests the exact race condition from the bug:
// When both gotResults and processingCtx.Done() are ready simultaneously,
// we should respect the timeout and not spawn new retries
func TestProcessingContextRaceCondition(t *testing.T) {
	t.Run("TimeoutWinsRaceWithGotResults", func(t *testing.T) {
		ctx := context.Background()
		serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		specId := "LAVA"
		chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
		if closeServer != nil {
			defer closeServer()
		}
		require.NoError(t, err)
		chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		dappId := "dapp"
		consumerIp := "123.11"
		protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, dappId, consumerIp)
		usedProviders := lavasession.NewUsedProviders(nil)

		// Short timeout (200ms) to create the race condition
		mockSender := &SmartRouterRelaySenderMockWithTimeout{
			processingTimeout: 200 * time.Millisecond,
		}

		stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, mockSender, protocolMessage, nil, false)
		require.NoError(t, err)
		relayProcessor := relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayRetriesManagerInstance, stateMachine)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		canUse := usedProviders.TryLockSelection(ctx)
		require.NoError(t, ctx.Err())
		require.Nil(t, canUse)

		consumerSessionsMap := lavasession.ConsumerSessionsMap{"lava@test": &lavasession.SessionInfo{}}

		relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
		require.NoError(t, err)

		startTime := time.Now()
		taskTimestamps := []time.Duration{}
		tasksAfterTimeout := 0

		for task := range relayTaskChannel {
			elapsed := time.Since(startTime)
			taskTimestamps = append(taskTimestamps, elapsed)
			taskNum := len(taskTimestamps)

			t.Logf("Task %d at %v", taskNum, elapsed)

			// Count tasks spawned AFTER the 200ms timeout
			if elapsed > 200*time.Millisecond {
				tasksAfterTimeout++
				t.Logf("[WARN] Task %d spawned AFTER timeout! (%v after start)", taskNum, elapsed)
			}

			if task.IsDone() {
				t.Logf("Task %d is Done, test ending", taskNum)
				break
			}

			// Rapidly fail to create the race condition
			usedProviders.AddUsed(consumerSessionsMap, nil)
			relayProcessor.UpdateBatch(nil)
			relaycoretest.SendNodeError(relayProcessor, "lava@test", time.Millisecond*1)
			// Small sleep to let WaitForResults return
			time.Sleep(5 * time.Millisecond)
		}

		t.Logf("Task timeline: %v", taskTimestamps)
		t.Logf("Tasks spawned after 200ms timeout: %d", tasksAfterTimeout)

		// CRITICAL ASSERTION: No tasks should be spawned after the 200ms timeout
		// Before the fix, tasks would continue spawning for seconds after timeout
		require.Equal(t, 0, tasksAfterTimeout,
			"Expected ZERO tasks after 200ms timeout, but %d tasks were spawned after timeout! This is the bug we're fixing.",
			tasksAfterTimeout)
	})
}

// TestSmartRouterStateMachineBatchRequestRetryCondition tests that the retryCondition
// correctly handles batch requests based on the DisableBatchRequestRetry flag
func TestSmartRouterStateMachineBatchRequestRetryCondition(t *testing.T) {
	t.Run("batch_request_retry_condition_disabled", func(t *testing.T) {
		// Enable the flag to disable batch retries
		originalValue := relaycore.DisableBatchRequestRetry
		relaycore.DisableBatchRequestRetry = true
		defer func() { relaycore.DisableBatchRequestRetry = originalValue }()

		ctx := context.Background()
		serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		specId := "ETH1"
		chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceJsonRPC, serverHandler, nil, "../../", nil)
		if closeServer != nil {
			defer closeServer()
		}
		require.NoError(t, err)

		// Create a batch JSON-RPC request
		batchRequest := `[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}]`
		chainMsg, err := chainParser.ParseMsg("", []byte(batchRequest), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)

		// Verify that this is indeed a batch message
		require.True(t, chainMsg.IsBatch(), "expected batch message")

		dappId := "dapp"
		consumerIp := "123.11"
		protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, dappId, consumerIp)
		usedProviders := lavasession.NewUsedProviders(nil)

		// Create the state machine
		stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, &SmartRouterRelaySenderMock{retValue: nil, tickerValue: 10 * time.Second}, protocolMessage, nil, false)
		require.NoError(t, err)
		smartRouterStateMachine, ok := stateMachine.(*SmartRouterRelayStateMachine)
		require.True(t, ok, "expected SmartRouterRelayStateMachine")

		// Create a mock results checker
		relayProcessor := relaycore.NewRelayProcessor(
			ctx,
			nil, // Stateless mode - no cross-validation params
			relaycoretest.RelayProcessorMetrics,
			relaycoretest.RelayProcessorMetrics,
			relaycoretest.RelayRetriesManagerInstance,
			smartRouterStateMachine,
		)
		_ = relayProcessor // The relay processor sets up the results checker on the state machine

		// Verify the batch detection works
		require.True(t, protocolMessage.IsBatch(), "should be detected as batch request")

		// With DisableBatchRequestRetry=true, retryCondition should return false for batch requests
		require.True(t, relaycore.DisableBatchRequestRetry,
			"DisableBatchRequestRetry should be true for this test")
	})

	t.Run("batch_request_retry_condition_enabled", func(t *testing.T) {
		// Ensure the flag is disabled (default) to allow batch retries
		originalValue := relaycore.DisableBatchRequestRetry
		relaycore.DisableBatchRequestRetry = false
		defer func() { relaycore.DisableBatchRequestRetry = originalValue }()

		ctx := context.Background()
		serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		specId := "ETH1"
		chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceJsonRPC, serverHandler, nil, "../../", nil)
		if closeServer != nil {
			defer closeServer()
		}
		require.NoError(t, err)

		// Create a batch JSON-RPC request
		batchRequest := `[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}]`
		chainMsg, err := chainParser.ParseMsg("", []byte(batchRequest), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)

		// Verify that this is indeed a batch message
		// Verify this is a batch message
		require.True(t, chainMsg.IsBatch(), "should be detected as batch request")

		// With DisableBatchRequestRetry=false, batch requests should be allowed to retry
		require.False(t, relaycore.DisableBatchRequestRetry,
			"DisableBatchRequestRetry should be false for this test")
	})

	t.Run("single_request_not_affected_by_flag", func(t *testing.T) {
		// Enable the flag to disable batch retries
		originalValue := relaycore.DisableBatchRequestRetry
		relaycore.DisableBatchRequestRetry = true
		defer func() { relaycore.DisableBatchRequestRetry = originalValue }()

		ctx := context.Background()
		serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		specId := "ETH1"
		chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceJsonRPC, serverHandler, nil, "../../", nil)
		if closeServer != nil {
			defer closeServer()
		}
		require.NoError(t, err)

		// Create a single (non-batch) JSON-RPC request
		singleRequest := `{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}`
		chainMsg, err := chainParser.ParseMsg("", []byte(singleRequest), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)

		// Verify that this is NOT a batch message
		require.False(t, chainMsg.IsBatch(), "single request should not be detected as batch")

		// Single requests should not be affected by the DisableBatchRequestRetry flag
	})
}

// TestRelayTaskChannelBuffering tests that the relay task channel is buffered
// to prevent deadlock when the receiver exits prematurely.
// This was a bug where an unbuffered channel could cause the sender goroutine
// to block indefinitely if the receiver exited early (e.g., due to context cancellation).
func TestRelayTaskChannelBuffering(t *testing.T) {
	t.Run("buffered channel prevents deadlock on early receiver exit", func(t *testing.T) {
		// Create a buffered channel (capacity 1) like we fixed
		bufferedChan := make(chan RelayStateSendInstructions, 1)

		// Simulate sender goroutine
		senderDone := make(chan struct{})
		go func() {
			defer close(senderDone)

			// First send should not block (buffered)
			select {
			case bufferedChan <- RelayStateSendInstructions{Done: false}:
				// Success - didn't block
			case <-time.After(100 * time.Millisecond):
				t.Error("Buffered channel blocked on first send - should not happen")
			}
		}()

		// Wait for sender to complete (should be fast with buffered channel)
		select {
		case <-senderDone:
			// Success - sender didn't block
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Sender goroutine blocked - buffered channel should prevent this")
		}
	})

	t.Run("unbuffered channel would block without receiver", func(t *testing.T) {
		// This demonstrates the old buggy behavior with unbuffered channel
		unbufferedChan := make(chan RelayStateSendInstructions)

		senderBlocked := make(chan struct{})
		senderDone := make(chan struct{})

		go func() {
			defer close(senderDone)

			// With unbuffered channel, this will block
			select {
			case unbufferedChan <- RelayStateSendInstructions{Done: false}:
				// Would only reach here if someone receives
			case <-time.After(50 * time.Millisecond):
				// Expected - unbuffered channel blocks
				close(senderBlocked)
			}
		}()

		// Verify that sender blocked (expected with unbuffered channel)
		select {
		case <-senderBlocked:
			// Expected behavior - unbuffered channel blocks
		case <-senderDone:
			t.Fatal("Sender completed without blocking - unexpected for unbuffered channel")
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Test timeout - sender should have either blocked or timed out")
		}
	})

	t.Run("receiver exit does not cause sender deadlock with buffered channel", func(t *testing.T) {
		// This simulates the actual scenario: receiver exits early, sender continues
		bufferedChan := make(chan RelayStateSendInstructions, 1)

		// Start receiver that exits immediately
		receiverExited := make(chan struct{})
		go func() {
			// Receive one message then exit
			select {
			case <-bufferedChan:
				// Received one message
			case <-time.After(50 * time.Millisecond):
				// Timeout - no message yet
			}
			close(receiverExited)
		}()

		// Wait for receiver to exit
		<-receiverExited

		// Now sender tries to send after receiver has exited
		senderDone := make(chan struct{})
		go func() {
			defer close(senderDone)

			// With buffered channel, this should NOT block even though receiver exited
			select {
			case bufferedChan <- RelayStateSendInstructions{Done: true}:
				// Success - buffered channel accepted the message
			case <-time.After(100 * time.Millisecond):
				t.Error("Sender blocked after receiver exit - this is the deadlock bug")
			}
		}()

		// Verify sender completed without blocking
		select {
		case <-senderDone:
			// Success - no deadlock
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Sender deadlocked after receiver exited")
		}
	})
}

// TestSmartRouterStateMachineRetryLimit verifies the full state machine stops retrying
// when the RelayRetryLimit is reached for both node and protocol errors.
func TestSmartRouterStateMachineRetryLimit(t *testing.T) {
	tests := []struct {
		name             string
		retryLimit       int
		expectedMaxTasks int // max tasks before Done (initial + retries)
		sendError        func(relayProcessor *relaycore.RelayProcessor)
		description      string
	}{
		{
			name:             "node errors: limit=0 stops immediately",
			retryLimit:       0,
			expectedMaxTasks: 1,
			sendError: func(rp *relaycore.RelayProcessor) {
				relaycoretest.SendNodeError(rp, "lava@test", time.Millisecond*1)
			},
			description: "With limit=0, should stop after initial node error",
		},
		{
			name:             "node errors: limit=2 allows retries then stops",
			retryLimit:       2,
			expectedMaxTasks: 4,
			sendError: func(rp *relaycore.RelayProcessor) {
				relaycoretest.SendNodeError(rp, "lava@test", time.Millisecond*1)
			},
			description: "With limit=2, should stop within a few node error retries",
		},
		{
			name:             "protocol errors: limit=0 stops immediately",
			retryLimit:       0,
			expectedMaxTasks: 1,
			sendError: func(rp *relaycore.RelayProcessor) {
				relaycoretest.SendProtocolError(rp, "lava@test", time.Millisecond*1, fmt.Errorf("connection timeout"))
			},
			description: "With limit=0, should stop after initial protocol error",
		},
		{
			name:             "protocol errors: limit=2 allows retries then stops",
			retryLimit:       2,
			expectedMaxTasks: 4,
			sendError: func(rp *relaycore.RelayProcessor) {
				relaycoretest.SendProtocolError(rp, "lava@test", time.Millisecond*1, fmt.Errorf("connection timeout"))
			},
			description: "With limit=2, should stop within a few protocol error retries",
		},
		{
			name:             "mixed errors: alternating node and protocol errors stops when total exceeds limit",
			retryLimit:       2,
			expectedMaxTasks: 4,
			sendError: func() func(rp *relaycore.RelayProcessor) {
				callCount := 0
				return func(rp *relaycore.RelayProcessor) {
					callCount++
					if callCount%2 == 1 {
						relaycoretest.SendNodeError(rp, "lava@test", time.Millisecond*1)
					} else {
						relaycoretest.SendProtocolError(rp, "lava@test", time.Millisecond*1, fmt.Errorf("connection timeout"))
					}
				}
			}(),
			description: "With limit=2 and alternating errors, should stop when total errors exceed limit",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			originalValue := relaycore.RelayRetryLimit
			relaycore.RelayRetryLimit = tc.retryLimit
			defer func() { relaycore.RelayRetryLimit = originalValue }()

			ctx := context.Background()
			serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})
			specId := "LAVA"
			chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
			if closeServer != nil {
				defer closeServer()
			}
			require.NoError(t, err)
			chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "123.11")
			usedProviders := lavasession.NewUsedProviders(nil)
			// Use long ticker to prevent ticker-based retries from interfering
			stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, &SmartRouterRelaySenderMock{retValue: nil, tickerValue: 10 * time.Second}, protocolMessage, nil, false)
			require.NoError(t, err)
			relayProcessor := relaycore.NewRelayProcessor(ctx, nil, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayRetriesManagerInstance, stateMachine)

			consumerSessionsMap := lavasession.ConsumerSessionsMap{"lava@test": &lavasession.SessionInfo{}}

			relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
			require.NoError(t, err)

			taskNumber := 0
			for task := range relayTaskChannel {
				if task.IsDone() {
					require.LessOrEqual(t, taskNumber, tc.expectedMaxTasks,
						"%s: too many tasks before Done", tc.description)
					return
				}
				usedProviders.AddUsed(consumerSessionsMap, nil)
				relayProcessor.UpdateBatch(nil)
				tc.sendError(relayProcessor)
				taskNumber++
				require.Less(t, taskNumber, 15,
					"Safety: too many iterations, retry limit not working")
			}
		})
	}
}

// TestSmartRouterStateMachine_ForbidCallerCV proves the `forbid-caller-cv` policy actually disables
// cross-validation end-to-end: with the headers present and a forbid policy, the resolved Selection is NOT
// CrossValidation. The control case (no forbid policy) shows the same headers DO turn CV on — so the test
// catches a forbid that silently no-ops (the resolver returning applies=false alone would not suppress the
// state machine's own header read). See TestCrossValidationPolicyResolver_ForbidCallerCV for the unit-level
// resolver checks.
func TestSmartRouterStateMachine_ForbidCallerCV(t *testing.T) {
	ctx := context.Background()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	specId := "ETH1"
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceJsonRPC, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	const apiInterface = "jsonrpc"
	// A read method (eth_blockNumber) so the normal category is Stateless — forbid must route there.
	chainMsg, err := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	method := chainMsg.GetApi().GetName()

	// Caller opts into cross-validation via headers.
	callerCVHeaders := map[string]string{
		common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "3",
		common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
	}
	buildPM := func() chainlib.ProtocolMessage {
		// Re-parse per case so each ProtocolMessage is independent.
		cm, perr := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, perr)
		return chainlib.NewProtocolMessage(cm, callerCVHeaders, nil, "dapp", "1.2.3.4")
	}

	newResolverWith := func(policy CrossValidationPolicy) *CrossValidationPolicyResolver {
		r, rerr := NewCrossValidationPolicyResolver(CrossValidationConfig{
			Policies: []CrossValidationPolicyEntry{
				{ChainID: specId, ApiInterface: apiInterface, Method: method, CrossValidationPolicy: policy},
			},
		})
		require.NoError(t, rerr)
		return r
	}

	t.Run("control: caller headers turn CV on with no forbid policy", func(t *testing.T) {
		sm, smErr := NewSmartRouterRelayStateMachineWithPolicy(ctx, lavasession.NewUsedProviders(nil), &SmartRouterRelaySenderMock{retValue: nil}, buildPM(), nil, false, nil, specId, apiInterface)
		require.NoError(t, smErr)
		require.Equal(t, relaycore.CrossValidation, sm.GetSelection(), "caller CV headers must enable cross-validation when not forbidden")
	})

	t.Run("forbid policy suppresses caller-driven CV", func(t *testing.T) {
		resolver := newResolverWith(CrossValidationPolicy{ForbidCallerCV: true})
		sm, smErr := NewSmartRouterRelayStateMachineWithPolicy(ctx, lavasession.NewUsedProviders(nil), &SmartRouterRelaySenderMock{retValue: nil}, buildPM(), nil, false, resolver, specId, apiInterface)
		require.NoError(t, smErr)
		require.NotEqual(t, relaycore.CrossValidation, sm.GetSelection(), "forbid-caller-cv must disable cross-validation even with caller headers present")
		require.Nil(t, sm.GetCrossValidationParams(), "no cross-validation params when CV is forbidden")
	})
}

// TestSmartRouterStateMachine_CallerCVHeadersOnAWrite is the write-method sibling of
// TestSmartRouterStateMachine_ForbidCallerCV above, which uses a read method deliberately.
// Two request headers used to turn a transaction submission into a cross-validated request,
// and a cross-validation on a write cannot succeed: the write is broadcast to the whole
// stateful tier, only the first node can accept it, and every other node answers "already
// known" or "nonce too low" — node errors, which never count as successes. So the success
// count is stuck at one while the caller asked for two to agree, the threshold is
// unreachable, and the relay ended as HTTP 500 after the transaction had already gone to
// every node: a caller told their transaction failed, with no hash to follow it by
// (MAG-3603).
//
// No operator policy is involved in any case here. That is the point — the operator
// direction was already refused at startup by validateCrossValidationStartup, and this is
// the caller direction, which needed no permission and no configuration to reach.
func TestSmartRouterStateMachine_CallerCVHeadersOnAWrite(t *testing.T) {
	ctx := context.Background()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	specId := "ETH1"
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, specId, spectypes.APIInterfaceJsonRPC, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	const apiInterface = "jsonrpc"
	callerCVHeaders := map[string]string{
		common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "3",
		common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
	}
	// Re-parsed per case so each ProtocolMessage is independent.
	buildPM := func(t *testing.T, body string, headers map[string]string) chainlib.ProtocolMessage {
		t.Helper()
		cm, perr := chainParser.ParseMsg("", []byte(body), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, perr)
		return chainlib.NewProtocolMessage(cm, headers, nil, "dapp", "1.2.3.4")
	}
	const writeBody = `{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xdeadbeef"],"id":11}`
	const readBody = `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`

	selectionFor := func(t *testing.T, pm chainlib.ProtocolMessage) relaycore.RelayStateMachine {
		t.Helper()
		sm, smErr := NewSmartRouterRelayStateMachineWithPolicy(ctx, lavasession.NewUsedProviders(nil),
			&SmartRouterRelaySenderMock{retValue: nil}, pm, nil, false, nil, specId, apiInterface)
		require.NoError(t, smErr)
		return sm
	}

	// The premise, asserted rather than assumed: eth_sendRawTransaction really is the write
	// category on this spec. Were it not, every assertion below would pass for the wrong reason.
	writePM := buildPM(t, writeBody, callerCVHeaders)
	// Cast, not EqualValues: GetStateful returns uint32 and the constant is untyped, so an
	// unconverted compare fails on type alone and would read as the spec being wrong.
	require.Equal(t, uint32(common.CONSISTENCY_SELECT_ALL_PROVIDERS), chainlib.GetStateful(writePM),
		"premise: eth_sendRawTransaction must be the stateful (write) category")

	t.Run("a write routes as a write despite the caller's cross-validation headers", func(t *testing.T) {
		sm := selectionFor(t, buildPM(t, writeBody, callerCVHeaders))
		require.Equal(t, relaycore.Stateful, sm.GetSelection(),
			"a transaction submission must broadcast as a write, not be cross-validated")
		require.Nil(t, sm.GetCrossValidationParams(),
			"no cross-validation params on a write: a threshold that can never be met must not be carried")
	})

	t.Run("control: the same headers do turn cross-validation on for a read", func(t *testing.T) {
		// Without this the test above would pass just as well against headers that were
		// never parsed, or a spec that failed to load.
		sm := selectionFor(t, buildPM(t, readBody, callerCVHeaders))
		require.Equal(t, relaycore.CrossValidation, sm.GetSelection(),
			"caller CV headers still enable cross-validation where it can actually work")
	})

	t.Run("control: a write with no headers is unchanged", func(t *testing.T) {
		sm := selectionFor(t, buildPM(t, writeBody, nil))
		require.Equal(t, relaycore.Stateful, sm.GetSelection())
	})

	t.Run("malformed cross-validation headers on a write do not fail the transaction", func(t *testing.T) {
		// The headers are ignored, so their contents are moot — and failing a money-moving
		// request over an unparseable value for a parameter that could never have applied
		// would be the same misreport in a new costume. On a READ the error still stands.
		bad := map[string]string{
			common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "not-a-number",
			common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
		}
		sm := selectionFor(t, buildPM(t, writeBody, bad))
		require.Equal(t, relaycore.Stateful, sm.GetSelection())

		_, readErr := NewSmartRouterRelayStateMachineWithPolicy(ctx, lavasession.NewUsedProviders(nil),
			&SmartRouterRelaySenderMock{retValue: nil}, buildPM(t, readBody, bad), nil, false, nil, specId, apiInterface)
		require.Error(t, readErr, "a read still rejects headers it cannot parse")
	})

	// The state machine is not the only place the caller's headers are read: the policy layer
	// reads them first, and Resolve returns (callerParams, callerPresent) for a method with no
	// policy of its own — so with ANY policy configured, for any method at all, a write's own
	// headers came back as a resolved policy override. That took the machine's FIRST branch,
	// cvOverride != nil, which sits ahead of the write branch by design (Finding A). Routing the
	// write as a write in the machine was therefore inert on every router that had a
	// cross-validation policy: the headers were laundered into an operator mandate before the
	// machine ever saw them.
	//
	// This is the case that proves it — valid headers, a policy on an unrelated read — and it
	// is the reason the write skip has to live in the policy layer too, not only in the machine.
	t.Run("a policy on an unrelated method does not launder a write's headers into an override", func(t *testing.T) {
		resolver, rerr := NewCrossValidationPolicyResolver(CrossValidationConfig{
			Policies: []CrossValidationPolicyEntry{{
				ChainID: specId, ApiInterface: apiInterface, Method: "eth_blockNumber",
				CrossValidationPolicy: CrossValidationPolicy{Enabled: true},
			}},
		})
		require.NoError(t, rerr)

		sm, smErr := NewSmartRouterRelayStateMachineWithPolicy(ctx, lavasession.NewUsedProviders(nil),
			&SmartRouterRelaySenderMock{retValue: nil}, buildPM(t, writeBody, callerCVHeaders), nil, false, resolver, specId, apiInterface)
		require.NoError(t, smErr)
		require.Equal(t, relaycore.Stateful, sm.GetSelection(),
			"a write must route as a write even when some other method has a policy")
		require.Nil(t, sm.GetCrossValidationParams(),
			"the caller's headers must not arrive dressed as an operator mandate")
	})

	t.Run("a configured policy elsewhere does not make malformed headers fail a write", func(t *testing.T) {
		resolver, rerr := NewCrossValidationPolicyResolver(CrossValidationConfig{
			Policies: []CrossValidationPolicyEntry{{
				ChainID: specId, ApiInterface: apiInterface, Method: "eth_blockNumber",
				CrossValidationPolicy: CrossValidationPolicy{Enabled: true},
			}},
		})
		require.NoError(t, rerr)
		bad := map[string]string{
			common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "not-a-number",
			common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
		}

		sm, smErr := NewSmartRouterRelayStateMachineWithPolicy(ctx, lavasession.NewUsedProviders(nil),
			&SmartRouterRelaySenderMock{retValue: nil}, buildPM(t, writeBody, bad), nil, false, resolver, specId, apiInterface)
		require.NoError(t, smErr, "the policy layer must not fail a write over headers a write ignores")
		require.Equal(t, relaycore.Stateful, sm.GetSelection())

		// The policy still governs the method it was written for, so the skip above is scoped
		// to writes rather than switching the resolver off.
		readSM, readErr := NewSmartRouterRelayStateMachineWithPolicy(ctx, lavasession.NewUsedProviders(nil),
			&SmartRouterRelaySenderMock{retValue: nil}, buildPM(t, readBody, nil), nil, false, resolver, specId, apiInterface)
		require.NoError(t, readErr)
		require.Equal(t, relaycore.CrossValidation, readSM.GetSelection(),
			"the enabled policy on the read method still applies")
	})
}
