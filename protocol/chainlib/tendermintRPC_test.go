package chainlib

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTendermintChainParser_Spec(t *testing.T) {
	// create a new instance of RestChainParser
	apip, err := NewTendermintRpcChainParser()
	if err != nil {
		t.Errorf("Error creating RestChainParser: %v", err)
	}

	// set the spec
	spec := spectypes.Spec{
		Enabled:                       true,
		ReliabilityThreshold:          10,
		AllowedBlockLagForQosSync:     11,
		AverageBlockTime:              12000,
		BlockDistanceForFinalizedData: 13,
		BlocksInFinalizationProof:     14,
	}
	apip.SetSpec(spec)

	// fetch chain block stats
	allowedBlockLagForQosSync, averageBlockTime, blockDistanceForFinalizedData, blocksInFinalizationProof := apip.ChainBlockStats()

	// convert block time
	AverageBlockTime := time.Duration(apip.spec.AverageBlockTime) * time.Millisecond

	// check that the spec was set correctly
	assert.Equal(t, apip.spec.AllowedBlockLagForQosSync, allowedBlockLagForQosSync)
	assert.Equal(t, apip.spec.BlockDistanceForFinalizedData, blockDistanceForFinalizedData)
	assert.Equal(t, apip.spec.BlocksInFinalizationProof, blocksInFinalizationProof)
	assert.Equal(t, AverageBlockTime, averageBlockTime)
}

func TestTendermintChainParser_NilGuard(t *testing.T) {
	var apip *TendermintChainParser

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("apip methods missing nill guard, panicked with: %v", r)
		}
	}()

	apip.SetSpec(spectypes.Spec{})
	apip.ChainBlockStats()
	apip.getSupportedApi("", "")
	apip.ParseMsg("", []byte{}, "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
}

func TestTendermintGetSupportedApi(t *testing.T) {
	// Test case 1: Successful scenario, returns a supported API
	apip := &TendermintChainParser{
		BaseChainParser: BaseChainParser{
			serverApis: map[ApiKey]ApiContainer{{Name: "API1", ConnectionType: connectionType_test}: {api: &spectypes.Api{Name: "API1", Enabled: true}, collectionKey: CollectionKey{ConnectionType: connectionType_test}}},
		},
	}
	api, err := apip.getSupportedApi("API1", connectionType_test)
	assert.NoError(t, err)
	assert.Equal(t, "API1", api.api.Name)

	// Test case 2: Returns error if the API does not exist
	apip = &TendermintChainParser{
		BaseChainParser: BaseChainParser{
			serverApis: map[ApiKey]ApiContainer{{Name: "API1", ConnectionType: connectionType_test}: {api: &spectypes.Api{Name: "API1", Enabled: true}, collectionKey: CollectionKey{ConnectionType: connectionType_test}}},
		},
	}
	apiCont, err := apip.getSupportedApi("API2", connectionType_test)
	if err == nil {
		assert.Equal(t, "Default-API2", apiCont.api.Name)
	} else {
		assert.ErrorIs(t, err, common.APINotSupportedError)
	}

	// Test case 3: Returns error if the API is disabled
	apip = &TendermintChainParser{
		BaseChainParser: BaseChainParser{
			serverApis: map[ApiKey]ApiContainer{{Name: "API1", ConnectionType: connectionType_test}: {api: &spectypes.Api{Name: "API1", Enabled: false}, collectionKey: CollectionKey{ConnectionType: connectionType_test}}},
		},
	}
	_, err = apip.getSupportedApi("API1", connectionType_test)
	assert.Error(t, err)
}

func TestTendermintParseMessage(t *testing.T) {
	apip := &TendermintChainParser{
		BaseChainParser: BaseChainParser{
			serverApis: map[ApiKey]ApiContainer{
				{Name: "API1", ConnectionType: connectionType_test}: {api: &spectypes.Api{
					Name:    "API1",
					Enabled: true,
					BlockParsing: spectypes.BlockParser{
						ParserArg:  []string{"latest"},
						ParserFunc: spectypes.PARSER_FUNC_DEFAULT,
					},
				}, collectionKey: CollectionKey{ConnectionType: connectionType_test}},
			},
			apiCollections: map[CollectionKey]*spectypes.ApiCollection{{ConnectionType: connectionType_test}: {Enabled: true, CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceTendermintRPC}}},
		},
	}

	data := rpcInterfaceMessages.TendermintrpcMessage{
		JsonrpcMessage: rpcInterfaceMessages.JsonrpcMessage{
			Method: "API1",
		},
		Path: "",
	}

	marshalledData, _ := json.Marshal(data)

	msg, err := apip.ParseMsg("API1", marshalledData, connectionType_test, nil, extensionslib.ExtensionInfo{LatestBlock: 0})

	assert.Nil(t, err)
	assert.Equal(t, msg.GetApi().Name, apip.serverApis[ApiKey{Name: "API1", ConnectionType: connectionType_test}].api.Name)
	requestedBlock, _ := msg.RequestedBlock()
	assert.Equal(t, requestedBlock, int64(-2))
}

func TestTendermintRpcChainProxy(t *testing.T) {
	ctx := context.Background()
	serverHandle := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle the incoming request and provide the desired response
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{
			"jsonrpc": "2.0",
			"id": 1,"result": {
				"sync_info": {
					"latest_block_height": "1947"
				},
				"block_id": {
					"hash": "ABABABABABABABABABABABAB"
				}
			}
		}`)
	})

	chainParser, chainProxy, chainFetcher, closeServer, _, err := CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceTendermintRPC, serverHandle, nil, "../../", nil)
	require.NoError(t, err)
	require.NotNil(t, chainParser)
	require.NotNil(t, chainProxy)
	require.NotNil(t, chainFetcher)
	block, err := chainFetcher.FetchLatestBlockNum(ctx)
	require.Greater(t, block, int64(0))
	require.NoError(t, err)
	_, err = chainFetcher.FetchBlockHashByNum(ctx, block)
	require.NoError(t, err)
	if closeServer != nil {
		closeServer()
	}
}

func TestTendermintRpcBatchSizeLimit(t *testing.T) {
	ctx := context.Background()

	// Set a batch size limit of 2
	originalLimit := MaxBatchRequestSize
	MaxBatchRequestSize = 2
	defer func() { MaxBatchRequestSize = originalLimit }()

	serverHandle := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"block_id":{},"block":{}}}`)
	})

	chainParser, _, _, closeServer, _, err := CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceTendermintRPC, serverHandle, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	// Test: batch within limit should succeed
	batchWithinLimit := `[{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"99"}},{"jsonrpc":"2.0","id":2,"method":"block","params":{"height":"100"}}]`
	_, err = chainParser.ParseMsg("", []byte(batchWithinLimit), "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)

	// Test: batch exceeding limit should fail
	batchExceedingLimit := `[{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"99"}},{"jsonrpc":"2.0","id":2,"method":"block","params":{"height":"100"}},{"jsonrpc":"2.0","id":3,"method":"block","params":{"height":"101"}}]`
	_, err = chainParser.ParseMsg("", []byte(batchExceedingLimit), "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrBatchRequestSizeExceeded))

	// Test: single request should always succeed regardless of limit
	singleRequest := `{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"99"}}`
	_, err = chainParser.ParseMsg("", []byte(singleRequest), "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)

	// Test: when limit is 0 (unlimited), large batches should succeed
	MaxBatchRequestSize = 0
	largeBatch := `[{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"99"}},{"jsonrpc":"2.0","id":2,"method":"block","params":{"height":"100"}},{"jsonrpc":"2.0","id":3,"method":"block","params":{"height":"101"}},{"jsonrpc":"2.0","id":4,"method":"block","params":{"height":"102"}},{"jsonrpc":"2.0","id":5,"method":"block","params":{"height":"103"}}]`
	_, err = chainParser.ParseMsg("", []byte(largeBatch), "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
}

func TestTendermintRpcBatchCall(t *testing.T) {
	ctx := context.Background()
	gotCalled := false
	batchCallData := `[{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"99"}},{"jsonrpc":"2.0","id":2,"method":"block","params":{"height":"100"}}]`
	const response = `[{"jsonrpc":"2.0","id":1,"result":{"block_id":{},"block":{}}},{"jsonrpc":"2.0","id":2,"result":{"block_id":{},"block":{}}}]`
	serverHandle := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCalled = true
		data := make([]byte, len([]byte(batchCallData)))
		r.Body.Read(data)
		// require.NoError(t, err)
		require.Equal(t, batchCallData, string(data))
		// Handle the incoming request and provide the desired response
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, response)
	})

	chainParser, chainProxy, chainFetcher, closeServer, _, err := CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceTendermintRPC, serverHandle, nil, "../../", nil)
	require.NoError(t, err)
	require.NotNil(t, chainParser)
	require.NotNil(t, chainProxy)
	require.NotNil(t, chainFetcher)

	chainMessage, err := chainParser.ParseMsg("", []byte(batchCallData), "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)

	// A batch's compute units must be the sum of its member methods' compute_units.
	// Guards that batch CU accounting stays driven solely by compute_units after
	// extra_compute_units is removed from the spec model.
	sumCU := uint64(0)
	for _, single := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"99"}}`,
		`{"jsonrpc":"2.0","id":2,"method":"block","params":{"height":"100"}}`,
	} {
		singleMsg, errSingle := chainParser.ParseMsg("", []byte(single), "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, errSingle)
		sumCU += singleMsg.GetApi().ComputeUnits
	}
	require.Equal(t, sumCU, chainMessage.GetApi().ComputeUnits, "batch CU must equal the sum of member compute_units")

	requestedBlock, earliestReqBlock := chainMessage.RequestedBlock()
	require.Equal(t, int64(100), requestedBlock)
	require.Equal(t, int64(99), earliestReqBlock)
	relayReply, _, _, _, _, err := chainProxy.SendNodeMsg(ctx, nil, chainMessage, nil)
	require.True(t, gotCalled)
	require.NoError(t, err)
	require.NotNil(t, relayReply)
	require.Equal(t, response, string(relayReply.RelayReply.Data))
	defer func() {
		if closeServer != nil {
			closeServer()
		}
	}()
}

func TestTendermintRpcSingleElementBatchRequestedBlock(t *testing.T) {
	ctx := context.Background()
	serverHandle := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `[{"jsonrpc":"2.0","id":1,"result":{"block_id":{},"block":{}}}]`)
	})

	chainParser, _, _, closeServer, _, err := CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceTendermintRPC, serverHandle, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	// Single-element batch requesting a historical block
	singleBatch := `[{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"500"}}]`
	chainMessage, err := chainParser.ParseMsg("", []byte(singleBatch), "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)

	// Must be detected as batch
	require.True(t, chainMessage.IsBatch(), "single-element array must be treated as batch")

	// earliestRequestedBlock must equal latestRequestedBlock, NOT spectypes.LATEST_BLOCK
	latest, earliest := chainMessage.RequestedBlock()
	require.Equal(t, int64(500), latest, "latestRequestedBlock should be 500")
	require.Equal(t, int64(500), earliest, "earliestRequestedBlock must equal latest for single-element batch, not LATEST_BLOCK")
}

func TestTendermintRpcBatchCallWithSameID(t *testing.T) {
	ctx := context.Background()
	gotCalled := false
	batchCallData := `[{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"99"}},{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"100"}}]`
	const response = `[{"jsonrpc":"2.0","id":1,"result":{"block_id1111111111":{},"block":{}}},{"jsonrpc":"2.0","id":1,"result":{"block_id222222":{},"block":{}}}]`

	const nodeResponse = `[{"jsonrpc":"2.0","id":1,"result":{"block_id1111111111":{},"block":{}}},{"jsonrpc":"2.0","id":2,"result":{"block_id222222":{},"block":{}}}]`
	nodeBatchCallData := `[{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"99"}},{"jsonrpc":"2.0","id":2,"method":"block","params":{"height":"100"}}]`
	serverHandle := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCalled = true
		data := make([]byte, len([]byte(nodeBatchCallData)))
		r.Body.Read(data)
		// require.NoError(t, err)
		require.Equal(t, nodeBatchCallData, string(data))
		// Handle the incoming request and provide the desired response
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, nodeResponse)
	})

	chainParser, chainProxy, chainFetcher, closeServer, _, err := CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceTendermintRPC, serverHandle, nil, "../../", nil)
	require.NoError(t, err)
	require.NotNil(t, chainParser)
	require.NotNil(t, chainProxy)
	require.NotNil(t, chainFetcher)

	chainMessage, err := chainParser.ParseMsg("", []byte(batchCallData), "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)

	// A batch's compute units must be the sum of its member methods' compute_units.
	// Guards that batch CU accounting stays driven solely by compute_units after
	// extra_compute_units is removed from the spec model.
	sumCU := uint64(0)
	for _, single := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"99"}}`,
		`{"jsonrpc":"2.0","id":2,"method":"block","params":{"height":"100"}}`,
	} {
		singleMsg, errSingle := chainParser.ParseMsg("", []byte(single), "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, errSingle)
		sumCU += singleMsg.GetApi().ComputeUnits
	}
	require.Equal(t, sumCU, chainMessage.GetApi().ComputeUnits, "batch CU must equal the sum of member compute_units")

	requestedBlock, earliestReqBlock := chainMessage.RequestedBlock()
	require.Equal(t, int64(100), requestedBlock)
	require.Equal(t, int64(99), earliestReqBlock)
	relayReply, _, _, _, _, err := chainProxy.SendNodeMsg(ctx, nil, chainMessage, nil)
	require.True(t, gotCalled)
	require.NoError(t, err)
	require.NotNil(t, relayReply)
	require.Equal(t, response, string(relayReply.RelayReply.Data))
	defer func() {
		if closeServer != nil {
			closeServer()
		}
	}()
}

func TestTendermintURIRPC(t *testing.T) {
	ctx := context.Background()
	serverHandle := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle the incoming request and provide the desired response
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{
			"jsonrpc": "2.0",
			"id": 1,"result": "ok"
		}`)
	})

	chainParser, chainProxy, chainFetcher, closeServer, _, err := CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceTendermintRPC, serverHandle, nil, "../../", nil)
	require.NoError(t, err)
	require.NotNil(t, chainParser)
	require.NotNil(t, chainProxy)
	require.NotNil(t, chainFetcher)
	requestUrl := "tx_search?query=%22recv_packet.packet_src_channel=%27channel-227%27%20AND%20recv_packet.packet_sequence=%271123%27%20%20AND%20recv_packet.packet_dst_channel=%27channel-3%27%22"
	chainMessage, err := chainParser.ParseMsg(requestUrl, nil, "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	nodeMessage, ok := chainMessage.GetRPCMessage().(*rpcInterfaceMessages.TendermintrpcMessage)
	require.True(t, ok)
	params := nodeMessage.GetParams()
	casted, ok := params.(map[string]interface{})
	require.True(t, ok)
	_, ok = casted["query"]
	require.True(t, ok)
	if closeServer != nil {
		closeServer()
	}
}

func TestTendermintRpcChainListener_Shutdown_DrainsWSWaitGroup(t *testing.T) {
	listener := &TendermintRpcChainListener{
		app: fiber.New(fiber.Config{DisableStartupMessage: true}),
	}
	listener.wsWG.Add(1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		listener.wsWG.Done()
	}()
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, listener.Shutdown(ctx))
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
	require.Less(t, elapsed, 1*time.Second)
}

func TestTendermintRpcChainListener_Shutdown_RespectsContextDeadline(t *testing.T) {
	listener := &TendermintRpcChainListener{
		app: fiber.New(fiber.Config{DisableStartupMessage: true}),
	}
	listener.wsWG.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_ = listener.Shutdown(ctx)
	elapsed := time.Since(start)
	require.Less(t, elapsed, 1*time.Second)
	listener.wsWG.Done()
}

func TestTendermintRpcChainListener_Shutdown_NilApp(t *testing.T) {
	listener := &TendermintRpcChainListener{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, listener.Shutdown(ctx))
}

// startTestTendermintListener spins up a TendermintRpcChainListener bound to 127.0.0.1:0
// and returns the listener plus its dynamic address once Serve is ready.
func startTestTendermintListener(t *testing.T, ctx context.Context) (*TendermintRpcChainListener, string) {
	t.Helper()
	// ListenToMessages uses the custom rand package which requires initialization.
	// The package-level TestMain (chain_router_test.go) does not call InitRandomSeed,
	// so we do it here. InitRandomSeed is idempotent.
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	endpoint := &lavasession.RPCEndpoint{
		NetworkAddress:  "127.0.0.1:0",
		ChainID:         "COS5",
		ApiInterface:    "tendermintrpc",
		HealthCheckPath: "/lava/health",
	}
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	listener := NewTendermintRpcChainListener(ctx, endpoint, nil, nil, logger, nil, nil)

	cmdFlags := common.ConsumerCmdFlags{}
	go listener.Serve(ctx, cmdFlags)

	// Wait for the listener to bind and report its address.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if addr := listener.GetListeningAddress(); addr != "" {
			return listener, addr
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("tendermint listener never reported a listening address")
	return nil, ""
}

func TestTendermintRpcChainListener_GracefulShutdown_SendsWSCloseFrame_1001(t *testing.T) {
	serveCtx, cancelServe := context.WithCancel(context.Background())
	defer cancelServe()
	listener, addr := startTestTendermintListener(t, serveCtx)

	wsURL := "ws://" + addr + "/websocket"
	client, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err, "WS dial should succeed")
	defer client.Close()

	cancelServe()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	_ = listener.Shutdown(shutdownCtx)

	_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, readErr := client.ReadMessage()
	require.Error(t, readErr)
	closeErr, ok := readErr.(*websocket.CloseError)
	require.True(t, ok, "expected *websocket.CloseError, got %T: %v", readErr, readErr)
	require.Equal(t, websocket.CloseGoingAway, closeErr.Code,
		"expected close code 1001 (Going Away), got %d", closeErr.Code)
}

func TestTendermintRpcChainListener_GracefulShutdown_RejectsNewConnectionsAfterShutdown(t *testing.T) {
	serveCtx, cancelServe := context.WithCancel(context.Background())
	defer cancelServe()
	listener, addr := startTestTendermintListener(t, serveCtx)

	cancelServe()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	_ = listener.Shutdown(shutdownCtx)

	// Give the OS a moment to release the listener socket.
	time.Sleep(100 * time.Millisecond)

	// New GET attempts should fail at the connection layer.
	httpClient := &http.Client{Timeout: 2 * time.Second}
	resp, err := httpClient.Get("http://" + addr + "/")
	if err == nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err, "GET after Shutdown should fail with connection error")
}
