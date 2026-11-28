package chainlib

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/parser"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestParseMessage(t *testing.T) {
	apip := &RestChainParser{
		BaseChainParser: BaseChainParser{
			serverApis:     restApiContainers(t, connectionType_test, &spectypes.Api{Name: "API1", Enabled: true}),
			apiCollections: map[CollectionKey]*spectypes.ApiCollection{{ConnectionType: connectionType_test}: {Enabled: true, CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceRest}}},
		},
	}

	msg, err := apip.ParseMsg("API1", []byte("test message"), connectionType_test, nil, extensionslib.ExtensionInfo{LatestBlock: 0})

	assert.Nil(t, err)
	assert.Equal(t, msg.GetApi().Name, apip.serverApis[ApiKey{Name: "API1", ConnectionType: connectionType_test}].api.Name)

	restMessage := rpcInterfaceMessages.RestMessage{
		Msg:         []byte("test message"),
		Path:        "API1",
		SpecPath:    "API1",
		BaseMessage: chainproxy.BaseMessage{Headers: []pairingtypes.Metadata{}},
	}

	assert.Equal(t, &restMessage, msg.GetRPCMessage())
}

func TestRestChainProxy(t *testing.T) {
	ctx := context.Background()

	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle the incoming request and provide the desired response
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"block": { "header": {"height": "244591"}}}`)
	})
	chainParser, chainProxy, chainFetcher, closeServer, _, err := CreateChainLibMocks(ctx, "COSMOSHUB", spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
	require.NoError(t, err)
	require.NotNil(t, chainParser)
	require.NotNil(t, chainProxy)
	require.NotNil(t, chainFetcher)
	block, err := chainFetcher.FetchLatestBlockNum(ctx)
	require.Greater(t, block, int64(0))
	require.NoError(t, err)

	chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	reqBlock, _ := chainMsg.RequestedBlock()
	require.Equal(t, int64(17), reqBlock)
	if closeServer != nil {
		closeServer()
	}
}

func TestParsingRequestedBlocksHeadersRest(t *testing.T) {
	ctx := context.Background()
	callbackHeaderNameToCheck := ""
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle the incoming request and provide the desired response
		w.WriteHeader(http.StatusOK)
		header := r.Header.Get(callbackHeaderNameToCheck)
		if header != "" {
			fmt.Fprint(w, `{"block": { "header": {"height": "244590"}}}`)
		} else {
			fmt.Fprint(w, `{"block": { "header": {"height": "244591"}}}`)
		}
	})
	chainParser, chainRouter, _, closeServer, _, err := CreateChainLibMocks(ctx, "COSMOSHUB", spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
	require.NoError(t, err)
	defer func() {
		if closeServer != nil {
			closeServer()
		}
	}()
	parsingForCrafting, apiCollection, ok := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	require.True(t, ok)
	collectionData := apiCollection.CollectionData
	headerParsingDirective, _, ok := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_SET_LATEST_IN_METADATA)
	callbackHeaderNameToCheck = headerParsingDirective.GetApiName() // this causes the callback to modify the response to simulate a real behavior
	require.True(t, ok)
	block := 244590
	metadata := []pairingtypes.Metadata{{Name: headerParsingDirective.GetApiName(), Value: fmt.Sprintf(headerParsingDirective.FunctionTemplate, block)}}

	tests := []struct {
		desc           string
		metadata       []pairingtypes.Metadata
		block          int64
		requestedBlock int64
	}{
		{
			desc:           "no metadata",
			metadata:       []pairingtypes.Metadata{},
			block:          244591,
			requestedBlock: spectypes.LATEST_BLOCK,
		},
		{
			desc:           "with-metadata",
			metadata:       metadata,
			block:          244590,
			requestedBlock: 244590,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			chainMessage, err := chainParser.ParseMsg(parsingForCrafting.ApiName, []byte{}, collectionData.Type, test.metadata, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			require.NoError(t, err)
			latestReqBlock, _ := chainMessage.RequestedBlock()
			require.Equal(t, test.requestedBlock, latestReqBlock)
			reply, _, _, err := chainRouter.SendNodeMsg(ctx, chainMessage, nil)
			require.NoError(t, err)
			parserInput, err := FormatResponseForParsing(reply.RelayReply, chainMessage)
			require.NoError(t, err)
			parsedInput := parser.ParseBlockFromReply(parserInput, parsingForCrafting.ResultParsing, nil)
			require.Equal(t, test.block, parsedInput.GetBlock())
		})
	}
}

func TestSettingRequestedBlocksHeadersRest(t *testing.T) {
	ctx := context.Background()
	callbackHeaderNameToCheck := ""
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle the incoming request and provide the desired response
		w.WriteHeader(http.StatusOK)
		header := r.Header.Get(callbackHeaderNameToCheck)
		if header != "" {
			parsedBlock, err := strconv.ParseUint(header, 0, 64)
			require.NoError(t, err)
			if parsedBlock < 244591 {
				fmt.Fprintf(w, `{"block": { "header": {"height": "%d"}}}`, parsedBlock)
				return
			}
		}
		fmt.Fprint(w, `{"block": { "header": {"height": "244591"}}}`)
	})
	chainParser, chainRouter, _, closeServer, _, err := CreateChainLibMocks(ctx, "COSMOSHUB", spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
	require.NoError(t, err)
	defer func() {
		if closeServer != nil {
			closeServer()
		}
	}()
	parsingForCrafting, apiCollection, ok := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	require.True(t, ok)
	collectionData := apiCollection.CollectionData
	headerParsingDirective, _, ok := chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_SET_LATEST_IN_METADATA)
	callbackHeaderNameToCheck = headerParsingDirective.GetApiName() // this causes the callback to modify the response to simulate a real behavior
	require.True(t, ok)
	block := 244590
	metadata := []pairingtypes.Metadata{{Name: headerParsingDirective.GetApiName(), Value: fmt.Sprintf(headerParsingDirective.FunctionTemplate, block)}}

	tests := []struct {
		desc           string
		metadata       []pairingtypes.Metadata
		block          int64
		requestedBlock int64
	}{
		// Disabled due to inconsistency in cosmos sdk when adding these headers
		// {
		// 	desc:           "no metadata",
		// 	metadata:       []pairingtypes.Metadata{},
		// 	block:          244589,
		// 	requestedBlock: spectypes.LATEST_BLOCK,
		// },
		{
			desc:           "with-metadata",
			metadata:       metadata,
			block:          244590,
			requestedBlock: 244590,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			chainMessage, err := chainParser.ParseMsg(parsingForCrafting.ApiName, []byte{}, collectionData.Type, test.metadata, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			require.NoError(t, err)
			latestReqBlock, _ := chainMessage.RequestedBlock()
			require.Equal(t, test.requestedBlock, latestReqBlock)
			chainMessage.UpdateLatestBlockInMessage(test.block, true) // will update the block only if it's a latest request
			latestReqBlock, _ = chainMessage.RequestedBlock()
			require.Equal(t, test.block, latestReqBlock) // expected behavior is that it doesn't change the original requested block
			reply, _, _, err := chainRouter.SendNodeMsg(ctx, chainMessage, nil)
			require.NoError(t, err)
			parserInput, err := FormatResponseForParsing(reply.RelayReply, chainMessage)
			require.NoError(t, err)
			parsedInput := parser.ParseBlockFromReply(parserInput, parsingForCrafting.ResultParsing, nil)
			require.Equal(t, test.block, parsedInput.GetBlock())
		})
	}
}

func TestRegexParsing(t *testing.T) {
	chainParser, _, _, closeServer, _, err := CreateChainLibMocks(context.Background(), "COSMOSHUB", spectypes.APIInterfaceRest, nil, nil, "../../", nil)
	require.NoError(t, err)
	defer func() {
		if closeServer != nil {
			closeServer()
		}
	}()
	for _, api := range []string{
		"/cosmos/staking/v1beta1/delegations/",
		"/cosmos/distribution/v1beta1/delegators/cosmos1e9ma89h83azrfnqqy0u255zqxq0xluza6ydf9n/rewards/",
		"/cosmos/distribution/v1beta1/delegators/cosmos1e9ma89h83azrfnqqy0u255zqxq0xluza6ydf9n/rewards/cosmosvaloper1e9ma89h83azrfnqqy0u255zqxq0xluzaq8wk3d",
	} {
		_, err := chainParser.ParseMsg(api, nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
	}
	// A trailing slash is optional: the spec names this api without one, and the request
	// resolves to it rather than falling through to the synthetic Default- api.
	for api, expectedApiName := range map[string]string{
		"/cosmos/staking/v1beta1/delegations/cosmos17ym998u666u8w2qgjd5m7w7ydjqmu3mlgl7ua2":  "/cosmos/staking/v1beta1/delegations/{delegator_addr}",
		"/cosmos/staking/v1beta1/delegations/cosmos17ym998u666u8w2qgjd5m7w7ydjqmu3mlgl7ua2/": "/cosmos/staking/v1beta1/delegations/{delegator_addr}",
	} {
		chainMessage, err := chainParser.ParseMsg(api, nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		require.Equal(t, expectedApiName, chainMessage.GetApi().GetName())
	}

	// The point of matching the templated api is not the metric label: the synthetic
	// Default- api bills a flat 20 compute units and parses no block at all, so a request
	// that falls through is billed wrong and routed to the latest block. With the api
	// resolved, the height in the path is the requested block again.
	for _, api := range []string{
		"/cosmos/base/tendermint/v1beta1/blocks/244590",
		"/cosmos/base/tendermint/v1beta1/blocks/244590/",
	} {
		chainMessage, err := chainParser.ParseMsg(api, nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		require.NoError(t, err)
		require.Equal(t, "/cosmos/base/tendermint/v1beta1/blocks/{height}", chainMessage.GetApi().GetName())
		require.EqualValues(t, 10, chainMessage.GetApi().ComputeUnits)
		requestedBlock, _ := chainMessage.RequestedBlock()
		require.EqualValues(t, 244590, requestedBlock)
	}

	// The spec names /blocks/latest next to /blocks/{height}, so both cover this path and
	// the answer used to depend on map iteration order — with or without the slash. Repeated
	// because a single call passes at random.
	for _, api := range []string{
		"/cosmos/base/tendermint/v1beta1/blocks/latest",
		"/cosmos/base/tendermint/v1beta1/blocks/latest/",
	} {
		for i := 0; i < 32; i++ {
			chainMessage, err := chainParser.ParseMsg(api, nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			require.Equal(t, "/cosmos/base/tendermint/v1beta1/blocks/latest", chainMessage.GetApi().GetName())
		}
	}

	// Only the slash is relaxed — a path the spec genuinely does not describe still falls
	// through, which is what keeps Default-* a usable spec-gap signal.
	for _, api := range []string{
		"/not/a/real/chain/path",
		"/not/a/real/chain/path/",
	} {
		chainMessage, err := chainParser.ParseMsg(api, nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
		if err == nil {
			require.Equal(t, "Default-"+api, chainMessage.GetApi().GetName())
		} else {
			assert.ErrorIs(t, err, common.APINotSupportedError)
		}
	}
}

func TestRestChainListener_Shutdown_NilApp(t *testing.T) {
	listener := &RestChainListener{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Should not panic when Shutdown is called before Serve has captured an app.
	require.NoError(t, listener.Shutdown(ctx))
}

// restHealthRelayStub is the slice of RelaySender the rest relay path uses: it records
// what it was asked to relay, so a test can prove the relay was never reached.
type restHealthRelayStub struct {
	mu   sync.Mutex
	urls []string
}

func (s *restHealthRelayStub) SendRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, analytics *metrics.RelayMetrics, metadataValues []pairingtypes.Metadata) (*common.RelayResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.urls = append(s.urls, url)
	return &common.RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte(`{"relayed":true}`)}, StatusCode: http.StatusOK}, nil
}

func (s *restHealthRelayStub) ParseRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, metadata []pairingtypes.Metadata) (ProtocolMessage, error) {
	return nil, errors.New("not used")
}

func (s *restHealthRelayStub) SendParsedRelay(ctx context.Context, analytics *metrics.RelayMetrics, protocolMessage ProtocolMessage) (*common.RelayResult, error) {
	return nil, errors.New("not used")
}

func (s *restHealthRelayStub) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.urls...)
}

// The health route hands a websocket upgrade past the health handler so a client
// that dialled the bare URL is not answered with a health body. rest serves no
// websockets and registers no GET route, so the handler after the health route is
// the relay catch-all: handing on there answers a health probe with a chain
// response. The health path is a liveness check for the router and must never
// reach a provider, whatever headers the probe carries.
func TestRestChainListener_HealthPathNeverReachesTheRelay(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	for _, healthPath := range []string{common.DEFAULT_HEALTH_PATH, "/"} {
		t.Run(healthPath, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			stub := &restHealthRelayStub{}
			logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
			require.NoError(t, err)
			endpoint := &routersession.RPCEndpoint{
				NetworkAddress:  "127.0.0.1:0",
				ChainID:         "COSMOSHUB",
				ApiInterface:    "rest",
				HealthCheckPath: healthPath,
			}
			listener := NewRestChainListener(ctx, endpoint, stub, alwaysHealthyReporter{}, logger)
			go listener.Serve(ctx, common.ConsumerCmdFlags{})

			var addr string
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) && addr == "" {
				addr = listener.GetListeningAddress()
				time.Sleep(20 * time.Millisecond)
			}
			require.NotEmpty(t, addr, "listener never reported a listening address")

			httpClient := &http.Client{Timeout: 3 * time.Second}

			// A plain probe is the health check, as it always was.
			plain, err := httpClient.Get("http://" + addr + healthPath)
			require.NoError(t, err)
			plainBody, err := io.ReadAll(plain.Body)
			plain.Body.Close()
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, plain.StatusCode)
			require.Equal(t, "Health status OK", string(plainBody))

			// So is one carrying the upgrade headers: there is nothing on this
			// listener to upgrade it to.
			req, err := http.NewRequest(http.MethodGet, "http://"+addr+healthPath, nil)
			require.NoError(t, err)
			req.Header.Set("Connection", "Upgrade")
			req.Header.Set("Upgrade", "websocket")
			req.Header.Set("Sec-WebSocket-Version", "13")
			req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
			upgrade, err := httpClient.Do(req)
			require.NoError(t, err)
			upgradeBody, err := io.ReadAll(upgrade.Body)
			upgrade.Body.Close()
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, upgrade.StatusCode)
			require.Equal(t, "Health status OK", string(upgradeBody),
				"an upgrade on the health path must still be answered by the health check, not the chain")

			require.Empty(t, stub.seen(), "the health path must never reach the relay")
		})
	}
}

// restBodyRelayStub records the method and body of every request the listener relays.
type restBodyRelayStub struct {
	restHealthRelayStub
	relayed []restRelayedRequest
}

type restRelayedRequest struct {
	method string
	body   string
}

func (s *restBodyRelayStub) SendRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, analytics *metrics.RelayMetrics, metadataValues []pairingtypes.Metadata) (*common.RelayResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.relayed = append(s.relayed, restRelayedRequest{method: connectionType, body: req})
	return &common.RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte(`{}`)}, StatusCode: http.StatusOK}, nil
}

func (s *restBodyRelayStub) last() restRelayedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.relayed) == 0 {
		return restRelayedRequest{}
	}
	return s.relayed[len(s.relayed)-1]
}

// The listener routes POST to its own handler and every other method to the catch-all.
// PUT and PATCH carry a body as POST does (Concordium's wallet-proxy submits
// transactions with PUT), so the catch-all must relay it byte for byte. A GET is relayed
// with an empty body whatever the client attached.
func TestRestChainListener_RelaysTheBodyOfMethodsThatCarryOne(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stub := &restBodyRelayStub{}
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	endpoint := &routersession.RPCEndpoint{
		NetworkAddress:  "127.0.0.1:0",
		ChainID:         "COSMOSHUB",
		ApiInterface:    "rest",
		HealthCheckPath: common.DEFAULT_HEALTH_PATH,
	}
	listener := NewRestChainListener(ctx, endpoint, stub, alwaysHealthyReporter{}, logger)
	go listener.Serve(ctx, common.ConsumerCmdFlags{})

	var addr string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && addr == "" {
		addr = listener.GetListeningAddress()
		time.Sleep(20 * time.Millisecond)
	}
	require.NotEmpty(t, addr, "listener never reported a listening address")

	// Not JSON and not text: the listener must not interpret the body, only carry it.
	const body = "\x00\x01\xfe raw {bytes"
	httpClient := &http.Client{Timeout: 3 * time.Second}
	for _, tc := range []struct {
		method   string
		wantBody string
	}{
		{http.MethodPost, body},
		{http.MethodPut, body},
		{http.MethodPatch, body},
		{http.MethodGet, ""},
	} {
		t.Run(tc.method, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, "http://"+addr+"/v0/submitRawTransaction", strings.NewReader(body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/octet-stream")
			resp, err := httpClient.Do(req)
			require.NoError(t, err)
			_, err = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			got := stub.last()
			require.Equal(t, tc.method, got.method)
			require.Equal(t, tc.wantBody, got.body)
		})
	}
}

// restRefusingRelayStub answers every relay with APINotSupportedError, the error ParseRelay
// returns for a verb the spec does not declare.
type restRefusingRelayStub struct{ restHealthRelayStub }

func (s *restRefusingRelayStub) SendRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, analytics *metrics.RelayMetrics, metadataValues []pairingtypes.Metadata) (*common.RelayResult, error) {
	return nil, common.APINotSupportedError
}

// The catch-all handler maps an undeclared api to 501; the POST handler used to answer the same
// refusal with a masked 500 (MAG-4278).
func TestRestListener_UndeclaredVerbIs501OnEveryHandler(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	listener := NewRestChainListener(ctx, &routersession.RPCEndpoint{
		NetworkAddress:  "127.0.0.1:0",
		ChainID:         "COSMOSHUB",
		ApiInterface:    "rest",
		HealthCheckPath: common.DEFAULT_HEALTH_PATH,
	}, &restRefusingRelayStub{}, alwaysHealthyReporter{}, logger)
	go listener.Serve(ctx, common.ConsumerCmdFlags{})
	var addr string
	for deadline := time.Now().Add(3 * time.Second); addr == "" && time.Now().Before(deadline); {
		if addr = listener.GetListeningAddress(); addr == "" {
			time.Sleep(20 * time.Millisecond)
		}
	}
	require.NotEmpty(t, addr, "listener never reported a listening address")

	httpClient := &http.Client{Timeout: 3 * time.Second}
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		t.Run(method, func(t *testing.T) {
			req, err := http.NewRequest(method, "http://"+addr+"/cosmos/tx/v1beta1/simulate", strings.NewReader("{}"))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			resp, err := httpClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			require.Equal(t, http.StatusNotImplemented, resp.StatusCode, "%s: %s", method, body)
		})
	}
}

// cosmosHubRESTParser is COSMOSHUB's REST parser from the bundled specs. Without listBroadcast, the
// spec loses POST /cosmos/tx/v1beta1/txs, as THORCHAIN's lists no broadcast today (its REST POST
// collection names decode, encode and simulate only), and the broadcast is relayed as an unlisted
// read (defaultApiContainer).
func cosmosHubRESTParser(t *testing.T, listBroadcast bool) ChainParser {
	t.Helper()
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "COSMOSHUB")
	require.NoError(t, err)
	if !listBroadcast {
		dropped := 0
		for _, collection := range spec.ApiCollections {
			if collection.CollectionData.ApiInterface != spectypes.APIInterfaceRest || collection.CollectionData.Type != http.MethodPost {
				continue
			}
			collection.Apis = slices.DeleteFunc(collection.Apis, func(api *spectypes.Api) bool {
				if api.Name == "/cosmos/tx/v1beta1/txs" {
					dropped++
					return true
				}
				return false
			})
		}
		require.Equal(t, 1, dropped, "COSMOSHUB lists the broadcast once")
	}
	parser, err := NewChainParser(spectypes.APIInterfaceRest)
	require.NoError(t, err)
	parser.SetSpec(spec)
	return parser
}

// A POST to Cosmos SDK's broadcast path is the broadcast, whose tx_response.code is the node's verdict
// on the transaction, whether the spec lists it as a stateful write or not (MAG-5326). A GET of the
// same path is the search by events, a read.
func TestRestParseMsg_TheCosmosBroadcastPathIsABroadcast(t *testing.T) {
	refusal := []byte(`{"tx_response":{"height":"0","txhash":"ABCDEF","codespace":"sdk","code":32,"raw_log":"account sequence mismatch"}}`)
	listed, unlisted := cosmosHubRESTParser(t, true), cosmosHubRESTParser(t, false)
	cases := []struct {
		name          string
		parser        ChainParser
		method, path  string
		wantBroadcast bool
	}{
		{"listed as a stateful write", listed, http.MethodPost, "/cosmos/tx/v1beta1/txs", true},
		{"not listed", unlisted, http.MethodPost, "/cosmos/tx/v1beta1/txs", true},
		{"not listed, with a trailing slash", unlisted, http.MethodPost, "/cosmos/tx/v1beta1/txs/", true},
		{"not listed, with a query", unlisted, http.MethodPost, "/cosmos/tx/v1beta1/txs?x=1", true},
		{"the search by events", unlisted, http.MethodGet, "/cosmos/tx/v1beta1/txs?events=tx.height%3D17", false},
		{"another POST", unlisted, http.MethodPost, "/cosmos/tx/v1beta1/simulate", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg, err := tc.parser.ParseMsg(tc.path, []byte(`{"tx_bytes":"CpIB","mode":"BROADCAST_MODE_SYNC"}`), tc.method, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			restMessage, ok := msg.GetRPCMessage().(*rpcInterfaceMessages.RestMessage)
			require.True(t, ok)
			require.Equal(t, tc.wantBroadcast, restMessage.Broadcast)
			hasError, _ := msg.CheckResponseError(refusal, http.StatusOK)
			require.Equal(t, tc.wantBroadcast, hasError, "a tx_response.code is the node's error on the broadcast only")
		})
	}
	msg, err := unlisted.ParseMsg("/cosmos/tx/v1beta1/txs", nil, http.MethodPost, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(msg.GetApi().Name, DefaultApiName), "the spec without the broadcast relays it as an unlisted read")
}

// A path with dot segments, plain or percent-encoded, is read as the path the sender asks the node
// for once they are resolved (common.JoinURLPath): the same api, the same broadcast check, the same
// requested block and the same message path as the resolved path. Before, the parser matched the
// path as sent, so it could match another api than the one the node was asked for.
func TestRestParseMsg_DotSegmentsAreReadAsTheNodeGetsThem(t *testing.T) {
	parser := cosmosHubRESTParser(t, true)
	body := []byte(`{"tx_bytes":"CpIB","mode":"BROADCAST_MODE_SYNC"}`)
	for _, tc := range []struct {
		name, method, path, resolved string
	}{
		{"a dot segment in a write", http.MethodPost, "/cosmos/tx/v1beta1/./txs", "/cosmos/tx/v1beta1/txs"},
		{"a dot-dot segment in a write", http.MethodPost, "/x/../cosmos/tx/v1beta1/txs", "/cosmos/tx/v1beta1/txs"},
		{"a percent-encoded dot segment in a write", http.MethodPost, "/cosmos/tx/v1beta1/%2e/txs", "/cosmos/tx/v1beta1/txs"},
		{"a percent-encoded slash and dot-dot segment, with a query", http.MethodPost, "/cosmos/tx/v1beta1/simulate%2f%2E%2E/txs?x=1", "/cosmos/tx/v1beta1/txs?x=1"},
		{"a dot-dot segment in a read with its block in the path", http.MethodGet, "/cosmos/base/tendermint/v1beta1/blocks/x/../244590", "/cosmos/base/tendermint/v1beta1/blocks/244590"},
		{"a dot-dot segment that climbs to the root before more slashes", http.MethodPost, "/..///cosmos/tx/v1beta1/txs", "/cosmos/tx/v1beta1/txs"},
		{"a percent-encoded one that does the same", http.MethodPost, "/x/%2E%2E///cosmos/tx/v1beta1/txs", "/cosmos/tx/v1beta1/txs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, err := parser.ParseMsg(tc.resolved, body, tc.method, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			got, err := parser.ParseMsg(tc.path, body, tc.method, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			require.False(t, strings.HasPrefix(want.GetApi().GetName(), DefaultApiName), "the resolved path is a spec api")
			require.Equal(t, want.GetApi().GetName(), got.GetApi().GetName())
			require.Equal(t, want.GetApi().Category.Stateful, got.GetApi().Category.Stateful)
			wantBlock, _ := want.RequestedBlock()
			gotBlock, _ := got.RequestedBlock()
			require.Equal(t, wantBlock, gotBlock)
			wantRest, ok := want.GetRPCMessage().(*rpcInterfaceMessages.RestMessage)
			require.True(t, ok)
			gotRest, ok := got.GetRPCMessage().(*rpcInterfaceMessages.RestMessage)
			require.True(t, ok)
			require.Equal(t, wantRest.Broadcast, gotRest.Broadcast)
			require.Equal(t, wantRest.Path, gotRest.Path)
		})
	}
}
