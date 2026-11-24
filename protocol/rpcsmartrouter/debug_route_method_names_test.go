package rpcsmartrouter

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relaycoretest"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// The unlisted REST path the tests below send: the caller's own path, with a key in it.
const (
	debugRouteSecret       = "AbC123SecretKey"
	debugRouteUnlistedPath = "/v1/" + debugRouteSecret + "/status?apikey=" + debugRouteSecret
	debugRouteListedPath   = "/cosmos/base/tendermint/v1beta1/blocks/latest"
)

// A request is named by URL on REST and on a Tendermint URI, and in the body everywhere else. An
// interface the list does not know counts as a URL, so it withholds rather than shows.
func TestRequestNamedByURL(t *testing.T) {
	for _, tc := range []struct {
		apiInterface string
		body         []byte
		want         bool
	}{
		{spectypes.APIInterfaceRest, nil, true},
		{spectypes.APIInterfaceRest, []byte(`{"a":1}`), true},
		{spectypes.APIInterfaceTendermintRPC, nil, true},
		{spectypes.APIInterfaceTendermintRPC, []byte(`{"jsonrpc":"2.0","method":"status","id":1}`), false},
		{spectypes.APIInterfaceJsonRPC, nil, false},
		{spectypes.APIInterfaceGraphQL, nil, false},
		{spectypes.APIInterfaceGrpc, nil, false},
		{"a-later-interface", nil, true},
	} {
		require.Equal(t, tc.want, requestNamedByURL(tc.apiInterface, tc.body), "%s body=%q", tc.apiInterface, tc.body)
	}
}

// MAG-5422: GET /debug/relay-errors names a request as relay_end does, end to end through the relay,
// for a node error and for a protocol error alike. An unlisted REST path, which is the caller's own
// path and may carry a credential, is counted as Default-REDACTED; a listed one loses its leading
// slash. Before, both kept the path as written.
func TestDebugRelayErrors_NameARequestAsRelayEndDoes(t *testing.T) {
	rand.InitRandomSeed()
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), "COSMOSHUB", spectypes.APIInterfaceRest, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)

	for _, tc := range []struct {
		name       string
		path       string
		node       relayEndNode
		wantMethod string
		nodeErrors bool
	}{
		{"a node error on an unlisted path", debugRouteUnlistedPath, relayEndNode{"node-a", relayEndReply(400, `{"code":3,"message":"bad request"}`, 0)}, "Default-REDACTED", true},
		{"a protocol error on an unlisted path", debugRouteUnlistedPath, relayEndNode{name: "node-a"}, "Default-REDACTED", false},
		{"a node error on a listed path", debugRouteListedPath, relayEndNode{"node-a", relayEndReply(400, `{"code":3,"message":"bad request"}`, 0)}, "cosmos/base/tendermint/v1beta1/blocks/latest", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newRelayEndServer(t, "COSMOSHUB", spectypes.APIInterfaceRest, chainParser, tc.node)
			server.relayErrors = relaycore.NewRelayErrorTally()

			ctx, cancel := context.WithTimeout(utils.WithUniqueIdentifier(context.Background(), 5422), relayEndShortWait)
			defer cancel()
			_, _ = server.SendRelay(ctx, tc.path, "", http.MethodGet, relayEndNoCustomerID, "", nil, nil)

			counts := server.relayErrors.Counts()
			require.Len(t, counts, 1, "%+v", counts)
			require.Equal(t, tc.wantMethod, counts[0].Method)
			require.NotContains(t, counts[0].Method, debugRouteSecret)
			if tc.nodeErrors {
				require.Positive(t, counts[0].NodeErrors)
			} else {
				require.Positive(t, counts[0].ProtocolErrors)
			}
		})
	}
}

// MAG-5422: a request that names its method in the body keeps that name on GET /debug/relay-errors,
// as relay_end keeps it, on a node error and on a protocol error alike: an unlisted JSON-RPC method,
// a batch with an unlisted member, and an unlisted method in a Tendermint JSON-RPC body. Only a name
// the URL carries is withheld, so a site that withheld every unlisted name fails here.
func TestDebugRelayErrors_KeepABodyNamedRequestsNameAsRelayEndDoes(t *testing.T) {
	rand.InitRandomSeed()
	read := captureRelayEnds(t)
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	parsers := map[string]chainlib.ChainParser{}
	for _, listener := range []struct{ chainID, apiInterface string }{
		{"ETH1", spectypes.APIInterfaceJsonRPC},
		{"COSMOSHUB", spectypes.APIInterfaceTendermintRPC},
	} {
		chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), listener.chainID, listener.apiInterface, noop, nil, "../../", nil)
		if closeServer != nil {
			t.Cleanup(closeServer)
		}
		require.NoError(t, err)
		parsers[listener.apiInterface] = chainParser
	}

	const nodeDown = `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"node is down"}}`
	// connectionType is what each listener relays a POST body with: the JSON-RPC listener names the
	// method, the Tendermint listener none.
	for _, tc := range []struct {
		name, chainID, apiInterface, connectionType, body, nodeReply, wantMethod string
	}{
		{
			"an unlisted JSON-RPC method", "ETH1", spectypes.APIInterfaceJsonRPC, http.MethodPost,
			`{"jsonrpc":"2.0","id":1,"method":"eth_notAMethod","params":[]}`, nodeDown, "Default-eth_notAMethod",
		},
		{
			"a batch with an unlisted member", "ETH1", spectypes.APIInterfaceJsonRPC, http.MethodPost,
			`[{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]},{"jsonrpc":"2.0","id":2,"method":"eth_notAMethod","params":[]}]`,
			`[` + nodeDown + `,{"jsonrpc":"2.0","id":2,"error":{"code":-32603,"message":"node is down"}}]`, "eth_blockNumber&Default-eth_notAMethod",
		},
		{
			"an unlisted method in a Tendermint body", "COSMOSHUB", spectypes.APIInterfaceTendermintRPC, "",
			`{"jsonrpc":"2.0","id":1,"method":"not_a_method","params":[]}`, nodeDown, "Default-not_a_method",
		},
	} {
		for _, nodeError := range []bool{true, false} {
			kind, node := "a protocol error", relayEndNode{name: "node-a"}
			if nodeError {
				kind, node.handler = "a node error", relayEndReply(http.StatusOK, tc.nodeReply, 0)
			}
			t.Run(kind+" on "+tc.name, func(t *testing.T) {
				server := newRelayEndServer(t, tc.chainID, tc.apiInterface, parsers[tc.apiInterface], node)
				server.relayErrors = relaycore.NewRelayErrorTally()
				guid := relayEndCaseGUID(t.Name())
				ctx, cancel := context.WithTimeout(utils.WithUniqueIdentifier(context.Background(), guid), relayEndShortWait)
				defer cancel()
				_, _ = server.SendRelay(ctx, "", tc.body, tc.connectionType, relayEndNoCustomerID, "", nil, nil)

				var event map[string]any
				require.NoError(t, json.Unmarshal([]byte(onlyRelayEnd(t, read(), guid)), &event))
				require.Equal(t, tc.wantMethod, event["method"], "relay_end keeps a name the body carries")
				counts := server.relayErrors.Counts()
				require.Len(t, counts, 1, "%+v", counts)
				require.Equal(t, tc.wantMethod, counts[0].Method, "the row is named as relay_end names the request")
				if nodeError {
					require.Positive(t, counts[0].NodeErrors)
				} else {
					require.Positive(t, counts[0].ProtocolErrors)
				}
			})
		}
	}
}

// MAG-5422: GET /debug/cross-validation-events names a request as relay_end does, on both recording
// paths. A dissent on an unlisted REST path is recorded as Default-REDACTED, never as the caller's
// path; a listed path loses its leading slash; an unlisted method named in the body keeps its name,
// as relay_end keeps it.
func TestCrossValidationEvents_NameARequestAsRelayEndDoes(t *testing.T) {
	restAPI := func(name string) *spectypes.Api {
		return &spectypes.Api{Name: name, Category: spectypes.SpecCategory{Deterministic: true}}
	}
	for _, tc := range []struct {
		apiInterface, apiName, want string
	}{
		{spectypes.APIInterfaceRest, "Default-" + debugRouteUnlistedPath, "Default-REDACTED"},
		{spectypes.APIInterfaceRest, debugRouteListedPath, "cosmos/base/tendermint/v1beta1/blocks/latest"},
		{spectypes.APIInterfaceJsonRPC, "Default-eth_notAMethod", "Default-eth_notAMethod"},
	} {
		t.Run("reply time: "+tc.want, func(t *testing.T) {
			useCrossValidationEventRing(t, 32)
			srv, _ := newCrossValidationEventTestServer(t)
			srv.listenEndpoint = &routersession.RPCEndpoint{ChainID: "COSMOSHUB", ApiInterface: tc.apiInterface}
			ctx := utils.WithUniqueIdentifier(context.Background(), 542201)

			hashA, hashB := sha256.Sum256([]byte("A")), sha256.Sum256([]byte("B"))
			rp := &MockRelayProcessorForHeaders{
				crossValidationParams:           &common.CrossValidationParams{AgreementThreshold: 2, MaxParticipants: 3},
				selection:                       relaycore.CrossValidation,
				crossValidationQueriedProviders: []string{"p1", "p2", "p3"},
				successResults: []common.RelayResult{
					{ProviderInfo: common.ProviderInfo{ProviderAddress: "p1"}, ResponseHash: hashA},
					{ProviderInfo: common.ProviderInfo{ProviderAddress: "p2"}, ResponseHash: hashA},
					{ProviderInfo: common.ProviderInfo{ProviderAddress: "p3"}, ResponseHash: hashB},
				},
			}
			relayResult := &common.RelayResult{
				ProviderInfo:    common.ProviderInfo{ProviderAddress: "p1"},
				CrossValidation: 2,
				ResponseHash:    hashA,
				Reply:           &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
			}
			srv.appendHeadersToRelayResult(ctx, relayResult, 0, rp, &MockProtocolMessage{api: restAPI(tc.apiName), requestedBlock: 100}, tc.apiName, nil, true)

			events, _, enabled := readCrossValidationEvents(crossValidationEventFilter{RequestID: "542201"})
			require.True(t, enabled)
			require.Len(t, events, 1)
			require.Equal(t, tc.want, events[0].Method)
			require.NotContains(t, events[0].Method, debugRouteSecret)
		})

		t.Run("straggler: "+tc.want, func(t *testing.T) {
			useCrossValidationEventRing(t, 32)
			srv, chainParser := newCrossValidationEventTestServer(t)
			srv.listenEndpoint = &routersession.RPCEndpoint{ChainID: "COSMOSHUB", ApiInterface: tc.apiInterface}
			ctx := utils.WithUniqueIdentifier(context.Background(), 542202)

			// A cross-validation processor with two agreeing answers and p3 still in flight, the state
			// the straggler watcher starts from (as TestCrossValidationEvents_StragglerPathRecords).
			push := func(rp *relaycore.RelayProcessor, provider string, body string) {
				rp.SetResponse(&relaycore.RelayResponse{RelayResult: common.RelayResult{
					Reply: &pairingtypes.RelayReply{Data: []byte(body)}, ProviderInfo: common.ProviderInfo{ProviderAddress: provider}, StatusCode: 200,
				}})
			}
			cm, err := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xDEAD","latest"],"id":1}`), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			pm := chainlib.NewProtocolMessage(cm, map[string]string{
				common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "3",
				common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
			}, nil, "dapp", "1.2.3.4")
			sm, err := NewSmartRouterRelayStateMachineWithPolicy(context.Background(), routersession.NewUsedProviders(nil), &SmartRouterRelaySenderMock{retValue: nil}, pm, nil, false, nil, "ETH1", "jsonrpc")
			require.NoError(t, err)
			rp := relaycore.NewRelayProcessor(context.Background(), sm.GetCrossValidationParams(), relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, sm)
			rp.SetCrossValidationQueriedProviders([]string{"p1", "p2", "p3"})
			push(rp, "p1", `{"jsonrpc":"2.0","id":1,"result":"0xAAAA"}`)
			push(rp, "p2", `{"jsonrpc":"2.0","id":1,"result":"0xAAAA"}`)
			require.Len(t, rp.NodeResults(), 2)
			successResults, _, _ := rp.GetResultsData()
			relayResult := &common.RelayResult{
				ProviderInfo:    common.ProviderInfo{ProviderAddress: "p1"},
				CrossValidation: 2,
				ResponseHash:    successResults[0].ResponseHash,
				Reply:           &pairingtypes.RelayReply{Metadata: []pairingtypes.Metadata{}},
			}

			srv.watchCrossValidationStragglers(ctx, rp, relayResult, &MockProtocolMessage{api: restAPI(tc.apiName), requestedBlock: 100}, tc.apiName, []string{"p3"}, nil)
			push(rp, "p3", `{"jsonrpc":"2.0","id":1,"result":"0xBBBB"}`)

			var events []crossValidationEvent
			require.Eventually(t, func() bool {
				events, _, _ = readCrossValidationEvents(crossValidationEventFilter{RequestID: "542202"})
				return len(events) == 1
			}, 5*time.Second, 10*time.Millisecond, "the straggler watcher records the late dissent")
			require.Equal(t, crossValidationEventSourceStraggler, events[0].Source)
			require.Equal(t, tc.want, events[0].Method)
			require.NotContains(t, events[0].Method, debugRouteSecret)
		})
	}
}
