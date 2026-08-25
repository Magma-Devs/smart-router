package endpointstate

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestBlockNumRequestBody pins the GET_BLOCKNUM template resolution. The bug it guards: the cosmos
// gRPC GET_BLOCKNUM directive (Service/GetLatestBlock) carries the method in api_name and a
// legitimately-EMPTY request body, but the poller treated an empty function_template as "method
// undefined" and hard-failed — so every cosmos-gRPC endpoint's per-endpoint ChainTracker retried
// forever ("GET_BLOCKNUM missing function template apiInterface=grpc"). gRPC must resolve an empty
// template to "{}"; REST/Tendermint must still reject it (they genuinely need a path/method).
func TestBlockNumRequestBody(t *testing.T) {
	for _, tc := range []struct {
		name         string
		apiInterface string
		template     string
		wantBody     string
		wantOK       bool
	}{
		{"grpc empty template → {}", spectypes.APIInterfaceGrpc, "", "{}", true},
		{"grpc explicit template passes through", spectypes.APIInterfaceGrpc, `{"height":"%d"}`, `{"height":"%d"}`, true},
		{"rest empty template is a hard error", spectypes.APIInterfaceRest, "", "", false},
		{"rest path passes through", spectypes.APIInterfaceRest, "/cosmos/base/tendermint/v1beta1/blocks/latest", "/cosmos/base/tendermint/v1beta1/blocks/latest", true},
		{"tendermintrpc empty template is a hard error", spectypes.APIInterfaceTendermintRPC, "", "", false},
		{"jsonrpc empty template is a hard error", spectypes.APIInterfaceJsonRPC, "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, ok := blockNumRequestBody(tc.apiInterface, tc.template)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.wantBody, string(body))
			} else {
				require.Nil(t, body)
			}
		})
	}
}

// TestEndpointPoller_CustomMessage_POSTDelegatesToConnection verifies the
// Solana path: SVMChainTracker calls CustomMessage with the getLatestBlockhash
// JSON-RPC body. The previous implementation returned a hard error, so on every
// Solana-family chain the per-endpoint ChainTracker silently failed to start —
// no OnNewBlock callback, no per-endpoint metrics, backup rows stuck at N/A.
// This test asserts that CustomMessage now delegates to the direct RPC connection
// and returns the real response payload.
func TestEndpointPoller_CustomMessage_POSTDelegatesToConnection(t *testing.T) {
	const (
		url        = "https://solana.lava.build:443/"
		svmRequest = `{"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash","params":[{"commitment":"finalized"}]}`
		svmResp    = `{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":100},"value":{"blockhash":"abc","lastValidBlockHeight":42}}}`
	)

	conn := &mockDirectRPCConnection{
		url: url,
		responses: map[string][]byte{
			svmRequest: []byte(svmResp),
		},
	}
	fetcher := NewEndpointPoller(
		&lavasession.Endpoint{NetworkAddress: url, Enabled: true},
		conn,
		nil, // chainParser unused by the POST path
		"SOLANA",
		"jsonrpc",
	)

	got, err := fetcher.CustomMessage(context.Background(), "", []byte(svmRequest), "POST", "getLatestBlockhash")
	require.NoError(t, err,
		"CustomMessage must not return a stub error — SVMChainTracker depends on it for getLatestBlockhash")
	require.Equal(t, svmResp, string(got),
		"CustomMessage must return the actual upstream response body")
}

// TestEndpointMonitor_ForcesBlocksToSave1ForSolana guards the blocksToSave
// override that sidesteps SVMChainTracker's slot-cache-only-for-latest-block limitation.
// When blocksToSave > 1 the ChainTracker init loop fetches hashes for historical blocks,
// and on every Solana-family chain those fetches fail with "slot not found in cache",
// killing the tracker before OnNewBlock can fire.
func TestEndpointMonitor_ForcesBlocksToSave1ForSolana(t *testing.T) {
	ctx := t.Context()

	for _, tc := range []struct {
		chainID  string
		expected uint64
		reason   string
	}{
		{"SOLANA", 1, "Solana mainnet must force blocksToSave=1 to avoid SVMChainTracker slot-cache misses"},
		{"SOLANAT", 1, "Solana testnet uses the same SVMChainTracker"},
		{"KOII", 1, "KOII is a Solana fork — same chain tracker family"},
		{"ETH", 10, "EVM chains keep the caller-requested blocksToSave"},
		{"LAVA", 10, "non-SVM chains keep the caller-requested blocksToSave"},
	} {
		t.Run(tc.chainID, func(t *testing.T) {
			m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
				ChainID:      tc.chainID,
				ApiInterface: "jsonrpc",
				BlocksToSave: 10,
			})
			require.NotNil(t, m)
			defer m.Stop()
			require.Equal(t, tc.expected, m.blocksToSave, tc.reason)
		})
	}
}

// TestEndpointPoller_CustomMessage_PropagatesMissingConnection guards the
// remaining nil-connection check in sendRawRequest: with no direct connection the
// fetcher must surface an error rather than treat an empty body as a successful
// fetch. (The old per-socket health gate is gone — a live-but-failing socket now
// surfaces its failure through SendRequest itself, which the relay path turns into
// a QoS penalty, instead of being pre-empted by a latched health bit.)
func TestEndpointPoller_CustomMessage_PropagatesMissingConnection(t *testing.T) {
	fetcher := NewEndpointPoller(
		&lavasession.Endpoint{NetworkAddress: "https://solana.lava.build:443/", Enabled: true},
		nil, // no direct connection
		nil,
		"SOLANA",
		"jsonrpc",
	)

	_, err := fetcher.CustomMessage(context.Background(), "", []byte(`{}`), "POST", "getLatestBlockhash")
	require.Error(t, err, "CustomMessage must fail when there is no direct connection")
}

// recordingConnection captures exactly what the poller handed the transport, so a test can
// assert WHICH url, method and body a given (apiInterface, connectionType) pair targets. It
// implements both DirectRPCConnection (the jsonrpc / tendermintrpc / gRPC route) and
// HTTPDirectRPCDoer (the REST route), so one mock can show that REST leaves through
// DoHTTPRequest carrying a path while jsonrpc stays on SendRequest against the base URL.
type recordingConnection struct {
	url        string
	statusCode int    // status returned by DoHTTPRequest; 0 means 200
	respBody   []byte // body returned by both routes

	httpCalls []lavasession.HTTPRequestParams
	sendCalls []recordedSend
}

type recordedSend struct {
	data    []byte
	headers map[string]string
}

func (m *recordingConnection) SendRequest(ctx context.Context, data []byte, headers map[string]string) (*lavasession.DirectRPCResponse, error) {
	m.sendCalls = append(m.sendCalls, recordedSend{data: data, headers: headers})
	return &lavasession.DirectRPCResponse{Data: m.respBody, StatusCode: http.StatusOK}, nil
}

func (m *recordingConnection) DoHTTPRequest(ctx context.Context, params lavasession.HTTPRequestParams) (*lavasession.HTTPDirectRPCResponse, error) {
	m.httpCalls = append(m.httpCalls, params)
	status := m.statusCode
	if status == 0 {
		status = http.StatusOK
	}
	return &lavasession.HTTPDirectRPCResponse{StatusCode: status, Body: m.respBody}, nil
}

func (m *recordingConnection) GetProtocol() lavasession.DirectRPCProtocol {
	return lavasession.DirectRPCProtocolHTTP
}
func (m *recordingConnection) Close() error                { return nil }
func (m *recordingConnection) GetURL() string              { return m.url }
func (m *recordingConnection) GetNodeUrl() *common.NodeUrl { return nil }

// TestEndpointPoller_SendRawRequestRouting pins WHERE each (apiInterface, connectionType)
// pair sends its poll — the one decision in this file that has now been wrong twice.
//
// REST POST used to hand the body to SendRequest, which POSTs to the bare base URL: Tron
// answered 405 on every poll until endpoint availability hit zero and the interface stopped
// serving ("No pairings available", MAG-2597). Because the fix keys on apiInterface rather
// than on connectionType, the rows that matter most here are the NEGATIVE ones — jsonrpc,
// tendermintrpc and gRPC all reach that branch with connectionType POST and must keep
// targeting the base URL, gRPC with its method in a header. A REST fix that silently
// redirected every jsonrpc chain would be a far worse bug than the one it fixed, and only a
// test can hold that line in CI (the live probe that found the bug cannot).
func TestEndpointPoller_SendRawRequestRouting(t *testing.T) {
	const baseURL = "https://node.example.com/gateway/KEY"

	for _, tc := range []struct {
		name            string
		apiInterface    string
		connectionType  string
		requestData     string
		apiName         string
		wantURL         string // non-empty: expect the REST route, targeting this full URL
		wantMethod      string
		wantBody        string
		wantContentType string
		wantGrpcHeader  string // non-empty: expect this x-grpc-method on the SendRequest route
	}{
		{
			name:           "rest GET appends the template to the base URL and sends no body",
			apiInterface:   spectypes.APIInterfaceRest,
			connectionType: "GET",
			requestData:    "/cosmos/base/tendermint/v1beta1/blocks/latest",
			apiName:        "/cosmos/base/tendermint/v1beta1/blocks/latest",
			wantURL:        baseURL + "/cosmos/base/tendermint/v1beta1/blocks/latest",
			wantMethod:     "GET",
		},
		{
			name:            "rest POST appends apiName to the base URL and sends the template as the body",
			apiInterface:    spectypes.APIInterfaceRest,
			connectionType:  "POST",
			requestData:     `{"detail":false}`,
			apiName:         "/wallet/getnowblock",
			wantURL:         baseURL + "/wallet/getnowblock",
			wantMethod:      "POST",
			wantBody:        `{"detail":false}`,
			wantContentType: "application/json",
		},
		{
			name:           "jsonrpc POST keeps the base URL and names the method in the body",
			apiInterface:   spectypes.APIInterfaceJsonRPC,
			connectionType: "POST",
			requestData:    `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`,
			apiName:        "eth_blockNumber",
			wantBody:       `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`,
		},
		{
			name:           "tendermintrpc POST keeps the base URL",
			apiInterface:   spectypes.APIInterfaceTendermintRPC,
			connectionType: "POST",
			requestData:    `{"jsonrpc":"2.0","id":1,"method":"status","params":[]}`,
			apiName:        "status",
			wantBody:       `{"jsonrpc":"2.0","id":1,"method":"status","params":[]}`,
		},
		{
			name:           "grpc POST keeps the base URL and carries the method in a header",
			apiInterface:   spectypes.APIInterfaceGrpc,
			connectionType: "POST",
			requestData:    "{}",
			apiName:        "cosmos.base.tendermint.v1beta1.Service/GetLatestBlock",
			wantBody:       "{}",
			wantGrpcHeader: "cosmos.base.tendermint.v1beta1.Service/GetLatestBlock",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &recordingConnection{url: baseURL, respBody: []byte(`{"ok":true}`)}
			poller := NewEndpointPoller(
				&lavasession.Endpoint{NetworkAddress: baseURL, Enabled: true},
				conn,
				nil, // chainParser unused: sendRawRequest does no parsing
				"TRX",
				tc.apiInterface,
			)

			got, err := poller.sendRawRequest(context.Background(), []byte(tc.requestData), tc.connectionType, tc.apiName, metrics.TrackerRequestKindLatestBlock)
			require.NoError(t, err)
			require.Equal(t, `{"ok":true}`, string(got), "the upstream body must be returned verbatim")

			if tc.wantURL != "" {
				require.Empty(t, conn.sendCalls, "REST must not fall through to the base-URL SendRequest route")
				require.Len(t, conn.httpCalls, 1)
				call := conn.httpCalls[0]
				require.Equal(t, tc.wantURL, call.URL, "REST polls the method path, not the host root (MAG-2597)")
				require.Equal(t, tc.wantMethod, call.Method)
				require.Equal(t, tc.wantBody, string(call.Body))
				require.Equal(t, tc.wantContentType, call.ContentType)
				return
			}

			require.Empty(t, conn.httpCalls, "only REST may be rewritten onto a path — this interface must keep the base URL")
			require.Len(t, conn.sendCalls, 1)
			call := conn.sendCalls[0]
			require.Equal(t, tc.wantBody, string(call.data))
			require.Equal(t, "application/json", call.headers["Content-Type"])
			require.Equal(t, tc.wantGrpcHeader, call.headers[lavasession.GRPCMethodHeader],
				"gRPC dials the method from this header; every other interface must leave it unset")
		})
	}
}

// TestEndpointPoller_CustomMessage_PathArgument pins the contract of CustomMessage's `path`
// argument. It used to be ignored outright, which was harmless only because REST non-GET had
// no path of its own — every request went to the base URL. Now that REST non-GET routes on a
// path, ignoring it would silently send a caller that passed "/wallet/getnowblock" to the
// method name instead. The gRPC row is the other half of the contract: there apiName is the
// dialed method, so a path must never displace it.
func TestEndpointPoller_CustomMessage_PathArgument(t *testing.T) {
	const baseURL = "https://node.example.com/gateway/KEY"

	for _, tc := range []struct {
		name           string
		apiInterface   string
		path           string
		apiName        string
		wantURL        string // non-empty: expect the REST route with this URL
		wantGrpcHeader string
	}{
		{
			name:         "rest POST prefers an explicit path over apiName",
			apiInterface: spectypes.APIInterfaceRest,
			path:         "/wallet/getnowblock",
			apiName:      "getnowblock",
			wantURL:      baseURL + "/wallet/getnowblock",
		},
		{
			name:         "rest POST falls back to apiName when no path is given",
			apiInterface: spectypes.APIInterfaceRest,
			path:         "",
			apiName:      "/wallet/getnowblock",
			wantURL:      baseURL + "/wallet/getnowblock",
		},
		{
			name:           "grpc POST ignores path so it cannot displace the method header",
			apiInterface:   spectypes.APIInterfaceGrpc,
			path:           "/some/http/path",
			apiName:        "cosmos.base.tendermint.v1beta1.Service/GetLatestBlock",
			wantGrpcHeader: "cosmos.base.tendermint.v1beta1.Service/GetLatestBlock",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &recordingConnection{url: baseURL, respBody: []byte(`{"ok":true}`)}
			poller := NewEndpointPoller(
				&lavasession.Endpoint{NetworkAddress: baseURL, Enabled: true},
				conn,
				nil,
				"TRX",
				tc.apiInterface,
			)

			_, err := poller.CustomMessage(context.Background(), tc.path, []byte(`{}`), "POST", tc.apiName)
			require.NoError(t, err)

			if tc.wantURL != "" {
				require.Len(t, conn.httpCalls, 1)
				require.Equal(t, tc.wantURL, conn.httpCalls[0].URL)
				return
			}

			require.Empty(t, conn.httpCalls)
			require.Len(t, conn.sendCalls, 1)
			require.Equal(t, tc.wantGrpcHeader, conn.sendCalls[0].headers[lavasession.GRPCMethodHeader])
		})
	}
}

// TestEndpointPoller_RESTRejectionSurfacesHTTPStatus pins the failure shape of the bug this
// change fixes: an upstream rejection on the REST route must come back as a typed
// *HTTPStatusError carrying the code, because that is what the relay path reads to classify
// the error (rpcsmartrouter_server.go reads StatusCode off this type). A bare error would
// classify a 405 as an unknown transport failure.
func TestEndpointPoller_RESTRejectionSurfacesHTTPStatus(t *testing.T) {
	const baseURL = "https://api.trongrid.io"

	conn := &recordingConnection{
		url:        baseURL,
		statusCode: http.StatusMethodNotAllowed,
		respBody:   []byte("405 Not Allowed"),
	}
	poller := NewEndpointPoller(
		&lavasession.Endpoint{NetworkAddress: baseURL, Enabled: true},
		conn,
		nil,
		"TRX",
		spectypes.APIInterfaceRest,
	)

	_, err := poller.sendRawRequest(context.Background(), []byte(`{}`), "POST", "/wallet/getnowblock", metrics.TrackerRequestKindLatestBlock)
	require.Error(t, err)

	var statusErr *lavasession.HTTPStatusError
	require.True(t, errors.As(err, &statusErr), "REST rejections must stay typed for error classification")
	require.Equal(t, http.StatusMethodNotAllowed, statusErr.StatusCode)
	require.Equal(t, "405 Not Allowed", string(statusErr.Body), "the upstream body must survive for diagnostics")
}
