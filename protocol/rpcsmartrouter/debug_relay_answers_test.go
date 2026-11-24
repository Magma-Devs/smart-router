package rpcsmartrouter

// Tests for GET /debug/relay-answers (MAG-4358): the answers one request received, in the order
// they reached the router, read with the request id its reply carries.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlibtest"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relayprotocol"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// relayAnswerRow is a row of the route, decoded by its field names.
type relayAnswerRow struct {
	Seq             uint64
	ElapsedMs       int64
	ChainID         string
	ApiInterface    string
	RequestID       string
	Method          string
	ProviderAddress string
	StatusCode      int
	Outcome         string
}

func useRelayAnswerRecording(t *testing.T) {
	t.Helper()
	relaycore.EnableRelayAnswerRecording(relaycore.RelayAnswerRingCapacity, relayAnswerMethod)
	t.Cleanup(relaycore.DisableRelayAnswerRecording)
}

func getRelayAnswers(t *testing.T, query string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	newEventsMux().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/debug/relay-answers"+query, nil))
	return recorder
}

// sendWriteAs is sendWrite on a request whose guid is guid, the id the listener stamps on a
// request and the reply carries as Router-Guid.
func sendWriteAs(t *testing.T, guid uint64, tc writeInterfaceCase, chainParser chainlib.ChainParser, server *RPCSmartRouterServer) (*common.RelayResult, error) {
	t.Helper()
	ctx := utils.WithUniqueIdentifier(context.Background(), guid)
	chainMessage, err := chainParser.ParseMsg(tc.path, []byte(tc.body), tc.method, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	relayData := relayprotocol.NewRelayData(ctx, tc.method, tc.path, []byte(tc.body), 0, spectypes.NOT_APPLICABLE, tc.apiInterface,
		chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, nil, relayData, "dapp", "1.2.3.4")
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return server.SendParsedRelay(callCtx, nil, protocolMessage)
}

// TestDebugRelayAnswers_AWriteShowsTheRefusalBeforeTheAcceptance is the ticket's acceptance case,
// end to end: a REST write goes to three primaries, one refuses it at once with a 400, one accepts
// it 300 ms later, and the third accepts it 600 ms later, after the router has replied. Read with
// the reply's request id, the route lists all three providers in the order their answers reached
// the router, the status each answered with, and what the router made of each.
func TestDebugRelayAnswers_AWriteShowsTheRefusalBeforeTheAcceptance(t *testing.T) {
	rand.InitRandomSeed()
	useRelayAnswerRecording(t)
	tc := writeInterfaceCases[1]
	require.Equal(t, spectypes.APIInterfaceRest, tc.apiInterface)
	chainParser := tc.parser(t)
	refuser := writeUpstream{name: "refuser-" + t.Name(), status: http.StatusBadRequest, body: `{"code":3,"message":"tx already in mempool"}`}
	acceptor := writeUpstream{name: "acceptor-" + t.Name(), status: http.StatusOK, body: tc.success, delay: 300 * time.Millisecond}
	late := writeUpstream{name: "late-" + t.Name(), status: http.StatusOK, body: tc.success, delay: 600 * time.Millisecond}
	server := newWriteServer(t, tc, chainParser, []writeUpstream{late, acceptor, refuser}, nil, make([]int64, 3))

	const guid = 4358
	result, err := sendWriteAs(t, guid, tc, chainParser, server)
	require.NoError(t, err)
	require.Equal(t, acceptor.name, result.ProviderInfo.ProviderAddress, "setup: the first acceptance is the reply")

	var rows []relayAnswerRow
	require.Eventually(t, func() bool {
		recorder := getRelayAnswers(t, "?request_id="+strconv.Itoa(guid))
		return recorder.Code == http.StatusOK && json.Unmarshal(recorder.Body.Bytes(), &rows) == nil && len(rows) == 3
	}, statefulDeliveryLinger, 20*time.Millisecond, "the route lists every provider that answered the write")

	type answer struct {
		Provider string
		Status   int
		Outcome  string
	}
	got := make([]answer, 0, len(rows))
	for _, row := range rows {
		got = append(got, answer{row.ProviderAddress, row.StatusCode, row.Outcome})
	}
	require.Equal(t, []answer{
		{refuser.name, http.StatusBadRequest, relaycore.RelayAnswerNodeError},
		{acceptor.name, http.StatusOK, relaycore.RelayAnswerSuccess},
		{late.name, http.StatusOK, relaycore.RelayAnswerUnread},
	}, got, "the answers in the order they reached the router")

	require.Less(t, rows[0].Seq, rows[1].Seq, "Seq is the order key a caller that re-sorts the rows reads")
	require.Less(t, rows[1].Seq, rows[2].Seq)
	require.GreaterOrEqual(t, rows[1].ElapsedMs, int64(300), "the acceptance was held back 300 ms")
	require.Less(t, rows[0].ElapsedMs, rows[1].ElapsedMs)
	for _, row := range rows {
		require.Equal(t, strconv.Itoa(guid), row.RequestID)
		require.Equal(t, tc.chainID, row.ChainID)
		require.Equal(t, tc.apiInterface, row.ApiInterface)
		require.Equal(t, "cosmos/tx/v1beta1/txs", row.Method, "relay_end's name for the api, without the leading slash")
	}

	recorder := getRelayAnswers(t, "?request_id=1")
	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `[]`, recorder.Body.String(), "another request's id matches none of these answers")
}

func TestDebugRelayAnswers_RecorderOffIsNot200(t *testing.T) {
	relaycore.DisableRelayAnswerRecording()
	recorder := getRelayAnswers(t, "")
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code, "an empty 200 would read as \"nothing answered\"")
}

func TestDebugRelayAnswers_GetOnly(t *testing.T) {
	useRelayAnswerRecording(t)
	recorder := httptest.NewRecorder()
	newEventsMux().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/debug/relay-answers", nil))
	require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
}

func TestDebugRelayAnswers_EmptyAndDroppedHeader(t *testing.T) {
	useRelayAnswerRecording(t)
	recorder := getRelayAnswers(t, "?request_id=1&limit=5")
	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `[]`, recorder.Body.String())
	require.Equal(t, "0", recorder.Header().Get("X-Relay-Answers-Dropped"))
}

// An unlisted REST api is named by the path the client sent, which may carry a credential. The ring
// names the request as relay_end does (TestRelayEnd_UnlistedRestPathIsWithheld), so the route
// shows Default-REDACTED, for the request and in the listing of every request alike.
func TestDebugRelayAnswers_AnUnlistedRestPathIsWithheld(t *testing.T) {
	rand.InitRandomSeed()
	useRelayAnswerRecording(t)
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlibtest.CreateChainLibMocks(context.Background(), "COSMOSHUB", spectypes.APIInterfaceRest, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	server := newRelayEndServer(t, "COSMOSHUB", spectypes.APIInterfaceRest, chainParser, relayEndNode{"node-a", relayEndReply(200, `{}`, 0)})
	const guid = 4359
	_, err = sendRelayEnd(t, server, guid, relayEndShortWait, "/v1/AbC123SecretKey/status?apikey=AbC123SecretKey", "")
	require.NoError(t, err)

	recorder := getRelayAnswers(t, "?request_id="+strconv.Itoa(guid))
	require.Equal(t, http.StatusOK, recorder.Code)
	var rows []relayAnswerRow
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &rows))
	require.Len(t, rows, 1)
	require.Equal(t, "Default-REDACTED", rows[0].Method)
	require.NotContains(t, getRelayAnswers(t, "").Body.String(), "AbC123SecretKey", "nor in the listing of every request")
}

// An unlisted method named in a body is the client's own text, of any length. The ring keeps the
// last few thousand answers, so its name is capped as relay_end caps it: one long request cannot
// make the ring as big as the request times its capacity.
func TestDebugRelayAnswers_AnUnlistedMethodIsCapped(t *testing.T) {
	rand.InitRandomSeed()
	useRelayAnswerRecording(t)
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlibtest.CreateChainLibMocks(context.Background(), "COSMOSHUB", spectypes.APIInterfaceTendermintRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	server := newRelayEndServer(t, "COSMOSHUB", spectypes.APIInterfaceTendermintRPC, chainParser,
		relayEndNode{"node-a", relayEndReply(200, `{"jsonrpc":"2.0","id":1,"result":{}}`, 0)})
	const guid = 4360
	long := "not_a_method_" + strings.Repeat("x", 64*1024)
	ctx, cancel := context.WithTimeout(utils.WithUniqueIdentifier(context.Background(), guid), relayEndShortWait)
	defer cancel()
	_, _ = server.SendRelay(ctx, "", `{"jsonrpc":"2.0","method":"`+long+`","params":[],"id":1}`, "", relayEndNoCustomerID, "", nil, nil)

	recorder := getRelayAnswers(t, "?request_id="+strconv.Itoa(guid))
	require.Equal(t, http.StatusOK, recorder.Code)
	var rows []relayAnswerRow
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &rows))
	require.Len(t, rows, 1, "the unlisted method was relayed and its answer recorded")
	require.True(t, strings.HasPrefix(rows[0].Method, "Default-not_a_method_"), rows[0].Method[:40])
	require.LessOrEqual(t, len(rows[0].Method), relayEndMethodLimit+len("...truncated..."))
}
