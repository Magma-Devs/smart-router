package rpcsmartrouter

// Tests for the relay_end record store and GET /debug/relay-end (MAG-4899). The acceptance tests
// send each request through the real JSON-RPC HTTP listener, so the GUID they look up is the
// Lava-Guid header the client actually got.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// useRelayEndStore installs a fresh store for one test and removes it afterwards, so the
// package-level recorder neither leaks between tests nor keeps the records of this package's other
// relay tests.
func useRelayEndStore(t *testing.T, capacity, byteBudget int) {
	t.Helper()
	enableRelayEndStore(capacity, byteBudget)
	t.Cleanup(func() { relayEndRecorder.Store(nil) })
}

func newRelayEndMux() http.Handler {
	var offsetNano atomic.Int64
	return buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})
}

// getRelayEnd asks the debug mux for guid's record with method.
func getRelayEnd(t *testing.T, method, guid string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	newRelayEndMux().ServeHTTP(recorder, httptest.NewRequest(method, "/debug/relay-end?guid="+guid, nil))
	return recorder
}

// requireOneRelayEndRecord reads guid's record and requires that the router wrote exactly one.
func requireOneRelayEndRecord(t *testing.T, guid string) map[string]any {
	t.Helper()
	response := getRelayEnd(t, http.MethodGet, guid)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.Equal(t, "1", response.Header().Get("X-Relay-End-Records"), "one relay_end per relay")
	t.Logf("relay_end record: %s", response.Body.String())
	var record map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &record), "the record is one JSON object")
	return record
}

// serveRelayEndJSONRPC puts server behind a real JSON-RPC HTTP listener, which mints each request's
// GUID and returns it as Lava-Guid, and returns its address.
func serveRelayEndJSONRPC(t *testing.T, server *RPCSmartRouterServer) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	endpoint := &routersession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC, HealthCheckPath: common.DEFAULT_HEALTH_PATH}
	listener := chainlib.NewJrpcChainListener(ctx, endpoint, server, nil, logs, nil, NewNoOpWSSubscriptionManager("ETH1", spectypes.APIInterfaceJsonRPC))
	go listener.Serve(ctx, common.ConsumerCmdFlags{})
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = listener.Shutdown(shutdownCtx)
	})
	require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, 3*time.Second, time.Millisecond)
	return listener.GetListeningAddress()
}

// postRelayEnd sends body to the listener and returns the reply's Lava-Guid and body.
func postRelayEnd(t *testing.T, address, body string) (guid, reply string) {
	t.Helper()
	response, err := http.Post("http://"+address+"/", "application/json", bytes.NewBufferString(body))
	require.NoError(t, err)
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	guid = response.Header.Get(common.GUID_HEADER_NAME)
	require.NotEmpty(t, guid, "every reply names its request")
	return guid, string(data)
}

func TestDebugRelayEnd_RecordOfARequest(t *testing.T) {
	rand.InitRandomSeed() // the session manager and the listener draw from it
	useRelayEndStore(t, debugLogBufferCapacity, relayEndStoreByteBudget)
	read := captureRelayEnds(t)
	server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, ethJsonRPCParser(t), relayEndNode{"node-a", relayEndReply(200, relayEndBalance, 0)})
	address := serveRelayEndJSONRPC(t, server)

	guid, reply := postRelayEnd(t, address, relayEndRead)
	require.Contains(t, reply, `"result":"0x10"`)

	record := requireOneRelayEndRecord(t, guid)
	require.Equal(t, guid, record["GUID"], "the record is the request the reply names")
	require.Equal(t, relayEndEvent, record["event"])
	require.Equal(t, float64(relayEndSchema), record["schema"])
	require.Equal(t, relayOutcomeOK, record["outcome"])
	require.Equal(t, "node-a", record["served_by"])

	// The record is the event the log line carries, field for field; only the time it was kept at
	// differs from the time the line was written.
	parsedGUID, err := strconv.ParseUint(guid, 10, 64)
	require.NoError(t, err)
	var logged map[string]any
	require.NoError(t, json.Unmarshal([]byte(onlyRelayEnd(t, read(), parsedGUID)), &logged))
	require.Contains(t, record, "time")
	delete(record, "time")
	delete(logged, "time")
	require.Equal(t, logged, record)
}

func TestDebugRelayEnd_RetriedRequestHasOneRecord(t *testing.T) {
	rand.InitRandomSeed() // the session manager and the listener draw from it
	useRelayEndStore(t, debugLogBufferCapacity, relayEndStoreByteBudget)
	// The only primary refuses the connection, so the read is retried on the backup, which answers.
	server, _ := newRelayEndServerWithBackups(t, "ETH1", spectypes.APIInterfaceJsonRPC, ethJsonRPCParser(t),
		[]relayEndNode{{"node-a", nil}},
		[]relayEndNode{{"node-b", relayEndReply(200, relayEndBalance, 0)}})
	address := serveRelayEndJSONRPC(t, server)

	guid, reply := postRelayEnd(t, address, relayEndRead)
	require.Contains(t, reply, `"result":"0x10"`)

	record := requireOneRelayEndRecord(t, guid)
	require.Equal(t, guid, record["GUID"])
	require.Equal(t, relayOutcomeOK, record["outcome"])
	require.Equal(t, "node-b", record["served_by"])
	attempts, isList := record["attempts"].([]any)
	require.True(t, isList, "a retried relay lists its attempts")
	require.Len(t, attempts, 2, "the request was retried: both attempts are in its one record")
	refused, isObject := attempts[0].(map[string]any)
	require.True(t, isObject)
	require.Equal(t, "node-a", refused["upstream"])
	require.Equal(t, attemptOutcomeTransportError, refused["outcome"])
	answered, isObject := attempts[1].(map[string]any)
	require.True(t, isObject)
	require.Equal(t, "node-b", answered["upstream"])
}

func TestDebugRelayEnd_SubmitNamesItsTransaction(t *testing.T) {
	rand.InitRandomSeed() // the session manager and the listener draw from it
	useRelayEndStore(t, debugLogBufferCapacity, relayEndStoreByteBudget)
	server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, ethJsonRPCParser(t), relayEndNode{"node-a", relayEndReply(200, relayEndTxHash, 0)})
	address := serveRelayEndJSONRPC(t, server)

	guid, reply := postRelayEnd(t, address, relayEndWrite)
	require.Contains(t, reply, "0x5c504ed4")

	record := requireOneRelayEndRecord(t, guid)
	require.Equal(t, guid, record["GUID"])
	require.Equal(t, "eth_sendRawTransaction", record["method"])
	require.Equal(t, true, record["stateful"])
	require.Equal(t, "0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060", record["tx_hash"])
}

func TestDebugRelayEnd_Refusals(t *testing.T) {
	t.Run("unknown GUID is 404", func(t *testing.T) {
		useRelayEndStore(t, debugLogBufferCapacity, relayEndStoreByteBudget)
		recordRelayEnd(&relayEnd{guid: "1"})
		require.Equal(t, http.StatusNotFound, getRelayEnd(t, http.MethodGet, "2").Code)
	})
	t.Run("POST is 405", func(t *testing.T) {
		useRelayEndStore(t, debugLogBufferCapacity, relayEndStoreByteBudget)
		recordRelayEnd(&relayEnd{guid: "1"})
		require.Equal(t, http.StatusMethodNotAllowed, getRelayEnd(t, http.MethodPost, "1").Code)
	})
	t.Run("no GUID is 400", func(t *testing.T) {
		useRelayEndStore(t, debugLogBufferCapacity, relayEndStoreByteBudget)
		require.Equal(t, http.StatusBadRequest, getRelayEnd(t, http.MethodGet, "").Code)
	})
	t.Run("no store is 503, never a 404", func(t *testing.T) {
		require.Nil(t, relayEndRecorder.Load(), "off is the production default")
		require.Equal(t, http.StatusServiceUnavailable, getRelayEnd(t, http.MethodGet, "1").Code)
	})
}

func TestRelayEndStore_OffByDefault(t *testing.T) {
	recordRelayEnd(&relayEnd{guid: "1"})
	_, writes, enabled := readRelayEnd("1")
	require.False(t, enabled, "a read must report that nothing was kept")
	require.Zero(t, writes)
}

// TestRelayEndStore_KeepsTheNewest pins the bound: at capacity the oldest GUID goes.
func TestRelayEndStore_KeepsTheNewest(t *testing.T) {
	useRelayEndStore(t, 2, relayEndStoreByteBudget)
	for _, guid := range []string{"1", "2", "3"} {
		recordRelayEnd(&relayEnd{guid: guid})
	}
	_, writes, enabled := readRelayEnd("1")
	require.True(t, enabled)
	require.Zero(t, writes, "the oldest GUID was dropped")
	for _, guid := range []string{"2", "3"} {
		line, writes, _ := readRelayEnd(guid)
		require.Equal(t, 1, writes)
		require.Contains(t, string(line), `"GUID":"`+guid+`"`)
	}
	require.Len(t, relayEndRecorder.Load().records, 2)
}

// TestRelayEndStore_CountsASecondWrite pins that a second event with one GUID is visible to the
// reader rather than replacing the first silently: a test asserting one record per relay must be
// able to fail.
func TestRelayEndStore_CountsASecondWrite(t *testing.T) {
	useRelayEndStore(t, 2, relayEndStoreByteBudget)
	recordRelayEnd(&relayEnd{guid: "1", outcome: relayOutcomeNoAnswer})
	recordRelayEnd(&relayEnd{guid: "1", outcome: relayOutcomeOK})
	recordRelayEnd(&relayEnd{guid: "2"})

	response := getRelayEnd(t, http.MethodGet, "1")
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "2", response.Header().Get("X-Relay-End-Records"))
	require.Contains(t, response.Body.String(), `"outcome":"ok"`, "the body is the last event written")
	_, writes, _ := readRelayEnd("2")
	require.Equal(t, 1, writes, "a rewritten GUID holds one place, so it does not push another out")
}

func TestRelayEndStore_KeepsNoEventWithoutAGUID(t *testing.T) {
	useRelayEndStore(t, 2, relayEndStoreByteBudget)
	recordRelayEnd(&relayEnd{})
	require.Empty(t, relayEndRecorder.Load().records)
}

// TestRelayEndStore_HoldsAByteBudget pins the byte bound: a record's size follows its attempts, so a
// count alone does not bound the store's memory. With room for two records' bytes and a count bound
// far away, the third record pushes the oldest out.
func TestRelayEndStore_HoldsAByteBudget(t *testing.T) {
	size := len(relayEndLine(&relayEnd{guid: "1"}))
	useRelayEndStore(t, 100, 2*size+size/2)
	for _, guid := range []string{"1", "2", "3"} {
		recordRelayEnd(&relayEnd{guid: guid})
	}
	_, writes, _ := readRelayEnd("1")
	require.Zero(t, writes, "the oldest GUID was dropped for bytes, not count")
	for _, guid := range []string{"2", "3"} {
		_, writes, _ := readRelayEnd(guid)
		require.Equal(t, 1, writes)
	}
	requireRelayEndStoreBytesAdd(t)
}

// A rewrite of a GUID keeps its record and drops nothing, even past the budget, and the bytes follow
// the new line. The next new record restores the bound. The budget holds the first event exactly, so
// the longer rewrite passes it.
func TestRelayEndStore_ARewriteKeepsItsRecord(t *testing.T) {
	size := len(relayEndLine(&relayEnd{guid: "1"}))
	useRelayEndStore(t, 100, size)
	recordRelayEnd(&relayEnd{guid: "1"})
	recordRelayEnd(&relayEnd{guid: "1", method: "eth_getLogs", outcome: relayOutcomeOK, servedBy: "a-node-with-a-long-name"})

	line, writes, _ := readRelayEnd("1")
	require.Equal(t, 2, writes)
	require.Contains(t, string(line), "eth_getLogs", "the record is the last event written")
	requireRelayEndStoreBytesAdd(t)

	recordRelayEnd(&relayEnd{guid: "2"})
	_, writes, _ = readRelayEnd("1")
	require.Zero(t, writes, "the next new record drops the oldest GUID to fit the budget")
	requireRelayEndStoreBytesAdd(t)
	require.LessOrEqual(t, relayEndRecorder.Load().bytes, size)
}

// One record larger than the whole budget is still kept, alone.
func TestRelayEndStore_KeepsOneRecordLargerThanTheBudget(t *testing.T) {
	useRelayEndStore(t, 100, 10)
	recordRelayEnd(&relayEnd{guid: "1"})
	recordRelayEnd(&relayEnd{guid: "2"})

	_, writes, _ := readRelayEnd("1")
	require.Zero(t, writes)
	_, writes, _ = readRelayEnd("2")
	require.Equal(t, 1, writes)
	requireRelayEndStoreBytesAdd(t)
}

// requireRelayEndStoreBytesAdd checks the store's byte count is the sum of the lines it holds.
func requireRelayEndStoreBytesAdd(t *testing.T) {
	t.Helper()
	store := relayEndRecorder.Load()
	store.mu.Lock()
	defer store.mu.Unlock()
	sum := 0
	for _, record := range store.records {
		sum += len(record.line)
	}
	require.Equal(t, sum, store.bytes)
	require.Len(t, store.order, len(store.records))
}

// TestEnableDebugRecorders_InstallsTheRelayEndStore pins what enableDebugRecorders installs: without
// the relay_end store, every router started with --debug-address answers /debug/relay-end with 503.
// That Start calls it is exercised only where a router boots (k3d): no unit test runs Start. The
// cleanup turns the log buffer back off, as utils.DisableDebugLogBuffer asks of a test that enables
// it, and puts both recorders back as they were.
func TestEnableDebugRecorders_InstallsTheRelayEndStore(t *testing.T) {
	previousCrossValidation, previousRelayEnd := crossValidationEventRecorder.Load(), relayEndRecorder.Load()
	t.Cleanup(func() {
		utils.DisableDebugLogBuffer()
		crossValidationEventRecorder.Store(previousCrossValidation)
		relayEndRecorder.Store(previousRelayEnd)
		relaycore.DisableRelayAnswerRecording()
	})
	relayEndRecorder.Store(nil)
	relaycore.DisableRelayAnswerRecording()

	enableDebugRecorders()

	store := relayEndRecorder.Load()
	require.NotNil(t, store, "the relay_end store is installed")
	require.Equal(t, debugLogBufferCapacity, store.capacity)
	require.Equal(t, relayEndStoreByteBudget, store.byteBudget)
	require.NotNil(t, crossValidationEventRecorder.Load(), "the cross-validation ring is installed")
	_, _, answersOn := relaycore.ReadRelayAnswers("", 1)
	require.True(t, answersOn, "the relay-answer ring is installed")
	utils.FormatInfo("enableDebugRecorders probe line")
	found := false
	for _, line := range utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 100) {
		found = found || bytes.Contains(line, []byte("enableDebugRecorders probe line"))
	}
	require.True(t, found, "the /debug/logs buffer is on")
}

// A subscription request's relay_end is kept too: ReportSubscriptionRelay writes through the same
// emitRelayEnd as every other relay.
func TestDebugRelayEnd_ASubscriptionRequestIsKept(t *testing.T) {
	useRelayEndStore(t, debugLogBufferCapacity, relayEndStoreByteBudget)
	server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, ethJsonRPCParser(t))
	subscribe, err := server.ParseRelay(context.Background(), "", `{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`, http.MethodPost, relayEndNoCustomerID, "", nil)
	require.NoError(t, err)
	ctx := chainlib.WithSubscriptionUpstream(utils.WithUniqueIdentifier(context.Background(), 4242))

	server.ReportSubscriptionRelay(ctx, subscribe, time.Now(), nil)

	record := requireOneRelayEndRecord(t, "4242")
	require.Equal(t, "4242", record["GUID"])
	require.Equal(t, "eth_subscribe", record["method"])
}
