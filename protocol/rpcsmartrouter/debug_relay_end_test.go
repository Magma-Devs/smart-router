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
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// useRelayEndStore installs a fresh store for one test and removes it afterwards, so the
// package-level recorder neither leaks between tests nor keeps the records of this package's other
// relay tests.
func useRelayEndStore(t *testing.T, capacity int) {
	t.Helper()
	enableRelayEndStore(capacity)
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
	useRelayEndStore(t, debugLogBufferCapacity)
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
	useRelayEndStore(t, debugLogBufferCapacity)
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
	useRelayEndStore(t, debugLogBufferCapacity)
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
		useRelayEndStore(t, debugLogBufferCapacity)
		recordRelayEnd(&relayEnd{guid: "1"})
		require.Equal(t, http.StatusNotFound, getRelayEnd(t, http.MethodGet, "2").Code)
	})
	t.Run("POST is 405", func(t *testing.T) {
		useRelayEndStore(t, debugLogBufferCapacity)
		recordRelayEnd(&relayEnd{guid: "1"})
		require.Equal(t, http.StatusMethodNotAllowed, getRelayEnd(t, http.MethodPost, "1").Code)
	})
	t.Run("no GUID is 400", func(t *testing.T) {
		useRelayEndStore(t, debugLogBufferCapacity)
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
	useRelayEndStore(t, 2)
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
	useRelayEndStore(t, 2)
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
	useRelayEndStore(t, 2)
	recordRelayEnd(&relayEnd{})
	require.Empty(t, relayEndRecorder.Load().records)
}
