package chainlib

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// longBodyWithRunesAtTheCuts returns a request body longer than relayMsgLogMaxChars with a
// 3-byte rune across each cut, and the form the log lines must carry: the head keeps
// relayMsgLogMaxChars-relayMsgLogMaxChars/3 bytes and the tail relayMsgLogMaxChars/3, each less the
// rune it would split.
func longBodyWithRunesAtTheCuts() (body, logged string) {
	head, tail := relayMsgLogMaxChars-relayMsgLogMaxChars/3, relayMsgLogMaxChars/3
	a, c := strings.Repeat("a", head-1), strings.Repeat("c", tail-2)
	return a + "€" + strings.Repeat("b", 100) + "€" + c, a + "...truncated..." + c
}

func TestRelayBodyForLog(t *testing.T) {
	body, logged := longBodyWithRunesAtTheCuts()
	require.Equal(t, logged, relayBodyForLog(body, false))
	require.True(t, utf8.ValidString(relayBodyForLog(body, false)))
	require.Equal(t, body, relayBodyForLog(body, true), "--debug-relays logs the whole body")
	require.Equal(t, `{"id":1}`, relayBodyForLog(`{"id":1}`, false), "a short body is logged whole")
}

// failingRelay fails every request, so a listener writes its request line and then its failure
// lines: the UniqueGuidResponseForError line and the in/out line at error level.
type failingRelay struct{}

func (failingRelay) SendRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, analytics *metrics.RelayMetrics, metadataValues []pairingtypes.Metadata) (*common.RelayResult, error) {
	return nil, errors.New("no upstream answered")
}

func (failingRelay) ParseRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, metadata []pairingtypes.Metadata) (ProtocolMessage, error) {
	return nil, errors.New("the frame does not parse")
}

func (failingRelay) SendParsedRelay(ctx context.Context, analytics *metrics.RelayMetrics, protocolMessage ProtocolMessage) (*common.RelayResult, error) {
	return nil, errors.New("not used")
}

// streamingRelaySender is stubRelaySender with a gRPC subscription manager behind it, so a served
// gRPC listener takes its stream path.
type streamingRelaySender struct {
	*stubRelaySender
	manager *stubGRPCSubscriptionManager
}

func (s streamingRelaySender) GetGRPCSubscriptionManager() GRPCSubscriptionManager {
	return s.manager
}

// wsSendFailingStub parses every websocket frame as a plain relay and fails to send it.
type wsSendFailingStub struct{ wsRelayStub }

func (wsSendFailingStub) SendParsedRelay(ctx context.Context, analytics *metrics.RelayMetrics, protocolMessage ProtocolMessage) (*common.RelayResult, error) {
	return nil, errors.New("no upstream answered")
}

type newTestListener func(context.Context, *lavasession.RPCEndpoint, RelaySender, *metrics.RPCConsumerLogs) ChainListener

// serveWithFlags starts a listener on an ephemeral port in front of relay, under cmdFlags, and
// returns its address. The listener stops with the test.
func serveWithFlags(t *testing.T, newListener newTestListener, apiInterface string, relay RelaySender, cmdFlags common.ConsumerCmdFlags) string {
	t.Helper()
	// GenerateUniqueIdentifier uses the custom rand package, which the package-level TestMain
	// does not seed. InitRandomSeed is idempotent.
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	listener := newListener(ctx, &lavasession.RPCEndpoint{
		NetworkAddress:  "127.0.0.1:0",
		ChainID:         "LAV1",
		ApiInterface:    apiInterface,
		HealthCheckPath: common.DEFAULT_HEALTH_PATH,
	}, relay, logger)
	go listener.Serve(ctx, cmdFlags)
	addr := ""
	for deadline := time.Now().Add(3 * time.Second); addr == "" && time.Now().Before(deadline); {
		if addr = listener.GetListeningAddress(); addr == "" {
			time.Sleep(20 * time.Millisecond)
		}
	}
	require.NotEmpty(t, addr, "listener never reported a listening address")
	return addr
}

// loggedRecords decodes every record in the debug ring, keyed by nothing: a test picks its lines by
// message and GUID.
func loggedRecords(t *testing.T) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 0) {
		record := map[string]any{}
		require.NoError(t, json.Unmarshal(line, &record))
		records = append(records, record)
	}
	return records
}

// requireLinesOfGUID returns the records a Loki line filter for the GUID, |= "\"GUID\":\"<guid>\"",
// finds, keyed by message.
func requireLinesOfGUID(t *testing.T, guid string) map[string]map[string]any {
	t.Helper()
	require.NotEmpty(t, guid)
	found := map[string]map[string]any{}
	for _, line := range utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 0) {
		if !strings.Contains(string(line), `"GUID":"`+guid+`"`) {
			continue
		}
		record := map[string]any{}
		require.NoError(t, json.Unmarshal(line, &record))
		message, _ := record["message"].(string)
		found[message] = record
	}
	return found
}

// A request body holds what the client sent, a signed transaction included, and a listener logs it
// on its request line at info and on its failure line at error. Both carry the body's head and
// tail, cut on rune boundaries, unless --debug-relays asks for whole bodies. Every line of a failed
// request carries its GUID under GUID, so a search for the request finds the failure too.
func TestListeners_LogTheBodyCutUnlessDebugRelays(t *testing.T) {
	utils.EnableDebugLogBuffer(1000)
	t.Cleanup(utils.DisableDebugLogBuffer)

	body, cut := longBodyWithRunesAtTheCuts()
	newJsonRPC := func(ctx context.Context, endpoint *lavasession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
		return NewJrpcChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger, nil, nil)
	}
	newRest := func(ctx context.Context, endpoint *lavasession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
		return NewRestChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger)
	}
	newTendermint := func(ctx context.Context, endpoint *lavasession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
		return NewTendermintRpcChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger, nil, nil)
	}
	newGraphQL := func(ctx context.Context, endpoint *lavasession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
		return NewGraphQLChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger)
	}

	for _, debugRelays := range []bool{false, true} {
		want := cut
		if debugRelays {
			want = body
		}
		flags := common.ConsumerCmdFlags{DebugRelays: debugRelays}
		name := func(listener string) string {
			if debugRelays {
				return listener + " with --debug-relays"
			}
			return listener
		}

		for _, tc := range []struct {
			name, apiInterface, method, path  string
			requestLine, bodyKey, failureLine string
			newListener                       newTestListener
		}{
			{"jsonrpc POST", spectypes.APIInterfaceJsonRPC, http.MethodPost, "/", "Consumer received a new JSON-RPC request", "body", "jsonrpc http", newJsonRPC},
			{"tendermintrpc POST", spectypes.APIInterfaceTendermintRPC, http.MethodPost, "/", "in <<<", "_msg", "tendermint http in/out", newTendermint},
			{"rest POST", spectypes.APIInterfaceRest, http.MethodPost, "/cosmos/tx/v1beta1/txs", "Consumer received a new REST POST request", "body", "http in/out", newRest},
			{"rest PUT", spectypes.APIInterfaceRest, http.MethodPut, "/cosmos/tx/v1beta1/txs", "Consumer received a new REST non-POST request", "body", "http in/out", newRest},
			{"graphql POST", spectypes.APIInterfaceGraphQL, http.MethodPost, "/", "Consumer received a new GraphQL request", "body", "graphql in/out", newGraphQL},
		} {
			t.Run(name(tc.name), func(t *testing.T) {
				utils.ClearDebugLogBuffer()
				addr := serveWithFlags(t, tc.newListener, tc.apiInterface, failingRelay{}, flags)
				requestID := "req-" + strings.ReplaceAll(name(tc.name), " ", "-")
				req, err := http.NewRequest(tc.method, "http://"+addr+tc.path, strings.NewReader(body))
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-Request-Id", requestID)
				resp, err := http.DefaultClient.Do(req)
				require.NoError(t, err)
				_, _ = io.Copy(io.Discard, resp.Body)
				require.NoError(t, resp.Body.Close())

				var requestLine map[string]any
				for _, line := range utils.ReadDebugLogBuffer(requestID, time.Time{}, time.Time{}, 0) {
					record := map[string]any{}
					require.NoError(t, json.Unmarshal(line, &record))
					if record["message"] == tc.requestLine {
						requestLine = record
					}
				}
				require.NotNil(t, requestLine, "no %q line for the request", tc.requestLine)
				require.Equal(t, "info", requestLine["level"])
				require.Equal(t, want, requestLine[tc.bodyKey], "the request line's body")

				guid, _ := requestLine["GUID"].(string)
				lines := requireLinesOfGUID(t, guid)
				require.Contains(t, lines, tc.failureLine, "the failure line carries the request's GUID")
				require.Equal(t, "error", lines[tc.failureLine]["level"])
				require.Equal(t, want, lines[tc.failureLine]["request"], "the failure line's body")
				require.Contains(t, lines, "UniqueGuidResponseForError", "the error-masking line carries the request's GUID under GUID")
			})
		}

		for _, ws := range []struct {
			name          string
			relay         RelaySender
			subscriptions WSSubscriptionManager
		}{
			{"websocket frame that does not parse", failingRelay{}, nil},
			{"websocket relay that fails", wsSendFailingStub{}, nil},
			{"websocket subscribe that fails", wsSubscribeStub{}, failingSubscriptions{}},
		} {
			t.Run(name(ws.name), func(t *testing.T) {
				utils.ClearDebugLogBuffer()
				newListener := func(ctx context.Context, endpoint *lavasession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
					return NewJrpcChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger, nil, ws.subscriptions)
				}
				addr := serveWithFlags(t, newListener, spectypes.APIInterfaceJsonRPC, ws.relay, flags)
				client, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
				require.NoError(t, err)
				defer client.Close()
				require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte(body)))
				require.NoError(t, client.SetReadDeadline(time.Now().Add(3*time.Second)))
				_, _, err = client.ReadMessage() // the error reply, sent after the failure is logged
				require.NoError(t, err)

				// The line's path is the address the connection came in on: a connection an
				// earlier case closed may still log its read failure into the ring.
				var failureLine map[string]any
				for _, record := range loggedRecords(t) {
					if record["message"] == "jsonrpc ws msg" && record["path"] == addr {
						require.Nil(t, failureLine, "one failed frame, one failure line")
						failureLine = record
					}
				}
				require.NotNil(t, failureLine, "no failure line for the frame")
				require.Equal(t, "error", failureLine["level"])
				require.Equal(t, want, failureLine["request"], "the failure line's frame")
				guid, _ := failureLine["GUID"].(string)
				require.Contains(t, requireLinesOfGUID(t, guid), "UniqueGuidResponseForError", "the error-masking line carries the frame's GUID under GUID")
			})
		}

		t.Run(name("grpc unary"), func(t *testing.T) {
			utils.ClearDebugLogBuffer()
			sender := &stubRelaySender{parser: grpcParserWithSubscription()}
			newGrpc := func(ctx context.Context, endpoint *lavasession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
				return NewGrpcChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger, sender.parser)
			}
			addr := serveWithFlags(t, newGrpc, spectypes.APIInterfaceGrpc, sender, flags)
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer conn.Close()
			requestID := "req-" + strings.ReplaceAll(name("grpc unary"), " ", "-")
			ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(context.Background(), "X-Request-Id", requestID), 5*time.Second)
			defer cancel()
			var reply []byte
			require.Error(t, conn.Invoke(ctx, "/"+unaryApiName, []byte(body), &reply, grpc.ForceCodec(grpcproxy.RawBytesCodec{}), grpc.WaitForReady(true)))

			var requestLine map[string]any
			for _, line := range utils.ReadDebugLogBuffer(requestID, time.Time{}, time.Time{}, 0) {
				record := map[string]any{}
				require.NoError(t, json.Unmarshal(line, &record))
				if record["message"] == "in <<< GRPC Relay " {
					requestLine = record
				}
			}
			require.NotNil(t, requestLine, "no request line for the call")
			guid, _ := requestLine["GUID"].(string)
			lines := requireLinesOfGUID(t, guid)
			require.Contains(t, lines, "grpc in/out")
			// The gRPC listener hands the body over in the path slot.
			require.Equal(t, want, lines["grpc in/out"]["path"], "the failure line's body")
			require.Contains(t, lines, "UniqueGuidResponseForError", "the error-masking line carries the call's GUID under GUID")
		})

		t.Run(name("grpc stream subscribe"), func(t *testing.T) {
			utils.ClearDebugLogBuffer()
			manager := newStubGRPCSubscriptionManager()
			manager.startErr = errors.New("no upstream answered")
			sender := streamingRelaySender{stubRelaySender: &stubRelaySender{parser: grpcParserWithSubscription()}, manager: manager}
			newGrpc := func(ctx context.Context, endpoint *lavasession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
				return NewGrpcChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger, sender.parser)
			}
			addr := serveWithFlags(t, newGrpc, spectypes.APIInterfaceGrpc, sender, flags)
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, "/"+streamingApiName, grpc.ForceCodec(grpcproxy.RawBytesCodec{}), grpc.WaitForReady(true))
			require.NoError(t, err)
			require.NoError(t, stream.SendMsg([]byte(body)))
			require.NoError(t, stream.CloseSend())
			var reply []byte
			require.Error(t, stream.RecvMsg(&reply))

			var failureLine map[string]any
			for _, record := range loggedRecords(t) {
				if record["message"] == "grpc stream in/out" {
					failureLine = record
				}
			}
			require.NotNil(t, failureLine, "no failure line for the subscribe")
			require.Equal(t, want, failureLine["path"], "the failure line's body")
		})
	}
}
