package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// The golden files in testdata/relay_end are the event as docs/RELAY-END-EVENT.md describes it,
// one per exit path. Regenerate with: go test ./protocol/rpcsmartrouter/ -run RelayEnd -update-relay-end
var updateRelayEndGolden = flag.Bool("update-relay-end", false, "rewrite testdata/relay_end from the captured events")

// captureRelayEnds turns on the in-memory log sink, which LogEvent writes to like the
// others, and returns a reader for the relay_end lines written since.
func captureRelayEnds(t *testing.T) func() []string {
	t.Helper()
	utils.EnableDebugLogBuffer(200000)
	t.Cleanup(utils.DisableDebugLogBuffer)
	return func() []string {
		var lines []string
		for _, record := range utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 200000) {
			if strings.Contains(string(record), `"event":"relay_end"`) {
				lines = append(lines, strings.TrimSpace(string(record)))
			}
		}
		return lines
	}
}

// onlyRelayEnd returns the one relay_end written for guid, failing on none or several.
func onlyRelayEnd(t *testing.T, lines []string, guid uint64) string {
	t.Helper()
	var mine []string
	for _, line := range lines {
		if strings.Contains(line, `"GUID":"`+strconv.FormatUint(guid, 10)+`"`) {
			mine = append(mine, line)
		}
	}
	require.Len(t, mine, 1, "exactly one relay_end per relay")
	t.Logf("relay_end: %s", mine[0])
	return mine[0]
}

var relayEndPort = regexp.MustCompile(`127\.0\.0\.1:\d+`)

// normalizeRelayEnd drops what differs between runs: the timestamp, latencies and loopback ports.
// Attempts of one batch go out in arbitrary order, so a case that fans out sorts them.
func normalizeRelayEnd(t *testing.T, line string, sortAttempts bool) map[string]any {
	t.Helper()
	var event map[string]any
	require.NoError(t, json.Unmarshal([]byte(line), &event), line)
	delete(event, "time")
	requireRelayEndNumber(t, event, "latency_ms")
	event["latency_ms"] = "<ms>"
	// Error texts quote the loopback port and, in their attribute dump, the GUID.
	guid := requireRelayEndString(t, event, "GUID")
	stable := func(text string) string {
		text = relayEndPort.ReplaceAllString(text, "127.0.0.1:<port>")
		if guid != "" {
			text = strings.ReplaceAll(text, guid, "<GUID>")
		}
		return text
	}
	event["error"] = stable(requireRelayEndString(t, event, "error"))

	raw, present := event["attempts"]
	if !present {
		return event
	}
	list, isList := raw.([]any)
	require.True(t, isList, "attempts is an array")
	attempts := make([]map[string]any, 0, len(list))
	for _, item := range list {
		attempt, isObject := item.(map[string]any)
		require.True(t, isObject, "an attempt is an object")
		if _, known := attempt["latency_ms"]; known {
			requireRelayEndNumber(t, attempt, "latency_ms")
			attempt["latency_ms"] = "<ms>"
		}
		attempt["message"] = stable(requireRelayEndString(t, attempt, "message"))
		attempts = append(attempts, attempt)
	}
	if sortAttempts {
		sort.SliceStable(attempts, func(i, j int) bool {
			return fmt.Sprint(attempts[i]["upstream"]) < fmt.Sprint(attempts[j]["upstream"])
		})
	}
	event["attempts"] = attempts
	return event
}

func requireRelayEndString(t *testing.T, object map[string]any, key string) string {
	t.Helper()
	value, isString := object[key].(string)
	require.True(t, isString, "%s is a string", key)
	return value
}

func requireRelayEndNumber(t *testing.T, object map[string]any, key string) {
	t.Helper()
	_, isNumber := object[key].(float64)
	require.True(t, isNumber, "%s is a number", key)
}

func requireRelayEndGolden(t *testing.T, name string, event map[string]any) {
	t.Helper()
	var got bytes.Buffer
	encoder := json.NewEncoder(&got)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	require.NoError(t, encoder.Encode(event))
	path := filepath.Join("testdata", "relay_end", name+".json")
	if *updateRelayEndGolden {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, got.Bytes(), 0o644))
		return
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err, "golden file missing: run with -update-relay-end")
	require.JSONEq(t, string(want), got.String())
}

// relayEndNode is one upstream. A nil handler is an address that refuses connections.
type relayEndNode struct {
	name    string
	handler http.HandlerFunc
}

func relayEndReply(status int, body string, delay time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

// relayEndSilent takes the request and never answers. The body is drained so the server notices
// when the router gives up on the attempt and hangs up.
func relayEndSilent(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	<-r.Context().Done()
}

// newRelayEndServer is a real router over real upstreams, every one a primary.
func newRelayEndServer(t *testing.T, chainID, apiInterface string, chainParser chainlib.ChainParser, nodes ...relayEndNode) *RPCSmartRouterServer {
	t.Helper()
	ctx := context.Background()
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: chainID, ApiInterface: apiInterface}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, chainID)
	csm := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())
	primaries := map[uint64]*lavasession.ConsumerSessionsWithProvider{}
	for i, node := range nodes {
		var url string
		if node.handler == nil {
			refused := httptest.NewServer(http.NotFoundHandler())
			url = refused.URL
			refused.Close()
		} else {
			upstream := httptest.NewServer(node.handler)
			t.Cleanup(upstream.Close)
			// Runs first: a write's attempt outlives its request, and Close waits for its handler.
			t.Cleanup(upstream.CloseClientConnections)
			url = upstream.URL
		}
		connection, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: url}, 5, "")
		require.NoError(t, err)
		endpoint := &lavasession.Endpoint{NetworkAddress: url, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{connection}}
		provider := lavasession.NewConsumerSessionWithProvider(node.name, []*lavasession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		primaries[uint64(i)] = provider
	}
	if len(nodes) > 0 {
		require.NoError(t, csm.UpdateAllProviders(1, primaries, map[uint64]*lavasession.ConsumerSessionsWithProvider{}))
	}
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	return &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        chainParser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     csm,
		rpcSmartRouterLogs: logs,
	}
}

// sendRelayEnd sends body the way every HTTP listener does: SendRelay, with the GUID the
// listener minted on the context.
func sendRelayEnd(t *testing.T, server *RPCSmartRouterServer, guid uint64, timeout time.Duration, url, body string, metadata ...pairingtypes.Metadata) (*common.RelayResult, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(utils.WithUniqueIdentifier(context.Background(), guid), timeout)
	defer cancel()
	return server.SendRelay(ctx, url, body, http.MethodPost, "dapp", "", nil, metadata)
}

const (
	relayEndRead      = `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0000000000000000000000000000000000000001","latest"],"id":1}`
	relayEndWrite     = `{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x02f8b00184773594008504a817c80083030d4094a0b86991c6218b36c1d19d4a2e9eb0ce3606eb4880b844a9059cbb"],"id":1}`
	relayEndBalance   = `{"jsonrpc":"2.0","id":1,"result":"0x10"}`
	relayEndTxHash    = `{"jsonrpc":"2.0","id":1,"result":"0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060"}`
	relayEndReverted  = `{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}}`
	relayEndNonceLow  = `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"nonce too low: next nonce 7, tx nonce 6"}}`
	relayEndShortWait = 5 * time.Second
)

// relayEndCaseGUID is stable per case name, so adding a case leaves the other golden files alone.
func relayEndCaseGUID(name string) uint64 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(name))
	return 7_000_000 + uint64(hash.Sum32()%1_000_000)
}

func TestRelayEnd_ExitPaths(t *testing.T) {
	rand.InitRandomSeed()
	eth := ethJsonRPCParser(t)

	cases := []struct {
		name string
		// fanOut marks a stateful relay, whose single batch goes out in arbitrary order.
		fanOut   bool
		nodes    func(t *testing.T) []relayEndNode
		url      string
		body     string
		metadata []pairingtypes.Metadata
		timeout  time.Duration
		check    func(t *testing.T, result *common.RelayResult, err error)
	}{
		{
			name: "read_ok",
			nodes: func(*testing.T) []relayEndNode {
				return []relayEndNode{{"node-a", relayEndReply(200, relayEndBalance, 0)}}
			},
			body:  relayEndRead,
			check: func(t *testing.T, _ *common.RelayResult, err error) { require.NoError(t, err) },
		},
		{
			name: "read_node_error",
			nodes: func(*testing.T) []relayEndNode {
				return []relayEndNode{{"node-a", relayEndReply(200, relayEndReverted, 0)}}
			},
			body: relayEndRead,
			check: func(t *testing.T, result *common.RelayResult, err error) {
				require.NoError(t, err, "a node error is the node's answer")
				require.Contains(t, string(result.Reply.Data), "execution reverted")
			},
		},
		{
			name:   "write_node_error",
			fanOut: true,
			nodes: func(*testing.T) []relayEndNode {
				return []relayEndNode{
					{"node-a", relayEndReply(200, relayEndNonceLow, 0)},
					{"node-b", relayEndReply(200, relayEndNonceLow, 300*time.Millisecond)},
				}
			},
			body:  relayEndWrite,
			check: func(t *testing.T, _ *common.RelayResult, err error) { require.NoError(t, err) },
		},
		{
			name:   "write_accepted_after_a_rejection",
			fanOut: true,
			nodes: func(*testing.T) []relayEndNode {
				return []relayEndNode{
					{"node-a", relayEndReply(200, relayEndNonceLow, 0)},
					{"node-b", relayEndReply(200, relayEndTxHash, 300*time.Millisecond)},
				}
			},
			body: relayEndWrite,
			check: func(t *testing.T, result *common.RelayResult, err error) {
				require.NoError(t, err)
				require.Contains(t, string(result.Reply.Data), "0x5c504ed4")
			},
		},
		{
			name:   "write_unknown",
			fanOut: true,
			nodes: func(t *testing.T) []relayEndNode {
				return []relayEndNode{{"node-a", relayEndReply(200, relayEndNonceLow, 0)}, {"node-b", relayEndSilent}}
			},
			body:    relayEndWrite,
			timeout: 1500 * time.Millisecond,
			check:   func(t *testing.T, _ *common.RelayResult, err error) { require.ErrorIs(t, err, errUnknownWriteOutcome) },
		},
		{
			// Nothing comes back at all: the router returns no result, and the client still gets 500.
			name:   "write_unknown_every_upstream_silent",
			fanOut: true,
			nodes: func(*testing.T) []relayEndNode {
				return []relayEndNode{{"node-a", relayEndSilent}, {"node-b", relayEndSilent}}
			},
			body:    relayEndWrite,
			timeout: 1500 * time.Millisecond,
			check: func(t *testing.T, result *common.RelayResult, err error) {
				require.ErrorIs(t, err, errUnknownWriteOutcome)
				require.Nil(t, result)
			},
		},
		{
			name:  "no_answer",
			nodes: func(*testing.T) []relayEndNode { return []relayEndNode{{"node-a", nil}} },
			body:  relayEndRead,
			check: func(t *testing.T, _ *common.RelayResult, err error) { require.Error(t, err) },
		},
		{
			name:   "pinned_write",
			fanOut: true,
			nodes: func(*testing.T) []relayEndNode {
				return []relayEndNode{{"node-a", relayEndReply(200, relayEndTxHash, 0)}, {"node-b", relayEndReply(200, relayEndTxHash, 0)}}
			},
			body:     relayEndWrite,
			metadata: []pairingtypes.Metadata{{Name: common.SELECT_PROVIDER_HEADER_NAME, Value: "node-b"}},
			check:    func(t *testing.T, result *common.RelayResult, err error) { require.NoError(t, err) },
		},
		{
			name: "pinned_provider_unavailable",
			nodes: func(*testing.T) []relayEndNode {
				return []relayEndNode{{"node-a", relayEndReply(200, relayEndBalance, 0)}}
			},
			body:     relayEndRead,
			metadata: []pairingtypes.Metadata{{Name: common.SELECT_PROVIDER_HEADER_NAME, Value: "node-z"}},
			check:    func(t *testing.T, _ *common.RelayResult, err error) { require.Error(t, err) },
		},
		{
			name:  "rejected_no_providers",
			nodes: func(*testing.T) []relayEndNode { return nil },
			body:  relayEndRead,
			check: func(t *testing.T, _ *common.RelayResult, err error) { require.Error(t, err) },
		},
		{
			name: "rejected_unparsable_body",
			nodes: func(*testing.T) []relayEndNode {
				return []relayEndNode{{"node-a", relayEndReply(200, relayEndBalance, 0)}}
			},
			body:  `{"jsonrpc":"2.0","method":`,
			check: func(t *testing.T, _ *common.RelayResult, err error) { require.Error(t, err) },
		},
		{
			name: "batch",
			nodes: func(*testing.T) []relayEndNode {
				return []relayEndNode{{"node-a", relayEndReply(200, `[{"jsonrpc":"2.0","id":1,"result":"0x10"},{"jsonrpc":"2.0","id":2,"result":"0x1"}]`, 0)}}
			},
			body:  `[{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0000000000000000000000000000000000000001","latest"],"id":1},{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":2}]`,
			check: func(t *testing.T, _ *common.RelayResult, err error) { require.NoError(t, err) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			read := captureRelayEnds(t)
			server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, eth, tc.nodes(t)...)
			guid := relayEndCaseGUID(tc.name)
			timeout := tc.timeout
			if timeout == 0 {
				timeout = relayEndShortWait
			}
			result, err := sendRelayEnd(t, server, guid, timeout, tc.url, tc.body, tc.metadata...)
			tc.check(t, result, err)
			requireRelayEndGolden(t, tc.name, normalizeRelayEnd(t, onlyRelayEnd(t, read(), guid), tc.fanOut))
		})
	}
}

// A REST path is the api name, and the collector would rewrite it as /REDACTED with its slash.
func TestRelayEnd_RestWriteMethodKeepsItsPath(t *testing.T) {
	rand.InitRandomSeed()
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), "COSMOSHUB", spectypes.APIInterfaceRest, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)

	read := captureRelayEnds(t)
	server := newRelayEndServer(t, "COSMOSHUB", spectypes.APIInterfaceRest, chainParser,
		relayEndNode{"node-a", relayEndReply(200, `{"tx_response":{"txhash":"5C504ED4","code":0}}`, 0)})
	const guid = 7100
	_, err = sendRelayEnd(t, server, guid, relayEndShortWait, "/cosmos/tx/v1beta1/txs", `{"tx_bytes":"CpIBCo8B","mode":"BROADCAST_MODE_SYNC"}`)
	require.NoError(t, err)

	line := onlyRelayEnd(t, read(), guid)
	require.Contains(t, line, `"method":"cosmos/tx/v1beta1/txs"`)
	require.Equal(t, line, collectorBarePathRedaction.ReplaceAllString(line, "${1}REDACTED"), "the collector's path redaction leaves the line as it is")
	requireRelayEndGolden(t, "rest_write", normalizeRelayEnd(t, line, false))

	// The control: the same value with its slash is exactly what the collector rewrites.
	require.NotEqual(t, `"method":"/cosmos/tx/v1beta1/txs"`, collectorBarePathRedaction.ReplaceAllString(`"method":"/cosmos/tx/v1beta1/txs"`, "${1}REDACTED"))
}

// collectorBarePathRedaction is the fleet collector's bare-path stage, with the delimiter captured
// so a replacement keeps it, as the collector's non-capturing group does.
var collectorBarePathRedaction = regexp.MustCompile(`(^|[\s"=(])/[^\s"'` + "`" + `<>\\^{}|\[\],;)]+`)

func TestRelayEnd_CacheHit(t *testing.T) {
	rand.InitRandomSeed()
	primary, rcs := startCacheServerForTest(t)
	eth := ethJsonRPCParser(t)
	const body = `{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x64",false],"id":7}`
	hashKey, _, err := ethProtocolMessage(t, eth, body, 0).HashCacheRequest("ETH1")
	require.NoError(t, err)

	for i, entry := range []struct{ name, data string }{
		{"cache_hit", `{"jsonrpc":"2.0","id":1,"result":{"number":"0x64"}}`},
		{"cache_hit_node_error", `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"header not found"}}`},
	} {
		t.Run(entry.name, func(t *testing.T) {
			_, err := rcs.SetRelay(context.Background(), &pairingtypes.RelayCacheSet{
				RequestHash: hashKey, ChainId: "ETH1", RequestedBlock: 100, SeenBlock: 100,
				Response:         &pairingtypes.RelayReply{Data: []byte(entry.data), LatestBlock: 100},
				AverageBlockTime: int64(15 * time.Second), StatusCode: http.StatusOK,
			})
			require.NoError(t, err)
			require.Eventually(t, func() bool { return directGetETH(rcs, hashKey, 100, 100).GetReply() != nil }, 3*time.Second, 25*time.Millisecond)

			read := captureRelayEnds(t)
			// The only upstream refuses connections, so an answer can only have come from the cache.
			server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, eth, relayEndNode{"node-a", nil})
			server.cache = primary
			guid := uint64(7200 + i)
			result, err := sendRelayEnd(t, server, guid, relayEndShortWait, "", body)
			require.NoError(t, err)
			require.Equal(t, common.CacheTierPrimary, result.CacheLookup.ServedTier)
			requireRelayEndGolden(t, entry.name, normalizeRelayEnd(t, onlyRelayEnd(t, read(), guid), false))
		})
	}
}

// relayEndUnit builds an event from hand-made inputs, for the rules no live relay reaches cheaply.
func relayEndUnit(t *testing.T, in relayEndInput) *relayEnd {
	t.Helper()
	server := &RPCSmartRouterServer{listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"}}
	return server.buildRelayEnd(utils.WithUniqueIdentifier(context.Background(), 1), in)
}

func TestRelayEnd_RouterError(t *testing.T) {
	t.Run("a fault of the router's machinery", func(t *testing.T) {
		fault := routerFault{errors.New("ProcessRelaySend channel closed unexpectedly")}
		end := relayEndUnit(t, relayEndInput{err: fmt.Errorf("failed processing responses from RPC endpoints: %w", fault), cause: fault, start: time.Now()})
		require.Equal(t, relayOutcomeRouterError, end.outcome)
		require.Equal(t, common.LavaErrorRelayProcessingFailed.Name, end.errorCode)
		require.Equal(t, "ProcessRelaySend channel closed unexpectedly", end.errorText)
	})
	t.Run("neither an answer nor an error", func(t *testing.T) {
		end := relayEndUnit(t, relayEndInput{start: time.Now()})
		require.Equal(t, relayOutcomeRouterError, end.outcome)
	})
	t.Run("the mark survives the error wrapping the state machine does", func(t *testing.T) {
		require.True(t, isRouterFault(fmt.Errorf("batch: %w", routerFault{errors.New("RelayPrivateData is nil")})))
		require.False(t, isRouterFault(lavasession.PairingListEmptyError))
	})
}

func TestRelayEnd_ErrorCodeRules(t *testing.T) {
	cases := []struct {
		name     string
		cause    error
		result   *common.RelayResult
		attempts relayEndAttempts
		want     string
	}{
		{"empty pool", utils.LavaFormatError("ETH1 could not get a provider address", lavasession.PairingListEmptyError), nil, nil, common.LavaErrorNoProviders.Name},
		{"every endpoint disabled", lavasession.AllProviderEndpointsDisabledError, nil, nil, common.LavaErrorAllEndpointsDisabled.Name},
		{"pinned provider unavailable", utils.LavaFormatError("Selected provider not available", lavasession.SelectedProviderUnavailableError), nil, nil, common.LavaErrorProviderUnavailable.Name},
		{"pinned provider already failed", lavasession.SelectedProviderAlreadyFailedError, nil, nil, common.LavaErrorProviderUnavailable.Name},
		{"batch too large", chainlib.ErrBatchRequestSizeExceeded, nil, nil, common.LavaErrorBatchSizeExceeded.Name},
		// validateCrossValidationCapacity wraps the empty-pool sentinel, so the reason has to win.
		{"cross-validation cannot be staffed", utils.LavaFormatError("requested cross-validation maxParticipants exceeds available candidate endpoints", lavasession.PairingListEmptyError), &common.RelayResult{CrossValidationFailureReason: common.CrossValidationReasonInsufficientCapacity}, nil, common.LavaErrorInsufficientProviders.Name},
		{"cross-validation disagreed", errors.New("cross-validation failed"), &common.RelayResult{CrossValidationFailureReason: common.CrossValidationReasonNoAgreement}, nil, common.LavaErrorHashConsensusError.Name},
		{"the budget ran out", context.DeadlineExceeded, nil, nil, common.LavaErrorContextDeadline.Name},
		{"the last attempt's code", errors.New("failed relay, insufficient results"), nil, relayEndAttempts{{errorCode: "PROTOCOL_CONNECTION_REFUSED"}, {}, {errorCode: "PROTOCOL_CONNECTION_RESET"}, {}}, "PROTOCOL_CONNECTION_RESET"},
		{"nothing to name", errors.New("invalid character"), nil, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, relayEndErrorCode(tc.cause, tc.result, tc.attempts))
		})
	}
}

func TestRelayEnd_AttemptsFollowTheDispatchHistory(t *testing.T) {
	reply := &pairingtypes.RelayReply{Data: []byte(`{}`)}
	successes := []common.RelayResult{
		{ProviderInfo: common.ProviderInfo{ProviderAddress: "node-b"}, Reply: reply, AttemptLatency: 40 * time.Millisecond},
	}
	protocolErrors := []relaycore.RelayError{
		{
			Err: errors.New("dial tcp: connection refused"), ProviderInfo: common.ProviderInfo{ProviderAddress: "node-a"}, LavaError: common.LavaErrorConnectionRefused,
			Response: &relaycore.RelayResponse{RelayResult: common.RelayResult{AttemptLatency: 2 * time.Millisecond}},
		},
	}

	// node-a failed, node-b answered, and node-a's second attempt has not come back.
	attempts := relayEndAttemptsOf([]string{"node-a", "node-b", "node-a"}, successes, nil, protocolErrors, false)
	require.Len(t, attempts, 3)
	require.Equal(t, []string{"node-a", "node-b", "node-a"}, []string{attempts[0].upstream, attempts[1].upstream, attempts[2].upstream})
	require.Equal(t, attemptOutcomeTransportError, attempts[0].outcome)
	require.Equal(t, "PROTOCOL_CONNECTION_REFUSED", attempts[0].errorCode)
	require.Equal(t, attemptOutcomeOK, attempts[1].outcome)
	require.Equal(t, 40*time.Millisecond, attempts[1].latency)
	require.Equal(t, attemptOutcomeCancelled, attempts[2].outcome, "the router stopped waiting for it")
	require.Zero(t, attempts[2].latency, "an attempt with no result has no latency to report")

	ranOut := relayEndAttemptsOf([]string{"node-a"}, nil, nil, nil, true)
	require.Equal(t, attemptOutcomeTimeout, ranOut[0].outcome, "silent when the budget ran out")
}

// A node's reply settles its upstream, so only a transport error names why a write is unknown, as in
// writeOutcomeIsUnknown, even when the reply's classification carries MayHaveReachedNode.
func TestRelayEnd_WriteUnknownCodeComesFromATransportError(t *testing.T) {
	nodeReply := relayEndNodeErrorAttempt(relaycore.RelayError{
		Err: errors.New("something odd"), LavaError: common.LavaErrorUnknown,
		Response: &relaycore.RelayResponse{RelayResult: common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: "node-a"}}},
	})
	require.True(t, common.LavaErrorUnknown.MayHaveReachedNode, "the case under test")
	reset := relayEndProtocolErrorAttempt(relaycore.RelayError{
		Err: errors.New("connection reset by peer"), LavaError: common.LavaErrorConnectionReset,
		ProviderInfo: common.ProviderInfo{ProviderAddress: "node-b"},
	}, false)

	require.Equal(t, common.LavaErrorConnectionReset.Name, relayEndWriteUnknownCode(nil, relayEndAttempts{nodeReply, reset}, false))
	require.Equal(t, common.LavaErrorContextDeadline.Name, relayEndWriteUnknownCode(nil, relayEndAttempts{nodeReply, reset}, true))
}

func TestRelayEnd_AttemptOutcomes(t *testing.T) {
	rateLimited := relayEndNodeErrorAttempt(relaycore.RelayError{
		Err: errors.New("too many requests"), LavaError: common.LavaErrorNodeRateLimited,
		Response: &relaycore.RelayResponse{RelayResult: common.RelayResult{ProviderInfo: common.ProviderInfo{ProviderAddress: "node-a"}, IsRateLimited: true}},
	})
	require.Equal(t, attemptOutcomeRateLimited, rateLimited.outcome)
	require.Equal(t, "node-a", rateLimited.upstream)

	for _, tc := range []struct {
		name         string
		err          error
		lavaError    *common.LavaError
		ranOutOfRoad bool
		want         string
	}{
		{"a hedge loser", context.Canceled, common.LavaErrorContextCanceled, false, attemptOutcomeCancelled},
		{"cancelled when the budget ran out", context.Canceled, common.LavaErrorContextCanceled, true, attemptOutcomeTimeout},
		{"a deadline", context.DeadlineExceeded, common.LavaErrorContextDeadline, false, attemptOutcomeTimeout},
		{"a 429", common.StatusCodeError429, common.LavaErrorRateLimited, false, attemptOutcomeRateLimited},
		{"a reset", errors.New("connection reset by peer"), common.LavaErrorConnectionReset, false, attemptOutcomeTransportError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempt := relayEndProtocolErrorAttempt(relaycore.RelayError{Err: tc.err, LavaError: tc.lavaError, ProviderInfo: common.ProviderInfo{ProviderAddress: "node-a"}}, tc.ranOutOfRoad)
			require.Equal(t, tc.want, attempt.outcome)
			require.Equal(t, tc.lavaError.Name, attempt.errorCode)
		})
	}
}

func TestRelayEnd_MethodNeverStartsWithASlash(t *testing.T) {
	const rest, jsonrpc, grpc, tendermint = spectypes.APIInterfaceRest, spectypes.APIInterfaceJsonRPC, spectypes.APIInterfaceGrpc, spectypes.APIInterfaceTendermintRPC
	require.Equal(t, "cosmos/tx/v1beta1/txs", relayEndMethod("/cosmos/tx/v1beta1/txs", rest))
	require.Equal(t, "cosmos/bank/v1beta1/balances/{address}", relayEndMethod("/cosmos/bank/v1beta1/balances/{address}", rest))
	require.Equal(t, "sui.rpc.v2.LedgerService/GetServiceInfo", relayEndMethod("sui.rpc.v2.LedgerService/GetServiceInfo", grpc))
	require.Equal(t, "eth_call", relayEndMethod("eth_call", jsonrpc))
	require.Equal(t, "eth_getBalance&eth_chainId", relayEndMethod("eth_getBalance&eth_chainId", jsonrpc))
	require.Equal(t, "", relayEndMethod("", jsonrpc))

	// An unlisted name is what the client sent: kept where it comes from a body or names a gRPC
	// service, withheld where it is a URL's path or query, however many segments it has.
	require.Equal(t, "Default-eth_notAMethod", relayEndMethod("Default-eth_notAMethod", jsonrpc))
	require.Equal(t, "Default-/pkg.v1.Svc/Missing", relayEndMethod("Default-/pkg.v1.Svc/Missing", grpc))
	require.Equal(t, "Default-REDACTED", relayEndMethod("Default-/v1/AbC123SecretKey/status", rest))
	require.Equal(t, "Default-REDACTED", relayEndMethod("Default-AbC123SecretKey", tendermint), "a single-segment URI path has no slash")
	require.Equal(t, "status&Default-REDACTED", relayEndMethod("status&Default-AbC123SecretKey", tendermint), "every member of a batch")

	capped := relayEndMethod(strings.Repeat("eth_getBalance&", 200), jsonrpc)
	require.LessOrEqual(t, len(capped), relayEndMethodLimit+len("...truncated..."))
}

// An unlisted REST path is relayed, and the event names it without the path the client sent.
func TestRelayEnd_UnlistedRestPathIsWithheld(t *testing.T) {
	rand.InitRandomSeed()
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), "COSMOSHUB", spectypes.APIInterfaceRest, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)

	read := captureRelayEnds(t)
	server := newRelayEndServer(t, "COSMOSHUB", spectypes.APIInterfaceRest, chainParser, relayEndNode{"node-a", relayEndReply(200, `{}`, 0)})
	const guid = 7101
	_, err = sendRelayEnd(t, server, guid, relayEndShortWait, "/v1/AbC123SecretKey/status?apikey=AbC123SecretKey", "")
	require.NoError(t, err)

	line := onlyRelayEnd(t, read(), guid)
	require.Contains(t, line, `"method":"Default-REDACTED"`)
	require.NotContains(t, line, "AbC123SecretKey")
}

// The free-text fields go through the router's own redactor before they are written.
func TestRelayEnd_TextIsRedactedAndCapped(t *testing.T) {
	text := relayEndText(`Post "https://eth-mainnet.example.com/v2/AbC123SecretKey": dial tcp: connection refused`)
	require.NotContains(t, text, "AbC123SecretKey")
	require.Contains(t, text, "eth-mainnet.example.com")
	require.LessOrEqual(t, len(relayEndText(strings.Repeat("x", 4*relayEndTextLimit))), relayEndTextLimit+len("...truncated..."))
}
