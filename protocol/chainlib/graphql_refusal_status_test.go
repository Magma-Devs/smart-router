package chainlib

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// parsingRelay is a RelaySender that reads the request with the real GraphQL parser and hands the
// listener whatever that parse returns, which is what the router's relay path does. So a test can
// send one of the ticket's own reproduction bodies and read the answer the caller would get, rather
// than injecting an error and trusting that the parse produces it.
type parsingRelay struct {
	relayed int
}

func (r *parsingRelay) SendRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, analytics *metrics.RelayMetrics, metadataValues []pairingtypes.Metadata) (*common.RelayResult, error) {
	if _, err := rpcInterfaceMessages.ParseGraphQLMsg([]byte(req)); err != nil {
		// A refusal reaches the listener with no status, exactly as it does in production: the
		// router asked no provider, so the relay result carries nothing.
		return &common.RelayResult{}, err
	}
	r.relayed++
	return &common.RelayResult{
		Reply:      &pairingtypes.RelayReply{Data: []byte(`{"data":{"chainIdentifier":"35834a8a"}}`)},
		StatusCode: http.StatusOK,
	}, nil
}

func (r *parsingRelay) ParseRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, metadata []pairingtypes.Metadata) (ProtocolMessage, error) {
	return nil, errors.New("not used")
}

func (r *parsingRelay) SendParsedRelay(ctx context.Context, analytics *metrics.RelayMetrics, protocolMessage ProtocolMessage) (*common.RelayResult, error) {
	return nil, errors.New("not used")
}

// serveGraphQLListener brings up a GraphQL listener over relay and returns a function that POSTs a
// body to it and reads back the status and body a caller would see.
func serveGraphQLListener(t *testing.T, relay RelaySender) func(body string) (int, string) {
	t.Helper()
	post := serveGraphQLListenerReplies(t, relay)
	return func(body string) (int, string) {
		t.Helper()
		reply := post(body)
		return reply.status, reply.body
	}
}

// graphQLListenerReply is what a caller of the GraphQL listener reads: the status, the headers and
// the body.
type graphQLListenerReply struct {
	status int
	header http.Header
	body   string
}

// serveGraphQLListenerReplies is serveGraphQLListener with the reply's headers too.
func serveGraphQLListenerReplies(t *testing.T, relay RelaySender) func(body string) graphQLListenerReply {
	t.Helper()
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	listener := NewGraphQLChainListener(ctx, &routersession.RPCEndpoint{
		NetworkAddress:  "127.0.0.1:0",
		ChainID:         "SUI",
		ApiInterface:    spectypes.APIInterfaceGraphQL,
		HealthCheckPath: common.DEFAULT_HEALTH_PATH,
	}, relay, alwaysHealthyReporter{}, logger)
	go listener.Serve(ctx, common.ConsumerCmdFlags{})

	addr := ""
	for deadline := time.Now().Add(5 * time.Second); addr == "" && time.Now().Before(deadline); {
		if addr = listener.GetListeningAddress(); addr == "" {
			time.Sleep(20 * time.Millisecond)
		}
	}
	require.NotEmpty(t, addr, "listener never reported a listening address")

	client := &http.Client{Timeout: 10 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	return func(body string) graphQLListenerReply {
		t.Helper()
		response, err := client.Post("http://"+addr+"/", "application/json", strings.NewReader(body))
		require.NoError(t, err)
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		return graphQLListenerReply{status: response.StatusCode, header: response.Header, body: string(raw)}
	}
}

// graphQLErrorEntryForTest is one entry of a GraphQL response's errors list.
type graphQLErrorEntryForTest struct {
	Message    string            `json:"message"`
	Extensions map[string]string `json:"extensions"`
}

// requireGraphQLError reads a body as a GraphQL response and returns the message of its one error,
// failing unless the body is the shape a GraphQL client reads for a request error: an object with
// an errors list whose entries carry a message, and no data key (GraphQL specification, Response
// Format).
func requireGraphQLError(t *testing.T, body string) string {
	t.Helper()
	return requireGraphQLErrorEntry(t, body).Message
}

// requireGraphQLErrorEntry is requireGraphQLError returning the whole entry, its extensions included.
func requireGraphQLErrorEntry(t *testing.T, body string) graphQLErrorEntryForTest {
	t.Helper()
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(body), &fields), "the body is not a JSON object: %s", body)
	_, hasData := fields["data"]
	require.False(t, hasData,
		"a refusal is a request error, raised before anything ran, so it carries no data key: %s", body)
	var response struct {
		Errors []graphQLErrorEntryForTest `json:"errors"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &response), "the body is not JSON: %s", body)
	require.Len(t, response.Errors, 1, "a GraphQL client reads the errors list: %s", body)
	require.NotEmpty(t, response.Errors[0].Message, "an error entry carries a message: %s", body)
	require.NotEmpty(t, response.Errors[0].Extensions["guid"],
		"the router's own request id stays readable, in extensions: %s", body)
	return response.Errors[0]
}

// TestGraphQLListener_RefusalIsAClientErrorWithAGraphQLBody is MAG-4577's size-limit half.
//
// The router refuses three GraphQL requests by limits of its own: a document that nests deeper than
// MaxGraphQLQueryDepth, an operation that selects more than MaxGraphQLRootFields, and fragment
// spreads that nest more than MaxGraphQLFragmentNesting. Nothing fails — the router reads the
// request and declines to serve it, asking no provider, and its own relay_end line says so.
//
// It used to answer all of them HTTP 500, the status for a failure of the server, with a body of
// `{"error":"{\"Error_GUID\":…,\"Error\":…}"}`: a JSON document inside a JSON string, with no
// errors list. Every alert rule between the caller and the router counted the router as down for
// those requests, a retry policy that retries 5xx resent a request that can never succeed, and a
// standard GraphQL client found no errors list and reported a transport failure instead of the
// reason.
//
// The bodies here are the ticket's own reproduction steps 2 and 3, and the controls one step below
// each limit, which must still be relayed.
func TestGraphQLListener_RefusalIsAClientErrorWithAGraphQLBody(t *testing.T) {
	relay := &parsingRelay{}
	post := serveGraphQLListenerReplies(t, relay)

	nest := func(levels int) string {
		return "{ " + strings.Repeat("a { ", levels-1) + "b" + strings.Repeat(" }", levels-1) + " }"
	}
	rootFields := func(count int) string {
		fields := make([]string, count)
		for i := range fields {
			fields[i] = fmt.Sprintf("a%d: chainIdentifier", i)
		}
		return "{ " + strings.Join(fields, " ") + " }"
	}
	// A chain of fragment spreads: the root spreads f0, which spreads f1, and so on. The walk
	// starts at nesting 0 on the operation's own selection set and adds one per spread, so a
	// chain of `depth` fragments is walked to nesting `depth` and the limit refuses it from
	// depth MaxGraphQLFragmentNesting + 1.
	fragmentChain := func(depth int) string {
		query := "{ ...f0 }"
		for i := 0; i < depth; i++ {
			if i == depth-1 {
				query += fmt.Sprintf(" fragment f%d on Query { chainIdentifier }", i)
				continue
			}
			query += fmt.Sprintf(" fragment f%d on Query { ...f%d }", i, i+1)
		}
		return query
	}
	envelope := func(query string) string {
		body, err := json.Marshal(map[string]string{"query": query})
		require.NoError(t, err)
		return string(body)
	}

	for _, tc := range []struct {
		name, query, wantMessage string
	}{
		{
			name:        "a document nested one level past the depth limit",
			query:       nest(rpcInterfaceMessages.MaxGraphQLQueryDepth + 1),
			wantMessage: fmt.Sprintf("graphql query nests deeper than %d levels", rpcInterfaceMessages.MaxGraphQLQueryDepth),
		},
		{
			name:        "an operation selecting one root field past the limit",
			query:       rootFields(rpcInterfaceMessages.MaxGraphQLRootFields + 1),
			wantMessage: fmt.Sprintf("graphql operation selects more than %d root fields", rpcInterfaceMessages.MaxGraphQLRootFields),
		},
		{
			name:        "fragment spreads nested one past the limit",
			query:       fragmentChain(rpcInterfaceMessages.MaxGraphQLFragmentNesting + 1),
			wantMessage: fmt.Sprintf("graphql fragments nest more than %d deep", rpcInterfaceMessages.MaxGraphQLFragmentNesting),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			relayed := relay.relayed
			reply := post(envelope(tc.query))
			status, body := reply.status, reply.body

			require.GreaterOrEqual(t, status, 400, "a refusal is a client error, not a server failure: %d", status)
			require.Less(t, status, 500, "a refusal is a client error, not a server failure: %d", status)
			require.Equal(t, graphQLRefusalStatus, status)
			entry := requireGraphQLErrorEntry(t, body)
			require.Contains(t, entry.Message, tc.wantMessage)
			require.NotContains(t, entry.Extensions, "code",
				"a limit's reply is the one MAG-4577 decided, with no code: %s", body)
			require.Equal(t, entry.Extensions["guid"], reply.header.Get(common.GUID_HEADER_NAME),
				"the reply carries the request id in Lava-Guid too, as every relayed reply does")
			require.NotContains(t, body, "Error_GUID",
				"the reason must not be JSON inside a JSON string any more")
			require.Equal(t, relayed, relay.relayed, "a refused request reaches no provider")
		})
	}

	// The controls, one step below each limit, are still served.
	for _, tc := range []struct {
		name, query string
	}{
		{"a document at the depth limit", nest(rpcInterfaceMessages.MaxGraphQLQueryDepth)},
		{"an operation at the root-field limit", rootFields(rpcInterfaceMessages.MaxGraphQLRootFields)},
		{"fragment spreads at the nesting limit", fragmentChain(rpcInterfaceMessages.MaxGraphQLFragmentNesting)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			relayed := relay.relayed
			reply := post(envelope(tc.query))
			require.Equal(t, http.StatusOK, reply.status, "body: %s", reply.body)
			require.Equal(t, relayed+1, relay.relayed, "the control must reach a provider")
		})
	}
}

// TestGraphQLListener_UnreadableRequestIsRefused is MAG-4351, as its owner decided on 2026-11-09: a
// request the router cannot read is refused, not forwarded. It used to get HTTP 500, the status for
// a failure of the server, for the client's own typo, and a body with no errors list, so a GraphQL
// client could not read the reason.
//
// The rows are the ticket's ten requests (PR-542-EXAMPLES-2026-10-19.md), then the rest of the
// decision's list and the two more QA found (comment 30525). Each unreadable one gets HTTP 400, a
// GraphQL error document with no data key, the code the decision names in extensions with the
// request id beside it, the same id in Lava-Guid, and reaches no provider. The ticket's two controls
// are readable, so they are still relayed: an unknown field is the node's to answer.
func TestGraphQLListener_UnreadableRequestIsRefused(t *testing.T) {
	relay := &parsingRelay{}
	post := serveGraphQLListenerReplies(t, relay)

	const parseFailed = rpcInterfaceMessages.GraphQLParseFailedCode
	const refused = rpcInterfaceMessages.GraphQLRequestRefusedCode
	for _, tc := range []struct {
		name, body, wantCode, wantMessage string
	}{
		// The ticket's ten, less its two controls.
		{"a query with a syntax error", `{"query":"{ checkpoint { sequenceNumber }"}`, parseFailed, "failed parsing graphql query: Expected Name, found <EOF> (line 1, column 32)"},
		{"a query that is not GraphQL", `{"query":"not graphql at all"}`, parseFailed, `failed parsing graphql query: Unexpected Name "not" (line 1, column 1)`},
		{"an empty query", `{"query":""}`, refused, "graphql request carries no query"},
		{"a body that is not JSON", `{"query":`, refused, "graphql request body is not JSON"},
		{"a batch", `[{"query":"{ chainIdentifier }"},{"query":"{ checkpoint { digest } }"}]`, refused, "graphql request body is a JSON array"},
		{"two operations and no operation name", `{"query":"query A { chainIdentifier } query B { checkpoint { digest } }"}`, refused, "graphql document defines multiple operations and the request names none"},
		{"an undefined fragment", `{"query":"{ ...Missing }"}`, refused, `graphql query spreads undefined fragment "Missing"`},
		{"a subscription", `{"query":"subscription { checkpoints { cursor node { sequenceNumber } } }"}`, refused, "graphql subscriptions are not served over HTTP POST"},
		// The rest of the decision's list, and QA's two (comment 30525).
		{"no query member", `{"variables":{}}`, refused, "graphql request carries no query"},
		{"an operation name that matches none", `{"query":"query A { chainIdentifier }","operationName":"B"}`, refused, `graphql document defines no operation named "B"`},
		{"a cyclic fragment", `{"query":"{ ...A } fragment A on Query { ...B } fragment B on Query { ...A }"}`, refused, `graphql fragment "A" is cyclic`},
		{"variables that are not an object", `{"query":"{ chainIdentifier }","variables":7}`, refused, `graphql request member "variables" must be an object`},
		{"a GraphQL document sent as the body", `{ chainIdentifier }`, refused, "graphql request body is not JSON"},
		{"a body that is JSON but not an object", `null`, refused, "graphql request body is not a JSON object"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			relayed := relay.relayed
			reply := post(tc.body)

			require.Equal(t, graphQLRefusalStatus, reply.status, "a client error, not a server failure: %s", reply.body)
			require.Equal(t, fiber.MIMEApplicationJSONCharsetUTF8, reply.header.Get(fiber.HeaderContentType))
			entry := requireGraphQLErrorEntry(t, reply.body)
			require.Equal(t, tc.wantCode, entry.Extensions["code"], "the decision's code: %s", reply.body)
			require.Contains(t, entry.Message, tc.wantMessage)
			require.Equal(t, entry.Extensions["guid"], reply.header.Get(common.GUID_HEADER_NAME),
				"Lava-Guid carries the id the body's guid carries")
			require.NotContains(t, reply.body, "Error_GUID", "the old 500 body")
			require.NotContains(t, reply.body, "graphQLRequestEnvelope", "a decoder's text names the router's Go types")
			require.Equal(t, relayed, relay.relayed, "an unreadable request reaches no provider")
		})
	}

	// The ticket's two controls parse, so they are relayed: a valid read, and an unknown field,
	// which the node answers with its own GRAPHQL_VALIDATION_FAILED.
	for _, tc := range []struct{ name, body string }{
		{"a valid read", `{"query":"{ chainIdentifier }"}`},
		{"an unknown field", `{"query":"{ noSuchField }"}`},
	} {
		t.Run("control: "+tc.name, func(t *testing.T) {
			relayed := relay.relayed
			reply := post(tc.body)
			require.Equal(t, http.StatusOK, reply.status, "body: %s", reply.body)
			require.Equal(t, relayed+1, relay.relayed, "a readable request reaches a provider")
		})
	}
}

// TestConvertToGraphQLError pins the body shape on its own, so a listener change cannot quietly
// alter what a client reads.
func TestConvertToGraphQLError(t *testing.T) {
	body := convertToGraphQLError("graphql operation selects more than 300 root fields", "", "7699427112166331654")
	require.JSONEq(t, `{"errors":[{"message":"graphql operation selects more than 300 root fields","extensions":{"guid":"7699427112166331654"}}]}`, string(body))

	withoutGUID := convertToGraphQLError("refused", "", "")
	require.JSONEq(t, `{"errors":[{"message":"refused"}]}`, string(withoutGUID))

	// MAG-4351's shape: the code beside the guid, still no data key.
	unreadable := convertToGraphQLError("failed parsing graphql query: Expected Name, found <EOF> (line 1, column 32)",
		rpcInterfaceMessages.GraphQLParseFailedCode, "7699427112166331654")
	require.JSONEq(t, `{"errors":[{"message":"failed parsing graphql query: Expected Name, found <EOF> (line 1, column 32)","extensions":{"code":"GRAPHQL_PARSE_FAILED","guid":"7699427112166331654"}}]}`, string(unreadable))
}

// A refused request is past one of the router's size limits, so it is the request most likely to be
// large, and its failure line is written at error level. Like every other listener failure line it
// carries the body cut to its head and tail, whole only under --debug-relays (MAG-4454).
func TestGraphQLListener_RefusalLogsTheBodyCutUnlessDebugRelays(t *testing.T) {
	utils.EnableDebugLogBuffer(1000)
	t.Cleanup(utils.DisableDebugLogBuffer)

	fields := make([]string, rpcInterfaceMessages.MaxGraphQLRootFields+1)
	for i := range fields {
		fields[i] = fmt.Sprintf("a%d: chainIdentifier", i)
	}
	body, err := json.Marshal(map[string]string{"query": "{ " + strings.Join(fields, " ") + " }"})
	require.NoError(t, err)
	newGraphQL := func(ctx context.Context, endpoint *routersession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
		return NewGraphQLChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger)
	}

	for _, debugRelays := range []bool{false, true} {
		t.Run(fmt.Sprintf("debugRelays=%v", debugRelays), func(t *testing.T) {
			utils.ClearDebugLogBuffer()
			addr := serveWithFlags(t, newGraphQL, spectypes.APIInterfaceGraphQL, &parsingRelay{}, common.ConsumerCmdFlags{DebugRelays: debugRelays})
			requestID := fmt.Sprintf("req-refusal-%v", debugRelays)
			req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/", strings.NewReader(string(body)))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Request-Id", requestID)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			_, _ = io.Copy(io.Discard, resp.Body)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, graphQLRefusalStatus, resp.StatusCode)

			var guid string
			for _, line := range utils.ReadDebugLogBuffer(requestID, time.Time{}, time.Time{}, 0) {
				record := map[string]any{}
				require.NoError(t, json.Unmarshal(line, &record))
				if record["message"] == "Consumer received a new GraphQL request" {
					guid, _ = record["GUID"].(string)
				}
			}
			lines := requireLinesOfGUID(t, guid)
			require.Contains(t, lines, "graphql in/out", "the refusal's failure line carries the request's GUID")
			require.Equal(t, "error", lines["graphql in/out"]["level"])
			require.Equal(t, relayBodyForLog(string(body), debugRelays), lines["graphql in/out"]["request"], "the failure line's body")
		})
	}
}
