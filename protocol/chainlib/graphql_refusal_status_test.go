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

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
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
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logger, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	listener := NewGraphQLChainListener(ctx, &lavasession.RPCEndpoint{
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
	return func(body string) (int, string) {
		t.Helper()
		response, err := client.Post("http://"+addr+"/", "application/json", strings.NewReader(body))
		require.NoError(t, err)
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		return response.StatusCode, string(raw)
	}
}

// requireGraphQLError reads a body as a GraphQL response and returns the message of its one error,
// failing unless the body is the shape a GraphQL client reads: an object with a null data and an
// errors list whose entries carry a message (GraphQL specification, Response Format).
func requireGraphQLError(t *testing.T, body string) string {
	t.Helper()
	var response struct {
		Data   *json.RawMessage `json:"data"`
		Errors []struct {
			Message    string            `json:"message"`
			Extensions map[string]string `json:"extensions"`
		} `json:"errors"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &response), "the body is not JSON: %s", body)
	require.Nil(t, response.Data, "a refused request resolved no data: %s", body)
	require.Len(t, response.Errors, 1, "a GraphQL client reads the errors list: %s", body)
	require.NotEmpty(t, response.Errors[0].Message, "an error entry carries a message: %s", body)
	require.NotEmpty(t, response.Errors[0].Extensions["guid"],
		"the router's own request id stays readable, in extensions: %s", body)
	return response.Errors[0].Message
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
	post := serveGraphQLListener(t, relay)

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
			status, body := post(envelope(tc.query))

			require.GreaterOrEqual(t, status, 400, "a refusal is a client error, not a server failure: %d", status)
			require.Less(t, status, 500, "a refusal is a client error, not a server failure: %d", status)
			require.Equal(t, graphQLRefusalStatus, status)
			require.Contains(t, requireGraphQLError(t, body), tc.wantMessage)
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
			status, body := post(envelope(tc.query))
			require.Equal(t, http.StatusOK, status, "body: %s", body)
			require.Equal(t, relayed+1, relay.relayed, "the control must reach a provider")
		})
	}
}

// TestGraphQLListener_UnreadableRequestKeepsItsCurrentAnswer draws the line this change does not
// cross. A document the router cannot READ — a syntax error, a missing query, a subscription — is
// MAG-4351's and MAG-4577's subscription half, and MAG-4351 decided those are forwarded to a node
// and answered with the node's own reply. Until that lands they keep today's answer, and this test
// says so rather than leaving a reader to wonder whether the refusal branch was meant to catch them.
func TestGraphQLListener_UnreadableRequestKeepsItsCurrentAnswer(t *testing.T) {
	post := serveGraphQLListener(t, &parsingRelay{})

	for _, tc := range []struct{ name, body string }{
		{"a query with a syntax error", `{"query":"{ checkpoint { sequenceNumber }"}`},
		{"no query at all", `{"query":""}`},
		{"a subscription", `{"query":"subscription { checkpoints { cursor node { sequenceNumber } } }"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := post(tc.body)
			require.Equal(t, http.StatusInternalServerError, status,
				"unchanged until MAG-4351 decides the forward: %s", body)
			require.Contains(t, body, "Error_GUID")
		})
	}
}

// TestConvertToGraphQLError pins the body shape on its own, so a listener change cannot quietly
// alter what a client reads.
func TestConvertToGraphQLError(t *testing.T) {
	body := convertToGraphQLError("graphql operation selects more than 300 root fields", "7699427112166331654")
	require.JSONEq(t, `{"data":null,"errors":[{"message":"graphql operation selects more than 300 root fields","extensions":{"guid":"7699427112166331654"}}]}`, string(body))

	withoutGUID := convertToGraphQLError("refused", "")
	require.JSONEq(t, `{"data":null,"errors":[{"message":"refused"}]}`, string(withoutGUID))
}
