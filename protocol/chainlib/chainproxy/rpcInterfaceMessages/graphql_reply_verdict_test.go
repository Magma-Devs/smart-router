package rpcInterfaceMessages

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// graphQLEffect is what the router does with a reply, read off the error registry the way the
// relay does: retried on another provider, the endpoint blamed, the provider paused.
type graphQLEffect struct {
	nodeError bool
	retried   bool
	blamed    bool
	paused    bool
}

var (
	effectAnswer    = graphQLEffect{}
	effectCaller    = graphQLEffect{nodeError: true}
	effectElsewhere = graphQLEffect{nodeError: true, retried: true}
	effectFault     = graphQLEffect{nodeError: true, retried: true, blamed: true}
	effectThrottled = graphQLEffect{nodeError: true, retried: true, paused: true}
)

func readEffect(t *testing.T, message *GraphQLMessage, body string, status int) graphQLEffect {
	t.Helper()
	hasError, errorMessage := message.CheckResponseError([]byte(body), status)
	if !hasError {
		return effectAnswer
	}
	// As the sender files it: a read by GraphQL's own rows, a write by the shared rows.
	classified := message.OwnClassification([]byte(body), status)
	if message.IsMutation() {
		require.Nil(t, classified, "a write is filed by the shared rows until MAG-5122")
		classified = common.ClassifyError(nil, common.ChainFamilyUnknown, common.TransportREST, message.ClassificationStatus([]byte(body), status), errorMessage)
	}
	require.NotNil(t, classified, "a read's error is always filed by its own rows")
	return graphQLEffect{
		nodeError: true,
		retried:   classified.Retryable,
		blamed:    classified.EndpointAtFault(),
		paused:    classified.SubCategory.IsRateLimit(),
	}
}

func parsedRead(t *testing.T) *GraphQLMessage {
	t.Helper()
	read, err := ParseGraphQLMsg([]byte(`{"query":"{ checkpoint(sequenceNumber: 1) { digest } }"}`))
	require.NoError(t, err)
	return read
}

// TestGraphQLReadVerdicts pins the read rows of MAG-5121: the order of checks, the default rows and
// the reading rules. Each case is the reply one provider sends; the effect is what the router does
// with that provider's reply.
func TestGraphQLReadVerdicts(t *testing.T) {
	read := parsedRead(t)

	tests := []struct {
		name   string
		status int
		body   string
		want   graphQLEffect
	}{
		// P6: an answer.
		{"data and no errors is the answer", 200, `{"data":{"checkpoint":{"digest":"x"}}}`, effectAnswer},
		{"a null field is an answer", 200, `{"data":{"checkpoint":null}}`, effectAnswer},

		// Default rows.
		{"GRAPHQL_VALIDATION_FAILED is the caller's", 200, `{"data":null,"errors":[{"message":"Unknown field","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectCaller},
		{"BAD_USER_INPUT is the caller's", 200, `{"data":null,"errors":[{"message":"bad","extensions":{"code":"BAD_USER_INPUT"}}]}`, effectCaller},
		// Measured 16 Nov: a query nested 100 deep passes the router (limit 256) and every Sui, IOTA
		// and Fuel node refuses it at the parse step (limit 64). Sui sends GRAPHQL_PARSE_FAILED;
		// IOTA and Fuel send the same message with no code (next case).
		{"GRAPHQL_PARSE_FAILED is the caller's", 200, `{"data":null,"errors":[{"message":"recursion limit exceeded.","extensions":{"code":"GRAPHQL_PARSE_FAILED"}}]}`, effectCaller},
		// Fuel and IOTA (live, 16 Nov) send no extensions.code on any error, so their caller's
		// errors read as unknown until they have chain rows (MAG-4910): retried on every provider,
		// then served as is.
		{"Fuel's codeless parse refusal is unknown", 200, `{"data":null,"errors":[{"message":"recursion limit exceeded."}]}`, effectElsewhere},
		{"Fuel's codeless unknown field is unknown", 200, `{"data":null,"errors":[{"message":"Unknown field \"notAField\" on type \"Query\".","locations":[{"line":1,"column":3}]}]}`, effectElsewhere},
		{"INTERNAL_SERVER_ERROR is a provider fault", 200, `{"data":null,"errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, effectFault},
		{"UNAVAILABLE is a provider fault", 200, `{"data":null,"errors":[{"message":"down","extensions":{"code":"UNAVAILABLE"}}]}`, effectFault},
		{"REQUEST_TIMEOUT is a provider fault", 200, `{"data":null,"errors":[{"message":"Request timed out","extensions":{"code":"REQUEST_TIMEOUT"}}]}`, effectFault},
		{"DEADLINE_EXCEEDED is a provider fault", 200, `{"data":null,"errors":[{"message":"late","extensions":{"code":"DEADLINE_EXCEEDED"}}]}`, effectFault},
		{"FEATURE_UNAVAILABLE is tried on another provider", 200, `{"data":null,"errors":[{"message":"not here","extensions":{"code":"FEATURE_UNAVAILABLE"}}]}`, effectElsewhere},
		// Test 9: a safeguard, not seen yet.
		{"RESOURCE_EXHAUSTED without a path pauses the provider", 200, `{"data":null,"errors":[{"message":"quota","extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, effectThrottled},
		{"RESOURCE_EXHAUSTED with a path is unknown", 200, `{"data":null,"errors":[{"message":"Display output too large","path":["object","display"],"extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, effectElsewhere},
		{"RESOURCE_EXHAUSTED with an empty path pauses the provider", 200, `{"data":null,"errors":[{"message":"quota","path":[],"extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, effectThrottled},

		// When unsure, retry (Change 7): unknown is retried, never blamed, never paused.
		{"test 5: a codeless error with no path and no data is unknown", 200, `{"data":null,"errors":[{"message":"bad"}]}`, effectElsewhere},
		{"test 4: a codeless error on a field beside data is unknown", 200, `{"data":{"a":1,"b":null},"errors":[{"message":"b failed","path":["b"]}]}`, effectElsewhere},
		{"a code no row matches is unknown", 200, `{"data":null,"errors":[{"message":"db down","extensions":{"code":"postgres-error"}}]}`, effectElsewhere},
		{"a numeric code counts as no code", 200, `{"data":null,"errors":[{"message":"bad","extensions":{"code":400}}]}`, effectElsewhere},
		{"extensions that is not an object counts as no code", 200, `{"data":null,"errors":[{"message":"bad","extensions":"x"}]}`, effectElsewhere},
		// No quota text row: a message can echo what the caller sent, so its words never pause a
		// provider. Sui's wrong-type error repeats the caller's literal.
		{"a caller's literal 'too many requests' echoed in an error is unknown, not throttled", 200, `{"data":null,"errors":[{"message":"Expected input type \"UInt53\", found \"too many requests\"."}]}`, effectElsewhere},
		{"RESOURCE_EXHAUSTED with a path and a quota-like message stays unknown", 200, `{"data":null,"errors":[{"message":"Too many requests","path":["object"],"extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, effectElsewhere},
		{"an errors entry that is not an object is still read", 200, `{"errors":["bad"]}`, effectElsewhere},
		{"a message that is not text is still read", 200, `{"errors":[{"message":{"text":"bad"},"extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, effectFault},

		// Several errors: the reply is retried if any error retries.
		{"the caller's error beside a provider fault is retried and blamed", 200,
			`{"data":{"a":null,"b":null},"errors":[{"message":"bad","path":["a"],"extensions":{"code":"BAD_USER_INPUT"}},{"message":"boom","path":["b"],"extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, effectFault},
		{"the caller's error beside an unknown one is retried", 200,
			`{"data":null,"errors":[{"message":"bad","extensions":{"code":"BAD_USER_INPUT"}},{"message":"what"}]}`, effectElsewhere},
		{"a field limit beside a provider fault is blamed, not paused", 200,
			`{"data":{"a":null,"b":null},"errors":[{"message":"Exceeded the maximum number","path":["a"],"extensions":{"code":"RESOURCE_EXHAUSTED"}},{"message":"boom","path":["b"],"extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, effectFault},
		{"a quota beside a provider fault pauses the provider", 200,
			`{"data":null,"errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}},{"message":"quota","extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, effectThrottled},

		// P4: a 2xx that is not a GraphQL reply.
		{"test 1: an empty object is a provider fault", 200, `{}`, effectFault},
		{"null data and no errors is a provider fault", 200, `{"data":null}`, effectFault},
		{"an empty errors list and no data is a provider fault", 200, `{"errors":[]}`, effectFault},

		// P2: a 429 is throttled, whatever the body says (Change 1, test 8).
		{"test 8: a 429 whose body is a field limit pauses the provider", 429,
			`{"errors":[{"message":"Too many requests","path":["object"],"extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, effectThrottled},
		{"a 429 whose body is the caller's error pauses the provider", 429,
			`{"errors":[{"message":"bad","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectThrottled},

		// P3: 401, 402, 403 refuse us, whatever the body says.
		{"a 403 whose body is a provider fault is a refusal", 403,
			`{"errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, effectElsewhere},
		{"a 401 is a refusal", 401, `{"message":"bad key"}`, effectElsewhere},

		// P5 under an error status: the rows first, then the status.
		{"a 400 with GRAPHQL_VALIDATION_FAILED is the caller's", 400,
			`{"errors":[{"message":"Unknown field","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectCaller},
		{"a 400 with INTERNAL_SERVER_ERROR is a provider fault", 400,
			`{"errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, effectFault},
		{"a 500 with GRAPHQL_VALIDATION_FAILED is the caller's", 500,
			`{"errors":[{"message":"Unknown field","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectCaller},
		// GraphQL's own status table files an unnamed 4xx as unrecognised, retried elsewhere, which
		// is stronger than the caller's error beside it.
		{"a 400 with the caller's error beside a codeless one is retried elsewhere", 400,
			`{"errors":[{"message":"bad","extensions":{"code":"BAD_USER_INPUT"}},{"message":"what"}]}`, effectElsewhere},
		{"a 503 with a codeless error falls to the status", 503,
			`{"errors":[{"message":"down"}]}`, effectFault},

		// P7: a bare status.
		{"a bare 502 is a provider fault", 502, `bad gateway`, effectFault},
		{"a bare 501 is tried on another provider", 501, ``, effectElsewhere},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, readEffect(t, read, test.body, test.status))
		})
	}
}

// TestGraphQLReadVerdictMessage pins the message each verdict carries: the deciding error's own
// text, which the log line shows and the registry's message rows read.
func TestGraphQLReadVerdictMessage(t *testing.T) {
	read := parsedRead(t)

	_, message := read.CheckResponseError([]byte(`{"errors":[{"message":"bad","extensions":{"code":"BAD_USER_INPUT"}},{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`), 200)
	require.Equal(t, "boom", message, "the error that decides the reply names it")

	_, message = read.CheckResponseError([]byte(`{"errors":[{"message":"first"},{"message":"second"}]}`), 200)
	require.Equal(t, "first", message, "between equal kinds, the first error decides")

	_, message = read.CheckResponseError([]byte(`{}`), 200)
	require.Equal(t, graphQLNotAResponsePrefix+"{}", message)

	_, message = read.CheckResponseError([]byte(`<html><body>502 Bad Gateway</body></html>`), 200)
	require.Equal(t, graphQLNotJSONPrefix+"<html><body>502 Bad Gateway</body></html>", message,
		"a page is said to be not JSON, not to lack a data object")

	_, message = read.CheckResponseError([]byte(`{"errors":["bad"]}`), 200)
	require.Equal(t, "bad", message, "an entry that is a string keeps its text, not its JSON quotes")

	// Under an error status the status table decides each unmatched error, once per reply; each
	// error still carries its own message.
	_, message = read.CheckResponseError([]byte(`{"errors":[{"message":"first"},{"message":"second"}]}`), 503)
	require.Equal(t, "first", message)
}

// TestGraphQLWriteVerdictKinds pins that a write's verdict names its kind, so nothing that reads
// the kind takes a write's error for an answer.
func TestGraphQLWriteVerdictKinds(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		want   graphQLVerdictKind
	}{
		{"an answer", 200, `{"data":{"executeTransaction":{"digest":"d"}}}`, graphQLVerdictAnswer},
		{"a codeless error is the caller's", 200, `{"errors":[{"message":"Invalid user signature"}]}`, graphQLVerdictCaller},
		{"RESOURCE_EXHAUSTED is throttled", 200, `{"errors":[{"message":"m","extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, graphQLVerdictThrottled},
		{"INTERNAL_SERVER_ERROR is a fault", 200, `{"errors":[{"message":"m","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, graphQLVerdictFault},
		{"a body that is not a response is a fault", 200, `{}`, graphQLVerdictFault},
		{"a 503 is a fault", 503, `down`, graphQLVerdictFault},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			verdict, _ := writeVerdict([]byte(test.body), test.status)
			require.Equal(t, test.want, verdict.kind)
		})
	}
}

// BenchmarkGraphQLReadVerdictManyErrorsUnder400 measures a 4xx reply with many codeless errors,
// whose status-table verdict is worked out once per reply, not once per error.
func BenchmarkGraphQLReadVerdictManyErrorsUnder400(b *testing.B) {
	body := []byte(`{"data":null,"errors":[` + strings.TrimSuffix(strings.Repeat(`{"message":"codeless error with some text in it"},`, 8000), ",") + `]}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		readVerdict(body, 400)
	}
}

// TestGraphQLWriteVerdictsUnchanged pins that MAG-5121 leaves a mutation's verdicts as they were
// (MAG-5122 owns them), except that a reply is never made unreadable by its shape.
func TestGraphQLWriteVerdictsUnchanged(t *testing.T) {
	write, err := ParseGraphQLMsg([]byte(`{"query":"mutation { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { digest } }"}`))
	require.NoError(t, err)

	tests := []struct {
		name       string
		status     int
		body       string
		wantError  bool
		wantStatus int
	}{
		{"an answer", 200, `{"data":{"executeTransaction":{"digest":"d"}}}`, false, 200},
		{"a codeless error is the caller's", 200, `{"errors":[{"message":"Invalid user signature"}]}`, true, 400},
		{"a numeric code is the caller's, not a bad gateway", 200, `{"errors":[{"message":"bad","extensions":{"code":400}}]}`, true, 400},
		{"RESOURCE_EXHAUSTED with a path is still a rate limit", 200, `{"errors":[{"message":"m","path":["executeTransaction"],"extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, true, 429},
		{"FEATURE_UNAVAILABLE keeps its 403", 200, `{"errors":[{"message":"m","extensions":{"code":"FEATURE_UNAVAILABLE"}}]}`, true, 403},
		{"GRAPHQL_PARSE_FAILED is the caller's", 200, `{"errors":[{"message":"m","extensions":{"code":"GRAPHQL_PARSE_FAILED"}}]}`, true, 400},
		{"a body that is not a response is a bad gateway", 200, `{}`, true, 502},
		{"an error status is its own", 503, `{"errors":[{"message":"m","extensions":{"code":"BAD_USER_INPUT"}}]}`, true, 503},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hasError, _ := write.CheckResponseError([]byte(test.body), test.status)
			require.Equal(t, test.wantError, hasError)
			require.Equal(t, test.wantStatus, write.ClassificationStatus([]byte(test.body), test.status))
		})
	}
}

// TestGraphQLUnrecognizedErrorIsBlameless pins the registry entry the unknown verdict and an
// unnamed 4xx stand on: retried, never blamed, never paused. The registry's own fallback for an
// unrecognised error is retried but still counts against the endpoint's availability.
func TestGraphQLUnrecognizedErrorIsBlameless(t *testing.T) {
	classified := common.RouterErrorNodeUnrecognizedError
	require.True(t, classified.Retryable)
	require.False(t, classified.EndpointAtFault())
	require.True(t, classified.SubCategory.IsNodeCapability())
	require.False(t, classified.SubCategory.IsRateLimit())
}

// TestGraphQLReadStatusTable pins GraphQL's own status table (MAG-5121 slice 3): a read's reply
// outside 2xx whose body no row reads is filed by its status alone, never by the shared REST rows,
// whose body-text rows and status rows mean other things on other interfaces.
func TestGraphQLReadStatusTable(t *testing.T) {
	read := parsedRead(t)
	tests := []struct {
		name   string
		status int
		body   string
		want   *common.RouterError
		effect graphQLEffect
	}{
		// Any 4xx no row names: a refusal the router cannot read, asked of another provider.
		{"test 12: a 400 with a message", 400, `{"message":"bad"}`, common.RouterErrorNodeUnrecognizedError, effectElsewhere},
		{"test 12: a 400 with a JSON-RPC error body", 400, `{"jsonrpc":"2.0","id":null,"error":{"code":-32604,"message":"this request method is not supported"}}`, common.RouterErrorNodeUnrecognizedError, effectElsewhere},
		{"Change 7: a codeless GraphQL error under a 400", 400, `{"errors":[{"message":"Cannot query field"}]}`, common.RouterErrorNodeUnrecognizedError, effectElsewhere},
		{"test 12: a 413 with text", 413, `Payload Too Large`, common.RouterErrorNodeUnrecognizedError, effectElsewhere},
		{"test 12: a 415", 415, ``, common.RouterErrorNodeUnrecognizedError, effectElsewhere},
		{"test 12: a 422", 422, `{"message":"unprocessable"}`, common.RouterErrorNodeUnrecognizedError, effectElsewhere},
		{"a 409 is not a known transaction", 409, `conflict`, common.RouterErrorNodeUnrecognizedError, effectElsewhere},
		{"a 410 is not a pruned height", 410, `gone`, common.RouterErrorNodeUnrecognizedError, effectElsewhere},
		{"a 451 is a refusal the router cannot read", 451, ``, common.RouterErrorNodeUnrecognizedError, effectElsewhere},

		// GraphQL has one url: a missing route or a redirect says the url is wrong.
		{"a 404 is a wrong url, blamed", 404, `404 page not found`, common.RouterErrorNodeURLRefused, effectFault},
		{"a 405 is a wrong url, blamed", 405, `Method Not Allowed`, common.RouterErrorNodeURLRefused, effectFault},
		{"a 301 is a wrong url, blamed", 301, ``, common.RouterErrorNodeURLRefused, effectFault},
		{"a 304 is a wrong url, blamed", 304, ``, common.RouterErrorNodeURLRefused, effectFault},

		{"a 408 is a provider fault", 408, ``, common.RouterErrorNodeServiceUnavailable, effectFault},
		{"a 401 is a refusal", 401, ``, common.RouterErrorNodeAccessDenied, effectElsewhere},
		{"a 402 is a refusal", 402, ``, common.RouterErrorNodeAccessDenied, effectElsewhere},
		{"a 403 is a refusal", 403, ``, common.RouterErrorNodeAccessDenied, effectElsewhere},
		{"a 429 pauses", 429, ``, common.RouterErrorNodeRateLimited, effectThrottled},

		// 5xx: the body's words are not read.
		{"test 12: a 500 saying transaction not found is a provider fault", 500, `transaction not found`, common.RouterErrorNodeInternalError, effectFault},
		{"a 500 saying rate limit is a provider fault", 500, `rate limit exceeded`, common.RouterErrorNodeInternalError, effectFault},
		{"a 501 is tried on another provider", 501, ``, common.RouterErrorNodeUnimplemented, effectElsewhere},
		{"a 502", 502, ``, common.RouterErrorNodeBadGateway, effectFault},
		{"a 503", 503, ``, common.RouterErrorNodeServiceUnavailable, effectFault},
		{"a 504", 504, ``, common.RouterErrorNodeGatewayTimeout, effectFault},
		{"a 505 is a provider fault", 505, ``, common.RouterErrorNodeServerError, effectFault},
		{"a 520 is a provider fault", 520, ``, common.RouterErrorNodeServerError, effectFault},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, read.OwnClassification([]byte(test.body), test.status), "filed under %s", test.want.Name)
			require.Equal(t, test.effect, readEffect(t, read, test.body, test.status))
		})
	}
}

// TestGraphQLReadsNeverUseTheSharedRows: every read reply that is an error, under every status and
// body shape, is filed by GraphQL's own rows. A nil would send it to the shared REST rows.
func TestGraphQLReadsNeverUseTheSharedRows(t *testing.T) {
	read := parsedRead(t)
	bodies := []string{
		``, `text`, `<html>Too Many Requests</html>`, `{}`, `{"message":"transaction not found"}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}`,
		`{"errors":[{"message":"m"}]}`, `{"data":{"x":1}}`,
		`{"errors":[{"message":"m","extensions":{"code":"postgres-error"}}]}`,
	}
	for status := 100; status < 600; status++ {
		for _, body := range bodies {
			hasError, _ := read.CheckResponseError([]byte(body), status)
			if !hasError {
				continue
			}
			require.NotNil(t, read.OwnClassification([]byte(body), status), "status %d, body %q", status, body)
		}
	}
}
