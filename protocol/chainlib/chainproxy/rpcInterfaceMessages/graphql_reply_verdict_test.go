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
	classified := common.ClassifyError(nil, common.ChainFamilyUnknown, common.TransportREST, message.ClassificationStatus([]byte(body), status), errorMessage)
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
		// and Fuel node refuses it at the parse step (limit 64).
		{"GRAPHQL_PARSE_FAILED is the caller's", 200, `{"data":null,"errors":[{"message":"recursion limit exceeded.","extensions":{"code":"GRAPHQL_PARSE_FAILED"}}]}`, effectCaller},
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
		{"an errors entry that is not an object is still read", 200, `{"errors":["bad"]}`, effectElsewhere},
		{"a message that is not text is still read", 200, `{"errors":[{"message":{"text":"bad"},"extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, effectFault},

		// Quota text (Change 12): a vendor quota in words pauses the provider, as a 429 does.
		{"'Too many requests' with no code pauses the provider", 200, `{"errors":[{"message":"Too many requests"}]}`, effectThrottled},
		{"'rate limit exceeded' in any letter case pauses the provider", 200, `{"errors":[{"message":"Per anonymous IP RATE LIMIT EXCEEDED"}]}`, effectThrottled},
		{"a quota text beside data pauses the provider", 200, `{"data":{"a":null},"errors":[{"message":"too many requests","path":["a"]}]}`, effectThrottled},
		{"a quota text under an unmatched code pauses the provider", 200, `{"errors":[{"message":"Too Many Requests","extensions":{"code":"THROTTLED"}}]}`, effectThrottled},
		{"a quota text under a numeric code pauses the provider", 200, `{"errors":[{"message":"Too many requests","extensions":{"code":-32090}}]}`, effectThrottled},
		{"a quota text under a 400 pauses the provider", 400, `{"errors":[{"message":"rate limit exceeded"}]}`, effectThrottled},
		{"a quota text under a 503 stays a provider fault", 503, `{"errors":[{"message":"Too many requests"}]}`, effectFault},
		{"a real code beats a quota text", 200, `{"errors":[{"message":"Too many requests in the batch","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectCaller},
		{"'too many' alone is not a quota text", 200, `{"errors":[{"message":"too many keys supplied to multi-get"}]}`, effectElsewhere},
		{"'rate limit' alone is not a quota text", 200, `{"errors":[{"message":"Rate limit settings could not be loaded"}]}`, effectElsewhere},
		{"'quota' is not a quota text", 200, `{"errors":[{"message":"You've reached your monthly quota of Request Units"}]}`, effectElsewhere},
		{"a quota text past the scan limit is not read", 200, `{"errors":[{"message":"` + strings.Repeat("x", graphQLQuotaTextScanLimit) + ` too many requests"}]}`, effectElsewhere},

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
	require.Contains(t, message, graphQLNotAResponsePrefix)
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

// TestGraphQLQuotaTextCostsNothing pins that looking for a quota text allocates nothing, however
// long the message: only its first graphQLQuotaTextScanLimit bytes are read, in place.
func TestGraphQLQuotaTextCostsNothing(t *testing.T) {
	huge := strings.Repeat("a", 10<<20)
	allocations := testing.AllocsPerRun(10, func() {
		require.False(t, isGraphQLQuotaText(huge))
		require.True(t, isGraphQLQuotaText("TOO MANY REQUESTS"))
	})
	require.Zero(t, allocations)
}

// TestGraphQLRetryElsewhereStatusIsBlameless pins the registry row the unknown and elsewhere
// verdicts stand on: retried, never blamed, never paused. The registry's own fallback for an
// unrecognised error is retried but still counts against the endpoint's availability.
func TestGraphQLRetryElsewhereStatusIsBlameless(t *testing.T) {
	classified := common.ClassifyError(nil, common.ChainFamilyUnknown, common.TransportREST, graphQLRetryElsewhereStatus, "")
	require.True(t, classified.Retryable)
	require.False(t, classified.EndpointAtFault())
	require.True(t, classified.SubCategory.IsNodeCapability())
	require.False(t, classified.SubCategory.IsRateLimit())
}
