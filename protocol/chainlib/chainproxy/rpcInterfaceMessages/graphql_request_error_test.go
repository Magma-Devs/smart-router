package rpcInterfaceMessages

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// parseRequestError parses body and returns the request error it must fail with.
func parseRequestError(t *testing.T, body string) *GraphQLRequestError {
	t.Helper()
	_, err := ParseGraphQLMsg([]byte(body))
	require.Error(t, err)
	var requestError *GraphQLRequestError
	require.True(t, errors.As(err, &requestError), "not a request error: %v", err)
	return requestError
}

// TestParseGraphQLMsgRequestErrors (MAG-4351): every way ParseGraphQLMsg can refuse a request is a
// GraphQLRequestError, answered with a GraphQL response and a non-5xx status, the way Sui's server
// answers the same request.
func TestParseGraphQLMsgRequestErrors(t *testing.T) {
	t.Run("a syntax error carries the parser's message and position", func(t *testing.T) {
		requestError := parseRequestError(t, `{"query":"{ checkpoint { sequenceNumber }"}`)
		require.Equal(t, http.StatusOK, requestError.HTTPStatus)
		require.Equal(t, GraphQLParseFailedCode, requestError.Code)
		require.Equal(t, []GraphQLErrorLocation{{Line: 1, Column: 32}}, requestError.Locations)
		require.Contains(t, requestError.Message, "Expected Name")
		require.NotContains(t, requestError.Message, "failed parsing", "the client reads the parser's words, not the router's")
		require.Contains(t, requestError.Error(), "failed parsing graphql query", "logs keep the router's description")
	})

	t.Run("an empty query fails at its first position", func(t *testing.T) {
		for _, body := range []string{`{"query":""}`, `{"query":"   "}`, `{"variables":{}}`, `{"query":null}`} {
			requestError := parseRequestError(t, body)
			require.Equal(t, http.StatusOK, requestError.HTTPStatus, body)
			require.Equal(t, GraphQLParseFailedCode, requestError.Code, body)
			require.Equal(t, []GraphQLErrorLocation{{Line: 1, Column: 1}}, requestError.Locations, body)
		}
	})

	t.Run("a body that is not a GraphQL request is a 400", func(t *testing.T) {
		for _, body := range []string{`{"query": `, ``, `{ chainIdentifier }`, `{"query":123}`, `{"query":"{ x }","variables":"x"}`, `"text"`} {
			requestError := parseRequestError(t, body)
			require.Equal(t, http.StatusBadRequest, requestError.HTTPStatus, body)
			require.Equal(t, GraphQLBadRequestCode, requestError.Code, body)
			require.NotContains(t, requestError.Message, "graphQLRequestEnvelope", "the client is not shown the router's Go types")
		}
	})

	t.Run("a batch is named as one", func(t *testing.T) {
		requestError := parseRequestError(t, ` [{"query":"{ chainIdentifier }"}]`)
		require.Equal(t, http.StatusBadRequest, requestError.HTTPStatus)
		require.Contains(t, requestError.Message, "batched GraphQL requests are not supported")
	})

	t.Run("an operation that cannot be picked is answered in Sui's words, without a code", func(t *testing.T) {
		for body, message := range map[string]string{
			`{"query":"query A { chainIdentifier } query B { chainIdentifier }"}`:  "Operation name required in request.",
			`{"query":"query A { chainIdentifier }","operationName":"Z"}`:          `Unknown operation named "Z"`,
			`{"query":"subscription { checkpoints { cursor } }"}`:                  "Subscriptions are not supported on this transport.",
			`{"query":"{ chainIdentifier }","operationName":"NotTheAnonymousOne"}`: `Unknown operation named "NotTheAnonymousOne"`,
		} {
			requestError := parseRequestError(t, body)
			require.Equal(t, http.StatusOK, requestError.HTTPStatus, body)
			require.Empty(t, requestError.Code, body)
			require.Equal(t, message, requestError.Message, body)
		}
	})

	t.Run("a document with no operation", func(t *testing.T) {
		requestError := parseRequestError(t, `{"query":"fragment A on Query { chainIdentifier }"}`)
		require.Equal(t, http.StatusOK, requestError.HTTPStatus)
		require.Equal(t, GraphQLParseFailedCode, requestError.Code)
		require.Equal(t, "document does not contain an operation", requestError.Message)
	})

	t.Run("a spread that cannot be resolved fails validation at the spread", func(t *testing.T) {
		undefined := parseRequestError(t, `{"query":"{ ...Missing }"}`)
		require.Equal(t, GraphQLValidationFailedCode, undefined.Code)
		require.Equal(t, []GraphQLErrorLocation{{Line: 1, Column: 3}}, undefined.Locations, "the spread starts at its dots")

		spaced := parseRequestError(t, `{"query":"{ ...  Missing }"}`)
		require.Equal(t, []GraphQLErrorLocation{{Line: 1, Column: 3}}, spaced.Locations)

		cyclic := parseRequestError(t, `{"query":"{ ...A } fragment A on Query { ...B } fragment B on Query { ...A }"}`)
		require.Equal(t, GraphQLValidationFailedCode, cyclic.Code)
		require.Equal(t, `Cannot spread fragment "A" within itself.`, cyclic.Message)
		require.Contains(t, cyclic.Error(), "cyclic")
	})
}

// TestGraphQLRequestErrorResponse pins the body: a GraphQL response with data null, one error, and
// extensions only when there is a code.
func TestGraphQLRequestErrorResponse(t *testing.T) {
	coded := &GraphQLRequestError{
		Message:    `Expected Name, found <EOF>`,
		Code:       GraphQLParseFailedCode,
		Locations:  []GraphQLErrorLocation{{Line: 1, Column: 32}},
		HTTPStatus: http.StatusOK,
	}
	require.JSONEq(t,
		`{"data":null,"errors":[{"message":"Expected Name, found <EOF>","locations":[{"line":1,"column":32}],"extensions":{"code":"GRAPHQL_PARSE_FAILED"}}]}`,
		string(coded.Response()))

	uncoded := &GraphQLRequestError{Message: "Operation name required in request.", HTTPStatus: http.StatusOK}
	require.JSONEq(t, `{"data":null,"errors":[{"message":"Operation name required in request."}]}`, string(uncoded.Response()))
}

// TestGraphQLRequestErrorSurvivesWrapping: the listener finds the request error with errors.As, so
// a caller that wraps it with %w must not hide it.
func TestGraphQLRequestErrorSurvivesWrapping(t *testing.T) {
	_, err := ParseGraphQLMsg([]byte(`{"query":"not graphql"}`))
	wrapped := fmt.Errorf("relay: %w", err)
	var requestError *GraphQLRequestError
	require.True(t, errors.As(wrapped, &requestError))
	require.Equal(t, GraphQLParseFailedCode, requestError.Code)
}
