package rpcInterfaceMessages

import (
	"bytes"
	"errors"
	"net/http"

	"github.com/goccy/go-json"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// The extensions codes the router answers a request it cannot read or run with. The first two are
// the codes a Sui GraphQL node uses for the same failures; BAD_REQUEST names a body that is not a
// GraphQL-over-HTTP request at all, which a Sui node refuses with a bare 400.
const (
	GraphQLParseFailedCode      = "GRAPHQL_PARSE_FAILED"
	GraphQLValidationFailedCode = "GRAPHQL_VALIDATION_FAILED"
	GraphQLBadRequestCode       = "BAD_REQUEST"
)

// GraphQLRequestError is a GraphQL request the router answers itself because it cannot tell what
// the request is (MAG-4351). Every operation on a GraphQL endpoint shares one path, so the router
// parses the document to find the root fields that name the request's api; a document it cannot
// parse, or an operation it cannot pick, never reaches a node.
//
// It is answered the way a node answers the same request, as a GraphQL response
// ({"data":null,"errors":[…]}), never as a 5xx: a GraphQL client reads the errors array, so the
// router's own {"error":…} body hid the mistake and its position behind what looked like a
// transport failure, and a client that retries a 5xx resent a request that can never succeed.
// The status follows Sui's GraphQL server (probed on graphql.mainnet.sui.io and
// mainnet.sui.rpcpool.com, 2026-10-18): 200 for a document it cannot read or run, 400 for a
// body that is not a GraphQL-over-HTTP request.
type GraphQLRequestError struct {
	// Message is errors[0].message, what the client reads.
	Message string
	// Code is errors[0].extensions.code. Empty means no extensions block, which is how Sui answers
	// an operation it cannot pick and a subscription.
	Code string
	// Locations are errors[0].locations, positions in the query document.
	Locations []GraphQLErrorLocation
	// HTTPStatus is the status the reply carries.
	HTTPStatus int
	// Err is the router's own description of the failure, the text logs and Error() carry.
	Err error
}

// GraphQLErrorLocation is a position in a GraphQL document, 1-based, as errors[].locations carries it.
type GraphQLErrorLocation struct {
	Line   int `json:"line"`
	Column int `json:"column"`
}

func (e *GraphQLRequestError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Message
}

func (e *GraphQLRequestError) Unwrap() error { return e.Err }

// Response is the GraphQL-over-HTTP body the request is answered with.
func (e *GraphQLRequestError) Response() []byte {
	type graphQLErrorExtensions struct {
		Code string `json:"code"`
	}
	type graphQLError struct {
		Message    string                  `json:"message"`
		Locations  []GraphQLErrorLocation  `json:"locations,omitempty"`
		Extensions *graphQLErrorExtensions `json:"extensions,omitempty"`
	}
	entry := graphQLError{Message: e.Message, Locations: e.Locations}
	if e.Code != "" {
		entry.Extensions = &graphQLErrorExtensions{Code: e.Code}
	}
	body, err := json.Marshal(struct {
		Data   *struct{}      `json:"data"`
		Errors []graphQLError `json:"errors"`
	}{Errors: []graphQLError{entry}})
	if err != nil {
		// Unreachable: every field is a string, an int or a slice of them.
		return []byte(`{"data":null,"errors":[{"message":"the request could not be read"}]}`)
	}
	return body
}

// newGraphQLBadRequestError is a body that is not a GraphQL-over-HTTP request: not JSON, JSON of
// another shape, or a batch. A Sui node answers each with a 400.
func newGraphQLBadRequestError(data []byte, cause error) *GraphQLRequestError {
	message := `The request body is not a JSON GraphQL request: it must be an object with a "query" string, and optionally an "operationName" string and a "variables" object.`
	if trimmed := bytes.TrimLeft(data, " \t\r\n"); len(trimmed) > 0 && trimmed[0] == '[' {
		message = "batched GraphQL requests are not supported: send one request per HTTP POST."
	}
	return &GraphQLRequestError{Message: message, Code: GraphQLBadRequestCode, HTTPStatus: http.StatusBadRequest, Err: cause}
}

// newGraphQLParseError is a document that does not parse, with the parser's own message and the
// position it stopped at.
func newGraphQLParseError(cause error) *GraphQLRequestError {
	requestError := &GraphQLRequestError{Message: cause.Error(), Code: GraphQLParseFailedCode, HTTPStatus: http.StatusOK, Err: cause}
	var parseError *gqlerror.Error
	if errors.As(cause, &parseError) {
		requestError.Message = parseError.Message
		for _, location := range parseError.Locations {
			requestError.Locations = append(requestError.Locations, GraphQLErrorLocation{Line: location.Line, Column: location.Column})
		}
	}
	return requestError
}

// newGraphQLNoDocumentError is a request with an empty or absent query. Sui reads it as an empty
// document and fails it at its first position.
func newGraphQLNoDocumentError(cause error) *GraphQLRequestError {
	return &GraphQLRequestError{
		Message:    "The request carries no GraphQL document.",
		Code:       GraphQLParseFailedCode,
		Locations:  []GraphQLErrorLocation{{Line: 1, Column: 1}},
		HTTPStatus: http.StatusOK,
		Err:        cause,
	}
}

// newGraphQLOperationError is a document that parses but whose operation cannot be picked or
// run, in the words Sui's server uses and, like it, without a code.
func newGraphQLOperationError(message string, cause error) *GraphQLRequestError {
	return &GraphQLRequestError{Message: message, HTTPStatus: http.StatusOK, Err: cause}
}

// fragmentSpreadLocation is where a fragment spread sits in the document, nil when the parser
// recorded no position. The parser positions a spread at its name; the spread itself starts at the
// "..." in front of it, which is where a GraphQL server reports it (Sui answers `{ ...Missing }`
// at column 3, not 6), so the position is moved back over the dots and any blanks between.
func fragmentSpreadLocation(spread *ast.FragmentSpread) *GraphQLErrorLocation {
	if spread == nil || spread.Position == nil {
		return nil
	}
	location := &GraphQLErrorLocation{Line: spread.Position.Line, Column: spread.Position.Column}
	if spread.Position.Src == nil {
		return location
	}
	input := []rune(spread.Position.Src.Input)
	start := spread.Position.Start
	if start <= 0 || start > len(input) {
		return location
	}
	dots := start
	for dots > 0 && (input[dots-1] == ' ' || input[dots-1] == '\t' || input[dots-1] == ',') {
		dots--
	}
	if dots >= 3 && string(input[dots-3:dots]) == "..." {
		location.Column -= start - (dots - 3)
	}
	return location
}

// newGraphQLCodedError is a document that cannot run for a reason that has a code: a fragment
// spread that cannot be resolved fails validation, at the spread.
func newGraphQLCodedError(code, message string, at *GraphQLErrorLocation, cause error) *GraphQLRequestError {
	requestError := &GraphQLRequestError{Message: message, Code: code, HTTPStatus: http.StatusOK, Err: cause}
	if at != nil {
		requestError.Locations = []GraphQLErrorLocation{*at}
	}
	return requestError
}
