package rpcInterfaceMessages

import (
	"errors"
	"fmt"

	"github.com/vektah/gqlparser/v2/gqlerror"
)

// ErrGraphQLRequestRefused marks a GraphQL request the router answers itself, with a client-error
// status and a GraphQL error document, and sends to no provider. Four families:
//
//   - A request past one of the router's OWN limits: MaxGraphQLQueryDepth, MaxGraphQLRootFields or
//     MaxGraphQLFragmentNesting. These are not the node's limits and not a fault of the document.
//     The router sets them to bound what its own parse and per-operation pipeline cost (MAG-4359,
//     MAG-4360), so it has to refuse them itself: forwarding one is the thing the limit exists to
//     prevent.
//   - A request the router and the node would read differently (MAG-4908): a body whose top-level
//     keys the two would read as two different requests (checkGraphQLRequestKeys), and a document
//     that holds a mutation beside another operation (refuseMixedMutationDocument). The node could
//     run a write the router did not classify as one.
//   - A request the router cannot READ (MAG-4351, the owner's decision of 2026-11-09): a body that
//     is not one JSON object, a member of the wrong type, no query, a query that does not parse, an
//     operation that cannot be selected, a fragment that is cyclic or undefined, a subscription.
//     The router cannot name the operation, so it has nothing to route, cache or cross-validate by;
//     every provider refuses the same document, so asking one spends a call and the vendor's quota
//     on an answer the router already has; and a document a provider's parser accepts where the
//     router's did not could hold a mutation, which a forward would run with read handling.
//   - A write of a shape the router does not send (MAG-5120, check Q8): a mutation that selects
//     more than one root field (refuseMultiRootMutation). The write path answers one write per
//     request, one copy per provider and one verdict, so a second root field would share the
//     first's verdict. Its reply, and the mixed document's, says Lava-Write-Outcome: not_sent.
//
// Every refusal carries an extensions.code: GRAPHQL_PARSE_FAILED for a query that does not parse,
// REQUEST_REFUSED for every other one (MAG-5120).
//
// The listener matches on it to answer with a client-error status and a GraphQL error document
// instead of the generic 500 a status-less error used to get (MAG-4577). Match with errors.As on
// *GraphQLRefusal rather than reading the message: the text is the client's, not an identity.
var ErrGraphQLRequestRefused = errors.New("graphql request refused by the router")

// The GraphQL error codes a refusal carries in its extensions.code (MAG-4351, MAG-5120). A client
// reads the code to tell why, the way it reads a node's.
const (
	// GraphQLParseFailedCode is a query the router's parser cannot read. It is the code Sui's own
	// server answers a parse failure with.
	GraphQLParseFailedCode = "GRAPHQL_PARSE_FAILED"
	// GraphQLRequestRefusedCode is every other refusal: a request the router cannot read (the body,
	// a member, the operation, a fragment, a subscription), one past the router's own limits, one
	// whose wrapper the router and the node would read differently, and a write of a shape the
	// router does not send (the GraphQL chapter's checks Q2 to Q8, MAG-5120).
	GraphQLRequestRefusedCode = "REQUEST_REFUSED"
)

// GraphQLWriteOutcomeNotSent is the Lava-Write-Outcome a refusal of a write's shape answers with
// (MAG-5120, check Q8): the router refused the write before any provider was asked, so the caller
// knows for certain that nothing ran and can fix the request and send it again. It is the
// "not_sent" of the write verdicts MAG-5122 answers with.
const GraphQLWriteOutcomeNotSent = "not_sent"

// GraphQLRefusal is one of those refusals. Its message is written for the caller and is what the
// listener puts in the GraphQL error document, so it names the limit and the number, or what the
// router could not read and where: the caller can only act on a refusal it can read.
//
// It carries its own message rather than letting the listener use err.Error(), because the error
// travels up through the relay path and anything that wraps it on the way would otherwise end up in
// the client's error document. For the same reason no refusal quotes a decoder's own error text:
// that names the router's Go types, and a refusal is sent unmasked.
type GraphQLRefusal struct {
	message string
	// code is the reply's extensions.code: GraphQLParseFailedCode for a query that does not parse,
	// GraphQLRequestRefusedCode for every other refusal (MAG-5120). The limits and MAG-4908's
	// refusals were answered without one until the GraphQL chapter gave every refusal a code.
	code string
	// writeNotSent marks the refusal of a write's shape (check Q8): its reply says
	// Lava-Write-Outcome: not_sent.
	writeNotSent bool
}

// RefuseGraphQLRequest builds a refusal whose message is formatted for the caller, with the code
// REQUEST_REFUSED: every refusal but a parse failure (MAG-5120).
func RefuseGraphQLRequest(format string, args ...interface{}) *GraphQLRefusal {
	return &GraphQLRefusal{message: fmt.Sprintf(format, args...), code: GraphQLRequestRefusedCode}
}

// refuseGraphQLWriteShape builds the refusal of a write the router does not send as it is (check Q8,
// MAG-5120): a document that holds a mutation beside another operation, or a mutation that selects
// more than one root field. Nothing reached a provider, so the reply says so (writeNotSent).
func refuseGraphQLWriteShape(format string, args ...interface{}) *GraphQLRefusal {
	refusal := RefuseGraphQLRequest(format, args...)
	refusal.writeNotSent = true
	return refusal
}

// refuseUnparsableGraphQLQuery builds the refusal of a query the parser cannot read (MAG-4351). The
// caller is told the parser's reason and where it stopped, read from the parser's error fields.
func refuseUnparsableGraphQLQuery(err error) *GraphQLRefusal {
	message := "failed parsing graphql query"
	var parseErr *gqlerror.Error
	if errors.As(err, &parseErr) {
		message += ": " + parseErr.Message
		if len(parseErr.Locations) > 0 {
			message += fmt.Sprintf(" (line %d, column %d)", parseErr.Locations[0].Line, parseErr.Locations[0].Column)
		}
	}
	return &GraphQLRefusal{message: message, code: GraphQLParseFailedCode}
}

func (r *GraphQLRefusal) Error() string { return r.message }

// Is makes errors.Is(err, ErrGraphQLRequestRefused) true for every refusal, so a caller can ask the
// family question without naming the type.
func (r *GraphQLRefusal) Is(target error) bool { return target == ErrGraphQLRequestRefused }

// ClientMessage is what the caller is told. Router-authored, apart from what it quotes of the
// caller's own request (a member name, an operation or fragment name, the token the parser stopped
// at), so unlike an upstream error it carries nothing to redact.
func (r *GraphQLRefusal) ClientMessage() string { return r.message }

// Code is the refusal's GraphQL error code for extensions.code.
func (r *GraphQLRefusal) Code() string { return r.code }

// WriteOutcome is the Lava-Write-Outcome the refusal's reply carries: GraphQLWriteOutcomeNotSent
// on the refusal of a write's shape, "" on every other refusal, which says nothing about a write.
func (r *GraphQLRefusal) WriteOutcome() string {
	if r.writeNotSent {
		return GraphQLWriteOutcomeNotSent
	}
	return ""
}

// GraphQLRefusalFrom returns the refusal in err's chain, or nil when err is not one.
func GraphQLRefusalFrom(err error) *GraphQLRefusal {
	var refusal *GraphQLRefusal
	if errors.As(err, &refusal) {
		return refusal
	}
	return nil
}
