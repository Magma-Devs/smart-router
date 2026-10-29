package rpcInterfaceMessages

import (
	"errors"
	"fmt"
)

// ErrGraphQLRequestRefused marks a GraphQL request the router will not serve because it exceeds one
// of the router's OWN limits: MaxGraphQLQueryDepth, MaxGraphQLRootFields or
// MaxGraphQLFragmentNesting.
//
// These three are not the node's limits and not a fault of the document. The router sets them to
// bound what its own parse and per-operation pipeline cost (MAG-4359, MAG-4360), so it has to refuse
// them itself: forwarding one is the thing the limit exists to prevent. That is what separates them
// from a request the router cannot READ — an unparsable document, a batch, a missing operation —
// which MAG-4351 decided to forward to a node and answer with the node's own reply.
//
// The listener matches on it to answer with a client-error status and a GraphQL error document
// instead of the generic 500 a status-less error used to get (MAG-4577). Match with errors.As on
// *GraphQLRefusal rather than reading the message: the text is the client's, not an identity.
var ErrGraphQLRequestRefused = errors.New("graphql request refused by a router limit")

// GraphQLRefusal is one of those refusals. Its message is written for the caller and is what the
// listener puts in the GraphQL error document, so it names the limit and the number: the caller can
// only act on a refusal it can read.
//
// It carries its own message rather than letting the listener use err.Error(), because the error
// travels up through the relay path and anything that wraps it on the way would otherwise end up in
// the client's error document.
type GraphQLRefusal struct {
	message string
}

// RefuseGraphQLRequest builds a refusal whose message is formatted for the caller.
func RefuseGraphQLRequest(format string, args ...interface{}) *GraphQLRefusal {
	return &GraphQLRefusal{message: fmt.Sprintf(format, args...)}
}

func (r *GraphQLRefusal) Error() string { return r.message }

// Is makes errors.Is(err, ErrGraphQLRequestRefused) true for every refusal, so a caller can ask the
// family question without naming the type.
func (r *GraphQLRefusal) Is(target error) bool { return target == ErrGraphQLRequestRefused }

// ClientMessage is what the caller is told. Router-authored and constant apart from the limit's own
// number, so unlike an upstream error it carries nothing to redact.
func (r *GraphQLRefusal) ClientMessage() string { return r.message }

// GraphQLRefusalFrom returns the refusal in err's chain, or nil when err is not one.
func GraphQLRefusalFrom(err error) *GraphQLRefusal {
	var refusal *GraphQLRefusal
	if errors.As(err, &refusal) {
		return refusal
	}
	return nil
}
