package rpcInterfaceMessages

import (
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/lexer"
	gqlparser "github.com/vektah/gqlparser/v2/parser"
)

// MaxGraphQLQueryDepth is how deeply a GraphQL document may nest, counting every bracket that opens
// a level: selection sets, argument lists, list and object values, list types.
//
// gqlparser parses by recursion and sets no depth limit of its own, and a goroutine that runs out
// of stack ends the process: that is a fatal error, not a panic a recover() can catch. The limit
// keeps the parse to a small stack whatever the document holds (MAG-4359).
//
// It refuses nothing a node answers: Sui caps a query's depth at 20 (serviceConfig maxQueryDepth)
// and answers a selection nested 200 deep with GRAPHQL_PARSE_FAILED "recursion limit exceeded".
const MaxGraphQLQueryDepth = 256

// parseGraphQLQuery parses a GraphQL document once it is known to nest no deeper than
// MaxGraphQLQueryDepth.
//
// Both failures it returns are refusals, and they are different answers the caller must not merge:
// a document past the depth limit is refused by the ROUTER's own limit, with the limit's message and
// no code (MAG-4577), while a document the parser cannot read is the document's own fault, and says
// so with GRAPHQL_PARSE_FAILED (MAG-4351). Wrapping the first in "failed parsing graphql query" made
// the router's own limit look like a parse failure, which is the wrong message for the caller: the
// document may be perfectly readable, and the router declined to read it.
func parseGraphQLQuery(query string) (*ast.QueryDocument, error) {
	source := &ast.Source{Input: query}
	if err := checkGraphQLQueryDepth(source); err != nil {
		return nil, err
	}
	doc, err := gqlparser.ParseQuery(source)
	if err != nil {
		return nil, refuseUnparsableGraphQLQuery(err)
	}
	return doc, nil
}

// checkGraphQLQueryDepth refuses a document that nests deeper than MaxGraphQLQueryDepth. It reads
// the document with gqlparser's own lexer, which loops rather than recurses, so a bracket inside a
// string, a block string or a comment is not counted.
//
// The count matches the parser's recursion up to the first closing bracket of the wrong kind, and
// the parser stops at that bracket with a syntax error. A document the lexer cannot read is left to
// the parser, which stops at the same place with its own message.
func checkGraphQLQueryDepth(source *ast.Source) error {
	tokens := lexer.New(source)
	depth := 0
	for {
		token, err := tokens.ReadToken()
		if err != nil {
			return nil //nolint:nilerr // the parser stops at the same token and reports it in its own words
		}
		switch token.Kind {
		case lexer.EOF:
			return nil
		case lexer.BraceL, lexer.BracketL, lexer.ParenL:
			depth++
			if depth > MaxGraphQLQueryDepth {
				// A refusal, not a parse error: the document is readable, the router declines to
				// read it. The position is kept in the message, because it is the one thing that
				// tells the caller where to cut (MAG-4577).
				return RefuseGraphQLRequest("graphql query nests deeper than %d levels (line %d, column %d)",
					MaxGraphQLQueryDepth, token.Pos.Line, token.Pos.Column)
			}
		case lexer.BraceR, lexer.BracketR, lexer.ParenR:
			if depth > 0 {
				depth--
			}
		}
	}
}
